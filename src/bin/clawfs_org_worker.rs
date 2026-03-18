//! Org-scoped hosted accelerator worker for ClawFS.
//!
//! This binary manages maintenance and relay commit authority for **all** ClawFS
//! volumes under a single organization discovery prefix.  Unlike the
//! per-volume `clawfs_maintenance_worker` and `clawfs_relay_executor` binaries
//! (which bind a single `--object-prefix` at startup), this worker discovers
//! volumes dynamically by scanning `<discovery-prefix>/*/metadata/superblock.bin`
//! and loads per-volume policy from
//! `<volume-prefix>/metadata/accelerator_policy.json`.
//!
//! # Compatibility
//!
//! The single-volume `clawfs_maintenance_worker` and `clawfs_relay_executor`
//! binaries remain fully supported for narrow deployments or rollback.  Operators
//! should use THIS binary only when a single process must manage many volumes
//! inside one organizational boundary.
//!
//! # HTTP API
//!
//! ```text
//! GET  /health                       → aggregate status
//! GET  /health/volumes               → per-volume list
//! GET  /health/volumes/:prefix       → single volume detail
//! POST /relay_write                  → relay write (when --enable-relay)
//! ```
//!
//! # Invariants
//!
//! - `--discovery-prefix` is required and is the sole scope boundary.
//!   The service NEVER operates outside this prefix.
//! - `--advertise-url` is required when `--enable-relay` is true.
//! - `--replica-id` is required for HA relay ownership; must be stable
//!   across restarts.
//! - Per-volume `shard_size` is read from the discovered superblock; no
//!   global CLI shard size applies to volume operations.
//! - Existing single-volume binaries are unchanged.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use clap::Parser;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use serde::Serialize;
use tokio::time::sleep;
use tracing::{info, warn};
use uuid::Uuid;

use clawfs::config::{Config, ObjectStoreProvider};
use clawfs::maintenance::{CompactionConfig, MaintenanceRoundContext, run_maintenance_round};
use clawfs::metadata::create_object_store;
use clawfs::org_policy::{PolicyLoadResult, VolumeAcceleratorPolicy, load_volume_policy};
use clawfs::org_registry::{DiscoveredVolume, OrgVolumeRegistry, VolumeHealth, normalize_prefix};
use clawfs::relay::{
    DEFAULT_RELAY_ATTEMPT_TIMEOUT, DedupStore, OwnershipAcquireResult, RelayWriteRequest,
    RelayWriteResponse, acquire_relay_ownership, relay_commit_pipeline,
};

// ── CLI ────────────────────────────────────────────────────────────────────

#[derive(Debug, Parser)]
#[command(
    name = "clawfs-org-worker",
    version,
    about = "Org-scoped hosted accelerator: maintenance and relay for many ClawFS volumes"
)]
struct Cli {
    // ── Object store ──────────────────────────────────────────────────────
    /// Object store provider.
    #[arg(long, value_enum, default_value_t = ObjectStoreProvider::Local)]
    object_provider: ObjectStoreProvider,

    /// Local store path (for `local` provider).
    #[arg(long)]
    store_path: Option<PathBuf>,

    /// Bucket name (for `aws` and `gcs` providers).
    #[arg(long)]
    bucket: Option<String>,

    /// AWS or GCS region.
    #[arg(long)]
    region: Option<String>,

    /// Custom object store endpoint URL.
    #[arg(long)]
    endpoint: Option<String>,

    // ── Discovery ─────────────────────────────────────────────────────────
    /// Org-scoped discovery root.  The worker scans this prefix for ClawFS
    /// volumes (identified by `<prefix>/metadata/superblock.bin`).
    /// The worker NEVER operates outside this boundary.
    #[arg(long)]
    discovery_prefix: String,

    /// How often to rescan for new/removed volumes (seconds).
    #[arg(long, default_value_t = 300)]
    scan_interval_secs: u64,

    /// Maximum number of simultaneously-loaded volume contexts.
    #[arg(long, default_value_t = 64)]
    max_active_volumes: usize,

    // ── Maintenance ───────────────────────────────────────────────────────
    /// Enable delta/segment compaction, checkpointing, and lifecycle cleanup.
    #[arg(long, default_value_t = true)]
    enable_maintenance: bool,

    /// Maximum concurrent per-volume maintenance rounds.
    #[arg(long, default_value_t = 4)]
    maintenance_concurrency: usize,

    /// How often to check for due maintenance work (seconds).
    #[arg(long, default_value_t = 30)]
    maintenance_poll_secs: u64,

    // ── Relay ─────────────────────────────────────────────────────────────
    /// Enable relay_write ownership and commit forwarding.
    #[arg(long, default_value_t = false)]
    enable_relay: bool,

    /// Listen address for the HTTP server (relay + health).
    #[arg(long, default_value = "0.0.0.0:8080")]
    relay_listen: String,

    /// Publicly reachable URL of this replica.  Required when
    /// `--enable-relay` is true.  Used in relay owner records so peer
    /// replicas can forward requests to this instance.
    #[arg(long)]
    advertise_url: Option<String>,

    /// Stable replica identity used in relay owner records and lease logs.
    /// Must be unique across all replicas and stable across restarts.
    /// Required when `--enable-relay` is true.
    #[arg(long)]
    replica_id: Option<String>,

    /// HMAC-SHA256 secret for validating relay JWT session tokens.
    /// When set, all `/relay_write` requests must include a valid Bearer
    /// token whose `prefix` claim matches the request's `volume_prefix`.
    /// When unset, requests are accepted without authentication (dev mode).
    #[arg(long, env = "CLAWFS_RELAY_JWT_SECRET")]
    jwt_secret: Option<String>,

    /// Idempotency key TTL (seconds).
    #[arg(long, default_value_t = 86_400)]
    dedup_ttl_secs: u64,

    // ── Misc ──────────────────────────────────────────────────────────────
    /// Log object store I/O at debug level.
    #[arg(long, default_value_t = false)]
    log_storage_io: bool,

    /// Local directory for temporary worker state (stage/cache/state files).
    #[arg(long, default_value = "/tmp/clawfs-org-worker")]
    state_dir: PathBuf,

    /// Stable client identity used in maintenance lease records.
    /// Defaults to a random UUID; for HA relay ownership use `--replica-id`.
    #[arg(long)]
    client_id: Option<String>,
}

// ── App state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
#[allow(dead_code)]
struct AppState {
    registry: Arc<OrgVolumeRegistry>,
    dedup: Arc<DedupStore>,
    jwt_secret: Option<String>,
    replica_id: String,
    /// Own HTTP relay endpoint (empty when relay is disabled).
    advertise_url: String,
    discovery_prefix: String,
    enable_relay: bool,
    /// Object store for reading per-volume relay_owner.json records.
    object_store: Arc<dyn object_store::ObjectStore>,
    /// HTTP client for peer-to-peer forwarding (connection-pooled).
    forward_client: reqwest::Client,
}

// ── Entry point ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    // Validate relay invariants.
    if cli.enable_relay {
        if cli.advertise_url.is_none() {
            bail!("--advertise-url is required when --enable-relay is set");
        }
        if cli.replica_id.is_none() {
            bail!("--replica-id is required when --enable-relay is set");
        }
    }

    let discovery_prefix = normalize_prefix(&cli.discovery_prefix);
    if discovery_prefix.is_empty() {
        bail!("--discovery-prefix must not be empty");
    }

    let client_id = cli
        .client_id
        .clone()
        .or_else(|| cli.replica_id.clone())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let replica_id = cli.replica_id.clone().unwrap_or_else(|| client_id.clone());

    std::fs::create_dir_all(&cli.state_dir)?;

    let handle = tokio::runtime::Handle::current();

    let registry = Arc::new(OrgVolumeRegistry::new(
        cli.max_active_volumes,
        3600, // idle eviction after 1 hour
        cli.state_dir.clone(),
        cli.object_provider,
        cli.bucket.clone(),
        cli.region.clone(),
        cli.endpoint.clone(),
        handle.clone(),
    ));

    let dedup = DedupStore::new(Duration::from_secs(cli.dedup_ttl_secs));

    // Periodic dedup eviction.
    {
        let dedup_clone = dedup.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(3600)).await;
                dedup_clone.evict_expired();
            }
        });
    }

    // Build the object store used for discovery and relay ownership checks.
    let discovery_config = build_discovery_config(&cli);
    let (discovery_store, _) =
        create_object_store(&discovery_config).context("create discovery object store")?;

    let forward_client = reqwest::Client::builder()
        .timeout(DEFAULT_RELAY_ATTEMPT_TIMEOUT)
        .build()
        .context("build forward HTTP client")?;

    let state = AppState {
        registry: Arc::clone(&registry),
        dedup,
        jwt_secret: cli.jwt_secret.clone(),
        replica_id: replica_id.clone(),
        advertise_url: cli.advertise_url.clone().unwrap_or_default(),
        discovery_prefix: discovery_prefix.clone(),
        enable_relay: cli.enable_relay,
        object_store: Arc::clone(&discovery_store),
        forward_client,
    };

    info!(
        discovery_prefix = %discovery_prefix,
        enable_maintenance = cli.enable_maintenance,
        enable_relay = cli.enable_relay,
        replica_id = %replica_id,
        "clawfs-org-worker starting"
    );

    // ── Graceful shutdown ─────────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    {
        let tx = shutdown_tx.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                info!("received SIGINT, shutting down");
                let _ = tx.send(true);
            }
        });
    }
    #[cfg(unix)]
    {
        let tx = shutdown_tx;
        tokio::spawn(async move {
            use tokio::signal::unix::{SignalKind, signal};
            if let Ok(mut s) = signal(SignalKind::terminate()) {
                s.recv().await;
                info!("received SIGTERM, shutting down");
                let _ = tx.send(true);
            }
        });
    }

    // ── HTTP server ───────────────────────────────────────────────────────
    let app = build_router(state);
    let listen_addr: SocketAddr = cli.relay_listen.parse().context("parse --relay-listen")?;
    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .context("bind relay listener")?;
    info!(listen = %listen_addr, "http server listening");
    let server = axum::serve(listener, app);

    // ── Discovery + maintenance loops ─────────────────────────────────────
    let registry2 = Arc::clone(&registry);
    let discovery_prefix2 = discovery_prefix.clone();
    let scan_interval = Duration::from_secs(cli.scan_interval_secs);
    let maint_poll = Duration::from_secs(cli.maintenance_poll_secs);
    let enable_maintenance = cli.enable_maintenance;
    let maintenance_concurrency = cli.maintenance_concurrency;
    let client_id2 = client_id.clone();

    tokio::spawn(async move {
        run_discovery_and_maintenance_loop(
            registry2,
            discovery_store,
            discovery_prefix2,
            scan_interval,
            maint_poll,
            enable_maintenance,
            maintenance_concurrency,
            client_id2,
            shutdown_rx,
        )
        .await;
    });

    server.await.context("http server error")?;
    info!("clawfs-org-worker stopped");
    Ok(())
}

// ── Discovery ──────────────────────────────────────────────────────────────

/// Discover ClawFS volumes under `discovery_prefix` by listing for
/// `<prefix>/metadata/superblock.bin` markers.
///
/// Uses `list_with_delimiter` to enumerate volume root prefixes efficiently
/// (one level deep at a time), then checks for the superblock marker.
/// The scope is strictly bounded to `discovery_prefix`.
pub async fn discover_volumes(
    store: &dyn ObjectStore,
    discovery_prefix: &str,
) -> Vec<DiscoveredVolume> {
    let mut found = Vec::new();

    // List one level of prefixes under the discovery root.
    let list_prefix = if discovery_prefix.is_empty() {
        None
    } else {
        Some(ObjectPath::from(format!("{discovery_prefix}/")))
    };

    let result = store.list_with_delimiter(list_prefix.as_ref()).await;
    let listing = match result {
        Ok(l) => l,
        Err(err) => {
            warn!(discovery_prefix = %discovery_prefix, error = %err, "discovery list_with_delimiter failed");
            return found;
        }
    };

    // Each common_prefix is a candidate volume root.
    for prefix_path in listing.common_prefixes {
        let candidate = prefix_path.as_ref().trim_matches('/').to_string();

        // Scope check: candidate must be under discovery_prefix.
        if !discovery_prefix.is_empty() && !candidate.starts_with(discovery_prefix) {
            warn!(
                candidate = %candidate,
                discovery_prefix = %discovery_prefix,
                "discovery_scope_violation_skipped"
            );
            continue;
        }

        // Normalize and validate the candidate prefix.
        let normalized = normalize_prefix(&candidate);
        if normalized.is_empty()
            || normalized
                .split('/')
                .any(|s| s.is_empty() || s == "." || s == "..")
        {
            warn!(candidate = %candidate, "discovery_invalid_prefix_skipped");
            continue;
        }

        // Verify the superblock marker exists.
        let superblock_key = format!("{normalized}/metadata/superblock.bin");
        match store.head(&ObjectPath::from(superblock_key.clone())).await {
            Ok(_) => {
                found.push(DiscoveredVolume { prefix: normalized });
            }
            Err(object_store::Error::NotFound { .. }) => {
                // Not a ClawFS volume root; skip silently.
            }
            Err(err) => {
                warn!(
                    candidate = %candidate,
                    superblock_key = %superblock_key,
                    error = %err,
                    "discovery_superblock_head_failed"
                );
            }
        }
    }

    info!(
        discovery_prefix = %discovery_prefix,
        volumes_found = found.len(),
        "discovery_scan_complete"
    );
    found
}

// ── Weighted round-robin scheduler ─────────────────────────────────────────

/// Fair weighted round-robin scheduler for per-volume maintenance dispatch.
///
/// Each volume participates in the schedule with a weight derived from its
/// observed backlog (`scheduler_weight()`).  A weight-1 idle volume appears
/// once per cycle; a weight-4 heavily-backlogged volume appears four times.
/// A cursor advances through the flat slot list across ticks so that no
/// volume is permanently starved: even idle volumes are serviced regularly.
///
/// Volume sets change dynamically as discovery finds new volumes.  The
/// scheduler rebuilds the slot list whenever the known volume set changes,
/// restarting the cursor from the beginning of the new cycle.
struct OrgScheduler {
    /// Cursor position into `slots`.
    cursor: usize,
    /// Flat list of volume prefixes, each repeated `weight` times.
    slots: Vec<String>,
    /// Sorted list of prefixes known to the current slot list (for detecting
    /// volume-set changes between discovery scans).
    known_prefixes: Vec<String>,
}

impl OrgScheduler {
    fn new() -> Self {
        Self {
            cursor: 0,
            slots: Vec::new(),
            known_prefixes: Vec::new(),
        }
    }

    /// Rebuild the slot list from (prefix, weight) pairs.
    ///
    /// Called when the volume set changes.  Volumes are ordered by prefix for
    /// stable, deterministic scheduling.  The cursor resets to 0.
    fn rebuild(&mut self, volumes: &[(String, u32)]) {
        // Sort by prefix for deterministic ordering.
        let mut sorted = volumes.to_vec();
        sorted.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        self.slots.clear();
        for (prefix, weight) in &sorted {
            for _ in 0..*weight {
                self.slots.push(prefix.clone());
            }
        }
        self.known_prefixes = sorted.into_iter().map(|(p, _)| p).collect();
        self.cursor = 0;
    }

    /// Return up to `n` volume prefixes for this tick, deduplicating across
    /// the same cycle so each prefix is dispatched at most once per tick.
    ///
    /// The cursor advances by the number of unique volumes returned.  When it
    /// wraps around the end of the slot list, it resets to 0 (new cycle).
    fn next_batch(&mut self, n: usize) -> Vec<String> {
        if self.slots.is_empty() {
            return Vec::new();
        }

        let total = self.slots.len();
        let mut result: Vec<String> = Vec::with_capacity(n);
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut steps = 0usize;

        while result.len() < n && steps < total {
            let idx = (self.cursor + steps) % total;
            let prefix = &self.slots[idx];
            if seen.insert(prefix.as_str()) {
                result.push(prefix.clone());
            }
            steps += 1;
        }

        // Advance cursor by total steps taken (including duplicates skipped).
        self.cursor = (self.cursor + steps) % total;
        result
    }

    /// Returns true if the volume set (sorted prefixes) has changed.
    fn volume_set_changed(&self, new_prefixes: &[String]) -> bool {
        self.known_prefixes != new_prefixes
    }
}

// ── Discovery + maintenance loop ───────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn run_discovery_and_maintenance_loop(
    registry: Arc<OrgVolumeRegistry>,
    store: Arc<dyn ObjectStore>,
    discovery_prefix: String,
    scan_interval: Duration,
    maint_poll: Duration,
    enable_maintenance: bool,
    maintenance_concurrency: usize,
    client_id: String,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let sem = Arc::new(tokio::sync::Semaphore::new(maintenance_concurrency));
    let compaction_config = CompactionConfig::default();
    let mut scheduler = OrgScheduler::new();

    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        // ── Phase 1: Discovery — load contexts for all discovered volumes ──
        let volumes = discover_volumes(store.as_ref(), &discovery_prefix).await;

        let mut loaded: Vec<Arc<clawfs::org_registry::VolumeContext>> = Vec::new();
        for volume in &volumes {
            // Load policy first; if invalid log and continue with defaults.
            let policy = match load_volume_policy(store.as_ref(), &volume.prefix).await {
                PolicyLoadResult::Ok(p) => p,
                PolicyLoadResult::Invalid(reason) => {
                    warn!(
                        volume_prefix = %volume.prefix,
                        reason = %reason,
                        "volume_policy_invalid"
                    );
                    VolumeAcceleratorPolicy::default()
                }
            };

            match registry.get_or_load(volume, policy).await {
                Ok(ctx) => loaded.push(ctx),
                Err(err) => {
                    warn!(
                        volume_prefix = %volume.prefix,
                        error = %err,
                        "volume_context_load_failed"
                    );
                }
            }
        }

        // ── Phase 2: Rebuild scheduler when volume set changes ────────────
        if enable_maintenance {
            let mut sorted_prefixes: Vec<String> = loaded
                .iter()
                .filter(|c| c.policy.maintenance_enabled)
                .map(|c| c.prefix.clone())
                .collect();
            sorted_prefixes.sort_unstable();

            if scheduler.volume_set_changed(&sorted_prefixes) {
                // Build (prefix, weight) pairs from loaded contexts.
                let weighted: Vec<(String, u32)> = loaded
                    .iter()
                    .filter(|c| c.policy.maintenance_enabled)
                    .map(|c| (c.prefix.clone(), c.scheduler_weight()))
                    .collect();
                scheduler.rebuild(&weighted);
                info!(
                    volume_count = weighted.len(),
                    "maintenance_scheduler_rebuilt"
                );
            }

            // ── Phase 3: Dispatch maintenance for this tick's batch ───────
            let batch_prefixes = scheduler.next_batch(maintenance_concurrency);

            for prefix in batch_prefixes {
                let ctx = match loaded.iter().find(|c| c.prefix == prefix) {
                    Some(c) => Arc::clone(c),
                    None => continue,
                };

                // Skip unhealthy volumes.
                if !ctx.health().is_healthy() {
                    continue;
                }

                // Acquire global concurrency permit (non-blocking).
                let sem_permit = match sem.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        // All slots busy this tick; defer remaining batch.
                        break;
                    }
                };

                // Enforce per-volume exclusivity: skip if already in-flight.
                let schedule_guard = match ctx.maintenance_schedule.try_lock() {
                    Ok(g) => g,
                    Err(_) => {
                        drop(sem_permit);
                        continue;
                    }
                };
                drop(schedule_guard);

                let ctx2 = Arc::clone(&ctx);
                let client_id2 = client_id.clone();
                let cc2 = compaction_config;

                tokio::spawn(async move {
                    let _permit = sem_permit;
                    let mut schedule = ctx2.maintenance_schedule.lock().await;
                    let checkpoint_config = ctx2.policy.checkpoint_config();
                    let lifecycle_policy = ctx2.policy.lifecycle_policy(ctx2.prefix.clone());

                    let round = run_maintenance_round(MaintenanceRoundContext {
                        metadata: &ctx2.metadata,
                        segments: &ctx2.segments,
                        superblock: &ctx2.superblock,
                        client_id: &client_id2,
                        compaction_config: &cc2,
                        checkpoint_config: checkpoint_config.as_ref(),
                        lifecycle_policy: lifecycle_policy.as_ref(),
                        schedule: &mut schedule,
                    })
                    .await;

                    // Store backlog score for the next scheduling cycle.
                    let backlog = (round.delta_backlog + round.segment_backlog) as u32;
                    ctx2.last_backlog_score
                        .store(backlog, std::sync::atomic::Ordering::Relaxed);

                    // Update health.
                    let health = if backlog > 0 {
                        VolumeHealth::HealthyBacklogged
                    } else {
                        VolumeHealth::HealthyIdle
                    };
                    ctx2.set_health(health);
                });
            }
        }

        // Evict idle contexts periodically.
        registry.maybe_evict();

        // Sleep for the shorter of scan_interval and maint_poll.
        let sleep_dur = scan_interval.min(maint_poll);
        tokio::select! {
            _ = sleep(sleep_dur) => {}
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() { break; }
            }
        }
    }
}

// ── HTTP router ────────────────────────────────────────────────────────────

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handle_health))
        .route("/health/volumes", get(handle_health_volumes))
        .route("/health/volumes/{prefix}", get(handle_health_volume))
        .route("/relay_write", post(handle_relay_write))
        .with_state(state)
}

// ── Health handlers ────────────────────────────────────────────────────────

#[derive(Serialize)]
struct AggregateHealth {
    status: &'static str,
    replica_id: String,
    discovery_prefix: String,
    discovered_volumes: usize,
    active_contexts: usize,
    unhealthy_count: usize,
}

async fn handle_health(State(state): State<AppState>) -> impl IntoResponse {
    let prefixes = state.registry.loaded_prefixes();
    let active = prefixes.len();
    let unhealthy = prefixes
        .iter()
        .filter_map(|p| state.registry.get_if_loaded(p))
        .filter(|ctx| !ctx.health().is_healthy())
        .count();

    Json(AggregateHealth {
        status: "ok",
        replica_id: state.replica_id.clone(),
        discovery_prefix: state.discovery_prefix.clone(),
        discovered_volumes: active,
        active_contexts: active,
        unhealthy_count: unhealthy,
    })
}

#[derive(Serialize)]
struct VolumeHealthSummary {
    prefix: String,
    health: String,
    relay_enabled: bool,
    maintenance_enabled: bool,
}

async fn handle_health_volumes(State(state): State<AppState>) -> impl IntoResponse {
    let summaries: Vec<VolumeHealthSummary> = state
        .registry
        .loaded_prefixes()
        .into_iter()
        .filter_map(|prefix| {
            state
                .registry
                .get_if_loaded(&prefix)
                .map(|ctx| VolumeHealthSummary {
                    health: ctx.health().as_str().to_string(),
                    relay_enabled: ctx.policy.relay_enabled,
                    maintenance_enabled: ctx.policy.maintenance_enabled,
                    prefix,
                })
        })
        .collect();

    Json(summaries)
}

async fn handle_health_volume(
    State(state): State<AppState>,
    Path(prefix): Path<String>,
) -> impl IntoResponse {
    let normalized = normalize_prefix(&prefix);
    match state.registry.get_if_loaded(&normalized) {
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "volume not loaded"})),
        )
            .into_response(),
        Some(ctx) => {
            let generation = ctx.superblock.snapshot().generation;
            Json(serde_json::json!({
                "prefix": ctx.prefix,
                "health": ctx.health().as_str(),
                "generation": generation,
                "shard_size": ctx.shard_size,
                "relay_enabled": ctx.policy.relay_enabled,
                "maintenance_enabled": ctx.policy.maintenance_enabled,
            }))
            .into_response()
        }
    }
}

// ── Relay handler ──────────────────────────────────────────────────────────

/// Maximum number of hops before a forwarded relay request is rejected.
/// Prevents infinite forwarding loops caused by misconfigured peer sets.
const MAX_RELAY_HOP_COUNT: u8 = 2;

/// Header used to carry the forwarding hop count between org-worker replicas.
const RELAY_HOP_HEADER: &str = "x-relay-hop-count";

/// Header advertising the forwarding replica's identity for diagnostics.
const RELAY_FORWARDED_BY_HEADER: &str = "x-relay-forwarded-by";

async fn handle_relay_write(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RelayWriteRequest>,
) -> impl IntoResponse {
    if !state.enable_relay {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "relay not enabled on this worker"})),
        )
            .into_response();
    }

    // ── Loop-protection: reject over-hopped forwarded requests ───────────
    let hop_count: u8 = headers
        .get(RELAY_HOP_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if hop_count >= MAX_RELAY_HOP_COUNT {
        warn!(
            hop_count,
            volume_prefix = %request.volume_prefix,
            "relay_write hop limit exceeded"
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "relay hop limit exceeded"})),
        )
            .into_response();
    }

    // ── JWT auth ─────────────────────────────────────────────────────────
    if let Some(secret) = &state.jwt_secret
        && let Err(reason) = validate_relay_token(&headers, &request.volume_prefix, secret)
    {
        warn!(reason = %reason, "relay_write auth rejected");
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": reason})),
        )
            .into_response();
    }

    // ── Scope check ───────────────────────────────────────────────────────
    let normalized_vol = normalize_prefix(&request.volume_prefix);
    if !state.discovery_prefix.is_empty() && !normalized_vol.starts_with(&state.discovery_prefix) {
        warn!(
            volume_prefix = %request.volume_prefix,
            discovery_prefix = %state.discovery_prefix,
            "relay_write volume out of scope"
        );
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "volume not in org discovery scope"})),
        )
            .into_response();
    }

    // ── Load volume context ───────────────────────────────────────────────
    let volume = DiscoveredVolume {
        prefix: normalized_vol.clone(),
    };
    let ctx = match state
        .registry
        .get_or_load(&volume, VolumeAcceleratorPolicy::default())
        .await
    {
        Ok(c) => c,
        Err(err) => {
            warn!(volume_prefix = %normalized_vol, error = %err, "relay_write context load failed");
            let response = RelayWriteResponse {
                status: clawfs::relay::RelayStatus::Failed,
                committed_generation: None,
                idempotency_key: request.idempotency_key,
                error: Some(format!("volume context unavailable: {err}")),
                actual_generation: None,
            };
            return (StatusCode::SERVICE_UNAVAILABLE, Json(response)).into_response();
        }
    };

    // ── Per-volume relay policy check ─────────────────────────────────────
    if !ctx.policy.relay_enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "relay not enabled for this volume"})),
        )
            .into_response();
    }

    // ── Ownership check ───────────────────────────────────────────────────
    // Attempt to acquire or confirm ownership.  This either confirms we hold
    // the lease (AlreadyOwned), freshly grants it (Acquired), or reveals who
    // the current owner is (OwnedByOther) so we can forward the request.
    let ownership = match acquire_relay_ownership(
        state.object_store.as_ref(),
        &normalized_vol,
        &state.replica_id,
        &state.advertise_url,
    )
    .await
    {
        Ok(r) => r,
        Err(err) => {
            warn!(error = %err, volume_prefix = %normalized_vol, "relay ownership check failed");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": format!("ownership check failed: {err}")})),
            )
                .into_response();
        }
    };

    match ownership {
        // ── We own the volume — process locally ──────────────────────────
        OwnershipAcquireResult::Acquired { record, .. }
        | OwnershipAcquireResult::AlreadyOwned { record, .. } => {
            process_relay_write_locally(state, ctx, record.epoch, request).await
        }

        // ── Another replica owns the volume — forward ─────────────────────
        OwnershipAcquireResult::OwnedByOther(owner) => {
            forward_relay_write(
                &state.forward_client,
                &owner.advertise_url,
                &headers,
                &request,
                hop_count + 1,
            )
            .await
        }
    }
}

/// Process a relay write request as the volume owner.
///
/// Applies the admission gate, acquires the per-volume commit lock, verifies
/// we still own the volume (lease-loss guard), reloads authoritative generation,
/// and runs `relay_commit_pipeline`.
async fn process_relay_write_locally(
    state: AppState,
    ctx: Arc<clawfs::org_registry::VolumeContext>,
    owner_epoch: u64,
    request: RelayWriteRequest,
) -> axum::response::Response {
    let normalized_vol = ctx.prefix.clone();

    // ── Admission gate: bound the relay request queue depth ──────────────
    // If the semaphore is exhausted (>= DEFAULT_RELAY_QUEUE_DEPTH requests
    // already waiting), reject immediately with 503 rather than queuing
    // without bound.
    let _admit_permit = match ctx.relay_admit_sem.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            warn!(
                volume_prefix = %normalized_vol,
                "relay queue full — returning 503"
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::http::HeaderMap::from_iter([(
                    axum::http::header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("5"),
                )]),
                Json(serde_json::json!({"error": "relay queue full, retry later"})),
            )
                .into_response();
        }
    };

    // ── Per-volume serialization: one commit pipeline at a time ──────────
    // The commit lock ensures requests that cleared the admission gate are
    // processed one-at-a-time in FIFO order for this volume.
    let _commit_guard = ctx.relay_commit_lock.lock().await;

    // ── Lease-loss guard: verify we still own the volume ─────────────────
    // The time between admission and lock acquisition may span TTL/2 in the
    // worst case.  Re-check ownership so a replica that lost its lease while
    // queued doesn't commit on behalf of the new owner.
    match acquire_relay_ownership(
        state.object_store.as_ref(),
        &normalized_vol,
        &state.replica_id,
        &state.advertise_url,
    )
    .await
    {
        Ok(OwnershipAcquireResult::Acquired { record, .. })
        | Ok(OwnershipAcquireResult::AlreadyOwned { record, .. })
            if record.epoch == owner_epoch =>
        {
            // Still our epoch — proceed.
        }
        _ => {
            // Lease lost while queued.  Caller should retry against new owner.
            warn!(volume_prefix = %normalized_vol, "relay lease lost while queued");
            let response = RelayWriteResponse {
                status: clawfs::relay::RelayStatus::Failed,
                committed_generation: None,
                idempotency_key: request.idempotency_key,
                error: Some("relay ownership lost while request was queued; retry".to_string()),
                actual_generation: None,
            };
            return (StatusCode::SERVICE_UNAVAILABLE, Json(response)).into_response();
        }
    }

    match relay_commit_pipeline(
        &request,
        &ctx.metadata,
        &ctx.segments,
        &ctx.superblock,
        &state.dedup,
        ctx.shard_size,
    )
    .await
    {
        Ok(response) => {
            let status = match response.status {
                clawfs::relay::RelayStatus::Failed => StatusCode::CONFLICT,
                _ => StatusCode::OK,
            };
            (status, Json(response)).into_response()
        }
        Err(err) => {
            warn!(error = %err, volume_prefix = %normalized_vol, "relay_write internal error");
            let response = RelayWriteResponse {
                status: clawfs::relay::RelayStatus::Failed,
                committed_generation: None,
                idempotency_key: request.idempotency_key,
                error: Some(format!("internal error: {err}")),
                actual_generation: None,
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// Forward a relay write request to the current volume owner.
///
/// Copies the Authorization header and request body, increments the hop
/// counter, and POSTs to `{owner_url}/relay_write`.  On failure returns a
/// retryable 503 so the client can retry after a backoff.
async fn forward_relay_write(
    client: &reqwest::Client,
    owner_url: &str,
    original_headers: &HeaderMap,
    request: &RelayWriteRequest,
    new_hop_count: u8,
) -> axum::response::Response {
    let target = format!("{}/{}", owner_url.trim_end_matches('/'), "relay_write");

    let mut builder = client.post(&target).json(request);

    // Propagate the Authorization header.
    if let Some(auth) = original_headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(v) = auth.to_str()
    {
        builder = builder.header(reqwest::header::AUTHORIZATION, v);
    }

    // Increment hop count and advertise the forwarding replica.
    builder = builder
        .header(RELAY_HOP_HEADER, new_hop_count.to_string())
        .header(RELAY_FORWARDED_BY_HEADER, "org-worker");

    match builder.send().await {
        Ok(resp) => {
            let status = resp.status();
            match resp.json::<RelayWriteResponse>().await {
                Ok(body) => (
                    StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                    Json(body),
                )
                    .into_response(),
                Err(err) => {
                    warn!(error = %err, target = %target, "relay forward response parse error");
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(serde_json::json!({"error": "invalid response from owner"})),
                    )
                        .into_response()
                }
            }
        }
        Err(err) => {
            warn!(error = %err, target = %target, "relay forward request failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": format!("forward failed: {err}")})),
            )
                .into_response()
        }
    }
}

// ── JWT validation ─────────────────────────────────────────────────────────
// Reuse the same approach as clawfs_relay_executor: validate HS256 JWT with a
// `prefix` claim that must match the request's volume_prefix exactly.

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RelayClaims {
    prefix: String,
}

fn validate_relay_token(
    headers: &HeaderMap,
    volume_prefix: &str,
    secret: &str,
) -> Result<(), String> {
    let auth = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| "Missing Authorization header".to_string())?;
    let token = auth
        .strip_prefix("Bearer ")
        .ok_or_else(|| "Authorization header must use Bearer scheme".to_string())?;

    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    validation.leeway = 0;
    let token_data = decode::<RelayClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map_err(|e| format!("JWT validation failed: {e}"))?;

    if token_data.claims.prefix != volume_prefix {
        return Err(format!(
            "JWT prefix '{}' does not match request volume_prefix '{}'",
            token_data.claims.prefix, volume_prefix
        ));
    }
    Ok(())
}

// ── Config builder ─────────────────────────────────────────────────────────

fn build_discovery_config(cli: &Cli) -> Config {
    let store_path = cli
        .store_path
        .clone()
        .unwrap_or_else(|| cli.state_dir.join("discovery-store"));
    Config {
        log_storage_io: cli.log_storage_io,
        inline_threshold: 4096,
        shard_size: 2048,
        inode_batch: 128,
        segment_batch: 128,
        pending_bytes: 1024 * 1024,
        entry_ttl_secs: 10,
        disable_journal: true,
        flush_interval_ms: 0,
        disable_cleanup: true,
        lookup_cache_ttl_ms: 0,
        dir_cache_ttl_ms: 0,
        metadata_poll_interval_ms: 0,
        segment_cache_bytes: 64 * 1024 * 1024,
        imap_delta_batch: 32,
        fuse_threads: 0,
        accelerator_mode: None,
        accelerator_endpoint: None,
        accelerator_fallback_policy: None,
        relay_fallback_policy: None,
        object_provider: cli.object_provider,
        bucket: cli.bucket.clone(),
        region: cli.region.clone(),
        endpoint: cli.endpoint.clone(),
        // The discovery config uses the discovery prefix as its object_prefix
        // so `create_object_store` builds the right store root.
        object_prefix: cli.discovery_prefix.trim_matches('/').to_string(),
        ..Config::with_paths(
            PathBuf::from("/tmp/clawfs-org-mnt"),
            store_path,
            cli.state_dir.join("discovery-cache"),
            cli.state_dir.join("discovery-state.bin"),
        )
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_defaults_are_reasonable() {
        // Parse a minimal CLI to verify defaults.
        let cli =
            Cli::try_parse_from(["clawfs-org-worker", "--discovery-prefix", "orgs/myorg"]).unwrap();

        assert_eq!(cli.scan_interval_secs, 300);
        assert_eq!(cli.max_active_volumes, 64);
        assert!(cli.enable_maintenance);
        assert!(!cli.enable_relay);
        assert_eq!(cli.maintenance_concurrency, 4);
        assert_eq!(cli.relay_listen, "0.0.0.0:8080");
    }

    #[test]
    fn discovery_prefix_normalization_round_trips() {
        assert_eq!(normalize_prefix("/orgs/myorg/"), "orgs/myorg");
        assert_eq!(normalize_prefix("orgs/myorg"), "orgs/myorg");
    }

    // ── OrgScheduler unit tests ───────────────────────────────────────────

    #[test]
    fn scheduler_empty_returns_empty_batch() {
        let mut s = OrgScheduler::new();
        assert!(s.next_batch(4).is_empty());
    }

    #[test]
    fn scheduler_single_volume_always_returned() {
        let mut s = OrgScheduler::new();
        s.rebuild(&[("vol1".to_string(), 1)]);
        let b = s.next_batch(4);
        assert_eq!(b, vec!["vol1"]);
    }

    #[test]
    fn scheduler_deduplicates_within_batch() {
        // A volume with weight=4 should appear only once per batch even though
        // it occupies 4 slots.
        let mut s = OrgScheduler::new();
        s.rebuild(&[("vol1".to_string(), 4)]);
        let b = s.next_batch(4);
        assert_eq!(b.len(), 1, "dedup should collapse weight=4 to 1 per tick");
        assert_eq!(b[0], "vol1");
    }

    #[test]
    fn scheduler_cursor_advances_across_ticks() {
        // 3 volumes, weight 1 each.  With batch size 2, tick 1 returns
        // [vol_a, vol_b], tick 2 returns [vol_c, vol_a], tick 3 returns
        // [vol_b, vol_c], etc.
        let mut s = OrgScheduler::new();
        s.rebuild(&[
            ("vol_a".to_string(), 1),
            ("vol_b".to_string(), 1),
            ("vol_c".to_string(), 1),
        ]);

        let t1 = s.next_batch(2);
        let t2 = s.next_batch(2);
        let t3 = s.next_batch(2);

        // Each tick advances the cursor; all three volumes appear across
        // three ticks with no permanent starvation.
        let all: Vec<&str> = t1
            .iter()
            .chain(t2.iter())
            .chain(t3.iter())
            .map(|s| s.as_str())
            .collect();
        assert!(all.contains(&"vol_a"), "vol_a must be scheduled");
        assert!(all.contains(&"vol_b"), "vol_b must be scheduled");
        assert!(all.contains(&"vol_c"), "vol_c must be scheduled");
    }

    #[test]
    fn scheduler_backlogged_volume_appears_more_often_per_cycle() {
        // vol_hot has weight=4, vol_cold has weight=1.
        // In a full cycle of 5 slots, vol_hot occupies 4 and vol_cold 1.
        let mut s = OrgScheduler::new();
        s.rebuild(&[("vol_cold".to_string(), 1), ("vol_hot".to_string(), 4)]);
        assert_eq!(s.slots.len(), 5, "total cycle length = 1+4=5");

        // Drain the full cycle in batches of 1.
        let mut visit_hot = 0usize;
        let mut visit_cold = 0usize;
        for _ in 0..5 {
            let b = s.next_batch(1);
            for p in &b {
                if p == "vol_hot" {
                    visit_hot += 1;
                } else {
                    visit_cold += 1;
                }
            }
        }
        // vol_hot gets ~4x the slot opportunities vs vol_cold across 5 ticks.
        assert!(
            visit_hot >= visit_cold,
            "backlogged volume should have more slots"
        );
    }

    #[test]
    fn scheduler_rebuilds_when_volume_set_changes() {
        let mut s = OrgScheduler::new();
        s.rebuild(&[("vol_a".to_string(), 1), ("vol_b".to_string(), 1)]);

        // Simulate discovery of a new volume.
        let new_prefixes: Vec<String> = vec![
            "vol_a".to_string(),
            "vol_b".to_string(),
            "vol_c".to_string(),
        ];
        assert!(s.volume_set_changed(&new_prefixes));

        s.rebuild(&[
            ("vol_a".to_string(), 1),
            ("vol_b".to_string(), 1),
            ("vol_c".to_string(), 1),
        ]);
        assert!(!s.volume_set_changed(&new_prefixes));
    }

    #[test]
    fn scheduler_cursor_wraps_correctly() {
        // 2 volumes, cursor should wrap around after a full cycle.
        let mut s = OrgScheduler::new();
        s.rebuild(&[("a".to_string(), 1), ("b".to_string(), 1)]);

        // Full cycle.
        let _t1 = s.next_batch(1); // cursor advances by 1
        let _t2 = s.next_batch(1); // cursor advances by 1, wraps to 0

        // After full cycle cursor is back at start; next batch starts from a.
        let t3 = s.next_batch(1);
        assert_eq!(t3, vec!["a"]);
    }
}
