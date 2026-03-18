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
use clawfs::relay::{DedupStore, RelayWriteRequest, RelayWriteResponse, relay_commit_pipeline};

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
    advertise_url: Option<String>,
    discovery_prefix: String,
    enable_relay: bool,
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

    let state = AppState {
        registry: Arc::clone(&registry),
        dedup,
        jwt_secret: cli.jwt_secret.clone(),
        replica_id: replica_id.clone(),
        advertise_url: cli.advertise_url.clone(),
        discovery_prefix: discovery_prefix.clone(),
        enable_relay: cli.enable_relay,
    };

    // Build the object store used for discovery (using the discovery prefix).
    let discovery_config = build_discovery_config(&cli);
    let (discovery_store, _) =
        create_object_store(&discovery_config).context("create discovery object store")?;

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

    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        // ── Discovery scan ─────────────────────────────────────────────
        let volumes = discover_volumes(store.as_ref(), &discovery_prefix).await;

        // Load (or touch) contexts for discovered volumes.
        for volume in &volumes {
            // Load policy first; if invalid mark unhealthy but continue.
            let policy = match load_volume_policy(store.as_ref(), &volume.prefix).await {
                PolicyLoadResult::Ok(p) => p,
                PolicyLoadResult::Invalid(reason) => {
                    warn!(
                        volume_prefix = %volume.prefix,
                        reason = %reason,
                        "volume_policy_invalid"
                    );
                    // Still try to load with defaults so maintenance can run.
                    VolumeAcceleratorPolicy::default()
                }
            };

            let ctx = match registry.get_or_load(volume, policy).await {
                Ok(c) => c,
                Err(err) => {
                    warn!(
                        volume_prefix = %volume.prefix,
                        error = %err,
                        "volume_context_load_failed"
                    );
                    continue;
                }
            };

            if !enable_maintenance || !ctx.policy.maintenance_enabled {
                continue;
            }

            // ── Maintenance round ──────────────────────────────────────
            // At most one round per volume in-flight; skip if locked.
            let sem_permit = match sem.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    // Global concurrency cap hit; defer this volume.
                    continue;
                }
            };

            // Only dispatch if not already running (try-lock the relay/commit
            // mutex as a proxy for "in-flight maintenance").
            // We use a dedicated maintenance_running flag via try_lock on the
            // maintenance_schedule mutex.
            let schedule_guard = match ctx.maintenance_schedule.try_lock() {
                Ok(g) => g,
                Err(_) => {
                    // Another task is already running maintenance for this volume.
                    drop(sem_permit);
                    continue;
                }
            };
            // Drop immediately — we just checked; the actual run holds it.
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

                // Update health based on backlog.
                let health = if round.delta_backlog > 0 || round.segment_backlog > 0 {
                    VolumeHealth::HealthyBacklogged
                } else {
                    VolumeHealth::HealthyIdle
                };
                ctx2.set_health(health);
            });
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

    // JWT auth (same prefix-scoped contract as clawfs_relay_executor).
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

    // Scope check: request must be within the discovery prefix.
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

    // Load volume context.
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

    // Check relay is enabled for this volume.
    if !ctx.policy.relay_enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "relay not enabled for this volume"})),
        )
            .into_response();
    }

    // Serialize commits for this volume via the per-volume lock.
    // If another commit is in-flight, this awaits until it completes.
    let _commit_guard = ctx.relay_commit_lock.lock().await;

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
}
