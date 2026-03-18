//! Relay write executor for ClawFS managed volumes.
//!
//! This binary runs near the object store and accepts relay_write requests from
//! clients, executes the commit pipeline in-region, and returns committed
//! generation numbers.  Multiple executor instances are safe: generation commits
//! are CAS-based, and an idempotency dedup store prevents double-commits on
//! retried requests.
//!
//! HTTP API:
//!   POST /relay_write  body: RelayWriteRequest JSON → RelayWriteResponse JSON
//!   GET  /health       → {"status":"ok","generation":<n>}

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use tracing::{info, warn};

use clawfs::config::{Config, ObjectStoreProvider};
use clawfs::metadata::MetadataStore;
use clawfs::relay::{DedupStore, RelayWriteRequest, RelayWriteResponse, relay_commit_pipeline};
use clawfs::segment::{SegmentManager, segment_prefix};
use clawfs::superblock::SuperblockManager;

/// Default idempotency key TTL: 24 hours.
const DEFAULT_DEDUP_TTL_SECS: u64 = 86_400;

#[derive(Debug, Parser)]
#[command(
    name = "clawfs-relay-executor",
    version,
    about = "Relay write executor for ClawFS managed volumes"
)]
struct Cli {
    /// Object store provider.
    #[arg(long, value_enum, default_value_t = ObjectStoreProvider::Local)]
    object_provider: ObjectStoreProvider,

    /// Local store path (for `local` provider).
    #[arg(long)]
    store_path: Option<PathBuf>,

    /// Bucket name (for `aws` and `gcs` providers).
    #[arg(long)]
    bucket: Option<String>,

    /// Object prefix within the bucket (e.g. `volumes/my-volume`).
    #[arg(long, default_value = "")]
    object_prefix: String,

    /// AWS or GCS region.
    #[arg(long)]
    region: Option<String>,

    /// Custom object store endpoint URL.
    #[arg(long)]
    endpoint: Option<String>,

    /// Superblock shard size (must match the volume's existing shard geometry).
    #[arg(long, default_value_t = 2048)]
    shard_size: u64,

    /// Listen address for the HTTP server.
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: String,

    /// Idempotency key TTL in seconds.
    #[arg(long, default_value_t = DEFAULT_DEDUP_TTL_SECS)]
    dedup_ttl_secs: u64,

    /// Log object store I/O at debug level.
    #[arg(long, default_value_t = false)]
    log_storage_io: bool,
}

#[derive(Clone)]
struct AppState {
    metadata: Arc<MetadataStore>,
    segments: Arc<SegmentManager>,
    superblock: Arc<SuperblockManager>,
    dedup: Arc<DedupStore>,
    shard_size: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let config = build_config(&cli);

    let handle = tokio::runtime::Handle::current();
    let (store, meta_prefix) = clawfs::metadata::create_object_store(&config)?;
    let seg_prefix = segment_prefix(&config.object_prefix);

    let metadata = Arc::new(
        MetadataStore::new_with_store(store.clone(), meta_prefix, &config, handle.clone()).await?,
    );
    let segments = Arc::new(SegmentManager::new_with_store(
        store, seg_prefix, &config, handle,
    )?);
    let superblock =
        Arc::new(SuperblockManager::load_or_init(metadata.clone(), config.shard_size).await?);

    info!(
        generation = superblock.snapshot().generation,
        listen = %cli.listen,
        "relay executor ready"
    );

    let dedup = DedupStore::new(Duration::from_secs(cli.dedup_ttl_secs));

    // Periodic dedup eviction (every hour).
    {
        let dedup_clone = dedup.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                dedup_clone.evict_expired();
            }
        });
    }

    let state = AppState {
        metadata,
        segments,
        superblock,
        dedup,
        shard_size: config.shard_size,
    };

    let app = Router::new()
        .route("/relay_write", post(handle_relay_write))
        .route("/health", get(handle_health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cli.listen).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_relay_write(
    State(state): State<AppState>,
    Json(request): Json<RelayWriteRequest>,
) -> impl IntoResponse {
    match relay_commit_pipeline(
        &request,
        &state.metadata,
        &state.segments,
        &state.superblock,
        &state.dedup,
        state.shard_size,
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
            warn!(error = %err, "relay_write internal error");
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

async fn handle_health(State(state): State<AppState>) -> impl IntoResponse {
    let generation = state.superblock.snapshot().generation;
    Json(serde_json::json!({
        "status": "ok",
        "generation": generation,
        "dedup_entries": state.dedup.len(),
    }))
}

fn build_config(cli: &Cli) -> Config {
    let store_path = cli
        .store_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("/tmp/clawfs-relay-store"));
    Config {
        log_storage_io: cli.log_storage_io,
        inline_threshold: 4096,
        shard_size: cli.shard_size,
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
        segment_cache_bytes: 256 * 1024 * 1024,
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
        object_prefix: cli.object_prefix.clone(),
        ..Config::with_paths(
            PathBuf::from("/tmp/clawfs-relay-mnt"),
            store_path,
            PathBuf::from("/tmp/clawfs-relay-cache"),
            PathBuf::from("/tmp/clawfs-relay-state"),
        )
    }
}
