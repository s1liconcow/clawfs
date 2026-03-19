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
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;
use tracing::{info, warn};

use clawfs::config::{Config, ObjectStoreProvider};
use clawfs::metadata::MetadataStore;
use clawfs::relay::{
    DedupStore, RelayStatus, RelayWriteRequest, RelayWriteResponse, WriteBackBuffer,
    WriteBackError, relay_commit_pipeline, run_flusher, run_wal_writer,
};
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

    /// HMAC-SHA256 secret for validating relay JWT session tokens.
    /// When set, all /relay_write requests must include a valid Bearer token.
    /// When unset, requests are accepted without authentication (dev/test mode).
    #[arg(long, env = "CLAWFS_RELAY_JWT_SECRET")]
    jwt_secret: Option<String>,

    /// Path to NVMe/local SSD for write-back WAL directory.
    /// When set, the executor can run in write-back mode and acknowledge writes
    /// after WAL fsync instead of waiting for object-store commit.
    #[arg(long, env = "CLAWFS_RELAY_NVME_PATH")]
    nvme_path: Option<PathBuf>,

    /// WAL segment rotation threshold in bytes.
    #[arg(long, default_value_t = 67_108_864)]
    wal_segment_bytes: u64,

    /// Background flusher wake interval in milliseconds.
    #[arg(long, default_value_t = 50)]
    flush_interval_ms: u64,

    /// Maximum buffered write-back entries before backpressure is applied.
    #[arg(long, default_value_t = 256)]
    max_buffer_entries: usize,

    /// Stamp relay_required=true on the volume superblock at startup.
    #[arg(long, default_value_t = false)]
    relay_required: bool,
}

#[derive(Clone)]
struct AppState {
    metadata: Arc<MetadataStore>,
    segments: Arc<SegmentManager>,
    superblock: Arc<SuperblockManager>,
    dedup: Arc<DedupStore>,
    #[allow(dead_code)]
    write_back: Option<Arc<WriteBackBuffer>>,
    shard_size: u64,
    jwt_secret: Option<String>,
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

    let dedup = DedupStore::new(Duration::from_secs(cli.dedup_ttl_secs));

    if cli.relay_required {
        let current_snapshot = superblock.snapshot();
        if !current_snapshot.relay_required {
            superblock.set_relay_required(true).await.map_err(|err| {
                anyhow::anyhow!("stamping relay_required=true on superblock: {err}")
            })?;
            info!(
                generation = superblock.snapshot().generation,
                "relay_required=true committed to superblock on startup"
            );
        } else {
            info!("relay_required=true already set in superblock; no update needed");
        }
    }

    let write_back: Option<Arc<WriteBackBuffer>> = if let Some(nvme_path) = &cli.nvme_path {
        let committed_gen = superblock.snapshot().generation;
        info!(
            committed_gen,
            nvme_path = %nvme_path.display(),
            max_buffer_entries = cli.max_buffer_entries,
            flush_interval_ms = cli.flush_interval_ms,
            "initializing write-back buffer"
        );

        let (buffer, wal_rx) =
            WriteBackBuffer::new(nvme_path.clone(), cli.max_buffer_entries, committed_gen)
                .await
                .map_err(|err| anyhow::anyhow!("initializing WriteBackBuffer: {err}"))?;

        let buffer_depth = buffer.buffer_depth().await;
        info!(
            buffer_depth,
            "write-back buffer initialized (recovered {} pending entries)", buffer_depth
        );

        {
            let wal_dir = nvme_path.clone();
            let seg_bytes = cli.wal_segment_bytes;
            tokio::spawn(async move {
                run_wal_writer(wal_rx, wal_dir, seg_bytes).await;
                warn!("run_wal_writer exited; write-back mode disabled until restart");
            });
        }

        {
            let buffer = buffer.clone();
            let metadata = metadata.clone();
            let segments = segments.clone();
            let superblock = superblock.clone();
            let dedup = dedup.clone();
            let shard_size = config.shard_size;
            let flush_interval = Duration::from_millis(cli.flush_interval_ms);
            tokio::spawn(async move {
                run_flusher(
                    buffer,
                    metadata,
                    segments,
                    superblock,
                    dedup,
                    shard_size,
                    flush_interval,
                )
                .await;
                warn!("run_flusher exited; write-back mode disabled until restart");
            });
        }

        Some(buffer)
    } else {
        if cli.relay_required {
            warn!(
                "--relay-required is set but --nvme-path is not; relay_required=true has been stamped on the superblock but write-back cache is disabled"
            );
        }
        None
    };

    info!(
        generation = superblock.snapshot().generation,
        listen = %cli.listen,
        "relay executor ready"
    );

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
        write_back,
        shard_size: config.shard_size,
        jwt_secret: cli.jwt_secret.clone(),
    };

    let app = Router::new()
        .route("/relay_write", post(handle_relay_write))
        .route("/health", get(handle_health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cli.listen).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

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

async fn handle_relay_write(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RelayWriteRequest>,
) -> impl IntoResponse {
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
    if let Some(wb) = &state.write_back {
        let idempotency_key = request.idempotency_key.clone();
        return match wb.assign_and_buffer(request, &state.dedup).await {
            Ok(generation) => (
                StatusCode::OK,
                Json(RelayWriteResponse {
                    status: RelayStatus::Committed,
                    committed_generation: Some(generation),
                    idempotency_key,
                    error: None,
                    actual_generation: None,
                }),
            )
                .into_response(),
            Err(WriteBackError::Duplicate {
                committed_generation,
            }) => (
                StatusCode::OK,
                Json(RelayWriteResponse {
                    status: RelayStatus::Duplicate,
                    committed_generation: Some(committed_generation),
                    idempotency_key,
                    error: None,
                    actual_generation: None,
                }),
            )
                .into_response(),
            Err(WriteBackError::GenerationMismatch { expected, actual }) => (
                StatusCode::CONFLICT,
                Json(RelayWriteResponse {
                    status: RelayStatus::Failed,
                    committed_generation: None,
                    idempotency_key,
                    error: Some(format!(
                        "generation mismatch: expected parent {} but logical_gen-1 is {}",
                        expected, actual
                    )),
                    actual_generation: Some(actual),
                }),
            )
                .into_response(),
            Err(WriteBackError::BufferFull) => {
                warn!("write_back buffer full; returning 429 to client");
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(serde_json::json!({"error": "write-back buffer full; retry"})),
                )
                    .into_response()
            }
            Err(WriteBackError::WalWriterGone) => {
                warn!("WAL writer task has exited; write-back mode is broken");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": "WAL writer unavailable; service restart required"})),
                )
                    .into_response()
            }
            Err(WriteBackError::WalIo(err)) => {
                warn!(error = %err, "WAL I/O error in assign_and_buffer");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("WAL I/O error: {err}")})),
                )
                    .into_response()
            }
        };
    }
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
                RelayStatus::Failed => StatusCode::CONFLICT,
                _ => StatusCode::OK,
            };
            (status, Json(response)).into_response()
        }
        Err(err) => {
            warn!(error = %err, "relay_write internal error");
            let response = RelayWriteResponse {
                status: RelayStatus::Failed,
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
    let halted = state
        .write_back
        .as_ref()
        .map(|wb| wb.is_flusher_halted())
        .unwrap_or(false);

    let mut body = serde_json::json!({
        "status": if halted { "degraded" } else { "ok" },
        "generation": generation,
        "dedup_entries": state.dedup.len(),
        "mode": if state.write_back.is_some() { "write_back" } else { "synchronous" },
    });

    if let Some(wb) = &state.write_back {
        let buffer_depth = wb.buffer_depth().await;
        let logical_gen = wb.logical_gen().await;
        let flush_lag = wb.flush_lag().await;
        body["write_back"] = serde_json::json!({
            "buffer_depth": buffer_depth,
            "logical_gen": logical_gen,
            "committed_gen": wb.committed_gen(),
            "flush_lag": flush_lag,
            "flusher_halted": halted,
        });
    }

    // Operators should alert on:
    // - flush_lag > 100 (flusher is falling behind)
    // - flusher_halted == true (critical: acknowledged writes are not durable upstream)
    // - HTTP 503 from this endpoint (same as flusher_halted)
    let status = if halted {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (status, Json(body)).into_response()
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

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde::Serialize;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;

    struct HealthHarness {
        _root: tempfile::TempDir,
        state: AppState,
    }

    async fn make_health_harness(halt_flusher: bool) -> HealthHarness {
        let root = tempdir().unwrap();
        let store_path = root.path().join("store");
        let cache_path = root.path().join("cache");
        let state_path = root.path().join("state");
        let mount_path = root.path().join("mnt");

        let cli = Cli {
            object_provider: ObjectStoreProvider::Local,
            store_path: Some(store_path.clone()),
            bucket: None,
            object_prefix: "test".to_string(),
            region: None,
            endpoint: None,
            shard_size: 2048,
            listen: "127.0.0.1:0".to_string(),
            dedup_ttl_secs: 60,
            log_storage_io: false,
            jwt_secret: None,
            nvme_path: None,
            wal_segment_bytes: 64 * 1024 * 1024,
            flush_interval_ms: 50,
            max_buffer_entries: 8,
            relay_required: false,
        };

        let mut config = build_config(&cli);
        config.mount_path = mount_path;
        config.store_path = store_path;
        config.local_cache_path = cache_path;
        config.state_path = state_path;

        let handle = tokio::runtime::Handle::current();
        let metadata = Arc::new(MetadataStore::new(&config, handle.clone()).await.unwrap());
        let superblock = Arc::new(
            SuperblockManager::load_or_init(metadata.clone(), config.shard_size)
                .await
                .unwrap(),
        );
        let segments = Arc::new(SegmentManager::new(&config, handle.clone()).unwrap());
        let dedup = DedupStore::new(Duration::from_secs(60));

        let write_back = if halt_flusher {
            let (buffer, _wal_rx) =
                WriteBackBuffer::new(root.path().join("wal"), 8, superblock.snapshot().generation)
                    .await
                    .unwrap();
            buffer.as_ref().force_halt_flusher_for_tests();
            Some(buffer)
        } else {
            let (buffer, _wal_rx) =
                WriteBackBuffer::new(root.path().join("wal"), 8, superblock.snapshot().generation)
                    .await
                    .unwrap();
            Some(buffer)
        };

        HealthHarness {
            _root: root,
            state: AppState {
                metadata,
                segments,
                superblock,
                dedup,
                write_back,
                shard_size: config.shard_size,
                jwt_secret: None,
            },
        }
    }

    #[derive(Serialize)]
    struct TestClaims {
        sub: String,
        prefix: String,
        iat: u64,
        exp: u64,
    }

    fn make_token(prefix: &str, secret: &str, ttl_secs: i64) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let exp = (now as i64 + ttl_secs) as u64;
        let claims = TestClaims {
            sub: "org/vol".to_string(),
            prefix: prefix.to_string(),
            iat: now,
            exp,
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    fn headers_with_bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", format!("Bearer {token}").parse().unwrap());
        headers
    }

    #[test]
    fn valid_token_accepted() {
        let secret = "test-secret";
        let prefix = "orgs/myorg/volumes/myvol";
        let token = make_token(prefix, secret, 3600);
        let headers = headers_with_bearer(&token);
        assert!(validate_relay_token(&headers, prefix, secret).is_ok());
    }

    #[test]
    fn wrong_prefix_rejected() {
        let secret = "test-secret";
        let token = make_token("orgs/myorg/volumes/other", secret, 3600);
        let headers = headers_with_bearer(&token);
        assert!(validate_relay_token(&headers, "orgs/myorg/volumes/myvol", secret).is_err());
    }

    #[test]
    fn expired_token_rejected() {
        let secret = "test-secret";
        let prefix = "orgs/myorg/volumes/myvol";
        // Use a clearly expired token (10 minutes ago) to exceed any leeway.
        let token = make_token(prefix, secret, -600);
        let headers = headers_with_bearer(&token);
        assert!(validate_relay_token(&headers, prefix, secret).is_err());
    }

    #[test]
    fn wrong_secret_rejected() {
        let prefix = "orgs/myorg/volumes/myvol";
        let token = make_token(prefix, "signing-secret", 3600);
        let headers = headers_with_bearer(&token);
        assert!(validate_relay_token(&headers, prefix, "different-secret").is_err());
    }

    #[test]
    fn missing_header_rejected() {
        let headers = HeaderMap::new();
        assert!(validate_relay_token(&headers, "orgs/myorg/volumes/myvol", "secret").is_err());
    }

    #[test]
    fn wrong_scheme_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Basic dXNlcjpwYXNz".parse().unwrap());
        assert!(validate_relay_token(&headers, "orgs/myorg/volumes/myvol", "secret").is_err());
    }

    #[tokio::test]
    async fn health_exposes_write_back_metrics() {
        let harness = make_health_harness(false).await;
        let response = handle_health(State(harness.state.clone()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "ok");
        assert_eq!(json["mode"], "write_back");
        assert_eq!(json["generation"].as_u64(), Some(1));
        assert_eq!(json["dedup_entries"].as_u64(), Some(0));
        let write_back = json["write_back"].as_object().expect("write_back metrics");
        assert_eq!(write_back["buffer_depth"].as_u64(), Some(0));
        assert_eq!(write_back["logical_gen"].as_u64(), Some(2));
        assert_eq!(write_back["committed_gen"].as_u64(), Some(1));
        assert_eq!(write_back["flush_lag"].as_u64(), Some(0));
        assert_eq!(write_back["flusher_halted"].as_bool(), Some(false));
    }

    #[tokio::test]
    async fn health_returns_503_when_flusher_halted() {
        let harness = make_health_harness(true).await;
        let response = handle_health(State(harness.state.clone()))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "degraded");
        assert_eq!(json["mode"], "write_back");
        assert_eq!(json["write_back"]["flusher_halted"].as_bool(), Some(true));
    }
}
