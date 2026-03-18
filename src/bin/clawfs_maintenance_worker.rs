//! Hosted maintenance worker for ClawFS managed volumes.
//!
//! This binary runs near the object store and handles background cleanup tasks
//! (delta and segment compaction) for managed volumes, relieving foreground
//! FUSE/NFS clients from cross-region maintenance work.
//!
//! Multiple worker instances targeting the same volume coexist safely via
//! superblock lease contention: only one worker runs each CleanupTaskKind
//! at a time.
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tokio::time::sleep;
use tracing::{info, warn};
use uuid::Uuid;

use clawfs::config::{Config, ObjectStoreProvider};
use clawfs::maintenance::{self, CompactionConfig};
use clawfs::metadata::MetadataStore;
use clawfs::segment::{SegmentManager, segment_prefix};
use clawfs::superblock::{CleanupTaskKind, SuperblockManager};

#[derive(Debug, Parser)]
#[command(
    name = "clawfs-maintenance-worker",
    version,
    about = "Hosted maintenance worker for ClawFS managed volumes"
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

    /// How often the worker checks for maintenance work (seconds).
    #[arg(long, default_value_t = 30)]
    poll_interval_secs: u64,

    /// Stable client identity used in lease records. Defaults to a random UUID.
    #[arg(long)]
    client_id: Option<String>,

    /// Local directory for worker state files (status, state db).
    #[arg(long, default_value = "/tmp/clawfs-worker")]
    state_dir: PathBuf,

    /// Log object store I/O at debug level.
    #[arg(long, default_value_t = false)]
    log_storage_io: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let client_id = cli
        .client_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let poll_interval = Duration::from_secs(cli.poll_interval_secs);
    let compaction_config = CompactionConfig::default();

    info!(
        client_id = %client_id,
        object_provider = ?cli.object_provider,
        bucket = ?cli.bucket,
        object_prefix = %cli.object_prefix,
        poll_interval_secs = cli.poll_interval_secs,
        "starting clawfs-maintenance-worker"
    );

    std::fs::create_dir_all(&cli.state_dir)?;
    let status_path = cli.state_dir.join("worker_status.txt");
    let state_path = cli.state_dir.join("worker_state.bin");

    let config = build_config(&cli, state_path);

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
        "volume loaded, entering maintenance loop"
    );

    // Write initial status file for container health checks.
    let _ = std::fs::write(
        &status_path,
        format!(
            "running\nclient_id={client_id}\npoll_interval_secs={}\n",
            cli.poll_interval_secs
        ),
    );

    // Set up graceful shutdown channel.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // SIGTERM handler (Unix only).
    #[cfg(unix)]
    {
        let tx = shutdown_tx.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{SignalKind, signal};
            match signal(SignalKind::terminate()) {
                Ok(mut sigterm) => {
                    sigterm.recv().await;
                    info!("received SIGTERM, initiating graceful shutdown");
                    let _ = tx.send(true);
                }
                Err(err) => warn!("failed to install SIGTERM handler: {err}"),
            }
        });
    }

    // Ctrl-C / SIGINT handler.
    {
        let tx = shutdown_tx;
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                info!("received SIGINT, initiating graceful shutdown");
                let _ = tx.send(true);
            }
        });
    }

    // Main maintenance loop.
    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        run_maintenance_round(
            &metadata,
            &segments,
            &superblock,
            &client_id,
            &compaction_config,
        )
        .await;

        // Sleep for the poll interval or wake early on shutdown.
        tokio::select! {
            _ = sleep(poll_interval) => {}
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() { break; }
            }
        }
    }

    let _ = std::fs::write(&status_path, "stopped\n");
    info!("maintenance worker stopped");
    Ok(())
}

async fn run_maintenance_round(
    metadata: &Arc<MetadataStore>,
    segments: &Arc<SegmentManager>,
    superblock: &Arc<SuperblockManager>,
    client_id: &str,
    config: &CompactionConfig,
) {
    let mut did_delta_work = false;

    // --- Delta compaction ---
    // Pre-check count to avoid acquiring a lease when there is nothing to do.
    let delta_count = {
        let md = metadata.clone();
        tokio::task::spawn_blocking(move || md.delta_file_count())
            .await
            .unwrap_or(Ok(0))
            .unwrap_or(0)
    };

    if delta_count > config.delta_compact_threshold {
        match maintenance::acquire_cleanup_lease(
            superblock,
            CleanupTaskKind::DeltaCompaction,
            client_id,
            config,
        )
        .await
        {
            Ok(true) => {
                match maintenance::run_delta_compaction(metadata, config).await {
                    Ok(result) => {
                        info!(
                            deltas_pruned = result.deltas_pruned,
                            duration_ms = result.duration.as_millis(),
                            "delta compaction complete"
                        );
                        did_delta_work = result.deltas_pruned > 0;
                    }
                    Err(err) => warn!("delta compaction failed: {err:?}"),
                }
                if let Err(err) = maintenance::release_cleanup_lease(
                    superblock,
                    CleanupTaskKind::DeltaCompaction,
                    client_id,
                )
                .await
                {
                    warn!("failed to release delta compaction lease: {err:?}");
                }
            }
            Ok(false) => {
                info!("delta compaction lease contended, skipping this cycle");
            }
            Err(err) => warn!("failed to acquire delta compaction lease: {err:?}"),
        }
    }

    // --- Segment compaction ---
    // Only run segment compaction if the volume is old enough and we didn't
    // just do delta work (avoid two heavy operations in the same cycle).
    if !did_delta_work {
        let current_generation = superblock.snapshot().generation;
        let cutoff_generation = current_generation.saturating_sub(config.segment_compact_lag);

        if cutoff_generation > 0 {
            match maintenance::acquire_cleanup_lease(
                superblock,
                CleanupTaskKind::SegmentCompaction,
                client_id,
                config,
            )
            .await
            {
                Ok(true) => {
                    match maintenance::run_segment_compaction(metadata, segments, config).await {
                        Ok(result) => {
                            info!(
                                segments_merged = result.segments_merged,
                                bytes_rewritten = result.bytes_rewritten,
                                duration_ms = result.duration.as_millis(),
                                "segment compaction complete"
                            );
                        }
                        Err(err) => warn!("segment compaction failed: {err:?}"),
                    }
                    if let Err(err) = maintenance::release_cleanup_lease(
                        superblock,
                        CleanupTaskKind::SegmentCompaction,
                        client_id,
                    )
                    .await
                    {
                        warn!("failed to release segment compaction lease: {err:?}");
                    }
                }
                Ok(false) => {
                    info!("segment compaction lease contended, skipping this cycle");
                }
                Err(err) => warn!("failed to acquire segment compaction lease: {err:?}"),
            }
        }
    }
}

fn build_config(cli: &Cli, state_path: PathBuf) -> Config {
    let store_path = cli
        .store_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("/tmp/clawfs-worker-store"));
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
        object_provider: cli.object_provider,
        bucket: cli.bucket.clone(),
        region: cli.region.clone(),
        endpoint: cli.endpoint.clone(),
        object_prefix: cli.object_prefix.clone(),
        ..Config::with_paths(
            PathBuf::from("/tmp/clawfs-worker-mnt"),
            store_path,
            PathBuf::from("/tmp/clawfs-worker-cache"),
            state_path,
        )
    }
}
