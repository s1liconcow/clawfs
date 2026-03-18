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
use clawfs::maintenance::{
    CheckpointConfig, CompactionConfig, LifecyclePolicy, MaintenanceRoundContext,
    MaintenanceSchedule, run_maintenance_round,
};
use clawfs::metadata::MetadataStore;
use clawfs::segment::{SegmentManager, segment_prefix};
use clawfs::superblock::SuperblockManager;

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

    /// Enable checkpoint creation and retention.
    #[arg(long, default_value_t = true)]
    checkpoint_maintenance: bool,

    /// Interval between checkpoints in seconds.
    #[arg(long, default_value_t = 86_400)]
    checkpoint_interval_secs: u64,

    /// Maximum number of checkpoints to retain.
    #[arg(long, default_value_t = 7)]
    checkpoint_max_checkpoints: usize,

    /// Retention window for checkpoints in days.
    #[arg(long, default_value_t = 7)]
    checkpoint_retention_days: u64,

    /// Enable lifecycle cleanup for expired hosted prefixes.
    #[arg(long, default_value_t = false)]
    lifecycle_cleanup: bool,

    /// Expiry window for hosted prefix cleanup in days.
    #[arg(long, default_value_t = 30)]
    lifecycle_expiry_days: u64,

    /// Require confirmation before prefix cleanup can delete objects.
    #[arg(long, default_value_t = true)]
    lifecycle_require_confirmation: bool,
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
    let checkpoint_config = cli.checkpoint_maintenance.then(|| CheckpointConfig {
        interval: Duration::from_secs(cli.checkpoint_interval_secs),
        max_checkpoints: cli.checkpoint_max_checkpoints,
        retention_days: cli.checkpoint_retention_days,
    });

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
    let lifecycle_policy = if cli.lifecycle_cleanup {
        let allowed_prefix = metadata.root_prefix().trim_matches('/').to_string();
        if allowed_prefix.is_empty() {
            warn!(
                "lifecycle cleanup requested but the volume prefix is empty; skipping lifecycle maintenance"
            );
            None
        } else {
            Some(LifecyclePolicy {
                expiry_days: cli.lifecycle_expiry_days,
                require_confirmation: cli.lifecycle_require_confirmation,
                allowed_prefix,
            })
        }
    } else {
        None
    };
    let mut maintenance_schedule = MaintenanceSchedule::new();

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
    let mut round: u64 = 0;
    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        let round_result = run_maintenance_round(MaintenanceRoundContext {
            metadata: &metadata,
            segments: &segments,
            superblock: &superblock,
            client_id: &client_id,
            compaction_config: &compaction_config,
            checkpoint_config: checkpoint_config.as_ref(),
            lifecycle_policy: lifecycle_policy.as_ref(),
            schedule: &mut maintenance_schedule,
        })
        .await;
        round = round.saturating_add(1);

        // Write structured health status after each round for container health checks.
        let generation = superblock.snapshot().generation;
        let _ = std::fs::write(
            &status_path,
            format!(
                "running\nclient_id={client_id}\npoll_interval_secs={}\nround={round}\ngeneration={generation}\ndelta_backlog={}\nsegment_backlog={}\ncheckpoint_backlog={}\n",
                cli.poll_interval_secs,
                round_result.delta_backlog,
                round_result.segment_backlog,
                round_result.checkpoint_backlog,
            ),
        );

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

// MaintenanceSchedule, RoundStatus, MaintenanceRoundContext, and
// run_maintenance_round are now defined in clawfs::maintenance and imported
// above.  The single-volume worker uses them directly.

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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::MaintenanceSchedule;

    #[test]
    fn maintenance_schedule_starts_due_and_is_reset_by_marks() {
        let mut schedule = MaintenanceSchedule::new();

        assert!(schedule.checkpoint_due(Duration::from_secs(1)));
        assert!(schedule.lifecycle_due());

        schedule.mark_checkpoint_ran();
        schedule.mark_lifecycle_ran();

        assert!(!schedule.checkpoint_due(Duration::from_secs(3600)));
        assert!(!schedule.lifecycle_due());
    }
}
