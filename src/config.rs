use std::path::PathBuf;

use clap::{Parser, ValueEnum};

/// CLI configuration for launching OsageFS.
#[derive(Parser, Debug)]
#[command(name = "osagefs", version, about = "OsageFS FUSE client")]
pub struct Cli {
    /// Path where the FUSE filesystem should be mounted.
    #[arg(long, value_name = "PATH")]
    pub mount_path: PathBuf,

    /// Path on the local filesystem that stands in for the S3-like bucket (local provider only).
    #[arg(long, value_name = "PATH")]
    pub store_path: PathBuf,

    /// Object store provider (local filesystem, AWS S3, or Google Cloud Storage).
    #[arg(long, value_enum, default_value_t = ObjectStoreProvider::Local)]
    pub object_provider: ObjectStoreProvider,

    /// Bucket name for AWS/GCS providers.
    #[arg(long)]
    pub bucket: Option<String>,

    /// Region for AWS-compatible providers.
    #[arg(long)]
    pub region: Option<String>,

    /// Custom endpoint/URL for AWS-compatible providers (useful for MinIO or other S3 clones).
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Prefix within the bucket/object store where OsageFS data is written.
    #[arg(long, default_value = "")]
    pub object_prefix: String,

    /// Optional Google Cloud service account JSON file.
    #[arg(long, value_name = "PATH")]
    pub gcs_service_account: Option<PathBuf>,

    /// Local state file for client id and allocation tracking.
    #[arg(long, value_name = "PATH")]
    pub state_path: Option<PathBuf>,

    /// Inline write threshold in bytes; payloads <= threshold stay inside metadata objects.
    #[arg(long, default_value_t = 4 * 1024)]
    pub inline_threshold: usize,

    /// Number of inodes mapped to a shard file.
    #[arg(long, default_value_t = 2048)]
    pub shard_size: u64,

    /// Number of inode numbers to reserve per allocation.
    #[arg(long, default_value_t = 1280)]
    pub inode_batch: u64,

    /// Number of segment ids to reserve per allocation.
    #[arg(long, default_value_t = 2560)]
    pub segment_batch: u64,

    /// Total pending data (bytes) before forcing a flush to the backing store.
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    pub pending_bytes: u64,

    /// Prefix under which auto-provisioned home directories should live.
    #[arg(long, default_value = "/home")]
    pub home_prefix: String,

    /// Optional JSONL performance log path for detailed timings.
    #[arg(long, value_name = "PATH")]
    pub perf_log: Option<PathBuf>,

    /// Disable the per-inode close-time journal (for benchmarking only).
    #[arg(long, default_value_t = false)]
    pub disable_journal: bool,

    /// Flush pending data on every close/fsync (safer, but slower).
    #[arg(long, default_value_t = false)]
    pub fsync_on_close: bool,

    /// Milliseconds between automatic background flushes (0 disables).
    #[arg(long, default_value_t = 1000)]
    pub flush_interval_ms: u64,

    /// Disable local background cleanup (leases, compaction) on this client.
    #[arg(long, default_value_t = false)]
    pub disable_cleanup: bool,

    /// Attribute/stat cache TTL in milliseconds (0 disables caching).
    #[arg(long, default_value_t = 5000)]
    pub lookup_cache_ttl_ms: u64,

    /// Directory entry cache TTL in milliseconds (0 disables caching).
    #[arg(long, default_value_t = 5000)]
    pub dir_cache_ttl_ms: u64,

    /// Poll interval for detecting remote metadata changes.
    #[arg(long, default_value_t = 2000)]
    pub metadata_poll_interval_ms: u64,

    /// Max bytes to keep in the local immutable-segment cache.
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    pub segment_cache_bytes: u64,

    /// Run the filesystem in the foreground (useful for debugging).
    #[arg(long, default_value_t = false)]
    pub foreground: bool,

    /// Optional path where structured logs should be written (defaults to osagefs.log).
    #[arg(long, value_name = "PATH")]
    pub log_file: Option<PathBuf>,

    /// Force DEBUG logging level regardless of log destination.
    #[arg(long, default_value_t = false)]
    pub debug_log: bool,

    /// Number of inode records to pack into each metadata delta file (higher reduces API calls).
    #[arg(long, default_value_t = 64)]
    pub imap_delta_batch: usize,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub mount_path: PathBuf,
    pub store_path: PathBuf,
    pub inline_threshold: usize,
    pub shard_size: u64,
    pub inode_batch: u64,
    pub segment_batch: u64,
    pub pending_bytes: u64,
    pub home_prefix: String,
    pub object_provider: ObjectStoreProvider,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub object_prefix: String,
    pub gcs_service_account: Option<PathBuf>,
    pub state_path: PathBuf,
    pub perf_log: Option<PathBuf>,
    pub disable_journal: bool,
    pub fsync_on_close: bool,
    pub flush_interval_ms: u64,
    pub disable_cleanup: bool,
    pub lookup_cache_ttl_ms: u64,
    pub dir_cache_ttl_ms: u64,
    pub metadata_poll_interval_ms: u64,
    pub segment_cache_bytes: u64,
    pub foreground: bool,
    pub log_file: Option<PathBuf>,
    pub debug_log: bool,
    pub imap_delta_batch: usize,
}

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        let state_path = cli
            .state_path
            .clone()
            .unwrap_or_else(|| default_state_path(&cli.mount_path));
        let log_file = cli
            .log_file
            .clone()
            .or_else(|| Some(PathBuf::from("osagefs.log")));
        Config {
            mount_path: cli.mount_path,
            store_path: cli.store_path,
            inline_threshold: cli.inline_threshold,
            shard_size: cli.shard_size,
            inode_batch: cli.inode_batch.max(1),
            segment_batch: cli.segment_batch.max(1),
            pending_bytes: cli.pending_bytes.clamp(1024, u64::MAX),
            home_prefix: cli.home_prefix,
            object_provider: cli.object_provider,
            bucket: cli.bucket,
            region: cli.region,
            endpoint: cli.endpoint,
            object_prefix: cli.object_prefix,
            gcs_service_account: cli.gcs_service_account,
            state_path,
            perf_log: cli.perf_log,
            disable_journal: cli.disable_journal,
            fsync_on_close: cli.fsync_on_close,
            flush_interval_ms: cli.flush_interval_ms,
            disable_cleanup: cli.disable_cleanup,
            lookup_cache_ttl_ms: cli.lookup_cache_ttl_ms,
            dir_cache_ttl_ms: cli.dir_cache_ttl_ms,
            metadata_poll_interval_ms: cli.metadata_poll_interval_ms,
            segment_cache_bytes: cli.segment_cache_bytes,
            foreground: cli.foreground,
            log_file,
            debug_log: cli.debug_log,
            imap_delta_batch: cli.imap_delta_batch.max(1),
        }
    }
}

fn default_state_path(mount: &PathBuf) -> PathBuf {
    mount.join(".osagefs_state.bin")
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ObjectStoreProvider {
    Local,
    Aws,
    Gcs,
}
