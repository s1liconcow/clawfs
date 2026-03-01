use std::path::{Path, PathBuf};

use clap::{ArgAction, Parser, ValueEnum};

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

    /// Path on the local filesystem for client-side caches/journals (defaults to ~/.osagefs/cache).
    #[arg(long, value_name = "PATH")]
    pub local_cache_path: Option<PathBuf>,

    /// Log backing-store and cache writes at INFO.
    #[arg(long, default_value_t = false)]
    pub log_storage_io: bool,

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
    #[arg(long, default_value_t = 32 * 1024)]
    pub inline_threshold: usize,

    /// Enable inline payload compression for metadata-resident file/symlink data.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub inline_compression: bool,

    /// Optional passphrase for inline payload encryption (enables encryption when set).
    #[arg(long)]
    pub inline_encryption_key: Option<String>,

    /// Enable immutable-segment payload compression.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub segment_compression: bool,

    /// Optional passphrase for immutable-segment payload encryption.
    #[arg(long)]
    pub segment_encryption_key: Option<String>,

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
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    pub pending_bytes: u64,

    /// Prefix under which auto-provisioned home directories should live.
    #[arg(long, default_value = "/home")]
    pub home_prefix: String,

    /// Optional JSONL performance log path for detailed timings.
    #[arg(long, value_name = "PATH")]
    pub perf_log: Option<PathBuf>,

    /// Optional compressed replay log path (.jsonl.gz) for operation-level tracing.
    #[arg(long, value_name = "PATH")]
    pub replay_log: Option<PathBuf>,

    /// Disable the per-inode close-time journal (for benchmarking only).
    #[arg(long, default_value_t = false)]
    pub disable_journal: bool,

    /// Flush pending data on every close/fsync (safer, but slower).
    #[arg(long, default_value_t = false)]
    pub fsync_on_close: bool,

    /// Max milliseconds dirty data is allowed to stay pending before auto-flush (0 disables).
    #[arg(long, default_value_t = 5000)]
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

    /// Mount with FUSE allow_other (requires user_allow_other in /etc/fuse.conf).
    #[arg(long, default_value_t = false)]
    pub allow_other: bool,

    /// Optional path where structured logs should be written (defaults to osagefs.log).
    #[arg(long, value_name = "PATH")]
    pub log_file: Option<PathBuf>,

    /// Force DEBUG logging level regardless of log destination.
    #[arg(long, default_value_t = false)]
    pub debug_log: bool,

    /// Number of inode records to pack into each metadata delta file (higher reduces API calls).
    #[arg(long, default_value_t = 512)]
    pub imap_delta_batch: usize,

    /// Disable FUSE writeback cache (disabled by default; writeback cache can
    /// lose writes on file-create-write-rename patterns under some kernels).
    #[arg(long, default_value_t = true)]
    pub no_writeback_cache: bool,

    /// Enable FUSE writeback cache for higher sequential write throughput.
    /// WARNING: may cause data corruption for create-write-rename patterns
    /// (e.g. git, editors) on some Linux kernels including WSL2.
    #[arg(long, default_value_t = false)]
    pub writeback_cache: bool,

    /// Number of FUSE dispatch threads (0 = single-threaded legacy mode).
    #[arg(long, default_value_t = default_fuse_threads())]
    pub fuse_threads: usize,

    #[arg(long, default_value_t = 5)]
    pub entry_ttl_secs: u64,

    /// Filesystem name reported to the kernel mount table.
    #[arg(long, default_value = "osagefs")]
    pub fuse_fsname: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub mount_path: PathBuf,
    pub store_path: PathBuf,
    pub local_cache_path: PathBuf,
    pub log_storage_io: bool,
    pub inline_threshold: usize,
    pub inline_compression: bool,
    pub inline_encryption_key: Option<String>,
    pub segment_compression: bool,
    pub segment_encryption_key: Option<String>,
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
    pub replay_log: Option<PathBuf>,
    pub disable_journal: bool,
    pub fsync_on_close: bool,
    pub flush_interval_ms: u64,
    pub disable_cleanup: bool,
    pub lookup_cache_ttl_ms: u64,
    pub dir_cache_ttl_ms: u64,
    pub metadata_poll_interval_ms: u64,
    pub segment_cache_bytes: u64,
    pub foreground: bool,
    pub allow_other: bool,
    pub log_file: Option<PathBuf>,
    pub debug_log: bool,
    pub imap_delta_batch: usize,
    pub writeback_cache: bool,
    pub fuse_threads: usize,
    pub entry_ttl_secs: u64,
    pub fuse_fsname: String,
}

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        let config_root = default_user_config_root();
        let state_path = cli
            .state_path
            .clone()
            .unwrap_or_else(|| default_state_path(&config_root));
        let log_file = cli
            .log_file
            .clone()
            .or_else(|| Some(PathBuf::from("osagefs.log")));
        let store_path = cli.store_path;
        let local_cache_path = cli
            .local_cache_path
            .clone()
            .unwrap_or_else(|| default_local_cache_path(&config_root));
        Config {
            mount_path: cli.mount_path,
            store_path,
            local_cache_path,
            log_storage_io: cli.log_storage_io,
            inline_threshold: cli.inline_threshold,
            inline_compression: cli.inline_compression,
            inline_encryption_key: cli
                .inline_encryption_key
                .and_then(|value| (!value.trim().is_empty()).then_some(value)),
            segment_compression: cli.segment_compression,
            segment_encryption_key: cli
                .segment_encryption_key
                .and_then(|value| (!value.trim().is_empty()).then_some(value)),
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
            replay_log: cli.replay_log,
            disable_journal: cli.disable_journal,
            fsync_on_close: cli.fsync_on_close,
            flush_interval_ms: cli.flush_interval_ms,
            disable_cleanup: cli.disable_cleanup,
            lookup_cache_ttl_ms: cli.lookup_cache_ttl_ms,
            dir_cache_ttl_ms: cli.dir_cache_ttl_ms,
            metadata_poll_interval_ms: cli.metadata_poll_interval_ms,
            segment_cache_bytes: cli.segment_cache_bytes,
            foreground: cli.foreground,
            allow_other: cli.allow_other,
            log_file,
            debug_log: cli.debug_log,
            imap_delta_batch: cli.imap_delta_batch.max(1),
            writeback_cache: cli.writeback_cache && !cli.no_writeback_cache,
            fuse_threads: cli.fuse_threads,
            entry_ttl_secs: cli.entry_ttl_secs,
            fuse_fsname: if cli.fuse_fsname.trim().is_empty() {
                "osagefs".to_string()
            } else {
                cli.fuse_fsname
            },
        }
    }
}

fn default_user_config_root() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".osagefs")
}

fn default_state_path(root: &Path) -> PathBuf {
    root.join("state").join("client_state.bin")
}

fn default_local_cache_path(root: &Path) -> PathBuf {
    root.join("cache")
}

fn default_fuse_threads() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ObjectStoreProvider {
    Local,
    Aws,
    Gcs,
}
