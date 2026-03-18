use std::path::{Path, PathBuf};

use clap::{ArgAction, Parser, ValueEnum};

use crate::clawfs::{AcceleratorFallbackPolicy, AcceleratorMode};

/// Default directory name for the FUSE mount point (relative to cwd).
pub const DEFAULT_MOUNT_DIR: &str = "clawfs-mnt";

/// CLI configuration for launching ClawFS.
#[derive(Parser, Debug)]
#[command(name = "clawfs", version, about = "ClawFS FUSE client")]
pub struct Cli {
    /// Path where the FUSE filesystem should be mounted.
    #[arg(long, value_name = "PATH")]
    pub mount_path: Option<PathBuf>,

    /// Path on the local filesystem that stands in for the S3-like bucket (local provider only).
    #[arg(long, value_name = "PATH")]
    pub store_path: PathBuf,

    /// Path on the local filesystem for client-side caches/journals (defaults to ~/.clawfs/cache).
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

    /// Prefix within the bucket/object store where ClawFS data is written.
    #[arg(long, default_value = "")]
    pub object_prefix: String,

    /// Optional Google Cloud service account JSON file.
    #[arg(long, value_name = "PATH")]
    pub gcs_service_account: Option<PathBuf>,

    /// Optional source object provider for overlay mounts of existing object data.
    #[arg(long, value_enum)]
    pub source_object_provider: Option<ObjectStoreProvider>,

    /// Source bucket name for overlay mounts.
    #[arg(long)]
    pub source_bucket: Option<String>,

    /// Prefix within the source bucket to expose.
    #[arg(long, default_value = "")]
    pub source_prefix: String,

    /// Local source path (when source provider is local).
    #[arg(long, value_name = "PATH")]
    pub source_store_path: Option<PathBuf>,

    /// Region for source AWS-compatible providers.
    #[arg(long)]
    pub source_region: Option<String>,

    /// Endpoint for source AWS-compatible providers.
    #[arg(long)]
    pub source_endpoint: Option<String>,

    /// Optional Google Cloud service account JSON file for source provider.
    #[arg(long, value_name = "PATH")]
    pub source_gcs_service_account: Option<PathBuf>,

    /// Source AWS access key id (separate credentials from overlay store).
    #[arg(long)]
    pub source_aws_access_key_id: Option<String>,

    /// Source AWS secret access key (separate credentials from overlay store).
    #[arg(long)]
    pub source_aws_secret_access_key: Option<String>,

    /// Allow HTTP for source AWS provider.
    #[arg(long, default_value_t = false)]
    pub source_aws_allow_http: bool,

    /// Force path-style access for source AWS provider.
    #[arg(long, default_value_t = false)]
    pub source_aws_force_path_style: bool,

    /// Allow HTTP for AWS object provider (useful for local testing).
    #[arg(long, default_value_t = false)]
    pub aws_allow_http: bool,

    /// Force path-style access for AWS object provider (e.g. http://host/bucket instead of http://bucket.host).
    #[arg(long, default_value_t = false)]
    pub aws_force_path_style: bool,

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

    /// Optional path where structured logs should be written (defaults to clawfs.log).
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
    #[arg(long, default_value = "clawfs")]
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
    pub accelerator_mode: Option<AcceleratorMode>,
    pub accelerator_endpoint: Option<String>,
    pub accelerator_fallback_policy: Option<AcceleratorFallbackPolicy>,
    pub object_prefix: String,
    pub telemetry_object_prefix: Option<String>,
    pub gcs_service_account: Option<PathBuf>,
    pub aws_allow_http: bool,
    pub aws_force_path_style: bool,
    pub source: Option<SourceStoreConfig>,
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

impl Config {
    pub fn with_paths(
        mount_path: PathBuf,
        store_path: PathBuf,
        local_cache_path: PathBuf,
        state_path: PathBuf,
    ) -> Self {
        Self {
            mount_path,
            store_path,
            local_cache_path,
            log_storage_io: false,
            inline_threshold: 32 * 1024,
            inline_compression: true,
            inline_encryption_key: None,
            segment_compression: true,
            segment_encryption_key: None,
            shard_size: 2048,
            inode_batch: 1280,
            segment_batch: 2560,
            pending_bytes: 256 * 1024 * 1024,
            home_prefix: "/home".to_string(),
            object_provider: ObjectStoreProvider::Local,
            bucket: None,
            region: None,
            endpoint: None,
            accelerator_mode: None,
            accelerator_endpoint: None,
            accelerator_fallback_policy: None,
            object_prefix: String::new(),
            telemetry_object_prefix: None,
            gcs_service_account: None,
            aws_allow_http: false,
            aws_force_path_style: false,
            source: None,
            state_path,
            perf_log: None,
            replay_log: None,
            disable_journal: false,
            fsync_on_close: false,
            flush_interval_ms: 5000,
            disable_cleanup: false,
            lookup_cache_ttl_ms: 5000,
            dir_cache_ttl_ms: 5000,
            metadata_poll_interval_ms: 2000,
            segment_cache_bytes: 512 * 1024 * 1024,
            foreground: false,
            allow_other: false,
            log_file: None,
            debug_log: false,
            imap_delta_batch: 512,
            writeback_cache: false,
            fuse_threads: default_fuse_threads(),
            entry_ttl_secs: 5,
            fuse_fsname: "clawfs".to_string(),
        }
    }

    pub fn accelerator_status(&self) -> crate::perf::AcceleratorStatus {
        crate::perf::AcceleratorStatus::from_config(self)
    }
}

#[derive(Debug, Clone)]
pub struct SourceStoreConfig {
    pub object_provider: ObjectStoreProvider,
    pub bucket: Option<String>,
    pub prefix: String,
    pub store_path: Option<PathBuf>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub gcs_service_account: Option<PathBuf>,
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    pub aws_allow_http: bool,
    pub aws_force_path_style: bool,
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
            .or_else(|| Some(PathBuf::from("clawfs.log")));
        let store_path = cli.store_path;
        let local_cache_path = cli
            .local_cache_path
            .clone()
            .unwrap_or_else(|| default_local_cache_path(&config_root));
        let source = if cli.source_bucket.is_some() || cli.source_store_path.is_some() {
            Some(SourceStoreConfig {
                object_provider: cli.source_object_provider.unwrap_or(cli.object_provider),
                bucket: cli.source_bucket,
                prefix: cli.source_prefix,
                store_path: cli.source_store_path,
                region: cli.source_region,
                endpoint: cli.source_endpoint,
                gcs_service_account: cli.source_gcs_service_account,
                aws_access_key_id: cli.source_aws_access_key_id,
                aws_secret_access_key: cli.source_aws_secret_access_key,
                aws_allow_http: cli.source_aws_allow_http,
                aws_force_path_style: cli.source_aws_force_path_style,
            })
        } else {
            None
        };

        Config {
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
            accelerator_mode: None,
            accelerator_endpoint: None,
            accelerator_fallback_policy: None,
            object_prefix: cli.object_prefix,
            gcs_service_account: cli.gcs_service_account,
            aws_allow_http: cli.aws_allow_http,
            aws_force_path_style: cli.aws_force_path_style,
            source,
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
                "clawfs".to_string()
            } else {
                cli.fuse_fsname
            },
            ..Config::with_paths(
                cli.mount_path.unwrap_or_else(|| {
                    std::env::current_dir()
                        .unwrap_or_else(|_| PathBuf::from("."))
                        .join(DEFAULT_MOUNT_DIR)
                }),
                store_path,
                local_cache_path,
                state_path,
            )
        }
    }
}

fn default_user_config_root() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".clawfs")
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ObjectStoreProvider {
    Local,
    Aws,
    Gcs,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Config};

    #[test]
    fn with_paths_defaults_do_not_enable_accelerator_state() {
        let config = Config::with_paths(
            "/tmp/clawfs-mnt-test".into(),
            "/tmp/clawfs-store-test".into(),
            "/tmp/clawfs-cache-test".into(),
            "/tmp/clawfs-state-test".into(),
        );

        assert_eq!(config.accelerator_mode, None);
        assert_eq!(config.accelerator_endpoint, None);
        assert_eq!(config.accelerator_fallback_policy, None);
    }

    #[test]
    fn cli_config_defaults_do_not_enable_accelerator_state() {
        let config = Config::from(Cli::parse_from([
            "clawfs",
            "--mount-path",
            "/tmp/clawfs-mnt",
            "--store-path",
            "/tmp/clawfs-store",
        ]));

        assert_eq!(config.accelerator_mode, None);
        assert_eq!(config.accelerator_endpoint, None);
        assert_eq!(config.accelerator_fallback_policy, None);
        assert_eq!(config.object_provider, super::ObjectStoreProvider::Local);
    }

    #[test]
    fn accelerator_status_defaults_to_not_configured() {
        let config = Config::with_paths(
            "/tmp/clawfs-mnt-test".into(),
            "/tmp/clawfs-store-test".into(),
            "/tmp/clawfs-cache-test".into(),
            "/tmp/clawfs-state-test".into(),
        );
        let status = config.accelerator_status();

        assert_eq!(status.accelerator_mode, "not_configured");
        assert_eq!(status.accelerator_endpoint, None);
        assert_eq!(status.accelerator_health.as_str(), "not_configured");
        assert_eq!(status.cleanup_owner.as_str(), "local");
        assert_eq!(status.coordination_status.as_str(), "disabled");
        assert_eq!(status.relay_status.as_str(), "not_configured");
    }
}
