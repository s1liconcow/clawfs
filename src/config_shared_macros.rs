#[macro_export]
macro_rules! shared_cli_struct {
    ($($writeback_fields:tt)*) => {
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
            #[arg(long, default_value_t = 8192)]
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

            /// Interval in seconds between local background cleanup polls.
            #[arg(long, default_value_t = 30)]
            pub cleanup_interval_secs: u64,

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

            $($writeback_fields)*

            /// Number of FUSE dispatch threads (0 = single-threaded legacy mode).
            #[arg(long, default_value_t = default_fuse_threads())]
            pub fuse_threads: usize,

            #[arg(long, default_value_t = 5)]
            pub entry_ttl_secs: u64,

            /// Filesystem name reported to the kernel mount table.
            #[arg(long, default_value = "clawfs")]
            pub fuse_fsname: String,
        }
    };
}

#[macro_export]
macro_rules! shared_config_struct {
    ($($extra_fields:tt)*) => {
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
            $($extra_fields)*
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
            pub cleanup_interval_secs: u64,
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
    };
}

#[macro_export]
macro_rules! shared_source_store_config_struct {
    () => {
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
    };
}

#[macro_export]
macro_rules! shared_config_with_paths_expr {
    ($mount_path:expr, $store_path:expr, $local_cache_path:expr, $state_path:expr, $writeback_cache:expr, $($extra_fields:tt)*) => {
        Self {
            mount_path: $mount_path,
            store_path: $store_path,
            local_cache_path: $local_cache_path,
            log_storage_io: false,
            inline_threshold: 32 * 1024,
            inline_compression: true,
            inline_encryption_key: None,
            segment_compression: true,
            segment_encryption_key: None,
            shard_size: 2048,
            inode_batch: 8192,
            segment_batch: 2560,
            pending_bytes: 256 * 1024 * 1024,
            home_prefix: "/home".to_string(),
            object_provider: ObjectStoreProvider::Local,
            bucket: None,
            region: None,
            endpoint: None,
            $($extra_fields)*
            object_prefix: String::new(),
            telemetry_object_prefix: None,
            gcs_service_account: None,
            aws_allow_http: false,
            aws_force_path_style: false,
            source: None,
            state_path: $state_path,
            perf_log: None,
            replay_log: None,
            disable_journal: false,
            fsync_on_close: false,
            flush_interval_ms: 5000,
            disable_cleanup: false,
            cleanup_interval_secs: 30,
            lookup_cache_ttl_ms: 5000,
            dir_cache_ttl_ms: 5000,
            metadata_poll_interval_ms: 2000,
            segment_cache_bytes: 512 * 1024 * 1024,
            foreground: false,
            allow_other: false,
            log_file: None,
            debug_log: false,
            imap_delta_batch: 512,
            writeback_cache: $writeback_cache,
            fuse_threads: default_fuse_threads(),
            entry_ttl_secs: 5,
            fuse_fsname: "clawfs".to_string(),
        }
    };
}

#[macro_export]
macro_rules! shared_source_from_cli_expr {
    ($cli:ident) => {
        if $cli.source_bucket.is_some() || $cli.source_store_path.is_some() {
            Some(SourceStoreConfig {
                object_provider: $cli.source_object_provider.unwrap_or($cli.object_provider),
                bucket: $cli.source_bucket,
                prefix: $cli.source_prefix,
                store_path: $cli.source_store_path,
                region: $cli.source_region,
                endpoint: $cli.source_endpoint,
                gcs_service_account: $cli.source_gcs_service_account,
                aws_access_key_id: $cli.source_aws_access_key_id,
                aws_secret_access_key: $cli.source_aws_secret_access_key,
                aws_allow_http: $cli.source_aws_allow_http,
                aws_force_path_style: $cli.source_aws_force_path_style,
            })
        } else {
            None
        }
    };
}

#[macro_export]
macro_rules! shared_config_from_cli_expr {
    ($cli:ident, $source:ident, $log_file:ident, $store_path:ident, $local_cache_path:ident, $state_path:ident, $($extra_fields:tt)*) => {
        Config {
            log_storage_io: $cli.log_storage_io,
            inline_threshold: $cli.inline_threshold,
            inline_compression: $cli.inline_compression,
            inline_encryption_key: $cli
                .inline_encryption_key
                .and_then(|value| (!value.trim().is_empty()).then_some(value)),
            segment_compression: $cli.segment_compression,
            segment_encryption_key: $cli
                .segment_encryption_key
                .and_then(|value| (!value.trim().is_empty()).then_some(value)),
            shard_size: $cli.shard_size,
            inode_batch: $cli.inode_batch.max(1),
            segment_batch: $cli.segment_batch.max(1),
            pending_bytes: $cli.pending_bytes.clamp(1024, u64::MAX),
            home_prefix: $cli.home_prefix,
            object_provider: $cli.object_provider,
            bucket: $cli.bucket,
            region: $cli.region,
            endpoint: $cli.endpoint,
            $($extra_fields)*
            object_prefix: $cli.object_prefix,
            gcs_service_account: $cli.gcs_service_account,
            aws_allow_http: $cli.aws_allow_http,
            aws_force_path_style: $cli.aws_force_path_style,
            source: $source,
            perf_log: $cli.perf_log,
            replay_log: $cli.replay_log,
            disable_journal: $cli.disable_journal,
            fsync_on_close: $cli.fsync_on_close,
            flush_interval_ms: $cli.flush_interval_ms,
            disable_cleanup: $cli.disable_cleanup,
            cleanup_interval_secs: $cli.cleanup_interval_secs.max(1),
            lookup_cache_ttl_ms: $cli.lookup_cache_ttl_ms,
            dir_cache_ttl_ms: $cli.dir_cache_ttl_ms,
            metadata_poll_interval_ms: $cli.metadata_poll_interval_ms,
            segment_cache_bytes: $cli.segment_cache_bytes,
            foreground: $cli.foreground,
            allow_other: $cli.allow_other,
            log_file: $log_file,
            debug_log: $cli.debug_log,
            imap_delta_batch: $cli.imap_delta_batch.max(1),
            writeback_cache: $cli.writeback_cache,
            fuse_threads: $cli.fuse_threads,
            entry_ttl_secs: $cli.entry_ttl_secs,
            fuse_fsname: if $cli.fuse_fsname.trim().is_empty() {
                "clawfs".to_string()
            } else {
                $cli.fuse_fsname
            },
            ..Config::with_paths(
                $cli.mount_path.unwrap_or_else(|| {
                    std::env::current_dir()
                        .unwrap_or_else(|_| PathBuf::from("."))
                        .join(DEFAULT_MOUNT_DIR)
                }),
                $store_path,
                $local_cache_path,
                $state_path,
            )
        }
    };
}

#[macro_export]
macro_rules! shared_default_path_helpers {
    () => {
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
    };
}

#[macro_export]
macro_rules! shared_source_store_to_public_expr {
    ($source:expr, $object_provider:expr) => {
        $crate::config::SourceStoreConfig {
            object_provider: $object_provider,
            bucket: $source.bucket.clone(),
            prefix: $source.prefix.clone(),
            store_path: $source.store_path.clone(),
            region: $source.region.clone(),
            endpoint: $source.endpoint.clone(),
            gcs_service_account: $source.gcs_service_account.clone(),
            aws_access_key_id: $source.aws_access_key_id.clone(),
            aws_secret_access_key: $source.aws_secret_access_key.clone(),
            aws_allow_http: $source.aws_allow_http,
            aws_force_path_style: $source.aws_force_path_style,
        }
    };
}

#[macro_export]
macro_rules! shared_config_to_public_expr {
    ($config:expr, $object_provider:expr, $source:expr) => {
        $crate::config::Config {
            mount_path: $config.mount_path.clone(),
            store_path: $config.store_path.clone(),
            local_cache_path: $config.local_cache_path.clone(),
            log_storage_io: $config.log_storage_io,
            inline_threshold: $config.inline_threshold,
            inline_compression: $config.inline_compression,
            inline_encryption_key: $config.inline_encryption_key.clone(),
            segment_compression: $config.segment_compression,
            segment_encryption_key: $config.segment_encryption_key.clone(),
            shard_size: $config.shard_size,
            inode_batch: $config.inode_batch,
            segment_batch: $config.segment_batch,
            pending_bytes: $config.pending_bytes,
            home_prefix: $config.home_prefix.clone(),
            object_provider: $object_provider,
            bucket: $config.bucket.clone(),
            region: $config.region.clone(),
            endpoint: $config.endpoint.clone(),
            object_prefix: $config.object_prefix.clone(),
            telemetry_object_prefix: $config.telemetry_object_prefix.clone(),
            gcs_service_account: $config.gcs_service_account.clone(),
            aws_allow_http: $config.aws_allow_http,
            aws_force_path_style: $config.aws_force_path_style,
            source: $source,
            state_path: $config.state_path.clone(),
            perf_log: $config.perf_log.clone(),
            replay_log: $config.replay_log.clone(),
            disable_journal: $config.disable_journal,
            fsync_on_close: $config.fsync_on_close,
            flush_interval_ms: $config.flush_interval_ms,
            disable_cleanup: $config.disable_cleanup,
            cleanup_interval_secs: $config.cleanup_interval_secs,
            lookup_cache_ttl_ms: $config.lookup_cache_ttl_ms,
            dir_cache_ttl_ms: $config.dir_cache_ttl_ms,
            metadata_poll_interval_ms: $config.metadata_poll_interval_ms,
            segment_cache_bytes: $config.segment_cache_bytes,
            foreground: $config.foreground,
            allow_other: $config.allow_other,
            log_file: $config.log_file.clone(),
            debug_log: $config.debug_log,
            imap_delta_batch: $config.imap_delta_batch,
            writeback_cache: $config.writeback_cache,
            fuse_threads: $config.fuse_threads,
            entry_ttl_secs: $config.entry_ttl_secs,
            fuse_fsname: $config.fuse_fsname.clone(),
        }
    };
}

#[macro_export]
macro_rules! shared_object_store_provider_enum {
    () => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
        pub enum ObjectStoreProvider {
            Local,
            Aws,
            Gcs,
        }
    };
}
