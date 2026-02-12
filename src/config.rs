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
    #[arg(long, default_value_t = 1024)]
    pub inline_threshold: usize,

    /// Number of inodes mapped to a shard file.
    #[arg(long, default_value_t = 2048)]
    pub shard_size: u64,

    /// Number of inode numbers to reserve per allocation.
    #[arg(long, default_value_t = 128)]
    pub inode_batch: u64,

    /// Number of segment ids to reserve per allocation.
    #[arg(long, default_value_t = 256)]
    pub segment_batch: u64,

    /// Total pending data (bytes) before forcing a flush to the backing store.
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    pub pending_bytes: u64,

    /// Run the filesystem in the foreground (useful for debugging).
    #[arg(long, default_value_t = false)]
    pub foreground: bool,
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
    pub object_provider: ObjectStoreProvider,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub object_prefix: String,
    pub gcs_service_account: Option<PathBuf>,
    pub state_path: PathBuf,
    pub foreground: bool,
}

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        let state_path = cli
            .state_path
            .clone()
            .unwrap_or_else(|| default_state_path(&cli.mount_path));
        Config {
            mount_path: cli.mount_path,
            store_path: cli.store_path,
            inline_threshold: cli.inline_threshold,
            shard_size: cli.shard_size,
            inode_batch: cli.inode_batch.max(1),
            segment_batch: cli.segment_batch.max(1),
            pending_bytes: cli.pending_bytes.clamp(1024, u64::MAX),
            object_provider: cli.object_provider,
            bucket: cli.bucket,
            region: cli.region,
            endpoint: cli.endpoint,
            object_prefix: cli.object_prefix,
            gcs_service_account: cli.gcs_service_account,
            state_path,
            foreground: cli.foreground,
        }
    }
}

fn default_state_path(mount: &PathBuf) -> PathBuf {
    mount.join(".osagefs_state.json")
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ObjectStoreProvider {
    Local,
    Aws,
    Gcs,
}
