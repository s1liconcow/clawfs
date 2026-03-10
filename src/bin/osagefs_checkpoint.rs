use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::runtime::Handle;

use osagefs::checkpoint::{create_checkpoint, restore_checkpoint};
use osagefs::config::{Config, ObjectStoreProvider};

#[derive(Debug, Parser)]
#[command(
    name = "osagefs_checkpoint",
    version,
    about = "Create and restore OsageFS superblock checkpoints"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Save the current superblock to a checkpoint file.
    Create {
        #[arg(long, value_name = "PATH")]
        store_path: PathBuf,
        #[arg(long, value_name = "PATH")]
        checkpoint_path: PathBuf,
        #[arg(long, default_value_t = 2048)]
        shard_size: u64,
        #[arg(long, default_value_t = false)]
        log_storage_io: bool,
        #[arg(long)]
        note: Option<String>,
    },
    /// Restore the superblock from a checkpoint file.
    Restore {
        #[arg(long, value_name = "PATH")]
        store_path: PathBuf,
        #[arg(long, value_name = "PATH")]
        checkpoint_path: PathBuf,
        #[arg(long, default_value_t = 2048)]
        shard_size: u64,
        #[arg(long, default_value_t = false)]
        log_storage_io: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let handle = Handle::current();

    match cli.command {
        Command::Create {
            store_path,
            checkpoint_path,
            shard_size,
            log_storage_io,
            note,
        } => {
            let config = build_config(store_path, shard_size, log_storage_io);
            let saved = create_checkpoint(&config, handle, &checkpoint_path, note).await?;
            println!(
                "checkpoint saved path={} generation={} next_inode={} next_segment={}",
                saved.checkpoint_path, saved.generation, saved.next_inode, saved.next_segment
            );
        }
        Command::Restore {
            store_path,
            checkpoint_path,
            shard_size,
            log_storage_io,
        } => {
            let config = build_config(store_path, shard_size, log_storage_io);
            let restored = restore_checkpoint(&config, handle, &checkpoint_path).await?;
            println!(
                "checkpoint restored path={} generation={} next_inode={} next_segment={}",
                restored.checkpoint_path,
                restored.generation,
                restored.next_inode,
                restored.next_segment
            );
        }
    }
    Ok(())
}

fn build_config(store_path: PathBuf, shard_size: u64, log_storage_io: bool) -> Config {
    Config {
        mount_path: PathBuf::from("/tmp/osagefs_checkpoint_mnt"),
        store_path,
        local_cache_path: PathBuf::from("/tmp/osagefs_checkpoint_cache"),
        log_storage_io,
        inline_threshold: 4096,
        inline_compression: true,
        inline_encryption_key: None,
        segment_compression: true,
        segment_encryption_key: None,
        shard_size,
        inode_batch: 128,
        segment_batch: 128,
        pending_bytes: 1024 * 1024,
        entry_ttl_secs: 10,
        home_prefix: "/home".to_string(),
        object_provider: ObjectStoreProvider::Local,
        bucket: None,
        region: None,
        endpoint: None,
        object_prefix: String::new(),
        gcs_service_account: None,
        aws_allow_http: false,
        aws_force_path_style: false,
        source: None,
        state_path: PathBuf::from("/tmp/osagefs_checkpoint_state"),
        perf_log: None,
        replay_log: None,
        disable_journal: true,
        fsync_on_close: false,
        flush_interval_ms: 0,
        disable_cleanup: true,
        lookup_cache_ttl_ms: 0,
        dir_cache_ttl_ms: 0,
        metadata_poll_interval_ms: 0,
        segment_cache_bytes: 0,
        foreground: false,
        allow_other: false,
        log_file: None,
        debug_log: false,
        imap_delta_batch: 32,
        writeback_cache: false,
        fuse_threads: 0,
        fuse_fsname: "osagefs".to_string(),
    }
}
