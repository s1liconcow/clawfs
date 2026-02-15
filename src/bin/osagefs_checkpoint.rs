use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use osagefs::checkpoint::{create_checkpoint, restore_checkpoint};

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
    match cli.command {
        Command::Create {
            store_path,
            checkpoint_path,
            shard_size,
            log_storage_io,
            note,
        } => {
            let saved = create_checkpoint(
                &store_path,
                shard_size,
                log_storage_io,
                &checkpoint_path,
                note,
            )
            .await?;
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
            let restored =
                restore_checkpoint(&store_path, shard_size, log_storage_io, &checkpoint_path)
                    .await?;
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
