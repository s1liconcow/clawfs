use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use fuser::MountOption;
use log::info;

use osagefs::config::{Cli, Config};
use osagefs::fs::OsageFs;
use osagefs::inode::{InodeRecord, ROOT_INODE};
use osagefs::metadata::MetadataStore;
use osagefs::segment::SegmentManager;
use osagefs::state::ClientStateManager;
use osagefs::superblock::{FilesystemState, SuperblockManager};

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let config: Config = cli.into();
    std::fs::create_dir_all(&config.mount_path)?;

    let runtime = tokio::runtime::Runtime::new()?;
    let handle = runtime.handle().clone();
    let metadata = Arc::new(runtime.block_on(MetadataStore::open(&config.store_path))?);
    let superblock = Arc::new(runtime.block_on(SuperblockManager::load_or_init(
        metadata.clone(),
        config.shard_size,
    ))?);
    ensure_root(&runtime, metadata.clone(), superblock.clone(), &config)?;
    let segments = Arc::new(SegmentManager::new(&config, handle.clone())?);
    let client_state = Arc::new(ClientStateManager::load(&config.state_path)?);

    let fs = OsageFs::new(
        config.clone(),
        metadata.clone(),
        superblock.clone(),
        segments,
        handle,
        client_state,
    );

    let mut options = vec![
        MountOption::FSName("osagefs".to_string()),
        MountOption::DefaultPermissions,
    ];
    if config.foreground {
        options.push(MountOption::AllowRoot);
    } else {
        options.push(MountOption::AllowRoot);
        options.push(MountOption::AutoUnmount);
    }
    info!("Mounting OsageFS at {}", config.mount_path.display());
    fuser::mount2(fs, &config.mount_path, &options)?;

    runtime.block_on(async {
        superblock.mark_clean().await.ok();
        metadata.shutdown().await.ok();
    });
    Ok(())
}

fn ensure_root(
    runtime: &tokio::runtime::Runtime,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    config: &Config,
) -> Result<()> {
    let uid = unsafe { libc::geteuid() as u32 };
    let gid = unsafe { libc::getegid() as u32 };
    let desired_mode = 0o40777;
    if let Some(mut existing) = runtime.block_on(metadata.get_inode(ROOT_INODE))? {
        if existing.uid != uid || existing.gid != gid || existing.mode != desired_mode {
            existing.uid = uid;
            existing.gid = gid;
            existing.mode = desired_mode;
            let (_, snapshot) = runtime.block_on(superblock.mutate(|sb| {
                sb.state = FilesystemState::Dirty;
            }))?;
            runtime.block_on(metadata.persist_inode(
                &existing,
                snapshot.generation,
                config.shard_size,
            ))?;
            runtime.block_on(superblock.mark_clean())?;
        }
        return Ok(());
    }
    let (_, snapshot) = runtime.block_on(superblock.mutate(|sb| {
        sb.state = FilesystemState::Dirty;
    }))?;
    let mut root = InodeRecord::new_directory(
        ROOT_INODE,
        ROOT_INODE,
        String::from(""),
        String::from("/"),
        uid,
        gid,
    );
    root.mode = desired_mode;
    runtime.block_on(metadata.persist_inode(&root, snapshot.generation, config.shard_size))?;
    runtime.block_on(superblock.mark_clean())?;
    Ok(())
}
