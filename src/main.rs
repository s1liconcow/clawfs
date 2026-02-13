use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use env_logger::Env;
use fuser::MountOption;
use log::{LevelFilter, info, warn};
use std::env;

use osagefs::config::{Cli, Config};
use osagefs::fs::OsageFs;
use osagefs::inode::{FileStorage, InodeRecord, ROOT_INODE};
use osagefs::journal::JournalManager;
use osagefs::metadata::MetadataStore;
use osagefs::perf::PerfLogger;
use osagefs::segment::{SegmentEntry, SegmentManager, SegmentPointer};
use osagefs::state::ClientStateManager;
use osagefs::superblock::{CleanupTaskKind, SuperblockManager};
use tokio::runtime::Handle;
use tokio::task;
use tokio::time::sleep;

const DELTA_COMPACT_THRESHOLD: usize = 128;
const DELTA_COMPACT_KEEP: usize = 32;
const SEGMENT_COMPACT_BATCH: usize = 8;
const SEGMENT_COMPACT_LAG: u64 = 3;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config: Config = cli.into();
    init_logging(config.log_file.as_deref(), config.debug_log)?;
    std::fs::create_dir_all(&config.mount_path)?;

    let runtime = tokio::runtime::Runtime::new()?;
    let handle = runtime.handle().clone();
    let metadata =
        Arc::new(runtime.block_on(MetadataStore::open(&config.store_path, config.shard_size))?);
    let superblock = Arc::new(runtime.block_on(SuperblockManager::load_or_init(
        metadata.clone(),
        config.shard_size,
    ))?);
    ensure_root(&runtime, metadata.clone(), superblock.clone(), &config)?;
    let segments = Arc::new(SegmentManager::new(&config, handle.clone())?);
    if config.metadata_poll_interval_ms > 0 {
        let poll_interval = Duration::from_millis(config.metadata_poll_interval_ms);
        spawn_metadata_poller(
            handle.clone(),
            metadata.clone(),
            segments.clone(),
            poll_interval,
        );
    }
    let client_state = Arc::new(ClientStateManager::load(&config.state_path)?);
    let client_id = client_state.client_id();
    if !config.disable_cleanup {
        spawn_cleanup_worker(
            handle.clone(),
            metadata.clone(),
            superblock.clone(),
            segments.clone(),
            client_id.clone(),
        );
    } else {
        info!(target: "cleanup", "local cleanup worker disabled via --disable-cleanup");
    }

    let perf_logger = if let Some(path) = config.perf_log.clone() {
        Some(Arc::new(PerfLogger::new(path)?))
    } else {
        None
    };

    let journal = if config.disable_journal {
        None
    } else {
        Some(Arc::new(JournalManager::new(&config.store_path)?))
    };
    let fs = OsageFs::new(
        config.clone(),
        metadata.clone(),
        superblock.clone(),
        segments,
        journal,
        handle,
        client_state,
        perf_logger.clone(),
    );

    let replayed = fs.replay_journal()?;
    if replayed > 0 {
        info!("Replayed {replayed} journaled entries before mount");
    }

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
            let snapshot = superblock.prepare_dirty_generation()?;
            let generation = snapshot.generation;
            if let Err(err) =
                runtime.block_on(metadata.persist_inode(&existing, generation, config.shard_size))
            {
                superblock.abort_generation(generation);
                return Err(err);
            }
            runtime.block_on(superblock.commit_generation(generation))?;
        }
        return Ok(());
    }
    let snapshot = superblock.prepare_dirty_generation()?;
    let generation = snapshot.generation;
    let mut root = InodeRecord::new_directory(
        ROOT_INODE,
        ROOT_INODE,
        String::from(""),
        String::from("/"),
        uid,
        gid,
    );
    root.mode = desired_mode;
    if let Err(err) = runtime.block_on(metadata.persist_inode(&root, generation, config.shard_size))
    {
        superblock.abort_generation(generation);
        return Err(err);
    }
    runtime.block_on(superblock.commit_generation(generation))?;
    if !config.home_prefix.is_empty() {
        ensure_directory_path(
            runtime,
            metadata.clone(),
            superblock.clone(),
            config,
            &config.home_prefix,
            uid,
            gid,
            0o40755,
        )?;
        let username = env::var("USER")
            .or_else(|_| env::var("LOGNAME"))
            .unwrap_or_else(|_| format!("uid{uid}"));
        let user_path = format!("{}/{}", config.home_prefix.trim_end_matches('/'), username);
        ensure_directory_path(
            runtime, metadata, superblock, config, &user_path, uid, gid, 0o40755,
        )?;
    }
    Ok(())
}

fn init_logging(log_path: Option<&Path>, force_debug: bool) -> Result<()> {
    let env = Env::default().default_filter_or("info");
    let mut builder = env_logger::Builder::from_env(env);
    if let Some(path) = log_path {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let writer = Arc::new(Mutex::new(BufWriter::new(file)));
        builder.format(move |buf, record| {
            let ts = buf.timestamp();
            let line = format!(
                "{} [{}] {} - {}",
                ts,
                record.level(),
                record.target(),
                record.args()
            );
            {
                if let Ok(mut guard) = writer.lock() {
                    let _ = writeln!(guard, "{}", line);
                    let _ = guard.flush();
                }
            }
            writeln!(buf, "{}", line)
        });
    }
    if force_debug {
        builder.filter_level(LevelFilter::Debug);
    }
    builder.try_init()?;
    Ok(())
}

fn spawn_metadata_poller(
    handle: Handle,
    metadata: Arc<MetadataStore>,
    segments: Arc<SegmentManager>,
    interval: Duration,
) {
    handle.spawn(async move {
        loop {
            let md = metadata.clone();
            let segs = segments.clone();
            let result = tokio::task::spawn_blocking(move || md.apply_external_deltas()).await;
            match result {
                Ok(Ok(records)) => {
                    for record in records {
                        if let FileStorage::Segment(ptr) = record.storage {
                            if let Err(err) = segs.prefetch_segment(&ptr) {
                                warn!("segment prefetch failed: {err:?}");
                            }
                        }
                    }
                }
                Ok(Err(err)) => warn!("metadata poll failed: {err:?}"),
                Err(err) => warn!("metadata poll join error: {err}"),
            }
            sleep(interval).await;
        }
    });
}

fn spawn_cleanup_worker(
    handle: Handle,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    segments: Arc<SegmentManager>,
    client_id: String,
) {
    handle.spawn(async move {
        let lease_ttl = Duration::from_secs(30);
        loop {
            let mut did_work = false;
            let delta_count = task::spawn_blocking({
                let md = metadata.clone();
                move || md.delta_file_count()
            })
            .await
            .unwrap_or(Ok(0))
            .unwrap_or(0);
            if delta_count > DELTA_COMPACT_THRESHOLD {
                if superblock
                    .try_acquire_cleanup(CleanupTaskKind::DeltaCompaction, &client_id, lease_ttl)
                    .await
                    .unwrap_or(false)
                {
                    if let Err(err) = task::spawn_blocking({
                        let md = metadata.clone();
                        move || md.prune_deltas(DELTA_COMPACT_KEEP)
                    })
                    .await
                    .unwrap_or(Ok(0))
                    {
                        warn!("delta prune failed: {err:?}");
                    }
                    if let Err(err) = superblock
                        .complete_cleanup(CleanupTaskKind::DeltaCompaction, &client_id)
                        .await
                    {
                        warn!("cleanup lease release failed: {err:?}");
                    }
                    did_work = true;
                }
            }
            if !did_work {
                let current_generation = superblock.snapshot().generation;
                let cutoff_generation = current_generation.saturating_sub(SEGMENT_COMPACT_LAG);
                if cutoff_generation == 0 {
                    continue;
                }
                let candidates = task::spawn_blocking({
                    let md = metadata.clone();
                    move || md.segment_candidates(SEGMENT_COMPACT_BATCH)
                })
                .await
                .unwrap_or(Ok(Vec::new()))
                .unwrap_or_default();
                let filtered: Vec<_> = candidates
                    .into_iter()
                    .filter(|(_, ptr)| ptr.generation < cutoff_generation)
                    .collect();
                if filtered.len() >= 2
                    && superblock
                        .try_acquire_cleanup(
                            CleanupTaskKind::SegmentCompaction,
                            &client_id,
                            lease_ttl,
                        )
                        .await
                        .unwrap_or(false)
                {
                    if let Err(err) = perform_segment_compaction(
                        metadata.clone(),
                        superblock.clone(),
                        segments.clone(),
                        filtered,
                    )
                    .await
                    {
                        warn!("segment compaction failed: {err:?}");
                    }
                    if let Err(err) = superblock
                        .complete_cleanup(CleanupTaskKind::SegmentCompaction, &client_id)
                        .await
                    {
                        warn!("cleanup lease release failed: {err:?}");
                    }
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn perform_segment_compaction(
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    segments: Arc<SegmentManager>,
    candidates: Vec<(InodeRecord, SegmentPointer)>,
) -> Result<()> {
    if candidates.len() < 2 {
        return Ok(());
    }
    let dataset = task::spawn_blocking({
        let segs = segments.clone();
        move || -> Result<Vec<(InodeRecord, SegmentPointer, Vec<u8>)>> {
            let mut out = Vec::new();
            for (record, ptr) in candidates {
                let data = segs.read_pointer(&ptr)?;
                out.push((record, ptr, data));
            }
            Ok(out)
        }
    })
    .await??;
    if dataset.is_empty() {
        return Ok(());
    }
    let snapshot = superblock.prepare_dirty_generation()?;
    let generation = snapshot.generation;
    let segment_id = superblock.reserve_segments(1).await?;
    let mut entries = Vec::with_capacity(dataset.len());
    for (record, _, data) in &dataset {
        entries.push(SegmentEntry {
            inode: record.inode,
            path: record.path.clone(),
            data: data.clone(),
        });
    }
    let segments_clone = segments.clone();
    let pointer_map: HashMap<u64, SegmentPointer> = tokio::task::spawn_blocking(move || {
        segments_clone
            .write_batch(generation, segment_id, entries)
            .map(|res| res.into_iter().collect())
    })
    .await??;
    let result: Result<()> = async {
        for (mut record, old_ptr, _) in dataset {
            if let Some(new_ptr) = pointer_map.get(&record.inode) {
                record.storage = FileStorage::Segment(new_ptr.clone());
                metadata
                    .persist_inode(&record, generation, snapshot.shard_size)
                    .await?;
                let segs = segments.clone();
                task::spawn_blocking(move || {
                    segs.delete_segment(old_ptr.generation, old_ptr.segment_id)
                })
                .await??;
            }
        }
        Ok(())
    }
    .await;
    if let Err(err) = result {
        superblock.abort_generation(generation);
        return Err(err);
    }
    superblock.commit_generation(generation).await?;
    Ok(())
}

fn ensure_directory_path(
    runtime: &tokio::runtime::Runtime,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    config: &Config,
    path: &str,
    uid: u32,
    gid: u32,
    mode: u32,
) -> Result<()> {
    let normalized = path.trim_matches('/');
    if normalized.is_empty() {
        return Ok(());
    }
    let mut current = ROOT_INODE;
    for component in normalized.split('/') {
        if component.is_empty() {
            continue;
        }
        let mut parent = runtime
            .block_on(metadata.get_inode(current))?
            .ok_or_else(|| anyhow::anyhow!("missing parent inode {}", current))?;
        if let Some(child_ino) = parent
            .children()
            .and_then(|children| children.get(component).copied())
        {
            let mut child = runtime
                .block_on(metadata.get_inode(child_ino))?
                .ok_or_else(|| anyhow::anyhow!("missing child inode {}", child_ino))?;
            if child.uid != uid || child.gid != gid || (child.mode & 0o777) != (mode & 0o777) {
                child.uid = uid;
                child.gid = gid;
                child.mode = (child.mode & !0o777) | (mode & 0o777);
                let snapshot = superblock.prepare_dirty_generation()?;
                let generation = snapshot.generation;
                if let Err(err) =
                    runtime.block_on(metadata.persist_inode(&child, generation, config.shard_size))
                {
                    superblock.abort_generation(generation);
                    return Err(err);
                }
                runtime.block_on(superblock.commit_generation(generation))?;
            }
            current = child_ino;
            continue;
        }

        let new_inode = runtime.block_on(superblock.reserve_inodes(1))?;
        let snapshot = superblock.prepare_dirty_generation()?;
        let generation = snapshot.generation;
        let child_path = if parent.path == "/" {
            format!("/{}", component)
        } else {
            format!("{}/{}", parent.path.trim_end_matches('/'), component)
        };
        let mut child = InodeRecord::new_directory(
            new_inode,
            current,
            component.to_string(),
            child_path,
            uid,
            gid,
        );
        child.mode = (child.mode & !0o777) | (mode & 0o777);
        if let Err(err) =
            runtime.block_on(metadata.persist_inode(&child, generation, config.shard_size))
        {
            superblock.abort_generation(generation);
            return Err(err);
        }
        if let Some(children) = parent.children_mut() {
            children.insert(component.to_string(), new_inode);
        }
        if let Err(err) =
            runtime.block_on(metadata.persist_inode(&parent, generation, config.shard_size))
        {
            superblock.abort_generation(generation);
            return Err(err);
        }
        runtime.block_on(superblock.commit_generation(generation))?;
        current = new_inode;
    }
    Ok(())
}
