use std::env;
use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
#[cfg(feature = "fuse")]
use fuser::MountOption;
use log::{info, warn};
use serde_json::json;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use tokio::runtime::Handle;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use crate::clawfs as clawfs_runtime;
use crate::config::Config;
use crate::fs::OsageFs;
use crate::inode::{FileStorage, InodeRecord, ROOT_INODE};
use crate::journal::JournalManager;
use crate::metadata::MetadataStore;
use crate::perf::PerfLogger;
use crate::replay::ReplayLogger;
use crate::segment::SegmentManager;
use crate::source::SourceObjectStore;
use crate::state::ClientStateManager;
use crate::superblock::{CleanupTaskKind, SuperblockManager};
use crate::telemetry::{TelemetryClient, set_panic_context};

const WELCOME_FILENAME: &str = "WELCOME.txt";
const WELCOME_CONTENT: &str = "Welcome to ClawFS!\n\
\n\
ClawFS is a log-structured, object-store-backed filesystem designed for fast,\n\
shared access to large working sets with durable metadata and batched writes.\n\
\n\
Great use cases:\n\
- AI training data and model artifacts shared across multiple machines\n\
- Shared home directories for teams, labs, or ephemeral compute nodes\n\
- High-throughput team access to large binaries, build outputs, and datasets\n\
\n\
Why teams use it:\n\
- Immutable segment writes for efficient object-store IO\n\
- Batched metadata updates for lower API overhead\n\
- Local staging, caching, and journal replay for practical durability and speed\n\
\n\
Enjoy building on ClawFS.\n";

#[cfg(feature = "fuse")]
pub fn run_mount_entry(config: Config, args: &[OsString]) -> Result<()> {
    if config.foreground {
        return run_mount(config);
    }

    spawn_background_mount(args, &config.mount_path)?;
    Ok(())
}

#[cfg(feature = "fuse")]
pub fn spawn_background_mount(args: &[OsString], mount_path: &Path) -> Result<()> {
    let existing_mounts = current_mount_count(mount_path);
    if existing_mounts > 0 {
        anyhow::bail!(
            "mount path {} is already mounted ({} {}); unmount it before starting a background mount",
            mount_path.display(),
            existing_mounts,
            if existing_mounts == 1 {
                "entry"
            } else {
                "entries"
            }
        );
    }

    let exe = env::current_exe()?;
    let mut child_args: Vec<_> = args.iter().skip(1).cloned().collect();
    child_args.push("--foreground".into());

    let mut command = Command::new(exe);
    command
        .args(&child_args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let mut child = command.spawn()?;

    for _ in 0..50 {
        match child.try_wait()? {
            Some(status) if !status.success() => {
                let mut stderr = String::new();
                if let Some(mut pipe) = child.stderr.take() {
                    use std::io::Read;
                    let _ = pipe.read_to_string(&mut stderr);
                }
                anyhow::bail!("mount failed (exit {}): {}", status, stderr.trim());
            }
            _ => {}
        }
        if current_mount_count(mount_path) > existing_mounts {
            println!("Mounted at {:?}", mount_path);
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    println!(
        "Mount process started (pid {}), but mount not yet visible",
        child.id()
    );
    Ok(())
}

#[cfg(feature = "fuse")]
pub fn current_mount_count(mount_path: &Path) -> usize {
    #[cfg(target_os = "linux")]
    {
        let mount_str = mount_path.to_string_lossy();
        if let Ok(mountinfo) = std::fs::read_to_string("/proc/self/mountinfo") {
            return count_mounts_for_target(&mountinfo, mount_str.as_ref(), 4);
        }
        if let Ok(mounts) = std::fs::read_to_string("/proc/mounts") {
            return count_mounts_for_target(&mounts, mount_str.as_ref(), 1);
        }
    }

    0
}

#[cfg(feature = "fuse")]
pub fn count_mounts_for_target(contents: &str, mount_target: &str, field_index: usize) -> usize {
    contents
        .lines()
        .filter_map(|line| line.split_whitespace().nth(field_index))
        .filter(|field| *field == mount_target)
        .count()
}

#[cfg(feature = "fuse")]
fn run_mount(mut config: Config) -> Result<()> {
    clawfs_runtime::apply_env_runtime_spec(&mut config)?;
    init_logging(config.log_file.as_deref(), config.debug_log)?;
    std::fs::create_dir_all(&config.mount_path)?;

    let telemetry = TelemetryClient::from_config(&config)?;
    let telemetry_session_id = telemetry.as_ref().map(|_| Uuid::new_v4().to_string());
    if let Some(client) = &telemetry {
        set_panic_context(
            Some(client.destination()),
            "mount_runtime",
            telemetry_session_id.clone(),
        );
        client.emit(
            "runtime.session_start",
            telemetry_session_id.as_deref(),
            json!({
                "volume": Option::<String>::None,
                "mode": "fuse",
            }),
        );
    }

    let runtime = tokio::runtime::Runtime::new()?;
    let handle = runtime.handle().clone();
    let metadata = Arc::new(runtime.block_on(MetadataStore::new(&config, handle.clone()))?);
    let segments = Arc::new(SegmentManager::new(&config, handle.clone())?);
    let superblock = Arc::new(runtime.block_on(SuperblockManager::load_or_init(
        metadata.clone(),
        config.shard_size,
    ))?);
    ensure_root(&runtime, metadata.clone(), superblock.clone(), &config)?;

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
    let superblock_snapshot = superblock.snapshot();
    client_state.reconcile_with_minimums(
        superblock_snapshot.next_inode,
        superblock_snapshot.next_segment,
    )?;
    let client_id = client_state.client_id();

    {
        use crate::maintenance::CleanupPolicy;
        let cleanup_policy = CleanupPolicy::from_config(&config);
        if config.disable_cleanup {
            info!(
                target: "cleanup",
                "cleanup_policy={} local cleanup worker disabled via --disable-cleanup",
                cleanup_policy.as_str()
            );
        } else if cleanup_policy.should_spawn_local_worker() {
            info!(
                target: "cleanup",
                "cleanup_policy={} spawning local cleanup worker",
                cleanup_policy.as_str()
            );
            spawn_cleanup_worker(
                handle.clone(),
                metadata.clone(),
                superblock.clone(),
                segments.clone(),
                client_id,
            );
        }
    }

    let perf_logger = if let Some(path) = config.perf_log.clone() {
        Some(Arc::new(PerfLogger::new(path)?))
    } else {
        None
    };
    let replay_logger = if let Some(path) = config.replay_log.clone() {
        Some(Arc::new(ReplayLogger::new(path)?))
    } else {
        None
    };
    if let Some(logger) = &replay_logger {
        logger.log_meta(
            "fs_config",
            json!({
                "mode": "fuse",
                "home_prefix": config.home_prefix.clone(),
                "inline_threshold": config.inline_threshold,
                "pending_bytes": config.pending_bytes,
                "fsync_on_close": config.fsync_on_close,
                "flush_interval_ms": config.flush_interval_ms,
                "disable_journal": config.disable_journal,
                "lookup_cache_ttl_ms": config.lookup_cache_ttl_ms,
                "dir_cache_ttl_ms": config.dir_cache_ttl_ms,
                "metadata_poll_interval_ms": config.metadata_poll_interval_ms,
                "segment_cache_bytes": config.segment_cache_bytes,
                "imap_delta_batch": config.imap_delta_batch,
                "source_enabled": config.source.is_some(),
                "source_provider": config
                    .source
                    .as_ref()
                    .map(|source| format!("{:?}", source.object_provider).to_lowercase()),
                "source_bucket": config.source.as_ref().and_then(|source| source.bucket.as_deref()),
                "source_prefix": config.source.as_ref().map(|source| source.prefix.as_str()),
                "bootstrap_user": env::var("USER")
                    .or_else(|_| env::var("LOGNAME"))
                    .ok(),
            }),
        );
    }

    let journal = if config.disable_journal {
        None
    } else {
        Some(Arc::new(JournalManager::new(&config.local_cache_path)?))
    };
    let source = if let Some(source_cfg) = &config.source {
        Some(Arc::new(SourceObjectStore::new(source_cfg)?))
    } else {
        None
    };
    let fs = OsageFs::new(
        config.clone(),
        metadata.clone(),
        superblock.clone(),
        segments,
        source,
        journal,
        handle,
        client_state,
        perf_logger,
        replay_logger,
        telemetry.clone(),
        telemetry_session_id.clone(),
        None,
    );

    let replayed = fs.replay_journal()?;
    if replayed > 0 {
        info!("Replayed {replayed} journaled entries before mount");
    }

    let mut options = vec![
        MountOption::FSName(config.fuse_fsname.clone()),
        MountOption::DefaultPermissions,
    ];
    let allow_other = if config.allow_other {
        true
    } else {
        env::var("CLAWFS_ALLOW_OTHER")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false)
    };
    if allow_other {
        options.push(MountOption::AllowOther);
    } else {
        options.push(MountOption::AllowRoot);
    }
    log_boot_config(&config, allow_other);
    if !config.foreground {
        options.push(MountOption::AutoUnmount);
    }
    let fuse_threads = config.fuse_threads;
    info!(
        "Mounting ClawFS at {} (fuse_threads={}, writeback_cache={})",
        config.mount_path.display(),
        fuse_threads,
        config.writeback_cache,
    );
    let mount_result = if fuse_threads > 0 {
        let mut session = fuser::Session::new(fs, &config.mount_path, &options)?;
        session.run_multithreaded(fuse_threads)
    } else {
        fuser::mount2(fs, &config.mount_path, &options)
    };

    if let Err(err) = mount_result {
        if let Some(client) = &telemetry {
            client.destination().emit_blocking(
                "command.mount_failure",
                telemetry_session_id.as_deref(),
                json!({ "error": sanitize_error(err.to_string()) }),
            );
        }
        return Err(err.into());
    }

    runtime.block_on(async {
        superblock.mark_clean().await.ok();
        metadata.shutdown().await.ok();
    });
    if let Some(client) = &telemetry {
        client.destination().emit_blocking(
            "runtime.session_stop_clean",
            telemetry_session_id.as_deref(),
            json!({ "mode": "fuse" }),
        );
    }
    set_panic_context(None, "mount_runtime", None);
    Ok(())
}

pub fn log_boot_config(config: &Config, allow_other: bool) {
    info!(
        target: "startup",
        "fs_boot_config {}",
        json!({
            "mode": "fuse",
            "mount_path": config.mount_path.display().to_string(),
            "store_path": config.store_path.display().to_string(),
            "local_cache_path": config.local_cache_path.display().to_string(),
            "state_path": config.state_path.display().to_string(),
            "object_provider": format!("{:?}", config.object_provider).to_lowercase(),
            "bucket": config.bucket.as_deref(),
            "region": config.region.as_deref(),
            "endpoint": config.endpoint.as_deref(),
            "object_prefix": &config.object_prefix,
            "source_enabled": config.source.is_some(),
            "source_provider": config
                .source
                .as_ref()
                .map(|source| format!("{:?}", source.object_provider).to_lowercase()),
            "source_bucket": config.source.as_ref().and_then(|source| source.bucket.as_deref()),
            "source_prefix": config.source.as_ref().map(|source| source.prefix.as_str()),
            "gcs_service_account": config
                .gcs_service_account
                .as_ref()
                .map(|path| path.display().to_string()),
            "home_prefix": &config.home_prefix,
            "inline_threshold": config.inline_threshold,
            "inline_compression": config.inline_compression,
            "inline_encryption_enabled": config.inline_encryption_key.is_some(),
            "segment_compression": config.segment_compression,
            "segment_encryption_enabled": config.segment_encryption_key.is_some(),
            "pending_bytes": config.pending_bytes,
            "fsync_on_close": config.fsync_on_close,
            "flush_interval_ms": config.flush_interval_ms,
            "disable_journal": config.disable_journal,
            "disable_cleanup": config.disable_cleanup,
            "lookup_cache_ttl_ms": config.lookup_cache_ttl_ms,
            "dir_cache_ttl_ms": config.dir_cache_ttl_ms,
            "metadata_poll_interval_ms": config.metadata_poll_interval_ms,
            "segment_cache_bytes": config.segment_cache_bytes,
            "imap_delta_batch": config.imap_delta_batch,
            "fuse_fsname": &config.fuse_fsname,
            "allow_other_effective": allow_other,
            "foreground": config.foreground,
            "perf_log": config.perf_log.as_ref().map(|path| path.display().to_string()),
            "replay_log": config.replay_log.as_ref().map(|path| path.display().to_string()),
            "log_file": config.log_file.as_ref().map(|path| path.display().to_string()),
            "debug_log": config.debug_log,
        })
    );
}

pub fn ensure_root(
    runtime: &tokio::runtime::Runtime,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    config: &Config,
) -> Result<()> {
    let uid = crate::compat::current_uid();
    let gid = crate::compat::current_gid();
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
        String::new(),
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
    ensure_welcome_file(
        runtime,
        metadata.clone(),
        superblock.clone(),
        config,
        uid,
        gid,
    )?;
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

pub fn ensure_welcome_file(
    runtime: &tokio::runtime::Runtime,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    config: &Config,
    uid: u32,
    gid: u32,
) -> Result<()> {
    let mut root = runtime
        .block_on(metadata.get_inode(ROOT_INODE))?
        .ok_or_else(|| anyhow::anyhow!("missing root inode {}", ROOT_INODE))?;
    if root
        .children()
        .map(|children| children.contains_key(WELCOME_FILENAME))
        .unwrap_or(false)
    {
        return Ok(());
    }

    let new_inode = runtime.block_on(superblock.reserve_inodes(1))?;
    let snapshot = superblock.prepare_dirty_generation()?;
    let generation = snapshot.generation;

    let mut welcome = InodeRecord::new_file(
        new_inode,
        ROOT_INODE,
        WELCOME_FILENAME.to_string(),
        format!("/{}", WELCOME_FILENAME),
        uid,
        gid,
    );
    let bytes = WELCOME_CONTENT.as_bytes().to_vec();
    welcome.size = bytes.len() as u64;
    welcome.storage = FileStorage::Inline(bytes);
    welcome.mode = 0o100644;

    if let Err(err) =
        runtime.block_on(metadata.persist_inode(&welcome, generation, config.shard_size))
    {
        superblock.abort_generation(generation);
        return Err(err);
    }
    if let Some(children) = root.children_mut() {
        children.insert(WELCOME_FILENAME.to_string(), new_inode);
    }
    if let Err(err) = runtime.block_on(metadata.persist_inode(&root, generation, config.shard_size))
    {
        superblock.abort_generation(generation);
        return Err(err);
    }
    runtime.block_on(superblock.commit_generation(generation))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn ensure_directory_path(
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
        let mut dir = InodeRecord::new_directory(
            new_inode,
            current,
            component.to_string(),
            if parent.path == "/" {
                format!("/{}", component)
            } else {
                format!("{}/{}", parent.path.trim_end_matches('/'), component)
            },
            uid,
            gid,
        );
        dir.mode = (dir.mode & !0o777) | (mode & 0o777);

        if let Err(err) =
            runtime.block_on(metadata.persist_inode(&dir, generation, config.shard_size))
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

struct TracingWriter {
    file: Option<Arc<Mutex<BufWriter<std::fs::File>>>>,
}

impl Write for TracingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut stderr = std::io::stderr().lock();
        ignore_broken_pipe(stderr.write_all(buf))?;
        if let Some(file) = &self.file {
            let mut guard = file
                .lock()
                .map_err(|_| std::io::Error::other("log file lock poisoned"))?;
            guard.write_all(buf)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut stderr = std::io::stderr().lock();
        ignore_broken_pipe(stderr.flush())?;
        if let Some(file) = &self.file {
            let mut guard = file
                .lock()
                .map_err(|_| std::io::Error::other("log file lock poisoned"))?;
            guard.flush()?;
        }
        Ok(())
    }
}

pub fn ignore_broken_pipe(result: std::io::Result<()>) -> std::io::Result<()> {
    match result {
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        other => other,
    }
}

pub(crate) fn init_logging(log_path: Option<&Path>, force_debug: bool) -> Result<()> {
    let file = if let Some(path) = log_path {
        Some(Arc::new(Mutex::new(BufWriter::new(
            OpenOptions::new().create(true).append(true).open(path)?,
        ))))
    } else {
        None
    };
    let filter = if force_debug {
        EnvFilter::new("debug")
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };
    if let Err(err) = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(true)
        .with_level(true)
        .without_time()
        .with_writer(move || TracingWriter { file: file.clone() })
        .try_init()
    {
        let msg = err.to_string();
        if !msg.contains("already") {
            anyhow::bail!(msg);
        }
    }
    Ok(())
}

pub fn spawn_metadata_poller(
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
                        match record.storage {
                            FileStorage::LegacySegment(ptr) => {
                                if let Err(err) = segs.prefetch_segment(&ptr) {
                                    warn!("segment prefetch failed: {err:?}");
                                }
                            }
                            FileStorage::Segments(extents) => {
                                for extent in extents {
                                    if let Err(err) = segs.prefetch_segment(&extent.pointer) {
                                        warn!("segment prefetch failed: {err:?}");
                                    }
                                }
                            }
                            _ => {}
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
    use crate::maintenance::{self, CompactionConfig};
    let config = CompactionConfig::default();
    handle.spawn(async move {
        loop {
            let mut did_work = false;
            let delta_count = tokio::task::spawn_blocking({
                let md = metadata.clone();
                move || md.delta_file_count()
            })
            .await
            .unwrap_or(Ok(0))
            .unwrap_or(0);
            if delta_count > config.delta_compact_threshold
                && maintenance::acquire_cleanup_lease(
                    &superblock,
                    CleanupTaskKind::DeltaCompaction,
                    &client_id,
                    &config,
                )
                .await
                .unwrap_or(false)
            {
                if let Err(err) = maintenance::run_delta_compaction(&metadata, &config).await {
                    warn!("delta compaction failed: {err:?}");
                }
                if let Err(err) = maintenance::release_cleanup_lease(
                    &superblock,
                    CleanupTaskKind::DeltaCompaction,
                    &client_id,
                )
                .await
                {
                    warn!("cleanup lease release failed: {err:?}");
                }
                did_work = true;
            }
            if !did_work
                && maintenance::has_pending_segment_compaction_work(&metadata, &superblock, &config)
                    .await
                    .unwrap_or(false)
                && maintenance::acquire_cleanup_lease(
                    &superblock,
                    CleanupTaskKind::SegmentCompaction,
                    &client_id,
                    &config,
                )
                .await
                .unwrap_or(false)
            {
                if let Err(err) =
                    maintenance::run_segment_compaction(&metadata, &segments, &config).await
                {
                    warn!("segment compaction failed: {err:?}");
                }
                if let Err(err) = maintenance::release_cleanup_lease(
                    &superblock,
                    CleanupTaskKind::SegmentCompaction,
                    &client_id,
                )
                .await
                {
                    warn!("cleanup lease release failed: {err:?}");
                }
            }
            sleep(Duration::from_secs(30)).await;
        }
    });
}

pub fn sanitize_error(error: String) -> String {
    let first_line = error.lines().next().unwrap_or("unknown error").trim();
    first_line.chars().take(160).collect()
}
