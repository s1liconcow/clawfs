use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use env_logger::Env;
use fuser::MountOption;
use log::{LevelFilter, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use tokio::runtime::Handle;
use tokio::task;
use tokio::time::sleep;
use uuid::Uuid;

use crate::clawfs as clawfs_runtime;
use crate::config::Config;
use crate::fs::OsageFs;
use crate::inode::{FileStorage, InodeRecord, ROOT_INODE, SegmentExtent};
use crate::journal::JournalManager;
use crate::metadata::MetadataStore;
use crate::perf::PerfLogger;
use crate::replay::ReplayLogger;
use crate::segment::{SegmentEntry, SegmentManager, SegmentPayload, SegmentPointer};
use crate::source::SourceObjectStore;
use crate::state::ClientStateManager;
use crate::superblock::{CleanupTaskKind, SuperblockManager};
use crate::telemetry::{TelemetryClient, set_panic_context};

const DELTA_COMPACT_THRESHOLD: usize = 128;
const DELTA_COMPACT_KEEP: usize = 32;
const SEGMENT_COMPACT_BATCH: usize = 8;
const SEGMENT_COMPACT_LAG: u64 = 3;
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

type SegmentCompactionEntry = (InodeRecord, Vec<SegmentPointer>, Vec<u8>);

#[derive(Debug, Clone)]
pub struct HostedControlPlane {
    pub api_url: String,
    pub api_token: String,
    pub volume_slug: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub storage_mode: Option<String>,
}

pub fn run_mount_entry(
    config: Config,
    args: &[OsString],
    hosted: Option<&HostedControlPlane>,
) -> Result<()> {
    if config.foreground {
        return run_mount(config, hosted.cloned());
    }

    spawn_background_mount(args, &config.mount_path)?;
    Ok(())
}

fn spawn_background_mount(args: &[OsString], mount_path: &Path) -> Result<()> {
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
    let mount_str = mount_path.to_string_lossy().to_string();

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
        if let Ok(mounts) = std::fs::read_to_string("/proc/mounts")
            && mounts.lines().any(|line| line.contains(&mount_str))
        {
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

fn run_mount(mut config: Config, hosted: Option<HostedControlPlane>) -> Result<()> {
    if let Some(storage_mode) = hosted
        .as_ref()
        .and_then(|hosted| hosted.storage_mode.as_deref())
    {
        unsafe {
            env::set_var(crate::clawfs::STORAGE_MODE_ENV, storage_mode);
        }
    }
    clawfs_runtime::apply_env_runtime_spec(&mut config)?;
    init_logging(config.log_file.as_deref(), config.debug_log)?;
    std::fs::create_dir_all(&config.mount_path)?;

    let has_control_plane_creds = if let Some(hosted) = hosted.as_ref() {
        apply_hosted_credentials(&mut config, hosted)?
    } else {
        false
    };
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
                "volume": hosted.as_ref().map(|value| value.volume_slug.clone()),
                "mode": "fuse",
            }),
        );
    }

    let runtime = tokio::runtime::Runtime::new()?;
    let handle = runtime.handle().clone();
    let metadata = Arc::new(runtime.block_on(MetadataStore::new(&config, handle.clone()))?);
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
    if has_control_plane_creds && let Some(hosted) = hosted.clone() {
        spawn_credential_refresh_loop(
            &handle,
            config.state_path.clone(),
            hosted,
            telemetry.clone(),
            telemetry_session_id.clone(),
        );
    }

    let client_state = Arc::new(ClientStateManager::load(&config.state_path)?);
    let superblock_snapshot = superblock.snapshot();
    client_state.reconcile_with_minimums(
        superblock_snapshot.next_inode,
        superblock_snapshot.next_segment,
    )?;
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
        perf_logger.clone(),
        replay_logger.clone(),
        telemetry.clone(),
        telemetry_session_id.clone(),
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

fn apply_hosted_credentials(config: &mut Config, hosted: &HostedControlPlane) -> Result<bool> {
    let mut has_control_plane_creds = false;

    if let (Some(access_key_id), Some(secret_access_key)) = (
        hosted.access_key_id.as_deref(),
        hosted.secret_access_key.as_deref(),
    ) {
        unsafe {
            env::set_var("AWS_ACCESS_KEY_ID", access_key_id);
            env::set_var("AWS_SECRET_ACCESS_KEY", secret_access_key);
        }
        has_control_plane_creds = true;
    } else if let Some(creds) =
        provision_credentials_from_control_plane(hosted, &config.state_path)?
    {
        apply_provisioned_credentials(config, &creds);
        has_control_plane_creds = true;
    }

    Ok(has_control_plane_creds)
}

fn apply_provisioned_credentials(config: &mut Config, creds: &ControlPlaneCredentials) {
    unsafe {
        env::set_var("AWS_ACCESS_KEY_ID", &creds.access_key_id);
        env::set_var("AWS_SECRET_ACCESS_KEY", &creds.secret_access_key);
    }
    if config.bucket.is_none() {
        config.bucket = Some(creds.bucket.clone());
    }
    if config.endpoint.is_none() {
        config.endpoint = Some(creds.endpoint.clone());
    }
    if config.region.is_none() {
        config.region = Some(creds.region.clone());
    }
    if config.object_prefix.is_empty() {
        config.object_prefix = creds.prefix.clone();
    }
    info!(
        "Provisioned storage credentials from control plane (key={}...)",
        &creds.access_key_id[..creds.access_key_id.len().min(12)]
    );
}

fn log_boot_config(config: &Config, allow_other: bool) {
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

fn ensure_welcome_file(
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
            if delta_count > DELTA_COMPACT_THRESHOLD
                && superblock
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
                    .filter(|record| {
                        record
                            .segment_pointer()
                            .map(|ptr| ptr.generation < cutoff_generation)
                            .unwrap_or(false)
                    })
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
    candidates: Vec<InodeRecord>,
) -> Result<()> {
    if candidates.len() < 2 {
        return Ok(());
    }
    let dataset = task::spawn_blocking({
        let segs = segments.clone();
        move || -> Result<Vec<SegmentCompactionEntry>> {
            let mut out = Vec::new();
            for record in candidates {
                match record.storage.clone() {
                    FileStorage::LegacySegment(ptr) => {
                        let data = segs.read_pointer(&ptr)?;
                        out.push((record, vec![ptr], data));
                    }
                    FileStorage::Segments(extents) => {
                        let mut buffer = vec![0u8; record.size as usize];
                        let mut pointers = Vec::new();
                        for extent in extents {
                            let chunk = segs.read_pointer(&extent.pointer)?;
                            let start = extent.logical_offset as usize;
                            let end = start + chunk.len();
                            if end > buffer.len() {
                                buffer.resize(end, 0);
                            }
                            buffer[start..end].copy_from_slice(&chunk);
                            pointers.push(extent.pointer);
                        }
                        out.push((record, pointers, buffer));
                    }
                    FileStorage::Inline(_)
                    | FileStorage::InlineEncoded(_)
                    | FileStorage::ExternalObject(_) => {}
                }
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
            logical_offset: 0,
            payload: SegmentPayload::Bytes(data.clone()),
        });
    }
    let segments_clone = segments.clone();
    let pointer_map: HashMap<u64, Vec<SegmentExtent>> = tokio::task::spawn_blocking(move || {
        segments_clone
            .write_batch(generation, segment_id, entries)
            .map(|res| {
                let mut map: HashMap<u64, Vec<SegmentExtent>> = HashMap::new();
                for (inode, extent) in res {
                    map.entry(inode).or_default().push(extent);
                }
                map
            })
    })
    .await??;
    let mut segments_to_delete = HashSet::new();
    let result: Result<()> = async {
        for (mut record, old_ptrs, _) in dataset {
            if let Some(new_extents) = pointer_map.get(&record.inode) {
                record.storage = FileStorage::Segments(new_extents.clone());
                metadata
                    .persist_inode(&record, generation, snapshot.shard_size)
                    .await?;
                for ptr in old_ptrs {
                    segments_to_delete.insert((ptr.generation, ptr.segment_id));
                }
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
    for (generation, seg_id) in segments_to_delete {
        let segs = segments.clone();
        task::spawn_blocking(move || segs.delete_segment(generation, seg_id)).await??;
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlPlaneCredentials {
    access_key_id: String,
    secret_access_key: String,
    bucket: String,
    endpoint: String,
    region: String,
    prefix: String,
    #[serde(default)]
    expires_at: Option<String>,
}

const CREDENTIAL_REFRESH_BUFFER_SECS: i64 = 300;
const CREDENTIAL_REFRESH_POLL_SECS: u64 = 60;

fn credential_cache_path(state_path: &Path) -> PathBuf {
    state_path
        .parent()
        .unwrap_or(state_path)
        .join("credentials.json")
}

fn load_cached_credentials(state_path: &Path) -> Option<ControlPlaneCredentials> {
    let path = credential_cache_path(state_path);
    let data = std::fs::read_to_string(&path).ok()?;
    let creds: ControlPlaneCredentials = serde_json::from_str(&data).ok()?;
    if credentials_still_valid(&creds) {
        Some(creds)
    } else {
        info!("Cached credentials expired or expiring soon, will refresh");
        None
    }
}

fn save_cached_credentials(state_path: &Path, creds: &ControlPlaneCredentials) {
    let path = credential_cache_path(state_path);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(data) = serde_json::to_string_pretty(creds)
        && let Err(err) = std::fs::write(&path, &data)
    {
        warn!("Failed to cache credentials to {}: {err}", path.display());
    }
}

fn credentials_still_valid(creds: &ControlPlaneCredentials) -> bool {
    let Some(ref expires_str) = creds.expires_at else {
        return true;
    };
    let Ok(expires) =
        time::OffsetDateTime::parse(expires_str, &time::format_description::well_known::Rfc3339)
    else {
        warn!(
            "Could not parse expires_at '{}', treating as expired",
            expires_str
        );
        return false;
    };
    let now = time::OffsetDateTime::now_utc();
    let remaining = expires - now;
    remaining.whole_seconds() > CREDENTIAL_REFRESH_BUFFER_SECS
}

fn fetch_credentials_from_api(hosted: &HostedControlPlane) -> Result<ControlPlaneCredentials> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let resp = client
        .post(format!(
            "{}/api/volumes/by-slug/{}/credentials",
            hosted.api_url, hosted.volume_slug
        ))
        .header("Authorization", format!("Bearer {}", hosted.api_token))
        .send()?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        anyhow::bail!(
            "control plane credential request failed ({}): {}",
            status,
            body
        );
    }

    let body: serde_json::Value = resp.json()?;
    Ok(ControlPlaneCredentials {
        access_key_id: body["access_key_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing access_key_id in response"))?
            .to_string(),
        secret_access_key: body["secret_access_key"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing secret_access_key in response"))?
            .to_string(),
        bucket: body["bucket"].as_str().unwrap_or_default().to_string(),
        endpoint: body["endpoint"].as_str().unwrap_or_default().to_string(),
        region: body["region"].as_str().unwrap_or("auto").to_string(),
        prefix: body["prefix"].as_str().unwrap_or_default().to_string(),
        expires_at: body["expires_at"].as_str().map(|value| value.to_string()),
    })
}

fn provision_credentials_from_control_plane(
    hosted: &HostedControlPlane,
    state_path: &Path,
) -> Result<Option<ControlPlaneCredentials>> {
    if let Some(cached) = load_cached_credentials(state_path) {
        info!(
            "Using cached credentials (key={}..., expires={:?})",
            &cached.access_key_id[..cached.access_key_id.len().min(12)],
            cached.expires_at,
        );
        return Ok(Some(cached));
    }

    info!(
        "Requesting credentials from control plane for volume '{}'",
        hosted.volume_slug
    );
    let creds = fetch_credentials_from_api(hosted)?;
    save_cached_credentials(state_path, &creds);
    Ok(Some(creds))
}

fn spawn_credential_refresh_loop(
    handle: &Handle,
    state_path: PathBuf,
    hosted: HostedControlPlane,
    telemetry: Option<Arc<TelemetryClient>>,
    session_id: Option<String>,
) {
    handle.spawn(async move {
        loop {
            sleep(Duration::from_secs(CREDENTIAL_REFRESH_POLL_SECS)).await;

            if load_cached_credentials(&state_path)
                .is_some_and(|cached| credentials_still_valid(&cached))
            {
                continue;
            }

            info!("Credential refresh: fetching new credentials from control plane");
            let state_path = state_path.clone();
            let hosted = hosted.clone();
            let refresh_volume_slug = hosted.volume_slug.clone();
            let refresh_hosted = hosted.clone();
            let result = tokio::task::spawn_blocking(move || {
                fetch_credentials_from_api(&refresh_hosted).inspect(|creds| {
                    save_cached_credentials(&state_path, creds);
                })
            })
            .await;

            match result {
                Ok(Ok(creds)) => {
                    unsafe {
                        env::set_var("AWS_ACCESS_KEY_ID", &creds.access_key_id);
                        env::set_var("AWS_SECRET_ACCESS_KEY", &creds.secret_access_key);
                    }
                    if let Some(client) = &telemetry {
                        client.emit(
                            "control_plane.credential_refresh_success",
                            session_id.as_deref(),
                            json!({ "volume": refresh_volume_slug.clone() }),
                        );
                    }
                    info!(
                        "Credential refresh: updated (key={}..., expires={:?})",
                        &creds.access_key_id[..creds.access_key_id.len().min(12)],
                        creds.expires_at,
                    );
                }
                Ok(Err(err)) => {
                    if let Some(client) = &telemetry {
                        client.emit(
                            "control_plane.credential_refresh_failure",
                            session_id.as_deref(),
                            json!({
                                "volume": refresh_volume_slug.clone(),
                                "error": sanitize_error(err.to_string()),
                            }),
                        );
                    }
                    warn!("Credential refresh failed: {err}");
                }
                Err(err) => {
                    if let Some(client) = &telemetry {
                        client.emit(
                            "control_plane.credential_refresh_failure",
                            session_id.as_deref(),
                            json!({
                                "volume": refresh_volume_slug.clone(),
                                "error": sanitize_error(err.to_string()),
                            }),
                        );
                    }
                    warn!("Credential refresh task panicked: {err}");
                }
            }
        }
    });
}

fn sanitize_error(error: String) -> String {
    let first_line = error.lines().next().unwrap_or("unknown error").trim();
    first_line.chars().take(160).collect()
}
