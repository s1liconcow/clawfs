use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};

use clawfs::config::{Config, ObjectStoreProvider};
use clawfs::fs::OsageFs;
use clawfs::inode::{InodeRecord, ROOT_INODE};
use clawfs::journal::JournalManager;
use clawfs::metadata::MetadataStore;
use clawfs::replay::ReplayLogger;
use clawfs::segment::SegmentManager;
use clawfs::state::ClientStateManager;
use clawfs::superblock::SuperblockManager;

use crate::cwd::CwdTracker;
use crate::fd_table::FdTable;
use crate::prefix::PrefixRouter;

/// Global singleton holding the bootstrapped ClawFS runtime.
static CLAWFS_RUNTIME: OnceLock<ClawfsRuntime> = OnceLock::new();

/// Flag set after fork() — all hooks fall through once poisoned.
static FORK_POISONED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub struct ClawfsRuntime {
    pub fs: Arc<OsageFs>,
    pub fd_table: FdTable,
    pub prefix_router: PrefixRouter,
    pub cwd: CwdTracker,
    /// Kept alive to sustain async workers; not directly accessed.
    _tokio_rt: tokio::runtime::Runtime,
}

impl ClawfsRuntime {
    /// Get the global runtime, if initialized.
    pub fn get() -> Option<&'static ClawfsRuntime> {
        if FORK_POISONED.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }
        CLAWFS_RUNTIME.get()
    }

    /// Initialize the global runtime. Called once from `#[ctor]`.
    /// Returns `Ok(true)` if initialized, `Ok(false)` if CLAWFS_PREFIXES is not set (no-op mode).
    pub fn init() -> Result<bool> {
        let prefixes = match std::env::var("CLAWFS_PREFIXES") {
            Ok(val) if !val.trim().is_empty() => val,
            _ => return Ok(false),
        };

        let router = PrefixRouter::new(&prefixes);
        if router.is_empty() {
            return Ok(false);
        }

        let store_path = std::env::var("CLAWFS_STORE_PATH").unwrap_or_else(|_| {
            let cache = default_cache_root();
            cache.join("store").to_string_lossy().to_string()
        });

        let config_root = default_config_root();
        let cache_root = default_cache_root();

        let state_path = std::env::var("CLAWFS_STATE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| config_root.join("state").join("preload_state.bin"));

        let local_cache_path = std::env::var("CLAWFS_LOCAL_CACHE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| cache_root.join("cache"));

        let mut config = Config {
            mount_path: PathBuf::from("/clawfs-preload"),
            store_path: PathBuf::from(&store_path),
            local_cache_path: local_cache_path.clone(),
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
            object_prefix: String::new(),
            gcs_service_account: None,
            aws_allow_http: false,
            aws_force_path_style: false,
            source: None,
            state_path: state_path.clone(),
            perf_log: None,
            replay_log: None,
            disable_journal: false,
            fsync_on_close: false,
            flush_interval_ms: 5000,
            disable_cleanup: true,
            lookup_cache_ttl_ms: 1000,
            dir_cache_ttl_ms: 1000,
            metadata_poll_interval_ms: 2000,
            segment_cache_bytes: 512 * 1024 * 1024,
            foreground: true,
            allow_other: false,
            log_file: None,
            debug_log: false,
            imap_delta_batch: 512,
            writeback_cache: false,
            fuse_threads: 0,
            entry_ttl_secs: 5,
            fuse_fsname: "clawfs".to_string(),
        };

        // Apply env-driven runtime spec (storage mode, provider overrides, etc.)
        clawfs::clawfs::apply_env_runtime_spec(&mut config).context("applying env runtime spec")?;

        // Ensure directories exist.
        if matches!(config.object_provider, ObjectStoreProvider::Local) {
            std::fs::create_dir_all(&config.store_path).context("creating store_path")?;
        }
        std::fs::create_dir_all(&config.local_cache_path).context("creating local_cache_path")?;
        if let Some(parent) = config.state_path.parent() {
            std::fs::create_dir_all(parent).context("creating state_path parent")?;
        }

        let worker_threads: usize = std::env::var("CLAWFS_TOKIO_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2);

        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .thread_name("clawfs-preload")
            .build()
            .context("building tokio runtime")?;

        let handle = tokio_rt.handle().clone();

        let fs = tokio_rt.block_on(async {
            let metadata = Arc::new(
                MetadataStore::new(&config, handle.clone())
                    .await
                    .context("MetadataStore::new")?,
            );
            let superblock = Arc::new(
                SuperblockManager::load_or_init(metadata.clone(), config.shard_size)
                    .await
                    .context("SuperblockManager::load_or_init")?,
            );

            ensure_root(metadata.clone(), superblock.clone(), &config)
                .await
                .context("ensure_root")?;

            let segments = Arc::new(
                SegmentManager::new(&config, handle.clone()).context("SegmentManager::new")?,
            );
            let client_state = Arc::new(
                ClientStateManager::load(&config.state_path).context("ClientStateManager::load")?,
            );
            let journal = if config.disable_journal {
                None
            } else {
                Some(Arc::new(
                    JournalManager::new(&config.local_cache_path).context("JournalManager::new")?,
                ))
            };

            let replay_logger = config
                .replay_log
                .as_ref()
                .map(ReplayLogger::new)
                .transpose()
                .context("ReplayLogger::new")?
                .map(Arc::new);

            let fs = Arc::new(OsageFs::new(
                config,
                metadata,
                superblock,
                segments,
                None,
                journal,
                handle,
                client_state,
                None,
                replay_logger,
            ));

            // Replay pending journal entries.
            let fs_clone = fs.clone();
            let replayed = tokio::task::spawn_blocking(move || fs_clone.replay_journal())
                .await
                .context("replay task join")?
                .context("replay_journal")?;
            if replayed > 0 {
                log::info!("clawfs-preload: replayed {replayed} journal entries");
            }

            Ok::<_, anyhow::Error>(fs)
        })?;

        let runtime = ClawfsRuntime {
            fs,
            fd_table: FdTable::new(),
            prefix_router: router,
            cwd: CwdTracker::new(),
            _tokio_rt: tokio_rt,
        };

        let _ = CLAWFS_RUNTIME.set(runtime);

        // Register fork poison handler.
        unsafe {
            libc::pthread_atfork(None, None, Some(post_fork_child));
        }

        log::info!("clawfs-preload: initialized with prefixes={prefixes}");
        Ok(true)
    }
}

/// Post-fork child handler: poison all hooks so they fall through to real libc.
extern "C" fn post_fork_child() {
    FORK_POISONED.store(true, std::sync::atomic::Ordering::Relaxed);
}

fn default_config_root() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".clawfs")
}

fn default_cache_root() -> PathBuf {
    default_config_root()
}

/// Ensure the root inode exists, creating it if needed.
async fn ensure_root(
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    config: &Config,
) -> Result<()> {
    let uid = unsafe { libc::geteuid() as u32 };
    let gid = unsafe { libc::getegid() as u32 };
    let desired_mode = 0o40777;

    if let Some(mut existing) = metadata.get_inode(ROOT_INODE).await? {
        if existing.uid != uid || existing.gid != gid || existing.mode != desired_mode {
            existing.uid = uid;
            existing.gid = gid;
            existing.mode = desired_mode;
            let snapshot = superblock.prepare_dirty_generation()?;
            let generation = snapshot.generation;
            if let Err(err) = metadata
                .persist_inode(&existing, generation, config.shard_size)
                .await
            {
                superblock.abort_generation(generation);
                return Err(err);
            }
            superblock.commit_generation(generation).await?;
        }
        return Ok(());
    }

    // Create root inode from scratch.
    let snapshot = superblock.prepare_dirty_generation()?;
    let generation = snapshot.generation;
    let mut root = InodeRecord::new_directory(
        ROOT_INODE,
        ROOT_INODE,
        String::new(),
        "/".to_string(),
        uid,
        gid,
    );
    root.mode = desired_mode;
    if let Err(err) = metadata
        .persist_inode(&root, generation, config.shard_size)
        .await
    {
        superblock.abort_generation(generation);
        return Err(err);
    }
    superblock.commit_generation(generation).await?;
    Ok(())
}
