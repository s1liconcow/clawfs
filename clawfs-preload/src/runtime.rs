use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};

use clawfs::config::{Config, ObjectStoreProvider};
use clawfs::fs::OsageFs;
use clawfs::inode::{FileStorage, InodeRecord, ROOT_INODE};
use clawfs::journal::JournalManager;
use clawfs::metadata::MetadataStore;
use clawfs::replay::ReplayLogger;
use clawfs::segment::SegmentManager;
use clawfs::state::ClientStateManager;
use clawfs::superblock::SuperblockManager;

use crate::cwd::CwdTracker;
use crate::fd_table::FdTable;
use crate::prefix::PrefixRouter;

/// Lightweight config parsed at ctor time — no I/O beyond local dirs.
pub struct LazyRuntimeConfig {
    pub config: Config,
    pub prefix_router: PrefixRouter,
    pub cwd: CwdTracker,
}

/// Global lightweight config — set once in `#[ctor]`, never does network I/O.
static CLAWFS_CONFIG: OnceLock<LazyRuntimeConfig> = OnceLock::new();

/// Global singleton holding the full bootstrapped ClawFS runtime.
/// Initialized lazily on first access to a ClawFS path.
static CLAWFS_RUNTIME: OnceLock<Option<ClawfsRuntime>> = OnceLock::new();

/// Flag set after fork() — all hooks fall through once poisoned.
static FORK_POISONED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub struct ClawfsRuntime {
    pub fs: Arc<OsageFs>,
    pub fd_table: FdTable,
    /// Kept alive to sustain async workers; not directly accessed.
    _tokio_rt: tokio::runtime::Runtime,
}

fn is_fork_poisoned() -> bool {
    FORK_POISONED.load(std::sync::atomic::Ordering::Relaxed)
}

impl ClawfsRuntime {
    /// Get the global runtime if it has already been fully initialized.
    /// Returns `None` if fork-poisoned or if runtime hasn't been initialized yet.
    pub fn get() -> Option<&'static ClawfsRuntime> {
        if is_fork_poisoned() {
            return None;
        }
        CLAWFS_RUNTIME.get().and_then(|opt| opt.as_ref())
    }

    /// Get the lightweight config (prefix router + cwd tracker) without
    /// triggering full runtime initialization. Fast, no I/O.
    pub fn config() -> Option<&'static LazyRuntimeConfig> {
        if is_fork_poisoned() {
            return None;
        }
        CLAWFS_CONFIG.get()
    }

    /// Trigger full runtime initialization if not already done.
    /// Called when we know a ClawFS path is being accessed.
    pub fn ensure_runtime() -> Option<&'static ClawfsRuntime> {
        if is_fork_poisoned() {
            return None;
        }
        let cfg = CLAWFS_CONFIG.get()?;
        CLAWFS_RUNTIME
            .get_or_init(|| match do_full_init(&cfg.config) {
                Ok(rt) => {
                    crate::inotify::spawn_poller();

                    // Restore virtual CWD from parent process.
                    if let Ok(val) = std::env::var("CLAWFS_VIRTUAL_CWD") {
                        if let Some((full, inner)) = val.split_once('\n') {
                            let _ = crate::dispatch::dispatch_chdir_lazy(
                                &rt,
                                &cfg.cwd,
                                &cfg.prefix_router,
                                full,
                                inner,
                            );
                            log::trace!(
                                "restored virtual CWD from env: full={full:?} inner={inner:?}"
                            );
                        }
                    }

                    log::info!("clawfs-preload: full runtime initialized (lazy)");
                    Some(rt)
                }
                Err(e) => {
                    eprintln!("clawfs-preload: lazy initialization failed: {e:#}");
                    None
                }
            })
            .as_ref()
    }

    /// Initialize the lightweight config. Called once from `#[ctor]`.
    /// Returns `Ok(true)` if config was set, `Ok(false)` if CLAWFS_PREFIXES is not set.
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
            disable_journal: true,
            disable_cleanup: true,
            lookup_cache_ttl_ms: 1000,
            dir_cache_ttl_ms: 1000,
            foreground: true,
            fuse_threads: 0,
            accelerator_mode: None,
            accelerator_endpoint: None,
            accelerator_fallback_policy: None,
            ..Config::with_paths(
                PathBuf::from("/clawfs-preload"),
                PathBuf::from(&store_path),
                local_cache_path.clone(),
                state_path.clone(),
            )
        };

        clawfs::clawfs::apply_env_runtime_spec(&mut config).context("applying env runtime spec")?;

        // Ensure local directories exist (fast, local-only I/O).
        if matches!(config.object_provider, ObjectStoreProvider::Local) {
            std::fs::create_dir_all(&config.store_path).context("creating store_path")?;
        }
        std::fs::create_dir_all(&config.local_cache_path).context("creating local_cache_path")?;
        if let Some(parent) = config.state_path.parent() {
            std::fs::create_dir_all(parent).context("creating state_path parent")?;
        }

        let cwd = CwdTracker::new();

        // Pre-populate virtual CWD from parent process so that try_classify()
        // can resolve relative paths (e.g. "." after `cd clawfs`) before the
        // full runtime is lazily initialized.  The inode is set to 0 here and
        // will be corrected during ensure_runtime() → dispatch_chdir_lazy().
        if let Ok(val) = std::env::var("CLAWFS_VIRTUAL_CWD") {
            if let Some((full, inner)) = val.split_once('\n') {
                cwd.set_clawfs(full.to_string(), inner.to_string(), 0);
                log::trace!("pre-populated virtual CWD from env: full={full:?} inner={inner:?}");
            }
        }

        let lazy_config = LazyRuntimeConfig {
            config,
            prefix_router: router,
            cwd,
        };

        let _ = CLAWFS_CONFIG.set(lazy_config);

        // Register fork poison handler early — must happen before any tokio runtime.
        unsafe {
            libc::pthread_atfork(None, None, Some(post_fork_child));
        }

        log::info!("clawfs-preload: config loaded, prefixes={prefixes} (runtime deferred)");
        Ok(true)
    }
}

/// Perform the expensive full initialization: tokio runtime, metadata store,
/// superblock, segments, journal replay. Called lazily on first ClawFS path access.
fn do_full_init(config: &Config) -> Result<ClawfsRuntime> {
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

    // Create a single shared object store for both metadata and segments.
    let (shared_store, meta_prefix) = clawfs::metadata::create_object_store(config)?;
    let seg_prefix = clawfs::segment::segment_prefix(&config.object_prefix);

    let fs = tokio_rt.block_on(async {
        let metadata = Arc::new(
            MetadataStore::new_with_store(
                shared_store.clone(),
                meta_prefix,
                config,
                handle.clone(),
            )
            .await
            .context("MetadataStore::new_with_store")?,
        );
        let superblock = Arc::new(
            SuperblockManager::load_or_init(metadata.clone(), config.shard_size)
                .await
                .context("SuperblockManager::load_or_init")?,
        );

        ensure_root(metadata.clone(), superblock.clone(), config)
            .await
            .context("ensure_root")?;

        let segments = Arc::new(
            SegmentManager::new_with_store(shared_store, seg_prefix, config, handle.clone())
                .context("SegmentManager::new_with_store")?,
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
            config.clone(),
            metadata,
            superblock,
            segments,
            None,
            journal,
            handle,
            client_state,
            None,
            replay_logger,
            None,
            None,
            None,
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

    Ok(ClawfsRuntime {
        fs,
        fd_table: FdTable::new(),
        _tokio_rt: tokio_rt,
    })
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
    ensure_welcome_file(metadata, superblock, config, uid, gid).await?;
    Ok(())
}

async fn ensure_welcome_file(
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    config: &Config,
    uid: u32,
    gid: u32,
) -> Result<()> {
    let mut root = metadata
        .get_inode(ROOT_INODE)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing root inode {}", ROOT_INODE))?;
    if root
        .children()
        .map(|children| children.contains_key(WELCOME_FILENAME))
        .unwrap_or(false)
    {
        return Ok(());
    }

    let new_inode = superblock.reserve_inodes(1).await?;
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

    if let Err(err) = metadata
        .persist_inode(&welcome, generation, config.shard_size)
        .await
    {
        superblock.abort_generation(generation);
        return Err(err);
    }
    if let Some(children) = root.children_mut() {
        children.insert(WELCOME_FILENAME.to_string(), new_inode);
    }
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
