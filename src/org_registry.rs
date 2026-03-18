//! Org-scoped volume registry: lazy-loaded, eviction-bounded context cache.
//!
//! The `OrgVolumeRegistry` holds a `DashMap` of per-volume contexts
//! (`VolumeContext`), each carrying the `MetadataStore`, `SegmentManager`, and
//! `SuperblockManager` for one discovered ClawFS volume.
//!
//! Contexts are:
//! - **Created lazily** on first demand so startup does not eagerly open every
//!   volume under the discovery prefix.
//! - **Deduplicated** at init time so two concurrent first-accesses to the same
//!   prefix don't race to create duplicate stores.
//! - **Evicted** when the registry exceeds `max_active_volumes` and an LRU
//!   candidate has been idle longer than `idle_eviction_secs`.
//! - **Health-tracked** so maintenance and relay code can skip unhealthy
//!   volumes without crashing the process.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tokio::sync::Mutex as TokioMutex;
use tracing::{info, warn};

use crate::config::{Config, ObjectStoreProvider};
use crate::maintenance::{CompactionConfig, MaintenanceSchedule};
use crate::metadata::MetadataStore;
use crate::org_policy::VolumeAcceleratorPolicy;
use crate::relay::DEFAULT_RELAY_QUEUE_DEPTH;
use crate::segment::{SegmentManager, segment_prefix};
use crate::superblock::SuperblockManager;

// ── Health state ───────────────────────────────────────────────────────────

/// Per-volume health state understood by the maintenance scheduler and the
/// org worker's health endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeHealth {
    /// No backlog and no due work; volume is quiet.
    HealthyIdle,
    /// Due work exists; the scheduler is servicing it normally.
    HealthyBacklogged,
    /// `accelerator_policy.json` is invalid or unreadable.
    UnhealthyPolicy(String),
    /// `MetadataStore` or `SuperblockManager` failed to initialize.
    UnhealthyInit(String),
    /// A superblock lease acquisition failed; likely another worker is active.
    TransientLeaseContention,
}

impl VolumeHealth {
    pub fn is_healthy(&self) -> bool {
        matches!(
            self,
            VolumeHealth::HealthyIdle
                | VolumeHealth::HealthyBacklogged
                | VolumeHealth::TransientLeaseContention
        )
    }

    pub fn as_str(&self) -> &str {
        match self {
            VolumeHealth::HealthyIdle => "healthy_idle",
            VolumeHealth::HealthyBacklogged => "healthy_backlogged",
            VolumeHealth::UnhealthyPolicy(_) => "unhealthy_policy",
            VolumeHealth::UnhealthyInit(_) => "unhealthy_init",
            VolumeHealth::TransientLeaseContention => "transient_lease_contention",
        }
    }
}

// ── Volume context ─────────────────────────────────────────────────────────

/// All runtime state for one discovered ClawFS volume.
///
/// Relay and maintenance subsystems borrow from this context instead of
/// creating their own stores.
pub struct VolumeContext {
    /// Normalized volume prefix (no leading/trailing slashes).
    pub prefix: String,
    /// Shard geometry read from the superblock at discovery time.
    pub shard_size: u64,
    /// Policy loaded from `<prefix>/metadata/accelerator_policy.json`.
    pub policy: VolumeAcceleratorPolicy,
    /// Metadata store (inode shards, deltas, superblock).
    pub metadata: Arc<MetadataStore>,
    /// Segment manager (immutable segment objects).
    pub segments: Arc<SegmentManager>,
    /// Superblock manager (generation commits, cleanup leases).
    pub superblock: Arc<SuperblockManager>,
    /// Current health state; updated after each maintenance round.
    pub health: Mutex<VolumeHealth>,
    /// Monotonic timestamp of last use (seconds since an arbitrary epoch).
    /// Updated on every access so eviction can identify idle contexts.
    pub last_used_secs: AtomicU64,
    /// Per-volume maintenance schedule (last-run timestamps).
    pub maintenance_schedule: TokioMutex<MaintenanceSchedule>,
    /// Relay commit lock: at most one in-flight commit pipeline per volume.
    /// The org worker's relay handler must hold this before calling
    /// `relay_commit_pipeline`.
    pub relay_commit_lock: TokioMutex<()>,
    /// Admission gate for queued relay write requests.
    ///
    /// At most `DEFAULT_RELAY_QUEUE_DEPTH` requests may wait for the commit
    /// lock at a time.  Requests beyond this limit receive a 503 immediately
    /// rather than piling up unboundedly in memory.
    pub relay_admit_sem: Arc<tokio::sync::Semaphore>,
    /// Compaction config; currently a process-wide default but carried here
    /// so per-volume overrides can be wired in later.
    pub compaction_config: CompactionConfig,
    /// Backlog score from the most recent maintenance round; used by the
    /// weighted round-robin scheduler to prioritize backlogged volumes.
    /// 0 = idle, positive = backlogged (higher → more work pending).
    pub last_backlog_score: std::sync::atomic::AtomicU32,
}

impl VolumeContext {
    /// Mark this context as recently used.
    pub fn touch(&self) {
        // Use an approximate clock via a global counter rather than real-time
        // to keep this lock-free.  Eviction ordering will be approximate but
        // correct for its intended purpose.
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_used_secs.store(secs, Ordering::Relaxed);
    }

    /// Return the health state.
    pub fn health(&self) -> VolumeHealth {
        self.health.lock().clone()
    }

    /// Update the health state.
    pub fn set_health(&self, health: VolumeHealth) {
        *self.health.lock() = health;
    }

    /// Scheduling weight for the weighted round-robin maintenance scheduler.
    ///
    /// Backlogged volumes receive higher weight (up to 4) so they are revisited
    /// more frequently than idle volumes (weight 1).  Unhealthy volumes are
    /// excluded from the schedule entirely (weight 0).
    ///
    /// weight = max(1, min(backlog_score, 4)) for healthy volumes, 0 otherwise.
    pub fn scheduler_weight(&self) -> u32 {
        if !self.health().is_healthy() {
            return 0;
        }
        let score = self.last_backlog_score.load(Ordering::Relaxed);
        score.clamp(1, 4)
    }
}

// ── Discovered volume record ───────────────────────────────────────────────

/// Lightweight record produced by the discovery scan before a full context is
/// loaded.
#[derive(Debug, Clone)]
pub struct DiscoveredVolume {
    /// Normalized prefix (no leading/trailing slashes).
    pub prefix: String,
}

// ── Registry ───────────────────────────────────────────────────────────────

/// Per-volume context cache.
///
/// Thread-safe: may be shared across the maintenance scheduler, relay
/// handler, and HTTP health endpoint.
pub struct OrgVolumeRegistry {
    /// Loaded contexts, keyed by normalized prefix.
    contexts: DashMap<String, Arc<VolumeContext>>,
    /// Mutex per prefix held during lazy init to deduplicate concurrent first
    /// accesses.
    init_locks: DashMap<String, Arc<TokioMutex<()>>>,
    /// Maximum number of simultaneously-loaded contexts.  When exceeded,
    /// idle contexts above this limit are evicted.
    max_active: usize,
    /// Minimum seconds of idle time before a context is eligible for eviction.
    idle_eviction_secs: u64,
    /// Base path for per-volume temp/stage/cache directories.
    base_temp_dir: PathBuf,
    /// Object store provider used when constructing per-volume configs.
    object_provider: ObjectStoreProvider,
    /// Bucket for object store (when using aws/gcs).
    bucket: Option<String>,
    /// Region for object store.
    region: Option<String>,
    /// Custom endpoint override.
    endpoint: Option<String>,
    /// Tokio runtime handle for blocking-in-async construction.
    handle: Handle,
}

impl OrgVolumeRegistry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_active: usize,
        idle_eviction_secs: u64,
        base_temp_dir: PathBuf,
        object_provider: ObjectStoreProvider,
        bucket: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        handle: Handle,
    ) -> Self {
        Self {
            contexts: DashMap::new(),
            init_locks: DashMap::new(),
            max_active,
            idle_eviction_secs,
            base_temp_dir,
            object_provider,
            bucket,
            region,
            endpoint,
            handle,
        }
    }

    /// Number of currently-loaded contexts.
    pub fn loaded_count(&self) -> usize {
        self.contexts.len()
    }

    /// Return all currently-loaded prefixes.
    pub fn loaded_prefixes(&self) -> Vec<String> {
        self.contexts.iter().map(|e| e.key().clone()).collect()
    }

    /// Get a loaded context without triggering lazy init.  Returns `None` if
    /// the context has not been loaded yet.
    pub fn get_if_loaded(&self, prefix: &str) -> Option<Arc<VolumeContext>> {
        let key = normalize_prefix(prefix);
        self.contexts.get(&key).map(|e| {
            e.value().touch();
            Arc::clone(e.value())
        })
    }

    /// Get or lazily initialize a volume context.
    ///
    /// If two callers race for the same uncached prefix, only one will run
    /// init; the other waits on the per-prefix init lock.
    pub async fn get_or_load(
        &self,
        volume: &DiscoveredVolume,
        policy: VolumeAcceleratorPolicy,
    ) -> Result<Arc<VolumeContext>> {
        let key = normalize_prefix(&volume.prefix);

        // Fast path: already loaded.
        if let Some(ctx) = self.contexts.get(&key) {
            ctx.touch();
            return Ok(Arc::clone(ctx.value()));
        }

        // Acquire per-prefix init lock to deduplicate concurrent first access.
        let init_lock = {
            self.init_locks
                .entry(key.clone())
                .or_insert_with(|| Arc::new(TokioMutex::new(())))
                .clone()
        };
        let _guard = init_lock.lock().await;

        // Check again after acquiring lock (another task may have finished).
        if let Some(ctx) = self.contexts.get(&key) {
            ctx.touch();
            return Ok(Arc::clone(ctx.value()));
        }

        // Load the context.
        let ctx = self.load_context(key.clone(), policy).await?;
        let ctx = Arc::new(ctx);
        self.contexts.insert(key.clone(), Arc::clone(&ctx));
        self.init_locks.remove(&key);

        // Evict idle entries if over capacity.
        self.maybe_evict();

        Ok(ctx)
    }

    /// Upsert policy for an already-loaded context (used after policy refresh).
    pub fn update_policy(&self, prefix: &str, policy: VolumeAcceleratorPolicy) {
        let key = normalize_prefix(prefix);
        if let Some(ctx) = self.contexts.get(&key) {
            // Policy is immutable in VolumeContext once created; swap the
            // health/maintenance state only if the context is healthy.
            // For now just log; full per-context policy live-reload is
            // a future enhancement.
            let _ = policy; // suppress unused-variable warning
            info!(
                volume_prefix = %key,
                "policy refresh noted; will apply on next load"
            );
            let _ = ctx; // keep borrow alive
        }
    }

    /// Remove a context from the registry (e.g., volume was deleted).
    pub fn remove(&self, prefix: &str) {
        let key = normalize_prefix(prefix);
        self.contexts.remove(&key);
        self.init_locks.remove(&key);
    }

    /// Evict idle contexts if the registry exceeds `max_active`.
    pub fn maybe_evict(&self) {
        if self.contexts.len() <= self.max_active {
            return;
        }
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let cutoff = now_secs.saturating_sub(self.idle_eviction_secs);

        // Collect eviction candidates: idle AND not currently holding relay
        // or maintenance locks.  We can't check in-flight locks here without
        // try_lock, so we only evict contexts that are clearly idle.
        let mut candidates: Vec<(u64, String)> = self
            .contexts
            .iter()
            .filter_map(|e| {
                let last = e.value().last_used_secs.load(Ordering::Relaxed);
                if last <= cutoff {
                    Some((last, e.key().clone()))
                } else {
                    None
                }
            })
            .collect();

        // Evict oldest-first until within cap.
        candidates.sort_unstable();
        let target_evict = self.contexts.len().saturating_sub(self.max_active);
        for (_, key) in candidates.into_iter().take(target_evict) {
            self.contexts.remove(&key);
            warn!(
                volume_prefix = %key,
                "registry_evict_idle_context"
            );
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────

    async fn load_context(
        &self,
        prefix: String,
        policy: VolumeAcceleratorPolicy,
    ) -> Result<VolumeContext> {
        info!(volume_prefix = %prefix, "registry_loading_context");

        let config = self.build_per_volume_config(&prefix);
        let handle = self.handle.clone();

        let (store, meta_prefix) =
            crate::metadata::create_object_store(&config).context("create object store")?;
        let seg_prefix = segment_prefix(&config.object_prefix);

        let metadata = Arc::new(
            MetadataStore::new_with_store(store.clone(), meta_prefix, &config, handle.clone())
                .await
                .context("init MetadataStore")?,
        );

        let segments = Arc::new(
            SegmentManager::new_with_store(store, seg_prefix, &config, handle)
                .context("init SegmentManager")?,
        );

        let superblock =
            Arc::new(SuperblockManager::load_or_init(metadata.clone(), config.shard_size).await?);

        let shard_size = superblock.snapshot().shard_size;

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(VolumeContext {
            prefix: prefix.clone(),
            shard_size,
            policy,
            metadata,
            segments,
            superblock,
            health: Mutex::new(VolumeHealth::HealthyIdle),
            last_used_secs: AtomicU64::new(now_secs),
            maintenance_schedule: TokioMutex::new(MaintenanceSchedule::new()),
            relay_commit_lock: TokioMutex::new(()),
            compaction_config: CompactionConfig::default(),
            last_backlog_score: std::sync::atomic::AtomicU32::new(0),
            relay_admit_sem: Arc::new(tokio::sync::Semaphore::new(DEFAULT_RELAY_QUEUE_DEPTH)),
        })
    }

    fn build_per_volume_config(&self, prefix: &str) -> Config {
        // Temp/stage/cache are namespaced under the volume prefix so
        // different volumes don't collide on disk.
        let safe_prefix = prefix.replace('/', "_");
        let store_path = self.base_temp_dir.join("store").join(&safe_prefix);
        let cache_path = self.base_temp_dir.join("cache").join(&safe_prefix);
        let state_path = self
            .base_temp_dir
            .join("state")
            .join(format!("{safe_prefix}.bin"));

        std::fs::create_dir_all(&store_path).ok();
        std::fs::create_dir_all(&cache_path).ok();
        if let Some(p) = state_path.parent() {
            std::fs::create_dir_all(p).ok();
        }

        Config {
            log_storage_io: false,
            inline_threshold: 4096,
            // shard_size is read from superblock after init; start with default
            shard_size: 2048,
            inode_batch: 128,
            segment_batch: 128,
            pending_bytes: 1024 * 1024,
            entry_ttl_secs: 10,
            disable_journal: true,
            flush_interval_ms: 0,
            disable_cleanup: true,
            lookup_cache_ttl_ms: 0,
            dir_cache_ttl_ms: 0,
            metadata_poll_interval_ms: 0,
            segment_cache_bytes: 128 * 1024 * 1024,
            imap_delta_batch: 32,
            fuse_threads: 0,
            accelerator_mode: None,
            accelerator_endpoint: None,
            accelerator_fallback_policy: None,
            relay_fallback_policy: None,
            object_provider: self.object_provider,
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            object_prefix: prefix.to_string(),
            ..Config::with_paths(
                PathBuf::from("/tmp/clawfs-org-mnt"),
                store_path,
                cache_path,
                state_path,
            )
        }
    }
}

// ── Utilities ──────────────────────────────────────────────────────────────

/// Normalize a volume prefix: strip leading/trailing slashes.
pub fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_strips_slashes() {
        assert_eq!(normalize_prefix("/orgs/myorg/vol1/"), "orgs/myorg/vol1");
        assert_eq!(normalize_prefix("already/clean"), "already/clean");
        assert_eq!(normalize_prefix(""), "");
    }

    #[test]
    fn volume_health_is_healthy() {
        assert!(VolumeHealth::HealthyIdle.is_healthy());
        assert!(VolumeHealth::HealthyBacklogged.is_healthy());
        assert!(VolumeHealth::TransientLeaseContention.is_healthy());
        assert!(!VolumeHealth::UnhealthyInit("err".into()).is_healthy());
        assert!(!VolumeHealth::UnhealthyPolicy("err".into()).is_healthy());
    }

    #[test]
    fn health_as_str_is_stable() {
        assert_eq!(VolumeHealth::HealthyIdle.as_str(), "healthy_idle");
        assert_eq!(
            VolumeHealth::UnhealthyPolicy("x".into()).as_str(),
            "unhealthy_policy"
        );
    }
}
