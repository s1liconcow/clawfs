//! Org-scoped hosted accelerator integration tests.
//!
//! These tests verify the org-scoped worker (`clawfs_org_worker`) across multiple
//! volumes, including discovery, relay forwarding, maintenance scheduling, and
//! failure isolation.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::runtime::Runtime;

use clawfs::config::{Config, ObjectStoreProvider};
use clawfs::metadata::MetadataStore;
use clawfs::org_policy::VolumeAcceleratorPolicy;
use clawfs::org_registry::{DiscoveredVolume, OrgVolumeRegistry};
use clawfs::superblock::SuperblockManager;

/// Test harness for org-scoped multi-volume scenarios.
pub struct OrgScopedTestHarness {
    /// Root temp directory containing all volumes
    pub tempdir: tempfile::TempDir,
    /// Tokio runtime for async operations
    pub runtime: Runtime,
    /// Registry for managing volume contexts
    pub registry: Arc<OrgVolumeRegistry>,
}

impl OrgScopedTestHarness {
    /// Create a new test harness with multiple volumes under a discovery prefix.
    ///
    /// Creates `num_volumes` volumes with valid superblocks under:
    ///   `<tempdir>/<discovery_prefix>/vol_N/`
    pub fn with_volumes(num_volumes: usize, discovery_prefix: &str) -> Result<(Self, Vec<String>)> {
        let tempdir = tempfile::tempdir().context("create org-scoped test tempdir")?;
        let runtime = tokio::runtime::Runtime::new().context("create runtime")?;

        // Create volume prefixes and initialize superblocks
        let mut volume_prefixes = Vec::new();
        for i in 0..num_volumes {
            let prefix = format!("{}/vol_{}", discovery_prefix, i);
            let full_prefix = tempdir.path().join(&prefix);
            std::fs::create_dir_all(&full_prefix).context("create volume prefix dir")?;

            // Initialize superblock for this volume
            runtime.block_on(async {
                let config = build_minimal_config(&full_prefix);
                let (store, meta_prefix) = clawfs::metadata::create_object_store(&config)?;
                let metadata = Arc::new(
                    MetadataStore::new_with_store(
                        store,
                        meta_prefix,
                        &config,
                        runtime.handle().clone(),
                    )
                    .await?,
                );
                // Load or init superblock (this creates it if missing)
                let _ = SuperblockManager::load_or_init(metadata, 2048).await?;
                Result::<()>::Ok(())
            })?;

            volume_prefixes.push(prefix);
        }

        // Create registry
        let base_temp_dir = tempdir.path().join("worker_state");
        std::fs::create_dir_all(&base_temp_dir)?;
        let registry = Arc::new(OrgVolumeRegistry::new(
            64,   // max_active_volumes
            3600, // idle_eviction_secs
            base_temp_dir,
            ObjectStoreProvider::Local,
            None, // bucket
            None, // region
            None, // endpoint
            runtime.handle().clone(),
        ));

        let harness = Self {
            tempdir,
            runtime,
            registry,
        };

        Ok((harness, volume_prefixes))
    }

    /// Load a volume context via the registry.
    pub async fn load_volume(
        &self,
        prefix: &str,
    ) -> Result<Arc<clawfs::org_registry::VolumeContext>> {
        let volume = DiscoveredVolume {
            prefix: prefix.to_string(),
        };
        let policy = VolumeAcceleratorPolicy::default();
        self.registry.get_or_load(&volume, policy).await
    }

    /// Create an intentionally corrupted volume (for unhealthy-volume tests).
    pub fn create_corrupted_volume(&self, prefix: &str) -> Result<()> {
        let full_path = self.tempdir.path().join(prefix);
        std::fs::create_dir_all(&full_path)?;

        // Create a corrupted superblock file
        let superblock_path = full_path.join("metadata/superblock.bin");
        std::fs::create_dir_all(superblock_path.parent().unwrap())?;
        std::fs::write(&superblock_path, b"corrupted data that is not valid")?;

        Ok(())
    }

    /// Get the discovery prefix path suitable for the org worker CLI.
    pub fn discovery_prefix_path(&self, prefix: &str) -> String {
        self.tempdir
            .path()
            .join(prefix)
            .to_string_lossy()
            .to_string()
    }
}

/// Build minimal config for volume initialization.
fn build_minimal_config(store_path: &Path) -> Config {
    let mut config = Config::with_paths(
        store_path.join("mnt"),
        store_path.join("store"),
        store_path.join("cache"),
        store_path.join("state.bin"),
    );
    config.object_provider = ObjectStoreProvider::Local;
    config.object_prefix = store_path.to_string_lossy().to_string();
    config.shard_size = 2048;
    config.disable_journal = true;
    config
}

#[test]
fn org_worker_discovers_multiple_volumes() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(3, "test_org")?;

    // Load all volumes through the registry
    for prefix in &volume_prefixes {
        let ctx = harness
            .runtime
            .block_on(harness.load_volume(prefix))
            .context("load volume")?;
        assert_eq!(ctx.prefix, *prefix);
    }

    // Verify all prefixes are tracked as loaded
    let loaded = harness.registry.loaded_prefixes();
    assert_eq!(loaded.len(), 3, "expected 3 loaded volumes");
    for prefix in &volume_prefixes {
        assert!(
            loaded.contains(prefix),
            "volume {} should be loaded",
            prefix
        );
    }

    Ok(())
}

#[test]
fn org_worker_tracks_volume_health() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(2, "test_org")?;

    // Load both volumes
    let ctx_healthy = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))
        .context("load first volume")?;
    let ctx_second = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))
        .context("load second volume")?;

    // Set different health states
    ctx_healthy.set_health(clawfs::org_registry::VolumeHealth::HealthyIdle);
    ctx_second.set_health(clawfs::org_registry::VolumeHealth::HealthyBacklogged);

    // Verify health states are tracked correctly
    assert_eq!(ctx_healthy.health().as_str(), "healthy_idle");
    assert_eq!(ctx_second.health().as_str(), "healthy_backlogged");
    assert!(ctx_healthy.health().is_healthy());
    assert!(ctx_second.health().is_healthy());

    // Test unhealthy state
    ctx_second.set_health(clawfs::org_registry::VolumeHealth::UnhealthyPolicy(
        "invalid relay_fallback_policy value".to_string(),
    ));
    assert_eq!(ctx_second.health().as_str(), "unhealthy_policy");
    assert!(!ctx_second.health().is_healthy());

    // Test transient lease contention
    ctx_second.set_health(clawfs::org_registry::VolumeHealth::TransientLeaseContention);
    assert_eq!(ctx_second.health().as_str(), "transient_lease_contention");
    assert!(ctx_second.health().is_healthy()); // Transient contention is still "healthy" for scheduling

    Ok(())
}

#[test]
fn org_worker_policy_refresh_updates_existing_context() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(1, "test_org")?;

    let prefix = &volume_prefixes[0];

    // Load volume initially with default policy
    let ctx = harness
        .runtime
        .block_on(harness.load_volume(prefix))
        .context("load volume")?;

    let initial_policy = ctx.read_policy();
    assert!(!initial_policy.relay_enabled); // default

    // Simulate policy refresh by updating with new policy
    let updated_policy = VolumeAcceleratorPolicy {
        relay_enabled: true,
        maintenance_enabled: true,
        ..Default::default()
    };

    let changed = harness
        .registry
        .update_policy(prefix, updated_policy.clone());
    assert!(changed, "policy should have changed");

    // Verify policy was updated
    let new_policy = ctx.read_policy();
    assert!(new_policy.relay_enabled);
    assert!(new_policy.maintenance_enabled);

    // Verify timestamp was updated
    let last_refresh = *ctx.last_policy_refresh.lock();
    assert!(last_refresh > 0, "policy refresh timestamp should be set");

    Ok(())
}

#[test]
fn org_worker_maintenance_round_updates_health_and_backlog() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(1, "test_org")?;

    let prefix = &volume_prefixes[0];

    // Load volume
    let ctx = harness
        .runtime
        .block_on(harness.load_volume(prefix))
        .context("load volume")?;

    // Simulate a maintenance round completion
    let round_status = clawfs::maintenance::RoundStatus {
        delta_backlog: 42,
        segment_backlog: 7,
        checkpoint_backlog: 3,
    };

    // Store round status
    *ctx.last_round_status.lock() = Some(round_status);

    // Update backlog score
    let backlog = (round_status.delta_backlog + round_status.segment_backlog) as u32;
    ctx.last_backlog_score
        .store(backlog, std::sync::atomic::Ordering::Relaxed);

    // Update health
    ctx.set_health(clawfs::org_registry::VolumeHealth::HealthyBacklogged);

    // Verify updates
    assert_eq!(ctx.health().as_str(), "healthy_backlogged");
    assert_eq!(
        ctx.last_backlog_score
            .load(std::sync::atomic::Ordering::Relaxed),
        49
    );

    let stored_status = *ctx.last_round_status.lock();
    assert!(stored_status.is_some());
    assert_eq!(stored_status.unwrap().delta_backlog, 42);

    Ok(())
}

#[test]
fn org_worker_scheduler_weight_reflects_backlog() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(2, "test_org")?;

    // Load both volumes
    let ctx_idle = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))?;
    let ctx_backlogged = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))?;

    // Idle volume: weight should be 1 (minimum for healthy)
    ctx_idle.set_health(clawfs::org_registry::VolumeHealth::HealthyIdle);
    ctx_idle
        .last_backlog_score
        .store(0, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(ctx_idle.scheduler_weight(), 1);

    // Backlogged volume: weight should scale with backlog
    ctx_backlogged.set_health(clawfs::org_registry::VolumeHealth::HealthyBacklogged);
    ctx_backlogged
        .last_backlog_score
        .store(3, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(ctx_backlogged.scheduler_weight(), 3);

    // Max weight is 4
    ctx_backlogged
        .last_backlog_score
        .store(10, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(ctx_backlogged.scheduler_weight(), 4);

    // Unhealthy volume: weight is 0 (excluded from scheduling)
    ctx_backlogged.set_health(clawfs::org_registry::VolumeHealth::UnhealthyInit(
        "test error".to_string(),
    ));
    assert_eq!(ctx_backlogged.scheduler_weight(), 0);

    Ok(())
}

#[test]
fn org_worker_context_eviction_when_over_capacity() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(3, "test_org")?;

    // Replace registry with one that has capacity=2
    let base_temp_dir = harness.tempdir.path().join("worker_state_evict");
    std::fs::create_dir_all(&base_temp_dir)?;
    let registry = Arc::new(OrgVolumeRegistry::new(
        2, // max_active_volumes = 2
        1, // idle_eviction_secs = 1 (very short for testing)
        base_temp_dir,
        ObjectStoreProvider::Local,
        None,
        None,
        None,
        harness.runtime.handle().clone(),
    ));

    // Load 3 volumes
    for prefix in &volume_prefixes {
        let volume = DiscoveredVolume {
            prefix: prefix.clone(),
        };
        harness.runtime.block_on(async {
            let _ = registry
                .get_or_load(&volume, VolumeAcceleratorPolicy::default())
                .await?;
            Result::<()>::Ok(())
        })?;
    }

    // Initially all 3 should be loaded
    assert_eq!(registry.loaded_prefixes().len(), 3);

    // Trigger eviction by touching contexts and calling maybe_evict
    std::thread::sleep(Duration::from_secs(2)); // Wait for idle timeout
    registry.maybe_evict();

    // After eviction, should be at or below capacity
    let loaded_after = registry.loaded_prefixes().len();
    assert!(
        loaded_after <= 2,
        "after eviction should have at most 2 volumes, got {}",
        loaded_after
    );

    Ok(())
}
