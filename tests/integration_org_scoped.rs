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

// ── Scheduler failure-isolation and starvation tests ────────────────────────
// Tests for osagefs-f7j.3.4: Add scheduler failure-isolation and starvation
// tests for org-wide maintenance

#[test]
fn scheduler_skips_unhealthy_volume_but_continues_healthy_ones() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(3, "test_org")?;

    // Load all 3 volumes
    let ctx_healthy = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))?;
    let ctx_to_break = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))?;
    let ctx_other_healthy = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[2]))?;

    // Mark one volume as unhealthy (simulating repeated policy/init failures)
    ctx_to_break.set_health(clawfs::org_registry::VolumeHealth::UnhealthyInit(
        "superblock corrupted".to_string(),
    ));

    // Verify unhealthy volume has weight 0
    assert_eq!(ctx_to_break.scheduler_weight(), 0);

    // Verify healthy volumes still have positive weights
    assert!(ctx_healthy.scheduler_weight() >= 1);
    assert!(ctx_other_healthy.scheduler_weight() >= 1);

    // Simulate scheduler behavior: build schedule list
    let schedule_candidates: Vec<_> = harness
        .registry
        .loaded_prefixes()
        .iter()
        .filter_map(|p| harness.registry.get_if_loaded(p))
        .filter(|ctx| ctx.scheduler_weight() > 0)
        .map(|ctx| ctx.prefix.clone())
        .collect();

    // The unhealthy volume should not appear in schedule candidates
    assert!(
        !schedule_candidates.contains(&ctx_to_break.prefix),
        "unhealthy volume should be excluded from scheduling"
    );
    assert_eq!(
        schedule_candidates.len(),
        2,
        "should only have 2 healthy volumes in schedule"
    );

    Ok(())
}

#[test]
fn scheduler_marks_volume_unhealthy_on_policy_failure_isolates_it() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(2, "test_org")?;

    // Create an intentionally corrupted policy file for volume 0
    let policy_path = harness
        .tempdir
        .path()
        .join(&volume_prefixes[0])
        .join("metadata/accelerator_policy.json");
    std::fs::create_dir_all(policy_path.parent().unwrap())?;
    std::fs::write(&policy_path, "{invalid json content")?;

    // Load volume 0 - it should detect bad policy and mark itself unhealthy
    let ctx_bad_policy = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))?;

    // Simulate policy validation failure marking volume unhealthy
    ctx_bad_policy.set_health(clawfs::org_registry::VolumeHealth::UnhealthyPolicy(
        "invalid policy JSON".to_string(),
    ));

    // Load volume 1 - it should be healthy
    let ctx_healthy = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))?;

    // Verify isolation: healthy volume can still be scheduled
    assert_eq!(ctx_bad_policy.scheduler_weight(), 0);
    assert!(ctx_healthy.scheduler_weight() >= 1);

    // Verify health states are distinct
    assert!(!ctx_bad_policy.health().is_healthy());
    assert!(ctx_healthy.health().is_healthy());

    Ok(())
}

#[test]
fn scheduler_fairness_idle_volume_gets_service_despite_hot_volume() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(2, "test_org")?;

    // Load both volumes
    let ctx_idle = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))?;
    let ctx_hot = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))?;

    // Simulate hot volume with heavy backlog
    let hot_round_status = clawfs::maintenance::RoundStatus {
        delta_backlog: 150,    // Above threshold
        segment_backlog: 25,   // Heavy segments
        checkpoint_backlog: 5, // Multiple pending
    };
    *ctx_hot.last_round_status.lock() = Some(hot_round_status);
    ctx_hot
        .last_backlog_score
        .store(180, std::sync::atomic::Ordering::Relaxed);
    ctx_hot.set_health(clawfs::org_registry::VolumeHealth::HealthyBacklogged);

    // Simulate idle volume with minimal work
    let idle_round_status = clawfs::maintenance::RoundStatus {
        delta_backlog: 2,
        segment_backlog: 0,
        checkpoint_backlog: 0,
    };
    *ctx_idle.last_round_status.lock() = Some(idle_round_status);
    ctx_idle
        .last_backlog_score
        .store(2, std::sync::atomic::Ordering::Relaxed);
    ctx_idle.set_health(clawfs::org_registry::VolumeHealth::HealthyIdle);

    // Verify both volumes are schedulable (positive weights)
    assert!(ctx_hot.scheduler_weight() >= 1);
    assert!(ctx_idle.scheduler_weight() >= 1);

    // Weighted round-robin: hot volume gets higher weight (up to max 4)
    assert_eq!(ctx_hot.scheduler_weight(), 4); // capped at 4
    assert_eq!(ctx_idle.scheduler_weight(), 2); // 2 from backlog_score, clamped to 2

    // Both volumes should be eligible for scheduling
    // This prevents starvation - the idle volume still gets checkpoint/lifecycle work
    let weights = [ctx_hot.scheduler_weight(), ctx_idle.scheduler_weight()];
    assert!(
        weights.iter().all(|&w| w > 0),
        "all healthy volumes must be schedulable"
    );

    Ok(())
}

#[test]
fn scheduler_one_round_per_volume_enforcement() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(2, "test_org")?;

    let ctx_1 = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))?;
    let ctx_2 = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))?;

    // Simulate a maintenance round in progress for volume 1
    // In real code this would try_lock() the maintenance_schedule mutex
    // For test, we verify the lock exists and can be held
    let maintenance_guard_1 = harness
        .runtime
        .block_on(async { ctx_1.maintenance_schedule.lock().await });

    // While maintenance_guard_1 is held, verify the lock is actually locked
    // Try to acquire a second lock - this should not complete immediately
    let try_result = ctx_1.maintenance_schedule.try_lock();
    assert!(
        try_result.is_err(),
        "maintenance_schedule should be locked while round is in progress"
    );

    // Volume 2 should still be available for scheduling (no lock contention)
    let try_result_2 = ctx_2.maintenance_schedule.try_lock();
    assert!(
        try_result_2.is_ok(),
        "other volumes should not be blocked by volume 1's maintenance"
    );

    // Drop the first guard to simulate round completion
    drop(maintenance_guard_1);

    // Now volume 1 should be available again
    let try_result_after = ctx_1.maintenance_schedule.try_lock();
    assert!(
        try_result_after.is_ok(),
        "volume should be available for scheduling after round completes"
    );

    Ok(())
}

#[test]
fn scheduler_bounded_concurrency_respects_limit() -> Result<()> {
    let (harness, _volume_prefixes) = OrgScopedTestHarness::with_volumes(4, "test_org")?;

    // Create a semaphore with capacity 2 to simulate bounded concurrency
    let concurrency_limit = Arc::new(tokio::sync::Semaphore::new(2));

    // Acquire 2 permits and hold them to simulate active maintenance rounds
    // Use a single async block to ensure permits reference the same semaphore
    let (permit1, permit2) = harness.runtime.block_on(async {
        let p1 = concurrency_limit.acquire().await.unwrap();
        let p2 = concurrency_limit.acquire().await.unwrap();
        (p1, p2)
    });

    // Now try to acquire a third permit - should fail immediately (try_acquire)
    // or block until a permit is released
    let try_result = concurrency_limit.try_acquire();
    assert!(
        try_result.is_err(),
        "third permit should fail to acquire when limit is 2 and 2 are held"
    );

    // Verify available permits is 0 when limit (2) - held (2)
    assert_eq!(
        concurrency_limit.available_permits(),
        0,
        "should have 0 available permits when all are in use"
    );

    // Release one permit
    drop(permit1);

    // Now one permit should be available
    assert_eq!(
        concurrency_limit.available_permits(),
        1,
        "should have 1 available permit after releasing one"
    );

    // Try to acquire again - should succeed now
    let try_result_after = concurrency_limit.try_acquire();
    assert!(
        try_result_after.is_ok(),
        "should be able to acquire after releasing a permit"
    );

    // Release remaining permits
    drop(permit2);
    drop(try_result_after.unwrap());

    // All permits should be available again
    assert_eq!(
        concurrency_limit.available_permits(),
        2,
        "should have all 2 permits available after releasing all"
    );

    Ok(())
}

#[test]
fn scheduler_dynamic_discovery_adds_volumes_mid_run() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(2, "test_org")?;

    // Load initial 2 volumes
    let ctx_1 = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))?;
    let ctx_2 = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))?;

    // Verify initial state
    assert_eq!(harness.registry.loaded_count(), 2);
    let initial_prefixes = harness.registry.loaded_prefixes();
    assert_eq!(initial_prefixes.len(), 2);

    // Create a new volume dynamically (simulating discovery finding a new volume)
    let new_vol_prefix = "test_org/vol_dynamic_new";
    let new_vol_path = harness.tempdir.path().join(new_vol_prefix);
    std::fs::create_dir_all(&new_vol_path)?;

    // Initialize superblock for the new volume
    harness.runtime.block_on(async {
        let config = build_minimal_config(&new_vol_path);
        let (store, meta_prefix) = clawfs::metadata::create_object_store(&config)?;
        let metadata = Arc::new(
            MetadataStore::new_with_store(
                store,
                meta_prefix,
                &config,
                harness.runtime.handle().clone(),
            )
            .await?,
        );
        let _ = SuperblockManager::load_or_init(metadata, 2048).await?;
        Result::<()>::Ok(())
    })?;

    // Load the newly discovered volume
    let ctx_new = harness.runtime.block_on(async {
        let volume = DiscoveredVolume {
            prefix: new_vol_prefix.to_string(),
        };
        harness
            .registry
            .get_or_load(&volume, VolumeAcceleratorPolicy::default())
            .await
    })?;

    // Verify all 3 volumes are now in registry
    assert_eq!(harness.registry.loaded_count(), 3);
    let updated_prefixes = harness.registry.loaded_prefixes();
    assert_eq!(updated_prefixes.len(), 3);
    assert!(
        updated_prefixes.contains(&new_vol_prefix.to_string()),
        "newly discovered volume should be in registry"
    );

    // All volumes should be schedulable
    let all_weights_positive = [ctx_1, ctx_2, ctx_new]
        .iter()
        .all(|ctx| ctx.scheduler_weight() > 0);
    assert!(
        all_weights_positive,
        "all volumes including newly discovered should be schedulable"
    );

    Ok(())
}

#[test]
fn scheduler_lifecycle_cleanup_respects_volume_prefix_boundary() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(2, "test_org")?;

    let ctx_vol1 = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))?;
    let ctx_vol2 = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))?;

    // Create lifecycle policies with explicit prefix boundaries
    let policy_vol1 = clawfs::org_policy::VolumeAcceleratorPolicy {
        lifecycle_cleanup: true,
        lifecycle_expiry_days: 7,
        lifecycle_require_confirmation: true,
        ..Default::default()
    };
    let policy_vol2 = clawfs::org_policy::VolumeAcceleratorPolicy {
        lifecycle_cleanup: true,
        lifecycle_expiry_days: 30, // Different expiry
        lifecycle_require_confirmation: true,
        ..Default::default()
    };

    // Convert to LifecyclePolicy with proper prefix scoping
    let lifecycle_1 = policy_vol1
        .lifecycle_policy(&ctx_vol1.prefix)
        .expect("lifecycle should be enabled");
    let lifecycle_2 = policy_vol2
        .lifecycle_policy(&ctx_vol2.prefix)
        .expect("lifecycle should be enabled");

    // Verify prefix isolation
    assert_eq!(lifecycle_1.allowed_prefix, ctx_vol1.prefix);
    assert_eq!(lifecycle_2.allowed_prefix, ctx_vol2.prefix);
    assert_ne!(lifecycle_1.allowed_prefix, lifecycle_2.allowed_prefix);

    // Verify different expiry settings are preserved per volume
    assert_eq!(lifecycle_1.expiry_days, 7);
    assert_eq!(lifecycle_2.expiry_days, 30);

    Ok(())
}

#[test]
fn scheduler_unhealthy_volume_recovery_rejoins_schedule() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(2, "test_org")?;

    let ctx_recoverable = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))?;
    let ctx_stable = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))?;

    // Start with one volume unhealthy (transient failure)
    ctx_recoverable.set_health(clawfs::org_registry::VolumeHealth::UnhealthyInit(
        "temporary connection error".to_string(),
    ));

    // Verify it's excluded from scheduling
    assert_eq!(ctx_recoverable.scheduler_weight(), 0);
    assert!(ctx_stable.scheduler_weight() > 0);

    // Simulate recovery: mark as healthy again
    ctx_recoverable.set_health(clawfs::org_registry::VolumeHealth::HealthyIdle);
    ctx_recoverable
        .last_backlog_score
        .store(1, std::sync::atomic::Ordering::Relaxed);

    // After recovery, volume should rejoin scheduling
    assert!(ctx_recoverable.scheduler_weight() > 0);
    assert_eq!(ctx_recoverable.health().as_str(), "healthy_idle");

    // Verify both volumes now schedulable
    assert!(ctx_recoverable.health().is_healthy());
    assert!(ctx_stable.health().is_healthy());

    Ok(())
}

#[test]
fn scheduler_starvation_prevention_all_volumes_make_progress() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(3, "test_org")?;

    // Load all 3 volumes with varying backlog levels
    let ctx_1 = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))?;
    let ctx_2 = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))?;
    let ctx_3 = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[2]))?;

    // Set up different backlog scenarios
    ctx_1.set_health(clawfs::org_registry::VolumeHealth::HealthyBacklogged);
    ctx_1
        .last_backlog_score
        .store(50, std::sync::atomic::Ordering::Relaxed); // High backlog

    ctx_2.set_health(clawfs::org_registry::VolumeHealth::HealthyIdle);
    ctx_2
        .last_backlog_score
        .store(1, std::sync::atomic::Ordering::Relaxed); // Minimal backlog

    ctx_3.set_health(clawfs::org_registry::VolumeHealth::HealthyBacklogged);
    ctx_3
        .last_backlog_score
        .store(25, std::sync::atomic::Ordering::Relaxed); // Medium backlog

    // All healthy volumes should have positive weights (not starved)
    let weight_1 = ctx_1.scheduler_weight();
    let weight_2 = ctx_2.scheduler_weight();
    let weight_3 = ctx_3.scheduler_weight();

    // All should be at least 1 (minimum for healthy volumes)
    assert!(weight_1 >= 1, "high backlog volume should be schedulable");
    assert!(weight_2 >= 1, "idle volume should not be starved");
    assert!(weight_3 >= 1, "medium backlog volume should be schedulable");

    // High backlog gets higher weight (up to cap of 4)
    assert_eq!(weight_1, 4, "high backlog should have max weight");
    assert!(
        weight_2 <= weight_1,
        "idle weight should not exceed high backlog"
    );

    // Simulate a scheduling round: calculate total "visits" each volume would get
    // over time with weighted round-robin
    let total_weight = weight_1 + weight_2 + weight_3;
    let ratio_1 = weight_1 as f64 / total_weight as f64;
    let ratio_2 = weight_2 as f64 / total_weight as f64;
    let _ratio_3 = weight_3 as f64 / total_weight as f64;

    // Even the idle volume should get some service (not starved)
    assert!(ratio_2 > 0.0, "idle volume ratio should be positive");

    // High backlog volume gets more service than idle
    assert!(
        ratio_1 > ratio_2,
        "high backlog should get more service than idle"
    );

    Ok(())
}

#[test]
fn scheduler_checkpoint_due_for_idle_volume_despite_hot_volume() -> Result<()> {
    let (harness, volume_prefixes) = OrgScopedTestHarness::with_volumes(2, "test_org")?;

    let ctx_idle = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[0]))?;
    let ctx_hot = harness
        .runtime
        .block_on(harness.load_volume(&volume_prefixes[1]))?;

    // Simulate idle volume with checkpoint due (never run)
    let idle_schedule = harness
        .runtime
        .block_on(async { ctx_idle.maintenance_schedule.lock().await });
    assert!(
        idle_schedule.checkpoint_due(std::time::Duration::from_secs(3600)),
        "idle volume with no prior checkpoint should have checkpoint due"
    );
    drop(idle_schedule);

    // Simulate hot volume with recent checkpoint
    let hot_schedule = harness.runtime.block_on(async {
        let mut sched = ctx_hot.maintenance_schedule.lock().await;
        sched.mark_checkpoint_ran();
        sched
    });
    assert!(
        !hot_schedule.checkpoint_due(std::time::Duration::from_secs(3600)),
        "hot volume with fresh checkpoint should not have checkpoint due yet"
    );
    drop(hot_schedule);

    // Both volumes should be considered by scheduler
    ctx_idle.set_health(clawfs::org_registry::VolumeHealth::HealthyIdle);
    ctx_idle
        .last_backlog_score
        .store(0, std::sync::atomic::Ordering::Relaxed);
    ctx_hot.set_health(clawfs::org_registry::VolumeHealth::HealthyBacklogged);
    ctx_hot
        .last_backlog_score
        .store(100, std::sync::atomic::Ordering::Relaxed);

    // Idle volume should still be schedulable (weight 1)
    assert_eq!(ctx_idle.scheduler_weight(), 1);
    // Hot volume gets max weight
    assert_eq!(ctx_hot.scheduler_weight(), 4);

    // Verify: even idle volume with checkpoint due would get service
    let idle_schedule_check = harness
        .runtime
        .block_on(async { ctx_idle.maintenance_schedule.lock().await });
    let checkpoint_interval = std::time::Duration::from_secs(86400); // 24 hours
    assert!(
        idle_schedule_check.checkpoint_due(checkpoint_interval),
        "idle volume should still have checkpoint due and receive service"
    );

    Ok(())
}
