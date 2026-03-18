mod common;

use std::time::Duration;

use anyhow::{Context, Result};
use clawfs::clawfs::{AcceleratorFallbackPolicy, AcceleratorMode};
use clawfs::config::Config;
use clawfs::coordination::CoordinationSubscriber;
use clawfs::maintenance::{
    CompactionConfig, acquire_cleanup_lease, release_cleanup_lease, run_segment_compaction,
};
use clawfs::superblock::CleanupTaskKind;
use tempfile::tempdir;

use common::{
    HostedTestHarness, MockControlPlane, MockCoordinationEndpoint, assert_generation_advanced,
};

#[test]
fn bootstrap_applies_managed_runtime_contract() -> Result<()> {
    let tempdir = tempdir().context("create bootstrap tempdir")?;
    let control_plane =
        MockControlPlane::direct_plus_cache("https://accelerator.example", "http://events.example");
    let mut config = Config::with_paths(
        tempdir.path().join("mnt"),
        tempdir.path().join("store"),
        tempdir.path().join("cache"),
        tempdir.path().join("state.bin"),
    );

    control_plane.apply(&mut config);

    assert_eq!(
        config.accelerator_mode,
        Some(AcceleratorMode::DirectPlusCache)
    );
    assert_eq!(
        config.accelerator_endpoint.as_deref(),
        Some("https://accelerator.example")
    );
    assert_eq!(
        config.accelerator_fallback_policy,
        Some(AcceleratorFallbackPolicy::PollAndDirect)
    );
    assert!(config.disable_cleanup);
    assert_eq!(
        control_plane.hosted.event_endpoint.as_deref(),
        Some("http://events.example")
    );
    assert!(control_plane.hosted.relay_fallback_policy.is_none());
    Ok(())
}

#[test]
fn hosted_maintenance_cycle_prunes_deltas() -> Result<()> {
    let harness = HostedTestHarness::new("maintenance-state")?;
    harness.create_test_volume_with_deltas(12)?;

    let before = harness.volume.metadata.delta_file_count()?;
    let compaction_config = CompactionConfig {
        delta_compact_threshold: 4,
        delta_compact_keep: 2,
        ..CompactionConfig::default()
    };
    let client_id = "cleanup-worker";

    let worker_rt = tokio::runtime::Runtime::new().context("create maintenance runtime")?;
    worker_rt.block_on(async {
        assert!(
            acquire_cleanup_lease(
                &harness.volume.superblock,
                CleanupTaskKind::DeltaCompaction,
                client_id,
                &compaction_config,
            )
            .await?
        );
        Result::<()>::Ok(())
    })?;

    let pruned = harness
        .volume
        .metadata
        .prune_deltas(compaction_config.delta_compact_keep)?;
    assert!(pruned > 0, "expected delta pruning");

    worker_rt.block_on(async {
        release_cleanup_lease(
            &harness.volume.superblock,
            CleanupTaskKind::DeltaCompaction,
            client_id,
        )
        .await?;
        Result::<()>::Ok(())
    })?;

    let after = harness.volume.metadata.delta_file_count()?;
    assert!(after < before, "delta backlog should shrink");
    assert!(
        harness
            .volume
            .superblock
            .snapshot()
            .cleanup_leases
            .is_empty()
    );
    Ok(())
}

#[test]
fn hosted_segment_compaction_rewrites_old_segments() -> Result<()> {
    let harness = HostedTestHarness::new("segment-state")?;
    harness.create_test_volume_with_segments(6)?;

    let before = harness.volume.metadata.segment_candidates(8)?.len();
    let compaction_config = CompactionConfig {
        segment_compact_batch: 4,
        segment_compact_lag: 1,
        ..CompactionConfig::default()
    };

    let worker_rt = tokio::runtime::Runtime::new().context("create segment runtime")?;
    worker_rt.block_on(async {
        let result = run_segment_compaction(
            &harness.volume.metadata,
            &harness.volume.segments,
            &compaction_config,
        )
        .await?;
        anyhow::ensure!(result.segments_merged > 0, "expected segment compaction");
        Result::<()>::Ok(())
    })?;

    let after = harness.volume.metadata.segment_candidates(8)?.len();
    assert!(after <= before, "segment backlog should not grow");
    Ok(())
}

#[test]
fn coordination_refreshes_metadata_and_recovers_after_disconnect() -> Result<()> {
    let harness = HostedTestHarness::new("coordination-a")?;
    let peer = harness.open_peer_client("coordination-b")?;
    let endpoint = MockCoordinationEndpoint::spawn(&harness.volume.runtime)?;

    let subscriber = CoordinationSubscriber::new_with_staleness_timeout(
        endpoint.url.clone(),
        peer.metadata.clone(),
        peer.superblock.clone(),
        Duration::from_millis(25),
        Duration::from_millis(25),
        Duration::from_secs(1),
    );

    let root_before = peer
        .runtime
        .block_on(async { peer.metadata.get_inode(clawfs::inode::ROOT_INODE).await })?
        .context("missing root inode")?;
    assert!(root_before.is_dir());
    assert!(
        peer.runtime
            .block_on(async { peer.metadata.get_inode(2).await })?
            .is_none(),
        "peer should not see the first file before refresh"
    );

    harness.create_test_volume_with_deltas(1)?;
    let committed_generation = harness.volume.superblock.snapshot().generation;
    endpoint.push_generation_hint(committed_generation, "client-a");

    let events = peer
        .runtime
        .block_on(async { subscriber.poll_once().await })?;
    assert!(!events.is_empty(), "expected coordination events");
    peer.runtime
        .block_on(async { subscriber.apply_events(events).await })?;
    peer.runtime.block_on(async {
        assert_generation_advanced(&peer.metadata, committed_generation).await
    })?;
    assert!(
        peer.runtime
            .block_on(async { peer.metadata.get_inode(2).await })?
            .is_some(),
        "peer should see the first file after refresh"
    );
    assert_eq!(subscriber.last_applied_generation(), committed_generation);

    endpoint.set_available(false);
    let err = peer
        .runtime
        .block_on(async { subscriber.poll_once().await });
    assert!(
        err.is_err(),
        "expected polling failure while endpoint is down"
    );

    harness.create_test_volume_with_deltas(1)?;
    let next_generation = harness.volume.superblock.snapshot().generation;
    endpoint.set_available(true);
    endpoint.push_generation_hint(next_generation, "client-a");

    let events = peer
        .runtime
        .block_on(async { subscriber.poll_once().await })?;
    peer.runtime
        .block_on(async { subscriber.apply_events(events).await })?;
    peer.runtime
        .block_on(async { assert_generation_advanced(&peer.metadata, next_generation).await })?;
    assert!(
        peer.runtime
            .block_on(async { peer.metadata.get_inode(3).await })?
            .is_some(),
        "peer should see the second file after reconnect"
    );
    Ok(())
}
