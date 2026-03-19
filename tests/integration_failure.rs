mod common;

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clawfs::coordination::CoordinationSubscriber;
use clawfs::maintenance::{CompactionConfig, acquire_cleanup_lease, release_cleanup_lease};
use clawfs::relay::{RelayClient, RelayStatus};
use clawfs::superblock::CleanupTaskKind;

use common::{
    HostedTestHarness, MockCoordinationEndpoint, MockRelayExecutor, assert_generation_advanced,
    build_coordination_subscriber_with_staleness_timeout, build_relay_write_request,
};

fn lease_config() -> CompactionConfig {
    CompactionConfig {
        lease_ttl_secs: 30,
        ..CompactionConfig::default()
    }
}

#[test]
fn cleanup_lease_expires_and_next_worker_wins() -> Result<()> {
    let harness = HostedTestHarness::new("failure-lease-expiry")?;
    let config = lease_config();

    harness.volume.runtime.block_on(async {
        assert!(
            acquire_cleanup_lease(
                &harness.volume.superblock,
                CleanupTaskKind::DeltaCompaction,
                "worker-a",
                &config,
            )
            .await?
        );
        Result::<()>::Ok(())
    })?;

    harness.inject_lease_expiry(CleanupTaskKind::DeltaCompaction)?;

    harness.volume.runtime.block_on(async {
        assert!(
            acquire_cleanup_lease(
                &harness.volume.superblock,
                CleanupTaskKind::DeltaCompaction,
                "worker-b",
                &config,
            )
            .await?
        );
        Result::<()>::Ok(())
    })?;

    let snapshot = harness.volume.superblock.snapshot();
    assert_eq!(snapshot.cleanup_leases.len(), 1);
    assert_eq!(snapshot.cleanup_leases[0].client_id, "worker-b");

    harness.volume.runtime.block_on(async {
        release_cleanup_lease(
            &harness.volume.superblock,
            CleanupTaskKind::DeltaCompaction,
            "worker-b",
        )
        .await?;
        Result::<()>::Ok(())
    })?;

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
fn cleanup_lease_contention_allows_only_one_winner() -> Result<()> {
    let harness = HostedTestHarness::new("failure-lease-contention")?;
    let config = lease_config();
    let superblock = harness.volume.superblock.clone();

    let worker_a = harness.volume.runtime.block_on(async {
        acquire_cleanup_lease(
            &superblock,
            CleanupTaskKind::SegmentCompaction,
            "worker-a",
            &config,
        )
        .await
    })?;
    let worker_b = harness.volume.runtime.block_on(async {
        acquire_cleanup_lease(
            &superblock,
            CleanupTaskKind::SegmentCompaction,
            "worker-b",
            &config,
        )
        .await
    })?;

    assert!(worker_a, "first worker should win the lease");
    assert!(
        !worker_b,
        "second worker should back off while the lease is held"
    );

    harness.volume.runtime.block_on(async {
        release_cleanup_lease(&superblock, CleanupTaskKind::SegmentCompaction, "worker-a").await?;
        Result::<()>::Ok(())
    })?;

    let reacquired = harness.volume.runtime.block_on(async {
        acquire_cleanup_lease(
            &superblock,
            CleanupTaskKind::SegmentCompaction,
            "worker-b",
            &config,
        )
        .await
    })?;
    assert!(
        reacquired,
        "loser should acquire after the lease is released"
    );
    Ok(())
}

#[test]
fn coordination_gap_detects_missing_events_and_refreshes_metadata() -> Result<()> {
    let harness = HostedTestHarness::new("failure-coord-gap")?;
    let peer = harness.open_peer_client("failure-coord-gap-peer")?;
    let endpoint = MockCoordinationEndpoint::spawn(&harness.volume.runtime)?;
    let subscriber = CoordinationSubscriber::new_with_staleness_timeout(
        endpoint.url.clone(),
        peer.metadata.clone(),
        peer.superblock.clone(),
        Duration::from_millis(5),
        Duration::from_millis(5),
        Duration::from_millis(40),
    );

    harness.create_test_volume_with_deltas(1)?;
    let gen2 = harness.volume.superblock.snapshot().generation;
    harness.create_test_volume_with_deltas(1)?;
    let gen3 = harness.volume.superblock.snapshot().generation;

    endpoint.inject_event_gap(1);
    endpoint.push_generation_hint(gen2, "client-a");
    endpoint.push_generation_hint(gen3, "client-b");

    let events = peer
        .runtime
        .block_on(async { subscriber.poll_once().await })?;
    peer.runtime
        .block_on(async { subscriber.apply_events(events).await })?;

    peer.runtime
        .block_on(async { assert_generation_advanced(&peer.metadata, gen3).await })?;
    assert_eq!(subscriber.last_seen_sequence(), 2);
    assert!(
        peer.runtime
            .block_on(async { peer.metadata.get_inode(3).await })?
            .is_some(),
        "peer should see the second file after the gap-triggered refresh",
    );
    Ok(())
}

#[test]
fn stale_generation_hint_is_ignored_until_fresh_refresh_arrives() -> Result<()> {
    let harness = HostedTestHarness::new("failure-coord-stale")?;
    let peer = harness.open_peer_client("failure-coord-stale-peer")?;
    let endpoint = MockCoordinationEndpoint::spawn(&harness.volume.runtime)?;
    let subscriber = CoordinationSubscriber::new_with_staleness_timeout(
        endpoint.url.clone(),
        peer.metadata.clone(),
        peer.superblock.clone(),
        Duration::from_millis(5),
        Duration::from_millis(5),
        Duration::from_millis(40),
    );

    harness.create_test_volume_with_deltas(1)?;
    let gen2 = harness.volume.superblock.snapshot().generation;
    endpoint.push_generation_hint(gen2, "client-a");
    let events = peer
        .runtime
        .block_on(async { subscriber.poll_once().await })?;
    peer.runtime
        .block_on(async { subscriber.apply_events(events).await })?;
    peer.runtime
        .block_on(async { assert_generation_advanced(&peer.metadata, gen2).await })?;

    harness.create_test_volume_with_deltas(1)?;
    let gen3 = harness.volume.superblock.snapshot().generation;

    endpoint.push_generation_hint(gen2, "stale-client");
    let stale_events = peer
        .runtime
        .block_on(async { subscriber.poll_once().await })?;
    peer.runtime
        .block_on(async { subscriber.apply_events(stale_events).await })?;
    assert_eq!(subscriber.last_applied_generation(), gen2);
    assert!(
        peer.metadata.get_cached_inode(3).is_none(),
        "stale generation hint should not populate the cached newer inode",
    );

    endpoint.push_generation_hint(gen3, "fresh-client");
    let fresh_events = peer
        .runtime
        .block_on(async { subscriber.poll_once().await })?;
    peer.runtime
        .block_on(async { subscriber.apply_events(fresh_events).await })?;
    peer.runtime
        .block_on(async { assert_generation_advanced(&peer.metadata, gen3).await })?;
    assert!(
        peer.runtime
            .block_on(async { peer.metadata.get_inode(3).await })?
            .is_some(),
        "fresh generation hint should refresh the newer inode",
    );
    Ok(())
}

#[test]
fn coordination_watchdog_reconciles_after_silence() -> Result<()> {
    let harness = HostedTestHarness::new("failure-coord-watchdog")?;
    let peer = harness.open_peer_client("failure-coord-watchdog-peer")?;
    let endpoint = MockCoordinationEndpoint::spawn(&harness.volume.runtime)?;
    let subscriber = build_coordination_subscriber_with_staleness_timeout(
        &peer.runtime,
        &endpoint,
        peer.metadata.clone(),
        peer.superblock.clone(),
        5,
        Duration::from_millis(50),
    );

    harness.create_test_volume_with_deltas(1)?;
    let gen2 = harness.volume.superblock.snapshot().generation;

    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline && subscriber.last_applied_generation() < gen2 {
        std::thread::sleep(Duration::from_millis(10));
    }

    let observed_generation = peer.runtime.block_on(async {
        peer.metadata
            .load_superblock()
            .await?
            .context("missing superblock")
            .map(|sb| sb.block.generation)
    })?;
    assert!(
        observed_generation >= 2,
        "watchdog should advance the peer generation"
    );
    assert!(
        subscriber.last_applied_generation() >= 2,
        "watchdog should refresh the subscriber generation"
    );
    assert!(
        peer.runtime
            .block_on(async { peer.metadata.get_inode(2).await })?
            .is_some(),
        "watchdog should refresh the new inode from the object store",
    );

    subscriber.abort();
    Ok(())
}

#[test]
fn relay_timeout_after_commit_retries_with_same_key() -> Result<()> {
    let harness = HostedTestHarness::new("failure-relay-timeout")?;
    let server = MockRelayExecutor::spawn(&harness.volume.runtime, &harness.volume)?;
    let first_request = server.url.clone();
    server.delay_first_response_after_commit(Duration::from_secs(2));

    let client = RelayClient::with_retry_policy(
        first_request,
        Duration::from_millis(750),
        1,
        Duration::from_millis(5),
        None,
    )?;
    let request = build_relay_write_request(&harness.volume, "relay-timeout-client", 1);

    let first_attempt_handle = harness.volume.runtime.handle().spawn({
        let client = client.clone();
        let request = request.clone();
        async move { client.submit_relay_write(request).await }
    });

    while server.commit_count() < 1 {
        std::thread::sleep(Duration::from_millis(10));
    }

    let response = harness
        .volume
        .runtime
        .block_on(async { client.submit_relay_write(request.clone()).await })?;
    assert!(matches!(
        response.status,
        RelayStatus::Committed | RelayStatus::Duplicate
    ));
    assert!(response.committed_generation.unwrap_or(0) >= 2);
    assert_eq!(response.idempotency_key, request.idempotency_key);
    assert_eq!(server.request_count(), 2);
    assert!(harness.volume.superblock.snapshot().generation >= 2);
    let _first_attempt_result = harness
        .volume
        .runtime
        .block_on(async { first_attempt_handle.await.context("join timeout request") })?;
    Ok(())
}

#[test]
fn relay_crash_before_commit_retries_cleanly() -> Result<()> {
    let harness = HostedTestHarness::new("failure-relay-crash")?;
    let server = MockRelayExecutor::spawn(&harness.volume.runtime, &harness.volume)?;
    server.set_available(false);

    let failing_client = RelayClient::with_retry_policy(
        server.url.clone(),
        Duration::from_millis(100),
        0,
        Duration::from_millis(5),
        None,
    )?;
    let request = build_relay_write_request(&harness.volume, "relay-crash-client", 1);

    let first_attempt = harness
        .volume
        .runtime
        .block_on(async { failing_client.submit_relay_write(request.clone()).await });
    assert!(
        first_attempt.is_err(),
        "crash path should fail before commit"
    );
    server.set_available(true);

    let retry_client = RelayClient::with_retry_policy(
        server.url.clone(),
        Duration::from_millis(100),
        1,
        Duration::from_millis(5),
        None,
    )?;
    let response = harness
        .volume
        .runtime
        .block_on(async { retry_client.submit_relay_write(request).await })?;
    assert!(matches!(
        response.status,
        RelayStatus::Committed | RelayStatus::Duplicate
    ));
    assert!(response.committed_generation.unwrap_or(0) >= 2);
    assert!(harness.volume.superblock.snapshot().generation >= 2);
    Ok(())
}

#[test]
fn relay_duplicate_submission_returns_deduped_commit() -> Result<()> {
    let harness = HostedTestHarness::new("failure-relay-duplicate")?;
    let server = MockRelayExecutor::spawn(&harness.volume.runtime, &harness.volume)?;
    let client = RelayClient::with_retry_policy(
        server.url.clone(),
        Duration::from_millis(25),
        1,
        Duration::from_millis(5),
        None,
    )?;
    let request = build_relay_write_request(&harness.volume, "relay-duplicate-client", 1);

    let committed = harness
        .volume
        .runtime
        .block_on(async { client.submit_relay_write(request.clone()).await })?;
    let duplicate = harness
        .volume
        .runtime
        .block_on(async { client.submit_relay_write(request).await })?;

    assert_eq!(committed.status, RelayStatus::Committed);
    assert_eq!(duplicate.status, RelayStatus::Duplicate);
    assert_eq!(
        committed.committed_generation,
        duplicate.committed_generation
    );
    assert_eq!(server.request_count(), 2);
    Ok(())
}

#[test]
fn relay_generation_race_commits_once_and_rejects_stale_request() -> Result<()> {
    let harness = HostedTestHarness::new("failure-relay-race")?;
    let peer = harness.open_peer_client("failure-relay-race-peer")?;
    let server_a = MockRelayExecutor::spawn(&harness.volume.runtime, &harness.volume)?;
    let server_b = MockRelayExecutor::spawn(&peer.runtime, &peer)?;
    server_a.delay_first_request_before_commit(Duration::from_millis(200));

    let client_a = RelayClient::with_retry_policy(
        server_a.url.clone(),
        Duration::from_secs(2),
        0,
        Duration::from_millis(5),
        None,
    )?;
    let client_b = RelayClient::with_retry_policy(
        server_b.url.clone(),
        Duration::from_secs(2),
        0,
        Duration::from_millis(5),
        None,
    )?;
    let request_a = build_relay_write_request(&harness.volume, "relay-race-a", 1);
    let request_b = build_relay_write_request(&peer, "relay-race-b", 1);

    let request_a_handle = harness.volume.runtime.handle().spawn({
        let client_a = client_a.clone();
        let request_a = request_a.clone();
        async move { client_a.submit_relay_write(request_a).await }
    });
    std::thread::sleep(Duration::from_millis(50));
    let result_b = harness
        .volume
        .runtime
        .block_on(async { client_b.submit_relay_write(request_b).await })?;
    let result_a = harness
        .volume
        .runtime
        .block_on(async { request_a_handle.await.context("join race request") })??;

    let statuses = [result_a.status, result_b.status];
    assert!(statuses.contains(&RelayStatus::Committed));
    assert!(statuses.contains(&RelayStatus::Failed));

    let failed = if result_a.status == RelayStatus::Failed {
        result_a
    } else {
        result_b
    };
    assert!(
        failed.actual_generation.unwrap_or(0) >= 2,
        "stale relay response should report a current generation"
    );
    assert!(harness.volume.superblock.snapshot().generation >= 2);
    Ok(())
}

#[test]
fn relay_takeover_after_commit_reconciles_successfully() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let harness = HostedTestHarness::new("failure-relay-takeover")?;
    let server = MockRelayExecutor::spawn(&harness.volume.runtime, &harness.volume)?;
    server.delay_first_response_after_commit(Duration::from_secs(2));

    let initial_generation = harness.volume.superblock.snapshot().generation;
    let expected_generation = initial_generation + 1;

    let client = RelayClient::with_retry_policy(
        server.url.clone(),
        Duration::from_millis(500), // Short timeout to trigger retry
        2,                          // Allow a few retries
        Duration::from_millis(5),
        None,
    )?;
    let request = build_relay_write_request(&harness.volume, "relay-takeover-client", 1);

    // First attempt will commit but timeout
    let first_attempt_handle = harness.volume.runtime.handle().spawn({
        let client = client.clone();
        let request = request.clone();
        async move { client.submit_relay_write(request).await }
    });

    // Wait for commit to happen on server
    while server.commit_count() < 1 {
        std::thread::sleep(Duration::from_millis(10));
    }

    // Now clear dedup store to simulate takeover
    server.clear_dedup();

    // The second attempt (retry from client) should happen now.
    // It should receive RelayStatus::Duplicate because current_gen == expected + 1

    let response = harness
        .volume
        .runtime
        .block_on(async { client.submit_relay_write(request.clone()).await })?;

    assert_eq!(response.status, RelayStatus::Duplicate);
    assert_eq!(response.committed_generation, Some(expected_generation));
    assert_eq!(server.commit_count(), 1); // Should NOT have committed again

    let _ = harness.volume.runtime.block_on(first_attempt_handle)?;

    Ok(())
}
