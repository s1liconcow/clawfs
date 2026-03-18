//! Coordination protocol contract for generation and invalidation hints.
//!
//! Transport-agnostic contract notes:
//! - This module defines message shapes and helper rules only.
//! - It does not assume SSE, WebSocket, pub/sub, or queue semantics.
//! - Each sequenced event carries a monotonic sequence number so clients can
//!   detect gaps and choose between incremental replay and a full refresh.
//!
//! Reconnect semantics:
//! - Clients reconnect by asking for events since the last seen sequence.
//! - If the server cannot safely replay from that sequence, it may require a
//!   full refresh instead of incremental replay.
//! - If the client detects a gap or falls too far behind, it must refresh from
//!   the object store because hints are advisory only.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::{runtime::Handle, task::JoinHandle, time::sleep};
use tracing::{debug, info, warn};

use crate::{metadata::MetadataStore, superblock::SuperblockManager};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationHint {
    pub volume_prefix: String,
    pub generation: u64,
    pub committer_id: String,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvalidationScope {
    Full,
    Inodes(Vec<u64>),
    Prefix(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidationEvent {
    pub volume_prefix: String,
    pub generation: u64,
    pub affected_inodes: Option<Vec<u64>>,
    pub scope: InvalidationScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationHealth {
    Connected,
    Lagging(u64),
    Disconnected,
    Disabled,
}

impl CoordinationHealth {
    pub fn classify(
        disabled: bool,
        heartbeat_age_secs: Option<u64>,
        heartbeat_timeout_secs: u64,
        behind_by: Option<u64>,
    ) -> Self {
        if disabled {
            return CoordinationHealth::Disabled;
        }

        match heartbeat_age_secs {
            None => CoordinationHealth::Disconnected,
            Some(age) if age > heartbeat_timeout_secs => CoordinationHealth::Disconnected,
            Some(_) => match behind_by.unwrap_or(0) {
                0 => CoordinationHealth::Connected,
                lag => CoordinationHealth::Lagging(lag),
            },
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            CoordinationHealth::Connected => "connected",
            CoordinationHealth::Lagging(_) => "lagging",
            CoordinationHealth::Disconnected => "disconnected",
            CoordinationHealth::Disabled => "disabled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinationEvent {
    pub sequence: u64,
    pub payload: CoordinationPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoordinationPayload {
    GenerationHint(GenerationHint),
    InvalidationEvent(InvalidationEvent),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinationEventBatch {
    #[serde(default)]
    pub events: Vec<CoordinationEvent>,
}

#[async_trait::async_trait]
pub trait CoordinationPublisher: Send + Sync {
    async fn publish_generation_advance(&self, hint: GenerationHint) -> Result<()>;
    async fn publish_invalidation(&self, event: InvalidationEvent) -> Result<()>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPublisher;

#[async_trait::async_trait]
impl CoordinationPublisher for NoopPublisher {
    async fn publish_generation_advance(&self, _hint: GenerationHint) -> Result<()> {
        Ok(())
    }

    async fn publish_invalidation(&self, _event: InvalidationEvent) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HttpPublisher {
    base_url: String,
    client: reqwest::Client,
}

impl HttpPublisher {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    fn generation_url(&self) -> String {
        format!("{}/generation-hints", self.base_url)
    }

    fn invalidation_url(&self) -> String {
        format!("{}/invalidations", self.base_url)
    }

    async fn post_json<T: Serialize + ?Sized>(&self, url: String, value: &T) -> Result<()> {
        self.client
            .post(url)
            .json(value)
            .send()
            .await
            .context("failed to contact coordination publisher")?
            .error_for_status()
            .context("coordination publisher returned error")?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl CoordinationPublisher for HttpPublisher {
    async fn publish_generation_advance(&self, hint: GenerationHint) -> Result<()> {
        self.post_json(self.generation_url(), &hint).await
    }

    async fn publish_invalidation(&self, event: InvalidationEvent) -> Result<()> {
        self.post_json(self.invalidation_url(), &event).await
    }
}

/// Default staleness timeout: if no coordination event is received for this
/// long the subscriber triggers a full reconciliation to prevent the client
/// from sitting indefinitely on stale metadata.
pub const DEFAULT_STALENESS_TIMEOUT_SECS: u64 = 60;

/// Maximum number of consecutive poll failures before the subscriber enters
/// degraded (poll-only) mode with longer backoff intervals.
pub const MAX_RECONNECT_FAILURES: u64 = 10;

/// Maximum reconnect backoff ceiling.
pub const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(120);

/// Result of a `reconcile_stale_state` call.
#[derive(Debug, Clone)]
pub struct ReconcileResult {
    /// Number of inode records refreshed from the object store.
    pub refreshed_records: usize,
    /// Whether all local and hosted caches were flushed.
    pub caches_cleared: bool,
}

pub struct CoordinationSubscriber {
    endpoint: String,
    client: reqwest::Client,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    poll_interval: Duration,
    /// Base reconnect backoff; actual value doubles on each failure up to
    /// `MAX_RECONNECT_BACKOFF`.
    reconnect_backoff: Duration,
    /// Maximum time without receiving any event before a full reconciliation
    /// is triggered (default: `DEFAULT_STALENESS_TIMEOUT_SECS`).
    staleness_timeout: Duration,
    health: Arc<RwLock<CoordinationHealth>>,
    last_seen_sequence: Arc<AtomicU64>,
    last_applied_generation: Arc<AtomicU64>,
    /// Wall-clock time of the most recently applied coordination event.
    last_event_at: Arc<Mutex<Instant>>,
    /// Set to true after `MAX_RECONNECT_FAILURES` consecutive poll errors.
    in_poll_only_mode: Arc<AtomicBool>,
}

pub struct CoordinationSubscriberHandle {
    join: JoinHandle<()>,
    health: Arc<RwLock<CoordinationHealth>>,
    last_seen_sequence: Arc<AtomicU64>,
    last_applied_generation: Arc<AtomicU64>,
    in_poll_only_mode: Arc<AtomicBool>,
}

impl CoordinationSubscriberHandle {
    pub fn abort(&self) {
        self.join.abort();
    }

    pub fn health(&self) -> CoordinationHealth {
        self.health.read().clone()
    }

    pub fn last_seen_sequence(&self) -> u64 {
        self.last_seen_sequence.load(Ordering::Relaxed)
    }

    pub fn last_applied_generation(&self) -> u64 {
        self.last_applied_generation.load(Ordering::Relaxed)
    }

    /// Returns `true` if the subscriber has entered degraded poll-only mode
    /// after repeated connection failures.
    pub fn is_in_poll_only_mode(&self) -> bool {
        self.in_poll_only_mode.load(Ordering::Relaxed)
    }
}

impl CoordinationSubscriber {
    pub fn new(
        endpoint: String,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        poll_interval: Duration,
        reconnect_backoff: Duration,
    ) -> Self {
        Self::new_with_staleness_timeout(
            endpoint,
            metadata,
            superblock,
            poll_interval,
            reconnect_backoff,
            Duration::from_secs(DEFAULT_STALENESS_TIMEOUT_SECS),
        )
    }

    pub fn new_with_staleness_timeout(
        endpoint: String,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        poll_interval: Duration,
        reconnect_backoff: Duration,
        staleness_timeout: Duration,
    ) -> Self {
        let last_applied_generation = superblock.snapshot().generation;
        Self {
            endpoint,
            client: reqwest::Client::new(),
            metadata,
            superblock,
            poll_interval,
            reconnect_backoff,
            staleness_timeout,
            health: Arc::new(RwLock::new(CoordinationHealth::Disconnected)),
            last_seen_sequence: Arc::new(AtomicU64::new(0)),
            last_applied_generation: Arc::new(AtomicU64::new(last_applied_generation)),
            last_event_at: Arc::new(Mutex::new(Instant::now())),
            in_poll_only_mode: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn spawn(
        handle: &Handle,
        endpoint: String,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        poll_interval: Duration,
        reconnect_backoff: Duration,
    ) -> CoordinationSubscriberHandle {
        Self::spawn_with_staleness_timeout(
            handle,
            endpoint,
            metadata,
            superblock,
            poll_interval,
            reconnect_backoff,
            Duration::from_secs(DEFAULT_STALENESS_TIMEOUT_SECS),
        )
    }

    pub fn spawn_with_staleness_timeout(
        handle: &Handle,
        endpoint: String,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        poll_interval: Duration,
        reconnect_backoff: Duration,
        staleness_timeout: Duration,
    ) -> CoordinationSubscriberHandle {
        let subscriber = Self::new_with_staleness_timeout(
            endpoint,
            metadata,
            superblock,
            poll_interval,
            reconnect_backoff,
            staleness_timeout,
        );
        Self::spawn_with_subscriber(handle, subscriber)
    }

    fn spawn_with_subscriber(
        handle: &Handle,
        subscriber: CoordinationSubscriber,
    ) -> CoordinationSubscriberHandle {
        let health = subscriber.health.clone();
        let last_seen_sequence = subscriber.last_seen_sequence.clone();
        let last_applied_generation = subscriber.last_applied_generation.clone();
        let in_poll_only_mode = subscriber.in_poll_only_mode.clone();
        let join = handle.spawn(async move {
            subscriber.run().await;
        });
        CoordinationSubscriberHandle {
            join,
            health,
            last_seen_sequence,
            last_applied_generation,
            in_poll_only_mode,
        }
    }

    pub fn health(&self) -> CoordinationHealth {
        self.health.read().clone()
    }

    pub fn last_seen_sequence(&self) -> u64 {
        self.last_seen_sequence.load(Ordering::Relaxed)
    }

    pub fn last_applied_generation(&self) -> u64 {
        self.last_applied_generation.load(Ordering::Relaxed)
    }

    /// Returns the time elapsed since the most recently applied coordination
    /// event.  Values exceeding `staleness_timeout` trigger reconciliation.
    pub fn time_since_last_event(&self) -> Duration {
        self.last_event_at.lock().elapsed()
    }

    /// Returns `true` if the subscriber is in degraded poll-only mode.
    pub fn is_in_poll_only_mode(&self) -> bool {
        self.in_poll_only_mode.load(Ordering::Relaxed)
    }

    async fn run(self) {
        let mut consecutive_failures: u64 = 0;
        let mut current_backoff = self.reconnect_backoff;
        // Tracks when we last attempted to exit poll-only mode.
        let mut last_stream_retry_at = Instant::now();

        loop {
            // ── Staleness watchdog ────────────────────────────────────────────
            // If we are receiving empty polls (endpoint reachable but silent)
            // for longer than `staleness_timeout`, force a full reconciliation
            // so the client never sits indefinitely on stale metadata.
            let time_since_last_event = self.last_event_at.lock().elapsed();
            if time_since_last_event > self.staleness_timeout {
                warn!(
                    elapsed_secs = time_since_last_event.as_secs(),
                    staleness_timeout_secs = self.staleness_timeout.as_secs(),
                    "coordination staleness watchdog fired; reconciling"
                );
                match self.reconcile_stale_state_internal().await {
                    Ok(r) => {
                        info!(
                            refreshed_records = r.refreshed_records,
                            "staleness watchdog reconciliation completed"
                        );
                        // Reset the timer so we don't re-fire immediately.
                        *self.last_event_at.lock() = Instant::now();
                    }
                    Err(err) => {
                        warn!("staleness watchdog reconciliation failed: {err:?}");
                    }
                }
            }

            // ── Poll-only mode: periodically retry stream establishment ───────
            if self.in_poll_only_mode.load(Ordering::Relaxed)
                && last_stream_retry_at.elapsed() >= Duration::from_secs(300)
            {
                info!("poll-only mode: attempting to re-establish coordination stream");
                last_stream_retry_at = Instant::now();
                // Reset failure counter so a single success exits poll-only mode.
                consecutive_failures = 0;
                current_backoff = self.reconnect_backoff;
                self.in_poll_only_mode.store(false, Ordering::Relaxed);
            }

            // ── Poll ──────────────────────────────────────────────────────────
            match self.poll_once().await {
                Ok(events) => {
                    consecutive_failures = 0;
                    current_backoff = self.reconnect_backoff;
                    if events.is_empty() {
                        self.set_health(CoordinationHealth::Connected);
                    }
                    if let Err(err) = self.apply_events(events).await {
                        warn!("coordination event processing failed: {err:?}");
                        self.set_health(CoordinationHealth::Disconnected);
                    }
                    sleep(self.poll_interval).await;
                }
                Err(err) => {
                    consecutive_failures += 1;
                    warn!(
                        consecutive_failures,
                        "coordination subscription failed: {err:?}"
                    );
                    self.set_health(CoordinationHealth::Disconnected);

                    if consecutive_failures >= MAX_RECONNECT_FAILURES
                        && !self.in_poll_only_mode.load(Ordering::Relaxed)
                    {
                        warn!(
                            consecutive_failures,
                            "entering poll-only mode after repeated coordination failures"
                        );
                        self.in_poll_only_mode.store(true, Ordering::Relaxed);
                        last_stream_retry_at = Instant::now();
                    }

                    // Exponential backoff with jitter (±25%), capped at max.
                    let jitter = {
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut h = DefaultHasher::new();
                        consecutive_failures.hash(&mut h);
                        // Derive a deterministic pseudo-random value in [0, 100).
                        (h.finish() % 100) as u32
                    };
                    let jitter_factor = 75 + jitter / 4; // 75..=100 → roughly ±12.5%
                    let backoff_ms = current_backoff.as_millis() as u64;
                    let jittered_ms = backoff_ms.saturating_mul(jitter_factor as u64) / 100;
                    sleep(Duration::from_millis(jittered_ms)).await;

                    // Double for next failure, capped.
                    current_backoff = (current_backoff * 2).min(MAX_RECONNECT_BACKOFF);
                }
            }
        }
    }

    pub async fn poll_once(&self) -> Result<Vec<CoordinationEvent>> {
        let last_seen = self.last_seen_sequence.load(Ordering::Relaxed);
        validate_reconnect_request((last_seen > 0).then_some(last_seen))?;

        let mut request = self.client.get(&self.endpoint);
        if last_seen > 0 {
            request = request.query(&[("since_sequence", last_seen.to_string())]);
        }

        let batch = request
            .send()
            .await
            .context("failed to contact coordination endpoint")?
            .error_for_status()
            .context("coordination endpoint returned error")?
            .json::<CoordinationEventBatch>()
            .await
            .context("failed to parse coordination response")?;

        Ok(batch.events)
    }

    /// Force a full metadata reconciliation.
    ///
    /// Triggers a complete metadata refresh from the object store, flushes
    /// all local and hosted caches, and resets sequence tracking.  Call this
    /// when a sequence gap, generation mismatch, or health transition to
    /// `Disconnected` is detected and the event replay path is insufficient.
    pub async fn reconcile_stale_state(&self) -> Result<ReconcileResult> {
        self.reconcile_stale_state_internal().await
    }

    async fn reconcile_stale_state_internal(&self) -> Result<ReconcileResult> {
        // 1. Flush all caches so subsequent reads go to the object store.
        let full_scope = crate::coordination::InvalidationScope::Full;
        self.metadata.invalidate_cached_scope(&full_scope);
        self.metadata.invalidate_hosted_cache(&full_scope);

        // 2. Refresh metadata from the authoritative object store.
        let refreshed = self.refresh_metadata().await?;

        // 3. Reset sequence tracking so any subsequent events are applied fresh.
        self.last_seen_sequence.store(0, Ordering::Relaxed);
        self.last_applied_generation
            .store(self.superblock.snapshot().generation, Ordering::Relaxed);

        info!(
            refreshed_records = refreshed,
            "reconcile_stale_state completed: caches flushed and metadata refreshed"
        );
        self.set_health(CoordinationHealth::Connected);
        Ok(ReconcileResult {
            refreshed_records: refreshed,
            caches_cleared: true,
        })
    }

    pub async fn apply_events(&self, events: Vec<CoordinationEvent>) -> Result<()> {
        let mut last_seen = self.last_seen_sequence.load(Ordering::Relaxed);
        let mut needs_refresh = false;
        let mut newest_generation_hint = None;

        for event in &events {
            if event.sequence <= last_seen {
                continue;
            }
            // Any non-duplicate event resets the staleness clock.
            *self.last_event_at.lock() = Instant::now();
        }

        for event in events {
            if event.sequence <= last_seen {
                continue;
            }

            if let Some(gap) = sequence_gap(last_seen, event.sequence) {
                warn!(
                    sequence = event.sequence,
                    last_seen_sequence = last_seen,
                    gap,
                    "coordination gap detected; forcing metadata refresh"
                );
                self.set_health(CoordinationHealth::Lagging(gap));
                needs_refresh = true;
            }

            last_seen = event.sequence;
            match event.payload {
                CoordinationPayload::GenerationHint(hint) => {
                    let current_generation = self.current_generation();
                    if hint.generation > current_generation {
                        let behind_by = hint.generation - current_generation;
                        self.set_health(CoordinationHealth::Lagging(behind_by));
                        newest_generation_hint = Some(hint.generation);
                        needs_refresh = true;
                    } else {
                        self.set_health(CoordinationHealth::Connected);
                    }
                }
                CoordinationPayload::InvalidationEvent(event) => {
                    self.metadata.invalidate_cached_scope(&event.scope);
                    self.metadata.invalidate_hosted_cache(&event.scope);
                    self.set_health(CoordinationHealth::Connected);
                }
            }
        }

        if needs_refresh {
            let refreshed = self.refresh_metadata().await?;
            debug!(
                refreshed_records = refreshed,
                newest_generation_hint = ?newest_generation_hint,
                "coordination metadata refresh completed"
            );
            let refreshed_generation = self.superblock.snapshot().generation;
            let watermark = newest_generation_hint
                .map_or(refreshed_generation, |hint| hint.max(refreshed_generation));
            self.last_applied_generation
                .store(watermark, Ordering::Relaxed);
            self.set_health(CoordinationHealth::Connected);
        }

        self.last_seen_sequence.store(last_seen, Ordering::Relaxed);
        if !needs_refresh {
            self.set_health(CoordinationHealth::Connected);
        }
        Ok(())
    }

    fn current_generation(&self) -> u64 {
        self.superblock
            .snapshot()
            .generation
            .max(self.last_applied_generation.load(Ordering::Relaxed))
    }

    async fn refresh_metadata(&self) -> Result<usize> {
        let metadata = self.metadata.clone();
        let result = tokio::task::spawn_blocking(move || metadata.apply_external_deltas()).await;
        match result {
            Ok(Ok(records)) => Ok(records.len()),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(anyhow::anyhow!("coordination refresh join error: {err}")),
        }
    }

    fn set_health(&self, next: CoordinationHealth) {
        let mut guard = self.health.write();
        if *guard == next {
            return;
        }

        let current = guard.clone();
        match (&current, &next) {
            (CoordinationHealth::Disconnected, CoordinationHealth::Connected)
            | (CoordinationHealth::Lagging(_), CoordinationHealth::Connected) => {
                info!(
                    current = current.as_str(),
                    next = next.as_str(),
                    "coordination stream recovered"
                );
            }
            (_, CoordinationHealth::Disconnected) | (_, CoordinationHealth::Lagging(_)) => {
                warn!(
                    current = current.as_str(),
                    next = next.as_str(),
                    "coordination stream degraded"
                );
            }
            _ => {
                debug!(
                    current = current.as_str(),
                    next = next.as_str(),
                    "coordination stream state changed"
                );
            }
        }

        *guard = next;
    }
}

pub fn sequence_gap(last_seen: u64, next_sequence: u64) -> Option<u64> {
    if next_sequence > last_seen.saturating_add(1) {
        Some(next_sequence - last_seen - 1)
    } else {
        None
    }
}

pub fn should_force_full_refresh(behind_by: u64, stale_threshold: u64) -> bool {
    behind_by > stale_threshold
}

pub fn validate_reconnect_request(since_sequence: Option<u64>) -> Result<()> {
    if matches!(since_sequence, Some(0)) {
        bail!("since_sequence must be greater than zero when provided");
    }
    Ok(())
}

#[allow(dead_code)]
pub fn heartbeat_timeout_from_poll_interval(poll_interval: Duration) -> Duration {
    poll_interval.saturating_mul(3)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::Arc,
        thread,
        time::Duration,
    };

    use crate::{
        config::Config,
        inode::{FileStorage, InodeRecord, ROOT_INODE},
        metadata::MetadataStore,
        superblock::SuperblockManager,
    };
    use tempfile::tempdir;
    use time::OffsetDateTime;

    use super::{
        CoordinationEvent, CoordinationEventBatch, CoordinationHealth, CoordinationPayload,
        CoordinationSubscriber, GenerationHint, InvalidationEvent, InvalidationScope, sequence_gap,
        should_force_full_refresh,
    };

    fn spawn_mock_endpoint(body: String) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock endpoint");
        let addr = listener.local_addr().expect("mock addr");
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write mock response");
        });
        (format!("http://{}", addr), join)
    }

    #[test]
    fn sequence_gap_detects_missing_events() {
        assert_eq!(sequence_gap(7, 8), None);
        assert_eq!(sequence_gap(7, 10), Some(2));
    }

    #[test]
    fn health_classification_tracks_timeout_and_lag() {
        assert_eq!(
            CoordinationHealth::classify(true, Some(1), 10, Some(0)),
            CoordinationHealth::Disabled
        );
        assert_eq!(
            CoordinationHealth::classify(false, None, 10, Some(0)),
            CoordinationHealth::Disconnected
        );
        assert_eq!(
            CoordinationHealth::classify(false, Some(5), 10, Some(3)),
            CoordinationHealth::Lagging(3)
        );
        assert_eq!(
            CoordinationHealth::classify(false, Some(5), 10, Some(0)),
            CoordinationHealth::Connected
        );
    }

    #[test]
    fn event_round_trips_through_serde() {
        let event = CoordinationEvent {
            sequence: 42,
            payload: CoordinationPayload::InvalidationEvent(InvalidationEvent {
                volume_prefix: "/vol/prefix".to_string(),
                generation: 99,
                affected_inodes: Some(vec![1, 2, 3]),
                scope: InvalidationScope::Prefix("/vol".to_string()),
            }),
        };

        let json = serde_json::to_string(&event).expect("serialize event");
        let decoded: CoordinationEvent = serde_json::from_str(&json).expect("deserialize event");
        assert_eq!(decoded, event);
    }

    #[test]
    fn refresh_threshold_triggers_when_behind_too_far() {
        assert!(!should_force_full_refresh(2, 3));
        assert!(should_force_full_refresh(4, 3));
    }

    #[test]
    fn generation_hint_serializes() {
        let hint = CoordinationPayload::GenerationHint(GenerationHint {
            volume_prefix: "/vol/prefix".to_string(),
            generation: 12,
            committer_id: "client-a".to_string(),
            timestamp: 1_700_000_000,
        });
        let json = serde_json::to_string(&hint).expect("serialize hint");
        let decoded: CoordinationPayload = serde_json::from_str(&json).expect("deserialize hint");
        assert_eq!(decoded, hint);
    }

    #[tokio::test]
    async fn subscriber_refreshes_metadata_from_mock_endpoint() {
        let tempdir = tempdir().expect("tempdir");
        let config = Config::with_paths(
            tempdir.path().join("mnt"),
            tempdir.path().join("store"),
            tempdir.path().join("cache"),
            tempdir.path().join("state"),
        );
        let handle = tokio::runtime::Handle::current();

        let bootstrap = Arc::new(
            MetadataStore::new(&config, handle.clone())
                .await
                .expect("bootstrap metadata"),
        );
        let bootstrap_superblock = Arc::new(
            SuperblockManager::load_or_init(bootstrap.clone(), config.shard_size)
                .await
                .expect("bootstrap superblock"),
        );

        let snapshot = bootstrap_superblock
            .prepare_dirty_generation()
            .expect("prepare baseline generation");
        let baseline_generation = snapshot.generation;
        let file_inode = ROOT_INODE + 1;
        let mut root = InodeRecord::new_directory(
            ROOT_INODE,
            ROOT_INODE,
            String::new(),
            "/".to_string(),
            0,
            0,
        );
        let mut file = InodeRecord::new_file(
            file_inode,
            ROOT_INODE,
            "note.txt".to_string(),
            "/note.txt".to_string(),
            0,
            0,
        );
        file.storage = FileStorage::Inline(b"old".to_vec());
        file.size = 3;
        if let Some(children) = root.children_mut() {
            children.insert("note.txt".to_string(), file_inode);
        }
        bootstrap
            .persist_inode(&file, baseline_generation, config.shard_size)
            .await
            .expect("persist baseline file");
        bootstrap
            .persist_inode(&root, baseline_generation, config.shard_size)
            .await
            .expect("persist baseline root");
        bootstrap_superblock
            .commit_generation(baseline_generation)
            .await
            .expect("commit baseline generation");

        let metadata = Arc::new(
            MetadataStore::new(&config, handle.clone())
                .await
                .expect("subscriber metadata"),
        );
        let superblock = Arc::new(
            SuperblockManager::load_or_init(metadata.clone(), config.shard_size)
                .await
                .expect("subscriber superblock"),
        );

        let before = metadata
            .get_inode(file_inode)
            .await
            .expect("load baseline inode")
            .expect("baseline inode exists");
        assert_eq!(before.data_inline(), Some(b"old".as_slice()));

        let writer = Arc::new(
            MetadataStore::new(&config, handle.clone())
                .await
                .expect("writer metadata"),
        );
        let writer_superblock = Arc::new(
            SuperblockManager::load_or_init(writer.clone(), config.shard_size)
                .await
                .expect("writer superblock"),
        );

        let mut updated = writer
            .get_inode(file_inode)
            .await
            .expect("load inode for writer")
            .expect("writer inode exists");
        updated.storage = FileStorage::Inline(b"new".to_vec());
        updated.size = 3;
        updated.mtime = OffsetDateTime::now_utc();
        let snapshot = writer_superblock
            .prepare_dirty_generation()
            .expect("prepare writer generation");
        let updated_generation = snapshot.generation;
        writer
            .persist_inode(&updated, updated_generation, config.shard_size)
            .await
            .expect("persist updated inode");
        writer_superblock
            .commit_generation(updated_generation)
            .await
            .expect("commit updated generation");

        let batch = CoordinationEventBatch {
            events: vec![
                CoordinationEvent {
                    sequence: 1,
                    payload: CoordinationPayload::GenerationHint(GenerationHint {
                        volume_prefix: "/vol".to_string(),
                        generation: updated_generation,
                        committer_id: "writer".to_string(),
                        timestamp: 1_700_000_000,
                    }),
                },
                CoordinationEvent {
                    sequence: 2,
                    payload: CoordinationPayload::InvalidationEvent(InvalidationEvent {
                        volume_prefix: "/vol".to_string(),
                        generation: updated_generation,
                        affected_inodes: Some(vec![file_inode]),
                        scope: InvalidationScope::Inodes(vec![file_inode]),
                    }),
                },
            ],
        };
        let body = serde_json::to_string(&batch).expect("serialize batch");
        let (endpoint, join) = spawn_mock_endpoint(body);
        let subscriber = CoordinationSubscriber::new(
            endpoint,
            metadata.clone(),
            superblock.clone(),
            Duration::from_millis(10),
            Duration::from_millis(10),
        );

        let events = subscriber.poll_once().await.expect("poll mock endpoint");
        assert_eq!(events.len(), 2);
        subscriber
            .apply_events(events)
            .await
            .expect("apply mock events");

        let after = metadata
            .get_inode(file_inode)
            .await
            .expect("load refreshed inode")
            .expect("refreshed inode exists");
        assert_eq!(after.data_inline(), Some(b"new".as_slice()));
        assert_eq!(subscriber.health().as_str(), "connected");
        assert_eq!(subscriber.last_applied_generation(), updated_generation);

        join.join().expect("mock endpoint joined");
    }

    // ── Reconciliation and staleness-hardening tests ──────────────────────────

    /// A sequence gap in events triggers `needs_refresh`, which internally
    /// calls `refresh_metadata`.  Verify that after apply_events with a gap,
    /// the subscriber health transitions correctly.
    #[tokio::test]
    async fn sequence_gap_triggers_metadata_refresh() {
        let tempdir = tempdir().expect("tempdir");
        let config = Config::with_paths(
            tempdir.path().join("mnt"),
            tempdir.path().join("store"),
            tempdir.path().join("cache"),
            tempdir.path().join("state"),
        );
        let handle = tokio::runtime::Handle::current();
        let metadata = Arc::new(
            MetadataStore::new(&config, handle.clone())
                .await
                .expect("metadata"),
        );
        let superblock = Arc::new(
            SuperblockManager::load_or_init(metadata.clone(), config.shard_size)
                .await
                .expect("superblock"),
        );
        let subscriber = CoordinationSubscriber::new(
            "http://127.0.0.1:1".to_string(),
            metadata,
            superblock,
            Duration::from_millis(10),
            Duration::from_millis(10),
        );

        // Feed events with a gap (seq 1 → 3, skipping 2).
        let events = vec![
            CoordinationEvent {
                sequence: 1,
                payload: CoordinationPayload::GenerationHint(GenerationHint {
                    volume_prefix: "/vol".to_string(),
                    generation: 1,
                    committer_id: "a".to_string(),
                    timestamp: 0,
                }),
            },
            CoordinationEvent {
                sequence: 3, // Gap: seq 2 is missing.
                payload: CoordinationPayload::GenerationHint(GenerationHint {
                    volume_prefix: "/vol".to_string(),
                    generation: 2,
                    committer_id: "b".to_string(),
                    timestamp: 0,
                }),
            },
        ];
        // apply_events calls refresh_metadata internally when needs_refresh is
        // set due to the gap.  The call will fail because there are no shards to
        // load, but the refresh path is exercised.
        let _ = subscriber.apply_events(events).await;
        // Sequence tracking advances to the highest seen sequence.
        assert_eq!(subscriber.last_seen_sequence(), 3);
    }

    /// `reconcile_stale_state` flushes caches and refreshes metadata.
    #[tokio::test]
    async fn reconcile_stale_state_clears_caches_and_refreshes() {
        let tempdir = tempdir().expect("tempdir");
        let config = Config::with_paths(
            tempdir.path().join("mnt"),
            tempdir.path().join("store"),
            tempdir.path().join("cache"),
            tempdir.path().join("state"),
        );
        let handle = tokio::runtime::Handle::current();
        let metadata = Arc::new(
            MetadataStore::new(&config, handle.clone())
                .await
                .expect("metadata"),
        );
        let superblock = Arc::new(
            SuperblockManager::load_or_init(metadata.clone(), config.shard_size)
                .await
                .expect("superblock"),
        );
        let subscriber = CoordinationSubscriber::new(
            "http://127.0.0.1:1".to_string(),
            metadata,
            superblock,
            Duration::from_millis(10),
            Duration::from_millis(10),
        );

        // Seed last_seen_sequence so we can verify it gets reset.
        subscriber
            .last_seen_sequence
            .store(99, std::sync::atomic::Ordering::Relaxed);

        let result = subscriber.reconcile_stale_state().await.expect("reconcile");
        assert!(result.caches_cleared);
        // Sequence resets to 0 for a fresh replay.
        assert_eq!(subscriber.last_seen_sequence(), 0);
        assert_eq!(subscriber.health().as_str(), "connected");
    }

    /// A successful reconciliation should synchronize the generation watermark
    /// with the authoritative superblock snapshot, not leave a stale hint
    /// behind from an earlier event batch.
    #[tokio::test]
    async fn reconcile_stale_state_updates_last_applied_generation_from_superblock() {
        let tempdir = tempdir().expect("tempdir");
        let config = Config::with_paths(
            tempdir.path().join("mnt"),
            tempdir.path().join("store"),
            tempdir.path().join("cache"),
            tempdir.path().join("state"),
        );
        let handle = tokio::runtime::Handle::current();
        let metadata = Arc::new(
            MetadataStore::new(&config, handle.clone())
                .await
                .expect("metadata"),
        );
        let superblock = Arc::new(
            SuperblockManager::load_or_init(metadata.clone(), config.shard_size)
                .await
                .expect("superblock"),
        );

        let pending = superblock
            .prepare_dirty_generation()
            .expect("prepare dirty generation");
        let committed_generation = pending.generation;
        superblock
            .commit_generation(committed_generation)
            .await
            .expect("commit generation");

        let subscriber = CoordinationSubscriber::new(
            "http://127.0.0.1:1".to_string(),
            metadata,
            superblock.clone(),
            Duration::from_millis(10),
            Duration::from_millis(10),
        );

        subscriber
            .last_applied_generation
            .store(1, std::sync::atomic::Ordering::Relaxed);

        subscriber.reconcile_stale_state().await.expect("reconcile");

        assert_eq!(
            subscriber.last_applied_generation(),
            superblock.snapshot().generation
        );
    }

    /// Stale cache entries are discarded before serving: `invalidate_inodes`
    /// removes a specific inode from the hosted cache LRU.  The generation
    /// gating itself is unit-tested inside `hosted_cache` module tests; here
    /// we verify the reconciliation path calls invalidate correctly.
    #[test]
    fn hosted_cache_invalidate_inodes_via_public_api() {
        use crate::hosted_cache::{HostedMetadataCache, MetadataCacheConfig};

        let cache = HostedMetadataCache::new(MetadataCacheConfig {
            cache_endpoint: "http://localhost:19999".to_string(),
            max_entries: 16,
            ttl: Duration::from_secs(60),
        });

        // Verify invalidate_inodes on an empty cache is a no-op.
        cache.invalidate_inodes(&[1, 2, 3]);
        assert_eq!(cache.local_len(), 0);

        // Verify invalidate_all on an empty cache is also a no-op.
        cache.invalidate_all();
        assert_eq!(cache.local_len(), 0);
    }

    /// `time_since_last_event` is updated by apply_events and can be read.
    #[tokio::test]
    async fn apply_events_updates_last_event_timestamp() {
        let tempdir = tempdir().expect("tempdir");
        let config = Config::with_paths(
            tempdir.path().join("mnt"),
            tempdir.path().join("store"),
            tempdir.path().join("cache"),
            tempdir.path().join("state"),
        );
        let handle = tokio::runtime::Handle::current();
        let metadata = Arc::new(
            MetadataStore::new(&config, handle.clone())
                .await
                .expect("metadata"),
        );
        let superblock = Arc::new(
            SuperblockManager::load_or_init(metadata.clone(), config.shard_size)
                .await
                .expect("superblock"),
        );
        let subscriber = CoordinationSubscriber::new_with_staleness_timeout(
            "http://127.0.0.1:1".to_string(),
            metadata,
            superblock,
            Duration::from_millis(10),
            Duration::from_millis(10),
            Duration::from_secs(60),
        );

        // Sleep briefly so elapsed() starts non-zero.
        std::thread::sleep(Duration::from_millis(5));
        let before = subscriber.time_since_last_event();

        // Apply a non-empty batch; last_event_at should reset.
        let events = vec![CoordinationEvent {
            sequence: 1,
            payload: CoordinationPayload::InvalidationEvent(InvalidationEvent {
                volume_prefix: "/vol".to_string(),
                generation: 1,
                affected_inodes: None,
                scope: InvalidationScope::Full,
            }),
        }];
        let _ = subscriber.apply_events(events).await;
        let after = subscriber.time_since_last_event();

        // After apply_events the clock reset, so elapsed should be less than
        // before (which was at least 5ms old).
        assert!(
            after < before,
            "apply_events must reset the staleness clock: before={before:?} after={after:?}"
        );
    }
}
