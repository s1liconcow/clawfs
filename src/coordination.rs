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
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use parking_lot::RwLock;
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

pub struct CoordinationSubscriber {
    endpoint: String,
    client: reqwest::Client,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    poll_interval: Duration,
    reconnect_backoff: Duration,
    health: Arc<RwLock<CoordinationHealth>>,
    last_seen_sequence: Arc<AtomicU64>,
    last_applied_generation: Arc<AtomicU64>,
}

pub struct CoordinationSubscriberHandle {
    join: JoinHandle<()>,
    health: Arc<RwLock<CoordinationHealth>>,
    last_seen_sequence: Arc<AtomicU64>,
    last_applied_generation: Arc<AtomicU64>,
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
}

impl CoordinationSubscriber {
    pub fn new(
        endpoint: String,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        poll_interval: Duration,
        reconnect_backoff: Duration,
    ) -> Self {
        let last_applied_generation = superblock.snapshot().generation;
        Self {
            endpoint,
            client: reqwest::Client::new(),
            metadata,
            superblock,
            poll_interval,
            reconnect_backoff,
            health: Arc::new(RwLock::new(CoordinationHealth::Disconnected)),
            last_seen_sequence: Arc::new(AtomicU64::new(0)),
            last_applied_generation: Arc::new(AtomicU64::new(last_applied_generation)),
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
        let subscriber = Self::new(
            endpoint,
            metadata,
            superblock,
            poll_interval,
            reconnect_backoff,
        );
        let health = subscriber.health.clone();
        let last_seen_sequence = subscriber.last_seen_sequence.clone();
        let last_applied_generation = subscriber.last_applied_generation.clone();
        let join = handle.spawn(async move {
            subscriber.run().await;
        });
        CoordinationSubscriberHandle {
            join,
            health,
            last_seen_sequence,
            last_applied_generation,
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

    async fn run(self) {
        loop {
            match self.poll_once().await {
                Ok(events) => {
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
                    warn!("coordination subscription failed: {err:?}");
                    self.set_health(CoordinationHealth::Disconnected);
                    sleep(self.reconnect_backoff).await;
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

    pub async fn apply_events(&self, events: Vec<CoordinationEvent>) -> Result<()> {
        let mut last_seen = self.last_seen_sequence.load(Ordering::Relaxed);
        let mut needs_refresh = false;
        let mut newest_generation_hint = None;

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
            if let Some(generation) = newest_generation_hint {
                self.last_applied_generation
                    .store(generation, Ordering::Relaxed);
            }
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
}
