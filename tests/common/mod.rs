// Test utilities shared across integration tests.
// Items are conditionally used based on which tests are compiled.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

use clawfs::clawfs::{AcceleratorFallbackPolicy, AcceleratorMode};
use clawfs::compat::{current_gid, current_uid};
use clawfs::config::{Config, ObjectStoreProvider};
use clawfs::coordination::{
    CoordinationEvent, CoordinationEventBatch, CoordinationPayload, CoordinationSubscriber,
    GenerationHint, InvalidationEvent, InvalidationScope,
};
use clawfs::fs::OsageFs;
use clawfs::inode::{FileStorage, InodeRecord, ROOT_INODE};
use clawfs::journal::JournalManager;
use clawfs::metadata::{MetadataStore, create_object_store};
use clawfs::relay::{
    DedupStore, RelayStatus, RelayWriteRequest, RelayWriteResponse, relay_commit_pipeline,
};
use clawfs::segment::{SegmentEntry, SegmentManager, SegmentPayload, segment_prefix};
use clawfs::state::ClientStateManager;
use clawfs::superblock::{CleanupTaskKind, SuperblockManager};

pub struct VolumeHandle {
    pub runtime: Runtime,
    pub config: Config,
    pub metadata: Arc<MetadataStore>,
    pub segments: Arc<SegmentManager>,
    pub superblock: Arc<SuperblockManager>,
    #[allow(dead_code)]
    pub fs: OsageFs,
}

pub struct HostedTestHarness {
    pub tempdir: TempDir,
    pub volume: VolumeHandle,
    delta_counter: AtomicU64,
    #[allow(dead_code)]
    segment_counter: AtomicU64,
}

#[derive(Clone)]
pub struct MockControlPlane {
    pub hosted: clawfs::frontdoor::HostedVolumeConfig,
}

pub struct MockCoordinationEndpoint {
    pub url: String,
    state: Arc<MockCoordinationState>,
    join: tokio::task::JoinHandle<()>,
}

#[allow(dead_code)]
pub struct MockRelayExecutor {
    pub url: String,
    state: Arc<MockRelayState>,
    join: tokio::task::JoinHandle<()>,
}

struct MockCoordinationState {
    available: AtomicBool,
    next_sequence: AtomicU64,
    dropped_events: AtomicU64,
    events: Mutex<Vec<CoordinationEvent>>,
}

#[allow(dead_code)]
struct MockRelayState {
    available: AtomicBool,
    request_count: AtomicU64,
    commit_count: AtomicU64,
    fail_next_before_commit: AtomicBool,
    first_request_precommit_delay_ms: Mutex<Option<Duration>>,
    first_response_delay_ms: Mutex<Option<Duration>>,
    metadata: Arc<MetadataStore>,
    segments: Arc<SegmentManager>,
    superblock: Arc<SuperblockManager>,
    dedup: Arc<DedupStore>,
    shard_size: u64,
}

#[derive(serde::Deserialize)]
struct SinceSequenceQuery {
    since_sequence: Option<u64>,
}

impl HostedTestHarness {
    pub fn new(state_name: &str) -> Result<Self> {
        Self::with_control_plane(state_name, None)
    }

    pub fn with_control_plane(
        state_name: &str,
        control_plane: Option<&MockControlPlane>,
    ) -> Result<Self> {
        let tempdir = tempfile::tempdir().context("create hosted test tempdir")?;
        let mut config = build_config(tempdir.path(), state_name);
        if let Some(control_plane) = control_plane {
            control_plane.apply(&mut config);
        }
        let volume = build_volume_handle(config)?;
        Ok(Self {
            tempdir,
            volume,
            delta_counter: AtomicU64::new(0),
            segment_counter: AtomicU64::new(0),
        })
    }

    pub fn open_peer_client(&self, state_name: &str) -> Result<VolumeHandle> {
        let mut config = self.volume.config.clone();
        let suffix = state_name.trim();
        let sibling_root = self.tempdir.path().join("peer");
        std::fs::create_dir_all(&sibling_root).context("create peer root")?;
        config.mount_path = sibling_root.join("mnt");
        config.store_path = self.tempdir.path().join("store");
        config.local_cache_path = sibling_root.join("cache");
        config.state_path = sibling_root.join(suffix);
        build_volume_handle(config)
    }

    #[allow(dead_code)]
    pub fn inject_lease_expiry(&self, kind: CleanupTaskKind) -> Result<()> {
        self.volume.runtime.block_on(async {
            let current = self
                .volume
                .metadata
                .load_superblock()
                .await?
                .context("missing superblock")?;
            if let Some(lease) = current
                .block
                .cleanup_leases
                .iter()
                .find(|lease| lease.kind == kind)
            {
                self.volume
                    .superblock
                    .complete_cleanup(kind, &lease.client_id)
                    .await?;
            }
            Result::<()>::Ok(())
        })
    }

    pub fn create_test_volume_with_deltas(&self, count: usize) -> Result<()> {
        for _ in 0..count {
            let seq = self.delta_counter.fetch_add(1, Ordering::Relaxed) as usize;
            self.volume.runtime.block_on(async {
                let generation = self.volume.superblock.prepare_dirty_generation()?;
                let inode = self.volume.superblock.reserve_inodes(1).await?;
                let name = format!("delta-{seq:04}.txt");
                let uid = current_uid();
                let gid = current_gid();
                let mut root = self
                    .volume
                    .metadata
                    .get_inode(ROOT_INODE)
                    .await?
                    .context("missing root inode")?;
                let mut file = InodeRecord::new_file(
                    inode,
                    ROOT_INODE,
                    name.clone(),
                    format!("/{}", name),
                    uid,
                    gid,
                );
                let payload = format!("delta payload {seq}\n").into_bytes();
                file.size = payload.len() as u64;
                file.storage = FileStorage::Inline(payload);
                if let Some(children) = root.children_mut() {
                    children.insert(name, inode);
                }
                self.volume
                    .metadata
                    .persist_inode(&file, generation.generation, self.volume.config.shard_size)
                    .await?;
                self.volume
                    .metadata
                    .persist_inode(&root, generation.generation, self.volume.config.shard_size)
                    .await?;
                self.volume
                    .superblock
                    .commit_generation(generation.generation)
                    .await?;
                Result::<()>::Ok(())
            })?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn create_test_volume_with_segments(&self, count: usize) -> Result<()> {
        for _ in 0..count {
            let seq = self.segment_counter.fetch_add(1, Ordering::Relaxed) as usize;
            self.volume.runtime.block_on(async {
                let snapshot = self.volume.superblock.prepare_dirty_generation()?;
                let generation = snapshot.generation;
                let inode = self.volume.superblock.reserve_inodes(1).await?;
                let segment_id = self.volume.superblock.reserve_segments(1).await?;
                let name = format!("segment-{seq:04}.bin");
                let uid = current_uid();
                let gid = current_gid();
                let mut root = self
                    .volume
                    .metadata
                    .get_inode(ROOT_INODE)
                    .await?
                    .context("missing root inode")?;
                let payload = vec![seq as u8; 8192];
                let entries = vec![SegmentEntry {
                    inode,
                    path: format!("/{}", name),
                    logical_offset: 0,
                    payload: SegmentPayload::Bytes(payload.clone()),
                }];
                let extents = self
                    .volume
                    .segments
                    .write_batch(generation, segment_id, entries)?;
                let mut file = InodeRecord::new_file(
                    inode,
                    ROOT_INODE,
                    name.clone(),
                    format!("/{}", name),
                    uid,
                    gid,
                );
                file.size = payload.len() as u64;
                let mut file_extents = Vec::new();
                for (entry_inode, extent) in extents {
                    anyhow::ensure!(entry_inode == inode, "segment extent inode mismatch");
                    file_extents.push(extent);
                }
                file.storage = FileStorage::Segments(file_extents);
                if let Some(children) = root.children_mut() {
                    children.insert(name, inode);
                }
                self.volume
                    .metadata
                    .persist_inode(&file, generation, self.volume.config.shard_size)
                    .await?;
                self.volume
                    .metadata
                    .persist_inode(&root, generation, self.volume.config.shard_size)
                    .await?;
                self.volume.superblock.commit_generation(generation).await?;
                Result::<()>::Ok(())
            })?;
        }
        Ok(())
    }
}

impl MockControlPlane {
    #[allow(dead_code)]
    pub fn direct_plus_cache(
        accelerator_endpoint: impl Into<String>,
        event_endpoint: impl Into<String>,
    ) -> Self {
        Self {
            hosted: clawfs::frontdoor::HostedVolumeConfig {
                provider: ObjectStoreProvider::Local,
                bucket: "managed-test-bucket".to_string(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                storage_mode: Some("hosted_free".to_string()),
                accelerator_endpoint: Some(accelerator_endpoint.into()),
                accelerator_mode: Some(AcceleratorMode::DirectPlusCache),
                accelerator_fallback_policy: Some(AcceleratorFallbackPolicy::PollAndDirect),
                relay_fallback_policy: None,
                event_endpoint: Some(event_endpoint.into()),
                event_settings: Some(clawfs::launch::EventSettings::from_poll_interval_ms(50)),
                accelerator_session_token: Some("test-session-token".to_string()),
                accelerator_session_expiry: None,
                object_prefix: Some("managed/volume".to_string()),
                telemetry_object_prefix: None,
            },
        }
    }

    #[allow(dead_code)]
    pub fn relay_write(accelerator_endpoint: impl Into<String>) -> Self {
        Self {
            hosted: clawfs::frontdoor::HostedVolumeConfig {
                provider: ObjectStoreProvider::Local,
                bucket: "managed-test-bucket".to_string(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
                storage_mode: Some("hosted_free".to_string()),
                accelerator_endpoint: Some(accelerator_endpoint.into()),
                accelerator_mode: Some(AcceleratorMode::RelayWrite),
                accelerator_fallback_policy: Some(AcceleratorFallbackPolicy::FailClosed),
                relay_fallback_policy: Some(clawfs::relay::RelayOutagePolicy::FailClosed),
                event_endpoint: None,
                event_settings: None,
                accelerator_session_token: Some("test-session-token".to_string()),
                accelerator_session_expiry: None,
                object_prefix: Some("managed/volume".to_string()),
                telemetry_object_prefix: None,
            },
        }
    }

    pub fn apply(&self, config: &mut Config) {
        config.disable_cleanup = true;
        config.accelerator_mode = self.hosted.accelerator_mode;
        config.accelerator_endpoint = self.hosted.accelerator_endpoint.clone();
        config.accelerator_fallback_policy = self.hosted.accelerator_fallback_policy;
        config.relay_fallback_policy = self.hosted.relay_fallback_policy;
        if let Some(prefix) = &self.hosted.object_prefix {
            config.object_prefix = prefix.clone();
        }
    }
}

impl MockCoordinationEndpoint {
    pub fn spawn(runtime: &Runtime) -> Result<Self> {
        let state = Arc::new(MockCoordinationState {
            available: AtomicBool::new(true),
            next_sequence: AtomicU64::new(0),
            dropped_events: AtomicU64::new(0),
            events: Mutex::new(Vec::new()),
        });
        let listener = runtime.block_on(async {
            TcpListener::bind("127.0.0.1:0")
                .await
                .context("bind mock coordination endpoint")
        })?;
        let addr = listener
            .local_addr()
            .context("mock coordination local addr")?;
        let app = Router::new()
            .route("/", get(handle_poll))
            .with_state(state.clone());
        let join = runtime.spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            url: format!("http://{}", addr),
            state,
            join,
        })
    }

    #[allow(dead_code)]
    pub fn set_available(&self, available: bool) {
        self.state.available.store(available, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn inject_network_partition(&self, duration: Duration) {
        self.set_available(false);
        let state = self.state.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            state.available.store(true, Ordering::Relaxed);
        });
    }

    #[allow(dead_code)]
    pub fn inject_event_gap(&self, skip_count: usize) {
        self.state
            .dropped_events
            .fetch_add(skip_count as u64, Ordering::Relaxed);
    }

    pub fn push_generation_hint(&self, generation: u64, committer_id: impl Into<String>) -> u64 {
        self.push_event(CoordinationPayload::GenerationHint(GenerationHint {
            volume_prefix: "managed/volume".to_string(),
            generation,
            committer_id: committer_id.into(),
            timestamp: time::OffsetDateTime::now_utc().unix_timestamp(),
        }))
    }

    #[allow(dead_code)]
    pub fn push_invalidation(&self, scope: InvalidationScope, generation: u64) -> u64 {
        self.push_event(CoordinationPayload::InvalidationEvent(InvalidationEvent {
            volume_prefix: "managed/volume".to_string(),
            generation,
            affected_inodes: None,
            scope,
        }))
    }

    fn push_event(&self, payload: CoordinationPayload) -> u64 {
        let sequence = self.state.next_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        if self
            .state
            .dropped_events
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                if remaining > 0 {
                    Some(remaining - 1)
                } else {
                    None
                }
            })
            .is_ok()
        {
            return sequence;
        }
        self.state
            .events
            .lock()
            .push(CoordinationEvent { sequence, payload });
        sequence
    }
}

impl Drop for MockCoordinationEndpoint {
    fn drop(&mut self) {
        self.join.abort();
    }
}

#[allow(dead_code)]
impl MockRelayExecutor {
    pub fn spawn(runtime: &Runtime, volume: &VolumeHandle) -> Result<Self> {
        let state = Arc::new(MockRelayState {
            available: AtomicBool::new(true),
            request_count: AtomicU64::new(0),
            commit_count: AtomicU64::new(0),
            fail_next_before_commit: AtomicBool::new(false),
            first_request_precommit_delay_ms: Mutex::new(None),
            first_response_delay_ms: Mutex::new(None),
            metadata: volume.metadata.clone(),
            segments: volume.segments.clone(),
            superblock: volume.superblock.clone(),
            dedup: DedupStore::new(Duration::from_secs(3600)),
            shard_size: volume.config.shard_size,
        });
        let listener = runtime.block_on(async {
            TcpListener::bind("127.0.0.1:0")
                .await
                .context("bind mock relay executor")
        })?;
        let addr = listener.local_addr().context("mock relay local addr")?;
        let app = Router::new()
            .route("/relay_write", post(handle_relay_write))
            .route("/health", get(handle_relay_health))
            .with_state(state.clone());
        let join = runtime.spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            url: format!("http://{}", addr),
            state,
            join,
        })
    }

    #[allow(dead_code)]
    pub fn inject_network_partition(&self, duration: Duration) {
        self.state.available.store(false, Ordering::Relaxed);
        let state = self.state.clone();
        std::thread::spawn(move || {
            std::thread::sleep(duration);
            state.available.store(true, Ordering::Relaxed);
        });
    }

    #[allow(dead_code)]
    pub fn fail_next_request_before_commit(&self) {
        self.state
            .fail_next_before_commit
            .store(true, Ordering::Relaxed);
    }

    pub fn set_available(&self, available: bool) {
        self.state.available.store(available, Ordering::Relaxed);
    }

    pub fn delay_first_request_before_commit(&self, delay: Duration) {
        *self.state.first_request_precommit_delay_ms.lock() = Some(delay);
    }

    pub fn delay_first_response_after_commit(&self, delay: Duration) {
        *self.state.first_response_delay_ms.lock() = Some(delay);
    }

    pub fn request_count(&self) -> u64 {
        self.state.request_count.load(Ordering::Relaxed)
    }

    pub fn commit_count(&self) -> u64 {
        self.state.commit_count.load(Ordering::Relaxed)
    }
}

impl Drop for MockRelayExecutor {
    fn drop(&mut self) {
        self.join.abort();
    }
}

pub async fn assert_generation_advanced(metadata: &MetadataStore, expected: u64) -> Result<()> {
    let superblock = metadata
        .load_superblock()
        .await?
        .context("missing superblock")?;
    anyhow::ensure!(
        superblock.block.generation == expected,
        "expected generation {}, found {}",
        expected,
        superblock.block.generation
    );
    Ok(())
}

#[allow(dead_code)]
pub async fn wait_for_generation(
    subscriber: &CoordinationSubscriber,
    expected_generation: u64,
    timeout: Duration,
) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if subscriber.last_applied_generation() >= expected_generation {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    anyhow::bail!(
        "timed out waiting for generation {} (last_applied={})",
        expected_generation,
        subscriber.last_applied_generation()
    );
}

async fn handle_poll(
    State(state): State<Arc<MockCoordinationState>>,
    Query(query): Query<SinceSequenceQuery>,
) -> impl IntoResponse {
    if !state.available.load(Ordering::Relaxed) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    let since_sequence = query.since_sequence.unwrap_or(0);
    let events = state
        .events
        .lock()
        .iter()
        .filter(|event| event.sequence > since_sequence)
        .cloned()
        .collect::<Vec<_>>();
    Json(CoordinationEventBatch { events }).into_response()
}

#[allow(dead_code)]
async fn handle_relay_write(
    State(state): State<Arc<MockRelayState>>,
    body: Bytes,
) -> impl IntoResponse {
    let request: RelayWriteRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid relay request payload: {err}"),
            )
                .into_response();
        }
    };
    state.request_count.fetch_add(1, Ordering::Relaxed);
    if !state.available.load(Ordering::Relaxed) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let precommit_delay = if state.request_count.load(Ordering::Relaxed) == 1 {
        state.first_request_precommit_delay_ms.lock().take()
    } else {
        None
    };
    if let Some(delay) = precommit_delay {
        tokio::time::sleep(delay).await;
    }
    if state.fail_next_before_commit.swap(false, Ordering::Relaxed) {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    match relay_commit_pipeline(
        &request,
        &state.metadata,
        &state.segments,
        &state.superblock,
        &state.dedup,
        state.shard_size,
    )
    .await
    {
        Ok(response) => {
            state.commit_count.fetch_add(1, Ordering::Relaxed);
            let first_response_delay = if state.request_count.load(Ordering::Relaxed) == 1 {
                state.first_response_delay_ms.lock().take()
            } else {
                None
            };
            if let Some(delay) = first_response_delay {
                tokio::time::sleep(delay).await;
            }
            let status = match response.status {
                RelayStatus::Failed => StatusCode::CONFLICT,
                _ => StatusCode::OK,
            };
            (status, Json(response)).into_response()
        }
        Err(err) => {
            let response = RelayWriteResponse {
                status: RelayStatus::Failed,
                committed_generation: None,
                idempotency_key: request.idempotency_key,
                error: Some(format!("internal error: {err}")),
                actual_generation: None,
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

#[allow(dead_code)]
async fn handle_relay_health(State(state): State<Arc<MockRelayState>>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "requests": state.request_count.load(Ordering::Relaxed),
        "available": state.available.load(Ordering::Relaxed),
    }))
}

fn build_config(root: &Path, state_name: &str) -> Config {
    let mut config = Config::with_paths(
        root.join("mnt"),
        root.join("store"),
        root.join("cache"),
        root.join(state_name),
    );
    config.inline_compression = false;
    config.segment_compression = false;
    config.inline_threshold = 4096;
    config.shard_size = 8;
    config.inode_batch = 8;
    config.segment_batch = 8;
    config.pending_bytes = 64 * 1024 * 1024;
    config.flush_interval_ms = 0;
    config.lookup_cache_ttl_ms = 0;
    config.dir_cache_ttl_ms = 0;
    config.metadata_poll_interval_ms = 0;
    config.segment_cache_bytes = 0;
    config.disable_journal = true;
    config.disable_cleanup = true;
    config.imap_delta_batch = 8;
    config.entry_ttl_secs = 5;
    config.fuse_threads = 0;
    config.log_file = None;
    config.foreground = true;
    config.object_provider = ObjectStoreProvider::Local;
    config
}

fn build_volume_handle(config: Config) -> Result<VolumeHandle> {
    let runtime = Runtime::new().context("create hosted test runtime")?;
    let handle = runtime.handle().clone();
    let (store, meta_prefix) = create_object_store(&config)?;
    let seg_prefix = segment_prefix(&config.object_prefix);

    let metadata = Arc::new(
        runtime
            .block_on(MetadataStore::new_with_store(
                store.clone(),
                meta_prefix,
                &config,
                handle.clone(),
            ))
            .context("create metadata store")?,
    );
    let segments = Arc::new(
        SegmentManager::new_with_store(store, seg_prefix, &config, handle.clone())
            .context("create segment manager")?,
    );
    let superblock = Arc::new(
        runtime
            .block_on(SuperblockManager::load_or_init(
                metadata.clone(),
                config.shard_size,
            ))
            .context("load or init superblock")?,
    );
    bootstrap_root(&runtime, metadata.clone(), superblock.clone(), &config)?;
    let client_state =
        Arc::new(ClientStateManager::load(&config.state_path).context("load client state")?);
    let journal = if config.disable_journal {
        None
    } else {
        Some(Arc::new(
            JournalManager::new(&config.local_cache_path).context("create journal")?,
        ))
    };
    let fs = OsageFs::new(
        config.clone(),
        metadata.clone(),
        superblock.clone(),
        segments.clone(),
        None,
        journal,
        handle,
        client_state,
        None,
        None,
        None,
        None,
        Some(Arc::new(clawfs::coordination::NoopPublisher)
            as Arc<dyn clawfs::coordination::CoordinationPublisher>),
    );

    Ok(VolumeHandle {
        runtime,
        config,
        metadata,
        segments,
        superblock,
        fs,
    })
}

fn bootstrap_root(
    runtime: &Runtime,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    config: &Config,
) -> Result<()> {
    if runtime.block_on(metadata.get_inode(ROOT_INODE))?.is_some() {
        return Ok(());
    }

    let generation = superblock.prepare_dirty_generation()?.generation;
    let mut root = InodeRecord::new_directory(
        ROOT_INODE,
        ROOT_INODE,
        String::new(),
        "/".to_string(),
        current_uid(),
        current_gid(),
    );
    root.mode = 0o40777;
    if let Err(err) = runtime.block_on(metadata.persist_inode(&root, generation, config.shard_size))
    {
        superblock.abort_generation(generation);
        return Err(err);
    }
    runtime.block_on(superblock.commit_generation(generation))?;
    Ok(())
}

#[allow(dead_code)]
pub fn build_coordination_subscriber(
    runtime: &Runtime,
    endpoint: &MockCoordinationEndpoint,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    poll_interval_ms: u64,
) -> clawfs::coordination::CoordinationSubscriberHandle {
    CoordinationSubscriber::spawn(
        runtime.handle(),
        endpoint.url.clone(),
        metadata,
        superblock,
        Duration::from_millis(poll_interval_ms),
        Duration::from_millis(poll_interval_ms.max(1)),
    )
}

#[allow(dead_code)]
pub fn build_coordination_subscriber_with_staleness_timeout(
    runtime: &Runtime,
    endpoint: &MockCoordinationEndpoint,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    poll_interval_ms: u64,
    staleness_timeout: Duration,
) -> clawfs::coordination::CoordinationSubscriberHandle {
    clawfs::coordination::CoordinationSubscriber::spawn_with_staleness_timeout(
        runtime.handle(),
        endpoint.url.clone(),
        metadata,
        superblock,
        Duration::from_millis(poll_interval_ms),
        Duration::from_millis(poll_interval_ms.max(1)),
        staleness_timeout,
    )
}

#[allow(dead_code)]
pub fn build_relay_write_request(
    volume: &VolumeHandle,
    client_id: impl Into<String>,
    journal_sequence: u64,
) -> RelayWriteRequest {
    RelayWriteRequest::new(
        client_id,
        volume.config.object_prefix.clone(),
        journal_sequence,
        volume.superblock.snapshot().generation,
        Vec::new(),
        Vec::new(),
    )
}
