//! Relay-write protocol contract.
//!
//! Authority boundaries:
//! - The client remains authoritative for local journal state until it receives
//!   a `Committed` response with a committed generation.
//! - The relay server becomes authoritative for the remote commit once it
//!   accepts the request, uploads objects, and commits the generation.
//! - `Accepted` only means the request has been queued or admitted for work;
//!   it is not safe to clear the local journal on that response.
//!
//! Outage policy and retry semantics:
//! - Idempotency keys are deterministic from client identity, volume prefix,
//!   and journal sequence.
//! - Servers should retain idempotency keys for a bounded window that covers
//!   the retry-after-timeout path and the period needed for committed results
//!   to remain discoverable to retried requests.
//! - Eviction must never violate the rule that a retried request with the same
//!   idempotency key can discover the committed result for the dedup window.
//! - Relay outage policy is explicit: fail closed by default, direct fallback
//!   only with operator opt-in, and queue-and-retry must remain bounded.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use parking_lot::Mutex;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, Notify, Semaphore, mpsc, oneshot};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::clawfs::AcceleratorMode;
use crate::inode::InodeRecord;
use crate::metadata::MetadataStore;
use crate::metadata::fvt;
use crate::segment::{SegmentEntry, SegmentManager};
use crate::superblock::SuperblockManager;

pub const DEFAULT_RELAY_QUEUE_DEPTH: usize = 32;
pub const DEFAULT_RELAY_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_RELAY_MAX_RETRIES: usize = 3;
pub const DEFAULT_RELAY_RETRY_BACKOFF: Duration = Duration::from_millis(250);
pub const DEFAULT_RELAY_WRITE_PATH: &str = "/relay_write";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayStatus {
    Accepted,
    Committed,
    Failed,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayOutagePolicy {
    FailClosed,
    DirectWriteFallback,
    QueueAndRetry,
}

impl RelayOutagePolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FailClosed => "fail_closed",
            Self::DirectWriteFallback => "direct_write_fallback",
            Self::QueueAndRetry => "queue_and_retry",
        }
    }

    pub const fn default_for_mode(mode: AcceleratorMode) -> Self {
        match mode {
            AcceleratorMode::RelayWrite => Self::FailClosed,
            AcceleratorMode::Direct | AcceleratorMode::DirectPlusCache => Self::FailClosed,
        }
    }

    pub const fn normalize_for_mode(self, mode: AcceleratorMode) -> Self {
        match mode {
            AcceleratorMode::RelayWrite => self,
            AcceleratorMode::Direct | AcceleratorMode::DirectPlusCache => Self::FailClosed,
        }
    }

    pub const fn queue_limit(self) -> Option<usize> {
        match self {
            Self::QueueAndRetry => Some(DEFAULT_RELAY_QUEUE_DEPTH),
            _ => None,
        }
    }
}

impl FromStr for RelayOutagePolicy {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "fail_closed" => Ok(Self::FailClosed),
            "direct_write_fallback" => Ok(Self::DirectWriteFallback),
            "queue_and_retry" => Ok(Self::QueueAndRetry),
            other => bail!(
                "unsupported relay outage policy {other:?}; expected fail_closed, direct_write_fallback, or queue_and_retry"
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayOutageAction {
    FailClosed,
    DirectWriteFallback,
    QueueAndRetry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayOutageState {
    Normal,
    Blocked { since: Instant },
    FallingBack { since: Instant },
    Queuing { depth: usize },
}

impl RelayOutageState {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Blocked { .. } => "blocked",
            Self::FallingBack { .. } => "falling_back",
            Self::Queuing { .. } => "queuing",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayOutageDecision {
    pub action: RelayOutageAction,
    pub state: RelayOutageState,
    pub queue_limit: Option<usize>,
}

impl RelayOutagePolicy {
    pub fn decide_on_failure(self, queue_depth: usize, now: Instant) -> RelayOutageDecision {
        match self {
            Self::FailClosed => RelayOutageDecision {
                action: RelayOutageAction::FailClosed,
                state: RelayOutageState::Blocked { since: now },
                queue_limit: None,
            },
            Self::DirectWriteFallback => RelayOutageDecision {
                action: RelayOutageAction::DirectWriteFallback,
                state: RelayOutageState::FallingBack { since: now },
                queue_limit: None,
            },
            Self::QueueAndRetry => {
                let limit = DEFAULT_RELAY_QUEUE_DEPTH;
                if queue_depth < limit {
                    RelayOutageDecision {
                        action: RelayOutageAction::QueueAndRetry,
                        state: RelayOutageState::Queuing {
                            depth: queue_depth + 1,
                        },
                        queue_limit: Some(limit),
                    }
                } else {
                    RelayOutageDecision {
                        action: RelayOutageAction::FailClosed,
                        state: RelayOutageState::Blocked { since: now },
                        queue_limit: Some(limit),
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayWriteRequest {
    pub idempotency_key: String,
    pub client_id: String,
    pub volume_prefix: String,
    pub journal_sequence: u64,
    pub segment_data: Vec<SegmentEntry>,
    pub metadata_deltas: Vec<InodeRecord>,
    pub expected_parent_generation: u64,
}

impl RelayWriteRequest {
    pub fn new(
        client_id: impl Into<String>,
        volume_prefix: impl Into<String>,
        journal_sequence: u64,
        expected_parent_generation: u64,
        segment_data: Vec<SegmentEntry>,
        metadata_deltas: Vec<InodeRecord>,
    ) -> Self {
        let client_id = client_id.into();
        let volume_prefix = volume_prefix.into();
        let idempotency_key =
            Self::derive_idempotency_key(&client_id, &volume_prefix, journal_sequence);
        Self {
            idempotency_key,
            client_id,
            volume_prefix,
            journal_sequence,
            segment_data,
            metadata_deltas,
            expected_parent_generation,
        }
    }

    pub fn derive_idempotency_key(
        client_id: &str,
        volume_prefix: &str,
        journal_sequence: u64,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(client_id.as_bytes());
        hasher.update([0]);
        hasher.update(volume_prefix.as_bytes());
        hasher.update([0]);
        hasher.update(journal_sequence.to_string().as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub fn validate(&self) -> Result<()> {
        let expected = Self::derive_idempotency_key(
            &self.client_id,
            &self.volume_prefix,
            self.journal_sequence,
        );
        if self.idempotency_key != expected {
            bail!(
                "relay write idempotency key mismatch for client={} volume_prefix={} sequence={}",
                self.client_id,
                self.volume_prefix,
                self.journal_sequence
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelayWriteResponse {
    pub status: RelayStatus,
    pub committed_generation: Option<u64>,
    pub idempotency_key: String,
    pub error: Option<String>,
    /// Server's current generation at the time of the response.  Set on
    /// `Failed` responses caused by generation mismatch so that clients can
    /// distinguish "generation advanced past our write" from other failures.
    #[serde(default)]
    pub actual_generation: Option<u64>,
}

impl RelayWriteResponse {
    pub fn validate(&self) -> Result<()> {
        match self.status {
            RelayStatus::Accepted => {
                if self.committed_generation.is_some() {
                    bail!("accepted relay response must not include committed_generation");
                }
            }
            RelayStatus::Committed => {
                if self.committed_generation.is_none() {
                    bail!("committed relay response must include committed_generation");
                }
                if self.error.is_some() {
                    bail!("committed relay response must not include error");
                }
            }
            RelayStatus::Failed => {
                if self.committed_generation.is_some() {
                    bail!("failed relay response must not include committed_generation");
                }
                if self.error.is_none() {
                    bail!("failed relay response must include error");
                }
            }
            RelayStatus::Duplicate => {
                if self.committed_generation.is_none() {
                    bail!("duplicate relay response must include committed_generation");
                }
                if self.error.is_some() {
                    bail!("duplicate relay response must not include error");
                }
            }
        }
        Ok(())
    }
}

/// Decision produced by the client-side relay replay reconciliation check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayReplayDecision {
    /// Proceed with relay retry — the generation is still at or one past the
    /// expected value, so a retry (or dedup hit) can resolve the write.
    Retry,
    /// The remote generation has advanced more than one step past the
    /// `expected_parent_generation`.  This write can never be committed;
    /// the journal should be cleared as permanently stale.
    ClearStale {
        remote_generation: u64,
        expected_parent: u64,
    },
}

/// Determine what the client should do when replaying a pending relay write.
///
/// Called after learning the current remote generation (e.g. from a Failed
/// response or from an explicit superblock fetch at mount time).
///
/// Rules:
/// - If the write has already been committed (`committed_generation.is_some()`),
///   the caller should just clear the journal — this function is not needed.
/// - If `remote_generation > expected_parent_generation + 1`, the generation
///   has advanced past our write by more than one step.  Another client (or a
///   previous attempt) committed a different generation in between.  The write
///   is permanently unrecoverable → `ClearStale`.
/// - Otherwise (`remote_generation <= expected_parent_generation + 1`),
///   a retry may still succeed (exact match) or the dedup store on the server
///   will return a `Duplicate` if our previous attempt was already committed.
pub fn relay_replay_reconcile(
    remote_generation: u64,
    expected_parent_generation: u64,
) -> RelayReplayDecision {
    if remote_generation > expected_parent_generation.saturating_add(1) {
        RelayReplayDecision::ClearStale {
            remote_generation,
            expected_parent: expected_parent_generation,
        }
    } else {
        RelayReplayDecision::Retry
    }
}

#[derive(Debug, Clone)]
pub struct RelayClient {
    base_url: String,
    client: reqwest::Client,
    attempt_timeout: Duration,
    max_retries: usize,
    retry_backoff: Duration,
    session_token: Option<String>,
}

impl RelayClient {
    pub fn new(base_url: impl Into<String>, session_token: Option<String>) -> Result<Self> {
        Self::with_retry_policy(
            base_url,
            DEFAULT_RELAY_ATTEMPT_TIMEOUT,
            DEFAULT_RELAY_MAX_RETRIES,
            DEFAULT_RELAY_RETRY_BACKOFF,
            session_token,
        )
    }

    pub fn with_retry_policy(
        base_url: impl Into<String>,
        attempt_timeout: Duration,
        max_retries: usize,
        retry_backoff: Duration,
        session_token: Option<String>,
    ) -> Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let client = reqwest::Client::builder()
            .timeout(attempt_timeout)
            .build()
            .context("building relay client")?;
        Ok(Self {
            base_url,
            client,
            attempt_timeout,
            max_retries,
            retry_backoff,
            session_token,
        })
    }

    pub async fn submit_relay_write(
        &self,
        request: RelayWriteRequest,
    ) -> Result<RelayWriteResponse> {
        request.validate()?;
        let url = format!("{}{}", self.base_url, DEFAULT_RELAY_WRITE_PATH);
        let mut last_err: Option<anyhow::Error> = None;

        for attempt in 0..=self.max_retries {
            let attempt_no = attempt + 1;
            let mut req_builder = self.client.post(&url).json(&request);
            if let Some(token) = &self.session_token {
                req_builder = req_builder.bearer_auth(token);
            }
            match req_builder.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp
                        .text()
                        .await
                        .context("reading relay write response body")?;
                    let retryable = matches!(
                        status,
                        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
                    ) || status.is_server_error();

                    match serde_json::from_str::<RelayWriteResponse>(&body) {
                        Ok(response) => {
                            response.validate()?;
                            if response.idempotency_key != request.idempotency_key {
                                bail!(
                                    "relay response idempotency_key mismatch: expected {} got {}",
                                    request.idempotency_key,
                                    response.idempotency_key
                                );
                            }

                            match response.status {
                                RelayStatus::Accepted => {
                                    info!(
                                        target: "relay",
                                        idempotency_key = %request.idempotency_key,
                                        "relay_write_accepted"
                                    );
                                }
                                RelayStatus::Committed | RelayStatus::Duplicate => {
                                    info!(
                                        target: "relay",
                                        idempotency_key = %request.idempotency_key,
                                        committed_generation = ?response.committed_generation,
                                        "relay_write_confirmed"
                                    );
                                }
                                RelayStatus::Failed => {
                                    warn!(
                                        target: "relay",
                                        idempotency_key = %request.idempotency_key,
                                        "relay_write_failed"
                                    );
                                }
                            }

                            if status.is_success()
                                || (status == StatusCode::CONFLICT
                                    && matches!(response.status, RelayStatus::Failed))
                            {
                                return Ok(response);
                            }

                            if retryable && attempt < self.max_retries {
                                warn!(
                                    target: "relay",
                                    idempotency_key = %request.idempotency_key,
                                    attempt = attempt_no,
                                    status = %status,
                                    "relay_write_http_retry"
                                );
                                last_err = Some(anyhow::anyhow!(
                                    "relay write http {} on attempt {}: {}",
                                    status,
                                    attempt_no,
                                    body
                                ));
                                sleep(self.retry_backoff).await;
                                continue;
                            }

                            return Ok(response);
                        }
                        Err(parse_err) => {
                            let err = anyhow::anyhow!(
                                "relay write http {} on attempt {}: {}",
                                status,
                                attempt_no,
                                body
                            )
                            .context(parse_err);
                            if retryable && attempt < self.max_retries {
                                warn!(
                                    target: "relay",
                                    idempotency_key = %request.idempotency_key,
                                    attempt = attempt_no,
                                    status = %status,
                                    "relay_write_http_retry"
                                );
                                last_err = Some(err);
                                sleep(self.retry_backoff).await;
                                continue;
                            }
                            return Err(err).context("relay write HTTP failure");
                        }
                    }
                }
                Err(err) => {
                    let wrapped = anyhow::Error::new(err)
                        .context(format!("relay write attempt {} to {}", attempt_no, url));
                    if attempt < self.max_retries {
                        warn!(
                            target: "relay",
                            idempotency_key = %request.idempotency_key,
                            attempt = attempt_no,
                            timeout_ms = self.attempt_timeout.as_millis() as u64,
                            "relay_write_retry"
                        );
                        last_err = Some(wrapped);
                        sleep(self.retry_backoff).await;
                        continue;
                    }
                    return Err(wrapped).context("relay write failed after retries");
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("relay write failed without error")))
    }
}

/// Idempotency dedup store for the relay executor.
///
/// Records committed relay write results keyed by idempotency key.  Entries are
/// evicted after `ttl` so the store stays bounded while remaining discoverable
/// for retried requests within the retry window.
pub struct DedupStore {
    records: Mutex<HashMap<String, DedupRecord>>,
    ttl: Duration,
}

struct DedupRecord {
    committed_generation: u64,
    committed_at: Instant,
}

impl DedupStore {
    pub fn new(ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            records: Mutex::new(HashMap::new()),
            ttl,
        })
    }

    /// Look up a previous committed result.  Returns `None` if the key has
    /// never been committed or if the entry has expired.
    pub fn lookup(&self, key: &str) -> Option<u64> {
        let records = self.records.lock();
        records.get(key).and_then(|r| {
            if r.committed_at.elapsed() < self.ttl {
                Some(r.committed_generation)
            } else {
                None
            }
        })
    }

    /// Record a newly committed result.
    pub fn record(&self, key: String, committed_generation: u64) {
        let mut records = self.records.lock();
        records.insert(
            key,
            DedupRecord {
                committed_generation,
                committed_at: Instant::now(),
            },
        );
    }

    /// Evict all entries older than the TTL.
    pub fn evict_expired(&self) {
        let mut records = self.records.lock();
        let ttl = self.ttl;
        records.retain(|_, r| r.committed_at.elapsed() < ttl);
    }

    pub fn len(&self) -> usize {
        self.records.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all entries (simulating a fresh takeover by a new owner).
    pub fn clear(&self) {
        let mut records = self.records.lock();
        records.clear();
    }
}

// ── Write-back buffer skeleton ─────────────────────────────────────────────

/// Magic bytes for write-back WAL records.
pub const WB_WAL_MAGIC: &[u8; 6] = b"CLWBE1";
/// WAL segment rotation threshold.
pub const WAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// In-flight WAL submission from the HTTP handler to the WAL writer task.
#[allow(dead_code)]
pub struct WalSubmission {
    fb_bytes: Vec<u8>,
    generation: u64,
    ack: oneshot::Sender<std::result::Result<(), io::Error>>,
}

/// One entry buffered in memory (WAL-written, not yet object-store committed).
#[derive(Clone, Debug)]
pub struct WBEntry {
    pub assigned_generation: u64,
    pub request: RelayWriteRequest,
}

/// State protected by an async mutex.
///
/// Held only for generation assignment and queue push, never across I/O.
#[allow(dead_code)]
#[derive(Debug)]
struct WBState {
    logical_gen: u64,
    pending: VecDeque<WBEntry>,
}

/// Errors surfaced by the write-back buffer pipeline.
#[derive(Debug)]
pub enum WriteBackError {
    GenerationMismatch { expected: u64, actual: u64 },
    Duplicate { committed_generation: u64 },
    BufferFull,
    WalWriterGone,
    WalIo(io::Error),
}

impl fmt::Display for WriteBackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationMismatch { expected, actual } => {
                write!(
                    f,
                    "write-back generation mismatch: expected {expected}, actual {actual}"
                )
            }
            Self::Duplicate {
                committed_generation,
            } => {
                write!(
                    f,
                    "write-back duplicate committed at generation {committed_generation}"
                )
            }
            Self::BufferFull => write!(f, "write-back buffer is full"),
            Self::WalWriterGone => write!(f, "write-back WAL writer task is not available"),
            Self::WalIo(err) => write!(f, "write-back WAL I/O error: {err}"),
        }
    }
}

impl std::error::Error for WriteBackError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::WalIo(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for WriteBackError {
    fn from(value: io::Error) -> Self {
        Self::WalIo(value)
    }
}

// ── WAL record codec ──────────────────────────────────────────────────────

/// Serialize a relay write into the FlatBuffer WAL record format.
#[allow(dead_code)]
pub(crate) fn build_wal_record_fb(generation: u64, request: &RelayWriteRequest) -> Vec<u8> {
    let segment_payload =
        bincode::serialize(&request.segment_data).expect("serialize segment_data for WAL record");
    let delta_payload = bincode::serialize(&request.metadata_deltas)
        .expect("serialize metadata_deltas for WAL record");

    let capacity = 512
        + request.idempotency_key.len()
        + request.client_id.len()
        + request.volume_prefix.len()
        + segment_payload.len()
        + delta_payload.len();
    let mut fbb = FlatBufferBuilder::with_capacity(capacity);

    let idempotency_key_wip = fbb.create_string(&request.idempotency_key);
    let client_id_wip = fbb.create_string(&request.client_id);
    let volume_prefix_wip = fbb.create_string(&request.volume_prefix);
    let segment_payload_wip = fbb.create_vector(segment_payload.as_slice());
    let delta_payload_wip = fbb.create_vector(delta_payload.as_slice());

    let start = fbb.start_table();
    fbb.push_slot_always::<u64>(fvt(0), generation);
    fbb.push_slot_always::<WIPOffset<_>>(fvt(1), idempotency_key_wip);
    fbb.push_slot_always::<WIPOffset<_>>(fvt(2), client_id_wip);
    fbb.push_slot_always::<WIPOffset<_>>(fvt(3), volume_prefix_wip);
    fbb.push_slot_always::<u64>(fvt(4), request.journal_sequence);
    fbb.push_slot_always::<u64>(fvt(5), request.expected_parent_generation);
    fbb.push_slot_always::<WIPOffset<_>>(fvt(6), segment_payload_wip);
    fbb.push_slot_always::<WIPOffset<_>>(fvt(7), delta_payload_wip);
    let root = fbb.end_table(start);
    fbb.finish_minimal(root);
    fbb.finished_data().to_vec()
}

/// Decode a relay write-back record from raw FlatBuffer bytes.
#[allow(dead_code)]
pub(crate) fn read_wal_record_fb(bytes: &[u8]) -> Result<WBEntry> {
    let table = unsafe { flatbuffers::root_unchecked::<flatbuffers::Table<'_>>(bytes) };

    let generation = unsafe { table.get::<u64>(fvt(0), Some(0)) }.unwrap_or(0);
    anyhow::ensure!(generation != 0, "corrupt WAL record: generation=0");

    let idempotency_key = unsafe { table.get::<flatbuffers::ForwardsUOffset<&str>>(fvt(1), None) }
        .context("missing WAL record idempotency_key")?
        .to_string();
    let client_id = unsafe { table.get::<flatbuffers::ForwardsUOffset<&str>>(fvt(2), None) }
        .context("missing WAL record client_id")?
        .to_string();
    let volume_prefix = unsafe { table.get::<flatbuffers::ForwardsUOffset<&str>>(fvt(3), None) }
        .context("missing WAL record volume_prefix")?
        .to_string();
    let journal_sequence = unsafe { table.get::<u64>(fvt(4), Some(0)) }.unwrap_or(0);
    let expected_parent_generation = unsafe { table.get::<u64>(fvt(5), Some(0)) }.unwrap_or(0);

    let segment_payload_bytes = unsafe {
        table.get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(fvt(6), None)
    }
    .context("missing WAL record segment_payload")?
    .bytes();
    let delta_payload_bytes = unsafe {
        table.get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(fvt(7), None)
    }
    .context("missing WAL record delta_payload")?
    .bytes();

    let segment_data: Vec<SegmentEntry> = bincode::deserialize(segment_payload_bytes)
        .context("deserializing segment_data from WAL record")?;
    let metadata_deltas: Vec<InodeRecord> = bincode::deserialize(delta_payload_bytes)
        .context("deserializing metadata_deltas from WAL record")?;

    let request = RelayWriteRequest {
        idempotency_key,
        client_id,
        volume_prefix,
        journal_sequence,
        segment_data,
        metadata_deltas,
        expected_parent_generation,
    };

    Ok(WBEntry {
        assigned_generation: generation,
        request,
    })
}

/// List WAL segment files as `(sequence, path)` pairs sorted by sequence.
#[allow(dead_code)]
pub(crate) async fn list_wal_segments(wal_dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut segments = Vec::new();
    let mut dir = match fs::read_dir(wal_dir).await {
        Ok(dir) => dir,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("reading WAL directory {}", wal_dir.display()));
        }
    };

    while let Some(entry) = dir
        .next_entry()
        .await
        .with_context(|| format!("iterating WAL directory {}", wal_dir.display()))?
    {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(hex_seq) = name
            .strip_prefix("seg_")
            .and_then(|rest| rest.strip_suffix(".fb"))
        else {
            continue;
        };
        let Ok(seq) = u64::from_str_radix(hex_seq, 16) else {
            continue;
        };
        segments.push((seq, path));
    }

    segments.sort_by_key(|(seq, _)| *seq);
    Ok(segments)
}

/// Create a new WAL segment and write the magic header.
#[allow(dead_code)]
pub(crate) async fn create_new_wal_segment(wal_dir: &Path, seq: u64) -> Result<File> {
    fs::create_dir_all(wal_dir)
        .await
        .with_context(|| format!("creating WAL directory {}", wal_dir.display()))?;

    let path = wal_dir.join(format!("seg_{seq:016x}.fb"));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .await
        .with_context(|| format!("opening WAL segment {}", path.display()))?;
    file.write_all(WB_WAL_MAGIC)
        .await
        .with_context(|| format!("writing WAL magic to {}", path.display()))?;
    file.sync_data()
        .await
        .with_context(|| format!("syncing WAL magic header {}", path.display()))?;
    Ok(file)
}

/// Open the active WAL segment for append, creating the first one if needed.
#[allow(dead_code)]
pub(crate) async fn open_active_wal_segment(wal_dir: &Path) -> Result<(File, u64, u64)> {
    let segments = list_wal_segments(wal_dir).await?;
    if segments.is_empty() {
        let file = create_new_wal_segment(wal_dir, 1).await?;
        return Ok((file, 1, WB_WAL_MAGIC.len() as u64));
    }

    let (seq, path) = segments
        .last()
        .cloned()
        .expect("segments are non-empty after empty check");
    let file = OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("opening active WAL segment {}", path.display()))?;
    let bytes_written = fs::metadata(&path)
        .await
        .with_context(|| format!("reading WAL segment metadata {}", path.display()))?
        .len();
    Ok((file, seq, bytes_written))
}

/// Recover pending WAL entries from existing segments.
#[allow(dead_code)]
pub(crate) async fn recover_from_wal(
    wal_dir: &Path,
    committed_gen: u64,
) -> Result<(VecDeque<WBEntry>, u64)> {
    let segments = list_wal_segments(wal_dir).await?;
    if segments.is_empty() {
        return Ok((VecDeque::new(), committed_gen));
    }

    let mut pending = VecDeque::new();

    for (seq, path) in segments {
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) => {
                warn!(seq, path = %path.display(), error = %err, "corrupt WAL segment open failed, skipping");
                continue;
            }
        };

        let mut magic = [0u8; WB_WAL_MAGIC.len()];
        if let Err(err) = file.read_exact(&mut magic).await {
            warn!(seq, path = %path.display(), error = %err, "corrupt WAL segment truncated before magic, skipping");
            continue;
        }
        if magic != *WB_WAL_MAGIC {
            warn!(seq, path = %path.display(), "corrupt WAL segment bad magic, skipping");
            continue;
        }

        loop {
            let mut first = [0u8; 1];
            match file.read(&mut first).await {
                Ok(0) => break,
                Ok(1) => {}
                Ok(_) => unreachable!(),
                Err(err) => {
                    warn!(seq, path = %path.display(), error = %err, "truncated WAL length prefix, skipping rest of segment");
                    break;
                }
            }

            let mut len_buf = [0u8; 4];
            len_buf[0] = first[0];
            if let Err(err) = file.read_exact(&mut len_buf[1..]).await {
                warn!(seq, path = %path.display(), error = %err, "truncated WAL length prefix, skipping rest of segment");
                break;
            }

            let len = u32::from_le_bytes(len_buf) as usize;
            let mut record_bytes = vec![0u8; len];
            if let Err(err) = file.read_exact(&mut record_bytes).await {
                warn!(seq, path = %path.display(), error = %err, "truncated WAL record body, skipping rest of segment");
                break;
            }

            match read_wal_record_fb(&record_bytes) {
                Ok(entry) => {
                    if entry.assigned_generation > committed_gen {
                        pending.push_back(entry);
                    }
                }
                Err(err) => {
                    warn!(seq, path = %path.display(), error = %err, "corrupt WAL record, skipping rest of segment");
                    break;
                }
            }
        }
    }

    let mut expected = committed_gen.saturating_add(1);
    let mut truncate_at = pending.len();
    for (idx, entry) in pending.iter().enumerate() {
        if entry.assigned_generation != expected {
            warn!(
                expected,
                found = entry.assigned_generation,
                "WAL generation gap detected during recovery"
            );
            truncate_at = idx;
            break;
        }
        expected = expected.saturating_add(1);
    }
    if truncate_at < pending.len() {
        pending.truncate(truncate_at);
    }

    let highest_gen = pending
        .back()
        .map(|entry| entry.assigned_generation)
        .unwrap_or(committed_gen);
    Ok((pending, highest_gen))
}

fn notify_batch_error(batch: Vec<WalSubmission>, kind: io::ErrorKind, message: String) {
    for sub in batch {
        let _ = sub.ack.send(Err(io::Error::new(kind, message.clone())));
    }
}

/// Drain WAL submissions, batch them, fsync once, and wake the waiters.
pub async fn run_wal_writer(
    mut rx: mpsc::Receiver<WalSubmission>,
    wal_dir: PathBuf,
    segment_bytes: u64,
) {
    let (file, mut current_seq, mut bytes_written) = open_active_wal_segment(&wal_dir)
        .await
        .expect("WAL segment open failed");
    let mut writer = tokio::io::BufWriter::with_capacity(1024 * 1024, file);

    loop {
        let Some(first) = rx.recv().await else {
            if let Err(err) = writer.flush().await {
                error!(error = %err, "WAL writer flush failed on shutdown");
            } else if let Err(err) = writer.get_mut().sync_data().await {
                error!(error = %err, "WAL writer sync_data failed on shutdown");
            }
            break;
        };

        let mut batch = vec![first];
        while let Ok(more) = rx.try_recv() {
            batch.push(more);
        }

        let mut total_bytes = 0u64;
        for sub in &batch {
            let len = sub.fb_bytes.len() as u32;
            if let Err(err) = writer.write_all(&len.to_le_bytes()).await {
                error!(error = %err, "WAL write failed (length prefix)");
                notify_batch_error(
                    batch,
                    io::ErrorKind::BrokenPipe,
                    "WAL write failed".to_string(),
                );
                return;
            }
            if let Err(err) = writer.write_all(&sub.fb_bytes).await {
                error!(error = %err, "WAL write failed (record bytes)");
                notify_batch_error(
                    batch,
                    io::ErrorKind::BrokenPipe,
                    "WAL write failed".to_string(),
                );
                return;
            }
            total_bytes += 4 + sub.fb_bytes.len() as u64;
        }

        if let Err(err) = writer.flush().await {
            error!(error = %err, "WAL BufWriter flush failed");
            notify_batch_error(batch, err.kind(), err.to_string());
            return;
        }
        if let Err(err) = writer.get_mut().sync_data().await {
            error!(error = %err, "WAL sync_data failed");
            notify_batch_error(batch, io::ErrorKind::Other, err.to_string());
            return;
        }

        bytes_written = bytes_written.saturating_add(total_bytes);

        if bytes_written >= segment_bytes {
            let new_seq = current_seq.saturating_add(1);
            match create_new_wal_segment(&wal_dir, new_seq).await {
                Ok(new_file) => {
                    writer = tokio::io::BufWriter::with_capacity(1024 * 1024, new_file);
                    current_seq = new_seq;
                    bytes_written = WB_WAL_MAGIC.len() as u64;
                    info!(seq = new_seq, "WAL segment rotated");
                }
                Err(err) => {
                    error!(error = %err, "WAL segment rotation failed");
                    notify_batch_error(batch, io::ErrorKind::Other, err.to_string());
                    return;
                }
            }
        }

        for sub in batch {
            let _ = sub.ack.send(Ok(()));
        }
    }
}

/// Delete committed WAL segments while keeping the active tail segment.
#[allow(dead_code)]
async fn gc_committed_wal_segments(wal_dir: &Path, committed_gen: u64) -> Result<()> {
    let segments = list_wal_segments(wal_dir).await?;
    if segments.len() <= 1 {
        return Ok(());
    }

    for (seq, path) in segments.iter().take(segments.len() - 1) {
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) => {
                warn!(seq, path = %path.display(), error = %err, "wal_segment_open_failed");
                continue;
            }
        };

        let mut magic = [0u8; WB_WAL_MAGIC.len()];
        if let Err(err) = file.read_exact(&mut magic).await {
            warn!(seq, path = %path.display(), error = %err, "wal_segment_magic_read_failed");
            continue;
        }
        if magic != *WB_WAL_MAGIC {
            warn!(seq, path = %path.display(), "wal_segment_bad_magic");
            continue;
        }

        let mut max_generation = 0u64;
        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    warn!(seq, path = %path.display(), error = %err, "wal_segment_length_read_failed");
                    break;
                }
            }

            let len = u32::from_le_bytes(len_buf) as usize;
            let mut record_bytes = vec![0u8; len];
            match file.read_exact(&mut record_bytes).await {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    warn!(seq, path = %path.display(), error = %err, "wal_segment_record_read_failed");
                    break;
                }
            }

            match read_wal_record_fb(&record_bytes) {
                Ok(entry) => {
                    max_generation = max_generation.max(entry.assigned_generation);
                }
                Err(err) => {
                    warn!(seq, path = %path.display(), error = %err, "wal_segment_record_decode_failed");
                    break;
                }
            }
        }

        if max_generation <= committed_gen {
            match fs::remove_file(&path).await {
                Ok(()) => {
                    info!(seq, path = %path.display(), "wal_segment_deleted");
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    warn!(seq, path = %path.display(), error = %err, "wal_segment_delete_failed");
                }
            }
        }
    }

    Ok(())
}

/// Drain the in-memory pending queue by committing each entry in order.
pub async fn run_flusher(
    buffer: Arc<WriteBackBuffer>,
    meta: Arc<MetadataStore>,
    segments: Arc<SegmentManager>,
    superblock: Arc<SuperblockManager>,
    dedup: Arc<DedupStore>,
    shard_size: u64,
    flush_interval: Duration,
) {
    loop {
        tokio::select! {
            _ = buffer.flush_notify.notified() => {}
            _ = tokio::time::sleep(flush_interval) => {}
        }

        let Some(entry) = ({
            let state = buffer.state.lock().await;
            state.pending.front().cloned()
        }) else {
            continue;
        };

        match relay_commit_pipeline(
            &entry.request,
            &meta,
            &segments,
            &superblock,
            &dedup,
            shard_size,
        )
        .await
        {
            Ok(response) => match response.status {
                RelayStatus::Committed | RelayStatus::Duplicate => {
                    if let Some(committed_generation) = response.committed_generation {
                        buffer
                            .committed_gen
                            .store(committed_generation, Ordering::Release);
                    } else {
                        buffer
                            .committed_gen
                            .store(entry.assigned_generation, Ordering::Release);
                    }

                    {
                        let mut state = buffer.state.lock().await;
                        state.pending.pop_front();
                    }

                    buffer.admit_sem.add_permits(1);

                    if let Err(e) =
                        gc_committed_wal_segments(&buffer.wal_dir, entry.assigned_generation).await
                    {
                        warn!(
                            error = %e,
                            gen = entry.assigned_generation,
                            "wal_gc_failed"
                        );
                    }

                    let has_more = {
                        let state = buffer.state.lock().await;
                        state.pending.front().is_some()
                    };
                    if has_more {
                        buffer.flush_notify.notify_one();
                    }
                }
                RelayStatus::Failed => {
                    error!(
                        gen = entry.assigned_generation,
                        error = ?response.error,
                        actual_gen = ?response.actual_generation,
                        "flusher_halted: relay_required invariant violated - generation mismatch indicates direct write bypass"
                    );
                    buffer.flusher_halted.store(true, Ordering::Release);
                    return;
                }
                RelayStatus::Accepted => {
                    warn!(
                        gen = entry.assigned_generation,
                        "unexpected Accepted from relay_commit_pipeline; retrying"
                    );
                    tokio::time::sleep(flush_interval).await;
                }
            },
            Err(e) => {
                warn!(
                    error = %e,
                    gen = entry.assigned_generation,
                    "flusher_commit_failed; retrying after backoff"
                );
                tokio::time::sleep(flush_interval).await;
            }
        }
    }
}

/// Shared write-back state for relay writes.
#[allow(dead_code)]
#[derive(Debug)]
pub struct WriteBackBuffer {
    state: AsyncMutex<WBState>,
    wal_tx: mpsc::Sender<WalSubmission>,
    flush_notify: Notify,
    admit_sem: Semaphore,
    max_buffer_entries: usize,
    committed_gen: AtomicU64,
    wal_dir: PathBuf,
    flusher_halted: AtomicBool,
}

impl WriteBackBuffer {
    /// Initialize the write-back buffer from the on-disk WAL state.
    #[allow(dead_code)]
    pub(crate) async fn new(
        wal_dir: PathBuf,
        max_buffer_entries: usize,
        committed_gen: u64,
    ) -> Result<(Arc<Self>, mpsc::Receiver<WalSubmission>)> {
        fs::create_dir_all(&wal_dir)
            .await
            .with_context(|| format!("creating WAL dir {}", wal_dir.display()))?;

        let (pending, highest_gen) = recover_from_wal(&wal_dir, committed_gen).await?;
        let logical_gen = highest_gen.saturating_add(1);
        let initial_permits = max_buffer_entries.saturating_sub(pending.len());
        let (wal_tx, wal_rx) = mpsc::channel(max_buffer_entries.saturating_mul(2).max(16));

        let buffer = Arc::new(Self {
            state: AsyncMutex::new(WBState {
                logical_gen,
                pending,
            }),
            wal_tx,
            flush_notify: Notify::new(),
            admit_sem: Semaphore::new(initial_permits),
            max_buffer_entries,
            committed_gen: AtomicU64::new(committed_gen),
            wal_dir,
            flusher_halted: AtomicBool::new(false),
        });

        if !buffer.state.lock().await.pending.is_empty() {
            buffer.flush_notify.notify_one();
        }

        Ok((buffer, wal_rx))
    }

    /// Last generation acknowledged by the object store.
    pub fn committed_gen(&self) -> u64 {
        self.committed_gen.load(Ordering::Acquire)
    }

    /// True when the flusher has halted after a relay-required violation.
    pub fn is_flusher_halted(&self) -> bool {
        self.flusher_halted.load(Ordering::Acquire)
    }

    /// Number of WAL-written entries waiting for object-store commit.
    pub async fn buffer_depth(&self) -> usize {
        self.state.lock().await.pending.len()
    }

    /// Next generation that will be assigned to an incoming request.
    pub async fn logical_gen(&self) -> u64 {
        self.state.lock().await.logical_gen
    }

    /// Number of acknowledged generations that are not yet durable upstream.
    pub async fn flush_lag(&self) -> u64 {
        let logical = self.state.lock().await.logical_gen;
        let committed = self.committed_gen();
        logical.saturating_sub(1).saturating_sub(committed)
    }

    /// Assign the next generation and stage the request in memory.
    #[allow(dead_code)]
    async fn assign_generation(
        &self,
        request: RelayWriteRequest,
        dedup: &Arc<DedupStore>,
    ) -> Result<(WBEntry, Vec<u8>), WriteBackError> {
        if let Some(committed_generation) = dedup.lookup(&request.idempotency_key) {
            return Err(WriteBackError::Duplicate {
                committed_generation,
            });
        }

        let permit = self
            .admit_sem
            .acquire()
            .await
            .map_err(|_| WriteBackError::WalWriterGone)?;
        permit.forget();

        let mut state = self.state.lock().await;
        let last_assigned = state.logical_gen.saturating_sub(1);
        if request.expected_parent_generation != last_assigned {
            self.admit_sem.add_permits(1);
            return Err(WriteBackError::GenerationMismatch {
                expected: request.expected_parent_generation,
                actual: last_assigned,
            });
        }

        let assigned_generation = state.logical_gen;
        state.logical_gen = state.logical_gen.saturating_add(1);

        let fb_bytes = build_wal_record_fb(assigned_generation, &request);
        let entry = WBEntry {
            assigned_generation,
            request,
        };
        state.pending.push_back(entry.clone());

        Ok((entry, fb_bytes))
    }

    async fn assign_generation_and_wal(
        &self,
        request: RelayWriteRequest,
        dedup: &Arc<DedupStore>,
    ) -> Result<u64, WriteBackError> {
        if let Some(committed_generation) = dedup.lookup(&request.idempotency_key) {
            return Err(WriteBackError::Duplicate {
                committed_generation,
            });
        }

        let permit = self
            .admit_sem
            .acquire()
            .await
            .map_err(|_| WriteBackError::WalWriterGone)?;
        permit.forget();

        let mut state = self.state.lock().await;
        let last_assigned = state.logical_gen.saturating_sub(1);
        if request.expected_parent_generation != last_assigned {
            self.admit_sem.add_permits(1);
            return Err(WriteBackError::GenerationMismatch {
                expected: request.expected_parent_generation,
                actual: last_assigned,
            });
        }

        let assigned_generation = state.logical_gen;
        state.logical_gen = state.logical_gen.saturating_add(1);

        let fb_bytes = build_wal_record_fb(assigned_generation, &request);
        let entry = WBEntry {
            assigned_generation,
            request,
        };
        state.pending.push_back(entry);

        let (ack_tx, ack_rx) = oneshot::channel();
        if self
            .wal_tx
            .send(WalSubmission {
                fb_bytes,
                generation: assigned_generation,
                ack: ack_tx,
            })
            .await
            .is_err()
        {
            state.pending.pop_back();
            state.logical_gen = assigned_generation;
            self.admit_sem.add_permits(1);
            return Err(WriteBackError::WalWriterGone);
        }
        drop(state);

        ack_rx
            .await
            .map_err(|_| WriteBackError::WalWriterGone)?
            .map_err(WriteBackError::WalIo)?;

        Ok(assigned_generation)
    }

    /// Public write-back hot path: stage, fsync the WAL, and wake the flusher.
    pub async fn assign_and_buffer(
        &self,
        request: RelayWriteRequest,
        dedup: &Arc<DedupStore>,
    ) -> Result<u64, WriteBackError> {
        let generation = self.assign_generation_and_wal(request, dedup).await?;
        self.flush_notify.notify_one();
        Ok(generation)
    }
}

// ── Relay ownership ─────────────────────────────────────────────────────────

/// TTL for relay ownership leases (seconds).  Matches the cleanup-lease TTL
/// used by the maintenance worker so operators have a single mental model.
pub const RELAY_OWNER_LEASE_TTL_SECS: u64 = 30;

/// Background renewal cadence (seconds).  Set to TTL/3 so a single missed
/// renewal interval does not expire the lease; two consecutive failures cause
/// voluntary release.
pub const RELAY_OWNER_RENEWAL_INTERVAL_SECS: u64 = 10;

/// Object store path suffix for the per-volume relay owner record, relative to
/// the volume prefix.  Kept separate from the superblock to avoid write
/// amplification on the generation-commit path.
pub const RELAY_OWNER_SUFFIX: &str = "metadata/relay_owner.json";

/// Durable per-volume relay ownership record stored at
/// `<volume_prefix>/metadata/relay_owner.json`.
///
/// The record is a separate object from the superblock to avoid contention on
/// the CAS-commit path and to allow ownership to change independently of
/// generation commits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelayOwnerRecord {
    /// Stable identity of the replica that holds ownership.  Must be
    /// consistent across process restarts; operators set this via
    /// `--replica-id`.
    pub replica_id: String,
    /// HTTP URL of this replica's relay endpoint.  Must be reachable from
    /// other replicas in the trusted backend network for request forwarding.
    pub advertise_url: String,
    /// Unix timestamp (seconds) after which this lease is expired and another
    /// replica may take over.
    pub lease_until: u64,
    /// Fencing token incremented on every acquisition (initial or takeover).
    /// Relay commit requests carry the epoch of the owner they were routed to;
    /// a stale-epoch commit is rejected by the current owner.
    pub epoch: u64,
}

impl RelayOwnerRecord {
    /// Returns `true` if the lease is still live at `now_secs` (unix time).
    pub fn is_live_at(&self, now_secs: u64) -> bool {
        self.lease_until > now_secs
    }
}

/// Outcome of `acquire_relay_ownership`.
#[derive(Debug)]
pub enum OwnershipAcquireResult {
    /// Ownership was freshly acquired (initial acquisition or takeover of an
    /// expired lease).  Contains the committed record and its ETag (needed
    /// for `renew_relay_ownership`).
    Acquired {
        record: RelayOwnerRecord,
        etag: String,
    },
    /// We already hold the lease (same `replica_id` in the existing record).
    /// Contains the existing record and its ETag.
    AlreadyOwned {
        record: RelayOwnerRecord,
        etag: String,
    },
    /// A live lease is held by a different replica.  Callers should back off
    /// and retry, or forward the request to `record.advertise_url`.
    OwnedByOther(RelayOwnerRecord),
}

/// Build the object store path for a volume's relay owner record.
pub fn relay_owner_path(volume_prefix: &str) -> object_store::path::Path {
    let trimmed = volume_prefix.trim_matches('/');
    if trimmed.is_empty() {
        object_store::path::Path::from(RELAY_OWNER_SUFFIX)
    } else {
        object_store::path::Path::from(format!("{trimmed}/{RELAY_OWNER_SUFFIX}"))
    }
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn owner_put_payload(record: &RelayOwnerRecord) -> Result<object_store::PutPayload> {
    let b = serde_json::to_vec(record).context("serialize relay owner record")?;
    Ok(object_store::PutPayload::from_bytes(Bytes::from(b)))
}

/// Attempt to acquire relay ownership for a volume.
///
/// Uses conditional object-store puts for atomicity:
/// - `PutMode::Create` (if-not-exists) for the initial acquisition.
/// - `PutMode::Update(etag)` for takeover of an expired lease.
///
/// Stores that do not support conditional puts (e.g. `LocalFileSystem` in
/// tests) fall back to an unconditional put — this loses atomicity guarantees
/// in concurrent environments but is acceptable for local/test use.
///
/// # Returns
///
/// - `Acquired` — freshly obtained (initial or takeover).
/// - `AlreadyOwned` — this `replica_id` already holds the lease.
/// - `OwnedByOther` — another live lease exists; caller should back off.
pub async fn acquire_relay_ownership(
    store: &dyn object_store::ObjectStore,
    volume_prefix: &str,
    replica_id: &str,
    advertise_url: &str,
) -> Result<OwnershipAcquireResult> {
    use object_store::{PutMode, PutOptions, UpdateVersion};

    let path = relay_owner_path(volume_prefix);
    let now = now_unix_secs();

    let new_record = RelayOwnerRecord {
        replica_id: replica_id.to_string(),
        advertise_url: advertise_url.to_string(),
        lease_until: now + RELAY_OWNER_LEASE_TTL_SECS,
        epoch: 0,
    };

    // ── Try initial creation (if-not-exists) ─────────────────────────────
    let create_opts = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    match store
        .put_opts(&path, owner_put_payload(&new_record)?, create_opts)
        .await
    {
        Ok(result) => {
            let etag = result.e_tag.unwrap_or_default();
            info!(
                volume_prefix,
                replica_id,
                epoch = 0,
                "relay_ownership_acquired"
            );
            return Ok(OwnershipAcquireResult::Acquired {
                record: new_record,
                etag,
            });
        }
        Err(object_store::Error::AlreadyExists { .. }) => {
            // Fall through to read-then-conditionally-update path.
        }
        Err(object_store::Error::NotImplemented) => {
            // Non-conditional store (e.g. LocalFileSystem in tests).
            let result = store
                .put(&path, owner_put_payload(&new_record)?)
                .await
                .context("unconditional relay owner put")?;
            let etag = result.e_tag.unwrap_or_default();
            info!(
                volume_prefix,
                replica_id,
                epoch = 0,
                "relay_ownership_acquired_unconditional"
            );
            return Ok(OwnershipAcquireResult::Acquired {
                record: new_record,
                etag,
            });
        }
        Err(e) => return Err(anyhow::Error::from(e).context("relay owner create")),
    }

    // ── Object already exists: read current state ─────────────────────────
    let get_result = store
        .get(&path)
        .await
        .context("read existing relay owner")?;
    let existing_etag = get_result.meta.e_tag.clone().unwrap_or_default();
    let body = get_result.bytes().await.context("read relay owner bytes")?;
    let existing: RelayOwnerRecord =
        serde_json::from_slice(&body).context("parse relay owner record")?;

    // Same replica — already owned.
    if existing.replica_id == replica_id {
        return Ok(OwnershipAcquireResult::AlreadyOwned {
            record: existing,
            etag: existing_etag,
        });
    }

    // Live foreign lease — back off.
    if existing.is_live_at(now_unix_secs()) {
        return Ok(OwnershipAcquireResult::OwnedByOther(existing));
    }

    // ── Expired lease: attempt takeover via conditional update ────────────
    let takeover = RelayOwnerRecord {
        replica_id: replica_id.to_string(),
        advertise_url: advertise_url.to_string(),
        lease_until: now_unix_secs() + RELAY_OWNER_LEASE_TTL_SECS,
        epoch: existing.epoch + 1,
    };
    let update_opts = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: Some(existing_etag),
            version: None,
        }),
        ..Default::default()
    };
    match store
        .put_opts(&path, owner_put_payload(&takeover)?, update_opts)
        .await
    {
        Ok(result) => {
            let etag = result.e_tag.unwrap_or_default();
            info!(
                volume_prefix,
                replica_id,
                epoch = takeover.epoch,
                "relay_ownership_takeover"
            );
            Ok(OwnershipAcquireResult::Acquired {
                record: takeover,
                etag,
            })
        }
        Err(object_store::Error::Precondition { .. }) => {
            // Another replica won the takeover race; treat as OwnedByOther.
            Ok(OwnershipAcquireResult::OwnedByOther(existing))
        }
        Err(object_store::Error::NotImplemented) => {
            let result = store
                .put(&path, owner_put_payload(&takeover)?)
                .await
                .context("unconditional relay owner takeover")?;
            let etag = result.e_tag.unwrap_or_default();
            Ok(OwnershipAcquireResult::Acquired {
                record: takeover,
                etag,
            })
        }
        Err(e) => Err(anyhow::Error::from(e).context("relay owner takeover")),
    }
}

/// Renew an active relay ownership lease.
///
/// Uses `PutMode::Update(etag)` to prevent overwriting a concurrent takeover.
/// Returns the new ETag on success.
///
/// If the update fails with `Precondition` the caller should treat the lease
/// as lost, stop accepting relay commits for the volume, and call
/// `release_relay_ownership` best-effort.
pub async fn renew_relay_ownership(
    store: &dyn object_store::ObjectStore,
    volume_prefix: &str,
    current_etag: &str,
    record: &RelayOwnerRecord,
) -> Result<String> {
    use object_store::{PutMode, PutOptions, UpdateVersion};

    let renewed = RelayOwnerRecord {
        lease_until: now_unix_secs() + RELAY_OWNER_LEASE_TTL_SECS,
        ..record.clone()
    };
    let opts = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: Some(current_etag.to_string()),
            version: None,
        }),
        ..Default::default()
    };
    let result = store
        .put_opts(
            &relay_owner_path(volume_prefix),
            owner_put_payload(&renewed)?,
            opts,
        )
        .await
        .context("relay owner renewal")?;
    Ok(result.e_tag.unwrap_or_default())
}

/// Release relay ownership by deleting the owner record.
///
/// Best-effort: `NotFound` is silently ignored because the lease may have
/// already expired and been overwritten by another replica.
pub async fn release_relay_ownership(
    store: &dyn object_store::ObjectStore,
    volume_prefix: &str,
) -> Result<()> {
    match store.delete(&relay_owner_path(volume_prefix)).await {
        Ok(()) => {
            info!(volume_prefix, "relay_ownership_released");
            Ok(())
        }
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(anyhow::Error::from(e).context("relay owner release")),
    }
}

/// Execute a relay write commit in-region.
///
/// The pipeline:
/// 1. Check `expected_parent_generation` matches current superblock.
/// 2. Check dedup store — return `Duplicate` if already committed.
/// 3. Write segment objects via `SegmentManager`.
/// 4. Persist metadata deltas via `MetadataStore`.
/// 5. CAS-commit the new generation via `SuperblockManager`.
/// 6. Record result in `DedupStore`.
///
/// Failures at any step before step 6 are returned as `Failed`.
/// The commit is atomic via CAS so partial state is never visible.
pub async fn relay_commit_pipeline(
    request: &RelayWriteRequest,
    meta: &Arc<MetadataStore>,
    segments: &Arc<SegmentManager>,
    superblock: &Arc<SuperblockManager>,
    dedup: &Arc<DedupStore>,
    shard_size: u64,
) -> Result<RelayWriteResponse> {
    // --- Idempotency check ---
    if let Some(prior_generation) = dedup.lookup(&request.idempotency_key) {
        info!(
            target: "relay",
            idempotency_key = %request.idempotency_key,
            committed_generation = prior_generation,
            "relay_write_duplicate"
        );
        return Ok(RelayWriteResponse {
            status: RelayStatus::Duplicate,
            committed_generation: Some(prior_generation),
            idempotency_key: request.idempotency_key.clone(),
            error: None,
            actual_generation: None,
        });
    }

    // --- Generation validation ---
    // Reload the in-memory superblock from the object store before validating
    // so that cold-start or post-outage relays have current state.  The reload
    // must happen before the validation check so that both the validation and
    // the subsequent prepare_dirty_generation see the same generation.
    superblock
        .reload()
        .await
        .context("relay superblock reload")?;
    let current = meta
        .load_superblock()
        .await?
        .context("missing superblock for relay commit")?;
    let current_generation = current.block.generation;

    if request.expected_parent_generation != current_generation {
        // If the current generation is exactly one step ahead of the expected
        // generation, this write might have been committed by a previous owner
        // before a takeover (which cleared the in-memory DedupStore).
        //
        // If the superblock's last_idempotency_key matches ours, then it IS a duplicate.
        if current_generation == request.expected_parent_generation.saturating_add(1)
            && current.block.last_idempotency_key.as_ref() == Some(&request.idempotency_key)
        {
            info!(
                target: "relay",
                idempotency_key = %request.idempotency_key,
                current_generation,
                expected = request.expected_parent_generation,
                "relay_write_duplicate_takeover_match"
            );
            return Ok(RelayWriteResponse {
                status: RelayStatus::Duplicate,
                committed_generation: Some(current_generation),
                idempotency_key: request.idempotency_key.clone(),
                error: None,
                actual_generation: Some(current_generation),
            });
        }

        let msg = format!(
            "generation mismatch: expected {} but current is {}",
            request.expected_parent_generation, current_generation
        );
        warn!(
            target: "relay",
            client_id = %request.client_id,
            expected = request.expected_parent_generation,
            actual = current_generation,
            "relay_write_generation_race"
        );
        return Ok(RelayWriteResponse {
            status: RelayStatus::Failed,
            committed_generation: None,
            idempotency_key: request.idempotency_key.clone(),
            error: Some(msg),
            // Provide the server's actual generation so the client can determine
            // whether the write is permanently stale (actual > expected + 1) or
            // might have been committed on a cold-restart dedup-store miss
            // (actual == expected + 1).
            actual_generation: Some(current_generation),
        });
    }

    // --- Prepare a new generation ---
    let new_generation = current_generation.saturating_add(1);
    let snapshot = superblock
        .prepare_dirty_generation()
        .context("relay prepare_dirty_generation")?;
    let prepared_generation = snapshot.generation;
    assert_eq!(
        prepared_generation, new_generation,
        "generation must advance by 1"
    );

    // --- Write segments (if any) ---
    if !request.segment_data.is_empty() {
        let segment_id = current.block.next_segment;
        segments
            .write_batch(new_generation, segment_id, request.segment_data.clone())
            .context("relay segment write_batch")?;
    }

    // --- Persist metadata deltas ---
    if !request.metadata_deltas.is_empty() {
        meta.persist_inodes_batch(&request.metadata_deltas, new_generation, shard_size, 512)
            .await
            .context("relay persist_inodes_batch")?;
        meta.sync_metadata_writes()
            .await
            .context("relay sync_metadata_writes")?;
    }

    // --- CAS-commit the generation ---
    if let Err(err) = superblock
        .commit_generation_idempotent(prepared_generation, Some(request.idempotency_key.clone()))
        .await
    {
        superblock.abort_generation(prepared_generation);
        warn!(
            target: "relay",
            client_id = %request.client_id,
            generation = prepared_generation,
            err = %err,
            "relay_write_commit_failed"
        );
        return Ok(RelayWriteResponse {
            status: RelayStatus::Failed,
            committed_generation: None,
            idempotency_key: request.idempotency_key.clone(),
            error: Some(format!("commit_generation failed: {err}")),
            actual_generation: None,
        });
    }

    // --- Record in dedup store ---
    dedup.record(request.idempotency_key.clone(), new_generation);

    info!(
        target: "relay",
        client_id = %request.client_id,
        committed_generation = new_generation,
        segments = request.segment_data.len(),
        deltas = request.metadata_deltas.len(),
        "relay_write_committed"
    );

    Ok(RelayWriteResponse {
        status: RelayStatus::Committed,
        committed_generation: Some(new_generation),
        idempotency_key: request.idempotency_key.clone(),
        error: None,
        actual_generation: None,
    })
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore as _;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use tokio::runtime::Handle;
    use tokio::runtime::Runtime;

    use super::{
        DEFAULT_RELAY_WRITE_PATH, DedupStore, RelayClient, RelayOutageAction, RelayOutagePolicy,
        RelayOutageState, RelayStatus, RelayWriteRequest, RelayWriteResponse, WAL_SEGMENT_BYTES,
        WB_WAL_MAGIC, WalSubmission, WriteBackBuffer, WriteBackError, build_wal_record_fb,
        create_new_wal_segment, gc_committed_wal_segments, list_wal_segments,
        open_active_wal_segment, read_wal_record_fb, recover_from_wal, run_flusher, run_wal_writer,
    };
    use crate::config::Config;
    use crate::inode::InodeRecord;
    use crate::metadata::MetadataStore;
    use crate::segment::SegmentManager;
    use crate::segment::{SegmentEntry, SegmentPayload};
    use crate::superblock::SuperblockManager;

    fn assert_inode_record_round_trips(expected: &InodeRecord, actual: &InodeRecord) {
        assert_eq!(actual.inode, expected.inode);
        assert_eq!(actual.parent, expected.parent);
        assert_eq!(actual.name, expected.name);
        assert_eq!(actual.path, expected.path);
        assert_eq!(actual.size, expected.size);
        assert_eq!(actual.mode, expected.mode);
        assert_eq!(actual.uid, expected.uid);
        assert_eq!(actual.gid, expected.gid);
        assert_eq!(actual.atime, expected.atime);
        assert_eq!(actual.mtime, expected.mtime);
        assert_eq!(actual.ctime, expected.ctime);
        assert_eq!(actual.link_count, expected.link_count);
        assert_eq!(actual.rdev, expected.rdev);
        match (&expected.storage, &actual.storage) {
            (
                crate::inode::FileStorage::Inline(expected_bytes),
                crate::inode::FileStorage::Inline(actual_bytes),
            ) => {
                assert_eq!(actual_bytes, expected_bytes);
            }
            _ => panic!("expected inline storage to round-trip"),
        }
    }

    fn make_test_request(parent_gen: u64) -> RelayWriteRequest {
        RelayWriteRequest::new("test-client", "test/prefix", 1, parent_gen, vec![], vec![])
    }

    #[test]
    fn idempotency_key_is_deterministic() {
        let a = RelayWriteRequest::derive_idempotency_key("client-a", "/vol/prefix", 42);
        let b = RelayWriteRequest::derive_idempotency_key("client-a", "/vol/prefix", 42);
        let c = RelayWriteRequest::derive_idempotency_key("client-a", "/vol/prefix", 43);

        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn request_round_trips_through_serde() {
        let request = RelayWriteRequest {
            idempotency_key: RelayWriteRequest::derive_idempotency_key(
                "client-a",
                "/vol/prefix",
                42,
            ),
            client_id: "client-a".to_string(),
            volume_prefix: "/vol/prefix".to_string(),
            journal_sequence: 42,
            segment_data: Vec::new(),
            metadata_deltas: vec![InodeRecord::new_file(
                1,
                2,
                "name".to_string(),
                "/name".to_string(),
                1000,
                1000,
            )],
            expected_parent_generation: 7,
        };

        let json = serde_json::to_string(&request).expect("serialize request");
        let decoded: RelayWriteRequest = serde_json::from_str(&json).expect("deserialize request");
        assert_eq!(decoded.idempotency_key, request.idempotency_key);
        assert_eq!(decoded.client_id, request.client_id);
        assert_eq!(decoded.volume_prefix, request.volume_prefix);
        assert_eq!(decoded.journal_sequence, request.journal_sequence);
        assert_eq!(
            decoded.expected_parent_generation,
            request.expected_parent_generation
        );
        assert_eq!(decoded.metadata_deltas.len(), 1);
    }

    #[test]
    fn wal_record_round_trips_through_flatbuffer_codec() {
        let request = RelayWriteRequest::new(
            "client-a",
            "/vol/prefix",
            9,
            11,
            vec![SegmentEntry {
                inode: 1,
                path: "/name".to_string(),
                logical_offset: 0,
                payload: SegmentPayload::Bytes(vec![1, 2, 3, 4]),
            }],
            vec![InodeRecord::new_file(
                1,
                2,
                "name".to_string(),
                "/name".to_string(),
                1000,
                1000,
            )],
        );

        let bytes = build_wal_record_fb(77, &request);
        let entry = read_wal_record_fb(&bytes).expect("decode WAL record");

        assert_eq!(entry.assigned_generation, 77);
        assert_eq!(entry.request.idempotency_key, request.idempotency_key);
        assert_eq!(entry.request.client_id, request.client_id);
        assert_eq!(entry.request.volume_prefix, request.volume_prefix);
        assert_eq!(entry.request.journal_sequence, request.journal_sequence);
        assert_eq!(
            entry.request.expected_parent_generation,
            request.expected_parent_generation
        );
        assert_eq!(entry.request.segment_data.len(), 1);
        assert_eq!(entry.request.metadata_deltas.len(), 1);
        assert_eq!(
            entry.request.segment_data[0].inode,
            request.segment_data[0].inode
        );
        assert_eq!(
            entry.request.segment_data[0].path,
            request.segment_data[0].path
        );
        assert_eq!(
            entry.request.segment_data[0].logical_offset,
            request.segment_data[0].logical_offset
        );
        match (
            &entry.request.segment_data[0].payload,
            &request.segment_data[0].payload,
        ) {
            (SegmentPayload::Bytes(actual), SegmentPayload::Bytes(expected)) => {
                assert_eq!(actual, expected);
            }
            _ => panic!("expected byte payload to round-trip"),
        }
        assert_inode_record_round_trips(
            &request.metadata_deltas[0],
            &entry.request.metadata_deltas[0],
        );
    }

    #[test]
    fn wal_record_round_trips_with_empty_payloads() {
        let request =
            RelayWriteRequest::new("client-1", "orgs/acme/volumes/vol1", 42, 99, vec![], vec![]);

        let fb_bytes = build_wal_record_fb(100, &request);
        assert!(!fb_bytes.is_empty());

        let entry = read_wal_record_fb(&fb_bytes).expect("roundtrip should succeed");
        assert_eq!(entry.assigned_generation, 100);
        assert_eq!(entry.request.client_id, "client-1");
        assert_eq!(entry.request.volume_prefix, "orgs/acme/volumes/vol1");
        assert_eq!(entry.request.journal_sequence, 42);
        assert_eq!(entry.request.expected_parent_generation, 99);
        assert_eq!(entry.request.idempotency_key, request.idempotency_key);
        assert!(entry.request.segment_data.is_empty());
        assert!(entry.request.metadata_deltas.is_empty());
    }

    #[test]
    fn wal_record_rejects_zero_generation() {
        let request = RelayWriteRequest::new("c", "p", 1, 0, vec![], vec![]);
        let fb_bytes = build_wal_record_fb(0, &request);
        assert!(read_wal_record_fb(&fb_bytes).is_err());
    }

    #[test]
    fn wal_segment_helpers_create_list_and_reopen() {
        let rt = Runtime::new().expect("create runtime");
        rt.block_on(async {
            let dir = tempdir().expect("create tempdir");
            let wal_dir = dir.path();

            assert!(
                list_wal_segments(wal_dir)
                    .await
                    .expect("list segments")
                    .is_empty()
            );

            let (mut file, seq, bytes_written) = open_active_wal_segment(wal_dir)
                .await
                .expect("open active segment");
            assert_eq!(seq, 1);
            assert_eq!(bytes_written, WB_WAL_MAGIC.len() as u64);

            file.write_all(b"abc").await.expect("append payload");
            file.sync_data().await.expect("sync payload");

            let segments = list_wal_segments(wal_dir).await.expect("list segments");
            assert_eq!(segments.len(), 1);
            assert_eq!(segments[0].0, 1);

            let (_file2, seq2, bytes2) = open_active_wal_segment(wal_dir)
                .await
                .expect("reopen active segment");
            assert_eq!(seq2, 1);
            assert!(bytes2 >= WB_WAL_MAGIC.len() as u64 + 3);

            let _ = create_new_wal_segment(wal_dir, 2)
                .await
                .expect("create second segment");
            let segments = list_wal_segments(wal_dir).await.expect("list segments");
            assert_eq!(segments.len(), 2);
            assert_eq!(segments[1].0, 2);
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_segment_rotation_lists_active_segments_in_order() {
        let dir = tempdir().expect("create tempdir");
        let wal_dir = dir.path();

        let _ = open_active_wal_segment(wal_dir)
            .await
            .expect("open active segment");
        let _ = create_new_wal_segment(wal_dir, 2)
            .await
            .expect("create second segment");

        let segments = list_wal_segments(wal_dir).await.expect("list segments");
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].0, 1);
        assert_eq!(segments[1].0, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wal_segment_rotation_uses_expected_file_name() {
        let dir = tempdir().expect("create tempdir");
        let wal_dir = dir.path();

        let _ = create_new_wal_segment(wal_dir, 0xDEADBEEF)
            .await
            .expect("create named segment");
        let segments = list_wal_segments(wal_dir).await.expect("list segments");
        let filename = segments[0].1.file_name().unwrap().to_str().unwrap();
        assert_eq!(filename, "seg_00000000deadbeef.fb");
    }

    #[test]
    fn recover_from_wal_skips_committed_and_truncates_gaps() {
        let rt = Runtime::new().expect("create runtime");
        rt.block_on(async {
            let dir = tempdir().expect("create tempdir");
            let wal_dir = dir.path();

            let request = |seq: u64| {
                RelayWriteRequest::new(
                    "client-a",
                    "/vol/prefix",
                    seq,
                    seq.saturating_sub(1),
                    vec![SegmentEntry {
                        inode: seq,
                        path: format!("/name-{seq}"),
                        logical_offset: 0,
                        payload: SegmentPayload::Bytes(vec![seq as u8]),
                    }],
                    vec![InodeRecord::new_file(
                        seq,
                        2,
                        format!("name-{seq}"),
                        format!("/name-{seq}"),
                        1000,
                        1000,
                    )],
                )
            };

            let mut file = create_new_wal_segment(wal_dir, 1)
                .await
                .expect("create segment");
            for generation in [1_u64, 2, 4] {
                let record = build_wal_record_fb(generation, &request(generation));
                let len = (record.len() as u32).to_le_bytes();
                file.write_all(&len).await.expect("write length");
                file.write_all(&record).await.expect("write record");
            }
            file.flush().await.expect("flush wal segment");
            file.sync_data().await.expect("sync wal segment");

            let (pending, highest_gen) = recover_from_wal(wal_dir, 1).await.expect("recover wal");
            assert_eq!(highest_gen, 2);
            assert_eq!(pending.len(), 1);
            let entry = pending.front().expect("pending entry");
            assert_eq!(entry.assigned_generation, 2);
            assert_eq!(entry.request.journal_sequence, 2);
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_finds_pending_entries_after_crash() {
        let dir = tempdir().expect("create tempdir");
        let committed_gen = 5u64;
        let wal_dir = dir.path().to_path_buf();
        let (buf, wal_rx) = WriteBackBuffer::new(wal_dir.clone(), 16, committed_gen)
            .await
            .expect("create buffer");
        let writer_handle =
            tokio::spawn(run_wal_writer(wal_rx, wal_dir.clone(), WAL_SEGMENT_BYTES));
        let dedup = DedupStore::new(Duration::from_secs(3600));

        for parent_gen in committed_gen..(committed_gen + 3) {
            let req = RelayWriteRequest::new("c", "p", parent_gen, parent_gen, vec![], vec![]);
            let assigned_gen = buf
                .assign_and_buffer(req, &dedup)
                .await
                .expect("assign and buffer");
            assert_eq!(assigned_gen, parent_gen + 1);
        }

        assert_eq!(buf.buffer_depth().await, 3);
        drop(buf);
        writer_handle.await.expect("writer task");

        let (buf2, wal_rx2) = WriteBackBuffer::new(wal_dir.clone(), 16, committed_gen)
            .await
            .expect("recover buffer");
        drop(wal_rx2);
        assert_eq!(
            buf2.buffer_depth().await,
            3,
            "should recover 3 pending entries"
        );
        assert_eq!(
            buf2.logical_gen().await,
            committed_gen + 4,
            "logical_gen should be committed_gen + 3 entries + 1"
        );
        assert_eq!(
            buf2.committed_gen(),
            committed_gen,
            "committed_gen starts at superblock.generation"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_skips_committed_entries_after_partial_flush() {
        let dir = tempdir().expect("create tempdir");
        let wal_dir = dir.path().to_path_buf();
        let (buf, wal_rx) = WriteBackBuffer::new(wal_dir.clone(), 16, 1)
            .await
            .expect("create buffer");
        let writer_handle =
            tokio::spawn(run_wal_writer(wal_rx, wal_dir.clone(), WAL_SEGMENT_BYTES));
        let dedup = DedupStore::new(Duration::from_secs(3600));

        for parent_gen in 1u64..6 {
            let req = RelayWriteRequest::new("c", "p", parent_gen, parent_gen, vec![], vec![]);
            buf.assign_and_buffer(req, &dedup)
                .await
                .expect("assign and buffer");
        }
        drop(buf);
        writer_handle.await.expect("writer task");

        let (buf2, wal_rx2) = WriteBackBuffer::new(wal_dir.clone(), 16, 4)
            .await
            .expect("recover buffer");
        drop(wal_rx2);
        assert_eq!(
            buf2.buffer_depth().await,
            2,
            "only gens 5 and 6 should be recovered"
        );
        assert_eq!(buf2.logical_gen().await, 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_handles_truncated_wal_suffix() {
        let dir = tempdir().expect("create tempdir");
        let wal_dir = dir.path().to_path_buf();
        let (buf, wal_rx) = WriteBackBuffer::new(wal_dir.clone(), 16, 1)
            .await
            .expect("create buffer");
        let writer_handle =
            tokio::spawn(run_wal_writer(wal_rx, wal_dir.clone(), WAL_SEGMENT_BYTES));
        let dedup = DedupStore::new(Duration::from_secs(3600));

        let req = RelayWriteRequest::new("c", "p", 1, 1, vec![], vec![]);
        buf.assign_and_buffer(req, &dedup)
            .await
            .expect("assign and buffer");
        drop(buf);
        writer_handle.await.expect("writer task");

        let segs = list_wal_segments(&wal_dir)
            .await
            .expect("list wal segments");
        let mut f = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&segs[0].1)
            .await
            .expect("open wal segment");
        f.write_all(&[0xFF, 0xFF]).await.expect("append garbage");
        drop(f);

        let (buf2, wal_rx2) = WriteBackBuffer::new(wal_dir.clone(), 16, 1)
            .await
            .expect("recover buffer");
        drop(wal_rx2);
        assert_eq!(
            buf2.buffer_depth().await,
            1,
            "one valid entry should be recovered"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gc_committed_wal_segments_deletes_committed_segments_but_keeps_active_tail() {
        let dir = tempdir().expect("create tempdir");
        let wal_dir = dir.path();

        let mut first = create_new_wal_segment(wal_dir, 1)
            .await
            .expect("create first wal segment");
        for generation in [2_u64, 3] {
            let record = build_wal_record_fb(generation, &make_test_request(generation - 1));
            first
                .write_all(&(record.len() as u32).to_le_bytes())
                .await
                .expect("write length");
            first.write_all(&record).await.expect("write record");
        }
        first.flush().await.expect("flush first segment");
        first.sync_data().await.expect("sync first segment");

        let mut second = create_new_wal_segment(wal_dir, 2)
            .await
            .expect("create second wal segment");
        let record = build_wal_record_fb(4, &make_test_request(3));
        second
            .write_all(&(record.len() as u32).to_le_bytes())
            .await
            .expect("write length");
        second.write_all(&record).await.expect("write record");
        second.flush().await.expect("flush second segment");
        second.sync_data().await.expect("sync second segment");

        gc_committed_wal_segments(wal_dir, 3)
            .await
            .expect("gc committed wal segments");

        let segments = list_wal_segments(wal_dir).await.expect("list segments");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].0, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_wal_writer_persists_and_replays_batches() {
        let dir = tempdir().expect("create tempdir");
        let wal_dir = dir.path().to_path_buf();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let writer_handle = tokio::spawn(run_wal_writer(rx, wal_dir.clone(), WAL_SEGMENT_BYTES));

        let mut acks = Vec::new();
        for i in 1u64..=20 {
            let request = RelayWriteRequest::new("client-a", "/vol/prefix", i, i, vec![], vec![]);
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            tx.send(WalSubmission {
                fb_bytes: build_wal_record_fb(i + 1, &request),
                generation: i + 1,
                ack: ack_tx,
            })
            .await
            .expect("send wal submission");
            acks.push(ack_rx);
        }

        for ack_rx in acks {
            assert!(ack_rx.await.expect("batch ack").is_ok());
        }

        drop(tx);
        writer_handle.await.expect("writer task");

        let (pending, highest_gen) = recover_from_wal(&wal_dir, 1).await.expect("recover wal");
        assert_eq!(pending.len(), 20);
        assert_eq!(highest_gen, 21);
        let gens: Vec<u64> = pending
            .iter()
            .map(|entry| entry.assigned_generation)
            .collect();
        let expected: Vec<u64> = (2u64..=21).collect();
        assert_eq!(gens, expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writeback_assign_generation_tracks_state_and_metrics() {
        let dir = tempdir().expect("create tempdir");
        let buffer = WriteBackBuffer::new(dir.path().to_path_buf(), 4, 5)
            .await
            .expect("create buffer")
            .0;
        let dedup = DedupStore::new(Duration::from_secs(3600));

        assert_eq!(buffer.committed_gen(), 5);
        assert!(!buffer.is_flusher_halted());
        assert_eq!(buffer.buffer_depth().await, 0);
        assert_eq!(buffer.logical_gen().await, 6);
        assert_eq!(buffer.flush_lag().await, 0);

        let request = RelayWriteRequest::new("client-a", "/vol/prefix", 1, 5, vec![], vec![]);
        let (entry, fb_bytes) = buffer
            .assign_generation(request, &dedup)
            .await
            .expect("assign generation");

        assert_eq!(entry.assigned_generation, 6);
        assert!(!fb_bytes.is_empty());
        assert_eq!(buffer.buffer_depth().await, 1);
        assert_eq!(buffer.logical_gen().await, 7);
        assert_eq!(buffer.flush_lag().await, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writeback_assign_and_buffer_commits_through_writer() {
        let dir = tempdir().expect("create tempdir");
        let wal_dir = dir.path().to_path_buf();
        let (buffer, wal_rx) = WriteBackBuffer::new(wal_dir.clone(), 4, 1)
            .await
            .expect("create buffer");
        let writer_handle =
            tokio::spawn(run_wal_writer(wal_rx, wal_dir.clone(), WAL_SEGMENT_BYTES));
        let dedup = DedupStore::new(Duration::from_secs(3600));

        let request = RelayWriteRequest::new("client-a", "/vol/prefix", 1, 1, vec![], vec![]);
        let generation = buffer
            .assign_and_buffer(request, &dedup)
            .await
            .expect("assign and buffer");
        assert_eq!(generation, 2);
        assert_eq!(buffer.buffer_depth().await, 1);

        drop(buffer);
        writer_handle.await.expect("writer task");
        let (pending, highest_gen) = recover_from_wal(&wal_dir, 1).await.expect("recover wal");
        assert_eq!(pending.len(), 1);
        assert_eq!(highest_gen, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_flusher_commits_pending_entries_and_releases_backpressure() {
        let dir = tempdir().expect("create tempdir");
        let wal_dir = dir.path().join("wal");
        let store_path = dir.path().join("store");
        let cache_path = dir.path().join("cache");
        let state_path = dir.path().join("state");
        let mount_path = dir.path().join("mnt");
        let config = Config::with_paths(mount_path, store_path, cache_path, state_path);
        let handle = Handle::current();

        let metadata = Arc::new(
            MetadataStore::new(&config, handle.clone())
                .await
                .expect("create metadata store"),
        );
        let segments =
            Arc::new(SegmentManager::new(&config, handle).expect("create segment manager"));
        let superblock = Arc::new(
            SuperblockManager::load_or_init(metadata.clone(), config.shard_size)
                .await
                .expect("create superblock manager"),
        );
        let dedup = DedupStore::new(Duration::from_secs(3600));
        let (buffer, wal_rx) = WriteBackBuffer::new(wal_dir.clone(), 4, 1)
            .await
            .expect("create write-back buffer");
        let writer_handle =
            tokio::spawn(run_wal_writer(wal_rx, wal_dir.clone(), WAL_SEGMENT_BYTES));
        let flusher_handle = tokio::spawn(run_flusher(
            buffer.clone(),
            metadata.clone(),
            segments.clone(),
            superblock.clone(),
            dedup.clone(),
            config.shard_size,
            Duration::from_millis(10),
        ));

        let request = RelayWriteRequest::new("client-a", "/vol/prefix", 1, 1, vec![], vec![]);
        let generation = buffer
            .assign_and_buffer(request, &dedup)
            .await
            .expect("assign and buffer");
        assert_eq!(generation, 2);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if buffer.committed_gen() >= 2 && buffer.buffer_depth().await == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("flusher should commit the pending entry");

        assert_eq!(buffer.committed_gen(), 2);
        assert_eq!(buffer.buffer_depth().await, 0);
        assert_eq!(buffer.flush_lag().await, 0);
        assert_eq!(superblock.snapshot().generation, 2);

        flusher_handle.abort();
        writer_handle.abort();
        let _ = flusher_handle.await;
        let _ = writer_handle.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writeback_assign_generation_rejects_duplicates_and_mismatch() {
        let dir = tempdir().expect("create tempdir");
        let buffer = WriteBackBuffer::new(dir.path().to_path_buf(), 4, 5)
            .await
            .expect("create buffer")
            .0;
        let dedup = DedupStore::new(Duration::from_secs(3600));

        dedup.record("some-idem-key".to_string(), 42);
        let duplicate = RelayWriteRequest {
            idempotency_key: "some-idem-key".to_string(),
            client_id: "c".to_string(),
            volume_prefix: "p".to_string(),
            journal_sequence: 1,
            segment_data: vec![],
            metadata_deltas: vec![],
            expected_parent_generation: 5,
        };
        let err = buffer
            .assign_generation(duplicate, &dedup)
            .await
            .expect_err("duplicate should be rejected");
        assert!(matches!(
            err,
            WriteBackError::Duplicate {
                committed_generation: 42
            }
        ));

        let mismatch = RelayWriteRequest::new("client-a", "/vol/prefix", 2, 3, vec![], vec![]);
        let err = buffer
            .assign_generation(mismatch, &dedup)
            .await
            .expect_err("mismatch should be rejected");
        assert!(matches!(
            err,
            WriteBackError::GenerationMismatch {
                expected: 3,
                actual: 5
            }
        ));
        assert_eq!(buffer.buffer_depth().await, 0);
        assert_eq!(buffer.logical_gen().await, 6);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writeback_assign_generation_applies_backpressure() {
        let dir = tempdir().expect("create tempdir");
        let buffer = WriteBackBuffer::new(dir.path().to_path_buf(), 1, 1)
            .await
            .expect("create buffer")
            .0;
        let dedup = DedupStore::new(Duration::from_secs(3600));

        let first = RelayWriteRequest::new("client-a", "/vol/prefix", 1, 1, vec![], vec![]);
        let (entry, _) = buffer
            .assign_generation(first, &dedup)
            .await
            .expect("first assignment");
        assert_eq!(entry.assigned_generation, 2);

        let second = RelayWriteRequest::new("client-a", "/vol/prefix", 2, 2, vec![], vec![]);
        let timed_out = tokio::time::timeout(
            Duration::from_millis(100),
            buffer.assign_generation(second, &dedup),
        )
        .await;
        assert!(timed_out.is_err(), "buffer should block when full");
    }

    #[test]
    fn request_constructor_derives_idempotency_key() {
        let request = RelayWriteRequest::new(
            "client-a",
            "/vol/prefix",
            9,
            11,
            vec![SegmentEntry {
                inode: 1,
                path: "/name".to_string(),
                logical_offset: 0,
                payload: SegmentPayload::Bytes(vec![1, 2, 3]),
            }],
            vec![InodeRecord::new_file(
                1,
                2,
                "name".to_string(),
                "/name".to_string(),
                1000,
                1000,
            )],
        );

        assert_eq!(
            request.idempotency_key,
            RelayWriteRequest::derive_idempotency_key("client-a", "/vol/prefix", 9)
        );
        request.validate().expect("request key is valid");
    }

    #[test]
    fn response_validation_matches_status_contract() {
        let committed = RelayWriteResponse {
            status: RelayStatus::Committed,
            committed_generation: Some(9),
            idempotency_key: "abc".to_string(),
            error: None,
            actual_generation: None,
        };
        committed.validate().expect("committed response is valid");

        let duplicate = RelayWriteResponse {
            status: RelayStatus::Duplicate,
            committed_generation: Some(9),
            idempotency_key: "abc".to_string(),
            error: None,
            actual_generation: None,
        };
        duplicate.validate().expect("duplicate response is valid");

        let accepted = RelayWriteResponse {
            status: RelayStatus::Accepted,
            committed_generation: None,
            idempotency_key: "abc".to_string(),
            error: None,
            actual_generation: None,
        };
        accepted.validate().expect("accepted response is valid");

        let failed = RelayWriteResponse {
            status: RelayStatus::Failed,
            committed_generation: None,
            idempotency_key: "abc".to_string(),
            error: Some("boom".to_string()),
            actual_generation: None,
        };
        failed.validate().expect("failed response is valid");

        let invalid = RelayWriteResponse {
            status: RelayStatus::Accepted,
            committed_generation: Some(9),
            idempotency_key: "abc".to_string(),
            error: None,
            actual_generation: None,
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn outage_policy_defaults_and_round_trips() {
        let policy = RelayOutagePolicy::QueueAndRetry;
        let json = serde_json::to_string(&policy).expect("serialize policy");
        let decoded: RelayOutagePolicy = serde_json::from_str(&json).expect("deserialize policy");
        assert_eq!(decoded, policy);
        assert_eq!(
            RelayOutagePolicy::default_for_mode(crate::clawfs::AcceleratorMode::RelayWrite),
            RelayOutagePolicy::FailClosed
        );
        assert_eq!(
            RelayOutagePolicy::default_for_mode(crate::clawfs::AcceleratorMode::Direct),
            RelayOutagePolicy::FailClosed
        );
    }

    #[test]
    fn outage_policy_decision_is_bounded() {
        let now = Instant::now();

        let fail_closed = RelayOutagePolicy::FailClosed.decide_on_failure(0, now);
        assert_eq!(fail_closed.action, RelayOutageAction::FailClosed);
        assert!(matches!(
            fail_closed.state,
            RelayOutageState::Blocked { .. }
        ));
        assert_eq!(fail_closed.queue_limit, None);

        let fallback = RelayOutagePolicy::DirectWriteFallback.decide_on_failure(0, now);
        assert_eq!(fallback.action, RelayOutageAction::DirectWriteFallback);
        assert!(matches!(
            fallback.state,
            RelayOutageState::FallingBack { .. }
        ));

        let queued = RelayOutagePolicy::QueueAndRetry.decide_on_failure(0, now);
        assert_eq!(queued.action, RelayOutageAction::QueueAndRetry);
        assert!(matches!(
            queued.state,
            RelayOutageState::Queuing { depth: 1 }
        ));
        assert_eq!(queued.queue_limit, Some(super::DEFAULT_RELAY_QUEUE_DEPTH));

        let saturated = RelayOutagePolicy::QueueAndRetry
            .decide_on_failure(super::DEFAULT_RELAY_QUEUE_DEPTH, now);
        assert_eq!(saturated.action, RelayOutageAction::FailClosed);
        assert!(matches!(saturated.state, RelayOutageState::Blocked { .. }));
        assert_eq!(
            saturated.queue_limit,
            Some(super::DEFAULT_RELAY_QUEUE_DEPTH)
        );
    }

    #[test]
    fn idempotency_key_differs_by_client_and_volume() {
        let key_a = RelayWriteRequest::derive_idempotency_key("client-a", "/vol/prefix", 1);
        let key_b = RelayWriteRequest::derive_idempotency_key("client-b", "/vol/prefix", 1);
        let key_c = RelayWriteRequest::derive_idempotency_key("client-a", "/other/prefix", 1);

        assert_ne!(
            key_a, key_b,
            "different client_ids must produce different keys"
        );
        assert_ne!(
            key_a, key_c,
            "different volume prefixes must produce different keys"
        );
        // All keys are lowercase hex of sha256 (64 chars).
        assert_eq!(key_a.len(), 64);
    }

    #[test]
    fn relay_status_serializes_all_variants() {
        for status in [
            RelayStatus::Accepted,
            RelayStatus::Committed,
            RelayStatus::Failed,
            RelayStatus::Duplicate,
        ] {
            let json = serde_json::to_string(&status).expect("serialize status");
            let decoded: RelayStatus = serde_json::from_str(&json).expect("deserialize status");
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn relay_write_path_is_stable() {
        assert_eq!(DEFAULT_RELAY_WRITE_PATH, "/relay_write");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn relay_client_retries_after_http_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock relay server");
        let addr = listener.local_addr().expect("listener address");
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_for_server = hits.clone();

        thread::spawn(move || {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept connection");
                hits_for_server.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                if attempt == 0 {
                    let response = b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                    stream.write_all(response).expect("write 500 response");
                } else {
                    let body = serde_json::to_string(&RelayWriteResponse {
                        status: RelayStatus::Committed,
                        committed_generation: Some(99),
                        idempotency_key: RelayWriteRequest::derive_idempotency_key(
                            "client-a",
                            "/vol/prefix",
                            7,
                        ),
                        error: None,
                        actual_generation: None,
                    })
                    .expect("serialize response");
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("write committed response");
                }
            }
        });

        let client = RelayClient::with_retry_policy(
            format!("http://{}", addr),
            Duration::from_millis(100),
            1,
            Duration::from_millis(1),
            None,
        )
        .expect("build client");
        let request = RelayWriteRequest::new(
            "client-a",
            "/vol/prefix",
            7,
            8,
            vec![SegmentEntry {
                inode: 1,
                path: "/name".to_string(),
                logical_offset: 0,
                payload: SegmentPayload::Bytes(vec![1, 2, 3]),
            }],
            vec![InodeRecord::new_file(
                1,
                2,
                "name".to_string(),
                "/name".to_string(),
                1000,
                1000,
            )],
        );

        let response = client
            .submit_relay_write(request)
            .await
            .expect("submit write");
        assert_eq!(response.status, RelayStatus::Committed);
        assert_eq!(response.committed_generation, Some(99));
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn relay_client_sends_bearer_token() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock relay server");
        let addr = listener.local_addr().expect("listener address");
        let saw_auth = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_auth_clone = saw_auth.clone();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept connection");
            let mut buf = [0u8; 8192];
            let n = stream.read(&mut buf).unwrap_or(0);
            let request_text = String::from_utf8_lossy(&buf[..n]);
            let lower = request_text.to_ascii_lowercase();
            if lower.contains("authorization: bearer test-token-xyz") {
                saw_auth_clone.store(true, Ordering::SeqCst);
            }
            let body = serde_json::to_string(&RelayWriteResponse {
                status: RelayStatus::Committed,
                committed_generation: Some(1),
                idempotency_key: RelayWriteRequest::derive_idempotency_key(
                    "client-a",
                    "/vol/prefix",
                    1,
                ),
                error: None,
                actual_generation: None,
            })
            .expect("serialize response");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });

        let client = RelayClient::with_retry_policy(
            format!("http://{}", addr),
            Duration::from_millis(200),
            0,
            Duration::from_millis(1),
            Some("test-token-xyz".to_string()),
        )
        .expect("build client");

        let request = RelayWriteRequest::new("client-a", "/vol/prefix", 1, 0, vec![], vec![]);
        client
            .submit_relay_write(request)
            .await
            .expect("submit write");
        assert!(
            saw_auth.load(Ordering::SeqCst),
            "Authorization: Bearer header was not sent"
        );
    }

    #[test]
    fn dedup_store_records_and_lookups() {
        let dedup = DedupStore::new(Duration::from_secs(3600));

        assert!(dedup.lookup("key-a").is_none(), "new key returns None");
        assert!(dedup.is_empty());

        dedup.record("key-a".to_string(), 42);
        assert_eq!(dedup.lookup("key-a"), Some(42));
        assert_eq!(dedup.len(), 1);

        // Different key not found.
        assert!(dedup.lookup("key-b").is_none());
    }

    #[test]
    fn dedup_store_expired_entries_return_none() {
        // TTL of 0 means entries expire immediately.
        let dedup = DedupStore::new(Duration::ZERO);
        dedup.record("key-a".to_string(), 99);
        // An immediately-expired entry should not be returned.
        assert!(
            dedup.lookup("key-a").is_none(),
            "expired entry must not be returned"
        );
    }

    #[test]
    fn dedup_store_evict_removes_expired() {
        let dedup = DedupStore::new(Duration::ZERO);
        dedup.record("key-a".to_string(), 1);
        dedup.record("key-b".to_string(), 2);
        // After eviction with zero TTL all entries should be gone.
        dedup.evict_expired();
        assert!(dedup.is_empty(), "all expired entries should be evicted");
    }

    // ── relay_replay_reconcile ────────────────────────────────────────────

    #[test]
    fn reconcile_returns_retry_when_generation_matches() {
        // Server is still at the expected generation — no advance, normal retry.
        let decision = super::relay_replay_reconcile(5, 5);
        assert_eq!(decision, super::RelayReplayDecision::Retry);
    }

    #[test]
    fn reconcile_returns_retry_when_generation_advanced_by_one() {
        // Server advanced by exactly 1 — could be our write on a cold dedup
        // restart; proceed with retry so dedup logic can confirm.
        let decision = super::relay_replay_reconcile(6, 5);
        assert_eq!(decision, super::RelayReplayDecision::Retry);
    }

    #[test]
    fn reconcile_returns_clear_stale_when_generation_advanced_by_more_than_one() {
        // Server is at 8 but we expected 5 → generation advanced 3 past us.
        // The write can never be committed.
        let decision = super::relay_replay_reconcile(8, 5);
        assert_eq!(
            decision,
            super::RelayReplayDecision::ClearStale {
                remote_generation: 8,
                expected_parent: 5,
            }
        );
    }

    #[test]
    fn reconcile_handles_zero_generation() {
        // Edge case: fresh volume with expected_parent = 0, server at 0 → retry.
        assert_eq!(
            super::relay_replay_reconcile(0, 0),
            super::RelayReplayDecision::Retry
        );
        // And 1 past 0 → still retry.
        assert_eq!(
            super::relay_replay_reconcile(1, 0),
            super::RelayReplayDecision::Retry
        );
        // Two past 0 → stale.
        assert_eq!(
            super::relay_replay_reconcile(2, 0),
            super::RelayReplayDecision::ClearStale {
                remote_generation: 2,
                expected_parent: 0,
            }
        );
    }

    #[test]
    fn failed_response_includes_actual_generation() {
        let response = super::RelayWriteResponse {
            status: RelayStatus::Failed,
            committed_generation: None,
            idempotency_key: "key".to_string(),
            error: Some("generation mismatch".to_string()),
            actual_generation: Some(10),
        };
        response
            .validate()
            .expect("failed response with actual_generation is valid");
        assert_eq!(response.actual_generation, Some(10));
    }

    #[test]
    fn actual_generation_defaults_to_none_on_committed() {
        // Verify backward-compat: committed responses omit actual_generation.
        let json = r#"{"status":"committed","committed_generation":5,"idempotency_key":"abc","error":null}"#;
        let resp: super::RelayWriteResponse =
            serde_json::from_str(json).expect("deserialize committed");
        assert_eq!(
            resp.actual_generation, None,
            "old response format should default to None"
        );
    }

    // ── Relay ownership unit tests ────────────────────────────────────────

    #[test]
    fn relay_owner_record_serde_round_trips() {
        let record = super::RelayOwnerRecord {
            replica_id: "worker-us-east-1a".to_string(),
            advertise_url: "http://10.0.1.5:8080".to_string(),
            lease_until: 1_710_800_000,
            epoch: 3,
        };
        let json = serde_json::to_string(&record).unwrap();
        let parsed: super::RelayOwnerRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, record);
    }

    #[test]
    fn relay_owner_is_live_checks_lease_until() {
        let record = super::RelayOwnerRecord {
            replica_id: "r1".to_string(),
            advertise_url: "http://localhost:8080".to_string(),
            lease_until: 1_000,
            epoch: 0,
        };
        assert!(record.is_live_at(999), "should be live before lease_until");
        assert!(
            !record.is_live_at(1_000),
            "should be expired at lease_until"
        );
        assert!(
            !record.is_live_at(1_001),
            "should be expired after lease_until"
        );
    }

    #[test]
    fn relay_owner_path_with_prefix() {
        let path = super::relay_owner_path("orgs/myorg/vol1");
        assert_eq!(path.as_ref(), "orgs/myorg/vol1/metadata/relay_owner.json");
    }

    #[test]
    fn relay_owner_path_empty_prefix() {
        let path = super::relay_owner_path("");
        assert_eq!(path.as_ref(), "metadata/relay_owner.json");
    }

    #[tokio::test]
    async fn acquire_relay_ownership_initial_acquisition() {
        let store = object_store::memory::InMemory::new();
        let result = super::acquire_relay_ownership(
            &store,
            "orgs/test/vol1",
            "replica-a",
            "http://replica-a:8080",
        )
        .await
        .expect("acquire");

        match result {
            super::OwnershipAcquireResult::Acquired { record, .. } => {
                assert_eq!(record.replica_id, "replica-a");
                assert_eq!(record.epoch, 0);
            }
            other => panic!("expected Acquired, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn acquire_relay_ownership_already_owned_returns_existing() {
        let store = object_store::memory::InMemory::new();

        // First acquisition.
        super::acquire_relay_ownership(&store, "vol1", "replica-a", "http://a:8080")
            .await
            .unwrap();

        // Second acquisition from the same replica.
        let result = super::acquire_relay_ownership(&store, "vol1", "replica-a", "http://a:8080")
            .await
            .expect("re-acquire");

        match result {
            super::OwnershipAcquireResult::AlreadyOwned { record, .. } => {
                assert_eq!(record.replica_id, "replica-a");
            }
            other => panic!("expected AlreadyOwned, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn acquire_relay_ownership_live_foreign_returns_owned_by_other() {
        let store = object_store::memory::InMemory::new();

        // Replica-a acquires.
        super::acquire_relay_ownership(&store, "vol1", "replica-a", "http://a:8080")
            .await
            .unwrap();

        // Replica-b attempts to acquire while a's lease is still live.
        let result = super::acquire_relay_ownership(&store, "vol1", "replica-b", "http://b:8080")
            .await
            .expect("attempt acquire");

        match result {
            super::OwnershipAcquireResult::OwnedByOther(record) => {
                assert_eq!(record.replica_id, "replica-a");
            }
            other => panic!("expected OwnedByOther, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn acquire_relay_ownership_expired_lease_takeover() {
        use super::{RELAY_OWNER_SUFFIX, relay_owner_path};

        let store = object_store::memory::InMemory::new();

        // Manually write an already-expired owner record.
        let expired = super::RelayOwnerRecord {
            replica_id: "old-replica".to_string(),
            advertise_url: "http://old:8080".to_string(),
            lease_until: 1, // expired long ago
            epoch: 5,
        };
        let path = relay_owner_path("vol1");
        let bytes = serde_json::to_vec(&expired).unwrap();
        store
            .put(
                &path,
                object_store::PutPayload::from_bytes(bytes::Bytes::from(bytes)),
            )
            .await
            .unwrap();

        // New replica should be able to take over.
        let result =
            super::acquire_relay_ownership(&store, "vol1", "new-replica", "http://new:8080")
                .await
                .expect("takeover");

        match result {
            super::OwnershipAcquireResult::Acquired { record, .. } => {
                assert_eq!(record.replica_id, "new-replica");
                assert_eq!(record.epoch, 6, "epoch must be incremented on takeover");
            }
            other => panic!("expected Acquired (takeover), got {other:?}"),
        }

        // Suppress dead-code warning for imported constant.
        let _ = RELAY_OWNER_SUFFIX;
    }

    #[tokio::test]
    async fn release_relay_ownership_idempotent() {
        let store = object_store::memory::InMemory::new();

        super::acquire_relay_ownership(&store, "vol1", "r1", "http://r1:8080")
            .await
            .unwrap();

        // Release once.
        super::release_relay_ownership(&store, "vol1")
            .await
            .expect("first release");

        // Release again should be a no-op (not-found ignored).
        super::release_relay_ownership(&store, "vol1")
            .await
            .expect("idempotent release");
    }
}
