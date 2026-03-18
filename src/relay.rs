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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use parking_lot::Mutex;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::clawfs::AcceleratorMode;
use crate::inode::InodeRecord;
use crate::metadata::MetadataStore;
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
    if let Err(err) = superblock.commit_generation(prepared_generation).await {
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
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{
        DEFAULT_RELAY_WRITE_PATH, DedupStore, RelayClient, RelayOutageAction, RelayOutagePolicy,
        RelayOutageState, RelayStatus, RelayWriteRequest, RelayWriteResponse,
    };
    use crate::inode::InodeRecord;
    use crate::segment::{SegmentEntry, SegmentPayload};

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
}
