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

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::str::FromStr;
use std::time::Instant;

use crate::clawfs::AcceleratorMode;
use crate::inode::InodeRecord;
use crate::segment::SegmentEntry;

pub const DEFAULT_RELAY_QUEUE_DEPTH: usize = 32;

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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelayWriteResponse {
    pub status: RelayStatus,
    pub committed_generation: Option<u64>,
    pub idempotency_key: String,
    pub error: Option<String>,
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

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::{
        RelayOutageAction, RelayOutagePolicy, RelayOutageState, RelayStatus, RelayWriteRequest,
        RelayWriteResponse,
    };
    use crate::inode::InodeRecord;

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
    fn response_validation_matches_status_contract() {
        let committed = RelayWriteResponse {
            status: RelayStatus::Committed,
            committed_generation: Some(9),
            idempotency_key: "abc".to_string(),
            error: None,
        };
        committed.validate().expect("committed response is valid");

        let duplicate = RelayWriteResponse {
            status: RelayStatus::Duplicate,
            committed_generation: Some(9),
            idempotency_key: "abc".to_string(),
            error: None,
        };
        duplicate.validate().expect("duplicate response is valid");

        let accepted = RelayWriteResponse {
            status: RelayStatus::Accepted,
            committed_generation: None,
            idempotency_key: "abc".to_string(),
            error: None,
        };
        accepted.validate().expect("accepted response is valid");

        let failed = RelayWriteResponse {
            status: RelayStatus::Failed,
            committed_generation: None,
            idempotency_key: "abc".to_string(),
            error: Some("boom".to_string()),
        };
        failed.validate().expect("failed response is valid");

        let invalid = RelayWriteResponse {
            status: RelayStatus::Accepted,
            committed_generation: Some(9),
            idempotency_key: "abc".to_string(),
            error: None,
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
}
