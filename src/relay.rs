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
//! Retry and deduplication:
//! - Idempotency keys are deterministic from client identity, volume prefix,
//!   and journal sequence.
//! - Servers should retain idempotency keys for a bounded window that covers
//!   the retry-after-timeout path and the period needed for committed results
//!   to remain discoverable to retried requests.
//! - Eviction must never violate the rule that a retried request with the same
//!   idempotency key can discover the committed result for the dedup window.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::inode::InodeRecord;
use crate::segment::SegmentEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayStatus {
    Accepted,
    Committed,
    Failed,
    Duplicate,
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
    use super::{RelayStatus, RelayWriteRequest, RelayWriteResponse};
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
}
