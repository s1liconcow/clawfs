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

use std::time::Duration;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

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
    use super::{
        CoordinationEvent, CoordinationHealth, CoordinationPayload, GenerationHint,
        InvalidationEvent, InvalidationScope, sequence_gap, should_force_full_refresh,
    };

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
}
