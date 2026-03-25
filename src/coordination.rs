//! Coordination protocol contract for generation and invalidation hints.
//!
//! Transport-agnostic contract — defines message shapes and the publisher trait
//! only.  Concrete implementations (SSE, WebSocket, pub/sub) live in the hosted
//! private crate.
//!
//! Hints are advisory: clients that miss or drop them fall back to polling the
//! object store.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// A hint that a new generation has been committed by another client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationHint {
    pub volume_prefix: String,
    pub generation: u64,
    pub committer_id: String,
    pub timestamp: i64,
}

/// Scope of a cache invalidation event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvalidationScope {
    Full,
    Inodes(Vec<u64>),
    Prefix(String),
}

/// An advisory cache-invalidation event emitted after a successful generation
/// commit so other clients can refresh the affected inodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidationEvent {
    pub volume_prefix: String,
    pub generation: u64,
    pub affected_inodes: Option<Vec<u64>>,
    pub scope: InvalidationScope,
}

/// Trait implemented by hosted coordination transports.  All methods are
/// advisory and fire-and-forget — callers must not treat errors as fatal.
#[async_trait::async_trait]
pub trait CoordinationPublisher: Send + Sync {
    async fn publish_generation_advance(&self, hint: GenerationHint) -> Result<()>;
    async fn publish_invalidation(&self, event: InvalidationEvent) -> Result<()>;
}

/// No-op publisher used when no coordination backend is configured.
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
