use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;

use crate::metadata::MetadataStore;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FilesystemState {
    Clean,
    Dirty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Superblock {
    pub generation: u64,
    pub next_inode: u64,
    pub next_segment: u64,
    pub shard_size: u64,
    pub version: u32,
    pub state: FilesystemState,
    pub cleanup_leases: Vec<CleanupLease>,
    /// Idempotency key of the most recently committed relay write, persisted
    /// in the superblock so a new owner can detect takeover duplicates even
    /// after its in-memory DedupStore is empty.  `#[serde(default)]` makes
    /// this field backward-compatible with superblocks stored before it was
    /// added — missing values deserialise as `None`.
    #[serde(default)]
    pub last_idempotency_key: Option<String>,
    /// When true, clients must use the relay write-back path and direct writes
    /// must fail closed.
    #[serde(default)]
    pub relay_required: bool,
}

impl Superblock {
    pub fn bootstrap(shard_size: u64) -> Self {
        Self {
            generation: 1,
            next_inode: 2,
            next_segment: 1,
            shard_size,
            version: 1,
            state: FilesystemState::Clean,
            cleanup_leases: Vec::new(),
            last_idempotency_key: None,
            relay_required: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CleanupTaskKind {
    DeltaCompaction,
    SegmentCompaction,
}

impl CleanupTaskKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DeltaCompaction => "delta_compaction",
            Self::SegmentCompaction => "segment_compaction",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupLease {
    pub kind: CleanupTaskKind,
    pub client_id: String,
    pub lease_until: i64,
}

struct SuperblockState {
    block: Superblock,
    version: String,
    pending_generation: Option<u64>,
}

pub struct SuperblockManager {
    store: Arc<MetadataStore>,
    state: Mutex<SuperblockState>,
}

#[derive(Debug, Error)]
#[error("stale generation {attempted}; current superblock generation is {current}")]
pub struct StaleGenerationError {
    pub attempted: u64,
    pub current: u64,
}

impl SuperblockManager {
    pub async fn load_or_init(store: Arc<MetadataStore>, shard_size: u64) -> Result<Self> {
        let (block, version) = match store.load_superblock().await? {
            Some(existing) => {
                let mut block = existing.block;
                if block.cleanup_leases.is_empty() {
                    block.cleanup_leases = Vec::new();
                }
                (block, existing.version)
            }
            None => {
                let bootstrap = Superblock::bootstrap(shard_size);
                let version = store.store_superblock(&bootstrap).await?;
                (bootstrap, version)
            }
        };
        Ok(Self {
            store,
            state: Mutex::new(SuperblockState {
                block,
                version,
                pending_generation: None,
            }),
        })
    }

    pub fn snapshot(&self) -> Superblock {
        self.state.lock().block.clone()
    }

    pub async fn reload(&self) -> Result<()> {
        if let Some(wrapper) = self.store.load_superblock().await? {
            let mut guard = self.state.lock();
            // Preserve pending_generation logic?
            // If we reload, and we had a pending_generation, we might be desynchronized if on-disk generation changed.
            // But reload is mostly for CAS retry loops where we want to re-apply an operation.
            guard.block = wrapper.block;
            guard.version = wrapper.version;
        }
        Ok(())
    }

    async fn update_with_retry<F, R>(&self, mut action: F) -> Result<R>
    where
        F: FnMut(&mut Superblock) -> Result<R>,
    {
        loop {
            let (mut block, expected_version) = {
                let guard = self.state.lock();
                (guard.block.clone(), guard.version.clone())
            };

            let result = action(&mut block)?;

            match self
                .store
                .store_superblock_conditional(&block, &expected_version)
                .await
            {
                Ok(new_version) => {
                    let mut guard = self.state.lock();
                    guard.block = block;
                    guard.version = new_version;
                    return Ok(result);
                }
                Err(_) => {
                    self.reload().await?;
                }
            }
        }
    }

    pub fn prepare_dirty_generation(&self) -> Result<Superblock> {
        let mut guard = self.state.lock();
        if guard.pending_generation.is_some() {
            bail!("generation already in progress");
        }
        let next_generation = guard.block.generation.saturating_add(1);
        guard.pending_generation = Some(next_generation);
        guard.block.state = FilesystemState::Dirty;
        let mut snapshot = guard.block.clone();
        snapshot.generation = next_generation;
        snapshot.state = FilesystemState::Dirty;
        Ok(snapshot)
    }

    pub fn abort_generation(&self, generation: u64) {
        let mut guard = self.state.lock();
        if guard.pending_generation == Some(generation) {
            guard.pending_generation = None;
            guard.block.state = FilesystemState::Clean;
        }
    }

    pub async fn mark_clean(&self) -> Result<()> {
        self.update_with_retry(|block| {
            block.state = FilesystemState::Clean;
            Ok(())
        })
        .await?;
        let mut guard = self.state.lock();
        guard.pending_generation = None;
        Ok(())
    }

    pub async fn reserve_inodes(&self, count: u64) -> Result<u64> {
        let count = count.max(1);
        self.update_with_retry(|block| {
            let start = block.next_inode;
            block.next_inode = block.next_inode.saturating_add(count);
            Ok(start)
        })
        .await
    }

    pub async fn reserve_segments(&self, count: u64) -> Result<u64> {
        let count = count.max(1);
        self.update_with_retry(|block| {
            let start = block.next_segment;
            block.next_segment = block.next_segment.saturating_add(count);
            Ok(start)
        })
        .await
    }

    pub async fn set_relay_required(&self, required: bool) -> Result<()> {
        self.update_with_retry(|block| {
            block.relay_required = required;
            Ok(())
        })
        .await
    }

    /// Accept a generation that was committed by an external party (e.g. the
    /// relay server).  Reloads the on-disk superblock to get the current
    /// version, then clears the local `pending_generation` flag without
    /// writing to object storage.  The local superblock state is authoritative
    /// after the reload.
    pub async fn accept_externally_committed_generation(&self, generation: u64) -> Result<()> {
        self.reload().await?;
        let mut guard = self.state.lock();
        if guard.pending_generation == Some(generation) {
            guard.pending_generation = None;
        }
        Ok(())
    }

    pub async fn commit_generation(&self, generation: u64) -> Result<()> {
        self.commit_generation_idempotent(generation, None).await
    }

    pub async fn commit_generation_idempotent(
        &self,
        generation: u64,
        idempotency_key: Option<String>,
    ) -> Result<()> {
        loop {
            let (expected_version, snapshot) = {
                let mut guard = self.state.lock();
                if guard.pending_generation != Some(generation) {
                    bail!("generation {} not pending", generation);
                }
                if guard.block.generation >= generation {
                    guard.pending_generation = None;
                    return Err(StaleGenerationError {
                        attempted: generation,
                        current: guard.block.generation,
                    }
                    .into());
                }

                let expected_version = guard.version.clone();
                guard.block.generation = generation;
                guard.block.state = FilesystemState::Clean;
                guard.block.last_idempotency_key = idempotency_key.clone();
                (expected_version, guard.block.clone())
            };

            // Use true storage CAS via ETag check (If-Match)
            match self
                .store
                .store_superblock_conditional(&snapshot, &expected_version)
                .await
            {
                Ok(new_version) => {
                    let mut guard = self.state.lock();
                    guard.version = new_version;
                    guard.pending_generation = None;
                    return Ok(());
                }
                Err(_) => {
                    self.reload().await?;
                    // Loop to retry with new base state
                }
            }
        }
    }

    pub async fn try_acquire_cleanup(
        &self,
        kind: CleanupTaskKind,
        client_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let deadline = OffsetDateTime::now_utc().unix_timestamp() + ttl.as_secs() as i64;
        self.update_with_retry(|block| {
            let now = OffsetDateTime::now_utc().unix_timestamp();
            block.cleanup_leases.retain(|lease| lease.lease_until > now);
            if block.cleanup_leases.iter().any(|lease| lease.kind == kind) {
                return Ok(false);
            }
            block.cleanup_leases.push(CleanupLease {
                kind,
                client_id: client_id.to_string(),
                lease_until: deadline,
            });
            Ok(true)
        })
        .await
    }

    pub async fn complete_cleanup(&self, kind: CleanupTaskKind, client_id: &str) -> Result<()> {
        self.update_with_retry(|block| {
            block
                .cleanup_leases
                .retain(|lease| !(lease.kind == kind && lease.client_id == client_id));
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::runtime::Runtime;

    use crate::config::Config;
    use crate::metadata::MetadataStore;

    fn test_config(root: &std::path::Path) -> Config {
        Config {
            inline_threshold: 512,
            shard_size: 64,
            inode_batch: 8,
            segment_batch: 8,
            pending_bytes: 8 * 1024 * 1024,
            entry_ttl_secs: 5,
            home_prefix: "/home".into(),
            disable_journal: true,
            flush_interval_ms: 0,
            disable_cleanup: true,
            lookup_cache_ttl_ms: 0,
            dir_cache_ttl_ms: 0,
            metadata_poll_interval_ms: 0,
            segment_cache_bytes: 0,
            imap_delta_batch: 32,
            fuse_threads: 0,
            ..Config::with_paths(
                root.join("mnt"),
                root.join("store"),
                root.join("cache"),
                root.join("state.bin"),
            )
        }
    }

    /// Superblocks stored before `last_idempotency_key` was added did not
    /// include that field in their JSON.  Without `#[serde(default)]` the
    /// deserializer would reject them with a missing-field error.  This test
    /// asserts that such payloads round-trip to `None`.
    #[test]
    fn superblock_deserializes_without_last_idempotency_key() {
        let json = r#"{
            "generation": 42,
            "next_inode": 100,
            "next_segment": 5,
            "shard_size": 8,
            "version": 1,
            "state": "Clean",
            "cleanup_leases": []
        }"#;
        let sb: Superblock = serde_json::from_str(json)
            .expect("superblock without last_idempotency_key must deserialize");
        assert_eq!(sb.generation, 42);
        assert_eq!(
            sb.last_idempotency_key, None,
            "missing field must default to None"
        );
    }

    /// Superblocks stored before `relay_required` was added should deserialize
    /// with the default permissive value.
    #[test]
    fn superblock_deserializes_without_relay_required() {
        let json = r#"{
            "generation": 42,
            "next_inode": 100,
            "next_segment": 5,
            "shard_size": 8,
            "version": 1,
            "state": "Clean",
            "cleanup_leases": [],
            "last_idempotency_key": null
        }"#;
        let sb: Superblock =
            serde_json::from_str(json).expect("superblock without relay_required must deserialize");
        assert!(!sb.relay_required);
    }

    /// A superblock that *does* include the field must preserve it.
    #[test]
    fn superblock_preserves_last_idempotency_key() {
        let original = Superblock {
            generation: 7,
            next_inode: 10,
            next_segment: 2,
            shard_size: 8,
            version: 1,
            state: FilesystemState::Clean,
            cleanup_leases: Vec::new(),
            last_idempotency_key: Some("abc123".to_string()),
            relay_required: true,
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let decoded: Superblock = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded.last_idempotency_key, Some("abc123".to_string()));
        assert!(decoded.relay_required);
    }

    #[test]
    fn stale_local_generation_commit_is_rejected_instead_of_regressing_superblock() {
        let dir = tempdir().unwrap();
        let runtime = Runtime::new().unwrap();
        let config = test_config(dir.path());
        let store_a = Arc::new(
            runtime
                .block_on(MetadataStore::new(&config, runtime.handle().clone()))
                .unwrap(),
        );
        let store_b = Arc::new(
            runtime
                .block_on(MetadataStore::new(&config, runtime.handle().clone()))
                .unwrap(),
        );
        let manager_a = runtime
            .block_on(SuperblockManager::load_or_init(store_a, config.shard_size))
            .unwrap();
        let manager_b = runtime
            .block_on(SuperblockManager::load_or_init(store_b, config.shard_size))
            .unwrap();

        let generation_b = manager_b.prepare_dirty_generation().unwrap().generation;
        let generation_a = manager_a.prepare_dirty_generation().unwrap().generation;
        assert_eq!(generation_a, generation_b);

        runtime
            .block_on(manager_a.commit_generation(generation_a))
            .unwrap();
        let err = runtime
            .block_on(manager_b.commit_generation(generation_b))
            .unwrap_err();
        let stale = err
            .downcast_ref::<StaleGenerationError>()
            .expect("stale local commit must be rejected explicitly");
        assert_eq!(stale.attempted, generation_b);
        assert_eq!(stale.current, generation_a);

        let final_store = Arc::new(
            runtime
                .block_on(MetadataStore::new(&config, runtime.handle().clone()))
                .unwrap(),
        );
        let final_manager = runtime
            .block_on(SuperblockManager::load_or_init(
                final_store,
                config.shard_size,
            ))
            .unwrap();
        assert_eq!(final_manager.snapshot().generation, generation_a);
    }
}
