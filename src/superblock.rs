use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
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
    pub last_idempotency_key: Option<String>,
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

    pub(crate) async fn reload(&self) -> Result<()> {
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
