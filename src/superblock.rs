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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CleanupTaskKind {
    DeltaCompaction,
    SegmentCompaction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupLease {
    pub kind: CleanupTaskKind,
    pub client_id: String,
    pub lease_until: i64,
}

struct SuperblockState {
    block: Superblock,
    pending_generation: Option<u64>,
}

pub struct SuperblockManager {
    store: Arc<MetadataStore>,
    state: Mutex<SuperblockState>,
}

impl SuperblockManager {
    pub async fn load_or_init(store: Arc<MetadataStore>, shard_size: u64) -> Result<Self> {
        let block = match store.load_superblock().await? {
            Some(mut existing) => {
                if existing.cleanup_leases.is_empty() {
                    existing.cleanup_leases = Vec::new();
                }
                existing
            }
            None => {
                let bootstrap = Superblock::bootstrap(shard_size);
                store.store_superblock(&bootstrap).await?;
                bootstrap
            }
        };
        Ok(Self {
            store,
            state: Mutex::new(SuperblockState {
                block,
                pending_generation: None,
            }),
        })
    }

    pub fn snapshot(&self) -> Superblock {
        self.state.lock().block.clone()
    }

    async fn persist(&self, snapshot: &Superblock) -> Result<()> {
        self.store.store_superblock(snapshot).await
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
        let snapshot = {
            let mut guard = self.state.lock();
            guard.block.state = FilesystemState::Clean;
            guard.pending_generation = None;
            guard.block.clone()
        };
        self.persist(&snapshot).await
    }

    pub async fn reserve_inodes(&self, count: u64) -> Result<u64> {
        let count = count.max(1);
        let (start, snapshot) = {
            let mut guard = self.state.lock();
            let start = guard.block.next_inode;
            guard.block.next_inode = guard.block.next_inode.saturating_add(count);
            let snapshot = guard.block.clone();
            (start, snapshot)
        };
        self.persist(&snapshot).await?;
        Ok(start)
    }

    pub async fn reserve_segments(&self, count: u64) -> Result<u64> {
        let count = count.max(1);
        let (start, snapshot) = {
            let mut guard = self.state.lock();
            let start = guard.block.next_segment;
            guard.block.next_segment = guard.block.next_segment.saturating_add(count);
            let snapshot = guard.block.clone();
            (start, snapshot)
        };
        self.persist(&snapshot).await?;
        Ok(start)
    }

    pub async fn commit_generation(&self, generation: u64) -> Result<()> {
        let (expected, snapshot) = {
            let mut guard = self.state.lock();
            if guard.pending_generation != Some(generation) {
                bail!("generation {} not pending", generation);
            }
            guard.pending_generation = None;
            let expected = guard.block.generation;
            guard.block.generation = generation;
            guard.block.state = FilesystemState::Clean;
            (expected, guard.block.clone())
        };
        self.store
            .compare_and_swap_superblock(expected, &snapshot)
            .await
    }

    pub async fn try_acquire_cleanup(
        &self,
        kind: CleanupTaskKind,
        client_id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let deadline = OffsetDateTime::now_utc().unix_timestamp() + ttl.as_secs() as i64;
        let snapshot = {
            let mut guard = self.state.lock();
            let now = OffsetDateTime::now_utc().unix_timestamp();
            guard
                .block
                .cleanup_leases
                .retain(|lease| lease.lease_until > now);
            if guard
                .block
                .cleanup_leases
                .iter()
                .any(|lease| lease.kind == kind)
            {
                return Ok(false);
            }
            guard.block.cleanup_leases.push(CleanupLease {
                kind,
                client_id: client_id.to_string(),
                lease_until: deadline,
            });
            guard.block.clone()
        };
        self.persist(&snapshot).await?;
        Ok(true)
    }

    pub async fn complete_cleanup(&self, kind: CleanupTaskKind, client_id: &str) -> Result<()> {
        let snapshot = {
            let mut guard = self.state.lock();
            guard
                .block
                .cleanup_leases
                .retain(|lease| !(lease.kind == kind && lease.client_id == client_id));
            guard.block.clone()
        };
        self.persist(&snapshot).await
    }
}
