use std::sync::Arc;

use anyhow::Result;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

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
        }
    }
}

pub struct SuperblockManager {
    store: Arc<MetadataStore>,
    inner: Mutex<Superblock>,
}

impl SuperblockManager {
    pub async fn load_or_init(store: Arc<MetadataStore>, shard_size: u64) -> Result<Self> {
        let block = match store.load_superblock().await? {
            Some(existing) => existing,
            None => {
                let bootstrap = Superblock::bootstrap(shard_size);
                store.store_superblock(&bootstrap).await?;
                bootstrap
            }
        };
        Ok(Self {
            store,
            inner: Mutex::new(block),
        })
    }

    pub fn snapshot(&self) -> Superblock {
        self.inner.lock().clone()
    }

    async fn persist(&self, snapshot: &Superblock) -> Result<()> {
        self.store.store_superblock(snapshot).await
    }

    pub async fn mutate<F, R>(&self, f: F) -> Result<(R, Superblock)>
    where
        F: FnOnce(&mut Superblock) -> R,
    {
        let (result, snapshot) = {
            let mut guard = self.inner.lock();
            let value = f(&mut guard);
            guard.generation = guard.generation.saturating_add(1);
            let snapshot = guard.clone();
            (value, snapshot)
        };
        self.persist(&snapshot).await?;
        Ok((result, snapshot))
    }

    pub async fn mark_clean(&self) -> Result<()> {
        self.mutate(|sb| {
            sb.state = FilesystemState::Clean;
        })
        .await
        .map(|_| ())
    }

    pub async fn reserve_inodes(&self, count: u64) -> Result<u64> {
        let count = count.max(1);
        let (start, snapshot) = {
            let mut guard = self.inner.lock();
            let start = guard.next_inode;
            guard.next_inode = guard.next_inode.saturating_add(count);
            let snapshot = guard.clone();
            (start, snapshot)
        };
        self.persist(&snapshot).await?;
        Ok(start)
    }

    pub async fn reserve_segments(&self, count: u64) -> Result<u64> {
        let count = count.max(1);
        let (start, snapshot) = {
            let mut guard = self.inner.lock();
            let start = guard.next_segment;
            guard.next_segment = guard.next_segment.saturating_add(count);
            let snapshot = guard.clone();
            (start, snapshot)
        };
        self.persist(&snapshot).await?;
        Ok(start)
    }

    pub async fn commit_generation(&self, generation: u64) -> Result<()> {
        let mut guard = self.inner.lock();
        guard.generation = generation;
        guard.state = FilesystemState::Clean;
        let snapshot = guard.clone();
        drop(guard);
        self.persist(&snapshot).await
    }
}
