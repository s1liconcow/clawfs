use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use slatedb::{Db, object_store::local::LocalFileSystem};

use crate::inode::{InodeRecord, InodeShard};
use crate::superblock::Superblock;

const SUPERBLOCK_KEY: &[u8] = b"superblock";
const DELTA_THRESHOLD: u64 = 32;

#[derive(Clone)]
pub struct MetadataStore {
    db: Arc<Db>,
}

impl MetadataStore {
    pub async fn open<P: AsRef<Path>>(store_root: P) -> Result<Self> {
        let metadata_root = store_root.as_ref().join("metadata");
        std::fs::create_dir_all(&metadata_root)
            .with_context(|| format!("creating metadata root {}", metadata_root.display()))?;
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(metadata_root.clone())?);
        let db = Db::open("osagefs", object_store).await?;
        Ok(Self { db: Arc::new(db) })
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.db.close().await?;
        Ok(())
    }

    pub async fn load_superblock(&self) -> Result<Option<Superblock>> {
        let value = self.db.get(SUPERBLOCK_KEY).await?;
        if let Some(bytes) = value {
            let sb = serde_json::from_slice::<Superblock>(&bytes)?;
            Ok(Some(sb))
        } else {
            Ok(None)
        }
    }

    pub async fn store_superblock(&self, sb: &Superblock) -> Result<()> {
        let json = serde_json::to_vec(sb)?;
        self.db.put(SUPERBLOCK_KEY, json).await?;
        Ok(())
    }

    pub async fn get_inode(&self, inode: u64) -> Result<Option<InodeRecord>> {
        let key = inode_key(inode);
        if let Some(bytes) = self.db.get(key.as_bytes()).await? {
            Ok(Some(serde_json::from_slice(&bytes)?))
        } else {
            Ok(None)
        }
    }

    pub async fn persist_inode(
        &self,
        record: &InodeRecord,
        generation: u64,
        shard_size: u64,
    ) -> Result<()> {
        let key = inode_key(record.inode);
        let json = serde_json::to_vec(record)?;
        self.db.put(key.as_bytes(), json.clone()).await?;
        let delta_key = delta_key(record.shard_index(shard_size), generation, record.inode);
        self.db.put(delta_key.as_bytes(), json).await?;
        self.update_shard(record, shard_size).await?;
        self.bump_delta_count(record.shard_index(shard_size))
            .await?;
        Ok(())
    }

    pub async fn remove_inode(&self, inode: u64, generation: u64, shard_size: u64) -> Result<()> {
        let key = inode_key(inode);
        self.db.delete(key.as_bytes()).await?;
        let shard_id = if shard_size == 0 {
            0
        } else {
            inode / shard_size
        };
        let tombstone = InodeRecord::tombstone(inode);
        let json = serde_json::to_vec(&tombstone)?;
        let delta_key = delta_key(shard_id, generation, inode);
        self.db.put(delta_key.as_bytes(), json.clone()).await?;
        self.update_shard(&tombstone, shard_size).await?;
        self.bump_delta_count(shard_id).await?;
        Ok(())
    }

    async fn update_shard(&self, record: &InodeRecord, shard_size: u64) -> Result<()> {
        let shard_id = record.shard_index(shard_size);
        let mut shard = self
            .load_shard(shard_id)
            .await?
            .unwrap_or_else(|| InodeShard::new(shard_id));
        shard.upsert(record.clone());
        let key = shard_key(shard_id);
        let json = serde_json::to_vec(&shard)?;
        self.db.put(key.as_bytes(), json).await?;
        Ok(())
    }

    pub async fn load_shard(&self, shard_id: u64) -> Result<Option<InodeShard>> {
        let key = shard_key(shard_id);
        if let Some(bytes) = self.db.get(key.as_bytes()).await? {
            Ok(Some(serde_json::from_slice(&bytes)?))
        } else {
            Ok(None)
        }
    }

    async fn bump_delta_count(&self, shard_id: u64) -> Result<()> {
        let key = delta_count_key(shard_id);
        let mut current = if let Some(bytes) = self.db.get(key.as_bytes()).await? {
            deserialize_u64(&bytes)?
        } else {
            0
        };
        current += 1;
        self.db.put(key.as_bytes(), current.to_be_bytes()).await?;
        if current >= DELTA_THRESHOLD {
            self.compact_shard(shard_id).await?;
            self.db.put(key.as_bytes(), 0u64.to_be_bytes()).await?;
        }
        Ok(())
    }

    async fn compact_shard(&self, shard_id: u64) -> Result<()> {
        let prefix = delta_prefix(shard_id);
        let start = prefix.into_bytes();
        let mut iter = self.db.scan(start.clone()..).await?;
        while let Some(kv) = iter.next().await? {
            if !kv.key.starts_with(&start) {
                break;
            }
            self.db.delete(kv.key.clone()).await?;
        }
        Ok(())
    }
}

fn inode_key(inode: u64) -> String {
    format!("inode:{inode:016x}")
}

fn shard_key(shard_id: u64) -> String {
    format!("imap_shard:{shard_id:08x}")
}

fn delta_key(shard_id: u64, generation: u64, inode: u64) -> String {
    format!("{}:{generation:020}:{inode:016x}", delta_prefix(shard_id))
}

fn delta_prefix(shard_id: u64) -> String {
    format!("imap_delta:{shard_id:08x}")
}

fn delta_count_key(shard_id: u64) -> String {
    format!("delta_count:{shard_id:08x}")
}

fn deserialize_u64(bytes: &Bytes) -> Result<u64> {
    anyhow::ensure!(bytes.len() >= 8, "counter value truncated");
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    Ok(u64::from_be_bytes(buf))
}
