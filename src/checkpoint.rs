use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::codec::{deserialize_flex, write_flexbuffer};
use crate::metadata::MetadataStore;
use crate::superblock::Superblock;

const CHECKPOINT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointData {
    pub version: u32,
    pub created_at_unix: i64,
    pub note: Option<String>,
    pub superblock: Superblock,
}

#[derive(Debug, Clone)]
pub struct CheckpointResult {
    pub generation: u64,
    pub next_inode: u64,
    pub next_segment: u64,
    pub checkpoint_path: String,
}

pub async fn create_checkpoint(
    store_path: &Path,
    shard_size: u64,
    log_storage_io: bool,
    checkpoint_path: &Path,
    note: Option<String>,
) -> Result<CheckpointResult> {
    let store = MetadataStore::open(store_path, shard_size, log_storage_io).await?;
    let superblock = store
        .load_superblock()
        .await?
        .ok_or_else(|| anyhow::anyhow!("superblock is missing under {}", store_path.display()))?;
    let checkpoint = CheckpointData {
        version: CHECKPOINT_FORMAT_VERSION,
        created_at_unix: time::OffsetDateTime::now_utc().unix_timestamp(),
        note: note.and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then_some(trimmed.to_string())
        }),
        superblock: superblock.clone(),
    };
    write_flexbuffer(checkpoint_path, &checkpoint)?;
    Ok(CheckpointResult {
        generation: superblock.generation,
        next_inode: superblock.next_inode,
        next_segment: superblock.next_segment,
        checkpoint_path: checkpoint_path.display().to_string(),
    })
}

pub async fn restore_checkpoint(
    store_path: &Path,
    shard_size: u64,
    log_storage_io: bool,
    checkpoint_path: &Path,
) -> Result<CheckpointResult> {
    let bytes = std::fs::read(checkpoint_path)
        .with_context(|| format!("reading checkpoint {}", checkpoint_path.display()))?;
    let checkpoint: CheckpointData = deserialize_flex(&bytes)
        .with_context(|| format!("parsing checkpoint {}", checkpoint_path.display()))?;
    if checkpoint.version != CHECKPOINT_FORMAT_VERSION {
        bail!(
            "unsupported checkpoint format version {}",
            checkpoint.version
        );
    }

    let store = MetadataStore::open(store_path, shard_size, log_storage_io).await?;
    store.store_superblock(&checkpoint.superblock).await?;
    Ok(CheckpointResult {
        generation: checkpoint.superblock.generation,
        next_inode: checkpoint.superblock.next_inode,
        next_segment: checkpoint.superblock.next_segment,
        checkpoint_path: checkpoint_path.display().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::{create_checkpoint, restore_checkpoint};
    use crate::metadata::MetadataStore;
    use crate::superblock::SuperblockManager;

    #[test]
    fn checkpoint_restore_round_trip_resets_superblock() {
        let temp = tempdir().unwrap();
        let store_path = temp.path().join("store");
        let checkpoint_path = temp.path().join("cp.bin");
        let runtime = tokio::runtime::Runtime::new().unwrap();

        runtime.block_on(async {
            let metadata = Arc::new(MetadataStore::open(&store_path, 1024, false).await.unwrap());
            let superblock = SuperblockManager::load_or_init(metadata.clone(), 1024)
                .await
                .unwrap();
            superblock.reserve_inodes(100).await.unwrap();
            let snapshot = superblock.prepare_dirty_generation().unwrap();
            superblock
                .commit_generation(snapshot.generation)
                .await
                .unwrap();

            let first_generation = superblock.snapshot().generation;
            create_checkpoint(
                &store_path,
                1024,
                false,
                &checkpoint_path,
                Some(String::from("before second commit")),
            )
            .await
            .unwrap();

            let snapshot = superblock.prepare_dirty_generation().unwrap();
            superblock
                .commit_generation(snapshot.generation)
                .await
                .unwrap();
            let newer_generation = superblock.snapshot().generation;
            assert!(newer_generation > first_generation);

            restore_checkpoint(&store_path, 1024, false, &checkpoint_path)
                .await
                .unwrap();

            let metadata = MetadataStore::open(&store_path, 1024, false).await.unwrap();
            let restored = metadata.load_superblock().await.unwrap().unwrap();
            assert_eq!(restored.generation, first_generation);
        });
    }
}
