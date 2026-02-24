use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use crate::codec::{deserialize_flex, write_flexbuffer};
use crate::config::Config;
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
    config: &Config,
    handle: Handle,
    checkpoint_path: &Path,
    note: Option<String>,
) -> Result<CheckpointResult> {
    let store = MetadataStore::new(config, handle).await?;
    let superblock = store
        .load_superblock()
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "superblock is missing under {}",
                config.store_path.display()
            )
        })?
        .block;

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
    config: &Config,
    handle: Handle,
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

    let store = MetadataStore::new(config, handle).await?;
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
    use std::path::PathBuf;
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::{create_checkpoint, restore_checkpoint};
    use crate::config::{Config, ObjectStoreProvider};
    use crate::metadata::MetadataStore;
    use crate::superblock::SuperblockManager;

    fn test_config(store_path: PathBuf) -> Config {
        Config {
            mount_path: PathBuf::from("/tmp/mnt"),
            store_path,
            local_cache_path: PathBuf::from("/tmp/cache"),
            log_storage_io: false,
            inline_threshold: 1024,
            inline_compression: true,
            inline_encryption_key: None,
            segment_compression: true,
            segment_encryption_key: None,
            shard_size: 1024,
            inode_batch: 128,
            segment_batch: 128,
            pending_bytes: 1024,
            home_prefix: "/home".to_string(),
            object_provider: ObjectStoreProvider::Local,
            bucket: None,
            region: None,
            endpoint: None,
            object_prefix: String::new(),
            gcs_service_account: None,
            state_path: PathBuf::from("/tmp/state"),
            perf_log: None,
            replay_log: None,
            disable_journal: false,
            fsync_on_close: false,
            flush_interval_ms: 0,
            disable_cleanup: false,
            lookup_cache_ttl_ms: 0,
            dir_cache_ttl_ms: 0,
            metadata_poll_interval_ms: 0,
            segment_cache_bytes: 0,
            foreground: false,
            allow_other: false,
            log_file: None,
            debug_log: false,
            imap_delta_batch: 32,
            writeback_cache: false,
            fuse_threads: 0,
        }
    }

    #[test]
    fn checkpoint_restore_round_trip_resets_superblock() {
        let temp = tempdir().unwrap();
        let store_path = temp.path().join("store");
        let checkpoint_path = temp.path().join("cp.bin");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let config = test_config(store_path.clone());

        runtime.block_on(async {
            let metadata = Arc::new(MetadataStore::new(&config, handle.clone()).await.unwrap());
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
                &config,
                handle.clone(),
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

            restore_checkpoint(&config, handle.clone(), &checkpoint_path)
                .await
                .unwrap();

            let metadata = MetadataStore::new(&config, handle.clone()).await.unwrap();
            let restored = metadata.load_superblock().await.unwrap().unwrap();
            assert_eq!(restored.block.generation, first_generation);
        });
    }
}
