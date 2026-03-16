use std::io::Write as _;
use std::path::Path;

use anyhow::{Context, Result, bail};
use flatbuffers::FlatBufferBuilder;
use tokio::runtime::Handle;

use crate::config::Config;
use crate::metadata::MetadataStore;
use crate::metadata::fvt;
use crate::superblock::{CleanupLease, CleanupTaskKind, FilesystemState, Superblock};

const CHECKPOINT_FORMAT_VERSION: u32 = 1;

/// Magic bytes for the FlatBuffer checkpoint format.
const CHECKPOINT_FB_MAGIC: &[u8; 6] = b"OSGCP2";

// ── FlatBuffer Checkpoint schema ──────────────────────────────────────────
//
// CheckpointFB table:
//   0(vt=4)  version:          u32
//   1(vt=6)  created_at_unix:  i64
//   2(vt=8)  note:             string   (optional)
//   3(vt=10) sb_generation:    u64
//   4(vt=12) sb_next_inode:    u64
//   5(vt=14) sb_next_segment:  u64
//   6(vt=16) sb_shard_size:    u64
//   7(vt=18) sb_version:       u32
//   8(vt=20) sb_state:         u8       (0=Clean, 1=Dirty)
//   9(vt=22) sb_cleanup_leases: [CleanupLeaseFB]
//
// CleanupLeaseFB table:
//   0(vt=4)  kind:        u8   (0=DeltaCompaction, 1=SegmentCompaction)
//   1(vt=6)  client_id:   string
//   2(vt=8)  lease_until:  i64

#[derive(Debug, Clone)]
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

    let data = serialize_checkpoint_fb(&checkpoint);
    let parent = checkpoint_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    std::fs::create_dir_all(&parent)
        .with_context(|| format!("creating dir {}", parent.display()))?;
    let mut tmp = tempfile::NamedTempFile::new_in(&parent)
        .with_context(|| format!("creating temp file in {}", parent.display()))?;
    tmp.write_all(&data)?;
    tmp.as_file().sync_all()?;
    tmp.persist(checkpoint_path)
        .map(|_| ())
        .with_context(|| format!("persisting checkpoint to {}", checkpoint_path.display()))?;

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
    let checkpoint = deserialize_checkpoint_fb(&bytes)
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

// ── FlatBuffer checkpoint serialization ───────────────────────────────────

fn build_cleanup_lease_fb<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    lease: &CleanupLease,
) -> flatbuffers::WIPOffset<flatbuffers::TableFinishedWIPOffset> {
    let client_id_wip = fbb.create_string(&lease.client_id);
    let kind_u8 = match lease.kind {
        CleanupTaskKind::DeltaCompaction => 0u8,
        CleanupTaskKind::SegmentCompaction => 1u8,
    };
    let start = fbb.start_table();
    fbb.push_slot_always::<u8>(fvt(0), kind_u8);
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(fvt(1), client_id_wip);
    fbb.push_slot_always::<i64>(fvt(2), lease.lease_until);
    fbb.end_table(start)
}

fn serialize_checkpoint_fb(cp: &CheckpointData) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::with_capacity(512);

    let note_wip = cp.note.as_deref().map(|n| fbb.create_string(n));

    let leases_wip = if !cp.superblock.cleanup_leases.is_empty() {
        let lease_wips: Vec<flatbuffers::WIPOffset<_>> = cp
            .superblock
            .cleanup_leases
            .iter()
            .map(|l| build_cleanup_lease_fb(&mut fbb, l))
            .collect();
        Some(fbb.create_vector(&lease_wips))
    } else {
        None
    };

    let sb = &cp.superblock;
    let state_u8 = match sb.state {
        FilesystemState::Clean => 0u8,
        FilesystemState::Dirty => 1u8,
    };

    let start = fbb.start_table();
    fbb.push_slot_always::<u32>(fvt(0), cp.version);
    fbb.push_slot_always::<i64>(fvt(1), cp.created_at_unix);
    if let Some(wip) = note_wip {
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(fvt(2), wip);
    }
    fbb.push_slot_always::<u64>(fvt(3), sb.generation);
    fbb.push_slot_always::<u64>(fvt(4), sb.next_inode);
    fbb.push_slot_always::<u64>(fvt(5), sb.next_segment);
    fbb.push_slot_always::<u64>(fvt(6), sb.shard_size);
    fbb.push_slot_always::<u32>(fvt(7), sb.version);
    fbb.push_slot_always::<u8>(fvt(8), state_u8);
    if let Some(wip) = leases_wip {
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(fvt(9), wip);
    }
    let root = fbb.end_table(start);
    fbb.finish_minimal(root);

    let fb = fbb.finished_data();
    let mut out = Vec::with_capacity(CHECKPOINT_FB_MAGIC.len() + fb.len());
    out.extend_from_slice(CHECKPOINT_FB_MAGIC);
    out.extend_from_slice(fb);
    out
}

fn deserialize_checkpoint_fb(data: &[u8]) -> Result<CheckpointData> {
    let fb_data = data
        .strip_prefix(CHECKPOINT_FB_MAGIC.as_slice())
        .ok_or_else(|| anyhow::anyhow!("unsupported checkpoint encoding (missing OSGCP2 magic)"))?;

    let doc = unsafe { flatbuffers::root_unchecked::<flatbuffers::Table<'_>>(fb_data) };

    let version = unsafe { doc.get::<u32>(fvt(0), Some(0)) }.unwrap_or(0);
    let created_at_unix = unsafe { doc.get::<i64>(fvt(1), Some(0)) }.unwrap_or(0);
    let note = unsafe { doc.get::<flatbuffers::ForwardsUOffset<&str>>(fvt(2), None) }
        .map(|s| s.to_string());
    let generation = unsafe { doc.get::<u64>(fvt(3), Some(0)) }.unwrap_or(0);
    let next_inode = unsafe { doc.get::<u64>(fvt(4), Some(0)) }.unwrap_or(0);
    let next_segment = unsafe { doc.get::<u64>(fvt(5), Some(0)) }.unwrap_or(0);
    let shard_size = unsafe { doc.get::<u64>(fvt(6), Some(0)) }.unwrap_or(0);
    let sb_version = unsafe { doc.get::<u32>(fvt(7), Some(0)) }.unwrap_or(0);
    let state_u8 = unsafe { doc.get::<u8>(fvt(8), Some(0)) }.unwrap_or(0);
    let state = match state_u8 {
        1 => FilesystemState::Dirty,
        _ => FilesystemState::Clean,
    };

    let cleanup_leases = unsafe {
        doc.get::<flatbuffers::ForwardsUOffset<
            flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<flatbuffers::Table<'_>>>,
        >>(fvt(9), None)
    }
    .map(|vec| {
        (0..vec.len())
            .filter_map(|i| read_cleanup_lease_fb(vec.get(i)))
            .collect()
    })
    .unwrap_or_default();

    Ok(CheckpointData {
        version,
        created_at_unix,
        note,
        superblock: Superblock {
            generation,
            next_inode,
            next_segment,
            shard_size,
            version: sb_version,
            state,
            cleanup_leases,
        },
    })
}

fn read_cleanup_lease_fb(t: flatbuffers::Table<'_>) -> Option<CleanupLease> {
    let kind_u8 = unsafe { t.get::<u8>(fvt(0), Some(0)) }.unwrap_or(0);
    let kind = match kind_u8 {
        0 => CleanupTaskKind::DeltaCompaction,
        1 => CleanupTaskKind::SegmentCompaction,
        _ => return None,
    };
    let client_id =
        unsafe { t.get::<flatbuffers::ForwardsUOffset<&str>>(fvt(1), None) }?.to_string();
    let lease_until = unsafe { t.get::<i64>(fvt(2), Some(0)) }.unwrap_or(0);
    Some(CleanupLease {
        kind,
        client_id,
        lease_until,
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
            entry_ttl_secs: 5,
            home_prefix: "/home".to_string(),
            object_provider: ObjectStoreProvider::Local,
            bucket: None,
            region: None,
            endpoint: None,
            object_prefix: String::new(),
            telemetry_object_prefix: None,
            gcs_service_account: None,
            aws_allow_http: false,
            aws_force_path_style: false,
            source: None,
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
            fuse_fsname: "clawfs".to_string(),
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
