use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use futures::StreamExt;
use futures::future::try_join_all;
use libc::{ENOENT, ENOTDIR};
use log::{debug, info};
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::runtime::Handle;

use crate::config::{Config, ObjectStoreProvider};
use crate::inode::{
    FileStorage, InlinePayload, InlinePayloadCodec, InodeKind, InodeRecord, InodeShard,
    SegmentExtent,
};
use crate::segment::SegmentPointer;
use crate::superblock::Superblock;

/// How long an ENOENT result is served from the negative cache before we
/// re-check the backing store.  Short enough that new files created by the
/// same or another client become visible promptly.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_millis(1_500);

const SUPERBLOCK_FILE: &str = "superblock.bin";
const METADATA_FORMAT_VERSION: u32 = 1;
const METADATA_FB_MAGIC: &[u8] = b"OSGFB1";

fn serialize_metadata<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(value)?;
    let mut builder = FlatBufferBuilder::with_capacity(payload.len() + 64);
    let vector = builder.create_vector(payload.as_slice());
    builder.finish_minimal(vector);
    let fb = builder.finished_data();

    let mut out = Vec::with_capacity(METADATA_FB_MAGIC.len() + fb.len());
    out.extend_from_slice(METADATA_FB_MAGIC);
    out.extend_from_slice(fb);
    Ok(out)
}

fn deserialize_metadata<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let fb_bytes = bytes
        .strip_prefix(METADATA_FB_MAGIC)
        .ok_or_else(|| anyhow!("unsupported metadata encoding (missing OSGFB1 magic)"))?;
    let vector = flatbuffers::root::<flatbuffers::Vector<'_, u8>>(fb_bytes)
        .map_err(|err| anyhow!("flatbuffer decode failed: {err}"))?;
    let payload: Vec<u8> = vector.iter().collect();
    Ok(serde_json::from_slice(&payload)?)
}

// ── OSGFB2: schema-based FlatBuffers for shards and deltas ───────────────────
//
// Replaces the OSGFB1 format (JSON bytes wrapped in a FlatBuffer byte-vector).
// OSGFB2 uses proper FlatBuffer tables, storing Vec<u8> payloads as compact
// binary byte vectors instead of JSON integer arrays, giving ~3× smaller
// serialized size for inline-storage inodes.
//
// Schema (field index → VOffset = (index+2)*2):
//
// SegExtent table:
//   0(vt=4)  logical_offset: u64
//   1(vt=6)  segment_id:     u64
//   2(vt=8)  generation:     u64
//   3(vt=10) offset:         u64
//   4(vt=12) length:         u64
//
// InodeRecord table:
//    0(vt=4)  inode:         u64
//    1(vt=6)  parent:        u64
//    2(vt=8)  name:          string
//    3(vt=10) path:          string
//    4(vt=12) kind_tag:      u8   (0=File,1=Dir,2=Symlink,3=Tombstone)
//    5(vt=14) dir_keys:      [string]  (Directory only)
//    6(vt=16) dir_values:    [u64]     (Directory only)
//    7(vt=18) size:          u64
//    8(vt=20) mode:          u32
//    9(vt=22) uid:           u32
//   10(vt=24) gid:           u32
//   11(vt=26) atime_ns:      i64  (nanoseconds since Unix epoch)
//   12(vt=28) mtime_ns:      i64
//   13(vt=30) ctime_ns:      i64
//   14(vt=32) link_count:    u32
//   15(vt=34) rdev:          u32
//   16(vt=36) storage_tag:   u8   (0=Inline,1=InlineEncoded,2=Segments)
//   17(vt=38) storage_bytes: [u8] (Inline payload OR InlineEncoded.payload)
//   18(vt=40) storage_codec: u8   (InlineEncoded: 1=Lz4,2=ChaCha,3=Lz4ChaCha)
//   19(vt=42) storage_orig:  u64  (InlineEncoded original_len; 0 = absent)
//   20(vt=44) storage_nonce: [u8] (InlineEncoded nonce, 12 bytes)
//   21(vt=46) seg_extents:   [SegExtent] (Segments)
//
// StoredDocument table (covers both shard and delta):
//   0(vt=4) version:    u32
//   1(vt=6) generation: u64
//   2(vt=8) records:    [InodeRecord]
// ─────────────────────────────────────────────────────────────────────────────

const METADATA_FB2_MAGIC: &[u8] = b"OSGFB2";

/// VOffset for FlatBuffer table field at zero-based `index`: `(index + 2) * 2`.
#[inline(always)]
pub(crate) const fn fvt(index: u16) -> u16 {
    (index + 2) * 2
}

// ── Write path ────────────────────────────────────────────────────────────────

fn build_seg_extent_fb<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    ext: &SegmentExtent,
) -> WIPOffset<flatbuffers::TableFinishedWIPOffset> {
    let start = fbb.start_table();
    fbb.push_slot_always::<u64>(fvt(0), ext.logical_offset);
    fbb.push_slot_always::<u64>(fvt(1), ext.pointer.segment_id);
    fbb.push_slot_always::<u64>(fvt(2), ext.pointer.generation);
    fbb.push_slot_always::<u64>(fvt(3), ext.pointer.offset);
    fbb.push_slot_always::<u64>(fvt(4), ext.pointer.length);
    fbb.end_table(start)
}

pub(crate) fn build_inode_record_fb<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    record: &InodeRecord,
) -> WIPOffset<flatbuffers::TableFinishedWIPOffset> {
    // All reference types (strings, vectors, nested tables) must be built
    // BEFORE start_table() since they write into the same buffer.
    let name_wip = fbb.create_string(&record.name);
    let path_wip = fbb.create_string(&record.path);

    let (kind_tag, dir_keys_wip, dir_values_wip) = match &record.kind {
        InodeKind::File => (0u8, None, None),
        InodeKind::Directory { children } => {
            let keys: Vec<WIPOffset<&str>> =
                children.keys().map(|k| fbb.create_string(k)).collect();
            let keys_wip = fbb.create_vector(&keys);
            let vals: Vec<u64> = children.values().copied().collect();
            let vals_wip = fbb.create_vector(&vals);
            (1u8, Some(keys_wip), Some(vals_wip))
        }
        InodeKind::Symlink => (2u8, None, None),
        InodeKind::Tombstone => (3u8, None, None),
    };

    let (storage_tag, storage_bytes_wip, codec_u8, orig_len, nonce_wip, extents_wip) =
        match &record.storage {
            FileStorage::Inline(bytes) => {
                let wip = fbb.create_vector(bytes.as_slice());
                (0u8, Some(wip), 0u8, 0u64, None, None)
            }
            FileStorage::InlineEncoded(p) => {
                let bytes_wip = fbb.create_vector(p.payload.as_slice());
                let nonce_wip = p.nonce.as_ref().map(|n| fbb.create_vector(n.as_slice()));
                let codec_u8 = match p.codec {
                    InlinePayloadCodec::None => 0u8,
                    InlinePayloadCodec::Lz4 => 1u8,
                    InlinePayloadCodec::ChaCha20Poly1305 => 2u8,
                    InlinePayloadCodec::Lz4ChaCha20Poly1305 => 3u8,
                };
                (
                    1u8,
                    Some(bytes_wip),
                    codec_u8,
                    p.original_len.unwrap_or(0),
                    nonce_wip,
                    None,
                )
            }
            FileStorage::LegacySegment(ptr) => {
                let ext = SegmentExtent {
                    logical_offset: 0,
                    pointer: ptr.clone(),
                };
                let ext_wip = build_seg_extent_fb(fbb, &ext);
                let vec_wip = fbb.create_vector(&[ext_wip]);
                (2u8, None, 0u8, 0u64, None, Some(vec_wip))
            }
            FileStorage::Segments(extents) => {
                let ext_wips: Vec<WIPOffset<_>> = extents
                    .iter()
                    .map(|e| build_seg_extent_fb(fbb, e))
                    .collect();
                let vec_wip = fbb.create_vector(&ext_wips);
                (2u8, None, 0u8, 0u64, None, Some(vec_wip))
            }
        };

    let atime_ns = record.atime.unix_timestamp() * 1_000_000_000 + record.atime.nanosecond() as i64;
    let mtime_ns = record.mtime.unix_timestamp() * 1_000_000_000 + record.mtime.nanosecond() as i64;
    let ctime_ns = record.ctime.unix_timestamp() * 1_000_000_000 + record.ctime.nanosecond() as i64;

    let start = fbb.start_table();
    fbb.push_slot_always::<u64>(fvt(0), record.inode);
    fbb.push_slot_always::<u64>(fvt(1), record.parent);
    fbb.push_slot_always::<WIPOffset<_>>(fvt(2), name_wip);
    fbb.push_slot_always::<WIPOffset<_>>(fvt(3), path_wip);
    fbb.push_slot_always::<u8>(fvt(4), kind_tag);
    if let Some(wip) = dir_keys_wip {
        fbb.push_slot_always::<WIPOffset<_>>(fvt(5), wip);
    }
    if let Some(wip) = dir_values_wip {
        fbb.push_slot_always::<WIPOffset<_>>(fvt(6), wip);
    }
    fbb.push_slot_always::<u64>(fvt(7), record.size);
    fbb.push_slot_always::<u32>(fvt(8), record.mode);
    fbb.push_slot_always::<u32>(fvt(9), record.uid);
    fbb.push_slot_always::<u32>(fvt(10), record.gid);
    fbb.push_slot_always::<i64>(fvt(11), atime_ns);
    fbb.push_slot_always::<i64>(fvt(12), mtime_ns);
    fbb.push_slot_always::<i64>(fvt(13), ctime_ns);
    fbb.push_slot_always::<u32>(fvt(14), record.link_count);
    fbb.push_slot_always::<u32>(fvt(15), record.rdev);
    fbb.push_slot_always::<u8>(fvt(16), storage_tag);
    if let Some(wip) = storage_bytes_wip {
        fbb.push_slot_always::<WIPOffset<_>>(fvt(17), wip);
    }
    if codec_u8 != 0 {
        fbb.push_slot_always::<u8>(fvt(18), codec_u8);
    }
    if orig_len > 0 {
        fbb.push_slot_always::<u64>(fvt(19), orig_len);
    }
    if let Some(wip) = nonce_wip {
        fbb.push_slot_always::<WIPOffset<_>>(fvt(20), wip);
    }
    if let Some(wip) = extents_wip {
        fbb.push_slot_always::<WIPOffset<_>>(fvt(21), wip);
    }
    fbb.end_table(start)
}

/// Serialize `records` as an OSGFB2 FlatBuffer document (with magic prefix).
fn serialize_inodes_fb2<'a>(
    version: u32,
    generation: u64,
    records: impl IntoIterator<Item = &'a InodeRecord>,
) -> Vec<u8> {
    let records: Vec<&InodeRecord> = records.into_iter().collect();
    // Estimate capacity based on actual storage sizes to avoid reallocations.
    // Per record: ~200 bytes fixed overhead (vtable, table, scalar fields,
    // string length prefixes) + variable-length strings + storage payload.
    let estimated_cap: usize = records.iter().fold(256, |acc, r| {
        let storage_bytes = match &r.storage {
            FileStorage::Inline(bytes) => bytes.len(),
            FileStorage::InlineEncoded(p) => p.payload.len() + 16,
            FileStorage::Segments(exts) => exts.len() * 48,
            FileStorage::LegacySegment(_) => 48,
        };
        let children_bytes = match &r.kind {
            InodeKind::Directory { children } => {
                children.keys().map(|k| k.len() + 12).sum::<usize>() + children.len() * 8
            }
            _ => 0,
        };
        acc + 200 + r.name.len() + r.path.len() + storage_bytes + children_bytes
    });
    let mut fbb = FlatBufferBuilder::with_capacity(estimated_cap);

    let rec_wips: Vec<WIPOffset<_>> = records
        .iter()
        .map(|r| build_inode_record_fb(&mut fbb, r))
        .collect();
    let records_vec = fbb.create_vector(&rec_wips);

    let start = fbb.start_table();
    fbb.push_slot_always::<u32>(fvt(0), version);
    fbb.push_slot_always::<u64>(fvt(1), generation);
    fbb.push_slot_always::<WIPOffset<_>>(fvt(2), records_vec);
    let root = fbb.end_table(start);
    fbb.finish_minimal(root);

    let fb = fbb.finished_data();
    let mut out = Vec::with_capacity(METADATA_FB2_MAGIC.len() + fb.len());
    out.extend_from_slice(METADATA_FB2_MAGIC);
    out.extend_from_slice(fb);
    out
}

// ── Read path ─────────────────────────────────────────────────────────────────

fn read_seg_extent_fb2(t: flatbuffers::Table<'_>) -> SegmentExtent {
    // Safety: t was produced by our own OSGFB2 writer so the schema matches.
    let logical_offset = unsafe { t.get::<u64>(fvt(0), Some(0)) }.unwrap_or(0);
    let segment_id = unsafe { t.get::<u64>(fvt(1), Some(0)) }.unwrap_or(0);
    let generation = unsafe { t.get::<u64>(fvt(2), Some(0)) }.unwrap_or(0);
    let offset = unsafe { t.get::<u64>(fvt(3), Some(0)) }.unwrap_or(0);
    let length = unsafe { t.get::<u64>(fvt(4), Some(0)) }.unwrap_or(0);
    SegmentExtent {
        logical_offset,
        pointer: SegmentPointer {
            segment_id,
            generation,
            offset,
            length,
        },
    }
}

pub(crate) fn read_inode_record_fb2(t: flatbuffers::Table<'_>) -> Result<InodeRecord> {
    use flatbuffers::{ForwardsUOffset, Table, Vector};

    // Safety: t was produced by our own OSGFB2 writer so the schema matches.
    let inode = unsafe { t.get::<u64>(fvt(0), Some(0)) }.unwrap_or(0);
    let parent = unsafe { t.get::<u64>(fvt(1), Some(0)) }.unwrap_or(0);
    let name = unsafe { t.get::<ForwardsUOffset<&str>>(fvt(2), Some("")) }
        .unwrap_or("")
        .to_owned();
    let path = unsafe { t.get::<ForwardsUOffset<&str>>(fvt(3), Some("")) }
        .unwrap_or("")
        .to_owned();
    let kind_tag = unsafe { t.get::<u8>(fvt(4), Some(0)) }.unwrap_or(0);
    let size = unsafe { t.get::<u64>(fvt(7), Some(0)) }.unwrap_or(0);
    let mode = unsafe { t.get::<u32>(fvt(8), Some(0)) }.unwrap_or(0);
    let uid = unsafe { t.get::<u32>(fvt(9), Some(0)) }.unwrap_or(0);
    let gid = unsafe { t.get::<u32>(fvt(10), Some(0)) }.unwrap_or(0);
    let atime_ns = unsafe { t.get::<i64>(fvt(11), Some(0)) }.unwrap_or(0);
    let mtime_ns = unsafe { t.get::<i64>(fvt(12), Some(0)) }.unwrap_or(0);
    let ctime_ns = unsafe { t.get::<i64>(fvt(13), Some(0)) }.unwrap_or(0);
    let link_count = unsafe { t.get::<u32>(fvt(14), Some(0)) }.unwrap_or(0);
    let rdev = unsafe { t.get::<u32>(fvt(15), Some(0)) }.unwrap_or(0);
    let storage_tag = unsafe { t.get::<u8>(fvt(16), Some(0)) }.unwrap_or(0);
    let storage_codec_u8 = unsafe { t.get::<u8>(fvt(18), Some(0)) }.unwrap_or(0);
    let storage_orig_len = unsafe { t.get::<u64>(fvt(19), Some(0)) }.unwrap_or(0);

    let kind = match kind_tag {
        0 => InodeKind::File,
        1 => {
            let keys_vec = unsafe {
                t.get::<ForwardsUOffset<Vector<'_, ForwardsUOffset<&str>>>>(fvt(5), None)
            };
            let vals_vec = unsafe { t.get::<ForwardsUOffset<Vector<'_, u64>>>(fvt(6), None) };
            let mut children = BTreeMap::new();
            if let (Some(kv), Some(vv)) = (keys_vec, vals_vec) {
                for i in 0..kv.len().min(vv.len()) {
                    children.insert(kv.get(i).to_owned(), vv.get(i));
                }
            }
            InodeKind::Directory {
                children: Arc::new(children),
            }
        }
        2 => InodeKind::Symlink,
        3 => InodeKind::Tombstone,
        v => anyhow::bail!("fb2: unknown kind_tag {v}"),
    };

    let storage = match storage_tag {
        0 => {
            let bytes =
                unsafe { t.get::<ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(fvt(17), None) }
                    .map(|v| v.bytes().to_vec())
                    .unwrap_or_default();
            FileStorage::Inline(bytes)
        }
        1 => {
            let payload =
                unsafe { t.get::<ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(fvt(17), None) }
                    .map(|v| v.bytes().to_vec())
                    .unwrap_or_default();
            let nonce: Option<[u8; 12]> =
                unsafe { t.get::<ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(fvt(20), None) }
                    .and_then(|v| {
                        let b = v.bytes();
                        if b.len() == 12 {
                            let mut arr = [0u8; 12];
                            arr.copy_from_slice(b);
                            Some(arr)
                        } else {
                            None
                        }
                    });
            let codec = match storage_codec_u8 {
                0 => InlinePayloadCodec::None,
                1 => InlinePayloadCodec::Lz4,
                2 => InlinePayloadCodec::ChaCha20Poly1305,
                3 => InlinePayloadCodec::Lz4ChaCha20Poly1305,
                v => anyhow::bail!("fb2: unknown codec {v}"),
            };
            let original_len = (storage_orig_len > 0).then_some(storage_orig_len);
            FileStorage::InlineEncoded(InlinePayload {
                codec,
                payload,
                original_len,
                nonce,
            })
        }
        2 => {
            let exts_vec = unsafe {
                t.get::<ForwardsUOffset<flatbuffers::Vector<'_, ForwardsUOffset<Table<'_>>>>>(
                    fvt(21),
                    None,
                )
            };
            let extents = match exts_vec {
                None => Vec::new(),
                Some(ev) => (0..ev.len())
                    .map(|i| read_seg_extent_fb2(ev.get(i)))
                    .collect(),
            };
            FileStorage::Segments(extents)
        }
        v => anyhow::bail!("fb2: unknown storage_tag {v}"),
    };

    let atime = time::OffsetDateTime::from_unix_timestamp_nanos(atime_ns as i128)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    let mtime = time::OffsetDateTime::from_unix_timestamp_nanos(mtime_ns as i128)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    let ctime = time::OffsetDateTime::from_unix_timestamp_nanos(ctime_ns as i128)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);

    Ok(InodeRecord {
        inode,
        parent,
        name,
        path,
        kind,
        size,
        mode,
        uid,
        gid,
        atime,
        mtime,
        ctime,
        link_count,
        rdev,
        storage,
    })
}

fn deserialize_fb2_document(data: &[u8]) -> Result<(u32, u64, Vec<InodeRecord>)> {
    use flatbuffers::{ForwardsUOffset, Table, Vector};

    // Safety: called only after verifying OSGFB2 magic; data was written by our writer.
    let doc = unsafe { flatbuffers::root_unchecked::<Table<'_>>(data) };
    let version = unsafe { doc.get::<u32>(fvt(0), Some(0)) }.unwrap_or(0);
    let generation = unsafe { doc.get::<u64>(fvt(1), Some(0)) }.unwrap_or(0);
    let records_vec =
        unsafe { doc.get::<ForwardsUOffset<Vector<'_, ForwardsUOffset<Table<'_>>>>>(fvt(2), None) };

    let records = match records_vec {
        None => Vec::new(),
        Some(rv) => (0..rv.len())
            .map(|i| read_inode_record_fb2(rv.get(i)))
            .collect::<Result<Vec<_>>>()?,
    };
    Ok((version, generation, records))
}

/// Deserialize a shard, supporting both OSGFB1 (JSON) and OSGFB2 (schema FlatBuffers).
fn deserialize_shard(bytes: &[u8]) -> Result<StoredShard> {
    if let Some(fb2_bytes) = bytes.strip_prefix(METADATA_FB2_MAGIC) {
        let (version, generation, records) = deserialize_fb2_document(fb2_bytes)?;
        let entries = records.into_iter().map(|r| (r.inode, r)).collect();
        Ok(StoredShard {
            version,
            generation,
            entries,
        })
    } else {
        deserialize_metadata::<StoredShard>(bytes)
    }
}

/// Deserialize a delta, supporting both OSGFB1 (JSON) and OSGFB2 (schema FlatBuffers).
fn deserialize_delta(bytes: &[u8]) -> Result<StoredDelta> {
    if let Some(fb2_bytes) = bytes.strip_prefix(METADATA_FB2_MAGIC) {
        let (version, generation, records) = deserialize_fb2_document(fb2_bytes)?;
        Ok(StoredDelta {
            version,
            generation,
            records,
        })
    } else {
        deserialize_metadata::<StoredDelta>(bytes)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredDelta {
    version: u32,
    generation: u64,
    records: Vec<InodeRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredShard {
    version: u32,
    generation: u64,
    entries: Vec<(u64, InodeRecord)>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredSuperblock {
    version: u32,
    block: Superblock,
}

#[derive(Debug, Clone)]
pub struct VersionedSuperblock {
    pub block: Superblock,
    pub version: String,
}

#[derive(Clone)]
struct CacheEntry {
    record: InodeRecord,
    refreshed: Instant,
    /// Generation at which this record was committed.  Used to prevent stale
    /// shard reloads from overwriting a fresher cache entry.
    generation: u64,
}

#[derive(Clone)]
struct ShardEntry {
    shard: InodeShard,
    generation: u64,
}

pub struct MetadataStore {
    store: Arc<dyn ObjectStore>,
    root_prefix: String,
    shard_size: u64,
    cache: Mutex<HashMap<u64, CacheEntry>>,
    shards: Mutex<HashMap<u64, ShardEntry>>,
    last_delta_generation: Mutex<u64>,
    log_storage_io: bool,
    /// Short-lived cache of inode numbers known not to exist.  Avoids
    /// repeated shard loads for ENOENT lookups (git, cargo, ripgrep…).
    negative_cache: Mutex<HashMap<u64, Instant>>,
    handle: Handle,
}

impl MetadataStore {
    pub async fn new(config: &Config, handle: Handle) -> Result<Self> {
        let (store, prefix) = create_object_store(config)?;

        let store = Self {
            store,
            root_prefix: prefix,
            shard_size: config.shard_size,
            cache: Mutex::new(HashMap::new()),
            shards: Mutex::new(HashMap::new()),
            last_delta_generation: Mutex::new(0),
            log_storage_io: config.log_storage_io,
            negative_cache: Mutex::new(HashMap::new()),
            handle,
        };
        store.load_latest_imaps().await?;
        Ok(store)
    }

    fn log_backing(&self, args: fmt::Arguments<'_>) {
        if self.log_storage_io {
            info!(target: "backing", "{}", args);
        } else {
            debug!(target: "backing", "{}", args);
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    fn superblock_path(&self) -> ObjectPath {
        let base = self.root_prefix.trim_matches('/');
        if base.is_empty() {
            ObjectPath::from(format!("metadata/{}", SUPERBLOCK_FILE))
        } else {
            ObjectPath::from(format!("{}/metadata/{}", base, SUPERBLOCK_FILE))
        }
    }

    fn imap_prefix(&self) -> ObjectPath {
        let base = self.root_prefix.trim_matches('/');
        if base.is_empty() {
            ObjectPath::from("metadata/imaps")
        } else {
            ObjectPath::from(format!("{}/metadata/imaps", base))
        }
    }

    fn delta_prefix(&self) -> ObjectPath {
        let base = self.root_prefix.trim_matches('/');
        if base.is_empty() {
            ObjectPath::from("metadata/imap_deltas")
        } else {
            ObjectPath::from(format!("{}/metadata/imap_deltas", base))
        }
    }

    pub async fn load_superblock(&self) -> Result<Option<VersionedSuperblock>> {
        let path = self.superblock_path();
        let result = self.store.get(&path).await;
        match result {
            Ok(get_result) => {
                let version = get_result.meta.e_tag.clone().unwrap_or_else(|| {
                    get_result
                        .meta
                        .last_modified
                        .timestamp_nanos_opt()
                        .unwrap_or(0)
                        .to_string()
                });
                let bytes = get_result.bytes().await?;
                let stored: StoredSuperblock = deserialize_metadata(&bytes)?;
                anyhow::ensure!(
                    stored.version == METADATA_FORMAT_VERSION,
                    "unsupported superblock format version {}",
                    stored.version
                );
                Ok(Some(VersionedSuperblock {
                    block: stored.block,
                    version,
                }))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(anyhow::Error::from(e).context(format!("reading superblock {}", path))),
        }
    }

    pub async fn store_superblock(&self, sb: &Superblock) -> Result<String> {
        let stored = StoredSuperblock {
            version: METADATA_FORMAT_VERSION,
            block: sb.clone(),
        };
        let bytes = serialize_metadata(&stored)?;
        let path = self.superblock_path();
        let put_result = self
            .store
            .put(&path, PutPayload::from_bytes(Bytes::from(bytes)))
            .await?;
        self.log_backing(format_args!(
            "synced backing file path={} type=superblock generation={}",
            path, sb.generation
        ));
        Ok(put_result.e_tag.unwrap_or_default())
    }

    pub async fn store_superblock_conditional(
        &self,
        sb: &Superblock,
        expected_version: &str,
    ) -> Result<String> {
        let stored = StoredSuperblock {
            version: METADATA_FORMAT_VERSION,
            block: sb.clone(),
        };
        let bytes = serialize_metadata(&stored)?;
        let path = self.superblock_path();

        let opts = object_store::PutOptions {
            mode: object_store::PutMode::Update(object_store::UpdateVersion {
                e_tag: Some(expected_version.to_string()),
                version: None,
            }),
            ..Default::default()
        };

        let result = self
            .store
            .put_opts(&path, PutPayload::from_bytes(Bytes::from(bytes)), opts)
            .await;

        match result {
            Ok(put_result) => {
                self.log_backing(format_args!(
                    "synced backing file path={} type=superblock generation={} (conditional if-match={})",
                    path, sb.generation, expected_version
                ));
                Ok(put_result.e_tag.unwrap_or_default())
            }
            Err(object_store::Error::NotImplemented) => {
                // Fallback for stores that don't support conditional put (like LocalFileSystem)
                // Note: This loses atomicity guarantees in concurrent environments.
                self.store_superblock(sb).await
            }
            Err(e) => Err(anyhow::Error::from(e).context(format!(
                "conditional put failed for superblock at version {}",
                expected_version
            ))),
        }
    }

    pub async fn compare_and_swap_superblock(
        &self,
        expected_generation: u64,
        sb: &Superblock,
    ) -> Result<()> {
        let current = self.load_superblock().await?;
        if let Some(existing) = current {
            if existing.block.generation != expected_generation {
                return Err(anyhow!(
                    "superblock generation mismatch: expected {}, found {}",
                    expected_generation,
                    existing.block.generation
                ));
            }
            self.store_superblock_conditional(sb, &existing.version)
                .await?;
        } else if expected_generation != 0 {
            return Err(anyhow!(
                "superblock missing while expecting generation {}",
                expected_generation
            ));
        } else {
            // Bootstrap: first write.
            let stored = StoredSuperblock {
                version: METADATA_FORMAT_VERSION,
                block: sb.clone(),
            };
            let bytes = serialize_metadata(&stored)?;
            let payload = Bytes::from(bytes);
            let path = self.superblock_path();

            let opts = object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            };
            let result = self
                .store
                .put_opts(&path, PutPayload::from_bytes(payload.clone()), opts)
                .await;

            match result {
                Ok(_) => {}
                Err(object_store::Error::NotImplemented) => {
                    self.store
                        .put(&path, PutPayload::from_bytes(payload))
                        .await?;
                }
                Err(e) => {
                    return Err(
                        anyhow::Error::from(e).context("initial superblock bootstrap failed")
                    );
                }
            }
        }
        Ok(())
    }

    /// Returns the committed record for this inode if it is present in the
    /// positive in-memory cache.  Does NOT trigger a shard reload and does NOT
    /// interact with the negative cache.  Safe to call from synchronous
    /// contexts (e.g. inside flush_pending under flush_lock) where callers
    /// only need extents that were committed by a prior flush.
    pub fn get_cached_inode(&self, inode: u64) -> Option<InodeRecord> {
        self.cache.lock().get(&inode).map(|e| e.record.clone())
    }

    /// Fast-path child lookup that avoids cloning the parent inode record.
    /// Returns `Some(result)` when the parent is present in the positive cache,
    /// otherwise `None` so callers can fall back to normal load/reload logic.
    pub fn lookup_cached_child(
        &self,
        parent: u64,
        name: &str,
    ) -> Option<std::result::Result<u64, i32>> {
        let cache = self.cache.lock();
        let entry = cache.get(&parent)?;
        if matches!(entry.record.kind, InodeKind::Tombstone) {
            return Some(Err(ENOENT));
        }
        match &entry.record.kind {
            InodeKind::Directory { children } => Some(children.get(name).copied().ok_or(ENOENT)),
            _ => Some(Err(ENOTDIR)),
        }
    }

    pub async fn get_inode_with_ttl(
        &self,
        inode: u64,
        file_ttl: Duration,
        dir_ttl: Duration,
    ) -> Result<Option<InodeRecord>> {
        let ttl = |rec: &InodeRecord| if rec.is_dir() { dir_ttl } else { file_ttl };

        // Fast path: positive cache hit.
        if let Some(entry) = self.cache.lock().get(&inode).cloned() {
            let allowed = ttl(&entry.record);
            if allowed.is_zero() || entry.refreshed.elapsed() <= allowed {
                return Ok(Some(entry.record));
            }
        }

        // Negative cache: skip shard load if we recently confirmed ENOENT.
        {
            let neg = self.negative_cache.lock();
            if let Some(&expires) = neg.get(&inode)
                && Instant::now() < expires
            {
                return Ok(None);
            }
        }

        self.reload_shard_for_inode(inode).await?;

        let result = self
            .cache
            .lock()
            .get(&inode)
            .map(|entry| entry.record.clone());

        // Populate the negative cache on confirmed ENOENT.
        if result.is_none() {
            self.negative_cache
                .lock()
                .insert(inode, Instant::now() + NEGATIVE_CACHE_TTL);
        }

        Ok(result)
    }

    pub async fn get_inode(&self, inode: u64) -> Result<Option<InodeRecord>> {
        self.get_inode_with_ttl(inode, Duration::ZERO, Duration::ZERO)
            .await
    }

    pub async fn persist_inode(
        &self,
        record: &InodeRecord,
        generation: u64,
        shard_size: u64,
    ) -> Result<()> {
        self.persist_inodes_batch(std::slice::from_ref(record), generation, shard_size, 1)
            .await
    }

    pub async fn persist_inodes_batch(
        &self,
        records: &[InodeRecord],
        generation: u64,
        shard_size: u64,
        delta_batch: usize,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        // Phase 1: update in-memory caches + shards under lock.
        // Normalize once and share between cache and shard to halve
        // normalization overhead.
        let mut touched_shard_ids = HashSet::new();
        {
            let mut cache = self.cache.lock();
            let mut shards = self.shards.lock();
            let mut neg = self.negative_cache.lock();
            for record in records {
                neg.remove(&record.inode);
                let shard_id = record.shard_index(shard_size);
                let entry = shards.entry(shard_id).or_insert_with(|| ShardEntry {
                    shard: InodeShard::new(shard_id),
                    generation,
                });
                entry.generation = generation;
                touched_shard_ids.insert(shard_id);
                if matches!(record.kind, InodeKind::Tombstone) {
                    entry.shard.inodes.remove(&record.inode);
                    cache.insert(
                        record.inode,
                        CacheEntry::with_generation(record.clone(), generation),
                    );
                } else {
                    // Normalize once, clone for cache, move into shard.
                    let mut normalized = record.clone();
                    normalized.normalize_storage();
                    let for_cache = normalized.clone();
                    entry.shard.inodes.insert(normalized.inode, normalized);
                    cache.insert(
                        record.inode,
                        CacheEntry {
                            record: for_cache,
                            refreshed: Instant::now(),
                            generation,
                        },
                    );
                }
            }
        }

        // Phases 2+3: pre-serialize touched shards and delta records
        // concurrently using scoped threads. Shard serialization holds the
        // shards lock on the current thread while delta serialization runs
        // on a separate thread (no lock needed).
        const MAX_DELTA_SINGLE_FILE: usize = 50_000;
        let delta_prefix = self.delta_prefix();
        let imap_prefix = self.imap_prefix();

        #[allow(clippy::type_complexity)]
        let (shard_writes, delta_writes): (
            Vec<(ObjectPath, Bytes, u64, usize)>,
            Vec<(ObjectPath, Bytes, usize)>,
        ) = std::thread::scope(|scope| {
            // Spawn delta serialization on a separate thread.
            let delta_handle = scope.spawn(|| -> Vec<(ObjectPath, Bytes, usize)> {
                if records.is_empty() {
                    return vec![];
                }
                if records.len() <= MAX_DELTA_SINGLE_FILE {
                    let bloom = records
                        .iter()
                        .fold(0u128, |mask, r| mask | bloom_mask(r.inode));
                    let filename = format!("d_{generation:020}_{:032x}.bin", bloom);
                    let path = delta_prefix.child(filename.as_str());
                    let data =
                        serialize_inodes_fb2(METADATA_FORMAT_VERSION, generation, records.iter());
                    vec![(path, Bytes::from(data), records.len())]
                } else {
                    let chunk_size = delta_batch.max(1);
                    records
                        .chunks(chunk_size)
                        .filter(|c| !c.is_empty())
                        .map(|chunk_records| {
                            let bloom = chunk_records
                                .iter()
                                .fold(0u128, |mask, r| mask | bloom_mask(r.inode));
                            let filename = format!("d_{generation:020}_{:032x}.bin", bloom);
                            let path = delta_prefix.child(filename.as_str());
                            let data = serialize_inodes_fb2(
                                METADATA_FORMAT_VERSION,
                                generation,
                                chunk_records.iter(),
                            );
                            (path, Bytes::from(data), chunk_records.len())
                        })
                        .collect()
                }
            });

            // Serialize shards under lock (sequential — the delta thread
            // already runs in parallel with this block).
            let shard_writes: Vec<(ObjectPath, Bytes, u64, usize)> = {
                let shards = self.shards.lock();
                let mut shard_ids: Vec<u64> = touched_shard_ids.into_iter().collect();
                shard_ids.sort_unstable();

                shard_ids
                    .iter()
                    .filter_map(|&shard_id| {
                        let entry = shards.get(&shard_id)?;
                        let data = serialize_inodes_fb2(
                            METADATA_FORMAT_VERSION,
                            generation,
                            entry.shard.inodes.values(),
                        );
                        let count = entry.shard.inodes.len();
                        let filename = format!("i_{generation:020}_{:08x}.bin", shard_id);
                        let path = imap_prefix.child(filename.as_str());
                        Some((path, Bytes::from(data), shard_id, count))
                    })
                    .collect()
            };

            let delta_writes = delta_handle.join().expect("delta serialize thread");
            (shard_writes, delta_writes)
        });

        // Update last_delta_generation before issuing writes.
        if !delta_writes.is_empty() {
            let mut guard = self.last_delta_generation.lock();
            *guard = (*guard).max(generation);
        }

        // Phase 4: write all shards and deltas in parallel.
        // Collect into a uniform Vec of boxed futures so shard and delta futures
        // (which have different concrete types) can be driven together.
        let log_storage_io = self.log_storage_io;
        let store = self.store.clone();
        let mut all_futs: Vec<
            std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>,
        > = Vec::with_capacity(shard_writes.len() + delta_writes.len());

        for (path, data, shard_id, count) in shard_writes {
            let store = store.clone();
            all_futs.push(Box::pin(async move {
                store
                    .put(&path, PutPayload::from_bytes(data))
                    .await
                    .with_context(|| {
                        format!("writing imap shard={shard_id} generation={generation}")
                    })?;
                if log_storage_io {
                    info!(
                        target: "backing",
                        "synced backing file path={} type=imap shard={} generation={} entries={}",
                        path, shard_id, generation, count
                    );
                } else {
                    debug!(
                        target: "backing",
                        "synced backing file path={} type=imap shard={} generation={} entries={}",
                        path, shard_id, generation, count
                    );
                }
                Ok(())
            }));
        }

        for (path, data, record_count) in delta_writes {
            let store = store.clone();
            all_futs.push(Box::pin(async move {
                store
                    .put(&path, PutPayload::from_bytes(data))
                    .await
                    .with_context(|| format!("writing delta generation={generation}"))?;
                if log_storage_io {
                    info!(
                        target: "backing",
                        "synced backing file path={} type=delta generation={} records={}",
                        path, generation, record_count
                    );
                } else {
                    debug!(
                        target: "backing",
                        "synced backing file path={} type=delta generation={} records={}",
                        path, generation, record_count
                    );
                }
                Ok(())
            }));
        }

        try_join_all(all_futs).await?;
        Ok(())
    }

    pub async fn sync_metadata_writes(&self) -> Result<()> {
        // Object store writes (put) are atomic and durable upon success.
        // No directory fsync needed.
        Ok(())
    }

    pub async fn remove_inode(&self, inode: u64, generation: u64, shard_size: u64) -> Result<()> {
        self.cache.lock().remove(&inode);
        self.negative_cache.lock().remove(&inode);
        let shard_id = shard_for_inode(inode, shard_size);
        {
            let mut shards = self.shards.lock();
            if let Some(entry) = shards.get_mut(&shard_id) {
                entry.shard.inodes.remove(&inode);
                entry.generation = generation;
            }
        }
        self.write_shard_ref(generation, shard_id).await?;
        let tombstone = InodeRecord::tombstone(inode);
        self.write_delta_ref(generation, &[tombstone]).await
    }

    async fn write_shard_ref(&self, generation: u64, shard_id: u64) -> Result<()> {
        let filename = format!("i_{generation:020}_{:08x}.bin", shard_id);
        let path = self.imap_prefix().child(filename.as_str());
        let (data, count) = {
            let shards = self.shards.lock();
            let entry = match shards.get(&shard_id) {
                Some(e) => e,
                None => return Ok(()),
            };
            (
                serialize_inodes_fb2(
                    METADATA_FORMAT_VERSION,
                    generation,
                    entry.shard.inodes.values(),
                ),
                entry.shard.inodes.len(),
            )
        };
        self.store
            .put(&path, PutPayload::from_bytes(Bytes::from(data)))
            .await?;
        self.log_backing(format_args!(
            "synced backing file path={} type=imap shard={} generation={} entries={}",
            path, shard_id, generation, count
        ));
        Ok(())
    }

    async fn write_delta_ref(&self, generation: u64, records: &[InodeRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let bloom = records
            .iter()
            .fold(0u128, |mask, record| mask | bloom_mask(record.inode));
        let filename = format!("d_{generation:020}_{:032x}.bin", bloom);
        let path = self.delta_prefix().child(filename.as_str());
        let data = serialize_inodes_fb2(METADATA_FORMAT_VERSION, generation, records.iter());
        {
            let mut guard = self.last_delta_generation.lock();
            *guard = (*guard).max(generation);
        }
        self.store
            .put(&path, PutPayload::from_bytes(Bytes::from(data)))
            .await?;
        self.log_backing(format_args!(
            "synced backing file path={} type=delta generation={} records={}",
            path,
            generation,
            records.len()
        ));
        Ok(())
    }

    async fn load_latest_imaps(&self) -> Result<()> {
        let mut candidates: HashMap<u64, Vec<(u64, ObjectPath)>> = HashMap::new();
        let prefix = self.imap_prefix();

        // Ensure prefix exists (optional check or just list)
        // Note: list(Some(prefix)) works even if prefix is "virtual" directory

        let mut stream = self.store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            let meta = item?;
            let name = meta.location.filename().unwrap_or_default();
            if let Some((generation, shard_id)) = parse_imap_filename(name) {
                candidates
                    .entry(shard_id)
                    .or_default()
                    .push((generation, meta.location));
            }
        }

        let mut loaded_shards = Vec::new();
        let mut max_generation = 0;

        for (shard_id, mut files) in candidates {
            files.sort_by(|a, b| b.0.cmp(&a.0));
            let Some((generation, path)) = files.into_iter().next() else {
                continue;
            };
            let bytes = self.store.get(&path).await?.bytes().await?;
            let stored: StoredShard = deserialize_shard(&bytes).with_context(|| {
                format!(
                    "decoding newest shard failed shard={} generation={} path={}",
                    shard_id, generation, path
                )
            })?;
            anyhow::ensure!(
                stored.version == METADATA_FORMAT_VERSION,
                "unsupported shard version {} for shard={} generation={} path={}",
                stored.version,
                shard_id,
                generation,
                path
            );
            loaded_shards.push((shard_id, stored));
        }

        let mut cache = self.cache.lock();
        let mut shard_map = self.shards.lock();
        for (shard_id, stored) in loaded_shards {
            let mut shard = InodeShard::new(shard_id);
            for (ino, record) in stored.entries {
                shard.inodes.insert(ino, record);
            }
            for (ino, record) in &shard.inodes {
                cache.insert(
                    *ino,
                    CacheEntry::with_generation(record.clone(), stored.generation),
                );
            }
            max_generation = max_generation.max(stored.generation);
            shard_map.insert(
                shard_id,
                ShardEntry {
                    shard,
                    generation: stored.generation,
                },
            );
        }
        *self.last_delta_generation.lock() = max_generation;
        Ok(())
    }

    pub fn apply_external_deltas(&self) -> Result<Vec<InodeRecord>> {
        // This needs to be async or blocking. Since we are in a non-async method
        // that's often called from blocking context (or we need to change signature),
        // we use the handle to block.
        // However, `spawn_metadata_poller` calls this inside `spawn_blocking`.
        // So we can use `self.handle.block_on`.

        self.handle.block_on(async {
            let mut newest = *self.last_delta_generation.lock();
            let mut files = Vec::new();
            let prefix = self.delta_prefix();

            let mut stream = self.store.list(Some(&prefix));
            while let Some(item) = stream.next().await {
                let meta = item?;
                let name = meta.location.filename().unwrap_or_default();
                if let Some(generation) = parse_delta_filename(name)
                    && generation > newest
                {
                    files.push((generation, meta.location));
                }
            }

            files.sort_by_key(|(generation, _)| *generation);
            let mut updated_records = Vec::new();
            for (generation, path) in files {
                let bytes = self.store.get(&path).await?.bytes().await?;
                let stored: StoredDelta = deserialize_delta(&bytes)?;
                anyhow::ensure!(
                    stored.version == METADATA_FORMAT_VERSION,
                    "unsupported delta version {}",
                    stored.version
                );
                for record in stored.records {
                    if matches!(record.kind, crate::inode::InodeKind::Tombstone) {
                        self.cache.lock().remove(&record.inode);
                    } else {
                        self.cache.lock().insert(
                            record.inode,
                            CacheEntry::with_generation(record.clone(), generation),
                        );
                        updated_records.push(record.clone());
                    }
                }
                newest = newest.max(generation);
            }
            *self.last_delta_generation.lock() = newest;
            Ok(updated_records)
        })
    }

    pub fn delta_file_count(&self) -> Result<usize> {
        self.handle.block_on(async {
            let prefix = self.delta_prefix();
            let mut count = 0;
            let mut stream = self.store.list(Some(&prefix));
            while let Some(item) = stream.next().await {
                let _ = item?;
                count += 1;
            }
            Ok(count)
        })
    }

    pub fn prune_deltas(&self, keep: usize) -> Result<usize> {
        self.handle.block_on(async {
            let prefix = self.delta_prefix();
            let mut files: Vec<(u64, ObjectPath)> = Vec::new();

            let mut stream = self.store.list(Some(&prefix));
            while let Some(item) = stream.next().await {
                let meta = item?;
                let name = meta.location.filename().unwrap_or_default();
                if let Some(generation) = parse_delta_filename(name) {
                    files.push((generation, meta.location));
                }
            }

            files.sort_by_key(|(generation, _)| *generation);
            let mut removed = 0;
            if files.len() > keep {
                let excess = files.len() - keep;
                for (_, path) in files.into_iter().take(excess) {
                    self.store.delete(&path).await?;
                    removed += 1;
                }
            }
            Ok(removed)
        })
    }

    pub fn segment_candidates(&self, max: usize) -> Result<Vec<InodeRecord>> {
        let mut candidates = Vec::new();
        let shards = self.shards.lock();
        for entry in shards.values() {
            for record in entry.shard.inodes.values() {
                if matches!(
                    record.storage,
                    FileStorage::LegacySegment(_) | FileStorage::Segments(_)
                ) {
                    candidates.push(record.clone());
                }
            }
        }
        drop(shards);
        candidates.sort_by_key(|record| {
            record
                .segment_pointer()
                .map(|ptr| (ptr.generation, ptr.segment_id))
                .unwrap_or((u64::MAX, u64::MAX))
        });
        if candidates.len() > max {
            candidates.truncate(max);
        }
        Ok(candidates)
    }

    async fn reload_shard_for_inode(&self, inode: u64) -> Result<()> {
        let shard_id = shard_for_inode(inode, self.shard_size);
        self.reload_shard(shard_id).await
    }

    async fn reload_shard(&self, shard_id: u64) -> Result<()> {
        let mut candidates = Vec::new();
        let prefix = self.imap_prefix();

        let mut stream = self.store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            let meta = item?;
            let name = meta.location.filename().unwrap_or_default();
            if let Some((generation, shard)) = parse_imap_filename(name)
                && shard == shard_id
            {
                candidates.push((generation, meta.location));
            }
        }
        candidates.sort_by(|a, b| b.0.cmp(&a.0));

        if let Some((generation, path)) = candidates.into_iter().next() {
            let bytes = self.store.get(&path).await?.bytes().await?;
            let stored: StoredShard = deserialize_shard(&bytes).with_context(|| {
                format!(
                    "decoding newest shard failed shard={} generation={} path={}",
                    shard_id, generation, path
                )
            })?;
            anyhow::ensure!(
                stored.version == METADATA_FORMAT_VERSION,
                "unsupported shard version {} for shard={} generation={} path={}",
                stored.version,
                shard_id,
                generation,
                path
            );
            let mut shard = InodeShard::new(shard_id);
            for (ino, record) in stored.entries {
                shard.inodes.insert(ino, record);
            }
            let mut cache = self.cache.lock();
            for (ino, record) in &shard.inodes {
                // Only update the cache if this shard reload is at least as
                // fresh as the existing entry.  A concurrent
                // `persist_inodes_batch` may have already committed a newer
                // generation between our directory listing and this point;
                // overwriting that entry would revert directory children or
                // file storage to a stale snapshot.
                let dominated = cache
                    .get(ino)
                    .map(|existing| existing.generation > generation)
                    .unwrap_or(false);
                if !dominated {
                    cache.insert(
                        *ino,
                        CacheEntry::with_generation(record.clone(), generation),
                    );
                }
            }
            let mut shard_map = self.shards.lock();
            let dominated_shard = shard_map
                .get(&shard_id)
                .map(|existing| existing.generation > generation)
                .unwrap_or(false);
            if !dominated_shard {
                shard_map.insert(shard_id, ShardEntry { shard, generation });
            }
        }
        Ok(())
    }
}

fn shard_for_inode(inode: u64, shard_size: u64) -> u64 {
    if shard_size == 0 {
        0
    } else {
        inode / shard_size
    }
}

fn bloom_mask(inode: u64) -> u128 {
    let mut hash = inode;
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    let bit = (hash & 0x7f) as u32;
    1u128 << bit
}

fn parse_imap_filename(name: &str) -> Option<(u64, u64)> {
    if !name.starts_with('i') {
        return None;
    }
    let trimmed = name.trim_start_matches('i').trim_start_matches('_');
    let parts: Vec<&str> = trimmed.split('_').collect();
    if parts.len() != 2 {
        return None;
    }
    let generation = parts[0].parse::<u64>().ok()?;
    let shard_str = parts[1].trim_end_matches(".bin");
    let shard_id = u64::from_str_radix(shard_str, 16).ok()?;
    Some((generation, shard_id))
}

fn parse_delta_filename(name: &str) -> Option<u64> {
    if !name.starts_with('d') {
        return None;
    }
    let trimmed = name.trim_start_matches('d').trim_start_matches('_');
    let parts: Vec<&str> = trimmed.split('_').collect();
    if parts.is_empty() {
        return None;
    }
    let generation = parts[0].parse::<u64>().ok()?;
    Some(generation)
}

impl CacheEntry {
    fn with_generation(record: InodeRecord, generation: u64) -> Self {
        let mut normalized = record;
        normalized.normalize_storage();
        Self {
            record: normalized,
            refreshed: Instant::now(),
            generation,
        }
    }
}

fn create_object_store(config: &Config) -> Result<(Arc<dyn ObjectStore>, String)> {
    match config.object_provider {
        ObjectStoreProvider::Local => {
            std::fs::create_dir_all(&config.store_path)
                .with_context(|| format!("creating store root {}", config.store_path.display()))?;
            let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path.clone())?)
                as Arc<dyn ObjectStore>;
            Ok((store, normalize_prefix(&config.object_prefix)))
        }
        ObjectStoreProvider::Aws => {
            let bucket = config
                .bucket
                .clone()
                .context("--bucket is required for AWS provider")?;
            let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);
            let region = config
                .region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_string());
            builder = builder.with_region(&region);
            if let Some(endpoint) = &config.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            let store = Arc::new(builder.build()?) as Arc<dyn ObjectStore>;
            Ok((store, normalize_prefix(&config.object_prefix)))
        }
        ObjectStoreProvider::Gcs => {
            let bucket = config
                .bucket
                .clone()
                .context("--bucket is required for GCS provider")?;
            let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);
            if let Some(sa_path) = &config.gcs_service_account {
                let creds = sa_path.to_string_lossy().into_owned();
                builder = builder.with_service_account_path(creds);
            }
            let store = Arc::new(builder.build()?) as Arc<dyn ObjectStore>;
            Ok((store, normalize_prefix(&config.object_prefix)))
        }
    }
}

fn normalize_prefix(user_prefix: &str) -> String {
    let trimmed = user_prefix.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test: directory children with file-extension names (e.g., `.h`,
    /// `.c`) must survive a FlatBuffer serialize → deserialize round-trip without
    /// truncation.  A kernel compile regression showed `uprobes.h` being stored as
    /// `uprobes` after the OSGFB2 format migration.
    #[test]
    fn fb2_round_trip_preserves_directory_children_with_extensions() {
        let filenames: Vec<&str> = vec![
            "uprobes.h",
            "mm.h",
            "sched.h",
            "types.h",
            "kernel.h",
            "list.h",
            "rculist.h",
            "spinlock.h",
            "mutex.h",
            "rwsem.h",
            "completion.h",
            "wait.h",
            "pid.h",
            "cred.h",
            "signal.h",
            "resource.h",
            "securebits.h",
            "rbtree.h",
            "rwlock.h",
            "atomic.h",
            "page-flags.h",
            "mmzone.h",
            "topology.h",
            "cpumask.h",
            "percpu.h",
            "smp.h",
            "preempt.h",
            "irqflags.h",
            "bottom_half.h",
            "lockdep.h",
            "Makefile",
            "Kconfig",
            ".gitignore",
            "kvm_host.h",
            "mapping.o.cmd",
        ];
        let mut children = BTreeMap::new();
        for (i, name) in filenames.iter().enumerate() {
            children.insert(name.to_string(), 1000 + i as u64);
        }

        let record = InodeRecord {
            inode: 42,
            parent: 1,
            name: "linux".to_string(),
            path: "/include/linux".to_string(),
            kind: InodeKind::Directory {
                children: Arc::new(children.clone()),
            },
            size: 0,
            mode: 0o40755,
            uid: 1000,
            gid: 1000,
            atime: time::OffsetDateTime::now_utc(),
            mtime: time::OffsetDateTime::now_utc(),
            ctime: time::OffsetDateTime::now_utc(),
            link_count: 2,
            rdev: 0,
            storage: FileStorage::Inline(Vec::new()),
        };

        let data = serialize_inodes_fb2(METADATA_FORMAT_VERSION, 1, std::iter::once(&record));
        let fb_data = data.strip_prefix(METADATA_FB2_MAGIC).unwrap();
        let (version, generation, records) = deserialize_fb2_document(fb_data).unwrap();

        assert_eq!(version, METADATA_FORMAT_VERSION);
        assert_eq!(generation, 1);
        assert_eq!(records.len(), 1);

        let result = &records[0];
        assert_eq!(result.inode, 42);
        assert_eq!(result.name, "linux");
        assert_eq!(result.path, "/include/linux");

        let result_children = result.children().unwrap();
        assert_eq!(
            result_children.len(),
            children.len(),
            "children count mismatch: expected {} got {}",
            children.len(),
            result_children.len()
        );
        for (name, &ino) in &children {
            let got = result_children.get(name);
            assert_eq!(
                got,
                Some(&ino),
                "child {:?} (ino {}) missing or wrong after round-trip; got {:?}",
                name,
                ino,
                got,
            );
        }
    }

    /// Regression test: inline file data (like `.cmd` dependency files) must
    /// survive FlatBuffer round-trip without truncation.
    #[test]
    fn fb2_round_trip_preserves_inline_file_data() {
        // Simulate a .cmd dependency file content
        let cmd_content = b"deps_kernel/dma/mapping.o := \\\n  \
            kernel/dma/mapping.c \\\n  \
            include/linux/uprobes.h \\\n  \
            include/linux/mm.h \\\n  \
            include/linux/sched.h \\\n  \
            include/linux/types.h \\\n";

        let record = InodeRecord {
            inode: 99,
            parent: 42,
            name: ".mapping.o.cmd".to_string(),
            path: "/kernel/dma/.mapping.o.cmd".to_string(),
            kind: InodeKind::File,
            size: cmd_content.len() as u64,
            mode: 0o100644,
            uid: 1000,
            gid: 1000,
            atime: time::OffsetDateTime::now_utc(),
            mtime: time::OffsetDateTime::now_utc(),
            ctime: time::OffsetDateTime::now_utc(),
            link_count: 1,
            rdev: 0,
            storage: FileStorage::Inline(cmd_content.to_vec()),
        };

        let data = serialize_inodes_fb2(METADATA_FORMAT_VERSION, 1, std::iter::once(&record));
        let fb_data = data.strip_prefix(METADATA_FB2_MAGIC).unwrap();
        let (_, _, records) = deserialize_fb2_document(fb_data).unwrap();

        assert_eq!(records.len(), 1);
        let result = &records[0];
        assert_eq!(result.name, ".mapping.o.cmd");
        assert_eq!(result.size, cmd_content.len() as u64);
        match &result.storage {
            FileStorage::Inline(bytes) => {
                assert_eq!(
                    bytes.as_slice(),
                    cmd_content.as_slice(),
                    "inline data mismatch after round-trip"
                );
            }
            other => panic!("expected Inline storage, got {:?}", other),
        }
    }

    /// Stress test: many records (directories + files) serialized together in
    /// a single OSGFB2 document must all survive the round-trip.
    #[test]
    fn fb2_round_trip_many_records_mixed() {
        let mut records = Vec::new();

        // A directory with 500 children (simulating a large kernel include dir)
        let mut children = BTreeMap::new();
        for i in 0..500u64 {
            children.insert(format!("header_{i:04}.h"), 2000 + i);
        }
        records.push(InodeRecord {
            inode: 10,
            parent: 1,
            name: "linux".to_string(),
            path: "/include/linux".to_string(),
            kind: InodeKind::Directory {
                children: Arc::new(children),
            },
            size: 0,
            mode: 0o40755,
            uid: 0,
            gid: 0,
            atime: time::OffsetDateTime::now_utc(),
            mtime: time::OffsetDateTime::now_utc(),
            ctime: time::OffsetDateTime::now_utc(),
            link_count: 2,
            rdev: 0,
            storage: FileStorage::Inline(Vec::new()),
        });

        // 500 file records with inline data
        for i in 0..500u64 {
            let content = format!("/* header_{i:04}.h */\n#ifndef _H_{i}\n#define _H_{i}\n#endif\n");
            records.push(InodeRecord {
                inode: 2000 + i,
                parent: 10,
                name: format!("header_{i:04}.h"),
                path: format!("/include/linux/header_{i:04}.h"),
                kind: InodeKind::File,
                size: content.len() as u64,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                atime: time::OffsetDateTime::now_utc(),
                mtime: time::OffsetDateTime::now_utc(),
                ctime: time::OffsetDateTime::now_utc(),
                link_count: 1,
                rdev: 0,
                storage: FileStorage::Inline(content.into_bytes()),
            });
        }

        let data = serialize_inodes_fb2(
            METADATA_FORMAT_VERSION,
            1,
            records.iter(),
        );
        let fb_data = data.strip_prefix(METADATA_FB2_MAGIC).unwrap();
        let (_, _, result_records) = deserialize_fb2_document(fb_data).unwrap();

        assert_eq!(result_records.len(), records.len());

        // Verify directory children
        let dir = &result_records[0];
        let dir_children = dir.children().unwrap();
        assert_eq!(dir_children.len(), 500);
        for i in 0..500u64 {
            let name = format!("header_{i:04}.h");
            assert_eq!(
                dir_children.get(&name),
                Some(&(2000 + i)),
                "child {} missing or wrong",
                name,
            );
        }

        // Verify file records
        for i in 0..500u64 {
            let file = &result_records[1 + i as usize];
            let expected_name = format!("header_{i:04}.h");
            assert_eq!(file.name, expected_name, "file name mismatch at index {}", i);
            let expected_content =
                format!("/* header_{i:04}.h */\n#ifndef _H_{i}\n#define _H_{i}\n#endif\n");
            match &file.storage {
                FileStorage::Inline(bytes) => {
                    assert_eq!(
                        bytes.as_slice(),
                        expected_content.as_bytes(),
                        "inline data mismatch for {}",
                        expected_name,
                    );
                }
                other => panic!("expected Inline for {}, got {:?}", expected_name, other),
            }
        }
    }
}
