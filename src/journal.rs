//! Append-only write-ahead log (WAL) for crash-safe pending inode tracking.
//!
//! Each `persist_entry` call appends a length-prefixed flexbuffer record to a
//! single WAL file (`journal/wal.bin`) without an individual fsync, reducing
//! the per-write cost from 4 syscalls (creat+write+rename+close) to a single
//! buffered `write`.  An explicit `sync_entries` — called only on `fsync()`
//! paths — issues one `fdatasync` + one `sync_all` on the directory.
//!
//! `clear_entry` appends a lightweight tombstone record and, when no live
//! entries remain, truncates the WAL back to zero in a single `set_len(0)`.
//!
//! On recovery, `load_entries` replays the WAL and returns the last live
//! (non-tombstoned) entry per inode.

use std::ffi::OsString;
use std::fs;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::codec::{deserialize_flex, serialize_flex};
use crate::inode::{
    FileStorage, InlinePayload, InlinePayloadCodec, InodeKind, InodeRecord, SegmentExtent,
};
use crate::segment::{SegmentPointer, StagedChunk};

const JOURNAL_VERSION: u32 = 1;
/// Maximum number of total WAL records (live + tombstones) before we rewrite
/// the WAL to keep it from growing unboundedly across many flush cycles.
const WAL_COMPACT_THRESHOLD: usize = 8_192;
/// BufWriter internal buffer – large enough to batch many small-file entries.
const WAL_BUF_BYTES: usize = 256 * 1024;

/// Magic bytes for the fast binary journal format (version 2).
const JOURNAL_FAST_MAGIC: &[u8; 4] = b"JN2\0";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalPayload {
    None,
    Inline(Vec<u8>),
    StageFile(StagedChunk),
    StageChunks(Vec<StagedChunk>),
}

#[derive(Debug, Clone)]
pub struct JournalEntry {
    pub record: InodeRecord,
    pub payload: JournalPayload,
}

#[derive(Serialize, Deserialize)]
struct StoredJournalEntry {
    version: u32,
    record: InodeRecord,
    payload: JournalPayload,
}

// ── WAL state held under the lock ─────────────────────────────────────────

struct WalState {
    /// Buffered writer for the active WAL file.  `None` before the first
    /// `persist_entry` or after a truncation.
    writer: Option<BufWriter<fs::File>>,
    /// Number of non-tombstoned entries currently in the WAL.
    live_count: usize,
    /// Total records written (live + tombstones); drives compact threshold.
    total_records: usize,
}

pub struct JournalManager {
    dir: PathBuf,
    wal_path: PathBuf,
    state: Mutex<WalState>,
}

impl JournalManager {
    pub fn new<P: AsRef<Path>>(local_root: P) -> Result<Self> {
        let dir = local_root.as_ref().join("journal");
        fs::create_dir_all(&dir)
            .with_context(|| format!("creating journal dir {}", dir.display()))?;
        let wal_path = dir.join("wal.bin");
        Ok(Self {
            dir,
            wal_path,
            state: Mutex::new(WalState {
                writer: None,
                live_count: 0,
                total_records: 0,
            }),
        })
    }

    /// Append `entry` to the WAL without fsync.  Fast path: one buffered write.
    pub fn persist_entry(&self, entry: &JournalEntry) -> Result<()> {
        self.persist_record(&entry.record, &entry.payload)
    }

    /// Append a record + payload to the WAL without fsync.  Borrows both
    /// arguments so callers can avoid constructing an intermediate
    /// `JournalEntry` (and the associated clone).
    pub fn persist_record(&self, record: &InodeRecord, payload: &JournalPayload) -> Result<()> {
        // Fast binary serialization outside the lock to reduce lock hold time.
        let mut buf = Vec::with_capacity(1024);
        serialize_journal_fast(&mut buf, record, payload);
        let len = buf.len() as u32;

        let mut state = self.state.lock();
        let writer = self.ensure_writer(&mut state)?;
        writer
            .write_all(&len.to_le_bytes())
            .context("writing WAL record length")?;
        writer.write_all(&buf).context("writing WAL record data")?;
        state.live_count += 1;
        state.total_records += 1;
        Ok(())
    }

    /// Flush buffered writes and issue a single `fdatasync` + directory sync.
    /// Called only on explicit `fsync()` paths; not per-entry.
    pub fn sync_entries(&self, _inodes: &[u64]) -> Result<()> {
        let mut state = self.state.lock();
        if let Some(writer) = state.writer.as_mut() {
            writer.flush().context("flushing WAL buffer")?;
            writer.get_ref().sync_data().context("fdatasync WAL file")?;
        }
        self.sync_dir()
    }

    /// Append a tombstone for `inode`.  When no live entries remain, truncate
    /// the WAL to zero bytes (the common case after a complete flush cycle).
    pub fn clear_entry(&self, inode: u64) -> Result<()> {
        let tombstone = InodeRecord::tombstone(inode);
        let stored = StoredJournalEntry {
            version: JOURNAL_VERSION,
            record: tombstone,
            payload: JournalPayload::None,
        };
        let data = serialize_flex(&stored)?;
        let len = data.len() as u32;

        let mut state = self.state.lock();

        // Decrement live count first; if this was the last live entry we
        // truncate instead of appending.
        state.live_count = state.live_count.saturating_sub(1);

        if state.live_count == 0 {
            // Nothing pending — truncate the WAL in-place.
            self.truncate_wal(&mut state)?;
            return Ok(());
        }

        // Still have live entries; record the tombstone so recovery skips it.
        let writer = self.ensure_writer(&mut state)?;
        writer
            .write_all(&len.to_le_bytes())
            .context("writing WAL tombstone length")?;
        writer
            .write_all(&data)
            .context("writing WAL tombstone data")?;
        state.total_records += 1;

        // Compact when the ratio of tombstones to live entries grows large.
        if state.total_records > WAL_COMPACT_THRESHOLD {
            drop(state); // release lock before the more expensive compact
            self.compact()?;
        }

        Ok(())
    }

    /// Replay the WAL and return the last live entry per inode.
    /// Tombstoned inodes are excluded from the result.
    pub fn load_entries(&self) -> Result<Vec<JournalEntry>> {
        if !self.wal_path.exists() {
            return Ok(Vec::new());
        }
        let raw = fs::read(&self.wal_path)
            .with_context(|| format!("reading WAL {}", self.wal_path.display()))?;

        // Replay: last writer wins per inode.
        let mut live: std::collections::HashMap<u64, JournalEntry> =
            std::collections::HashMap::new();
        let mut cursor = std::io::Cursor::new(&raw);

        loop {
            // Read 4-byte little-endian length prefix.
            let mut len_buf = [0u8; 4];
            match cursor.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e).context("reading WAL record length"),
            }
            let record_len = u32::from_le_bytes(len_buf) as usize;
            let pos = cursor.position() as usize;
            if pos + record_len > raw.len() {
                // Truncated record at end of WAL — skip (could be a partial write).
                break;
            }
            let slice = &raw[pos..pos + record_len];
            cursor.seek(SeekFrom::Current(record_len as i64))?;

            // Try fast binary format first, fall back to flexbuffers.
            let (record, payload) = if let Some((record, payload)) = deserialize_journal_fast(slice)
            {
                (record, payload)
            } else if let Ok(stored) = deserialize_flex::<StoredJournalEntry>(slice) {
                if stored.version != JOURNAL_VERSION {
                    continue;
                }
                (stored.record, stored.payload)
            } else {
                continue; // corrupt record, skip
            };
            if matches!(record.kind, InodeKind::Tombstone) {
                live.remove(&record.inode);
            } else {
                live.insert(record.inode, JournalEntry { record, payload });
            }
        }

        // Sync live_count with what we found on disk.
        {
            let mut state = self.state.lock();
            state.live_count = live.len();
            state.total_records = live.len(); // compact state after load
        }

        Ok(live.into_values().collect())
    }

    // ── Private helpers ───────────────────────────────────────────────────

    /// Ensure the WAL writer is open, creating the file if needed.
    /// Must be called while holding the state lock.
    fn ensure_writer<'a>(&self, state: &'a mut WalState) -> Result<&'a mut BufWriter<fs::File>> {
        if state.writer.is_none() {
            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.wal_path)
                .with_context(|| format!("opening WAL {}", self.wal_path.display()))?;
            state.writer = Some(BufWriter::with_capacity(WAL_BUF_BYTES, file));
        }
        Ok(state.writer.as_mut().expect("writer just set"))
    }

    /// Truncate the WAL to zero bytes and reset counters.
    /// Must be called while holding the state lock.
    fn truncate_wal(&self, state: &mut WalState) -> Result<()> {
        // Flush any buffered data first, then truncate via set_len.
        if let Some(writer) = state.writer.take() {
            let mut file = writer
                .into_inner()
                .map_err(|_| anyhow::anyhow!("WAL BufWriter flush error during truncate"))?;
            file.set_len(0).context("truncating WAL")?;
            file.seek(SeekFrom::Start(0))
                .context("seeking WAL to start")?;
            // Re-open for future appends.
            state.writer = Some(BufWriter::with_capacity(WAL_BUF_BYTES, file));
        } else if self.wal_path.exists() {
            fs::OpenOptions::new()
                .write(true)
                .open(&self.wal_path)
                .and_then(|f| f.set_len(0))
                .context("truncating WAL file")?;
        }
        state.live_count = 0;
        state.total_records = 0;
        Ok(())
    }

    /// Rewrite the WAL with only the currently-live entries, discarding all
    /// tombstones.  This keeps the WAL from growing after many flush cycles.
    fn compact(&self) -> Result<()> {
        let live_entries = self.load_entries()?;
        if live_entries.is_empty() {
            let mut state = self.state.lock();
            self.truncate_wal(&mut state)?;
            return Ok(());
        }

        // Write a compact replacement WAL to a temp file, then rename.
        let tmp_path = self.wal_path.with_extension("tmp");
        {
            let tmp_file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .with_context(|| format!("creating compact WAL {}", tmp_path.display()))?;
            let mut writer = BufWriter::with_capacity(WAL_BUF_BYTES, tmp_file);
            for entry in &live_entries {
                let stored = StoredJournalEntry {
                    version: JOURNAL_VERSION,
                    record: entry.record.clone(),
                    payload: entry.payload.clone(),
                };
                let data = serialize_flex(&stored)?;
                writer.write_all(&(data.len() as u32).to_le_bytes())?;
                writer.write_all(&data)?;
            }
            writer.flush()?;
        }
        fs::rename(&tmp_path, &self.wal_path)
            .with_context(|| format!("renaming compact WAL to {}", self.wal_path.display()))?;

        // Reopen appender on the new WAL.
        let mut state = self.state.lock();
        state.writer = None; // drop the old handle; ensure_writer reopens
        state.live_count = live_entries.len();
        state.total_records = live_entries.len();
        Ok(())
    }

    fn sync_dir(&self) -> Result<()> {
        let dir = fs::OpenOptions::new()
            .read(true)
            .open(&self.dir)
            .with_context(|| format!("opening journal dir {}", self.dir.display()))?;
        dir.sync_all()
            .with_context(|| format!("syncing journal dir {}", self.dir.display()))
    }
}

// ── Fast binary journal serialization ────────────────────────────────────────
//
// Replaces flexbuffers for the write path with a compact binary format that
// avoids serde reflection overhead and per-entry allocations.  The reader
// detects the format via a 4-byte magic header and falls back to flexbuffers
// for legacy WAL entries.

fn serialize_journal_fast(buf: &mut Vec<u8>, record: &InodeRecord, payload: &JournalPayload) {
    buf.extend_from_slice(JOURNAL_FAST_MAGIC);
    buf.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
    write_inode_record_bin(buf, record);
    write_journal_payload_bin(buf, payload);
}

fn write_inode_record_bin(buf: &mut Vec<u8>, r: &InodeRecord) {
    buf.extend_from_slice(&r.inode.to_le_bytes());
    buf.extend_from_slice(&r.parent.to_le_bytes());
    write_str_bin(buf, &r.name);
    write_str_bin(buf, &r.path);
    match &r.kind {
        InodeKind::File => buf.push(0),
        InodeKind::Directory { children } => {
            buf.push(1);
            buf.extend_from_slice(&(children.len() as u32).to_le_bytes());
            for (key, &val) in children.as_ref() {
                write_str_bin(buf, key);
                buf.extend_from_slice(&val.to_le_bytes());
            }
        }
        InodeKind::Symlink => buf.push(2),
        InodeKind::Tombstone => buf.push(3),
    }
    buf.extend_from_slice(&r.size.to_le_bytes());
    buf.extend_from_slice(&r.mode.to_le_bytes());
    buf.extend_from_slice(&r.uid.to_le_bytes());
    buf.extend_from_slice(&r.gid.to_le_bytes());
    write_time_bin(buf, &r.atime);
    write_time_bin(buf, &r.mtime);
    write_time_bin(buf, &r.ctime);
    buf.extend_from_slice(&r.link_count.to_le_bytes());
    buf.extend_from_slice(&r.rdev.to_le_bytes());
    write_storage_bin(buf, &r.storage);
}

fn write_str_bin(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn write_time_bin(buf: &mut Vec<u8>, t: &time::OffsetDateTime) {
    buf.extend_from_slice(&t.unix_timestamp().to_le_bytes());
    buf.extend_from_slice(&(t.nanosecond() as i32).to_le_bytes());
}

fn write_storage_bin(buf: &mut Vec<u8>, s: &FileStorage) {
    match s {
        FileStorage::Inline(bytes) => {
            buf.push(0);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        FileStorage::InlineEncoded(p) => {
            buf.push(1);
            buf.push(match p.codec {
                InlinePayloadCodec::None => 0,
                InlinePayloadCodec::Lz4 => 1,
                InlinePayloadCodec::ChaCha20Poly1305 => 2,
                InlinePayloadCodec::Lz4ChaCha20Poly1305 => 3,
            });
            buf.extend_from_slice(&p.original_len.unwrap_or(0).to_le_bytes());
            if let Some(nonce) = &p.nonce {
                buf.push(1);
                buf.extend_from_slice(nonce);
            } else {
                buf.push(0);
            }
            buf.extend_from_slice(&(p.payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&p.payload);
        }
        FileStorage::LegacySegment(ptr) => {
            buf.push(2);
            buf.extend_from_slice(&1u32.to_le_bytes()); // 1 extent
            write_seg_extent_bin(buf, 0, ptr);
        }
        FileStorage::Segments(extents) => {
            buf.push(2);
            buf.extend_from_slice(&(extents.len() as u32).to_le_bytes());
            for ext in extents {
                write_seg_extent_bin(buf, ext.logical_offset, &ext.pointer);
            }
        }
    }
}

fn write_seg_extent_bin(buf: &mut Vec<u8>, logical_offset: u64, ptr: &SegmentPointer) {
    buf.extend_from_slice(&logical_offset.to_le_bytes());
    buf.extend_from_slice(&ptr.segment_id.to_le_bytes());
    buf.extend_from_slice(&ptr.generation.to_le_bytes());
    buf.extend_from_slice(&ptr.offset.to_le_bytes());
    buf.extend_from_slice(&ptr.length.to_le_bytes());
}

fn write_journal_payload_bin(buf: &mut Vec<u8>, p: &JournalPayload) {
    match p {
        JournalPayload::None => buf.push(0),
        JournalPayload::Inline(bytes) => {
            buf.push(1);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        JournalPayload::StageFile(chunk) => {
            buf.push(2);
            write_staged_chunk_bin(buf, chunk);
        }
        JournalPayload::StageChunks(chunks) => {
            buf.push(3);
            buf.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
            for chunk in chunks {
                write_staged_chunk_bin(buf, chunk);
            }
        }
    }
}

fn write_staged_chunk_bin(buf: &mut Vec<u8>, chunk: &StagedChunk) {
    let path_bytes = chunk.path.as_os_str().as_encoded_bytes();
    buf.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(path_bytes);
    buf.extend_from_slice(&chunk.offset.to_le_bytes());
    buf.extend_from_slice(&chunk.len.to_le_bytes());
    buf.extend_from_slice(&chunk.logical_offset.to_le_bytes());
}

// ── Fast binary journal deserialization ──────────────────────────────────────

fn deserialize_journal_fast(data: &[u8]) -> Option<(InodeRecord, JournalPayload)> {
    let mut cursor = data;
    if cursor.len() < 8 {
        return None;
    }
    if &cursor[..4] != JOURNAL_FAST_MAGIC {
        return None;
    }
    cursor = &cursor[4..];
    let _version = read_u32(&mut cursor)?;
    let record = read_inode_record_bin(&mut cursor)?;
    let payload = read_journal_payload_bin(&mut cursor)?;
    Some((record, payload))
}

fn read_u8(cursor: &mut &[u8]) -> Option<u8> {
    if cursor.is_empty() {
        return None;
    }
    let val = cursor[0];
    *cursor = &cursor[1..];
    Some(val)
}

fn read_u32(cursor: &mut &[u8]) -> Option<u32> {
    if cursor.len() < 4 {
        return None;
    }
    let val = u32::from_le_bytes(cursor[..4].try_into().ok()?);
    *cursor = &cursor[4..];
    Some(val)
}

fn read_u64(cursor: &mut &[u8]) -> Option<u64> {
    if cursor.len() < 8 {
        return None;
    }
    let val = u64::from_le_bytes(cursor[..8].try_into().ok()?);
    *cursor = &cursor[8..];
    Some(val)
}

fn read_i64(cursor: &mut &[u8]) -> Option<i64> {
    if cursor.len() < 8 {
        return None;
    }
    let val = i64::from_le_bytes(cursor[..8].try_into().ok()?);
    *cursor = &cursor[8..];
    Some(val)
}

fn read_i32(cursor: &mut &[u8]) -> Option<i32> {
    if cursor.len() < 4 {
        return None;
    }
    let val = i32::from_le_bytes(cursor[..4].try_into().ok()?);
    *cursor = &cursor[4..];
    Some(val)
}

fn read_bytes(cursor: &mut &[u8], len: usize) -> Option<Vec<u8>> {
    if cursor.len() < len {
        return None;
    }
    let val = cursor[..len].to_vec();
    *cursor = &cursor[len..];
    Some(val)
}

fn read_str_bin(cursor: &mut &[u8]) -> Option<String> {
    let len = read_u32(cursor)? as usize;
    let bytes = read_bytes(cursor, len)?;
    String::from_utf8(bytes).ok()
}

fn read_time_bin(cursor: &mut &[u8]) -> Option<time::OffsetDateTime> {
    let secs = read_i64(cursor)?;
    let nanos = read_i32(cursor)?;
    time::OffsetDateTime::from_unix_timestamp(secs)
        .ok()
        .map(|t| t.replace_nanosecond(nanos as u32).unwrap_or(t))
}

fn read_seg_extent_bin(cursor: &mut &[u8]) -> Option<SegmentExtent> {
    let logical_offset = read_u64(cursor)?;
    let segment_id = read_u64(cursor)?;
    let generation = read_u64(cursor)?;
    let offset = read_u64(cursor)?;
    let length = read_u64(cursor)?;
    Some(SegmentExtent::new(
        logical_offset,
        SegmentPointer {
            segment_id,
            generation,
            offset,
            length,
        },
    ))
}

fn read_storage_bin(cursor: &mut &[u8]) -> Option<FileStorage> {
    let tag = read_u8(cursor)?;
    match tag {
        0 => {
            let len = read_u32(cursor)? as usize;
            let data = read_bytes(cursor, len)?;
            Some(FileStorage::Inline(data))
        }
        1 => {
            let codec_u8 = read_u8(cursor)?;
            let codec = match codec_u8 {
                0 => InlinePayloadCodec::None,
                1 => InlinePayloadCodec::Lz4,
                2 => InlinePayloadCodec::ChaCha20Poly1305,
                3 => InlinePayloadCodec::Lz4ChaCha20Poly1305,
                _ => return None,
            };
            let orig_len_raw = read_u64(cursor)?;
            let has_nonce = read_u8(cursor)?;
            let nonce = if has_nonce != 0 {
                let nonce_bytes = read_bytes(cursor, 12)?;
                let mut arr = [0u8; 12];
                arr.copy_from_slice(&nonce_bytes);
                Some(arr)
            } else {
                None
            };
            let payload_len = read_u32(cursor)? as usize;
            let payload = read_bytes(cursor, payload_len)?;
            Some(FileStorage::InlineEncoded(InlinePayload {
                codec,
                payload,
                original_len: if orig_len_raw == 0 {
                    None
                } else {
                    Some(orig_len_raw)
                },
                nonce,
            }))
        }
        2 => {
            let num = read_u32(cursor)? as usize;
            let mut extents = Vec::with_capacity(num);
            for _ in 0..num {
                extents.push(read_seg_extent_bin(cursor)?);
            }
            Some(FileStorage::Segments(extents))
        }
        _ => None,
    }
}

fn read_inode_record_bin(cursor: &mut &[u8]) -> Option<InodeRecord> {
    let inode = read_u64(cursor)?;
    let parent = read_u64(cursor)?;
    let name = read_str_bin(cursor)?;
    let path = read_str_bin(cursor)?;
    let kind_tag = read_u8(cursor)?;
    let kind = match kind_tag {
        0 => InodeKind::File,
        1 => {
            let num_children = read_u32(cursor)? as usize;
            let mut children = std::collections::BTreeMap::new();
            for _ in 0..num_children {
                let key = read_str_bin(cursor)?;
                let val = read_u64(cursor)?;
                children.insert(key, val);
            }
            InodeKind::Directory {
                children: std::sync::Arc::new(children),
            }
        }
        2 => InodeKind::Symlink,
        3 => InodeKind::Tombstone,
        _ => return None,
    };
    let size = read_u64(cursor)?;
    let mode = read_u32(cursor)?;
    let uid = read_u32(cursor)?;
    let gid = read_u32(cursor)?;
    let atime = read_time_bin(cursor)?;
    let mtime = read_time_bin(cursor)?;
    let ctime = read_time_bin(cursor)?;
    let link_count = read_u32(cursor)?;
    let rdev = read_u32(cursor)?;
    let storage = read_storage_bin(cursor)?;
    Some(InodeRecord {
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

fn read_staged_chunk_bin(cursor: &mut &[u8]) -> Option<StagedChunk> {
    let path_len = read_u32(cursor)? as usize;
    let path_bytes = read_bytes(cursor, path_len)?;
    let path = PathBuf::from(OsString::from_vec(path_bytes));
    let offset = read_u64(cursor)?;
    let len = read_u64(cursor)?;
    let logical_offset = read_u64(cursor)?;
    Some(StagedChunk {
        path,
        offset,
        len,
        logical_offset,
    })
}

fn read_journal_payload_bin(cursor: &mut &[u8]) -> Option<JournalPayload> {
    let tag = read_u8(cursor)?;
    match tag {
        0 => Some(JournalPayload::None),
        1 => {
            let len = read_u32(cursor)? as usize;
            let data = read_bytes(cursor, len)?;
            Some(JournalPayload::Inline(data))
        }
        2 => {
            let chunk = read_staged_chunk_bin(cursor)?;
            Some(JournalPayload::StageFile(chunk))
        }
        3 => {
            let num = read_u32(cursor)? as usize;
            let mut chunks = Vec::with_capacity(num);
            for _ in 0..num {
                chunks.push(read_staged_chunk_bin(cursor)?);
            }
            Some(JournalPayload::StageChunks(chunks))
        }
        _ => None,
    }
}
