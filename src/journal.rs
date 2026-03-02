//! Append-only write-ahead log (WAL) for crash-safe pending inode tracking.
//!
//! Each `persist_entry` call appends a length-prefixed FlatBuffer record to a
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

use std::fs;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::codec::{deserialize_flex, serialize_flex};
use crate::inode::{InodeKind, InodeRecord};
use crate::metadata::{build_inode_record_fb, fvt, read_inode_record_fb2};
use crate::segment::StagedChunk;

const JOURNAL_VERSION: u32 = 1;
/// Maximum number of total WAL records (live + tombstones) before we rewrite
/// the WAL to keep it from growing unboundedly across many flush cycles.
const WAL_COMPACT_THRESHOLD: usize = 8_192;
/// BufWriter internal buffer – large enough to batch many small-file entries.
const WAL_BUF_BYTES: usize = 256 * 1024;

/// Magic bytes for the FlatBuffer journal format (version 2).
const JOURNAL_FB2_MAGIC: &[u8; 6] = b"OSGJN2";

// ── FlatBuffer Journal Document schema ────────────────────────────────────
//
// JournalDocument table:
//   0(vt=4) version:         u32
//   1(vt=6) record:          InodeRecord table  (same schema as metadata OSGFB2)
//   2(vt=8) payload_tag:     u8   (0=None, 1=Inline, 2=StageFile, 3=StageChunks)
//   3(vt=10) payload_bytes:  [u8] (for Inline variant)
//   4(vt=12) staged_chunks:  [StagedChunkFB]
//
// StagedChunkFB table:
//   0(vt=4) path:            string
//   1(vt=6) offset:          u64
//   2(vt=8) len:             u64
//   3(vt=10) logical_offset: u64

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
        // FlatBuffer serialization outside the lock to reduce lock hold time.
        let buf = serialize_journal_fb2(record, payload);
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

            // Try FlatBuffer format first, fall back to flexbuffers.
            let (record, payload) =
                if let Some((record, payload)) = deserialize_journal_fb2(slice) {
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

// ── FlatBuffer journal serialization (OSGJN2) ────────────────────────────────
//
// Replaces flexbuffers for the write path. Reuses the same InodeRecord
// FlatBuffer table as the metadata OSGFB2 format. The reader detects the
// format via a 6-byte magic header and falls back to flexbuffers for legacy
// WAL entries.

fn build_staged_chunk_fb<'fbb>(
    fbb: &mut FlatBufferBuilder<'fbb>,
    chunk: &StagedChunk,
) -> WIPOffset<flatbuffers::TableFinishedWIPOffset> {
    let path_str = chunk.path.to_string_lossy();
    let path_wip = fbb.create_string(&path_str);

    let start = fbb.start_table();
    fbb.push_slot_always::<WIPOffset<_>>(fvt(0), path_wip);
    fbb.push_slot_always::<u64>(fvt(1), chunk.offset);
    fbb.push_slot_always::<u64>(fvt(2), chunk.len);
    fbb.push_slot_always::<u64>(fvt(3), chunk.logical_offset);
    fbb.end_table(start)
}

fn serialize_journal_fb2(record: &InodeRecord, payload: &JournalPayload) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::with_capacity(512);

    // Build the inode record (reuses metadata's builder).
    let record_wip = build_inode_record_fb(&mut fbb, record);

    // Build payload-specific data before start_table().
    let (payload_tag, payload_bytes_wip, chunks_wip) = match payload {
        JournalPayload::None => (0u8, None, None),
        JournalPayload::Inline(bytes) => {
            let wip = fbb.create_vector(bytes.as_slice());
            (1u8, Some(wip), None)
        }
        JournalPayload::StageFile(chunk) => {
            let chunk_wip = build_staged_chunk_fb(&mut fbb, chunk);
            let vec_wip = fbb.create_vector(&[chunk_wip]);
            (2u8, None, Some(vec_wip))
        }
        JournalPayload::StageChunks(chunks) => {
            let chunk_wips: Vec<WIPOffset<_>> = chunks
                .iter()
                .map(|c| build_staged_chunk_fb(&mut fbb, c))
                .collect();
            let vec_wip = fbb.create_vector(&chunk_wips);
            (3u8, None, Some(vec_wip))
        }
    };

    let start = fbb.start_table();
    fbb.push_slot_always::<u32>(fvt(0), JOURNAL_VERSION);
    fbb.push_slot_always::<WIPOffset<_>>(fvt(1), record_wip);
    fbb.push_slot_always::<u8>(fvt(2), payload_tag);
    if let Some(wip) = payload_bytes_wip {
        fbb.push_slot_always::<WIPOffset<_>>(fvt(3), wip);
    }
    if let Some(wip) = chunks_wip {
        fbb.push_slot_always::<WIPOffset<_>>(fvt(4), wip);
    }
    let root = fbb.end_table(start);
    fbb.finish_minimal(root);

    let fb = fbb.finished_data();
    let mut out = Vec::with_capacity(JOURNAL_FB2_MAGIC.len() + fb.len());
    out.extend_from_slice(JOURNAL_FB2_MAGIC);
    out.extend_from_slice(fb);
    out
}

// ── FlatBuffer journal deserialization ────────────────────────────────────────

fn read_staged_chunk_fb(t: flatbuffers::Table<'_>) -> Option<StagedChunk> {
    let path_str = unsafe { t.get::<flatbuffers::ForwardsUOffset<&str>>(fvt(0), None) }?;
    let offset = unsafe { t.get::<u64>(fvt(1), Some(0)) }.unwrap_or(0);
    let len = unsafe { t.get::<u64>(fvt(2), Some(0)) }.unwrap_or(0);
    let logical_offset = unsafe { t.get::<u64>(fvt(3), Some(0)) }.unwrap_or(0);
    Some(StagedChunk {
        path: PathBuf::from(path_str),
        offset,
        len,
        logical_offset,
    })
}

fn deserialize_journal_fb2(data: &[u8]) -> Option<(InodeRecord, JournalPayload)> {
    if data.len() < JOURNAL_FB2_MAGIC.len() + 4 {
        return None;
    }
    if &data[..JOURNAL_FB2_MAGIC.len()] != JOURNAL_FB2_MAGIC {
        return None;
    }
    let fb_data = &data[JOURNAL_FB2_MAGIC.len()..];

    // Safety: data was written by our own OSGJN2 writer so the schema matches.
    let doc = unsafe { flatbuffers::root_unchecked::<flatbuffers::Table<'_>>(fb_data) };

    let _version = unsafe { doc.get::<u32>(fvt(0), Some(0)) }.unwrap_or(0);

    // Read the InodeRecord sub-table.
    let record_table = unsafe {
        doc.get::<flatbuffers::ForwardsUOffset<flatbuffers::Table<'_>>>(fvt(1), None)
    }?;
    let record = read_inode_record_fb2(record_table).ok()?;

    // Read payload.
    let payload_tag = unsafe { doc.get::<u8>(fvt(2), Some(0)) }.unwrap_or(0);
    let payload = match payload_tag {
        0 => JournalPayload::None,
        1 => {
            let bytes_vec = unsafe {
                doc.get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(fvt(3), None)
            }?;
            JournalPayload::Inline(bytes_vec.bytes().to_vec())
        }
        2 => {
            // StageFile: single chunk in the chunks vector.
            let chunks_vec = unsafe {
                doc.get::<flatbuffers::ForwardsUOffset<
                    flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<flatbuffers::Table<'_>>>,
                >>(fvt(4), None)
            }?;
            if chunks_vec.is_empty() {
                return None;
            }
            let chunk = read_staged_chunk_fb(chunks_vec.get(0))?;
            JournalPayload::StageFile(chunk)
        }
        3 => {
            // StageChunks: multiple chunks.
            let chunks_vec = unsafe {
                doc.get::<flatbuffers::ForwardsUOffset<
                    flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<flatbuffers::Table<'_>>>,
                >>(fvt(4), None)
            }?;
            let mut chunks = Vec::with_capacity(chunks_vec.len());
            for i in 0..chunks_vec.len() {
                chunks.push(read_staged_chunk_fb(chunks_vec.get(i))?);
            }
            JournalPayload::StageChunks(chunks)
        }
        _ => return None,
    };

    Some((record, payload))
}
