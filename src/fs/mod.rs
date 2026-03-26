use std::collections::{HashMap, HashSet};

use dashmap::{DashMap, DashSet};
use std::convert::TryFrom;
use std::ffi::OsStr;
use std::path::Path;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use crate::compat::{
    EEXIST, EFBIG, EINVAL, EIO, EISDIR, ENAMETOOLONG, ENOENT, ENOTDIR, ENOTEMPTY, EPERM, O_EXCL,
    O_TRUNC, S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFMT, S_IFREG, S_IFSOCK,
};
use anyhow::{Result, anyhow};
#[cfg(feature = "fuse")]
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use parking_lot::Mutex;
use time::OffsetDateTime;
use tokio::runtime::Handle;

use crate::codec::{
    InlineCodecConfig, decode_inline_storage as decode_inline_payload_storage,
    encode_inline_storage as encode_inline_payload_storage,
};
use crate::config::Config;
use crate::inode::{FileStorage, InodeKind, InodeRecord, ROOT_INODE, SegmentExtent};
use crate::journal::{JournalManager, JournalPayload};
use crate::metadata::MetadataStore;
use crate::perf::PerfLogger;
use crate::replay::ReplayLogger;
use crate::segment::{SegmentEntry, SegmentManager, SegmentPayload, StagedChunk};
use crate::source::{DiscoveredEntry, SourceObjectStore};
use crate::state::ClientStateManager;
use crate::superblock::SuperblockManager;
use crate::telemetry::TelemetryClient;
use log::{debug, error, info};
use serde_json::json;

const FUSE_NODE_GENERATION: u64 = 1;
const STATFS_BLOCK_SIZE: u32 = 4096;
const STATFS_BLOCKS: u64 = 1u64 << 48; // 1 exabyte of 4KiB blocks
const STATFS_FILES: u64 = 1u64 << 52; // generous inode pool to appear "infinite"
const ADAPTIVE_PENDING_MULTIPLIER: u64 = 16;
const ADAPTIVE_PENDING_MAX_BYTES: u64 = 512 * 1024 * 1024;
const ADAPTIVE_LARGE_WRITE_MIN_BYTES: u64 = 256 * 1024;
const NAME_MAX_BYTES: usize = 255;
static PROCESS_EXITING: AtomicBool = AtomicBool::new(false);

#[cfg(all(unix, target_os = "linux"))]
const RENAME_NOREPLACE_FLAG: u32 = libc::RENAME_NOREPLACE;
#[cfg(not(all(unix, target_os = "linux")))]
const RENAME_NOREPLACE_FLAG: u32 = 0;

#[derive(Clone)]
pub struct OsageFs {
    config: Config,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    segments: Arc<SegmentManager>,
    source: Option<Arc<SourceObjectStore>>,
    handle: Handle,
    client_state: Arc<ClientStateManager>,
    active_inodes: Arc<DashMap<u64, Arc<Mutex<ActiveInode>>>>,
    pending_inodes: Arc<DashSet<u64>>,
    pending_bytes: Arc<AtomicU64>,
    perf: Option<Arc<PerfLogger>>,
    replay: Option<Arc<ReplayLogger>>,
    telemetry: Option<Arc<TelemetryClient>>,
    telemetry_session_id: Option<String>,
    coordination_publisher: Option<Arc<dyn crate::coordination::CoordinationPublisher>>,
    mount_ready_emitted: Arc<AtomicBool>,
    fsync_on_close: bool,
    flush_interval: Option<Duration>,
    last_flush: Arc<AtomicU64>,
    flush_lock: Arc<Mutex<()>>,
    dir_locks: Arc<DashMap<u64, Arc<Mutex<()>>>>,
    flush_scheduled: Arc<AtomicBool>,
    fuse_entry_ttl: Duration,
    lookup_cache_ttl: Duration,
    dir_cache_ttl: Duration,
    journal: Option<Arc<JournalManager>>,
    flush_commit_hook: Arc<std::sync::OnceLock<Arc<dyn flush::FlushCommitHook>>>,
}

mod core;
mod flush;
pub use flush::{FlushCommitDecision, FlushCommitHook};
#[cfg(feature = "fuse")]
mod fuse;
mod nfs;
mod ops;
mod write_path;

#[cfg(feature = "fuse")]
fn file_type(record: &InodeRecord) -> FileType {
    match record.kind {
        InodeKind::Directory { .. } => FileType::Directory,
        InodeKind::File => OsageFs::mode_to_file_type(record.mode),
        InodeKind::Symlink => FileType::Symlink,
        InodeKind::Tombstone => FileType::RegularFile,
    }
}

#[cfg(unix)]
fn path_to_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;

    path.as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn path_to_bytes(path: &Path) -> Vec<u8> {
    path.to_string_lossy().into_owned().into_bytes()
}

fn to_system_time(ts: OffsetDateTime) -> SystemTime {
    let secs = ts.unix_timestamp();
    let nanos = ts.nanosecond();
    if secs >= 0 {
        SystemTime::UNIX_EPOCH
            + Duration::from_secs(secs as u64)
            + Duration::from_nanos(nanos as u64)
    } else {
        SystemTime::UNIX_EPOCH
    }
}

pub fn set_process_exiting(exiting: bool) {
    PROCESS_EXITING.store(exiting, AtomicOrdering::Relaxed);
}

pub(crate) fn process_exiting() -> bool {
    PROCESS_EXITING.load(AtomicOrdering::Relaxed)
}

pub(crate) fn current_thread_label() -> String {
    if process_exiting() {
        "teardown".to_string()
    } else {
        format!("{:?}", thread::current().id())
    }
}

fn from_system_time(ts: SystemTime) -> OffsetDateTime {
    match ts.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            let secs = duration.as_secs() as i64;
            let nanos = duration.subsec_nanos();
            OffsetDateTime::from_unix_timestamp(secs)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH)
                .replace_nanosecond(nanos)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        }
        Err(err) => {
            let duration = err.duration();
            let secs = duration.as_secs() as i64;
            let nanos = duration.subsec_nanos();
            OffsetDateTime::from_unix_timestamp(-secs)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH)
                .replace_nanosecond(nanos)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        }
    }
}

#[derive(Clone)]
pub(crate) struct PendingEntry {
    record: InodeRecord,
    data: Option<PendingData>,
}

#[derive(Default)]
pub(crate) struct ActiveInode {
    pub pending: Option<PendingEntry>,
    pub flushing: Option<PendingEntry>,
}

#[derive(Clone, Copy)]
pub(crate) struct StageWriteContext {
    prev_size: u64,
    write_offset: u64,
}

#[derive(Clone)]
pub(crate) enum PendingData {
    Inline(Arc<Vec<u8>>),
    Staged(PendingSegments),
}

#[derive(Clone)]
pub(crate) struct PendingSegments {
    /// Committed segment extents from the last flush that back regions of the
    /// file not yet overwritten by staged chunks.  These are carried forward
    /// from the previous committed storage and merged with new staged chunks at
    /// flush time, avoiding a full file read on partial overwrites.
    pub(crate) base_extents: Arc<Vec<SegmentExtent>>,
    /// Staged chunks representing new or modified data, each with an explicit
    /// logical file offset.
    chunks: Arc<Vec<StagedChunk>>,
    /// Total logical file size.  May exceed the sum of `chunks` lengths when
    /// `base_extents` are present.
    total_len: u64,
}

impl PendingSegments {
    fn new() -> Self {
        Self {
            base_extents: Arc::new(Vec::new()),
            chunks: Arc::new(Vec::new()),
            total_len: 0,
        }
    }

    fn from_chunk(chunk: StagedChunk) -> Self {
        let mut segments = Self::new();
        segments.append(chunk);
        segments
    }

    /// Create pending segments backed by existing committed extents, without
    /// reading any segment data.  Used by `write_large_segments` to avoid
    /// materialising a large file just to apply a partial write on top of it.
    pub(crate) fn from_committed_extents(extents: Vec<SegmentExtent>, total_len: u64) -> Self {
        Self {
            base_extents: Arc::new(extents),
            chunks: Arc::new(Vec::new()),
            total_len,
        }
    }

    /// Sum of bytes in staged chunks only (excludes base_extents).  Used for
    /// `pending_bytes` accounting: only newly-staged data contributes to the
    /// in-flight dirty-byte watermark.
    pub(crate) fn staged_bytes(&self) -> u64 {
        self.chunks.iter().map(|c| c.len).sum()
    }

    fn append(&mut self, chunk: StagedChunk) {
        if chunk.len == 0 {
            return;
        }
        let mut chunk = chunk;
        // An appended chunk always starts at the current logical end.
        chunk.logical_offset = self.total_len;
        self.total_len = self.total_len.saturating_add(chunk.len);
        Arc::make_mut(&mut self.chunks).push(chunk);
    }

    fn ensure_offset(&mut self, target: u64) -> Result<()> {
        if target <= self.total_len {
            return Ok(());
        }
        // Preserve sparse-hole semantics by extending logical length without
        // materializing a staged zero chunk for the gap.
        self.total_len = target;
        Ok(())
    }

    fn write_range(
        &mut self,
        manager: &SegmentManager,
        offset: u64,
        chunk: StagedChunk,
    ) -> Result<()> {
        if chunk.len == 0 {
            return Ok(());
        }
        let mut chunk = chunk;
        chunk.logical_offset = offset;
        let write_end = offset.saturating_add(chunk.len);
        let mut pending_chunk = Some(chunk);
        let chunks = Arc::make_mut(&mut self.chunks);
        let mut result = Vec::with_capacity(chunks.len() + 1);
        for existing in chunks.drain(..) {
            let chunk_start = existing.logical_offset;
            let chunk_end = chunk_start.saturating_add(existing.len);
            if chunk_end <= offset || chunk_start >= write_end {
                if chunk_start >= write_end
                    && let Some(new_chunk) = pending_chunk.take()
                {
                    result.push(new_chunk);
                }
                result.push(existing);
            } else {
                if chunk_start < offset {
                    let left_len = offset - chunk_start;
                    if left_len > 0 {
                        let left = manager
                            .slice_staged_chunk(&existing, 0, left_len)
                            .map_err(|_| anyhow!("slice left"))?;
                        result.push(left);
                    }
                }
                if let Some(new_chunk) = pending_chunk.take() {
                    result.push(new_chunk);
                }
                if chunk_end > write_end {
                    let right_offset = write_end - chunk_start;
                    let right_len = chunk_end - write_end;
                    if right_len > 0 {
                        let right = manager
                            .slice_staged_chunk(&existing, right_offset, right_len)
                            .map_err(|_| anyhow!("slice right"))?;
                        result.push(right);
                    }
                }
                manager
                    .release_staged_chunk(&existing)
                    .map_err(|_| anyhow!("release chunk"))?;
            }
        }
        if let Some(new_chunk) = pending_chunk.take() {
            result.push(new_chunk);
        }
        self.chunks = Arc::new(result);
        self.total_len = self.total_len.max(write_end);
        Ok(())
    }
}

impl PendingData {
    fn len(&self) -> u64 {
        match self {
            PendingData::Inline(bytes) => bytes.len() as u64,
            PendingData::Staged(segments) => segments.total_len,
        }
    }
}

#[cfg(test)]
mod tests;
