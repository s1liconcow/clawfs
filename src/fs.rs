use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::convert::TryFrom;
use std::ffi::OsStr;
use std::path::Path;
use std::process;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Result, anyhow};
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use libc::{EEXIST, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, ENOTEMPTY, EPERM};
use parking_lot::Mutex;
use time::OffsetDateTime;
use tokio::runtime::Handle;

use crate::config::Config;
use crate::inode::{FileStorage, InodeKind, InodeRecord, ROOT_INODE, SegmentExtent};
use crate::journal::{JournalEntry, JournalManager, JournalPayload};
use crate::metadata::MetadataStore;
use crate::perf::PerfLogger;
use crate::segment::{SegmentEntry, SegmentManager, StagedChunk};
use crate::state::ClientStateManager;
use crate::superblock::SuperblockManager;
use log::{debug, error, info};
use serde_json::json;

const TTL: Duration = Duration::from_secs(1);
const STATFS_BLOCK_SIZE: u32 = 4096;
const STATFS_BLOCKS: u64 = 1u64 << 48; // 1 exabyte of 4KiB blocks
const STATFS_FILES: u64 = 1u64 << 52; // generous inode pool to appear "infinite"

#[cfg(target_os = "linux")]
const RENAME_NOREPLACE_FLAG: u32 = libc::RENAME_NOREPLACE as u32;
#[cfg(not(target_os = "linux"))]
const RENAME_NOREPLACE_FLAG: u32 = 0;

pub struct OsageFs {
    config: Config,
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    segments: Arc<SegmentManager>,
    handle: Handle,
    client_state: Arc<ClientStateManager>,
    pending_inodes: Mutex<HashMap<u64, PendingEntry>>,
    mutating_inodes: Mutex<HashMap<u64, PendingEntry>>,
    flushing_inodes: Mutex<HashMap<u64, PendingEntry>>,
    pending_bytes: Mutex<u64>,
    perf: Option<Arc<PerfLogger>>,
    fsync_on_close: bool,
    flush_interval: Option<Duration>,
    last_flush: Mutex<Instant>,
    flush_lock: Mutex<()>,
    mutation_lock: Mutex<()>,
    lookup_cache_ttl: Duration,
    dir_cache_ttl: Duration,
    journal: Option<Arc<JournalManager>>,
}

impl OsageFs {
    pub fn new(
        config: Config,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        segments: Arc<SegmentManager>,
        journal: Option<Arc<JournalManager>>,
        handle: Handle,
        client_state: Arc<ClientStateManager>,
        perf: Option<Arc<PerfLogger>>,
    ) -> Self {
        let fsync_on_close = config.fsync_on_close;
        let flush_interval = if config.flush_interval_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(config.flush_interval_ms))
        };
        let lookup_cache_ttl = Duration::from_millis(config.lookup_cache_ttl_ms);
        let dir_cache_ttl = Duration::from_millis(config.dir_cache_ttl_ms);
        Self {
            config,
            metadata,
            superblock,
            segments,
            handle,
            client_state,
            pending_inodes: Mutex::new(HashMap::new()),
            mutating_inodes: Mutex::new(HashMap::new()),
            flushing_inodes: Mutex::new(HashMap::new()),
            pending_bytes: Mutex::new(0),
            perf,
            fsync_on_close,
            flush_interval,
            last_flush: Mutex::new(Instant::now()),
            flush_lock: Mutex::new(()),
            mutation_lock: Mutex::new(()),
            lookup_cache_ttl,
            dir_cache_ttl,
            journal,
        }
    }

    pub fn replay_journal(&self) -> Result<usize> {
        let Some(journal) = &self.journal else {
            return Ok(0);
        };
        let entries = journal.load_entries()?;
        if entries.is_empty() {
            return Ok(0);
        }
        let mut restored = 0;
        for entry in entries {
            let inode = entry.record.inode;
            let record = entry.record;
            let data_opt = match entry.payload {
                JournalPayload::None => None,
                JournalPayload::Inline(bytes) => Some(PendingData::Inline(bytes)),
                JournalPayload::StageFile(chunk) => {
                    if chunk.path.exists() {
                        Some(PendingData::Staged(PendingSegments::from_chunk(chunk)))
                    } else {
                        log::warn!(
                            "staged payload {} missing for inode {}",
                            chunk.path.display(),
                            inode
                        );
                        None
                    }
                }
                JournalPayload::StageChunks(chunks) => {
                    let mut present = Vec::new();
                    for chunk in chunks {
                        if chunk.path.exists() {
                            present.push(chunk);
                        } else {
                            log::warn!(
                                "staged payload {} missing for inode {}",
                                chunk.path.display(),
                                inode
                            );
                        }
                    }
                    if present.is_empty() {
                        None
                    } else {
                        let total = present.iter().map(|c| c.len).sum();
                        Some(PendingData::Staged(PendingSegments {
                            chunks: present,
                            total_len: total,
                        }))
                    }
                }
            };
            let data_len = data_opt.as_ref().map(|d| d.len()).unwrap_or(0);
            {
                let mut map = self.pending_inodes.lock();
                if let Some(old) = map.insert(
                    inode,
                    PendingEntry {
                        record,
                        data: data_opt,
                    },
                ) {
                    if let Some(old_data) = old.data {
                        self.release_pending_data(old_data);
                    }
                }
            }
            if data_len > 0 {
                let mut total = self.pending_bytes.lock();
                *total = total.saturating_add(data_len);
            }
            restored += 1;
        }
        debug!("replay_journal staged {} entries", restored);
        self.flush_pending()
            .map_err(|code| anyhow!("failed to flush replayed journal: {code}"))?;
        Ok(restored)
    }

    fn block_on<F, T>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        self.handle.block_on(fut)
    }

    fn allocate_inode_id(&self) -> Result<u64> {
        self.client_state
            .next_inode_id(self.config.inode_batch, |count| {
                self.block_on(self.superblock.reserve_inodes(count))
            })
    }

    fn allocate_segment_id(&self) -> Result<u64> {
        self.client_state
            .next_segment_id(self.config.segment_batch, |count| {
                self.block_on(self.superblock.reserve_segments(count))
            })
    }

    fn build_child_path(parent: &InodeRecord, name: &str) -> String {
        if parent.inode == ROOT_INODE {
            format!("/{}", name)
        } else if parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path.trim_end_matches('/'), name)
        }
    }

    fn record_attr(record: &InodeRecord) -> FileAttr {
        FileAttr {
            ino: record.inode,
            size: record.size,
            blocks: (record.size + 511) / 512,
            atime: to_system_time(record.atime),
            mtime: to_system_time(record.mtime),
            ctime: to_system_time(record.ctime),
            crtime: to_system_time(record.ctime),
            kind: match record.kind {
                InodeKind::Directory { .. } => FileType::Directory,
                InodeKind::File => FileType::RegularFile,
                InodeKind::Symlink => FileType::Symlink,
                InodeKind::Tombstone => FileType::RegularFile,
            },
            perm: (record.mode & 0o7777) as u16,
            nlink: if record.is_dir() {
                2 + record.children().map(|c| c.len() as u32).unwrap_or(0)
            } else {
                record.link_count.max(1)
            },
            uid: record.uid,
            gid: record.gid,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }

    fn load_inode(&self, ino: u64) -> std::result::Result<InodeRecord, i32> {
        if let Some(entry) = self.pending_inodes.lock().get(&ino) {
            if matches!(entry.record.kind, InodeKind::Tombstone) {
                return Err(ENOENT);
            }
            return Ok(entry.record.clone());
        }
        if let Some(entry) = self.mutating_inodes.lock().get(&ino) {
            if matches!(entry.record.kind, InodeKind::Tombstone) {
                return Err(ENOENT);
            }
            return Ok(entry.record.clone());
        }
        if let Some(entry) = self.flushing_inodes.lock().get(&ino) {
            if matches!(entry.record.kind, InodeKind::Tombstone) {
                return Err(ENOENT);
            }
            return Ok(entry.record.clone());
        }
        self.block_on(self.metadata.get_inode_with_ttl(
            ino,
            self.lookup_cache_ttl,
            self.dir_cache_ttl,
        ))
        .map_err(|_| EIO)?
        .ok_or_else(|| {
            debug!(
                "load_inode miss ino={} pending={} mutating={} flushing={}",
                ino,
                self.pending_inodes.lock().contains_key(&ino),
                self.mutating_inodes.lock().contains_key(&ino),
                self.flushing_inodes.lock().contains_key(&ino)
            );
            ENOENT
        })
    }

    fn read_file_bytes(&self, record: &InodeRecord) -> Result<Vec<u8>> {
        if let Some(entry) = self.pending_inodes.lock().get(&record.inode) {
            if let Some(data) = &entry.data {
                return self.read_pending_bytes(data);
            }
        }
        if let Some(entry) = self.mutating_inodes.lock().get(&record.inode) {
            if let Some(data) = &entry.data {
                return self.read_pending_bytes(data);
            }
        }
        if let Some(entry) = self.flushing_inodes.lock().get(&record.inode) {
            if let Some(data) = &entry.data {
                return self.read_pending_bytes(data);
            }
        }
        match &record.storage {
            FileStorage::Inline(bytes) => Ok(bytes.clone()),
            FileStorage::LegacySegment(ptr) => self.segments.read_pointer(ptr),
            FileStorage::Segments(extents) => {
                let mut buffer = vec![0u8; record.size as usize];
                let mut ordered = extents.to_vec();
                ordered.sort_by_key(|ext| ext.logical_offset);
                for extent in ordered {
                    let start = extent.logical_offset as usize;
                    let bytes = self.segments.read_pointer(&extent.pointer)?;
                    let end = start + bytes.len();
                    if end > buffer.len() {
                        buffer.resize(end, 0);
                    }
                    buffer[start..end].copy_from_slice(&bytes);
                }
                Ok(buffer)
            }
        }
    }

    fn stage_inode(&self, record: InodeRecord) -> std::result::Result<(), i32> {
        let inode = record.inode;
        let mut map = self.pending_inodes.lock();
        match map.entry(inode) {
            Entry::Occupied(mut occupied) => {
                if matches!(record.kind, InodeKind::Tombstone) {
                    if let Some(data) = occupied.get_mut().data.take() {
                        let len = data.len();
                        self.release_pending_data(data);
                        if len > 0 {
                            let mut total = self.pending_bytes.lock();
                            *total = total.saturating_sub(len);
                        }
                    }
                }
                occupied.get_mut().record = record.clone();
            }
            Entry::Vacant(vacant) => {
                vacant.insert(PendingEntry {
                    record: record.clone(),
                    data: None,
                });
            }
        }
        drop(map);
        let kind = record.kind.clone();
        if let Some(journal) = &self.journal {
            let entry = JournalEntry {
                record,
                payload: JournalPayload::None,
            };
            journal.persist_entry(&entry).map_err(|_| EIO)?;
        }
        debug!(
            "stage_inode inode={} kind={:?} metadata-staged",
            inode, kind
        );
        self.flush_if_interval_elapsed()?;
        Ok(())
    }

    fn drop_pending_entry(&self, inode: u64) {
        let entry = {
            let mut map = self.pending_inodes.lock();
            map.remove(&inode)
        };
        if let Some(entry) = entry {
            if let Some(data) = entry.data {
                let len = data.len();
                self.release_pending_data(data);
                if len > 0 {
                    let mut total = self.pending_bytes.lock();
                    *total = total.saturating_sub(len);
                }
            }
        }
    }

    fn snapshot_journal_payload(&self, data: &PendingData) -> JournalPayload {
        match data {
            PendingData::Inline(bytes) => JournalPayload::Inline(bytes.clone()),
            PendingData::Staged(segments) => JournalPayload::StageChunks(segments.chunks.clone()),
        }
    }

    fn read_pending_bytes(&self, data: &PendingData) -> Result<Vec<u8>> {
        match data {
            PendingData::Inline(bytes) => Ok(bytes.clone()),
            PendingData::Staged(segments) => {
                let mut buffer = Vec::with_capacity(segments.total_len as usize);
                for chunk in &segments.chunks {
                    let bytes = self
                        .segments
                        .read_staged_chunk(chunk)
                        .map_err(|err| anyhow!("pending read failed: {err:?}"))?;
                    buffer.extend_from_slice(&bytes);
                }
                Ok(buffer)
            }
        }
    }

    fn release_pending_data(&self, data: PendingData) {
        if let PendingData::Staged(segments) = data {
            for chunk in segments.chunks {
                if let Err(err) = self.segments.release_staged_chunk(&chunk) {
                    log::warn!(
                        "failed to release staged payload {}: {err:?}",
                        chunk.path.display()
                    );
                }
            }
        }
    }

    fn update_parent(
        &self,
        parent: &mut InodeRecord,
        name: String,
        child: u64,
    ) -> std::result::Result<(), i32> {
        let entries = parent.children_mut().ok_or(ENOTDIR)?;
        entries.insert(name, child);
        parent.update_times();
        self.stage_inode(parent.clone())
    }

    fn remove_from_parent(
        &self,
        parent: &mut InodeRecord,
        name: &str,
    ) -> std::result::Result<(), i32> {
        let entries = parent.children_mut().ok_or(ENOTDIR)?;
        entries.remove(name);
        parent.update_times();
        self.stage_inode(parent.clone())
    }

    fn unlink_file_entry(
        &self,
        parent: &mut InodeRecord,
        name: &str,
        record: &mut InodeRecord,
    ) -> std::result::Result<(), i32> {
        self.remove_from_parent(parent, name)?;
        if record.link_count > 1 {
            record.dec_links();
            record.update_times();
            self.stage_inode(record.clone())?
        } else {
            self.drop_pending_entry(record.inode);
            let tombstone = InodeRecord::tombstone(record.inode);
            self.stage_inode(tombstone)?
        }
        Ok(())
    }

    fn refresh_descendant_paths(&self, inode: &InodeRecord) -> std::result::Result<(), i32> {
        if let Some(children) = inode.children() {
            let entries: Vec<(String, u64)> = children
                .iter()
                .map(|(name, ino)| (name.clone(), *ino))
                .collect();
            for (name, child_ino) in entries {
                let mut child = self.load_inode(child_ino)?;
                child.parent = inode.inode;
                child.name = name.clone();
                child.path = Self::build_child_path(inode, &name);
                child.update_times();
                self.stage_inode(child.clone())?;
                if child.is_dir() {
                    self.refresh_descendant_paths(&child)?;
                }
            }
        }
        Ok(())
    }

    fn is_descendant(&self, ancestor: u64, mut candidate: u64) -> std::result::Result<bool, i32> {
        if ancestor == candidate {
            return Ok(true);
        }
        let mut seen = HashSet::new();
        while seen.insert(candidate) {
            if candidate == ancestor {
                return Ok(true);
            }
            if candidate == ROOT_INODE {
                break;
            }
            let inode = self.load_inode(candidate)?;
            if inode.parent == candidate {
                break;
            }
            candidate = inode.parent;
        }
        Ok(false)
    }

    fn stage_file(
        &self,
        mut record: InodeRecord,
        data: Vec<u8>,
        ctx: Option<StageWriteContext>,
    ) -> std::result::Result<(), i32> {
        let start = Instant::now();
        record.size = data.len() as u64;
        record.update_times();
        let inode = record.inode;
        let new_len = data.len() as u64;
        let append_range = ctx.and_then(|context| {
            if context.prev_size <= context.write_offset && context.prev_size <= new_len {
                Some(context.prev_size as usize..data.len())
            } else {
                None
            }
        });
        let prev_entry = {
            let mut map = self.pending_inodes.lock();
            let entry = map.remove(&inode);
            let mut mutating = self.mutating_inodes.lock();
            mutating.insert(
                inode,
                entry.clone().unwrap_or(PendingEntry {
                    record: record.clone(),
                    data: None,
                }),
            );
            entry
        };
        let original_entry = prev_entry.clone();
        let mut prev_data = prev_entry.and_then(|entry| entry.data);
        let prev_len = prev_data.as_ref().map(|d| d.len()).unwrap_or(0);
        let inline_cap = self.config.inline_threshold as u64;
        let pending_data = if new_len <= inline_cap {
            if let Some(old) = prev_data.take() {
                self.release_pending_data(old);
            }
            PendingData::Inline(data)
        } else {
            let mut segments = match prev_data.take() {
                Some(PendingData::Staged(segs)) => segs,
                Some(PendingData::Inline(bytes)) => {
                    let chunk = match self.segments.stage_payload(&bytes) {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            self.restore_mutation_on_error(inode, original_entry);
                            return Err(EIO);
                        }
                    };
                    PendingSegments::from_chunk(chunk)
                }
                None => PendingSegments::new(),
            };
            if segments.total_len == 0 {
                let chunk = match self.segments.stage_payload(&data) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        self.restore_mutation_on_error(inode, original_entry);
                        return Err(EIO);
                    }
                };
                PendingData::Staged(PendingSegments::from_chunk(chunk))
            } else if let Some(range) = append_range {
                if range.start < range.end {
                    let chunk = match self.segments.stage_payload(&data[range]) {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            self.restore_mutation_on_error(inode, original_entry);
                            return Err(EIO);
                        }
                    };
                    segments.append(chunk);
                }
                PendingData::Staged(segments)
            } else {
                let chunk = match self.segments.stage_payload(&data) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        self.restore_mutation_on_error(inode, original_entry);
                        return Err(EIO);
                    }
                };
                let old = segments;
                let pending = PendingData::Staged(PendingSegments::from_chunk(chunk));
                self.release_pending_data(PendingData::Staged(old));
                pending
            }
        };
        let journal_payload = if self.journal.is_some() {
            Some(self.snapshot_journal_payload(&pending_data))
        } else {
            None
        };
        {
            let mut map = self.pending_inodes.lock();
            map.insert(
                inode,
                PendingEntry {
                    record: record.clone(),
                    data: Some(pending_data),
                },
            );
        }
        self.mutating_inodes.lock().remove(&inode);

        let delta = new_len as i64 - prev_len as i64;
        let mut total = self.pending_bytes.lock();
        if delta >= 0 {
            *total = total.saturating_add(delta as u64);
        } else {
            *total = total.saturating_sub((-delta) as u64);
        }
        let pending_total = *total;
        let should_flush = pending_total >= self.config.pending_bytes;
        drop(total);
        if let (Some(journal), Some(journal_payload)) = (&self.journal, journal_payload) {
            let journal_entry = JournalEntry {
                record: record.clone(),
                payload: journal_payload,
            };
            journal.persist_entry(&journal_entry).map_err(|_| EIO)?;
        }
        let path = record.path.clone();
        self.log_perf(
            "stage_file",
            start.elapsed(),
            json!({
                "inode": inode,
                "bytes": new_len,
                "pending_total": pending_total,
                "triggered_flush": should_flush,
                "filename": path,
            }),
        );
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        debug!(
            "stage_file pid={} tid={} inode={} path={} bytes={} pending={} flush_triggered={}",
            pid, tid, inode, record.path, new_len, pending_total, should_flush
        );
        if should_flush {
            self.flush_pending()?;
        } else {
            self.flush_if_interval_elapsed()?;
        }
        Ok(())
    }

    fn append_file(&self, mut record: InodeRecord, data: &[u8]) -> std::result::Result<(), i32> {
        if data.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        let inode = record.inode;
        record.update_times();
        let current_size = record.size;
        let new_len = current_size.saturating_add(data.len() as u64);
        let inline_cap = self.config.inline_threshold as u64;
        if new_len > inline_cap {
            return self.write_large_segments(record, current_size, data);
        }
        let mut needs_existing = false;
        {
            let mut map = self.pending_inodes.lock();
            let entry = map.entry(inode).or_insert_with(|| PendingEntry {
                record: record.clone(),
                data: None,
            });
            if entry.data.is_none() {
                entry.data = Some(PendingData::Inline(Vec::new()));
                needs_existing = record.size > 0;
            }
        }
        if needs_existing {
            let existing = self.read_file_bytes(&record).map_err(|_| EIO)?;
            let mut map = self.pending_inodes.lock();
            if let Some(entry) = map.get_mut(&inode) {
                entry.data = Some(PendingData::Inline(existing));
            }
        }
        let appended = data.len() as u64;
        let (prev_len, new_len, journal_payload) = {
            let mut map = self.pending_inodes.lock();
            let entry = map.get_mut(&inode).expect("pending entry must exist");
            entry.record = record.clone();
            let slot = entry
                .data
                .as_mut()
                .expect("pending data must exist before append");
            let prev_len = slot.len();
            match slot {
                PendingData::Inline(buf) => {
                    if buf.len() as u64 != record.size {
                        buf.resize(record.size as usize, 0);
                    }
                    buf.extend_from_slice(data);
                    if buf.len() as u64 > inline_cap {
                        let bytes = std::mem::take(buf);
                        let chunk = self.segments.stage_payload(&bytes).map_err(|_| EIO)?;
                        *slot = PendingData::Staged(PendingSegments::from_chunk(chunk));
                    }
                }
                PendingData::Staged(_) => unreachable!("staged append handled elsewhere"),
            }
            let current_len = slot.len();
            entry.record.size = current_len;
            record.size = current_len;
            let payload = self
                .journal
                .as_ref()
                .map(|_| self.snapshot_journal_payload(slot));
            (prev_len, current_len, payload)
        };
        let delta = new_len as i64 - prev_len as i64;
        let mut total = self.pending_bytes.lock();
        if delta >= 0 {
            *total = total.saturating_add(delta as u64);
        } else {
            *total = total.saturating_sub((-delta) as u64);
        }
        let pending_total = *total;
        let should_flush = pending_total >= self.config.pending_bytes;
        drop(total);
        if let (Some(journal), Some(payload)) = (&self.journal, journal_payload) {
            let entry = JournalEntry {
                record: record.clone(),
                payload,
            };
            journal.persist_entry(&entry).map_err(|_| EIO)?;
        }
        let path = record.path.clone();
        self.log_perf(
            "stage_file",
            start.elapsed(),
            json!({
                "inode": inode,
                "bytes": new_len,
                "appended_bytes": appended,
                "pending_total": pending_total,
                "triggered_flush": should_flush,
                "filename": path,
            }),
        );
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        debug!(
            "append_file pid={} tid={} inode={} path={} appended={} new_size={} pending={} flush_triggered={}",
            pid, tid, inode, record.path, appended, new_len, pending_total, should_flush
        );
        if should_flush {
            self.flush_pending()?;
        } else {
            self.flush_if_interval_elapsed()?;
        }
        Ok(())
    }

    fn write_large_segments(
        &self,
        mut record: InodeRecord,
        offset: u64,
        data: &[u8],
    ) -> std::result::Result<(), i32> {
        if data.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        let inode = record.inode;
        record.update_times();
        let prev_entry = {
            let mut map = self.pending_inodes.lock();
            let entry = map.remove(&inode);
            let mut mutating = self.mutating_inodes.lock();
            mutating.insert(
                inode,
                entry.clone().unwrap_or(PendingEntry {
                    record: record.clone(),
                    data: None,
                }),
            );
            entry
        };
        let original_entry = prev_entry.clone();
        let mut entry = prev_entry.unwrap_or(PendingEntry {
            record: record.clone(),
            data: None,
        });
        entry.record = record.clone();
        let mut data_state = entry.data.take();
        if data_state.is_none() {
            let existing = match self.read_file_bytes(&entry.record) {
                Ok(existing) => existing,
                Err(_) => {
                    self.restore_mutation_on_error(inode, original_entry);
                    return Err(EIO);
                }
            };
            if existing.is_empty() {
                data_state = Some(PendingData::Staged(PendingSegments::new()));
            } else if existing.len() as u64 <= self.config.inline_threshold as u64 {
                data_state = Some(PendingData::Inline(existing));
            } else {
                let chunk = match self.segments.stage_payload(&existing) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        self.restore_mutation_on_error(inode, original_entry);
                        return Err(EIO);
                    }
                };
                data_state = Some(PendingData::Staged(PendingSegments::from_chunk(chunk)));
            }
        }
        let mut segments = match data_state {
            Some(PendingData::Staged(segs)) => segs,
            Some(PendingData::Inline(bytes)) => {
                let chunk = match self.segments.stage_payload(&bytes) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        self.restore_mutation_on_error(inode, original_entry);
                        return Err(EIO);
                    }
                };
                PendingSegments::from_chunk(chunk)
            }
            None => PendingSegments::new(),
        };
        let prev_len = segments.total_len;
        if let Err(_) = segments.ensure_offset(&self.segments, offset) {
            self.release_pending_data(PendingData::Staged(segments));
            self.restore_mutation_on_error(inode, original_entry);
            return Err(EIO);
        }
        let staged_chunk = match self.segments.stage_payload(data) {
            Ok(chunk) => chunk,
            Err(_) => {
                self.release_pending_data(PendingData::Staged(segments));
                self.restore_mutation_on_error(inode, original_entry);
                return Err(EIO);
            }
        };
        if let Err(_) = segments.write_range(&self.segments, offset, staged_chunk) {
            self.release_pending_data(PendingData::Staged(segments));
            self.restore_mutation_on_error(inode, original_entry);
            return Err(EIO);
        }
        let new_len = segments.total_len;
        entry.record.size = new_len;
        record.size = new_len;
        entry.data = Some(PendingData::Staged(segments));
        let journal_payload = if self.journal.is_some() {
            entry
                .data
                .as_ref()
                .map(|data| self.snapshot_journal_payload(data))
        } else {
            None
        };
        {
            let mut map = self.pending_inodes.lock();
            map.insert(inode, entry);
        }
        self.mutating_inodes.lock().remove(&inode);
        let delta = new_len as i64 - prev_len as i64;
        let mut total = self.pending_bytes.lock();
        if delta >= 0 {
            *total = total.saturating_add(delta as u64);
        } else {
            *total = total.saturating_sub((-delta) as u64);
        }
        let pending_total = *total;
        let should_flush = pending_total >= self.config.pending_bytes;
        drop(total);
        if let (Some(journal), Some(payload)) = (&self.journal, journal_payload) {
            let journal_entry = JournalEntry {
                record: record.clone(),
                payload,
            };
            journal.persist_entry(&journal_entry).map_err(|_| EIO)?;
        }
        let path = record.path.clone();
        self.log_perf(
            "stage_segments",
            start.elapsed(),
            json!({
                "inode": inode,
                "bytes": new_len,
                "write_offset": offset,
                "write_len": data.len(),
                "pending_total": pending_total,
                "triggered_flush": should_flush,
                "filename": path,
            }),
        );
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        debug!(
            "stage_segments pid={} tid={} inode={} path={} offset={} len={} new_size={} pending={} flush_triggered={}",
            pid,
            tid,
            inode,
            record.path,
            offset,
            data.len(),
            new_len,
            pending_total,
            should_flush
        );
        if should_flush {
            self.flush_pending()?;
        } else {
            self.flush_if_interval_elapsed()?;
        }
        Ok(())
    }

    fn restore_mutation_on_error(&self, inode: u64, original: Option<PendingEntry>) {
        {
            let mut map = self.pending_inodes.lock();
            match original {
                Some(entry) => {
                    map.insert(inode, entry);
                }
                None => {
                    map.remove(&inode);
                }
            }
        }
        self.mutating_inodes.lock().remove(&inode);
    }

    fn flush_pending(&self) -> std::result::Result<(), i32> {
        let _flush_guard = self.flush_lock.lock();
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        let mut prepared_generation: Option<u64> = None;
        let mut drained_pending: Option<HashMap<u64, PendingEntry>> = None;
        let result = (|| {
            let start = Instant::now();
            let pending = {
                let mut guard = self.pending_inodes.lock();
                if guard.is_empty() {
                    self.touch_last_flush();
                    return Ok(());
                }
                let has_staged = guard
                    .values()
                    .any(|entry| matches!(entry.data, Some(PendingData::Staged(_))));
                if has_staged {
                    self.segments.rotate_stage_file();
                }
                let drained = std::mem::take(&mut *guard);
                let mut flushing = self.flushing_inodes.lock();
                for (inode, entry) in drained.iter() {
                    flushing.insert(*inode, entry.clone());
                }
                drained
            };
            drained_pending = Some(pending);
            let pending = drained_pending
                .as_ref()
                .expect("drained pending map must be present");
            debug!(
                "flush_pending pid={} tid={} preparing {} inodes",
                pid,
                tid,
                pending.len()
            );
            let snapshot = self
                .superblock
                .prepare_dirty_generation()
                .map_err(|_| EIO)?;
            let target_generation = snapshot.generation;
            prepared_generation = Some(target_generation);
            let mut segment_entries = Vec::new();
            let mut records = Vec::new();
            let mut flushed_bytes: u64 = 0;
            let mut inline_files = 0;
            let mut segment_files = 0;
            let mut inline_bytes: u64 = 0;
            let mut segment_bytes: u64 = 0;
            let mut metadata_only = 0;
            let mut flushed_inodes = Vec::new();
            for (inode, pending_entry) in pending.iter() {
                flushed_inodes.push(*inode);
                match &pending_entry.data {
                    Some(PendingData::Inline(data_bytes)) => {
                        let mut record = pending_entry.record.clone();
                        let data_len = data_bytes.len() as u64;
                        flushed_bytes = flushed_bytes.saturating_add(data_len);
                        record.size = data_len;
                        if data_len <= self.config.inline_threshold as u64 {
                            record.storage = FileStorage::Inline(data_bytes.clone());
                            inline_files += 1;
                            inline_bytes = inline_bytes.saturating_add(data_len);
                            records.push(record);
                        } else {
                            segment_entries.push(SegmentEntry {
                                inode: record.inode,
                                path: record.path.clone(),
                                data: data_bytes.clone(),
                            });
                            record.storage = FileStorage::Inline(Vec::new());
                            segment_files += 1;
                            segment_bytes = segment_bytes.saturating_add(data_len);
                            records.push(record);
                        }
                    }
                    Some(PendingData::Staged(segments)) => {
                        let mut record = pending_entry.record.clone();
                        let mut data_bytes = Vec::with_capacity(record.size as usize);
                        for chunk in &segments.chunks {
                            let chunk_bytes =
                                self.segments.read_staged_chunk(&chunk).map_err(|_| EIO)?;
                            data_bytes.extend_from_slice(&chunk_bytes);
                        }
                        let data_len = data_bytes.len() as u64;
                        flushed_bytes = flushed_bytes.saturating_add(data_len);
                        record.size = data_len;
                        segment_entries.push(SegmentEntry {
                            inode: record.inode,
                            path: record.path.clone(),
                            data: data_bytes,
                        });
                        record.storage = FileStorage::Inline(Vec::new());
                        segment_files += 1;
                        segment_bytes = segment_bytes.saturating_add(data_len);
                        records.push(record);
                    }
                    None => {
                        metadata_only += 1;
                        records.push(pending_entry.record.clone());
                    }
                }
            }
            let mut pointer_map = HashMap::new();
            let mut segment_id_logged = None;
            let mut segment_write_duration = Duration::from_secs(0);
            if !segment_entries.is_empty() {
                let segment_id = self.allocate_segment_id().map_err(|_| EIO)?;
                let seg_start = Instant::now();
                let pointers = self
                    .segments
                    .write_batch(target_generation, segment_id, segment_entries)
                    .map_err(|_| EIO)?;
                pointer_map = pointers.into_iter().collect();
                segment_id_logged = Some(segment_id);
                segment_write_duration = seg_start.elapsed();
            }
            let persist_start = Instant::now();
            for record in records.iter_mut() {
                if record.size > self.config.inline_threshold as u64 {
                    if let Some(ptr) = pointer_map.get(&record.inode) {
                        record.storage =
                            FileStorage::Segments(vec![SegmentExtent::new(0, ptr.clone())]);
                    } else {
                        return Err(EIO);
                    }
                }
            }
            self.block_on(self.metadata.persist_inodes_batch(
                &records,
                target_generation,
                self.config.shard_size,
                self.config.imap_delta_batch,
            ))
            .map_err(|_| EIO)?;
            let metadata_duration = persist_start.elapsed();
            let commit_start = Instant::now();
            if self
                .block_on(self.superblock.commit_generation(target_generation))
                .is_err()
            {
                self.superblock.abort_generation(target_generation);
                prepared_generation = None;
                return Err(EIO);
            }
            prepared_generation = None;
            let commit_duration = commit_start.elapsed();
            let mut total = self.pending_bytes.lock();
            *total = total.saturating_sub(flushed_bytes);
            let pending_remaining = *total;
            drop(total);
            let flushed_entries = drained_pending
                .take()
                .expect("drained pending map must exist until flush commit");
            {
                let mut flushing = self.flushing_inodes.lock();
                for inode in flushed_entries.keys() {
                    flushing.remove(inode);
                }
            }
            for pending_entry in flushed_entries.into_values() {
                if let Some(data) = pending_entry.data {
                    self.release_pending_data(data);
                }
            }
            if let Some(journal) = &self.journal {
                let clearable_inodes = {
                    let guard = self.pending_inodes.lock();
                    flushed_inodes
                        .into_iter()
                        .filter(|inode| !guard.contains_key(inode))
                        .collect::<Vec<_>>()
                };
                for inode in clearable_inodes {
                    journal.clear_entry(inode).map_err(|_| EIO)?;
                }
            }
            self.log_perf(
                "flush_pending",
                start.elapsed(),
                json!({
                    "target_generation": target_generation,
                    "files": inline_files + segment_files,
                    "inline_files": inline_files,
                    "segment_files": segment_files,
                    "metadata_only": metadata_only,
                    "inline_bytes": inline_bytes,
                    "segment_bytes": segment_bytes,
                    "flushed_bytes": flushed_bytes,
                    "segment_id": segment_id_logged,
                    "segment_write_ms": segment_write_duration.as_secs_f64() * 1000.0,
                    "metadata_ms": metadata_duration.as_secs_f64() * 1000.0,
                    "commit_ms": commit_duration.as_secs_f64() * 1000.0,
                    "pending_remaining": pending_remaining,
                }),
            );
            debug!(
                "flush_pending pid={} tid={} gen={} inline_files={} segment_files={} metadata_only={} inline_bytes={} segment_bytes={} pending_remaining={}",
                pid,
                tid,
                target_generation,
                inline_files,
                segment_files,
                metadata_only,
                inline_bytes,
                segment_bytes,
                pending_remaining
            );
            self.touch_last_flush();
            Ok(())
        })();
        if let Err(code) = result {
            if let Some(pending_gen) = prepared_generation.take() {
                self.superblock.abort_generation(pending_gen);
            }
            if let Some(pending) = drained_pending.take() {
                self.restore_pending_after_failed_flush(pending);
            }
            error!("flush_pending failed pid={} tid={} code={}", pid, tid, code);
        }
        result
    }

    fn restore_pending_after_failed_flush(&self, restored: HashMap<u64, PendingEntry>) {
        let mut dropped_bytes = 0u64;
        let restored_inodes: Vec<u64> = restored.keys().copied().collect();
        let mut map = self.pending_inodes.lock();
        for (inode, entry) in restored {
            match map.entry(inode) {
                Entry::Vacant(vacant) => {
                    vacant.insert(entry);
                }
                Entry::Occupied(_) => {
                    if let Some(data) = entry.data {
                        dropped_bytes = dropped_bytes.saturating_add(data.len());
                        self.release_pending_data(data);
                    }
                }
            }
        }
        drop(map);
        let mut flushing = self.flushing_inodes.lock();
        for inode in restored_inodes {
            flushing.remove(&inode);
        }
        drop(flushing);
        if dropped_bytes > 0 {
            let mut total = self.pending_bytes.lock();
            *total = total.saturating_sub(dropped_bytes);
        }
    }

    fn log_perf(&self, event: &str, duration: Duration, details: serde_json::Value) {
        if let Some(logger) = &self.perf {
            logger.log(event, duration, details);
        }
    }

    fn flush_if_interval_elapsed(&self) -> std::result::Result<(), i32> {
        let Some(interval) = self.flush_interval else {
            return Ok(());
        };
        let should_flush = {
            let guard = self.last_flush.lock();
            guard.elapsed() >= interval
        };
        if should_flush {
            debug!("flush_interval {:?} elapsed, triggering flush", interval);
            self.flush_pending()?;
        }
        Ok(())
    }

    fn touch_last_flush(&self) {
        *self.last_flush.lock() = Instant::now();
    }

    fn log_fuse_error(&self, op: &str, detail: &str, code: i32) {
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        error!(
            "{} failed pid={} tid={} {} errno={}",
            op, pid, tid, detail, code
        );
    }

    pub fn nfs_lookup(&self, parent: u64, name: &str) -> std::result::Result<InodeRecord, i32> {
        let parent_inode = self.load_inode(parent)?;
        let child = parent_inode
            .children()
            .and_then(|children| children.get(name).copied())
            .ok_or(ENOENT)?;
        self.load_inode(child)
    }

    pub fn nfs_getattr(&self, ino: u64) -> std::result::Result<InodeRecord, i32> {
        self.load_inode(ino)
    }

    pub fn nfs_readdir(&self, ino: u64) -> std::result::Result<Vec<(u64, String)>, i32> {
        let inode = self.load_inode(ino)?;
        if !inode.is_dir() {
            return Err(ENOTDIR);
        }
        let mut entries = Vec::new();
        if let Some(children) = inode.children() {
            entries.reserve(children.len());
            for (name, child) in children {
                entries.push((*child, name.clone()));
            }
        }
        Ok(entries)
    }

    pub fn nfs_setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
    ) -> std::result::Result<InodeRecord, i32> {
        let _mutation_guard = self.mutation_lock.lock();
        let mut record = self.load_inode(ino)?;
        if let Some(new_mode) = mode {
            record.mode = (record.mode & !0o7777) | (new_mode & 0o7777);
        }
        if let Some(new_uid) = uid {
            record.uid = new_uid;
        }
        if let Some(new_gid) = gid {
            record.gid = new_gid;
        }
        let mut data = self.read_file_bytes(&record).map_err(|_| EIO)?;
        if let Some(target_size) = size {
            if record.is_dir() {
                return Err(EISDIR);
            }
            data.resize(target_size as usize, 0);
        }
        record.update_times();
        self.stage_file(record.clone(), data, None)?;
        Ok(record)
    }

    pub fn nfs_create(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let _mutation_guard = self.mutation_lock.lock();
        let mut parent_inode = self.load_inode(parent)?;
        if !parent_inode.is_dir() {
            return Err(ENOTDIR);
        }
        let name = name.to_string();
        if parent_inode
            .children()
            .map(|children| children.contains_key(&name))
            .unwrap_or(false)
        {
            return Err(EEXIST);
        }
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, &name);
        let mut file = InodeRecord::new_file(inode_id, parent, name.clone(), path, uid, gid);
        file.update_times();
        self.stage_inode(file.clone())?;
        self.update_parent(&mut parent_inode, name, inode_id)?;
        if self.load_inode(inode_id).is_err() {
            error!(
                "nfs_create visibility check failed ino={} parent={} pending={} flushing={}",
                inode_id,
                parent,
                self.pending_inodes.lock().contains_key(&inode_id),
                self.flushing_inodes.lock().contains_key(&inode_id)
            );
        }
        Ok(file)
    }

    pub fn nfs_mkdir(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let _mutation_guard = self.mutation_lock.lock();
        let mut parent_inode = self.load_inode(parent)?;
        if !parent_inode.is_dir() {
            return Err(ENOTDIR);
        }
        let name = name.to_string();
        if parent_inode
            .children()
            .map(|children| children.contains_key(&name))
            .unwrap_or(false)
        {
            return Err(EEXIST);
        }
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, &name);
        let mut dir = InodeRecord::new_directory(inode_id, parent, name.clone(), path, uid, gid);
        dir.update_times();
        self.stage_inode(dir.clone())?;
        self.update_parent(&mut parent_inode, name, inode_id)?;
        Ok(dir)
    }

    pub fn nfs_symlink(
        &self,
        parent: u64,
        name: &str,
        target: Vec<u8>,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let _mutation_guard = self.mutation_lock.lock();
        let mut parent_inode = self.load_inode(parent)?;
        if !parent_inode.is_dir() {
            return Err(ENOTDIR);
        }
        let name = name.to_string();
        if parent_inode
            .children()
            .map(|children| children.contains_key(&name))
            .unwrap_or(false)
        {
            return Err(EEXIST);
        }
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, &name);
        let record =
            InodeRecord::new_symlink(inode_id, parent, name.clone(), path, uid, gid, target);
        self.stage_inode(record.clone())?;
        self.update_parent(&mut parent_inode, name, inode_id)?;
        Ok(record)
    }

    pub fn nfs_read(&self, ino: u64, offset: u64, size: u32) -> std::result::Result<Vec<u8>, i32> {
        let record = self.load_inode(ino)?;
        if record.is_dir() {
            return Err(EISDIR);
        }
        let data = self.read_file_bytes(&record).map_err(|_| EIO)?;
        let offset = offset as usize;
        if offset >= data.len() {
            return Ok(Vec::new());
        }
        let end = std::cmp::min(offset + size as usize, data.len());
        Ok(data[offset..end].to_vec())
    }

    pub fn nfs_readlink(&self, ino: u64) -> std::result::Result<Vec<u8>, i32> {
        let record = self.load_inode(ino)?;
        if let Some(target) = record.symlink_target_bytes() {
            Ok(target.to_vec())
        } else {
            Err(EINVAL)
        }
    }

    pub fn nfs_write(&self, ino: u64, offset: u64, data: &[u8]) -> std::result::Result<u32, i32> {
        let _mutation_guard = self.mutation_lock.lock();
        let mut record = match self.load_inode(ino) {
            Ok(record) => record,
            Err(code) => {
                let metadata_exists = self
                    .block_on(self.metadata.get_inode(ino))
                    .map(|opt| opt.is_some())
                    .unwrap_or(false);
                error!(
                    "nfs_write load_inode failed ino={} code={} offset={} len={} pending={} flushing={} metadata_exists={}",
                    ino,
                    code,
                    offset,
                    data.len(),
                    self.pending_inodes.lock().contains_key(&ino),
                    self.flushing_inodes.lock().contains_key(&ino),
                    metadata_exists
                );
                return Err(code);
            }
        };
        if record.is_dir() {
            return Err(EISDIR);
        }
        let prev_size = record.size;
        let write_end = offset.saturating_add(data.len() as u64);
        if offset == prev_size {
            if write_end <= self.config.inline_threshold as u64 {
                self.append_file(record, data)?;
            } else {
                self.write_large_segments(record, offset, data)?;
            }
            return Ok(data.len() as u32);
        }
        if write_end > self.config.inline_threshold as u64 {
            self.write_large_segments(record, offset, data)?;
            return Ok(data.len() as u32);
        }
        let mut existing = self.read_file_bytes(&record).map_err(|_| EIO)?;
        let offset = offset as usize;
        if offset > existing.len() {
            existing.resize(offset, 0);
        }
        if offset + data.len() > existing.len() {
            existing.resize(offset + data.len(), 0);
        }
        existing[offset..offset + data.len()].copy_from_slice(data);
        record.update_times();
        let ctx = StageWriteContext {
            prev_size,
            write_offset: offset as u64,
        };
        self.stage_file(record, existing, Some(ctx))?;
        Ok(data.len() as u32)
    }

    pub fn nfs_remove_file(&self, parent: u64, name: &str) -> std::result::Result<(), i32> {
        let _mutation_guard = self.mutation_lock.lock();
        let mut parent_inode = self.load_inode(parent)?;
        if !parent_inode.is_dir() {
            return Err(ENOTDIR);
        }
        let child_ino = parent_inode
            .children()
            .and_then(|children| children.get(name).copied())
            .ok_or(ENOENT)?;
        let mut child = self.load_inode(child_ino)?;
        if child.is_dir() {
            return Err(EISDIR);
        }
        self.unlink_file_entry(&mut parent_inode, name, &mut child)
    }

    pub fn nfs_remove_dir(&self, parent: u64, name: &str) -> std::result::Result<(), i32> {
        let _mutation_guard = self.mutation_lock.lock();
        let mut parent_inode = self.load_inode(parent)?;
        if !parent_inode.is_dir() {
            return Err(ENOTDIR);
        }
        let child_ino = parent_inode
            .children()
            .and_then(|children| children.get(name).copied())
            .ok_or(ENOENT)?;
        let child = self.load_inode(child_ino)?;
        if !child.is_dir() {
            return Err(ENOTDIR);
        }
        if child.children().map(|c| !c.is_empty()).unwrap_or(false) {
            return Err(ENOTEMPTY);
        }
        let tombstone = InodeRecord::tombstone(child_ino);
        self.stage_inode(tombstone)?;
        self.remove_from_parent(&mut parent_inode, name)?;
        Ok(())
    }

    pub fn nfs_rename(
        &self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
        flags: u32,
    ) -> std::result::Result<(), i32> {
        let _mutation_guard = self.mutation_lock.lock();
        if flags & !(RENAME_NOREPLACE_FLAG) != 0 {
            return Err(EINVAL);
        }
        let mut src_parent = self.load_inode(parent)?;
        if !src_parent.is_dir() {
            return Err(ENOTDIR);
        }
        let mut dst_parent = self.load_inode(newparent)?;
        if !dst_parent.is_dir() {
            return Err(ENOTDIR);
        }
        let old_name = name.to_string();
        let new_name = newname.to_string();
        let child_ino = src_parent
            .children()
            .and_then(|children| children.get(&old_name).copied())
            .ok_or(ENOENT)?;
        if parent == newparent && old_name == new_name {
            return Ok(());
        }
        let mut target = self.load_inode(child_ino)?;
        if target.is_dir() && self.is_descendant(target.inode, newparent)? {
            return Err(EINVAL);
        }
        if let Some(existing) = dst_parent
            .children()
            .and_then(|children| children.get(&new_name).copied())
        {
            if flags & RENAME_NOREPLACE_FLAG != 0 {
                return Err(EEXIST);
            }
            if existing != target.inode {
                let mut victim = self.load_inode(existing)?;
                if victim.is_dir() {
                    if !target.is_dir() {
                        return Err(EISDIR);
                    }
                    if victim.children().map(|c| !c.is_empty()).unwrap_or(false) {
                        return Err(ENOTEMPTY);
                    }
                    self.remove_from_parent(&mut dst_parent, &new_name)?;
                    let tombstone = InodeRecord::tombstone(victim.inode);
                    self.stage_inode(tombstone)?;
                } else {
                    self.unlink_file_entry(&mut dst_parent, &new_name, &mut victim)?;
                }
            }
        }
        self.remove_from_parent(&mut src_parent, &old_name)?;
        self.update_parent(&mut dst_parent, new_name.clone(), target.inode)?;
        target.parent = dst_parent.inode;
        target.name = new_name;
        target.path = Self::build_child_path(&dst_parent, &target.name);
        target.update_times();
        self.stage_inode(target.clone())?;
        if target.is_dir() {
            self.refresh_descendant_paths(&target)?;
        }
        Ok(())
    }

    pub fn nfs_flush(&self) -> std::result::Result<(), i32> {
        self.flush_pending()
    }
}

impl Filesystem for OsageFs {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), libc::c_int> {
        Ok(())
    }

    fn destroy(&mut self) {
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        info!("destroy invoked pid={} tid={}", pid, tid);
        if let Err(code) = self.flush_pending() {
            error!(
                "flush during destroy failed pid={} tid={} code={}",
                pid, tid, code
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let res = (|| {
            let mut record = self.load_inode(ino)?;
            if let Some(new_mode) = mode {
                record.mode = (record.mode & !0o7777) | (new_mode & 0o7777);
            }
            if let Some(new_uid) = uid {
                record.uid = new_uid;
            }
            if let Some(new_gid) = gid {
                record.gid = new_gid;
            }
            let mut data = self.read_file_bytes(&record).map_err(|_| EIO)?;
            if let Some(target_size) = size {
                if record.is_dir() {
                    return Err(EISDIR);
                }
                data.resize(target_size as usize, 0);
            }
            record.update_times();
            let attr = Self::record_attr(&record);
            self.stage_file(record, data, None)?;
            Ok(attr)
        })();
        match res {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(code) => reply.error(code),
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(EINVAL);
                return;
            }
        };
        let response = (|| {
            let parent_inode = self.load_inode(parent)?;
            let child = parent_inode
                .children()
                .and_then(|children| children.get(name_str).copied())
                .ok_or(ENOENT)?;
            let child_inode = self.load_inode(child)?;
            Ok(child_inode)
        })();
        match response {
            Ok(inode) => {
                let attr = Self::record_attr(&inode);
                let generation = self.superblock.snapshot().generation;
                reply.entry(&TTL, &attr, generation);
            }
            Err(code) => reply.error(code),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self.load_inode(ino) {
            Ok(record) => {
                let attr = Self::record_attr(&record);
                reply.attr(&TTL, &attr);
            }
            Err(code) => reply.error(code),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let response = (|| {
            let inode = self.load_inode(ino)?;
            if !inode.is_dir() {
                return Err(ENOTDIR);
            }
            let mut entries = Vec::new();
            entries.push((ino, FileType::Directory, String::from(".")));
            let parent = if ino == ROOT_INODE { ino } else { inode.parent };
            entries.push((parent, FileType::Directory, String::from("..")));
            if let Some(children) = inode.children() {
                for (name, child) in children {
                    let child_inode = self.load_inode(*child)?;
                    entries.push((child_inode.inode, file_type(&child_inode), name.clone()));
                }
            }
            Ok(entries)
        })();
        match response {
            Ok(entries) => {
                for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(ino, (i + 1) as i64, kind, name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(code) => reply.error(code),
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let uid = req.uid();
        let gid = req.gid();
        let res = (|| {
            let mut parent_inode = self.load_inode(parent)?;
            if !parent_inode.is_dir() {
                return Err(ENOTDIR);
            }
            let name = name.to_str().ok_or(EINVAL)?.to_string();
            if parent_inode
                .children()
                .map(|children| children.contains_key(&name))
                .unwrap_or(false)
            {
                return Err(EEXIST);
            }
            let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
            let path = Self::build_child_path(&parent_inode, &name);
            let mut dir =
                InodeRecord::new_directory(inode_id, parent, name.clone(), path, uid, gid);
            dir.update_times();
            self.stage_inode(dir.clone())?;
            self.update_parent(&mut parent_inode, name, inode_id)?;
            Ok(dir)
        })();
        match res {
            Ok(dir) => {
                let attr = Self::record_attr(&dir);
                let generation = self.superblock.snapshot().generation;
                reply.entry(&TTL, &attr, generation);
            }
            Err(code) => reply.error(code),
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let uid = req.uid();
        let gid = req.gid();
        let res = (|| {
            let mut parent_inode = self.load_inode(parent)?;
            if !parent_inode.is_dir() {
                return Err(ENOTDIR);
            }
            let name = name.to_str().ok_or(EINVAL)?.to_string();
            if parent_inode
                .children()
                .map(|children| children.contains_key(&name))
                .unwrap_or(false)
            {
                return Err(EEXIST);
            }
            let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
            let path = Self::build_child_path(&parent_inode, &name);
            let mut file = InodeRecord::new_file(inode_id, parent, name.clone(), path, uid, gid);
            file.update_times();
            self.stage_inode(file.clone())?;
            self.update_parent(&mut parent_inode, name, inode_id)?;
            Ok(file)
        })();
        match res {
            Ok(file) => {
                let attr = Self::record_attr(&file);
                let generation = self.superblock.snapshot().generation;
                // FUSE open reply flags are FOPEN_* bits, not O_* request flags.
                reply.created(&TTL, &attr, generation, 0, 0);
            }
            Err(code) => {
                let detail = format!("parent={} name={}", parent, name.to_string_lossy());
                self.log_fuse_error("create", &detail, code);
                reply.error(code);
            }
        }
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        let uid = req.uid();
        let gid = req.gid();
        let res = (|| {
            let mut parent_inode = self.load_inode(parent)?;
            if !parent_inode.is_dir() {
                return Err(ENOTDIR);
            }
            let name = name.to_str().ok_or(EINVAL)?.to_string();
            if parent_inode
                .children()
                .map(|children| children.contains_key(&name))
                .unwrap_or(false)
            {
                return Err(EEXIST);
            }
            let target = path_to_bytes(link);
            let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
            let path = Self::build_child_path(&parent_inode, &name);
            let record =
                InodeRecord::new_symlink(inode_id, parent, name.clone(), path, uid, gid, target);
            self.stage_inode(record.clone())?;
            self.update_parent(&mut parent_inode, name, inode_id)?;
            Ok(record)
        })();
        match res {
            Ok(record) => {
                let attr = Self::record_attr(&record);
                let generation = self.superblock.snapshot().generation;
                reply.entry(&TTL, &attr, generation);
            }
            Err(code) => reply.error(code),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.load_inode(ino) {
            Ok(_) => {
                // FUSE open reply flags are FOPEN_* bits, not O_* request flags.
                reply.opened(0, 0)
            }
            Err(code) => reply.error(code),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let res: std::result::Result<Vec<u8>, i32> = (|| {
            let record = self.load_inode(ino)?;
            if record.is_dir() {
                return Err(EISDIR);
            }
            let data = self.read_file_bytes(&record).map_err(|_| EIO)?;
            let offset = offset as usize;
            if offset >= data.len() {
                return Ok(Vec::new());
            }
            let end = std::cmp::min(offset + size as usize, data.len());
            Ok(data[offset..end].to_vec())
        })();
        match res {
            Ok(bytes) => reply.data(&bytes),
            Err(code) => reply.error(code),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.load_inode(ino) {
            Ok(record) => {
                if let Some(target) = record.symlink_target_bytes() {
                    reply.data(target);
                } else {
                    reply.error(EINVAL);
                }
            }
            Err(code) => reply.error(code),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let res = (|| {
            let mut record = self.load_inode(ino)?;
            if record.is_dir() {
                return Err(EISDIR);
            }
            let prev_size = record.size;
            let write_end = (offset as u64).saturating_add(data.len() as u64);
            if offset as u64 == prev_size {
                if write_end <= self.config.inline_threshold as u64 {
                    self.append_file(record, data)?;
                } else {
                    self.write_large_segments(record, offset as u64, data)?;
                }
                return Ok(data.len() as u32);
            }
            if write_end > self.config.inline_threshold as u64 {
                self.write_large_segments(record, offset as u64, data)?;
                return Ok(data.len() as u32);
            }
            let mut existing = self.read_file_bytes(&record).map_err(|_| EIO)?;
            let offset = offset as usize;
            if offset > existing.len() {
                existing.resize(offset, 0);
            }
            if offset + data.len() > existing.len() {
                existing.resize(offset + data.len(), 0);
            }
            existing[offset..offset + data.len()].copy_from_slice(data);
            record.update_times();
            let ctx = StageWriteContext {
                prev_size,
                write_offset: offset as u64,
            };
            self.stage_file(record, existing, Some(ctx))?;
            Ok(data.len() as u32)
        })();
        match res {
            Ok(size) => reply.written(size),
            Err(code) => {
                let detail = format!("ino={} offset={} len={}", ino, offset, data.len());
                self.log_fuse_error("write", &detail, code);
                reply.error(code);
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let res = (|| {
            let mut parent_inode = self.load_inode(parent)?;
            if !parent_inode.is_dir() {
                return Err(ENOTDIR);
            }
            let name_str = name.to_str().ok_or(EINVAL)?;
            let child_ino = parent_inode
                .children()
                .and_then(|children| children.get(name_str).copied())
                .ok_or(ENOENT)?;
            let mut child = self.load_inode(child_ino)?;
            if child.is_dir() {
                return Err(EISDIR);
            }
            self.unlink_file_entry(&mut parent_inode, name_str, &mut child)?;
            Ok(())
        })();
        match res {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let res = (|| {
            let mut parent_inode = self.load_inode(parent)?;
            if !parent_inode.is_dir() {
                return Err(ENOTDIR);
            }
            let name_str = name.to_str().ok_or(EINVAL)?;
            let child_ino = parent_inode
                .children()
                .and_then(|children| children.get(name_str).copied())
                .ok_or(ENOENT)?;
            let child = self.load_inode(child_ino)?;
            if !child.is_dir() {
                return Err(ENOTDIR);
            }
            if child.children().map(|c| !c.is_empty()).unwrap_or(false) {
                return Err(ENOTEMPTY);
            }
            let tombstone = InodeRecord::tombstone(child_ino);
            self.stage_inode(tombstone)?;
            self.remove_from_parent(&mut parent_inode, name_str)?;
            Ok(())
        })();
        match res {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        let res = (|| {
            if flags & !(RENAME_NOREPLACE_FLAG) != 0 {
                return Err(EINVAL);
            }
            let mut src_parent = self.load_inode(parent)?;
            if !src_parent.is_dir() {
                return Err(ENOTDIR);
            }
            let mut dst_parent = self.load_inode(newparent)?;
            if !dst_parent.is_dir() {
                return Err(ENOTDIR);
            }
            let old_name = name.to_str().ok_or(EINVAL)?.to_string();
            let new_name = newname.to_str().ok_or(EINVAL)?.to_string();
            let child_ino = src_parent
                .children()
                .and_then(|children| children.get(&old_name).copied())
                .ok_or(ENOENT)?;
            if parent == newparent && old_name == new_name {
                return Ok(());
            }
            let mut target = self.load_inode(child_ino)?;
            if target.is_dir() && self.is_descendant(target.inode, newparent)? {
                return Err(EINVAL);
            }
            if let Some(existing) = dst_parent
                .children()
                .and_then(|children| children.get(&new_name).copied())
            {
                if flags & RENAME_NOREPLACE_FLAG != 0 {
                    return Err(EEXIST);
                }
                if existing != target.inode {
                    let mut victim = self.load_inode(existing)?;
                    if victim.is_dir() {
                        if !target.is_dir() {
                            return Err(EISDIR);
                        }
                        if victim.children().map(|c| !c.is_empty()).unwrap_or(false) {
                            return Err(ENOTEMPTY);
                        }
                        self.remove_from_parent(&mut dst_parent, &new_name)?;
                        let tombstone = InodeRecord::tombstone(victim.inode);
                        self.stage_inode(tombstone)?;
                    } else {
                        self.unlink_file_entry(&mut dst_parent, &new_name, &mut victim)?;
                    }
                }
            }
            self.remove_from_parent(&mut src_parent, &old_name)?;
            self.update_parent(&mut dst_parent, new_name.clone(), target.inode)?;
            target.parent = dst_parent.inode;
            target.name = new_name;
            target.path = Self::build_child_path(&dst_parent, &target.name);
            target.update_times();
            self.stage_inode(target.clone())?;
            if target.is_dir() {
                self.refresh_descendant_paths(&target)?;
            }
            Ok(())
        })();
        match res {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let res = (|| {
            let mut file = self.load_inode(ino)?;
            if file.is_dir() {
                return Err(EPERM);
            }
            let mut parent_inode = self.load_inode(newparent)?;
            if !parent_inode.is_dir() {
                return Err(ENOTDIR);
            }
            let name = newname.to_str().ok_or(EINVAL)?.to_string();
            if parent_inode
                .children()
                .map(|children| children.contains_key(&name))
                .unwrap_or(false)
            {
                return Err(EEXIST);
            }
            file.inc_links();
            file.update_times();
            self.stage_inode(file.clone())?;
            self.update_parent(&mut parent_inode, name, file.inode)?;
            Ok(file)
        })();
        match res {
            Ok(record) => {
                let attr = Self::record_attr(&record);
                let generation = self.superblock.snapshot().generation;
                reply.entry(&TTL, &attr, generation);
            }
            Err(code) => reply.error(code),
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(
            STATFS_BLOCKS,
            STATFS_BLOCKS,
            STATFS_BLOCKS,
            STATFS_FILES,
            STATFS_FILES,
            STATFS_BLOCK_SIZE,
            255,
            STATFS_BLOCK_SIZE,
        );
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        if self.fsync_on_close {
            match self.flush_pending() {
                Ok(()) => reply.ok(),
                Err(code) => reply.error(code),
            }
        } else {
            reply.ok();
        }
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.flush_pending() {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if self.fsync_on_close {
            match self.flush_pending() {
                Ok(()) => reply.ok(),
                Err(code) => reply.error(code),
            }
        } else {
            reply.ok();
        }
    }
}

fn file_type(record: &InodeRecord) -> FileType {
    match record.kind {
        InodeKind::Directory { .. } => FileType::Directory,
        InodeKind::File => FileType::RegularFile,
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

#[derive(Clone)]
struct PendingEntry {
    record: InodeRecord,
    data: Option<PendingData>,
}

#[derive(Clone, Copy)]
struct StageWriteContext {
    prev_size: u64,
    write_offset: u64,
}

#[derive(Clone)]
enum PendingData {
    Inline(Vec<u8>),
    Staged(PendingSegments),
}

#[derive(Clone)]
struct PendingSegments {
    chunks: Vec<StagedChunk>,
    total_len: u64,
}

impl PendingSegments {
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            total_len: 0,
        }
    }

    fn from_chunk(chunk: StagedChunk) -> Self {
        let mut segments = Self::new();
        segments.append(chunk);
        segments
    }

    fn append(&mut self, chunk: StagedChunk) {
        if chunk.len == 0 {
            return;
        }
        self.total_len = self.total_len.saturating_add(chunk.len);
        self.chunks.push(chunk);
    }

    fn ensure_offset(&mut self, manager: &SegmentManager, target: u64) -> Result<()> {
        if target <= self.total_len {
            return Ok(());
        }
        let gap = target - self.total_len;
        if gap == 0 {
            return Ok(());
        }
        let gap_len = usize::try_from(gap).map_err(|_| anyhow!("gap too large"))?;
        let zeros = vec![0u8; gap_len];
        let chunk = manager
            .stage_payload(&zeros)
            .map_err(|_| anyhow!("stage gap"))?;
        self.append(chunk);
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
        let write_end = offset.saturating_add(chunk.len);
        let mut cursor = 0u64;
        let mut pending_chunk = Some(chunk);
        let mut result = Vec::with_capacity(self.chunks.len() + 1);
        for existing in self.chunks.drain(..) {
            let chunk_len = existing.len;
            let chunk_start = cursor;
            let chunk_end = chunk_start + chunk_len;
            if chunk_end <= offset || chunk_start >= write_end {
                if chunk_start >= write_end {
                    if let Some(new_chunk) = pending_chunk.take() {
                        result.push(new_chunk);
                    }
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
            cursor = chunk_end;
        }
        if let Some(new_chunk) = pending_chunk.take() {
            result.push(new_chunk);
        }
        self.chunks = result;
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
mod tests {
    use super::*;
    use std::env;
    use std::path::Path;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::tempdir;

    use crate::config::ObjectStoreProvider;
    use crate::journal::JournalManager;

    struct TestHarness {
        runtime: tokio::runtime::Runtime,
        metadata: Arc<MetadataStore>,
        fs: OsageFs,
        config: Config,
    }

    impl TestHarness {
        fn new(root: &Path, state_name: &str, pending_bytes: u64) -> Self {
            Self::with_config(root, state_name, pending_bytes, |_| {})
        }

        fn with_config<F>(root: &Path, state_name: &str, pending_bytes: u64, tweak: F) -> Self
        where
            F: FnOnce(&mut Config),
        {
            let mut config = test_config(root, state_name, pending_bytes);
            tweak(&mut config);
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let metadata = Arc::new(
                runtime
                    .block_on(MetadataStore::open(
                        &config.store_path,
                        config.shard_size,
                        config.log_storage_io,
                    ))
                    .unwrap(),
            );
            let superblock = Arc::new(
                runtime
                    .block_on(SuperblockManager::load_or_init(
                        metadata.clone(),
                        config.shard_size,
                    ))
                    .unwrap(),
            );
            ensure_root_for_tests(&runtime, metadata.clone(), superblock.clone(), &config);
            let segments =
                Arc::new(SegmentManager::new(&config, runtime.handle().clone()).unwrap());
            let client_state = Arc::new(ClientStateManager::load(&config.state_path).unwrap());
            let journal = if config.disable_journal {
                None
            } else {
                Some(Arc::new(
                    JournalManager::new(&config.local_cache_path).unwrap(),
                ))
            };
            let fs = OsageFs::new(
                config.clone(),
                metadata.clone(),
                superblock,
                segments,
                journal,
                runtime.handle().clone(),
                client_state,
                None,
            );
            Self {
                runtime,
                metadata,
                fs,
                config,
            }
        }
    }

    fn test_config(root: &Path, state_name: &str, pending_bytes: u64) -> Config {
        Config {
            mount_path: root.join("mnt"),
            store_path: root.join("store"),
            local_cache_path: root.join("cache"),
            inline_threshold: 512,
            shard_size: 64,
            inode_batch: 8,
            segment_batch: 8,
            pending_bytes,
            object_provider: ObjectStoreProvider::Local,
            bucket: None,
            region: None,
            endpoint: None,
            object_prefix: String::new(),
            gcs_service_account: None,
            state_path: root.join(state_name),
            foreground: false,
            home_prefix: "/home".into(),
            perf_log: None,
            disable_journal: false,
            fsync_on_close: false,
            flush_interval_ms: 0,
            disable_cleanup: false,
            lookup_cache_ttl_ms: 0,
            dir_cache_ttl_ms: 0,
            metadata_poll_interval_ms: 0,
            segment_cache_bytes: 0,
            log_file: None,
            debug_log: false,
            imap_delta_batch: 32,
            log_storage_io: false,
        }
    }

    fn ensure_root_for_tests(
        runtime: &tokio::runtime::Runtime,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        config: &Config,
    ) {
        let uid = unsafe { libc::geteuid() as u32 };
        let gid = unsafe { libc::getegid() as u32 };
        let desired_mode = 0o40777;
        if let Some(mut root) = runtime.block_on(metadata.get_inode(ROOT_INODE)).unwrap() {
            if root.uid != uid || root.gid != gid || root.mode != desired_mode {
                root.uid = uid;
                root.gid = gid;
                root.mode = desired_mode;
                let snapshot = superblock.prepare_dirty_generation().unwrap();
                let generation = snapshot.generation;
                runtime
                    .block_on(metadata.persist_inode(&root, generation, config.shard_size))
                    .unwrap();
                runtime
                    .block_on(superblock.commit_generation(generation))
                    .unwrap();
            }
            return;
        }
        let snapshot = superblock.prepare_dirty_generation().unwrap();
        let generation = snapshot.generation;
        let mut root = InodeRecord::new_directory(
            ROOT_INODE,
            ROOT_INODE,
            String::from(""),
            String::from("/"),
            uid,
            gid,
        );
        root.mode = desired_mode;
        runtime
            .block_on(metadata.persist_inode(&root, generation, config.shard_size))
            .unwrap();
        runtime
            .block_on(superblock.commit_generation(generation))
            .unwrap();
    }

    fn apply_write(fs: &OsageFs, inode: u64, offset: usize, payload: &[u8]) {
        let mut record = fs.load_inode(inode).unwrap();
        let mut bytes = fs.read_file_bytes(&record).unwrap();
        if offset + payload.len() > bytes.len() {
            bytes.resize(offset + payload.len(), 0);
        }
        bytes[offset..offset + payload.len()].copy_from_slice(payload);
        record.update_times();
        fs.stage_file(record, bytes, None).unwrap();
    }

    fn stage_named_file(harness: &TestHarness, name: &str, data: Vec<u8>) -> u64 {
        let inode = harness.fs.allocate_inode_id().unwrap();
        let record = make_file(inode, name);
        harness.fs.stage_file(record, data, None).unwrap();
        let mut root = harness.fs.load_inode(ROOT_INODE).unwrap();
        harness
            .fs
            .update_parent(&mut root, name.to_string(), inode)
            .unwrap();
        inode
    }

    fn load_named_inode(fs: &OsageFs, name: &str) -> InodeRecord {
        let root = fs.load_inode(ROOT_INODE).unwrap();
        let children = root.children().unwrap();
        let inode = *children.get(name).unwrap();
        fs.load_inode(inode).unwrap()
    }

    fn make_file(inode: u64, name: &str) -> InodeRecord {
        InodeRecord::new_file(
            inode,
            ROOT_INODE,
            name.to_string(),
            format!("/{}", name),
            0,
            0,
        )
    }

    fn make_symlink(inode: u64, name: &str, target: &str) -> InodeRecord {
        InodeRecord::new_symlink(
            inode,
            ROOT_INODE,
            name.to_string(),
            format!("/{}", name),
            0,
            0,
            target.as_bytes().to_vec(),
        )
    }

    #[test]
    fn single_client_inline_and_segment_flush() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "client_a.bin", 8 * 1024 * 1024);

        let inode_inline = harness.fs.allocate_inode_id().unwrap();
        let record_inline = make_file(inode_inline, "foo.txt");
        harness
            .fs
            .stage_file(record_inline, b"hello".to_vec(), None)
            .unwrap();

        let inode_seg = harness.fs.allocate_inode_id().unwrap();
        let record_seg = make_file(inode_seg, "bar.bin");
        let data = vec![7u8; harness.config.inline_threshold + 128];
        harness
            .fs
            .stage_file(record_seg, data.clone(), None)
            .unwrap();
        harness.fs.flush_pending().unwrap();

        let stored_inline = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_inline))
            .unwrap()
            .unwrap();
        match stored_inline.storage {
            FileStorage::Inline(ref bytes) => assert_eq!(bytes, b"hello"),
            _ => panic!("expected inline storage"),
        }

        let stored_seg = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_seg))
            .unwrap()
            .unwrap();
        match stored_seg.storage {
            FileStorage::Segments(_) => {
                let roundtrip = harness.fs.read_file_bytes(&stored_seg).unwrap();
                assert_eq!(roundtrip, data);
            }
            _ => panic!("expected segment storage"),
        }
    }

    #[test]
    fn multiple_clients_flush_independently() {
        let dir = tempdir().unwrap();

        let inode_a = {
            let harness = TestHarness::new(dir.path(), "client_one.bin", 8 * 1024 * 1024);
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, "client1.txt");
            harness
                .fs
                .stage_file(record, b"alpha".to_vec(), None)
                .unwrap();
            harness.fs.flush_pending().unwrap();
            inode
        };

        let inode_b = {
            let harness = TestHarness::new(dir.path(), "client_two.bin", 8 * 1024 * 1024);
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, "client2.txt");
            harness
                .fs
                .stage_file(record, b"beta".to_vec(), None)
                .unwrap();
            harness.fs.flush_pending().unwrap();
            inode
        };

        let harness = TestHarness::new(dir.path(), "client_reader.bin", 8 * 1024 * 1024);
        let a = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_a))
            .unwrap()
            .unwrap();
        let b = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_b))
            .unwrap()
            .unwrap();
        assert!(matches!(a.storage, FileStorage::Inline(_)));
        assert!(matches!(b.storage, FileStorage::Inline(_)));

        let state_a = std::fs::read(dir.path().join("client_one.bin")).unwrap();
        let state_b = std::fs::read(dir.path().join("client_two.bin")).unwrap();
        assert_ne!(state_a, state_b);
    }

    #[test]
    fn staged_extents_handle_random_writes() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "random.bin", 64 * 1024 * 1024);
        let inode = stage_named_file(&harness, "random.dat", Vec::new());
        let patterns: Vec<(usize, Vec<u8>)> = vec![
            (0, vec![1u8; 512 * 1024]),
            (256 * 1024, vec![2u8; 256 * 1024]),
            (768 * 1024, vec![3u8; 192 * 1024]),
            (512 * 1024, vec![4u8; 128 * 1024]),
            (1_200_000, vec![5u8; 400 * 1024]),
        ];
        let total_len = patterns
            .iter()
            .map(|(offset, bytes)| offset + bytes.len())
            .max()
            .unwrap();
        let mut expected = vec![0u8; total_len];
        for (offset, payload) in &patterns {
            expected[*offset..*offset + payload.len()].copy_from_slice(payload);
            let record = harness.fs.load_inode(inode).unwrap();
            harness
                .fs
                .write_large_segments(record, *offset as u64, payload)
                .unwrap();
        }
        harness.fs.flush_pending().unwrap();
        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode))
            .unwrap()
            .unwrap();
        let bytes = harness.fs.read_file_bytes(&stored).unwrap();
        assert_eq!(&bytes[..total_len], &expected[..]);
    }

    #[test]
    fn write_path_meets_perf_target() {
        let dir = tempdir().unwrap();
        let harness =
            TestHarness::with_config(dir.path(), "throughput.bin", 256 * 1024 * 1024, |cfg| {
                cfg.disable_journal = true;
                cfg.flush_interval_ms = 0;
            });
        let inode = stage_named_file(&harness, "throughput.dat", Vec::new());
        let chunk_size = 256 * 1024;
        let iterations = 80;
        let total_bytes = chunk_size * iterations;
        let payload = vec![0xAB; chunk_size];
        let start = Instant::now();
        for i in 0..iterations {
            let offset = (i * chunk_size) as u64;
            let record = harness.fs.load_inode(inode).unwrap();
            harness
                .fs
                .write_large_segments(record, offset, &payload)
                .unwrap();
        }
        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        assert!(secs > 0.0, "elapsed time too small for measurement");
        let throughput = (total_bytes as f64 / (1024.0 * 1024.0)) / secs;
        assert!(
            throughput >= 10.0,
            "throughput {:.2} MiB/s below 10 MiB/s in {:?}",
            throughput,
            elapsed
        );
    }

    #[test]
    fn untar_style_perf_mixture() {
        if env::var("OSAGEFS_RUN_PERF").is_err() {
            eprintln!("skipping untar_style_perf_mixture; set OSAGEFS_RUN_PERF=1 to enable");
            return;
        }
        let dir = tempdir().unwrap();
        let harness = TestHarness::with_config(dir.path(), "untar.bin", 256 * 1024 * 1024, |cfg| {
            cfg.disable_journal = true;
            cfg.flush_interval_ms = 0;
        });
        let mut sizes = Vec::new();
        sizes.extend(std::iter::repeat(4 * 1024).take(2000));
        sizes.extend(std::iter::repeat(64 * 1024).take(512));
        sizes.extend(std::iter::repeat(512 * 1024).take(64));
        let mut total_bytes = 0usize;
        let start = Instant::now();
        for (idx, size) in sizes.into_iter().enumerate() {
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, &format!("untar_{idx}"));
            let payload = vec![(idx & 0xff) as u8; size];
            harness.fs.stage_file(record, payload, None).unwrap();
            total_bytes += size;
        }
        harness.fs.flush_pending().unwrap();
        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        assert!(secs > 0.0, "elapsed time too small for throughput");
        let throughput = (total_bytes as f64 / (1024.0 * 1024.0)) / secs;
        assert!(
            throughput >= 10.0,
            "untar-style throughput {:.2} MiB/s below 10 MiB/s in {:?}",
            throughput,
            elapsed
        );
    }

    #[test]
    fn stress_flush_respects_pending_threshold() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "stress.bin", 1024);
        let mut inodes = Vec::new();
        for i in 0..10 {
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, &format!("stress{i}"));
            harness
                .fs
                .stage_file(record, vec![i as u8; 600], None)
                .unwrap();
            inodes.push(inode);
        }
        harness.fs.flush_pending().unwrap();
        assert!(harness.fs.pending_inodes.lock().is_empty());
        assert_eq!(*harness.fs.pending_bytes.lock(), 0);
        for inode in inodes {
            let record = harness
                .runtime
                .block_on(harness.metadata.get_inode(inode))
                .unwrap()
                .unwrap();
            assert_eq!(record.size, 600);
        }
    }

    #[test]
    fn journal_replay_flushes_staged_entries() {
        let dir = tempdir().unwrap();
        let inode_id;
        {
            let harness = TestHarness::new(dir.path(), "journal.bin", 8 * 1024 * 1024);
            inode_id = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode_id, "pending.txt");
            harness
                .fs
                .stage_file(record, b"hello".to_vec(), None)
                .unwrap();
            // drop harness without flushing to simulate crash
        }

        let harness = TestHarness::new(dir.path(), "journal.bin", 8 * 1024 * 1024);
        let replayed = harness.fs.replay_journal().unwrap();
        assert_eq!(replayed, 1);
        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_id))
            .unwrap()
            .unwrap();
        assert_eq!(stored.size, 5);
        assert!(matches!(stored.storage, FileStorage::Inline(_)));
    }

    #[test]
    fn symlink_roundtrip_persists_target() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "symlink.bin", 8 * 1024 * 1024);
        let inode = harness.fs.allocate_inode_id().unwrap();
        let mut root = harness.fs.load_inode(ROOT_INODE).unwrap();
        let record = make_symlink(inode, "link", "/tmp/actual");
        harness.fs.stage_inode(record.clone()).unwrap();
        harness
            .fs
            .update_parent(&mut root, "link".to_string(), inode)
            .unwrap();
        harness.fs.flush_pending().unwrap();

        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode))
            .unwrap()
            .unwrap();
        assert!(stored.is_symlink());
        assert_eq!(stored.symlink_target_bytes().unwrap(), b"/tmp/actual");
    }

    #[test]
    fn hardlinks_update_reference_counts() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "hardlinks.bin", 8 * 1024 * 1024);
        let inode = harness.fs.allocate_inode_id().unwrap();
        let primary_name = "file_a";
        let secondary_name = "file_b";
        let file = make_file(inode, primary_name);
        harness
            .fs
            .stage_file(file.clone(), b"payload".to_vec(), None)
            .unwrap();
        let mut root = harness.fs.load_inode(ROOT_INODE).unwrap();
        harness
            .fs
            .update_parent(&mut root, primary_name.to_string(), inode)
            .unwrap();
        harness.fs.flush_pending().unwrap();

        // create second hardlink
        let mut root = harness.fs.load_inode(ROOT_INODE).unwrap();
        let mut stored_file = harness.fs.load_inode(inode).unwrap();
        stored_file.inc_links();
        harness.fs.stage_inode(stored_file.clone()).unwrap();
        harness
            .fs
            .update_parent(&mut root, secondary_name.to_string(), inode)
            .unwrap();
        harness.fs.flush_pending().unwrap();

        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode))
            .unwrap()
            .unwrap();
        assert_eq!(stored.link_count, 2);
        let root = harness.fs.load_inode(ROOT_INODE).unwrap();
        let children = root.children().unwrap();
        assert!(children.contains_key(primary_name));
        assert!(children.contains_key(secondary_name));

        // remove the second link and ensure reference count drops back to 1
        let mut root = harness.fs.load_inode(ROOT_INODE).unwrap();
        let mut current = harness.fs.load_inode(inode).unwrap();
        harness
            .fs
            .unlink_file_entry(&mut root, secondary_name, &mut current)
            .unwrap();
        harness.fs.flush_pending().unwrap();
        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode))
            .unwrap()
            .unwrap();
        assert_eq!(stored.link_count, 1);
        let root = harness.fs.load_inode(ROOT_INODE).unwrap();
        let children = root.children().unwrap();
        assert!(children.contains_key(primary_name));
        assert!(!children.contains_key(secondary_name));
    }

    #[test]
    fn stress_varied_workloads() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "stresswork.bin", 64 * 1024 * 1024);

        // Single file create + verify
        let inode_single = harness.fs.allocate_inode_id().unwrap();
        let record_single = make_file(inode_single, "single.txt");
        harness
            .fs
            .stage_file(record_single.clone(), b"alpha".to_vec(), None)
            .unwrap();
        harness.fs.flush_pending().unwrap();
        let stored_single = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_single))
            .unwrap()
            .unwrap();
        assert_eq!(stored_single.size, 5);
        assert_eq!(
            harness.fs.read_file_bytes(&stored_single).unwrap(),
            b"alpha"
        );

        // Burst of 1000 files of varying sizes
        let mut samples = Vec::new();
        for i in 0..1000 {
            let inode = harness.fs.allocate_inode_id().unwrap();
            let mut record = make_file(inode, &format!("bulk_{i}"));
            record.mode = 0o100600;
            let len = 32 + (i % 128) as usize;
            let data = vec![(i & 0xff) as u8; len];
            harness.fs.stage_file(record, data.clone(), None).unwrap();
            if i % 200 == 0 {
                samples.push((inode, data));
            }
        }
        harness.fs.flush_pending().unwrap();
        for (inode, data) in samples {
            let stored = harness
                .runtime
                .block_on(harness.metadata.get_inode(inode))
                .unwrap()
                .unwrap();
            assert_eq!(stored.size as usize, data.len());
            assert_eq!(harness.fs.read_file_bytes(&stored).unwrap(), data);
        }

        // Offset writes: start, middle, end
        let inode_offsets = harness.fs.allocate_inode_id().unwrap();
        let base = make_file(inode_offsets, "offsets.bin");
        harness
            .fs
            .stage_file(base.clone(), vec![0u8; 4096], None)
            .unwrap();
        harness.fs.flush_pending().unwrap();
        apply_write(&harness.fs, inode_offsets, 0, b"START");
        apply_write(&harness.fs, inode_offsets, 2048, b"MIDDLE");
        apply_write(&harness.fs, inode_offsets, 4096, b"TAIL");
        harness.fs.flush_pending().unwrap();
        let stored_offset = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_offsets))
            .unwrap()
            .unwrap();
        let bytes = harness.fs.read_file_bytes(&stored_offset).unwrap();
        assert!(bytes.starts_with(b"START"));
        assert_eq!(&bytes[2048..2054], b"MIDDLE");
        assert_eq!(&bytes[bytes.len() - 4..], b"TAIL");

        // Attribute mutation
        let attr_inode = harness.fs.allocate_inode_id().unwrap();
        let attr_file = make_file(attr_inode, "attrs");
        harness
            .fs
            .stage_file(attr_file.clone(), b"data".to_vec(), None)
            .unwrap();
        harness.fs.flush_pending().unwrap();
        let mut record = harness.fs.load_inode(attr_inode).unwrap();
        record.mode = 0o100700;
        record.uid = 1234;
        record.gid = 4321;
        harness.fs.stage_inode(record).unwrap();
        harness.fs.flush_pending().unwrap();
        let stored_attr = harness
            .runtime
            .block_on(harness.metadata.get_inode(attr_inode))
            .unwrap()
            .unwrap();
        assert_eq!(stored_attr.mode & 0o777, 0o700);
        assert_eq!(stored_attr.uid, 1234);
        assert_eq!(stored_attr.gid, 4321);
    }

    #[test]
    fn script_style_workload_without_fuse() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "scriptstyle.bin", 64 * 1024 * 1024);

        // Step 1: single file
        stage_named_file(&harness, "single.txt", b"alpha".to_vec());

        // Step 2: 1000 bulk files with small payloads
        for i in 1..=1000 {
            let name = format!("bulk_{i}.txt");
            let payload = format!("file-{i:04}").into_bytes();
            stage_named_file(&harness, &name, payload);
        }
        let root = harness.fs.load_inode(ROOT_INODE).unwrap();
        assert_eq!(root.children().unwrap().len(), 1001);

        // Step 3: varied payload sizes
        let small_bytes = vec![0xAB; 512];
        stage_named_file(&harness, "small.bin", small_bytes.clone());
        let medium_bytes = vec![0xBC; 65_536];
        stage_named_file(&harness, "medium.bin", medium_bytes.clone());
        let large_bytes = vec![0xCD; 2 * 1024 * 1024];
        stage_named_file(&harness, "large.bin", large_bytes.clone());

        // Step 4: offset writes
        let offsets_inode = stage_named_file(&harness, "offsets.bin", vec![0u8; 4096]);
        apply_write(&harness.fs, offsets_inode, 0, b"START");
        apply_write(&harness.fs, offsets_inode, 2048, b"MIDDLE");
        apply_write(&harness.fs, offsets_inode, 4096, b"TAIL");

        // Step 5: attribute changes
        let attrs_inode = stage_named_file(&harness, "attrs.txt", b"data".to_vec());
        let mut attrs = harness.fs.load_inode(attrs_inode).unwrap();
        attrs.mode = 0o100700;
        attrs.uid = 777;
        attrs.gid = 888;
        harness.fs.stage_inode(attrs).unwrap();

        harness.fs.flush_pending().unwrap();

        // Validate small/medium/large contents
        let small = load_named_inode(&harness.fs, "small.bin");
        assert_eq!(small.size as usize, small_bytes.len());
        assert_eq!(harness.fs.read_file_bytes(&small).unwrap(), small_bytes);

        let medium = load_named_inode(&harness.fs, "medium.bin");
        assert_eq!(medium.size as usize, medium_bytes.len());
        assert_eq!(harness.fs.read_file_bytes(&medium).unwrap(), medium_bytes);

        let large = load_named_inode(&harness.fs, "large.bin");
        assert_eq!(large.size as usize, large_bytes.len());
        assert_eq!(harness.fs.read_file_bytes(&large).unwrap(), large_bytes);

        let offsets = load_named_inode(&harness.fs, "offsets.bin");
        let bytes = harness.fs.read_file_bytes(&offsets).unwrap();
        assert!(bytes.starts_with(b"START"));
        assert_eq!(&bytes[2048..2054], b"MIDDLE");
        assert_eq!(&bytes[bytes.len() - 4..], b"TAIL");

        let attrs_after = load_named_inode(&harness.fs, "attrs.txt");
        assert_eq!(attrs_after.mode & 0o777, 0o700);
        assert_eq!(attrs_after.uid, 777);
        assert_eq!(attrs_after.gid, 888);
    }

    #[test]
    fn concurrent_flush_does_not_hide_pending_inodes() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::with_config(dir.path(), "concurrent_flush.bin", 1 << 30, |cfg| {
            cfg.disable_journal = true;
            cfg.flush_interval_ms = 0;
        });
        let file = harness.fs.nfs_create(ROOT_INODE, "race.bin", 0, 0).unwrap();

        thread::scope(|scope| {
            let fs = &harness.fs;
            let inode = file.inode;
            let writer = scope.spawn(move || {
                let payload = vec![0x5Au8; 8192];
                for i in 0..64u64 {
                    fs.nfs_write(inode, i * payload.len() as u64, &payload)
                        .unwrap();
                }
            });
            let flusher = scope.spawn(move || {
                for _ in 0..64 {
                    fs.flush_pending().unwrap();
                }
            });
            writer.join().unwrap();
            flusher.join().unwrap();
        });

        harness.fs.flush_pending().unwrap();
        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(file.inode))
            .unwrap()
            .unwrap();
        assert!(stored.size > 0);
    }

    #[test]
    fn load_inode_visible_during_large_segment_mutation() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::with_config(dir.path(), "visibility.bin", 1 << 30, |cfg| {
            cfg.disable_journal = true;
            cfg.flush_interval_ms = 0;
        });
        let file = harness.fs.nfs_create(ROOT_INODE, "visibility.dat", 0, 0).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let write_payload = vec![0x33u8; 256 * 1024];

        thread::scope(|scope| {
            let fs = &harness.fs;
            let start = barrier.clone();
            let inode = file.inode;
            let writer = scope.spawn(move || {
                start.wait();
                for i in 0..128u64 {
                    let record = fs.load_inode(inode).unwrap();
                    fs.write_large_segments(record, i * write_payload.len() as u64, &write_payload)
                        .unwrap();
                }
            });
            let fs = &harness.fs;
            let start = barrier.clone();
            let inode = file.inode;
            let reader = scope.spawn(move || {
                start.wait();
                for _ in 0..50_000 {
                    let record = fs.load_inode(inode);
                    assert!(
                        !matches!(record, Err(code) if code == ENOENT),
                        "inode visibility hole during mutation"
                    );
                }
            });
            writer.join().unwrap();
            reader.join().unwrap();
        });
    }
}
