use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use libc::{EEXIST, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, ENOTEMPTY, EPERM};
use parking_lot::Mutex;
use time::OffsetDateTime;
use tokio::runtime::Handle;

use crate::config::Config;
use crate::inode::{FileStorage, InodeKind, InodeRecord, ROOT_INODE};
use crate::metadata::MetadataStore;
use crate::segment::{SegmentEntry, SegmentManager};
use crate::state::ClientStateManager;
use crate::superblock::{FilesystemState, SuperblockManager};

const TTL: Duration = Duration::from_secs(1);

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
    pending_files: Mutex<HashMap<u64, PendingFile>>,
    pending_bytes: Mutex<u64>,
}

impl OsageFs {
    pub fn new(
        config: Config,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        segments: Arc<SegmentManager>,
        handle: Handle,
        client_state: Arc<ClientStateManager>,
    ) -> Self {
        Self {
            config,
            metadata,
            superblock,
            segments,
            handle,
            client_state,
            pending_files: Mutex::new(HashMap::new()),
            pending_bytes: Mutex::new(0),
        }
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
        if let Some(entry) = self.pending_files.lock().get(&ino) {
            return Ok(entry.record.clone());
        }
        self.block_on(self.metadata.get_inode(ino))
            .map_err(|_| EIO)?
            .ok_or(ENOENT)
    }

    fn persist_inode(&self, record: &InodeRecord, generation: u64) -> std::result::Result<(), i32> {
        self.block_on(
            self.metadata
                .persist_inode(record, generation, self.config.shard_size),
        )
        .map_err(|_| EIO)
    }

    fn remove_inode(&self, ino: u64, generation: u64) -> std::result::Result<(), i32> {
        self.block_on(
            self.metadata
                .remove_inode(ino, generation, self.config.shard_size),
        )
        .map_err(|_| EIO)
    }

    fn read_file_bytes(&self, record: &InodeRecord) -> Result<Vec<u8>> {
        if let Some(entry) = self.pending_files.lock().get(&record.inode) {
            return Ok(entry.data.clone());
        }
        match &record.storage {
            FileStorage::Inline(bytes) => Ok(bytes.clone()),
            FileStorage::Segment(ptr) => self.segments.read_pointer(ptr),
        }
    }

    fn update_parent(
        &self,
        parent: &mut InodeRecord,
        name: String,
        child: u64,
        generation: u64,
    ) -> std::result::Result<(), i32> {
        let entries = parent.children_mut().ok_or(ENOTDIR)?;
        entries.insert(name, child);
        parent.update_times();
        self.persist_inode(parent, generation)
    }

    fn remove_from_parent(
        &self,
        parent: &mut InodeRecord,
        name: &str,
        generation: u64,
    ) -> std::result::Result<(), i32> {
        let entries = parent.children_mut().ok_or(ENOTDIR)?;
        entries.remove(name);
        parent.update_times();
        self.persist_inode(parent, generation)
    }

    fn unlink_file_entry(
        &self,
        parent: &mut InodeRecord,
        name: &str,
        record: &mut InodeRecord,
        generation: u64,
    ) -> std::result::Result<(), i32> {
        self.remove_from_parent(parent, name, generation)?;
        if record.link_count > 1 {
            record.dec_links();
            record.update_times();
            self.update_pending_metadata(record);
            self.persist_inode(record, generation)
        } else {
            self.pending_files.lock().remove(&record.inode);
            self.remove_inode(record.inode, generation)
        }
    }

    fn refresh_descendant_paths(
        &self,
        inode: &InodeRecord,
        generation: u64,
    ) -> std::result::Result<(), i32> {
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
                self.persist_inode(&child, generation)?;
                if child.is_dir() {
                    self.refresh_descendant_paths(&child, generation)?;
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

    fn stage_file(&self, mut record: InodeRecord, data: Vec<u8>) -> std::result::Result<(), i32> {
        record.size = data.len() as u64;
        record.update_times();
        let inode = record.inode;
        let new_len = data.len() as u64;
        let mut map = self.pending_files.lock();
        let prev_len = map.get(&inode).map(|p| p.data.len() as u64).unwrap_or(0);
        map.insert(inode, PendingFile { record, data });
        drop(map);

        let delta = new_len as i64 - prev_len as i64;
        let mut total = self.pending_bytes.lock();
        if delta >= 0 {
            *total = total.saturating_add(delta as u64);
        } else {
            *total = total.saturating_sub((-delta) as u64);
        }
        let should_flush = *total >= self.config.pending_bytes;
        drop(total);
        if should_flush {
            self.flush_pending()?;
        }
        Ok(())
    }

    fn flush_pending(&self) -> std::result::Result<(), i32> {
        let pending = {
            let mut guard = self.pending_files.lock();
            if guard.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *guard)
        };
        let target_generation = self.superblock.snapshot().generation.saturating_add(1);
        let mut segment_entries = Vec::new();
        let mut records = Vec::new();
        let mut flushed_bytes: u64 = 0;
        for (_, pending_file) in pending.into_iter() {
            let mut record = pending_file.record;
            let data = pending_file.data;
            let data_len = data.len() as u64;
            flushed_bytes = flushed_bytes.saturating_add(data_len);
            record.size = data_len;
            if data_len <= self.config.inline_threshold as u64 {
                record.storage = FileStorage::Inline(data.clone());
            } else {
                segment_entries.push(SegmentEntry {
                    inode: record.inode,
                    path: record.path.clone(),
                    data,
                });
                record.storage = FileStorage::Inline(Vec::new());
            }
            records.push(record);
        }
        let mut pointer_map = HashMap::new();
        if !segment_entries.is_empty() {
            let segment_id = self.allocate_segment_id().map_err(|_| EIO)?;
            let pointers = self
                .segments
                .write_batch(target_generation, segment_id, segment_entries)
                .map_err(|_| EIO)?;
            pointer_map = pointers.into_iter().collect();
        }
        for mut record in records {
            if record.size > self.config.inline_threshold as u64 {
                if let Some(ptr) = pointer_map.get(&record.inode) {
                    record.storage = FileStorage::Segment(ptr.clone());
                } else {
                    return Err(EIO);
                }
            }
            self.persist_inode(&record, target_generation)?;
        }
        self.block_on(self.superblock.commit_generation(target_generation))
            .map_err(|_| EIO)?;
        let mut total = self.pending_bytes.lock();
        *total = total.saturating_sub(flushed_bytes);
        Ok(())
    }

    fn begin_transaction(&self) -> std::result::Result<u64, i32> {
        self.block_on(self.superblock.mutate(|sb| {
            sb.state = FilesystemState::Dirty;
        }))
        .map(|(_, snapshot)| snapshot.generation)
        .map_err(|_| EIO)
    }

    fn finish_transaction(&self) -> std::result::Result<(), i32> {
        self.block_on(self.superblock.mark_clean()).map_err(|_| EIO)
    }

    fn update_pending_metadata(&self, record: &InodeRecord) {
        if let Some(entry) = self.pending_files.lock().get_mut(&record.inode) {
            entry.record = record.clone();
        }
    }
}

impl Filesystem for OsageFs {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), libc::c_int> {
        Ok(())
    }

    fn destroy(&mut self) {
        let _ = self.flush_pending();
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
            self.stage_file(record, data)?;
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
            let generation = self.begin_transaction()?;
            self.persist_inode(&dir, generation)?;
            self.update_parent(&mut parent_inode, name, inode_id, generation)?;
            self.finish_transaction()?;
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
        flags: i32,
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
            let generation = self.begin_transaction()?;
            self.persist_inode(&file, generation)?;
            self.update_parent(&mut parent_inode, name, inode_id, generation)?;
            self.finish_transaction()?;
            Ok(file)
        })();
        match res {
            Ok(file) => {
                let attr = Self::record_attr(&file);
                let generation = self.superblock.snapshot().generation;
                reply.created(&TTL, &attr, generation, 0, flags as u32);
            }
            Err(code) => reply.error(code),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        match self.load_inode(ino) {
            Ok(_) => reply.opened(0, flags as u32),
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
            self.stage_file(record, existing)?;
            Ok(data.len() as u32)
        })();
        match res {
            Ok(size) => reply.written(size),
            Err(code) => reply.error(code),
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
            let generation = self.begin_transaction()?;
            self.unlink_file_entry(&mut parent_inode, name_str, &mut child, generation)?;
            self.finish_transaction()?;
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
            let generation = self.begin_transaction()?;
            self.remove_inode(child_ino, generation)?;
            self.remove_from_parent(&mut parent_inode, name_str, generation)?;
            self.finish_transaction()?;
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
            let generation = self.begin_transaction()?;
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
                        self.remove_from_parent(&mut dst_parent, &new_name, generation)?;
                        self.remove_inode(victim.inode, generation)?;
                    } else {
                        self.unlink_file_entry(
                            &mut dst_parent,
                            &new_name,
                            &mut victim,
                            generation,
                        )?;
                    }
                }
            }
            self.remove_from_parent(&mut src_parent, &old_name, generation)?;
            self.update_parent(&mut dst_parent, new_name.clone(), target.inode, generation)?;
            target.parent = dst_parent.inode;
            target.name = new_name;
            target.path = Self::build_child_path(&dst_parent, &target.name);
            target.update_times();
            self.update_pending_metadata(&target);
            self.persist_inode(&target, generation)?;
            if target.is_dir() {
                self.refresh_descendant_paths(&target, generation)?;
            }
            self.finish_transaction()?;
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
            self.update_pending_metadata(&file);
            let generation = self.begin_transaction()?;
            self.persist_inode(&file, generation)?;
            self.update_parent(&mut parent_inode, name, file.inode, generation)?;
            self.finish_transaction()?;
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
        reply.statfs(1_000_000, 500_000, 500_000, 1_000_000, 0, 4096, 255, 0);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        match self.flush_pending() {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
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
        match self.flush_pending() {
            Ok(()) => reply.ok(),
            Err(code) => reply.error(code),
        }
    }
}

fn file_type(record: &InodeRecord) -> FileType {
    match record.kind {
        InodeKind::Directory { .. } => FileType::Directory,
        InodeKind::File => FileType::RegularFile,
        InodeKind::Tombstone => FileType::RegularFile,
    }
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

struct PendingFile {
    record: InodeRecord,
    data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::config::ObjectStoreProvider;

    struct TestHarness {
        runtime: tokio::runtime::Runtime,
        metadata: Arc<MetadataStore>,
        fs: OsageFs,
        config: Config,
    }

    impl TestHarness {
        fn new(root: &Path, state_name: &str, pending_bytes: u64) -> Self {
            let config = test_config(root, state_name, pending_bytes);
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let metadata = Arc::new(
                runtime
                    .block_on(MetadataStore::open(&config.store_path))
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
            let fs = OsageFs::new(
                config.clone(),
                metadata.clone(),
                superblock,
                segments,
                runtime.handle().clone(),
                client_state,
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
                let (_, snapshot) = runtime
                    .block_on(superblock.mutate(|sb| {
                        sb.state = FilesystemState::Dirty;
                    }))
                    .unwrap();
                runtime
                    .block_on(metadata.persist_inode(&root, snapshot.generation, config.shard_size))
                    .unwrap();
                runtime.block_on(superblock.mark_clean()).unwrap();
            }
            return;
        }
        let (_, snapshot) = runtime
            .block_on(superblock.mutate(|sb| {
                sb.state = FilesystemState::Dirty;
            }))
            .unwrap();
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
            .block_on(metadata.persist_inode(&root, snapshot.generation, config.shard_size))
            .unwrap();
        runtime.block_on(superblock.mark_clean()).unwrap();
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

    #[test]
    fn single_client_inline_and_segment_flush() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "client_a.json", 8 * 1024 * 1024);

        let inode_inline = harness.fs.allocate_inode_id().unwrap();
        let record_inline = make_file(inode_inline, "foo.txt");
        harness
            .fs
            .stage_file(record_inline, b"hello".to_vec())
            .unwrap();

        let inode_seg = harness.fs.allocate_inode_id().unwrap();
        let record_seg = make_file(inode_seg, "bar.bin");
        let data = vec![7u8; harness.config.inline_threshold + 128];
        harness.fs.stage_file(record_seg, data.clone()).unwrap();
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
            FileStorage::Segment(_) => {
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
            let harness = TestHarness::new(dir.path(), "client_one.json", 8 * 1024 * 1024);
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, "client1.txt");
            harness.fs.stage_file(record, b"alpha".to_vec()).unwrap();
            harness.fs.flush_pending().unwrap();
            inode
        };

        let inode_b = {
            let harness = TestHarness::new(dir.path(), "client_two.json", 8 * 1024 * 1024);
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, "client2.txt");
            harness.fs.stage_file(record, b"beta".to_vec()).unwrap();
            harness.fs.flush_pending().unwrap();
            inode
        };

        let harness = TestHarness::new(dir.path(), "client_reader.json", 8 * 1024 * 1024);
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

        let state_a = std::fs::read_to_string(dir.path().join("client_one.json")).unwrap();
        let state_b = std::fs::read_to_string(dir.path().join("client_two.json")).unwrap();
        assert_ne!(state_a, state_b);
    }

    #[test]
    fn stress_flush_respects_pending_threshold() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "stress.json", 1024);
        let mut inodes = Vec::new();
        for i in 0..10 {
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, &format!("stress{i}"));
            harness.fs.stage_file(record, vec![i as u8; 600]).unwrap();
            inodes.push(inode);
        }
        harness.fs.flush_pending().unwrap();
        assert!(harness.fs.pending_files.lock().is_empty());
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
}
