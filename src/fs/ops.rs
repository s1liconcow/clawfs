use super::*;

const S_ISVTX: u32 = 0o1000;
const S_ISGID: u32 = 0o2000;
const S_ISUID: u32 = 0o4000;

impl OsageFs {
    fn lookup_child_fast_path(
        &self,
        parent: u64,
        name: &str,
    ) -> Option<std::result::Result<u64, i32>> {
        if let Some(active_arc) = self.active_inodes.get(&parent) {
            let state = active_arc.lock();
            let record = state
                .pending
                .as_ref()
                .map(|entry| &entry.record)
                .or_else(|| state.flushing.as_ref().map(|entry| &entry.record));
            if let Some(record) = record {
                if matches!(record.kind, InodeKind::Tombstone) {
                    return Some(Err(ENOENT));
                }
                return match &record.kind {
                    InodeKind::Directory { children } => {
                        Some(children.get(name).copied().ok_or(ENOENT))
                    }
                    _ => Some(Err(ENOTDIR)),
                };
            }
        }
        self.metadata
            .lookup_cached_child(parent, name, self.dir_cache_ttl)
    }

    pub(crate) fn op_lookup(
        &self,
        parent: u64,
        name: &str,
    ) -> std::result::Result<InodeRecord, i32> {
        if let Some(child) = self.lookup_child_fast_path(parent, name) {
            match child {
                Ok(ino) => return self.load_inode(ino),
                Err(code) if code != ENOENT || self.source.is_none() => return Err(code),
                Err(_) => {}
            }
        }
        let parent_inode = self.load_inode(parent)?;
        if let Some(child) = parent_inode
            .children()
            .and_then(|children| children.get(name).copied())
        {
            return self.load_inode(child);
        }
        if let Some(imported) = self.maybe_import_source_child(&parent_inode, name)? {
            return Ok(imported);
        }
        Err(ENOENT)
    }

    pub(crate) fn op_getattr(&self, ino: u64) -> std::result::Result<InodeRecord, i32> {
        self.load_inode(ino)
    }

    pub(crate) fn op_readdir_nfs(&self, ino: u64) -> std::result::Result<Vec<(u64, String)>, i32> {
        let mut inode = self.load_inode(ino)?;
        if !inode.is_dir() {
            return Err(ENOTDIR);
        }
        self.import_source_children_for_dir(&inode)?;
        inode = self.load_inode(ino)?;
        let mut entries = Vec::new();
        if let Some(children) = inode.children() {
            entries.reserve(children.len());
            for (name, child) in children {
                entries.push((*child, name.clone()));
            }
        }
        Ok(entries)
    }

    #[cfg(feature = "fuse")]
    pub(crate) fn op_readdir_fuse(
        &self,
        ino: u64,
    ) -> std::result::Result<Vec<(u64, FileType, String)>, i32> {
        let mut inode = self.load_inode(ino)?;
        if !inode.is_dir() {
            return Err(ENOTDIR);
        }
        self.import_source_children_for_dir(&inode)?;
        inode = self.load_inode(ino)?;
        let mut entries = Vec::new();
        entries.push((ino, FileType::Directory, String::from(".")));
        let parent = if ino == ROOT_INODE { ino } else { inode.parent };
        entries.push((parent, FileType::Directory, String::from("..")));
        if let Some(children) = inode.children() {
            let child_inos: Vec<u64> = children.values().copied().collect();
            let loaded = self.load_inodes_batch(&child_inos)?;
            for (name, child) in children {
                if let Some(child_inode) = loaded.get(child) {
                    entries.push((child_inode.inode, file_type(child_inode), name.clone()));
                }
            }
        }
        Ok(entries)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn op_nfs_setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<OffsetDateTime>,
        mtime: Option<OffsetDateTime>,
    ) -> std::result::Result<InodeRecord, i32> {
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
        record.ctime = OffsetDateTime::now_utc();
        if let Some(ts) = atime {
            record.atime = ts;
        }
        if let Some(ts) = mtime {
            record.mtime = ts;
        }

        if let Some(target_size) = size {
            // Size change: must read and restage file data.
            if record.is_dir() {
                return Err(EISDIR);
            }
            if target_size > record.size && record.size == 0 {
                // Sparse growth on an empty file: represent the hole via size +
                // empty segment extents instead of allocating target_size zeros.
                record.size = target_size;
                record.storage = FileStorage::Segments(Vec::new());
                self.stage_inode(record.clone())?;
                return Ok(record);
            }
            if mtime.is_none() {
                record.mtime = record.ctime;
            }
            let mut data = self.read_file_bytes(&record).map_err(|_| EIO)?;
            Self::resize_file_data_for_setattr(&mut data, target_size)?;
            record.size = data.len() as u64;
            self.stage_file(record.clone(), data, None)?;
            Ok(record)
        } else {
            // Metadata-only: preserve file data via stage_inode (avoids the
            // read_file_bytes race with concurrent flush for large files).
            self.stage_inode(record.clone())?;
            Ok(record)
        }
    }

    /// Enforce sticky-bit restriction: if parent dir has the sticky bit set,
    /// only root, the directory owner, or the file owner may remove/rename the child.
    fn check_sticky(
        caller_uid: u32,
        parent: &InodeRecord,
        child: &InodeRecord,
    ) -> std::result::Result<(), i32> {
        if parent.mode & S_ISVTX != 0
            && caller_uid != 0
            && caller_uid != parent.uid
            && caller_uid != child.uid
        {
            return Err(EPERM);
        }
        Ok(())
    }

    /// Resolve effective GID for a new entry: inherit parent GID when parent has SGID.
    fn effective_gid(parent: &InodeRecord, caller_gid: u32) -> u32 {
        if parent.mode & S_ISGID != 0 {
            parent.gid
        } else {
            caller_gid
        }
    }

    #[cfg(feature = "fuse")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn op_fuse_setattr(
        &self,
        ino: u64,
        caller_uid: u32,
        _caller_gid: u32,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
    ) -> std::result::Result<FileAttr, i32> {
        let mut record = self.load_inode(ino)?;

        // chmod permission: only owner or root may change mode bits.
        //
        // Exception: allow a mode change that *only* clears SUID/SGID (rwx bits
        // unchanged) from any caller.  When a non-privileged user writes to a
        // setuid/setgid file, the kernel calls file_remove_privs() which sends a
        // FUSE setattr with req.uid() = writer's uid and the new mode = old mode
        // with SUID/SGID stripped.  We must honour this or the write() syscall
        // itself fails with EPERM (file_remove_privs returns the setattr error).
        let is_priv_strip_only = mode.is_some_and(|m| {
            let old = record.mode & 0o7777;
            let new = m & 0o7777;
            (old & 0o0777) == (new & 0o0777)              // rwx bits unchanged
                && (old & (S_ISUID | S_ISGID)) != 0       // old had SUID/SGID
                && (new & (S_ISUID | S_ISGID)) == 0 // new has neither
        });
        if mode.is_some() && !is_priv_strip_only && caller_uid != 0 && record.uid != caller_uid {
            return Err(EPERM);
        }

        // chown permission checks (POSIX / Linux semantics):
        //   - uid change: root only (non-root cannot reassign uid).
        //   - gid change: root always; owner may change to any group they belong to.
        //     FUSE does not expose supplementary groups, so we allow the owner to
        //     change the gid freely (permissive but necessary given the API limit).
        if let Some(new_uid) = uid
            && caller_uid != 0
            && new_uid != record.uid
        {
            return Err(EPERM);
        }
        if let Some(new_gid) = gid {
            if caller_uid != 0 && record.uid != caller_uid {
                // Non-owner cannot change the gid.
                return Err(EPERM);
            }
            let _ = new_gid; // owner-can-change-gid is allowed (supplementary groups not checkable)
        }

        // Apply mode change.
        if let Some(new_mode) = mode {
            record.mode = (record.mode & !0o7777) | (new_mode & 0o7777);
        }

        // Apply ownership change.
        let ownership_changed = uid.is_some() || gid.is_some();
        if let Some(new_uid) = uid {
            record.uid = new_uid;
        }
        if let Some(new_gid) = gid {
            record.gid = new_gid;
        }

        // Linux: when a non-privileged user changes ownership, strip SUID/SGID bits.
        // (For directories, Linux does not strip SGID; we mirror that behaviour.)
        if ownership_changed && caller_uid != 0 {
            if record.is_dir() {
                record.mode &= !(S_ISUID);
            } else {
                record.mode &= !(S_ISUID | S_ISGID);
            }
        }

        if let Some(next_atime) = atime {
            record.atime = match next_atime {
                TimeOrNow::SpecificTime(ts) => from_system_time(ts),
                TimeOrNow::Now => OffsetDateTime::now_utc(),
            };
        }
        if let Some(next_mtime) = mtime {
            record.mtime = match next_mtime {
                TimeOrNow::SpecificTime(ts) => from_system_time(ts),
                TimeOrNow::Now => OffsetDateTime::now_utc(),
            };
        }
        record.ctime = OffsetDateTime::now_utc();

        if let Some(target_size) = size {
            log::debug!(
                "op_fuse_setattr SIZE ino={} current_size={} target_size={} writeback={}",
                record.inode,
                record.size,
                target_size,
                self.config.writeback_cache
            );
            // Size change: must read and restage file data to truncate/extend.
            if record.is_dir() {
                return Err(EISDIR);
            }
            if target_size > record.size && record.size == 0 && !self.config.writeback_cache {
                // Sparse growth on an empty file: keep content holes implicit.
                // Skip this when writeback cache is enabled: the kernel sends
                // setattr(size=N) before the actual write data arrives.  If we
                // stage a metadata-only entry with Segments([]) here, a timer
                // flush can commit it as a correctly-sized file full of zeros
                // before the write lands.  Falling through to the general path
                // creates a zero-filled pending entry with data: Some(...), so
                // the flush always has content (and the subsequent write
                // overwrites it under the per-inode lock).
                record.size = target_size;
                record.storage = FileStorage::Segments(Vec::new());
                if mtime.is_none() {
                    record.mtime = OffsetDateTime::now_utc();
                }
                let staged = self.stage_inode_visible(record)?;
                return Ok(Self::record_attr(&staged));
            }
            let mut data = self.read_file_bytes(&record).map_err(|_| EIO)?;
            Self::resize_file_data_for_setattr(&mut data, target_size)?;
            record.size = data.len() as u64;
            if mtime.is_none() {
                record.mtime = OffsetDateTime::now_utc();
            }
            let attr = Self::record_attr(&record);
            self.stage_file(record, data, None)?;
            Ok(attr)
        } else {
            // Metadata-only change (mode/uid/gid/timestamps, no size change).
            // Do NOT call read_file_bytes + stage_file here: for large files
            // the record loaded from flushing_inodes carries a stale
            // Inline([]) storage placeholder.  If the concurrent flush
            // completes between load_inode and read_file_bytes, the staged
            // chunks are gone and read_file_bytes falls back to that stale
            // placeholder, returning empty bytes.  stage_file would then
            // persist an empty file, corrupting the content.
            // stage_inode updates the record in-place when the inode is
            // already in pending_inodes (preserving the data field), and
            // creates a metadata-only pending entry otherwise (handled in
            // flush_pending_selected via stale-storage detection).
            let staged = self.stage_inode_visible(record)?;
            Ok(Self::record_attr(&staged))
        }
    }

    pub(crate) fn op_create(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let parent_inode = self.load_inode(parent)?;
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
        let effective_gid = Self::effective_gid(&parent_inode, gid);
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, &name);
        let mut file =
            InodeRecord::new_file(inode_id, parent, name.clone(), path, uid, effective_gid);
        file.update_times();
        self.stage_inode(file.clone())?;
        self.update_parent_move(parent_inode, name, inode_id)?;
        Ok(file)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn op_create_fuse(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> std::result::Result<(InodeRecord, bool), i32> {
        let parent_inode = self.load_inode(parent)?;
        if !parent_inode.is_dir() {
            return Err(ENOTDIR);
        }
        if let Some(existing_ino) = parent_inode
            .children()
            .and_then(|children| children.get(name).copied())
        {
            if flags & O_EXCL != 0 {
                return Err(EEXIST);
            }
            let mut existing = self.load_inode(existing_ino)?;
            if existing.is_dir() {
                return Err(EISDIR);
            }
            if flags & O_TRUNC != 0 && existing.size > 0 {
                existing.update_times();
                self.stage_file(existing.clone(), Vec::new(), None)?;
                existing = self.load_inode(existing_ino)?;
            }
            return Ok((existing, false));
        }
        let effective_gid = Self::effective_gid(&parent_inode, gid);
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, name);
        let mut file =
            InodeRecord::new_file(inode_id, parent, name.to_string(), path, uid, effective_gid);
        file.mode = Self::apply_umask(S_IFREG | (mode & 0o7777), umask);
        file.update_times();
        self.stage_inode(file.clone())?;
        self.update_parent_move(parent_inode, name.to_string(), inode_id)?;
        Ok((file, true))
    }

    pub(crate) fn op_mkdir(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let parent_inode = self.load_inode(parent)?;
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
        let effective_gid = Self::effective_gid(&parent_inode, gid);
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, &name);
        let mut dir =
            InodeRecord::new_directory(inode_id, parent, name.clone(), path, uid, effective_gid);
        dir.update_times();
        self.stage_inode(dir.clone())?;
        self.update_parent_move(parent_inode, name, inode_id)?;
        Ok(dir)
    }

    pub(crate) fn op_mkdir_fuse(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u32,
        umask: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let parent_inode = self.load_inode(parent)?;
        // Determine whether SGID should propagate to the new directory.
        let propagate_sgid = parent_inode.mode & S_ISGID != 0;
        let mut dir = self.op_mkdir(parent, name, uid, gid)?;
        let mut applied_mode = Self::apply_umask(S_IFDIR | (mode & 0o7777), umask);
        // SGID propagates to subdirectories.
        if propagate_sgid {
            applied_mode |= S_ISGID;
        }
        dir.mode = applied_mode;
        self.stage_inode(dir.clone())?;
        Ok(dir)
    }

    pub(crate) fn op_mknod(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u32,
        rdev: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let parent_inode = self.load_inode(parent)?;
        if !parent_inode.is_dir() {
            return Err(ENOTDIR);
        }
        if parent_inode
            .children()
            .map(|children| children.contains_key(name))
            .unwrap_or(false)
        {
            return Err(EEXIST);
        }
        let effective_gid = Self::effective_gid(&parent_inode, gid);
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, name);
        let mut node =
            InodeRecord::new_file(inode_id, parent, name.to_string(), path, uid, effective_gid);
        node.mode = Self::normalize_node_mode(mode);
        node.rdev = rdev;
        node.update_times();
        self.stage_inode(node.clone())?;
        self.update_parent_move(parent_inode, name.to_string(), inode_id)?;
        Ok(node)
    }

    pub(crate) fn op_symlink(
        &self,
        parent: u64,
        name: &str,
        target: Vec<u8>,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let parent_inode = self.load_inode(parent)?;
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
        let effective_gid = Self::effective_gid(&parent_inode, gid);
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, &name);
        let record = InodeRecord::new_symlink(
            inode_id,
            parent,
            name.clone(),
            path,
            uid,
            effective_gid,
            target,
        );
        self.stage_inode(record.clone())?;
        self.update_parent_move(parent_inode, name, inode_id)?;
        Ok(record)
    }

    pub(crate) fn op_read(
        &self,
        ino: u64,
        offset: u64,
        size: u32,
    ) -> std::result::Result<Vec<u8>, i32> {
        let record = self.load_inode(ino)?;
        if record.is_dir() {
            return Err(EISDIR);
        }
        let result = self.read_file_range(&record, offset, size);
        if let Ok(ref bytes) = result {
            let is_zero = bytes.iter().all(|&b| b == 0);
            if record.size > 0 && is_zero && !bytes.is_empty() {
                log::warn!(
                    "op_read ZERO_DATA ino={} offset={} size={} returned={} record_size={} storage={:?}",
                    ino,
                    offset,
                    size,
                    bytes.len(),
                    record.size,
                    record.storage
                );
            }
        }
        result.map_err(|e| {
            let storage_desc = match &record.storage {
                FileStorage::Segments(exts) => {
                    for (i, ext) in exts.iter().enumerate() {
                        log::error!(
                            "  op_read extent[{}] logical_offset={} gen={} seg={} ptr_off={} ptr_len={}",
                            i, ext.logical_offset,
                            ext.pointer.generation, ext.pointer.segment_id,
                            ext.pointer.offset, ext.pointer.length
                        );
                    }
                    format!("segments({})", exts.len())
                }
                _ => "other".to_string(),
            };
            log::error!(
                "op_read EIO ino={} offset={} size={} record_size={} storage={} err={:#}",
                ino, offset, size, record.size, storage_desc, e
            );
            EIO
        })
    }

    pub(crate) fn op_readlink(&self, ino: u64) -> std::result::Result<Vec<u8>, i32> {
        let record = self.load_inode(ino)?;
        if !record.is_symlink() {
            return Err(EINVAL);
        }
        self.read_file_bytes(&record).map_err(|_| EIO)
    }

    pub(crate) fn op_write(
        &self,
        ino: u64,
        offset: u64,
        data: &[u8],
    ) -> std::result::Result<u32, i32> {
        let mut record = self.load_inode(ino)?;
        if record.is_dir() {
            return Err(EISDIR);
        }
        record = self.copy_up_external_inode_if_needed(record)?;
        let write_end = offset.saturating_add(data.len() as u64);
        // For any non-append write on a file that is already larger than the
        // inline threshold, stay on the segment path. Using the inline path
        // here would materialize and restage the full file for tiny writes
        // (e.g. 4KiB overwrite at offset 0 on a multi-GB file).
        if record.size > self.config.inline_threshold as u64
            || write_end > self.config.inline_threshold as u64
        {
            self.write_large_segments(record, offset, data)?;
            return Ok(data.len() as u32);
        }
        self.write_inline_range(record, Some(offset), data)?;
        Ok(data.len() as u32)
    }

    pub(crate) fn op_open(&self, ino: u64, flags: i32) -> std::result::Result<(), i32> {
        let mut record = self.load_inode(ino)?;
        if flags & O_TRUNC != 0 && !record.is_dir() {
            record.update_times();
            self.stage_file(record, Vec::new(), None)?;
        }
        Ok(())
    }

    pub(crate) fn op_remove_file(
        &self,
        parent: u64,
        name: &str,
        caller_uid: u32,
    ) -> std::result::Result<(), i32> {
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
        Self::check_sticky(caller_uid, &parent_inode, &child)?;
        self.unlink_file_entry(&mut parent_inode, name, &mut child)
    }

    pub(crate) fn op_remove_dir(
        &self,
        parent: u64,
        name: &str,
        caller_uid: u32,
    ) -> std::result::Result<(), i32> {
        let parent_inode = self.load_inode(parent)?;
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
        Self::check_sticky(caller_uid, &parent_inode, &child)?;
        let tombstone = InodeRecord::tombstone(child_ino);
        self.stage_inode(tombstone)?;
        self.remove_from_parent_move(parent_inode, name)?;
        Ok(())
    }

    pub(crate) fn op_rename(
        &self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
        flags: u32,
        caller_uid: u32,
    ) -> std::result::Result<(), i32> {
        // Sticky-bit check on source parent: ensure caller may remove `name` from it.
        let parent_inode = self.load_inode(parent)?;
        let child_ino = parent_inode
            .children()
            .and_then(|ch| ch.get(name).copied())
            .ok_or(ENOENT)?;
        let child_inode = self.load_inode(child_ino)?;
        Self::check_sticky(caller_uid, &parent_inode, &child_inode)?;

        // Sticky-bit check on destination parent: ensure caller may overwrite `newname`
        // if it already exists there.
        if newparent != parent || newname != name {
            let newparent_inode = self.load_inode(newparent)?;
            if let Some(dst_ino) = newparent_inode
                .children()
                .and_then(|ch| ch.get(newname).copied())
            {
                let dst_inode = self.load_inode(dst_ino)?;
                Self::check_sticky(caller_uid, &newparent_inode, &dst_inode)?;
            }
        }

        self.rename_entry(parent, name, newparent, newname, flags)
    }

    pub(crate) fn op_link(
        &self,
        ino: u64,
        newparent: u64,
        newname: &str,
    ) -> std::result::Result<InodeRecord, i32> {
        let mut file = self.load_inode(ino)?;
        if file.is_dir() {
            return Err(EPERM);
        }
        let parent_inode = self.load_inode(newparent)?;
        if !parent_inode.is_dir() {
            return Err(ENOTDIR);
        }
        if parent_inode
            .children()
            .map(|children| children.contains_key(newname))
            .unwrap_or(false)
        {
            return Err(EEXIST);
        }
        file.inc_links();
        file.update_times();
        self.stage_inode(file.clone())?;
        self.update_parent_move(parent_inode, newname.to_string(), file.inode)?;
        Ok(file)
    }

    pub(crate) fn op_flush_all(&self) -> std::result::Result<(), i32> {
        self.flush_pending()
    }

    pub(crate) fn op_flush_inode(&self, ino: u64) -> std::result::Result<(), i32> {
        self.sync_local_for_inode(ino)?;
        self.flush_pending_for_inode(ino)
    }
}
