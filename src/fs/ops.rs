use super::*;

impl OsageFs {
    pub(crate) fn op_lookup(
        &self,
        parent: u64,
        name: &str,
    ) -> std::result::Result<InodeRecord, i32> {
        let parent_inode = self.load_inode(parent)?;
        let child = parent_inode
            .children()
            .and_then(|children| children.get(name).copied())
            .ok_or(ENOENT)?;
        self.load_inode(child)
    }

    pub(crate) fn op_getattr(&self, ino: u64) -> std::result::Result<InodeRecord, i32> {
        self.load_inode(ino)
    }

    pub(crate) fn op_readdir_nfs(&self, ino: u64) -> std::result::Result<Vec<(u64, String)>, i32> {
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

    pub(crate) fn op_readdir_fuse(
        &self,
        ino: u64,
    ) -> std::result::Result<Vec<(u64, FileType, String)>, i32> {
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
    }

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
        let mut data = self.read_file_bytes(&record).map_err(|_| EIO)?;
        if let Some(target_size) = size {
            if record.is_dir() {
                return Err(EISDIR);
            }
            Self::resize_file_data_for_setattr(&mut data, target_size)?;
            record.size = data.len() as u64;
        }
        record.ctime = OffsetDateTime::now_utc();
        if let Some(ts) = atime {
            record.atime = ts;
        }
        if let Some(ts) = mtime {
            record.mtime = ts;
        } else if size.is_some() {
            record.mtime = record.ctime;
        }
        self.stage_file(record.clone(), data, None)?;
        Ok(record)
    }

    pub(crate) fn op_fuse_setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
    ) -> std::result::Result<FileAttr, i32> {
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
            Self::resize_file_data_for_setattr(&mut data, target_size)?;
            record.size = data.len() as u64;
            if mtime.is_none() {
                record.mtime = OffsetDateTime::now_utc();
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
        let attr = Self::record_attr(&record);
        self.stage_file(record, data, None)?;
        Ok(attr)
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
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, &name);
        let mut file = InodeRecord::new_file(inode_id, parent, name.clone(), path, uid, gid);
        file.update_times();
        self.stage_inode(file.clone())?;
        self.update_parent_move(parent_inode, name, inode_id)?;
        Ok(file)
    }

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
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, name);
        let mut file = InodeRecord::new_file(inode_id, parent, name.to_string(), path, uid, gid);
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
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, &name);
        let mut dir = InodeRecord::new_directory(inode_id, parent, name.clone(), path, uid, gid);
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
        let mut dir = self.op_mkdir(parent, name, uid, gid)?;
        dir.mode = Self::apply_umask(S_IFDIR | (mode & 0o7777), umask);
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
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, name);
        let mut node = InodeRecord::new_file(inode_id, parent, name.to_string(), path, uid, gid);
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
        let inode_id = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(&parent_inode, &name);
        let record =
            InodeRecord::new_symlink(inode_id, parent, name.clone(), path, uid, gid, target);
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
        self.read_file_range(&record, offset, size).map_err(|_| EIO)
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

    pub(crate) fn op_open(&self, ino: u64, flags: i32) -> std::result::Result<(), i32> {
        let mut record = self.load_inode(ino)?;
        if flags & O_TRUNC != 0 && !record.is_dir() && record.size > 0 {
            record.update_times();
            self.stage_file(record, Vec::new(), None)?;
        }
        Ok(())
    }

    pub(crate) fn op_remove_file(&self, parent: u64, name: &str) -> std::result::Result<(), i32> {
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

    pub(crate) fn op_remove_dir(&self, parent: u64, name: &str) -> std::result::Result<(), i32> {
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
    ) -> std::result::Result<(), i32> {
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
