use super::*;
use std::sync::atomic::Ordering;

impl OsageFs {
    // FUSE create semantics differ from nfs_create: O_CREAT without O_EXCL should open
    // an existing file instead of returning EEXIST when a parallel creator won the race.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn fuse_create_file(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> std::result::Result<(InodeRecord, bool), i32> {
        match self.op_create_fuse(parent, name, uid, gid, mode, umask, flags) {
            Ok(result) => Ok(result),
            Err(code) => {
                self.log_fuse_error(
                    "fuse_create_file",
                    &format!("parent={} name={} flags={}", parent, name, flags),
                    code,
                );
                Err(code)
            }
        }
    }
}

impl Filesystem for OsageFs {
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
        // Allow deeper in-flight queues under fsync-heavy small-file workloads.
        let _ = config.set_max_background(1024);
        let _ = config.set_congestion_threshold(768);
        let _ = config.add_capabilities(fuser::consts::FUSE_AUTO_INVAL_DATA);
        if self.config.writeback_cache {
            let _ = config.add_capabilities(fuser::consts::FUSE_WRITEBACK_CACHE);
        }
        if !self.mount_ready_emitted.swap(true, Ordering::AcqRel)
            && let Some(telemetry) = &self.telemetry
        {
            telemetry.emit(
                "command.mount_ready",
                self.telemetry_session_id.as_deref(),
                json!({ "mode": "fuse" }),
            );
        }
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
        req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let replay = self.replay_start();
        let res = self.op_fuse_setattr(
            ino,
            req.uid(),
            req.gid(),
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
        );
        match res {
            Ok(attr) => {
                self.log_replay(
                    "fuse",
                    "setattr",
                    replay,
                    None,
                    json!({ "ino": ino, "size": size, "mode": mode, "uid": uid, "gid": gid }),
                );
                reply.attr(&self.fuse_attr_ttl_for_attr(&attr), &attr)
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "setattr",
                    replay,
                    Some(code),
                    json!({ "ino": ino, "size": size, "mode": mode, "uid": uid, "gid": gid }),
                );
                reply.error(code)
            }
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let replay = self.replay_start();
        let name_owned = match Self::validate_os_name(name) {
            Ok(n) => n,
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "lookup",
                    replay,
                    Some(code),
                    json!({ "parent": parent, "name": name.to_string_lossy() }),
                );
                reply.error(code);
                return;
            }
        };
        let name_str = name_owned.as_str();
        let response = self.op_lookup(parent, name_str);
        match response {
            Ok(inode) => {
                self.log_replay(
                    "fuse",
                    "lookup",
                    replay,
                    None,
                    json!({ "parent": parent, "name": name_str, "ino": inode.inode }),
                );
                let attr = Self::record_attr(&inode);
                reply.entry(&self.fuse_attr_ttl(&inode), &attr, FUSE_NODE_GENERATION);
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "lookup",
                    replay,
                    Some(code),
                    json!({ "parent": parent, "name": name_str }),
                );
                if code != ENOENT {
                    let detail = format!("parent={} name={}", parent, name_str);
                    self.log_fuse_error("lookup", &detail, code);
                }
                reply.error(code)
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let replay = self.replay_start();
        match self.op_getattr(ino) {
            Ok(record) => {
                self.log_replay("fuse", "getattr", replay, None, json!({ "ino": ino }));
                let attr = Self::record_attr(&record);
                reply.attr(&self.fuse_attr_ttl(&record), &attr);
            }
            Err(code) => {
                self.log_replay("fuse", "getattr", replay, Some(code), json!({ "ino": ino }));
                reply.error(code)
            }
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
        let replay = self.replay_start();
        let response = self.op_readdir_fuse(ino);
        match response {
            Ok(entries) => {
                let entry_count = entries.len();
                for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(ino, (i + 1) as i64, kind, name) {
                        break;
                    }
                }
                self.log_replay(
                    "fuse",
                    "readdir",
                    replay,
                    None,
                    json!({ "ino": ino, "offset": offset, "entries": entry_count }),
                );
                reply.ok();
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "readdir",
                    replay,
                    Some(code),
                    json!({ "ino": ino, "offset": offset }),
                );
                reply.error(code)
            }
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(parent);
        let uid = req.uid();
        let gid = req.gid();
        let res = (|| {
            let name = Self::validate_os_name(name)?;
            self.op_mkdir_fuse(parent, &name, uid, gid, mode, umask)
        })();
        match res {
            Ok(dir) => {
                self.log_replay(
                    "fuse",
                    "mkdir",
                    replay,
                    None,
                    json!({ "parent": parent, "name": name.to_string_lossy(), "uid": uid, "gid": gid, "ino": dir.inode }),
                );
                let attr = Self::record_attr(&dir);
                reply.entry(&self.fuse_attr_ttl(&dir), &attr, FUSE_NODE_GENERATION);
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "mkdir",
                    replay,
                    Some(code),
                    json!({ "parent": parent, "name": name.to_string_lossy(), "uid": uid, "gid": gid }),
                );
                reply.error(code)
            }
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(parent);
        let uid = req.uid();
        let gid = req.gid();
        let res = (|| {
            let name = Self::validate_os_name(name)?;
            self.fuse_create_file(parent, &name, uid, gid, mode, umask, flags)
        })();
        match res {
            Ok((file, created)) => {
                self.log_replay(
                    "fuse",
                    "create",
                    replay,
                    None,
                    json!({
                        "parent": parent,
                        "name": name.to_string_lossy(),
                        "uid": uid,
                        "gid": gid,
                        "ino": file.inode,
                        "created": created,
                        "mode": mode,
                        "umask": umask,
                        "flags": flags,
                        "stored_mode": file.mode,
                    }),
                );
                let attr = Self::record_attr(&file);
                reply.created(
                    &self.fuse_attr_ttl(&file),
                    &attr,
                    FUSE_NODE_GENERATION,
                    0,
                    0,
                );
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "create",
                    replay,
                    Some(code),
                    json!({
                        "parent": parent,
                        "name": name.to_string_lossy(),
                        "uid": uid,
                        "gid": gid,
                        "mode": mode,
                        "umask": umask,
                        "flags": flags,
                    }),
                );
                let detail = format!("parent={} name={}", parent, name.to_string_lossy());
                self.log_fuse_error("create", &detail, code);
                reply.error(code);
            }
        }
    }

    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(parent);
        let uid = req.uid();
        let gid = req.gid();
        let res = (|| {
            let name = Self::validate_os_name(name)?;
            self.op_mknod(parent, &name, uid, gid, mode, rdev)
        })();

        match res {
            Ok(node) => {
                self.log_replay(
                    "fuse",
                    "mknod",
                    replay,
                    None,
                    json!({ "parent": parent, "name": name.to_string_lossy(), "uid": uid, "gid": gid, "mode": mode, "rdev": rdev, "ino": node.inode }),
                );
                let attr = Self::record_attr(&node);
                reply.entry(&self.fuse_attr_ttl(&node), &attr, FUSE_NODE_GENERATION);
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "mknod",
                    replay,
                    Some(code),
                    json!({ "parent": parent, "name": name.to_string_lossy(), "uid": uid, "gid": gid, "mode": mode, "rdev": rdev }),
                );
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
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(parent);
        let uid = req.uid();
        let gid = req.gid();
        let target_len = path_to_bytes(link).len();
        let res = (|| {
            let name = Self::validate_os_name(name)?;
            self.op_symlink(parent, &name, path_to_bytes(link), uid, gid)
        })();
        match res {
            Ok(record) => {
                self.log_replay(
                    "fuse",
                    "symlink",
                    replay,
                    None,
                    json!({ "parent": parent, "name": name.to_string_lossy(), "uid": uid, "gid": gid, "target_len": target_len, "ino": record.inode }),
                );
                let attr = Self::record_attr(&record);
                reply.entry(&self.fuse_attr_ttl(&record), &attr, FUSE_NODE_GENERATION);
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "symlink",
                    replay,
                    Some(code),
                    json!({ "parent": parent, "name": name.to_string_lossy(), "uid": uid, "gid": gid, "target_len": target_len }),
                );
                reply.error(code)
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let replay = self.replay_start();
        let res = self.op_open(ino, flags);
        match res {
            Ok(_) => {
                self.log_replay(
                    "fuse",
                    "open",
                    replay,
                    None,
                    json!({ "ino": ino, "flags": flags }),
                );
                reply.opened(0, 0)
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "open",
                    replay,
                    Some(code),
                    json!({ "ino": ino, "flags": flags }),
                );
                let detail = format!("ino={}", ino);
                self.log_fuse_error("open", &detail, code);
                reply.error(code);
            }
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
        let replay = self.replay_start();
        let res = self.op_read(ino, offset as u64, size);
        match res {
            Ok(bytes) => {
                self.log_replay(
                    "fuse",
                    "read",
                    replay,
                    None,
                    json!({ "ino": ino, "offset": offset, "requested": size, "returned": bytes.len() }),
                );
                reply.data(&bytes)
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "read",
                    replay,
                    Some(code),
                    json!({ "ino": ino, "offset": offset, "requested": size }),
                );
                reply.error(code)
            }
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let replay = self.replay_start();
        match self.op_readlink(ino) {
            Ok(target) => {
                self.log_replay(
                    "fuse",
                    "readlink",
                    replay,
                    None,
                    json!({ "ino": ino, "returned": target.len() }),
                );
                reply.data(&target)
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "readlink",
                    replay,
                    Some(code),
                    json!({ "ino": ino }),
                );
                reply.error(code)
            }
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
        let replay = self.replay_start();
        let res = self.op_write(ino, offset as u64, data);
        match res {
            Ok(size) => {
                self.log_replay(
                    "fuse",
                    "write",
                    replay,
                    None,
                    json!({ "ino": ino, "offset": offset, "len": data.len(), "written": size }),
                );
                reply.written(size)
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "write",
                    replay,
                    Some(code),
                    json!({ "ino": ino, "offset": offset, "len": data.len() }),
                );
                let detail = format!("ino={} offset={} len={}", ino, offset, data.len());
                self.log_fuse_error("write", &detail, code);
                reply.error(code);
            }
        }
    }

    fn unlink(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(parent);
        let caller_uid = req.uid();
        let res = (|| {
            let name = Self::validate_os_name(name)?;
            self.op_remove_file(parent, &name, caller_uid)
        })();
        match res {
            Ok(()) => {
                self.log_replay(
                    "fuse",
                    "unlink",
                    replay,
                    None,
                    json!({ "parent": parent, "name": name.to_string_lossy() }),
                );
                reply.ok()
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "unlink",
                    replay,
                    Some(code),
                    json!({ "parent": parent, "name": name.to_string_lossy() }),
                );
                let detail = format!("parent={} name={}", parent, name.to_string_lossy());
                self.log_fuse_error("unlink", &detail, code);
                reply.error(code);
            }
        }
    }

    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(parent);
        let caller_uid = req.uid();
        let res = (|| {
            let name = Self::validate_os_name(name)?;
            self.op_remove_dir(parent, &name, caller_uid)
        })();
        match res {
            Ok(()) => {
                self.log_replay(
                    "fuse",
                    "rmdir",
                    replay,
                    None,
                    json!({ "parent": parent, "name": name.to_string_lossy() }),
                );
                reply.ok()
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "rmdir",
                    replay,
                    Some(code),
                    json!({ "parent": parent, "name": name.to_string_lossy() }),
                );
                reply.error(code)
            }
        }
    }

    fn rename(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        let replay = self.replay_start();
        let (_dir_guard_a, _dir_guard_b) = self.lock_dir_pair(parent, newparent);
        let caller_uid = req.uid();
        let res = (|| {
            let old_name = Self::validate_os_name(name)?;
            let new_name = Self::validate_os_name(newname)?;
            self.op_rename(parent, &old_name, newparent, &new_name, flags, caller_uid)
        })();
        match res {
            Ok(()) => {
                self.log_replay(
                    "fuse",
                    "rename",
                    replay,
                    None,
                    json!({
                        "parent": parent,
                        "name": name.to_string_lossy(),
                        "newparent": newparent,
                        "newname": newname.to_string_lossy(),
                        "flags": flags,
                    }),
                );
                reply.ok()
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "rename",
                    replay,
                    Some(code),
                    json!({
                        "parent": parent,
                        "name": name.to_string_lossy(),
                        "newparent": newparent,
                        "newname": newname.to_string_lossy(),
                        "flags": flags,
                    }),
                );
                reply.error(code)
            }
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
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(newparent);
        let res = (|| {
            let name = Self::validate_os_name(newname)?;
            self.op_link(ino, newparent, &name)
        })();
        match res {
            Ok(record) => {
                self.log_replay(
                    "fuse",
                    "link",
                    replay,
                    None,
                    json!({ "ino": ino, "newparent": newparent, "newname": newname.to_string_lossy() }),
                );
                let attr = Self::record_attr(&record);
                reply.entry(&self.fuse_attr_ttl(&record), &attr, FUSE_NODE_GENERATION);
            }
            Err(code) => {
                self.log_replay(
                    "fuse",
                    "link",
                    replay,
                    Some(code),
                    json!({ "ino": ino, "newparent": newparent, "newname": newname.to_string_lossy() }),
                );
                reply.error(code)
            }
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
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        let replay = self.replay_start();
        if self.fsync_on_close {
            match self.op_flush_inode(ino) {
                Ok(()) => {
                    self.log_replay("fuse", "flush", replay, None, json!({ "ino": ino }));
                    reply.ok()
                }
                Err(code) => {
                    self.log_replay("fuse", "flush", replay, Some(code), json!({ "ino": ino }));
                    self.log_fuse_error("flush", &format!("ino={}", ino), code);
                    reply.error(code)
                }
            }
        } else {
            self.log_replay(
                "fuse",
                "flush",
                replay,
                None,
                json!({ "ino": ino, "skipped": true }),
            );
            reply.ok();
        }
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let replay = self.replay_start();
        match self.op_flush_inode(ino) {
            Ok(()) => {
                self.log_replay("fuse", "fsync", replay, None, json!({ "ino": ino }));
                reply.ok()
            }
            Err(code) => {
                self.log_replay("fuse", "fsync", replay, Some(code), json!({ "ino": ino }));
                self.log_fuse_error("fsync", &format!("ino={}", ino), code);
                reply.error(code)
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let replay = self.replay_start();
        if self.fsync_on_close {
            match self.op_flush_inode(ino) {
                Ok(()) => {
                    self.log_replay("fuse", "release", replay, None, json!({ "ino": ino }));
                    reply.ok()
                }
                Err(code) => {
                    self.log_replay("fuse", "release", replay, Some(code), json!({ "ino": ino }));
                    self.log_fuse_error("release", &format!("ino={}", ino), code);
                    reply.error(code)
                }
            }
        } else {
            self.log_replay(
                "fuse",
                "release",
                replay,
                None,
                json!({ "ino": ino, "skipped": true }),
            );
            reply.ok();
        }
    }
}
