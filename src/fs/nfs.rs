use super::*;

impl OsageFs {
    /// Emit telemetry for an error without constructing replay JSON.
    #[inline]
    fn emit_errno_only(&self, layer: &str, op: &str, errno: Option<i32>) {
        if let (Some(errno), Some(telemetry)) = (errno, &self.telemetry) {
            telemetry.emit_errno_sampled(layer, op, errno);
        }
    }

    pub fn nfs_lookup(&self, parent: u64, name: &str) -> std::result::Result<InodeRecord, i32> {
        let replay = self.replay_start();
        let result = self.op_lookup(parent, name);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "lookup",
                replay,
                errno,
                json!({ "parent": parent, "name": name, "ino": result.as_ref().ok().map(|record| record.inode) }),
            );
        } else {
            self.emit_errno_only("nfs", "lookup", errno);
        }
        result
    }

    pub fn nfs_getattr(&self, ino: u64) -> std::result::Result<InodeRecord, i32> {
        let replay = self.replay_start();
        let result = self.op_getattr(ino);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay("nfs", "getattr", replay, errno, json!({ "ino": ino }));
        } else {
            self.emit_errno_only("nfs", "getattr", errno);
        }
        result
    }

    pub fn nfs_readdir(&self, ino: u64) -> std::result::Result<Vec<(u64, String)>, i32> {
        let replay = self.replay_start();
        let result = self.op_readdir_nfs(ino);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "readdir",
                replay,
                errno,
                json!({ "ino": ino, "entries": result.as_ref().ok().map_or(0usize, |e| e.len()) }),
            );
        } else {
            self.emit_errno_only("nfs", "readdir", errno);
        }
        result
    }

    /// Like `nfs_readdir` but includes `d_type` (DT_* constant) for each entry.
    /// Uses the FUSE readdir path which batch-loads inodes to determine file types.
    #[cfg(feature = "fuse")]
    pub fn nfs_readdir_plus(&self, ino: u64) -> std::result::Result<Vec<(u64, u8, String)>, i32> {
        use crate::compat::{DT_BLK, DT_CHR, DT_DIR, DT_FIFO, DT_LNK, DT_REG, DT_SOCK};
        use fuser::FileType;

        let replay = self.replay_start();
        let result = self.op_readdir_fuse(ino).map(|entries| {
            entries
                .into_iter()
                // Filter out "." and ".." — the preload layer cannot resolve ".."
                // above the volume root, and the old nfs_readdir never included them.
                .filter(|(_, _, name)| name != "." && name != "..")
                .map(|(ino, ft, name)| {
                    let dt = match ft {
                        FileType::RegularFile => DT_REG,
                        FileType::Directory => DT_DIR,
                        FileType::Symlink => DT_LNK,
                        FileType::BlockDevice => DT_BLK,
                        FileType::CharDevice => DT_CHR,
                        FileType::NamedPipe => DT_FIFO,
                        FileType::Socket => DT_SOCK,
                    };
                    (ino, dt, name)
                })
                .collect()
        });
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "readdir_plus",
                replay,
                errno,
                json!({ "ino": ino, "entries": result.as_ref().ok().map_or(0usize, |e: &Vec<(u64, u8, String)>| e.len()) }),
            );
        } else {
            self.emit_errno_only("nfs", "readdir_plus", errno);
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub fn nfs_setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<OffsetDateTime>,
        mtime: Option<OffsetDateTime>,
    ) -> std::result::Result<InodeRecord, i32> {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(ino);
        let result = self.op_nfs_setattr(ino, mode, uid, gid, size, atime, mtime);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "setattr",
                replay,
                errno,
                json!({ "ino": ino, "mode": mode, "uid": uid, "gid": gid, "size": size }),
            );
        } else {
            self.emit_errno_only("nfs", "setattr", errno);
        }
        result
    }

    pub fn nfs_create(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(parent);
        let result = self.op_create(parent, name, uid, gid);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "create",
                replay,
                errno,
                json!({ "parent": parent, "name": name, "uid": uid, "gid": gid, "ino": result.as_ref().ok().map(|record| record.inode) }),
            );
        } else {
            self.emit_errno_only("nfs", "create", errno);
        }
        result
    }

    pub fn nfs_mkdir(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(parent);
        let result = self.op_mkdir(parent, name, uid, gid);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "mkdir",
                replay,
                errno,
                json!({ "parent": parent, "name": name, "uid": uid, "gid": gid, "ino": result.as_ref().ok().map(|record| record.inode) }),
            );
        } else {
            self.emit_errno_only("nfs", "mkdir", errno);
        }
        result
    }

    pub fn nfs_symlink(
        &self,
        parent: u64,
        name: &str,
        target: Vec<u8>,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<InodeRecord, i32> {
        let replay = self.replay_start();
        let target_len = target.len();
        let _dir_guard = self.lock_dir(parent);
        let result = self.op_symlink(parent, name, target, uid, gid);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "symlink",
                replay,
                errno,
                json!({ "parent": parent, "name": name, "uid": uid, "gid": gid, "target_len": target_len, "ino": result.as_ref().ok().map(|record| record.inode) }),
            );
        } else {
            self.emit_errno_only("nfs", "symlink", errno);
        }
        result
    }

    pub fn nfs_read(&self, ino: u64, offset: u64, size: u32) -> std::result::Result<Vec<u8>, i32> {
        let replay = self.replay_start();
        let result = self.op_read(ino, offset, size);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "read",
                replay,
                errno,
                json!({
                    "ino": ino,
                    "offset": offset,
                    "requested": size,
                    "returned": result.as_ref().ok().map_or(0, |bytes| bytes.len()),
                }),
            );
        } else {
            self.emit_errno_only("nfs", "read", errno);
        }
        result
    }

    pub fn nfs_readlink(&self, ino: u64) -> std::result::Result<Vec<u8>, i32> {
        let replay = self.replay_start();
        let result = self.op_readlink(ino);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "readlink",
                replay,
                errno,
                json!({ "ino": ino, "returned": result.as_ref().ok().map_or(0, |bytes| bytes.len()) }),
            );
        } else {
            self.emit_errno_only("nfs", "readlink", errno);
        }
        result
    }

    pub fn nfs_write(&self, ino: u64, offset: u64, data: &[u8]) -> std::result::Result<u32, i32> {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(ino);
        let result = self.op_write(ino, offset, data);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "write",
                replay,
                errno,
                json!({
                    "ino": ino,
                    "offset": offset,
                    "len": data.len(),
                    "written": result.as_ref().ok().copied().unwrap_or(0),
                }),
            );
        } else {
            self.emit_errno_only("nfs", "write", errno);
        }
        result
    }

    pub fn nfs_remove_file(
        &self,
        parent: u64,
        name: &str,
        caller_uid: u32,
    ) -> std::result::Result<(), i32> {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(parent);
        let result = self.op_remove_file(parent, name, caller_uid);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "unlink",
                replay,
                errno,
                json!({ "parent": parent, "name": name }),
            );
        } else {
            self.emit_errno_only("nfs", "unlink", errno);
        }
        result
    }

    pub fn nfs_remove_dir(
        &self,
        parent: u64,
        name: &str,
        caller_uid: u32,
    ) -> std::result::Result<(), i32> {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(parent);
        let result = self.op_remove_dir(parent, name, caller_uid);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "rmdir",
                replay,
                errno,
                json!({ "parent": parent, "name": name }),
            );
        } else {
            self.emit_errno_only("nfs", "rmdir", errno);
        }
        result
    }

    pub fn nfs_rename(
        &self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
        flags: u32,
        caller_uid: u32,
    ) -> std::result::Result<(), i32> {
        let replay = self.replay_start();
        let (_dir_guard_a, _dir_guard_b) = self.lock_dir_pair(parent, newparent);
        let result = self.op_rename(parent, name, newparent, newname, flags, caller_uid);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay(
                "nfs",
                "rename",
                replay,
                errno,
                json!({ "parent": parent, "name": name, "newparent": newparent, "newname": newname, "flags": flags }),
            );
        } else {
            self.emit_errno_only("nfs", "rename", errno);
        }
        result
    }

    pub fn nfs_flush(&self) -> std::result::Result<(), i32> {
        let replay = self.replay_start();
        let result = self.op_flush_all();
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay("nfs", "flush", replay, errno, json!({}));
        } else {
            self.emit_errno_only("nfs", "flush", errno);
        }
        result
    }

    pub fn nfs_flush_inode(&self, ino: u64) -> std::result::Result<(), i32> {
        let replay = self.replay_start();
        let result = self.op_flush_inode(ino);
        let errno = result.as_ref().err().copied();
        if replay.is_some() {
            self.log_replay("nfs", "flush_inode", replay, errno, json!({ "ino": ino }));
        } else {
            self.emit_errno_only("nfs", "flush_inode", errno);
        }
        result
    }
}
