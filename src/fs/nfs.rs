use super::*;

impl OsageFs {
    pub fn nfs_lookup(&self, parent: u64, name: &str) -> std::result::Result<InodeRecord, i32> {
        let replay = self.replay_start();
        let result = self.op_lookup(parent, name);
        self.log_replay(
            "nfs",
            "lookup",
            replay,
            result.as_ref().err().copied(),
            json!({ "parent": parent, "name": name, "ino": result.as_ref().ok().map(|record| record.inode) }),
        );
        result
    }

    pub fn nfs_getattr(&self, ino: u64) -> std::result::Result<InodeRecord, i32> {
        let replay = self.replay_start();
        let result = self.op_getattr(ino);
        self.log_replay(
            "nfs",
            "getattr",
            replay,
            result.as_ref().err().copied(),
            json!({ "ino": ino }),
        );
        result
    }

    pub fn nfs_readdir(&self, ino: u64) -> std::result::Result<Vec<(u64, String)>, i32> {
        let replay = self.replay_start();
        let result = self.op_readdir_nfs(ino);
        self.log_replay(
            "nfs",
            "readdir",
            replay,
            result.as_ref().err().copied(),
            json!({ "ino": ino, "entries": result.as_ref().ok().map_or(0usize, |entries| entries.len()) }),
        );
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
        self.log_replay(
            "nfs",
            "setattr",
            replay,
            result.as_ref().err().copied(),
            json!({ "ino": ino, "mode": mode, "uid": uid, "gid": gid, "size": size }),
        );
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
        self.log_replay(
            "nfs",
            "create",
            replay,
            result.as_ref().err().copied(),
            json!({ "parent": parent, "name": name, "uid": uid, "gid": gid, "ino": result.as_ref().ok().map(|record| record.inode) }),
        );
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
        self.log_replay(
            "nfs",
            "mkdir",
            replay,
            result.as_ref().err().copied(),
            json!({ "parent": parent, "name": name, "uid": uid, "gid": gid, "ino": result.as_ref().ok().map(|record| record.inode) }),
        );
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
        self.log_replay(
            "nfs",
            "symlink",
            replay,
            result.as_ref().err().copied(),
            json!({ "parent": parent, "name": name, "uid": uid, "gid": gid, "target_len": target_len, "ino": result.as_ref().ok().map(|record| record.inode) }),
        );
        result
    }

    pub fn nfs_read(&self, ino: u64, offset: u64, size: u32) -> std::result::Result<Vec<u8>, i32> {
        let replay = self.replay_start();
        let result = self.op_read(ino, offset, size);
        self.log_replay(
            "nfs",
            "read",
            replay,
            result.as_ref().err().copied(),
            json!({
                "ino": ino,
                "offset": offset,
                "requested": size,
                "returned": result.as_ref().ok().map_or(0, |bytes| bytes.len()),
            }),
        );
        result
    }

    pub fn nfs_readlink(&self, ino: u64) -> std::result::Result<Vec<u8>, i32> {
        let replay = self.replay_start();
        let result = self.op_readlink(ino);
        self.log_replay(
            "nfs",
            "readlink",
            replay,
            result.as_ref().err().copied(),
            json!({ "ino": ino, "returned": result.as_ref().ok().map_or(0, |bytes| bytes.len()) }),
        );
        result
    }

    pub fn nfs_write(&self, ino: u64, offset: u64, data: &[u8]) -> std::result::Result<u32, i32> {
        let replay = self.replay_start();
        let _dir_guard = self.lock_dir(ino);
        let result = self.op_write(ino, offset, data);
        self.log_replay(
            "nfs",
            "write",
            replay,
            result.as_ref().err().copied(),
            json!({
                "ino": ino,
                "offset": offset,
                "len": data.len(),
                "written": result.as_ref().ok().copied().unwrap_or(0),
            }),
        );
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
        self.log_replay(
            "nfs",
            "unlink",
            replay,
            result.as_ref().err().copied(),
            json!({ "parent": parent, "name": name }),
        );
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
        self.log_replay(
            "nfs",
            "rmdir",
            replay,
            result.as_ref().err().copied(),
            json!({ "parent": parent, "name": name }),
        );
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
        self.log_replay(
            "nfs",
            "rename",
            replay,
            result.as_ref().err().copied(),
            json!({ "parent": parent, "name": name, "newparent": newparent, "newname": newname, "flags": flags }),
        );
        result
    }

    pub fn nfs_flush(&self) -> std::result::Result<(), i32> {
        let replay = self.replay_start();
        let result = self.op_flush_all();
        self.log_replay(
            "nfs",
            "flush",
            replay,
            result.as_ref().err().copied(),
            json!({}),
        );
        result
    }
}
