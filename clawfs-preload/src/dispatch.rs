use clawfs::inode::{InodeRecord, ROOT_INODE};

use crate::fd_table::FdEntry;
use crate::runtime::ClawfsRuntime;

/// Walk a ClawFS inner path (e.g. "/foo/bar") component by component,
/// returning (parent_ino, target_ino, basename). For the root path "/",
/// returns (ROOT_INODE, ROOT_INODE, "").
pub fn resolve_path(rt: &ClawfsRuntime, inner: &str) -> Result<(u64, u64, String), i32> {
    if inner == "/" {
        return Ok((ROOT_INODE, ROOT_INODE, String::new()));
    }

    let components: Vec<&str> = inner.split('/').filter(|c| !c.is_empty()).collect();

    if components.is_empty() {
        return Ok((ROOT_INODE, ROOT_INODE, String::new()));
    }

    let mut current_ino = ROOT_INODE;
    // Walk all components except the last to find the parent.
    for &component in &components[..components.len() - 1] {
        let record = rt.fs.nfs_lookup(current_ino, component)?;
        if !record.is_dir() {
            return Err(libc::ENOTDIR);
        }
        current_ino = record.inode;
    }

    let basename = components.last().unwrap().to_string();
    let parent_ino = current_ino;

    // Look up the final component.
    match rt.fs.nfs_lookup(parent_ino, &basename) {
        Ok(record) => Ok((parent_ino, record.inode, basename)),
        Err(libc::ENOENT) => Err(libc::ENOENT),
        Err(e) => Err(e),
    }
}

/// Like resolve_path but only resolves to the parent directory + basename,
/// without requiring the target to exist.
pub fn resolve_parent(rt: &ClawfsRuntime, inner: &str) -> Result<(u64, String), i32> {
    if inner == "/" {
        return Ok((ROOT_INODE, String::new()));
    }

    let components: Vec<&str> = inner.split('/').filter(|c| !c.is_empty()).collect();

    if components.is_empty() {
        return Ok((ROOT_INODE, String::new()));
    }

    let mut current_ino = ROOT_INODE;
    for &component in &components[..components.len() - 1] {
        let record = rt.fs.nfs_lookup(current_ino, component)?;
        if !record.is_dir() {
            return Err(libc::ENOTDIR);
        }
        current_ino = record.inode;
    }

    let basename = components.last().unwrap().to_string();
    Ok((current_ino, basename))
}

/// Dispatch `open` — create-if-needed, then register in fd table.
pub fn dispatch_open(
    rt: &ClawfsRuntime,
    inner: &str,
    flags: i32,
    _mode: libc::mode_t,
) -> Result<i32, i32> {
    let uid = unsafe { libc::geteuid() as u32 };
    let gid = unsafe { libc::getegid() as u32 };

    let creating = flags & libc::O_CREAT != 0;

    let (parent_ino, basename) = resolve_parent(rt, inner)?;
    if basename.is_empty() {
        // Trying to open "/" — return an fd for the root directory.
        let fd = rt.fd_table.insert(
            ROOT_INODE,
            ROOT_INODE,
            String::new(),
            "/".to_string(),
            flags,
            true,
        );
        return Ok(fd);
    }

    let record = match rt.fs.nfs_lookup(parent_ino, &basename) {
        Ok(rec) => {
            if creating && (flags & libc::O_EXCL != 0) {
                return Err(libc::EEXIST);
            }
            // Handle O_TRUNC on existing files.
            if flags & libc::O_TRUNC != 0 && !rec.is_dir() {
                rt.fs
                    .nfs_setattr(rec.inode, None, None, None, Some(0), None, None)?;
            }
            rec
        }
        Err(libc::ENOENT) if creating => {
            // Create the file.
            rt.fs.nfs_create(parent_ino, &basename, uid, gid)?
        }
        Err(e) => return Err(e),
    };

    let is_dir = record.is_dir();
    let fd = rt.fd_table.insert(
        record.inode,
        parent_ino,
        basename,
        inner.to_string(),
        flags,
        is_dir,
    );
    Ok(fd)
}

/// Dispatch `close` — remove from fd table, optionally flush.
pub fn dispatch_close(rt: &ClawfsRuntime, fd: i32) -> Result<(), i32> {
    let entry = rt.fd_table.remove(fd).ok_or(libc::EBADF)?;
    // Best-effort flush for files opened for writing.
    if entry.flags & (libc::O_WRONLY | libc::O_RDWR) != 0 && !entry.is_dir {
        let _ = rt.fs.nfs_flush();
    }
    Ok(())
}

/// Dispatch `read` — read from the inode at the fd's current offset.
pub fn dispatch_read(rt: &ClawfsRuntime, entry: &FdEntry, buf: &mut [u8]) -> Result<usize, i32> {
    let offset = entry.get_offset();
    if offset < 0 {
        return Err(libc::EINVAL);
    }
    let data = rt
        .fs
        .nfs_read(entry.inode, offset as u64, buf.len() as u32)?;
    let n = data.len().min(buf.len());
    buf[..n].copy_from_slice(&data[..n]);
    entry.advance_offset(n as i64);
    Ok(n)
}

/// Dispatch `write` — write data at the fd's current offset.
pub fn dispatch_write(rt: &ClawfsRuntime, entry: &FdEntry, data: &[u8]) -> Result<usize, i32> {
    let offset = entry.get_offset();
    if offset < 0 {
        return Err(libc::EINVAL);
    }

    // Handle O_APPEND: seek to end before writing.
    let write_offset = if entry.flags & libc::O_APPEND != 0 {
        let attr = rt.fs.nfs_getattr(entry.inode)?;
        attr.size
    } else {
        offset as u64
    };

    let written = rt.fs.nfs_write(entry.inode, write_offset, data)? as usize;
    if entry.flags & libc::O_APPEND != 0 {
        // After append, offset should be at end of file.
        let attr = rt.fs.nfs_getattr(entry.inode)?;
        entry.set_offset(attr.size as i64);
    } else {
        entry.advance_offset(written as i64);
    }
    Ok(written)
}

/// Dispatch `lseek`.
pub fn dispatch_lseek(
    rt: &ClawfsRuntime,
    entry: &FdEntry,
    offset: i64,
    whence: i32,
) -> Result<i64, i32> {
    let new_offset = match whence {
        libc::SEEK_SET => offset,
        libc::SEEK_CUR => entry.get_offset() + offset,
        libc::SEEK_END => {
            let attr = rt.fs.nfs_getattr(entry.inode)?;
            attr.size as i64 + offset
        }
        _ => return Err(libc::EINVAL),
    };

    if new_offset < 0 {
        return Err(libc::EINVAL);
    }

    Ok(entry.set_offset(new_offset))
}

/// Dispatch `stat`/`lstat` — fill a libc::stat from an InodeRecord.
pub fn dispatch_stat(rt: &ClawfsRuntime, inner: &str) -> Result<libc::stat, i32> {
    let (_, target_ino, _) = resolve_path(rt, inner)?;
    let record = rt.fs.nfs_getattr(target_ino)?;
    Ok(inode_to_stat(&record))
}

/// Dispatch `fstat` — stat by inode from fd entry.
pub fn dispatch_fstat(rt: &ClawfsRuntime, entry: &FdEntry) -> Result<libc::stat, i32> {
    let record = rt.fs.nfs_getattr(entry.inode)?;
    Ok(inode_to_stat(&record))
}

/// Dispatch `access`.
pub fn dispatch_access(rt: &ClawfsRuntime, inner: &str, _mode: i32) -> Result<(), i32> {
    // Just check that the path exists.
    let (_, target_ino, _) = resolve_path(rt, inner)?;
    let _ = rt.fs.nfs_getattr(target_ino)?;
    Ok(())
}

/// Dispatch `mkdir`.
pub fn dispatch_mkdir(rt: &ClawfsRuntime, inner: &str) -> Result<(), i32> {
    let uid = unsafe { libc::geteuid() as u32 };
    let gid = unsafe { libc::getegid() as u32 };
    let (parent_ino, basename) = resolve_parent(rt, inner)?;
    if basename.is_empty() {
        return Err(libc::EEXIST);
    }
    rt.fs.nfs_mkdir(parent_ino, &basename, uid, gid)?;
    rt.fs.nfs_flush()?;
    Ok(())
}

/// Dispatch `unlink`.
pub fn dispatch_unlink(rt: &ClawfsRuntime, inner: &str) -> Result<(), i32> {
    let uid = unsafe { libc::geteuid() as u32 };
    let (parent_ino, basename) = resolve_parent(rt, inner)?;
    if basename.is_empty() {
        return Err(libc::EISDIR);
    }
    log::trace!("dispatch_unlink(inner={inner:?})");
    rt.fs.nfs_remove_file(parent_ino, &basename, uid)?;
    let result = rt.fs.nfs_flush();
    log::trace!("dispatch_unlink flush -> {result:?}");
    result
}

/// Dispatch `rmdir`.
pub fn dispatch_rmdir(rt: &ClawfsRuntime, inner: &str) -> Result<(), i32> {
    let uid = unsafe { libc::geteuid() as u32 };
    let (parent_ino, basename) = resolve_parent(rt, inner)?;
    if basename.is_empty() {
        return Err(libc::EBUSY); // can't rmdir "/"
    }
    rt.fs.nfs_remove_dir(parent_ino, &basename, uid)?;
    rt.fs.nfs_flush()
}

/// Dispatch `rename`.
pub fn dispatch_rename(rt: &ClawfsRuntime, old_inner: &str, new_inner: &str) -> Result<(), i32> {
    let uid = unsafe { libc::geteuid() as u32 };
    let (old_parent, old_name) = resolve_parent(rt, old_inner)?;
    let (new_parent, new_name) = resolve_parent(rt, new_inner)?;
    if old_name.is_empty() || new_name.is_empty() {
        return Err(libc::EINVAL);
    }
    rt.fs
        .nfs_rename(old_parent, &old_name, new_parent, &new_name, 0, uid)?;
    rt.fs.nfs_flush()
}

/// Dispatch `pread` — read at explicit offset without updating fd offset.
pub fn dispatch_pread(
    rt: &ClawfsRuntime,
    entry: &FdEntry,
    buf: &mut [u8],
    offset: i64,
) -> Result<usize, i32> {
    if offset < 0 {
        return Err(libc::EINVAL);
    }
    let data = rt
        .fs
        .nfs_read(entry.inode, offset as u64, buf.len() as u32)?;
    let n = data.len().min(buf.len());
    buf[..n].copy_from_slice(&data[..n]);
    Ok(n)
}

/// Dispatch `pwrite` — write at explicit offset without updating fd offset.
pub fn dispatch_pwrite(
    rt: &ClawfsRuntime,
    entry: &FdEntry,
    data: &[u8],
    offset: i64,
) -> Result<usize, i32> {
    if offset < 0 {
        return Err(libc::EINVAL);
    }
    let written = rt.fs.nfs_write(entry.inode, offset as u64, data)? as usize;
    Ok(written)
}

/// Dispatch `truncate`.
pub fn dispatch_truncate(rt: &ClawfsRuntime, inner: &str, length: i64) -> Result<(), i32> {
    if length < 0 {
        return Err(libc::EINVAL);
    }
    let (_, target_ino, _) = resolve_path(rt, inner)?;
    rt.fs.nfs_setattr(
        target_ino,
        None,
        None,
        None,
        Some(length as u64),
        None,
        None,
    )?;
    Ok(())
}

/// Dispatch `ftruncate`.
pub fn dispatch_ftruncate(rt: &ClawfsRuntime, entry: &FdEntry, length: i64) -> Result<(), i32> {
    if length < 0 {
        return Err(libc::EINVAL);
    }
    rt.fs.nfs_setattr(
        entry.inode,
        None,
        None,
        None,
        Some(length as u64),
        None,
        None,
    )?;
    Ok(())
}

/// Dispatch `fsync`/`fdatasync`.
pub fn dispatch_fsync(rt: &ClawfsRuntime, _entry: &FdEntry) -> Result<(), i32> {
    rt.fs.nfs_flush()
}

/// Dispatch `symlink`.
pub fn dispatch_symlink(rt: &ClawfsRuntime, target: &str, link_inner: &str) -> Result<(), i32> {
    let uid = unsafe { libc::geteuid() as u32 };
    let gid = unsafe { libc::getegid() as u32 };
    let (parent_ino, basename) = resolve_parent(rt, link_inner)?;
    if basename.is_empty() {
        return Err(libc::EEXIST);
    }
    rt.fs
        .nfs_symlink(parent_ino, &basename, target.as_bytes().to_vec(), uid, gid)?;
    rt.fs.nfs_flush()?;
    Ok(())
}

/// Dispatch `readlink`.
pub fn dispatch_readlink(rt: &ClawfsRuntime, inner: &str) -> Result<Vec<u8>, i32> {
    let (_, target_ino, _) = resolve_path(rt, inner)?;
    rt.fs.nfs_readlink(target_ino)
}

/// Dispatch `chdir` for a ClawFS path. Returns true if handled.
pub fn dispatch_chdir(rt: &ClawfsRuntime, full_path: &str, inner: &str) -> Result<(), i32> {
    let (_, target_ino, _) = resolve_path(rt, inner)?;
    let record = rt.fs.nfs_getattr(target_ino)?;
    if !record.is_dir() {
        return Err(libc::ENOTDIR);
    }
    rt.cwd
        .set_clawfs(full_path.to_string(), inner.to_string(), target_ino);
    Ok(())
}

/// Dispatch `readdir` via nfs_readdir_plus, caching results (with d_type) in the FdEntry.
pub fn dispatch_readdir_fill(rt: &ClawfsRuntime, entry: &FdEntry) -> Result<(), i32> {
    let mut dir = entry.dir_entries.lock();
    if dir.is_none() {
        let entries = rt.fs.nfs_readdir_plus(entry.inode)?;
        *dir = Some((entries, 0));
    }
    Ok(())
}

/// Convert an InodeRecord to a libc::stat struct.
fn inode_to_stat(record: &InodeRecord) -> libc::stat {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    st.st_ino = record.inode as libc::ino_t;
    st.st_nlink = record.link_count as libc::nlink_t;
    st.st_mode = record.mode;
    st.st_uid = record.uid;
    st.st_gid = record.gid;
    st.st_size = record.size as libc::off_t;
    st.st_blksize = 4096;
    st.st_blocks = record.size.div_ceil(512) as libc::blkcnt_t;
    st.st_rdev = record.rdev as libc::dev_t;

    st.st_atime = record.atime.unix_timestamp();
    st.st_atime_nsec = record.atime.nanosecond() as i64;
    st.st_mtime = record.mtime.unix_timestamp();
    st.st_mtime_nsec = record.mtime.nanosecond() as i64;
    st.st_ctime = record.ctime.unix_timestamp();
    st.st_ctime_nsec = record.ctime.nanosecond() as i64;

    st
}
