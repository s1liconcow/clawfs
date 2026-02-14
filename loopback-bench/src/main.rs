use std::collections::HashMap;
use std::ffi::{CString, OsStr, OsString};
use std::fs::{self, OpenOptions};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use clap::Parser;
use fuser::{FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request};
use libc;
use parking_lot::Mutex;

const TTL: Duration = Duration::from_secs(1);
const ROOT_INO: u64 = 1;

#[derive(Parser, Debug)]
struct Cli {
    /// Directory on the host filesystem that should be mirrored.
    #[arg(long)]
    source: PathBuf,

    /// Mount point where the FUSE filesystem will be attached.
    #[arg(long)]
    mount: PathBuf,

    /// Run in the foreground instead of forking to the background.
    #[arg(long, default_value_t = false)]
    foreground: bool,

    /// Allow other users to access the mount.
    #[arg(long, default_value_t = false)]
    allow_other: bool,
}

struct LoopbackFs {
    root: PathBuf,
    ino_to_path: Mutex<HashMap<u64, PathBuf>>, // relative path
    path_to_ino: Mutex<HashMap<PathBuf, u64>>,
    next_ino: AtomicU64,
    handle_map: Mutex<HashMap<u64, RawFd>>,
    next_handle: AtomicU64,
}

impl LoopbackFs {
    fn new(root: PathBuf) -> Self {
        let mut ino_to_path = HashMap::new();
        ino_to_path.insert(ROOT_INO, PathBuf::new());
        let mut path_to_ino = HashMap::new();
        path_to_ino.insert(PathBuf::new(), ROOT_INO);
        Self {
            root,
            ino_to_path: Mutex::new(ino_to_path),
            path_to_ino: Mutex::new(path_to_ino),
            next_ino: AtomicU64::new(ROOT_INO + 1),
            handle_map: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
        }
    }

    fn relative_path(&self, ino: u64) -> Option<PathBuf> {
        self.ino_to_path.lock().get(&ino).cloned()
    }

    fn register_path(&self, rel: PathBuf) -> u64 {
        let mut by_path = self.path_to_ino.lock();
        if let Some(ino) = by_path.get(&rel) {
            return *ino;
        }
        let ino = self.next_ino.fetch_add(1, Ordering::SeqCst);
        by_path.insert(rel.clone(), ino);
        self.ino_to_path.lock().insert(ino, rel);
        ino
    }

    fn absolute_path(&self, rel: &Path) -> PathBuf {
        if rel.as_os_str().is_empty() {
            self.root.clone()
        } else {
            self.root.join(rel)
        }
    }

    fn file_attr(&self, path: &Path, ino: u64) -> Result<FileAttr> {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("stat failed for {}", path.display()))?;
        let file_type = if metadata.is_dir() {
            FileType::Directory
        } else if metadata.is_symlink() {
            FileType::Symlink
        } else {
            FileType::RegularFile
        };
        let size = metadata.len();
        let blocks = metadata.blocks();
        let perm = metadata.permissions();
        let mode = perm.mode() as u16;
        let nlink = metadata.nlink() as u32;
        let uid = metadata.uid();
        let gid = metadata.gid();
        let atime = metadata.accessed().unwrap_or(SystemTime::UNIX_EPOCH);
        let mtime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let ctime = metadata.created().unwrap_or(SystemTime::UNIX_EPOCH);
        Ok(FileAttr {
            ino,
            size,
            blocks,
            atime,
            mtime,
            ctime,
            crtime: ctime,
            kind: file_type,
            perm: mode,
            nlink,
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        })
    }

    fn reply_attr_for_path(&self, rel: &Path, ino: u64, reply: ReplyAttr) {
        let abs = self.absolute_path(rel);
        match self.file_attr(&abs, ino) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(err) => {
                eprintln!("getattr error: {err:?}");
                reply.error(libc::EIO);
            }
        }
    }

    fn entry_for_path(&self, rel: PathBuf) -> Result<(u64, FileAttr)> {
        let abs = self.absolute_path(&rel);
        let ino = self.register_path(rel.clone());
        let attr = self.file_attr(&abs, ino)?;
        Ok((ino, attr))
    }

    fn errno_for(err: &anyhow::Error) -> i32 {
        if let Some(io_err) = err.downcast_ref::<io::Error>() {
            return match io_err.kind() {
                io::ErrorKind::NotFound => libc::ENOENT,
                io::ErrorKind::PermissionDenied => libc::EACCES,
                io::ErrorKind::AlreadyExists => libc::EEXIST,
                io::ErrorKind::InvalidInput => libc::EINVAL,
                _ => libc::EIO,
            };
        }
        libc::EIO
    }

    fn register_handle(&self, fd: RawFd) -> u64 {
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        self.handle_map.lock().insert(handle, fd);
        handle
    }

    fn raw_fd(&self, handle: u64) -> Option<RawFd> {
        self.handle_map.lock().get(&handle).copied()
    }

    fn close_handle(&self, handle: u64) {
        if let Some(fd) = self.handle_map.lock().remove(&handle) {
            unsafe {
                libc::close(fd);
            }
        }
    }

    fn open_fd(&self, abs: &Path, flags: i32, mode: Option<u32>) -> Result<RawFd> {
        let bytes = abs.as_os_str().as_bytes();
        let cstr = CString::new(bytes).context("path contains NUL")?;
        let fd = unsafe { libc::open(cstr.as_ptr(), flags, mode.unwrap_or(0o666)) };
        if fd < 0 {
            Err(io::Error::last_os_error().into())
        } else {
            Ok(fd)
        }
    }

    fn child_path(&self, parent: u64, name: &OsStr) -> Result<PathBuf> {
        let mut rel = self
            .relative_path(parent)
            .ok_or_else(|| anyhow::anyhow!("missing parent inode {parent}"))?;
        if !rel.as_os_str().is_empty() {
            rel.push(name);
        } else {
            rel = PathBuf::from(name);
        }
        Ok(rel)
    }
}

impl Filesystem for LoopbackFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.child_path(parent, name).and_then(|rel| self.entry_for_path(rel)) {
            Ok((_ino, attr)) => reply.entry(&TTL, &attr, 0),
            Err(err) => {
                let errno = LoopbackFs::errno_for(&err);
                if errno == libc::EIO {
                    eprintln!("lookup error: {err:?}");
                }
                reply.error(errno);
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        if let Some(rel) = self.relative_path(ino) {
            self.reply_attr_for_path(&rel, ino, reply);
        } else {
            reply.error(libc::ENOENT);
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
        let Some(rel) = self.relative_path(ino) else {
            reply.error(libc::ENOENT);
            return;
        };
        let abs = self.absolute_path(&rel);
        let mut entries = Vec::new();
        entries.push((ino, FileType::Directory, OsString::from(".")));
        let parent_ino = if rel.as_os_str().is_empty() {
            ROOT_INO
        } else {
            let mut parent = rel.clone();
            parent.pop();
            self.register_path(parent)
        };
        entries.push((parent_ino, FileType::Directory, OsString::from("..")));
        match fs::read_dir(&abs) {
            Ok(iter) => {
                for entry in iter.flatten() {
                    let name = entry.file_name();
                    let mut child = rel.clone();
                    child.push(&name);
                    let child_ino = self.register_path(child);
                    let kind = if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        FileType::Directory
                    } else if entry.file_type().map(|t| t.is_symlink()).unwrap_or(false) {
                        FileType::Symlink
                    } else {
                        FileType::RegularFile
                    };
                    entries.push((child_ino, kind, name));
                }
            }
            Err(err) => {
                eprintln!("readdir error: {err:?}");
                reply.error(libc::EIO);
                return;
            }
        }
        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(ino, (i + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let Some(rel) = self.relative_path(ino) else {
            reply.error(libc::ENOENT);
            return;
        };
        let abs = self.absolute_path(&rel);
        match self.open_fd(&abs, flags | libc::O_CLOEXEC, None) {
            Ok(fd) => {
                let handle = self.register_handle(fd);
                reply.opened(handle, flags as u32);
            }
            Err(err) => {
                let errno = LoopbackFs::errno_for(&err);
                reply.error(errno);
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if self.relative_path(ino).is_none() {
            reply.error(libc::ENOENT);
            return;
        }
        let Some(fd) = self.raw_fd(fh) else {
            reply.error(libc::EIO);
            return;
        };
        let mut buf = vec![0u8; size as usize];
        let rc = unsafe {
            libc::pread(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                size as usize,
                offset as libc::off_t,
            )
        };
        if rc < 0 {
            reply.error(libc::EIO);
            return;
        }
        buf.truncate(rc as usize);
        reply.data(&buf);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if self.relative_path(ino).is_none() {
            reply.error(libc::ENOENT);
            return;
        }
        let Some(fd) = self.raw_fd(fh) else {
            reply.error(libc::EIO);
            return;
        };
        let rc = unsafe {
            libc::pwrite(
                fd,
                data.as_ptr() as *const libc::c_void,
                data.len(),
                offset as libc::off_t,
            )
        };
        if rc < 0 {
            reply.error(libc::EIO);
        } else {
            reply.written(rc as u32);
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let Ok(rel) = self.child_path(parent, name) else {
            reply.error(libc::ENOENT);
            return;
        };
        let abs = self.absolute_path(&rel);
        match OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .mode(mode)
            .open(&abs)
        {
            Ok(_file) => match self.entry_for_path(rel.clone()) {
                Ok((_ino, attr)) => {
                    match self.open_fd(&self.absolute_path(&rel), libc::O_RDWR | libc::O_CLOEXEC, None) {
                        Ok(fd) => {
                            let handle = self.register_handle(fd);
                            reply.created(&TTL, &attr, 0, handle, 0);
                        }
                        Err(err) => {
                            eprintln!("create open error: {err:?}");
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(err) => {
                    eprintln!("create attr error: {err:?}");
                    reply.error(libc::EIO);
                }
            },
            Err(err) => {
                eprintln!("create error: {err:?}");
                reply.error(libc::EIO);
            }
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Ok(rel) = self.child_path(parent, name) else {
            reply.error(libc::ENOENT);
            return;
        };
        let abs = self.absolute_path(&rel);
        match fs::create_dir(&abs) {
            Ok(()) => {
                let _ = fs::set_permissions(&abs, fs::Permissions::from_mode(mode));
                match self.entry_for_path(rel) {
                    Ok((_ino, attr)) => reply.entry(&TTL, &attr, 0),
                    Err(err) => {
                        eprintln!("mkdir attr error: {err:?}");
                        reply.error(libc::EIO);
                    }
                }
            }
            Err(err) => {
                eprintln!("mkdir error: {err:?}");
                reply.error(libc::EIO);
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Ok(rel) = self.child_path(parent, name) else {
            reply.error(libc::ENOENT);
            return;
        };
        let abs = self.absolute_path(&rel);
        match fs::remove_file(&abs) {
            Ok(()) => {
                if let Some(ino) = self.path_to_ino.lock().remove(&rel) {
                    self.ino_to_path.lock().remove(&ino);
                }
                reply.ok();
            }
            Err(err) => {
                eprintln!("unlink error: {err:?}");
                reply.error(libc::EIO);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Ok(rel) = self.child_path(parent, name) else {
            reply.error(libc::ENOENT);
            return;
        };
        let abs = self.absolute_path(&rel);
        match fs::remove_dir(&abs) {
            Ok(()) => {
                if let Some(ino) = self.path_to_ino.lock().remove(&rel) {
                    self.ino_to_path.lock().remove(&ino);
                }
                reply.ok();
            }
            Err(err) => {
                eprintln!("rmdir error: {err:?}");
                reply.error(libc::EIO);
            }
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let (Ok(old_rel), Ok(new_rel)) = (self.child_path(parent, name), self.child_path(newparent, newname)) else {
            reply.error(libc::ENOENT);
            return;
        };
        let old_abs = self.absolute_path(&old_rel);
        let new_abs = self.absolute_path(&new_rel);
        match fs::rename(&old_abs, &new_abs) {
            Ok(()) => {
                let mut paths = self.path_to_ino.lock();
                if let Some(&ino) = paths.get(&old_rel) {
                    paths.remove(&old_rel);
                    paths.insert(new_rel.clone(), ino);
                    self.ino_to_path.lock().insert(ino, new_rel);
                }
                reply.ok();
            }
            Err(err) => {
                eprintln!("rename error: {err:?}");
                reply.error(libc::EIO);
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let Some(rel) = self.relative_path(ino) else {
            reply.error(libc::ENOENT);
            return;
        };
        let abs = self.absolute_path(&rel);
        if let Some(mode) = mode {
            let _ = fs::set_permissions(&abs, fs::Permissions::from_mode(mode));
        }
        if let Some(len) = size {
            if let Err(err) = OpenOptions::new().write(true).open(&abs).and_then(|file| file.set_len(len)) {
                eprintln!("truncate error: {err:?}");
                reply.error(libc::EIO);
                return;
            }
        }
        self.reply_attr_for_path(&rel, ino, reply);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.close_handle(fh);
        reply.ok();
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let source = fs::canonicalize(&cli.source)
        .with_context(|| format!("source {} missing", cli.source.display()))?;
    let mount = cli.mount.clone();
    if !mount.exists() {
        fs::create_dir_all(&mount)
            .with_context(|| format!("creating mount {}", mount.display()))?;
    }
    let fs = LoopbackFs::new(source);
    let mut options = vec![MountOption::FSName("loopback".into()), MountOption::DefaultPermissions];
    if cli.allow_other {
        options.push(MountOption::AllowOther);
    }
    if !cli.foreground {
        options.push(MountOption::AutoUnmount);
    }
    fuser::mount2(fs, &mount, &options)?;
    Ok(())
}
