//! Fake inotify for ClawFS paths.
//!
//! Each `inotify_init`/`inotify_init1` call creates a pipe.  The read end is
//! returned to the caller as the "inotify fd".  A background thread polls
//! watched inodes every 500 ms and writes `inotify_event` structs to the write
//! end when size or mtime changes.  Because the read end is a real pipe,
//! `poll`/`select`/`epoll` and non-blocking `read` all work without any
//! additional interception.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

static INOTIFY: OnceLock<Arc<Mutex<InotifyState>>> = OnceLock::new();

fn state() -> &'static Arc<Mutex<InotifyState>> {
    INOTIFY.get_or_init(|| Arc::new(Mutex::new(InotifyState::default())))
}

#[derive(Default)]
struct InotifyState {
    instances: HashMap<i32, InotifyInstance>,
}

struct InotifyInstance {
    write_fd: i32,
    watches: HashMap<i32, WatchEntry>,
    next_wd: i32,
}

struct WatchEntry {
    inode: u64,
    mask: u32,
    last_size: u64,
    last_mtime_sec: i64,
    last_mtime_nsec: i64,
}

/// Create a fake inotify instance backed by a pipe.  `flags` comes from
/// `inotify_init1` (only `IN_NONBLOCK`/`IN_CLOEXEC` are meaningful).
/// Returns the read end of the pipe (the "inotify fd"), or -1 on error.
pub fn create(flags: i32) -> i32 {
    let mut pipe_fds = [0i32; 2];
    let pipe_flags = libc::O_CLOEXEC | (flags & libc::O_NONBLOCK);
    if unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), pipe_flags) } != 0 {
        return -1;
    }
    let (read_fd, write_fd) = (pipe_fds[0], pipe_fds[1]);
    state().lock().instances.insert(
        read_fd,
        InotifyInstance {
            write_fd,
            watches: HashMap::new(),
            next_wd: 1,
        },
    );
    log::debug!("inotify: created fake instance read_fd={read_fd} write_fd={write_fd}");
    read_fd
}

/// Add a watch for a ClawFS inode.  Captures the current file state as the
/// baseline so the first poll does not fire spuriously.
/// Returns the watch descriptor (>= 1) or a negative errno.
pub fn add_watch(inotify_fd: i32, inode: u64, mask: u32) -> i32 {
    let wd = {
        let mut st = state().lock();
        let inst = match st.instances.get_mut(&inotify_fd) {
            Some(i) => i,
            None => return -libc::EBADF,
        };
        // Reuse existing wd if the same inode is already watched.
        for (&existing_wd, entry) in &inst.watches {
            if entry.inode == inode {
                log::trace!(
                    "inotify: add_watch fd={inotify_fd} inode={inode} -> reuse wd={existing_wd}"
                );
                return existing_wd;
            }
        }
        let wd = inst.next_wd;
        inst.next_wd += 1;
        inst.watches.insert(
            wd,
            WatchEntry {
                inode,
                mask,
                last_size: 0,
                last_mtime_sec: 0,
                last_mtime_nsec: 0,
            },
        );
        wd
    };

    // Bootstrap baseline state outside the lock to avoid holding it during nfs_getattr.
    if let Some(rt) = crate::runtime::ClawfsRuntime::get() {
        if let Ok(record) = rt.fs.nfs_getattr(inode) {
            let mut st = state().lock();
            if let Some(inst) = st.instances.get_mut(&inotify_fd) {
                if let Some(entry) = inst.watches.get_mut(&wd) {
                    entry.last_size = record.size;
                    entry.last_mtime_sec = record.mtime.unix_timestamp();
                    entry.last_mtime_nsec = record.mtime.nanosecond() as i64;
                }
            }
        }
    }

    log::debug!("inotify: add_watch fd={inotify_fd} inode={inode} mask={mask:#x} -> wd={wd}");
    wd
}

/// Remove a watch descriptor.
pub fn rm_watch(inotify_fd: i32, wd: i32) -> bool {
    let removed = state()
        .lock()
        .instances
        .get_mut(&inotify_fd)
        .and_then(|inst| inst.watches.remove(&wd))
        .is_some();
    log::debug!("inotify: rm_watch fd={inotify_fd} wd={wd} removed={removed}");
    removed
}

/// Remove an inotify instance on close.  Returns the write_fd to close.
pub fn close_instance(inotify_fd: i32) -> Option<i32> {
    let inst = state().lock().instances.remove(&inotify_fd)?;
    log::debug!(
        "inotify: close_instance fd={inotify_fd} write_fd={}",
        inst.write_fd
    );
    Some(inst.write_fd)
}

/// Returns true if fd is a fake inotify fd.
pub fn is_inotify_fd(fd: i32) -> bool {
    state().lock().instances.contains_key(&fd)
}

/// Spawn the background inotify poller thread.  Safe to call multiple times;
/// each call spawns an independent thread (only called once from runtime init).
pub fn spawn_poller() {
    std::thread::Builder::new()
        .name("clawfs-inotify-poll".into())
        .spawn(poll_loop)
        .ok();
}

fn poll_loop() {
    loop {
        std::thread::sleep(std::time::Duration::from_millis(500));
        poll_once();
    }
}

fn poll_once() {
    let rt = match crate::runtime::ClawfsRuntime::get() {
        Some(rt) => rt,
        None => return,
    };

    // Snapshot watches so we don't hold the lock during nfs_getattr.
    #[allow(clippy::type_complexity)]
    let snapshots: Vec<(i32, i32, i32, u64, u32, u64, i64, i64)> = {
        let st = state().lock();
        st.instances
            .iter()
            .flat_map(|(&ifd, inst)| {
                let wfd = inst.write_fd;
                inst.watches.iter().map(move |(&wd, e)| {
                    (
                        ifd,
                        wfd,
                        wd,
                        e.inode,
                        e.mask,
                        e.last_size,
                        e.last_mtime_sec,
                        e.last_mtime_nsec,
                    )
                })
            })
            .collect()
    };

    for (inotify_fd, write_fd, wd, inode, mask, last_size, last_mtime_sec, last_mtime_nsec) in
        snapshots
    {
        let record = match rt.fs.nfs_getattr(inode) {
            Ok(r) => r,
            Err(_) => continue,
        };

        let cur_mtime_sec = record.mtime.unix_timestamp();
        let cur_mtime_nsec = record.mtime.nanosecond() as i64;
        let changed = record.size != last_size
            || cur_mtime_sec != last_mtime_sec
            || cur_mtime_nsec != last_mtime_nsec;

        if !changed {
            continue;
        }

        // Update stored baseline.
        {
            let mut st = state().lock();
            if let Some(inst) = st.instances.get_mut(&inotify_fd) {
                if let Some(entry) = inst.watches.get_mut(&wd) {
                    entry.last_size = record.size;
                    entry.last_mtime_sec = cur_mtime_sec;
                    entry.last_mtime_nsec = cur_mtime_nsec;
                }
            }
        }

        let event_mask =
            mask & (libc::IN_MODIFY | libc::IN_ATTRIB | libc::IN_CLOSE_WRITE | libc::IN_CREATE);
        if event_mask != 0 {
            write_event(write_fd, wd, event_mask);
            log::debug!(
                "inotify: sent event wd={wd} mask={event_mask:#x} inode={inode} size={}",
                record.size
            );
        }
    }
}

#[repr(C)]
struct InotifyEvent {
    wd: i32,
    mask: u32,
    cookie: u32,
    len: u32,
}

fn write_event(write_fd: i32, wd: i32, mask: u32) {
    let event = InotifyEvent {
        wd,
        mask,
        cookie: 0,
        len: 0,
    };
    let bytes = unsafe {
        std::slice::from_raw_parts(
            &event as *const InotifyEvent as *const u8,
            std::mem::size_of::<InotifyEvent>(),
        )
    };
    unsafe {
        libc::write(write_fd, bytes.as_ptr() as *const libc::c_void, bytes.len());
    }
}
