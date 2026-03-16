//! dlsym(RTLD_NEXT) trampolines and reentrancy guard.
//!
//! Each intercepted libc function:
//! 1. Checks reentrancy guard (fall through if re-entrant)
//! 2. Checks runtime initialized (fall through if not)
//! 3. Resolves path or fd
//! 4. Classifies via PrefixRouter
//! 5. Dispatches to ClawFS or falls through to real libc

use std::cell::Cell;
use std::ffi::{CStr, CString};
use std::path::PathBuf;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::OnceLock;

use crate::dispatch;
use crate::errno::set_errno;
use crate::fd_table::FdEntry;
use crate::runtime::ClawfsRuntime;
use dashmap::DashMap;

// ---------------------------------------------------------------------------
// Reentrancy guard
// ---------------------------------------------------------------------------

thread_local! {
    static REENTRANT: Cell<bool> = const { Cell::new(false) };
}

struct ReentrancyGuard;

static FD_PROXY_THREADS: OnceLock<DashMap<i32, std::thread::JoinHandle<()>>> = OnceLock::new();

impl ReentrancyGuard {
    /// Try to enter the guard. Returns `Some(ReentrancyGuard)` if we're not
    /// already inside a hook, `None` if we are (caller should fall through).
    fn enter() -> Option<Self> {
        REENTRANT.with(|r| {
            if r.get() {
                None
            } else {
                r.set(true);
                Some(ReentrancyGuard)
            }
        })
    }
}

impl Drop for ReentrancyGuard {
    fn drop(&mut self) {
        REENTRANT.with(|r| r.set(false));
    }
}

// ---------------------------------------------------------------------------
// Real function pointers (resolved lazily via dlsym)
// ---------------------------------------------------------------------------

macro_rules! real_fn {
    ($static_name:ident, $getter:ident, $libc_name:expr, $fn_type:ty) => {
        static $static_name: AtomicPtr<libc::c_void> = AtomicPtr::new(std::ptr::null_mut());

        #[allow(non_snake_case)]
        fn $getter() -> $fn_type {
            let mut ptr = $static_name.load(Ordering::Relaxed);
            if ptr.is_null() {
                let sym = CString::new($libc_name).expect("CString::new failed");
                ptr = unsafe { libc::dlsym(libc::RTLD_NEXT, sym.as_ptr()) };
                if ptr.is_null() {
                    eprintln!(
                        "clawfs-preload: FATAL: dlsym(RTLD_NEXT, {:?}) returned null",
                        sym
                    );
                    std::process::abort();
                }
                $static_name.store(ptr, Ordering::Relaxed);
            }
            unsafe { std::mem::transmute(ptr) }
        }
    };
}

// Declare real function pointers.
real_fn!(
    REAL_OPEN_PTR,
    get_real_open,
    "open",
    unsafe extern "C" fn(*const libc::c_char, i32, libc::mode_t) -> i32
);
real_fn!(
    REAL_OPEN64_PTR,
    get_real_open64,
    "open64",
    unsafe extern "C" fn(*const libc::c_char, i32, libc::mode_t) -> i32
);
real_fn!(
    REAL_OPENAT_PTR,
    get_real_openat,
    "openat",
    unsafe extern "C" fn(i32, *const libc::c_char, i32, libc::mode_t) -> i32
);
real_fn!(
    REAL_CLOSE_PTR,
    get_real_close,
    "close",
    unsafe extern "C" fn(i32) -> i32
);
real_fn!(
    REAL_READ_PTR,
    get_real_read,
    "read",
    unsafe extern "C" fn(i32, *mut libc::c_void, libc::size_t) -> libc::ssize_t
);
real_fn!(
    REAL_WRITE_PTR,
    get_real_write,
    "write",
    unsafe extern "C" fn(i32, *const libc::c_void, libc::size_t) -> libc::ssize_t
);
real_fn!(
    REAL___WRITE_PTR,
    get_real___write,
    "__write",
    unsafe extern "C" fn(i32, *const libc::c_void, libc::size_t) -> libc::ssize_t
);
real_fn!(
    REAL___WRITE_NOCANCEL_PTR,
    get_real___write_nocancel,
    "__write_nocancel",
    unsafe extern "C" fn(i32, *const libc::c_void, libc::size_t) -> libc::ssize_t
);
real_fn!(
    REAL_WRITEV_PTR,
    get_real_writev,
    "writev",
    unsafe extern "C" fn(i32, *const libc::iovec, libc::c_int) -> libc::ssize_t
);
real_fn!(
    REAL_FFLUSH_PTR,
    get_real_fflush,
    "fflush",
    unsafe extern "C" fn(*mut libc::FILE) -> i32
);
real_fn!(
    REAL_FWRITE_PTR,
    get_real_fwrite,
    "fwrite",
    unsafe extern "C" fn(
        *const libc::c_void,
        libc::size_t,
        libc::size_t,
        *mut libc::FILE,
    ) -> libc::size_t
);
real_fn!(
    REAL_FWRITE_UNLOCKED_PTR,
    get_real_fwrite_unlocked,
    "fwrite_unlocked",
    unsafe extern "C" fn(
        *const libc::c_void,
        libc::size_t,
        libc::size_t,
        *mut libc::FILE,
    ) -> libc::size_t
);
real_fn!(
    REAL_FPUTS_PTR,
    get_real_fputs,
    "fputs",
    unsafe extern "C" fn(*const libc::c_char, *mut libc::FILE) -> i32
);
real_fn!(
    REAL_FPUTC_PTR,
    get_real_fputc,
    "fputc",
    unsafe extern "C" fn(i32, *mut libc::FILE) -> i32
);
real_fn!(
    REAL_PUTC_PTR,
    get_real_putc,
    "putc",
    unsafe extern "C" fn(i32, *mut libc::FILE) -> i32
);
real_fn!(
    REAL_PUTCHAR_PTR,
    get_real_putchar,
    "putchar",
    unsafe extern "C" fn(i32) -> i32
);
real_fn!(
    REAL_PUTS_PTR,
    get_real_puts,
    "puts",
    unsafe extern "C" fn(*const libc::c_char) -> i32
);
real_fn!(
    REAL_LSEEK_PTR,
    get_real_lseek,
    "lseek",
    unsafe extern "C" fn(i32, libc::off_t, i32) -> libc::off_t
);
real_fn!(
    REAL_LSEEK64_PTR,
    get_real_lseek64,
    "lseek64",
    unsafe extern "C" fn(i32, libc::off64_t, i32) -> libc::off64_t
);
real_fn!(
    REAL_XSTAT_PTR,
    get_real___xstat,
    "__xstat",
    unsafe extern "C" fn(i32, *const libc::c_char, *mut libc::stat) -> i32
);
real_fn!(
    REAL_LXSTAT_PTR,
    get_real___lxstat,
    "__lxstat",
    unsafe extern "C" fn(i32, *const libc::c_char, *mut libc::stat) -> i32
);
real_fn!(
    REAL_FXSTAT_PTR,
    get_real___fxstat,
    "__fxstat",
    unsafe extern "C" fn(i32, i32, *mut libc::stat) -> i32
);
real_fn!(
    REAL_STAT_PTR,
    get_real_stat,
    "stat",
    unsafe extern "C" fn(*const libc::c_char, *mut libc::stat) -> i32
);
real_fn!(
    REAL_LSTAT_PTR,
    get_real_lstat,
    "lstat",
    unsafe extern "C" fn(*const libc::c_char, *mut libc::stat) -> i32
);
real_fn!(
    REAL_FSTAT_PTR,
    get_real_fstat,
    "fstat",
    unsafe extern "C" fn(i32, *mut libc::stat) -> i32
);
real_fn!(
    REAL_ACCESS_PTR,
    get_real_access,
    "access",
    unsafe extern "C" fn(*const libc::c_char, i32) -> i32
);
real_fn!(
    REAL_MKDIR_PTR,
    get_real_mkdir,
    "mkdir",
    unsafe extern "C" fn(*const libc::c_char, libc::mode_t) -> i32
);
real_fn!(
    REAL_UNLINK_PTR,
    get_real_unlink,
    "unlink",
    unsafe extern "C" fn(*const libc::c_char) -> i32
);
real_fn!(
    REAL_RMDIR_PTR,
    get_real_rmdir,
    "rmdir",
    unsafe extern "C" fn(*const libc::c_char) -> i32
);
real_fn!(
    REAL_RENAME_PTR,
    get_real_rename,
    "rename",
    unsafe extern "C" fn(*const libc::c_char, *const libc::c_char) -> i32
);
real_fn!(
    REAL_FSTATAT_PTR,
    get_real_fstatat,
    "fstatat",
    unsafe extern "C" fn(i32, *const libc::c_char, *mut libc::stat, i32) -> i32
);
real_fn!(
    REAL_FSTATAT64_PTR,
    get_real_fstatat64,
    "fstatat64",
    unsafe extern "C" fn(i32, *const libc::c_char, *mut libc::stat, i32) -> i32
);
real_fn!(
    REAL_STAT64_PTR,
    get_real_stat64,
    "stat64",
    unsafe extern "C" fn(*const libc::c_char, *mut libc::stat) -> i32
);
real_fn!(
    REAL_LSTAT64_PTR,
    get_real_lstat64,
    "lstat64",
    unsafe extern "C" fn(*const libc::c_char, *mut libc::stat) -> i32
);
real_fn!(
    REAL_FSTAT64_PTR,
    get_real_fstat64,
    "fstat64",
    unsafe extern "C" fn(i32, *mut libc::stat) -> i32
);

// Phase 2 real function pointers.
real_fn!(
    REAL_PREAD_PTR,
    get_real_pread,
    "pread",
    unsafe extern "C" fn(i32, *mut libc::c_void, libc::size_t, libc::off_t) -> libc::ssize_t
);
real_fn!(
    REAL_PREAD64_PTR,
    get_real_pread64,
    "pread64",
    unsafe extern "C" fn(i32, *mut libc::c_void, libc::size_t, libc::off64_t) -> libc::ssize_t
);
real_fn!(
    REAL_PWRITE_PTR,
    get_real_pwrite,
    "pwrite",
    unsafe extern "C" fn(i32, *const libc::c_void, libc::size_t, libc::off_t) -> libc::ssize_t
);
real_fn!(
    REAL_PWRITE64_PTR,
    get_real_pwrite64,
    "pwrite64",
    unsafe extern "C" fn(i32, *const libc::c_void, libc::size_t, libc::off64_t) -> libc::ssize_t
);
real_fn!(
    REAL_TRUNCATE_PTR,
    get_real_truncate,
    "truncate",
    unsafe extern "C" fn(*const libc::c_char, libc::off_t) -> i32
);
real_fn!(
    REAL_FTRUNCATE_PTR,
    get_real_ftruncate,
    "ftruncate",
    unsafe extern "C" fn(i32, libc::off_t) -> i32
);
real_fn!(
    REAL_FSYNC_PTR,
    get_real_fsync,
    "fsync",
    unsafe extern "C" fn(i32) -> i32
);
real_fn!(
    REAL_FDATASYNC_PTR,
    get_real_fdatasync,
    "fdatasync",
    unsafe extern "C" fn(i32) -> i32
);
real_fn!(
    REAL_DUP_PTR,
    get_real_dup,
    "dup",
    unsafe extern "C" fn(i32) -> i32
);
real_fn!(
    REAL_DUP2_PTR,
    get_real_dup2,
    "dup2",
    unsafe extern "C" fn(i32, i32) -> i32
);
real_fn!(
    REAL_DUP3_PTR,
    get_real_dup3,
    "dup3",
    unsafe extern "C" fn(i32, i32, i32) -> i32
);
real_fn!(
    REAL_SYMLINK_PTR,
    get_real_symlink,
    "symlink",
    unsafe extern "C" fn(*const libc::c_char, *const libc::c_char) -> i32
);
real_fn!(
    REAL_READLINK_PTR,
    get_real_readlink,
    "readlink",
    unsafe extern "C" fn(*const libc::c_char, *mut libc::c_char, libc::size_t) -> libc::ssize_t
);
real_fn!(
    REAL_READLINKAT_PTR,
    get_real_readlinkat,
    "readlinkat",
    unsafe extern "C" fn(
        i32,
        *const libc::c_char,
        *mut libc::c_char,
        libc::size_t,
    ) -> libc::ssize_t
);
real_fn!(
    REAL_CHDIR_PTR,
    get_real_chdir,
    "chdir",
    unsafe extern "C" fn(*const libc::c_char) -> i32
);
real_fn!(
    REAL_GETCWD_PTR,
    get_real_getcwd,
    "getcwd",
    unsafe extern "C" fn(*mut libc::c_char, libc::size_t) -> *mut libc::c_char
);
real_fn!(
    REAL_OPENDIR_PTR,
    get_real_opendir,
    "opendir",
    unsafe extern "C" fn(*const libc::c_char) -> *mut libc::DIR
);
real_fn!(
    REAL_CLOSEDIR_PTR,
    get_real_closedir,
    "closedir",
    unsafe extern "C" fn(*mut libc::DIR) -> i32
);
real_fn!(
    REAL_READDIR_PTR,
    get_real_readdir,
    "readdir",
    unsafe extern "C" fn(*mut libc::DIR) -> *mut libc::dirent
);
real_fn!(
    REAL_READDIR64_PTR,
    get_real_readdir64,
    "readdir64",
    unsafe extern "C" fn(*mut libc::DIR) -> *mut libc::dirent64
);
real_fn!(
    REAL_DIRFD_PTR,
    get_real_dirfd,
    "dirfd",
    unsafe extern "C" fn(*mut libc::DIR) -> i32
);
real_fn!(
    REAL_FDOPENDIR_PTR,
    get_real_fdopendir,
    "fdopendir",
    unsafe extern "C" fn(i32) -> *mut libc::DIR
);
real_fn!(
    REAL_SEEKDIR_PTR,
    get_real_seekdir,
    "seekdir",
    unsafe extern "C" fn(*mut libc::DIR, libc::c_long)
);
real_fn!(
    REAL_TELLDIR_PTR,
    get_real_telldir,
    "telldir",
    unsafe extern "C" fn(*mut libc::DIR) -> libc::c_long
);
real_fn!(
    REAL_REWINDDIR_PTR,
    get_real_rewinddir,
    "rewinddir",
    unsafe extern "C" fn(*mut libc::DIR)
);
real_fn!(
    REAL_MKDIRAT_PTR,
    get_real_mkdirat,
    "mkdirat",
    unsafe extern "C" fn(i32, *const libc::c_char, libc::mode_t) -> i32
);
real_fn!(
    REAL_UNLINKAT_PTR,
    get_real_unlinkat,
    "unlinkat",
    unsafe extern "C" fn(i32, *const libc::c_char, i32) -> i32
);
real_fn!(
    REAL_RENAMEAT_PTR,
    get_real_renameat,
    "renameat",
    unsafe extern "C" fn(i32, *const libc::c_char, i32, *const libc::c_char) -> i32
);
real_fn!(
    REAL_RENAMEAT2_PTR,
    get_real_renameat2,
    "renameat2",
    unsafe extern "C" fn(i32, *const libc::c_char, i32, *const libc::c_char, libc::c_uint) -> i32
);
real_fn!(
    REAL_OPENAT64_PTR,
    get_real_openat64,
    "openat64",
    unsafe extern "C" fn(i32, *const libc::c_char, i32, libc::mode_t) -> i32
);
real_fn!(
    REAL_TRUNCATE64_PTR,
    get_real_truncate64,
    "truncate64",
    unsafe extern "C" fn(*const libc::c_char, libc::off64_t) -> i32
);
real_fn!(
    REAL_FTRUNCATE64_PTR,
    get_real_ftruncate64,
    "ftruncate64",
    unsafe extern "C" fn(i32, libc::off64_t) -> i32
);
real_fn!(
    REAL_STATX_PTR,
    get_real_statx,
    "statx",
    unsafe extern "C" fn(i32, *const libc::c_char, i32, libc::c_uint, *mut libc::statx) -> i32
);

// stdio FILE* functions.
real_fn!(
    REAL_FOPEN_PTR,
    get_real_fopen,
    "fopen",
    unsafe extern "C" fn(*const libc::c_char, *const libc::c_char) -> *mut libc::FILE
);
real_fn!(
    REAL_FOPEN64_PTR,
    get_real_fopen64,
    "fopen64",
    unsafe extern "C" fn(*const libc::c_char, *const libc::c_char) -> *mut libc::FILE
);
real_fn!(
    REAL_FCLOSE_PTR,
    get_real_fclose,
    "fclose",
    unsafe extern "C" fn(*mut libc::FILE) -> i32
);
real_fn!(
    REAL_FDOPEN_PTR,
    get_real_fdopen,
    "fdopen",
    unsafe extern "C" fn(i32, *const libc::c_char) -> *mut libc::FILE
);
real_fn!(
    REAL_FREAD_PTR,
    get_real_fread,
    "fread",
    unsafe extern "C" fn(
        *mut libc::c_void,
        libc::size_t,
        libc::size_t,
        *mut libc::FILE,
    ) -> libc::size_t
);
real_fn!(
    REAL_FREAD_UNLOCKED_PTR,
    get_real_fread_unlocked,
    "fread_unlocked",
    unsafe extern "C" fn(
        *mut libc::c_void,
        libc::size_t,
        libc::size_t,
        *mut libc::FILE,
    ) -> libc::size_t
);
real_fn!(
    REAL_FILENO_UNLOCKED_PTR,
    get_real_fileno_unlocked,
    "fileno_unlocked",
    unsafe extern "C" fn(*mut libc::FILE) -> i32
);
real_fn!(
    REAL_FGETS_PTR,
    get_real_fgets,
    "fgets",
    unsafe extern "C" fn(*mut libc::c_char, i32, *mut libc::FILE) -> *mut libc::c_char
);
real_fn!(
    REAL_FGETC_PTR,
    get_real_fgetc,
    "fgetc",
    unsafe extern "C" fn(*mut libc::FILE) -> i32
);
real_fn!(
    REAL_FSEEK_PTR,
    get_real_fseek,
    "fseek",
    unsafe extern "C" fn(*mut libc::FILE, libc::c_long, i32) -> i32
);
real_fn!(
    REAL_FSEEKO_PTR,
    get_real_fseeko,
    "fseeko",
    unsafe extern "C" fn(*mut libc::FILE, libc::off_t, i32) -> i32
);
real_fn!(
    REAL_FSEEKO64_PTR,
    get_real_fseeko64,
    "fseeko64",
    unsafe extern "C" fn(*mut libc::FILE, libc::off64_t, i32) -> i32
);
real_fn!(
    REAL_FTELL_PTR,
    get_real_ftell,
    "ftell",
    unsafe extern "C" fn(*mut libc::FILE) -> libc::c_long
);
real_fn!(
    REAL_FTELLO_PTR,
    get_real_ftello,
    "ftello",
    unsafe extern "C" fn(*mut libc::FILE) -> libc::off_t
);
real_fn!(
    REAL_FTELLO64_PTR,
    get_real_ftello64,
    "ftello64",
    unsafe extern "C" fn(*mut libc::FILE) -> libc::off64_t
);
real_fn!(
    REAL_REWIND_PTR,
    get_real_rewind,
    "rewind",
    unsafe extern "C" fn(*mut libc::FILE)
);
real_fn!(
    REAL_FEOF_PTR,
    get_real_feof,
    "feof",
    unsafe extern "C" fn(*mut libc::FILE) -> i32
);

// Missing *at-family functions.
real_fn!(
    REAL_SYMLINKAT_PTR,
    get_real_symlinkat,
    "symlinkat",
    unsafe extern "C" fn(*const libc::c_char, i32, *const libc::c_char) -> i32
);
real_fn!(
    REAL_FACCESSAT_PTR,
    get_real_faccessat,
    "faccessat",
    unsafe extern "C" fn(i32, *const libc::c_char, i32, i32) -> i32
);
// ioctl/fcntl: defined as fixed-arg (3 params) — ABI-compatible with variadic
// on x86_64 since the calling convention passes args in registers regardless.
real_fn!(
    REAL_IOCTL_PTR,
    get_real_ioctl,
    "ioctl",
    unsafe extern "C" fn(i32, libc::c_ulong, libc::c_ulong) -> i32
);
real_fn!(
    REAL_FCNTL_PTR,
    get_real_fcntl,
    "fcntl",
    unsafe extern "C" fn(i32, libc::c_int, libc::c_int) -> i32
);
real_fn!(
    REAL_FCNTL64_PTR,
    get_real_fcntl64,
    "fcntl64",
    unsafe extern "C" fn(i32, libc::c_int, libc::c_int) -> i32
);
real_fn!(
    REAL_INOTIFY_INIT_PTR,
    get_real_inotify_init,
    "inotify_init",
    unsafe extern "C" fn() -> i32
);
real_fn!(
    REAL_INOTIFY_INIT1_PTR,
    get_real_inotify_init1,
    "inotify_init1",
    unsafe extern "C" fn(i32) -> i32
);
real_fn!(
    REAL_INOTIFY_ADD_WATCH_PTR,
    get_real_inotify_add_watch,
    "inotify_add_watch",
    unsafe extern "C" fn(i32, *const libc::c_char, u32) -> i32
);
real_fn!(
    REAL_INOTIFY_RM_WATCH_PTR,
    get_real_inotify_rm_watch,
    "inotify_rm_watch",
    unsafe extern "C" fn(i32, i32) -> i32
);

// ---------------------------------------------------------------------------
// Helper: extract path string from C pointer
// ---------------------------------------------------------------------------

fn c_path_to_str(path: *const libc::c_char) -> Option<String> {
    if path.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(path) }
        .to_str()
        .ok()
        .map(String::from)
}

fn dispatch_write_fd(
    rt: &ClawfsRuntime,
    fd: i32,
    buf: *const libc::c_void,
    count: libc::size_t,
    hook_name: &str,
) -> Option<libc::ssize_t> {
    if fd_proxy_threads().contains_key(&fd) {
        return None;
    }
    let entry = rt.fd_table.get(fd)?;
    let slice = unsafe { std::slice::from_raw_parts(buf as *const u8, count) };
    let result = dispatch::dispatch_write(rt, &entry, slice);
    if fd <= libc::STDERR_FILENO {
        log::trace!("{hook_name}(clawfs remapped fd={fd}, count={count}) -> {result:?}");
    }
    Some(match result {
        Ok(n) => {
            if fd <= libc::STDERR_FILENO {
                let _ = rt.fs.nfs_flush();
            }
            n as libc::ssize_t
        }
        Err(e) => set_errno(e) as libc::ssize_t,
    })
}

fn dispatch_writev_fd(
    rt: &ClawfsRuntime,
    fd: i32,
    iov: *const libc::iovec,
    iovcnt: libc::c_int,
    hook_name: &str,
) -> Option<libc::ssize_t> {
    if fd_proxy_threads().contains_key(&fd) {
        return None;
    }
    let entry = rt.fd_table.get(fd)?;
    if iovcnt < 0 {
        return Some(set_errno(libc::EINVAL) as libc::ssize_t);
    }
    let iovecs = unsafe { std::slice::from_raw_parts(iov, iovcnt as usize) };
    let total_len = match iovecs
        .iter()
        .try_fold(0usize, |acc, part| acc.checked_add(part.iov_len))
    {
        Some(total) => total,
        None => return Some(set_errno(libc::EINVAL) as libc::ssize_t),
    };
    let mut combined = Vec::with_capacity(total_len);
    for part in iovecs {
        let slice = unsafe { std::slice::from_raw_parts(part.iov_base as *const u8, part.iov_len) };
        combined.extend_from_slice(slice);
    }
    let result = dispatch::dispatch_write(rt, &entry, &combined);
    if fd <= libc::STDERR_FILENO {
        log::trace!("{hook_name}(clawfs remapped fd={fd}, iovcnt={iovcnt}, total={total_len}) -> {result:?}");
    }
    Some(match result {
        Ok(n) => n as libc::ssize_t,
        Err(e) => set_errno(e) as libc::ssize_t,
    })
}

fn dispatch_stdio_write(rt: &ClawfsRuntime, stream: *mut libc::FILE, data: &[u8]) -> Option<usize> {
    let fd = unsafe { stream_fd(stream) };
    if fd_proxy_threads().contains_key(&fd) {
        return None;
    }
    let entry = rt.fd_table.get(fd)?;
    match dispatch::dispatch_write(rt, &entry, data) {
        Ok(written) => {
            if fd <= libc::STDERR_FILENO {
                log::trace!("stdio write(clawfs remapped fd={fd}, count={})", data.len());
                let _ = rt.fs.nfs_flush();
            }
            Some(written)
        }
        Err(e) => {
            set_errno(e);
            None
        }
    }
}

fn fd_proxy_threads() -> &'static DashMap<i32, std::thread::JoinHandle<()>> {
    FD_PROXY_THREADS.get_or_init(DashMap::new)
}

fn get_stdio_stream(fd: i32) -> Option<*mut libc::FILE> {
    let symbol = match fd {
        libc::STDOUT_FILENO => "stdout",
        libc::STDERR_FILENO => "stderr",
        libc::STDIN_FILENO => "stdin",
        _ => return None,
    };
    let sym = CString::new(symbol).ok()?;
    let ptr = unsafe { libc::dlsym(libc::RTLD_DEFAULT, sym.as_ptr()) };
    if ptr.is_null() {
        return None;
    }
    Some(unsafe { *(ptr as *mut *mut libc::FILE) })
}

fn install_fd_proxy(
    rt: &'static ClawfsRuntime,
    newfd: i32,
    entry: std::sync::Arc<FdEntry>,
    cloexec: bool,
) -> Result<(), i32> {
    log::trace!("install_fd_proxy(fd={newfd}, cloexec={cloexec})");
    let mut pipe_fds = [0; 2];
    let pipe_flags = if cloexec { libc::O_CLOEXEC } else { 0 };
    if unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), pipe_flags) } != 0 {
        return Err(unsafe { *libc::__errno_location() });
    }
    let read_fd = pipe_fds[0];
    let write_fd = pipe_fds[1];

    if unsafe { get_real_dup2()(write_fd, newfd) } < 0 {
        let errno = unsafe { *libc::__errno_location() };
        unsafe {
            get_real_close()(read_fd);
            get_real_close()(write_fd);
        }
        return Err(errno);
    }
    unsafe {
        get_real_close()(write_fd);
    }
    if cloexec {
        unsafe {
            get_real_fcntl()(newfd, libc::F_SETFD, libc::FD_CLOEXEC);
        }
    }

    let handle = std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            let n = unsafe {
                get_real_read()(
                    read_fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len() as libc::size_t,
                )
            };
            if n <= 0 {
                break;
            }
            log::trace!("fd_proxy(fd={newfd}) drained {} bytes", n);
            let write_result = dispatch::dispatch_write(rt, &entry, &buf[..n as usize]);
            log::trace!("fd_proxy(fd={newfd}) dispatch_write -> {write_result:?}");
            let flush_result = rt.fs.nfs_flush();
            log::trace!("fd_proxy(fd={newfd}) flush -> {flush_result:?}");
        }
        fd_proxy_threads().remove(&newfd);
        let flush_result = rt.fs.nfs_flush();
        log::trace!("fd_proxy(fd={newfd}) final flush -> {flush_result:?}");
        unsafe {
            get_real_close()(read_fd);
        }
    });
    if let Some((_, old_handle)) = fd_proxy_threads().remove(&newfd) {
        let _ = old_handle.join();
    }
    fd_proxy_threads().insert(newfd, handle);
    Ok(())
}

fn cleanup_fd_proxy(fd: i32) {
    if let Some((_, handle)) = fd_proxy_threads().remove(&fd) {
        log::trace!("cleanup_fd_proxy(fd={fd})");
        unsafe {
            get_real_close()(fd);
        }
        let _ = handle.join();
    }
}

/// Try to handle a path-based operation through ClawFS. Returns `None` if
/// the path doesn't match any ClawFS prefix (caller should fall through to real libc).
fn try_classify(rt: &ClawfsRuntime, path: &str) -> Option<String> {
    if path.starts_with('/') {
        let result = rt.prefix_router.classify(path).map(|cp| cp.inner);
        log::trace!("classify abs {:?} -> {:?}", path, result);
        result
    } else {
        // Virtual CWD inside ClawFS — resolve relative to it.
        if let Some((_, inner_cwd, _)) = rt.cwd.get_clawfs() {
            let raw = format!("{}/{}", inner_cwd.trim_end_matches('/'), path);
            let inner = crate::prefix::normalize_path(&raw)
                .to_str()
                .unwrap_or("/")
                .to_string();
            log::trace!("classify rel {:?} (virtual cwd) -> {:?}", path, inner);
            return Some(inner);
        }
        // Fall back to real OS CWD.
        let abs = std::env::current_dir().ok()?.join(path);
        let result = rt.prefix_router.classify(abs.to_str()?).map(|cp| cp.inner);
        log::trace!("classify rel {:?} (-> {:?}) -> {:?}", path, abs, result);
        result
    }
}

fn host_path_for_dirfd(dirfd: i32) -> Option<PathBuf> {
    let proc_fd_path = format!("/proc/self/fd/{dirfd}");
    let target = std::fs::read_link(proc_fd_path).ok()?;
    let metadata = std::fs::metadata(&target).ok()?;
    if metadata.is_dir() {
        Some(target)
    } else {
        None
    }
}

/// Resolve a path that may be relative to a dirfd (for `*at` family).
/// Returns the inner ClawFS path if it can be resolved, or None for fall-through.
fn resolve_at_path(rt: &ClawfsRuntime, dirfd: i32, path: &str) -> Option<String> {
    if path.starts_with('/') {
        return try_classify(rt, path);
    }
    // Relative path with AT_FDCWD: resolve against virtual or real cwd.
    if dirfd == libc::AT_FDCWD {
        if let Some((_, inner_cwd, _)) = rt.cwd.get_clawfs() {
            let raw = format!("{}/{}", inner_cwd.trim_end_matches('/'), path);
            let inner = crate::prefix::normalize_path(&raw)
                .to_str()
                .unwrap_or("/")
                .to_string();
            log::trace!(
                "resolve_at AT_FDCWD (virtual cwd) {:?} -> {:?}",
                path,
                inner
            );
            return Some(inner);
        }
        // Cwd is on the host: resolve against the real process cwd then classify.
        let abs = std::env::current_dir().ok()?.join(path);
        let result = rt.prefix_router.classify(abs.to_str()?).map(|cp| cp.inner);
        log::trace!("resolve_at AT_FDCWD (host cwd) {:?} -> {:?}", path, result);
        return result;
    }
    // Relative path with a ClawFS dirfd: resolve using the fd's inner_path.
    let entry = rt.fd_table.get(dirfd)?;
    if entry.is_dir {
        let inner = format!("{}/{}", entry.inner_path.trim_end_matches('/'), path);
        log::trace!("resolve_at dirfd={} {:?} -> {:?}", dirfd, path, inner);
        return Some(inner);
    }
    log::trace!(
        "resolve_at dirfd={} {:?}: dirfd is not a ClawFS directory",
        dirfd,
        path
    );
    None.or_else(|| {
        let abs = host_path_for_dirfd(dirfd)?.join(path);
        let result = rt.prefix_router.classify(abs.to_str()?).map(|cp| cp.inner);
        log::trace!(
            "resolve_at dirfd={} (host dirfd) {:?} -> {:?}",
            dirfd,
            path,
            result
        );
        result
    })
}

// ---------------------------------------------------------------------------
// Intercepted functions
// ---------------------------------------------------------------------------

/// Core `open` logic for ClawFS paths. Returns a real kernel fd (backed by
/// /dev/null) that is mapped in `fd_table` to the ClawFS FdEntry. This ensures
/// that programs which pass the fd to unintercepted libc functions (like
/// `fdopen`) still get a valid kernel fd.
fn do_open(rt: &ClawfsRuntime, path_str: &str, inner: &str, flags: i32, mode: libc::mode_t) -> i32 {
    // Create the ClawFS FdEntry via dispatch (returns a synthetic fd).
    let synth_fd = match dispatch::dispatch_open(rt, inner, flags, mode) {
        Ok(fd) => fd,
        Err(e) => return set_errno(e) as i32,
    };
    // Get a real kernel fd by opening /dev/null.
    let devnull = CString::new("/dev/null").unwrap();
    let kernel_fd = unsafe { get_real_open()(devnull.as_ptr(), libc::O_RDWR, 0) };
    if kernel_fd < 0 {
        let _ = dispatch::dispatch_close(rt, synth_fd);
        return set_errno(libc::ENOMEM) as i32;
    }
    // Map the kernel fd to the same ClawFS FdEntry.
    rt.fd_table.dup_to(synth_fd, kernel_fd);
    // Remove the synthetic fd — only the kernel fd is used from here on.
    rt.fd_table.remove(synth_fd);
    log::trace!(
        "open({:?}) -> kernel_fd={} (synth was {})",
        path_str,
        kernel_fd,
        synth_fd
    );
    kernel_fd
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn open(path: *const libc::c_char, flags: i32, mode: libc::mode_t) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    return do_open(rt, &path_str, &inner, flags, mode);
                }
            }
        }
    }
    unsafe { get_real_open()(path, flags, mode) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn open64(path: *const libc::c_char, flags: i32, mode: libc::mode_t) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    return do_open(rt, &path_str, &inner, flags, mode);
                }
            }
        }
    }
    unsafe { get_real_open64()(path, flags, mode) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn openat(
    dirfd: i32,
    path: *const libc::c_char,
    flags: i32,
    mode: libc::mode_t,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = resolve_at_path(rt, dirfd, &path_str) {
                    return do_open(rt, &path_str, &inner, flags, mode);
                }
            }
        }
    }
    unsafe { get_real_openat()(dirfd, path, flags, mode) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn close(fd: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if rt.fd_table.is_clawfs_fd(fd) {
                log::trace!("close(clawfs fd={})", fd);
                if fd_proxy_threads().contains_key(&fd) {
                    rt.fd_table.remove(fd);
                    cleanup_fd_proxy(fd);
                    return 0;
                }
                let result = dispatch::dispatch_close(rt, fd);
                // Also close the underlying kernel fd (if it's a real fd).
                // Synthetic fds (>= FD_BASE) don't have a kernel fd to close.
                if fd < crate::fd_table::FD_BASE {
                    unsafe { get_real_close()(fd) };
                }
                return match result {
                    Ok(()) => 0,
                    Err(e) => set_errno(e) as i32,
                };
            }
            // Close the write end of the pipe when the inotify read end is closed.
            if let Some(write_fd) = crate::inotify::close_instance(fd) {
                unsafe { get_real_close()(write_fd) };
                // Fall through to also close the read end (fd itself).
            }
        }
    }
    unsafe { get_real_close()(fd) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn read(
    fd: i32,
    buf: *mut libc::c_void,
    count: libc::size_t,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                let slice = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, count) };
                let result = dispatch::dispatch_read(rt, &entry, slice);
                log::trace!(
                    "read(clawfs fd={}, count={}) -> {:?}",
                    fd,
                    count,
                    result.as_ref().map(|n| *n).map_err(|e| *e)
                );
                return match result {
                    Ok(n) => n as libc::ssize_t,
                    Err(e) => set_errno(e) as libc::ssize_t,
                };
            }
        }
    }
    unsafe { get_real_read()(fd, buf, count) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn write(
    fd: i32,
    buf: *const libc::c_void,
    count: libc::size_t,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(result) = dispatch_write_fd(rt, fd, buf, count, "write") {
                return result;
            }
        }
    }
    unsafe { get_real_write()(fd, buf, count) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn __write(
    fd: i32,
    buf: *const libc::c_void,
    count: libc::size_t,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(result) = dispatch_write_fd(rt, fd, buf, count, "__write") {
                return result;
            }
        }
    }
    unsafe { get_real___write()(fd, buf, count) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn __write_nocancel(
    fd: i32,
    buf: *const libc::c_void,
    count: libc::size_t,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(result) = dispatch_write_fd(rt, fd, buf, count, "__write_nocancel") {
                return result;
            }
        }
    }
    unsafe { get_real___write_nocancel()(fd, buf, count) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn writev(
    fd: i32,
    iov: *const libc::iovec,
    iovcnt: libc::c_int,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(result) = dispatch_writev_fd(rt, fd, iov, iovcnt, "writev") {
                return result;
            }
        }
    }
    unsafe { get_real_writev()(fd, iov, iovcnt) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fwrite(
    ptr: *const libc::c_void,
    size: libc::size_t,
    nmemb: libc::size_t,
    stream: *mut libc::FILE,
) -> libc::size_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let total = size.saturating_mul(nmemb);
            let data = unsafe { std::slice::from_raw_parts(ptr as *const u8, total) };
            if let Some(written) = dispatch_stdio_write(rt, stream, data) {
                return if size == 0 { nmemb } else { written / size };
            }
        }
    }
    unsafe { get_real_fwrite()(ptr, size, nmemb, stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fwrite_unlocked(
    ptr: *const libc::c_void,
    size: libc::size_t,
    nmemb: libc::size_t,
    stream: *mut libc::FILE,
) -> libc::size_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let total = size.saturating_mul(nmemb);
            let data = unsafe { std::slice::from_raw_parts(ptr as *const u8, total) };
            if let Some(written) = dispatch_stdio_write(rt, stream, data) {
                return if size == 0 { nmemb } else { written / size };
            }
        }
    }
    unsafe { get_real_fwrite_unlocked()(ptr, size, nmemb, stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fputs(s: *const libc::c_char, stream: *mut libc::FILE) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let cstr = unsafe { CStr::from_ptr(s) };
            if dispatch_stdio_write(rt, stream, cstr.to_bytes()).is_some() {
                return 1;
            }
        }
    }
    unsafe { get_real_fputs()(s, stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fputc(c: i32, stream: *mut libc::FILE) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let byte = [c as u8];
            if dispatch_stdio_write(rt, stream, &byte).is_some() {
                return c;
            }
        }
    }
    unsafe { get_real_fputc()(c, stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn putc(c: i32, stream: *mut libc::FILE) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let byte = [c as u8];
            if dispatch_stdio_write(rt, stream, &byte).is_some() {
                return c;
            }
        }
    }
    unsafe { get_real_putc()(c, stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn putchar(c: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let byte = [c as u8];
            if dispatch_write_fd(
                rt,
                libc::STDOUT_FILENO,
                byte.as_ptr() as *const libc::c_void,
                byte.len(),
                "putchar",
            )
            .is_some()
            {
                return c;
            }
        }
    }
    unsafe { get_real_putchar()(c) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn puts(s: *const libc::c_char) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let cstr = unsafe { CStr::from_ptr(s) };
            if dispatch_write_fd(
                rt,
                libc::STDOUT_FILENO,
                cstr.to_bytes().as_ptr() as *const libc::c_void,
                cstr.to_bytes().len(),
                "puts",
            )
            .is_some()
                && dispatch_write_fd(
                    rt,
                    libc::STDOUT_FILENO,
                    b"\n".as_ptr() as *const libc::c_void,
                    1,
                    "puts",
                )
                .is_some()
            {
                return 1;
            }
        }
    }
    unsafe { get_real_puts()(s) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lseek(fd: i32, offset: libc::off_t, whence: i32) -> libc::off_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                let result = dispatch::dispatch_lseek(rt, &entry, offset, whence);
                log::trace!(
                    "lseek(clawfs fd={}, offset={}, whence={}) -> {:?}",
                    fd,
                    offset,
                    whence,
                    result
                );
                return match result {
                    Ok(pos) => pos as libc::off_t,
                    Err(e) => set_errno(e) as libc::off_t,
                };
            }
        }
    }
    unsafe { get_real_lseek()(fd, offset, whence) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lseek64(fd: i32, offset: libc::off64_t, whence: i32) -> libc::off64_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                let result = dispatch::dispatch_lseek(rt, &entry, offset, whence);
                log::trace!(
                    "lseek64(clawfs fd={}, offset={}, whence={}) -> {:?}",
                    fd,
                    offset,
                    whence,
                    result
                );
                return match result {
                    Ok(pos) => pos as libc::off64_t,
                    Err(e) => set_errno(e) as libc::off64_t,
                };
            }
        }
    }
    unsafe { get_real_lseek64()(fd, offset, whence) }
}

// ---------------------------------------------------------------------------
// stat family
// ---------------------------------------------------------------------------

/// Helper for stat/lstat — shared logic.
fn do_stat(rt: &ClawfsRuntime, path: &str, buf: *mut libc::stat) -> Option<i32> {
    let inner = try_classify(rt, path)?;
    Some(match dispatch::dispatch_stat(rt, &inner) {
        Ok(st) => {
            unsafe { *buf = st };
            0
        }
        Err(e) => set_errno(e) as i32,
    })
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stat(path: *const libc::c_char, buf: *mut libc::stat) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(result) = do_stat(rt, &path_str, buf) {
                    return result;
                }
            }
        }
    }
    unsafe { get_real_stat()(path, buf) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lstat(path: *const libc::c_char, buf: *mut libc::stat) -> i32 {
    // Phase 1: lstat == stat (no symlink distinction).
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(result) = do_stat(rt, &path_str, buf) {
                    return result;
                }
            }
        }
    }
    unsafe { get_real_lstat()(path, buf) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fstat(fd: i32, buf: *mut libc::stat) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_fstat(rt, &entry) {
                    Ok(st) => {
                        log::trace!("fstat(clawfs fd={}) -> size={}", fd, st.st_size);
                        unsafe { *buf = st };
                        0
                    }
                    Err(e) => {
                        log::trace!("fstat(clawfs fd={}) -> err={}", fd, e);
                        set_errno(e) as i32
                    }
                };
            }
        }
    }
    unsafe { get_real_fstat()(fd, buf) }
}

// On some glibc versions, stat/lstat/fstat are actually implemented via
// __xstat/__lxstat/__fxstat. We hook both to cover all cases.

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn __xstat(ver: i32, path: *const libc::c_char, buf: *mut libc::stat) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(result) = do_stat(rt, &path_str, buf) {
                    return result;
                }
            }
        }
    }
    unsafe { get_real___xstat()(ver, path, buf) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn __lxstat(
    ver: i32,
    path: *const libc::c_char,
    buf: *mut libc::stat,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(result) = do_stat(rt, &path_str, buf) {
                    return result;
                }
            }
        }
    }
    unsafe { get_real___lxstat()(ver, path, buf) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn __fxstat(ver: i32, fd: i32, buf: *mut libc::stat) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_fstat(rt, &entry) {
                    Ok(st) => {
                        unsafe { *buf = st };
                        0
                    }
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real___fxstat()(ver, fd, buf) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
/// `fstatat` / `newfstatat` — used by modern glibc and Python's os.stat().
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fstatat(
    dirfd: i32,
    path: *const libc::c_char,
    buf: *mut libc::stat,
    flags: i32,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = resolve_at_path(rt, dirfd, &path_str) {
                    return match dispatch::dispatch_stat(rt, &inner) {
                        Ok(st) => {
                            unsafe { *buf = st };
                            0
                        }
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_fstatat()(dirfd, path, buf, flags) }
}

/// # Safety
/// Alias for `fstatat` on glibc systems where `__fstatat` is the internal name.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn __fstatat(
    dirfd: i32,
    path: *const libc::c_char,
    buf: *mut libc::stat,
    flags: i32,
) -> i32 {
    fstatat(dirfd, path, buf, flags)
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
/// `fstatat64` — used by Python and other applications compiled with _FILE_OFFSET_BITS=64.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fstatat64(
    dirfd: i32,
    path: *const libc::c_char,
    buf: *mut libc::stat,
    flags: i32,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = resolve_at_path(rt, dirfd, &path_str) {
                    return match dispatch::dispatch_stat(rt, &inner) {
                        Ok(st) => {
                            unsafe { *buf = st };
                            0
                        }
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_fstatat64()(dirfd, path, buf, flags) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn stat64(path: *const libc::c_char, buf: *mut libc::stat) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(result) = do_stat(rt, &path_str, buf) {
                    return result;
                }
            }
        }
    }
    unsafe { get_real_stat64()(path, buf) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lstat64(path: *const libc::c_char, buf: *mut libc::stat) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(result) = do_stat(rt, &path_str, buf) {
                    return result;
                }
            }
        }
    }
    unsafe { get_real_lstat64()(path, buf) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fstat64(fd: i32, buf: *mut libc::stat) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_fstat(rt, &entry) {
                    Ok(st) => {
                        unsafe { *buf = st };
                        0
                    }
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real_fstat64()(fd, buf) }
}

// ---------------------------------------------------------------------------
// Directory / namespace operations
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn access(path: *const libc::c_char, mode: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    return match dispatch::dispatch_access(rt, &inner, mode) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_access()(path, mode) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
///
/// `euidaccess` checks permissions using the effective UID/GID.
/// For ClawFS, we treat it identically to `access`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn euidaccess(path: *const libc::c_char, mode: i32) -> i32 {
    // Delegate to our access implementation which already handles ClawFS paths.
    unsafe { access(path, mode) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn eaccess(path: *const libc::c_char, mode: i32) -> i32 {
    unsafe { access(path, mode) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mkdir(path: *const libc::c_char, mode: libc::mode_t) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    return match dispatch::dispatch_mkdir(rt, &inner) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_mkdir()(path, mode) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn unlink(path: *const libc::c_char) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    return match dispatch::dispatch_unlink(rt, &inner) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_unlink()(path) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rmdir(path: *const libc::c_char) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    return match dispatch::dispatch_rmdir(rt, &inner) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_rmdir()(path) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rename(oldpath: *const libc::c_char, newpath: *const libc::c_char) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let (Some(old_str), Some(new_str)) = (c_path_to_str(oldpath), c_path_to_str(newpath))
            {
                let old_inner = try_classify(rt, &old_str);
                let new_inner = try_classify(rt, &new_str);
                match (old_inner, new_inner) {
                    (Some(oi), Some(ni)) => {
                        return match dispatch::dispatch_rename(rt, &oi, &ni) {
                            Ok(()) => 0,
                            Err(e) => set_errno(e) as i32,
                        };
                    }
                    // Cross-boundary rename: one side ClawFS, other side host.
                    (Some(_), None) | (None, Some(_)) => {
                        return set_errno(libc::EXDEV) as i32;
                    }
                    (None, None) => {}
                }
            }
        }
    }
    unsafe { get_real_rename()(oldpath, newpath) }
}

// ---------------------------------------------------------------------------
// Phase 2: pread/pwrite
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pread(
    fd: i32,
    buf: *mut libc::c_void,
    count: libc::size_t,
    offset: libc::off_t,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                let slice = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, count) };
                return match dispatch::dispatch_pread(rt, &entry, slice, offset) {
                    Ok(n) => n as libc::ssize_t,
                    Err(e) => set_errno(e) as libc::ssize_t,
                };
            }
        }
    }
    unsafe { get_real_pread()(fd, buf, count, offset) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pread64(
    fd: i32,
    buf: *mut libc::c_void,
    count: libc::size_t,
    offset: libc::off64_t,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                let slice = unsafe { std::slice::from_raw_parts_mut(buf as *mut u8, count) };
                return match dispatch::dispatch_pread(rt, &entry, slice, offset) {
                    Ok(n) => n as libc::ssize_t,
                    Err(e) => set_errno(e) as libc::ssize_t,
                };
            }
        }
    }
    unsafe { get_real_pread64()(fd, buf, count, offset) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pwrite(
    fd: i32,
    buf: *const libc::c_void,
    count: libc::size_t,
    offset: libc::off_t,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                let slice = unsafe { std::slice::from_raw_parts(buf as *const u8, count) };
                return match dispatch::dispatch_pwrite(rt, &entry, slice, offset) {
                    Ok(n) => n as libc::ssize_t,
                    Err(e) => set_errno(e) as libc::ssize_t,
                };
            }
        }
    }
    unsafe { get_real_pwrite()(fd, buf, count, offset) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pwrite64(
    fd: i32,
    buf: *const libc::c_void,
    count: libc::size_t,
    offset: libc::off64_t,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                let slice = unsafe { std::slice::from_raw_parts(buf as *const u8, count) };
                return match dispatch::dispatch_pwrite(rt, &entry, slice, offset) {
                    Ok(n) => n as libc::ssize_t,
                    Err(e) => set_errno(e) as libc::ssize_t,
                };
            }
        }
    }
    unsafe { get_real_pwrite64()(fd, buf, count, offset) }
}

// ---------------------------------------------------------------------------
// Phase 2: truncate/ftruncate
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn truncate(path: *const libc::c_char, length: libc::off_t) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    return match dispatch::dispatch_truncate(rt, &inner, length) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_truncate()(path, length) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn truncate64(path: *const libc::c_char, length: libc::off64_t) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    return match dispatch::dispatch_truncate(rt, &inner, length) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_truncate64()(path, length) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ftruncate(fd: i32, length: libc::off_t) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_ftruncate(rt, &entry, length) {
                    Ok(()) => 0,
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real_ftruncate()(fd, length) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ftruncate64(fd: i32, length: libc::off64_t) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_ftruncate(rt, &entry, length) {
                    Ok(()) => 0,
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real_ftruncate64()(fd, length) }
}

// ---------------------------------------------------------------------------
// Phase 2: fsync/fdatasync
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fsync(fd: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_fsync(rt, &entry) {
                    Ok(()) => 0,
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real_fsync()(fd) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fdatasync(fd: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_fsync(rt, &entry) {
                    Ok(()) => 0,
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real_fdatasync()(fd) }
}

// ---------------------------------------------------------------------------
// Phase 2: dup/dup2/dup3
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dup(oldfd: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if rt.fd_table.is_clawfs_fd(oldfd) {
                if oldfd < crate::fd_table::FD_BASE {
                    // Kernel fd: dup the real fd, then map the new fd in fd_table.
                    let new_fd = unsafe { get_real_dup()(oldfd) };
                    if new_fd < 0 {
                        return new_fd;
                    }
                    rt.fd_table.dup_to(oldfd, new_fd);
                    return new_fd;
                }
                // Synthetic fd: use fd_table.dup (old behavior).
                return match rt.fd_table.dup(oldfd) {
                    Some(new_fd) => new_fd,
                    None => set_errno(libc::EBADF) as i32,
                };
            }
        }
    }
    unsafe { get_real_dup()(oldfd) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dup2(oldfd: i32, newfd: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if rt.fd_table.is_clawfs_fd(oldfd) {
                log::trace!("dup2(clawfs fd={oldfd} -> fd={newfd})");
                cleanup_fd_proxy(newfd);
                // For kernel fds, also perform the real dup2 so newfd is valid.
                if oldfd < crate::fd_table::FD_BASE {
                    let ret = unsafe { get_real_dup2()(oldfd, newfd) };
                    if ret < 0 {
                        return ret;
                    }
                }
                return match rt.fd_table.dup_to(oldfd, newfd) {
                    Some(fd) => {
                        if let Some(entry) = rt.fd_table.get(fd) {
                            if entry.flags & (libc::O_WRONLY | libc::O_RDWR) != 0
                                && !entry.is_dir
                                && install_fd_proxy(rt, fd, entry, false).is_err()
                            {
                                rt.fd_table.remove(fd);
                                return set_errno(libc::EBADF) as i32;
                            }
                        }
                        fd
                    }
                    None => set_errno(libc::EBADF) as i32,
                };
            }
            // If newfd is a ClawFS fd but oldfd isn't, close the ClawFS fd first.
            if rt.fd_table.is_clawfs_fd(newfd) {
                log::trace!("dup2(host fd={oldfd} -> clawfs fd={newfd}), finalizing clawfs fd");
                if fd_proxy_threads().contains_key(&newfd) {
                    if let Some(stream) = get_stdio_stream(newfd) {
                        unsafe {
                            let _ = get_real_fflush()(stream);
                        }
                    }
                    rt.fd_table.remove(newfd);
                    cleanup_fd_proxy(newfd);
                } else {
                    let result = dispatch::dispatch_close(rt, newfd);
                    log::trace!("dispatch_close(clawfs fd={newfd}) -> {result:?}");
                }
            }
        }
    }
    unsafe { get_real_dup2()(oldfd, newfd) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dup3(oldfd: i32, newfd: i32, flags: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if rt.fd_table.is_clawfs_fd(oldfd) {
                if oldfd == newfd {
                    return set_errno(libc::EINVAL) as i32;
                }
                log::trace!("dup3(clawfs fd={oldfd} -> fd={newfd}, flags={flags})");
                cleanup_fd_proxy(newfd);
                // For kernel fds, also perform the real dup3 so newfd is valid.
                if oldfd < crate::fd_table::FD_BASE {
                    let ret = unsafe { get_real_dup3()(oldfd, newfd, flags) };
                    if ret < 0 {
                        return ret;
                    }
                }
                return match rt.fd_table.dup_to(oldfd, newfd) {
                    Some(fd) => {
                        if let Some(entry) = rt.fd_table.get(fd) {
                            if entry.flags & (libc::O_WRONLY | libc::O_RDWR) != 0
                                && !entry.is_dir
                                && install_fd_proxy(rt, fd, entry, flags & libc::O_CLOEXEC != 0)
                                    .is_err()
                            {
                                rt.fd_table.remove(fd);
                                return set_errno(libc::EBADF) as i32;
                            }
                        }
                        fd
                    }
                    None => set_errno(libc::EBADF) as i32,
                };
            }
            if rt.fd_table.is_clawfs_fd(newfd) {
                log::trace!("dup3(host fd={oldfd} -> clawfs fd={newfd}), finalizing clawfs fd");
                if fd_proxy_threads().contains_key(&newfd) {
                    if let Some(stream) = get_stdio_stream(newfd) {
                        unsafe {
                            let _ = get_real_fflush()(stream);
                        }
                    }
                    rt.fd_table.remove(newfd);
                    cleanup_fd_proxy(newfd);
                } else {
                    let result = dispatch::dispatch_close(rt, newfd);
                    log::trace!("dispatch_close(clawfs fd={newfd}) -> {result:?}");
                }
            }
        }
    }
    unsafe { get_real_dup3()(oldfd, newfd, flags) }
}

// ---------------------------------------------------------------------------
// Phase 2: symlink/readlink
// ---------------------------------------------------------------------------

/// Parse `/proc/self/fd/<N>` or `/proc/<pid>/fd/<N>` and return the fd number.
fn parse_proc_fd_path(path: &str) -> Option<i32> {
    let rest = path.strip_prefix("/proc/")?;
    let after_slash = if let Some(r) = rest.strip_prefix("self/fd/") {
        r
    } else {
        let slash = rest.find('/')?;
        rest[slash + 1..].strip_prefix("fd/")?
    };
    // Only a bare integer — no trailing components.
    if after_slash.is_empty() || after_slash.contains('/') {
        return None;
    }
    after_slash.parse().ok()
}

/// Write a string into a C char buffer, returning the byte count (capped at bufsiz).
fn write_str_to_buf(s: &str, buf: *mut libc::c_char, bufsiz: libc::size_t) -> libc::ssize_t {
    let bytes = s.as_bytes();
    let n = bytes.len().min(bufsiz);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, n);
    }
    n as libc::ssize_t
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn symlink(
    target: *const libc::c_char,
    linkpath: *const libc::c_char,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let (Some(target_str), Some(link_str)) =
                (c_path_to_str(target), c_path_to_str(linkpath))
            {
                if let Some(inner) = try_classify(rt, &link_str) {
                    return match dispatch::dispatch_symlink(rt, &target_str, &inner) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_symlink()(target, linkpath) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn readlink(
    path: *const libc::c_char,
    buf: *mut libc::c_char,
    bufsiz: libc::size_t,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                // /proc/self/fd/<N> — return the full external path for ClawFS fds.
                if let Some(fd) = parse_proc_fd_path(&path_str) {
                    if let Some(entry) = rt.fd_table.get(fd) {
                        if let Some(full) = rt.prefix_router.full_path(&entry.inner_path) {
                            log::trace!(
                                "readlink({:?}) -> {:?} (clawfs fd={})",
                                path_str,
                                full,
                                fd
                            );
                            return write_str_to_buf(&full, buf, bufsiz);
                        }
                    }
                }
                // ClawFS symlinks.
                if let Some(inner) = try_classify(rt, &path_str) {
                    return match dispatch::dispatch_readlink(rt, &inner) {
                        Ok(target) => {
                            let n = target.len().min(bufsiz);
                            unsafe {
                                std::ptr::copy_nonoverlapping(target.as_ptr(), buf as *mut u8, n);
                            }
                            n as libc::ssize_t
                        }
                        Err(e) => set_errno(e) as libc::ssize_t,
                    };
                }
            }
        }
    }
    unsafe { get_real_readlink()(path, buf, bufsiz) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn readlinkat(
    dirfd: i32,
    path: *const libc::c_char,
    buf: *mut libc::c_char,
    bufsiz: libc::size_t,
) -> libc::ssize_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                // /proc/self/fd/<N> — always absolute, dirfd is irrelevant.
                if let Some(fd) = parse_proc_fd_path(&path_str) {
                    if let Some(entry) = rt.fd_table.get(fd) {
                        if let Some(full) = rt.prefix_router.full_path(&entry.inner_path) {
                            log::trace!(
                                "readlinkat({}, {:?}) -> {:?} (clawfs fd={})",
                                dirfd,
                                path_str,
                                full,
                                fd
                            );
                            return write_str_to_buf(&full, buf, bufsiz);
                        }
                    }
                }
                // ClawFS symlinks.
                if let Some(inner) = resolve_at_path(rt, dirfd, &path_str) {
                    return match dispatch::dispatch_readlink(rt, &inner) {
                        Ok(target) => {
                            let n = target.len().min(bufsiz);
                            unsafe {
                                std::ptr::copy_nonoverlapping(target.as_ptr(), buf as *mut u8, n);
                            }
                            n as libc::ssize_t
                        }
                        Err(e) => set_errno(e) as libc::ssize_t,
                    };
                }
            }
        }
    }
    unsafe { get_real_readlinkat()(dirfd, path, buf, bufsiz) }
}

// ---------------------------------------------------------------------------
// Phase 2: chdir/getcwd
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn chdir(path: *const libc::c_char) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    let full = rt
                        .prefix_router
                        .full_path(&inner)
                        .unwrap_or_else(|| path_str.to_string());
                    return match dispatch::dispatch_chdir(rt, &full, &inner) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
                // Changing to a host path — reset virtual cwd.
                rt.cwd.set_host();
            }
        }
    }
    unsafe { get_real_chdir()(path) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn getcwd(buf: *mut libc::c_char, size: libc::size_t) -> *mut libc::c_char {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some((full_path, _, _)) = rt.cwd.get_clawfs() {
                let bytes = full_path.as_bytes();
                if bytes.len() + 1 > size {
                    unsafe { *libc::__errno_location() = libc::ERANGE };
                    return std::ptr::null_mut();
                }
                unsafe {
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, bytes.len());
                    *buf.add(bytes.len()) = 0; // null-terminate
                }
                return buf;
            }
        }
    }
    unsafe { get_real_getcwd()(buf, size) }
}

// ---------------------------------------------------------------------------
// Phase 2: opendir/readdir/readdir64/closedir (DIR* emulation)
// ---------------------------------------------------------------------------

// We use the synthetic fd number cast to a DIR* pointer for tracking.
// This is safe because DIR* is opaque and our synthetic fds are in a range
// that won't collide with real heap pointers.

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendir(name: *const libc::c_char) -> *mut libc::DIR {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(name) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    match dispatch::dispatch_open(rt, &inner, libc::O_RDONLY, 0) {
                        Ok(fd) => {
                            // Pre-fill directory entries.
                            if let Some(entry) = rt.fd_table.get(fd) {
                                let _ = dispatch::dispatch_readdir_fill(rt, &entry);
                            }
                            return fd as usize as *mut libc::DIR;
                        }
                        Err(e) => {
                            return crate::errno::set_errno_null(e);
                        }
                    }
                }
            }
        }
    }
    unsafe { get_real_opendir()(name) }
}

/// Get the fd from a FILE* without locking the stream.
/// Uses `fileno_unlocked` to avoid deadlocking when called from `_unlocked`
/// stdio variants (e.g. `fread_unlocked`) where the caller already holds the
/// stream lock.
unsafe fn stream_fd(stream: *mut libc::FILE) -> i32 {
    unsafe { get_real_fileno_unlocked()(stream) }
}

fn is_synthetic_dir(dirp: *mut libc::DIR) -> bool {
    let val = dirp as usize;
    // Our synthetic fds start at 10_000_000. Real DIR* pointers are heap
    // addresses which on 64-bit are much larger (typically > 0x1_0000_0000).
    (10_000_000..100_000_000).contains(&val)
}

// Thread-local storage for readdir results (dirent struct must live until next call).
thread_local! {
    static READDIR_BUF: std::cell::RefCell<libc::dirent> = const { std::cell::RefCell::new(unsafe { std::mem::zeroed() }) };
    static READDIR64_BUF: std::cell::RefCell<libc::dirent64> = const { std::cell::RefCell::new(unsafe { std::mem::zeroed() }) };
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn readdir(dirp: *mut libc::DIR) -> *mut libc::dirent {
    if is_synthetic_dir(dirp) {
        if let Some(_guard) = ReentrancyGuard::enter() {
            if let Some(rt) = ClawfsRuntime::get() {
                let fd = dirp as usize as i32;
                if let Some(entry) = rt.fd_table.get(fd) {
                    let mut dir = entry.dir_entries.lock();
                    if let Some((entries, ref mut cursor)) = dir.as_mut() {
                        if *cursor >= entries.len() {
                            return std::ptr::null_mut(); // end of directory
                        }
                        let (ino, d_type, ref name) = entries[*cursor];
                        let off = (*cursor + 1) as i64;
                        *cursor += 1;
                        return READDIR_BUF.with(|buf| {
                            let mut de = buf.borrow_mut();
                            *de = unsafe { std::mem::zeroed() };
                            de.d_ino = ino as libc::ino_t;
                            de.d_off = off as libc::off_t;
                            de.d_type = d_type;
                            let name_bytes = name.as_bytes();
                            let n = name_bytes.len().min(255);
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    name_bytes.as_ptr(),
                                    de.d_name.as_mut_ptr() as *mut u8,
                                    n,
                                );
                                de.d_name[n] = 0;
                            }
                            &mut *de as *mut libc::dirent
                        });
                    }
                    // No entries cached — return end.
                    return std::ptr::null_mut();
                }
            }
        }
    }
    unsafe { get_real_readdir()(dirp) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn readdir64(dirp: *mut libc::DIR) -> *mut libc::dirent64 {
    if is_synthetic_dir(dirp) {
        if let Some(_guard) = ReentrancyGuard::enter() {
            if let Some(rt) = ClawfsRuntime::get() {
                let fd = dirp as usize as i32;
                if let Some(entry) = rt.fd_table.get(fd) {
                    let mut dir = entry.dir_entries.lock();
                    if let Some((entries, ref mut cursor)) = dir.as_mut() {
                        if *cursor >= entries.len() {
                            return std::ptr::null_mut();
                        }
                        let (ino, d_type, ref name) = entries[*cursor];
                        let off = (*cursor + 1) as i64;
                        *cursor += 1;
                        return READDIR64_BUF.with(|buf| {
                            let mut de = buf.borrow_mut();
                            *de = unsafe { std::mem::zeroed() };
                            de.d_ino = ino as libc::ino64_t;
                            de.d_off = off as libc::off64_t;
                            de.d_type = d_type;
                            let name_bytes = name.as_bytes();
                            let n = name_bytes.len().min(255);
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    name_bytes.as_ptr(),
                                    de.d_name.as_mut_ptr() as *mut u8,
                                    n,
                                );
                                de.d_name[n] = 0;
                            }
                            &mut *de as *mut libc::dirent64
                        });
                    }
                    return std::ptr::null_mut();
                }
            }
        }
    }
    unsafe { get_real_readdir64()(dirp) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn closedir(dirp: *mut libc::DIR) -> i32 {
    if is_synthetic_dir(dirp) {
        if let Some(_guard) = ReentrancyGuard::enter() {
            if let Some(rt) = ClawfsRuntime::get() {
                let fd = dirp as usize as i32;
                return match dispatch::dispatch_close(rt, fd) {
                    Ok(()) => {
                        if fd < crate::fd_table::FD_BASE {
                            unsafe {
                                get_real_close()(fd);
                            }
                        }
                        0
                    }
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real_closedir()(dirp) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dirfd(dirp: *mut libc::DIR) -> i32 {
    if is_synthetic_dir(dirp) {
        return dirp as usize as i32;
    }
    unsafe { get_real_dirfd()(dirp) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fdopendir(fd: i32) -> *mut libc::DIR {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if rt.fd_table.is_clawfs_fd(fd) {
                if let Some(entry) = rt.fd_table.get(fd) {
                    if !entry.is_dir {
                        set_errno(libc::ENOTDIR);
                        return std::ptr::null_mut();
                    }
                    let _ = dispatch::dispatch_readdir_fill(rt, &entry);
                }
                return fd as usize as *mut libc::DIR;
            }
        }
    }
    unsafe { get_real_fdopendir()(fd) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn seekdir(dirp: *mut libc::DIR, loc: libc::c_long) {
    if is_synthetic_dir(dirp) {
        if let Some(_guard) = ReentrancyGuard::enter() {
            if let Some(rt) = ClawfsRuntime::get() {
                let fd = dirp as usize as i32;
                if let Some(entry) = rt.fd_table.get(fd) {
                    let mut dir = entry.dir_entries.lock();
                    if let Some((_, ref mut cursor)) = dir.as_mut() {
                        *cursor = loc as usize;
                    }
                }
            }
        }
        return;
    }
    unsafe { get_real_seekdir()(dirp, loc) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn telldir(dirp: *mut libc::DIR) -> libc::c_long {
    if is_synthetic_dir(dirp) {
        if let Some(_guard) = ReentrancyGuard::enter() {
            if let Some(rt) = ClawfsRuntime::get() {
                let fd = dirp as usize as i32;
                if let Some(entry) = rt.fd_table.get(fd) {
                    let dir = entry.dir_entries.lock();
                    if let Some((_, cursor)) = dir.as_ref() {
                        return *cursor as libc::c_long;
                    }
                }
            }
        }
        return 0;
    }
    unsafe { get_real_telldir()(dirp) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rewinddir(dirp: *mut libc::DIR) {
    if is_synthetic_dir(dirp) {
        if let Some(_guard) = ReentrancyGuard::enter() {
            if let Some(rt) = ClawfsRuntime::get() {
                let fd = dirp as usize as i32;
                if let Some(entry) = rt.fd_table.get(fd) {
                    let mut dir = entry.dir_entries.lock();
                    if let Some((_, ref mut cursor)) = dir.as_mut() {
                        *cursor = 0;
                    }
                }
            }
        }
        return;
    }
    unsafe { get_real_rewinddir()(dirp) }
}

// ---------------------------------------------------------------------------
// creat / creat64
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn creat(path: *const libc::c_char, mode: libc::mode_t) -> i32 {
    // creat() is equivalent to open(path, O_CREAT|O_WRONLY|O_TRUNC, mode)
    open(path, libc::O_CREAT | libc::O_WRONLY | libc::O_TRUNC, mode)
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn creat64(path: *const libc::c_char, mode: libc::mode_t) -> i32 {
    open64(path, libc::O_CREAT | libc::O_WRONLY | libc::O_TRUNC, mode)
}

// ---------------------------------------------------------------------------
// faccessat / symlinkat
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn faccessat(
    dirfd: i32,
    path: *const libc::c_char,
    mode: i32,
    _flags: i32,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = resolve_at_path(rt, dirfd, &path_str) {
                    return match dispatch::dispatch_access(rt, &inner, mode) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_faccessat()(dirfd, path, mode, _flags) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn symlinkat(
    target: *const libc::c_char,
    newdirfd: i32,
    linkpath: *const libc::c_char,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let (Some(target_str), Some(link_str)) =
                (c_path_to_str(target), c_path_to_str(linkpath))
            {
                if let Some(inner) = resolve_at_path(rt, newdirfd, &link_str) {
                    return match dispatch::dispatch_symlink(rt, &target_str, &inner) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_symlinkat()(target, newdirfd, linkpath) }
}

// ---------------------------------------------------------------------------
// stdio FILE* interceptions: fopen, fclose, fread, fgets, fgetc, fseek, ftell
// ---------------------------------------------------------------------------
//
// Strategy: fopen() on a ClawFS path opens /dev/null to obtain a real kernel fd,
// maps that fd to the ClawFS FdEntry in fd_table, then calls real fdopen() on it.
// All subsequent stdio functions check fileno(stream) against fd_table to decide
// whether to dispatch to ClawFS or delegate to the real libc implementation.
// The kernel fd (/dev/null) is never actually read from or written to — every
// I/O operation is intercepted before reaching the kernel.

/// Parse a C fopen mode string into open() flags.
fn fopen_mode_to_flags(mode: &str) -> i32 {
    // Strip 'b' (binary, no effect on Linux), 'e' (O_CLOEXEC), 'x' (O_EXCL).
    let base: String = mode
        .chars()
        .filter(|c| !matches!(c, 'b' | 'e' | 'x'))
        .collect();
    let flags = match base.as_str() {
        "r" => libc::O_RDONLY,
        "r+" => libc::O_RDWR,
        "w" => libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
        "w+" => libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC,
        "a" => libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND,
        "a+" => libc::O_RDWR | libc::O_CREAT | libc::O_APPEND,
        _ => return -1,
    };
    let mut extra = 0;
    if mode.contains('e') {
        extra |= libc::O_CLOEXEC;
    }
    if mode.contains('x') {
        extra |= libc::O_EXCL;
    }
    flags | extra
}

/// Core fopen implementation shared by fopen and fopen64.
///
/// Strategy: open the ClawFS path to get a synthetic fd, then open /dev/null
/// to get a real kernel fd, map the ClawFS entry onto the kernel fd, and use
/// `fdopen()` to wrap the kernel fd into a `FILE*`. All subsequent stdio
/// operations (fread, fwrite, fclose, etc.) call `fileno(stream)` to get the
/// kernel fd, which we intercept via the fd_table.
unsafe fn do_fopen(
    path: *const libc::c_char,
    mode: *const libc::c_char,
    real_fn: unsafe extern "C" fn(*const libc::c_char, *const libc::c_char) -> *mut libc::FILE,
) -> *mut libc::FILE {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let (Some(path_str), Some(mode_str)) = (c_path_to_str(path), c_path_to_str(mode)) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    let flags = fopen_mode_to_flags(&mode_str);
                    if flags == -1 {
                        set_errno(libc::EINVAL);
                        return std::ptr::null_mut();
                    }
                    // Open in ClawFS to get a synthetic fd + FdEntry.
                    let synth_fd = match dispatch::dispatch_open(rt, &inner, flags, 0o644) {
                        Ok(fd) => fd,
                        Err(e) => {
                            set_errno(e);
                            return std::ptr::null_mut();
                        }
                    };
                    // Get a real kernel fd by opening /dev/null.
                    let devnull = CString::new("/dev/null").unwrap();
                    let kernel_fd = unsafe { get_real_open()(devnull.as_ptr(), libc::O_RDWR, 0) };
                    if kernel_fd < 0 {
                        let _ = dispatch::dispatch_close(rt, synth_fd);
                        return std::ptr::null_mut();
                    }
                    // Map the kernel fd to the same FdEntry as synth_fd.
                    rt.fd_table.dup_to(synth_fd, kernel_fd);
                    // Remove the synthetic fd — we only need the kernel fd now.
                    rt.fd_table.remove(synth_fd);
                    // Use fdopen to wrap the kernel fd into a FILE*.
                    let mode_cstr = CString::new(mode_str.as_str()).unwrap();
                    let stream = unsafe { get_real_fdopen()(kernel_fd, mode_cstr.as_ptr()) };
                    if stream.is_null() {
                        rt.fd_table.remove(kernel_fd);
                        unsafe { get_real_close()(kernel_fd) };
                        return std::ptr::null_mut();
                    }
                    log::trace!(
                        "fopen({:?}, {:?}) -> FILE*(fd={})",
                        path_str,
                        mode_str,
                        unsafe { stream_fd(stream) }
                    );
                    return stream;
                }
            }
        }
    }
    unsafe { real_fn(path, mode) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fopen(
    path: *const libc::c_char,
    mode: *const libc::c_char,
) -> *mut libc::FILE {
    unsafe { do_fopen(path, mode, get_real_fopen()) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fopen64(
    path: *const libc::c_char,
    mode: *const libc::c_char,
) -> *mut libc::FILE {
    unsafe { do_fopen(path, mode, get_real_fopen64()) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
///
/// Intercept `fdopen` so that programs calling `open()` + `fdopen()` (like
/// coreutils `sort`) can wrap a synthetic ClawFS fd in a `FILE*`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fdopen(fd: i32, mode: *const libc::c_char) -> *mut libc::FILE {
    // The fd is already a real kernel fd (backed by /dev/null from our open() intercept).
    // Just wrap it directly — no need for a second fd.
    let stream = unsafe { get_real_fdopen()(fd, mode) };
    if !stream.is_null() {
        // Verify fileno matches what we expect.
        let actual = unsafe { stream_fd(stream) };
        if actual != fd {
            log::warn!("fdopen: fileno mismatch expected={fd} actual={actual}");
        }
    }
    stream
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fclose(stream: *mut libc::FILE) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if fd >= 0 && rt.fd_table.is_clawfs_fd(fd) {
                log::trace!("fclose(clawfs fd={})", fd);
                let _ = dispatch::dispatch_close(rt, fd);
                // Fall through to real fclose to free the FILE struct and close kernel fd.
            }
        }
    }
    unsafe { get_real_fclose()(stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fread(
    ptr: *mut libc::c_void,
    size: libc::size_t,
    nmemb: libc::size_t,
    stream: *mut libc::FILE,
) -> libc::size_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                let total = size.saturating_mul(nmemb);
                if total == 0 {
                    return 0;
                }
                let buf = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, total) };
                return match dispatch::dispatch_read(rt, &entry, buf) {
                    Ok(0) => {
                        // EOF: delegate to real fread so glibc sets the
                        // EOF flag on the stream.
                        unsafe { get_real_fread()(ptr, size, nmemb, stream) }
                    }
                    Ok(n) => {
                        if size == 0 {
                            0
                        } else {
                            n / size
                        }
                    }
                    Err(e) => {
                        set_errno(e);
                        0
                    }
                };
            }
        }
    }
    unsafe { get_real_fread()(ptr, size, nmemb, stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fread_unlocked(
    ptr: *mut libc::c_void,
    size: libc::size_t,
    nmemb: libc::size_t,
    stream: *mut libc::FILE,
) -> libc::size_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                let total = size.saturating_mul(nmemb);
                if total == 0 {
                    return 0;
                }
                let buf = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, total) };
                return match dispatch::dispatch_read(rt, &entry, buf) {
                    Ok(0) => {
                        // EOF: delegate to the real fread_unlocked so glibc
                        // sets the EOF flag on the stream. The underlying fd
                        // is /dev/null which always returns EOF.
                        unsafe { get_real_fread_unlocked()(ptr, size, nmemb, stream) }
                    }
                    Ok(n) => {
                        if size == 0 {
                            0
                        } else {
                            n / size
                        }
                    }
                    Err(e) => {
                        set_errno(e);
                        0
                    }
                };
            }
        }
    }
    unsafe { get_real_fread_unlocked()(ptr, size, nmemb, stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fgets(
    buf: *mut libc::c_char,
    size: i32,
    stream: *mut libc::FILE,
) -> *mut libc::c_char {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                if size <= 1 {
                    if size == 1 {
                        unsafe { *buf = 0 };
                    }
                    return if size <= 0 { std::ptr::null_mut() } else { buf };
                }
                let max = (size - 1) as usize; // leave room for NUL
                let mut pos = 0usize;
                let mut one = [0u8; 1];
                while pos < max {
                    match dispatch::dispatch_read(rt, &entry, &mut one) {
                        Ok(0) => break, // EOF
                        Ok(_) => {
                            unsafe { *buf.add(pos) = one[0] as libc::c_char };
                            pos += 1;
                            if one[0] == b'\n' {
                                break;
                            }
                        }
                        Err(e) => {
                            if pos == 0 {
                                set_errno(e);
                                return std::ptr::null_mut();
                            }
                            break;
                        }
                    }
                }
                if pos == 0 {
                    return std::ptr::null_mut(); // EOF, nothing read
                }
                unsafe { *buf.add(pos) = 0 }; // NUL-terminate
                return buf;
            }
        }
    }
    unsafe { get_real_fgets()(buf, size, stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fgetc(stream: *mut libc::FILE) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                let mut one = [0u8; 1];
                return match dispatch::dispatch_read(rt, &entry, &mut one) {
                    Ok(0) => libc::EOF,
                    Ok(_) => one[0] as i32,
                    Err(e) => {
                        set_errno(e);
                        libc::EOF
                    }
                };
            }
        }
    }
    unsafe { get_real_fgetc()(stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn getc_unlocked(stream: *mut libc::FILE) -> i32 {
    // Same logic as fgetc — the "unlocked" variant just skips the FILE mutex
    // which is irrelevant since we dispatch to ClawFS directly.
    fgetc(stream)
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fseek(stream: *mut libc::FILE, offset: libc::c_long, whence: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_lseek(rt, &entry, offset, whence) {
                    Ok(_) => 0,
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real_fseek()(stream, offset, whence) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fseeko(stream: *mut libc::FILE, offset: libc::off_t, whence: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_lseek(rt, &entry, offset, whence) {
                    Ok(_) => 0,
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real_fseeko()(stream, offset, whence) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fseeko64(
    stream: *mut libc::FILE,
    offset: libc::off64_t,
    whence: i32,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_lseek(rt, &entry, offset, whence) {
                    Ok(_) => 0,
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real_fseeko64()(stream, offset, whence) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ftell(stream: *mut libc::FILE) -> libc::c_long {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                return entry.get_offset() as libc::c_long;
            }
        }
    }
    unsafe { get_real_ftell()(stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ftello(stream: *mut libc::FILE) -> libc::off_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                return entry.get_offset() as libc::off_t;
            }
        }
    }
    unsafe { get_real_ftello()(stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ftello64(stream: *mut libc::FILE) -> libc::off64_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                return entry.get_offset() as libc::off64_t;
            }
        }
    }
    unsafe { get_real_ftello64()(stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rewind(stream: *mut libc::FILE) {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                entry.set_offset(0);
                return;
            }
        }
    }
    unsafe { get_real_rewind()(stream) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn feof(stream: *mut libc::FILE) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            let fd = unsafe { stream_fd(stream) };
            if let Some(entry) = rt.fd_table.get(fd) {
                let offset = entry.get_offset();
                // Check if at or past end of file.
                if let Ok(record) = rt.fs.nfs_getattr(entry.inode) {
                    return if offset as u64 >= record.size { 1 } else { 0 };
                }
                return 0;
            }
        }
    }
    unsafe { get_real_feof()(stream) }
}

// ---------------------------------------------------------------------------
// Phase 2: *at family (mkdirat, unlinkat, renameat, renameat2, openat64)
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn openat64(
    dirfd: i32,
    path: *const libc::c_char,
    flags: i32,
    mode: libc::mode_t,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = resolve_at_path(rt, dirfd, &path_str) {
                    return do_open(rt, &path_str, &inner, flags, mode);
                }
            }
        }
    }
    unsafe { get_real_openat64()(dirfd, path, flags, mode) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mkdirat(dirfd: i32, path: *const libc::c_char, mode: libc::mode_t) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = resolve_at_path(rt, dirfd, &path_str) {
                    return match dispatch::dispatch_mkdir(rt, &inner) {
                        Ok(()) => 0,
                        Err(e) => set_errno(e) as i32,
                    };
                }
            }
        }
    }
    unsafe { get_real_mkdirat()(dirfd, path, mode) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn unlinkat(dirfd: i32, path: *const libc::c_char, flags: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = resolve_at_path(rt, dirfd, &path_str) {
                    if flags & libc::AT_REMOVEDIR != 0 {
                        return match dispatch::dispatch_rmdir(rt, &inner) {
                            Ok(()) => 0,
                            Err(e) => set_errno(e) as i32,
                        };
                    } else {
                        return match dispatch::dispatch_unlink(rt, &inner) {
                            Ok(()) => 0,
                            Err(e) => set_errno(e) as i32,
                        };
                    }
                }
            }
        }
    }
    unsafe { get_real_unlinkat()(dirfd, path, flags) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn renameat(
    olddirfd: i32,
    oldpath: *const libc::c_char,
    newdirfd: i32,
    newpath: *const libc::c_char,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let (Some(old_str), Some(new_str)) = (c_path_to_str(oldpath), c_path_to_str(newpath))
            {
                let old_inner = resolve_at_path(rt, olddirfd, &old_str);
                let new_inner = resolve_at_path(rt, newdirfd, &new_str);
                match (old_inner, new_inner) {
                    (Some(oi), Some(ni)) => {
                        return match dispatch::dispatch_rename(rt, &oi, &ni) {
                            Ok(()) => 0,
                            Err(e) => set_errno(e) as i32,
                        };
                    }
                    (Some(_), None) | (None, Some(_)) => {
                        return set_errno(libc::EXDEV) as i32;
                    }
                    (None, None) => {}
                }
            }
        }
    }
    unsafe { get_real_renameat()(olddirfd, oldpath, newdirfd, newpath) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn renameat2(
    olddirfd: i32,
    oldpath: *const libc::c_char,
    newdirfd: i32,
    newpath: *const libc::c_char,
    flags: libc::c_uint,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let (Some(old_str), Some(new_str)) = (c_path_to_str(oldpath), c_path_to_str(newpath))
            {
                let old_inner = resolve_at_path(rt, olddirfd, &old_str);
                let new_inner = resolve_at_path(rt, newdirfd, &new_str);
                match (old_inner, new_inner) {
                    (Some(oi), Some(ni)) => {
                        return match dispatch::dispatch_rename(rt, &oi, &ni) {
                            Ok(()) => 0,
                            Err(e) => set_errno(e) as i32,
                        };
                    }
                    (Some(_), None) | (None, Some(_)) => {
                        return set_errno(libc::EXDEV) as i32;
                    }
                    (None, None) => {}
                }
            }
        }
    }
    unsafe { get_real_renameat2()(olddirfd, oldpath, newdirfd, newpath, flags) }
}

// ---------------------------------------------------------------------------
// ioctl/fcntl — handle close-on-exec operations on synthetic fds
// ---------------------------------------------------------------------------

// FIOCLEX/FIONCLEX constants (not always in libc crate).
const FIOCLEX: libc::c_ulong = 0x5451;
const FIONCLEX: libc::c_ulong = 0x5450;

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
/// Defined as 3 fixed args — ABI-compatible with variadic on x86_64.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ioctl(fd: i32, request: libc::c_ulong, arg: libc::c_ulong) -> i32 {
    // For synthetic fds, handle FIOCLEX/FIONCLEX (close-on-exec) as no-ops.
    if request == FIOCLEX || request == FIONCLEX {
        if let Some(rt) = ClawfsRuntime::get() {
            if rt.fd_table.is_clawfs_fd(fd) {
                return 0;
            }
        }
    }
    unsafe { get_real_ioctl()(fd, request, arg) }
}

/// Shared fcntl logic for synthetic fds. Returns `Some(result)` if handled.
fn do_fcntl_synthetic(fd: i32, cmd: libc::c_int, arg: libc::c_int) -> Option<i32> {
    let rt = ClawfsRuntime::get()?;
    if !rt.fd_table.is_clawfs_fd(fd) {
        return None;
    }
    let result = match cmd {
        libc::F_GETFD => libc::FD_CLOEXEC,
        libc::F_SETFD => 0,
        libc::F_GETFL => rt.fd_table.get(fd).map_or(0, |e| e.flags),
        libc::F_SETFL => 0,
        libc::F_DUPFD | libc::F_DUPFD_CLOEXEC => match rt.fd_table.dup_min(fd, arg) {
            Some(new_fd) => {
                if let Some(entry) = rt.fd_table.get(new_fd) {
                    if entry.flags & (libc::O_WRONLY | libc::O_RDWR) != 0
                        && !entry.is_dir
                        && install_fd_proxy(rt, new_fd, entry, cmd == libc::F_DUPFD_CLOEXEC)
                            .is_err()
                    {
                        rt.fd_table.remove(new_fd);
                        set_errno(libc::EBADF) as i32
                    } else {
                        new_fd
                    }
                } else {
                    set_errno(libc::EBADF) as i32
                }
            }
            None => set_errno(libc::EINVAL) as i32,
        },
        _ => set_errno(libc::EINVAL) as i32,
    };
    log::trace!("fcntl(clawfs fd={fd}, cmd={cmd}, arg={arg}) -> {result}");
    Some(result)
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
/// Defined as 3 fixed args — ABI-compatible with variadic on x86_64.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fcntl(fd: i32, cmd: libc::c_int, arg: libc::c_int) -> i32 {
    if let Some(result) = do_fcntl_synthetic(fd, cmd, arg) {
        return result;
    }
    unsafe { get_real_fcntl()(fd, cmd, arg) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fcntl64(fd: i32, cmd: libc::c_int, arg: libc::c_int) -> i32 {
    if let Some(result) = do_fcntl_synthetic(fd, cmd, arg) {
        return result;
    }
    unsafe { get_real_fcntl64()(fd, cmd, arg) }
}

// ---------------------------------------------------------------------------
// inotify interception
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn inotify_init() -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if ClawfsRuntime::get().is_some() {
            let fd = crate::inotify::create(0);
            if fd >= 0 {
                log::debug!("inotify_init() -> fake fd={fd}");
                return fd;
            }
        }
    }
    unsafe { get_real_inotify_init()() }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn inotify_init1(flags: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if ClawfsRuntime::get().is_some() {
            let fd = crate::inotify::create(flags);
            if fd >= 0 {
                log::debug!("inotify_init1(flags={flags:#x}) -> fake fd={fd}");
                return fd;
            }
        }
    }
    unsafe { get_real_inotify_init1()(flags) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn inotify_add_watch(fd: i32, path: *const libc::c_char, mask: u32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if crate::inotify::is_inotify_fd(fd) {
                if let Some(path_str) = c_path_to_str(path) {
                    if let Some(inner) = try_classify(rt, &path_str) {
                        return match dispatch::resolve_path(rt, &inner) {
                            Ok((_, inode, _)) => {
                                let wd = crate::inotify::add_watch(fd, inode, mask);
                                if wd < 0 {
                                    set_errno(-wd) as i32
                                } else {
                                    wd
                                }
                            }
                            Err(e) => set_errno(e) as i32,
                        };
                    }
                    // Path not in ClawFS but fd is our fake pipe — can't forward to
                    // the kernel, so report the path as not found.
                    return set_errno(libc::ENOENT) as i32;
                }
            }
        }
    }
    unsafe { get_real_inotify_add_watch()(fd, path, mask) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn inotify_rm_watch(fd: i32, wd: i32) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if ClawfsRuntime::get().is_some() && crate::inotify::is_inotify_fd(fd) {
            return if crate::inotify::rm_watch(fd, wd) {
                0
            } else {
                set_errno(libc::EINVAL) as i32
            };
        }
    }
    unsafe { get_real_inotify_rm_watch()(fd, wd) }
}

// ---------------------------------------------------------------------------
// statx — used by modern ls (glibc 2.28+) and other tools
// ---------------------------------------------------------------------------

/// Populate a `statx` buffer from the simpler `libc::stat` struct.
fn stat_to_statx(st: &libc::stat, mask: libc::c_uint, out: *mut libc::statx) {
    // STATX_BASIC_STATS = 0x7ff covers all the classic stat fields.
    const STATX_BASIC_STATS: u32 = 0x0000_07ff;
    unsafe {
        let sx = &mut *out;
        *sx = std::mem::zeroed();
        sx.stx_mask = STATX_BASIC_STATS & mask;
        sx.stx_blksize = st.st_blksize as u32;
        sx.stx_nlink = st.st_nlink as u32;
        sx.stx_uid = st.st_uid;
        sx.stx_gid = st.st_gid;
        sx.stx_mode = st.st_mode as u16;
        sx.stx_ino = st.st_ino;
        sx.stx_size = st.st_size as u64;
        sx.stx_blocks = st.st_blocks as u64;
        sx.stx_atime.tv_sec = st.st_atime;
        sx.stx_atime.tv_nsec = st.st_atime_nsec as u32;
        sx.stx_mtime.tv_sec = st.st_mtime;
        sx.stx_mtime.tv_nsec = st.st_mtime_nsec as u32;
        sx.stx_ctime.tv_sec = st.st_ctime;
        sx.stx_ctime.tv_nsec = st.st_ctime_nsec as u32;
        // Device numbers from st_rdev / st_dev using Linux makedev encoding.
        sx.stx_rdev_major = ((st.st_rdev >> 8) & 0xfff) as u32;
        sx.stx_rdev_minor = ((st.st_rdev & 0xff) | ((st.st_rdev >> 12) & !0xff)) as u32;
        sx.stx_dev_major = ((st.st_dev >> 8) & 0xfff) as u32;
        sx.stx_dev_minor = ((st.st_dev & 0xff) | ((st.st_dev >> 12) & !0xff)) as u32;
    }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn statx(
    dirfd: i32,
    path: *const libc::c_char,
    flags: i32,
    mask: libc::c_uint,
    statxbuf: *mut libc::statx,
) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                // AT_EMPTY_PATH with an empty path: stat the fd itself.
                if path_str.is_empty() && (flags & libc::AT_EMPTY_PATH != 0) {
                    if let Some(entry) = rt.fd_table.get(dirfd) {
                        return match dispatch::dispatch_fstat(rt, &entry) {
                            Ok(st) => {
                                log::trace!(
                                    "statx(AT_EMPTY_PATH clawfs fd={}) -> size={}",
                                    dirfd,
                                    st.st_size
                                );
                                stat_to_statx(&st, mask, statxbuf);
                                0
                            }
                            Err(e) => set_errno(e) as i32,
                        };
                    }
                } else if !path_str.is_empty() {
                    if let Some(inner) = resolve_at_path(rt, dirfd, &path_str) {
                        return match dispatch::dispatch_stat(rt, &inner) {
                            Ok(st) => {
                                log::trace!("statx({:?}) -> size={}", path_str, st.st_size);
                                stat_to_statx(&st, mask, statxbuf);
                                0
                            }
                            Err(e) => {
                                log::trace!("statx({:?}) -> err={}", path_str, e);
                                set_errno(e) as i32
                            }
                        };
                    }
                }
            }
        }
    }
    unsafe { get_real_statx()(dirfd, path, flags, mask, statxbuf) }
}
