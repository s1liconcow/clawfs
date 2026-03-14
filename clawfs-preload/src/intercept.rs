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
use std::sync::atomic::{AtomicPtr, Ordering};

use crate::dispatch;
use crate::errno::set_errno;
use crate::runtime::ClawfsRuntime;

// ---------------------------------------------------------------------------
// Reentrancy guard
// ---------------------------------------------------------------------------

thread_local! {
    static REENTRANT: Cell<bool> = const { Cell::new(false) };
}

struct ReentrancyGuard;

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

/// Try to handle a path-based operation through ClawFS. Returns `None` if
/// the path doesn't match any ClawFS prefix (caller should fall through to real libc).
fn try_classify(rt: &ClawfsRuntime, path: &str) -> Option<String> {
    rt.prefix_router.classify(path).map(|cp| cp.inner)
}

/// Resolve a path that may be relative to a dirfd (for `*at` family).
/// Returns the inner ClawFS path if it can be resolved, or None for fall-through.
fn resolve_at_path(rt: &ClawfsRuntime, dirfd: i32, path: &str) -> Option<String> {
    if path.starts_with('/') {
        return try_classify(rt, path);
    }
    // Relative path with AT_FDCWD: resolve against virtual cwd.
    if dirfd == libc::AT_FDCWD {
        let (_, inner_cwd, _) = rt.cwd.get_clawfs()?;
        let inner = format!("{}/{}", inner_cwd.trim_end_matches('/'), path);
        return Some(inner);
    }
    // Relative path with a ClawFS dirfd: resolve using the fd's inner_path.
    let entry = rt.fd_table.get(dirfd)?;
    if !entry.is_dir {
        return None;
    }
    let inner = format!("{}/{}", entry.inner_path.trim_end_matches('/'), path);
    Some(inner)
}

// ---------------------------------------------------------------------------
// Intercepted functions
// ---------------------------------------------------------------------------

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn open(path: *const libc::c_char, flags: i32, mode: libc::mode_t) -> i32 {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(path_str) = c_path_to_str(path) {
                if let Some(inner) = try_classify(rt, &path_str) {
                    return match dispatch::dispatch_open(rt, &inner, flags, mode) {
                        Ok(fd) => fd,
                        Err(e) => set_errno(e) as i32,
                    };
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
                    return match dispatch::dispatch_open(rt, &inner, flags, mode) {
                        Ok(fd) => fd,
                        Err(e) => set_errno(e) as i32,
                    };
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
                    return match dispatch::dispatch_open(rt, &inner, flags, mode) {
                        Ok(fd) => fd,
                        Err(e) => set_errno(e) as i32,
                    };
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
                return match dispatch::dispatch_close(rt, fd) {
                    Ok(()) => 0,
                    Err(e) => set_errno(e) as i32,
                };
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
                return match dispatch::dispatch_read(rt, &entry, slice) {
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
            if let Some(entry) = rt.fd_table.get(fd) {
                let slice = unsafe { std::slice::from_raw_parts(buf as *const u8, count) };
                return match dispatch::dispatch_write(rt, &entry, slice) {
                    Ok(n) => n as libc::ssize_t,
                    Err(e) => set_errno(e) as libc::ssize_t,
                };
            }
        }
    }
    unsafe { get_real_write()(fd, buf, count) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lseek(fd: i32, offset: libc::off_t, whence: i32) -> libc::off_t {
    if let Some(_guard) = ReentrancyGuard::enter() {
        if let Some(rt) = ClawfsRuntime::get() {
            if let Some(entry) = rt.fd_table.get(fd) {
                return match dispatch::dispatch_lseek(rt, &entry, offset, whence) {
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
                return match dispatch::dispatch_lseek(rt, &entry, offset, whence) {
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
                        unsafe { *buf = st };
                        0
                    }
                    Err(e) => set_errno(e) as i32,
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
                return match rt.fd_table.dup_to(oldfd, newfd) {
                    Some(fd) => fd,
                    None => set_errno(libc::EBADF) as i32,
                };
            }
            // If newfd is a ClawFS fd but oldfd isn't, close the ClawFS fd first.
            if rt.fd_table.is_clawfs_fd(newfd) {
                rt.fd_table.remove(newfd);
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
                return match rt.fd_table.dup_to(oldfd, newfd) {
                    Some(fd) => fd,
                    None => set_errno(libc::EBADF) as i32,
                };
            }
            if rt.fd_table.is_clawfs_fd(newfd) {
                rt.fd_table.remove(newfd);
            }
        }
    }
    unsafe { get_real_dup3()(oldfd, newfd, flags) }
}

// ---------------------------------------------------------------------------
// Phase 2: symlink/readlink
// ---------------------------------------------------------------------------

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
                    return match dispatch::dispatch_chdir(rt, &path_str, &inner) {
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
                        let (ino, ref name) = entries[*cursor];
                        *cursor += 1;
                        return READDIR_BUF.with(|buf| {
                            let mut de = buf.borrow_mut();
                            *de = unsafe { std::mem::zeroed() };
                            de.d_ino = ino as libc::ino_t;
                            de.d_type = libc::DT_UNKNOWN;
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
                        let (ino, ref name) = entries[*cursor];
                        *cursor += 1;
                        return READDIR64_BUF.with(|buf| {
                            let mut de = buf.borrow_mut();
                            *de = unsafe { std::mem::zeroed() };
                            de.d_ino = ino as libc::ino64_t;
                            de.d_type = libc::DT_UNKNOWN;
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
                    Ok(()) => 0,
                    Err(e) => set_errno(e) as i32,
                };
            }
        }
    }
    unsafe { get_real_closedir()(dirp) }
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
                    return match dispatch::dispatch_open(rt, &inner, flags, mode) {
                        Ok(fd) => fd,
                        Err(e) => set_errno(e) as i32,
                    };
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
    if (request == FIOCLEX || request == FIONCLEX) && fd >= 10_000_000 {
        if let Some(rt) = ClawfsRuntime::get() {
            if rt.fd_table.is_clawfs_fd(fd) {
                return 0;
            }
        }
    }
    unsafe { get_real_ioctl()(fd, request, arg) }
}

/// Shared fcntl logic for synthetic fds. Returns `Some(result)` if handled.
fn do_fcntl_synthetic(fd: i32, cmd: libc::c_int) -> Option<i32> {
    if fd < 10_000_000 {
        return None;
    }
    let rt = ClawfsRuntime::get()?;
    if !rt.fd_table.is_clawfs_fd(fd) {
        return None;
    }
    Some(match cmd {
        libc::F_GETFD => libc::FD_CLOEXEC,
        libc::F_SETFD => 0,
        libc::F_GETFL => rt.fd_table.get(fd).map_or(0, |e| e.flags),
        libc::F_SETFL => 0,
        libc::F_DUPFD | libc::F_DUPFD_CLOEXEC => match rt.fd_table.dup(fd) {
            Some(new_fd) => new_fd,
            None => set_errno(libc::EBADF) as i32,
        },
        _ => set_errno(libc::EINVAL) as i32,
    })
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
/// Defined as 3 fixed args — ABI-compatible with variadic on x86_64.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fcntl(fd: i32, cmd: libc::c_int, arg: libc::c_int) -> i32 {
    if let Some(result) = do_fcntl_synthetic(fd, cmd) {
        return result;
    }
    unsafe { get_real_fcntl()(fd, cmd, arg) }
}

/// # Safety
/// Called by the dynamic linker as a libc function replacement.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fcntl64(fd: i32, cmd: libc::c_int, arg: libc::c_int) -> i32 {
    if let Some(result) = do_fcntl_synthetic(fd, cmd) {
        return result;
    }
    unsafe { get_real_fcntl64()(fd, cmd, arg) }
}
