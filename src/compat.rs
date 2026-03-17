// Portable errno constants and OS-abstraction helpers.
//
// On Unix these mirror the libc values exactly. On Windows they use the
// POSIX-compatible values from the Universal CRT so that the filesystem
// layer can return consistent error codes regardless of platform.

// ── errno constants ────────────────────────────────────────────────────

#[cfg(unix)]
pub use libc::{
    EACCES, EEXIST, EFBIG, EINVAL, EIO, EISDIR, ENAMETOOLONG, ENOENT, ENOTDIR, ENOTEMPTY, EPERM,
};

#[cfg(not(unix))]
pub const ENOENT: i32 = 2;
#[cfg(not(unix))]
pub const EPERM: i32 = 1;
#[cfg(not(unix))]
pub const EACCES: i32 = 13;
#[cfg(not(unix))]
pub const EEXIST: i32 = 17;
#[cfg(not(unix))]
pub const EINVAL: i32 = 22;
#[cfg(not(unix))]
pub const EIO: i32 = 5;
#[cfg(not(unix))]
pub const EISDIR: i32 = 21;
#[cfg(not(unix))]
pub const ENOTDIR: i32 = 20;
#[cfg(not(unix))]
pub const ENOTEMPTY: i32 = 39;
#[cfg(not(unix))]
pub const ENAMETOOLONG: i32 = 36;
#[cfg(not(unix))]
pub const EFBIG: i32 = 27;

// ── open(2) flags used in the fs layer ─────────────────────────────────

#[cfg(unix)]
pub use libc::{O_EXCL, O_TRUNC};

#[cfg(not(unix))]
pub const O_EXCL: i32 = 0x0080;
#[cfg(not(unix))]
pub const O_TRUNC: i32 = 0x0200;

// ── stat(2) mode‐type bits ─────────────────────────────────────────────

#[cfg(unix)]
pub use libc::{S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFMT, S_IFREG, S_IFSOCK};

#[cfg(not(unix))]
pub const S_IFMT: u32 = 0o170000;
#[cfg(not(unix))]
pub const S_IFREG: u32 = 0o100000;
#[cfg(not(unix))]
pub const S_IFDIR: u32 = 0o040000;
#[cfg(not(unix))]
pub const S_IFCHR: u32 = 0o020000;
#[cfg(not(unix))]
pub const S_IFBLK: u32 = 0o060000;
#[cfg(not(unix))]
pub const S_IFIFO: u32 = 0o010000;
#[cfg(not(unix))]
pub const S_IFSOCK: u32 = 0o140000;

// ── d_type constants (readdir) ─────────────────────────────────────────

#[cfg(unix)]
pub use libc::{DT_BLK, DT_CHR, DT_DIR, DT_FIFO, DT_LNK, DT_REG, DT_SOCK};

#[cfg(not(unix))]
pub const DT_REG: u8 = 8;
#[cfg(not(unix))]
pub const DT_DIR: u8 = 4;
#[cfg(not(unix))]
pub const DT_LNK: u8 = 10;
#[cfg(not(unix))]
pub const DT_BLK: u8 = 6;
#[cfg(not(unix))]
pub const DT_CHR: u8 = 2;
#[cfg(not(unix))]
pub const DT_FIFO: u8 = 1;
#[cfg(not(unix))]
pub const DT_SOCK: u8 = 12;

// ── uid / gid helpers ──────────────────────────────────────────────────

/// Return the effective uid of the current process (0 on Windows).
pub fn current_uid() -> u32 {
    #[cfg(unix)]
    {
        unsafe { libc::geteuid() as u32 }
    }
    #[cfg(not(unix))]
    {
        0
    }
}

/// Return the effective gid of the current process (0 on Windows).
pub fn current_gid() -> u32 {
    #[cfg(unix)]
    {
        unsafe { libc::getegid() as u32 }
    }
    #[cfg(not(unix))]
    {
        0
    }
}
