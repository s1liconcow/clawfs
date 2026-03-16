use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

/// Base for synthetic file descriptors. Chosen to be well above any realistic
/// host fd number while staying within i32 range.
pub(crate) const FD_BASE: i32 = 10_000_000;

/// State associated with an open ClawFS file descriptor.
#[allow(dead_code)]
pub struct FdEntry {
    pub inode: u64,
    pub parent_inode: u64,
    pub basename: String,
    /// The inner path within the ClawFS volume (e.g. "/subdir" for a dir fd).
    pub inner_path: String,
    pub flags: i32,
    /// Current file offset, shared across dup'd fds (via Arc sharing of FdEntry).
    pub offset: AtomicI64,
    pub is_dir: bool,
    /// Cached readdir results and cursor position for DIR* emulation.
    /// Each entry is (inode, d_type, name).
    #[allow(clippy::type_complexity)]
    pub dir_entries: Mutex<Option<(Vec<(u64, u8, String)>, usize)>>,
}

/// Thread-safe table of synthetic file descriptors for ClawFS files.
pub struct FdTable {
    map: DashMap<i32, Arc<FdEntry>>,
    next_fd: parking_lot::Mutex<i32>,
}

impl FdTable {
    pub fn new() -> Self {
        FdTable {
            map: DashMap::new(),
            next_fd: parking_lot::Mutex::new(FD_BASE),
        }
    }

    /// Insert a new entry and return the synthetic fd number.
    pub fn insert(
        &self,
        inode: u64,
        parent_inode: u64,
        basename: String,
        inner_path: String,
        flags: i32,
        is_dir: bool,
    ) -> i32 {
        let fd = {
            let mut next = self.next_fd.lock();
            let fd = *next;
            *next = next.wrapping_add(1);
            fd
        };
        let entry = Arc::new(FdEntry {
            inode,
            parent_inode,
            basename,
            inner_path,
            flags,
            offset: AtomicI64::new(0),
            is_dir,
            dir_entries: Mutex::new(None),
        });
        self.map.insert(fd, entry);
        fd
    }

    /// Look up an entry by fd number.
    pub fn get(&self, fd: i32) -> Option<Arc<FdEntry>> {
        self.map.get(&fd).map(|entry| entry.value().clone())
    }

    /// Remove an entry, returning it if it existed.
    pub fn remove(&self, fd: i32) -> Option<Arc<FdEntry>> {
        self.map.remove(&fd).map(|(_, entry)| entry)
    }

    /// Check whether the given fd is a synthetic ClawFS fd.
    pub fn is_clawfs_fd(&self, fd: i32) -> bool {
        self.map.contains_key(&fd)
    }

    /// Duplicate an existing fd to a new synthetic fd (for `dup`).
    /// The new fd shares the same `Arc<FdEntry>` (same offset).
    pub fn dup(&self, old_fd: i32) -> Option<i32> {
        let entry = self.get(old_fd)?;
        let new_fd = self.next_synthetic_fd(FD_BASE);
        self.map.insert(new_fd, entry);
        Some(new_fd)
    }

    /// Duplicate an existing fd to the lowest free descriptor number >= `min_fd`.
    pub fn dup_min(&self, old_fd: i32, min_fd: i32) -> Option<i32> {
        let entry = self.get(old_fd)?;
        let new_fd = self.next_synthetic_fd(min_fd.max(0));
        self.map.insert(new_fd, entry);
        Some(new_fd)
    }

    /// Duplicate an existing fd to a specific fd number (for `dup2`/`dup3`).
    /// If `new_fd` is already a ClawFS fd, it is closed first.
    /// Returns the new fd on success.
    pub fn dup_to(&self, old_fd: i32, new_fd: i32) -> Option<i32> {
        let entry = self.get(old_fd)?;
        // Close any existing ClawFS fd at new_fd.
        self.map.remove(&new_fd);
        self.map.insert(new_fd, entry);
        Some(new_fd)
    }

    fn next_synthetic_fd(&self, min_fd: i32) -> i32 {
        let mut next = self.next_fd.lock();
        let mut candidate = if min_fd >= FD_BASE {
            (*next).max(min_fd)
        } else {
            min_fd
        };
        while self.map.contains_key(&candidate) {
            candidate = candidate.wrapping_add(1);
        }
        if candidate >= FD_BASE {
            *next = candidate.wrapping_add(1);
        }
        candidate
    }
}

impl FdEntry {
    /// Get the current offset.
    pub fn get_offset(&self) -> i64 {
        self.offset.load(Ordering::Relaxed)
    }

    /// Set the offset, returning the new value.
    pub fn set_offset(&self, offset: i64) -> i64 {
        self.offset.store(offset, Ordering::Relaxed);
        offset
    }

    /// Atomically advance the offset by `delta`, returning the value *before* the advance.
    pub fn advance_offset(&self, delta: i64) -> i64 {
        self.offset.fetch_add(delta, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup() {
        let table = FdTable::new();
        let fd = table.insert(42, 1, "test.txt".into(), "/test.txt".into(), 0, false);
        assert!(fd >= FD_BASE);
        let entry = table.get(fd).unwrap();
        assert_eq!(entry.inode, 42);
        assert_eq!(entry.basename, "test.txt");
        assert_eq!(entry.inner_path, "/test.txt");
    }

    #[test]
    fn remove_entry() {
        let table = FdTable::new();
        let fd = table.insert(42, 1, "test.txt".into(), "/test.txt".into(), 0, false);
        assert!(table.is_clawfs_fd(fd));
        let removed = table.remove(fd).unwrap();
        assert_eq!(removed.inode, 42);
        assert!(!table.is_clawfs_fd(fd));
        assert!(table.get(fd).is_none());
    }

    #[test]
    fn offset_operations() {
        let entry = FdEntry {
            inode: 1,
            parent_inode: 0,
            basename: "f".into(),
            inner_path: "/f".into(),
            flags: 0,
            offset: AtomicI64::new(0),
            is_dir: false,
            dir_entries: Mutex::new(None),
        };
        assert_eq!(entry.get_offset(), 0);
        entry.set_offset(100);
        assert_eq!(entry.get_offset(), 100);
        let prev = entry.advance_offset(50);
        assert_eq!(prev, 100);
        assert_eq!(entry.get_offset(), 150);
    }

    #[test]
    fn host_fd_not_clawfs() {
        let table = FdTable::new();
        assert!(!table.is_clawfs_fd(3));
        assert!(!table.is_clawfs_fd(1000));
    }

    #[test]
    fn sequential_fds() {
        let table = FdTable::new();
        let fd1 = table.insert(1, 0, "a".into(), "/a".into(), 0, false);
        let fd2 = table.insert(2, 0, "b".into(), "/b".into(), 0, false);
        assert_eq!(fd2, fd1 + 1);
    }

    #[test]
    fn dup_shares_offset() {
        let table = FdTable::new();
        let fd = table.insert(42, 1, "test.txt".into(), "/test.txt".into(), 0, false);
        let dup_fd = table.dup(fd).unwrap();
        assert_ne!(fd, dup_fd);

        // Advancing offset on original should be visible on dup'd fd.
        let entry1 = table.get(fd).unwrap();
        entry1.advance_offset(100);
        let entry2 = table.get(dup_fd).unwrap();
        assert_eq!(entry2.get_offset(), 100);
    }

    #[test]
    fn dup_to_replaces_existing() {
        let table = FdTable::new();
        let fd1 = table.insert(1, 0, "a".into(), "/a".into(), 0, false);
        let fd2 = table.insert(2, 0, "b".into(), "/b".into(), 0, false);
        // dup fd1 onto fd2's slot.
        let result = table.dup_to(fd1, fd2).unwrap();
        assert_eq!(result, fd2);
        // fd2 should now point to inode 1.
        let entry = table.get(fd2).unwrap();
        assert_eq!(entry.inode, 1);
    }

    #[test]
    fn dup_nonexistent_returns_none() {
        let table = FdTable::new();
        assert!(table.dup(999).is_none());
        assert!(table.dup_to(999, 1000).is_none());
    }
}
