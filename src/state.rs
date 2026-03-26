use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use flatbuffers::FlatBufferBuilder;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::metadata::fvt;

const CLIENT_STATE_VERSION: u32 = 1;

/// Magic bytes for the FlatBuffer client state format.
const STATE_FB_MAGIC: &[u8; 6] = b"OSGST2";

// ── FlatBuffer ClientState schema ─────────────────────────────────────────
//
// ClientStateFB table:
//   0(vt=4) version:            u32
//   1(vt=6) client_id:          string
//   2(vt=8) inode_next:         u64
//   3(vt=10) inode_remaining:   u64
//   4(vt=12) segment_next:      u64
//   5(vt=14) segment_remaining: u64

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientState {
    pub client_id: String,
    pub inode_next: u64,
    pub inode_remaining: u64,
    pub segment_next: u64,
    pub segment_remaining: u64,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            client_id: Uuid::new_v4().to_string(),
            inode_next: 0,
            inode_remaining: 0,
            segment_next: 0,
            segment_remaining: 0,
        }
    }
}

pub struct ClientStateManager {
    path: PathBuf,
    lock_path: PathBuf,
    state: Mutex<ClientState>,
}

impl ClientStateManager {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let lock_path = path.with_extension("lock");
        let state = if path.exists() {
            let mut buf = Vec::new();
            File::open(&path)
                .with_context(|| format!("opening client state {}", path.display()))?
                .read_to_end(&mut buf)?;
            deserialize_state_fb(&buf).with_context(|| "parsing client state")?
        } else {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            fs::create_dir_all(parent)
                .with_context(|| format!("creating client state dir {}", parent.display()))?;
            let state = ClientState::default();
            let data = serialize_state_fb(&state);
            let mut file = File::create(&path)?;
            file.write_all(&data)?;
            state
        };
        Ok(Self {
            path,
            lock_path,
            state: Mutex::new(state),
        })
    }

    pub fn client_id(&self) -> String {
        self.state.lock().client_id.clone()
    }

    pub fn next_inode_id<F>(&self, batch: u64, reserve: F) -> Result<u64>
    where
        F: FnMut(u64) -> Result<u64>,
    {
        self.next_id(
            "inode",
            |state| (&mut state.inode_next, &mut state.inode_remaining),
            batch,
            reserve,
        )
    }

    pub fn next_segment_id<F>(&self, batch: u64, reserve: F) -> Result<u64>
    where
        F: FnMut(u64) -> Result<u64>,
    {
        self.next_id(
            "segment",
            |state| (&mut state.segment_next, &mut state.segment_remaining),
            batch,
            reserve,
        )
    }

    pub fn reconcile_with_minimums(&self, min_inode: u64, min_segment: u64) -> Result<()> {
        let mut state_guard = self.state.lock();
        let _lock = self.lock_file()?;
        let mut disk_state = self.load_state_from_disk_locked()?;
        let mut changed = false;
        if disk_state.inode_next < min_inode {
            disk_state.inode_next = min_inode;
            disk_state.inode_remaining = 0;
            changed = true;
        }
        if disk_state.segment_next < min_segment {
            disk_state.segment_next = min_segment;
            disk_state.segment_remaining = 0;
            changed = true;
        }
        if changed {
            self.persist_state_locked(&disk_state)?;
        }
        *state_guard = disk_state;
        Ok(())
    }

    fn next_id<F, G>(
        &self,
        kind: &'static str,
        selector: F,
        batch: u64,
        mut reserve: G,
    ) -> Result<u64>
    where
        F: Fn(&mut ClientState) -> (&mut u64, &mut u64),
        G: FnMut(u64) -> Result<u64>,
    {
        let mut state_guard = self.state.lock();
        let _lock = self.lock_file()?;
        let mut disk_state = self.load_state_from_disk_locked()?;
        let count = batch.max(1);
        let client_id = disk_state.client_id.clone();
        let (next, remaining) = selector(&mut disk_state);

        if *remaining == 0 {
            let start = reserve(count)?;
            *next = start;
            *remaining = count;
            log::debug!(
                "client_state reserve kind={} start={} count={} client_id={}",
                kind,
                start,
                count,
                client_id
            );
        }

        let id = *next;
        *next += 1;
        *remaining -= 1;
        log::debug!(
            "client_state consume kind={} id={} next={} remaining={} client_id={}",
            kind,
            id,
            *next,
            *remaining,
            client_id
        );
        self.persist_state_locked(&disk_state)?;
        *state_guard = disk_state;
        Ok(id)
    }

    fn persist_state_locked(&self, state: &ClientState) -> Result<()> {
        let data = serialize_state_fb(state);
        let tmp_path = self.path.with_extension("tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    fn lock_file(&self) -> Result<File> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.lock_path)
            .with_context(|| format!("opening client state lock {}", self.lock_path.display()))?;
        lock_file_exclusive(&file, &self.lock_path)?;
        Ok(file)
    }

    fn load_state_from_disk_locked(&self) -> Result<ClientState> {
        if self.path.exists() {
            let mut buf = Vec::new();
            File::open(&self.path)
                .with_context(|| format!("opening client state {}", self.path.display()))?
                .read_to_end(&mut buf)?;
            deserialize_state_fb(&buf).with_context(|| "parsing client state")
        } else {
            let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
            fs::create_dir_all(parent)
                .with_context(|| format!("creating client state dir {}", parent.display()))?;
            let state = ClientState::default();
            self.persist_state_locked(&state)?;
            Ok(state)
        }
    }
}

fn serialize_state_fb(state: &ClientState) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::with_capacity(256);

    let client_id_wip = fbb.create_string(&state.client_id);

    let start = fbb.start_table();
    fbb.push_slot_always::<u32>(fvt(0), CLIENT_STATE_VERSION);
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(fvt(1), client_id_wip);
    fbb.push_slot_always::<u64>(fvt(2), state.inode_next);
    fbb.push_slot_always::<u64>(fvt(3), state.inode_remaining);
    fbb.push_slot_always::<u64>(fvt(4), state.segment_next);
    fbb.push_slot_always::<u64>(fvt(5), state.segment_remaining);
    let root = fbb.end_table(start);
    fbb.finish_minimal(root);

    let fb = fbb.finished_data();
    let mut out = Vec::with_capacity(STATE_FB_MAGIC.len() + fb.len());
    out.extend_from_slice(STATE_FB_MAGIC);
    out.extend_from_slice(fb);
    out
}

fn deserialize_state_fb(data: &[u8]) -> Result<ClientState> {
    let fb_data = data
        .strip_prefix(STATE_FB_MAGIC.as_slice())
        .ok_or_else(|| {
            anyhow::anyhow!("unsupported client state encoding (missing OSGST2 magic)")
        })?;

    // Safety: data was written by our own writer so the schema matches.
    let doc = unsafe { flatbuffers::root_unchecked::<flatbuffers::Table<'_>>(fb_data) };

    let version = unsafe { doc.get::<u32>(fvt(0), Some(0)) }.unwrap_or(0);
    anyhow::ensure!(
        version == CLIENT_STATE_VERSION,
        "unsupported client state version {}",
        version
    );

    let client_id = unsafe { doc.get::<flatbuffers::ForwardsUOffset<&str>>(fvt(1), None) }
        .unwrap_or("")
        .to_string();
    let inode_next = unsafe { doc.get::<u64>(fvt(2), Some(0)) }.unwrap_or(0);
    let inode_remaining = unsafe { doc.get::<u64>(fvt(3), Some(0)) }.unwrap_or(0);
    let segment_next = unsafe { doc.get::<u64>(fvt(4), Some(0)) }.unwrap_or(0);
    let segment_remaining = unsafe { doc.get::<u64>(fvt(5), Some(0)) }.unwrap_or(0);

    Ok(ClientState {
        client_id,
        inode_next,
        inode_remaining,
        segment_next,
        segment_remaining,
    })
}

#[cfg(unix)]
fn lock_file_exclusive(file: &File, lock_path: &Path) -> anyhow::Result<()> {
    use std::os::fd::AsRawFd;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if result != 0 {
        return Err(anyhow::anyhow!(
            "locking client state {} failed: {}",
            lock_path.display(),
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn lock_file_exclusive(_file: &File, _lock_path: &Path) -> anyhow::Result<()> {
    // File locking is advisory on Unix and primarily guards against concurrent
    // client-state writes from multiple mounts.  On non-Unix platforms we
    // accept the small race since single-client NFS mode is the expected path.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn state_persists_across_instances() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.bin");
        let manager = ClientStateManager::load(&path).unwrap();
        let first = manager.next_inode_id(4, |_| Ok(100)).unwrap();
        assert_eq!(first, 100);
        let second = manager.next_inode_id(4, |_| Ok(200)).unwrap();
        assert_eq!(second, 101);
        drop(manager);

        let manager = ClientStateManager::load(&path).unwrap();
        let third = manager.next_inode_id(4, |_| Ok(300)).unwrap();
        assert_eq!(third, 102);
    }

    #[test]
    fn reconcile_discards_stale_pools() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.bin");
        let manager = ClientStateManager::load(&path).unwrap();

        // Seed an old inode pool [100..103].
        assert_eq!(manager.next_inode_id(4, |_| Ok(100)).unwrap(), 100);
        // Seed an old segment pool [200..203].
        assert_eq!(manager.next_segment_id(4, |_| Ok(200)).unwrap(), 200);

        // Fresh store/superblock requires newer ids.
        manager.reconcile_with_minimums(500, 600).unwrap();

        // Reconciliation should force a fresh reservation from new minimums.
        assert_eq!(manager.next_inode_id(4, |_| Ok(500)).unwrap(), 500);
        assert_eq!(manager.next_segment_id(4, |_| Ok(600)).unwrap(), 600);
    }

    #[test]
    fn two_instances_sharing_state_file_do_not_reuse_inode_ids() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("shared-state.bin");

        let manager_a = ClientStateManager::load(&path).unwrap();
        assert_eq!(manager_a.next_inode_id(4, |_| Ok(100)).unwrap(), 100);

        let manager_b = ClientStateManager::load(&path).unwrap();

        let a_second = manager_a.next_inode_id(4, |_| Ok(200)).unwrap();
        let b_first = manager_b.next_inode_id(4, |_| Ok(300)).unwrap();
        let a_third = manager_a.next_inode_id(4, |_| Ok(400)).unwrap();
        let b_second = manager_b.next_inode_id(4, |_| Ok(500)).unwrap();

        assert_eq!(a_second, 101);
        assert_eq!(b_first, 102);
        assert_eq!(a_third, 103);
        assert!(
            b_second > a_third,
            "refilled range must advance past live ids"
        );
    }

    #[test]
    fn two_instances_sharing_state_file_do_not_reuse_segment_ids() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("shared-state.bin");

        let manager_a = ClientStateManager::load(&path).unwrap();
        assert_eq!(manager_a.next_segment_id(4, |_| Ok(700)).unwrap(), 700);

        let manager_b = ClientStateManager::load(&path).unwrap();

        let a_second = manager_a.next_segment_id(4, |_| Ok(800)).unwrap();
        let b_first = manager_b.next_segment_id(4, |_| Ok(900)).unwrap();
        let a_third = manager_a.next_segment_id(4, |_| Ok(1000)).unwrap();
        let b_second = manager_b.next_segment_id(4, |_| Ok(1100)).unwrap();

        assert_eq!(a_second, 701);
        assert_eq!(b_first, 702);
        assert_eq!(a_third, 703);
        assert!(
            b_second > a_third,
            "refilled range must advance past live ids"
        );
    }

    #[test]
    fn reconcile_under_shared_state_reloads_instead_of_clobbering_live_pool() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("shared-state.bin");

        let manager_a = ClientStateManager::load(&path).unwrap();
        assert_eq!(manager_a.next_inode_id(4, |_| Ok(100)).unwrap(), 100);

        let manager_b = ClientStateManager::load(&path).unwrap();

        assert_eq!(manager_a.next_inode_id(4, |_| Ok(200)).unwrap(), 101);

        manager_b.reconcile_with_minimums(101, 0).unwrap();

        assert_eq!(manager_b.next_inode_id(4, |_| Ok(300)).unwrap(), 102);
        assert_eq!(manager_a.next_inode_id(4, |_| Ok(400)).unwrap(), 103);
    }
}
