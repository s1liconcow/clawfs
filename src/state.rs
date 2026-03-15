use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
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
            |state| (&mut state.segment_next, &mut state.segment_remaining),
            batch,
            reserve,
        )
    }

    pub fn reconcile_with_minimums(&self, min_inode: u64, min_segment: u64) -> Result<()> {
        let mut guard = self.state.lock();
        let mut changed = false;
        if guard.inode_next < min_inode {
            guard.inode_next = min_inode;
            guard.inode_remaining = 0;
            changed = true;
        }
        if guard.segment_next < min_segment {
            guard.segment_next = min_segment;
            guard.segment_remaining = 0;
            changed = true;
        }
        if changed {
            self.persist_locked(&guard)?;
        }
        Ok(())
    }

    fn next_id<F, G>(&self, selector: F, batch: u64, mut reserve: G) -> Result<u64>
    where
        F: Fn(&mut ClientState) -> (&mut u64, &mut u64),
        G: FnMut(u64) -> Result<u64>,
    {
        loop {
            let mut guard = self.state.lock();
            let (next, remaining) = selector(&mut guard);
            if *remaining > 0 {
                let id = *next;
                *next += 1;
                *remaining -= 1;
                self.persist_locked(&guard)?;
                return Ok(id);
            }
            let count = batch.max(1);
            drop(guard);
            let start = reserve(count)?;
            let mut guard = self.state.lock();
            let (next, remaining) = selector(&mut guard);
            if *remaining == 0 {
                *next = start;
                *remaining = count;
            }
            // loop to consume newly filled pool
        }
    }

    fn persist_locked(&self, state: &ClientState) -> Result<()> {
        let data = serialize_state_fb(state);
        let _lock = self.lock_file()?;
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
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(anyhow::anyhow!(
                "locking client state {} failed: {}",
                self.lock_path.display(),
                std::io::Error::last_os_error()
            ));
        }
        Ok(file)
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
}
