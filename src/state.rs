use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
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
    state: Mutex<ClientState>,
}

impl ClientStateManager {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state = if path.exists() {
            let mut buf = Vec::new();
            File::open(&path)
                .with_context(|| format!("opening client state {}", path.display()))?
                .read_to_end(&mut buf)?;
            serde_json::from_slice(&buf).with_context(|| "parsing client state")?
        } else {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            fs::create_dir_all(parent)
                .with_context(|| format!("creating client state dir {}", parent.display()))?;
            let state = ClientState::default();
            let mut file = File::create(&path)?;
            let data = serde_json::to_vec_pretty(&state)?;
            file.write_all(&data)?;
            state
        };
        Ok(Self {
            path,
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
        let data = serde_json::to_vec_pretty(state)?;
        let mut file = File::create(&self.path)?;
        file.write_all(&data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn state_persists_across_instances() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
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
}
