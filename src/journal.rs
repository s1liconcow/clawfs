use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::codec::{deserialize_flex, write_flexbuffer_unsynced};
use crate::inode::InodeRecord;
use crate::segment::StagedChunk;

const JOURNAL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalPayload {
    None,
    Inline(Vec<u8>),
    StageFile(StagedChunk),
    StageChunks(Vec<StagedChunk>),
}

#[derive(Debug, Clone)]
pub struct JournalEntry {
    pub record: InodeRecord,
    pub payload: JournalPayload,
}

#[derive(Serialize, Deserialize)]
struct StoredJournalEntry {
    version: u32,
    record: InodeRecord,
    payload: JournalPayload,
}

pub struct JournalManager {
    dir: PathBuf,
}

impl JournalManager {
    pub fn new<P: AsRef<Path>>(local_root: P) -> Result<Self> {
        let dir = local_root.as_ref().join("journal");
        fs::create_dir_all(&dir)
            .with_context(|| format!("creating journal dir {}", dir.display()))?;
        Ok(Self { dir })
    }

    pub fn persist_entry(&self, entry: &JournalEntry) -> Result<()> {
        let path = self.entry_path(entry.record.inode);
        let stored = StoredJournalEntry {
            version: JOURNAL_VERSION,
            record: entry.record.clone(),
            payload: entry.payload.clone(),
        };
        write_flexbuffer_unsynced(&path, &stored)
    }

    pub fn sync_entries(&self, inodes: &[u64]) -> Result<()> {
        for inode in inodes {
            let path = self.entry_path(*inode);
            if !path.exists() {
                continue;
            }
            let file = fs::OpenOptions::new()
                .read(true)
                .open(&path)
                .with_context(|| format!("opening journal entry {}", path.display()))?;
            file.sync_data()
                .with_context(|| format!("syncing journal entry {}", path.display()))?;
        }
        self.sync_dir()
    }

    pub fn clear_entry(&self, inode: u64) -> Result<()> {
        let path = self.entry_path(inode);
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("removing journal entry {}", path.display()))?;
        }
        Ok(())
    }

    pub fn load_entries(&self) -> Result<Vec<JournalEntry>> {
        let mut entries = Vec::new();
        if !self.dir.exists() {
            return Ok(entries);
        }
        for entry in fs::read_dir(&self.dir)
            .with_context(|| format!("listing journal dir {}", self.dir.display()))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let data = fs::read(entry.path())
                .with_context(|| format!("reading journal {}", entry.path().display()))?;
            let stored: StoredJournalEntry = deserialize_flex(&data)?;
            if stored.version != JOURNAL_VERSION {
                continue;
            }
            entries.push(JournalEntry {
                record: stored.record,
                payload: stored.payload,
            });
        }
        Ok(entries)
    }

    fn entry_path(&self, inode: u64) -> PathBuf {
        self.dir.join(format!("inode_{inode:020}.bin"))
    }

    fn sync_dir(&self) -> Result<()> {
        let dir = fs::OpenOptions::new()
            .read(true)
            .open(&self.dir)
            .with_context(|| format!("opening journal dir {}", self.dir.display()))?;
        dir.sync_all()
            .with_context(|| format!("syncing journal dir {}", self.dir.display()))
    }
}
