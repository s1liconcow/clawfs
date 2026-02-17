use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use log::debug;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::codec::{
    deserialize_flex, serialize_flex, write_flexbuffer, write_preserialized_unsynced,
};
use crate::inode::{FileStorage, InodeRecord, InodeShard};
use crate::superblock::Superblock;

/// How long an ENOENT result is served from the negative cache before we
/// re-check the backing store.  Short enough that new files created by the
/// same or another client become visible promptly.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_millis(1_500);

const SUPERBLOCK_FILE: &str = "superblock.bin";
const METADATA_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Serialize, Deserialize)]
struct StoredDelta {
    version: u32,
    generation: u64,
    records: Vec<InodeRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredShard {
    version: u32,
    generation: u64,
    entries: Vec<(u64, InodeRecord)>,
}

/// Borrow-friendly serialization struct for shards — avoids cloning every
/// InodeRecord just to serialize the shard snapshot to disk.  The flexbuffer
/// wire format is identical to [`StoredShard`] so deserialization is unchanged.
#[derive(Serialize)]
struct StoredShardRef<'a> {
    version: u32,
    generation: u64,
    entries: Vec<(u64, &'a InodeRecord)>,
}

/// Borrow-friendly serialization struct for deltas — avoids the
/// `records.to_vec()` clone that the owned [`StoredDelta`] required.
#[derive(Serialize)]
struct StoredDeltaRef<'a> {
    version: u32,
    generation: u64,
    records: &'a [InodeRecord],
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredSuperblock {
    version: u32,
    block: Superblock,
}

#[derive(Clone)]
struct CacheEntry {
    record: InodeRecord,
    refreshed: Instant,
}

#[derive(Clone)]
struct ShardEntry {
    shard: InodeShard,
    generation: u64,
}

pub struct MetadataStore {
    imap_dir: PathBuf,
    delta_dir: PathBuf,
    superblock_path: PathBuf,
    shard_size: u64,
    cache: Mutex<HashMap<u64, CacheEntry>>,
    shards: Mutex<HashMap<u64, ShardEntry>>,
    last_delta_generation: Mutex<u64>,
    log_storage_io: bool,
    /// Short-lived cache of inode numbers known not to exist.  Avoids
    /// repeated shard loads for ENOENT lookups (git, cargo, ripgrep…).
    negative_cache: Mutex<HashMap<u64, Instant>>,
}

impl MetadataStore {
    pub async fn open<P: AsRef<Path>>(
        store_root: P,
        shard_size: u64,
        log_storage_io: bool,
    ) -> Result<Self> {
        let metadata_root = store_root.as_ref().join("metadata");
        let imap_dir = metadata_root.join("imaps");
        let delta_dir = metadata_root.join("imap_deltas");
        fs::create_dir_all(&imap_dir)
            .with_context(|| format!("creating imap dir {}", imap_dir.display()))?;
        fs::create_dir_all(&delta_dir)
            .with_context(|| format!("creating delta dir {}", delta_dir.display()))?;
        let store = Self {
            imap_dir: imap_dir.clone(),
            delta_dir: delta_dir.clone(),
            superblock_path: metadata_root.join(SUPERBLOCK_FILE),
            shard_size,
            cache: Mutex::new(HashMap::new()),
            shards: Mutex::new(HashMap::new()),
            last_delta_generation: Mutex::new(0),
            log_storage_io,
            negative_cache: Mutex::new(HashMap::new()),
        };
        store.load_latest_imaps()?;
        Ok(store)
    }

    fn log_backing(&self, args: fmt::Arguments<'_>) {
        if self.log_storage_io {
            log::info!(target: "backing", "{}", args);
        } else {
            debug!(target: "backing", "{}", args);
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    pub async fn load_superblock(&self) -> Result<Option<Superblock>> {
        if !self.superblock_path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&self.superblock_path)
            .with_context(|| format!("reading superblock {}", self.superblock_path.display()))?;
        let stored: StoredSuperblock = deserialize_flex(&bytes)?;
        anyhow::ensure!(
            stored.version == METADATA_FORMAT_VERSION,
            "unsupported superblock format version {}",
            stored.version
        );
        let block = stored.block;
        Ok(Some(block))
    }

    pub async fn store_superblock(&self, sb: &Superblock) -> Result<()> {
        let stored = StoredSuperblock {
            version: METADATA_FORMAT_VERSION,
            block: sb.clone(),
        };
        write_flexbuffer(&self.superblock_path, &stored)?;
        self.log_backing(format_args!(
            "synced backing file path={} type=superblock generation={}",
            self.superblock_path.display(),
            sb.generation
        ));
        Ok(())
    }

    pub async fn compare_and_swap_superblock(
        &self,
        expected_generation: u64,
        sb: &Superblock,
    ) -> Result<()> {
        let current = self.load_superblock().await?;
        if let Some(existing) = current {
            if existing.generation != expected_generation {
                return Err(anyhow!(
                    "superblock generation mismatch: expected {}, found {}",
                    expected_generation,
                    existing.generation
                ));
            }
        } else if expected_generation != 0 {
            return Err(anyhow!(
                "superblock missing while expecting generation {}",
                expected_generation
            ));
        }
        self.store_superblock(sb).await
    }

    pub async fn get_inode_with_ttl(
        &self,
        inode: u64,
        file_ttl: Duration,
        dir_ttl: Duration,
    ) -> Result<Option<InodeRecord>> {
        let ttl = |rec: &InodeRecord| if rec.is_dir() { dir_ttl } else { file_ttl };

        // Fast path: positive cache hit.
        if let Some(entry) = self.cache.lock().get(&inode).cloned() {
            let allowed = ttl(&entry.record);
            if allowed.is_zero() || entry.refreshed.elapsed() <= allowed {
                return Ok(Some(entry.record));
            }
        }

        // Negative cache: skip shard load if we recently confirmed ENOENT.
        {
            let neg = self.negative_cache.lock();
            if let Some(&expires) = neg.get(&inode) {
                if Instant::now() < expires {
                    return Ok(None);
                }
            }
        }

        self.reload_shard_for_inode(inode)?;

        let result = self
            .cache
            .lock()
            .get(&inode)
            .map(|entry| entry.record.clone());

        // Populate the negative cache on confirmed ENOENT.
        if result.is_none() {
            self.negative_cache
                .lock()
                .insert(inode, Instant::now() + NEGATIVE_CACHE_TTL);
        }

        Ok(result)
    }

    pub async fn get_inode(&self, inode: u64) -> Result<Option<InodeRecord>> {
        self.get_inode_with_ttl(inode, Duration::ZERO, Duration::ZERO)
            .await
    }

    pub async fn persist_inode(
        &self,
        record: &InodeRecord,
        generation: u64,
        shard_size: u64,
    ) -> Result<()> {
        self.persist_inodes_batch(std::slice::from_ref(record), generation, shard_size, 1)
            .await
    }

    pub async fn persist_inodes_batch(
        &self,
        records: &[InodeRecord],
        generation: u64,
        shard_size: u64,
        delta_batch: usize,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        // Phase 1: update in-memory caches + shards under lock, collecting
        // only the set of touched shard IDs (NOT full shard clones).
        let mut touched_shard_ids = Vec::new();
        {
            let mut cache = self.cache.lock();
            let mut shards = self.shards.lock();
            let mut neg = self.negative_cache.lock();
            for record in records {
                neg.remove(&record.inode);
                cache.insert(record.inode, CacheEntry::new(record.clone()));
                let shard_id = record.shard_index(shard_size);
                let entry = shards.entry(shard_id).or_insert_with(|| ShardEntry {
                    shard: InodeShard::new(shard_id),
                    generation,
                });
                entry.shard.upsert(record.clone());
                entry.generation = generation;
                if !touched_shard_ids.contains(&shard_id) {
                    touched_shard_ids.push(shard_id);
                }
            }
        }

        // Phase 2: serialize touched shards from references (single lock
        // acquisition, no record clones, no redundant re-insert).
        let shard_writes: Vec<(PathBuf, Vec<u8>, u64, usize)> = {
            let shards = self.shards.lock();
            touched_shard_ids
                .iter()
                .filter_map(|&shard_id| {
                    let entry = shards.get(&shard_id)?;
                    let stored = StoredShardRef {
                        version: METADATA_FORMAT_VERSION,
                        generation,
                        entries: entry
                            .shard
                            .inodes
                            .iter()
                            .map(|(ino, rec)| (*ino, rec))
                            .collect(),
                    };
                    let data = serialize_flex(&stored).ok()?;
                    let count = entry.shard.inodes.len();
                    let filename = format!("i_{generation:020}_{:08x}.bin", shard_id);
                    Some((self.imap_dir.join(filename), data, shard_id, count))
                })
                .collect()
        };

        for (path, data, shard_id, count) in shard_writes {
            write_preserialized_unsynced(&path, &self.imap_dir, &data)?;
            self.log_backing(format_args!(
                "synced backing file path={} type=imap shard={} generation={} entries={}",
                path.display(),
                shard_id,
                generation,
                count
            ));
        }

        // Phase 3: write deltas using borrowed references (no to_vec clone).
        let chunk = delta_batch.max(1);
        for chunk_records in records.chunks(chunk) {
            self.write_delta_ref(generation, chunk_records)?;
        }
        Ok(())
    }

    /// Ensure all shard and delta files written during this flush cycle are
    /// durable on disk.  Must be called after `persist_inodes_batch` and
    /// before `commit_generation` so that the superblock CAS only advances
    /// once all referenced metadata objects are safely written.
    ///
    /// Two directory-level fsyncs replace the per-file fsyncs that the old
    /// `write_flexbuffer` (synced) variant issued for every shard and delta.
    pub async fn sync_metadata_writes(&self) -> Result<()> {
        sync_dir_path(&self.imap_dir)?;
        sync_dir_path(&self.delta_dir)?;
        Ok(())
    }

    pub async fn remove_inode(&self, inode: u64, generation: u64, shard_size: u64) -> Result<()> {
        self.cache.lock().remove(&inode);
        self.negative_cache.lock().remove(&inode);
        let shard_id = shard_for_inode(inode, shard_size);
        {
            let mut shards = self.shards.lock();
            if let Some(entry) = shards.get_mut(&shard_id) {
                entry.shard.inodes.remove(&inode);
                entry.generation = generation;
            }
        }
        self.write_shard_ref(generation, shard_id)?;
        let tombstone = InodeRecord::tombstone(inode);
        self.write_delta_ref(generation, &[tombstone])
    }

    /// Serialize a shard directly from the in-memory `self.shards` map using
    /// borrowed references, avoiding the full `InodeShard::clone()` +
    /// per-record clone that the legacy [`write_shard`] path required.
    fn write_shard_ref(&self, generation: u64, shard_id: u64) -> Result<()> {
        let filename = format!("i_{generation:020}_{:08x}.bin", shard_id);
        let path = self.imap_dir.join(filename);
        let (data, count) = {
            let shards = self.shards.lock();
            let entry = match shards.get(&shard_id) {
                Some(e) => e,
                None => return Ok(()),
            };
            let stored = StoredShardRef {
                version: METADATA_FORMAT_VERSION,
                generation,
                entries: entry
                    .shard
                    .inodes
                    .iter()
                    .map(|(ino, rec)| (*ino, rec))
                    .collect(),
            };
            (serialize_flex(&stored)?, entry.shard.inodes.len())
        };
        write_preserialized_unsynced(&path, &self.imap_dir, &data)?;
        self.log_backing(format_args!(
            "synced backing file path={} type=imap shard={} generation={} entries={}",
            path.display(),
            shard_id,
            generation,
            count
        ));
        Ok(())
    }

    /// Write a delta log using borrowed record references, avoiding the
    /// `records.to_vec()` clone that [`write_delta`] performed.
    fn write_delta_ref(&self, generation: u64, records: &[InodeRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let bloom = records
            .iter()
            .fold(0u128, |mask, record| mask | bloom_mask(record.inode));
        let filename = format!("d_{generation:020}_{:032x}.bin", bloom);
        let path = self.delta_dir.join(filename);
        let stored = StoredDeltaRef {
            version: METADATA_FORMAT_VERSION,
            generation,
            records,
        };
        let data = serialize_flex(&stored)?;
        {
            let mut guard = self.last_delta_generation.lock();
            *guard = (*guard).max(generation);
        }
        write_preserialized_unsynced(&path, &self.delta_dir, &data)?;
        self.log_backing(format_args!(
            "synced backing file path={} type=delta generation={} records={}",
            path.display(),
            generation,
            records.len()
        ));
        Ok(())
    }

    fn load_latest_imaps(&self) -> Result<()> {
        let mut latest: HashMap<u64, (u64, PathBuf)> = HashMap::new();
        if !self.imap_dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(&self.imap_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = match name.to_str() {
                Some(n) => n,
                None => continue,
            };
            if let Some((generation, shard_id)) = parse_imap_filename(name) {
                latest
                    .entry(shard_id)
                    .and_modify(|current| {
                        if generation > current.0 {
                            *current = (generation, entry.path());
                        }
                    })
                    .or_insert((generation, entry.path()));
            }
        }
        let mut cache = self.cache.lock();
        let mut shard_map = self.shards.lock();
        let mut max_generation = 0;
        for (shard_id, (_, path)) in latest {
            let bytes =
                fs::read(&path).with_context(|| format!("reading shard {}", path.display()))?;
            let stored: StoredShard = deserialize_flex(&bytes)?;
            anyhow::ensure!(
                stored.version == METADATA_FORMAT_VERSION,
                "unsupported shard version {}",
                stored.version
            );
            let mut shard = InodeShard::new(shard_id);
            for (ino, record) in stored.entries {
                shard.inodes.insert(ino, record);
            }
            for (ino, record) in &shard.inodes {
                cache.insert(*ino, CacheEntry::new(record.clone()));
            }
            max_generation = max_generation.max(stored.generation);
            shard_map.insert(
                shard_id,
                ShardEntry {
                    shard,
                    generation: stored.generation,
                },
            );
        }
        *self.last_delta_generation.lock() = max_generation;
        Ok(())
    }

    pub fn apply_external_deltas(&self) -> Result<Vec<InodeRecord>> {
        let mut newest = *self.last_delta_generation.lock();
        let mut files = Vec::new();
        if !self.delta_dir.exists() {
            return Ok(Vec::new());
        }
        for entry in fs::read_dir(&self.delta_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name_os = entry.file_name();
            let name = match name_os.to_str() {
                Some(n) => n,
                None => continue,
            };
            if let Some(generation) = parse_delta_filename(name) {
                if generation > newest {
                    files.push((generation, entry.path()));
                }
            }
        }
        files.sort_by_key(|(generation, _)| *generation);
        let mut updated_records = Vec::new();
        for (generation, path) in files {
            let bytes =
                fs::read(&path).with_context(|| format!("reading delta {}", path.display()))?;
            let stored: StoredDelta = deserialize_flex(&bytes)?;
            anyhow::ensure!(
                stored.version == METADATA_FORMAT_VERSION,
                "unsupported delta version {}",
                stored.version
            );
            for record in stored.records {
                if matches!(record.kind, crate::inode::InodeKind::Tombstone) {
                    self.cache.lock().remove(&record.inode);
                } else {
                    self.cache
                        .lock()
                        .insert(record.inode, CacheEntry::new(record.clone()));
                    updated_records.push(record.clone());
                }
            }
            newest = newest.max(generation);
        }
        *self.last_delta_generation.lock() = newest;
        Ok(updated_records)
    }

    pub fn delta_file_count(&self) -> Result<usize> {
        if !self.delta_dir.exists() {
            return Ok(0);
        }
        let mut count = 0;
        for entry in fs::read_dir(&self.delta_dir)? {
            if entry?.file_type()?.is_file() {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn prune_deltas(&self, keep: usize) -> Result<usize> {
        if !self.delta_dir.exists() {
            return Ok(0);
        }
        let mut files: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&self.delta_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            if let Some(generation) = entry.file_name().to_str().and_then(parse_delta_filename) {
                files.push((generation, entry.path()));
            }
        }
        files.sort_by_key(|(generation, _)| *generation);
        let mut removed = 0;
        if files.len() > keep {
            let excess = files.len() - keep;
            for (_, path) in files.into_iter().take(excess) {
                if path.exists() {
                    fs::remove_file(&path)?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    pub fn segment_candidates(&self, max: usize) -> Result<Vec<InodeRecord>> {
        let mut candidates = Vec::new();
        let shards = self.shards.lock();
        for entry in shards.values() {
            for record in entry.shard.inodes.values() {
                if matches!(
                    record.storage,
                    FileStorage::LegacySegment(_) | FileStorage::Segments(_)
                ) {
                    candidates.push(record.clone());
                }
            }
        }
        drop(shards);
        candidates.sort_by_key(|record| {
            record
                .segment_pointer()
                .map(|ptr| (ptr.generation, ptr.segment_id))
                .unwrap_or((u64::MAX, u64::MAX))
        });
        if candidates.len() > max {
            candidates.truncate(max);
        }
        Ok(candidates)
    }

    fn reload_shard_for_inode(&self, inode: u64) -> Result<()> {
        let shard_id = shard_for_inode(inode, self.shard_size);
        self.reload_shard(shard_id)
    }

    fn reload_shard(&self, shard_id: u64) -> Result<()> {
        let mut newest_path: Option<(u64, PathBuf)> = None;
        if !self.imap_dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(&self.imap_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name_os = entry.file_name();
            let name = match name_os.to_str() {
                Some(n) => n,
                None => continue,
            };
            if let Some((generation, shard)) = parse_imap_filename(name) {
                if shard == shard_id {
                    newest_path = match newest_path {
                        Some((existing_gen, _)) if existing_gen >= generation => newest_path,
                        _ => Some((generation, entry.path())),
                    };
                }
            }
        }
        if let Some((generation, path)) = newest_path {
            let bytes =
                fs::read(&path).with_context(|| format!("reading shard {}", path.display()))?;
            let stored: StoredShard = deserialize_flex(&bytes)?;
            anyhow::ensure!(
                stored.version == METADATA_FORMAT_VERSION,
                "unsupported shard version {}",
                stored.version
            );
            let mut shard = InodeShard::new(shard_id);
            for (ino, record) in stored.entries {
                shard.inodes.insert(ino, record);
            }
            let mut cache = self.cache.lock();
            for (ino, record) in &shard.inodes {
                cache.insert(*ino, CacheEntry::new(record.clone()));
            }
            self.shards
                .lock()
                .insert(shard_id, ShardEntry { shard, generation });
        }
        Ok(())
    }
}

fn shard_for_inode(inode: u64, shard_size: u64) -> u64 {
    if shard_size == 0 {
        0
    } else {
        inode / shard_size
    }
}

fn bloom_mask(inode: u64) -> u128 {
    let mut hash = inode;
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    let bit = (hash & 0x7f) as u32;
    1u128 << bit
}

fn parse_imap_filename(name: &str) -> Option<(u64, u64)> {
    if !name.starts_with('i') {
        return None;
    }
    let trimmed = name.trim_start_matches('i').trim_start_matches('_');
    let parts: Vec<&str> = trimmed.split('_').collect();
    if parts.len() != 2 {
        return None;
    }
    let generation = parts[0].parse::<u64>().ok()?;
    let shard_str = parts[1].trim_end_matches(".bin");
    let shard_id = u64::from_str_radix(shard_str, 16).ok()?;
    Some((generation, shard_id))
}

fn parse_delta_filename(name: &str) -> Option<u64> {
    if !name.starts_with('d') {
        return None;
    }
    let trimmed = name.trim_start_matches('d').trim_start_matches('_');
    let parts: Vec<&str> = trimmed.split('_').collect();
    if parts.len() < 1 {
        return None;
    }
    let generation = parts[0].parse::<u64>().ok()?;
    Some(generation)
}

impl CacheEntry {
    fn new(record: InodeRecord) -> Self {
        let mut normalized = record;
        normalized.normalize_storage();
        Self {
            record: normalized,
            refreshed: Instant::now(),
        }
    }
}

/// Open `dir` and call `sync_all()` on it, ensuring all previously written
/// files within the directory are durable before the superblock advances.
fn sync_dir_path(dir: &Path) -> Result<()> {
    let handle = std::fs::OpenOptions::new()
        .read(true)
        .open(dir)
        .with_context(|| format!("opening dir for sync {}", dir.display()))?;
    handle
        .sync_all()
        .with_context(|| format!("syncing dir {}", dir.display()))
}
