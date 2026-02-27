use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use bincode::Options;
use bytes::Bytes;
use futures::StreamExt;
use libc::{ENOENT, ENOTDIR};
use log::{debug, info};
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::runtime::Handle;

use crate::codec::deserialize_flex;
use crate::config::{Config, ObjectStoreProvider};
use crate::inode::{FileStorage, InodeKind, InodeRecord, InodeShard};
use crate::superblock::Superblock;

/// How long an ENOENT result is served from the negative cache before we
/// re-check the backing store.  Short enough that new files created by the
/// same or another client become visible promptly.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_millis(1_500);

const SUPERBLOCK_FILE: &str = "superblock.bin";
const METADATA_FORMAT_VERSION: u32 = 1;
const METADATA_BIN_MAGIC: &[u8] = b"OSGBIN1";

fn metadata_bin_opts() -> impl bincode::Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

fn serialize_metadata<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let payload = metadata_bin_opts().serialize(value)?;
    let mut out = Vec::with_capacity(METADATA_BIN_MAGIC.len() + payload.len());
    out.extend_from_slice(METADATA_BIN_MAGIC);
    out.extend_from_slice(&payload);
    Ok(out)
}

fn deserialize_metadata<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if let Some(payload) = bytes.strip_prefix(METADATA_BIN_MAGIC) {
        return Ok(metadata_bin_opts().deserialize(payload)?);
    }
    // Backward-compatible fallback for pre-bincode metadata blobs.
    deserialize_flex(bytes)
}

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

#[derive(Debug, Clone)]
pub struct VersionedSuperblock {
    pub block: Superblock,
    pub version: String,
}

#[derive(Clone)]
struct CacheEntry {
    record: InodeRecord,
    refreshed: Instant,
    /// Generation at which this record was committed.  Used to prevent stale
    /// shard reloads from overwriting a fresher cache entry.
    generation: u64,
}

#[derive(Clone)]
struct ShardEntry {
    shard: InodeShard,
    generation: u64,
}

pub struct MetadataStore {
    store: Arc<dyn ObjectStore>,
    root_prefix: String,
    shard_size: u64,
    cache: Mutex<HashMap<u64, CacheEntry>>,
    shards: Mutex<HashMap<u64, ShardEntry>>,
    last_delta_generation: Mutex<u64>,
    log_storage_io: bool,
    /// Short-lived cache of inode numbers known not to exist.  Avoids
    /// repeated shard loads for ENOENT lookups (git, cargo, ripgrep…).
    negative_cache: Mutex<HashMap<u64, Instant>>,
    handle: Handle,
}

impl MetadataStore {
    pub async fn new(config: &Config, handle: Handle) -> Result<Self> {
        let (store, prefix) = create_object_store(config)?;

        let store = Self {
            store,
            root_prefix: prefix,
            shard_size: config.shard_size,
            cache: Mutex::new(HashMap::new()),
            shards: Mutex::new(HashMap::new()),
            last_delta_generation: Mutex::new(0),
            log_storage_io: config.log_storage_io,
            negative_cache: Mutex::new(HashMap::new()),
            handle,
        };
        store.load_latest_imaps().await?;
        Ok(store)
    }

    fn log_backing(&self, args: fmt::Arguments<'_>) {
        if self.log_storage_io {
            info!(target: "backing", "{}", args);
        } else {
            debug!(target: "backing", "{}", args);
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    fn superblock_path(&self) -> ObjectPath {
        let base = self.root_prefix.trim_matches('/');
        if base.is_empty() {
            ObjectPath::from(format!("metadata/{}", SUPERBLOCK_FILE))
        } else {
            ObjectPath::from(format!("{}/metadata/{}", base, SUPERBLOCK_FILE))
        }
    }

    fn imap_prefix(&self) -> ObjectPath {
        let base = self.root_prefix.trim_matches('/');
        if base.is_empty() {
            ObjectPath::from("metadata/imaps")
        } else {
            ObjectPath::from(format!("{}/metadata/imaps", base))
        }
    }

    fn delta_prefix(&self) -> ObjectPath {
        let base = self.root_prefix.trim_matches('/');
        if base.is_empty() {
            ObjectPath::from("metadata/imap_deltas")
        } else {
            ObjectPath::from(format!("{}/metadata/imap_deltas", base))
        }
    }

    pub async fn load_superblock(&self) -> Result<Option<VersionedSuperblock>> {
        let path = self.superblock_path();
        let result = self.store.get(&path).await;
        match result {
            Ok(get_result) => {
                let version = get_result.meta.e_tag.clone().unwrap_or_else(|| {
                    get_result
                        .meta
                        .last_modified
                        .timestamp_nanos_opt()
                        .unwrap_or(0)
                        .to_string()
                });
                let bytes = get_result.bytes().await?;
                let stored: StoredSuperblock = deserialize_metadata(&bytes)?;
                anyhow::ensure!(
                    stored.version == METADATA_FORMAT_VERSION,
                    "unsupported superblock format version {}",
                    stored.version
                );
                Ok(Some(VersionedSuperblock {
                    block: stored.block,
                    version,
                }))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(anyhow::Error::from(e).context(format!("reading superblock {}", path))),
        }
    }

    pub async fn store_superblock(&self, sb: &Superblock) -> Result<String> {
        let stored = StoredSuperblock {
            version: METADATA_FORMAT_VERSION,
            block: sb.clone(),
        };
        let bytes = serialize_metadata(&stored)?;
        let path = self.superblock_path();
        let put_result = self
            .store
            .put(&path, PutPayload::from_bytes(Bytes::from(bytes)))
            .await?;
        self.log_backing(format_args!(
            "synced backing file path={} type=superblock generation={}",
            path, sb.generation
        ));
        Ok(put_result.e_tag.unwrap_or_default())
    }

    pub async fn store_superblock_conditional(
        &self,
        sb: &Superblock,
        expected_version: &str,
    ) -> Result<String> {
        let stored = StoredSuperblock {
            version: METADATA_FORMAT_VERSION,
            block: sb.clone(),
        };
        let bytes = serialize_metadata(&stored)?;
        let path = self.superblock_path();

        let opts = object_store::PutOptions {
            mode: object_store::PutMode::Update(object_store::UpdateVersion {
                e_tag: Some(expected_version.to_string()),
                version: None,
            }),
            ..Default::default()
        };

        let result = self
            .store
            .put_opts(&path, PutPayload::from_bytes(Bytes::from(bytes)), opts)
            .await;

        match result {
            Ok(put_result) => {
                self.log_backing(format_args!(
                    "synced backing file path={} type=superblock generation={} (conditional if-match={})",
                    path, sb.generation, expected_version
                ));
                Ok(put_result.e_tag.unwrap_or_default())
            }
            Err(object_store::Error::NotImplemented) => {
                // Fallback for stores that don't support conditional put (like LocalFileSystem)
                // Note: This loses atomicity guarantees in concurrent environments.
                self.store_superblock(sb).await
            }
            Err(e) => Err(anyhow::Error::from(e).context(format!(
                "conditional put failed for superblock at version {}",
                expected_version
            ))),
        }
    }

    pub async fn compare_and_swap_superblock(
        &self,
        expected_generation: u64,
        sb: &Superblock,
    ) -> Result<()> {
        let current = self.load_superblock().await?;
        if let Some(existing) = current {
            if existing.block.generation != expected_generation {
                return Err(anyhow!(
                    "superblock generation mismatch: expected {}, found {}",
                    expected_generation,
                    existing.block.generation
                ));
            }
            self.store_superblock_conditional(sb, &existing.version)
                .await?;
        } else if expected_generation != 0 {
            return Err(anyhow!(
                "superblock missing while expecting generation {}",
                expected_generation
            ));
        } else {
            // Bootstrap: first write.
            let stored = StoredSuperblock {
                version: METADATA_FORMAT_VERSION,
                block: sb.clone(),
            };
            let bytes = serialize_metadata(&stored)?;
            let payload = Bytes::from(bytes);
            let path = self.superblock_path();

            let opts = object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            };
            let result = self
                .store
                .put_opts(&path, PutPayload::from_bytes(payload.clone()), opts)
                .await;

            match result {
                Ok(_) => {}
                Err(object_store::Error::NotImplemented) => {
                    self.store
                        .put(&path, PutPayload::from_bytes(payload))
                        .await?;
                }
                Err(e) => {
                    return Err(
                        anyhow::Error::from(e).context("initial superblock bootstrap failed")
                    );
                }
            }
        }
        Ok(())
    }

    /// Returns the committed record for this inode if it is present in the
    /// positive in-memory cache.  Does NOT trigger a shard reload and does NOT
    /// interact with the negative cache.  Safe to call from synchronous
    /// contexts (e.g. inside flush_pending under flush_lock) where callers
    /// only need extents that were committed by a prior flush.
    pub fn get_cached_inode(&self, inode: u64) -> Option<InodeRecord> {
        self.cache.lock().get(&inode).map(|e| e.record.clone())
    }

    /// Fast-path child lookup that avoids cloning the parent inode record.
    /// Returns `Some(result)` when the parent is present in the positive cache,
    /// otherwise `None` so callers can fall back to normal load/reload logic.
    pub fn lookup_cached_child(
        &self,
        parent: u64,
        name: &str,
    ) -> Option<std::result::Result<u64, i32>> {
        let cache = self.cache.lock();
        let entry = cache.get(&parent)?;
        if matches!(entry.record.kind, InodeKind::Tombstone) {
            return Some(Err(ENOENT));
        }
        match &entry.record.kind {
            InodeKind::Directory { children } => Some(children.get(name).copied().ok_or(ENOENT)),
            _ => Some(Err(ENOTDIR)),
        }
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
            if let Some(&expires) = neg.get(&inode)
                && Instant::now() < expires
            {
                return Ok(None);
            }
        }

        self.reload_shard_for_inode(inode).await?;

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

        // Phase 1: update in-memory caches + shards under lock
        let mut touched_shard_ids = HashSet::new();
        {
            let mut cache = self.cache.lock();
            let mut shards = self.shards.lock();
            let mut neg = self.negative_cache.lock();
            for record in records {
                neg.remove(&record.inode);
                cache.insert(
                    record.inode,
                    CacheEntry::with_generation(record.clone(), generation),
                );
                let shard_id = record.shard_index(shard_size);
                let entry = shards.entry(shard_id).or_insert_with(|| ShardEntry {
                    shard: InodeShard::new(shard_id),
                    generation,
                });
                entry.shard.upsert(record.clone());
                entry.generation = generation;
                touched_shard_ids.insert(shard_id);
            }
        }

        // Phase 2: serialize touched shards
        let shard_writes: Vec<(ObjectPath, Vec<u8>, u64, usize)> = {
            let shards = self.shards.lock();
            let base_path = self.imap_prefix();
            let mut shard_ids: Vec<u64> = touched_shard_ids.into_iter().collect();
            shard_ids.sort_unstable();
            shard_ids
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
                    let data = serialize_metadata(&stored).ok()?;
                    let count = entry.shard.inodes.len();
                    let filename = format!("i_{generation:020}_{:08x}.bin", shard_id);
                    let path = base_path.child(filename.as_str());
                    Some((path, data, shard_id, count))
                })
                .collect()
        };

        for (path, data, shard_id, count) in shard_writes {
            self.store
                .put(&path, PutPayload::from_bytes(Bytes::from(data)))
                .await?;
            self.log_backing(format_args!(
                "synced backing file path={} type=imap shard={} generation={} entries={}",
                path, shard_id, generation, count
            ));
        }

        // Phase 3: write deltas
        let chunk = delta_batch.max(1);
        for chunk_records in records.chunks(chunk) {
            self.write_delta_ref(generation, chunk_records).await?;
        }
        Ok(())
    }

    pub async fn sync_metadata_writes(&self) -> Result<()> {
        // Object store writes (put) are atomic and durable upon success.
        // No directory fsync needed.
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
        self.write_shard_ref(generation, shard_id).await?;
        let tombstone = InodeRecord::tombstone(inode);
        self.write_delta_ref(generation, &[tombstone]).await
    }

    async fn write_shard_ref(&self, generation: u64, shard_id: u64) -> Result<()> {
        let filename = format!("i_{generation:020}_{:08x}.bin", shard_id);
        let path = self.imap_prefix().child(filename.as_str());
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
            (serialize_metadata(&stored)?, entry.shard.inodes.len())
        };
        self.store
            .put(&path, PutPayload::from_bytes(Bytes::from(data)))
            .await?;
        self.log_backing(format_args!(
            "synced backing file path={} type=imap shard={} generation={} entries={}",
            path, shard_id, generation, count
        ));
        Ok(())
    }

    async fn write_delta_ref(&self, generation: u64, records: &[InodeRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let bloom = records
            .iter()
            .fold(0u128, |mask, record| mask | bloom_mask(record.inode));
        let filename = format!("d_{generation:020}_{:032x}.bin", bloom);
        let path = self.delta_prefix().child(filename.as_str());
        let stored = StoredDeltaRef {
            version: METADATA_FORMAT_VERSION,
            generation,
            records,
        };
        let data = serialize_metadata(&stored)?;
        {
            let mut guard = self.last_delta_generation.lock();
            *guard = (*guard).max(generation);
        }
        self.store
            .put(&path, PutPayload::from_bytes(Bytes::from(data)))
            .await?;
        self.log_backing(format_args!(
            "synced backing file path={} type=delta generation={} records={}",
            path,
            generation,
            records.len()
        ));
        Ok(())
    }

    async fn load_latest_imaps(&self) -> Result<()> {
        let mut latest: HashMap<u64, (u64, ObjectPath)> = HashMap::new();
        let prefix = self.imap_prefix();

        // Ensure prefix exists (optional check or just list)
        // Note: list(Some(prefix)) works even if prefix is "virtual" directory

        let mut stream = self.store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            let meta = item?;
            let name = meta.location.filename().unwrap_or_default();
            if let Some((generation, shard_id)) = parse_imap_filename(name) {
                latest
                    .entry(shard_id)
                    .and_modify(|current| {
                        if generation > current.0 {
                            *current = (generation, meta.location.clone());
                        }
                    })
                    .or_insert((generation, meta.location));
            }
        }

        let mut loaded_shards = Vec::new();
        let mut max_generation = 0;

        // Parallel fetch could be better here, but keep it simple for now
        for (shard_id, (_, path)) in latest {
            let bytes = self.store.get(&path).await?.bytes().await?;
            let stored: StoredShard = deserialize_metadata(&bytes)?;
            anyhow::ensure!(
                stored.version == METADATA_FORMAT_VERSION,
                "unsupported shard version {}",
                stored.version
            );
            loaded_shards.push((shard_id, stored));
        }

        let mut cache = self.cache.lock();
        let mut shard_map = self.shards.lock();
        for (shard_id, stored) in loaded_shards {
            let mut shard = InodeShard::new(shard_id);
            for (ino, record) in stored.entries {
                shard.inodes.insert(ino, record);
            }
            for (ino, record) in &shard.inodes {
                cache.insert(
                    *ino,
                    CacheEntry::with_generation(record.clone(), stored.generation),
                );
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
        // This needs to be async or blocking. Since we are in a non-async method
        // that's often called from blocking context (or we need to change signature),
        // we use the handle to block.
        // However, `spawn_metadata_poller` calls this inside `spawn_blocking`.
        // So we can use `self.handle.block_on`.

        self.handle.block_on(async {
            let mut newest = *self.last_delta_generation.lock();
            let mut files = Vec::new();
            let prefix = self.delta_prefix();

            let mut stream = self.store.list(Some(&prefix));
            while let Some(item) = stream.next().await {
                let meta = item?;
                let name = meta.location.filename().unwrap_or_default();
                if let Some(generation) = parse_delta_filename(name)
                    && generation > newest
                {
                    files.push((generation, meta.location));
                }
            }

            files.sort_by_key(|(generation, _)| *generation);
            let mut updated_records = Vec::new();
            for (generation, path) in files {
                let bytes = self.store.get(&path).await?.bytes().await?;
                let stored: StoredDelta = deserialize_metadata(&bytes)?;
                anyhow::ensure!(
                    stored.version == METADATA_FORMAT_VERSION,
                    "unsupported delta version {}",
                    stored.version
                );
                for record in stored.records {
                    if matches!(record.kind, crate::inode::InodeKind::Tombstone) {
                        self.cache.lock().remove(&record.inode);
                    } else {
                        self.cache.lock().insert(
                            record.inode,
                            CacheEntry::with_generation(record.clone(), generation),
                        );
                        updated_records.push(record.clone());
                    }
                }
                newest = newest.max(generation);
            }
            *self.last_delta_generation.lock() = newest;
            Ok(updated_records)
        })
    }

    pub fn delta_file_count(&self) -> Result<usize> {
        self.handle.block_on(async {
            let prefix = self.delta_prefix();
            let mut count = 0;
            let mut stream = self.store.list(Some(&prefix));
            while let Some(item) = stream.next().await {
                let _ = item?;
                count += 1;
            }
            Ok(count)
        })
    }

    pub fn prune_deltas(&self, keep: usize) -> Result<usize> {
        self.handle.block_on(async {
            let prefix = self.delta_prefix();
            let mut files: Vec<(u64, ObjectPath)> = Vec::new();

            let mut stream = self.store.list(Some(&prefix));
            while let Some(item) = stream.next().await {
                let meta = item?;
                let name = meta.location.filename().unwrap_or_default();
                if let Some(generation) = parse_delta_filename(name) {
                    files.push((generation, meta.location));
                }
            }

            files.sort_by_key(|(generation, _)| *generation);
            let mut removed = 0;
            if files.len() > keep {
                let excess = files.len() - keep;
                for (_, path) in files.into_iter().take(excess) {
                    self.store.delete(&path).await?;
                    removed += 1;
                }
            }
            Ok(removed)
        })
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

    async fn reload_shard_for_inode(&self, inode: u64) -> Result<()> {
        let shard_id = shard_for_inode(inode, self.shard_size);
        self.reload_shard(shard_id).await
    }

    async fn reload_shard(&self, shard_id: u64) -> Result<()> {
        let mut newest_path: Option<(u64, ObjectPath)> = None;
        let prefix = self.imap_prefix();

        let mut stream = self.store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            let meta = item?;
            let name = meta.location.filename().unwrap_or_default();
            if let Some((generation, shard)) = parse_imap_filename(name)
                && shard == shard_id
            {
                newest_path = match newest_path {
                    Some((existing_gen, _)) if existing_gen >= generation => newest_path,
                    _ => Some((generation, meta.location)),
                };
            }
        }

        if let Some((generation, path)) = newest_path {
            let bytes = self.store.get(&path).await?.bytes().await?;
            let stored: StoredShard = deserialize_metadata(&bytes)?;
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
                // Only update the cache if this shard reload is at least as
                // fresh as the existing entry.  A concurrent
                // `persist_inodes_batch` may have already committed a newer
                // generation between our directory listing and this point;
                // overwriting that entry would revert directory children or
                // file storage to a stale snapshot.
                let dominated = cache
                    .get(ino)
                    .map(|existing| existing.generation > generation)
                    .unwrap_or(false);
                if !dominated {
                    cache.insert(
                        *ino,
                        CacheEntry::with_generation(record.clone(), generation),
                    );
                }
            }
            let mut shard_map = self.shards.lock();
            let dominated_shard = shard_map
                .get(&shard_id)
                .map(|existing| existing.generation > generation)
                .unwrap_or(false);
            if !dominated_shard {
                shard_map.insert(shard_id, ShardEntry { shard, generation });
            }
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
    if parts.is_empty() {
        return None;
    }
    let generation = parts[0].parse::<u64>().ok()?;
    Some(generation)
}

impl CacheEntry {
    fn with_generation(record: InodeRecord, generation: u64) -> Self {
        let mut normalized = record;
        normalized.normalize_storage();
        Self {
            record: normalized,
            refreshed: Instant::now(),
            generation,
        }
    }
}

fn create_object_store(config: &Config) -> Result<(Arc<dyn ObjectStore>, String)> {
    match config.object_provider {
        ObjectStoreProvider::Local => {
            std::fs::create_dir_all(&config.store_path)
                .with_context(|| format!("creating store root {}", config.store_path.display()))?;
            let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path.clone())?)
                as Arc<dyn ObjectStore>;
            Ok((store, normalize_prefix(&config.object_prefix)))
        }
        ObjectStoreProvider::Aws => {
            let bucket = config
                .bucket
                .clone()
                .context("--bucket is required for AWS provider")?;
            let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);
            let region = config
                .region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_string());
            builder = builder.with_region(&region);
            if let Some(endpoint) = &config.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            let store = Arc::new(builder.build()?) as Arc<dyn ObjectStore>;
            Ok((store, normalize_prefix(&config.object_prefix)))
        }
        ObjectStoreProvider::Gcs => {
            let bucket = config
                .bucket
                .clone()
                .context("--bucket is required for GCS provider")?;
            let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);
            if let Some(sa_path) = &config.gcs_service_account {
                let creds = sa_path.to_string_lossy().into_owned();
                builder = builder.with_service_account_path(creds);
            }
            let store = Arc::new(builder.build()?) as Arc<dyn ObjectStore>;
            Ok((store, normalize_prefix(&config.object_prefix)))
        }
    }
}

fn normalize_prefix(user_prefix: &str) -> String {
    let trimmed = user_prefix.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        trimmed.to_string()
    }
}
