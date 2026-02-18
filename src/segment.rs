use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use log::{debug, info, warn};
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectError, ObjectStore, PutPayload};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use uuid::Uuid;

use lru::LruCache;

use crate::codec::{EncodedBytes, InlineCodecConfig, decode_bytes, encode_bytes};
use crate::config::{Config, ObjectStoreProvider};
use crate::inode::InlinePayloadCodec;

const SEGMENT_MAGIC_V2: &[u8; 4] = b"OSG2";
const SEGMENT_ENTRY_CODEC_HEADER_LEN: usize = 1 + 8 + 8 + 12;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentPointer {
    pub segment_id: u64,
    pub generation: u64,
    pub offset: u64,
    pub length: u64,
}

pub struct SegmentEntry {
    pub inode: u64,
    pub path: String,
    pub payload: SegmentPayload,
}

pub enum SegmentPayload {
    Bytes(Vec<u8>),
    SharedBytes(Arc<Vec<u8>>),
    Staged(Vec<StagedChunk>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedChunk {
    pub path: PathBuf,
    pub offset: u64,
    pub len: u64,
}

/// Default in-memory decoded-extent cache budget (256 MiB).
const DECODED_CACHE_DEFAULT_BYTES: u64 = 256 * 1024 * 1024;

pub struct SegmentManager {
    store: Arc<dyn ObjectStore>,
    handle: Handle,
    root_prefix: String,
    stage_dir: PathBuf,
    cache_dir: PathBuf,
    cache_limit: u64,
    cache_state: Arc<Mutex<SegmentCache>>,
    cache_fill_inflight: Arc<Mutex<HashSet<(u64, u64)>>>,
    stage_state: Mutex<StageState>,
    log_storage_io: bool,
    segment_compression: bool,
    segment_encryption_key: Option<String>,
    /// In-memory LRU cache of decoded segment payloads.  Keyed by
    /// `(generation, segment_id, offset, length)`, stores the decoded bytes
    /// wrapped in `Arc` so that callers can slice without copying the full
    /// extent on every FUSE read.
    decoded_cache: Mutex<DecodedExtentCache>,
}

struct EncodedSegmentEntry {
    inode: u64,
    path: String,
    plain_len: u64,
    encoded: EncodedBytes,
}

#[derive(Default)]
struct SegmentCache {
    total_bytes: u64,
    entries: VecDeque<(PathBuf, u64)>,
}

/// In-memory LRU cache of decoded segment payloads.  Avoids redundant
/// decompression when the same extent is read repeatedly (e.g. sequential
/// FUSE reads of a large file stored as one segment extent).
struct DecodedExtentCache {
    lru: LruCache<(u64, u64, u64, u64), Arc<Vec<u8>>>,
    total_bytes: u64,
    budget: u64,
}

impl DecodedExtentCache {
    fn new(budget: u64) -> Self {
        Self {
            // unbounded cap — we evict based on byte budget instead
            lru: LruCache::unbounded(),
            total_bytes: 0,
            budget,
        }
    }

    fn get(&mut self, key: &(u64, u64, u64, u64)) -> Option<Arc<Vec<u8>>> {
        self.lru.get(key).cloned()
    }

    fn put(&mut self, key: (u64, u64, u64, u64), value: Arc<Vec<u8>>) {
        let entry_bytes = value.len() as u64;
        // Don't cache entries larger than half the budget.
        if entry_bytes > self.budget / 2 {
            return;
        }
        self.total_bytes += entry_bytes;
        self.lru.put(key, value);
        // Evict LRU entries until within budget.
        while self.total_bytes > self.budget {
            if let Some((_k, evicted)) = self.lru.pop_lru() {
                self.total_bytes = self.total_bytes.saturating_sub(evicted.len() as u64);
            } else {
                break;
            }
        }
    }
}

#[derive(Default)]
struct StageState {
    active: Option<ActiveStage>,
    ref_counts: HashMap<PathBuf, usize>,
}

struct ActiveStage {
    path: PathBuf,
    file: File,
    len: u64,
}

impl SegmentManager {
    pub fn new(config: &Config, handle: Handle) -> Result<Self> {
        let (store, prefix): (Arc<dyn ObjectStore>, String) = match config.object_provider {
            ObjectStoreProvider::Local => {
                fs::create_dir_all(&config.store_path).with_context(|| {
                    format!("creating segment root {}", config.store_path.display())
                })?;
                let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path.clone())?)
                    as Arc<dyn ObjectStore>;
                (store, segment_prefix(&config.object_prefix))
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
                (store, segment_prefix(&config.object_prefix))
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
                (store, segment_prefix(&config.object_prefix))
            }
        };
        let stage_dir = config.local_cache_path.join("segment_stage");
        let cache_dir = config.local_cache_path.join("segment_cache");
        fs::create_dir_all(&stage_dir)?;
        fs::create_dir_all(&cache_dir)?;
        let decoded_budget = if config.segment_cache_bytes > 0 {
            config.segment_cache_bytes
        } else {
            DECODED_CACHE_DEFAULT_BYTES
        };
        Ok(Self {
            store,
            handle,
            root_prefix: prefix,
            stage_dir,
            cache_dir,
            cache_limit: config.segment_cache_bytes,
            cache_state: Arc::new(Mutex::new(SegmentCache::default())),
            cache_fill_inflight: Arc::new(Mutex::new(HashSet::new())),
            stage_state: Mutex::new(StageState::default()),
            log_storage_io: config.log_storage_io,
            segment_compression: config.segment_compression,
            segment_encryption_key: config.segment_encryption_key.clone(),
            decoded_cache: Mutex::new(DecodedExtentCache::new(decoded_budget)),
        })
    }

    fn log_backing(&self, args: fmt::Arguments<'_>) {
        if self.log_storage_io {
            info!(target: "backing", "{}", args);
        } else {
            debug!(target: "backing", "{}", args);
        }
    }

    fn log_cache(&self, args: fmt::Arguments<'_>) {
        if self.log_storage_io {
            info!(target: "cache", "{}", args);
        } else {
            debug!(target: "cache", "{}", args);
        }
    }

    fn ensure_active_stage<'a>(&'a self, state: &'a mut StageState) -> Result<&'a mut ActiveStage> {
        if state.active.is_none() {
            state.active = Some(ActiveStage::new(&self.stage_dir)?);
        }
        Ok(state.active.as_mut().expect("active stage must exist"))
    }

    fn segment_codec_config(&self) -> InlineCodecConfig {
        InlineCodecConfig {
            compression: self.segment_compression,
            encryption_key: self.segment_encryption_key.clone(),
        }
    }

    pub fn write_batch(
        &self,
        generation: u64,
        segment_id: u64,
        entries: Vec<SegmentEntry>,
    ) -> Result<Vec<(u64, SegmentPointer)>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let entry_count = entries.len();
        let mut estimated_size = 4usize + 8usize;
        for entry in &entries {
            estimated_size = estimated_size
                .saturating_add(8)
                .saturating_add(8)
                .saturating_add(entry.path.len())
                .saturating_add(SEGMENT_ENTRY_CODEC_HEADER_LEN);
            match &entry.payload {
                SegmentPayload::Bytes(bytes) => {
                    estimated_size = estimated_size.saturating_add(bytes.len());
                }
                SegmentPayload::SharedBytes(bytes) => {
                    estimated_size = estimated_size.saturating_add(bytes.len());
                }
                SegmentPayload::Staged(chunks) => {
                    let total_len: usize = chunks.iter().map(|chunk| chunk.len as usize).sum();
                    estimated_size = estimated_size.saturating_add(total_len);
                }
            }
        }
        let codec_config = self.segment_codec_config();
        let use_parallel = self.should_parallel_encode(entry_count, estimated_size);
        let mut buffer = Vec::with_capacity(estimated_size);
        buffer.extend_from_slice(SEGMENT_MAGIC_V2);
        buffer.extend_from_slice(&(entry_count as u64).to_le_bytes());
        let mut pointers = Vec::with_capacity(entry_count);
        if use_parallel {
            let encoded_entries =
                self.encode_entries_parallel(entries, &codec_config, entry_count)?;
            for encoded_entry in encoded_entries {
                buffer.extend_from_slice(&encoded_entry.inode.to_le_bytes());
                let path_bytes = encoded_entry.path.as_bytes();
                buffer.extend_from_slice(&(path_bytes.len() as u64).to_le_bytes());
                buffer.extend_from_slice(path_bytes);
                let offset = buffer.len() as u64;
                buffer.push(codec_to_u8(encoded_entry.encoded.codec));
                buffer.extend_from_slice(&encoded_entry.plain_len.to_le_bytes());
                buffer
                    .extend_from_slice(&(encoded_entry.encoded.payload.len() as u64).to_le_bytes());
                buffer.extend_from_slice(&encoded_entry.encoded.nonce.unwrap_or([0u8; 12]));
                buffer.extend_from_slice(&encoded_entry.encoded.payload);
                pointers.push((
                    encoded_entry.inode,
                    SegmentPointer {
                        segment_id,
                        generation,
                        offset,
                        length: (SEGMENT_ENTRY_CODEC_HEADER_LEN
                            + encoded_entry.encoded.payload.len())
                            as u64,
                    },
                ));
            }
        } else {
            for entry in entries {
                let plain_bytes = match entry.payload {
                    SegmentPayload::Bytes(bytes) => bytes,
                    SegmentPayload::SharedBytes(bytes) => bytes.as_ref().to_vec(),
                    SegmentPayload::Staged(chunks) => {
                        let total_len = chunks.iter().map(|chunk| chunk.len).sum();
                        self.read_staged_chunks(&chunks, total_len)?
                    }
                };
                let plain_len = plain_bytes.len() as u64;
                let encoded = encode_bytes(&plain_bytes, &codec_config).with_context(|| {
                    format!("segment payload encoding failed for inode {}", entry.inode)
                })?;
                buffer.extend_from_slice(&entry.inode.to_le_bytes());
                let path_bytes = entry.path.as_bytes();
                buffer.extend_from_slice(&(path_bytes.len() as u64).to_le_bytes());
                buffer.extend_from_slice(path_bytes);
                let offset = buffer.len() as u64;
                buffer.push(codec_to_u8(encoded.codec));
                buffer.extend_from_slice(&plain_len.to_le_bytes());
                buffer.extend_from_slice(&(encoded.payload.len() as u64).to_le_bytes());
                buffer.extend_from_slice(&encoded.nonce.unwrap_or([0u8; 12]));
                buffer.extend_from_slice(&encoded.payload);
                pointers.push((
                    entry.inode,
                    SegmentPointer {
                        segment_id,
                        generation,
                        offset,
                        length: (SEGMENT_ENTRY_CODEC_HEADER_LEN + encoded.payload.len()) as u64,
                    },
                ));
            }
        }
        let total_bytes = buffer.len();
        let object_path = self.segment_path(generation, segment_id);
        let object_path_clone = object_path.clone();
        let store = self.store.clone();
        let payload = Bytes::from(buffer);
        self.run_store(
            async move {
                store
                    .put(&object_path_clone, PutPayload::from_bytes(payload))
                    .await
                    .map(|_| ())
            },
            || format!("writing segment {}", object_path),
        )?;
        // Populate the local cache asynchronously after the object-store write
        // succeeds.  Moving the cache write off the flush critical path removes
        // a synchronous double-write of the full segment buffer (e.g. 128 MiB
        // for a checkpoint) that was previously blocking the fsync return.
        if self.cache_limit > 0 {
            self.enqueue_cache_fill(generation, segment_id);
        }
        self.log_backing(format_args!(
            "synced backing file path={} type=segment generation={} segment_id={} entries={} bytes={}",
            object_path.to_string(),
            generation,
            segment_id,
            pointers.len(),
            total_bytes
        ));
        Ok(pointers)
    }

    fn should_parallel_encode(&self, entry_count: usize, estimated_size: usize) -> bool {
        let max_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(8);
        let approx_input_bytes = estimated_size.saturating_sub(12);
        max_workers > 1 && entry_count >= 64 && approx_input_bytes >= 2 * 1024 * 1024
    }

    fn encode_entries_parallel(
        &self,
        entries: Vec<SegmentEntry>,
        codec_config: &InlineCodecConfig,
        entry_count: usize,
    ) -> Result<Vec<EncodedSegmentEntry>> {
        let max_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(8);
        let workers = max_workers.min(entry_count);
        let chunk_size = entry_count.div_ceil(workers);
        let mut chunks: Vec<Vec<SegmentEntry>> = Vec::with_capacity(workers);
        let mut iter = entries.into_iter();
        loop {
            let chunk: Vec<SegmentEntry> = iter.by_ref().take(chunk_size).collect();
            if chunk.is_empty() {
                break;
            }
            chunks.push(chunk);
        }

        let mut handles = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let codec_config = codec_config.clone();
            let stage_dir = self.stage_dir.clone();
            handles.push(std::thread::spawn(
                move || -> Result<Vec<EncodedSegmentEntry>> {
                    let mut out = Vec::with_capacity(chunk.len());
                    for entry in chunk {
                        let plain_bytes = match entry.payload {
                            SegmentPayload::Bytes(bytes) => bytes,
                            SegmentPayload::SharedBytes(bytes) => bytes.as_ref().to_vec(),
                            SegmentPayload::Staged(chunks) => {
                                let total_len = chunks.iter().map(|chunk| chunk.len).sum();
                                read_staged_chunks_from_disk(&stage_dir, &chunks, total_len)?
                            }
                        };
                        let plain_len = plain_bytes.len() as u64;
                        let encoded =
                            encode_bytes(&plain_bytes, &codec_config).with_context(|| {
                                format!("segment payload encoding failed for inode {}", entry.inode)
                            })?;
                        out.push(EncodedSegmentEntry {
                            inode: entry.inode,
                            path: entry.path,
                            plain_len,
                            encoded,
                        });
                    }
                    Ok(out)
                },
            ));
        }

        let mut out = Vec::with_capacity(entry_count);
        for handle in handles {
            let chunk_entries = handle
                .join()
                .map_err(|_| anyhow::anyhow!("segment encode worker panicked"))??;
            out.extend(chunk_entries);
        }
        Ok(out)
    }

    pub fn read_pointer(&self, pointer: &SegmentPointer) -> Result<Vec<u8>> {
        let arc = self.read_pointer_arc(pointer)?;
        Ok(Arc::unwrap_or_clone(arc))
    }

    /// Read and decode a segment extent, returning an `Arc` so callers can
    /// slice the decoded bytes without copying the full extent.  Results are
    /// kept in an in-memory LRU cache keyed by pointer coordinates.
    pub fn read_pointer_arc(&self, pointer: &SegmentPointer) -> Result<Arc<Vec<u8>>> {
        let key = (
            pointer.generation,
            pointer.segment_id,
            pointer.offset,
            pointer.length,
        );

        // Fast path: decoded LRU cache hit.
        if let Some(cached) = self.decoded_cache.lock().get(&key) {
            return Ok(cached);
        }

        // Slow path: on-disk cache or object store fetch + decode.
        let decoded = if let Some(bytes) = self.read_from_cache(pointer)? {
            bytes
        } else {
            let range_end = pointer
                .offset
                .checked_add(pointer.length)
                .context("segment pointer range overflow")?;
            let entry = self.fetch_segment_range(
                pointer.generation,
                pointer.segment_id,
                pointer.offset..range_end,
            )?;
            self.enqueue_cache_fill(pointer.generation, pointer.segment_id);
            self.decode_pointer_entry(&entry)?
        };

        let arc = Arc::new(decoded);
        self.decoded_cache.lock().put(key, arc.clone());
        Ok(arc)
    }

    fn decode_pointer_from_segment(
        &self,
        full: &[u8],
        pointer: &SegmentPointer,
    ) -> Result<Vec<u8>> {
        if full.len() < 4 {
            anyhow::bail!("segment too small");
        }
        if !full.starts_with(SEGMENT_MAGIC_V2) {
            anyhow::bail!("unsupported segment magic");
        }
        let start = pointer.offset as usize;
        let end = start + pointer.length as usize;
        anyhow::ensure!(end <= full.len(), "segment pointer out of bounds");
        let slice = &full[start..end];
        self.decode_pointer_entry(slice)
    }

    fn decode_pointer_entry(&self, slice: &[u8]) -> Result<Vec<u8>> {
        anyhow::ensure!(
            slice.len() >= SEGMENT_ENTRY_CODEC_HEADER_LEN,
            "segment entry too small for codec header"
        );
        let codec = codec_from_u8(slice[0])?;
        let plain_len = u64::from_le_bytes(
            slice[1..9]
                .try_into()
                .expect("slice length already validated"),
        );
        let encoded_len = u64::from_le_bytes(
            slice[9..17]
                .try_into()
                .expect("slice length already validated"),
        ) as usize;
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&slice[17..29]);
        anyhow::ensure!(
            SEGMENT_ENTRY_CODEC_HEADER_LEN + encoded_len == slice.len(),
            "segment encoded length mismatch expected={} actual={}",
            encoded_len,
            slice.len().saturating_sub(SEGMENT_ENTRY_CODEC_HEADER_LEN)
        );
        let nonce_opt = if matches!(
            codec,
            InlinePayloadCodec::ChaCha20Poly1305 | InlinePayloadCodec::Lz4ChaCha20Poly1305
        ) {
            Some(nonce)
        } else {
            None
        };
        let original_len = matches!(
            codec,
            InlinePayloadCodec::Lz4 | InlinePayloadCodec::Lz4ChaCha20Poly1305
        )
        .then_some(plain_len);
        decode_bytes(
            codec,
            &slice[SEGMENT_ENTRY_CODEC_HEADER_LEN..],
            original_len,
            nonce_opt,
            self.segment_encryption_key.as_deref(),
        )
    }

    pub fn stage_payload(&self, data: &[u8]) -> Result<StagedChunk> {
        let mut state = self.stage_state.lock();
        let stage = self.ensure_active_stage(&mut state)?;
        let offset = stage.append(data)?;
        let chunk = StagedChunk {
            path: stage.path.clone(),
            offset,
            len: data.len() as u64,
        };
        *state.ref_counts.entry(chunk.path.clone()).or_insert(0) += 1;
        Ok(chunk)
    }

    pub fn slice_staged_chunk(
        &self,
        chunk: &StagedChunk,
        offset: u64,
        len: u64,
    ) -> Result<StagedChunk> {
        anyhow::ensure!(offset <= chunk.len, "chunk slice offset beyond bounds");
        anyhow::ensure!(offset + len <= chunk.len, "chunk slice exceeds length");
        anyhow::ensure!(len > 0, "chunk slice length must be positive");
        let mut state = self.stage_state.lock();
        *state.ref_counts.entry(chunk.path.clone()).or_insert(0) += 1;
        Ok(StagedChunk {
            path: chunk.path.clone(),
            offset: chunk.offset + offset,
            len,
        })
    }

    pub fn rotate_stage_file(&self) {
        let mut state = self.stage_state.lock();
        if let Some(mut active) = state.active.take() {
            if let Err(err) = active.sync() {
                warn!(
                    "failed to sync staged segment {}: {err:?}",
                    active.path.display()
                );
            }
        }
    }

    pub fn read_staged_chunk(&self, chunk: &StagedChunk) -> Result<Vec<u8>> {
        let mut file = File::open(&chunk.path)
            .with_context(|| format!("opening staged payload {}", chunk.path.display()))?;
        file.seek(SeekFrom::Start(chunk.offset))?;
        let mut buffer = vec![0u8; chunk.len as usize];
        file.read_exact(&mut buffer)
            .with_context(|| format!("reading staged payload {}", chunk.path.display()))?;
        Ok(buffer)
    }

    pub fn read_staged_chunks(&self, chunks: &[StagedChunk], total_len: u64) -> Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(total_len as usize);
        let appended = self.append_staged_chunks(&mut buffer, chunks)?;
        anyhow::ensure!(
            appended == total_len,
            "staged payload length mismatch expected={} actual={}",
            total_len,
            appended
        );
        Ok(buffer)
    }

    pub fn sync_staged_chunks(&self, chunks: &[StagedChunk]) -> Result<()> {
        let mut synced = HashSet::new();
        for chunk in chunks {
            if !synced.insert(chunk.path.clone()) {
                continue;
            }
            let file = OpenOptions::new()
                .read(true)
                .open(&chunk.path)
                .with_context(|| format!("opening staged payload {}", chunk.path.display()))?;
            file.sync_data()
                .with_context(|| format!("syncing staged payload {}", chunk.path.display()))?;
        }
        self.sync_stage_dir()
    }

    pub fn release_staged_chunk(&self, chunk: &StagedChunk) -> Result<()> {
        let mut state = self.stage_state.lock();
        if let Some(count) = state.ref_counts.get_mut(&chunk.path) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                state.ref_counts.remove(&chunk.path);
                if let Some(active) = state.active.as_mut() {
                    if active.path == chunk.path {
                        active.reset()?;
                        return Ok(());
                    }
                }
                let _ = fs::remove_file(&chunk.path);
            }
        }
        Ok(())
    }

    fn sync_stage_dir(&self) -> Result<()> {
        let dir = OpenOptions::new()
            .read(true)
            .open(&self.stage_dir)
            .with_context(|| format!("opening stage dir {}", self.stage_dir.display()))?;
        dir.sync_all()
            .with_context(|| format!("syncing stage dir {}", self.stage_dir.display()))
    }

    pub fn prefetch_segment(&self, pointer: &SegmentPointer) -> Result<()> {
        if self.cache_limit == 0 {
            return Ok(());
        }
        if self
            .cache_path(pointer.generation, pointer.segment_id)
            .exists()
        {
            return Ok(());
        }
        let data = self.fetch_segment(pointer.generation, pointer.segment_id)?;
        self.write_cache_file(pointer.generation, pointer.segment_id, &data)
    }

    pub fn delete_segment(&self, generation: u64, segment_id: u64) -> Result<()> {
        let path = self.segment_path(generation, segment_id);
        let path_clone = path.clone();
        let store = self.store.clone();
        let delete_result = self
            .handle
            .block_on(async move { store.delete(&path_clone).await });
        match delete_result {
            Ok(_) => {}
            Err(ObjectError::NotFound { .. }) => {
                debug!("segment {} already removed", path);
            }
            Err(err) => {
                return Err(anyhow::Error::from(err))
                    .with_context(|| format!("deleting segment {}", path));
            }
        }
        let cache_path = self.cache_path(generation, segment_id);
        let _ = fs::remove_file(cache_path);
        Ok(())
    }

    fn segment_path(&self, generation: u64, segment_id: u64) -> ObjectPath {
        let base = self.root_prefix.trim_matches('/');
        let dir = if base.is_empty() {
            "segs".to_string()
        } else {
            format!("{}/segs", base)
        };
        ObjectPath::from(format!("{}/s_{generation:020}_{segment_id:020}", dir))
    }

    fn run_store<F, T, C>(&self, fut: F, ctx: C) -> Result<T>
    where
        F: Future<Output = object_store::Result<T>> + Send + 'static,
        C: FnOnce() -> String,
    {
        self.handle
            .block_on(fut)
            .map_err(anyhow::Error::from)
            .with_context(ctx)
    }

    fn cache_path(&self, generation: u64, segment_id: u64) -> PathBuf {
        self.cache_dir
            .join(format!("s_{generation:020}_{segment_id:020}.bin"))
    }

    fn write_cache_file(&self, generation: u64, segment_id: u64, data: &[u8]) -> Result<()> {
        let wrote = Self::write_cache_file_with_state(
            &self.cache_dir,
            self.cache_limit,
            &self.cache_state,
            generation,
            segment_id,
            data,
        )?;
        if wrote {
            let path = self.cache_path(generation, segment_id);
            self.log_cache(format_args!(
                "synced local cache path={} bytes={}",
                path.display(),
                data.len()
            ));
        }
        Ok(())
    }

    fn read_from_cache(&self, pointer: &SegmentPointer) -> Result<Option<Vec<u8>>> {
        let path = self.cache_path(pointer.generation, pointer.segment_id);
        if !path.exists() {
            return Ok(None);
        }
        let full = fs::read(path)?;
        Ok(Some(self.decode_pointer_from_segment(&full, pointer)?))
    }

    fn fetch_segment(&self, generation: u64, segment_id: u64) -> Result<Vec<u8>> {
        let path = self.segment_path(generation, segment_id);
        let store = self.store.clone();
        let path_for_fetch = path.clone();
        let bytes = self.run_store(
            async move {
                let result = store.get(&path_for_fetch).await?;
                let data = result.bytes().await?;
                Ok::<Bytes, object_store::Error>(data)
            },
            || format!("fetching segment {}", path),
        )?;
        Ok(bytes.to_vec())
    }

    fn fetch_segment_range(
        &self,
        generation: u64,
        segment_id: u64,
        range: Range<u64>,
    ) -> Result<Vec<u8>> {
        let path = self.segment_path(generation, segment_id);
        let store = self.store.clone();
        let path_for_fetch = path.clone();
        let bytes = self.run_store(
            async move { store.get_range(&path_for_fetch, range).await },
            || format!("range-fetching segment {}", path),
        )?;
        Ok(bytes.to_vec())
    }

    fn enqueue_cache_fill(&self, generation: u64, segment_id: u64) {
        if self.cache_limit == 0 {
            return;
        }
        if self.cache_path(generation, segment_id).exists() {
            return;
        }
        {
            let mut inflight = self.cache_fill_inflight.lock();
            if !inflight.insert((generation, segment_id)) {
                return;
            }
        }
        let store = self.store.clone();
        let segment_path = self.segment_path(generation, segment_id);
        let cache_dir = self.cache_dir.clone();
        let cache_limit = self.cache_limit;
        let cache_state = self.cache_state.clone();
        let inflight = self.cache_fill_inflight.clone();
        let log_storage_io = self.log_storage_io;
        self.handle.spawn(async move {
            let result: Result<()> = async {
                let fetched = store.get(&segment_path).await?;
                let data = fetched.bytes().await?;
                SegmentManager::write_cache_file_with_state(
                    &cache_dir,
                    cache_limit,
                    &cache_state,
                    generation,
                    segment_id,
                    data.as_ref(),
                )?;
                if log_storage_io {
                    info!(
                        target: "cache",
                        "synced local cache path={} bytes={}",
                        SegmentManager::cache_path_for(&cache_dir, generation, segment_id).display(),
                        data.len()
                    );
                } else {
                    debug!(
                        target: "cache",
                        "synced local cache path={} bytes={}",
                        SegmentManager::cache_path_for(&cache_dir, generation, segment_id).display(),
                        data.len()
                    );
                }
                Ok::<(), anyhow::Error>(())
            }
            .await
            .with_context(|| {
                format!(
                    "prefetching segment {} generation={} segment_id={}",
                    segment_path, generation, segment_id
                )
            });
            if let Err(err) = result {
                warn!(
                    "segment cache prefetch failed generation={} segment_id={}: {err:#}",
                    generation, segment_id
                );
            }
            inflight.lock().remove(&(generation, segment_id));
        });
    }

    fn cache_path_for(cache_dir: &Path, generation: u64, segment_id: u64) -> PathBuf {
        cache_dir.join(format!("s_{generation:020}_{segment_id:020}.bin"))
    }

    fn write_cache_file_with_state(
        cache_dir: &Path,
        cache_limit: u64,
        cache_state: &Mutex<SegmentCache>,
        generation: u64,
        segment_id: u64,
        data: &[u8],
    ) -> Result<bool> {
        if cache_limit == 0 {
            return Ok(false);
        }
        let path = Self::cache_path_for(cache_dir, generation, segment_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, data)?;
        let mut cache = cache_state.lock();
        cache.entries.push_back((path.clone(), data.len() as u64));
        cache.total_bytes = cache.total_bytes.saturating_add(data.len() as u64);
        while cache.total_bytes > cache_limit {
            if let Some((old_path, size)) = cache.entries.pop_front() {
                cache.total_bytes = cache.total_bytes.saturating_sub(size);
                let _ = fs::remove_file(old_path);
            } else {
                break;
            }
        }
        Ok(true)
    }

    fn append_staged_chunks(&self, buffer: &mut Vec<u8>, chunks: &[StagedChunk]) -> Result<u64> {
        let mut appended = 0u64;
        let mut open_file: Option<(PathBuf, File)> = None;
        for chunk in chunks {
            if chunk.len == 0 {
                continue;
            }
            let needs_open = match open_file.as_ref() {
                Some((path, _)) => path != &chunk.path,
                None => true,
            };
            if needs_open {
                let file = File::open(&chunk.path)
                    .with_context(|| format!("opening staged payload {}", chunk.path.display()))?;
                open_file = Some((chunk.path.clone(), file));
            }
            let (_, file) = open_file.as_mut().expect("staged file must be open");
            file.seek(SeekFrom::Start(chunk.offset))?;
            let start = buffer.len();
            buffer.resize(start + chunk.len as usize, 0);
            file.read_exact(&mut buffer[start..]).with_context(|| {
                format!(
                    "reading staged payload {} @{}+{}",
                    chunk.path.display(),
                    chunk.offset,
                    chunk.len
                )
            })?;
            appended = appended.saturating_add(chunk.len);
        }
        Ok(appended)
    }
}

impl ActiveStage {
    fn new(stage_dir: &Path) -> Result<Self> {
        let filename = format!("stage_{}.bin", Uuid::new_v4());
        let path = stage_dir.join(filename);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("creating staged segment {}", path.display()))?;
        Ok(Self { path, file, len: 0 })
    }

    fn append(&mut self, data: &[u8]) -> Result<u64> {
        let offset = self.len;
        self.file.write_all(data)?;
        self.len += data.len() as u64;
        Ok(offset)
    }

    fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.len = 0;
        Ok(())
    }
}

fn segment_prefix(user_prefix: &str) -> String {
    let trimmed = user_prefix.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        trimmed.to_string()
    }
}

fn read_staged_chunks_from_disk(
    stage_dir: &Path,
    chunks: &[StagedChunk],
    total_len: u64,
) -> Result<Vec<u8>> {
    let mut buffer = Vec::with_capacity(total_len as usize);
    let mut open_file: Option<(PathBuf, File)> = None;
    let mut appended = 0u64;
    for chunk in chunks {
        if chunk.len == 0 {
            continue;
        }
        let needs_open = match open_file.as_ref() {
            Some((path, _)) => path != &chunk.path,
            None => true,
        };
        if needs_open {
            let resolved_path = if chunk.path.is_absolute() || chunk.path.starts_with(stage_dir) {
                chunk.path.clone()
            } else {
                stage_dir.join(&chunk.path)
            };
            let file = File::open(&resolved_path)
                .with_context(|| format!("opening staged payload {}", resolved_path.display()))?;
            open_file = Some((resolved_path, file));
        }
        let (path, file) = open_file.as_mut().expect("staged file must be open");
        file.seek(SeekFrom::Start(chunk.offset))?;
        let start = buffer.len();
        buffer.resize(start + chunk.len as usize, 0);
        file.read_exact(&mut buffer[start..]).with_context(|| {
            format!(
                "reading staged payload {} @{}+{}",
                path.display(),
                chunk.offset,
                chunk.len
            )
        })?;
        appended = appended.saturating_add(chunk.len);
    }
    anyhow::ensure!(
        appended == total_len,
        "staged payload length mismatch expected={} actual={}",
        total_len,
        appended
    );
    Ok(buffer)
}

fn codec_to_u8(codec: InlinePayloadCodec) -> u8 {
    match codec {
        InlinePayloadCodec::None => 0,
        InlinePayloadCodec::Lz4 => 1,
        InlinePayloadCodec::ChaCha20Poly1305 => 2,
        InlinePayloadCodec::Lz4ChaCha20Poly1305 => 3,
    }
}

fn codec_from_u8(raw: u8) -> Result<InlinePayloadCodec> {
    match raw {
        0 => Ok(InlinePayloadCodec::None),
        1 => Ok(InlinePayloadCodec::Lz4),
        2 => Ok(InlinePayloadCodec::ChaCha20Poly1305),
        3 => Ok(InlinePayloadCodec::Lz4ChaCha20Poly1305),
        _ => anyhow::bail!("unknown segment codec id {}", raw),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, ObjectStoreProvider};
    use std::path::Path;
    use tempfile::tempdir;

    fn build_config(root: &Path) -> Config {
        Config {
            mount_path: root.join("mnt"),
            store_path: root.join("data"),
            local_cache_path: root.join("cache"),
            inline_threshold: 1024,
            inline_compression: true,
            inline_encryption_key: None,
            segment_compression: true,
            segment_encryption_key: None,
            shard_size: 1024,
            inode_batch: 16,
            segment_batch: 32,
            pending_bytes: 1024 * 1024,
            object_provider: ObjectStoreProvider::Local,
            bucket: None,
            region: None,
            endpoint: None,
            object_prefix: "".into(),
            gcs_service_account: None,
            state_path: root.join("state.bin"),
            foreground: false,
            allow_other: false,
            home_prefix: "/home".into(),
            perf_log: None,
            replay_log: None,
            disable_journal: false,
            fsync_on_close: false,
            flush_interval_ms: 0,
            disable_cleanup: false,
            lookup_cache_ttl_ms: 0,
            dir_cache_ttl_ms: 0,
            metadata_poll_interval_ms: 0,
            segment_cache_bytes: 0,
            log_file: None,
            debug_log: false,
            imap_delta_batch: 32,
            log_storage_io: false,
        }
    }

    #[test]
    fn write_and_read_segment_pointer() {
        let dir = tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let config = build_config(dir.path());
        let manager = SegmentManager::new(&config, handle).unwrap();
        let entries = vec![SegmentEntry {
            inode: 42,
            path: "/foo.txt".into(),
            payload: SegmentPayload::Bytes(b"hello world".to_vec()),
        }];
        let pointers = manager.write_batch(7, 1, entries).unwrap();
        assert_eq!(pointers.len(), 1);
        let ptr = &pointers[0].1;
        let bytes = manager.read_pointer(ptr).unwrap();
        assert_eq!(bytes, b"hello world");
    }

    #[test]
    fn range_read_enqueues_cache_fill() {
        let dir = tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let mut config = build_config(dir.path());
        config.segment_cache_bytes = 4 * 1024 * 1024;
        let manager = SegmentManager::new(&config, handle).unwrap();
        let entries = vec![SegmentEntry {
            inode: 7,
            path: "/bar.txt".into(),
            payload: SegmentPayload::Bytes(b"range read data".to_vec()),
        }];
        let pointers = manager.write_batch(3, 9, entries).unwrap();
        let ptr = &pointers[0].1;
        let bytes = manager.read_pointer(ptr).unwrap();
        assert_eq!(bytes, b"range read data");

        let cache_path = manager.cache_path(ptr.generation, ptr.segment_id);
        for _ in 0..20 {
            if cache_path.exists() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("expected cache prefetch to create {}", cache_path.display());
    }
}
