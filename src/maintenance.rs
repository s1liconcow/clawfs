use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::inode::{FileStorage, InodeRecord, SegmentExtent};
use crate::metadata::MetadataStore;
use crate::segment::{SegmentEntry, SegmentManager, SegmentPayload};
use crate::superblock::{CleanupTaskKind, SuperblockManager};

const DEFAULT_DELTA_COMPACT_THRESHOLD: usize = 128;
const DEFAULT_DELTA_COMPACT_KEEP: usize = 32;
const DEFAULT_SEGMENT_COMPACT_BATCH: usize = 8;
const DEFAULT_SEGMENT_COMPACT_LAG: u64 = 3;
const DEFAULT_LEASE_TTL_SECS: i64 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionConfig {
    pub delta_compact_threshold: usize,
    pub delta_compact_keep: usize,
    pub segment_compact_batch: usize,
    pub segment_compact_lag: u64,
    pub lease_ttl_secs: i64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            delta_compact_threshold: DEFAULT_DELTA_COMPACT_THRESHOLD,
            delta_compact_keep: DEFAULT_DELTA_COMPACT_KEEP,
            segment_compact_batch: DEFAULT_SEGMENT_COMPACT_BATCH,
            segment_compact_lag: DEFAULT_SEGMENT_COMPACT_LAG,
            lease_ttl_secs: DEFAULT_LEASE_TTL_SECS,
        }
    }
}

impl CompactionConfig {
    pub fn lease_ttl(&self) -> Duration {
        Duration::from_secs(self.lease_ttl_secs.max(0) as u64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionResult {
    pub deltas_pruned: usize,
    pub segments_merged: usize,
    pub bytes_rewritten: u64,
    pub duration: Duration,
}

impl CompactionResult {
    fn new(
        deltas_pruned: usize,
        segments_merged: usize,
        bytes_rewritten: u64,
        duration: Duration,
    ) -> Self {
        Self {
            deltas_pruned,
            segments_merged,
            bytes_rewritten,
            duration,
        }
    }
}

pub async fn acquire_cleanup_lease(
    sb_mgr: &SuperblockManager,
    kind: CleanupTaskKind,
    client_id: &str,
    config: &CompactionConfig,
) -> Result<bool> {
    sb_mgr
        .try_acquire_cleanup(kind, client_id, config.lease_ttl())
        .await
}

pub async fn release_cleanup_lease(
    sb_mgr: &SuperblockManager,
    kind: CleanupTaskKind,
    client_id: &str,
) -> Result<()> {
    sb_mgr.complete_cleanup(kind, client_id).await
}

pub async fn run_delta_compaction(
    meta: &MetadataStore,
    config: &CompactionConfig,
) -> Result<CompactionResult> {
    let start = Instant::now();
    let delta_count = meta.delta_file_count()?;

    if delta_count <= config.delta_compact_threshold {
        return Ok(CompactionResult::new(0, 0, 0, start.elapsed()));
    }

    let pruned = meta.prune_deltas(config.delta_compact_keep)?;

    Ok(CompactionResult::new(pruned, 0, 0, start.elapsed()))
}

pub async fn run_segment_compaction(
    meta: &MetadataStore,
    segments: &SegmentManager,
    config: &CompactionConfig,
) -> Result<CompactionResult> {
    let start = Instant::now();
    let mut total_segments_merged = 0usize;
    let mut total_bytes_rewritten = 0u64;

    loop {
        let current = meta
            .load_superblock()
            .await?
            .context("missing superblock for segment compaction")?;
        let current_generation = current.block.generation;
        let cutoff_generation = current_generation.saturating_sub(config.segment_compact_lag);
        if cutoff_generation == 0 {
            break;
        }

        let candidates = meta
            .segment_candidates(config.segment_compact_batch)?
            .into_iter()
            .filter(|record| {
                record
                    .segment_pointer()
                    .map(|ptr| ptr.generation < cutoff_generation)
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();

        if candidates.len() < 2 {
            break;
        }

        let (merged, bytes) = compact_segment_batch(meta, segments, current, candidates).await?;
        if merged == 0 {
            break;
        }
        total_segments_merged = total_segments_merged.saturating_add(merged);
        total_bytes_rewritten = total_bytes_rewritten.saturating_add(bytes);
    }

    Ok(CompactionResult::new(
        0,
        total_segments_merged,
        total_bytes_rewritten,
        start.elapsed(),
    ))
}

async fn compact_segment_batch(
    meta: &MetadataStore,
    segments: &SegmentManager,
    current: crate::metadata::VersionedSuperblock,
    candidates: Vec<InodeRecord>,
) -> Result<(usize, u64)> {
    if candidates.len() < 2 {
        return Ok((0, 0));
    }

    let dataset = {
        let mut out = Vec::new();
        for record in candidates {
            match record.storage.clone() {
                FileStorage::LegacySegment(ptr) => {
                    let data = segments.read_pointer(&ptr)?;
                    out.push((record, vec![ptr], data));
                }
                FileStorage::Segments(extents) => {
                    let mut buffer = vec![0u8; record.size as usize];
                    let mut pointers = Vec::new();
                    for extent in extents {
                        let chunk = segments.read_pointer(&extent.pointer)?;
                        let start = extent.logical_offset as usize;
                        let end = start + chunk.len();
                        if end > buffer.len() {
                            buffer.resize(end, 0);
                        }
                        buffer[start..end].copy_from_slice(&chunk);
                        pointers.push(extent.pointer);
                    }
                    out.push((record, pointers, buffer));
                }
                FileStorage::Inline(_)
                | FileStorage::InlineEncoded(_)
                | FileStorage::ExternalObject(_) => {}
            }
        }
        out
    };

    if dataset.is_empty() {
        return Ok((0, 0));
    }

    let expected_generation = current.block.generation;
    let generation = expected_generation.saturating_add(1);
    let segment_id = current.block.next_segment;

    let mut entries = Vec::with_capacity(dataset.len());
    let mut bytes_rewritten = 0u64;
    for (record, _, data) in &dataset {
        bytes_rewritten = bytes_rewritten.saturating_add(data.len() as u64);
        entries.push(SegmentEntry {
            inode: record.inode,
            path: record.path.clone(),
            logical_offset: 0,
            payload: SegmentPayload::Bytes(data.clone()),
        });
    }

    let pointer_map: HashMap<u64, Vec<SegmentExtent>> = segments
        .write_batch(generation, segment_id, entries)
        .map(|res| {
            let mut map: HashMap<u64, Vec<SegmentExtent>> = HashMap::new();
            for (inode, extent) in res {
                map.entry(inode).or_default().push(extent);
            }
            map
        })?;

    let merged_count = dataset.len();
    let mut segments_to_delete = HashSet::new();
    for (mut record, old_ptrs, _) in dataset {
        if let Some(new_extents) = pointer_map.get(&record.inode) {
            record.storage = FileStorage::Segments(new_extents.clone());
            meta.persist_inode(&record, generation, current.block.shard_size)
                .await?;
            for ptr in old_ptrs {
                segments_to_delete.insert((ptr.generation, ptr.segment_id));
            }
        }
    }

    for (generation, seg_id) in segments_to_delete {
        segments.delete_segment(generation, seg_id)?;
    }

    let mut updated = current.block.clone();
    updated.generation = generation;
    updated.next_segment = segment_id.saturating_add(1);
    updated.state = crate::superblock::FilesystemState::Clean;
    meta.compare_and_swap_superblock(expected_generation, &updated)
        .await
        .context("committing compacted superblock")?;

    Ok((merged_count, bytes_rewritten))
}

#[cfg(test)]
mod tests {
    use super::CompactionConfig;

    #[test]
    fn defaults_match_current_thresholds() {
        let config = CompactionConfig::default();

        assert_eq!(config.delta_compact_threshold, 128);
        assert_eq!(config.delta_compact_keep, 32);
        assert_eq!(config.segment_compact_batch, 8);
        assert_eq!(config.segment_compact_lag, 3);
        assert_eq!(config.lease_ttl_secs, 30);
    }
}
