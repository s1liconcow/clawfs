#[allow(clippy::crate_in_macro_def)]
#[macro_export]
macro_rules! maintenance_shared_items {
    () => {
use std::cmp::{Reverse, max};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::checkpoint::encode_checkpoint_bytes;
use crate::inode::{FileStorage, InodeRecord, SegmentExtent};
use crate::metadata::MetadataStore;
use crate::segment::{SegmentEntry, SegmentManager, SegmentPayload};
use crate::superblock::{CleanupTaskKind, SuperblockManager};

maintenance_cleanup_policy!();

const DEFAULT_DELTA_COMPACT_THRESHOLD: usize = 128;
const DEFAULT_DELTA_COMPACT_KEEP: usize = 32;
const DEFAULT_SEGMENT_COMPACT_BATCH: usize = 8;
const DEFAULT_SEGMENT_COMPACT_LAG: u64 = 3;
const DEFAULT_LEASE_TTL_SECS: i64 = 30;
const DEFAULT_CLEANUP_INTERVAL_SECS: u64 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionConfig {
    pub delta_compact_threshold: usize,
    pub delta_compact_keep: usize,
    pub segment_compact_batch: usize,
    pub segment_compact_lag: u64,
    pub lease_ttl_secs: i64,
    pub cleanup_interval: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            delta_compact_threshold: DEFAULT_DELTA_COMPACT_THRESHOLD,
            delta_compact_keep: DEFAULT_DELTA_COMPACT_KEEP,
            segment_compact_batch: DEFAULT_SEGMENT_COMPACT_BATCH,
            segment_compact_lag: DEFAULT_SEGMENT_COMPACT_LAG,
            lease_ttl_secs: DEFAULT_LEASE_TTL_SECS,
            cleanup_interval: Duration::from_secs(DEFAULT_CLEANUP_INTERVAL_SECS),
        }
    }
}

impl CompactionConfig {
    pub fn from_config(config: &crate::config::Config) -> Self {
        Self {
            cleanup_interval: Duration::from_secs(config.cleanup_interval_secs.max(1)),
            ..Self::default()
        }
    }

    pub fn lease_ttl(&self) -> Duration {
        Duration::from_secs(self.lease_ttl_secs.max(0) as u64)
    }
}

const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 24 * 60 * 60;
const DEFAULT_CHECKPOINT_MAX_COUNT: usize = 7;
const DEFAULT_CHECKPOINT_RETENTION_DAYS: u64 = 7;
const DEFAULT_LIFECYCLE_EXPIRY_DAYS: u64 = 30;
const CHECKPOINT_OBJECT_DIR: &str = "metadata/checkpoints";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointConfig {
    pub interval: Duration,
    pub max_checkpoints: usize,
    pub retention_days: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(DEFAULT_CHECKPOINT_INTERVAL_SECS),
            max_checkpoints: DEFAULT_CHECKPOINT_MAX_COUNT,
            retention_days: DEFAULT_CHECKPOINT_RETENTION_DAYS,
        }
    }
}

impl CheckpointConfig {
    pub fn is_enabled(&self) -> bool {
        !self.interval.is_zero()
    }

    pub fn retention_policy(&self) -> RetentionPolicy {
        let max_count = self.max_checkpoints.max(1);
        let min_keep = max(1, self.max_checkpoints.min(2));
        RetentionPolicy {
            max_count,
            max_age_days: self.retention_days,
            min_keep,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub max_count: usize,
    pub max_age_days: u64,
    pub min_keep: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionResult {
    pub checkpoints_seen: usize,
    pub checkpoints_deleted: usize,
    pub checkpoints_kept: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecyclePolicy {
    pub expiry_days: u64,
    pub require_confirmation: bool,
    pub allowed_prefix: String,
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        Self {
            expiry_days: DEFAULT_LIFECYCLE_EXPIRY_DAYS,
            require_confirmation: true,
            allowed_prefix: String::new(),
        }
    }
}

impl LifecyclePolicy {
    pub fn with_allowed_prefix(mut self, allowed_prefix: impl Into<String>) -> Self {
        self.allowed_prefix = allowed_prefix.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionResult {
    pub deltas_pruned: usize,
    pub segments_merged: usize,
    pub bytes_rewritten: u64,
    pub duration: Duration,
    /// Number of delta files remaining in the store after compaction.
    pub delta_backlog: usize,
    /// Number of segment candidates remaining after compaction.
    pub segment_backlog: usize,
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
            delta_backlog: 0,
            segment_backlog: 0,
        }
    }

    /// Duration in milliseconds, for structured log fields.
    pub fn duration_ms(&self) -> u64 {
        self.duration.as_millis() as u64
    }
}

fn normalize_prefix_scope(prefix: &str) -> Result<String> {
    let trimmed = prefix.trim_matches('/');
    anyhow::ensure!(!trimmed.is_empty(), "prefix must not be empty");
    anyhow::ensure!(
        !trimmed
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == ".."),
        "prefix must be a normalized path without empty segments or dot segments"
    );
    Ok(trimmed.to_string())
}

fn prefix_within_scope(requested: &str, allowed: &str) -> bool {
    requested == allowed
        || requested
            .strip_prefix(allowed)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn checkpoint_bucket_prefix(root_prefix: &str) -> String {
    let base = root_prefix.trim_matches('/');
    if base.is_empty() {
        CHECKPOINT_OBJECT_DIR.to_string()
    } else {
        format!("{base}/{CHECKPOINT_OBJECT_DIR}")
    }
}

fn checkpoint_object_name(generation: u64, created_at_unix_nanos: i128) -> String {
    format!(
        "checkpoint-g{:020}-t{:020}.bin",
        generation,
        created_at_unix_nanos.max(0)
    )
}

fn checkpoint_object_key(
    root_prefix: &str,
    generation: u64,
    created_at_unix_nanos: i128,
) -> String {
    format!(
        "{}/{}",
        checkpoint_bucket_prefix(root_prefix),
        checkpoint_object_name(generation, created_at_unix_nanos)
    )
}

fn parse_checkpoint_object_name(name: &str) -> Option<(u64, i128)> {
    let stem = name.strip_suffix(".bin")?;
    let rest = stem.strip_prefix("checkpoint-g")?;
    let (generation_str, timestamp_str) = rest.split_once("-t")?;
    let generation = generation_str.parse().ok()?;
    let timestamp = timestamp_str.parse().ok()?;
    Some((generation, timestamp))
}

fn checkpoint_within_scope(location: &str, prefix: &str) -> bool {
    location == prefix
        || location
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

#[derive(Debug, Clone)]
struct CheckpointEntry {
    location: String,
    generation: u64,
    created_at_nanos: i128,
}

pub async fn create_checkpoint(
    meta: &MetadataStore,
    config: &CheckpointConfig,
) -> Result<crate::checkpoint::CheckpointResult> {
    let superblock = meta
        .load_superblock()
        .await?
        .context("missing superblock for checkpoint creation")?
        .block;
    let now = OffsetDateTime::now_utc();
    let created_at_unix = now.unix_timestamp();
    let created_at_nanos = now.unix_timestamp_nanos();
    let checkpoint_path =
        checkpoint_object_key(meta.root_prefix(), superblock.generation, created_at_nanos);
    let payload = encode_checkpoint_bytes(&superblock, created_at_unix, Some("hosted maintenance"));
    let store = meta.object_store();
    store
        .put(
            &ObjectPath::from(checkpoint_path.clone()),
            PutPayload::from_bytes(Bytes::from(payload)),
        )
        .await
        .context("writing checkpoint object")?;
    info!(
        target: "maintenance",
        checkpoint_path = %checkpoint_path,
        generation = superblock.generation,
        next_inode = superblock.next_inode,
        next_segment = superblock.next_segment,
        checkpoint_interval_secs = config.interval.as_secs(),
        "checkpoint_created"
    );
    Ok(crate::checkpoint::CheckpointResult {
        generation: superblock.generation,
        next_inode: superblock.next_inode,
        next_segment: superblock.next_segment,
        checkpoint_path,
    })
}

pub async fn enforce_retention_policy(
    meta: &MetadataStore,
    policy: &RetentionPolicy,
) -> Result<RetentionResult> {
    let prefix = meta.checkpoint_prefix();
    let store = meta.object_store();
    let now_nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let cutoff_nanos = if policy.max_age_days == 0 {
        None
    } else {
        let cutoff = OffsetDateTime::now_utc() - time::Duration::days(policy.max_age_days as i64);
        Some(cutoff.unix_timestamp_nanos())
    };

    let mut entries = Vec::new();
    let mut stream = store.list(Some(&prefix));
    while let Some(item) = stream.next().await {
        let meta = item?;
        let location = meta.location.as_ref().trim_matches('/').to_string();
        let filename = meta.location.filename().unwrap_or_default();
        let (generation, created_at_nanos) = match parse_checkpoint_object_name(filename) {
            Some(values) => values,
            None => {
                warn!(
                    target: "maintenance",
                    checkpoint_path = %location,
                    "checkpoint_retention_skipped_unrecognized_name"
                );
                continue;
            }
        };
        if !checkpoint_within_scope(&location, prefix.as_ref().trim_matches('/')) {
            warn!(
                target: "maintenance",
                checkpoint_path = %location,
                checkpoint_prefix = %prefix,
                "checkpoint_retention_skipped_out_of_scope"
            );
            continue;
        }
        entries.push(CheckpointEntry {
            location,
            generation,
            created_at_nanos,
        });
    }

    if entries.is_empty() {
        return Ok(RetentionResult {
            checkpoints_seen: 0,
            checkpoints_deleted: 0,
            checkpoints_kept: 0,
        });
    }

    entries.sort_by_key(|entry| Reverse((entry.created_at_nanos, entry.generation)));

    let effective_max_count = policy.max_count.max(policy.min_keep.max(1));
    let mut delete_indices = BTreeSet::new();

    if entries.len() > effective_max_count {
        let excess = entries.len() - effective_max_count;
        for idx in entries.len().saturating_sub(excess)..entries.len() {
            delete_indices.insert(idx);
        }
    }

    if let Some(cutoff_nanos) = cutoff_nanos {
        for (idx, entry) in entries
            .iter()
            .enumerate()
            .skip(policy.min_keep.min(entries.len()))
        {
            if entry.created_at_nanos < cutoff_nanos {
                delete_indices.insert(idx);
            }
        }
    }

    for idx in 0..policy.min_keep.min(entries.len()) {
        delete_indices.remove(&idx);
    }

    let mut deleted = 0usize;
    for idx in delete_indices {
        let entry = &entries[idx];
        info!(
            target: "maintenance",
            checkpoint_path = %entry.location,
            age_cutoff_nanos = cutoff_nanos.unwrap_or(now_nanos),
            "checkpoint_retention_delete"
        );
        store
            .delete(&ObjectPath::from(entry.location.clone()))
            .await
            .context("deleting checkpoint object")?;
        deleted += 1;
    }

    Ok(RetentionResult {
        checkpoints_seen: entries.len(),
        checkpoints_deleted: deleted,
        checkpoints_kept: entries.len().saturating_sub(deleted),
    })
}

pub async fn cleanup_expired_prefix(
    store: &dyn ObjectStore,
    prefix: &str,
    policy: &LifecyclePolicy,
) -> Result<()> {
    anyhow::ensure!(
        policy.require_confirmation,
        "lifecycle cleanup requires explicit confirmation"
    );
    if policy.expiry_days == 0 {
        return Ok(());
    }

    let allowed_prefix = normalize_prefix_scope(&policy.allowed_prefix)?;
    let requested_prefix = normalize_prefix_scope(prefix)?;
    anyhow::ensure!(
        prefix_within_scope(&requested_prefix, &allowed_prefix),
        "requested prefix {} is outside allowed prefix {}",
        requested_prefix,
        allowed_prefix
    );

    let prefix_path = ObjectPath::from(requested_prefix.clone());
    let mut entries = Vec::new();
    let mut stream = store.list(Some(&prefix_path));
    while let Some(item) = stream.next().await {
        let meta = item?;
        let location = meta.location.as_ref().trim_matches('/').to_string();
        if !checkpoint_within_scope(&location, &requested_prefix) {
            warn!(
                target: "maintenance",
                object_path = %location,
                requested_prefix = %requested_prefix,
                "lifecycle_cleanup_skipped_out_of_scope"
            );
            continue;
        }
        entries.push((
            location,
            meta.last_modified.timestamp() as i128 * 1_000_000_000
                + meta.last_modified.timestamp_subsec_nanos() as i128,
        ));
    }

    if entries.is_empty() {
        return Ok(());
    }

    let cutoff = (OffsetDateTime::now_utc() - time::Duration::days(policy.expiry_days as i64))
        .unix_timestamp_nanos();
    let newest = entries
        .iter()
        .map(|(_, last_modified_nanos)| *last_modified_nanos)
        .max()
        .unwrap_or_default();
    if newest >= cutoff {
        return Ok(());
    }

    for (location, _) in entries {
        info!(
            target: "maintenance",
            object_path = %location,
            expiry_days = policy.expiry_days,
            "lifecycle_cleanup_delete"
        );
        store
            .delete(&ObjectPath::from(location))
            .await
            .context("deleting expired lifecycle object")?;
    }

    Ok(())
}

pub async fn acquire_cleanup_lease(
    sb_mgr: &SuperblockManager,
    kind: CleanupTaskKind,
    client_id: &str,
    config: &CompactionConfig,
) -> Result<bool> {
    let acquired = sb_mgr
        .try_acquire_cleanup(kind, client_id, config.lease_ttl())
        .await?;
    if acquired {
        info!(
            target: "maintenance",
            lease_kind = kind.as_str(),
            client_id = %client_id,
            lease_ttl_secs = config.lease_ttl_secs,
            "cleanup_lease_acquired"
        );
    } else {
        warn!(
            target: "maintenance",
            lease_kind = kind.as_str(),
            client_id = %client_id,
            "cleanup_lease_contention"
        );
    }
    Ok(acquired)
}

pub async fn release_cleanup_lease(
    sb_mgr: &SuperblockManager,
    kind: CleanupTaskKind,
    client_id: &str,
) -> Result<()> {
    sb_mgr.complete_cleanup(kind, client_id).await?;
    info!(
        target: "maintenance",
        lease_kind = kind.as_str(),
        client_id = %client_id,
        "cleanup_lease_released"
    );
    Ok(())
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
    let remaining = meta.delta_file_count().unwrap_or(0);
    let duration = start.elapsed();

    info!(
        target: "maintenance",
        deltas_pruned = pruned,
        delta_backlog = remaining,
        duration_ms = duration.as_millis() as u64,
        "compaction_completed"
    );

    let mut result = CompactionResult::new(pruned, 0, 0, duration);
    result.delta_backlog = remaining;
    Ok(result)
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

    let remaining_candidates = meta
        .segment_candidates(config.segment_compact_batch)
        .map(|c| c.len())
        .unwrap_or(0);
    let duration = start.elapsed();

    if total_segments_merged > 0 {
        info!(
            target: "maintenance",
            segments_merged = total_segments_merged,
            bytes_rewritten = total_bytes_rewritten,
            segment_backlog = remaining_candidates,
            duration_ms = duration.as_millis() as u64,
            "compaction_completed"
        );
    }

    let mut result =
        CompactionResult::new(0, total_segments_merged, total_bytes_rewritten, duration);
    result.segment_backlog = remaining_candidates;
    Ok(result)
}

pub async fn has_pending_segment_compaction_work(
    meta: &MetadataStore,
    superblock: &SuperblockManager,
    config: &CompactionConfig,
) -> Result<bool> {
    let current_generation = superblock.snapshot().generation;
    let cutoff_generation = current_generation.saturating_sub(config.segment_compact_lag);
    if cutoff_generation == 0 {
        return Ok(false);
    }

    let candidates = meta.segment_candidates(config.segment_compact_batch)?;
    let eligible = candidates
        .into_iter()
        .filter(|record| {
            record
                .segment_pointer()
                .map(|ptr| ptr.generation < cutoff_generation)
                .unwrap_or(false)
        })
        .take(2)
        .count();
    Ok(eligible >= 2)
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

    let mut updated = current.block.clone();
    updated.generation = generation;
    updated.next_segment = segment_id.saturating_add(1);
    updated.state = crate::superblock::FilesystemState::Clean;
    meta.compare_and_swap_superblock(expected_generation, &updated)
        .await
        .context("committing compacted superblock")?;

    // Delete old segments only AFTER the superblock CAS succeeds.
    // If we deleted before and the CAS failed, metadata would still reference
    // the now-deleted segments, causing permanent data corruption (404 on read).
    for (old_gen, seg_id) in segments_to_delete {
        if let Err(err) = segments.delete_segment(old_gen, seg_id) {
            warn!(
                target: "maintenance",
                generation = old_gen,
                segment_id = seg_id,
                error = %err,
                "failed to delete old segment after compaction; will be retried"
            );
        }
    }

    Ok((merged_count, bytes_rewritten))
}

// ── Shared maintenance schedule and round runner ───────────────────────────
//
// These types are used by both the single-volume `clawfs_maintenance_worker`
// binary and the org-scoped `clawfs_org_worker`.  Keeping them in this module
// avoids duplicating the orchestration logic.

/// Tracks timing state for recurring per-volume maintenance tasks.
///
/// Each volume context should own one `MaintenanceSchedule`; do not share
/// a single schedule across volumes.
#[derive(Debug)]
pub struct MaintenanceSchedule {
    pub last_checkpoint_run: Option<Instant>,
    pub last_lifecycle_run: Option<Instant>,
}

/// Fixed interval for lifecycle sweeps when not overridden by policy.
pub const LIFECYCLE_SWEEP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

impl MaintenanceSchedule {
    pub fn new() -> Self {
        Self {
            last_checkpoint_run: None,
            last_lifecycle_run: None,
        }
    }

    pub fn checkpoint_due(&self, interval: Duration) -> bool {
        if interval.is_zero() {
            return false;
        }
        match self.last_checkpoint_run {
            Some(last) => last.elapsed() >= interval,
            None => true,
        }
    }

    pub fn lifecycle_due(&self) -> bool {
        match self.last_lifecycle_run {
            Some(last) => last.elapsed() >= LIFECYCLE_SWEEP_INTERVAL,
            None => true,
        }
    }

    pub fn mark_checkpoint_ran(&mut self) {
        self.last_checkpoint_run = Some(Instant::now());
    }

    pub fn mark_lifecycle_ran(&mut self) {
        self.last_lifecycle_run = Some(Instant::now());
    }
}

impl Default for MaintenanceSchedule {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of backlog metrics produced by a single maintenance round.
#[derive(Debug, Default, Clone, Copy)]
pub struct RoundStatus {
    pub delta_backlog: usize,
    pub segment_backlog: usize,
    pub checkpoint_backlog: usize,
}

/// All inputs needed to run one maintenance round for a single volume.
pub struct MaintenanceRoundContext<'a> {
    pub metadata: &'a Arc<MetadataStore>,
    pub segments: &'a Arc<SegmentManager>,
    pub superblock: &'a Arc<SuperblockManager>,
    pub client_id: &'a str,
    pub compaction_config: &'a CompactionConfig,
    pub checkpoint_config: Option<&'a CheckpointConfig>,
    pub lifecycle_policy: Option<&'a LifecyclePolicy>,
    pub schedule: &'a mut MaintenanceSchedule,
}

/// Run one complete maintenance round for a single volume.
///
/// Runs delta compaction → segment compaction → checkpoint → lifecycle sweep,
/// skipping tasks whose lease is held by another worker.  Returns a
/// `RoundStatus` with backlog counters regardless of individual task errors.
pub async fn run_maintenance_round(ctx: MaintenanceRoundContext<'_>) -> RoundStatus {
    let MaintenanceRoundContext {
        metadata,
        segments,
        superblock,
        client_id,
        compaction_config: config,
        checkpoint_config,
        lifecycle_policy,
        schedule,
    } = ctx;

    let mut did_delta_work = false;
    let mut status = RoundStatus::default();

    // ── Delta compaction ──────────────────────────────────────────────────
    let delta_count = {
        let md = metadata.clone();
        tokio::task::spawn_blocking(move || md.delta_file_count())
            .await
            .unwrap_or(Ok(0))
            .unwrap_or(0)
    };

    if delta_count > config.delta_compact_threshold {
        match acquire_cleanup_lease(
            superblock,
            CleanupTaskKind::DeltaCompaction,
            client_id,
            config,
        )
        .await
        {
            Ok(true) => {
                match run_delta_compaction(metadata, config).await {
                    Ok(result) => {
                        info!(
                            target: "maintenance",
                            deltas_pruned = result.deltas_pruned,
                            delta_backlog = result.delta_backlog,
                            duration_ms = result.duration_ms(),
                            "delta_compaction_complete"
                        );
                        did_delta_work = result.deltas_pruned > 0;
                        status.delta_backlog = result.delta_backlog;
                    }
                    Err(err) => {
                        tracing::error!(
                            target: "maintenance",
                            kind = "delta_compaction",
                            error = %err,
                            "compaction_failed"
                        );
                    }
                }
                if let Err(err) =
                    release_cleanup_lease(superblock, CleanupTaskKind::DeltaCompaction, client_id)
                        .await
                {
                    warn!("failed to release delta compaction lease: {err:?}");
                }
            }
            Ok(false) => {} // contention logged inside acquire_cleanup_lease
            Err(err) => tracing::error!(
                target: "maintenance",
                kind = "delta_compaction",
                error = %err,
                "lease_acquire_failed"
            ),
        }
    }

    // ── Segment compaction ────────────────────────────────────────────────
    // Only run if delta didn't run this cycle (avoid two heavy I/O rounds).
    if !did_delta_work {
        match has_pending_segment_compaction_work(metadata, superblock, config).await {
            Ok(true) => {
                match acquire_cleanup_lease(
                    superblock,
                    CleanupTaskKind::SegmentCompaction,
                    client_id,
                    config,
                )
                .await
                {
                    Ok(true) => {
                        match run_segment_compaction(metadata, segments, config).await {
                            Ok(result) => {
                                info!(
                                    target: "maintenance",
                                    segments_merged = result.segments_merged,
                                    bytes_rewritten = result.bytes_rewritten,
                                    segment_backlog = result.segment_backlog,
                                    duration_ms = result.duration_ms(),
                                    "segment_compaction_complete"
                                );
                                status.segment_backlog = result.segment_backlog;
                            }
                            Err(err) => {
                                tracing::error!(
                                    target: "maintenance",
                                    kind = "segment_compaction",
                                    error = %err,
                                    "compaction_failed"
                                );
                            }
                        }
                        if let Err(err) = release_cleanup_lease(
                            superblock,
                            CleanupTaskKind::SegmentCompaction,
                            client_id,
                        )
                        .await
                        {
                            warn!("failed to release segment compaction lease: {err:?}");
                        }
                    }
                    Ok(false) => {}
                    Err(err) => tracing::error!(
                        target: "maintenance",
                        kind = "segment_compaction",
                        error = %err,
                        "lease_acquire_failed"
                    ),
                }
            }
            Ok(false) => {}
            Err(err) => tracing::error!(
                target: "maintenance",
                kind = "segment_compaction_preflight",
                error = %err,
                "compaction_preflight_failed"
            ),
        }
    }

    // ── Checkpoint ────────────────────────────────────────────────────────
    if let Some(ckpt_cfg) = checkpoint_config.filter(|c| c.is_enabled())
        && schedule.checkpoint_due(ckpt_cfg.interval)
    {
        match create_checkpoint(metadata, ckpt_cfg).await {
            Ok(saved) => {
                schedule.mark_checkpoint_ran();
                match enforce_retention_policy(metadata, &ckpt_cfg.retention_policy()).await {
                    Ok(retention) => {
                        info!(
                            target: "maintenance",
                            checkpoint_path = %saved.checkpoint_path,
                            checkpoints_seen = retention.checkpoints_seen,
                            checkpoints_deleted = retention.checkpoints_deleted,
                            checkpoints_kept = retention.checkpoints_kept,
                            "checkpoint_retention_complete"
                        );
                        status.checkpoint_backlog = retention.checkpoints_kept;
                    }
                    Err(err) => tracing::error!(
                        target: "maintenance",
                        kind = "checkpoint_retention",
                        error = %err,
                        "retention_failed"
                    ),
                }
            }
            Err(err) => tracing::error!(
                target: "maintenance",
                kind = "checkpoint_creation",
                error = %err,
                "checkpoint_failed"
            ),
        }
    }

    // ── Lifecycle sweep ───────────────────────────────────────────────────
    if let Some(policy) = lifecycle_policy.filter(|p| p.expiry_days > 0)
        && schedule.lifecycle_due()
    {
        let store = metadata.object_store();
        match cleanup_expired_prefix(store.as_ref(), metadata.root_prefix(), policy).await {
            Ok(()) => {
                schedule.mark_lifecycle_ran();
                info!(
                    target: "maintenance",
                    prefix = %metadata.root_prefix(),
                    expiry_days = policy.expiry_days,
                    "lifecycle_cleanup_complete"
                );
            }
            Err(err) => tracing::error!(
                target: "maintenance",
                kind = "lifecycle_cleanup",
                error = %err,
                "lifecycle_cleanup_failed"
            ),
        }
    }

    status
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use super::{
        CheckpointConfig, CleanupPolicy, CompactionConfig, LifecyclePolicy, checkpoint_object_key,
        normalize_prefix_scope, prefix_within_scope,
    };
    maintenance_test_imports!();
    use crate::config::Config;

    fn base_config() -> Config {
        Config::with_paths(
            PathBuf::from("/tmp/mnt"),
            PathBuf::from("/tmp/store"),
            PathBuf::from("/tmp/cache"),
            PathBuf::from("/tmp/state"),
        )
    }

    #[test]
    fn defaults_match_current_thresholds() {
        let config = CompactionConfig::default();

        assert_eq!(config.delta_compact_threshold, 128);
        assert_eq!(config.delta_compact_keep, 32);
        assert_eq!(config.segment_compact_batch, 8);
        assert_eq!(config.segment_compact_lag, 3);
        assert_eq!(config.lease_ttl_secs, 30);
        assert_eq!(config.cleanup_interval, Duration::from_secs(30));
    }

    #[test]
    fn compaction_config_uses_configured_cleanup_interval() {
        let mut raw = base_config();
        raw.cleanup_interval_secs = 45;

        let config = CompactionConfig::from_config(&raw);
        assert_eq!(config.cleanup_interval, Duration::from_secs(45));
    }

    #[test]
    fn cleanup_policy_no_accelerator_is_local() {
        let config = base_config();
        assert_eq!(CleanupPolicy::from_config(&config), CleanupPolicy::Local);
        assert!(CleanupPolicy::Local.should_spawn_local_worker());
    }

    maintenance_cleanup_policy_tests!();

    #[test]
    fn checkpoint_defaults_are_daily_and_bounded() {
        let config = CheckpointConfig::default();

        assert_eq!(config.interval, Duration::from_secs(24 * 60 * 60));
        assert_eq!(config.max_checkpoints, 7);
        assert_eq!(config.retention_days, 7);

        let retention = config.retention_policy();
        assert_eq!(retention.max_count, 7);
        assert_eq!(retention.min_keep, 2);
        assert_eq!(retention.max_age_days, 7);
    }

    #[test]
    fn lifecycle_policy_defaults_require_confirmation() {
        let policy = LifecyclePolicy::default();

        assert!(policy.require_confirmation);
        assert_eq!(policy.expiry_days, 30);
        assert!(policy.allowed_prefix.is_empty());
    }

    #[test]
    fn prefix_scope_validation_requires_boundary_match() {
        assert_eq!(normalize_prefix_scope("/vol/root/").unwrap(), "vol/root");
        assert!(prefix_within_scope("vol/root", "vol/root"));
        assert!(prefix_within_scope("vol/root/checkpoints", "vol/root"));
        assert!(!prefix_within_scope("vol/root2", "vol/root"));
        assert!(!prefix_within_scope("vol/rootish/path", "vol/root"));
    }

    #[test]
    fn checkpoint_object_key_embeds_generation_and_timestamp() {
        let key = checkpoint_object_key("vol/root", 42, 123);
        assert_eq!(
            key,
            "vol/root/metadata/checkpoints/checkpoint-g00000000000000000042-t00000000000000000123.bin"
        );
    }
}
    };
}
