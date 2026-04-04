use super::*;
use std::sync::atomic::Ordering;

use crate::superblock::StaleGenerationError;

/// Decision returned by a [`FlushCommitHook`] after `pre_commit`.
pub enum FlushCommitDecision {
    /// Use the standard direct write path (`persist_inodes_batch` + sync + commit).
    Direct,
    /// The hook already committed on behalf of this client.
    /// `generation` is the committed superblock generation.
    Committed(u64),
    /// Abort this flush with `EIO`.
    Abort,
}

/// Extension hook called during the flush commit pipeline.
///
/// Implementors may route the commit through an alternate path (e.g. a relay
/// server) and signal back the outcome via [`FlushCommitDecision`].  When
/// `pre_commit` returns `Committed`, the core flush logic skips
/// `persist_inodes_batch` / `sync_metadata_writes` / `commit_generation` and
/// instead accepts the relay-committed generation via the superblock manager.
///
/// All methods are synchronous; use `tokio::task::block_in_place` (or the
/// provided `handle`) to bridge async operations if needed.
pub trait FlushCommitHook: Send + Sync {
    /// Called after segments are written and storage pointers merged into
    /// `records`, but before metadata is persisted locally.
    ///
    /// `segment_entries` is a clone of the entries that were passed to
    /// `write_batch` (captured before the write loop so the hook can build a
    /// relay request containing the raw segment data).
    fn pre_commit(
        &self,
        target_generation: u64,
        segment_entries: Vec<SegmentEntry>,
        records: &[InodeRecord],
        journal: Option<&Arc<JournalManager>>,
        handle: &Handle,
    ) -> FlushCommitDecision;

    /// Called after a successful direct-path `commit_generation`.
    /// Used by relay implementations to handle fallback-to-direct cleanup.
    fn on_direct_commit(&self, committed_generation: u64, journal: Option<&Arc<JournalManager>>);

    /// Called at the end of flush finalization (after per-inode journal clears).
    /// Used by relay implementations to clear persistent relay state.
    fn on_finalize(&self, committed_generation: u64, journal: Option<&Arc<JournalManager>>);
}

impl OsageFs {
    // Bound per-object segment serialization working set during flush. We keep
    // each write_batch call under this approximate payload budget and emit
    // multiple segment objects when needed.
    const FLUSH_SEGMENT_BATCH_BYTES: u64 = 128 * 1024 * 1024;
    const FLUSH_STALE_RETRY_LIMIT: usize = 3;

    fn segment_entry_payload_bytes(entry: &SegmentEntry) -> u64 {
        match &entry.payload {
            SegmentPayload::Bytes(bytes) => bytes.len() as u64,
            SegmentPayload::SharedBytes(bytes) => bytes.len() as u64,
            SegmentPayload::Staged(chunks) => chunks.iter().map(|chunk| chunk.len).sum(),
        }
    }

    pub(crate) fn flush_pending(&self) -> std::result::Result<(), i32> {
        for attempt in 0..=Self::FLUSH_STALE_RETRY_LIMIT {
            let result =
                self.flush_pending_selected_once(|guard| std::mem::take(&mut *guard), "all");
            if result != Err(libc::EAGAIN) || attempt == Self::FLUSH_STALE_RETRY_LIMIT {
                return result;
            }
            debug!(
                "flush_pending retrying after stale generation conflict attempt={}/{}",
                attempt + 1,
                Self::FLUSH_STALE_RETRY_LIMIT
            );
        }
        Err(EIO)
    }

    pub(crate) fn flush_pending_for_inode(&self, ino: u64) -> std::result::Result<(), i32> {
        for attempt in 0..=Self::FLUSH_STALE_RETRY_LIMIT {
            let result = self.flush_pending_selected_once(
                |guard| {
                    let mut selected = HashMap::new();
                    let mut cursor = ino;
                    let mut seen = HashSet::new();
                    while seen.insert(cursor) {
                        let Some(entry) = guard.remove(&cursor) else {
                            break;
                        };
                        let parent = entry.record.parent;
                        selected.insert(cursor, entry);
                        if parent == cursor {
                            break;
                        }
                        cursor = parent;
                    }
                    selected
                },
                "inode",
            );
            if result != Err(libc::EAGAIN) || attempt == Self::FLUSH_STALE_RETRY_LIMIT {
                return result;
            }
            debug!(
                "flush_pending_for_inode retrying after stale generation conflict ino={} attempt={}/{}",
                ino,
                attempt + 1,
                Self::FLUSH_STALE_RETRY_LIMIT
            );
        }
        Err(EIO)
    }

    pub(crate) fn sync_local_for_inode(&self, ino: u64) -> std::result::Result<(), i32> {
        let (inodes, staged_chunks) = {
            let mut cursor = ino;
            let mut seen = HashSet::new();
            let mut inodes = Vec::new();
            let mut staged_chunks = Vec::new();
            while seen.insert(cursor) {
                let Some(active_arc) = self.active_inodes.get(&cursor) else {
                    break;
                };
                let state = active_arc.lock();
                let Some(entry) = state.pending.as_ref() else {
                    break;
                };
                inodes.push(cursor);
                if let Some(PendingData::Staged(segments)) = &entry.data {
                    staged_chunks.extend(segments.chunks.iter().cloned());
                }
                let parent = entry.record.parent;
                drop(state);
                if parent == cursor {
                    break;
                }
                cursor = parent;
            }
            (inodes, staged_chunks)
        };

        if !staged_chunks.is_empty() {
            self.segments
                .sync_staged_chunks(&staged_chunks)
                .map_err(|err| {
                    error!(
                        "sync_local_for_inode failed to sync staged chunks ino={} err={err:?}",
                        ino
                    );
                    EIO
                })?;
        }
        if let Some(journal) = &self.journal {
            journal.sync_entries(&inodes).map_err(|err| {
                error!(
                    "sync_local_for_inode failed to sync journal ino={} err={err:?}",
                    ino
                );
                EIO
            })?;
        }
        Ok(())
    }

    pub(crate) fn flush_pending_selected_once<F>(
        &self,
        selector: F,
        scope: &str,
    ) -> std::result::Result<(), i32>
    where
        F: FnOnce(&mut HashMap<u64, PendingEntry>) -> HashMap<u64, PendingEntry>,
    {
        let flush_lock_wait_start = Instant::now();
        let _flush_guard = self.flush_lock.lock();
        let flush_lock_wait = flush_lock_wait_start.elapsed();
        let pid = process::id();
        let tid = super::current_thread_label();
        let mut prepared_generation: Option<u64> = None;
        let mut drained_pending: Option<HashMap<u64, PendingEntry>> = None;
        let result = (|| {
            let start = Instant::now();
            let drain_start = Instant::now();
            let mut drain_skipped_locked = 0usize;
            let mut rotate_stage_file_duration = Duration::from_secs(0);
            let pending = {
                let mut guard = HashMap::new();
                let keys: Vec<u64> = self
                    .pending_inodes
                    .iter()
                    .map(|entry| *entry.key())
                    .collect();
                for inode in keys {
                    if let Some(active_arc) = self.active_inodes.get(&inode) {
                        if let Some(mut state) = active_arc.try_lock() {
                            if let Some(entry) = state.pending.take() {
                                state.flushing = Some(entry.clone());
                                guard.insert(inode, entry);
                            } else {
                                self.pending_inodes.remove(&inode);
                            }
                        } else {
                            drain_skipped_locked = drain_skipped_locked.saturating_add(1);
                        }
                    }
                }
                if drain_skipped_locked > 0 {
                    debug!(
                        "flush_pending pid={} tid={} scope={} skipped_locked_inodes={}",
                        pid, tid, scope, drain_skipped_locked
                    );
                }
                if guard.is_empty() {
                    self.touch_last_flush();
                    return Ok(());
                }
                let drained = selector(&mut guard);
                // Reinsert non-selected
                for (inode, entry) in guard {
                    if let Some(active_arc) = self.active_inodes.get(&inode) {
                        let mut state = active_arc.lock();
                        if state.pending.is_none() {
                            state.pending = Some(entry);
                            self.pending_inodes.insert(inode);
                        }
                        state.flushing = None;
                    }
                }
                if drained.is_empty() {
                    self.touch_last_flush();
                    return Ok(());
                }
                let has_staged = drained
                    .values()
                    .any(|entry| matches!(entry.data, Some(PendingData::Staged(_))));
                if has_staged {
                    let rotate_start = Instant::now();
                    self.segments.rotate_stage_file();
                    rotate_stage_file_duration = rotate_start.elapsed();
                }
                drained
            };
            let drain_duration = drain_start
                .elapsed()
                .saturating_sub(rotate_stage_file_duration);
            let drained_inodes = pending.len();
            drained_pending = Some(pending);
            let pending = drained_pending
                .as_ref()
                .expect("drained pending map must be present");
            debug!(
                "flush_pending pid={} tid={} scope={} preparing {} inodes",
                pid,
                tid,
                scope,
                pending.len()
            );
            let prepare_start = Instant::now();
            let snapshot = self
                .superblock
                .prepare_dirty_generation()
                .map_err(|err| {
                    log::error!(
                        "flush_pending prepare_dirty_generation failed pid={} tid={} scope={} err={err:?}",
                        pid,
                        tid,
                        scope
                    );
                    EIO
                })?;
            let target_generation = snapshot.generation;
            let prepare_duration = prepare_start.elapsed();
            prepared_generation = Some(target_generation);
            // If relay is required by the superblock but no hook is installed,
            // the direct write path would bypass the relay server — fail closed.
            if snapshot.relay_required && self.flush_commit_hook.get().is_none() {
                log::error!(
                    "flush_pending relay_required=true but no FlushCommitHook installed; refusing direct write pid={} tid={} scope={}",
                    pid,
                    tid,
                    scope
                );
                self.superblock.abort_generation(target_generation);
                prepared_generation = None;
                return Err(EIO);
            }
            let mut segment_entries = Vec::new();
            let mut segment_data_inodes = HashSet::new();
            // Map from inode -> base committed extents captured when pending data
            // was staged. Used after write_batch to merge newly-written extents.
            let mut base_extents_map: HashMap<u64, Vec<SegmentExtent>> = HashMap::new();
            let mut records = Vec::new();
            let mut flushed_bytes: u64 = 0;
            let mut inline_files = 0;
            let mut segment_files = 0;
            let mut inline_bytes: u64 = 0;
            let mut segment_bytes: u64 = 0;
            let mut metadata_only = 0;
            let mut flushed_inodes = Vec::new();
            let classify_start = Instant::now();
            for (inode, pending_entry) in pending.iter() {
                flushed_inodes.push(*inode);
                match &pending_entry.data {
                    Some(PendingData::Inline(data_bytes)) => {
                        let mut record = pending_entry.record.clone();
                        let data_len = data_bytes.len() as u64;
                        flushed_bytes = flushed_bytes.saturating_add(data_len);
                        record.size = data_len;
                        if data_len <= self.config.inline_threshold as u64 {
                            record.storage = self.encode_inline_storage(data_bytes).map_err(|err| {
                                log::error!(
                                    "flush_pending inline encode failed pid={} tid={} scope={} ino={} len={} err={err:?}",
                                    pid,
                                    tid,
                                    scope,
                                    record.inode,
                                    data_len
                                );
                                EIO
                            })?;
                            inline_files += 1;
                            inline_bytes = inline_bytes.saturating_add(data_len);
                            records.push(record);
                        } else {
                            segment_entries.push(SegmentEntry {
                                inode: record.inode,
                                path: record.path.clone(),
                                logical_offset: 0,
                                payload: SegmentPayload::Bytes(data_bytes.as_ref().clone()),
                            });
                            segment_data_inodes.insert(record.inode);
                            record.storage = FileStorage::Inline(Vec::new());
                            segment_files += 1;
                            segment_bytes = segment_bytes.saturating_add(data_len);
                            records.push(record);
                        }
                    }
                    Some(PendingData::Staged(segments)) => {
                        let mut record = pending_entry.record.clone();
                        let staged = segments.staged_bytes();
                        let file_size = segments.total_len;
                        // Only newly-staged bytes count toward the flushed watermark;
                        // base_extents are already committed and were not re-uploaded.
                        flushed_bytes = flushed_bytes.saturating_add(staged);
                        record.size = file_size;
                        if staged > 0 {
                            let mut ordered_chunks = segments.chunks.as_ref().clone();
                            ordered_chunks.sort_by_key(|chunk| chunk.logical_offset);
                            let mut emitted = 0usize;
                            for chunk in ordered_chunks {
                                if chunk.len == 0 {
                                    continue;
                                }
                                let logical_offset = chunk.logical_offset;
                                segment_entries.push(SegmentEntry {
                                    inode: record.inode,
                                    path: record.path.clone(),
                                    logical_offset,
                                    payload: SegmentPayload::Staged(vec![chunk]),
                                });
                                emitted += 1;
                            }
                            if emitted > 0 {
                                segment_data_inodes.insert(record.inode);
                                // Save base extents to combine with newly written extents later.
                                base_extents_map
                                    .insert(record.inode, segments.base_extents.as_ref().clone());
                                record.storage = FileStorage::Inline(Vec::new());
                                segment_files += 1;
                                segment_bytes = segment_bytes.saturating_add(staged);
                                records.push(record);
                            } else {
                                record.storage =
                                    FileStorage::Segments(segments.base_extents.as_ref().clone());
                                metadata_only += 1;
                                records.push(record);
                            }
                        } else {
                            // No staged chunks — only base_extents with a metadata change.
                            // Preserve the committed extents as the storage pointer.
                            record.storage =
                                FileStorage::Segments(segments.base_extents.as_ref().clone());
                            metadata_only += 1;
                            records.push(record);
                        }
                    }
                    None => {
                        metadata_only += 1;
                        let mut record = pending_entry.record.clone();
                        // A metadata-only pending entry (data=None) whose record carries
                        // stale Inline([]) storage for a non-empty file was produced by a
                        // setattr that raced with a concurrent flush: load_inode returned
                        // the flushing record (storage placeholder = Inline([])) just
                        // before the prior flush committed and removed it from
                        // flushing_inodes.  flush_lock serializes flushes, so that prior
                        // flush has committed by the time we get here.  Reload the
                        // authoritative record from the metadata store (which has the
                        // correct Segments(extents) pointer) and merge our pending
                        // metadata changes on top.
                        if record.size > 0
                            && matches!(
                                record.storage,
                                FileStorage::Inline(ref v) if v.is_empty()
                            )
                        {
                            match self.block_on(self.metadata.get_inode(record.inode)) {
                                Ok(Some(fresh)) => {
                                    let merged_mode = record.mode;
                                    let merged_uid = record.uid;
                                    let merged_gid = record.gid;
                                    let merged_ctime = record.ctime;
                                    let merged_atime = record.atime;
                                    let merged_mtime = record.mtime;
                                    record = fresh;
                                    record.mode = merged_mode;
                                    record.uid = merged_uid;
                                    record.gid = merged_gid;
                                    record.ctime = merged_ctime;
                                    record.atime = merged_atime;
                                    record.mtime = merged_mtime;
                                }
                                Ok(None) => {
                                    // The prior data flush may not have committed yet
                                    // (very unlikely given flush_lock, but possible on
                                    // flush failure + recovery).  Skip this record to
                                    // avoid persisting a zero-storage record; the next
                                    // flush of the data entry will carry our metadata
                                    // changes if they were merged there.
                                    log::warn!(
                                        "flush: metadata-only entry ino={} has stale \
                                         Inline([]) storage but inode absent from metadata \
                                         store; skipping to prevent data pointer loss",
                                        record.inode
                                    );
                                    metadata_only -= 1;
                                    continue;
                                }
                                Err(err) => {
                                    log::warn!(
                                        "flush: metadata-only entry ino={} storage reload \
                                         failed: {err:?}; using stale storage as fallback",
                                        record.inode
                                    );
                                }
                            }
                        }
                        records.push(record);
                    }
                }
            }
            let classify_duration = classify_start.elapsed();
            // Capture a clone of segment entries for any installed FlushCommitHook before
            // write_batch consumes them.  Only clones when a hook is present.
            let hook_segment_entries: Vec<SegmentEntry> = if self.flush_commit_hook.get().is_some()
            {
                segment_entries.clone()
            } else {
                Vec::new()
            };
            let mut pointer_map: HashMap<u64, Vec<SegmentExtent>> = HashMap::new();
            let mut segment_id_logged = None;
            let mut segment_write_duration = Duration::from_secs(0);
            if !segment_entries.is_empty() {
                let mut write_batch =
                    |batch_entries: Vec<SegmentEntry>| -> std::result::Result<(), i32> {
                        let segment_id = self.allocate_segment_id().map_err(|err| {
                        log::error!(
                            "flush_pending allocate_segment_id failed pid={} tid={} scope={} err={err:?}",
                            pid,
                            tid,
                            scope
                        );
                        EIO
                    })?;
                        let seg_start = Instant::now();
                        let pointers = self
                        .segments
                        .write_batch(target_generation, segment_id, batch_entries)
                        .map_err(|err| {
                            log::error!(
                                "flush_pending segment write failed pid={} tid={} scope={} gen={} segment_id={} err={err:?}",
                                pid,
                                tid,
                                scope,
                                target_generation,
                                segment_id
                            );
                            EIO
                        })?;
                        for (inode, extent) in pointers {
                            pointer_map.entry(inode).or_default().push(extent);
                        }
                        if segment_id_logged.is_none() {
                            segment_id_logged = Some(segment_id);
                        }
                        segment_write_duration += seg_start.elapsed();
                        Ok(())
                    };

                let mut current_batch: Vec<SegmentEntry> = Vec::new();
                let mut current_bytes: u64 = 0;
                for entry in segment_entries {
                    let entry_bytes = Self::segment_entry_payload_bytes(&entry).max(1);
                    if !current_batch.is_empty()
                        && current_bytes.saturating_add(entry_bytes)
                            > Self::FLUSH_SEGMENT_BATCH_BYTES
                    {
                        write_batch(current_batch)?;
                        current_batch = Vec::new();
                        current_bytes = 0;
                    }
                    current_bytes = current_bytes.saturating_add(entry_bytes);
                    current_batch.push(entry);
                }
                if !current_batch.is_empty() {
                    write_batch(current_batch)?;
                }
            }
            let merge_start = Instant::now();
            let persist_start = Instant::now();
            for record in records.iter_mut() {
                if segment_data_inodes.contains(&record.inode) {
                    if let Some(new_extents) = pointer_map.get(&record.inode) {
                        // Merge existing committed extents with the newly written
                        // extents. New extents are appended last so that
                        // stable-sorted reads prefer it over any overlapping base
                        // extent at the same logical offset.
                        //
                        // We reload the authoritative committed extents from the
                        // metadata cache rather than relying solely on the
                        // `base_extents` captured when the pending entry was created.
                        // An async flush may have committed new extents for this
                        // inode *after* our pending entry was created (e.g. the
                        // previous async flush was still running when the next write
                        // arrived). Flushes are serialized by flush_lock, so the
                        // metadata cache is fully up-to-date at this point.  We only
                        // do the reload when base_extents is non-empty (i.e. there
                        // were already committed extents when the pending entry was
                        // created) to avoid an unnecessary shard-load for brand-new
                        // inodes that have never been persisted.
                        let base_extents = base_extents_map
                            .get(&record.inode)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]);
                        // Prefer cache for the hot path, but fall back to an
                        // authoritative metadata lookup on cache miss. Using
                        // stale `base_extents` here can drop recently committed
                        // extents when a write races with an async flush.
                        let authoritative_base: Vec<SegmentExtent> = if !base_extents.is_empty() {
                            match self.metadata.get_cached_inode(record.inode) {
                                Some(r) => match r.storage {
                                    FileStorage::Segments(exts) => exts,
                                    _ => base_extents.to_vec(),
                                },
                                None => {
                                    match self.block_on(self.metadata.get_inode(record.inode)) {
                                        Ok(Some(r)) => match r.storage {
                                            FileStorage::Segments(exts) => exts,
                                            _ => base_extents.to_vec(),
                                        },
                                        _ => base_extents.to_vec(),
                                    }
                                }
                            }
                        } else {
                            base_extents.to_vec()
                        };
                        let mut all_extents = authoritative_base;
                        all_extents.extend(new_extents.iter().cloned());
                        all_extents.sort_by_key(|ext| ext.logical_offset);
                        if all_extents.is_empty() {
                            log::error!(
                                "flush_pending empty segment extent map pid={} tid={} scope={} ino={} gen={}",
                                pid,
                                tid,
                                scope,
                                record.inode,
                                target_generation
                            );
                            return Err(EIO);
                        } else {
                            record.storage = FileStorage::Segments(all_extents);
                        }
                    } else {
                        log::error!(
                            "flush_pending missing segment pointer pid={} tid={} scope={} ino={} gen={}",
                            pid,
                            tid,
                            scope,
                            record.inode,
                            target_generation
                        );
                        return Err(EIO);
                    }
                }
            }
            let merge_duration = merge_start.elapsed();

            // --- FlushCommitHook: called after segments are written and storage
            // pointers are merged into records, before metadata is persisted.
            let hook = self.flush_commit_hook.get().cloned();
            let hook_decision = if let Some(ref h) = hook {
                h.pre_commit(
                    target_generation,
                    hook_segment_entries,
                    &records,
                    self.journal.as_ref(),
                    &self.handle,
                )
            } else {
                FlushCommitDecision::Direct
            };
            let relay_committed_gen: Option<u64> = match hook_decision {
                FlushCommitDecision::Committed(relay_gen) => Some(relay_gen),
                FlushCommitDecision::Abort => return Err(EIO),
                FlushCommitDecision::Direct => None,
            };

            let persist_only_duration;
            let sync_metadata_duration;
            let metadata_duration;
            let commit_duration;
            let committed_gen: u64;

            if let Some(relay_gen) = relay_committed_gen {
                // Relay path: hook already committed on our behalf.  Accept the
                // externally committed generation (reloads the superblock to get
                // the current version) and skip the local persist/sync/commit.
                self.block_on(self.superblock.accept_externally_committed_generation(relay_gen))
                    .map_err(|err| {
                        log::error!(
                            "flush_pending accept_externally_committed_generation failed pid={} tid={} scope={} gen={} err={err:?}",
                            pid,
                            tid,
                            scope,
                            relay_gen
                        );
                        EIO
                    })?;
                prepared_generation = None;
                committed_gen = relay_gen;
                persist_only_duration = Duration::ZERO;
                sync_metadata_duration = Duration::ZERO;
                metadata_duration = Duration::ZERO;
                commit_duration = Duration::ZERO;
            } else {
                // Direct path: persist metadata locally, sync, and commit.
                let persist_only_start = Instant::now();
                self.block_on(self.metadata.persist_inodes_batch(
                    records,
                    target_generation,
                    self.config.shard_size,
                    self.config.imap_delta_batch,
                ))
                .map_err(|err| {
                    log::error!(
                        "flush_pending metadata persist failed pid={} tid={} scope={} gen={} err={err:?}",
                        pid,
                        tid,
                        scope,
                        target_generation
                    );
                    EIO
                })?;
                persist_only_duration = persist_only_start.elapsed();
                // Flush all shard/delta writes to disk with a single directory
                // fsync on each metadata subdirectory before advancing the
                // superblock.  This replaces per-file fsyncs inside write_shard /
                // write_delta, reducing O(N) fsyncs to O(2) per flush cycle.
                let sync_metadata_start = Instant::now();
                self.block_on(self.metadata.sync_metadata_writes())
                    .map_err(|err| {
                        log::error!(
                            "flush_pending sync_metadata_writes failed pid={} tid={} scope={} gen={} err={err:?}",
                            pid,
                            tid,
                            scope,
                            target_generation
                        );
                        EIO
                    })?;
                sync_metadata_duration = sync_metadata_start.elapsed();
                metadata_duration = persist_start.elapsed();
                let commit_start_inner = Instant::now();
                if let Err(code) = self.block_on(self.superblock.commit_generation(target_generation)).map_err(|err| {
                    if let Some(stale) = err.downcast_ref::<StaleGenerationError>() {
                        log::warn!(
                            "flush_pending stale generation conflict pid={} tid={} scope={} attempted_gen={} current_gen={}",
                            pid,
                            tid,
                            scope,
                            stale.attempted,
                            stale.current
                        );
                        return libc::EAGAIN;
                    }
                    log::error!(
                        "flush_pending commit_generation failed pid={} tid={} scope={} gen={} err={err:?}",
                        pid,
                        tid,
                        scope,
                        target_generation
                    );
                    EIO
                }) {
                    self.superblock.abort_generation(target_generation);
                    prepared_generation = None;
                    return Err(code);
                }
                prepared_generation = None;
                committed_gen = target_generation;
                if let Some(ref h) = hook {
                    h.on_direct_commit(committed_gen, self.journal.as_ref());
                }
                commit_duration = commit_start_inner.elapsed();
            }

            // Post-commit coordination event emission (advisory, fire-and-forget).
            // Only emitted after the commit succeeds (direct or relay path).
            if let Some(publisher) = self.coordination_publisher.clone() {
                use crate::coordination::{GenerationHint, InvalidationEvent, InvalidationScope};
                let volume_prefix = self.config.object_prefix.clone();
                let committer_id = self.client_state.client_id();
                let timestamp = time::OffsetDateTime::now_utc().unix_timestamp();
                let affected: Vec<u64> = flushed_inodes.clone();
                let emit_gen = committed_gen;
                self.handle.spawn(async move {
                    let hint = GenerationHint {
                        volume_prefix: volume_prefix.clone(),
                        generation: emit_gen,
                        committer_id,
                        timestamp,
                    };
                    if let Err(err) = publisher.publish_generation_advance(hint).await {
                        tracing::warn!(
                            target: "coordination",
                            generation = emit_gen,
                            err = %err,
                            "publish_generation_advance failed (advisory; commit already succeeded)"
                        );
                    }
                    let inv = InvalidationEvent {
                        volume_prefix,
                        generation: emit_gen,
                        affected_inodes: Some(affected.clone()),
                        scope: InvalidationScope::Inodes(affected),
                    };
                    if let Err(err) = publisher.publish_invalidation(inv).await {
                        tracing::warn!(
                            target: "coordination",
                            generation = emit_gen,
                            err = %err,
                            "publish_invalidation failed (advisory; commit already succeeded)"
                        );
                    }
                });
            }

            let finalize_start = Instant::now();
            let finalize_pending_bytes_start = Instant::now();
            // Use a CAS loop that floors at 0 instead of wrapping, as a
            // defense-in-depth measure against residual accounting mismatches
            // (e.g. write during flushing state creating entries whose
            // staged_bytes exceeds the pending_bytes delta that was added).
            let pending_remaining = loop {
                let current = self.pending_bytes.load(Ordering::Relaxed);
                let new_val = current.saturating_sub(flushed_bytes);
                if self
                    .pending_bytes
                    .compare_exchange_weak(current, new_val, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    break new_val;
                }
            };
            let finalize_pending_bytes_duration = finalize_pending_bytes_start.elapsed();
            let flushed_entries = drained_pending
                .take()
                .expect("drained pending map must exist until flush commit");
            let finalize_clear_flushing_start = Instant::now();
            let mut finalize_flushing_lock_wait = Duration::from_secs(0);
            for inode in flushed_entries.keys() {
                if let Some(active_arc) = self.active_inodes.get(inode) {
                    let lock_start = Instant::now();
                    let mut state = active_arc.lock();
                    finalize_flushing_lock_wait += lock_start.elapsed();
                    state.flushing = None;
                    if state.pending.is_none() {
                        self.pending_inodes.remove(inode);
                    }
                }
            }
            let finalize_clear_flushing_duration = finalize_clear_flushing_start.elapsed();
            // Dispatch FUSE kernel cache invalidation to a background task so
            // the flush path is never blocked waiting on the /dev/fuse channel.
            // inval_inode is synchronous and can block when the channel buffer
            // is full; the kernel may also send getattr/read callbacks in
            // response, which need a free FUSE worker thread.  Doing this
            // inline with thousands of inodes (stress-ng, kernel compile)
            // saturates the channel and deadlocks.
            #[cfg(feature = "fuse")]
            if let Some(notifier) = self.kernel_notifier.get() {
                let notifier = notifier.clone();
                let flushed: Vec<u64> = flushed_inodes.clone();
                self.handle.spawn(async move {
                    tokio::task::spawn_blocking(move || {
                        for inode in flushed {
                            let _ = notifier.inval_inode(inode, 0, 0);
                        }
                    })
                    .await
                    .ok();
                });
            }
            let finalize_release_data_start = Instant::now();
            let mut released_staged_chunks = 0u64;
            for pending_entry in flushed_entries.into_values() {
                if let Some(data) = pending_entry.data {
                    if let PendingData::Staged(segments) = &data {
                        released_staged_chunks =
                            released_staged_chunks.saturating_add(segments.chunks.len() as u64);
                    }
                    self.release_pending_data(data);
                }
            }
            let finalize_release_data_duration = finalize_release_data_start.elapsed();
            let finalize_journal_phase_start = Instant::now();
            let mut finalize_journal_clear_duration = Duration::from_secs(0);
            let mut journal_cleared_inodes = 0u64;
            if let Some(journal) = &self.journal {
                let clearable_inodes = flushed_inodes
                    .into_iter()
                    .filter(|inode| {
                        self.active_inodes
                            .get(inode)
                            .is_none_or(|arc| arc.lock().pending.is_none())
                    })
                    .collect::<Vec<_>>();
                let journal_clear_start = Instant::now();
                for inode in clearable_inodes {
                    journal.clear_entry(inode).map_err(|err| {
                        log::error!(
                            "flush_pending journal clear failed pid={} tid={} scope={} ino={} err={err:?}",
                            pid,
                            tid,
                            scope,
                            inode
                        );
                        EIO
                    })?;
                    journal_cleared_inodes = journal_cleared_inodes.saturating_add(1);
                }
                finalize_journal_clear_duration = journal_clear_start.elapsed();
            }
            let finalize_journal_phase_duration = finalize_journal_phase_start.elapsed();
            if let Some(ref h) = hook {
                h.on_finalize(committed_gen, self.journal.as_ref());
            }
            let finalize_duration = finalize_start.elapsed();
            self.log_perf(
                "flush_pending",
                start.elapsed(),
                json!({
                    "scope": scope,
                    "committed_generation": committed_gen,
                    "files": inline_files + segment_files,
                    "inline_files": inline_files,
                    "segment_files": segment_files,
                    "metadata_only": metadata_only,
                    "inline_bytes": inline_bytes,
                    "segment_bytes": segment_bytes,
                    "flushed_bytes": flushed_bytes,
                    "drained_inodes": drained_inodes,
                    "drain_skipped_locked": drain_skipped_locked,
                    "segment_id": segment_id_logged,
                    "flush_lock_wait_ms": flush_lock_wait.as_secs_f64() * 1000.0,
                    "drain_select_ms": drain_duration.as_secs_f64() * 1000.0,
                    "rotate_stage_file_ms": rotate_stage_file_duration.as_secs_f64() * 1000.0,
                    "prepare_generation_ms": prepare_duration.as_secs_f64() * 1000.0,
                    "classify_records_ms": classify_duration.as_secs_f64() * 1000.0,
                    "segment_write_ms": segment_write_duration.as_secs_f64() * 1000.0,
                    "merge_segment_extents_ms": merge_duration.as_secs_f64() * 1000.0,
                    "metadata_persist_only_ms": persist_only_duration.as_secs_f64() * 1000.0,
                    "metadata_sync_only_ms": sync_metadata_duration.as_secs_f64() * 1000.0,
                    "metadata_ms": metadata_duration.as_secs_f64() * 1000.0,
                    "commit_ms": commit_duration.as_secs_f64() * 1000.0,
                    "finalize_ms": finalize_duration.as_secs_f64() * 1000.0,
                    "finalize_pending_bytes_ms": finalize_pending_bytes_duration.as_secs_f64() * 1000.0,
                    "finalize_clear_flushing_ms": finalize_clear_flushing_duration.as_secs_f64() * 1000.0,
                    "finalize_clear_flushing_lock_wait_ms": finalize_flushing_lock_wait.as_secs_f64() * 1000.0,
                    "finalize_release_pending_data_ms": finalize_release_data_duration.as_secs_f64() * 1000.0,
                    "released_staged_chunks": released_staged_chunks,
                    "finalize_journal_phase_ms": finalize_journal_phase_duration.as_secs_f64() * 1000.0,
                    "finalize_journal_clear_ms": finalize_journal_clear_duration.as_secs_f64() * 1000.0,
                    "journal_cleared_inodes": journal_cleared_inodes,
                    "pending_remaining": pending_remaining,
                }),
            );
            debug!(
                "flush_pending pid={} tid={} scope={} gen={} inline_files={} segment_files={} metadata_only={} inline_bytes={} segment_bytes={} pending_remaining={}",
                pid,
                tid,
                scope,
                committed_gen,
                inline_files,
                segment_files,
                metadata_only,
                inline_bytes,
                segment_bytes,
                pending_remaining
            );
            self.touch_last_flush();
            Ok(())
        })();
        if let Err(code) = result {
            if let Some(pending_gen) = prepared_generation.take() {
                self.superblock.abort_generation(pending_gen);
            }
            if let Some(pending) = drained_pending.take() {
                self.restore_pending_after_failed_flush(pending);
            }
            error!(
                "flush_pending failed pid={} tid={} scope={} code={}",
                pid, tid, scope, code
            );
        }
        result
    }

    pub(crate) fn restore_pending_after_failed_flush(&self, restored: HashMap<u64, PendingEntry>) {
        let mut dropped_bytes = 0u64;
        for (inode, entry) in restored {
            if let Some(active_arc) = self.active_inodes.get(&inode) {
                let mut state = active_arc.lock();
                if state.pending.is_none() {
                    state.pending = Some(entry);
                    self.pending_inodes.insert(inode);
                } else if let Some(data) = entry.data {
                    dropped_bytes = dropped_bytes.saturating_add(data.len());
                    self.release_pending_data(data);
                }
                state.flushing = None;
            }
        }
        if dropped_bytes > 0 {
            self.pending_bytes
                .fetch_sub(dropped_bytes, Ordering::Relaxed);
        }
    }

    pub(crate) fn log_perf(&self, event: &str, duration: Duration, details: serde_json::Value) {
        if let Some(logger) = &self.perf {
            logger.log(event, duration, details);
        }
    }

    pub(crate) fn replay_start(&self) -> Option<Instant> {
        (!super::process_exiting())
            .then_some(())
            .and_then(|_| self.replay.as_ref().map(|_| Instant::now()))
    }

    pub(crate) fn log_replay(
        &self,
        layer: &str,
        op: &str,
        start: Option<Instant>,
        errno: Option<i32>,
        mut details: serde_json::Value,
    ) {
        if let (Some(errno), Some(telemetry)) = (errno, &self.telemetry) {
            telemetry.emit_errno_sampled(layer, op, errno);
        }
        if super::process_exiting() {
            return;
        }
        let (Some(logger), Some(started)) = (&self.replay, start) else {
            return;
        };
        if let serde_json::Value::Object(ref mut map) = details {
            map.insert("pid".to_string(), json!(process::id()));
            map.insert("tid".to_string(), json!(super::current_thread_label()));
        }
        let duration = started.elapsed();
        let start_offset = logger.elapsed_since_start(started);
        logger.log_op(layer, op, start_offset, duration, errno, details);
    }

    pub(crate) fn adaptive_pending_limit(&self) -> u64 {
        let base = self.config.pending_bytes;
        let scaled = base.saturating_mul(ADAPTIVE_PENDING_MULTIPLIER);
        scaled.max(base).min(ADAPTIVE_PENDING_MAX_BYTES.max(base))
    }

    pub(crate) fn pending_flush_limit_for_write(&self, is_append: bool, write_len: usize) -> u64 {
        let base = self.config.pending_bytes;
        if !is_append || (write_len as u64) < ADAPTIVE_LARGE_WRITE_MIN_BYTES {
            return base;
        }
        let by_write_size = (write_len as u64).saturating_mul(8);
        self.adaptive_pending_limit().max(by_write_size).max(base)
    }

    pub(crate) fn trigger_async_flush(&self) {
        if self.flush_scheduled.swap(true, Ordering::AcqRel) {
            return;
        }
        let fs = self.clone();
        self.handle.spawn_blocking(move || {
            if let Err(code) = fs.flush_pending() {
                error!("background flush_pending failed errno={}", code);
            }
            fs.flush_scheduled.store(false, Ordering::Release);
            let has_pending = fs
                .active_inodes
                .iter()
                .any(|entry| entry.value().lock().pending.is_some());
            if has_pending {
                fs.trigger_async_flush();
            }
        });
    }

    pub(crate) fn flush_if_interval_elapsed(&self) -> std::result::Result<(), i32> {
        let Some(interval) = self.flush_interval else {
            return Ok(());
        };
        let now = super::core::epoch_millis_now();
        let last = self.last_flush.load(Ordering::Relaxed);
        let elapsed_ms = now.saturating_sub(last);
        if elapsed_ms < interval.as_millis() as u64 {
            return Ok(());
        }
        debug!(
            "flush_interval {:?} elapsed, scheduling background flush",
            interval
        );
        self.trigger_async_flush();
        Ok(())
    }

    pub(crate) fn touch_last_flush(&self) {
        self.last_flush
            .store(super::core::epoch_millis_now(), Ordering::Relaxed);
    }

    pub(crate) fn log_fuse_error(&self, op: &str, detail: &str, code: i32) {
        let pid = process::id();
        let tid = super::current_thread_label();
        error!(
            "{} failed pid={} tid={} {} errno={}",
            op, pid, tid, detail, code
        );
    }
}
