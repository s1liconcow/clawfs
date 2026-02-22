use super::*;
use std::sync::atomic::Ordering;

impl OsageFs {
    fn merge_ranges(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
        ranges.retain(|(start, end)| end > start);
        if ranges.is_empty() {
            return ranges;
        }
        ranges.sort_by_key(|(start, _)| *start);
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        for (start, end) in ranges {
            if let Some((_, last_end)) = merged.last_mut()
                && start <= *last_end
            {
                *last_end = (*last_end).max(end);
                continue;
            }
            merged.push((start, end));
        }
        merged
    }

    fn trim_base_extents_for_overwrites(
        mut base_extents: Vec<SegmentExtent>,
        base_size: u64,
        overwrite_ranges: &[(u64, u64)],
    ) -> Vec<SegmentExtent> {
        if base_extents.is_empty() || overwrite_ranges.is_empty() || base_size == 0 {
            return base_extents;
        }
        base_extents.sort_by_key(|ext| ext.logical_offset);
        let merged_overwrites = Self::merge_ranges(overwrite_ranges.to_vec());
        if merged_overwrites.is_empty() {
            return base_extents;
        }

        let mut trimmed: Vec<SegmentExtent> = Vec::with_capacity(base_extents.len());
        for (idx, extent) in base_extents.iter().enumerate() {
            let start = extent.logical_offset;
            let next_start = base_extents
                .get(idx + 1)
                .map(|next| next.logical_offset)
                .unwrap_or(base_size);
            let end = next_start.min(base_size);
            if end <= start {
                continue;
            }

            let mut cursor = start;
            for (ov_start, ov_end) in &merged_overwrites {
                if *ov_end <= cursor || *ov_start >= end {
                    continue;
                }
                if *ov_start > cursor {
                    trimmed.push(SegmentExtent::new(
                        cursor,
                        extent.pointer.clone(),
                    ));
                }
                cursor = cursor.max(*ov_end).min(end);
                if cursor >= end {
                    break;
                }
            }
            if cursor < end {
                trimmed.push(SegmentExtent::new(
                    cursor,
                    extent.pointer.clone(),
                ));
            }
        }
        trimmed
    }

    pub(crate) fn flush_pending(&self) -> std::result::Result<(), i32> {
        self.flush_pending_selected(|guard| std::mem::take(&mut *guard), "all")
    }

    pub(crate) fn flush_pending_for_inode(&self, ino: u64) -> std::result::Result<(), i32> {
        self.flush_pending_selected(
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
        )
    }

    pub(crate) fn sync_local_for_inode(&self, ino: u64) -> std::result::Result<(), i32> {
        let (inodes, staged_chunks) = {
            let pending = &self.pending_inodes;
            let mut cursor = ino;
            let mut seen = HashSet::new();
            let mut inodes = Vec::new();
            let mut staged_chunks = Vec::new();
            while seen.insert(cursor) {
                let Some(entry) = pending.get(&cursor) else {
                    break;
                };
                inodes.push(cursor);
                if let Some(PendingData::Staged(segments)) = &entry.data {
                    staged_chunks.extend(segments.chunks.iter().cloned());
                }
                let parent = entry.record.parent;
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

    pub(crate) fn flush_pending_selected<F>(
        &self,
        selector: F,
        scope: &str,
    ) -> std::result::Result<(), i32>
    where
        F: FnOnce(&mut HashMap<u64, PendingEntry>) -> HashMap<u64, PendingEntry>,
    {
        let _flush_guard = self.flush_lock.lock();
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        let mut prepared_generation: Option<u64> = None;
        let mut drained_pending: Option<HashMap<u64, PendingEntry>> = None;
        let result = (|| {
            let start = Instant::now();
            let pending = {
                let mut guard = HashMap::new();
                let keys: Vec<u64> = self
                    .pending_inodes
                    .iter()
                    .map(|entry| *entry.key())
                    .collect();
                for inode in keys {
                    // Insert into flushing_inodes BEFORE removing from pending_inodes so
                    // load_inode always finds the inode in at least one map.
                    if let Some(e) = self.pending_inodes.get(&inode) {
                        self.flushing_inodes.insert(inode, e.value().clone());
                    }
                    if let Some((_, entry)) = self.pending_inodes.remove(&inode) {
                        guard.insert(inode, entry);
                    }
                }
                if guard.is_empty() {
                    self.touch_last_flush();
                    return Ok(());
                }
                let drained = selector(&mut guard);
                // Reinsert non-selected into pending BEFORE removing from flushing so
                // there is no gap where load_inode cannot find the inode.
                for (inode, entry) in guard {
                    self.pending_inodes.insert(inode, entry);
                    self.flushing_inodes.remove(&inode);
                }
                if drained.is_empty() {
                    self.touch_last_flush();
                    return Ok(());
                }
                let has_staged = drained
                    .values()
                    .any(|entry| matches!(entry.data, Some(PendingData::Staged(_))));
                if has_staged {
                    self.segments.rotate_stage_file();
                }
                for (inode, entry) in drained.iter() {
                    self.flushing_inodes.insert(*inode, entry.clone());
                }
                drained
            };
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
            prepared_generation = Some(target_generation);
            let mut segment_entries = Vec::new();
            let mut segment_data_inodes = HashSet::new();
            // Map from inode -> base committed extents captured when pending data
            // was staged. Used after write_batch to merge newly-written extents.
            let mut base_extents_map: HashMap<u64, Vec<SegmentExtent>> = HashMap::new();
            let mut overwrite_ranges_map: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
            let mut records = Vec::new();
            let mut flushed_bytes: u64 = 0;
            let mut inline_files = 0;
            let mut segment_files = 0;
            let mut inline_bytes: u64 = 0;
            let mut segment_bytes: u64 = 0;
            let mut metadata_only = 0;
            let mut flushed_inodes = Vec::new();
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
                                overwrite_ranges_map
                                    .entry(record.inode)
                                    .or_default()
                                    .push((
                                        chunk.logical_offset,
                                        chunk.logical_offset.saturating_add(chunk.len),
                                    ));
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
            let mut pointer_map: HashMap<u64, Vec<SegmentExtent>> = HashMap::new();
            let mut segment_id_logged = None;
            let mut segment_write_duration = Duration::from_secs(0);
            if !segment_entries.is_empty() {
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
                    .write_batch(target_generation, segment_id, segment_entries)
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
                segment_id_logged = Some(segment_id);
                segment_write_duration = seg_start.elapsed();
            }
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
                        let overwrite_ranges = overwrite_ranges_map
                            .get(&record.inode)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]);
                        // Use get_cached_inode (cache-only, no shard reload, no
                        // negative-cache side effects) so we can safely call it
                        // from inside the synchronous flush path.
                        let (authoritative_base, base_size): (Vec<SegmentExtent>, u64) =
                            if !base_extents.is_empty() {
                                match self.metadata.get_cached_inode(record.inode) {
                                    Some(r) => match r.storage {
                                        FileStorage::Segments(exts) => (exts, r.size),
                                        _ => (base_extents.to_vec(), r.size),
                                    },
                                    None => (base_extents.to_vec(), record.size),
                                }
                            } else {
                                (base_extents.to_vec(), 0)
                            };
                        let mut all_extents =
                            Self::trim_base_extents_for_overwrites(
                                authoritative_base,
                                base_size,
                                overwrite_ranges,
                            );
                        all_extents.extend(new_extents.iter().cloned());
                        all_extents.sort_by_key(|ext| ext.logical_offset);
                        // Keep the newest extent when duplicate starts exist.
                        let mut deduped: Vec<SegmentExtent> = Vec::with_capacity(all_extents.len());
                        for extent in all_extents {
                            if let Some(last) = deduped.last_mut()
                                && last.logical_offset == extent.logical_offset
                            {
                                *last = extent;
                            } else {
                                deduped.push(extent);
                            }
                        }
                        if deduped.is_empty() {
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
                            record.storage = FileStorage::Segments(deduped);
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
            self.block_on(self.metadata.persist_inodes_batch(
                &records,
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
            // Flush all shard/delta writes to disk with a single directory
            // fsync on each metadata subdirectory before advancing the
            // superblock.  This replaces per-file fsyncs inside write_shard /
            // write_delta, reducing O(N) fsyncs to O(2) per flush cycle.
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
            let metadata_duration = persist_start.elapsed();
            let commit_start = Instant::now();
            if self
                .block_on(self.superblock.commit_generation(target_generation))
                .map_err(|err| {
                    log::error!(
                        "flush_pending commit_generation failed pid={} tid={} scope={} gen={} err={err:?}",
                        pid,
                        tid,
                        scope,
                        target_generation
                    );
                    err
                })
                .is_err()
            {
                self.superblock.abort_generation(target_generation);
                prepared_generation = None;
                return Err(EIO);
            }
            prepared_generation = None;
            let commit_duration = commit_start.elapsed();
            let mut total = self.pending_bytes.lock();
            *total = total.saturating_sub(flushed_bytes);
            let pending_remaining = *total;
            drop(total);
            let flushed_entries = drained_pending
                .take()
                .expect("drained pending map must exist until flush commit");
            for inode in flushed_entries.keys() {
                self.flushing_inodes.remove(inode);
            }
            for pending_entry in flushed_entries.into_values() {
                if let Some(data) = pending_entry.data {
                    self.release_pending_data(data);
                }
            }
            if let Some(journal) = &self.journal {
                let clearable_inodes = flushed_inodes
                    .into_iter()
                    .filter(|inode| !self.pending_inodes.contains_key(inode))
                    .collect::<Vec<_>>();
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
                }
            }
            self.log_perf(
                "flush_pending",
                start.elapsed(),
                json!({
                    "scope": scope,
                    "target_generation": target_generation,
                    "files": inline_files + segment_files,
                    "inline_files": inline_files,
                    "segment_files": segment_files,
                    "metadata_only": metadata_only,
                    "inline_bytes": inline_bytes,
                    "segment_bytes": segment_bytes,
                    "flushed_bytes": flushed_bytes,
                    "segment_id": segment_id_logged,
                    "segment_write_ms": segment_write_duration.as_secs_f64() * 1000.0,
                    "metadata_ms": metadata_duration.as_secs_f64() * 1000.0,
                    "commit_ms": commit_duration.as_secs_f64() * 1000.0,
                    "pending_remaining": pending_remaining,
                }),
            );
            debug!(
                "flush_pending pid={} tid={} scope={} gen={} inline_files={} segment_files={} metadata_only={} inline_bytes={} segment_bytes={} pending_remaining={}",
                pid,
                tid,
                scope,
                target_generation,
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
            // Use the entry API for an atomic check-and-insert so a concurrent write
            // that arrived after the flush started is not overwritten.
            // Insert into pending BEFORE removing from flushing to keep the inode visible.
            match self.pending_inodes.entry(inode) {
                dashmap::mapref::entry::Entry::Vacant(vac) => {
                    vac.insert(entry);
                }
                dashmap::mapref::entry::Entry::Occupied(_) => {
                    if let Some(data) = entry.data {
                        dropped_bytes = dropped_bytes.saturating_add(data.len());
                        self.release_pending_data(data);
                    }
                }
            }
            self.flushing_inodes.remove(&inode);
        }
        if dropped_bytes > 0 {
            let mut total = self.pending_bytes.lock();
            *total = total.saturating_sub(dropped_bytes);
        }
    }

    pub(crate) fn log_perf(&self, event: &str, duration: Duration, details: serde_json::Value) {
        if let Some(logger) = &self.perf {
            logger.log(event, duration, details);
        }
    }

    pub(crate) fn replay_start(&self) -> Option<Instant> {
        self.replay.as_ref().map(|_| Instant::now())
    }

    pub(crate) fn log_replay(
        &self,
        layer: &str,
        op: &str,
        start: Option<Instant>,
        errno: Option<i32>,
        mut details: serde_json::Value,
    ) {
        let (Some(logger), Some(started)) = (&self.replay, start) else {
            return;
        };
        if let serde_json::Value::Object(ref mut map) = details {
            map.insert("pid".to_string(), json!(process::id()));
            map.insert(
                "tid".to_string(),
                json!(format!("{:?}", thread::current().id())),
            );
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
            if !fs.pending_inodes.is_empty() {
                fs.trigger_async_flush();
            }
        });
    }

    pub(crate) fn flush_if_interval_elapsed(&self) -> std::result::Result<(), i32> {
        let Some(interval) = self.flush_interval else {
            return Ok(());
        };
        let elapsed = {
            let guard = self.last_flush.lock();
            guard.elapsed()
        };
        if elapsed < interval {
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
        *self.last_flush.lock() = Instant::now();
    }

    pub(crate) fn log_fuse_error(&self, op: &str, detail: &str, code: i32) {
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        error!(
            "{} failed pid={} tid={} {} errno={}",
            op, pid, tid, detail, code
        );
    }
}
