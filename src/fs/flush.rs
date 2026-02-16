use super::*;

impl OsageFs {
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

    pub(crate) fn flush_pending_selected<F>(&self, selector: F, scope: &str) -> std::result::Result<(), i32>
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
                let mut guard = self.pending_inodes.lock();
                if guard.is_empty() {
                    self.touch_last_flush();
                    return Ok(());
                }
                let drained = selector(&mut guard);
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
                let mut flushing = self.flushing_inodes.lock();
                for (inode, entry) in drained.iter() {
                    flushing.insert(*inode, entry.clone());
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
                                payload: SegmentPayload::Bytes(data_bytes.clone()),
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
                        let data_len = segments.total_len;
                        flushed_bytes = flushed_bytes.saturating_add(data_len);
                        record.size = data_len;
                        segment_entries.push(SegmentEntry {
                            inode: record.inode,
                            path: record.path.clone(),
                            payload: SegmentPayload::Staged(segments.chunks.clone()),
                        });
                        segment_data_inodes.insert(record.inode);
                        record.storage = FileStorage::Inline(Vec::new());
                        segment_files += 1;
                        segment_bytes = segment_bytes.saturating_add(data_len);
                        records.push(record);
                    }
                    None => {
                        metadata_only += 1;
                        records.push(pending_entry.record.clone());
                    }
                }
            }
            let mut pointer_map = HashMap::new();
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
                pointer_map = pointers.into_iter().collect();
                segment_id_logged = Some(segment_id);
                segment_write_duration = seg_start.elapsed();
            }
            let persist_start = Instant::now();
            for record in records.iter_mut() {
                if segment_data_inodes.contains(&record.inode) {
                    if let Some(ptr) = pointer_map.get(&record.inode) {
                        record.storage =
                            FileStorage::Segments(vec![SegmentExtent::new(0, ptr.clone())]);
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
            {
                let mut flushing = self.flushing_inodes.lock();
                for inode in flushed_entries.keys() {
                    flushing.remove(inode);
                }
            }
            for pending_entry in flushed_entries.into_values() {
                if let Some(data) = pending_entry.data {
                    self.release_pending_data(data);
                }
            }
            if let Some(journal) = &self.journal {
                let clearable_inodes = {
                    let guard = self.pending_inodes.lock();
                    flushed_inodes
                        .into_iter()
                        .filter(|inode| !guard.contains_key(inode))
                        .collect::<Vec<_>>()
                };
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
        let restored_inodes: Vec<u64> = restored.keys().copied().collect();
        let mut map = self.pending_inodes.lock();
        for (inode, entry) in restored {
            match map.entry(inode) {
                Entry::Vacant(vacant) => {
                    vacant.insert(entry);
                }
                Entry::Occupied(_) => {
                    if let Some(data) = entry.data {
                        dropped_bytes = dropped_bytes.saturating_add(data.len());
                        self.release_pending_data(data);
                    }
                }
            }
        }
        drop(map);
        let mut flushing = self.flushing_inodes.lock();
        for inode in restored_inodes {
            flushing.remove(&inode);
        }
        drop(flushing);
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
        if self.journal.is_none() {
            return base;
        }
        if !is_append || (write_len as u64) < ADAPTIVE_LARGE_WRITE_MIN_BYTES {
            return base;
        }
        let by_write_size = (write_len as u64).saturating_mul(8);
        self.adaptive_pending_limit().max(by_write_size).max(base)
    }

    pub(crate) fn should_defer_interval_flush(&self, elapsed: Duration) -> bool {
        if self.journal.is_none() {
            return false;
        }
        let Some(interval) = self.flush_interval else {
            return false;
        };
        let max_defer = interval
            .checked_mul(ADAPTIVE_INTERVAL_DEFER_MULTIPLIER)
            .unwrap_or(Duration::MAX);
        let bounded_max_defer = max_defer.max(Duration::from_secs(ADAPTIVE_MIN_MAX_DEFER_SECS));
        if elapsed >= bounded_max_defer {
            return false;
        }
        let pending_total = *self.pending_bytes.lock();
        if pending_total == 0 {
            return false;
        }
        let adaptive_limit = self.adaptive_pending_limit();
        if pending_total >= adaptive_limit {
            return false;
        }
        let pending = self.pending_inodes.lock();
        if pending.len() != 1 {
            return false;
        }
        let Some(entry) = pending.values().next() else {
            return false;
        };
        let Some(PendingData::Staged(staged)) = &entry.data else {
            return false;
        };
        staged.total_len > self.config.inline_threshold as u64
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
        if self.should_defer_interval_flush(elapsed) {
            debug!(
                "deferring interval flush elapsed={:?} pending_total={} adaptive_limit={}",
                elapsed,
                *self.pending_bytes.lock(),
                self.adaptive_pending_limit()
            );
            return Ok(());
        }
        debug!("flush_interval {:?} elapsed, triggering flush", interval);
        self.flush_pending()?;
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
