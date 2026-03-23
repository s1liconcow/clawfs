use super::*;
use std::sync::atomic::Ordering;

impl OsageFs {
    pub(crate) fn stage_file(
        &self,
        mut record: InodeRecord,
        data: Vec<u8>,
        ctx: Option<StageWriteContext>,
    ) -> std::result::Result<(), i32> {
        let start = Instant::now();
        record.size = data.len() as u64;
        let inode = record.inode;
        let new_len = data.len() as u64;
        let append_range = ctx.and_then(|context| {
            if context.prev_size <= context.write_offset && context.prev_size <= new_len {
                Some(context.prev_size as usize..data.len())
            } else {
                None
            }
        });
        // Insert a placeholder into mutating_inodes BEFORE removing from pending_inodes
        // so that load_inode always sees the inode in at least one map.
        let active_arc = self
            .active_inodes
            .entry(inode)
            .or_insert_with(|| Arc::new(Mutex::new(ActiveInode::default())))
            .clone();
        let mut state = active_arc.lock();
        let original_entry = state.pending.take();
        let mut prev_data = original_entry.as_ref().and_then(|e| e.data.clone());
        let prev_len = prev_data.as_ref().map(|d| d.len()).unwrap_or(0);
        let inline_cap = self.config.inline_threshold as u64;
        let pending_data = if new_len <= inline_cap {
            if let Some(old) = prev_data.take() {
                self.release_pending_data(old);
            }
            PendingData::Inline(Arc::new(data))
        } else {
            let mut segments = match prev_data.take() {
                Some(PendingData::Staged(segs)) => segs,
                Some(PendingData::Inline(bytes)) => {
                    let chunk = match self.segments.stage_payload(bytes.as_ref()) {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            state.pending = original_entry.clone();
                            return Err(EIO);
                        }
                    };
                    PendingSegments::from_chunk(chunk)
                }
                None => PendingSegments::new(),
            };
            if segments.total_len == 0 {
                let chunk = match self.segments.stage_payload(&data) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        state.pending = original_entry.clone();
                        return Err(EIO);
                    }
                };
                PendingData::Staged(PendingSegments::from_chunk(chunk))
            } else if let Some(range) = append_range {
                if range.start < range.end {
                    let chunk = match self.segments.stage_payload(&data[range]) {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            state.pending = original_entry.clone();
                            return Err(EIO);
                        }
                    };
                    segments.append(chunk);
                }
                PendingData::Staged(segments)
            } else {
                let chunk = match self.segments.stage_payload(&data) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        state.pending = original_entry.clone();
                        return Err(EIO);
                    }
                };
                let old = segments;
                let pending = PendingData::Staged(PendingSegments::from_chunk(chunk));
                self.release_pending_data(PendingData::Staged(old));
                pending
            }
        };
        let journal_payload = if self.journal.is_some() {
            Some(self.snapshot_journal_payload(&pending_data))
        } else {
            None
        };
        // Save fields needed after move.
        let path = record.path.clone();

        // Update state in-place.
        state.pending = Some(PendingEntry {
            record: record.clone(),
            data: Some(pending_data),
        });
        self.pending_inodes.insert(inode);
        drop(state);

        if let (Some(journal), Some(payload)) = (&self.journal, &journal_payload) {
            journal.persist_record(&record, payload).map_err(|_| EIO)?;
        }

        let pending_total = if new_len >= prev_len {
            self.pending_bytes
                .fetch_add(new_len - prev_len, Ordering::Relaxed)
                .saturating_add(new_len - prev_len)
        } else {
            self.pending_bytes
                .fetch_sub(prev_len - new_len, Ordering::Relaxed)
                .saturating_sub(prev_len - new_len)
        };
        let should_flush = pending_total >= self.config.pending_bytes;
        self.log_perf(
            "stage_file",
            start.elapsed(),
            json!({
                "inode": inode,
                "bytes": new_len,
                "pending_total": pending_total,
                "triggered_flush": should_flush,
                "filename": path,
            }),
        );
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        debug!(
            "stage_file pid={} tid={} inode={} path={} bytes={} pending={} flush_triggered={}",
            pid, tid, inode, path, new_len, pending_total, should_flush
        );
        if should_flush {
            self.trigger_async_flush();
        }
        self.flush_if_interval_elapsed()?;
        Ok(())
    }

    pub(crate) fn write_inline_range(
        &self,
        record: InodeRecord,
        offset: Option<u64>,
        data: &[u8],
    ) -> std::result::Result<(), i32> {
        if data.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        let inode = record.inode;
        let inline_cap = self.config.inline_threshold as u64;

        let active_arc = self
            .active_inodes
            .entry(inode)
            .or_insert_with(|| Arc::new(Mutex::new(ActiveInode::default())))
            .clone();
        let mut state = active_arc.lock();
        let original_entry = state.pending.take();
        let old_pending_data = original_entry.as_ref().and_then(|entry| entry.data.clone());
        let prev_len = old_pending_data
            .as_ref()
            .map(|data| data.len())
            .or_else(|| {
                state
                    .flushing
                    .as_ref()
                    .and_then(|entry| entry.data.as_ref().map(|data| data.len()))
            })
            .unwrap_or(0);

        let base_record = original_entry
            .as_ref()
            .map(|entry| entry.record.clone())
            .or_else(|| state.flushing.as_ref().map(|entry| entry.record.clone()))
            .unwrap_or(record);
        let mut existing = if let Some(data) = old_pending_data.as_ref() {
            self.slice_pending_bytes(data, 0, data.len()).map_err(|_| {
                state.pending = original_entry.clone();
                EIO
            })?
        } else if let Some(PendingEntry {
            data: Some(flushing_data),
            ..
        }) = &state.flushing
        {
            self.slice_pending_bytes(flushing_data, 0, flushing_data.len())
                .map_err(|_| {
                    state.pending = original_entry.clone();
                    EIO
                })?
        } else if base_record.size > 0 {
            self.read_file_range(&base_record, 0, base_record.size as u32)
                .map_err(|_| {
                    state.pending = original_entry.clone();
                    EIO
                })?
        } else {
            Vec::new()
        };

        let write_offset = offset.unwrap_or(existing.len() as u64);
        let target_len = write_offset.saturating_add(data.len() as u64);
        if target_len > inline_cap {
            state.pending = original_entry.clone();
            drop(state);
            return self.write_large_segments(base_record, write_offset, data);
        }

        let write_offset = usize::try_from(write_offset).map_err(|_| {
            state.pending = original_entry.clone();
            EFBIG
        })?;
        let write_end = write_offset.checked_add(data.len()).ok_or_else(|| {
            state.pending = original_entry.clone();
            EFBIG
        })?;
        if write_end > existing.len() {
            existing.resize(write_end, 0);
        }
        existing[write_offset..write_end].copy_from_slice(data);

        let mut working_record = base_record.clone();
        working_record.update_times();
        working_record.size = existing.len() as u64;
        let pending_data = PendingData::Inline(Arc::new(existing));
        let journal_payload = self
            .journal
            .as_ref()
            .map(|_| self.snapshot_journal_payload(&pending_data));

        state.pending = Some(PendingEntry {
            record: working_record.clone(),
            data: Some(pending_data),
        });
        self.pending_inodes.insert(inode);
        drop(state);

        if let Some(old_data) = old_pending_data {
            self.release_pending_data(old_data);
        }

        if let (Some(journal), Some(payload)) = (&self.journal, &journal_payload) {
            journal
                .persist_record(&working_record, payload)
                .map_err(|_| EIO)?;
        }

        let new_len = working_record.size;
        let pending_total = if new_len >= prev_len {
            self.pending_bytes
                .fetch_add(new_len - prev_len, Ordering::Relaxed)
                .saturating_add(new_len - prev_len)
        } else {
            self.pending_bytes
                .fetch_sub(prev_len - new_len, Ordering::Relaxed)
                .saturating_sub(prev_len - new_len)
        };
        let should_flush = pending_total >= self.config.pending_bytes;

        let path = &working_record.path;
        self.log_perf(
            "stage_file",
            start.elapsed(),
            json!({
                "inode": inode,
                "bytes": new_len,
                "write_offset": write_offset,
                "write_len": data.len(),
                "append": offset.is_none(),
                "pending_total": pending_total,
                "triggered_flush": should_flush,
                "filename": path,
            }),
        );
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        debug!(
            "write_inline_range pid={} tid={} inode={} path={} offset={} len={} new_size={} pending={} flush_triggered={}",
            pid,
            tid,
            inode,
            path,
            write_offset,
            data.len(),
            new_len,
            pending_total,
            should_flush
        );
        if should_flush {
            self.trigger_async_flush();
        }
        self.flush_if_interval_elapsed()?;
        Ok(())
    }

    pub(crate) fn write_large_segments(
        &self,
        mut record: InodeRecord,
        offset: u64,
        data: &[u8],
    ) -> std::result::Result<(), i32> {
        if data.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        let inode = record.inode;
        record.update_times();
        // Insert placeholder into mutating_inodes BEFORE removing from pending_inodes.
        let active_arc = self
            .active_inodes
            .entry(inode)
            .or_insert_with(|| Arc::new(Mutex::new(ActiveInode::default())))
            .clone();
        let mut state = active_arc.lock();
        let original_entry = state.pending.take();
        let mut entry = original_entry.clone().unwrap_or(PendingEntry {
            record: record.clone(),
            data: None,
        });
        entry.record = record.clone();
        let mut data_state = entry.data.take();
        if data_state.is_none() {
            // Correctness-first flushing race fallback: if a previous flush is in
            // progress for this inode, rebuild from the flushing bytes so
            // in-flight (not yet committed) writes are preserved.
            if let Some(PendingEntry {
                data: Some(flushing_data),
                ..
            }) = &state.flushing
            {
                let existing_len = flushing_data.len();
                let existing = match self.slice_pending_bytes(flushing_data, 0, existing_len) {
                    Ok(existing) => existing,
                    Err(_) => {
                        self.log_fuse_error(
                            "write_large_segments",
                            &format!("ino={} stage=read_flushing_existing", inode),
                            EIO,
                        );
                        state.pending = original_entry.clone();
                        return Err(EIO);
                    }
                };
                if existing.is_empty() {
                    data_state = Some(PendingData::Staged(PendingSegments::new()));
                } else if existing.len() as u64 <= self.config.inline_threshold as u64 {
                    data_state = Some(PendingData::Inline(Arc::new(existing)));
                } else {
                    let chunk = match self.segments.stage_payload(&existing) {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            self.log_fuse_error(
                                "write_large_segments",
                                &format!("ino={} stage=stage_flushing_existing", inode),
                                EIO,
                            );
                            state.pending = original_entry.clone();
                            return Err(EIO);
                        }
                    };
                    data_state = Some(PendingData::Staged(PendingSegments::from_chunk(chunk)));
                }
            }
            if data_state.is_none() {
                // When no flush is in progress and the file already has committed
                // segment storage, create pending segments backed by committed
                // extents without materialising file bytes.
                match &entry.record.storage {
                    FileStorage::Segments(extents)
                        if !extents.is_empty() && entry.record.size > 0 =>
                    {
                        data_state = Some(PendingData::Staged(
                            PendingSegments::from_committed_extents(
                                extents.clone(),
                                entry.record.size,
                            ),
                        ));
                    }
                    _ => {
                        // Empty file or inline storage: materialise existing
                        // bytes directly from committed storage.
                        let existing = self
                            .read_file_range_from_storage(&entry.record, 0, entry.record.size)
                            .map_err(|_| EIO);
                        let existing = match existing {
                            Ok(existing) => existing,
                            Err(_) => {
                                self.log_fuse_error(
                                    "write_large_segments",
                                    &format!("ino={} stage=read_existing", inode),
                                    EIO,
                                );
                                state.pending = original_entry.clone();
                                return Err(EIO);
                            }
                        };
                        if existing.is_empty() {
                            data_state = Some(PendingData::Staged(PendingSegments::new()));
                        } else if existing.len() as u64 <= self.config.inline_threshold as u64 {
                            data_state = Some(PendingData::Inline(Arc::new(existing)));
                        } else {
                            let chunk = match self.segments.stage_payload(&existing) {
                                Ok(chunk) => chunk,
                                Err(_) => {
                                    self.log_fuse_error(
                                        "write_large_segments",
                                        &format!("ino={} stage=stage_existing_inline", inode),
                                        EIO,
                                    );
                                    state.pending = original_entry.clone();
                                    return Err(EIO);
                                }
                            };
                            data_state =
                                Some(PendingData::Staged(PendingSegments::from_chunk(chunk)));
                        }
                    }
                }
            }
        }
        let mut segments = match data_state {
            Some(PendingData::Staged(segs)) => segs,
            Some(PendingData::Inline(bytes)) => {
                let chunk = match self.segments.stage_payload(bytes.as_ref()) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        state.pending = original_entry.clone();
                        return Err(EIO);
                    }
                };
                PendingSegments::from_chunk(chunk)
            }
            None => PendingSegments::new(),
        };
        // Track the original file size for append-detection (adaptive flush limit).
        let prev_len = segments.total_len;
        // Track staged bytes separately: base_extents don't count toward the
        // in-flight dirty-byte watermark since they are already committed.
        let prev_staged = segments.staged_bytes();
        if segments.ensure_offset(offset).is_err() {
            self.release_pending_data(PendingData::Staged(segments));
            self.log_fuse_error(
                "write_large_segments",
                &format!("ino={} stage=ensure_offset offset={}", inode, offset),
                EIO,
            );
            state.pending = original_entry.clone();
            return Err(EIO);
        }
        let staged_chunk = match self.segments.stage_payload(data) {
            Ok(chunk) => chunk,
            Err(_) => {
                self.release_pending_data(PendingData::Staged(segments));
                self.log_fuse_error(
                    "write_large_segments",
                    &format!("ino={} stage=stage_new_payload len={}", inode, data.len()),
                    EIO,
                );
                state.pending = original_entry.clone();
                return Err(EIO);
            }
        };
        if segments
            .write_range(&self.segments, offset, staged_chunk)
            .is_err()
        {
            self.release_pending_data(PendingData::Staged(segments));
            self.log_fuse_error(
                "write_large_segments",
                &format!(
                    "ino={} stage=write_range offset={} len={}",
                    inode,
                    offset,
                    data.len()
                ),
                EIO,
            );
            state.pending = original_entry.clone();
            return Err(EIO);
        }
        let new_len = segments.total_len;
        let new_staged = segments.staged_bytes();
        entry.record.size = new_len;
        record.size = new_len;
        entry.data = Some(PendingData::Staged(segments));
        let journal_payload = if self.journal.is_some() {
            entry
                .data
                .as_ref()
                .map(|data| self.snapshot_journal_payload(data))
        } else {
            None
        };

        state.pending = Some(entry);
        self.pending_inodes.insert(inode);
        drop(state);

        if let (Some(journal), Some(payload)) = (&self.journal, &journal_payload) {
            journal.persist_record(&record, payload).map_err(|_| EIO)?;
        }
        // Use the staged-bytes delta for pending_bytes, not total_len, so that
        // base_extents (already committed) don't inflate the dirty-byte counter.
        let pending_total = if new_staged >= prev_staged {
            self.pending_bytes
                .fetch_add(new_staged - prev_staged, Ordering::Relaxed)
                .saturating_add(new_staged - prev_staged)
        } else {
            self.pending_bytes
                .fetch_sub(prev_staged - new_staged, Ordering::Relaxed)
                .saturating_sub(prev_staged - new_staged)
        };
        let pending_limit = self.pending_flush_limit_for_write(offset == prev_len, data.len());
        let should_flush = pending_total >= pending_limit;

        let path = &record.path;
        self.log_perf(
            "stage_segments",
            start.elapsed(),
            json!({
                "inode": inode,
                "bytes": new_len,
                "write_offset": offset,
                "write_len": data.len(),
                "pending_total": pending_total,
                "pending_limit": pending_limit,
                "triggered_flush": should_flush,
                "filename": path,
            }),
        );
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        debug!(
            "stage_segments pid={} tid={} inode={} path={} offset={} len={} new_size={} pending={} pending_limit={} flush_triggered={}",
            pid,
            tid,
            inode,
            record.path,
            offset,
            data.len(),
            new_len,
            pending_total,
            pending_limit,
            should_flush
        );
        if should_flush {
            self.trigger_async_flush();
        }
        self.flush_if_interval_elapsed()?;
        Ok(())
    }
}
