use super::*;

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

        let delta = new_len as i64 - prev_len as i64;
        let mut total = self.pending_bytes.lock();
        if delta >= 0 {
            *total = total.saturating_add(delta as u64);
        } else {
            *total = total.saturating_sub((-delta) as u64);
        }
        let pending_total = *total;
        let should_flush = pending_total >= self.config.pending_bytes;
        drop(total);
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

    pub(crate) fn append_file(
        &self,
        mut record: InodeRecord,
        data: &[u8],
    ) -> std::result::Result<(), i32> {
        if data.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        let inode = record.inode;
        record.update_times();
        let current_size = record.size;
        let target_len = current_size.saturating_add(data.len() as u64);
        let inline_cap = self.config.inline_threshold as u64;
        if target_len > inline_cap {
            return self.write_large_segments(record, current_size, data);
        }

        // Insert placeholder into mutating_inodes BEFORE removing from pending_inodes.
        let active_arc = self
            .active_inodes
            .entry(inode)
            .or_insert_with(|| Arc::new(Mutex::new(ActiveInode::default())))
            .clone();
        let mut state = active_arc.lock();
        let original_entry = state.pending.take();

        let mut working_entry = original_entry.clone().unwrap_or(PendingEntry {
            record: record.clone(),
            data: None,
        });
        working_entry.record = record.clone();

        if working_entry.data.is_none() {
            if record.size > 0 {
                let existing = if let Some(PendingEntry { data: Some(d), .. }) = &state.flushing {
                    self.slice_pending_bytes(d, 0, record.size).map_err(|_| {
                        state.pending = original_entry.clone();
                        EIO
                    })?
                } else {
                    self.read_file_range_from_storage(&record, 0, record.size)
                        .map_err(|_| {
                            state.pending = original_entry.clone();
                            EIO
                        })?
                };
                working_entry.data = Some(PendingData::Inline(Arc::new(existing)));
            } else {
                working_entry.data = Some(PendingData::Inline(Arc::new(Vec::new())));
            }
        }

        let appended = data.len() as u64;
        let slot = working_entry
            .data
            .get_or_insert_with(|| PendingData::Inline(Arc::new(Vec::new())));
        let prev_len = slot.len();
        match slot {
            PendingData::Inline(buf) => {
                let buf = Arc::make_mut(buf);
                if buf.len() as u64 != record.size {
                    buf.resize(record.size as usize, 0);
                }
                buf.extend_from_slice(data);
                if buf.len() as u64 > inline_cap {
                    let bytes = std::mem::take(buf);
                    let chunk = self.segments.stage_payload(&bytes).map_err(|_| {
                        state.pending = original_entry.clone();
                        EIO
                    })?;
                    *slot = PendingData::Staged(PendingSegments::from_chunk(chunk));
                }
            }
            PendingData::Staged(segments) => {
                let chunk = self.segments.stage_payload(data).map_err(|_| {
                    state.pending = original_entry.clone();
                    EIO
                })?;
                segments.append(chunk);
            }
        }
        let new_len = slot.len();
        working_entry.record.size = new_len;
        record.size = new_len;

        let journal_payload = self
            .journal
            .as_ref()
            .map(|_| self.snapshot_journal_payload(slot));

        state.pending = Some(working_entry);
        self.pending_inodes.insert(inode);
        drop(state);

        if let (Some(journal), Some(payload)) = (&self.journal, &journal_payload) {
            journal.persist_record(&record, payload).map_err(|_| EIO)?;
        }

        let delta = new_len as i64 - prev_len as i64;
        let mut total = self.pending_bytes.lock();
        if delta >= 0 {
            *total = total.saturating_add(delta as u64);
        } else {
            *total = total.saturating_sub((-delta) as u64);
        }
        let pending_total = *total;
        let should_flush = pending_total >= self.config.pending_bytes;
        drop(total);

        let path = &record.path;
        self.log_perf(
            "stage_file",
            start.elapsed(),
            json!({
                "inode": inode,
                "bytes": new_len,
                "appended_bytes": appended,
                "pending_total": pending_total,
                "triggered_flush": should_flush,
                "filename": path,
            }),
        );
        let pid = process::id();
        let tid = format!("{:?}", thread::current().id());
        debug!(
            "append_file pid={} tid={} inode={} path={} appended={} new_size={} pending={} flush_triggered={}",
            pid, tid, inode, record.path, appended, new_len, pending_total, should_flush
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
            // When the file already has committed segment storage, create pending
            // segments backed by the existing extents without reading any segment
            // data.  The new write will be layered on top at flush time, avoiding
            // the O(n²) read amplification that previously occurred on every write
            // after a flush.
            match &entry.record.storage {
                FileStorage::Segments(extents) if !extents.is_empty() => {
                    data_state = Some(PendingData::Staged(
                        PendingSegments::from_committed_extents(extents.clone(), entry.record.size),
                    ));
                }
                _ => {
                    // Empty file or inline storage: materialise existing bytes.
                    // These are small (≤ inline_threshold) so reading is cheap.
                    let existing = if let Some(PendingEntry { data: Some(d), .. }) = &state.flushing
                    {
                        self.slice_pending_bytes(d, 0, record.size).map_err(|_| EIO)
                    } else {
                        self.read_file_range_from_storage(&entry.record, 0, entry.record.size)
                            .map_err(|_| EIO)
                    };
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
                        data_state = Some(PendingData::Staged(PendingSegments::from_chunk(chunk)));
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
        if let Err(_) = segments.ensure_offset(offset) {
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
        if let Err(_) = segments.write_range(&self.segments, offset, staged_chunk) {
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
        let delta = new_staged as i64 - prev_staged as i64;
        let mut total = self.pending_bytes.lock();
        if delta >= 0 {
            *total = total.saturating_add(delta as u64);
        } else {
            *total = total.saturating_sub((-delta) as u64);
        }
        let pending_total = *total;
        let pending_limit = self.pending_flush_limit_for_write(offset == prev_len, data.len());
        let should_flush = pending_total >= pending_limit;
        drop(total);

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
