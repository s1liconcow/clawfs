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
        let prev_entry = {
            let mut map = self.pending_inodes.lock();
            let entry = map.remove(&inode);
            let mut mutating = self.mutating_inodes.lock();
            mutating.insert(
                inode,
                entry.clone().unwrap_or(PendingEntry {
                    record: record.clone(),
                    data: None,
                }),
            );
            entry
        };
        let original_entry = prev_entry.clone();
        let mut prev_data = prev_entry.and_then(|entry| entry.data);
        let prev_len = prev_data.as_ref().map(|d| d.len()).unwrap_or(0);
        let inline_cap = self.config.inline_threshold as u64;
        let pending_data = if new_len <= inline_cap {
            if let Some(old) = prev_data.take() {
                self.release_pending_data(old);
            }
            PendingData::Inline(data)
        } else {
            let mut segments = match prev_data.take() {
                Some(PendingData::Staged(segs)) => segs,
                Some(PendingData::Inline(bytes)) => {
                    let chunk = match self.segments.stage_payload(&bytes) {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            self.restore_mutation_on_error(inode, original_entry);
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
                        self.restore_mutation_on_error(inode, original_entry);
                        return Err(EIO);
                    }
                };
                PendingData::Staged(PendingSegments::from_chunk(chunk))
            } else if let Some(range) = append_range {
                if range.start < range.end {
                    let chunk = match self.segments.stage_payload(&data[range]) {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            self.restore_mutation_on_error(inode, original_entry);
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
                        self.restore_mutation_on_error(inode, original_entry);
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
        {
            let mut map = self.pending_inodes.lock();
            map.insert(
                inode,
                PendingEntry {
                    record: record.clone(),
                    data: Some(pending_data),
                },
            );
        }
        self.mutating_inodes.lock().remove(&inode);

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
        if let (Some(journal), Some(journal_payload)) = (&self.journal, journal_payload) {
            let journal_entry = JournalEntry {
                record: record.clone(),
                payload: journal_payload,
            };
            journal.persist_entry(&journal_entry).map_err(|_| EIO)?;
        }
        let path = record.path.clone();
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
            pid, tid, inode, record.path, new_len, pending_total, should_flush
        );
        if should_flush {
            self.flush_pending()?;
        } else {
            self.flush_if_interval_elapsed()?;
        }
        Ok(())
    }

    pub(crate) fn append_file(&self, mut record: InodeRecord, data: &[u8]) -> std::result::Result<(), i32> {
        if data.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        let inode = record.inode;
        record.update_times();
        let current_size = record.size;
        let new_len = current_size.saturating_add(data.len() as u64);
        let inline_cap = self.config.inline_threshold as u64;
        if new_len > inline_cap {
            return self.write_large_segments(record, current_size, data);
        }
        let mut needs_existing = false;
        {
            let mut map = self.pending_inodes.lock();
            let entry = map.entry(inode).or_insert_with(|| PendingEntry {
                record: record.clone(),
                data: None,
            });
            if entry.data.is_none() {
                entry.data = Some(PendingData::Inline(Vec::new()));
                needs_existing = record.size > 0;
            }
        }
        if needs_existing {
            let existing = self.read_file_bytes(&record).map_err(|_| EIO)?;
            let mut map = self.pending_inodes.lock();
            if let Some(entry) = map.get_mut(&inode) {
                entry.data = Some(PendingData::Inline(existing));
            }
        }
        let appended = data.len() as u64;
        let (prev_len, new_len, journal_payload) = {
            let mut map = self.pending_inodes.lock();
            let entry = map.get_mut(&inode).expect("pending entry must exist");
            entry.record = record.clone();
            let slot = entry
                .data
                .as_mut()
                .expect("pending data must exist before append");
            let prev_len = slot.len();
            match slot {
                PendingData::Inline(buf) => {
                    if buf.len() as u64 != record.size {
                        buf.resize(record.size as usize, 0);
                    }
                    buf.extend_from_slice(data);
                    if buf.len() as u64 > inline_cap {
                        let bytes = std::mem::take(buf);
                        let chunk = self.segments.stage_payload(&bytes).map_err(|_| EIO)?;
                        *slot = PendingData::Staged(PendingSegments::from_chunk(chunk));
                    }
                }
                PendingData::Staged(segments) => {
                    let chunk = self.segments.stage_payload(data).map_err(|_| EIO)?;
                    segments.append(chunk);
                }
            }
            let current_len = slot.len();
            entry.record.size = current_len;
            record.size = current_len;
            let payload = self
                .journal
                .as_ref()
                .map(|_| self.snapshot_journal_payload(slot));
            (prev_len, current_len, payload)
        };
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
        if let (Some(journal), Some(payload)) = (&self.journal, journal_payload) {
            let entry = JournalEntry {
                record: record.clone(),
                payload,
            };
            journal.persist_entry(&entry).map_err(|_| EIO)?;
        }
        let path = record.path.clone();
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
            self.flush_pending()?;
        } else {
            self.flush_if_interval_elapsed()?;
        }
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
        let prev_entry = {
            let mut map = self.pending_inodes.lock();
            let entry = map.remove(&inode);
            let mut mutating = self.mutating_inodes.lock();
            mutating.insert(
                inode,
                entry.clone().unwrap_or(PendingEntry {
                    record: record.clone(),
                    data: None,
                }),
            );
            entry
        };
        let original_entry = prev_entry.clone();
        let mut entry = prev_entry.unwrap_or(PendingEntry {
            record: record.clone(),
            data: None,
        });
        entry.record = record.clone();
        let mut data_state = entry.data.take();
        if data_state.is_none() {
            let existing = match self.read_file_bytes(&entry.record) {
                Ok(existing) => existing,
                Err(_) => {
                    self.log_fuse_error(
                        "write_large_segments",
                        &format!("ino={} stage=read_existing", inode),
                        EIO,
                    );
                    self.restore_mutation_on_error(inode, original_entry);
                    return Err(EIO);
                }
            };
            if existing.is_empty() {
                data_state = Some(PendingData::Staged(PendingSegments::new()));
            } else if existing.len() as u64 <= self.config.inline_threshold as u64 {
                data_state = Some(PendingData::Inline(existing));
            } else {
                let chunk = match self.segments.stage_payload(&existing) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        self.log_fuse_error(
                            "write_large_segments",
                            &format!("ino={} stage=stage_existing_inline", inode),
                            EIO,
                        );
                        self.restore_mutation_on_error(inode, original_entry);
                        return Err(EIO);
                    }
                };
                data_state = Some(PendingData::Staged(PendingSegments::from_chunk(chunk)));
            }
        }
        let mut segments = match data_state {
            Some(PendingData::Staged(segs)) => segs,
            Some(PendingData::Inline(bytes)) => {
                let chunk = match self.segments.stage_payload(&bytes) {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        self.restore_mutation_on_error(inode, original_entry);
                        return Err(EIO);
                    }
                };
                PendingSegments::from_chunk(chunk)
            }
            None => PendingSegments::new(),
        };
        let prev_len = segments.total_len;
        if let Err(_) = segments.ensure_offset(&self.segments, offset) {
            self.release_pending_data(PendingData::Staged(segments));
            self.log_fuse_error(
                "write_large_segments",
                &format!("ino={} stage=ensure_offset offset={}", inode, offset),
                EIO,
            );
            self.restore_mutation_on_error(inode, original_entry);
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
                self.restore_mutation_on_error(inode, original_entry);
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
            self.restore_mutation_on_error(inode, original_entry);
            return Err(EIO);
        }
        let new_len = segments.total_len;
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
        {
            let mut map = self.pending_inodes.lock();
            map.insert(inode, entry);
        }
        self.mutating_inodes.lock().remove(&inode);
        let delta = new_len as i64 - prev_len as i64;
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
        if let (Some(journal), Some(payload)) = (&self.journal, journal_payload) {
            let journal_entry = JournalEntry {
                record: record.clone(),
                payload,
            };
            journal.persist_entry(&journal_entry).map_err(|_| EIO)?;
        }
        let path = record.path.clone();
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
            self.flush_pending()?;
        } else {
            self.flush_if_interval_elapsed()?;
        }
        Ok(())
    }

    pub(crate) fn restore_mutation_on_error(&self, inode: u64, original: Option<PendingEntry>) {
        {
            let mut map = self.pending_inodes.lock();
            match original {
                Some(entry) => {
                    map.insert(inode, entry);
                }
                None => {
                    map.remove(&inode);
                }
            }
        }
        self.mutating_inodes.lock().remove(&inode);
    }

}
