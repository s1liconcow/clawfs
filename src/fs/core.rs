use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;

pub(crate) fn epoch_millis_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl OsageFs {
    #[cfg(feature = "fuse")]
    pub(crate) fn mode_to_file_type(mode: u32) -> FileType {
        match mode & S_IFMT {
            x if x == S_IFDIR => FileType::Directory,
            x if x == S_IFIFO => FileType::NamedPipe,
            x if x == S_IFCHR => FileType::CharDevice,
            x if x == S_IFBLK => FileType::BlockDevice,
            x if x == S_IFSOCK => FileType::Socket,
            _ => FileType::RegularFile,
        }
    }

    pub(crate) fn apply_umask(mode: u32, umask: u32) -> u32 {
        let type_bits = mode & S_IFMT;
        let perm_bits = (mode & 0o7777) & !umask;
        let inode_type = if type_bits == 0 { S_IFREG } else { type_bits };
        inode_type | perm_bits
    }

    pub(crate) fn normalize_node_mode(mode: u32) -> u32 {
        let type_bits = mode & S_IFMT;
        let inode_type = if type_bits == 0 { S_IFREG } else { type_bits };
        inode_type | (mode & 0o7777)
    }

    pub(crate) fn validate_os_name(name: &OsStr) -> std::result::Result<String, i32> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            if name.as_bytes().len() > NAME_MAX_BYTES {
                return Err(ENAMETOOLONG);
            }
        }
        let name = name.to_str().ok_or(EINVAL)?.to_string();
        if name.len() > NAME_MAX_BYTES {
            return Err(ENAMETOOLONG);
        }
        Ok(name)
    }

    pub(crate) fn resize_file_data_for_setattr(
        data: &mut Vec<u8>,
        target_size: u64,
    ) -> std::result::Result<(), i32> {
        let target_len = usize::try_from(target_size).map_err(|_| EFBIG)?;
        if target_len <= data.len() {
            data.truncate(target_len);
            return Ok(());
        }

        let additional = target_len - data.len();
        data.try_reserve_exact(additional).map_err(|_| EFBIG)?;
        data.resize(target_len, 0);
        Ok(())
    }

    pub(crate) fn summarize_inode_kind(kind: &InodeKind) -> String {
        match kind {
            InodeKind::Directory { children } => {
                let child_count = children.len();
                let mut sample: Vec<&str> = children.keys().map(String::as_str).take(8).collect();
                sample.sort_unstable();
                if child_count > sample.len() {
                    format!(
                        "Directory(children={}, sample={:?}, truncated={})",
                        child_count,
                        sample,
                        child_count - sample.len()
                    )
                } else {
                    format!("Directory(children={}, sample={:?})", child_count, sample)
                }
            }
            InodeKind::File => "File".to_string(),
            InodeKind::Symlink => "Symlink".to_string(),
            InodeKind::Tombstone => "Tombstone".to_string(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        segments: Arc<SegmentManager>,
        source: Option<Arc<SourceObjectStore>>,
        journal: Option<Arc<JournalManager>>,
        handle: Handle,
        client_state: Arc<ClientStateManager>,
        perf: Option<Arc<PerfLogger>>,
        replay: Option<Arc<ReplayLogger>>,
        telemetry: Option<Arc<TelemetryClient>>,
        telemetry_session_id: Option<String>,
        coordination_publisher: Option<Arc<dyn crate::coordination::CoordinationPublisher>>,
    ) -> Self {
        let fsync_on_close = config.fsync_on_close;
        let flush_interval = if config.flush_interval_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(config.flush_interval_ms))
        };
        let lookup_cache_ttl = Duration::from_millis(config.lookup_cache_ttl_ms);
        let dir_cache_ttl = Duration::from_millis(config.dir_cache_ttl_ms);
        let fuse_entry_ttl = Duration::from_secs(config.entry_ttl_secs);
        Self {
            config,
            metadata,
            superblock,
            segments,
            source,
            handle,
            client_state,
            active_inodes: Arc::new(DashMap::new()),
            pending_queue: Arc::new(crossbeam_queue::SegQueue::new()),
            pending_bytes: Arc::new(AtomicU64::new(0)),
            perf,
            replay,
            telemetry,
            telemetry_session_id,
            coordination_publisher,
            mount_ready_emitted: Arc::new(AtomicBool::new(false)),
            fsync_on_close,
            flush_interval,
            last_flush: Arc::new(AtomicU64::new(epoch_millis_now())),
            flush_lock: Arc::new(Mutex::new(())),
            dir_locks: Arc::new(DashMap::new()),
            flush_scheduled: Arc::new(AtomicBool::new(false)),
            fuse_entry_ttl,
            lookup_cache_ttl,
            dir_cache_ttl,
            journal,
            flush_commit_hook: Arc::new(std::sync::OnceLock::new()),
            #[cfg(feature = "fuse")]
            kernel_notifier: Arc::new(std::sync::OnceLock::new()),
        }
    }

    pub fn set_flush_commit_hook(&self, hook: Arc<dyn flush::FlushCommitHook>) {
        let _ = self.flush_commit_hook.set(hook);
    }

    #[cfg(feature = "fuse")]
    pub fn set_kernel_notifier(&self, notifier: Arc<Notifier>) {
        let _ = self.kernel_notifier.set(notifier);
    }

    #[cfg(feature = "fuse")]
    pub fn kernel_notifier_slot(&self) -> Arc<std::sync::OnceLock<Arc<Notifier>>> {
        self.kernel_notifier.clone()
    }

    pub fn replay_journal(&self) -> Result<usize> {
        let Some(journal) = &self.journal else {
            return Ok(0);
        };

        let entries = journal.load_entries()?;
        if entries.is_empty() {
            return Ok(0);
        }
        let mut restored = 0;
        for entry in entries {
            let inode = entry.record.inode;
            let record = entry.record;
            let data_opt = match entry.payload {
                JournalPayload::None => None,
                JournalPayload::Inline(bytes) => Some(PendingData::Inline(Arc::new(bytes))),
                JournalPayload::StageFile(chunk) => {
                    if chunk.path.exists() {
                        Some(PendingData::Staged(PendingSegments::from_chunk(chunk)))
                    } else {
                        log::warn!(
                            "staged payload {} missing for inode {}",
                            chunk.path.display(),
                            inode
                        );
                        None
                    }
                }
                JournalPayload::StageChunks(chunks) => {
                    let mut present = Vec::new();
                    for chunk in chunks {
                        if chunk.path.exists() {
                            present.push(chunk);
                        } else {
                            log::warn!(
                                "staged payload {} missing for inode {}",
                                chunk.path.display(),
                                inode
                            );
                        }
                    }
                    if present.is_empty() {
                        None
                    } else {
                        // The journal record carries the committed storage pointer
                        // at the time of the write.  Reconstruct base_extents from
                        // it so that flush can merge them with the replayed chunks
                        // without re-reading segment data.
                        let base_extents = match &record.storage {
                            FileStorage::Segments(extents) if record.size > 0 => extents.clone(),
                            _ => Vec::new(),
                        };
                        Some(PendingData::Staged(PendingSegments {
                            base_extents: Arc::new(base_extents),
                            chunks: Arc::new(present),
                            total_len: record.size,
                        }))
                    }
                }
            };
            let data_len = data_opt.as_ref().map(|d| d.len()).unwrap_or(0);
            let active_arc = self
                .active_inodes
                .entry(inode)
                .or_insert_with(|| Arc::new(Mutex::new(ActiveInode::default())))
                .clone();
            let mut state = active_arc.lock();
            let old = state.pending.replace(PendingEntry {
                record,
                data: data_opt,
            });
            if !state.is_queued {
                state.is_queued = true;
                self.pending_queue.push(inode);
            }
            if let Some(old_entry) = old
                && let Some(old_data) = old_entry.data
            {
                self.release_pending_data(old_data);
            }
            drop(state);
            if data_len > 0 {
                self.pending_bytes.fetch_add(data_len, Ordering::Relaxed);
            }
            restored += 1;
        }
        debug!("replay_journal staged {} entries", restored);
        self.flush_pending()
            .map_err(|code| anyhow!("failed to flush replayed journal: {code}"))?;
        Ok(restored)
    }

    pub(crate) fn block_on<F, T>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        self.handle.block_on(fut)
    }

    pub(crate) fn allocate_inode_id(&self) -> Result<u64> {
        self.client_state
            .next_inode_id(self.config.inode_batch, |count| {
                self.block_on(self.superblock.reserve_inodes(count))
            })
    }

    pub(crate) fn allocate_segment_id(&self) -> Result<u64> {
        self.client_state
            .next_segment_id(self.config.segment_batch, |count| {
                self.block_on(self.superblock.reserve_segments(count))
            })
    }

    pub(crate) fn lock_dir(
        &self,
        parent: u64,
    ) -> parking_lot::lock_api::ArcMutexGuard<parking_lot::RawMutex, ()> {
        let lock = self
            .dir_locks
            .entry(parent)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        parking_lot::Mutex::lock_arc(&lock)
    }

    pub(crate) fn lock_dir_pair(
        &self,
        a: u64,
        b: u64,
    ) -> (
        parking_lot::lock_api::ArcMutexGuard<parking_lot::RawMutex, ()>,
        Option<parking_lot::lock_api::ArcMutexGuard<parking_lot::RawMutex, ()>>,
    ) {
        if a == b {
            return (self.lock_dir(a), None);
        }
        let (first, second) = if a < b { (a, b) } else { (b, a) };
        let g1 = self.lock_dir(first);
        let g2 = self.lock_dir(second);
        (g1, Some(g2))
    }

    pub(crate) fn build_child_path(parent: &InodeRecord, name: &str) -> String {
        if parent.inode == ROOT_INODE || parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path.trim_end_matches('/'), name)
        }
    }

    #[cfg(feature = "fuse")]
    pub(crate) fn record_attr(record: &InodeRecord) -> FileAttr {
        let attr_kind = match record.kind {
            InodeKind::Directory { .. } => FileType::Directory,
            InodeKind::Symlink => FileType::Symlink,
            InodeKind::File => Self::mode_to_file_type(record.mode),
            InodeKind::Tombstone => FileType::RegularFile,
        };
        FileAttr {
            ino: record.inode,
            size: record.size,
            blocks: record.size.div_ceil(512),
            atime: to_system_time(record.atime),
            mtime: to_system_time(record.mtime),
            ctime: to_system_time(record.ctime),
            crtime: to_system_time(record.ctime),
            kind: attr_kind,
            perm: (record.mode & 0o7777) as u16,
            nlink: if record.is_dir() {
                2 + record.children().map(|c| c.len() as u32).unwrap_or(0)
            } else {
                record.link_count
            },
            uid: record.uid,
            gid: record.gid,
            rdev: record.rdev,
            flags: 0,
            blksize: 4096,
        }
    }

    #[cfg(feature = "fuse")]
    pub(crate) fn fuse_attr_ttl(&self, record: &InodeRecord) -> Duration {
        if record.is_dir() {
            self.fuse_entry_ttl
        } else {
            Duration::ZERO
        }
    }

    #[cfg(feature = "fuse")]
    pub(crate) fn fuse_attr_ttl_for_attr(&self, attr: &FileAttr) -> Duration {
        if attr.kind == FileType::Directory {
            self.fuse_entry_ttl
        } else {
            Duration::ZERO
        }
    }

    /// Check all three in-memory dirty maps for an inode.  Returns `Some(Ok(record))`,
    /// `Some(Err(ENOENT))` (tombstone), or `None` (not found in any map).
    fn load_inode_in_memory(&self, ino: u64) -> Option<std::result::Result<InodeRecord, i32>> {
        if let Some(active_arc) = self.active_inodes.get(&ino) {
            let state = active_arc.lock();
            let mut record_opt = None;
            if let Some(entry) = &state.pending {
                record_opt = Some(entry.record.clone());
            } else if let Some(entry) = &state.flushing {
                record_opt = Some(entry.record.clone());
            }
            if let Some(record) = record_opt {
                if matches!(record.kind, InodeKind::Tombstone) {
                    return Some(Err(ENOENT));
                }
                return Some(Ok(record));
            }
        }
        None
    }

    pub(crate) fn load_inode(&self, ino: u64) -> std::result::Result<InodeRecord, i32> {
        if let Some(result) = self.load_inode_in_memory(ino) {
            return result;
        }
        let fetched = self.block_on(self.metadata.get_inode_with_ttl(
            ino,
            self.lookup_cache_ttl,
            self.dir_cache_ttl,
        ));
        let fetched = match fetched {
            Ok(record) => record,
            Err(err) => {
                error!(
                    "load_inode metadata error ino={} active={} err={:#}",
                    ino,
                    self.active_inodes.contains_key(&ino),
                    err
                );
                return Err(EIO);
            }
        };
        if let Some(record) = fetched {
            return Ok(record);
        }
        // Re-check in-memory maps: the inode may have been moving between
        // pending/mutating/flushing during our sequential checks above (each
        // DashMap lookup is atomic but the three-step sequence is not).  A
        // second pass after the (slow) metadata lookup catches this race.
        if let Some(result) = self.load_inode_in_memory(ino) {
            return result;
        }
        debug!(
            "load_inode miss ino={} active={}",
            ino,
            self.active_inodes.contains_key(&ino)
        );
        Err(ENOENT)
    }

    #[cfg(feature = "fuse")]
    pub(crate) fn load_inode_for_fuse_getattr(
        &self,
        ino: u64,
    ) -> std::result::Result<InodeRecord, i32> {
        if let Some(result) = self.load_inode_in_memory(ino) {
            return result;
        }
        let fetched = self.block_on(self.metadata.get_inode_with_ttl(
            ino,
            Duration::ZERO,
            self.dir_cache_ttl,
        ));
        let fetched = match fetched {
            Ok(record) => record,
            Err(err) => {
                error!(
                    "load_inode_for_fuse_getattr metadata error ino={} active={} err={:#}",
                    ino,
                    self.active_inodes.contains_key(&ino),
                    err
                );
                return Err(EIO);
            }
        };
        if let Some(record) = fetched {
            return Ok(record);
        }
        if let Some(result) = self.load_inode_in_memory(ino) {
            return result;
        }
        debug!(
            "load_inode_for_fuse_getattr miss ino={} active={}",
            ino,
            self.active_inodes.contains_key(&ino)
        );
        Err(ENOENT)
    }

    /// Batch-load multiple inodes. Checks active_inodes first, then fetches
    /// remaining inodes from the metadata cache in a single batch (one cache
    /// lock acquisition + grouped shard reloads for misses).
    pub(crate) fn load_inodes_batch(
        &self,
        inos: &[u64],
    ) -> std::result::Result<HashMap<u64, InodeRecord>, i32> {
        let mut result = HashMap::with_capacity(inos.len());
        let mut remaining = Vec::new();

        // Phase 1: resolve from active_inodes (pending/flushing state).
        for &ino in inos {
            if let Some(r) = self.load_inode_in_memory(ino) {
                match r {
                    Ok(record) => {
                        result.insert(ino, record);
                    }
                    Err(_) => {
                        // Tombstone or error — skip this inode.
                    }
                }
            } else {
                remaining.push(ino);
            }
        }

        if remaining.is_empty() {
            return Ok(result);
        }

        // Phase 2: batch-fetch from metadata cache.
        let fetched = self.block_on(self.metadata.get_inodes_cached_batch(
            &remaining,
            self.lookup_cache_ttl,
            self.dir_cache_ttl,
        ));
        match fetched {
            Ok(map) => {
                for (ino, record) in map {
                    result.insert(ino, record);
                }
            }
            Err(err) => {
                error!("load_inodes_batch metadata error: {:#}", err);
                return Err(EIO);
            }
        }

        // Phase 3: re-check in-memory maps for any remaining misses
        // (same race-prevention as load_inode's second pass).
        for &ino in &remaining {
            if result.contains_key(&ino) {
                continue;
            }
            if let Some(Ok(record)) = self.load_inode_in_memory(ino) {
                result.insert(ino, record);
            }
        }

        Ok(result)
    }

    pub(crate) fn read_file_bytes(&self, record: &InodeRecord) -> Result<Vec<u8>> {
        self.read_file_range_inner(record, 0, record.size)
    }

    pub(crate) fn read_file_range(
        &self,
        record: &InodeRecord,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>> {
        if size == 0 || offset >= record.size {
            return Ok(Vec::new());
        }
        let range_start = offset;
        let range_end = range_start.saturating_add(size as u64).min(record.size);
        if range_end <= range_start {
            return Ok(Vec::new());
        }
        self.read_file_range_inner(record, range_start, range_end)
    }

    fn read_file_range_inner(
        &self,
        record: &InodeRecord,
        range_start: u64,
        range_end: u64,
    ) -> Result<Vec<u8>> {
        if range_end <= range_start {
            return Ok(Vec::new());
        }
        let (pending_data, flushing_data) =
            if let Some(active_arc) = self.active_inodes.get(&record.inode) {
                let state = active_arc.lock();
                let p_data = state.pending.as_ref().and_then(|e| e.data.clone());
                let f_data = state.flushing.as_ref().and_then(|e| e.data.clone());
                (p_data, f_data)
            } else {
                (None, None)
            };
        if let Some(data) = pending_data {
            return self.slice_pending_bytes(&data, range_start, range_end);
        }
        if let Some(data) = flushing_data {
            return self.slice_pending_bytes(&data, range_start, range_end);
        }
        // When both pending and flushing data are None but the record
        // carries a stale placeholder storage (e.g. Inline([]) for a
        // non-zero-size file — set during flush classify), fall back to
        // the metadata cache for the authoritative committed storage.
        // This happens when stage_inode (rename, setattr, etc.) creates a
        // metadata-only pending entry from a record loaded while the inode
        // was in the flushing state, and the flush completes before the
        // next read.
        if record.size > 0
            && Self::is_placeholder_storage(&record.storage)
            && let Some(committed) = self.metadata.get_cached_inode(record.inode)
            && !Self::is_placeholder_storage(&committed.storage)
        {
            return self.read_file_range_from_storage(&committed, range_start, range_end);
        }
        self.read_file_range_from_storage(record, range_start, range_end)
    }

    fn is_placeholder_storage(storage: &FileStorage) -> bool {
        match storage {
            FileStorage::Inline(bytes) => bytes.is_empty(),
            FileStorage::InlineEncoded(bytes) => bytes.payload.is_empty(),
            _ => false,
        }
    }

    pub(crate) fn read_file_range_from_storage(
        &self,
        record: &InodeRecord,
        range_start: u64,
        range_end: u64,
    ) -> Result<Vec<u8>> {
        match &record.storage {
            FileStorage::Inline(_) | FileStorage::InlineEncoded(_) => {
                let bytes = self.decode_inline_storage(&record.storage)?;
                Ok(Self::slice_bytes_in_range(bytes, range_start, range_end))
            }
            FileStorage::LegacySegment(ptr) => {
                let bytes = self.segments.read_pointer_arc(ptr)?;
                Ok(Self::slice_arc_in_range(&bytes, range_start, range_end))
            }
            FileStorage::ExternalObject(ext) => {
                let Some(source) = &self.source else {
                    anyhow::bail!("source storage is not configured");
                };
                let start = range_start.min(ext.size);
                let end = range_end.min(ext.size);
                self.block_on(source.read_range(&ext.key, start, end))
            }
            FileStorage::Segments(extents) => {
                let out_len = (range_end - range_start) as usize;
                let mut buffer = vec![0u8; out_len];
                let plain_codec = self.segments.is_plain_codec();
                let mut start_idx = extents.partition_point(|ext| ext.logical_offset < range_start);
                start_idx = start_idx.saturating_sub(1);
                // Extents may overlap after partial overwrites. We read each
                // extent by its own payload length and overlay in sorted order
                // so newer extents (appended later, stable-sorted after base at
                // equal offsets) override older bytes.
                for extent in extents[start_idx..].iter() {
                    let extent_start = extent.logical_offset;
                    if extent_start >= range_end {
                        break;
                    }
                    if plain_codec {
                        let payload_len = extent
                            .pointer
                            .length
                            .saturating_sub(crate::segment::SEGMENT_ENTRY_CODEC_HEADER_LEN as u64);
                        let extent_end = extent_start.saturating_add(payload_len);
                        if extent_end <= range_start {
                            continue;
                        }
                        let overlap_start = extent_start.max(range_start);
                        let overlap_end = extent_end.min(range_end);
                        if overlap_end <= overlap_start {
                            continue;
                        }
                        let dst_start = (overlap_start - range_start) as usize;
                        let dst_end = (overlap_end - range_start) as usize;
                        let local_start = overlap_start - extent_start;
                        let local_end = overlap_end - extent_start;
                        match self.segments.read_pointer_subrange(
                            &extent.pointer,
                            local_start,
                            local_end,
                        ) {
                            Ok(bytes) => {
                                buffer[dst_start..dst_end]
                                    .copy_from_slice(&bytes[..dst_end - dst_start]);
                                continue;
                            }
                            Err(e) => {
                                log::error!(
                                    "read_file_range subrange read failed ino={} extent_offset={} range={}..{} err={:#}",
                                    record.inode,
                                    extent_start,
                                    range_start,
                                    range_end,
                                    e
                                );
                                return Err(e);
                            }
                        }
                    }

                    let bytes = match self.segments.read_pointer_arc(&extent.pointer) {
                        Ok(b) => b,
                        Err(e) => {
                            log::error!(
                                "read_file_range segment read failed ino={} extent_offset={} gen={} seg={} ptr_off={} ptr_len={} range={}..{} num_extents={} err={:#}",
                                record.inode,
                                extent_start,
                                extent.pointer.generation,
                                extent.pointer.segment_id,
                                extent.pointer.offset,
                                extent.pointer.length,
                                range_start,
                                range_end,
                                extents.len(),
                                e
                            );
                            return Err(e);
                        }
                    };
                    let extent_end = extent_start.saturating_add(bytes.len() as u64);
                    if extent_end <= range_start {
                        continue;
                    }
                    let overlap_start = extent_start.max(range_start);
                    let overlap_end = extent_end.min(range_end);
                    if overlap_end <= overlap_start {
                        continue;
                    }
                    let dst_start = (overlap_start - range_start) as usize;
                    let dst_end_actual = (overlap_end - range_start) as usize;
                    let src_start = (overlap_start - extent_start) as usize;
                    let src_end = (overlap_end - extent_start) as usize;
                    buffer[dst_start..dst_end_actual].copy_from_slice(&bytes[src_start..src_end]);
                }
                Ok(buffer)
            }
        }
    }

    pub(crate) fn inline_codec_config(&self) -> InlineCodecConfig {
        InlineCodecConfig {
            compression: self.config.inline_compression,
            encryption_key: self.config.inline_encryption_key.clone(),
        }
    }

    pub(crate) fn encode_inline_storage(&self, bytes: &[u8]) -> Result<FileStorage> {
        encode_inline_payload_storage(bytes, &self.inline_codec_config())
    }

    pub(crate) fn decode_inline_storage(&self, storage: &FileStorage) -> Result<Vec<u8>> {
        decode_inline_payload_storage(storage, self.config.inline_encryption_key.as_deref())
    }

    pub(crate) fn stage_inode(&self, record: InodeRecord) -> std::result::Result<(), i32> {
        let inode = record.inode;
        let kind_summary = Self::summarize_inode_kind(&record.kind);

        // Journal first — borrows record, avoids clone.
        if let Some(journal) = &self.journal {
            journal
                .persist_record(&record, &JournalPayload::None)
                .map_err(|_| EIO)?;
        }

        // Move record into pending map — zero clones.
        let active_arc = self
            .active_inodes
            .entry(inode)
            .or_insert_with(|| Arc::new(Mutex::new(ActiveInode::default())))
            .clone();
        let mut state = active_arc.lock();
        if let Some(occupied) = state.pending.as_mut() {
            if matches!(record.kind, InodeKind::Tombstone)
                && let Some(data) = occupied.data.take()
            {
                let len = data.len();
                self.release_pending_data(data);
                if len > 0 {
                    self.pending_bytes.fetch_sub(len, Ordering::Relaxed);
                }
            }
            occupied.record = record;
        } else {
            // When there is no existing pending entry, this is a metadata-only
            // stage (rename, setattr, etc.).  If the record carries a stale
            // placeholder storage (Inline([]) for a non-zero file) — typically
            // because it was loaded from a flushing entry — fix it from the
            // metadata cache so subsequent reads don't get zero bytes.
            let mut record = record;
            if record.size > 0
                && !matches!(record.kind, InodeKind::Tombstone)
                && Self::is_placeholder_storage(&record.storage)
                && let Some(committed) = self.metadata.get_cached_inode(inode)
                && !Self::is_placeholder_storage(&committed.storage)
            {
                record.storage = committed.storage;
            }
            state.pending = Some(PendingEntry { record, data: None });
        }
        if !state.is_queued {
            state.is_queued = true;
            self.pending_queue.push(inode);
        }
        drop(state);

        debug!(
            "stage_inode inode={} kind={} metadata-staged",
            inode, kind_summary
        );
        self.flush_if_interval_elapsed()?;
        Ok(())
    }

    pub(crate) fn snapshot_journal_payload(&self, data: &PendingData) -> JournalPayload {
        match data {
            PendingData::Inline(bytes) => JournalPayload::Inline(bytes.as_ref().clone()),
            PendingData::Staged(segments) => {
                JournalPayload::StageChunks(segments.chunks.as_ref().clone())
            }
        }
    }

    pub(crate) fn slice_pending_bytes(
        &self,
        data: &PendingData,
        range_start: u64,
        range_end: u64,
    ) -> Result<Vec<u8>> {
        match data {
            PendingData::Inline(bytes) => Ok(Self::slice_bytes_in_range(
                bytes.as_ref().clone(),
                range_start,
                range_end,
            )),
            PendingData::Staged(segments) if segments.base_extents.is_empty() => {
                // All pending data is in staged chunks with no committed backing.
                // Serve the requested range directly from overlapped chunks to
                // avoid materializing the full pending payload in memory.
                self.slice_staged_chunks_only(segments, range_start, range_end)
            }
            PendingData::Staged(segments) => {
                // File has committed base extents plus staged chunks layered on top.
                // Serve the range by reading base extents first, then overlaying
                // staged chunks (which represent newer writes).
                self.slice_staged_with_base_extents(segments, range_start, range_end)
            }
        }
    }

    /// Read a byte range from pending segments that have both committed base
    /// extents and locally-staged chunks.  Base extents provide data for
    /// regions not yet overwritten by staged chunks.
    fn slice_staged_chunks_only(
        &self,
        segments: &PendingSegments,
        range_start: u64,
        range_end: u64,
    ) -> Result<Vec<u8>> {
        let out_len = (range_end - range_start) as usize;
        let mut buffer = vec![0u8; out_len];

        let chunks = segments.chunks.as_ref();
        let start_idx = chunks
            .partition_point(|chunk| chunk.logical_offset.saturating_add(chunk.len) <= range_start);
        for chunk in chunks[start_idx..].iter() {
            let chunk_start = chunk.logical_offset;
            let chunk_end = chunk_start.saturating_add(chunk.len);
            if chunk_start >= range_end {
                break;
            }
            if chunk_end <= range_start {
                continue;
            }
            let overlap_start = chunk_start.max(range_start);
            let overlap_end = chunk_end.min(range_end);
            if overlap_end <= overlap_start {
                continue;
            }
            let in_chunk_start = overlap_start - chunk_start;
            let in_chunk_len = overlap_end - overlap_start;
            let chunk_bytes =
                self.segments
                    .read_staged_chunk_range(chunk, in_chunk_start, in_chunk_len)?;
            let dst_start = (overlap_start - range_start) as usize;
            let dst_end = (overlap_end - range_start) as usize;
            buffer[dst_start..dst_end].copy_from_slice(&chunk_bytes);
        }
        Ok(buffer)
    }

    /// Read a byte range from pending segments that have both committed base
    /// extents and locally-staged chunks.  Base extents provide data for
    /// regions not yet overwritten by staged chunks.
    fn slice_staged_with_base_extents(
        &self,
        segments: &PendingSegments,
        range_start: u64,
        range_end: u64,
    ) -> Result<Vec<u8>> {
        let out_len = (range_end - range_start) as usize;
        let mut buffer = vec![0u8; out_len];

        // Fill from committed base extents (sorted by logical offset). Extents
        // may overlap; overlay in order so later entries win.
        let plain_codec = self.segments.is_plain_codec();
        let base_extents = segments.base_extents.as_ref();
        let mut base_start_idx =
            base_extents.partition_point(|ext| ext.logical_offset < range_start);
        base_start_idx = base_start_idx.saturating_sub(1);
        for extent in base_extents[base_start_idx..].iter() {
            let extent_start = extent.logical_offset;
            if extent_start >= range_end {
                break;
            }
            if plain_codec {
                let payload_len = extent
                    .pointer
                    .length
                    .saturating_sub(crate::segment::SEGMENT_ENTRY_CODEC_HEADER_LEN as u64);
                let extent_end = extent_start.saturating_add(payload_len);
                if extent_end <= range_start {
                    continue;
                }
                let overlap_start = extent_start.max(range_start);
                let overlap_end = extent_end.min(range_end);
                if overlap_end <= overlap_start {
                    continue;
                }
                let dst_start = (overlap_start - range_start) as usize;
                let dst_end = (overlap_end - range_start) as usize;
                let local_start = overlap_start - extent_start;
                let local_end = overlap_end - extent_start;
                let bytes =
                    self.segments
                        .read_pointer_subrange(&extent.pointer, local_start, local_end)?;
                buffer[dst_start..dst_end].copy_from_slice(&bytes[..dst_end - dst_start]);
                continue;
            }

            let bytes = self.segments.read_pointer_arc(&extent.pointer)?;
            let extent_end = extent_start.saturating_add(bytes.len() as u64);
            if extent_end <= range_start {
                continue;
            }
            let overlap_start = extent_start.max(range_start);
            let overlap_end = extent_end.min(range_end);
            if overlap_end <= overlap_start {
                continue;
            }
            let dst_start = (overlap_start - range_start) as usize;
            let dst_end_actual = (overlap_end - range_start) as usize;
            let src_start = (overlap_start - extent_start) as usize;
            let src_end = (overlap_end - extent_start) as usize;
            buffer[dst_start..dst_end_actual].copy_from_slice(&bytes[src_start..src_end]);
        }

        // Overlay with staged chunks (sorted by logical offset; newer chunks
        // override base extents for overlapping regions).
        let chunks = segments.chunks.as_ref();
        let chunk_start_idx = chunks
            .partition_point(|chunk| chunk.logical_offset.saturating_add(chunk.len) <= range_start);
        for chunk in chunks[chunk_start_idx..].iter() {
            let chunk_start = chunk.logical_offset;
            let chunk_end = chunk_start.saturating_add(chunk.len);
            if chunk_start >= range_end {
                break;
            }
            if chunk_end <= range_start {
                continue;
            }
            let overlap_start = chunk_start.max(range_start);
            let overlap_end = chunk_end.min(range_end);
            if overlap_end <= overlap_start {
                continue;
            }
            let in_chunk_start = overlap_start - chunk_start;
            let in_chunk_len = overlap_end - overlap_start;
            let chunk_bytes =
                self.segments
                    .read_staged_chunk_range(chunk, in_chunk_start, in_chunk_len)?;
            let dst_start = (overlap_start - range_start) as usize;
            let dst_end = (overlap_end - range_start) as usize;
            buffer[dst_start..dst_end].copy_from_slice(&chunk_bytes);
        }

        Ok(buffer)
    }

    fn slice_arc_in_range(bytes: &[u8], range_start: u64, range_end: u64) -> Vec<u8> {
        let len = bytes.len() as u64;
        if range_start >= len {
            return Vec::new();
        }
        let end = range_end.min(len);
        if end <= range_start {
            return Vec::new();
        }
        bytes[range_start as usize..end as usize].to_vec()
    }

    fn slice_bytes_in_range(bytes: Vec<u8>, range_start: u64, range_end: u64) -> Vec<u8> {
        let len = bytes.len() as u64;
        if range_start >= len {
            return Vec::new();
        }
        let end = range_end.min(len);
        if end <= range_start {
            return Vec::new();
        }
        bytes[range_start as usize..end as usize].to_vec()
    }

    pub(crate) fn release_pending_data(&self, data: PendingData) {
        if let PendingData::Staged(segments) = data
            && let Err(err) = self
                .segments
                .release_staged_chunks(segments.chunks.as_ref())
        {
            log::warn!("failed to release staged payload batch: {err:?}");
        }
    }

    /// Owned variant — moves parent into pending map without cloning.
    /// Use this on the hot path (create, mkdir, mknod, symlink, link, unlink).
    pub(crate) fn update_parent_move(
        &self,
        mut parent: InodeRecord,
        name: String,
        child: u64,
    ) -> std::result::Result<(), i32> {
        let entries = parent.children_mut().ok_or(ENOTDIR)?;
        entries.insert(name, child);
        parent.update_times();
        self.stage_inode(parent)
    }

    /// Borrowed variant — clones parent into pending map.
    /// Use this in rename where the caller continues to use `parent` afterward.
    pub(crate) fn update_parent_ref(
        &self,
        parent: &mut InodeRecord,
        name: String,
        child: u64,
    ) -> std::result::Result<(), i32> {
        let entries = parent.children_mut().ok_or(ENOTDIR)?;
        entries.insert(name, child);
        parent.update_times();
        self.stage_inode(parent.clone())
    }

    /// Owned variant — moves parent into pending map without cloning.
    pub(crate) fn remove_from_parent_move(
        &self,
        mut parent: InodeRecord,
        name: &str,
    ) -> std::result::Result<(), i32> {
        let entries = parent.children_mut().ok_or(ENOTDIR)?;
        entries.remove(name);
        parent.update_times();
        self.stage_inode(parent)
    }

    /// Borrowed variant — clones parent into pending map.
    pub(crate) fn remove_from_parent_ref(
        &self,
        parent: &mut InodeRecord,
        name: &str,
    ) -> std::result::Result<(), i32> {
        let entries = parent.children_mut().ok_or(ENOTDIR)?;
        entries.remove(name);
        parent.update_times();
        self.stage_inode(parent.clone())
    }

    pub(crate) fn unlink_file_entry(
        &self,
        parent: &mut InodeRecord,
        name: &str,
        record: &mut InodeRecord,
    ) -> std::result::Result<(), i32> {
        self.remove_from_parent_ref(parent, name)?;
        if record.link_count > 1 {
            record.dec_links();
            record.update_times();
            self.stage_inode(record.clone())?
        } else {
            // Keep the inode record (with nlink=0) so open file handles can
            // continue I/O after unlink, matching POSIX semantics.
            record.link_count = 0;
            record.update_times();
            self.stage_inode(record.clone())?;
        }
        Ok(())
    }

    pub(crate) fn rename_entry(
        &self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
        flags: u32,
    ) -> std::result::Result<(), i32> {
        if flags & !(RENAME_NOREPLACE_FLAG) != 0 {
            return Err(EINVAL);
        }
        let old_name = name.to_string();
        let new_name = newname.to_string();
        if parent == newparent && old_name == new_name {
            return Ok(());
        }

        if parent == newparent {
            let mut dir = self.load_inode(parent)?;
            if !dir.is_dir() {
                return Err(ENOTDIR);
            }
            let child_ino = dir
                .children()
                .and_then(|children| children.get(&old_name).copied())
                .ok_or(ENOENT)?;
            let mut target = self.load_inode(child_ino)?;
            if target.is_dir() && self.is_descendant(target.inode, newparent)? {
                return Err(EINVAL);
            }
            if let Some(existing) = dir
                .children()
                .and_then(|children| children.get(&new_name).copied())
            {
                if flags & RENAME_NOREPLACE_FLAG != 0 {
                    return Err(EEXIST);
                }
                if existing != target.inode {
                    let mut victim = self.load_inode(existing)?;
                    if victim.is_dir() {
                        if !target.is_dir() {
                            return Err(EISDIR);
                        }
                        if victim.children().map(|c| !c.is_empty()).unwrap_or(false) {
                            return Err(ENOTEMPTY);
                        }
                        self.remove_from_parent_ref(&mut dir, &new_name)?;
                        let tombstone = InodeRecord::tombstone(victim.inode);
                        self.stage_inode(tombstone)?;
                    } else {
                        self.unlink_file_entry(&mut dir, &new_name, &mut victim)?;
                    }
                }
            }
            self.remove_from_parent_ref(&mut dir, &old_name)?;
            self.update_parent_ref(&mut dir, new_name.clone(), target.inode)?;
            target.parent = dir.inode;
            target.name = new_name;
            target.path = Self::build_child_path(&dir, &target.name);
            target.update_times();
            self.stage_inode(target.clone())?;
            if target.is_dir() {
                self.refresh_descendant_paths(&target)?;
            }
            return Ok(());
        }

        let mut src_parent = self.load_inode(parent)?;
        if !src_parent.is_dir() {
            return Err(ENOTDIR);
        }
        let mut dst_parent = self.load_inode(newparent)?;
        if !dst_parent.is_dir() {
            return Err(ENOTDIR);
        }
        let child_ino = src_parent
            .children()
            .and_then(|children| children.get(&old_name).copied())
            .ok_or(ENOENT)?;
        let mut target = self.load_inode(child_ino)?;
        if target.is_dir() && self.is_descendant(target.inode, newparent)? {
            return Err(EINVAL);
        }
        if let Some(existing) = dst_parent
            .children()
            .and_then(|children| children.get(&new_name).copied())
        {
            if flags & RENAME_NOREPLACE_FLAG != 0 {
                return Err(EEXIST);
            }
            if existing != target.inode {
                let mut victim = self.load_inode(existing)?;
                if victim.is_dir() {
                    if !target.is_dir() {
                        return Err(EISDIR);
                    }
                    if victim.children().map(|c| !c.is_empty()).unwrap_or(false) {
                        return Err(ENOTEMPTY);
                    }
                    self.remove_from_parent_ref(&mut dst_parent, &new_name)?;
                    let tombstone = InodeRecord::tombstone(victim.inode);
                    self.stage_inode(tombstone)?;
                } else {
                    self.unlink_file_entry(&mut dst_parent, &new_name, &mut victim)?;
                }
            }
        }
        self.remove_from_parent_ref(&mut src_parent, &old_name)?;
        self.update_parent_ref(&mut dst_parent, new_name.clone(), target.inode)?;
        target.parent = dst_parent.inode;
        target.name = new_name;
        target.path = Self::build_child_path(&dst_parent, &target.name);
        target.update_times();
        self.stage_inode(target.clone())?;
        if target.is_dir() {
            self.refresh_descendant_paths(&target)?;
        }
        Ok(())
    }

    pub(crate) fn refresh_descendant_paths(
        &self,
        inode: &InodeRecord,
    ) -> std::result::Result<(), i32> {
        if let Some(children) = inode.children() {
            let entries: Vec<(String, u64)> = children
                .iter()
                .map(|(name, ino)| (name.clone(), *ino))
                .collect();
            for (name, child_ino) in entries {
                let mut child = self.load_inode(child_ino)?;
                child.parent = inode.inode;
                child.name = name.clone();
                child.path = Self::build_child_path(inode, &name);
                child.update_times();
                self.stage_inode(child.clone())?;
                if child.is_dir() {
                    self.refresh_descendant_paths(&child)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn maybe_import_source_child(
        &self,
        parent: &InodeRecord,
        name: &str,
    ) -> std::result::Result<Option<InodeRecord>, i32> {
        let Some(source) = &self.source else {
            return Ok(None);
        };
        let discovered = self
            .block_on(source.lookup_child(&parent.path, name))
            .map_err(|_| EIO)?;
        let Some(discovered) = discovered else {
            return Ok(None);
        };

        let inode = self.allocate_inode_id().map_err(|_| EIO)?;
        let path = Self::build_child_path(parent, name);
        let mut child = match discovered {
            DiscoveredEntry::Directory => InodeRecord::new_directory(
                inode,
                parent.inode,
                name.to_string(),
                path,
                parent.uid,
                parent.gid,
            ),
            DiscoveredEntry::File(meta) => {
                let mut record = InodeRecord::new_file(
                    inode,
                    parent.inode,
                    name.to_string(),
                    path,
                    parent.uid,
                    parent.gid,
                );
                record.size = meta.size;
                if let Some(ts) = meta.last_modified_ns
                    && let Ok(parsed) = OffsetDateTime::from_unix_timestamp_nanos(ts as i128)
                {
                    record.atime = parsed;
                    record.mtime = parsed;
                    record.ctime = parsed;
                }
                record.storage = FileStorage::ExternalObject(crate::inode::ExternalObject {
                    key: meta.key,
                    size: meta.size,
                    etag: meta.etag,
                    last_modified_ns: meta.last_modified_ns,
                });
                record
            }
        };
        child.update_times();
        self.stage_inode(child.clone())?;
        let mut parent_record = parent.clone();
        self.update_parent_ref(&mut parent_record, name.to_string(), inode)?;
        Ok(Some(child))
    }

    pub(crate) fn import_source_children_for_dir(
        &self,
        parent: &InodeRecord,
    ) -> std::result::Result<(), i32> {
        let Some(source) = &self.source else {
            return Ok(());
        };
        let discovered = self
            .block_on(source.list_direct_children(&parent.path))
            .map_err(|_| EIO)?;
        if discovered.is_empty() {
            return Ok(());
        }

        let mut parent_record = parent.clone();
        let parent_inode = parent_record.inode;
        let parent_uid = parent_record.uid;
        let parent_gid = parent_record.gid;
        let existing_children: HashSet<String> = parent_record
            .children()
            .map(|c| c.keys().cloned().collect())
            .unwrap_or_default();
        if !parent_record.is_dir() {
            return Ok(());
        }
        let mut additions: Vec<(String, u64)> = Vec::new();
        for (name, entry) in discovered {
            if existing_children.contains(&name)
                || additions.iter().any(|(existing, _)| existing == &name)
            {
                continue;
            }
            let inode = self.allocate_inode_id().map_err(|_| EIO)?;
            let path = Self::build_child_path(parent, &name);
            let mut child = match entry {
                DiscoveredEntry::Directory => InodeRecord::new_directory(
                    inode,
                    parent_inode,
                    name.clone(),
                    path,
                    parent_uid,
                    parent_gid,
                ),
                DiscoveredEntry::File(meta) => {
                    let mut record = InodeRecord::new_file(
                        inode,
                        parent_inode,
                        name.clone(),
                        path,
                        parent_uid,
                        parent_gid,
                    );
                    record.size = meta.size;
                    if let Some(ts) = meta.last_modified_ns
                        && let Ok(parsed) = OffsetDateTime::from_unix_timestamp_nanos(ts as i128)
                    {
                        record.atime = parsed;
                        record.mtime = parsed;
                        record.ctime = parsed;
                    }
                    record.storage = FileStorage::ExternalObject(crate::inode::ExternalObject {
                        key: meta.key,
                        size: meta.size,
                        etag: meta.etag,
                        last_modified_ns: meta.last_modified_ns,
                    });
                    record
                }
            };
            child.update_times();
            self.stage_inode(child)?;
            additions.push((name, inode));
        }
        if !additions.is_empty() {
            let Some(children) = parent_record.children_mut() else {
                return Ok(());
            };
            for (name, inode) in additions {
                children.insert(name, inode);
            }
            parent_record.update_times();
            self.stage_inode(parent_record)?;
        }
        Ok(())
    }

    pub(crate) fn copy_up_external_inode_if_needed(
        &self,
        mut record: InodeRecord,
    ) -> std::result::Result<InodeRecord, i32> {
        let ext = match &record.storage {
            FileStorage::ExternalObject(ext) => ext.clone(),
            _ => return Ok(record),
        };
        let Some(source) = &self.source else {
            return Err(EIO);
        };
        let bytes = self.block_on(source.read_all(&ext.key)).map_err(|_| EIO)?;
        record.update_times();
        self.stage_file(record.clone(), bytes, None)?;
        self.load_inode(record.inode)
    }

    pub(crate) fn is_descendant(
        &self,
        ancestor: u64,
        mut candidate: u64,
    ) -> std::result::Result<bool, i32> {
        if ancestor == candidate {
            return Ok(true);
        }
        let mut seen = HashSet::new();
        while seen.insert(candidate) {
            if candidate == ancestor {
                return Ok(true);
            }
            if candidate == ROOT_INODE {
                break;
            }
            let inode = self.load_inode(candidate)?;
            if inode.parent == candidate {
                break;
            }
            candidate = inode.parent;
        }
        Ok(false)
    }
}
