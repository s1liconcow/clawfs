use super::*;

impl OsageFs {
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

    pub fn new(
        config: Config,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        segments: Arc<SegmentManager>,
        journal: Option<Arc<JournalManager>>,
        handle: Handle,
        client_state: Arc<ClientStateManager>,
        perf: Option<Arc<PerfLogger>>,
        replay: Option<Arc<ReplayLogger>>,
    ) -> Self {
        let fsync_on_close = config.fsync_on_close;
        let flush_interval = if config.flush_interval_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(config.flush_interval_ms))
        };
        let lookup_cache_ttl = Duration::from_millis(config.lookup_cache_ttl_ms);
        let dir_cache_ttl = Duration::from_millis(config.dir_cache_ttl_ms);
        Self {
            config,
            metadata,
            superblock,
            segments,
            handle,
            client_state,
            pending_inodes: Mutex::new(HashMap::new()),
            mutating_inodes: Mutex::new(HashMap::new()),
            flushing_inodes: Mutex::new(HashMap::new()),
            pending_bytes: Mutex::new(0),
            perf,
            replay,
            fsync_on_close,
            flush_interval,
            last_flush: Mutex::new(Instant::now()),
            flush_lock: Mutex::new(()),
            mutation_lock: Mutex::new(()),
            lookup_cache_ttl,
            dir_cache_ttl,
            journal,
        }
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
                JournalPayload::Inline(bytes) => Some(PendingData::Inline(bytes)),
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
                        let total = present.iter().map(|c| c.len).sum();
                        Some(PendingData::Staged(PendingSegments {
                            chunks: present,
                            total_len: total,
                        }))
                    }
                }
            };
            let data_len = data_opt.as_ref().map(|d| d.len()).unwrap_or(0);
            {
                let mut map = self.pending_inodes.lock();
                if let Some(old) = map.insert(
                    inode,
                    PendingEntry {
                        record,
                        data: data_opt,
                    },
                ) {
                    if let Some(old_data) = old.data {
                        self.release_pending_data(old_data);
                    }
                }
            }
            if data_len > 0 {
                let mut total = self.pending_bytes.lock();
                *total = total.saturating_add(data_len);
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

    pub(crate) fn build_child_path(parent: &InodeRecord, name: &str) -> String {
        if parent.inode == ROOT_INODE {
            format!("/{}", name)
        } else if parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path.trim_end_matches('/'), name)
        }
    }

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
            blocks: (record.size + 511) / 512,
            atime: to_system_time(record.atime),
            mtime: to_system_time(record.mtime),
            ctime: to_system_time(record.ctime),
            crtime: to_system_time(record.ctime),
            kind: attr_kind,
            perm: (record.mode & 0o7777) as u16,
            nlink: if record.is_dir() {
                2 + record.children().map(|c| c.len() as u32).unwrap_or(0)
            } else {
                record.link_count.max(1)
            },
            uid: record.uid,
            gid: record.gid,
            rdev: record.rdev,
            flags: 0,
            blksize: 4096,
        }
    }

    pub(crate) fn load_inode(&self, ino: u64) -> std::result::Result<InodeRecord, i32> {
        if let Some(entry) = self.pending_inodes.lock().get(&ino) {
            if matches!(entry.record.kind, InodeKind::Tombstone) {
                return Err(ENOENT);
            }
            return Ok(entry.record.clone());
        }
        if let Some(entry) = self.mutating_inodes.lock().get(&ino) {
            if matches!(entry.record.kind, InodeKind::Tombstone) {
                return Err(ENOENT);
            }
            return Ok(entry.record.clone());
        }
        if let Some(entry) = self.flushing_inodes.lock().get(&ino) {
            if matches!(entry.record.kind, InodeKind::Tombstone) {
                return Err(ENOENT);
            }
            return Ok(entry.record.clone());
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
                    "load_inode metadata error ino={} pending={} mutating={} flushing={} err={:#}",
                    ino,
                    self.pending_inodes.lock().contains_key(&ino),
                    self.mutating_inodes.lock().contains_key(&ino),
                    self.flushing_inodes.lock().contains_key(&ino),
                    err
                );
                return Err(EIO);
            }
        };
        fetched.ok_or_else(|| {
            debug!(
                "load_inode miss ino={} pending={} mutating={} flushing={}",
                ino,
                self.pending_inodes.lock().contains_key(&ino),
                self.mutating_inodes.lock().contains_key(&ino),
                self.flushing_inodes.lock().contains_key(&ino)
            );
            ENOENT
        })
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
        if let Some(entry) = self.pending_inodes.lock().get(&record.inode) {
            if let Some(data) = &entry.data {
                return self.slice_pending_bytes(data, range_start, range_end);
            }
        }
        if let Some(entry) = self.mutating_inodes.lock().get(&record.inode) {
            if let Some(data) = &entry.data {
                return self.slice_pending_bytes(data, range_start, range_end);
            }
        }
        if let Some(entry) = self.flushing_inodes.lock().get(&record.inode) {
            if let Some(data) = &entry.data {
                return self.slice_pending_bytes(data, range_start, range_end);
            }
        }
        match &record.storage {
            FileStorage::Inline(_) | FileStorage::InlineEncoded(_) => {
                let bytes = self.decode_inline_storage(&record.storage)?;
                Ok(Self::slice_bytes_in_range(bytes, range_start, range_end))
            }
            FileStorage::LegacySegment(ptr) => {
                let bytes = self.segments.read_pointer(ptr)?;
                Ok(Self::slice_bytes_in_range(bytes, range_start, range_end))
            }
            FileStorage::Segments(extents) => {
                let out_len = (range_end - range_start) as usize;
                let mut buffer = vec![0u8; out_len];
                let mut ordered = extents.to_vec();
                ordered.sort_by_key(|ext| ext.logical_offset);
                for extent in ordered {
                    let extent_start = extent.logical_offset;
                    let bytes = self.segments.read_pointer(&extent.pointer)?;
                    let extent_end = extent_start.saturating_add(bytes.len() as u64);
                    if extent_end <= range_start {
                        continue;
                    }
                    if extent_start >= range_end {
                        break;
                    }
                    let overlap_start = extent_start.max(range_start);
                    let overlap_end = extent_end.min(range_end);
                    if overlap_end <= overlap_start {
                        continue;
                    }
                    let src_start = (overlap_start - extent_start) as usize;
                    let src_end = (overlap_end - extent_start) as usize;
                    let dst_start = (overlap_start - range_start) as usize;
                    let dst_end = (overlap_end - range_start) as usize;
                    buffer[dst_start..dst_end].copy_from_slice(&bytes[src_start..src_end]);
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
        let mut map = self.pending_inodes.lock();
        match map.entry(inode) {
            Entry::Occupied(mut occupied) => {
                if matches!(record.kind, InodeKind::Tombstone) {
                    if let Some(data) = occupied.get_mut().data.take() {
                        let len = data.len();
                        self.release_pending_data(data);
                        if len > 0 {
                            let mut total = self.pending_bytes.lock();
                            *total = total.saturating_sub(len);
                        }
                    }
                }
                occupied.get_mut().record = record.clone();
            }
            Entry::Vacant(vacant) => {
                vacant.insert(PendingEntry {
                    record: record.clone(),
                    data: None,
                });
            }
        }
        drop(map);
        let kind_summary = Self::summarize_inode_kind(&record.kind);
        if let Some(journal) = &self.journal {
            let entry = JournalEntry {
                record,
                payload: JournalPayload::None,
            };
            journal.persist_entry(&entry).map_err(|_| EIO)?;
        }
        debug!(
            "stage_inode inode={} kind={} metadata-staged",
            inode, kind_summary
        );
        self.flush_if_interval_elapsed()?;
        Ok(())
    }

    pub(crate) fn drop_pending_entry(&self, inode: u64) {
        let entry = {
            let mut map = self.pending_inodes.lock();
            map.remove(&inode)
        };
        if let Some(entry) = entry {
            if let Some(data) = entry.data {
                let len = data.len();
                self.release_pending_data(data);
                if len > 0 {
                    let mut total = self.pending_bytes.lock();
                    *total = total.saturating_sub(len);
                }
            }
        }
    }

    pub(crate) fn snapshot_journal_payload(&self, data: &PendingData) -> JournalPayload {
        match data {
            PendingData::Inline(bytes) => JournalPayload::Inline(bytes.clone()),
            PendingData::Staged(segments) => JournalPayload::StageChunks(segments.chunks.clone()),
        }
    }

    pub(crate) fn read_pending_bytes(&self, data: &PendingData) -> Result<Vec<u8>> {
        match data {
            PendingData::Inline(bytes) => Ok(bytes.clone()),
            PendingData::Staged(segments) => self
                .segments
                .read_staged_chunks(&segments.chunks, segments.total_len)
                .map_err(|err| anyhow!("pending read failed: {err:?}")),
        }
    }

    fn slice_pending_bytes(
        &self,
        data: &PendingData,
        range_start: u64,
        range_end: u64,
    ) -> Result<Vec<u8>> {
        let bytes = self.read_pending_bytes(data)?;
        Ok(Self::slice_bytes_in_range(bytes, range_start, range_end))
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
        if let PendingData::Staged(segments) = data {
            for chunk in segments.chunks {
                if let Err(err) = self.segments.release_staged_chunk(&chunk) {
                    log::warn!(
                        "failed to release staged payload {}: {err:?}",
                        chunk.path.display()
                    );
                }
            }
        }
    }

    pub(crate) fn update_parent(
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

    pub(crate) fn remove_from_parent(
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
        self.remove_from_parent(parent, name)?;
        if record.link_count > 1 {
            record.dec_links();
            record.update_times();
            self.stage_inode(record.clone())?
        } else {
            self.drop_pending_entry(record.inode);
            let tombstone = InodeRecord::tombstone(record.inode);
            self.stage_inode(tombstone)?
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
                        self.remove_from_parent(&mut dir, &new_name)?;
                        let tombstone = InodeRecord::tombstone(victim.inode);
                        self.stage_inode(tombstone)?;
                    } else {
                        self.unlink_file_entry(&mut dir, &new_name, &mut victim)?;
                    }
                }
            }
            self.remove_from_parent(&mut dir, &old_name)?;
            self.update_parent(&mut dir, new_name.clone(), target.inode)?;
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
                    self.remove_from_parent(&mut dst_parent, &new_name)?;
                    let tombstone = InodeRecord::tombstone(victim.inode);
                    self.stage_inode(tombstone)?;
                } else {
                    self.unlink_file_entry(&mut dst_parent, &new_name, &mut victim)?;
                }
            }
        }
        self.remove_from_parent(&mut src_parent, &old_name)?;
        self.update_parent(&mut dst_parent, new_name.clone(), target.inode)?;
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

    pub(crate) fn refresh_descendant_paths(&self, inode: &InodeRecord) -> std::result::Result<(), i32> {
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

    pub(crate) fn is_descendant(&self, ancestor: u64, mut candidate: u64) -> std::result::Result<bool, i32> {
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
