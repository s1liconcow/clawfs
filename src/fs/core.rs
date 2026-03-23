use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;

#[macro_use]
#[path = "core_shared.inc.rs"]
mod core_shared;

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

    pub(crate) fn inode_identity_conflicts(existing: &InodeRecord, new: &InodeRecord) -> bool {
        use InodeKind::{Directory, File, Symlink, Tombstone};

        !matches!(
            (&existing.kind, &new.kind),
            (Tombstone, _)
                | (_, Tombstone)
                | (Directory { .. }, Directory { .. })
                | (File, File)
                | (Symlink, Symlink)
        )
    }

    fn abort_inode_identity_conflict(
        existing: &InodeRecord,
        new: &InodeRecord,
        state_label: &str,
    ) -> ! {
        log::error!(
            "inode identity conflict detected while staging inode={} state={} existing_kind={} existing_parent={} existing_name={} existing_path={} new_kind={} new_parent={} new_name={} new_path={}",
            new.inode,
            state_label,
            Self::summarize_inode_kind(&existing.kind),
            existing.parent,
            existing.name,
            existing.path,
            Self::summarize_inode_kind(&new.kind),
            new.parent,
            new.name,
            new.path
        );
        std::process::abort();
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
            pending_inodes: Arc::new(DashSet::new()),
            pending_bytes: Arc::new(AtomicU64::new(0)),
            perf,
            replay,
            telemetry,
            telemetry_session_id,
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
            self.pending_inodes.insert(inode);
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
}

fs_core_shared_items!();
