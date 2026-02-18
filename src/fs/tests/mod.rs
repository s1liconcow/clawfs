    use super::*;
    use std::env;
    use std::path::Path;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::tempdir;

    use crate::config::ObjectStoreProvider;
    use crate::journal::JournalManager;

    struct TestHarness {
        runtime: tokio::runtime::Runtime,
        metadata: Arc<MetadataStore>,
        fs: OsageFs,
        config: Config,
    }

    impl TestHarness {
        fn new(root: &Path, state_name: &str, pending_bytes: u64) -> Self {
            Self::with_config(root, state_name, pending_bytes, |_| {})
        }

        fn with_config<F>(root: &Path, state_name: &str, pending_bytes: u64, tweak: F) -> Self
        where
            F: FnOnce(&mut Config),
        {
            let mut config = test_config(root, state_name, pending_bytes);
            tweak(&mut config);
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let metadata = Arc::new(
                runtime
                    .block_on(MetadataStore::new(
                        &config,
                        runtime.handle().clone(),
                    ))
                    .unwrap(),
            );
            let superblock = Arc::new(
                runtime
                    .block_on(SuperblockManager::load_or_init(
                        metadata.clone(),
                        config.shard_size,
                    ))
                    .unwrap(),
            );
            ensure_root_for_tests(&runtime, metadata.clone(), superblock.clone(), &config);
            let segments =
                Arc::new(SegmentManager::new(&config, runtime.handle().clone()).unwrap());
            let client_state = Arc::new(ClientStateManager::load(&config.state_path).unwrap());
            let journal = if config.disable_journal {
                None
            } else {
                Some(Arc::new(
                    JournalManager::new(&config.local_cache_path).unwrap(),
                ))
            };
            let fs = OsageFs::new(
                config.clone(),
                metadata.clone(),
                superblock,
                segments,
                journal,
                runtime.handle().clone(),
                client_state,
                None,
                None,
            );
            Self {
                runtime,
                metadata,
                fs,
                config,
            }
        }
    }

    fn test_config(root: &Path, state_name: &str, pending_bytes: u64) -> Config {
        Config {
            mount_path: root.join("mnt"),
            store_path: root.join("store"),
            local_cache_path: root.join("cache"),
            inline_threshold: 512,
            inline_compression: true,
            inline_encryption_key: None,
            segment_compression: true,
            segment_encryption_key: None,
            shard_size: 64,
            inode_batch: 8,
            segment_batch: 8,
            pending_bytes,
            object_provider: ObjectStoreProvider::Local,
            bucket: None,
            region: None,
            endpoint: None,
            object_prefix: String::new(),
            gcs_service_account: None,
            state_path: root.join(state_name),
            foreground: false,
            allow_other: false,
            home_prefix: "/home".into(),
            perf_log: None,
            replay_log: None,
            disable_journal: false,
            fsync_on_close: false,
            flush_interval_ms: 0,
            disable_cleanup: false,
            lookup_cache_ttl_ms: 0,
            dir_cache_ttl_ms: 0,
            metadata_poll_interval_ms: 0,
            segment_cache_bytes: 0,
            log_file: None,
            debug_log: false,
            imap_delta_batch: 32,
            log_storage_io: false,
        }
    }

    #[test]
    fn summarize_inode_kind_truncates_directory_children() {
        let mut children = std::collections::BTreeMap::new();
        for i in 0..20u64 {
            children.insert(format!("f_{i}"), i + 10);
        }
        let summary = OsageFs::summarize_inode_kind(&InodeKind::Directory { children });
        assert!(summary.contains("Directory(children=20"));
        assert!(summary.contains("truncated="));
        assert!(summary.len() < 300);
    }

    fn ensure_root_for_tests(
        runtime: &tokio::runtime::Runtime,
        metadata: Arc<MetadataStore>,
        superblock: Arc<SuperblockManager>,
        config: &Config,
    ) {
        let uid = unsafe { libc::geteuid() as u32 };
        let gid = unsafe { libc::getegid() as u32 };
        let desired_mode = 0o40777;
        if let Some(mut root) = runtime.block_on(metadata.get_inode(ROOT_INODE)).unwrap() {
            if root.uid != uid || root.gid != gid || root.mode != desired_mode {
                root.uid = uid;
                root.gid = gid;
                root.mode = desired_mode;
                let snapshot = superblock.prepare_dirty_generation().unwrap();
                let generation = snapshot.generation;
                runtime
                    .block_on(metadata.persist_inode(&root, generation, config.shard_size))
                    .unwrap();
                runtime
                    .block_on(superblock.commit_generation(generation))
                    .unwrap();
            }
            return;
        }
        let snapshot = superblock.prepare_dirty_generation().unwrap();
        let generation = snapshot.generation;
        let mut root = InodeRecord::new_directory(
            ROOT_INODE,
            ROOT_INODE,
            String::from(""),
            String::from("/"),
            uid,
            gid,
        );
        root.mode = desired_mode;
        runtime
            .block_on(metadata.persist_inode(&root, generation, config.shard_size))
            .unwrap();
        runtime
            .block_on(superblock.commit_generation(generation))
            .unwrap();
    }

    fn apply_write(fs: &OsageFs, inode: u64, offset: usize, payload: &[u8]) {
        let mut record = fs.load_inode(inode).unwrap();
        let mut bytes = fs.read_file_bytes(&record).unwrap();
        if offset + payload.len() > bytes.len() {
            bytes.resize(offset + payload.len(), 0);
        }
        bytes[offset..offset + payload.len()].copy_from_slice(payload);
        record.update_times();
        fs.stage_file(record, bytes, None).unwrap();
    }

    fn stage_named_file(harness: &TestHarness, name: &str, data: Vec<u8>) -> u64 {
        let inode = harness.fs.allocate_inode_id().unwrap();
        let record = make_file(inode, name);
        harness.fs.stage_file(record, data, None).unwrap();
        let mut root = harness.fs.load_inode(ROOT_INODE).unwrap();
        harness
            .fs
            .update_parent_ref(&mut root, name.to_string(), inode)
            .unwrap();
        inode
    }

    fn load_named_inode(fs: &OsageFs, name: &str) -> InodeRecord {
        let root = fs.load_inode(ROOT_INODE).unwrap();
        let children = root.children().unwrap();
        let inode = *children.get(name).unwrap();
        fs.load_inode(inode).unwrap()
    }

    fn make_file(inode: u64, name: &str) -> InodeRecord {
        InodeRecord::new_file(
            inode,
            ROOT_INODE,
            name.to_string(),
            format!("/{}", name),
            0,
            0,
        )
    }

    fn make_symlink(inode: u64, name: &str, target: &str) -> InodeRecord {
        InodeRecord::new_symlink(
            inode,
            ROOT_INODE,
            name.to_string(),
            format!("/{}", name),
            0,
            0,
            target.as_bytes().to_vec(),
        )
    }

    #[test]
    fn single_client_inline_and_segment_flush() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "client_a.bin", 8 * 1024 * 1024);

        let inode_inline = harness.fs.allocate_inode_id().unwrap();
        let record_inline = make_file(inode_inline, "foo.txt");
        harness
            .fs
            .stage_file(record_inline, b"hello".to_vec(), None)
            .unwrap();

        let inode_seg = harness.fs.allocate_inode_id().unwrap();
        let record_seg = make_file(inode_seg, "bar.bin");
        let data = vec![7u8; harness.config.inline_threshold + 128];
        harness
            .fs
            .stage_file(record_seg, data.clone(), None)
            .unwrap();
        harness.fs.flush_pending().unwrap();

        let stored_inline = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_inline))
            .unwrap()
            .unwrap();
        assert_eq!(
            harness.fs.read_file_bytes(&stored_inline).unwrap(),
            b"hello"
        );

        let stored_seg = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_seg))
            .unwrap()
            .unwrap();
        match stored_seg.storage {
            FileStorage::Segments(_) => {
                let roundtrip = harness.fs.read_file_bytes(&stored_seg).unwrap();
                assert_eq!(roundtrip, data);
            }
            _ => panic!("expected segment storage"),
        }
    }

    #[test]
    fn multiple_clients_flush_independently() {
        let dir = tempdir().unwrap();

        let inode_a = {
            let harness = TestHarness::new(dir.path(), "client_one.bin", 8 * 1024 * 1024);
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, "client1.txt");
            harness
                .fs
                .stage_file(record, b"alpha".to_vec(), None)
                .unwrap();
            harness.fs.flush_pending().unwrap();
            inode
        };

        let inode_b = {
            let harness = TestHarness::new(dir.path(), "client_two.bin", 8 * 1024 * 1024);
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, "client2.txt");
            harness
                .fs
                .stage_file(record, b"beta".to_vec(), None)
                .unwrap();
            harness.fs.flush_pending().unwrap();
            inode
        };

        let harness = TestHarness::new(dir.path(), "client_reader.bin", 8 * 1024 * 1024);
        let a = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_a))
            .unwrap()
            .unwrap();
        let b = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_b))
            .unwrap()
            .unwrap();
        assert!(matches!(
            a.storage,
            FileStorage::Inline(_) | FileStorage::InlineEncoded(_)
        ));
        assert!(matches!(
            b.storage,
            FileStorage::Inline(_) | FileStorage::InlineEncoded(_)
        ));

        let state_a = std::fs::read(dir.path().join("client_one.bin")).unwrap();
        let state_b = std::fs::read(dir.path().join("client_two.bin")).unwrap();
        assert_ne!(state_a, state_b);
    }

    #[test]
    fn staged_extents_handle_random_writes() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "random.bin", 64 * 1024 * 1024);
        let inode = stage_named_file(&harness, "random.dat", Vec::new());
        let patterns: Vec<(usize, Vec<u8>)> = vec![
            (0, vec![1u8; 512 * 1024]),
            (256 * 1024, vec![2u8; 256 * 1024]),
            (768 * 1024, vec![3u8; 192 * 1024]),
            (512 * 1024, vec![4u8; 128 * 1024]),
            (1_200_000, vec![5u8; 400 * 1024]),
        ];
        let total_len = patterns
            .iter()
            .map(|(offset, bytes)| offset + bytes.len())
            .max()
            .unwrap();
        let mut expected = vec![0u8; total_len];
        for (offset, payload) in &patterns {
            expected[*offset..*offset + payload.len()].copy_from_slice(payload);
            let record = harness.fs.load_inode(inode).unwrap();
            harness
                .fs
                .write_large_segments(record, *offset as u64, payload)
                .unwrap();
        }
        harness.fs.flush_pending().unwrap();
        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode))
            .unwrap()
            .unwrap();
        let bytes = harness.fs.read_file_bytes(&stored).unwrap();
        assert_eq!(&bytes[..total_len], &expected[..]);
    }

    #[test]
    fn read_file_range_for_segment_storage_returns_expected_window() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "range_segments.bin", 64 * 1024 * 1024);
        let inode = stage_named_file(&harness, "range.dat", Vec::new());

        let first = vec![0x11; 4096];
        let second = vec![0x22; 4096];
        let record = harness.fs.load_inode(inode).unwrap();
        harness.fs.write_large_segments(record, 0, &first).unwrap();
        let record = harness.fs.load_inode(inode).unwrap();
        harness
            .fs
            .write_large_segments(record, 8192, &second)
            .unwrap();
        harness.fs.flush_pending().unwrap();

        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode))
            .unwrap()
            .unwrap();
        let actual = harness.fs.read_file_range(&stored, 2048, 8192).unwrap();
        let mut expected = Vec::with_capacity(8192);
        expected.extend_from_slice(&first[2048..]);
        expected.extend_from_slice(&vec![0u8; 4096]);
        expected.extend_from_slice(&second[..2048]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn op_read_returns_only_requested_window_for_large_segment_file() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "op_read_range.bin", 64 * 1024 * 1024);
        let inode = stage_named_file(&harness, "checkpoint.bin", Vec::new());
        let payload = vec![0x5A; 64 * 1024];
        let record = harness.fs.load_inode(inode).unwrap();
        harness
            .fs
            .write_large_segments(record, 0, &payload)
            .unwrap();
        harness.fs.flush_pending().unwrap();

        let read = harness.fs.op_read(inode, 32 * 1024, 4096).unwrap();
        assert_eq!(read.len(), 4096);
        assert_eq!(read, vec![0x5A; 4096]);
    }

    #[test]
    fn write_path_meets_perf_target() {
        let dir = tempdir().unwrap();
        let harness =
            TestHarness::with_config(dir.path(), "throughput.bin", 256 * 1024 * 1024, |cfg| {
                cfg.disable_journal = true;
                cfg.flush_interval_ms = 0;
            });
        let inode = stage_named_file(&harness, "throughput.dat", Vec::new());
        let chunk_size = 256 * 1024;
        let iterations = 80;
        let total_bytes = chunk_size * iterations;
        let payload = vec![0xAB; chunk_size];
        let start = Instant::now();
        for i in 0..iterations {
            let offset = (i * chunk_size) as u64;
            let record = harness.fs.load_inode(inode).unwrap();
            harness
                .fs
                .write_large_segments(record, offset, &payload)
                .unwrap();
        }
        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        assert!(secs > 0.0, "elapsed time too small for measurement");
        let throughput = (total_bytes as f64 / (1024.0 * 1024.0)) / secs;
        assert!(
            throughput >= 10.0,
            "throughput {:.2} MiB/s below 10 MiB/s in {:?}",
            throughput,
            elapsed
        );
    }

    #[test]
    fn adaptive_append_meets_perf_target_with_journal_and_interval() {
        let dir = tempdir().unwrap();
        let harness =
            TestHarness::with_config(dir.path(), "adaptive_perf.bin", 1024 * 1024, |cfg| {
                cfg.disable_journal = false;
                cfg.flush_interval_ms = 5000;
            });
        let inode = stage_named_file(&harness, "adaptive-throughput.dat", Vec::new());
        let chunk_size = 512 * 1024;
        let iterations = 128;
        let total_bytes = chunk_size * iterations;
        let payload = vec![0xCD; chunk_size];
        let start = Instant::now();
        for i in 0..iterations {
            let offset = (i * chunk_size) as u64;
            let record = harness.fs.load_inode(inode).unwrap();
            harness
                .fs
                .write_large_segments(record, offset, &payload)
                .unwrap();
        }
        harness.fs.flush_pending().unwrap();
        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        assert!(secs > 0.0, "elapsed time too small for measurement");
        let throughput = (total_bytes as f64 / (1024.0 * 1024.0)) / secs;
        eprintln!(
            "adaptive_append_meets_perf_target_with_journal_and_interval throughput={:.2} MiB/s elapsed={:?}",
            throughput, elapsed
        );
        assert!(
            throughput >= 5.0,
            "adaptive throughput {:.2} MiB/s below 5 MiB/s in {:?}",
            throughput,
            elapsed
        );
    }

    #[test]
    fn untar_style_perf_mixture() {
        if env::var("OSAGEFS_RUN_PERF").is_err() {
            eprintln!("skipping untar_style_perf_mixture; set OSAGEFS_RUN_PERF=1 to enable");
            return;
        }
        let dir = tempdir().unwrap();
        let harness = TestHarness::with_config(dir.path(), "untar.bin", 256 * 1024 * 1024, |cfg| {
            cfg.disable_journal = true;
            cfg.flush_interval_ms = 0;
        });
        let mut sizes = Vec::new();
        sizes.extend(std::iter::repeat(4 * 1024).take(2000));
        sizes.extend(std::iter::repeat(64 * 1024).take(512));
        sizes.extend(std::iter::repeat(512 * 1024).take(64));
        let mut total_bytes = 0usize;
        let start = Instant::now();
        for (idx, size) in sizes.into_iter().enumerate() {
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, &format!("untar_{idx}"));
            let payload = vec![(idx & 0xff) as u8; size];
            harness.fs.stage_file(record, payload, None).unwrap();
            total_bytes += size;
        }
        harness.fs.flush_pending().unwrap();
        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        assert!(secs > 0.0, "elapsed time too small for throughput");
        let throughput = (total_bytes as f64 / (1024.0 * 1024.0)) / secs;
        assert!(
            throughput >= 10.0,
            "untar-style throughput {:.2} MiB/s below 10 MiB/s in {:?}",
            throughput,
            elapsed
        );
    }

    #[test]
    fn stress_flush_respects_pending_threshold() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "stress.bin", 1024);
        let mut inodes = Vec::new();
        for i in 0..10 {
            let inode = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode, &format!("stress{i}"));
            harness
                .fs
                .stage_file(record, vec![i as u8; 600], None)
                .unwrap();
            inodes.push(inode);
        }
        harness.fs.flush_pending().unwrap();
        assert!(harness.fs.pending_inodes.lock().is_empty());
        assert_eq!(*harness.fs.pending_bytes.lock(), 0);
        for inode in inodes {
            let record = harness
                .runtime
                .block_on(harness.metadata.get_inode(inode))
                .unwrap()
                .unwrap();
            assert_eq!(record.size, 600);
        }
    }

    #[test]
    fn append_file_handles_staged_pending_without_panic() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "append_staged.bin", 1 << 20);
        let inode = harness.fs.allocate_inode_id().unwrap();
        let record = make_file(inode, "staged.dat");
        let initial = vec![0xAB; 5000];
        harness.fs.stage_file(record.clone(), initial.clone(), None).unwrap();

        let mut stale_view = harness.fs.load_inode(inode).unwrap();
        stale_view.size = 0;
        harness.fs.append_file(stale_view, b"xyz").unwrap();

        let pending_len = {
            let map = harness.fs.pending_inodes.lock();
            map.get(&inode)
                .and_then(|entry| entry.data.as_ref())
                .map(|data| data.len())
                .unwrap_or(0)
        };
        assert_eq!(pending_len, initial.len() as u64 + 3);
    }

    #[test]
    fn journal_replay_flushes_staged_entries() {
        let dir = tempdir().unwrap();
        let inode_id;
        {
            let harness = TestHarness::new(dir.path(), "journal.bin", 8 * 1024 * 1024);
            inode_id = harness.fs.allocate_inode_id().unwrap();
            let record = make_file(inode_id, "pending.txt");
            harness
                .fs
                .stage_file(record, b"hello".to_vec(), None)
                .unwrap();
            // drop harness without flushing to simulate crash
        }

        let harness = TestHarness::new(dir.path(), "journal.bin", 8 * 1024 * 1024);
        let replayed = harness.fs.replay_journal().unwrap();
        assert_eq!(replayed, 1);
        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_id))
            .unwrap()
            .unwrap();
        assert_eq!(stored.size, 5);
        assert!(matches!(
            stored.storage,
            FileStorage::Inline(_) | FileStorage::InlineEncoded(_)
        ));
    }

    #[test]
    fn adaptive_large_append_keeps_data_pending_with_journal() {
        let dir = tempdir().unwrap();
        let harness =
            TestHarness::with_config(dir.path(), "adaptive_pending.bin", 1024 * 1024, |cfg| {
                cfg.disable_journal = false;
                cfg.flush_interval_ms = 1000;
            });
        let inode = stage_named_file(&harness, "stream.bin", Vec::new());
        let chunk = vec![0xAB; 512 * 1024];
        for i in 0..6 {
            let record = harness.fs.load_inode(inode).unwrap();
            harness
                .fs
                .write_large_segments(record, (i * chunk.len()) as u64, &chunk)
                .unwrap();
        }
        let pending_total = *harness.fs.pending_bytes.lock();
        assert!(
            pending_total > harness.fs.config.pending_bytes,
            "expected adaptive pending to exceed base threshold, got {} vs {}",
            pending_total,
            harness.fs.config.pending_bytes
        );
        let record = harness.fs.load_inode(inode).unwrap();
        let staged = harness.fs.read_file_bytes(&record).unwrap();
        assert_eq!(staged.len(), chunk.len() * 6);
        harness.fs.flush_pending().unwrap();
    }

    #[test]
    fn adaptive_large_append_keeps_data_pending_without_journal() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::with_config(
            dir.path(),
            "adaptive_pending_no_journal.bin",
            1024 * 1024,
            |cfg| {
                cfg.disable_journal = true;
                cfg.flush_interval_ms = 0;
            },
        );
        let inode = stage_named_file(&harness, "stream.bin", Vec::new());
        let chunk = vec![0xAB; 512 * 1024];
        for i in 0..6 {
            let record = harness.fs.load_inode(inode).unwrap();
            harness
                .fs
                .write_large_segments(record, (i * chunk.len()) as u64, &chunk)
                .unwrap();
        }
        let pending_total = *harness.fs.pending_bytes.lock();
        assert!(
            pending_total > harness.fs.config.pending_bytes,
            "expected adaptive pending to exceed base threshold without journal, got {} vs {}",
            pending_total,
            harness.fs.config.pending_bytes
        );
        let record = harness.fs.load_inode(inode).unwrap();
        let staged = harness.fs.read_file_bytes(&record).unwrap();
        assert_eq!(staged.len(), chunk.len() * 6);
        harness.fs.flush_pending().unwrap();
    }

    #[test]
    fn adaptive_large_append_replays_after_crash() {
        let dir = tempdir().unwrap();
        let inode_id;
        let chunk = vec![0x5E; 512 * 1024];
        {
            let harness =
                TestHarness::with_config(dir.path(), "adaptive_replay.bin", 1024 * 1024, |cfg| {
                    cfg.disable_journal = false;
                    cfg.flush_interval_ms = 1000;
                });
            inode_id = stage_named_file(&harness, "stream.bin", Vec::new());
            for i in 0..6 {
                let record = harness.fs.load_inode(inode_id).unwrap();
                harness
                    .fs
                    .write_large_segments(record, (i * chunk.len()) as u64, &chunk)
                    .unwrap();
            }
            let pending_total = *harness.fs.pending_bytes.lock();
            assert!(
                pending_total > harness.fs.config.pending_bytes,
                "expected replay setup to leave pending data"
            );
            // drop without explicit flush to simulate crash
        }
        let harness =
            TestHarness::with_config(dir.path(), "adaptive_replay.bin", 1024 * 1024, |cfg| {
                cfg.disable_journal = false;
                cfg.flush_interval_ms = 1000;
            });
        let replayed = harness.fs.replay_journal().unwrap();
        assert!(replayed >= 1);
        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_id))
            .unwrap()
            .unwrap();
        let bytes = harness.fs.read_file_bytes(&stored).unwrap();
        assert_eq!(bytes.len(), chunk.len() * 6);
        assert_eq!(&bytes[..chunk.len()], &chunk[..]);
    }

    #[test]
    fn symlink_roundtrip_persists_target() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "symlink.bin", 8 * 1024 * 1024);
        let inode = harness.fs.allocate_inode_id().unwrap();
        let mut root = harness.fs.load_inode(ROOT_INODE).unwrap();
        let record = make_symlink(inode, "link", "/tmp/actual");
        harness.fs.stage_inode(record.clone()).unwrap();
        harness
            .fs
            .update_parent_ref(&mut root, "link".to_string(), inode)
            .unwrap();
        harness.fs.flush_pending().unwrap();

        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode))
            .unwrap()
            .unwrap();
        assert!(stored.is_symlink());
        assert_eq!(harness.fs.read_file_bytes(&stored).unwrap(), b"/tmp/actual");
    }

    #[test]
    fn hardlinks_update_reference_counts() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "hardlinks.bin", 8 * 1024 * 1024);
        let inode = harness.fs.allocate_inode_id().unwrap();
        let primary_name = "file_a";
        let secondary_name = "file_b";
        let file = make_file(inode, primary_name);
        harness
            .fs
            .stage_file(file.clone(), b"payload".to_vec(), None)
            .unwrap();
        let mut root = harness.fs.load_inode(ROOT_INODE).unwrap();
        harness
            .fs
            .update_parent_ref(&mut root, primary_name.to_string(), inode)
            .unwrap();
        harness.fs.flush_pending().unwrap();

        // create second hardlink
        let mut root = harness.fs.load_inode(ROOT_INODE).unwrap();
        let mut stored_file = harness.fs.load_inode(inode).unwrap();
        stored_file.inc_links();
        harness.fs.stage_inode(stored_file.clone()).unwrap();
        harness
            .fs
            .update_parent_ref(&mut root, secondary_name.to_string(), inode)
            .unwrap();
        harness.fs.flush_pending().unwrap();

        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode))
            .unwrap()
            .unwrap();
        assert_eq!(stored.link_count, 2);
        let root = harness.fs.load_inode(ROOT_INODE).unwrap();
        let children = root.children().unwrap();
        assert!(children.contains_key(primary_name));
        assert!(children.contains_key(secondary_name));

        // remove the second link and ensure reference count drops back to 1
        let mut root = harness.fs.load_inode(ROOT_INODE).unwrap();
        let mut current = harness.fs.load_inode(inode).unwrap();
        harness
            .fs
            .unlink_file_entry(&mut root, secondary_name, &mut current)
            .unwrap();
        harness.fs.flush_pending().unwrap();
        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode))
            .unwrap()
            .unwrap();
        assert_eq!(stored.link_count, 1);
        let root = harness.fs.load_inode(ROOT_INODE).unwrap();
        let children = root.children().unwrap();
        assert!(children.contains_key(primary_name));
        assert!(!children.contains_key(secondary_name));
    }

    #[test]
    fn stress_varied_workloads() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "stresswork.bin", 64 * 1024 * 1024);

        // Single file create + verify
        let inode_single = harness.fs.allocate_inode_id().unwrap();
        let record_single = make_file(inode_single, "single.txt");
        harness
            .fs
            .stage_file(record_single.clone(), b"alpha".to_vec(), None)
            .unwrap();
        harness.fs.flush_pending().unwrap();
        let stored_single = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_single))
            .unwrap()
            .unwrap();
        assert_eq!(stored_single.size, 5);
        assert_eq!(
            harness.fs.read_file_bytes(&stored_single).unwrap(),
            b"alpha"
        );

        // Burst of 1000 files of varying sizes
        let mut samples = Vec::new();
        for i in 0..1000 {
            let inode = harness.fs.allocate_inode_id().unwrap();
            let mut record = make_file(inode, &format!("bulk_{i}"));
            record.mode = 0o100600;
            let len = 32 + (i % 128) as usize;
            let data = vec![(i & 0xff) as u8; len];
            harness.fs.stage_file(record, data.clone(), None).unwrap();
            if i % 200 == 0 {
                samples.push((inode, data));
            }
        }
        harness.fs.flush_pending().unwrap();
        for (inode, data) in samples {
            let stored = harness
                .runtime
                .block_on(harness.metadata.get_inode(inode))
                .unwrap()
                .unwrap();
            assert_eq!(stored.size as usize, data.len());
            assert_eq!(harness.fs.read_file_bytes(&stored).unwrap(), data);
        }

        // Offset writes: start, middle, end
        let inode_offsets = harness.fs.allocate_inode_id().unwrap();
        let base = make_file(inode_offsets, "offsets.bin");
        harness
            .fs
            .stage_file(base.clone(), vec![0u8; 4096], None)
            .unwrap();
        harness.fs.flush_pending().unwrap();
        apply_write(&harness.fs, inode_offsets, 0, b"START");
        apply_write(&harness.fs, inode_offsets, 2048, b"MIDDLE");
        apply_write(&harness.fs, inode_offsets, 4096, b"TAIL");
        harness.fs.flush_pending().unwrap();
        let stored_offset = harness
            .runtime
            .block_on(harness.metadata.get_inode(inode_offsets))
            .unwrap()
            .unwrap();
        let bytes = harness.fs.read_file_bytes(&stored_offset).unwrap();
        assert!(bytes.starts_with(b"START"));
        assert_eq!(&bytes[2048..2054], b"MIDDLE");
        assert_eq!(&bytes[bytes.len() - 4..], b"TAIL");

        // Attribute mutation
        let attr_inode = harness.fs.allocate_inode_id().unwrap();
        let attr_file = make_file(attr_inode, "attrs");
        harness
            .fs
            .stage_file(attr_file.clone(), b"data".to_vec(), None)
            .unwrap();
        harness.fs.flush_pending().unwrap();
        let mut record = harness.fs.load_inode(attr_inode).unwrap();
        record.mode = 0o100700;
        record.uid = 1234;
        record.gid = 4321;
        harness.fs.stage_inode(record).unwrap();
        harness.fs.flush_pending().unwrap();
        let stored_attr = harness
            .runtime
            .block_on(harness.metadata.get_inode(attr_inode))
            .unwrap()
            .unwrap();
        assert_eq!(stored_attr.mode & 0o777, 0o700);
        assert_eq!(stored_attr.uid, 1234);
        assert_eq!(stored_attr.gid, 4321);
    }

    #[test]
    fn script_style_workload_without_fuse() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "scriptstyle.bin", 64 * 1024 * 1024);

        // Step 1: single file
        stage_named_file(&harness, "single.txt", b"alpha".to_vec());

        // Step 2: 1000 bulk files with small payloads
        for i in 1..=1000 {
            let name = format!("bulk_{i}.txt");
            let payload = format!("file-{i:04}").into_bytes();
            stage_named_file(&harness, &name, payload);
        }
        let root = harness.fs.load_inode(ROOT_INODE).unwrap();
        assert_eq!(root.children().unwrap().len(), 1001);

        // Step 3: varied payload sizes
        let small_bytes = vec![0xAB; 512];
        stage_named_file(&harness, "small.bin", small_bytes.clone());
        let medium_bytes = vec![0xBC; 65_536];
        stage_named_file(&harness, "medium.bin", medium_bytes.clone());
        let large_bytes = vec![0xCD; 2 * 1024 * 1024];
        stage_named_file(&harness, "large.bin", large_bytes.clone());

        // Step 4: offset writes
        let offsets_inode = stage_named_file(&harness, "offsets.bin", vec![0u8; 4096]);
        apply_write(&harness.fs, offsets_inode, 0, b"START");
        apply_write(&harness.fs, offsets_inode, 2048, b"MIDDLE");
        apply_write(&harness.fs, offsets_inode, 4096, b"TAIL");

        // Step 5: attribute changes
        let attrs_inode = stage_named_file(&harness, "attrs.txt", b"data".to_vec());
        let mut attrs = harness.fs.load_inode(attrs_inode).unwrap();
        attrs.mode = 0o100700;
        attrs.uid = 777;
        attrs.gid = 888;
        harness.fs.stage_inode(attrs).unwrap();

        harness.fs.flush_pending().unwrap();

        // Validate small/medium/large contents
        let small = load_named_inode(&harness.fs, "small.bin");
        assert_eq!(small.size as usize, small_bytes.len());
        assert_eq!(harness.fs.read_file_bytes(&small).unwrap(), small_bytes);

        let medium = load_named_inode(&harness.fs, "medium.bin");
        assert_eq!(medium.size as usize, medium_bytes.len());
        assert_eq!(harness.fs.read_file_bytes(&medium).unwrap(), medium_bytes);

        let large = load_named_inode(&harness.fs, "large.bin");
        assert_eq!(large.size as usize, large_bytes.len());
        assert_eq!(harness.fs.read_file_bytes(&large).unwrap(), large_bytes);

        let offsets = load_named_inode(&harness.fs, "offsets.bin");
        let bytes = harness.fs.read_file_bytes(&offsets).unwrap();
        assert!(bytes.starts_with(b"START"));
        assert_eq!(&bytes[2048..2054], b"MIDDLE");
        assert_eq!(&bytes[bytes.len() - 4..], b"TAIL");

        let attrs_after = load_named_inode(&harness.fs, "attrs.txt");
        assert_eq!(attrs_after.mode & 0o777, 0o700);
        assert_eq!(attrs_after.uid, 777);
        assert_eq!(attrs_after.gid, 888);
    }

    #[test]
    fn fuse_create_allows_existing_without_excl() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "fuse_create_existing.bin", 1 << 20);
        let file = harness
            .fs
            .nfs_create(ROOT_INODE, "existing.dat", 0, 0)
            .unwrap();
        harness.fs.nfs_write(file.inode, 0, b"hello").unwrap();
        harness.fs.flush_pending().unwrap();

        let (opened, created) = harness
            .fs
            .fuse_create_file(ROOT_INODE, "existing.dat", 0, 0, 0o644, 0, 0)
            .unwrap();
        assert!(!created);
        assert_eq!(opened.inode, file.inode);

        let result = harness
            .fs
            .fuse_create_file(ROOT_INODE, "existing.dat", 0, 0, 0o644, 0, libc::O_EXCL);
        assert!(matches!(result, Err(code) if code == EEXIST));
    }

    #[test]
    fn fuse_create_truncates_existing_when_requested() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "fuse_create_trunc.bin", 1 << 20);
        let file = harness
            .fs
            .nfs_create(ROOT_INODE, "truncate.dat", 0, 0)
            .unwrap();
        harness.fs.nfs_write(file.inode, 0, b"payload").unwrap();
        harness.fs.flush_pending().unwrap();

        let (opened, created) = harness
            .fs
            .fuse_create_file(ROOT_INODE, "truncate.dat", 0, 0, 0o644, 0, libc::O_TRUNC)
            .unwrap();
        assert!(!created);
        assert_eq!(opened.inode, file.inode);
        assert_eq!(opened.size, 0);
        assert_eq!(
            harness.fs.nfs_read(file.inode, 0, 64).unwrap(),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn nfs_setattr_huge_truncate_returns_efbig_and_keeps_fs_alive() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "setattr_huge_truncate.bin", 1 << 20);

        let file = harness.fs.nfs_create(ROOT_INODE, "huge.bin", 0, 0).unwrap();
        harness.fs.nfs_write(file.inode, 0, b"x").unwrap();

        let result =
            harness
                .fs
                .nfs_setattr(file.inode, None, None, None, Some(999_999_999_999_999), None, None);
        assert!(matches!(result, Err(code) if code == EFBIG));

        let record = harness.fs.load_inode(file.inode).unwrap();
        assert_eq!(record.size, 1);
        assert_eq!(
            harness.fs.nfs_read(file.inode, 0, 1).unwrap(),
            b"x".to_vec()
        );
    }

    #[test]
    fn rename_same_parent_drops_old_name() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "rename_same_parent.bin", 1 << 20);

        let lock = harness
            .fs
            .nfs_create(ROOT_INODE, "config.lock", 0, 0)
            .unwrap();
        harness
            .fs
            .nfs_rename(ROOT_INODE, "config.lock", ROOT_INODE, "config", 0)
            .unwrap();

        assert!(matches!(
            harness.fs.nfs_lookup(ROOT_INODE, "config.lock"),
            Err(code) if code == ENOENT
        ));
        let config = harness.fs.nfs_lookup(ROOT_INODE, "config").unwrap();
        assert_eq!(config.inode, lock.inode);

        let new_lock = harness
            .fs
            .nfs_create(ROOT_INODE, "config.lock", 0, 0)
            .unwrap();
        assert_ne!(new_lock.inode, lock.inode);
    }

    #[test]
    fn metadata_only_flush_preserves_large_file_pointer() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::with_config(dir.path(), "meta_only_large.bin", 1 << 20, |cfg| {
            cfg.disable_journal = true;
            cfg.flush_interval_ms = 0;
            cfg.inline_threshold = 4096;
        });

        let file = harness
            .fs
            .nfs_create(ROOT_INODE, "large-meta.dat", 0, 0)
            .unwrap();
        let payload = vec![0x5Au8; 64 * 1024];
        harness.fs.nfs_write(file.inode, 0, &payload).unwrap();
        harness.fs.flush_pending().unwrap();

        let mut record = harness.fs.load_inode(file.inode).unwrap();
        record.mode = 0o100640;
        harness.fs.stage_inode(record).unwrap();
        harness.fs.flush_pending().unwrap();

        let stored = harness.fs.load_inode(file.inode).unwrap();
        assert_eq!(stored.mode & 0o777, 0o640);
        assert_eq!(
            harness
                .fs
                .nfs_read(file.inode, 0, payload.len() as u32)
                .unwrap(),
            payload
        );
    }

    #[test]
    fn nfs_setattr_preserves_pending_large_payload_before_flush() {
        let dir = tempdir().unwrap();
        let harness =
            TestHarness::with_config(dir.path(), "setattr_pending_large.bin", 1 << 20, |cfg| {
                cfg.disable_journal = true;
                cfg.flush_interval_ms = 0;
                cfg.inline_threshold = 4096;
            });

        let file = harness
            .fs
            .nfs_create(ROOT_INODE, "pending-large.dat", 0, 0)
            .unwrap();
        let payload = vec![0xA7u8; 64 * 1024];
        harness.fs.nfs_write(file.inode, 0, &payload).unwrap();

        harness
            .fs
            .nfs_setattr(file.inode, Some(0o100640), None, None, None, None, None)
            .unwrap();

        let before_flush = harness
            .fs
            .nfs_read(file.inode, 0, payload.len() as u32)
            .unwrap();
        assert_eq!(before_flush, payload);

        harness.fs.flush_pending().unwrap();
        let after_flush = harness
            .fs
            .nfs_read(file.inode, 0, payload.len() as u32)
            .unwrap();
        assert_eq!(after_flush, payload);
    }

    #[test]
    fn nfs_setattr_applies_explicit_atime_and_mtime() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::new(dir.path(), "nfs_setattr_timestamps.bin", 1 << 20);

        let file = harness.fs.nfs_create(ROOT_INODE, "ts_test.txt", 0, 0).unwrap();

        // Set explicit atime=1900000000 and mtime=1950000000 (values from utimensat/05.t).
        let atime = OffsetDateTime::from_unix_timestamp(1_900_000_000).unwrap();
        let mtime = OffsetDateTime::from_unix_timestamp(1_950_000_000).unwrap();
        harness
            .fs
            .nfs_setattr(file.inode, None, None, None, None, Some(atime), Some(mtime))
            .unwrap();

        let record = harness.fs.load_inode(file.inode).unwrap();
        assert_eq!(record.atime.unix_timestamp(), 1_900_000_000, "atime must be set to the supplied value");
        assert_eq!(record.mtime.unix_timestamp(), 1_950_000_000, "mtime must be set to the supplied value");
    }

    #[test]
    fn flush_pending_for_inode_keeps_other_pending_entries() {
        let dir = tempdir().unwrap();
        let harness =
            TestHarness::with_config(dir.path(), "flush_inode_only.bin", 1 << 20, |cfg| {
                cfg.disable_journal = true;
                cfg.flush_interval_ms = 0;
            });
        let file_a = harness.fs.nfs_create(ROOT_INODE, "a.dat", 0, 0).unwrap();
        let file_b = harness.fs.nfs_create(ROOT_INODE, "b.dat", 0, 0).unwrap();
        harness.fs.nfs_write(file_a.inode, 0, b"aaaa").unwrap();
        harness.fs.nfs_write(file_b.inode, 0, b"bbbb").unwrap();

        harness.fs.flush_pending_for_inode(file_a.inode).unwrap();

        assert!(
            harness.fs.pending_inodes.lock().contains_key(&file_b.inode),
            "expected unrelated inode to remain pending"
        );
        let stored_a = harness
            .runtime
            .block_on(harness.metadata.get_inode(file_a.inode))
            .unwrap()
            .unwrap();
        assert_eq!(harness.fs.read_file_bytes(&stored_a).unwrap(), b"aaaa");

        harness.fs.flush_pending().unwrap();
        let stored_b = harness
            .runtime
            .block_on(harness.metadata.get_inode(file_b.inode))
            .unwrap()
            .unwrap();
        assert_eq!(harness.fs.read_file_bytes(&stored_b).unwrap(), b"bbbb");
    }

    #[test]
    fn flush_pending_for_inode_flushes_pending_ancestor_directories() {
        let dir = tempdir().unwrap();
        let harness =
            TestHarness::with_config(dir.path(), "flush_inode_ancestors.bin", 1 << 20, |cfg| {
                cfg.disable_journal = true;
                cfg.flush_interval_ms = 0;
            });
        let dir_a = harness.fs.nfs_mkdir(ROOT_INODE, "a", 0, 0).unwrap();
        let dir_b = harness.fs.nfs_mkdir(dir_a.inode, "b", 0, 0).unwrap();
        let file = harness
            .fs
            .nfs_create(dir_b.inode, "target.bin", 0, 0)
            .unwrap();
        harness.fs.nfs_write(file.inode, 0, b"payload").unwrap();

        harness.fs.flush_pending_for_inode(file.inode).unwrap();

        let pending = harness.fs.pending_inodes.lock();
        assert!(
            !pending.contains_key(&file.inode),
            "file inode should be flushed"
        );
        assert!(
            !pending.contains_key(&dir_b.inode),
            "direct parent should be flushed"
        );
        assert!(
            !pending.contains_key(&dir_a.inode),
            "ancestor directory should be flushed"
        );
        assert!(
            !pending.contains_key(&ROOT_INODE),
            "root directory should be flushed when pending"
        );
        drop(pending);

        let root = harness
            .runtime
            .block_on(harness.metadata.get_inode(ROOT_INODE))
            .unwrap()
            .unwrap();
        let a_ino = root.children().unwrap().get("a").copied();
        assert_eq!(a_ino, Some(dir_a.inode));
    }

    #[test]
    fn concurrent_flush_does_not_hide_pending_inodes() {
        let dir = tempdir().unwrap();
        let harness =
            TestHarness::with_config(dir.path(), "concurrent_flush.bin", 1 << 30, |cfg| {
                cfg.disable_journal = true;
                cfg.flush_interval_ms = 0;
            });
        let file = harness.fs.nfs_create(ROOT_INODE, "race.bin", 0, 0).unwrap();

        thread::scope(|scope| {
            let fs = &harness.fs;
            let inode = file.inode;
            let writer = scope.spawn(move || {
                let payload = vec![0x5Au8; 8192];
                for i in 0..64u64 {
                    fs.nfs_write(inode, i * payload.len() as u64, &payload)
                        .unwrap();
                }
            });
            let flusher = scope.spawn(move || {
                for _ in 0..64 {
                    fs.flush_pending().unwrap();
                }
            });
            writer.join().unwrap();
            flusher.join().unwrap();
        });

        harness.fs.flush_pending().unwrap();
        let stored = harness
            .runtime
            .block_on(harness.metadata.get_inode(file.inode))
            .unwrap()
            .unwrap();
        assert!(stored.size > 0);
    }

    #[test]
    fn load_inode_visible_during_large_segment_mutation() {
        let dir = tempdir().unwrap();
        let harness = TestHarness::with_config(dir.path(), "visibility.bin", 1 << 30, |cfg| {
            cfg.disable_journal = true;
            cfg.flush_interval_ms = 0;
        });
        let file = harness
            .fs
            .nfs_create(ROOT_INODE, "visibility.dat", 0, 0)
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let write_payload = vec![0x33u8; 256 * 1024];

        thread::scope(|scope| {
            let fs = &harness.fs;
            let start = barrier.clone();
            let inode = file.inode;
            let writer = scope.spawn(move || {
                start.wait();
                for i in 0..128u64 {
                    let record = fs.load_inode(inode).unwrap();
                    fs.write_large_segments(record, i * write_payload.len() as u64, &write_payload)
                        .unwrap();
                }
            });
            let fs = &harness.fs;
            let start = barrier.clone();
            let inode = file.inode;
            let reader = scope.spawn(move || {
                start.wait();
                for _ in 0..50_000 {
                    let record = fs.load_inode(inode);
                    assert!(
                        !matches!(record, Err(code) if code == ENOENT),
                        "inode visibility hole during mutation"
                    );
                }
            });
            writer.join().unwrap();
            reader.join().unwrap();
        });
    }
