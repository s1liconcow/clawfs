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
                .block_on(MetadataStore::new(&config, runtime.handle().clone()))
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
        let segments = Arc::new(SegmentManager::new(&config, runtime.handle().clone()).unwrap());
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
    let harness = TestHarness::with_config(dir.path(), "adaptive_perf.bin", 1024 * 1024, |cfg| {
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
    assert!(!harness
        .fs
        .active_inodes
        .iter()
        .any(|e| e.value().lock().pending.is_some()));
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
    harness
        .fs
        .stage_file(record.clone(), initial.clone(), None)
        .unwrap();

    let mut stale_view = harness.fs.load_inode(inode).unwrap();
    stale_view.size = 0;
    harness.fs.append_file(stale_view, b"xyz").unwrap();

    let pending_len = harness
        .fs
        .active_inodes
        .get(&inode)
        .map(|arc| arc.lock().pending.as_ref().and_then(|e| e.data.as_ref().map(|d| d.len())).unwrap_or(0))
        .unwrap_or(0);
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
    let harness = TestHarness::with_config(dir.path(), "adaptive_replay.bin", 1024 * 1024, |cfg| {
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

    let result =
        harness
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

    let result = harness.fs.nfs_setattr(
        file.inode,
        None,
        None,
        None,
        Some(999_999_999_999_999),
        None,
        None,
    );
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
        .nfs_rename(ROOT_INODE, "config.lock", ROOT_INODE, "config", 0, 0)
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

    let file = harness
        .fs
        .nfs_create(ROOT_INODE, "ts_test.txt", 0, 0)
        .unwrap();

    // Set explicit atime=1900000000 and mtime=1950000000 (values from utimensat/05.t).
    let atime = OffsetDateTime::from_unix_timestamp(1_900_000_000).unwrap();
    let mtime = OffsetDateTime::from_unix_timestamp(1_950_000_000).unwrap();
    harness
        .fs
        .nfs_setattr(file.inode, None, None, None, None, Some(atime), Some(mtime))
        .unwrap();

    let record = harness.fs.load_inode(file.inode).unwrap();
    assert_eq!(
        record.atime.unix_timestamp(),
        1_900_000_000,
        "atime must be set to the supplied value"
    );
    assert_eq!(
        record.mtime.unix_timestamp(),
        1_950_000_000,
        "mtime must be set to the supplied value"
    );
}

#[test]
fn flush_pending_for_inode_keeps_other_pending_entries() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::with_config(dir.path(), "flush_inode_only.bin", 1 << 20, |cfg| {
        cfg.disable_journal = true;
        cfg.flush_interval_ms = 0;
    });
    let file_a = harness.fs.nfs_create(ROOT_INODE, "a.dat", 0, 0).unwrap();
    let file_b = harness.fs.nfs_create(ROOT_INODE, "b.dat", 0, 0).unwrap();
    harness.fs.nfs_write(file_a.inode, 0, b"aaaa").unwrap();
    harness.fs.nfs_write(file_b.inode, 0, b"bbbb").unwrap();

    harness.fs.flush_pending_for_inode(file_a.inode).unwrap();

    assert!(
        harness
            .fs
            .active_inodes
            .get(&file_b.inode)
            .map_or(false, |arc| arc.lock().pending.is_some()),
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

    let pending = |ino| harness.fs.active_inodes.get(&ino).map_or(false, |arc| arc.lock().pending.is_some());
    assert!(
        !pending(file.inode),
        "file inode should be flushed"
    );
    assert!(
        !pending(dir_b.inode),
        "direct parent should be flushed"
    );
    assert!(
        !pending(dir_a.inode),
        "ancestor directory should be flushed"
    );
    assert!(
        !pending(ROOT_INODE),
        "root directory should be flushed when pending"
    );

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
    let harness = TestHarness::with_config(dir.path(), "concurrent_flush.bin", 1 << 30, |cfg| {
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

// ===== Permission / POSIX semantics tests =====

/// Helper: create a directory under parent with a given uid/gid, then set its mode bits.
fn make_dir_with_mode(
    fs: &OsageFs,
    parent: u64,
    name: &str,
    uid: u32,
    gid: u32,
    mode_bits: u32,
) -> InodeRecord {
    let dir = fs.op_mkdir(parent, name, uid, gid).unwrap();
    // op_fuse_setattr with caller_uid=0 (root) so it always succeeds
    fs.op_fuse_setattr(
        dir.inode,
        0,
        0,
        Some(mode_bits),
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    fs.load_inode(dir.inode).unwrap()
}

#[test]
fn chmod_by_owner_succeeds() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "chmod_owner.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let file = fs.op_create(ROOT_INODE, "test.txt", 1000, 1000).unwrap();
    // Owner (uid=1000) can chmod
    let result = fs.op_fuse_setattr(
        file.inode,
        1000,
        1000,
        Some(0o644),
        None,
        None,
        None,
        None,
        None,
    );
    assert!(result.is_ok());
    let updated = fs.load_inode(file.inode).unwrap();
    assert_eq!(updated.mode & 0o777, 0o644);
}

#[test]
fn chmod_by_non_owner_returns_eperm() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "chmod_noown.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let file = fs.op_create(ROOT_INODE, "test.txt", 1000, 1000).unwrap();
    // Non-owner (uid=1001) cannot chmod
    let result = fs.op_fuse_setattr(
        file.inode,
        1001,
        1001,
        Some(0o644),
        None,
        None,
        None,
        None,
        None,
    );
    assert_eq!(result.unwrap_err(), EPERM);
}

#[test]
fn chmod_by_root_always_succeeds() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "chmod_root.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let file = fs.op_create(ROOT_INODE, "test.txt", 1000, 1000).unwrap();
    // Root (uid=0) can always chmod
    let result = fs.op_fuse_setattr(file.inode, 0, 0, Some(0o600), None, None, None, None, None);
    assert!(result.is_ok());
    let updated = fs.load_inode(file.inode).unwrap();
    assert_eq!(updated.mode & 0o777, 0o600);
}

#[test]
fn chown_uid_by_non_root_returns_eperm() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "chown_uid.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let file = fs.op_create(ROOT_INODE, "test.txt", 1000, 1000).unwrap();
    // Even the file owner cannot change uid (Linux rule: uid change is root-only)
    let result = fs.op_fuse_setattr(
        file.inode,
        1000,
        1000,
        None,
        Some(1001),
        None,
        None,
        None,
        None,
    );
    assert_eq!(result.unwrap_err(), EPERM);
    // Non-owner also cannot change uid
    let result = fs.op_fuse_setattr(
        file.inode,
        1001,
        1001,
        None,
        Some(1002),
        None,
        None,
        None,
        None,
    );
    assert_eq!(result.unwrap_err(), EPERM);
}

#[test]
fn chown_uid_by_root_succeeds() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "chown_uid_root.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let file = fs.op_create(ROOT_INODE, "test.txt", 1000, 1000).unwrap();
    // Root can change uid
    let result = fs.op_fuse_setattr(file.inode, 0, 0, None, Some(2000), None, None, None, None);
    assert!(result.is_ok());
    let updated = fs.load_inode(file.inode).unwrap();
    assert_eq!(updated.uid, 2000);
}

#[test]
fn chown_gid_by_owner_succeeds() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "chown_gid_own.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let file = fs.op_create(ROOT_INODE, "test.txt", 1000, 1000).unwrap();
    // Owner (uid=1000) can change gid
    let result = fs.op_fuse_setattr(
        file.inode,
        1000,
        1000,
        None,
        None,
        Some(2000),
        None,
        None,
        None,
    );
    assert!(result.is_ok());
    let updated = fs.load_inode(file.inode).unwrap();
    assert_eq!(updated.gid, 2000);
}

#[test]
fn chown_gid_by_non_owner_returns_eperm() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "chown_gid_noown.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let file = fs.op_create(ROOT_INODE, "test.txt", 1000, 1000).unwrap();
    // Non-owner cannot change gid
    let result = fs.op_fuse_setattr(
        file.inode,
        1001,
        1001,
        None,
        None,
        Some(2000),
        None,
        None,
        None,
    );
    assert_eq!(result.unwrap_err(), EPERM);
}

#[test]
fn chown_clears_suid_sgid_for_non_root_owner_change() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "chown_suid.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    // Root creates file, sets SUID+SGID, then gives it to uid=1000
    let file = fs.op_create(ROOT_INODE, "setuid.bin", 0, 0).unwrap();
    fs.op_fuse_setattr(file.inode, 0, 0, Some(0o6755), None, None, None, None, None)
        .unwrap();
    fs.op_fuse_setattr(
        file.inode,
        0,
        0,
        None,
        Some(1000),
        Some(1000),
        None,
        None,
        None,
    )
    .unwrap();
    // Root chown should NOT clear SUID/SGID (root exemption)
    let after_root = fs.load_inode(file.inode).unwrap();
    assert_eq!(
        after_root.mode & 0o6000,
        0o6000,
        "root chown preserves SUID/SGID"
    );

    // Re-set SUID+SGID (file is now owned by uid=1000, so owner can chmod)
    fs.op_fuse_setattr(
        file.inode,
        1000,
        1000,
        Some(0o6755),
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    let before = fs.load_inode(file.inode).unwrap();
    assert_eq!(before.mode & 0o6000, 0o6000);

    // Owner (uid=1000) changes gid -> SUID+SGID must be cleared on files
    fs.op_fuse_setattr(
        file.inode,
        1000,
        1000,
        None,
        None,
        Some(2000),
        None,
        None,
        None,
    )
    .unwrap();
    let after = fs.load_inode(file.inode).unwrap();
    assert_eq!(
        after.mode & 0o6000,
        0,
        "non-root chown strips SUID+SGID from files"
    );
}

#[test]
fn sticky_bit_blocks_unlink_by_third_party() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "sticky_unlink.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    // Sticky dir owned by uid=1000; file in it owned by uid=1001
    let sticky_dir = make_dir_with_mode(fs, ROOT_INODE, "stickydir", 1000, 1000, 0o1777);
    let _file = fs
        .op_create(sticky_dir.inode, "victim.txt", 1001, 1001)
        .unwrap();
    // uid=1002 is neither dir owner nor file owner
    let result = fs.op_remove_file(sticky_dir.inode, "victim.txt", 1002);
    assert_eq!(result.unwrap_err(), EPERM);
}

#[test]
fn sticky_bit_allows_unlink_by_file_owner() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "sticky_file_own.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let sticky_dir = make_dir_with_mode(fs, ROOT_INODE, "stickydir", 1000, 1000, 0o1777);
    let _file = fs
        .op_create(sticky_dir.inode, "myfile.txt", 1001, 1001)
        .unwrap();
    // File owner (uid=1001) can remove even without owning the directory
    let result = fs.op_remove_file(sticky_dir.inode, "myfile.txt", 1001);
    assert!(result.is_ok());
}

#[test]
fn sticky_bit_allows_unlink_by_dir_owner() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "sticky_dir_own.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let sticky_dir = make_dir_with_mode(fs, ROOT_INODE, "stickydir", 1000, 1000, 0o1777);
    let _file = fs
        .op_create(sticky_dir.inode, "theirfile.txt", 1001, 1001)
        .unwrap();
    // Directory owner (uid=1000) can remove anyone's file
    let result = fs.op_remove_file(sticky_dir.inode, "theirfile.txt", 1000);
    assert!(result.is_ok());
}

#[test]
fn sticky_bit_blocks_rmdir_by_third_party() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "sticky_rmdir.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let sticky_dir = make_dir_with_mode(fs, ROOT_INODE, "stickydir", 1000, 1000, 0o1777);
    let _sub = fs.op_mkdir(sticky_dir.inode, "subdir", 1001, 1001).unwrap();
    // Third party (uid=1002) cannot rmdir a subdirectory they don't own
    let result = fs.op_remove_dir(sticky_dir.inode, "subdir", 1002);
    assert_eq!(result.unwrap_err(), EPERM);
}

#[test]
fn sticky_bit_blocks_rename_by_third_party() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "sticky_rename.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let sticky_dir = make_dir_with_mode(fs, ROOT_INODE, "stickydir", 1000, 1000, 0o1777);
    let _file = fs
        .op_create(sticky_dir.inode, "victim.txt", 1001, 1001)
        .unwrap();
    let other_dir = fs.op_mkdir(ROOT_INODE, "otherdir", 0, 0).unwrap();
    // uid=1002: not dir owner, not file owner → cannot rename out of sticky dir
    let result = fs.op_rename(
        sticky_dir.inode,
        "victim.txt",
        other_dir.inode,
        "moved.txt",
        0,
        1002,
    );
    assert_eq!(result.unwrap_err(), EPERM);
}

#[test]
fn sticky_bit_allows_rename_by_file_owner() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "sticky_ren_fown.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let sticky_dir = make_dir_with_mode(fs, ROOT_INODE, "stickydir", 1000, 1000, 0o1777);
    let _file = fs
        .op_create(sticky_dir.inode, "myfile.txt", 1001, 1001)
        .unwrap();
    let other_dir = fs.op_mkdir(ROOT_INODE, "otherdir", 0, 0).unwrap();
    // File owner (uid=1001) can rename their own file
    let result = fs.op_rename(
        sticky_dir.inode,
        "myfile.txt",
        other_dir.inode,
        "moved.txt",
        0,
        1001,
    );
    assert!(result.is_ok());
}

#[test]
fn sgid_dir_new_file_inherits_parent_gid() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "sgid_gid.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    // SGID dir owned by gid=5000
    let sgid_dir = make_dir_with_mode(fs, ROOT_INODE, "sgiddir", 1000, 5000, 0o2755);
    assert_ne!(sgid_dir.mode & 0o2000, 0, "SGID bit should be set on dir");
    // Caller with gid=9999 creates a file in the SGID dir
    let file = fs
        .op_create(sgid_dir.inode, "newfile.txt", 2000, 9999)
        .unwrap();
    // File should inherit gid=5000 from the directory, not caller's gid=9999
    assert_eq!(
        file.gid, 5000,
        "file gid should be inherited from SGID parent"
    );
}

#[test]
fn sgid_dir_new_subdir_inherits_gid_and_sgid_bit() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "sgid_subdir.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    // SGID dir owned by gid=5000
    let sgid_dir = make_dir_with_mode(fs, ROOT_INODE, "sgiddir", 1000, 5000, 0o2755);
    // Create subdirectory via op_mkdir_fuse (which propagates SGID bit to new subdirs)
    let subdir = fs
        .op_mkdir_fuse(sgid_dir.inode, "sub", 2000, 9999, 0o755, 0o022)
        .unwrap();
    assert_eq!(
        subdir.gid, 5000,
        "subdir gid should be inherited from SGID parent"
    );
    assert_ne!(
        subdir.mode & 0o2000,
        0,
        "SGID bit should propagate to new subdirectories"
    );
}

// chmod/12.t: writing to a SUID/SGID file by a non-owner clears those bits.
// The kernel sends a FUSE setattr (req.uid = writer) stripping SUID/SGID; we
// must allow it or the write() syscall itself fails.
#[test]
fn write_by_non_owner_clears_suid_bit() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "suid_clear.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    // Root creates a file with SUID set
    let f = fs.op_create(ROOT_INODE, "suidfile", 0, 0).unwrap();
    fs.op_fuse_setattr(f.inode, 0, 0, Some(0o4777), None, None, None, None, None)
        .unwrap();
    let file = fs.load_inode(f.inode).unwrap();
    assert_ne!(file.mode & 0o4000, 0, "SUID should be set initially");
    // Kernel strips SUID on behalf of writer uid=65534: setattr with new mode=0777
    let attr = fs
        .op_fuse_setattr(
            f.inode,
            65534,
            65534,
            Some(0o0777),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("priv-strip setattr should succeed even from non-owner");
    assert_eq!(attr.perm & 0o4000, 0, "SUID should be cleared");
    assert_eq!(attr.perm & 0o0777, 0o0777, "rwx bits should be unchanged");
}

#[test]
fn write_by_non_owner_clears_sgid_bit() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "sgid_clear.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    let f = fs.op_create(ROOT_INODE, "sgidfile", 0, 0).unwrap();
    fs.op_fuse_setattr(f.inode, 0, 0, Some(0o2777), None, None, None, None, None)
        .unwrap();
    let file = fs.load_inode(f.inode).unwrap();
    assert_ne!(file.mode & 0o2000, 0, "SGID should be set initially");
    let attr = fs
        .op_fuse_setattr(
            f.inode,
            65534,
            65534,
            Some(0o0777),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("priv-strip setattr should succeed even from non-owner");
    assert_eq!(attr.perm & 0o2000, 0, "SGID should be cleared");
    assert_eq!(attr.perm & 0o0777, 0o0777, "rwx bits should be unchanged");
}

#[test]
fn chmod_by_non_owner_non_strip_still_fails() {
    // Arbitrary mode change (not just stripping SUID/SGID) by non-owner → EPERM
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "chmod_eperm2.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    // Root creates file with SUID, gives to uid=0
    let f = fs.op_create(ROOT_INODE, "file", 0, 0).unwrap();
    fs.op_fuse_setattr(f.inode, 0, 0, Some(0o4755), None, None, None, None, None)
        .unwrap();
    // non-owner tries to change rwx bits as well → must still be EPERM
    let err = fs
        .op_fuse_setattr(
            f.inode,
            65534,
            65534,
            Some(0o0644),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
    assert_eq!(err, libc::EPERM, "non-owner rwx change should be EPERM");
}

#[test]
fn non_sgid_dir_file_uses_caller_gid() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::new(dir.path(), "no_sgid.bin", 8 * 1024 * 1024);
    let fs = &harness.fs;
    // Normal dir (no SGID), owned by gid=5000
    let normal_dir = make_dir_with_mode(fs, ROOT_INODE, "normaldir", 1000, 5000, 0o755);
    assert_eq!(normal_dir.mode & 0o2000, 0, "SGID bit should NOT be set");
    // Caller with gid=9999 creates file; should get caller's gid, not parent's
    let file = fs
        .op_create(normal_dir.inode, "newfile.txt", 2000, 9999)
        .unwrap();
    assert_eq!(file.gid, 9999, "without SGID parent, file keeps caller gid");
}

// Regression: metadata-only setattr (chmod) on a large file that is still
// in pending_inodes must NOT call read_file_bytes + stage_file (which could
// corrupt content by reading a stale Inline([]) placeholder).
// After the fix, op_fuse_setattr uses stage_inode for size=None, preserving
// the staged data in-place.
#[test]
fn fuse_setattr_chmod_on_large_pending_file_preserves_content() {
    let dir = tempdir().unwrap();
    let harness =
        TestHarness::with_config(dir.path(), "fuse_chmod_pending_large.bin", 1 << 20, |cfg| {
            cfg.disable_journal = true;
            cfg.flush_interval_ms = 0;
            cfg.inline_threshold = 512;
        });
    let fs = &harness.fs;

    // Write a file larger than inline_threshold so it enters staged storage.
    let payload = vec![0xABu8; 8 * 1024];
    let file = fs.nfs_create(ROOT_INODE, "large.bin", 1000, 1000).unwrap();
    fs.nfs_write(file.inode, 0, &payload).unwrap();

    // File is now in pending_inodes.  Perform a metadata-only chmod.
    // Before the fix this called read_file_bytes on the pending record,
    // which could return empty bytes and then stage_file would corrupt the
    // file; after the fix it calls stage_inode (data preserved in-place).
    fs.op_fuse_setattr(
        file.inode,
        1000,
        1000,
        Some(0o100640),
        None,
        None,
        None,
        None,
        None,
    )
    .expect("chmod on pending large file should succeed");

    // Content must survive the chmod.
    let before_flush = fs.nfs_read(file.inode, 0, payload.len() as u32).unwrap();
    assert_eq!(
        before_flush, payload,
        "content corrupted by chmod before flush"
    );

    fs.flush_pending().unwrap();

    let after_flush = fs.nfs_read(file.inode, 0, payload.len() as u32).unwrap();
    assert_eq!(
        after_flush, payload,
        "content corrupted by chmod after flush"
    );

    let stored = fs.load_inode(file.inode).unwrap();
    assert_eq!(stored.mode & 0o777, 0o640, "chmod mode not persisted");
    assert_eq!(stored.size, payload.len() as u64, "size must not change");
}

// Regression: metadata-only setattr on a large file that has already been
// flushed (inode lives in the metadata store with correct Segments storage)
// must persist both the correct storage pointer AND the metadata change.
#[test]
fn fuse_setattr_chmod_after_flush_preserves_large_file_content() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::with_config(
        dir.path(),
        "fuse_chmod_after_flush_large.bin",
        1 << 20,
        |cfg| {
            cfg.disable_journal = true;
            cfg.flush_interval_ms = 0;
            cfg.inline_threshold = 512;
        },
    );
    let fs = &harness.fs;

    let payload = vec![0xCDu8; 8 * 1024];
    let file = fs
        .nfs_create(ROOT_INODE, "flushed.bin", 1000, 1000)
        .unwrap();
    fs.nfs_write(file.inode, 0, &payload).unwrap();
    fs.flush_pending().unwrap();

    // Inode is now only in the metadata store.  chmod creates a metadata-only
    // pending entry (stage_inode).  A second flush must merge it with the
    // correct Segments pointer from the metadata store.
    fs.op_fuse_setattr(
        file.inode,
        1000,
        1000,
        Some(0o100600),
        None,
        None,
        None,
        None,
        None,
    )
    .expect("chmod after flush should succeed");

    fs.flush_pending().unwrap();

    let after = fs.load_inode(file.inode).unwrap();
    assert_eq!(
        after.mode & 0o777,
        0o600,
        "chmod mode not persisted after second flush"
    );
    assert_eq!(after.size, payload.len() as u64, "size must not change");

    let content = fs.nfs_read(file.inode, 0, payload.len() as u32).unwrap();
    assert_eq!(
        content, payload,
        "content corrupted after chmod + second flush"
    );
}

// Regression: if a metadata-only pending entry (data=None) carries a stale
// Inline([]) storage placeholder (the hallmark of a setattr that raced with
// a concurrent flush), flush_pending_selected must reload the authoritative
// record from the metadata store and merge the pending metadata changes
// rather than persisting the stale Inline([]) pointer.
#[test]
fn flush_stale_setattr_entry_merges_storage_from_metadata_store() {
    let dir = tempdir().unwrap();
    let harness = TestHarness::with_config(dir.path(), "flush_stale_setattr.bin", 1 << 20, |cfg| {
        cfg.disable_journal = true;
        cfg.flush_interval_ms = 0;
        cfg.inline_threshold = 512;
    });
    let fs = &harness.fs;

    // Write and flush a large file so the metadata store has its correct
    // Segments(extents) storage pointer.
    let payload = vec![0xEFu8; 8 * 1024];
    let file = fs.nfs_create(ROOT_INODE, "race.bin", 1000, 1000).unwrap();
    fs.nfs_write(file.inode, 0, &payload).unwrap();
    fs.flush_pending().unwrap();

    // Simulate the race: load the flushed record, corrupt its storage field
    // to Inline([]) (the stale placeholder written by flush before the
    // segment pointer is known), change its mode, and inject it as a
    // metadata-only pending entry — exactly what stage_inode produces when
    // called from setattr on a record loaded from flushing_inodes just
    // before the concurrent flush completes.
    let mut stale_record = fs.load_inode(file.inode).unwrap();
    stale_record.storage = FileStorage::Inline(Vec::new()); // stale placeholder
    stale_record.mode = (stale_record.mode & !0o7777) | 0o100604;

    let active_arc = fs.active_inodes.entry(file.inode).or_insert_with(|| std::sync::Arc::new(parking_lot::Mutex::new(crate::fs::ActiveInode::default()))).clone();
    active_arc.lock().pending = Some(PendingEntry {
        record: stale_record,
        data: None,
    });

    // flush_pending_selected must detect the stale Inline([]) storage,
    // reload from metadata store, and persist the merged record.
    fs.flush_pending().unwrap();

    let after = fs.load_inode(file.inode).unwrap();
    assert_eq!(
        after.mode & 0o777,
        0o604,
        "metadata change from stale entry not applied"
    );
    assert_eq!(after.size, payload.len() as u64, "size must be preserved");

    // Most importantly: content must still be readable (storage pointer intact).
    let content = fs.nfs_read(file.inode, 0, payload.len() as u32).unwrap();
    assert_eq!(
        content, payload,
        "stale Inline([]) storage overwrote correct Segments pointer — data lost"
    );
}

/// Regression test for the FIO seq_write_1m EIO bug.
///
/// Before the fix, writing to a large file at offset 0 after it had been
/// flushed to committed segment storage would call `read_file_bytes` to
/// materialise the entire file, which failed with EIO for files large
/// enough to exceed segment-cache limits.  The fix introduces base_extents
/// in PendingSegments so the write path carries forward committed extents
/// without reading them.
#[test]
fn overwrite_at_offset_zero_after_flush_does_not_eio() {
    let dir = tempdir().unwrap();
    // Use a small pending_bytes threshold so the file is flushed quickly.
    let harness = TestHarness::new(dir.path(), "overwrite_after_flush.bin", 1024 * 1024);
    // inline_threshold is 512 in test config; any payload > 512 bytes
    // triggers the segment write path.
    let chunk_size = harness.config.inline_threshold + 64;
    let initial_data: Vec<u8> = (0..chunk_size).map(|i| (i & 0xFF) as u8).collect();

    // Stage and flush the file so it lives in committed segment storage.
    let inode = harness.fs.allocate_inode_id().unwrap();
    let record = make_file(inode, "large.bin");
    harness
        .fs
        .stage_file(record, initial_data.clone(), None)
        .unwrap();
    harness.fs.flush_pending().unwrap();

    // Confirm the inode is committed in segment storage and not pending.
    assert!(
        !harness
            .fs
            .active_inodes
            .get(&inode)
            .map_or(false, |arc| arc.lock().pending.is_some()),
        "inode should not be pending after flush"
    );
    let committed = harness
        .runtime
        .block_on(harness.metadata.get_inode(inode))
        .unwrap()
        .unwrap();
    assert!(
        matches!(committed.storage, FileStorage::Segments(_)),
        "expected segment storage after flush"
    );

    // Overwrite at offset 0 (the bug trigger: previously called
    // read_file_bytes on the full committed file and failed with EIO).
    let overwrite_data: Vec<u8> = vec![0xAA; chunk_size / 2];
    let written = harness
        .fs
        .op_write(inode, 0, &overwrite_data)
        .expect("overwrite at offset 0 after flush should not EIO");
    assert_eq!(written as usize, overwrite_data.len());

    // The inode should now be pending with the overwrite staged.
    assert!(
        harness
            .fs
            .active_inodes
            .get(&inode)
            .map_or(false, |arc| arc.lock().pending.is_some()),
        "inode should be pending after overwrite"
    );

    // Read the overwritten region from the pending state.
    let read_back = harness
        .fs
        .op_read(inode, 0, overwrite_data.len() as u32)
        .expect("read from pending overwrite should succeed");
    assert_eq!(
        read_back, overwrite_data,
        "overwritten region should return new data"
    );

    // The unmodified tail should still return original committed data.
    let tail_offset = overwrite_data.len() as u64;
    let tail_len = (initial_data.len() - overwrite_data.len()) as u32;
    let tail = harness
        .fs
        .op_read(inode, tail_offset, tail_len)
        .expect("read of unmodified tail should succeed");
    assert_eq!(
        tail,
        initial_data[overwrite_data.len()..],
        "unmodified tail should return original data"
    );

    // Flush again and verify the committed state is correct end-to-end.
    harness.fs.flush_pending().unwrap();
    let final_record = harness
        .runtime
        .block_on(harness.metadata.get_inode(inode))
        .unwrap()
        .unwrap();
    let final_bytes = harness.fs.read_file_bytes(&final_record).unwrap();
    assert_eq!(final_bytes.len(), initial_data.len());
    assert_eq!(
        &final_bytes[..overwrite_data.len()],
        overwrite_data.as_slice()
    );
    assert_eq!(
        &final_bytes[overwrite_data.len()..],
        &initial_data[overwrite_data.len()..]
    );
}

/// Sequential large writes that cross a flush boundary should not re-read
/// previously committed data.  Validates the base_extents path for
/// sequential append workloads like fio prefill_seq.
#[test]
fn sequential_large_writes_across_flush_boundary() {
    let dir = tempdir().unwrap();
    // pending_bytes small enough to force flushes between writes.
    let chunk_size = 1024usize; // larger than inline_threshold (512)
    let harness = TestHarness::new(dir.path(), "seq_across_flush.bin", (chunk_size * 2) as u64);

    let inode = harness.fs.allocate_inode_id().unwrap();
    let record = make_file(inode, "seq.bin");
    harness.fs.stage_file(record, Vec::new(), None).unwrap();

    let mut expected = Vec::new();
    for i in 0..6u8 {
        let chunk: Vec<u8> = vec![i; chunk_size];
        let offset = expected.len() as u64;
        harness
            .fs
            .op_write(inode, offset, &chunk)
            .expect("sequential write should not EIO");
        expected.extend_from_slice(&chunk);
    }

    harness.fs.flush_pending().unwrap();
    let stored = harness
        .runtime
        .block_on(harness.metadata.get_inode(inode))
        .unwrap()
        .unwrap();
    eprintln!(
        "DEBUG: stored.size={} storage={:?}",
        stored.size,
        match &stored.storage {
            crate::inode::FileStorage::Segments(exts) => {
                format!(
                    "Segments({}): {:?}",
                    exts.len(),
                    exts.iter()
                        .map(|e| (
                            e.logical_offset,
                            e.pointer.generation,
                            e.pointer.segment_id,
                            e.pointer.offset,
                            e.pointer.length
                        ))
                        .collect::<Vec<_>>()
                )
            }
            other => format!("{:?}", other),
        }
    );
    let content = harness.fs.read_file_bytes(&stored).unwrap();
    assert_eq!(
        content, expected,
        "sequential writes across flush boundaries must be consistent"
    );
}

/// Regression test for the fio seq_read_1m failure.
/// Mirrors: prefill_seq → seq_write_1m (overwrite) → seq_read_1m (verify)
/// pending_bytes=2*block so flush fires after every 2 writes.
#[test]
fn prefill_seq_then_overwrite_then_read_is_consistent() {
    let dir = tempdir().unwrap();
    let block = 1024usize; // > inline_threshold (512)
    let num_blocks = 10usize;
    let harness =
        TestHarness::with_config(dir.path(), "fio_repro.bin", (block * 2) as u64, |cfg| {
            cfg.disable_journal = true;
            cfg.flush_interval_ms = 0;
        });

    // Phase 1: prefill_seq — sequential writes filling the file
    let inode = harness.fs.allocate_inode_id().unwrap();
    let record = make_file(inode, "seq.bin");
    harness.fs.stage_file(record, Vec::new(), None).unwrap();
    for i in 0..num_blocks {
        let data: Vec<u8> = vec![i as u8; block];
        harness
            .fs
            .op_write(inode, (i * block) as u64, &data)
            .unwrap_or_else(|e| panic!("prefill write block {} failed with errno={}", i, e));
    }
    harness.fs.flush_pending().unwrap();

    // Phase 2: seq_write_1m — overwrite from offset 0 with different data
    for i in 0..num_blocks {
        let data: Vec<u8> = vec![(i as u8).wrapping_add(100); block];
        harness
            .fs
            .op_write(inode, (i * block) as u64, &data)
            .unwrap_or_else(|e| panic!("overwrite block {} failed with errno={}", i, e));
    }
    harness.fs.flush_pending().unwrap();

    // Phase 3: seq_read_1m — read back every block and verify overwrite data
    for i in 0..num_blocks {
        let offset = (i * block) as u64;
        let got = harness
            .fs
            .op_read(inode, offset, block as u32)
            .unwrap_or_else(|e| panic!("read at offset {} failed with errno={}", offset, e));
        let expected: Vec<u8> = vec![(i as u8).wrapping_add(100); block];
        assert_eq!(
            got, expected,
            "block {} at offset {} has wrong data after prefill+overwrite",
            i, offset
        );
    }
}

#[test]
fn disjoint_overwrites_preserve_chunk_logical_offsets_after_flush() {
    let dir = tempdir().unwrap();
    let block = 1024usize; // > inline_threshold
    let harness = TestHarness::with_config(dir.path(), "disjoint_overwrite.bin", 1 << 20, |cfg| {
        cfg.disable_journal = true;
        cfg.flush_interval_ms = 0;
    });

    let inode = harness.fs.allocate_inode_id().unwrap();
    let record = make_file(inode, "disjoint.bin");
    harness.fs.stage_file(record, Vec::new(), None).unwrap();

    let mut expected = Vec::new();
    for i in 0..6u8 {
        let chunk = vec![i; block];
        harness
            .fs
            .op_write(inode, expected.len() as u64, &chunk)
            .expect("prefill write should succeed");
        expected.extend_from_slice(&chunk);
    }
    harness.fs.flush_pending().unwrap();

    let overwrite_a = vec![0xAA; block];
    let overwrite_b = vec![0xBB; block];
    let offset_a = block as u64;
    let offset_b = (block * 4) as u64;
    harness
        .fs
        .op_write(inode, offset_a, &overwrite_a)
        .expect("first disjoint overwrite should succeed");
    harness
        .fs
        .op_write(inode, offset_b, &overwrite_b)
        .expect("second disjoint overwrite should succeed");
    expected[block..(block * 2)].copy_from_slice(&overwrite_a);
    expected[(block * 4)..(block * 5)].copy_from_slice(&overwrite_b);

    harness.fs.flush_pending().unwrap();

    let stored = harness
        .runtime
        .block_on(harness.metadata.get_inode(inode))
        .unwrap()
        .unwrap();
    let got = harness.fs.read_file_bytes(&stored).unwrap();
    assert_eq!(
        got, expected,
        "disjoint staged chunks must persist at their own logical offsets"
    );
}

#[test]
fn partial_overwrite_keeps_unmodified_tail_after_flush() {
    let dir = tempdir().unwrap();
    let block = 1024usize; // > inline_threshold
    let harness = TestHarness::with_config(dir.path(), "partial_tail.bin", 1 << 20, |cfg| {
        cfg.disable_journal = true;
        cfg.flush_interval_ms = 0;
    });

    let inode = harness.fs.allocate_inode_id().unwrap();
    let record = make_file(inode, "partial.bin");
    harness.fs.stage_file(record, Vec::new(), None).unwrap();

    let mut expected = Vec::new();
    for i in 0..4u8 {
        let chunk = vec![i; block];
        harness
            .fs
            .op_write(inode, expected.len() as u64, &chunk)
            .expect("prefill write should succeed");
        expected.extend_from_slice(&chunk);
    }
    harness.fs.flush_pending().unwrap();

    let overwrite = vec![0xCC; block];
    let overwrite_offset = block as u64;
    harness
        .fs
        .op_write(inode, overwrite_offset, &overwrite)
        .expect("partial overwrite should succeed");
    expected[block..(block * 2)].copy_from_slice(&overwrite);

    harness.fs.flush_pending().unwrap();

    let stored = harness
        .runtime
        .block_on(harness.metadata.get_inode(inode))
        .unwrap()
        .unwrap();
    let got = harness.fs.read_file_bytes(&stored).unwrap();
    assert_eq!(
        got, expected,
        "partial overwrite must not drop or overread the untouched tail"
    );
}

#[test]
fn small_overwrite_on_large_file_does_not_restage_full_file() {
    let dir = tempdir().unwrap();
    let block = 1024usize; // > inline_threshold (512 in test config)
    let harness = TestHarness::with_config(dir.path(), "small_overwrite.bin", 1 << 20, |cfg| {
        cfg.disable_journal = true;
        cfg.flush_interval_ms = 0;
    });

    let inode = harness.fs.allocate_inode_id().unwrap();
    let record = make_file(inode, "small-overwrite.bin");
    harness.fs.stage_file(record, Vec::new(), None).unwrap();

    // Build and flush a large committed file first.
    for i in 0..8u8 {
        let chunk = vec![i; block];
        harness
            .fs
            .op_write(inode, (i as usize * block) as u64, &chunk)
            .expect("prefill write should succeed");
    }
    harness.fs.flush_pending().unwrap();

    // Tiny overwrite at offset 0 should NOT materialize/restage whole file.
    let tiny = vec![0xFE; 16];
    harness
        .fs
        .op_write(inode, 0, &tiny)
        .expect("tiny overwrite should succeed");

    let active_arc = harness.fs.active_inodes.get(&inode).expect("active");
    let state = active_arc.lock();
    let pending = state.pending.as_ref().expect("inode should be pending after tiny overwrite");
    let staged_bytes = match pending.data.as_ref().expect("pending data should exist") {
        PendingData::Staged(segs) => segs.staged_bytes(),
        PendingData::Inline(_) => panic!("large file tiny overwrite must stay on segment path"),
    };
    drop(state);
    assert!(
        staged_bytes <= (tiny.len() as u64 + block as u64),
        "expected tiny overwrite to stage near-write-size data, got staged_bytes={}",
        staged_bytes
    );
}
