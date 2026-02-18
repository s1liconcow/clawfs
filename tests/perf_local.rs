use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use osagefs::config::{Config, ObjectStoreProvider};
use osagefs::inode::{FileStorage, InodeKind, InodeRecord};
use osagefs::journal::{JournalEntry, JournalManager, JournalPayload};
use osagefs::metadata::MetadataStore;
use osagefs::segment::{SegmentEntry, SegmentManager, SegmentPayload};
use osagefs::superblock::SuperblockManager;
use tempfile::tempdir;
use time::OffsetDateTime;
use tokio::runtime::Runtime;

fn perf_config(root: &PathBuf) -> Config {
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
        inode_batch: 16,
        segment_batch: 32,
        pending_bytes: 128 * 1024 * 1024,
        home_prefix: "/home".into(),
        object_provider: ObjectStoreProvider::Local,
        bucket: None,
        region: None,
        endpoint: None,
        object_prefix: String::new(),
        gcs_service_account: None,
        state_path: root.join("state.bin"),
        allow_other: false,
        perf_log: None,
        replay_log: None,
        disable_journal: true,
        fsync_on_close: false,
        flush_interval_ms: 0,
        disable_cleanup: true,
        lookup_cache_ttl_ms: 0,
        dir_cache_ttl_ms: 0,
        metadata_poll_interval_ms: 0,
        segment_cache_bytes: 0,
        foreground: false,
        log_file: None,
        debug_log: false,
        imap_delta_batch: 16,
        log_storage_io: false,
    }
}

fn make_file_record(inode: u64, parent: u64, data: Vec<u8>) -> InodeRecord {
    InodeRecord {
        inode,
        parent,
        name: format!("f{inode}"),
        path: format!("/f{inode}"),
        kind: InodeKind::File,
        size: data.len() as u64,
        mode: 0o100644,
        uid: 1000,
        gid: 1000,
        atime: OffsetDateTime::now_utc(),
        mtime: OffsetDateTime::now_utc(),
        ctime: OffsetDateTime::now_utc(),
        link_count: 1,
        rdev: 0,
        storage: FileStorage::Inline(data),
    }
}

// ---------------------------------------------------------------------------
// CUJ: small-file burst flush (dev_smallfile_burst)
//
// Measures how long it takes to persist a batch of N inline inodes through
// MetadataStore + superblock CAS — the full metadata flush path that runs
// once per flush cycle during a 5 000-file burst.
// Before opt #1 (deferred dir fsync): O(N_shards + N_deltas) individual
// fsyncs.  After: 2 directory fsyncs total.
// ---------------------------------------------------------------------------
#[test]
fn flush_metadata_batch_latency() {
    if !perf_enabled("flush_metadata_batch_latency") {
        return;
    }
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    std::fs::create_dir_all(root.join("store")).unwrap();

    // Use realistic production defaults: shard_size=2048, delta_batch=64
    let shard_size = 2048u64;
    let delta_batch = 64usize;
    let n_inodes = 5_000usize;

    let mut config = perf_config(&root);

    config.shard_size = 2048;
    config.imap_delta_batch = 64;

    let runtime = Runtime::new().unwrap();
    let metadata = Arc::new(
        runtime
            .block_on(MetadataStore::new(&config, runtime.handle().clone()))
            .unwrap(),
    );
    let superblock = Arc::new(
        runtime
            .block_on(SuperblockManager::load_or_init(metadata.clone(), shard_size))
            .unwrap(),
    );

    let records: Vec<InodeRecord> = (2..=(n_inodes as u64 + 1))
        .map(|ino| make_file_record(ino, 1, vec![0x42u8; 512]))
        .collect();

    let start = Instant::now();

    let snapshot = superblock.prepare_dirty_generation().unwrap();
    let generation = snapshot.generation;
    runtime
        .block_on(metadata.persist_inodes_batch(&records, generation, shard_size, delta_batch))
        .unwrap();
    runtime
        .block_on(metadata.sync_metadata_writes())
        .unwrap();
    runtime
        .block_on(superblock.commit_generation(generation))
        .unwrap();

    let elapsed = start.elapsed();
    let ms = elapsed.as_secs_f64() * 1000.0;
    eprintln!(
        "flush_metadata_batch_latency n={} delta_batch={} elapsed={:.1}ms",
        n_inodes, delta_batch, ms
    );

    // With deferred dir-fsync this should complete in well under 500 ms on any
    // reasonable disk.  We set a generous bound so CI doesn't flap.
    assert!(
        ms < 5_000.0,
        "metadata batch flush took {:.1}ms — should be < 5000ms",
        ms
    );
}

// ---------------------------------------------------------------------------
// CUJ: checkpoint write + fsync (ai_checkpoint_loop)
//
// Measures end-to-end latency of staging 128 MiB of data and flushing it as
// a single segment, including the cache write.
// Before opt #2 (async cache): write_batch writes 128 MiB to cache AND store
// synchronously.  After: cache write is async/background.
// Before opt #3 (compression sampling): LZ4 is attempted on all 128 MiB.
// After: incompressible data detected from a small probe and skipped.
// ---------------------------------------------------------------------------
#[test]
fn large_segment_staged_flush_latency() {
    if !perf_enabled("large_segment_staged_flush_latency") {
        return;
    }
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let mut config = perf_config(&root);
    // Enable a realistic segment cache so the cache write path is exercised.
    config.segment_cache_bytes = 512 * 1024 * 1024;
    std::fs::create_dir_all(&config.store_path).unwrap();
    std::fs::create_dir_all(&config.local_cache_path).unwrap();
    let runtime = Runtime::new().unwrap();
    let segments = SegmentManager::new(&config, runtime.handle().clone()).unwrap();

    let checkpoint_mb = 128usize;
    let data_size = checkpoint_mb * 1024 * 1024;
    // Use pseudo-random-looking bytes (incompressible) to simulate an ML checkpoint.
    let payload: Vec<u8> = (0..data_size)
        .map(|i: usize| {
            i.wrapping_mul(6364136223846793005_usize)
                .wrapping_add(1442695040888963407_usize)
                .wrapping_shr(56) as u8
        })
        .collect();

    // Stage the payload (simulates FUSE writes landing in the staging area)
    let chunk = segments.stage_payload(&payload).unwrap();

    let start = Instant::now();
    segments
        .write_batch(
            1,
            1,
            vec![SegmentEntry {
                inode: 42,
                path: "/checkpoint.bin".into(),
                payload: SegmentPayload::Staged(vec![chunk]),
            }],
        )
        .unwrap();
    let elapsed = start.elapsed();
    let ms = elapsed.as_secs_f64() * 1000.0;
    let throughput = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    eprintln!(
        "large_segment_staged_flush_latency size={}MiB elapsed={:.1}ms throughput={:.1}MiB/s",
        checkpoint_mb, ms, throughput
    );

    // After async cache + compression-skip the flush should be essentially
    // one local-FS write at disk speed (200+ MiB/s).  Set a generous floor.
    assert!(
        throughput >= 50.0,
        "large staged flush only {:.1} MiB/s (need >= 50 MiB/s)",
        throughput
    );
}

// ---------------------------------------------------------------------------
// CUJ: journal write throughput (dev_smallfile_burst, dev_incremental_build)
//
// Measures how fast persist_entry runs for N inline-payload entries.
// Before opt #4 (WAL): each call creates a NamedTempFile + rename.
// After: each call appends to a single buffered WAL file.
// ---------------------------------------------------------------------------
#[test]
fn journal_write_throughput() {
    if !perf_enabled("journal_write_throughput") {
        return;
    }
    let temp = tempdir().expect("temp dir");
    let n = 5_000usize;
    let journal = JournalManager::new(temp.path()).unwrap();
    let inline_data = vec![0x42u8; 512];

    let start = Instant::now();
    for i in 0..n {
        let inode = (i + 2) as u64;
        let record = make_file_record(inode, 1, inline_data.clone());
        let entry = JournalEntry {
            record,
            payload: JournalPayload::Inline(inline_data.clone()),
        };
        journal.persist_entry(&entry).unwrap();
    }
    let elapsed = start.elapsed();
    let iops = n as f64 / elapsed.as_secs_f64();
    let ms = elapsed.as_secs_f64() * 1000.0;
    eprintln!(
        "journal_write_throughput n={} elapsed={:.1}ms iops={:.0}",
        n, ms, iops
    );

    // After WAL the journal should sustain well above 10 000 entries/s.
    assert!(
        iops >= 5_000.0,
        "journal write iops only {:.0} (need >= 5000)",
        iops
    );
}

#[test]
fn local_disk_stage_throughput() {
    if !perf_enabled("local_disk_stage_throughput") {
        return;
    }
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let config = perf_config(&root);
    std::fs::create_dir_all(&config.mount_path).unwrap();
    std::fs::create_dir_all(&config.store_path).unwrap();
    std::fs::create_dir_all(&config.local_cache_path).unwrap();
    let runtime = Runtime::new().unwrap();
    let segments = SegmentManager::new(&config, runtime.handle().clone()).unwrap();
    let chunk_size = 1 * 1024 * 1024; // 1 MiB writes
    let iterations = 40; // 40 MiB total
    let payload = vec![0x55u8; chunk_size];
    let start = Instant::now();
    for _ in 0..iterations {
        let chunk = segments.stage_payload(&payload).unwrap();
        segments.release_staged_chunk(&chunk).unwrap();
    }
    let elapsed = start.elapsed();
    let throughput = (chunk_size * iterations) as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    assert!(
        throughput >= 10.0,
        "local disk throughput only {:.2} MiB/s (need >= 10 MiB/s)",
        throughput
    );
}

#[test]
fn local_segment_batch_throughput() {
    if !perf_enabled("local_segment_batch_throughput") {
        return;
    }
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let config = perf_config(&root);
    std::fs::create_dir_all(&config.mount_path).unwrap();
    std::fs::create_dir_all(&config.store_path).unwrap();
    std::fs::create_dir_all(&config.local_cache_path).unwrap();
    let runtime = Runtime::new().unwrap();
    let segments = SegmentManager::new(&config, runtime.handle().clone()).unwrap();
    let entries_per_batch = 128usize;
    let entry_size = 256 * 1024usize;
    let batches = 4usize;
    let total_bytes = (entries_per_batch * entry_size * batches) as f64;
    let start = Instant::now();
    for batch in 0..batches {
        let mut entries = Vec::with_capacity(entries_per_batch);
        for i in 0..entries_per_batch {
            let inode = (batch * entries_per_batch + i + 1) as u64;
            entries.push(SegmentEntry {
                inode,
                path: format!("/bulk_{inode}.bin"),
                payload: SegmentPayload::Bytes(vec![(inode & 0xff) as u8; entry_size]),
            });
        }
        segments
            .write_batch(1, (batch + 1) as u64, entries)
            .unwrap();
    }
    let elapsed = start.elapsed();
    let throughput_mib = total_bytes / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    eprintln!(
        "local_segment_batch_throughput throughput={:.2} MiB/s elapsed={:?}",
        throughput_mib, elapsed
    );
    assert!(
        throughput_mib >= 80.0,
        "segment batch throughput only {:.2} MiB/s (need >= 80 MiB/s)",
        throughput_mib
    );
}

#[test]
fn local_segment_small_file_iops() {
    if !perf_enabled("local_segment_small_file_iops") {
        return;
    }
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let config = perf_config(&root);
    std::fs::create_dir_all(&config.mount_path).unwrap();
    std::fs::create_dir_all(&config.store_path).unwrap();
    std::fs::create_dir_all(&config.local_cache_path).unwrap();
    let runtime = Runtime::new().unwrap();
    let segments = SegmentManager::new(&config, runtime.handle().clone()).unwrap();
    let files = 8_000usize;
    let batch_size = 500usize;
    let file_size = 1024usize;
    let mut inode = 1u64;
    let start = Instant::now();
    for batch in 0..(files / batch_size) {
        let mut entries = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            entries.push(SegmentEntry {
                inode,
                path: format!("/small_{inode}.txt"),
                payload: SegmentPayload::Bytes(vec![(inode & 0xff) as u8; file_size]),
            });
            inode = inode.saturating_add(1);
        }
        segments
            .write_batch(2, (batch + 1) as u64, entries)
            .expect("write small batch");
    }
    let elapsed = start.elapsed();
    let iops = files as f64 / elapsed.as_secs_f64();
    eprintln!(
        "local_segment_small_file_iops files={} iops={:.0} elapsed={:?}",
        files, iops, elapsed
    );
    assert!(
        iops >= 20_000.0,
        "small-file iops only {:.0} ops/s (need >= 20000)",
        iops
    );
}

fn perf_enabled(name: &str) -> bool {
    if std::env::var("OSAGEFS_RUN_PERF").is_ok() {
        return true;
    }
    eprintln!("skipping {name}; set OSAGEFS_RUN_PERF=1 to enable");
    false
}
