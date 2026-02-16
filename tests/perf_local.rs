use std::path::PathBuf;
use std::time::Instant;

use osagefs::config::{Config, ObjectStoreProvider};
use osagefs::segment::{SegmentEntry, SegmentManager, SegmentPayload};
use tempfile::tempdir;
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
