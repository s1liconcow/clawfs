use std::path::PathBuf;
use std::time::Instant;

use osagefs::config::{Config, ObjectStoreProvider};
use osagefs::segment::SegmentManager;
use tempfile::tempdir;
use tokio::runtime::Runtime;

fn perf_config(root: &PathBuf) -> Config {
    Config {
        mount_path: root.join("mnt"),
        store_path: root.join("store"),
        local_cache_path: root.join("cache"),
        inline_threshold: 512,
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
        perf_log: None,
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
    if std::env::var("OSAGEFS_RUN_PERF").is_err() {
        eprintln!("skipping local_disk_stage_throughput; set OSAGEFS_RUN_PERF=1 to enable");
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
