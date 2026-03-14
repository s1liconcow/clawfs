use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use clawfs::codec::{InlineCodecConfig, encode_inline_storage};
use clawfs::inode::{InodeKind, InodeRecord};
use clawfs::journal::{JournalEntry, JournalManager, JournalPayload};
use clawfs::metadata::MetadataStore;
use clawfs::perf_bench::{
    PerfStats, generate_incompressible_payload, make_file_record, make_large_directory_record,
    perf_config, summarize_samples,
};
use clawfs::segment::{SegmentEntry, SegmentManager, SegmentPayload};
use clawfs::superblock::SuperblockManager;
use criterion::{Criterion, PlottingBackend, Throughput};
use serde_json::json;
use tempfile::tempdir;
use tokio::runtime::Runtime;

#[derive(Clone, Copy)]
struct RunResult {
    duration: Duration,
    metric: f64,
}

#[derive(Default)]
struct GuardMetrics {
    by_metric: BTreeMap<String, PerfStats>,
}

static GUARD_METRICS: OnceLock<Mutex<GuardMetrics>> = OnceLock::new();

fn guard_metrics() -> &'static Mutex<GuardMetrics> {
    GUARD_METRICS.get_or_init(|| Mutex::new(GuardMetrics::default()))
}

fn perf_profile() -> String {
    std::env::var("CLAWFS_PERF_PROFILE").unwrap_or_else(|_| "fast".to_string())
}

fn configured_criterion() -> Criterion {
    let profile = perf_profile();
    let base = Criterion::default()
        .configure_from_args()
        .with_plots()
        .plotting_backend(PlottingBackend::Plotters);
    match profile.as_str() {
        "fast" => base
            .sample_size(20)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(13)),
        "thorough" => base
            .sample_size(40)
            .warm_up_time(Duration::from_secs(4))
            .measurement_time(Duration::from_secs(12)),
        _ => base
            .sample_size(24)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(4)),
    }
}

fn guard_sample_count() -> usize {
    match perf_profile().as_str() {
        "fast" => 3,
        "thorough" => 9,
        _ => 5,
    }
}

fn collect_guard_metric<F>(name: &str, mut run_once: F)
where
    F: FnMut() -> RunResult,
{
    let runs = guard_sample_count();
    let mut samples = Vec::with_capacity(runs);
    for _ in 0..runs {
        samples.push(run_once().metric);
    }
    let stats = summarize_samples(&samples);
    eprintln!(
        "[criterion-guard] {} runs={} min/median/mean/max={:.3}/{:.3}/{:.3}/{:.3} stddev={:.3}",
        name, runs, stats.min, stats.median, stats.mean, stats.max, stats.stddev
    );
    guard_metrics()
        .lock()
        .expect("guard metric mutex")
        .by_metric
        .insert(name.to_string(), stats);
}

fn write_guard_metrics_if_requested() {
    let Some(path) = std::env::var_os("CLAWFS_BENCH_METRICS_FILE") else {
        return;
    };
    let metrics = guard_metrics().lock().expect("guard metric mutex");
    let mut file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
    {
        Ok(f) => f,
        Err(_) => return,
    };
    for (metric, stats) in &metrics.by_metric {
        let rec = json!({
            "metric": metric,
            "value": stats.median,
            "stddev": stats.stddev,
            "min": stats.min,
            "max": stats.max,
            "mean": stats.mean,
            "n": guard_sample_count(),
        });
        let _ = writeln!(file, "{rec}");
    }
}

/// Simulates the hot path for a Linux untar workload: encode inline payloads
/// (LZ4 compression) then persist 5 000 inode records to the metadata store.
/// This captures both the CPU cost of inline encoding and the I/O cost of
/// writing shard + delta objects — the two phases that dominate untar flush
/// latency in practice.
fn run_untar_flush_latency_once() -> RunResult {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    std::fs::create_dir_all(root.join("store")).expect("create store");

    let shard_size = 2048u64;
    let delta_batch = 512usize;
    let n_inodes = 5_000usize;
    // Typical small source file: 4 KiB, highly compressible text.
    let file_payload = vec![0x61u8; 4096]; // 'a' × 4096

    let mut config = perf_config(&root);
    config.shard_size = shard_size;
    config.imap_delta_batch = delta_batch;

    let codec_config = InlineCodecConfig {
        compression: true,
        encryption_key: None,
    };

    let runtime = Runtime::new().expect("tokio runtime");
    let metadata = std::sync::Arc::new(
        runtime
            .block_on(MetadataStore::new(&config, runtime.handle().clone()))
            .expect("metadata init"),
    );
    let superblock = std::sync::Arc::new(
        runtime
            .block_on(SuperblockManager::load_or_init(
                metadata.clone(),
                shard_size,
            ))
            .expect("superblock init"),
    );

    // Pre-build inode records with encoded (compressed) inline storage, mirroring
    // what flush_pending does before calling persist_inodes_batch.
    let records: Vec<InodeRecord> = (2..=(n_inodes as u64 + 1))
        .map(|ino| {
            let mut rec = make_file_record(ino, 1, file_payload.clone());
            rec.storage =
                encode_inline_storage(&file_payload, &codec_config).expect("encode inline storage");
            rec
        })
        .collect();

    let start = Instant::now();
    let snapshot = superblock.prepare_dirty_generation().expect("prepare gen");
    let generation = snapshot.generation;
    runtime
        .block_on(metadata.persist_inodes_batch(&records, generation, shard_size, delta_batch))
        .expect("persist inode batch");
    runtime
        .block_on(metadata.sync_metadata_writes())
        .expect("sync metadata");
    runtime
        .block_on(superblock.commit_generation(generation))
        .expect("commit generation");
    let elapsed = start.elapsed();
    RunResult {
        duration: elapsed,
        metric: elapsed.as_secs_f64() * 1000.0,
    }
}

fn run_flush_metadata_batch_latency_once() -> RunResult {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    std::fs::create_dir_all(root.join("store")).expect("create store");

    let shard_size = 2048u64;
    let delta_batch = 64usize;
    let n_inodes = 5_000usize;

    let mut config = perf_config(&root);
    config.shard_size = shard_size;
    config.imap_delta_batch = delta_batch;

    let runtime = Runtime::new().expect("tokio runtime");
    let metadata = std::sync::Arc::new(
        runtime
            .block_on(MetadataStore::new(&config, runtime.handle().clone()))
            .expect("metadata init"),
    );
    let superblock = std::sync::Arc::new(
        runtime
            .block_on(SuperblockManager::load_or_init(
                metadata.clone(),
                shard_size,
            ))
            .expect("superblock init"),
    );

    let records: Vec<InodeRecord> = (2..=(n_inodes as u64 + 1))
        .map(|ino| make_file_record(ino, 1, vec![0x42u8; 512]))
        .collect();

    let start = Instant::now();
    let snapshot = superblock.prepare_dirty_generation().expect("prepare gen");
    let generation = snapshot.generation;
    runtime
        .block_on(metadata.persist_inodes_batch(&records, generation, shard_size, delta_batch))
        .expect("persist inode batch");
    runtime
        .block_on(metadata.sync_metadata_writes())
        .expect("sync metadata");
    runtime
        .block_on(superblock.commit_generation(generation))
        .expect("commit generation");
    let elapsed = start.elapsed();
    RunResult {
        duration: elapsed,
        metric: elapsed.as_secs_f64() * 1000.0,
    }
}

fn run_large_segment_staged_flush_latency_once() -> RunResult {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let mut config = perf_config(&root);
    config.segment_cache_bytes = 512 * 1024 * 1024;
    std::fs::create_dir_all(&config.store_path).expect("create store");
    std::fs::create_dir_all(&config.local_cache_path).expect("create cache");
    let runtime = Runtime::new().expect("tokio runtime");
    let segments = SegmentManager::new(&config, runtime.handle().clone()).expect("segment manager");

    let checkpoint_mb = 128usize;
    let data_size = checkpoint_mb * 1024 * 1024;
    let payload = generate_incompressible_payload(data_size);
    let chunk = segments.stage_payload(&payload).expect("stage payload");

    let start = Instant::now();
    segments
        .write_batch(
            1,
            1,
            vec![SegmentEntry {
                inode: 42,
                path: "/checkpoint.bin".into(),
                logical_offset: 0,
                payload: SegmentPayload::Staged(vec![chunk]),
            }],
        )
        .expect("write batch");
    let elapsed = start.elapsed();
    let throughput = data_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    RunResult {
        duration: elapsed,
        metric: throughput,
    }
}

fn run_journal_write_iops_once() -> RunResult {
    let temp = tempdir().expect("temp dir");
    let n = 5_000usize;
    let inline_data = vec![0x42u8; 512];
    let journal = JournalManager::new(temp.path()).expect("journal manager");
    let start = Instant::now();
    for i in 0..n {
        let inode = (i + 2) as u64;
        let record = make_file_record(inode, 1, inline_data.clone());
        let entry = JournalEntry {
            record,
            payload: JournalPayload::Inline(inline_data.clone()),
        };
        journal
            .persist_entry(&entry)
            .expect("persist journal entry");
    }
    let elapsed = start.elapsed();
    let iops = n as f64 / elapsed.as_secs_f64();
    RunResult {
        duration: elapsed,
        metric: iops,
    }
}

fn run_local_disk_stage_mib_per_s_once() -> RunResult {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let config = perf_config(&root);
    std::fs::create_dir_all(&config.mount_path).expect("create mount path");
    std::fs::create_dir_all(&config.store_path).expect("create store");
    std::fs::create_dir_all(&config.local_cache_path).expect("create cache");
    let runtime = Runtime::new().expect("tokio runtime");
    let segments = SegmentManager::new(&config, runtime.handle().clone()).expect("segment manager");
    let chunk_size = 1024 * 1024usize;
    let iterations = 40usize;
    let payload = vec![0x55u8; chunk_size];
    let start = Instant::now();
    for _ in 0..iterations {
        let chunk = segments.stage_payload(&payload).expect("stage payload");
        segments
            .release_staged_chunk(&chunk)
            .expect("release staged chunk");
    }
    let elapsed = start.elapsed();
    let throughput = (chunk_size * iterations) as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    RunResult {
        duration: elapsed,
        metric: throughput,
    }
}

fn run_local_segment_batch_mib_per_s_once() -> RunResult {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let config = perf_config(&root);
    std::fs::create_dir_all(&config.mount_path).expect("create mount path");
    std::fs::create_dir_all(&config.store_path).expect("create store");
    std::fs::create_dir_all(&config.local_cache_path).expect("create cache");
    let runtime = Runtime::new().expect("tokio runtime");
    let segments = SegmentManager::new(&config, runtime.handle().clone()).expect("segment manager");
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
                logical_offset: 0,
                payload: SegmentPayload::Bytes(vec![(inode & 0xff) as u8; entry_size]),
            });
        }
        segments
            .write_batch(1, (batch + 1) as u64, entries)
            .expect("write segment batch");
    }
    let elapsed = start.elapsed();
    let throughput = total_bytes / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    RunResult {
        duration: elapsed,
        metric: throughput,
    }
}

fn run_local_segment_small_file_iops_once() -> RunResult {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let config = perf_config(&root);
    std::fs::create_dir_all(&config.mount_path).expect("create mount path");
    std::fs::create_dir_all(&config.store_path).expect("create store");
    std::fs::create_dir_all(&config.local_cache_path).expect("create cache");
    let runtime = Runtime::new().expect("tokio runtime");
    let segments = SegmentManager::new(&config, runtime.handle().clone()).expect("segment manager");
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
                logical_offset: 0,
                payload: SegmentPayload::Bytes(vec![(inode & 0xff) as u8; file_size]),
            });
            inode = inode.saturating_add(1);
        }
        segments
            .write_batch(2, (batch + 1) as u64, entries)
            .expect("write small-file batch");
    }
    let elapsed = start.elapsed();
    let iops = files as f64 / elapsed.as_secs_f64();
    RunResult {
        duration: elapsed,
        metric: iops,
    }
}

fn run_segment_sequential_read_mib_per_s_once() -> RunResult {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let mut config = perf_config(&root);
    config.segment_cache_bytes = 512 * 1024 * 1024;
    std::fs::create_dir_all(&config.store_path).expect("create store");
    std::fs::create_dir_all(&config.local_cache_path).expect("create cache");
    let runtime = Runtime::new().expect("tokio runtime");
    let segments = SegmentManager::new(&config, runtime.handle().clone()).expect("segment manager");

    let data_size = 128 * 1024 * 1024usize;
    let read_chunk = 128 * 1024usize;
    let payload = vec![0u8; data_size];
    let chunk = segments.stage_payload(&payload).expect("stage payload");
    let pointers = segments
        .write_batch(
            1,
            1,
            vec![SegmentEntry {
                inode: 42,
                path: "/large.bin".into(),
                logical_offset: 0,
                payload: SegmentPayload::Staged(vec![chunk]),
            }],
        )
        .expect("write batch");

    let mut extents: Vec<_> = pointers.into_iter().map(|(_, extent)| extent).collect();
    extents.sort_by_key(|extent| extent.logical_offset);
    assert!(
        !extents.is_empty(),
        "segment write produced no extents for sequential read benchmark"
    );

    let mut extent_sizes = Vec::with_capacity(extents.len());
    let mut reconstructed = 0usize;
    for extent in &extents {
        let arc = segments
            .read_pointer_arc(&extent.pointer)
            .expect("read extent pointer");
        extent_sizes.push(arc.len());
        reconstructed = reconstructed.saturating_add(arc.len());
    }
    assert_eq!(
        reconstructed, data_size,
        "reconstructed extent bytes should match staged payload size"
    );

    let reads = data_size / read_chunk;
    let start = Instant::now();
    let mut extent_idx = 0usize;
    let mut extent_start = extents[0].logical_offset as usize;
    let mut extent_len = extent_sizes[0];
    for read_idx in 0..reads {
        let mut file_off = read_idx * read_chunk;
        let mut remaining = read_chunk;
        while remaining > 0 {
            while file_off >= extent_start + extent_len {
                extent_idx += 1;
                assert!(
                    extent_idx < extents.len(),
                    "sequential read offset exceeded extent coverage"
                );
                extent_start = extents[extent_idx].logical_offset as usize;
                extent_len = extent_sizes[extent_idx];
            }

            let arc = segments
                .read_pointer_arc(&extents[extent_idx].pointer)
                .expect("read extent pointer");
            assert_eq!(arc.len(), extent_len);
            let local_off = file_off - extent_start;
            let take = remaining.min(extent_len - local_off);
            let _slice = &arc[local_off..local_off + take];
            file_off += take;
            remaining -= take;
        }
    }
    let elapsed = start.elapsed();
    let throughput = (reads * read_chunk) as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    RunResult {
        duration: elapsed,
        metric: throughput,
    }
}

fn run_inode_clone_directory_heavy_clones_per_s_once() -> RunResult {
    let children = 8_000usize;
    let iterations = 4_000usize;
    let record = make_large_directory_record(children);
    let start = Instant::now();
    let mut observed = 0usize;
    for _ in 0..iterations {
        let cloned = record.clone();
        if let InodeKind::Directory { children } = cloned.kind {
            observed = observed.saturating_add(children.len());
        }
    }
    let elapsed = start.elapsed();
    assert!(observed > 0);
    RunResult {
        duration: elapsed,
        metric: iterations as f64 / elapsed.as_secs_f64(),
    }
}

fn run_inline_resize_strategy_speedup_once() -> RunResult {
    let iters = 200_000usize;
    let payload = vec![0xAB; 4096];
    let mut old_sum = 0usize;
    let mut new_sum = 0usize;

    let old_start = Instant::now();
    for i in 0..iters {
        let mut buf = vec![0u8; 16];
        let offset = 64 + (i % 96);
        if offset > buf.len() {
            buf.resize(offset, 0);
        }
        if offset + payload.len() > buf.len() {
            buf.resize(offset + payload.len(), 0);
        }
        buf[offset..offset + payload.len()].copy_from_slice(&payload);
        old_sum = old_sum.saturating_add(buf[offset] as usize);
    }
    let old_elapsed = old_start.elapsed();

    let new_start = Instant::now();
    for i in 0..iters {
        let mut buf = vec![0u8; 16];
        let offset = 64 + (i % 96);
        let end = offset + payload.len();
        if end > buf.len() {
            buf.resize(end, 0);
        }
        buf[offset..offset + payload.len()].copy_from_slice(&payload);
        new_sum = new_sum.saturating_add(buf[offset] as usize);
    }
    let new_elapsed = new_start.elapsed();
    assert_eq!(old_sum, new_sum);

    RunResult {
        duration: old_elapsed + new_elapsed,
        metric: old_elapsed.as_secs_f64() / new_elapsed.as_secs_f64(),
    }
}

fn bench_untar_flush_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_local");
    group.bench_function("untar_flush_latency", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_untar_flush_latency_once().duration;
            }
            total
        });
    });
    group.finish();
}

fn bench_flush_metadata_batch_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_local");
    group.bench_function("flush_metadata_batch_latency", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_flush_metadata_batch_latency_once().duration;
            }
            total
        });
    });
    group.finish();
}

fn bench_large_segment_staged_flush_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_local");
    group.throughput(Throughput::Bytes((128 * 1024 * 1024) as u64));
    group.bench_function("large_segment_staged_flush_latency", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_large_segment_staged_flush_latency_once().duration;
            }
            total
        });
    });
    group.finish();
}

fn bench_journal_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_local");
    group.throughput(Throughput::Elements(5_000));
    group.bench_function("journal_write_throughput", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_journal_write_iops_once().duration;
            }
            total
        });
    });
    group.finish();
}

fn bench_local_disk_stage_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_local");
    group.throughput(Throughput::Bytes((40 * 1024 * 1024) as u64));
    group.bench_function("local_disk_stage_throughput", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_local_disk_stage_mib_per_s_once().duration;
            }
            total
        });
    });
    group.finish();
}

fn bench_local_segment_batch_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_local");
    group.throughput(Throughput::Bytes((128 * 256 * 1024 * 4) as u64));
    group.bench_function("local_segment_batch_throughput", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_local_segment_batch_mib_per_s_once().duration;
            }
            total
        });
    });
    group.finish();
}

fn bench_local_segment_small_file_iops(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_local");
    group.throughput(Throughput::Elements(8_000));
    group.bench_function("local_segment_small_file_iops", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_local_segment_small_file_iops_once().duration;
            }
            total
        });
    });
    group.finish();
}

fn bench_segment_sequential_read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_local");
    group.throughput(Throughput::Bytes((128 * 1024 * 1024) as u64));
    group.bench_function("segment_sequential_read_throughput", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_segment_sequential_read_mib_per_s_once().duration;
            }
            total
        });
    });
    group.finish();
}

fn bench_inode_clone_directory_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_local");
    group.throughput(Throughput::Elements(4_000));
    group.bench_function("inode_clone_directory_heavy", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_inode_clone_directory_heavy_clones_per_s_once().duration;
            }
            total
        });
    });
    group.finish();
}

fn bench_inline_resize_strategy_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_local");
    group.throughput(Throughput::Elements(200_000));
    group.bench_function("inline_resize_strategy_benchmark", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_inline_resize_strategy_speedup_once().duration;
            }
            total
        });
    });
    group.finish();
}

fn collect_guard_metrics() {
    collect_guard_metric(
        "flush_metadata_batch_latency_ms",
        run_flush_metadata_batch_latency_once,
    );
    collect_guard_metric(
        "large_segment_staged_flush_mib_per_s",
        run_large_segment_staged_flush_latency_once,
    );
    collect_guard_metric("journal_write_iops", run_journal_write_iops_once);
    collect_guard_metric(
        "local_disk_stage_mib_per_s",
        run_local_disk_stage_mib_per_s_once,
    );
    collect_guard_metric(
        "local_segment_batch_mib_per_s",
        run_local_segment_batch_mib_per_s_once,
    );
    collect_guard_metric(
        "local_segment_small_file_iops",
        run_local_segment_small_file_iops_once,
    );
    collect_guard_metric(
        "segment_sequential_read_mib_per_s",
        run_segment_sequential_read_mib_per_s_once,
    );
    collect_guard_metric(
        "inode_clone_directory_heavy_clones_per_s",
        run_inode_clone_directory_heavy_clones_per_s_once,
    );
    collect_guard_metric(
        "inline_resize_strategy_speedup_x",
        run_inline_resize_strategy_speedup_once,
    );
    collect_guard_metric("untar_flush_latency_ms", run_untar_flush_latency_once);
}

fn main() {
    let mut criterion = configured_criterion();
    bench_flush_metadata_batch_latency(&mut criterion);
    bench_large_segment_staged_flush_latency(&mut criterion);
    bench_journal_write_throughput(&mut criterion);
    bench_local_disk_stage_throughput(&mut criterion);
    bench_local_segment_batch_throughput(&mut criterion);
    bench_local_segment_small_file_iops(&mut criterion);
    bench_segment_sequential_read_throughput(&mut criterion);
    bench_inode_clone_directory_heavy(&mut criterion);
    bench_inline_resize_strategy_benchmark(&mut criterion);
    bench_untar_flush_latency(&mut criterion);
    criterion.final_summary();

    collect_guard_metrics();
    write_guard_metrics_if_requested();
}
