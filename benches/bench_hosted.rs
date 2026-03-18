//! Benchmark suite for hosted accelerator modes.
//!
//! Compares performance across four configurations:
//!   1. Baseline direct mode — no accelerator
//!   2. DirectPlusCache with offline endpoint — measures advisory-miss overhead
//!   3. DirectPlusCache read warm path — in-process LRU cache hit after first fetch
//!   4. Metadata visibility via apply_external_deltas — coordination-assisted refresh
//!
//! Run with:
//!   cargo bench --bench bench_hosted
//!   cargo bench --bench bench_hosted -- --output-format bencher  # for CI
//!
//! Guard metrics are printed at the end and optionally written to
//! $CLAWFS_BENCH_METRICS_FILE (JSONL, one record per metric).
//!
//! # What each benchmark measures
//!
//! | Benchmark | Metric | Expected result |
//! |-----------|--------|----------------|
//! | close_latency_baseline | ms / 50 create+write+flush | Baseline |
//! | close_latency_direct_plus_cache | ms / 50 create+write+flush | ~= baseline (write path unaffected) |
//! | hot_segment_read_cold | MiB/s | Lower — no cache warm-up |
//! | hot_segment_read_warm | MiB/s | Higher — decoded LRU eliminates re-decode |
//! | small_file_metadata_baseline | files/s | Baseline |
//! | small_file_metadata_dpc | files/s | ~= baseline (advisory miss on read path) |
//! | metadata_visibility_lag | ms | Time for external delta apply to converge |

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use clawfs::clawfs::AcceleratorMode;
use clawfs::inode::ROOT_INODE;
use clawfs::metadata::MetadataStore;
use clawfs::perf_bench::{
    PerfStats, generate_incompressible_payload, make_file_record, perf_config, summarize_samples,
};
use clawfs::segment::{SegmentEntry, SegmentManager, SegmentPayload};
use clawfs::superblock::SuperblockManager;
use criterion::{Criterion, PlottingBackend, Throughput};
use tempfile::tempdir;
use tokio::runtime::Runtime;

// ── Guard metrics (non-criterion, always-on comparison) ───────────────────────

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

fn guard_sample_count() -> usize {
    match perf_profile().as_str() {
        "fast" => 3,
        "thorough" => 9,
        _ => 5,
    }
}

fn configured_criterion() -> Criterion {
    let profile = perf_profile();
    let base = Criterion::default()
        .configure_from_args()
        .with_plots()
        .plotting_backend(PlottingBackend::Plotters);
    match profile.as_str() {
        "fast" => base
            .sample_size(10)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(8)),
        "thorough" => base
            .sample_size(20)
            .warm_up_time(Duration::from_secs(3))
            .measurement_time(Duration::from_secs(12)),
        _ => base
            .sample_size(10)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(6)),
    }
}

#[derive(Clone, Copy)]
struct RunResult {
    duration: Duration,
    metric: f64,
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
        "[hosted-bench] {} runs={} min/median/mean/max={:.3}/{:.3}/{:.3}/{:.3} stddev={:.3}",
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
        let rec = serde_json::json!({
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

fn print_comparison_table() {
    let guard = guard_metrics().lock().expect("guard metric mutex");
    if guard.by_metric.is_empty() {
        return;
    }
    eprintln!("\n[hosted-bench] Comparison table (median values)");
    eprintln!("| Metric | Baseline | DPC (miss) | DPC (warm) | Units |");
    eprintln!("|--------|----------|------------|------------|-------|");

    let get = |key: &str| -> String {
        guard
            .by_metric
            .get(key)
            .map(|s| format!("{:.3}", s.median))
            .unwrap_or_else(|| "-".to_string())
    };

    eprintln!(
        "| close_latency | {} | {} | - | ms/50ops |",
        get("close_latency_baseline_ms"),
        get("close_latency_dpc_ms"),
    );
    eprintln!(
        "| hot_segment_read | {} | {} | {} | MiB/s |",
        get("hot_segment_read_cold_mib_per_s"),
        get("hot_segment_read_dpc_miss_mib_per_s"),
        get("hot_segment_read_warm_mib_per_s"),
    );
    eprintln!(
        "| small_file_meta | {} | {} | - | files/s |",
        get("small_file_metadata_baseline_files_per_s"),
        get("small_file_metadata_dpc_files_per_s"),
    );
    eprintln!(
        "| metadata_visibility | {} | - | - | ms |",
        get("metadata_visibility_lag_ms"),
    );
    eprintln!();
}

// ── Benchmark helpers ─────────────────────────────────────────────────────────

/// Port that quickly refuses connections — used as a stand-in offline
/// accelerator endpoint so we measure the advisory-miss overhead.
const OFFLINE_ENDPOINT: &str = "http://127.0.0.1:1";

fn dpc_config(root: &std::path::Path) -> clawfs::config::Config {
    let mut cfg = perf_config(root);
    cfg.accelerator_mode = Some(AcceleratorMode::DirectPlusCache);
    cfg.accelerator_endpoint = Some(OFFLINE_ENDPOINT.to_string());
    cfg
}

// ── Benchmark 1: Close/flush latency ─────────────────────────────────────────

/// Create N small files, write 4 KB, and flush to object store.
/// Measures the write-side pipeline: inode create → segment encode → shard write.
/// DirectPlusCache is not expected to change this path since the cache is
/// read-only.  Any difference represents measurement noise or advisory overhead.
fn run_close_latency_once(use_dpc: bool) -> RunResult {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    let config = if use_dpc {
        dpc_config(root)
    } else {
        perf_config(root)
    };
    std::fs::create_dir_all(&config.store_path).expect("create store");
    std::fs::create_dir_all(&config.local_cache_path).expect("create cache");

    let runtime = Runtime::new().expect("tokio runtime");
    let metadata = Arc::new(
        runtime
            .block_on(MetadataStore::new(&config, runtime.handle().clone()))
            .expect("metadata init"),
    );
    let superblock = Arc::new(
        runtime
            .block_on(SuperblockManager::load_or_init(
                metadata.clone(),
                config.shard_size,
            ))
            .expect("superblock init"),
    );

    let n = 50usize;
    let payload = vec![0x42u8; 4096];

    let start = Instant::now();
    for i in 0..n {
        let inode = (i + 2) as u64;
        let record = make_file_record(inode, ROOT_INODE, payload.clone());
        let snapshot = superblock.prepare_dirty_generation().expect("prepare gen");
        let generation = snapshot.generation;
        runtime
            .block_on(metadata.persist_inode(&record, generation, config.shard_size))
            .expect("persist inode");
        runtime
            .block_on(superblock.commit_generation(generation))
            .expect("commit gen");
    }
    let elapsed = start.elapsed();
    RunResult {
        duration: elapsed,
        metric: elapsed.as_secs_f64() * 1000.0, // ms
    }
}

fn bench_close_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("hosted_close_latency");
    group.throughput(Throughput::Elements(50));
    group.bench_function("baseline", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_close_latency_once(false).duration;
            }
            total
        });
    });
    group.bench_function("direct_plus_cache", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_close_latency_once(true).duration;
            }
            total
        });
    });
    group.finish();
}

// ── Benchmark 2: Hot segment read latency ────────────────────────────────────

/// Write a 1 MB segment, then read the same extent N times.
///
/// `warm`: the second and subsequent reads are served from the decoded in-process
///         LRU, eliminating re-decompression.
/// `cold`: the decoded LRU is bypassed by using a tiny (0-byte) budget.
/// `dpc_miss`: DirectPlusCache with offline endpoint — same as cold but adds
///             one failed TCP connect per read.
fn run_hot_segment_read_once(warm: bool, use_dpc: bool) -> RunResult {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    let mut config = if use_dpc {
        dpc_config(root)
    } else {
        perf_config(root)
    };
    // Cold path: set cache budget to 0 so LRU holds nothing.
    // Warm path: large budget (128 MiB) keeps all decoded extents.
    config.segment_cache_bytes = if warm { 128 * 1024 * 1024 } else { 0 };
    std::fs::create_dir_all(&config.store_path).expect("create store");
    std::fs::create_dir_all(&config.local_cache_path).expect("create cache");

    let runtime = Runtime::new().expect("tokio runtime");
    let segments = SegmentManager::new(&config, runtime.handle().clone()).expect("segment manager");

    // Write a 1 MiB segment.
    let payload = generate_incompressible_payload(1024 * 1024);
    let entry = SegmentEntry {
        inode: 42,
        path: "/bigfile".to_string(),
        logical_offset: 0,
        payload: SegmentPayload::Bytes(payload),
    };
    let pointers = segments
        .write_batch(1, 1, vec![entry])
        .expect("write batch");
    let (_, extent) = pointers.into_iter().next().expect("pointer");
    let pointer = clawfs::segment::SegmentPointer {
        segment_id: 1,
        generation: 1,
        offset: extent.pointer.offset,
        length: extent.pointer.length,
    };

    // Warm-up: one read to populate object-store disk cache.
    let _ = segments.read_pointer(&pointer).expect("warmup read");

    let iterations = 100usize;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = segments.read_pointer(&pointer).expect("read pointer");
    }
    let elapsed = start.elapsed();
    let bytes_read = 1024 * 1024 * iterations;
    let throughput = bytes_read as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    RunResult {
        duration: elapsed,
        metric: throughput,
    }
}

fn bench_hot_segment_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("hosted_hot_segment_read");
    group.throughput(Throughput::Bytes(1024 * 1024 * 100));
    group.bench_function("cold_baseline", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_hot_segment_read_once(false, false).duration;
            }
            total
        });
    });
    group.bench_function("warm_lru_cache", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_hot_segment_read_once(true, false).duration;
            }
            total
        });
    });
    group.bench_function("dpc_offline_miss", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_hot_segment_read_once(false, true).duration;
            }
            total
        });
    });
    group.finish();
}

// ── Benchmark 3: Small-file metadata workload ────────────────────────────────

/// Create 200 files (metadata only), commit, then stat each.
/// Measures metadata-path throughput (shard writes, in-process cache reads).
fn run_small_file_metadata_once(use_dpc: bool) -> RunResult {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    let config = if use_dpc {
        dpc_config(root)
    } else {
        perf_config(root)
    };
    std::fs::create_dir_all(&config.store_path).expect("create store");
    std::fs::create_dir_all(&config.local_cache_path).expect("create cache");

    let runtime = Runtime::new().expect("tokio runtime");
    let metadata = Arc::new(
        runtime
            .block_on(MetadataStore::new(&config, runtime.handle().clone()))
            .expect("metadata init"),
    );
    let superblock = Arc::new(
        runtime
            .block_on(SuperblockManager::load_or_init(
                metadata.clone(),
                config.shard_size,
            ))
            .expect("superblock init"),
    );

    let n = 200usize;
    let records: Vec<_> = (2..=(n as u64 + 1))
        .map(|ino| make_file_record(ino, ROOT_INODE, vec![0u8; 128]))
        .collect();

    let start = Instant::now();
    let snapshot = superblock.prepare_dirty_generation().expect("prepare gen");
    let generation = snapshot.generation;
    runtime
        .block_on(metadata.persist_inodes_batch(
            &records,
            generation,
            config.shard_size,
            config.imap_delta_batch,
        ))
        .expect("persist batch");
    runtime
        .block_on(metadata.sync_metadata_writes())
        .expect("sync");
    runtime
        .block_on(superblock.commit_generation(generation))
        .expect("commit gen");

    // Stat each inode (exercises in-process LRU or shard reload).
    for ino in 2..=(n as u64 + 1) {
        let _ = runtime
            .block_on(metadata.get_inode(ino))
            .expect("get inode");
    }
    let elapsed = start.elapsed();
    let files_per_s = n as f64 / elapsed.as_secs_f64();
    RunResult {
        duration: elapsed,
        metric: files_per_s,
    }
}

fn bench_small_file_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("hosted_small_file_metadata");
    group.throughput(Throughput::Elements(200));
    group.bench_function("baseline", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_small_file_metadata_once(false).duration;
            }
            total
        });
    });
    group.bench_function("direct_plus_cache", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_small_file_metadata_once(true).duration;
            }
            total
        });
    });
    group.finish();
}

// ── Benchmark 4: Metadata visibility lag ─────────────────────────────────────

/// Measures how long `apply_external_deltas` takes when a second MetadataStore
/// instance refreshes from the object store after a writer commits.
/// This is the baseline coordination path (no SSE, just polling).
fn run_metadata_visibility_lag_once() -> RunResult {
    let temp = tempdir().expect("tempdir");
    let config = perf_config(temp.path());
    std::fs::create_dir_all(&config.store_path).expect("create store");
    std::fs::create_dir_all(&config.local_cache_path).expect("create cache");

    let runtime = Runtime::new().expect("tokio runtime");

    // Writer: commits a generation with 50 records.
    let writer_meta = Arc::new(
        runtime
            .block_on(MetadataStore::new(&config, runtime.handle().clone()))
            .expect("writer meta"),
    );
    let writer_sb = Arc::new(
        runtime
            .block_on(SuperblockManager::load_or_init(
                writer_meta.clone(),
                config.shard_size,
            ))
            .expect("writer sb"),
    );
    let records: Vec<_> = (2..=51u64)
        .map(|ino| make_file_record(ino, ROOT_INODE, vec![0u8; 64]))
        .collect();
    let snapshot = writer_sb.prepare_dirty_generation().expect("prepare gen");
    let generation = snapshot.generation;
    runtime
        .block_on(writer_meta.persist_inodes_batch(
            &records,
            generation,
            config.shard_size,
            config.imap_delta_batch,
        ))
        .expect("writer persist");
    runtime
        .block_on(writer_meta.sync_metadata_writes())
        .expect("writer sync");
    runtime
        .block_on(writer_sb.commit_generation(generation))
        .expect("writer commit");

    // Reader: separate MetadataStore pointing at the same store path.
    let reader_meta = Arc::new(
        runtime
            .block_on(MetadataStore::new(&config, runtime.handle().clone()))
            .expect("reader meta"),
    );

    // Measure how long it takes the reader to pick up the committed generation.
    let start = Instant::now();
    let refreshed = runtime
        .block_on(async {
            let meta = reader_meta.clone();
            tokio::task::spawn_blocking(move || meta.apply_external_deltas()).await
        })
        .expect("join")
        .expect("apply_external_deltas");
    let elapsed = start.elapsed();

    eprintln!(
        "[hosted-bench] metadata_visibility records_refreshed={} elapsed={:.2}ms",
        refreshed.len(),
        elapsed.as_secs_f64() * 1000.0
    );
    RunResult {
        duration: elapsed,
        metric: elapsed.as_secs_f64() * 1000.0, // ms
    }
}

fn bench_metadata_visibility_lag(c: &mut Criterion) {
    let mut group = c.benchmark_group("hosted_metadata_visibility");
    group.bench_function("apply_external_deltas", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += run_metadata_visibility_lag_once().duration;
            }
            total
        });
    });
    group.finish();
}

// ── Guard metrics collection ──────────────────────────────────────────────────

fn collect_guard_metrics() {
    collect_guard_metric("close_latency_baseline_ms", || {
        run_close_latency_once(false)
    });
    collect_guard_metric("close_latency_dpc_ms", || run_close_latency_once(true));
    collect_guard_metric("hot_segment_read_cold_mib_per_s", || {
        run_hot_segment_read_once(false, false)
    });
    collect_guard_metric("hot_segment_read_warm_mib_per_s", || {
        run_hot_segment_read_once(true, false)
    });
    collect_guard_metric("hot_segment_read_dpc_miss_mib_per_s", || {
        run_hot_segment_read_once(false, true)
    });
    collect_guard_metric("small_file_metadata_baseline_files_per_s", || {
        run_small_file_metadata_once(false)
    });
    collect_guard_metric("small_file_metadata_dpc_files_per_s", || {
        run_small_file_metadata_once(true)
    });
    collect_guard_metric(
        "metadata_visibility_lag_ms",
        run_metadata_visibility_lag_once,
    );
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() {
    let mut criterion = configured_criterion();
    bench_close_latency(&mut criterion);
    bench_hot_segment_read(&mut criterion);
    bench_small_file_metadata(&mut criterion);
    bench_metadata_visibility_lag(&mut criterion);
    criterion.final_summary();

    collect_guard_metrics();
    print_comparison_table();
    write_guard_metrics_if_requested();
}
