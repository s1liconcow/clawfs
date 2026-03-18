use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path as AxumPath, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use clawfs::clawfs::AcceleratorMode;
use clawfs::config::Config;
use clawfs::fs::OsageFs;
use clawfs::hosted_cache::{CachedDirEntry, CachedMetadataEntry};
use clawfs::maintenance::{self, CompactionConfig};
use clawfs::metadata::{MetadataStore, create_object_store};
use clawfs::perf::PerfLogger;
use clawfs::perf_bench::{
    generate_incompressible_payload, perf_config, perf_osagefs_with_config,
    perf_runtime_components, summarize_samples,
};
use clawfs::relay::{DedupStore, RelayOutagePolicy, RelayWriteRequest};
use clawfs::replay::ReplayLogger;
use clawfs::segment::{SegmentManager, segment_prefix};
use clawfs::superblock::SuperblockManager;
use clawfs::{compat, inode::ROOT_INODE};
use serde::Serialize;
use serde_json::json;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::time::sleep;

const DEFAULT_SIMULATED_RTT_MS: u64 = 8;
const DEFAULT_SAMPLE_COUNT: usize = 5;
const HOT_READ_REPEATS: usize = 100;
const SMALL_FILE_COUNT: usize = 1_000;
const HOT_READ_LEN: usize = 1024 * 1024;
const CLOSE_WRITE_LEN: usize = 4 * 1024;
const MAINTENANCE_FILE_COUNT: usize = 8;
const MAINTENANCE_FILE_SIZE: usize = 1024 * 1024;
const CACHE_VARIANT_ON_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchMode {
    Direct,
    HostedMaintenanceOnly,
    DirectPlusCache,
    RelayWrite,
}

impl BenchMode {
    fn label(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::HostedMaintenanceOnly => "hosted_maintenance_only",
            Self::DirectPlusCache => "direct_plus_cache",
            Self::RelayWrite => "relay_write",
        }
    }

    fn expectation(self) -> &'static str {
        match self {
            Self::Direct => "baseline direct path for comparison",
            Self::HostedMaintenanceOnly => {
                "foreground path should stay near baseline while maintenance bytes shift off the client"
            }
            Self::DirectPlusCache => {
                "metadata visibility lag and hot-read p99 should shrink when cache hits land"
            }
            Self::RelayWrite => "close/fsync should collapse toward one relay round trip",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CacheVariant {
    Off,
    On,
}

impl CacheVariant {
    fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::On => "on",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct BenchRow {
    workload: String,
    mode: String,
    cache_variant: String,
    p50_ms: f64,
    p99_ms: f64,
    min_ms: f64,
    mean_ms: f64,
    max_ms: f64,
    stddev_ms: f64,
    throughput: Option<f64>,
    throughput_unit: Option<String>,
    bytes_transferred: u64,
    expectation: String,
    assessment: String,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkReport {
    profile: String,
    simulated_rtt_ms: u64,
    sample_count: usize,
    rows: Vec<BenchRow>,
}

#[derive(Default)]
struct CacheState {
    inodes: parking_lot::RwLock<BTreeMap<u64, CachedMetadataEntry>>,
    directories: parking_lot::RwLock<BTreeMap<u64, Vec<CachedDirEntry>>>,
    segments: parking_lot::RwLock<BTreeMap<(u64, u64), Vec<u8>>>,
}

impl CacheState {
    fn insert_inode(&self, entry: CachedMetadataEntry) {
        self.inodes.write().insert(entry.inode, entry);
    }

    fn insert_directory(&self, parent_inode: u64, entries: Vec<CachedDirEntry>) {
        self.directories.write().insert(parent_inode, entries);
    }

    fn insert_segment(&self, generation: u64, segment_id: u64, bytes: Vec<u8>) {
        self.segments
            .write()
            .insert((generation, segment_id), bytes);
    }

    fn get_inode(&self, inode: u64) -> Option<CachedMetadataEntry> {
        self.inodes.read().get(&inode).cloned()
    }

    fn get_directory(&self, parent_inode: u64) -> Option<Vec<CachedDirEntry>> {
        self.directories.read().get(&parent_inode).cloned()
    }

    fn get_segment(&self, generation: u64, segment_id: u64) -> Option<Vec<u8>> {
        self.segments.read().get(&(generation, segment_id)).cloned()
    }
}

#[derive(Clone)]
struct RelayState {
    metadata: Arc<MetadataStore>,
    segments: Arc<SegmentManager>,
    superblock: Arc<SuperblockManager>,
    dedup: Arc<DedupStore>,
    shard_size: u64,
}

#[derive(Clone)]
struct SampleOutcome {
    duration: Duration,
    bytes_transferred: u64,
    throughput_units: f64,
    latency_samples: Option<Vec<f64>>,
}

#[derive(Clone, Copy)]
struct WorkloadSpec<'a> {
    mode: BenchMode,
    cache_variant: CacheVariant,
    workload: &'a str,
    expectation: &'a str,
    throughput_unit: Option<&'a str>,
}

fn bench_profile() -> String {
    std::env::var("CLAWFS_BENCH_PROFILE").unwrap_or_else(|_| "fast".to_string())
}

fn sample_count() -> usize {
    match bench_profile().as_str() {
        "fast" => DEFAULT_SAMPLE_COUNT,
        "thorough" => 9,
        other if other.starts_with("n=") => other[2..].parse().unwrap_or(DEFAULT_SAMPLE_COUNT),
        _ => DEFAULT_SAMPLE_COUNT,
    }
}

fn simulated_rtt() -> Duration {
    let ms = std::env::var("CLAWFS_BENCH_SIM_RTT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SIMULATED_RTT_MS);
    Duration::from_millis(ms.max(1))
}

fn perf_log_path() -> PathBuf {
    std::env::var_os("CLAWFS_BENCH_PERF_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/bench-hosted/perf.jsonl"))
}

fn replay_log_path() -> PathBuf {
    std::env::var_os("CLAWFS_BENCH_REPLAY_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/bench-hosted/replay.jsonl.gz"))
}

fn report_json_path() -> Option<PathBuf> {
    std::env::var_os("CLAWFS_BENCH_REPORT_JSON").map(PathBuf::from)
}

fn report_md_path() -> Option<PathBuf> {
    std::env::var_os("CLAWFS_BENCH_REPORT_MD").map(PathBuf::from)
}

fn scale_duration(base: Duration, factor: u32) -> Duration {
    Duration::from_millis(base.as_millis() as u64 * factor as u64)
}

fn sleep_for(runtime: &Runtime, duration: Duration) {
    runtime.block_on(async { sleep(duration).await });
}

fn percentile(samples: &[f64], percentile: f64) -> f64 {
    assert!(!samples.is_empty(), "need at least one sample");
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = percentile.clamp(0.0, 1.0) * (sorted.len().saturating_sub(1) as f64);
    let low = rank.floor() as usize;
    let high = rank.ceil() as usize;
    if low == high {
        sorted[low]
    } else {
        let weight = rank - low as f64;
        sorted[low] + (sorted[high] - sorted[low]) * weight
    }
}

fn summarize_latency(samples: &[f64]) -> (f64, f64, f64, f64, f64, f64) {
    let stats = summarize_samples(samples);
    let p50 = stats.median;
    let p99 = percentile(samples, 0.99);
    (p50, p99, stats.min, stats.mean, stats.max, stats.stddev)
}

fn benchmark_config(
    root: &Path,
    mode: BenchMode,
    accelerator_endpoint: Option<String>,
    segment_cache_bytes: u64,
) -> Config {
    let mut config = perf_config(root);
    config.disable_journal = false;
    config.disable_cleanup = matches!(
        mode,
        BenchMode::HostedMaintenanceOnly | BenchMode::DirectPlusCache | BenchMode::RelayWrite
    );
    config.segment_cache_bytes = segment_cache_bytes;
    config.accelerator_mode = match mode {
        BenchMode::Direct => None,
        BenchMode::HostedMaintenanceOnly => Some(AcceleratorMode::Direct),
        BenchMode::DirectPlusCache => Some(AcceleratorMode::DirectPlusCache),
        BenchMode::RelayWrite => Some(AcceleratorMode::RelayWrite),
    };
    config.accelerator_endpoint = accelerator_endpoint;
    config.accelerator_fallback_policy = Some(match mode {
        BenchMode::RelayWrite => clawfs::clawfs::AcceleratorFallbackPolicy::FailClosed,
        _ => clawfs::clawfs::AcceleratorFallbackPolicy::PollAndDirect,
    });
    config.relay_fallback_policy = Some(match mode {
        BenchMode::RelayWrite => RelayOutagePolicy::FailClosed,
        _ => RelayOutagePolicy::FailClosed,
    });
    config
}

fn build_fs(config: Config) -> (Runtime, OsageFs) {
    perf_osagefs_with_config(config)
}

fn build_fs_with_parts(
    config: Config,
) -> (
    Runtime,
    Arc<MetadataStore>,
    Arc<SuperblockManager>,
    Arc<SegmentManager>,
    Arc<clawfs::state::ClientStateManager>,
    OsageFs,
) {
    let (runtime, metadata, superblock, segments, client_state) =
        perf_runtime_components(config.clone());
    let fs = OsageFs::new(
        config,
        metadata.clone(),
        superblock.clone(),
        segments.clone(),
        None,
        None,
        runtime.handle().clone(),
        client_state.clone(),
        None,
        None,
        None,
        None,
        None,
    );
    (runtime, metadata, superblock, segments, client_state, fs)
}

fn start_cache_server(runtime: &Runtime, state: Arc<CacheState>) -> String {
    let listener = runtime
        .block_on(async { TcpListener::bind("127.0.0.1:0").await })
        .expect("bind cache");
    let addr = listener.local_addr().expect("cache addr");
    let app = Router::new()
        .route(
            "/inode/{inode}",
            get({
                let state = state.clone();
                move |AxumPath(inode): AxumPath<u64>| {
                    let state = state.clone();
                    async move {
                        match state.get_inode(inode) {
                            Some(entry) => Json(entry).into_response(),
                            None => axum::http::StatusCode::NOT_FOUND.into_response(),
                        }
                    }
                }
            }),
        )
        .route(
            "/readdir/{parent}",
            get({
                let state = state.clone();
                move |AxumPath(parent): AxumPath<u64>| {
                    let state = state.clone();
                    async move {
                        match state.get_directory(parent) {
                            Some(entries) => Json(entries).into_response(),
                            None => axum::http::StatusCode::NOT_FOUND.into_response(),
                        }
                    }
                }
            }),
        )
        .route(
            "/segment/{generation}/{segment_id}/{range}",
            get({
                let state = state.clone();
                move |AxumPath((generation, segment_id, range)): AxumPath<(u64, u64, String)>| {
                    let state = state.clone();
                    async move {
                        let Some((offset, length)) = range.split_once('+') else {
                            return axum::http::StatusCode::BAD_REQUEST.into_response();
                        };
                        let Ok(offset) = offset.parse::<usize>() else {
                            return axum::http::StatusCode::BAD_REQUEST.into_response();
                        };
                        let Ok(length) = length.parse::<usize>() else {
                            return axum::http::StatusCode::BAD_REQUEST.into_response();
                        };
                        let Some(bytes) = state.get_segment(generation, segment_id) else {
                            return axum::http::StatusCode::NOT_FOUND.into_response();
                        };
                        let Some(end) = offset.checked_add(length) else {
                            return axum::http::StatusCode::RANGE_NOT_SATISFIABLE.into_response();
                        };
                        if end > bytes.len() {
                            return axum::http::StatusCode::RANGE_NOT_SATISFIABLE.into_response();
                        }
                        bytes[offset..end].to_vec().into_response()
                    }
                }
            }),
        );
    runtime.handle().spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn handle_relay_write(
    State(state): State<RelayState>,
    Json(request): Json<RelayWriteRequest>,
) -> axum::response::Response {
    match clawfs::relay::relay_commit_pipeline(
        &request,
        &state.metadata,
        &state.segments,
        &state.superblock,
        &state.dedup,
        state.shard_size,
    )
    .await
    {
        Ok(response) => {
            let status = match response.status {
                clawfs::relay::RelayStatus::Failed => axum::http::StatusCode::CONFLICT,
                _ => axum::http::StatusCode::OK,
            };
            (status, Json(response)).into_response()
        }
        Err(err) => {
            let response = clawfs::relay::RelayWriteResponse {
                status: clawfs::relay::RelayStatus::Failed,
                committed_generation: None,
                idempotency_key: request.idempotency_key,
                error: Some(format!("internal error: {err}")),
                actual_generation: None,
            };
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(response),
            )
                .into_response()
        }
    }
}

async fn handle_health(State(state): State<RelayState>) -> axum::response::Response {
    let generation = state.superblock.snapshot().generation;
    Json(json!({
        "status": "ok",
        "generation": generation,
        "dedup_entries": state.dedup.len(),
    }))
    .into_response()
}

fn start_relay_server(runtime: &Runtime, config: &Config) -> String {
    let (metadata, segments, superblock, listener, addr) = runtime.block_on(async {
        let (store, meta_prefix) = create_object_store(config).expect("relay object store");
        let seg_prefix = segment_prefix(&config.object_prefix);
        let handle = tokio::runtime::Handle::current();
        let metadata = Arc::new(
            MetadataStore::new_with_store(
                store.clone(),
                meta_prefix.clone(),
                config,
                handle.clone(),
            )
            .await
            .expect("relay metadata"),
        );
        let segments = Arc::new(
            SegmentManager::new_with_store(
                store.clone(),
                seg_prefix.clone(),
                config,
                handle.clone(),
            )
            .expect("relay segments"),
        );
        let superblock = Arc::new(
            SuperblockManager::load_or_init(metadata.clone(), config.shard_size)
                .await
                .expect("relay superblock"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind relay");
        let addr = listener.local_addr().expect("relay addr");
        (metadata, segments, superblock, listener, addr)
    });
    let state = RelayState {
        metadata,
        segments,
        superblock,
        dedup: DedupStore::new(Duration::from_secs(86_400)),
        shard_size: config.shard_size,
    };
    let app = Router::new()
        .route("/relay_write", post(handle_relay_write))
        .route("/health", get(handle_health))
        .with_state(state);
    runtime.handle().spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

fn log_sample(
    perf: &PerfLogger,
    replay: Option<&ReplayLogger>,
    workload: &str,
    mode: BenchMode,
    cache_variant: CacheVariant,
    sample_index: usize,
    outcome: &SampleOutcome,
) {
    let details = json!({
        "workload": workload,
        "mode": mode.label(),
        "cache_variant": cache_variant.label(),
        "sample_index": sample_index,
        "duration_ms": outcome.duration.as_secs_f64() * 1000.0,
        "bytes_transferred": outcome.bytes_transferred,
        "throughput_units": outcome.throughput_units,
    });
    perf.log("hosted_benchmark_sample", outcome.duration, details.clone());
    if let Some(replay) = replay {
        replay.log_meta("hosted_benchmark_sample", details);
    }
}

fn collect_workload<F>(
    perf: &PerfLogger,
    replay: Option<&ReplayLogger>,
    spec: WorkloadSpec<'_>,
    mut sample_fn: F,
) -> BenchRow
where
    F: FnMut(usize) -> SampleOutcome,
{
    let samples = sample_count();
    let mut durations = Vec::with_capacity(samples);
    let mut latency_samples = Vec::new();
    let mut total_bytes = 0u64;
    let mut total_units = 0.0f64;
    for sample_index in 0..samples {
        let outcome = sample_fn(sample_index);
        total_bytes = total_bytes.saturating_add(outcome.bytes_transferred);
        total_units += outcome.throughput_units;
        durations.push(outcome.duration.as_secs_f64() * 1000.0);
        if let Some(inner) = outcome.latency_samples.as_ref() {
            latency_samples.extend(inner.iter().copied());
        }
        log_sample(
            perf,
            replay,
            spec.workload,
            spec.mode,
            spec.cache_variant,
            sample_index,
            &outcome,
        );
    }

    let summary_samples = if latency_samples.is_empty() {
        &durations
    } else {
        &latency_samples
    };
    let (p50_ms, p99_ms, min_ms, mean_ms, max_ms, stddev_ms) = summarize_latency(summary_samples);
    let elapsed_secs = durations.iter().sum::<f64>() / 1000.0;
    let throughput = spec.throughput_unit.and_then(|_| {
        if elapsed_secs > 0.0 {
            Some(total_units / elapsed_secs)
        } else {
            None
        }
    });

    BenchRow {
        workload: spec.workload.to_string(),
        mode: spec.mode.label().to_string(),
        cache_variant: spec.cache_variant.label().to_string(),
        p50_ms,
        p99_ms,
        min_ms,
        mean_ms,
        max_ms,
        stddev_ms,
        throughput,
        throughput_unit: spec.throughput_unit.map(str::to_string),
        bytes_transferred: total_bytes,
        expectation: spec.expectation.to_string(),
        assessment: String::new(),
    }
}

fn current_ids() -> (u32, u32) {
    (compat::current_uid(), compat::current_gid())
}

fn run_close_fsync_sample(
    mode: BenchMode,
    cache_variant: CacheVariant,
    simulated_rtt: Duration,
) -> SampleOutcome {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let _cache_runtime = Runtime::new().expect("cache runtime");
    let _cache_state = Arc::new(CacheState::default());
    let cache_endpoint = matches!(mode, BenchMode::DirectPlusCache)
        .then(|| start_cache_server(&_cache_runtime, _cache_state.clone()));
    let _relay_runtime = Runtime::new().expect("relay runtime");
    let relay_endpoint = matches!(mode, BenchMode::RelayWrite).then(|| {
        let config = benchmark_config(&root, mode, None, 0);
        start_relay_server(&_relay_runtime, &config)
    });
    let endpoint = relay_endpoint.or(cache_endpoint);
    let config = benchmark_config(
        &root,
        mode,
        endpoint,
        if matches!(cache_variant, CacheVariant::On) {
            CACHE_VARIANT_ON_SEGMENT_BYTES
        } else {
            0
        },
    );
    let (runtime, fs) = build_fs(config);
    let (uid, gid) = current_ids();
    let payload = vec![0x61u8; CLOSE_WRITE_LEN];
    let record = fs
        .nfs_create(ROOT_INODE, "close_latency.bin", uid, gid)
        .expect("create");
    let start = Instant::now();
    fs.nfs_write(record.inode, 0, &payload).expect("write");
    if !matches!(mode, BenchMode::RelayWrite) {
        sleep_for(&runtime, scale_duration(simulated_rtt, 3));
    }
    fs.nfs_flush().expect("flush");
    SampleOutcome {
        duration: start.elapsed(),
        bytes_transferred: payload.len() as u64,
        throughput_units: 1.0,
        latency_samples: None,
    }
}

fn run_visibility_lag_sample(
    mode: BenchMode,
    cache_variant: CacheVariant,
    simulated_rtt: Duration,
) -> SampleOutcome {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let cache_state = Arc::new(CacheState::default());
    let _cache_runtime = Runtime::new().expect("cache runtime");
    let cache_endpoint = matches!(mode, BenchMode::DirectPlusCache)
        .then(|| start_cache_server(&_cache_runtime, cache_state.clone()));
    let _relay_runtime = Runtime::new().expect("relay runtime");
    let relay_endpoint = matches!(mode, BenchMode::RelayWrite).then(|| {
        let config = benchmark_config(&root, mode, None, 0);
        start_relay_server(&_relay_runtime, &config)
    });
    let endpoint = relay_endpoint.or(cache_endpoint);
    let config = benchmark_config(
        &root,
        mode,
        endpoint,
        if matches!(cache_variant, CacheVariant::On) {
            CACHE_VARIANT_ON_SEGMENT_BYTES
        } else {
            0
        },
    );
    let (runtime_a, fs_a) = build_fs(config.clone());
    let (runtime_b, fs_b) = build_fs(config);
    let (uid, gid) = current_ids();
    let visible_name = "visibility_target.bin";
    let created = fs_a
        .nfs_create(ROOT_INODE, visible_name, uid, gid)
        .expect("create");
    fs_a.nfs_flush().expect("flush a");

    if matches!(mode, BenchMode::DirectPlusCache) {
        cache_state.insert_inode(CachedMetadataEntry {
            inode: created.inode,
            record: created.clone(),
            generation: 1,
        });
    }

    let start = Instant::now();
    let mut seen = false;
    for _ in 0..64 {
        if matches!(
            mode,
            BenchMode::Direct | BenchMode::HostedMaintenanceOnly | BenchMode::RelayWrite
        ) {
            sleep_for(&runtime_b, scale_duration(simulated_rtt, 1));
        }
        if fs_b.nfs_getattr(created.inode).is_ok() {
            seen = true;
            break;
        }
    }
    if !seen {
        panic!("visibility lag sample did not resolve");
    }
    let _runtime_a = runtime_a; // keep both runtimes alive until the sample completes
    SampleOutcome {
        duration: start.elapsed(),
        bytes_transferred: 4 * 1024,
        throughput_units: 1.0,
        latency_samples: None,
    }
}

fn run_small_file_sample(
    mode: BenchMode,
    cache_variant: CacheVariant,
    simulated_rtt: Duration,
) -> SampleOutcome {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let cache_state = Arc::new(CacheState::default());
    let _cache_runtime = Runtime::new().expect("cache runtime");
    let cache_endpoint = matches!(mode, BenchMode::DirectPlusCache)
        .then(|| start_cache_server(&_cache_runtime, cache_state.clone()));
    let _relay_runtime = Runtime::new().expect("relay runtime");
    let relay_endpoint = matches!(mode, BenchMode::RelayWrite).then(|| {
        let config = benchmark_config(&root, mode, None, 0);
        start_relay_server(&_relay_runtime, &config)
    });
    let endpoint = relay_endpoint.or(cache_endpoint);
    let cache_bytes = if matches!(cache_variant, CacheVariant::On) {
        CACHE_VARIANT_ON_SEGMENT_BYTES
    } else {
        0
    };
    let config = benchmark_config(&root, mode, endpoint, cache_bytes);
    let (runtime, fs) = build_fs(config);
    let (uid, gid) = current_ids();
    let mut created_inodes = Vec::with_capacity(SMALL_FILE_COUNT);
    let start = Instant::now();
    for i in 0..SMALL_FILE_COUNT {
        let name = format!("s_{i:05}.txt");
        let renamed = format!("r_{i:05}.txt");
        let rec = fs.nfs_create(ROOT_INODE, &name, uid, gid).expect("create");
        fs.nfs_rename(ROOT_INODE, &name, ROOT_INODE, &renamed, 0, uid)
            .expect("rename");
        created_inodes.push((rec.inode, renamed));
    }
    if matches!(mode, BenchMode::RelayWrite) {
        fs.nfs_flush().expect("flush");
    } else {
        sleep_for(&runtime, scale_duration(simulated_rtt, 2));
        fs.nfs_flush().expect("flush");
    }

    if matches!(mode, BenchMode::DirectPlusCache) {
        let mut dir_entries = Vec::with_capacity(created_inodes.len());
        for (inode, name) in &created_inodes {
            let record = fs.nfs_getattr(*inode).expect("populate cache");
            cache_state.insert_inode(CachedMetadataEntry {
                inode: *inode,
                record: record.clone(),
                generation: 1,
            });
            dir_entries.push(CachedDirEntry {
                name: name.clone(),
                inode: *inode,
            });
        }
        cache_state.insert_directory(ROOT_INODE, dir_entries);
    }

    let _entries = fs.nfs_readdir(ROOT_INODE).expect("readdir");
    for (inode, _) in &created_inodes {
        if matches!(
            mode,
            BenchMode::Direct | BenchMode::HostedMaintenanceOnly | BenchMode::RelayWrite
        ) {
            sleep_for(&runtime, scale_duration(simulated_rtt, 1));
        }
        let _ = fs.nfs_getattr(*inode).expect("stat");
    }

    SampleOutcome {
        duration: start.elapsed(),
        bytes_transferred: 0,
        throughput_units: SMALL_FILE_COUNT as f64,
        latency_samples: None,
    }
}

fn run_hot_read_sample(
    mode: BenchMode,
    cache_variant: CacheVariant,
    simulated_rtt: Duration,
) -> SampleOutcome {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let cache_bytes = if matches!(cache_variant, CacheVariant::On) {
        CACHE_VARIANT_ON_SEGMENT_BYTES
    } else {
        0
    };
    let _cache_runtime = Runtime::new().expect("cache runtime");
    let cache_state = Arc::new(CacheState::default());
    let cache_endpoint = matches!(mode, BenchMode::DirectPlusCache)
        .then(|| start_cache_server(&_cache_runtime, cache_state.clone()));
    let _relay_runtime = Runtime::new().expect("relay runtime");
    let relay_endpoint = matches!(mode, BenchMode::RelayWrite).then(|| {
        let config = benchmark_config(&root, mode, None, 0);
        start_relay_server(&_relay_runtime, &config)
    });
    let endpoint = relay_endpoint.or(cache_endpoint);
    let config = benchmark_config(&root, mode, endpoint, cache_bytes);
    let (runtime, fs) = build_fs(config);
    let (uid, gid) = current_ids();
    let record = fs
        .nfs_create(ROOT_INODE, "hot_read.bin", uid, gid)
        .expect("create");
    let payload = generate_incompressible_payload(HOT_READ_LEN);
    fs.nfs_write(record.inode, 0, &payload).expect("write");
    fs.nfs_flush().expect("flush");

    if matches!(mode, BenchMode::DirectPlusCache) {
        let refreshed = fs.nfs_getattr(record.inode).expect("stat hot_read");
        if let Some(extents) = refreshed.segment_extents() {
            for extent in extents {
                let start = extent.logical_offset as usize;
                let end = start
                    .checked_add(extent.pointer.length as usize)
                    .expect("segment extent overflow");
                cache_state.insert_segment(
                    extent.pointer.generation,
                    extent.pointer.segment_id,
                    payload[start..end].to_vec(),
                );
            }
        }
    }

    let read_size = HOT_READ_LEN as u32;
    let mut samples = Vec::with_capacity(HOT_READ_REPEATS);
    let start = Instant::now();
    for i in 0..HOT_READ_REPEATS {
        if matches!(cache_variant, CacheVariant::Off)
            || i == 0
            || matches!(mode, BenchMode::Direct | BenchMode::HostedMaintenanceOnly)
        {
            sleep_for(&runtime, scale_duration(simulated_rtt, 1));
        }
        let read_start = Instant::now();
        let _ = fs.nfs_read(record.inode, 0, read_size).expect("read");
        samples.push(read_start.elapsed().as_secs_f64() * 1000.0);
    }
    let total_elapsed = start.elapsed();
    SampleOutcome {
        duration: total_elapsed,
        bytes_transferred: if matches!(cache_variant, CacheVariant::On) {
            HOT_READ_LEN as u64
        } else {
            (HOT_READ_LEN * HOT_READ_REPEATS) as u64
        },
        throughput_units: (HOT_READ_LEN * HOT_READ_REPEATS) as f64,
        latency_samples: Some(samples),
    }
}

fn run_maintenance_sample(mode: BenchMode, simulated_rtt: Duration) -> SampleOutcome {
    let temp = tempdir().expect("temp dir");
    let root = temp.path().to_path_buf();
    let config = benchmark_config(&root, mode, None, 0);
    let (runtime, metadata, superblock, segments, client_state, fs) = build_fs_with_parts(config);
    let (uid, gid) = current_ids();

    for round in 0..3 {
        for i in 0..MAINTENANCE_FILE_COUNT {
            let name = format!("m{round}_{i:03}.bin");
            let record = fs.nfs_create(ROOT_INODE, &name, uid, gid).expect("create");
            let payload = vec![(round + i) as u8; MAINTENANCE_FILE_SIZE];
            fs.nfs_write(record.inode, 0, &payload).expect("write");
        }
        fs.nfs_flush().expect("flush");
    }

    let compaction = CompactionConfig {
        delta_compact_threshold: 0,
        delta_compact_keep: 0,
        segment_compact_batch: 8,
        segment_compact_lag: 1,
        lease_ttl_secs: 30,
    };

    let start = Instant::now();
    if matches!(mode, BenchMode::Direct) {
        sleep_for(&runtime, scale_duration(simulated_rtt, 3));
    }
    let result = runtime
        .block_on(async {
            maintenance::run_segment_compaction(&metadata, &segments, &compaction).await
        })
        .expect("segment compaction");
    let _ = (superblock, client_state);

    SampleOutcome {
        duration: start.elapsed(),
        bytes_transferred: if matches!(mode, BenchMode::Direct) {
            result.bytes_rewritten
        } else {
            0
        },
        throughput_units: result.bytes_rewritten as f64,
        latency_samples: None,
    }
}

fn assess_row(row: &BenchRow, baseline: &BenchRow) -> String {
    if row.workload == "maintenance_bandwidth" {
        if row.bytes_transferred < baseline.bytes_transferred {
            "beneficial".to_string()
        } else if row.bytes_transferred == baseline.bytes_transferred {
            "neutral".to_string()
        } else {
            "regression".to_string()
        }
    } else if row.p50_ms <= baseline.p50_ms * 1.05 && row.p99_ms <= baseline.p99_ms * 1.05 {
        "beneficial".to_string()
    } else if row.p50_ms >= baseline.p50_ms * 1.10 || row.p99_ms >= baseline.p99_ms * 1.10 {
        "regression".to_string()
    } else {
        "mixed".to_string()
    }
}

fn render_markdown(report: &BenchmarkReport) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# Hosted Accelerator Benchmarks\n\n- profile: `{}`\n- simulated RTT: `{} ms`\n- samples per row: `{}`\n",
        report.profile, report.simulated_rtt_ms, report.sample_count
    );
    let mut by_workload: BTreeMap<&str, Vec<&BenchRow>> = BTreeMap::new();
    for row in &report.rows {
        by_workload.entry(&row.workload).or_default().push(row);
    }
    for (workload, rows) in by_workload {
        let _ = writeln!(out, "## {}\n", workload);
        let expectation = rows
            .first()
            .map(|row| row.expectation.as_str())
            .unwrap_or("");
        let _ = writeln!(out, "Expected: {}\n", expectation);
        let _ = writeln!(
            out,
            "| mode | cache | p50 ms | p99 ms | throughput | bytes transferred | status |"
        );
        let _ = writeln!(out, "| --- | --- | ---: | ---: | ---: | ---: | --- |");
        for row in rows {
            let throughput = row
                .throughput
                .map(|value| {
                    if let Some(unit) = &row.throughput_unit {
                        format!("{value:.2} {unit}")
                    } else {
                        format!("{value:.2}")
                    }
                })
                .unwrap_or_else(|| "-".to_string());
            let _ = writeln!(
                out,
                "| {} | {} | {:.3} | {:.3} | {} | {} | {} |",
                row.mode,
                row.cache_variant,
                row.p50_ms,
                row.p99_ms,
                throughput,
                row.bytes_transferred,
                row.assessment,
            );
        }
        out.push('\n');
    }
    out
}

fn write_optional_file(path: Option<PathBuf>, contents: &str) {
    let Some(path) = path else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
    {
        let _ = file.write_all(contents.as_bytes());
    }
}

fn run_benchmarks() -> BenchmarkReport {
    let simulated_rtt = simulated_rtt();
    let perf_logger = PerfLogger::new(perf_log_path()).expect("perf logger");
    let replay_logger = ReplayLogger::new(replay_log_path()).expect("replay logger");
    let profile = bench_profile();

    let mut rows = Vec::new();
    let workloads = [
        "close_fsync_latency",
        "metadata_visibility_lag",
        "small_file_metadata_heavy",
        "hot_read_tail_latency",
        "maintenance_bandwidth",
    ];

    for workload in workloads {
        for mode in [
            BenchMode::Direct,
            BenchMode::HostedMaintenanceOnly,
            BenchMode::DirectPlusCache,
            BenchMode::RelayWrite,
        ] {
            let cache_variants: &[CacheVariant] = match workload {
                "hot_read_tail_latency" => &[CacheVariant::Off, CacheVariant::On],
                "metadata_visibility_lag" | "small_file_metadata_heavy" => {
                    if matches!(mode, BenchMode::DirectPlusCache) {
                        &[CacheVariant::On]
                    } else {
                        &[CacheVariant::Off]
                    }
                }
                _ => &[CacheVariant::Off],
            };

            for &cache_variant in cache_variants {
                let spec = WorkloadSpec {
                    mode,
                    cache_variant,
                    workload,
                    expectation: mode.expectation(),
                    throughput_unit: Some(match workload {
                        "close_fsync_latency" => "ops/s",
                        "metadata_visibility_lag" => "ops/s",
                        "small_file_metadata_heavy" => "files/s",
                        "hot_read_tail_latency" => "MiB/s",
                        "maintenance_bandwidth" => "MiB/s",
                        _ => unreachable!(),
                    }),
                };
                let row = match workload {
                    "close_fsync_latency" => {
                        collect_workload(&perf_logger, Some(&replay_logger), spec, |_| {
                            run_close_fsync_sample(mode, cache_variant, simulated_rtt)
                        })
                    }
                    "metadata_visibility_lag" => {
                        collect_workload(&perf_logger, Some(&replay_logger), spec, |_| {
                            run_visibility_lag_sample(mode, cache_variant, simulated_rtt)
                        })
                    }
                    "small_file_metadata_heavy" => {
                        collect_workload(&perf_logger, Some(&replay_logger), spec, |_| {
                            run_small_file_sample(mode, cache_variant, simulated_rtt)
                        })
                    }
                    "hot_read_tail_latency" => {
                        collect_workload(&perf_logger, Some(&replay_logger), spec, |_| {
                            run_hot_read_sample(mode, cache_variant, simulated_rtt)
                        })
                    }
                    "maintenance_bandwidth" => {
                        collect_workload(&perf_logger, Some(&replay_logger), spec, |_| {
                            run_maintenance_sample(mode, simulated_rtt)
                        })
                    }
                    _ => unreachable!(),
                };
                rows.push(row);
            }
        }
    }

    let baselines: BTreeMap<String, BenchRow> = rows
        .iter()
        .filter(|row| row.mode == "direct" && row.cache_variant == "off")
        .cloned()
        .map(|row| (row.workload.clone(), row))
        .collect();

    for row in &mut rows {
        if let Some(baseline) = baselines.get(row.workload.as_str()) {
            row.assessment = if row.mode == "direct" && row.cache_variant == "off" {
                "baseline".to_string()
            } else {
                assess_row(row, baseline)
            };
        }
    }

    let report = BenchmarkReport {
        profile,
        simulated_rtt_ms: simulated_rtt.as_millis() as u64,
        sample_count: sample_count(),
        rows,
    };

    let perf_summary = json!({
        "profile": report.profile,
        "simulated_rtt_ms": report.simulated_rtt_ms,
        "sample_count": report.sample_count,
        "rows": report.rows,
    });
    perf_logger.log(
        "hosted_benchmark_report",
        Duration::from_secs(0),
        perf_summary.clone(),
    );
    replay_logger.log_meta("hosted_benchmark_report", perf_summary);

    if let Some(path) = report_json_path() {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(file) = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
        {
            let _ = serde_json::to_writer_pretty(file, &report);
        }
    }

    write_optional_file(report_md_path(), &render_markdown(&report));
    report
}

fn main() {
    let report = run_benchmarks();
    print!("{}", render_markdown(&report));
}
