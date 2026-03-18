use std::collections::HashMap;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use clawfs::config::Config;
use clawfs::fs::OsageFs;
use clawfs::inode::{InodeRecord, ROOT_INODE};
use clawfs::journal::JournalManager;
use clawfs::metadata::MetadataStore;
use clawfs::segment::SegmentManager;
use clawfs::state::ClientStateManager;
use clawfs::superblock::SuperblockManager;
use flate2::read::MultiGzDecoder;
use libc::ENOENT;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;
use serde_json::Value;
use tokio::runtime::Runtime;

const WELCOME_FILENAME: &str = "WELCOME.txt";
const WELCOME_CONTENT: &str = "Welcome to ClawFS!\n\
\n\
ClawFS is a log-structured, object-store-backed filesystem designed for fast,\n\
shared access to large working sets with durable metadata and batched writes.\n\
\n\
Great use cases:\n\
- AI training data and model artifacts shared across multiple machines\n\
- Shared home directories for teams, labs, or ephemeral compute nodes\n\
- High-throughput team access to large binaries, build outputs, and datasets\n\
\n\
Why teams use it:\n\
- Immutable segment writes for efficient object-store IO\n\
- Batched metadata updates for lower API overhead\n\
- Local staging, caching, and journal replay for practical durability and speed\n\
\n\
Enjoy building on ClawFS.\n";

#[derive(Parser, Debug)]
#[command(
    name = "clawfs-replay",
    version,
    about = "Replay io traces directly through OsageFs API"
)]
struct Cli {
    #[arg(long, value_name = "PATH")]
    trace_path: PathBuf,

    #[arg(long, value_name = "PATH")]
    store_path: PathBuf,

    #[arg(long, value_name = "PATH")]
    local_cache_path: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    state_path: Option<PathBuf>,

    /// Home prefix for bootstrap. Defaults to trace fs_config when available.
    #[arg(long)]
    home_prefix: Option<String>,

    /// User name for bootstrap. Defaults to trace fs_config bootstrap_user when available.
    #[arg(long)]
    user_name: Option<String>,

    #[arg(long, default_value_t = 1.0)]
    speed: f64,

    #[arg(long, default_value_t = false)]
    ignore_timing: bool,

    #[arg(long, value_enum, default_value_t = LayerFilter::All)]
    layer: LayerFilter,

    #[arg(long, default_value_t = true)]
    continue_on_error: bool,

    #[arg(long, default_value_t = 200)]
    max_mismatch_logs: usize,

    #[arg(long, default_value_t = 1)]
    iterations: usize,

    #[arg(long)]
    seed: Option<u64>,

    #[arg(long, default_value_t = 0)]
    jitter_us: u64,

    #[arg(long, default_value_t = 0.0)]
    chaos_sleep_prob: f64,

    #[arg(long, default_value_t = 0)]
    chaos_sleep_max_us: u64,

    #[arg(long, default_value_t = 0.0)]
    chaos_flush_prob: f64,

    /// Use recorded `meta/fs_config` event to set replay config defaults.
    #[arg(long, default_value_t = true)]
    use_trace_config: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LayerFilter {
    All,
    Fuse,
    Nfs,
}

#[derive(Debug, Deserialize)]
struct ReplayEvent {
    seq: u64,
    layer: String,
    op: String,
    start_us: u64,
    errno: Option<i32>,
    details: Value,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct CapturedFsConfig {
    home_prefix: Option<String>,
    inline_threshold: Option<usize>,
    pending_bytes: Option<u64>,
    fsync_on_close: Option<bool>,
    flush_interval_ms: Option<u64>,
    disable_journal: Option<bool>,
    lookup_cache_ttl_ms: Option<u64>,
    dir_cache_ttl_ms: Option<u64>,
    metadata_poll_interval_ms: Option<u64>,
    segment_cache_bytes: Option<u64>,
    imap_delta_batch: Option<usize>,
    bootstrap_user: Option<String>,
}

#[derive(Debug, Clone)]
struct EffectiveFsConfig {
    home_prefix: String,
    inline_threshold: usize,
    pending_bytes: u64,
    fsync_on_close: bool,
    flush_interval_ms: u64,
    disable_journal: bool,
    lookup_cache_ttl_ms: u64,
    dir_cache_ttl_ms: u64,
    metadata_poll_interval_ms: u64,
    segment_cache_bytes: u64,
    imap_delta_batch: usize,
}

struct ParsedTrace {
    events: Vec<ReplayEvent>,
    captured: Option<CapturedFsConfig>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    validate_cli(&cli)?;

    let trace_text = read_trace_text(&cli.trace_path)?;
    if trace_text.repaired {
        eprintln!(
            "replay: repaired truncated trace {} (dropped_partial_lines={})",
            cli.trace_path.display(),
            trace_text.dropped_partial_lines
        );
    }
    let parsed = parse_trace(&trace_text.lines)?;

    let effective = effective_config(&cli, parsed.captured.as_ref());
    let uid = unsafe { libc::geteuid() as u32 };
    let gid = unsafe { libc::getegid() as u32 };
    let user_name = cli
        .user_name
        .clone()
        .or_else(|| {
            if cli.use_trace_config {
                parsed
                    .captured
                    .as_ref()
                    .and_then(|c| c.bootstrap_user.clone())
            } else {
                None
            }
        })
        .or_else(|| {
            std::env::var("USER")
                .or_else(|_| std::env::var("LOGNAME"))
                .ok()
        })
        .unwrap_or_else(|| format!("uid{uid}"));

    let config_root = default_user_config_root();
    let base_cache_path = cli
        .local_cache_path
        .clone()
        .unwrap_or_else(|| config_root.join("cache"));
    let base_state_path = cli
        .state_path
        .clone()
        .unwrap_or_else(|| config_root.join("state").join("replay_state.bin"));

    let base_seed = cli.seed.unwrap_or_else(rand::random);
    println!(
        "replay start: events={} iterations={} seed={} use_trace_config={} home_prefix={}",
        parsed.events.len(),
        cli.iterations,
        base_seed,
        cli.use_trace_config,
        effective.home_prefix
    );

    let mut total_replayed = 0usize;
    let mut total_skipped = 0usize;
    let mut total_unsupported = 0usize;
    let mut total_mismatches = 0usize;

    for iter in 0..cli.iterations {
        let iter_seed = base_seed.wrapping_add(iter as u64);
        let mut rng = StdRng::seed_from_u64(iter_seed);
        let iter_paths = iteration_paths(
            iter,
            cli.iterations,
            &cli.store_path,
            &base_cache_path,
            &base_state_path,
        );
        reset_iteration_paths(&iter_paths)?;

        let config = build_config(
            &effective,
            &iter_paths.store_path,
            &iter_paths.cache_path,
            &iter_paths.state_path,
        );
        let stats = run_iteration(
            &config,
            &parsed.events,
            &cli,
            &user_name,
            uid,
            gid,
            &mut rng,
        )?;

        total_replayed += stats.replayed;
        total_skipped += stats.skipped;
        total_unsupported += stats.unsupported;
        total_mismatches += stats.mismatched_errno;

        println!(
            "iteration {}: replayed={} skipped={} unsupported={} errno_mismatch={} final_flush_errno={:?}",
            iter + 1,
            stats.replayed,
            stats.skipped,
            stats.unsupported,
            stats.mismatched_errno,
            stats.final_flush_errno
        );
    }

    println!(
        "replay complete: iterations={} replayed={} skipped={} unsupported={} errno_mismatch={}",
        cli.iterations, total_replayed, total_skipped, total_unsupported, total_mismatches
    );
    Ok(())
}

fn validate_cli(cli: &Cli) -> Result<()> {
    if cli.speed <= 0.0 {
        return Err(anyhow!("--speed must be > 0"));
    }
    if cli.iterations == 0 {
        return Err(anyhow!("--iterations must be >= 1"));
    }
    for (name, val) in [
        ("chaos_sleep_prob", cli.chaos_sleep_prob),
        ("chaos_flush_prob", cli.chaos_flush_prob),
    ] {
        if !(0.0..=1.0).contains(&val) {
            return Err(anyhow!("--{} must be between 0 and 1", name));
        }
    }
    Ok(())
}

fn effective_config(cli: &Cli, captured: Option<&CapturedFsConfig>) -> EffectiveFsConfig {
    let mut cfg = EffectiveFsConfig {
        home_prefix: "/home".to_string(),
        inline_threshold: 4 * 1024,
        pending_bytes: 1024 * 1024 * 1024,
        fsync_on_close: false,
        flush_interval_ms: 0,
        disable_journal: true,
        lookup_cache_ttl_ms: 0,
        dir_cache_ttl_ms: 0,
        metadata_poll_interval_ms: 0,
        segment_cache_bytes: 512 * 1024 * 1024,
        imap_delta_batch: 256,
    };
    if cli.use_trace_config
        && let Some(c) = captured
    {
        if let Some(v) = &c.home_prefix {
            cfg.home_prefix = v.clone();
        }
        if let Some(v) = c.inline_threshold {
            cfg.inline_threshold = v;
        }
        if let Some(v) = c.pending_bytes {
            cfg.pending_bytes = v;
        }
        if let Some(v) = c.fsync_on_close {
            cfg.fsync_on_close = v;
        }
        if let Some(v) = c.flush_interval_ms {
            cfg.flush_interval_ms = v;
        }
        if let Some(v) = c.disable_journal {
            cfg.disable_journal = v;
        }
        if let Some(v) = c.lookup_cache_ttl_ms {
            cfg.lookup_cache_ttl_ms = v;
        }
        if let Some(v) = c.dir_cache_ttl_ms {
            cfg.dir_cache_ttl_ms = v;
        }
        if let Some(v) = c.metadata_poll_interval_ms {
            cfg.metadata_poll_interval_ms = v;
        }
        if let Some(v) = c.segment_cache_bytes {
            cfg.segment_cache_bytes = v;
        }
        if let Some(v) = c.imap_delta_batch {
            cfg.imap_delta_batch = v.max(1);
        }
    }
    if let Some(home) = &cli.home_prefix {
        cfg.home_prefix = home.clone();
    }
    cfg
}

#[derive(Default)]
struct IterationStats {
    replayed: usize,
    skipped: usize,
    unsupported: usize,
    mismatched_errno: usize,
    final_flush_errno: Option<i32>,
}

struct IterationPaths {
    store_path: PathBuf,
    cache_path: PathBuf,
    state_path: PathBuf,
}

fn iteration_paths(
    iter: usize,
    iterations: usize,
    store_path: &Path,
    cache_path: &Path,
    state_path: &Path,
) -> IterationPaths {
    if iterations == 1 {
        return IterationPaths {
            store_path: store_path.to_path_buf(),
            cache_path: cache_path.to_path_buf(),
            state_path: state_path.to_path_buf(),
        };
    }
    let iter_name = format!("iter_{:03}", iter + 1);
    let state_name = state_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("replay_state.bin");
    let state_parent = state_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    IterationPaths {
        store_path: store_path.join(&iter_name),
        cache_path: cache_path.join(&iter_name),
        state_path: state_parent.join(format!("{}.{}", state_name, iter_name)),
    }
}

fn reset_iteration_paths(paths: &IterationPaths) -> Result<()> {
    if paths.store_path.exists() {
        std::fs::remove_dir_all(&paths.store_path)?;
    }
    if paths.cache_path.exists() {
        std::fs::remove_dir_all(&paths.cache_path)?;
    }
    if paths.state_path.exists() {
        std::fs::remove_file(&paths.state_path)?;
    }
    std::fs::create_dir_all(&paths.store_path)?;
    std::fs::create_dir_all(&paths.cache_path)?;
    if let Some(parent) = paths.state_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn build_config(
    effective: &EffectiveFsConfig,
    store_path: &Path,
    cache_path: &Path,
    state_path: &Path,
) -> Config {
    Config {
        inline_threshold: effective.inline_threshold,
        pending_bytes: effective.pending_bytes,
        home_prefix: effective.home_prefix.clone(),
        entry_ttl_secs: 10,
        disable_journal: effective.disable_journal,
        fsync_on_close: effective.fsync_on_close,
        flush_interval_ms: effective.flush_interval_ms,
        disable_cleanup: true,
        lookup_cache_ttl_ms: effective.lookup_cache_ttl_ms,
        dir_cache_ttl_ms: effective.dir_cache_ttl_ms,
        metadata_poll_interval_ms: effective.metadata_poll_interval_ms,
        segment_cache_bytes: effective.segment_cache_bytes,
        foreground: true,
        imap_delta_batch: effective.imap_delta_batch,
        fuse_threads: 0,
        accelerator_mode: None,
        accelerator_endpoint: None,
        accelerator_fallback_policy: None,
        ..Config::with_paths(
            PathBuf::from("/tmp/clawfs-replay-unused-mount"),
            store_path.to_path_buf(),
            cache_path.to_path_buf(),
            state_path.to_path_buf(),
        )
    }
}

fn run_iteration(
    config: &Config,
    events: &[ReplayEvent],
    cli: &Cli,
    user_name: &str,
    uid: u32,
    gid: u32,
    rng: &mut StdRng,
) -> Result<IterationStats> {
    let runtime = Runtime::new()?;
    let handle = runtime.handle().clone();
    let metadata = Arc::new(runtime.block_on(MetadataStore::new(config, handle.clone()))?);
    let superblock = Arc::new(runtime.block_on(SuperblockManager::load_or_init(
        metadata.clone(),
        config.shard_size,
    ))?);
    runtime.block_on(ensure_root(
        metadata.clone(),
        superblock.clone(),
        config.shard_size,
    ))?;

    let segments = Arc::new(SegmentManager::new(config, handle.clone())?);
    let client_state = Arc::new(ClientStateManager::load(&config.state_path)?);
    let superblock_snapshot = superblock.snapshot();
    client_state.reconcile_with_minimums(
        superblock_snapshot.next_inode,
        superblock_snapshot.next_segment,
    )?;
    let fs = OsageFs::new(
        config.clone(),
        metadata,
        superblock,
        segments,
        None,
        None::<Arc<JournalManager>>,
        handle,
        client_state,
        None,
        None,
        None,
        None,
        None,
    );
    ensure_fresh_bootstrap(&fs, &config.home_prefix, user_name, uid, gid)?;

    let mut stats = IterationStats::default();
    let mut mapper = InodeMapper::new();
    let mut first_start_us = None::<u64>;
    let wall_start = Instant::now();
    let mut mismatch_logs = 0usize;

    for event in events {
        if !matches_layer(&event.layer, cli.layer) {
            stats.skipped += 1;
            continue;
        }
        schedule_event(event, cli, &wall_start, &mut first_start_us, rng);
        apply_chaos(&fs, cli, rng);

        match replay_event(&fs, event, &mut mapper) {
            ReplayOutcome::Applied(actual_errno) => {
                stats.replayed += 1;
                if actual_errno != event.errno {
                    stats.mismatched_errno += 1;
                    if mismatch_logs < cli.max_mismatch_logs {
                        eprintln!(
                            "errno mismatch seq={} op={} expected={:?} actual={:?}",
                            event.seq, event.op, event.errno, actual_errno
                        );
                        mismatch_logs += 1;
                    }
                    if !cli.continue_on_error {
                        return Err(anyhow!(
                            "stopped due to errno mismatch at seq {}",
                            event.seq
                        ));
                    }
                }
            }
            ReplayOutcome::Unsupported => stats.unsupported += 1,
            ReplayOutcome::Skipped => stats.skipped += 1,
        }
    }
    stats.final_flush_errno = fs.nfs_flush().err();
    Ok(stats)
}

fn schedule_event(
    event: &ReplayEvent,
    cli: &Cli,
    wall_start: &Instant,
    first_start_us: &mut Option<u64>,
    rng: &mut StdRng,
) {
    if cli.ignore_timing {
        return;
    }
    let base = *first_start_us.get_or_insert(event.start_us);
    let mut offset_us = event.start_us.saturating_sub(base) as i64;
    if cli.jitter_us > 0 {
        let jitter = rng.random_range(-(cli.jitter_us as i64)..=(cli.jitter_us as i64));
        offset_us = (offset_us + jitter).max(0);
    }
    let target_secs = (offset_us as f64) / 1_000_000.0 / cli.speed;
    let target = Duration::from_secs_f64(target_secs.max(0.0));
    let elapsed = wall_start.elapsed();
    if target > elapsed {
        thread::sleep(target - elapsed);
    }
}

fn apply_chaos(fs: &OsageFs, cli: &Cli, rng: &mut StdRng) {
    if cli.chaos_sleep_prob > 0.0
        && cli.chaos_sleep_max_us > 0
        && rng.random_bool(cli.chaos_sleep_prob)
    {
        let sleep_us = rng.random_range(0..=cli.chaos_sleep_max_us);
        thread::sleep(Duration::from_micros(sleep_us));
    }
    if cli.chaos_flush_prob > 0.0 && rng.random_bool(cli.chaos_flush_prob) {
        let _ = fs.nfs_flush();
    }
}

fn parse_trace(lines: &[String]) -> Result<ParsedTrace> {
    let mut events = Vec::with_capacity(lines.len());
    let mut captured = None;
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let event: ReplayEvent =
            serde_json::from_str(line).context("failed to parse replay log line")?;
        if event.layer == "meta" && event.op == "fs_config" {
            let cfg = serde_json::from_value::<CapturedFsConfig>(event.details.clone())
                .context("failed to parse fs_config metadata event")?;
            captured = Some(cfg);
            continue;
        }
        events.push(event);
    }
    Ok(ParsedTrace { events, captured })
}

struct TraceText {
    lines: Vec<String>,
    repaired: bool,
    dropped_partial_lines: usize,
}

fn read_trace_text(path: &PathBuf) -> Result<TraceText> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let is_gzip = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("gz"))
        .unwrap_or(false);

    let mut text = String::new();
    let mut repaired = false;
    if is_gzip {
        let mut decoder = MultiGzDecoder::new(file);
        match decoder.read_to_string(&mut text) {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                repaired = true;
            }
            Err(err) => return Err(err).context("failed to decode gzip trace"),
        }
    } else {
        let mut reader = std::io::BufReader::new(file);
        reader.read_to_string(&mut text)?;
    }

    let mut dropped_partial_lines = 0usize;
    let mut lines: Vec<String> = text.lines().map(ToString::to_string).collect();
    if repaired && let Some(last) = lines.last() {
        let trimmed = last.trim();
        if !trimmed.is_empty() && !trimmed.ends_with('}') {
            lines.pop();
            dropped_partial_lines = 1;
        }
    }
    Ok(TraceText {
        lines,
        repaired,
        dropped_partial_lines,
    })
}

fn matches_layer(layer: &str, filter: LayerFilter) -> bool {
    match filter {
        LayerFilter::All => true,
        LayerFilter::Fuse => layer == "fuse",
        LayerFilter::Nfs => layer == "nfs",
    }
}

fn ensure_fresh_bootstrap(
    fs: &OsageFs,
    home_prefix: &str,
    user_name: &str,
    uid: u32,
    gid: u32,
) -> Result<()> {
    ensure_file_exists(
        fs,
        ROOT_INODE,
        WELCOME_FILENAME,
        uid,
        gid,
        WELCOME_CONTENT.as_bytes(),
    )?;
    if home_prefix.is_empty() {
        return Ok(());
    }
    ensure_directory_path(fs, home_prefix, uid, gid)?;
    let user_path = format!("{}/{}", home_prefix.trim_end_matches('/'), user_name);
    ensure_directory_path(fs, &user_path, uid, gid)?;
    Ok(())
}

fn ensure_directory_path(fs: &OsageFs, path: &str, uid: u32, gid: u32) -> Result<u64> {
    let mut current = ROOT_INODE;
    for component in path.split('/').filter(|s| !s.is_empty()) {
        current = match fs.nfs_lookup(current, component) {
            Ok(found) => found.inode,
            Err(code) if code == ENOENT => {
                fs.nfs_mkdir(current, component, uid, gid)
                    .map_err(|errno| anyhow!("mkdir {} failed errno={}", component, errno))?
                    .inode
            }
            Err(errno) => return Err(anyhow!("lookup {} failed errno={}", component, errno)),
        };
    }
    Ok(current)
}

fn ensure_file_exists(
    fs: &OsageFs,
    parent: u64,
    name: &str,
    uid: u32,
    gid: u32,
    content: &[u8],
) -> Result<u64> {
    match fs.nfs_lookup(parent, name) {
        Ok(found) => Ok(found.inode),
        Err(code) if code == ENOENT => {
            let created = fs
                .nfs_create(parent, name, uid, gid)
                .map_err(|errno| anyhow!("create {} failed errno={}", name, errno))?;
            fs.nfs_write(created.inode, 0, content)
                .map_err(|errno| anyhow!("write {} failed errno={}", name, errno))?;
            Ok(created.inode)
        }
        Err(errno) => Err(anyhow!("lookup {} failed errno={}", name, errno)),
    }
}

fn replay_event(fs: &OsageFs, event: &ReplayEvent, mapper: &mut InodeMapper) -> ReplayOutcome {
    let details = &event.details;
    let op = event.op.as_str();
    let actual = match op {
        "lookup" => {
            let (Some(parent), Some(name)) = (d_u64(details, "parent"), d_str(details, "name"))
            else {
                return ReplayOutcome::Skipped;
            };
            match fs.nfs_lookup(mapper.map(parent), &name) {
                Ok(record) => {
                    if let Some(trace_ino) = d_u64(details, "ino") {
                        mapper.bind(trace_ino, record.inode);
                    }
                    None
                }
                Err(code) => Some(code),
            }
        }
        "getattr" | "open" | "statfs" => {
            let Some(ino) = d_u64(details, "ino") else {
                return ReplayOutcome::Skipped;
            };
            fs.nfs_getattr(mapper.map(ino)).err()
        }
        "readdir" => {
            let Some(ino) = d_u64(details, "ino") else {
                return ReplayOutcome::Skipped;
            };
            fs.nfs_readdir(mapper.map(ino)).err()
        }
        "setattr" => {
            let Some(ino) = d_u64(details, "ino") else {
                return ReplayOutcome::Skipped;
            };
            fs.nfs_setattr(
                mapper.map(ino),
                d_u32(details, "mode"),
                d_u32(details, "uid"),
                d_u32(details, "gid"),
                d_u64(details, "size"),
                None,
                None,
            )
            .err()
        }
        "create" => {
            let (Some(parent), Some(name)) = (d_u64(details, "parent"), d_str(details, "name"))
            else {
                return ReplayOutcome::Skipped;
            };
            let uid = d_u32(details, "uid").unwrap_or(0);
            let gid = d_u32(details, "gid").unwrap_or(0);
            match fs.nfs_create(mapper.map(parent), &name, uid, gid) {
                Ok(record) => {
                    if let Some(trace_ino) = d_u64(details, "ino") {
                        mapper.bind(trace_ino, record.inode);
                    }
                    None
                }
                Err(code) => Some(code),
            }
        }
        "mkdir" => {
            let (Some(parent), Some(name)) = (d_u64(details, "parent"), d_str(details, "name"))
            else {
                return ReplayOutcome::Skipped;
            };
            let uid = d_u32(details, "uid").unwrap_or(0);
            let gid = d_u32(details, "gid").unwrap_or(0);
            match fs.nfs_mkdir(mapper.map(parent), &name, uid, gid) {
                Ok(record) => {
                    if let Some(trace_ino) = d_u64(details, "ino") {
                        mapper.bind(trace_ino, record.inode);
                    }
                    None
                }
                Err(code) => Some(code),
            }
        }
        "symlink" => {
            let (Some(parent), Some(name)) = (d_u64(details, "parent"), d_str(details, "name"))
            else {
                return ReplayOutcome::Skipped;
            };
            let uid = d_u32(details, "uid").unwrap_or(0);
            let gid = d_u32(details, "gid").unwrap_or(0);
            let target_len = d_u64(details, "target_len").unwrap_or(16).max(1) as usize;
            let target = vec![b'x'; target_len];
            match fs.nfs_symlink(mapper.map(parent), &name, target, uid, gid) {
                Ok(record) => {
                    if let Some(trace_ino) = d_u64(details, "ino") {
                        mapper.bind(trace_ino, record.inode);
                    }
                    None
                }
                Err(code) => Some(code),
            }
        }
        "read" => {
            let (Some(ino), Some(offset), Some(requested)) = (
                d_u64(details, "ino"),
                d_u64(details, "offset"),
                d_u64(details, "requested"),
            ) else {
                return ReplayOutcome::Skipped;
            };
            fs.nfs_read(
                mapper.map(ino),
                offset,
                requested.min(u32::MAX as u64) as u32,
            )
            .err()
        }
        "write" => {
            let (Some(ino), Some(offset), Some(len)) = (
                d_u64(details, "ino"),
                d_u64(details, "offset"),
                d_u64(details, "len"),
            ) else {
                return ReplayOutcome::Skipped;
            };
            let payload = vec![0u8; len.min(16 * 1024 * 1024) as usize];
            fs.nfs_write(mapper.map(ino), offset, &payload).err()
        }
        "readlink" => {
            let Some(ino) = d_u64(details, "ino") else {
                return ReplayOutcome::Skipped;
            };
            fs.nfs_readlink(mapper.map(ino)).err()
        }
        "unlink" => {
            let (Some(parent), Some(name)) = (d_u64(details, "parent"), d_str(details, "name"))
            else {
                return ReplayOutcome::Skipped;
            };
            fs.nfs_remove_file(mapper.map(parent), &name, 0).err()
        }
        "rmdir" => {
            let (Some(parent), Some(name)) = (d_u64(details, "parent"), d_str(details, "name"))
            else {
                return ReplayOutcome::Skipped;
            };
            fs.nfs_remove_dir(mapper.map(parent), &name, 0).err()
        }
        "rename" => {
            let (Some(parent), Some(name), Some(newparent), Some(newname)) = (
                d_u64(details, "parent"),
                d_str(details, "name"),
                d_u64(details, "newparent"),
                d_str(details, "newname"),
            ) else {
                return ReplayOutcome::Skipped;
            };
            let flags = d_u64(details, "flags").unwrap_or(0) as u32;
            fs.nfs_rename(
                mapper.map(parent),
                &name,
                mapper.map(newparent),
                &newname,
                flags,
                0,
            )
            .err()
        }
        "flush" | "fsync" | "release" => fs.nfs_flush().err(),
        "link" => return ReplayOutcome::Unsupported,
        _ => return ReplayOutcome::Unsupported,
    };
    ReplayOutcome::Applied(actual)
}

enum ReplayOutcome {
    Applied(Option<i32>),
    Unsupported,
    Skipped,
}

struct InodeMapper {
    trace_to_runtime: HashMap<u64, u64>,
}

impl InodeMapper {
    fn new() -> Self {
        let mut trace_to_runtime = HashMap::new();
        trace_to_runtime.insert(ROOT_INODE, ROOT_INODE);
        Self { trace_to_runtime }
    }

    fn map(&self, trace_ino: u64) -> u64 {
        self.trace_to_runtime
            .get(&trace_ino)
            .copied()
            .unwrap_or(trace_ino)
    }

    fn bind(&mut self, trace_ino: u64, runtime_ino: u64) {
        self.trace_to_runtime.insert(trace_ino, runtime_ino);
    }
}

fn d_u64(details: &Value, key: &str) -> Option<u64> {
    details.get(key)?.as_u64()
}

fn d_u32(details: &Value, key: &str) -> Option<u32> {
    details
        .get(key)?
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
}

fn d_str(details: &Value, key: &str) -> Option<String> {
    details.get(key)?.as_str().map(ToString::to_string)
}

fn default_user_config_root() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".clawfs")
}

async fn ensure_root(
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    shard_size: u64,
) -> Result<()> {
    if metadata.get_inode(ROOT_INODE).await?.is_some() {
        return Ok(());
    }
    let uid = unsafe { libc::geteuid() as u32 };
    let gid = unsafe { libc::getegid() as u32 };
    let snapshot = superblock.prepare_dirty_generation()?;
    let generation = snapshot.generation;
    let mut root = InodeRecord::new_directory(
        ROOT_INODE,
        ROOT_INODE,
        String::new(),
        "/".to_string(),
        uid,
        gid,
    );
    root.mode = 0o40777;
    if let Err(err) = metadata.persist_inode(&root, generation, shard_size).await {
        superblock.abort_generation(generation);
        return Err(err);
    }
    superblock.commit_generation(generation).await?;
    Ok(())
}
