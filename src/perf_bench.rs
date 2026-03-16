use std::path::Path;
use std::sync::Arc;

use crate::config::Config;
use crate::inode::{FileStorage, InodeKind, InodeRecord};
use time::OffsetDateTime;

pub fn perf_config(root: &Path) -> Config {
    Config {
        inline_threshold: 512,
        shard_size: 64,
        inode_batch: 16,
        segment_batch: 32,
        pending_bytes: 128 * 1024 * 1024,
        entry_ttl_secs: 10,
        disable_journal: true,
        flush_interval_ms: 0,
        disable_cleanup: true,
        lookup_cache_ttl_ms: 0,
        dir_cache_ttl_ms: 0,
        metadata_poll_interval_ms: 0,
        segment_cache_bytes: 0,
        imap_delta_batch: 16,
        fuse_threads: 0,
        ..Config::with_paths(
            root.join("mnt"),
            root.join("store"),
            root.join("cache"),
            root.join("state.bin"),
        )
    }
}

pub fn make_file_record(inode: u64, parent: u64, data: Vec<u8>) -> InodeRecord {
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

pub fn generate_incompressible_payload(data_size: usize) -> Vec<u8> {
    (0..data_size)
        .map(|i: usize| {
            i.wrapping_mul(6364136223846793005_usize)
                .wrapping_add(1442695040888963407_usize)
                .wrapping_shr(56) as u8
        })
        .collect()
}

pub fn make_large_directory_record(children: usize) -> InodeRecord {
    let now = OffsetDateTime::now_utc();
    let mut map = std::collections::BTreeMap::new();
    for i in 0..children {
        map.insert(format!("f_{i:05}.txt"), (i + 2) as u64);
    }
    InodeRecord {
        inode: 1,
        parent: 1,
        name: "/".into(),
        path: "/".into(),
        kind: InodeKind::Directory {
            children: Arc::new(map),
        },
        size: 0,
        mode: 0o40755,
        uid: 1000,
        gid: 1000,
        atime: now,
        mtime: now,
        ctime: now,
        link_count: 1,
        rdev: 0,
        storage: FileStorage::Inline(Vec::new()),
    }
}

#[derive(Clone, Copy)]
pub struct PerfStats {
    pub min: f64,
    pub median: f64,
    pub mean: f64,
    pub max: f64,
    pub stddev: f64,
}

pub fn summarize_samples(samples: &[f64]) -> PerfStats {
    assert!(!samples.is_empty(), "need at least one sample");
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let min = sorted[0];
    let max = *sorted.last().unwrap_or(&min);
    let median = if sorted.len() % 2 == 1 {
        sorted[sorted.len() / 2]
    } else {
        let hi = sorted.len() / 2;
        (sorted[hi - 1] + sorted[hi]) / 2.0
    };
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let variance = samples
        .iter()
        .map(|v| {
            let delta = *v - mean;
            delta * delta
        })
        .sum::<f64>()
        / samples.len() as f64;
    let stddev = variance.sqrt();
    PerfStats {
        min,
        median,
        mean,
        max,
        stddev,
    }
}
