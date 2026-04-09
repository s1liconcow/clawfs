use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::json;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::clawfs::{AcceleratorMode, RelayOutagePolicy};

// ── Accelerator status types ──────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceleratorHealth {
    Healthy,
    Degraded,
    Disconnected,
    NotConfigured,
}

impl AcceleratorHealth {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Disconnected => "disconnected",
            Self::NotConfigured => "not_configured",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceleratorCleanupOwner {
    Local,
    Hosted,
    Unknown,
}

impl AcceleratorCleanupOwner {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Hosted => "hosted",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceleratorCoordinationStatus {
    Connected,
    PollingFallback,
    Disabled,
}

impl AcceleratorCoordinationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::PollingFallback => "polling_fallback",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceleratorRelayStatus {
    Active,
    Blocked,
    DirectFallback,
    Queued,
    NotConfigured,
}

impl AcceleratorRelayStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Blocked => "blocked",
            Self::DirectFallback => "direct_fallback",
            Self::Queued => "queued",
            Self::NotConfigured => "not_configured",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AcceleratorStatus {
    pub accelerator_mode: String,
    pub accelerator_endpoint: Option<String>,
    pub accelerator_health: AcceleratorHealth,
    pub cleanup_owner: AcceleratorCleanupOwner,
    pub coordination_status: AcceleratorCoordinationStatus,
    pub relay_status: AcceleratorRelayStatus,
}

impl AcceleratorStatus {
    pub fn from_parts(
        accelerator_mode: Option<AcceleratorMode>,
        accelerator_endpoint: Option<&str>,
        event_endpoint: Option<&str>,
        event_poll_interval_ms: Option<u64>,
        relay_fallback_policy: Option<RelayOutagePolicy>,
        disable_cleanup: bool,
    ) -> Self {
        let accelerator_mode_label = accelerator_mode
            .map(|mode| mode.as_str().to_string())
            .unwrap_or_else(|| "not_configured".to_string());
        let accelerator_endpoint = accelerator_endpoint.map(redact_accelerator_endpoint);

        let coordination_status = match accelerator_mode {
            None => AcceleratorCoordinationStatus::Disabled,
            Some(_) if event_endpoint.is_some() => AcceleratorCoordinationStatus::Connected,
            Some(_) if event_poll_interval_ms.unwrap_or(0) > 0 => {
                AcceleratorCoordinationStatus::PollingFallback
            }
            Some(_) => AcceleratorCoordinationStatus::Disabled,
        };

        let relay_status = match accelerator_mode {
            None | Some(AcceleratorMode::Direct) | Some(AcceleratorMode::DirectPlusCache) => {
                AcceleratorRelayStatus::NotConfigured
            }
            Some(AcceleratorMode::RelayWrite) => {
                if accelerator_endpoint.is_none() {
                    AcceleratorRelayStatus::Blocked
                } else {
                    match relay_fallback_policy.unwrap_or(RelayOutagePolicy::FailClosed) {
                        RelayOutagePolicy::FailClosed => AcceleratorRelayStatus::Active,
                        RelayOutagePolicy::DirectWriteFallback => {
                            AcceleratorRelayStatus::DirectFallback
                        }
                        RelayOutagePolicy::QueueAndRetry => AcceleratorRelayStatus::Queued,
                    }
                }
            }
        };

        let cleanup_owner = match accelerator_mode {
            Some(_) => AcceleratorCleanupOwner::Hosted,
            None if disable_cleanup => AcceleratorCleanupOwner::Unknown,
            None => AcceleratorCleanupOwner::Local,
        };

        let accelerator_health = match accelerator_mode {
            None => AcceleratorHealth::NotConfigured,
            Some(_) if accelerator_endpoint.is_none() => AcceleratorHealth::Disconnected,
            Some(_) if matches!(relay_status, AcceleratorRelayStatus::Blocked) => {
                AcceleratorHealth::Disconnected
            }
            Some(_)
                if matches!(
                    coordination_status,
                    AcceleratorCoordinationStatus::PollingFallback
                ) || matches!(
                    relay_status,
                    AcceleratorRelayStatus::DirectFallback | AcceleratorRelayStatus::Queued
                ) =>
            {
                AcceleratorHealth::Degraded
            }
            Some(_) if matches!(coordination_status, AcceleratorCoordinationStatus::Disabled) => {
                AcceleratorHealth::Degraded
            }
            Some(_) => AcceleratorHealth::Healthy,
        };

        Self {
            accelerator_mode: accelerator_mode_label,
            accelerator_endpoint,
            accelerator_health,
            cleanup_owner,
            coordination_status,
            relay_status,
        }
    }

    pub const fn is_warnworthy(&self) -> bool {
        !matches!(
            self.accelerator_health,
            AcceleratorHealth::Healthy | AcceleratorHealth::NotConfigured
        )
    }
}

/// Strip credentials and query parameters from an accelerator endpoint URL
/// before logging or telemetry emission.
pub fn redact_accelerator_endpoint(endpoint: &str) -> String {
    let without_fragment = endpoint.split('#').next().unwrap_or(endpoint);
    let without_query = without_fragment
        .split('?')
        .next()
        .unwrap_or(without_fragment);
    let mut redacted = without_query.to_string();
    if let Some(at_index) = redacted.find('@')
        && let Some(scheme_index) = redacted.find("://")
        && at_index > scheme_index + 3
    {
        redacted.replace_range(scheme_index + 3..at_index, "<redacted>");
    }
    redacted
}

// ── JSONL perf logger ─────────────────────────────────────────────────────

/// Thread-safe JSONL logger for capturing perf events emitted by ClawFS.
pub struct PerfLogger {
    writer: Mutex<BufWriter<File>>,
}

impl PerfLogger {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
        })
    }

    pub fn log<D: Serialize>(&self, event: &str, duration: Duration, details: D) {
        let record = json!({
            "ts": OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "unknown".to_string()),
            "event": event,
            "duration_ms": duration.as_secs_f64() * 1000.0,
            "details": details,
        });
        let mut guard = self.writer.lock();
        if serde_json::to_writer(&mut *guard, &record).is_ok() && guard.write_all(b"\n").is_ok() {
            if event == "flush_pending" {
                let _ = guard.flush();
            }
        } else {
            log::warn!("failed to write perf log event {event}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::path::Path;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;

    use crate::perf_bench::{generate_incompressible_payload, perf_config, summarize_samples};
    use crate::segment::{SegmentEntry, SegmentManager, SegmentPayload};

    fn wait_for_cache_file(path: &Path) {
        for _ in 0..50 {
            if path.exists() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("expected cache file {}", path.display());
    }

    #[test]
    fn perf_segment_cache_range_read_lift() {
        if env::var("CLAWFS_RUN_PERF").is_err() {
            eprintln!(
                "skipping perf_segment_cache_range_read_lift; set CLAWFS_RUN_PERF=1 to enable"
            );
            return;
        }

        let dir = tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let mut config = perf_config(dir.path());
        config.segment_cache_bytes = 256 * 1024 * 1024;
        config.segment_compression = false;
        let manager = SegmentManager::new(&config, runtime.handle().clone()).unwrap();

        let entry_count = 16u64;
        let entry_bytes = 4 * 1024 * 1024usize;
        let payload = generate_incompressible_payload(entry_bytes);
        let entries = (0..entry_count)
            .map(|i| SegmentEntry {
                inode: i + 1,
                path: format!("/bench_{i:02}.bin"),
                logical_offset: 0,
                payload: SegmentPayload::Bytes(payload.clone()),
            })
            .collect::<Vec<_>>();
        let pointers = manager.write_batch(1, 1, entries).unwrap();
        let target_pointer = pointers[(entry_count as usize) / 2].1.pointer.clone();

        let cache_path =
            manager.benchmark_cache_path(target_pointer.generation, target_pointer.segment_id);
        wait_for_cache_file(&cache_path);

        let warmups = 5usize;
        for _ in 0..warmups {
            let baseline = manager
                .benchmark_read_from_cache_full_file(&target_pointer)
                .unwrap()
                .unwrap();
            let optimized = manager
                .benchmark_read_from_cache_range(&target_pointer)
                .unwrap()
                .unwrap();
            assert_eq!(baseline, optimized);
        }

        let iterations = 30usize;
        let mut baseline_ms = Vec::with_capacity(iterations);
        let mut optimized_ms = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let start = Instant::now();
            let baseline = manager
                .benchmark_read_from_cache_full_file(&target_pointer)
                .unwrap()
                .unwrap();
            baseline_ms.push(start.elapsed().as_secs_f64() * 1000.0);

            let start = Instant::now();
            let optimized = manager
                .benchmark_read_from_cache_range(&target_pointer)
                .unwrap()
                .unwrap();
            optimized_ms.push(start.elapsed().as_secs_f64() * 1000.0);

            assert_eq!(baseline, optimized);
        }

        let baseline = summarize_samples(&baseline_ms);
        let optimized = summarize_samples(&optimized_ms);
        let speedup = baseline.mean / optimized.mean;
        eprintln!(
            concat!(
                "perf_segment_cache_range_read_lift ",
                "segment_entries={} entry_bytes={} cache_file_mb={:.1} ",
                "baseline_mean_ms={:.3} optimized_mean_ms={:.3} speedup={:.2}x ",
                "baseline_p50_ms={:.3} optimized_p50_ms={:.3}"
            ),
            entry_count,
            entry_bytes,
            (entry_count as f64 * entry_bytes as f64) / (1024.0 * 1024.0),
            baseline.mean,
            optimized.mean,
            speedup,
            baseline.median,
            optimized.median,
        );

        assert!(
            speedup > 1.2,
            "expected range cache read benchmark to improve over full-file cache read: baseline_mean_ms={:.3} optimized_mean_ms={:.3}",
            baseline.mean,
            optimized.mean
        );
    }
}
