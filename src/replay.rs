use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use flate2::Compression;
use flate2::write::GzEncoder;
use parking_lot::Mutex;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// Thread-safe compressed JSONL logger for recording replayable IO operations.
pub struct ReplayLogger {
    writer: Mutex<GzEncoder<BufWriter<File>>>,
    start: Instant,
    seq: AtomicU64,
    unflushed: AtomicU64,
    last_flush: Mutex<Instant>,
}

impl ReplayLogger {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: Mutex::new(GzEncoder::new(BufWriter::new(file), Compression::default())),
            start: Instant::now(),
            seq: AtomicU64::new(0),
            unflushed: AtomicU64::new(0),
            last_flush: Mutex::new(Instant::now()),
        })
    }

    pub fn log_op(
        &self,
        layer: &str,
        op: &str,
        start_offset: Duration,
        duration: Duration,
        errno: Option<i32>,
        details: Value,
    ) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let record = json!({
            "schema": "clawfs-replay-v1",
            "seq": seq,
            "ts": OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "unknown".to_string()),
            "layer": layer,
            "op": op,
            "start_us": start_offset.as_micros(),
            "duration_us": duration.as_micros(),
            "ok": errno.is_none(),
            "errno": errno,
            "details": details,
        });
        let mut guard = self.writer.lock();
        if serde_json::to_writer(&mut *guard, &record).is_ok() && guard.write_all(b"\n").is_ok() {
            let unflushed = self.unflushed.fetch_add(1, Ordering::Relaxed);
            let should_flush = if unflushed >= 64 {
                true
            } else {
                self.last_flush.lock().elapsed() > Duration::from_millis(500)
            };
            if should_flush {
                self.unflushed.store(0, Ordering::Relaxed);
                *self.last_flush.lock() = Instant::now();
                let _ = guard.flush();
            }
        } else {
            log::warn!("failed to write replay log event {layer}/{op}");
        }
    }

    pub fn log_meta(&self, op: &str, details: Value) {
        self.log_op(
            "meta",
            op,
            Duration::from_secs(0),
            Duration::from_secs(0),
            None,
            details,
        );
    }

    pub fn elapsed_since_start(&self, start: Instant) -> Duration {
        let now = Instant::now();
        let anchor = now.checked_duration_since(self.start).unwrap_or_default();
        let from_start = now.checked_duration_since(start).unwrap_or_default();
        anchor.checked_sub(from_start).unwrap_or_default()
    }
}

impl Drop for ReplayLogger {
    fn drop(&mut self) {
        let _ = self.writer.lock().flush();
    }
}
