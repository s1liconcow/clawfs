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
