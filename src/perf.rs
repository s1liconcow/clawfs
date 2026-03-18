use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::json;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::clawfs::AcceleratorMode;
use crate::config::Config;
use crate::relay::RelayOutagePolicy;

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
    pub fn from_config(config: &Config) -> Self {
        let accelerator_endpoint = config.accelerator_endpoint.as_deref();
        let event_endpoint = env_var("CLAWFS_EVENT_ENDPOINT");
        let event_poll_interval_ms =
            env_var("CLAWFS_EVENT_POLL_INTERVAL_MS").and_then(|value| value.parse::<u64>().ok());
        let relay_fallback_policy = config.relay_fallback_policy.or_else(|| {
            env_var("CLAWFS_RELAY_FALLBACK_POLICY")
                .and_then(|value| RelayOutagePolicy::from_str(&value).ok())
        });

        Self::from_parts(
            config.accelerator_mode,
            accelerator_endpoint,
            event_endpoint.as_deref(),
            event_poll_interval_ms,
            relay_fallback_policy,
            config.disable_cleanup,
        )
    }

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

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(value)
    })
}

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
    use super::{
        AcceleratorCleanupOwner, AcceleratorCoordinationStatus, AcceleratorHealth, AcceleratorMode,
        AcceleratorRelayStatus, AcceleratorStatus, redact_accelerator_endpoint,
    };

    #[test]
    fn accelerator_status_defaults_to_not_configured() {
        let status = AcceleratorStatus::from_parts(None, None, None, None, None, false);

        assert_eq!(status.accelerator_mode, "not_configured");
        assert_eq!(status.accelerator_endpoint, None);
        assert_eq!(status.accelerator_health, AcceleratorHealth::NotConfigured);
        assert_eq!(status.cleanup_owner, AcceleratorCleanupOwner::Local);
        assert_eq!(
            status.coordination_status,
            AcceleratorCoordinationStatus::Disabled
        );
        assert_eq!(status.relay_status, AcceleratorRelayStatus::NotConfigured);
    }

    #[test]
    fn accelerator_status_redacts_and_marks_relay_fallback() {
        let status = AcceleratorStatus::from_parts(
            Some(AcceleratorMode::RelayWrite),
            Some("https://user:secret@example.com/path?token=abc#frag"),
            Some("https://events.example.com/stream"),
            Some(2500),
            Some(crate::relay::RelayOutagePolicy::DirectWriteFallback),
            true,
        );

        assert_eq!(
            status.accelerator_endpoint.as_deref(),
            Some("https://<redacted>@example.com/path")
        );
        assert_eq!(
            status.coordination_status,
            AcceleratorCoordinationStatus::Connected
        );
        assert_eq!(status.relay_status, AcceleratorRelayStatus::DirectFallback);
        assert_eq!(status.cleanup_owner, AcceleratorCleanupOwner::Hosted);
        assert_eq!(status.accelerator_health, AcceleratorHealth::Degraded);
    }

    #[test]
    fn accelerator_status_marks_queueing_as_degraded() {
        let status = AcceleratorStatus::from_parts(
            Some(AcceleratorMode::RelayWrite),
            Some("https://accelerator.example.com"),
            None,
            None,
            Some(crate::relay::RelayOutagePolicy::QueueAndRetry),
            false,
        );

        assert_eq!(status.relay_status, AcceleratorRelayStatus::Queued);
        assert_eq!(status.accelerator_health, AcceleratorHealth::Degraded);
    }

    #[test]
    fn redact_accelerator_endpoint_strips_query_and_userinfo() {
        assert_eq!(
            redact_accelerator_endpoint("https://user:secret@example.com/path?token=abc#frag"),
            "https://<redacted>@example.com/path"
        );
    }
}
