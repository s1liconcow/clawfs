//! Per-volume accelerator policy loaded from bucket-resident policy objects.
//!
//! Each managed volume may carry a policy file at:
//!   `<volume_prefix>/metadata/accelerator_policy.json`
//!
//! The policy controls which org-worker subsystems operate on the volume and
//! what timing/retention parameters they use.  Missing fields fall back to
//! defaults that mirror the existing single-volume worker behaviour so
//! operators get familiar semantics without a policy file.
//!
//! A missing policy file is normal (all defaults apply).  An invalid JSON
//! file marks the volume as `UnhealthyPolicy` for that volume only; other
//! volumes continue to operate.

use std::time::Duration;

use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use serde::Deserialize;
use tracing::warn;

use crate::maintenance::{CheckpointConfig, LifecyclePolicy};

/// Path suffix for the policy object, relative to a volume prefix.
pub const POLICY_OBJECT_SUFFIX: &str = "metadata/accelerator_policy.json";

// ── Default helpers ────────────────────────────────────────────────────────

fn default_true() -> bool {
    true
}
fn default_checkpoint_interval_secs() -> u64 {
    86_400 // 24 h
}
fn default_checkpoint_max_checkpoints() -> usize {
    7
}
fn default_checkpoint_retention_days() -> u64 {
    7
}
fn default_lifecycle_expiry_days() -> u64 {
    30
}
fn default_lifecycle_require_confirmation() -> bool {
    true
}

// ── Policy struct ──────────────────────────────────────────────────────────

/// Bucket-resident policy object for a single ClawFS volume managed by the
/// org-scoped hosted accelerator worker.
///
/// All fields are optional in the JSON representation. Missing fields use the
/// defaults defined by the `Default` impl, which mirror the current
/// single-volume maintenance worker behaviour.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct VolumeAcceleratorPolicy {
    // ── Maintenance ──────────────────────────────────────────────────────
    /// Enable delta/segment compaction maintenance for this volume.
    /// Default: true.
    #[serde(default = "default_true")]
    pub maintenance_enabled: bool,

    // ── Relay ────────────────────────────────────────────────────────────
    /// Enable relay_write ownership and commit forwarding for this volume.
    /// Default: false (relay is opt-in).
    pub relay_enabled: bool,

    /// Relay outage fallback policy ("fail_closed", "direct_write_fallback",
    /// "queue_and_retry").  When absent the relay client default is used
    /// (fail_closed).
    pub relay_fallback_policy: Option<String>,

    // ── Checkpoints ──────────────────────────────────────────────────────
    /// Enable periodic checkpoint creation.  Default: true.
    #[serde(default = "default_true")]
    pub checkpoint_enabled: bool,

    /// Interval between checkpoints (seconds).  Default: 86400 (24 h).
    #[serde(default = "default_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,

    /// Maximum number of checkpoints to retain.  Default: 7.
    #[serde(default = "default_checkpoint_max_checkpoints")]
    pub checkpoint_max_checkpoints: usize,

    /// Retention window for checkpoints (days).  Default: 7.
    #[serde(default = "default_checkpoint_retention_days")]
    pub checkpoint_retention_days: u64,

    // ── Lifecycle cleanup ─────────────────────────────────────────────────
    /// Enable lifecycle object cleanup for this volume.  Default: false.
    pub lifecycle_cleanup: bool,

    /// Delete objects older than this many days.  Default: 30.
    #[serde(default = "default_lifecycle_expiry_days")]
    pub lifecycle_expiry_days: u64,

    /// Require explicit confirmation before lifecycle deletes.  Default: true.
    #[serde(default = "default_lifecycle_require_confirmation")]
    pub lifecycle_require_confirmation: bool,
}

impl Default for VolumeAcceleratorPolicy {
    fn default() -> Self {
        Self {
            maintenance_enabled: true,
            relay_enabled: false,
            relay_fallback_policy: None,
            checkpoint_enabled: true,
            checkpoint_interval_secs: default_checkpoint_interval_secs(),
            checkpoint_max_checkpoints: default_checkpoint_max_checkpoints(),
            checkpoint_retention_days: default_checkpoint_retention_days(),
            lifecycle_cleanup: false,
            lifecycle_expiry_days: default_lifecycle_expiry_days(),
            lifecycle_require_confirmation: true,
        }
    }
}

impl VolumeAcceleratorPolicy {
    /// Convert to a `CheckpointConfig` for use with the maintenance module.
    ///
    /// Returns `None` when checkpoint maintenance is disabled or the interval
    /// is zero.
    pub fn checkpoint_config(&self) -> Option<CheckpointConfig> {
        if !self.checkpoint_enabled || self.checkpoint_interval_secs == 0 {
            return None;
        }
        Some(CheckpointConfig {
            interval: Duration::from_secs(self.checkpoint_interval_secs),
            max_checkpoints: self.checkpoint_max_checkpoints,
            retention_days: self.checkpoint_retention_days,
        })
    }

    /// Convert to a `LifecyclePolicy` for use with the maintenance module.
    ///
    /// Returns `None` when lifecycle cleanup is disabled or expiry is zero.
    /// The `allowed_prefix` is taken from the caller so the policy struct
    /// itself does not need to know its own volume prefix.
    pub fn lifecycle_policy(&self, allowed_prefix: impl Into<String>) -> Option<LifecyclePolicy> {
        if !self.lifecycle_cleanup || self.lifecycle_expiry_days == 0 {
            return None;
        }
        let allowed = allowed_prefix.into();
        if allowed.is_empty() {
            warn!("lifecycle cleanup enabled but volume prefix is empty; skipping");
            return None;
        }
        Some(LifecyclePolicy {
            expiry_days: self.lifecycle_expiry_days,
            require_confirmation: self.lifecycle_require_confirmation,
            allowed_prefix: allowed,
        })
    }
}

// ── Loading helpers ────────────────────────────────────────────────────────

/// Outcome of attempting to load a volume's accelerator policy.
#[derive(Debug)]
pub enum PolicyLoadResult {
    /// Policy loaded (or defaults used because the file was absent).
    Ok(VolumeAcceleratorPolicy),
    /// Policy file exists but is invalid JSON or contains invalid values.
    /// The contained error message is for logging only.
    Invalid(String),
}

/// Load `accelerator_policy.json` from the object store for a given volume
/// prefix.
///
/// Returns `PolicyLoadResult::Ok(defaults)` when the policy file is absent so
/// callers always receive a valid policy.  Returns
/// `PolicyLoadResult::Invalid(reason)` when the file exists but cannot be
/// parsed.
pub async fn load_volume_policy(store: &dyn ObjectStore, volume_prefix: &str) -> PolicyLoadResult {
    let key = policy_object_path(volume_prefix);
    match store.get(&key).await {
        Err(object_store::Error::NotFound { .. }) => {
            // Absent file → use defaults (normal case).
            PolicyLoadResult::Ok(VolumeAcceleratorPolicy::default())
        }
        Err(err) => {
            // Transient error: treat as missing for now; caller may retry.
            warn!(
                volume_prefix = %volume_prefix,
                error = %err,
                "policy load error (treating as missing)"
            );
            PolicyLoadResult::Ok(VolumeAcceleratorPolicy::default())
        }
        Ok(get_result) => match get_result.bytes().await {
            Err(err) => PolicyLoadResult::Invalid(format!("failed to read policy bytes: {err}")),
            Ok(bytes) => match serde_json::from_slice::<VolumeAcceleratorPolicy>(&bytes) {
                Ok(policy) => PolicyLoadResult::Ok(policy),
                Err(err) => PolicyLoadResult::Invalid(format!("invalid policy JSON: {err}")),
            },
        },
    }
}

/// Build the object store path for a volume's accelerator policy.
pub fn policy_object_path(volume_prefix: &str) -> ObjectPath {
    let trimmed = volume_prefix.trim_matches('/');
    if trimmed.is_empty() {
        ObjectPath::from(POLICY_OBJECT_SUFFIX)
    } else {
        ObjectPath::from(format!("{trimmed}/{POLICY_OBJECT_SUFFIX}"))
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let p = VolumeAcceleratorPolicy::default();
        assert!(p.maintenance_enabled);
        assert!(!p.relay_enabled);
        assert!(p.checkpoint_enabled);
        assert_eq!(p.checkpoint_interval_secs, 86_400);
        assert!(!p.lifecycle_cleanup);
    }

    #[test]
    fn deserialize_empty_json_uses_defaults() {
        let p: VolumeAcceleratorPolicy = serde_json::from_str("{}").unwrap();
        assert!(p.maintenance_enabled);
        assert!(!p.relay_enabled);
        assert!(p.checkpoint_enabled);
        assert_eq!(p.checkpoint_interval_secs, 86_400);
        assert_eq!(p.checkpoint_max_checkpoints, 7);
    }

    #[test]
    fn deserialize_partial_overrides() {
        let json = r#"{"relay_enabled": true, "checkpoint_interval_secs": 3600}"#;
        let p: VolumeAcceleratorPolicy = serde_json::from_str(json).unwrap();
        assert!(p.maintenance_enabled); // default
        assert!(p.relay_enabled); // overridden
        assert_eq!(p.checkpoint_interval_secs, 3600); // overridden
        assert_eq!(p.checkpoint_max_checkpoints, 7); // default
    }

    #[test]
    fn checkpoint_config_disabled_when_not_enabled() {
        let p = VolumeAcceleratorPolicy {
            checkpoint_enabled: false,
            ..Default::default()
        };
        assert!(p.checkpoint_config().is_none());
    }

    #[test]
    fn lifecycle_policy_respects_allowed_prefix() {
        let p = VolumeAcceleratorPolicy {
            lifecycle_cleanup: true,
            ..Default::default()
        };
        let lp = p.lifecycle_policy("orgs/myorg/vol1").unwrap();
        assert_eq!(lp.allowed_prefix, "orgs/myorg/vol1");
        assert_eq!(lp.expiry_days, 30);
    }

    #[test]
    fn lifecycle_policy_none_when_disabled() {
        let p = VolumeAcceleratorPolicy::default();
        assert!(p.lifecycle_policy("orgs/myorg/vol1").is_none());
    }

    #[test]
    fn policy_object_path_with_prefix() {
        let path = policy_object_path("orgs/myorg/vol1");
        assert_eq!(
            path.as_ref(),
            "orgs/myorg/vol1/metadata/accelerator_policy.json"
        );
    }

    #[test]
    fn policy_object_path_empty_prefix() {
        let path = policy_object_path("");
        assert_eq!(path.as_ref(), "metadata/accelerator_policy.json");
    }
}
