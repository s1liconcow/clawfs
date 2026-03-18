use std::env;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::config::{Config, ObjectStoreProvider};

pub const STORAGE_MODE_ENV: &str = "CLAWFS_STORAGE_MODE";
const OBJECT_PROVIDER_ENV: &str = "CLAWFS_OBJECT_PROVIDER";
const BUCKET_ENV: &str = "CLAWFS_BUCKET";
const REGION_ENV: &str = "CLAWFS_REGION";
const ENDPOINT_ENV: &str = "CLAWFS_ENDPOINT";
const OBJECT_PREFIX_ENV: &str = "CLAWFS_OBJECT_PREFIX";
const STATE_PATH_ENV: &str = "CLAWFS_STATE_PATH";
const LOCAL_CACHE_PATH_ENV: &str = "CLAWFS_LOCAL_CACHE_PATH";
const MAX_PENDING_BYTES_ENV: &str = "CLAWFS_MAX_PENDING_BYTES";
const MAX_SEGMENT_CACHE_BYTES_ENV: &str = "CLAWFS_MAX_SEGMENT_CACHE_BYTES";
const MAX_LOGICAL_BYTES_ENV: &str = "CLAWFS_MAX_LOGICAL_BYTES";
const MAX_CHECKPOINTS_ENV: &str = "CLAWFS_MAX_CHECKPOINTS";
const IDLE_EXPIRY_SECS_ENV: &str = "CLAWFS_IDLE_EXPIRY_SECS";
const CLAWFS_AWS_ACCESS_KEY_ID_ENV: &str = "CLAWFS_AWS_ACCESS_KEY_ID";
const CLAWFS_AWS_SECRET_ACCESS_KEY_ENV: &str = "CLAWFS_AWS_SECRET_ACCESS_KEY";
const CLAWFS_AWS_SESSION_TOKEN_ENV: &str = "CLAWFS_AWS_SESSION_TOKEN";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    HostedFree,
    ByobPaid,
}

impl FromStr for StorageMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "hosted_free" => Ok(Self::HostedFree),
            "byob_paid" => Ok(Self::ByobPaid),
            other => bail!(
                "unsupported {STORAGE_MODE_ENV} value {other:?}; expected hosted_free or byob_paid"
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceleratorMode {
    #[default]
    Direct,
    DirectPlusCache,
    RelayWrite,
}

impl AcceleratorMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::DirectPlusCache => "direct_plus_cache",
            Self::RelayWrite => "relay_write",
        }
    }
}

impl AcceleratorMode {
    pub const fn default_fallback_policy(self) -> AcceleratorFallbackPolicy {
        match self {
            Self::Direct | Self::DirectPlusCache => AcceleratorFallbackPolicy::PollAndDirect,
            Self::RelayWrite => AcceleratorFallbackPolicy::FailClosed,
        }
    }

    pub const fn default_journal_clearing(self) -> AcceleratorJournalClearingRule {
        match self {
            Self::Direct | Self::DirectPlusCache => {
                AcceleratorJournalClearingRule::AfterLocalCommit
            }
            Self::RelayWrite => AcceleratorJournalClearingRule::AfterCommittedGeneration,
        }
    }
}

impl FromStr for AcceleratorMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "direct" => Ok(Self::Direct),
            "direct_plus_cache" => Ok(Self::DirectPlusCache),
            "relay_write" => Ok(Self::RelayWrite),
            other => bail!(
                "unsupported accelerator mode {other:?}; expected direct, direct_plus_cache, or relay_write"
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceleratorFallbackPolicy {
    #[default]
    PollAndDirect,
    FailClosed,
    DirectWriteFallback,
}

impl AcceleratorFallbackPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PollAndDirect => "poll_and_direct",
            Self::FailClosed => "fail_closed",
            Self::DirectWriteFallback => "direct_write_fallback",
        }
    }
}

impl AcceleratorFallbackPolicy {
    pub const fn normalize_for_mode(self, mode: AcceleratorMode) -> Self {
        match mode {
            AcceleratorMode::Direct | AcceleratorMode::DirectPlusCache => {
                AcceleratorFallbackPolicy::PollAndDirect
            }
            AcceleratorMode::RelayWrite => match self {
                AcceleratorFallbackPolicy::DirectWriteFallback => {
                    AcceleratorFallbackPolicy::DirectWriteFallback
                }
                _ => AcceleratorFallbackPolicy::FailClosed,
            },
        }
    }
}

impl FromStr for AcceleratorFallbackPolicy {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "poll_and_direct" => Ok(Self::PollAndDirect),
            "fail_closed" => Ok(Self::FailClosed),
            "direct_write_fallback" => Ok(Self::DirectWriteFallback),
            other => bail!(
                "unsupported accelerator fallback policy {other:?}; expected poll_and_direct, fail_closed, or direct_write_fallback"
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceleratorAuthorityBoundary {
    pub object_store_authoritative: bool,
    pub superblock_generation_authoritative: bool,
    pub caches_advisory: bool,
    pub event_streams_advisory: bool,
}

impl AcceleratorAuthorityBoundary {
    pub const fn new() -> Self {
        Self {
            object_store_authoritative: true,
            superblock_generation_authoritative: true,
            caches_advisory: true,
            event_streams_advisory: true,
        }
    }
}

impl Default for AcceleratorAuthorityBoundary {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceleratorJournalClearingRule {
    #[default]
    AfterLocalCommit,
    AfterCommittedGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceleratorContract {
    pub mode: AcceleratorMode,
    pub fallback_policy: AcceleratorFallbackPolicy,
    pub authority: AcceleratorAuthorityBoundary,
    pub journal_clearing: AcceleratorJournalClearingRule,
}

impl AcceleratorContract {
    pub const fn for_mode(mode: AcceleratorMode) -> Self {
        Self {
            mode,
            fallback_policy: mode.default_fallback_policy(),
            authority: AcceleratorAuthorityBoundary::new(),
            journal_clearing: mode.default_journal_clearing(),
        }
    }

    pub const fn with_fallback_policy(
        mut self,
        fallback_policy: AcceleratorFallbackPolicy,
    ) -> Self {
        self.fallback_policy = fallback_policy;
        self
    }

    pub const fn normalized(self) -> Self {
        Self {
            mode: self.mode,
            fallback_policy: self.fallback_policy.normalize_for_mode(self.mode),
            authority: AcceleratorAuthorityBoundary::new(),
            journal_clearing: self.mode.default_journal_clearing(),
        }
    }
}

impl Default for AcceleratorContract {
    fn default() -> Self {
        Self::for_mode(AcceleratorMode::default())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostedFreeLimits {
    pub max_pending_bytes: Option<u64>,
    pub max_segment_cache_bytes: Option<u64>,
    pub max_logical_bytes: Option<u64>,
    pub max_checkpoints: Option<u32>,
    pub idle_expiry_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSpec {
    pub storage_mode: StorageMode,
    pub object_provider: Option<ObjectStoreProvider>,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub object_prefix: Option<String>,
    pub state_path: Option<PathBuf>,
    pub local_cache_path: Option<PathBuf>,
    pub hosted_limits: HostedFreeLimits,
}

impl RuntimeSpec {
    fn apply(&self, config: &mut Config) -> Result<()> {
        if let Some(provider) = self.object_provider {
            config.object_provider = provider;
        }
        if let Some(bucket) = &self.bucket {
            config.bucket = Some(bucket.clone());
        }
        if let Some(region) = &self.region {
            config.region = Some(region.clone());
        }
        if let Some(endpoint) = &self.endpoint {
            config.endpoint = Some(endpoint.clone());
        }
        if let Some(prefix) = &self.object_prefix {
            config.object_prefix = prefix.clone();
        }
        if let Some(state_path) = &self.state_path {
            config.state_path = state_path.clone();
        }
        if let Some(local_cache_path) = &self.local_cache_path {
            config.local_cache_path = local_cache_path.clone();
        }

        if self.storage_mode == StorageMode::HostedFree {
            if let Some(limit) = self.hosted_limits.max_pending_bytes {
                config.pending_bytes = config.pending_bytes.min(limit.max(1024));
            }
            if let Some(limit) = self.hosted_limits.max_segment_cache_bytes {
                config.segment_cache_bytes = config.segment_cache_bytes.min(limit.max(1024));
            }
        }

        validate_runtime_config(self.storage_mode, config)
    }
}

pub fn apply_env_runtime_spec(config: &mut Config) -> Result<Option<RuntimeSpec>> {
    let Some(spec) = load_runtime_spec_from_env()? else {
        return Ok(None);
    };
    spec.apply(config)?;
    Ok(Some(spec))
}

pub fn load_runtime_spec_from_env() -> Result<Option<RuntimeSpec>> {
    let Some(storage_mode) = env_var(STORAGE_MODE_ENV) else {
        return Ok(None);
    };
    let storage_mode = StorageMode::from_str(&storage_mode)?;
    let object_provider = env_var(OBJECT_PROVIDER_ENV)
        .map(|value| parse_object_provider(OBJECT_PROVIDER_ENV, &value))
        .transpose()?;
    let bucket = env_var(BUCKET_ENV);
    let region = env_var(REGION_ENV);
    let endpoint = env_var(ENDPOINT_ENV);
    let object_prefix = env_var(OBJECT_PREFIX_ENV);
    let state_path = env_path(STATE_PATH_ENV);
    let local_cache_path = env_path(LOCAL_CACHE_PATH_ENV);
    let hosted_limits = HostedFreeLimits {
        max_pending_bytes: parse_optional_u64(MAX_PENDING_BYTES_ENV)?,
        max_segment_cache_bytes: parse_optional_u64(MAX_SEGMENT_CACHE_BYTES_ENV)?,
        max_logical_bytes: parse_optional_u64(MAX_LOGICAL_BYTES_ENV)?,
        max_checkpoints: parse_optional_u32(MAX_CHECKPOINTS_ENV)?,
        idle_expiry_secs: parse_optional_u64(IDLE_EXPIRY_SECS_ENV)?,
    };
    Ok(Some(RuntimeSpec {
        storage_mode,
        object_provider,
        bucket,
        region,
        endpoint,
        object_prefix,
        state_path,
        local_cache_path,
        hosted_limits,
    }))
}

pub fn aws_access_key_id() -> Option<String> {
    env_var(CLAWFS_AWS_ACCESS_KEY_ID_ENV).or_else(|| env_var("AWS_ACCESS_KEY_ID"))
}

pub fn aws_secret_access_key() -> Option<String> {
    env_var(CLAWFS_AWS_SECRET_ACCESS_KEY_ENV).or_else(|| env_var("AWS_SECRET_ACCESS_KEY"))
}

pub fn aws_session_token() -> Option<String> {
    env_var(CLAWFS_AWS_SESSION_TOKEN_ENV).or_else(|| env_var("AWS_SESSION_TOKEN"))
}

fn validate_runtime_config(storage_mode: StorageMode, config: &Config) -> Result<()> {
    if storage_mode == StorageMode::HostedFree
        && config.object_prefix.trim().trim_matches('/').is_empty()
    {
        bail!("{OBJECT_PREFIX_ENV} is required for hosted_free volumes");
    }
    match config.object_provider {
        ObjectStoreProvider::Local => {}
        ObjectStoreProvider::Aws | ObjectStoreProvider::Gcs => {
            if config
                .bucket
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
            {
                bail!(
                    "{} is required when {:?} object storage is selected",
                    BUCKET_ENV,
                    config.object_provider
                );
            }
        }
    }
    Ok(())
}

fn env_var(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_path(name: &str) -> Option<PathBuf> {
    env_var(name).map(PathBuf::from)
}

fn parse_optional_u64(name: &str) -> Result<Option<u64>> {
    env_var(name)
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("parsing {name} as u64"))
        })
        .transpose()
}

fn parse_optional_u32(name: &str) -> Result<Option<u32>> {
    env_var(name)
        .map(|value| {
            value
                .parse::<u32>()
                .with_context(|| format!("parsing {name} as u32"))
        })
        .transpose()
}

fn parse_object_provider(name: &str, value: &str) -> Result<ObjectStoreProvider> {
    match value.trim().to_ascii_lowercase().as_str() {
        "local" => Ok(ObjectStoreProvider::Local),
        "aws" => Ok(ObjectStoreProvider::Aws),
        "gcs" => Ok(ObjectStoreProvider::Gcs),
        other => bail!("unsupported {name} value {other:?}; expected local, aws, or gcs"),
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::{LazyLock, Mutex};

    use clap::Parser;

    use super::{
        AcceleratorAuthorityBoundary, AcceleratorContract, AcceleratorFallbackPolicy,
        AcceleratorJournalClearingRule, AcceleratorMode, BUCKET_ENV, MAX_PENDING_BYTES_ENV,
        MAX_SEGMENT_CACHE_BYTES_ENV, OBJECT_PREFIX_ENV, OBJECT_PROVIDER_ENV, STORAGE_MODE_ENV,
        StorageMode, apply_env_runtime_spec, load_runtime_spec_from_env,
    };
    use crate::config::{Cli, Config};

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
    const TEST_ENV_VARS: &[&str] = &[
        STORAGE_MODE_ENV,
        OBJECT_PROVIDER_ENV,
        BUCKET_ENV,
        OBJECT_PREFIX_ENV,
        MAX_PENDING_BYTES_ENV,
        MAX_SEGMENT_CACHE_BYTES_ENV,
        "CLAWFS_REGION",
        "CLAWFS_ENDPOINT",
        "CLAWFS_STATE_PATH",
        "CLAWFS_LOCAL_CACHE_PATH",
        "CLAWFS_MAX_LOGICAL_BYTES",
        "CLAWFS_MAX_CHECKPOINTS",
        "CLAWFS_IDLE_EXPIRY_SECS",
    ];

    #[test]
    fn load_runtime_spec_is_noop_without_env() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_test_env();
        assert_eq!(load_runtime_spec_from_env().expect("load spec"), None);
    }

    #[test]
    fn hosted_free_env_overrides_and_clamps_config() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_test_env();
        set_env(STORAGE_MODE_ENV, "hosted_free");
        set_env(OBJECT_PROVIDER_ENV, "aws");
        set_env(BUCKET_ENV, "clawfs-free");
        set_env(OBJECT_PREFIX_ENV, "free/account-a/vol-1");
        set_env(MAX_PENDING_BYTES_ENV, "2048");
        set_env(MAX_SEGMENT_CACHE_BYTES_ENV, "4096");

        let mut config = base_config();
        config.pending_bytes = 65_536;
        config.segment_cache_bytes = 131_072;

        let spec = apply_env_runtime_spec(&mut config)
            .expect("apply runtime spec")
            .expect("spec missing");

        assert_eq!(spec.storage_mode, StorageMode::HostedFree);
        assert_eq!(config.bucket.as_deref(), Some("clawfs-free"));
        assert_eq!(config.object_prefix, "free/account-a/vol-1");
        assert_eq!(config.pending_bytes, 2048);
        assert_eq!(config.segment_cache_bytes, 4096);
    }

    #[test]
    fn hosted_free_requires_prefix() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_test_env();
        set_env(STORAGE_MODE_ENV, "hosted_free");
        set_env(OBJECT_PROVIDER_ENV, "aws");
        set_env(BUCKET_ENV, "clawfs-free");

        let mut config = base_config();
        let err = apply_env_runtime_spec(&mut config).expect_err("missing prefix should fail");
        assert!(
            err.to_string().contains(OBJECT_PREFIX_ENV),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn accelerator_mode_defaults_and_round_trip() {
        let direct = AcceleratorContract::for_mode(AcceleratorMode::Direct);
        let direct_plus_cache = AcceleratorContract::for_mode(AcceleratorMode::DirectPlusCache);
        let relay_write = AcceleratorContract::for_mode(AcceleratorMode::RelayWrite);

        assert_eq!(AcceleratorMode::default(), AcceleratorMode::Direct);
        assert_eq!(
            direct.fallback_policy,
            AcceleratorFallbackPolicy::PollAndDirect
        );
        assert_eq!(
            direct_plus_cache.fallback_policy,
            AcceleratorFallbackPolicy::PollAndDirect
        );
        assert_eq!(
            relay_write.fallback_policy,
            AcceleratorFallbackPolicy::FailClosed
        );
        assert_eq!(
            direct.journal_clearing,
            AcceleratorJournalClearingRule::AfterLocalCommit
        );
        assert_eq!(
            relay_write.journal_clearing,
            AcceleratorJournalClearingRule::AfterCommittedGeneration
        );
        assert_eq!(direct.authority, AcceleratorAuthorityBoundary::default());

        let encoded = serde_json::to_string(&relay_write).expect("serialize contract");
        let decoded: AcceleratorContract =
            serde_json::from_str(&encoded).expect("deserialize contract");
        assert_eq!(decoded, relay_write);
    }

    #[test]
    fn accelerator_fallback_normalizes_to_mode_semantics() {
        assert_eq!(
            AcceleratorFallbackPolicy::FailClosed.normalize_for_mode(AcceleratorMode::Direct),
            AcceleratorFallbackPolicy::PollAndDirect
        );
        assert_eq!(
            AcceleratorFallbackPolicy::DirectWriteFallback
                .normalize_for_mode(AcceleratorMode::DirectPlusCache),
            AcceleratorFallbackPolicy::PollAndDirect
        );
        assert_eq!(
            AcceleratorFallbackPolicy::PollAndDirect
                .normalize_for_mode(AcceleratorMode::RelayWrite),
            AcceleratorFallbackPolicy::FailClosed
        );
        assert_eq!(
            AcceleratorFallbackPolicy::DirectWriteFallback
                .normalize_for_mode(AcceleratorMode::RelayWrite),
            AcceleratorFallbackPolicy::DirectWriteFallback
        );

        let contract = AcceleratorContract::for_mode(AcceleratorMode::RelayWrite)
            .with_fallback_policy(AcceleratorFallbackPolicy::PollAndDirect);
        let normalized = contract.normalized();
        assert_eq!(
            normalized.fallback_policy,
            AcceleratorFallbackPolicy::FailClosed
        );
        assert_eq!(
            normalized.journal_clearing,
            AcceleratorJournalClearingRule::AfterCommittedGeneration
        );
    }

    fn base_config() -> Config {
        Config::from(Cli::parse_from([
            "clawfs",
            "--mount-path",
            "/tmp/clawfs-mnt",
            "--store-path",
            "/tmp/clawfs-store",
        ]))
    }

    fn clear_test_env() {
        for key in TEST_ENV_VARS {
            // SAFETY: tests serialize access to process env via ENV_LOCK.
            unsafe { env::remove_var(key) };
        }
    }

    fn set_env(key: &str, value: &str) {
        // SAFETY: tests serialize access to process env via ENV_LOCK.
        unsafe { env::set_var(key, value) };
    }
}
