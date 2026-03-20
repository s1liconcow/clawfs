use std::env;
use std::path::PathBuf;
use std::str::FromStr;

use crate::config::{Config, ObjectStoreProvider};
use anyhow::{Context, Result, bail};

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
        BUCKET_ENV, MAX_PENDING_BYTES_ENV, MAX_SEGMENT_CACHE_BYTES_ENV, OBJECT_PREFIX_ENV,
        OBJECT_PROVIDER_ENV, STORAGE_MODE_ENV, StorageMode, apply_env_runtime_spec,
        load_runtime_spec_from_env,
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
