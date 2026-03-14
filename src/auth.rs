use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

pub const API_TOKEN_ENV: &str = "CLAWFS_API_TOKEN";
pub const API_BASE_URL_ENV: &str = "CLAWFS_API_BASE_URL";

const DEFAULT_API_BASE_URL: &str = "https://api.clawfs.dev";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum AuthProvider {
    Email,
    Google,
    Github,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthProfile {
    pub api_token: String,
    pub api_base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<AuthProvider>,
}

#[derive(Debug, Parser)]
pub struct LoginArgs {
    /// API token issued by the ClawFS hosted auth service.
    #[arg(long, env = API_TOKEN_ENV)]
    pub api_token: Option<String>,

    /// Hosted ClawFS API base URL.
    #[arg(long, env = API_BASE_URL_ENV, default_value = DEFAULT_API_BASE_URL)]
    pub api_base_url: String,

    /// Optional account id to store alongside the token.
    #[arg(long)]
    pub account_id: Option<String>,

    /// Optional email to store alongside the token.
    #[arg(long)]
    pub email: Option<String>,

    /// Optional auth provider metadata.
    #[arg(long, value_enum)]
    pub provider: Option<AuthProvider>,
}

#[derive(Debug, Parser)]
pub struct WhoamiArgs {
    /// Emit machine-readable JSON.
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

impl AuthProfile {
    pub fn from_login(args: LoginArgs) -> Result<Self> {
        let api_token = match args.api_token {
            Some(token) => normalize_token(token)?,
            None => prompt_for_token()?,
        };
        let api_base_url = normalize_api_base_url(args.api_base_url)?;
        Ok(Self {
            api_token,
            api_base_url,
            account_id: args.account_id.and_then(normalize_optional),
            email: args.email.and_then(normalize_optional),
            provider: args.provider,
        })
    }

    pub fn token_preview(&self) -> String {
        let mut chars = self.api_token.chars();
        let prefix: String = chars.by_ref().take(6).collect();
        if self.api_token.chars().count() <= 10 {
            return "********".to_string();
        }
        let suffix: String = self
            .api_token
            .chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("{prefix}...{suffix}")
    }
}

pub fn load_profile() -> Result<Option<AuthProfile>> {
    let path = auth_profile_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    let profile =
        serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))?;
    Ok(Some(profile))
}

pub fn store_profile(profile: &AuthProfile) -> Result<PathBuf> {
    let path = auth_profile_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(profile)?;
    fs::write(&path, payload).with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

pub fn clear_profile() -> Result<bool> {
    let path = auth_profile_path()?;
    match fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err).with_context(|| format!("removing {}", path.display())),
    }
}

pub fn resolved_api_token() -> Result<Option<String>> {
    if let Ok(value) = env::var(API_TOKEN_ENV) {
        let token = normalize_token(value)?;
        return Ok(Some(token));
    }
    Ok(load_profile()?.map(|profile| profile.api_token))
}

pub fn auth_profile_path() -> Result<PathBuf> {
    Ok(user_config_root()?.join("auth.json"))
}

pub fn user_config_root() -> Result<PathBuf> {
    if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(xdg).join("clawfs"));
    }
    if let Some(home) = env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(".clawfs"));
    }
    bail!("could not determine a ClawFS config directory; set HOME or XDG_CONFIG_HOME")
}

fn prompt_for_token() -> Result<String> {
    eprint!("ClawFS API token: ");
    io::stderr().flush().context("flushing token prompt")?;
    let mut token = String::new();
    io::stdin()
        .read_line(&mut token)
        .context("reading API token from stdin")?;
    normalize_token(token)
}

fn normalize_token(token: String) -> Result<String> {
    let token = token.trim().to_string();
    if token.is_empty() {
        bail!("API token cannot be empty")
    }
    Ok(token)
}

fn normalize_api_base_url(url: String) -> Result<String> {
    let url = url.trim().trim_end_matches('/').to_string();
    if url.is_empty() {
        bail!("API base URL cannot be empty")
    }
    Ok(url)
}

fn normalize_optional(value: String) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{LazyLock, Mutex};

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[test]
    fn store_and_load_profile_round_trip() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        let dir = tempfile::tempdir().expect("tempdir");
        // SAFETY: tests serialize env changes via ENV_LOCK.
        unsafe { env::set_var("XDG_CONFIG_HOME", dir.path()) };

        let profile = AuthProfile {
            api_token: "cf_demo_token".into(),
            api_base_url: "https://api.clawfs.dev".into(),
            account_id: Some("acct_123".into()),
            email: Some("demo@clawfs.dev".into()),
            provider: Some(AuthProvider::Github),
        };
        let path = store_profile(&profile).expect("store");
        assert!(path.exists());
        let loaded = load_profile().expect("load").expect("profile");
        assert_eq!(loaded, profile);

        // SAFETY: tests serialize env changes via ENV_LOCK.
        unsafe { env::remove_var("XDG_CONFIG_HOME") };
    }

    #[test]
    fn resolved_token_prefers_env() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        let dir = tempfile::tempdir().expect("tempdir");
        // SAFETY: tests serialize env changes via ENV_LOCK.
        unsafe {
            env::set_var("XDG_CONFIG_HOME", dir.path());
            env::set_var(API_TOKEN_ENV, "cf_env_override");
        }
        store_profile(&AuthProfile {
            api_token: "cf_profile_token".into(),
            api_base_url: "https://api.clawfs.dev".into(),
            account_id: None,
            email: None,
            provider: None,
        })
        .expect("store");

        let token = resolved_api_token().expect("resolve").expect("token");
        assert_eq!(token, "cf_env_override");

        // SAFETY: tests serialize env changes via ENV_LOCK.
        unsafe {
            env::remove_var(API_TOKEN_ENV);
            env::remove_var("XDG_CONFIG_HOME");
        }
    }
}
