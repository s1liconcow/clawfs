use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use clap::{CommandFactory, Parser};

use crate::auth::{
    API_BASE_URL_ENV, AuthProfile, LoginArgs, WhoamiArgs, clear_profile, load_profile,
    store_profile, user_config_root,
};
use crate::config::{Cli, Config};

const DEFAULT_VOLUME: &str = "default";
const DEFAULT_PREFIX: &str = "clawfs";
const DEFAULT_CLAWFS_APP_URL: &str = "https://app.clawfs.dev";
const CLAWFS_API_ENV: &str = "CLAWFS_API";

/// Configuration returned by the ClawFS API for summon
#[derive(Debug, Clone, serde::Deserialize)]
struct SummonApiConfig {
    /// Object store provider (s3, gcs, etc.)
    provider: String,
    /// Bucket name
    bucket: String,
    /// Region for the bucket
    #[serde(default)]
    region: Option<String>,
    /// Endpoint URL (for S3-compatible stores)
    #[serde(default)]
    endpoint: Option<String>,
    /// AWS-style access key ID
    #[serde(default)]
    access_key_id: Option<String>,
    /// AWS-style secret access key
    #[serde(default)]
    secret_access_key: Option<String>,
    /// Storage mode (hosted_free, byob_paid)
    #[serde(default)]
    storage_mode: Option<String>,
    /// Object prefix within the bucket
    #[serde(default)]
    object_prefix: Option<String>,
}

/// Fetches summon configuration from the ClawFS API
async fn fetch_summon_config(
    api_base_url: &str,
    api_token: &str,
    volume: &str,
) -> anyhow::Result<SummonApiConfig> {
    let client = reqwest::Client::new();
    let url = format!("{}/v1/volumes/{}/summon-config", api_base_url, volume);

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", api_token))
        .send()
        .await
        .context("failed to contact ClawFS API")?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response
            .text()
            .await
            .unwrap_or_else(|_| "(no response body)".to_string());
        anyhow::bail!("ClawFS API returned error {}: {}", status, text);
    }

    let config: SummonApiConfig = response
        .json()
        .await
        .context("failed to parse ClawFS API response")?;

    Ok(config)
}

pub enum DispatchAction {
    FallThrough,
    Handled,
    Mount(Box<Config>),
}

#[derive(Debug, Parser)]
#[command(
    name = "clawfs up",
    about = "Run a command with ClawFS preload enabled"
)]
struct UpArgs {
    /// Logical volume name; auto-created under the current account config root.
    #[arg(long, default_value = DEFAULT_VOLUME)]
    volume: String,

    /// Prefix path to intercept, relative to the current working directory by default.
    #[arg(long, default_value = DEFAULT_PREFIX, value_name = "PATH")]
    path: Option<PathBuf>,

    /// Command to run inside the summoned volume.
    #[arg(required = true, trailing_var_arg = true)]
    command: Vec<OsString>,
}

#[derive(Debug, Parser)]
#[command(name = "clawfs mount", about = "Mount a named ClawFS volume via FUSE")]
struct MountArgs {
    /// Logical volume name; auto-created if it does not exist.
    #[arg(long, default_value = DEFAULT_VOLUME)]
    volume: String,

    /// Mount path, relative to the current working directory by default.
    #[arg(long, value_name = "PATH")]
    path: Option<PathBuf>,
}

#[derive(Debug, Parser)]
#[command(
    name = "clawfs serve",
    about = "Serve a named ClawFS volume over user-mode NFS"
)]
struct ServeArgs {
    /// Logical volume name; auto-created if it does not exist.
    #[arg(long, default_value = DEFAULT_VOLUME)]
    volume: String,

    /// Address for the NFS server to bind to.
    #[arg(long, default_value = "127.0.0.1:2049")]
    listen: String,
}

#[derive(Debug)]
struct VolumePaths {
    store_path: PathBuf,
    cache_path: PathBuf,
    mount_state_path: PathBuf,
    preload_state_path: PathBuf,
    nfs_state_path: PathBuf,
}

impl VolumePaths {
    fn ensure_dirs(&self) -> Result<()> {
        fs::create_dir_all(&self.store_path)
            .with_context(|| format!("creating {}", self.store_path.display()))?;
        fs::create_dir_all(&self.cache_path)
            .with_context(|| format!("creating {}", self.cache_path.display()))?;
        for state in [
            &self.mount_state_path,
            &self.preload_state_path,
            &self.nfs_state_path,
        ] {
            if let Some(parent) = state.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
        }
        Ok(())
    }
}

pub fn dispatch(args: &[OsString]) -> Result<DispatchAction> {
    let Some(command) = args.get(1).and_then(|value| value.to_str()) else {
        return Ok(DispatchAction::FallThrough);
    };

    match command {
        "login" => {
            let parse_args = subcommand_args("clawfs login", args);
            let profile = AuthProfile::from_login(LoginArgs::parse_from(parse_args))?;
            let path = store_profile(&profile)?;
            println!(
                "stored API token for {} at {}",
                profile
                    .email
                    .as_deref()
                    .or(profile.account_id.as_deref())
                    .unwrap_or("this ClawFS account"),
                path.display()
            );
            Ok(DispatchAction::Handled)
        }
        "logout" => {
            let removed = clear_profile()?;
            if removed {
                println!("removed stored ClawFS auth profile");
            } else {
                println!("no stored ClawFS auth profile found");
            }
            Ok(DispatchAction::Handled)
        }
        "whoami" => {
            let parse_args = subcommand_args("clawfs whoami", args);
            let whoami = WhoamiArgs::parse_from(parse_args);
            print_whoami(whoami)?;
            Ok(DispatchAction::Handled)
        }
        "up" => {
            let parse_args = subcommand_args("clawfs up", args);
            let up = UpArgs::parse_from(parse_args);
            run_up(up)?;
            Ok(DispatchAction::Handled)
        }
        "mount" => {
            let parse_args = subcommand_args("clawfs mount", args);
            let mount = MountArgs::parse_from(parse_args);
            let config = build_mount_config(mount)?;
            Ok(DispatchAction::Mount(Box::new(config)))
        }
        "serve" => {
            let parse_args = subcommand_args("clawfs serve", args);
            let serve = ServeArgs::parse_from(parse_args);
            run_serve(serve)?;
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("login") => {
            LoginArgs::command().print_help()?;
            println!();
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("whoami") => {
            WhoamiArgs::command().print_help()?;
            println!();
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("up") => {
            UpArgs::command().print_help()?;
            println!();
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("mount") => {
            MountArgs::command().print_help()?;
            println!();
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("serve") => {
            ServeArgs::command().print_help()?;
            println!();
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("logout") => {
            println!("Usage: clawfs logout");
            println!();
            println!("Remove the stored ClawFS auth profile from the local config directory.");
            Ok(DispatchAction::Handled)
        }
        _ => Ok(DispatchAction::FallThrough),
    }
}

fn build_mount_config(args: MountArgs) -> Result<Config> {
    let volume_paths = volume_paths(&args.volume)?;
    volume_paths.ensure_dirs()?;
    let mount_path = resolve_mount_path(args.path)?;
    fs::create_dir_all(&mount_path)
        .with_context(|| format!("creating mount path {}", mount_path.display()))?;

    let cli = Cli::parse_from([
        OsString::from("clawfs"),
        OsString::from("--mount-path"),
        mount_path.as_os_str().to_os_string(),
        OsString::from("--store-path"),
        volume_paths.store_path.as_os_str().to_os_string(),
        OsString::from("--local-cache-path"),
        volume_paths.cache_path.as_os_str().to_os_string(),
        OsString::from("--state-path"),
        volume_paths.mount_state_path.as_os_str().to_os_string(),
        OsString::from("--foreground"),
    ]);

    Ok(cli.into())
}

fn run_up(args: UpArgs) -> Result<()> {
    let volume_paths = volume_paths(&args.volume)?;
    volume_paths.ensure_dirs()?;
    let prefix_path = resolve_prefix_path(args.path)?;
    let preload_lib = resolve_preload_library()?;

    // Get API base URL from env var or use default
    let api_base_url = env::var(CLAWFS_API_ENV)
        .or_else(|_| env::var(API_BASE_URL_ENV))
        .unwrap_or_else(|_| DEFAULT_CLAWFS_APP_URL.to_string());

    // Get API token
    let api_token = if let Ok(token) = env::var("CLAWFS_API_TOKEN") {
        token
    } else if let Some(profile) = load_profile()? {
        profile.api_token
    } else {
        bail!("No API token found. Run `clawfs login` first or set CLAWFS_API_TOKEN");
    };

    // Fetch summon configuration from the API
    let config = tokio::runtime::Runtime::new()
        .context("failed to create async runtime")?
        .block_on(fetch_summon_config(&api_base_url, &api_token, &args.volume))
        .context("failed to fetch summon configuration")?;

    let mut command = Command::new(&args.command[0]);
    command.args(args.command.iter().skip(1));

    let mut ld_preload = env::var_os("LD_PRELOAD").unwrap_or_default();
    if ld_preload.is_empty() {
        ld_preload = preload_lib.as_os_str().to_os_string();
    } else {
        let mut combined = preload_lib.as_os_str().to_os_string();
        combined.push(OsStr::new(":"));
        combined.push(&ld_preload);
        ld_preload = combined;
    }

    command.env("LD_PRELOAD", ld_preload);
    command.env("CLAWFS_PREFIXES", prefix_path.as_os_str());
    command.env("CLAWFS_STORE_PATH", &volume_paths.store_path);
    command.env("CLAWFS_LOCAL_CACHE_PATH", &volume_paths.cache_path);
    command.env("CLAWFS_STATE_PATH", &volume_paths.preload_state_path);
    command.env("CLAWFS_VOLUME", sanitize_volume_name(&args.volume)?);

    // Set object provider from API config
    let provider = match config.provider.as_str() {
        "s3" | "aws" => "aws",
        "gcs" | "gcp" | "google" => "gcs",
        _ => "local",
    };
    command.env("CLAWFS_OBJECT_PROVIDER", provider);
    command.env("CLAWFS_BUCKET", &config.bucket);
    if let Some(region) = config.region {
        command.env("CLAWFS_REGION", region);
    }
    if let Some(endpoint) = config.endpoint {
        command.env("CLAWFS_ENDPOINT", endpoint);
    }
    if let Some(access_key_id) = config.access_key_id {
        command.env("AWS_ACCESS_KEY_ID", access_key_id);
    }
    if let Some(secret_access_key) = config.secret_access_key {
        command.env("AWS_SECRET_ACCESS_KEY", secret_access_key);
    }
    if let Some(object_prefix) = config.object_prefix {
        command.env("CLAWFS_OBJECT_PREFIX", object_prefix);
    }

    // Set storage mode from API config
    let storage_mode = config.storage_mode.as_deref().unwrap_or("byob_paid");
    command.env("CLAWFS_STORAGE_MODE", storage_mode);

    // Always set the API token for the child process
    command.env("CLAWFS_API_TOKEN", api_token);

    Err(command.exec().into())
}

fn run_serve(args: ServeArgs) -> Result<()> {
    let volume_paths = volume_paths(&args.volume)?;
    volume_paths.ensure_dirs()?;
    let gateway = resolve_gateway_binary()?;

    let mut command = Command::new(gateway);
    command
        .arg("--mount-path")
        .arg("/tmp/clawfs-mnt")
        .arg("--store-path")
        .arg(&volume_paths.store_path)
        .arg("--local-cache-path")
        .arg(&volume_paths.cache_path)
        .arg("--state-path")
        .arg(&volume_paths.nfs_state_path)
        .arg("--listen")
        .arg(&args.listen)
        .arg("--protocol")
        .arg("v3");

    Err(command.exec().into())
}

fn print_whoami(args: WhoamiArgs) -> Result<()> {
    if let Some(profile) = load_profile()? {
        if args.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "authenticated": true,
                    "api_base_url": profile.api_base_url,
                    "account_id": profile.account_id,
                    "email": profile.email,
                    "provider": profile.provider,
                    "token_preview": profile.token_preview(),
                }))?
            );
        } else {
            println!("authenticated: yes");
            println!("api_base_url: {}", profile.api_base_url);
            if let Some(ref account_id) = profile.account_id {
                println!("account_id: {account_id}");
            }
            if let Some(ref email) = profile.email {
                println!("email: {email}");
            }
            if let Some(provider) = profile.provider {
                println!("provider: {}", format!("{provider:?}").to_ascii_lowercase());
            }
            println!("token: {}", profile.token_preview());
        }
    } else if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "authenticated": false
            }))?
        );
    } else {
        println!("authenticated: no");
        println!("Run `clawfs login` after you receive a ClawFS API token.");
    }
    Ok(())
}

fn subcommand_args(name: &str, args: &[OsString]) -> Vec<OsString> {
    std::iter::once(OsString::from(name))
        .chain(args.iter().skip(2).cloned())
        .collect()
}

fn resolve_prefix_path(path: Option<PathBuf>) -> Result<PathBuf> {
    let cwd = env::current_dir().context("reading current working directory")?;
    let resolved = match path {
        Some(path) if path.is_absolute() => path,
        Some(path) => cwd.join(path),
        None => cwd,
    };
    Ok(resolved)
}

fn resolve_mount_path(path: Option<PathBuf>) -> Result<PathBuf> {
    let cwd = env::current_dir().context("reading current working directory")?;
    let resolved = match path {
        Some(path) if path.is_absolute() => path,
        Some(path) => cwd.join(path),
        None => cwd.join(".clawfs"),
    };
    Ok(resolved)
}

fn volume_paths(volume: &str) -> Result<VolumePaths> {
    let volume = sanitize_volume_name(volume)?;
    let root = user_config_root()?.join("volumes").join(&volume);
    Ok(VolumePaths {
        store_path: root.join("store"),
        cache_path: root.join("cache"),
        mount_state_path: root.join("state").join("mount_state.bin"),
        preload_state_path: root.join("state").join("preload_state.bin"),
        nfs_state_path: root.join("state").join("nfs_state.bin"),
    })
}

fn sanitize_volume_name(volume: &str) -> Result<String> {
    let trimmed = volume.trim();
    if trimmed.is_empty() {
        bail!("volume name cannot be empty");
    }
    if trimmed == "." || trimmed == ".." || trimmed.contains('/') {
        bail!("volume name must be a simple path-free identifier");
    }
    Ok(trimmed.to_string())
}

fn resolve_preload_library() -> Result<PathBuf> {
    let exe = env::current_exe().context("resolving current executable")?;
    let exe_dir = exe
        .parent()
        .ok_or_else(|| anyhow!("current executable has no parent directory"))?;
    let candidates = [
        exe_dir.join("libclawfs_preload.so"),
        exe_dir.join("../lib/clawfs/libclawfs_preload.so"),
        exe_dir.join("../../clawfs-preload/target/release/libclawfs_preload.so"),
        exe_dir.join("../../clawfs-preload/target/debug/libclawfs_preload.so"),
    ];
    resolve_existing_path(&candidates, "libclawfs_preload.so")
}

fn resolve_gateway_binary() -> Result<PathBuf> {
    let exe = env::current_exe().context("resolving current executable")?;
    let exe_dir = exe
        .parent()
        .ok_or_else(|| anyhow!("current executable has no parent directory"))?;
    let candidates = [
        exe_dir.join("clawfs-nfs-gateway"),
        exe_dir.join("../../clawfs-nfs-gateway/target/release/clawfs-nfs-gateway"),
        exe_dir.join("../../clawfs-nfs-gateway/target/debug/clawfs-nfs-gateway"),
    ];
    resolve_existing_path(&candidates, "clawfs-nfs-gateway")
}

fn resolve_existing_path(candidates: &[PathBuf], label: &str) -> Result<PathBuf> {
    for candidate in candidates {
        if candidate.exists() {
            return candidate
                .canonicalize()
                .with_context(|| format!("canonicalizing {}", candidate.display()));
        }
    }
    bail!(
        "could not locate {label}; checked {}",
        format_candidates(candidates)
    )
}

fn format_candidates(candidates: &[PathBuf]) -> String {
    candidates
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::{resolve_mount_path, resolve_prefix_path, sanitize_volume_name};
    use std::path::PathBuf;

    #[test]
    fn sanitize_volume_accepts_simple_name() {
        assert_eq!(
            sanitize_volume_name("default").expect("volume"),
            "default".to_string()
        );
    }

    #[test]
    fn sanitize_volume_rejects_paths() {
        assert!(sanitize_volume_name("../bad").is_err());
        assert!(sanitize_volume_name("a/b").is_err());
    }

    #[test]
    fn relative_prefix_path_resolves_from_cwd() {
        let cwd = std::env::current_dir().expect("cwd");
        let path = resolve_prefix_path(Some(PathBuf::from("workspace"))).expect("path");
        assert_eq!(path, cwd.join("workspace"));
    }

    #[test]
    fn default_mount_path_is_hidden_dir_in_cwd() {
        let cwd = std::env::current_dir().expect("cwd");
        let path = resolve_mount_path(None).expect("path");
        assert_eq!(path, cwd.join(".clawfs"));
    }
}
