use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use anyhow::{Context, Result, anyhow, bail};
use clap::{CommandFactory, Parser, ValueEnum};
use log::warn;

use crate::auth::{
    API_BASE_URL_ENV, AuthProfile, LoginArgs, WhoamiArgs, clear_profile, load_profile,
    store_profile, user_config_root,
};
use crate::clawfs::STORAGE_MODE_ENV;
use crate::clawfs::{AcceleratorFallbackPolicy, AcceleratorMode};
use crate::config::{Cli, Config, ObjectStoreProvider};
use crate::launch::{EventSettings, HostedControlPlane};
use crate::relay::RelayOutagePolicy;
use crate::telemetry::{set_telemetry_enabled, telemetry_status};

const DEFAULT_VOLUME: &str = "default";
const DEFAULT_PREFIX: &str = "clawfs";
const DEFAULT_CLAWFS_APP_URL: &str = "https://app.clawfs.dev";
const CLAWFS_API_ENV: &str = "CLAWFS_API";
const CLI_LOGIN_POLL_INTERVAL: Duration = Duration::from_secs(2);
const CLI_LOGIN_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, serde::Deserialize)]
struct CliLoginStartResponse {
    login_url: String,
    poll_url: String,
    poll_interval_seconds: u64,
    expires_at: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum CliLoginPollResponse {
    Pending {
        #[allow(dead_code)]
        expires_at: String,
    },
    Complete {
        api_token: String,
        api_base_url: String,
        email: Option<String>,
        account_id: Option<String>,
        provider: Option<String>,
    },
}

#[derive(Debug, Clone, serde::Deserialize)]
struct SummonApiConfig {
    provider: String,
    bucket: String,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    access_key_id: Option<String>,
    #[serde(default)]
    secret_access_key: Option<String>,
    #[serde(default)]
    storage_mode: Option<String>,
    #[serde(default)]
    accelerator_endpoint: Option<String>,
    #[serde(default)]
    accelerator_mode: Option<String>,
    #[serde(default)]
    accelerator_fallback_policy: Option<String>,
    #[serde(default)]
    event_endpoint: Option<String>,
    #[serde(default)]
    event_poll_interval_ms: Option<u64>,
    #[serde(default)]
    relay_fallback_policy: Option<String>,
    #[serde(default)]
    accelerator_session_token: Option<String>,
    #[serde(default)]
    accelerator_session_expiry: Option<i64>,
    #[serde(default)]
    object_prefix: Option<String>,
    #[serde(default)]
    telemetry_object_prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HostedVolumeConfig {
    pub provider: ObjectStoreProvider,
    pub bucket: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub storage_mode: Option<String>,
    pub accelerator_endpoint: Option<String>,
    pub accelerator_mode: Option<AcceleratorMode>,
    pub accelerator_fallback_policy: Option<AcceleratorFallbackPolicy>,
    pub relay_fallback_policy: Option<RelayOutagePolicy>,
    pub event_endpoint: Option<String>,
    pub event_settings: Option<EventSettings>,
    pub accelerator_session_token: Option<String>,
    pub accelerator_session_expiry: Option<i64>,
    pub object_prefix: Option<String>,
    pub telemetry_object_prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HostedVolume {
    pub api_base_url: String,
    pub api_token: String,
    pub volume_slug: String,
    pub config: HostedVolumeConfig,
}

#[derive(Debug, Clone)]
pub struct HostedMountInvocation {
    pub config: Config,
    pub hosted: HostedControlPlane,
}

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

pub struct NfsMountInvocation {
    pub mount_path: PathBuf,
    pub volume_paths: VolumePaths,
    pub hosted: HostedVolume,
    pub foreground: bool,
}

pub enum DispatchAction {
    FallThrough,
    Handled,
    Mount(Box<HostedMountInvocation>),
    NfsMount(Box<NfsMountInvocation>),
}

const CLI_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Parser)]
#[command(
    name = "clawfs up",
    about = "Run a command with ClawFS preload enabled"
)]
struct UpArgs {
    #[arg(long, default_value = DEFAULT_VOLUME)]
    volume: String,

    #[arg(long, default_value = DEFAULT_PREFIX, value_name = "PATH")]
    path: Option<PathBuf>,

    #[arg(required = true, trailing_var_arg = true)]
    command: Vec<OsString>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Transport {
    Auto,
    Fuse,
    Nfs,
}

#[derive(Debug, Parser)]
#[command(
    name = "clawfs mount",
    about = "Mount a named ClawFS volume (auto-detects FUSE or NFS)"
)]
struct MountArgs {
    #[arg(long, default_value = DEFAULT_VOLUME)]
    volume: String,

    #[arg(long, value_name = "PATH")]
    path: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    foreground: bool,

    /// Transport to use: auto (default), fuse, or nfs
    #[arg(long, value_enum, default_value_t = Transport::Auto)]
    transport: Transport,
}

#[derive(Debug, Parser)]
#[command(
    name = "clawfs serve",
    about = "Serve a named ClawFS volume over user-mode NFS"
)]
struct ServeArgs {
    #[arg(long, default_value = DEFAULT_VOLUME)]
    volume: String,

    #[arg(long, default_value = "127.0.0.1:2049")]
    listen: String,
}

#[derive(Debug, Parser)]
#[command(
    name = "clawfs compact",
    about = "Trigger manual compaction of segments and metadata deltas"
)]
struct CompactArgs {
    #[arg(long, default_value = DEFAULT_VOLUME)]
    volume: String,

    /// Maximum number of segments to compact per batch
    #[arg(long, default_value_t = 32)]
    batch_size: usize,

    /// Only compact deltas, skip segment compaction
    #[arg(long, default_value_t = false)]
    deltas_only: bool,

    /// Only compact segments, skip delta compaction
    #[arg(long, default_value_t = false)]
    segments_only: bool,
}

#[derive(Debug, Parser)]
#[command(
    name = "clawfs umount",
    about = "Unmount a ClawFS volume (FUSE or NFS)"
)]
struct UmountArgs {
    #[arg(long, default_value = DEFAULT_VOLUME)]
    volume: String,

    #[arg(long, value_name = "PATH")]
    path: Option<PathBuf>,
}

#[derive(Debug)]
pub struct VolumePaths {
    pub store_path: PathBuf,
    pub cache_path: PathBuf,
    pub mount_state_path: PathBuf,
    pub preload_state_path: PathBuf,
    pub nfs_state_path: PathBuf,
    pub nfs_mount_info_path: PathBuf,
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
        "version" | "--version" | "-V" => {
            print_version();
            Ok(DispatchAction::Handled)
        }
        "login" => {
            let parse_args = subcommand_args("clawfs login", args);
            let login = LoginArgs::parse_from(parse_args);
            let result = if login.api_token.is_some() {
                AuthProfile::from_login(login)
            } else {
                run_browser_login(login)
            };
            match result {
                Ok(profile) => print_stored_profile(&profile)?,
                Err(err) => return Err(err),
            }
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
        "telemetry" => {
            handle_telemetry_command(args)?;
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
            match resolve_transport(mount.transport) {
                Transport::Fuse => {
                    let invocation = build_mount_invocation(mount)?;
                    Ok(DispatchAction::Mount(Box::new(invocation)))
                }
                Transport::Nfs | Transport::Auto => {
                    let invocation = build_nfs_mount_invocation(mount)?;
                    Ok(DispatchAction::NfsMount(Box::new(invocation)))
                }
            }
        }
        "serve" => {
            let parse_args = subcommand_args("clawfs serve", args);
            let serve = ServeArgs::parse_from(parse_args);
            run_serve(serve)?;
            Ok(DispatchAction::Handled)
        }
        "compact" => {
            let parse_args = subcommand_args("clawfs compact", args);
            let compact = CompactArgs::parse_from(parse_args);
            run_compact(compact)?;
            Ok(DispatchAction::Handled)
        }
        "umount" | "unmount" => {
            let parse_args = subcommand_args("clawfs umount", args);
            let umount = UmountArgs::parse_from(parse_args);
            run_umount(umount)?;
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("login") => {
            LoginArgs::command().print_help()?;
            println!();
            println!("Without `--api-token`, this prints a sign-in URL and waits for completion.");
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("whoami") => {
            WhoamiArgs::command().print_help()?;
            println!();
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("telemetry") => {
            print_telemetry_help();
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
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("compact") => {
            CompactArgs::command().print_help()?;
            println!();
            Ok(DispatchAction::Handled)
        }
        "help"
            if args.get(2).and_then(|value| value.to_str()) == Some("umount")
                || args.get(2).and_then(|value| value.to_str()) == Some("unmount") =>
        {
            UmountArgs::command().print_help()?;
            println!();
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("version") => {
            print_version_help();
            Ok(DispatchAction::Handled)
        }
        "help" if args.get(2).and_then(|value| value.to_str()) == Some("logout") => {
            println!("Usage: clawfs logout");
            println!();
            println!("Remove the stored ClawFS auth profile from the local config directory.");
            Ok(DispatchAction::Handled)
        }
        "help" | "--help" | "-h" => {
            print_general_help();
            Ok(DispatchAction::Handled)
        }
        _ => Ok(DispatchAction::FallThrough),
    }
}

pub fn print_general_help() {
    println!("ClawFS customer CLI");
    println!();
    println!("Commands:");
    println!("  clawfs login");
    println!("  clawfs logout");
    println!("  clawfs whoami");
    println!("  clawfs telemetry [status|enable|disable]");
    println!("  clawfs up -- [command...]");
    println!("  clawfs mount [--transport auto|fuse|nfs]");
    println!("  clawfs umount");
    println!("  clawfs serve");
    println!("  clawfs compact");
    println!("  clawfs version");
    println!();
    println!("Manual object-store configuration moved to `clawfsd`.");
}

fn print_version() {
    println!("clawfs {CLI_VERSION}");
}

fn print_version_help() {
    println!("Usage: clawfs version");
    println!();
    println!("Print the installed ClawFS CLI version.");
}

pub fn manual_cli_hint(args: &[OsString]) -> Option<&'static str> {
    let first = args.get(1)?.to_str()?;
    if first.starts_with("--") {
        return Some("manual storage configuration moved to `clawfsd`");
    }
    None
}

fn build_mount_invocation(args: MountArgs) -> Result<HostedMountInvocation> {
    let volume_paths = volume_paths(&args.volume)?;
    volume_paths.ensure_dirs()?;
    let mount_path = resolve_mount_path(args.path)?;
    fs::create_dir_all(&mount_path)
        .with_context(|| format!("creating mount path {}", mount_path.display()))?;

    let hosted = resolve_hosted_volume(&args.volume)?;
    let mut cli_args = vec![
        OsString::from("clawfs"),
        OsString::from("--mount-path"),
        mount_path.as_os_str().to_os_string(),
        OsString::from("--store-path"),
        volume_paths.store_path.as_os_str().to_os_string(),
        OsString::from("--local-cache-path"),
        volume_paths.cache_path.as_os_str().to_os_string(),
        OsString::from("--state-path"),
        volume_paths.mount_state_path.as_os_str().to_os_string(),
    ];
    append_hosted_storage_args(&mut cli_args, &hosted.config);
    if args.foreground {
        cli_args.push(OsString::from("--foreground"));
    }
    let cli = Cli::parse_from(cli_args);
    let mut config: Config = cli.into();
    config.telemetry_object_prefix = hosted.config.telemetry_object_prefix.clone();

    Ok(HostedMountInvocation {
        config,
        hosted: HostedControlPlane {
            api_url: hosted.api_base_url,
            api_token: hosted.api_token,
            volume_slug: hosted.volume_slug,
            access_key_id: hosted.config.access_key_id,
            secret_access_key: hosted.config.secret_access_key,
            storage_mode: hosted.config.storage_mode,
            accelerator_endpoint: hosted.config.accelerator_endpoint,
            accelerator_mode: hosted.config.accelerator_mode,
            accelerator_fallback_policy: hosted.config.accelerator_fallback_policy,
            relay_fallback_policy: hosted.config.relay_fallback_policy,
            event_endpoint: hosted.config.event_endpoint,
            event_settings: hosted.config.event_settings,
            accelerator_session_token: hosted.config.accelerator_session_token,
            accelerator_session_expiry: hosted.config.accelerator_session_expiry,
        },
    })
}

fn resolve_transport(requested: Transport) -> Transport {
    match requested {
        Transport::Fuse | Transport::Nfs => requested,
        Transport::Auto => {
            // Windows has no FUSE support; always use NFS
            if cfg!(target_os = "windows") {
                return Transport::Nfs;
            }
            if cfg!(target_os = "linux") && std::path::Path::new("/dev/fuse").exists() {
                Transport::Fuse
            } else {
                Transport::Nfs
            }
        }
    }
}

fn build_nfs_mount_invocation(args: MountArgs) -> Result<NfsMountInvocation> {
    let volume_paths = volume_paths(&args.volume)?;
    volume_paths.ensure_dirs()?;
    let mount_path = resolve_mount_path(args.path)?;
    fs::create_dir_all(&mount_path)
        .with_context(|| format!("creating mount path {}", mount_path.display()))?;

    let hosted = resolve_hosted_volume(&args.volume)?;

    Ok(NfsMountInvocation {
        mount_path,
        volume_paths,
        hosted,
        foreground: args.foreground,
    })
}

fn run_umount(args: UmountArgs) -> Result<()> {
    let volume_paths = volume_paths(&args.volume)?;
    let mount_path = resolve_mount_path(args.path)?;

    // Check if there's an NFS mount state file
    if volume_paths.nfs_mount_info_path.exists() {
        let state_data = fs::read_to_string(&volume_paths.nfs_mount_info_path)
            .context("reading NFS mount state")?;
        let state: crate::nfs_mount::NfsMountState =
            serde_json::from_str(&state_data).context("parsing NFS mount state")?;

        let mount_target = std::path::PathBuf::from(&state.mount_path);

        // Unmount the NFS filesystem
        match crate::nfs_mount::run_umount_cmd(&mount_target) {
            Ok(()) => println!("Unmounted {}", state.mount_path),
            Err(err) => eprintln!("warning: {err}"),
        }

        // Kill the gateway process
        crate::nfs_mount::kill_gateway(state.gateway_pid);
        println!("Stopped NFS gateway (pid {})", state.gateway_pid);

        // Remove state file
        let _ = fs::remove_file(&volume_paths.nfs_mount_info_path);
        return Ok(());
    }

    // Fall back to FUSE unmount (Unix only; Windows always uses NFS path)
    if cfg!(target_os = "windows") {
        bail!(
            "no NFS mount state found for this volume; nothing to unmount at {}",
            mount_path.display()
        );
    }

    let status = Command::new("fusermount")
        .arg("-u")
        .arg(&mount_path)
        .status();
    match status {
        Ok(s) if s.success() => {
            println!("Unmounted {}", mount_path.display());
        }
        Ok(s) => {
            bail!(
                "fusermount -u failed with status {} for {}",
                s,
                mount_path.display()
            );
        }
        Err(err) => {
            bail!(
                "failed to run fusermount -u on {}: {err}",
                mount_path.display()
            );
        }
    }
    Ok(())
}

fn run_up(args: UpArgs) -> Result<()> {
    let volume_paths = volume_paths(&args.volume)?;
    volume_paths.ensure_dirs()?;
    let prefix_path = resolve_prefix_path(args.path)?;
    let preload_lib = resolve_preload_library()?;
    let hosted = resolve_hosted_volume(&args.volume)?;

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
    command.env(
        "CLAWFS_OBJECT_PROVIDER",
        object_provider_name(hosted.config.provider),
    );
    command.env("CLAWFS_BUCKET", &hosted.config.bucket);
    apply_hosted_accelerator_env(&mut command, &hosted.config);
    if let Some(region) = hosted.config.region {
        command.env("CLAWFS_REGION", region);
    }
    if let Some(endpoint) = hosted.config.endpoint {
        command.env("CLAWFS_ENDPOINT", endpoint);
    }
    if let Some(access_key_id) = hosted.config.access_key_id {
        command.env("AWS_ACCESS_KEY_ID", access_key_id);
    }
    if let Some(secret_access_key) = hosted.config.secret_access_key {
        command.env("AWS_SECRET_ACCESS_KEY", secret_access_key);
    }
    if let Some(object_prefix) = hosted.config.object_prefix {
        command.env("CLAWFS_OBJECT_PREFIX", object_prefix);
    }

    command.env(
        STORAGE_MODE_ENV,
        hosted.config.storage_mode.as_deref().unwrap_or("byob_paid"),
    );
    command.env("CLAWFS_API_TOKEN", hosted.api_token);

    exec_command(command)
}

fn run_serve(args: ServeArgs) -> Result<()> {
    let volume_paths = volume_paths(&args.volume)?;
    volume_paths.ensure_dirs()?;
    let gateway = resolve_gateway_binary()?;
    let hosted = resolve_hosted_volume(&args.volume)?;

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
        .arg("v3")
        .arg("--object-provider")
        .arg(object_provider_name(hosted.config.provider))
        .arg("--bucket")
        .arg(&hosted.config.bucket);
    apply_hosted_accelerator_env(&mut command, &hosted.config);
    if let Some(region) = hosted.config.region {
        command.arg("--region").arg(region);
    }
    if let Some(endpoint) = hosted.config.endpoint {
        command.arg("--endpoint").arg(endpoint);
    }
    if let Some(object_prefix) = hosted.config.object_prefix {
        command.arg("--object-prefix").arg(object_prefix);
    }
    if let Some(access_key_id) = hosted.config.access_key_id {
        command.env("AWS_ACCESS_KEY_ID", access_key_id);
    }
    if let Some(secret_access_key) = hosted.config.secret_access_key {
        command.env("AWS_SECRET_ACCESS_KEY", secret_access_key);
    }

    exec_command(command)
}

#[cfg(unix)]
fn exec_command(mut command: Command) -> Result<()> {
    Err(command.exec().into())
}

#[cfg(not(unix))]
fn exec_command(mut command: Command) -> Result<()> {
    let status = command.status().context("failed to launch child process")?;
    std::process::exit(status.code().unwrap_or(1));
}

fn run_compact(args: CompactArgs) -> Result<()> {
    let volume_paths = volume_paths(&args.volume)?;
    volume_paths.ensure_dirs()?;
    let hosted = resolve_hosted_volume(&args.volume)?;

    let mut cli_args = vec![
        OsString::from("clawfs"),
        OsString::from("--mount-path"),
        OsString::from("/tmp/clawfs-compact-dummy"),
        OsString::from("--store-path"),
        volume_paths.store_path.as_os_str().to_os_string(),
        OsString::from("--local-cache-path"),
        volume_paths.cache_path.as_os_str().to_os_string(),
        OsString::from("--state-path"),
        volume_paths.mount_state_path.as_os_str().to_os_string(),
        OsString::from("--disable-cleanup"),
    ];
    append_hosted_storage_args(&mut cli_args, &hosted.config);
    let cli = Cli::parse_from(cli_args);
    let mut config: Config = cli.into();
    config.telemetry_object_prefix = hosted.config.telemetry_object_prefix.clone();

    let hosted_cp = HostedControlPlane {
        api_url: hosted.api_base_url,
        api_token: hosted.api_token,
        volume_slug: hosted.volume_slug,
        access_key_id: hosted.config.access_key_id,
        secret_access_key: hosted.config.secret_access_key,
        storage_mode: hosted.config.storage_mode,
        accelerator_endpoint: hosted.config.accelerator_endpoint,
        accelerator_mode: hosted.config.accelerator_mode,
        accelerator_fallback_policy: hosted.config.accelerator_fallback_policy,
        relay_fallback_policy: hosted.config.relay_fallback_policy,
        event_endpoint: hosted.config.event_endpoint,
        event_settings: hosted.config.event_settings,
        accelerator_session_token: hosted.config.accelerator_session_token,
        accelerator_session_expiry: hosted.config.accelerator_session_expiry,
    };

    crate::launch::run_compact_entry(
        config,
        &hosted_cp,
        args.batch_size,
        args.deltas_only,
        args.segments_only,
    )
}

fn resolve_hosted_volume(volume: &str) -> Result<HostedVolume> {
    let api_base_url = resolve_api_base_url();
    let api_token = resolve_api_token()?;
    let summon = tokio::runtime::Runtime::new()
        .context("failed to create async runtime")?
        .block_on(fetch_summon_config(&api_base_url, &api_token, volume))
        .context("failed to fetch summon configuration")?;
    let config = parse_hosted_volume_config(summon)?;

    Ok(HostedVolume {
        api_base_url,
        api_token,
        volume_slug: sanitize_volume_name(volume)?,
        config,
    })
}

fn resolve_api_base_url() -> String {
    if let Ok(value) = env::var(CLAWFS_API_ENV) {
        return value;
    }
    if let Ok(value) = env::var(API_BASE_URL_ENV) {
        return value;
    }
    if let Ok(Some(profile)) = load_profile() {
        return profile.api_base_url;
    }
    DEFAULT_CLAWFS_APP_URL.to_string()
}

fn resolve_api_token() -> Result<String> {
    if let Ok(token) = env::var("CLAWFS_API_TOKEN") {
        return Ok(token);
    }
    if let Some(profile) = load_profile()? {
        return Ok(profile.api_token);
    }
    bail!("No API token found. Run `clawfs login` first or set CLAWFS_API_TOKEN");
}

fn provider_from_api(provider: &str) -> ObjectStoreProvider {
    match provider {
        "s3" | "aws" => ObjectStoreProvider::Aws,
        "gcs" | "gcp" | "google" => ObjectStoreProvider::Gcs,
        _ => ObjectStoreProvider::Local,
    }
}

fn parse_accelerator_mode(value: Option<&str>) -> AcceleratorMode {
    match value {
        None => AcceleratorMode::Direct,
        Some(raw) => match raw.parse::<AcceleratorMode>() {
            Ok(mode) => mode,
            Err(err) => {
                warn!("invalid accelerator_mode {raw:?}: {err}; defaulting to direct");
                AcceleratorMode::Direct
            }
        },
    }
}

fn parse_accelerator_fallback_policy(
    value: Option<&str>,
    mode: AcceleratorMode,
) -> AcceleratorFallbackPolicy {
    let policy = match value {
        None => mode.default_fallback_policy(),
        Some(raw) => match raw.parse::<AcceleratorFallbackPolicy>() {
            Ok(policy) => policy,
            Err(err) => {
                warn!(
                    "invalid accelerator_fallback_policy {raw:?}: {err}; defaulting to {:?}",
                    mode.default_fallback_policy()
                );
                mode.default_fallback_policy()
            }
        },
    };
    policy.normalize_for_mode(mode)
}

fn parse_relay_fallback_policy(
    value: Option<&str>,
    mode: AcceleratorMode,
) -> Result<Option<RelayOutagePolicy>> {
    let Some(raw) = value else {
        return Ok(match mode {
            AcceleratorMode::RelayWrite => Some(RelayOutagePolicy::FailClosed),
            AcceleratorMode::Direct | AcceleratorMode::DirectPlusCache => None,
        });
    };
    if mode != AcceleratorMode::RelayWrite {
        bail!("relay_fallback_policy is only valid when accelerator_mode is relay_write");
    }
    let policy = raw.parse::<RelayOutagePolicy>().unwrap_or_else(|err| {
        warn!("invalid relay_fallback_policy {raw:?}: {err}; defaulting to fail_closed");
        RelayOutagePolicy::FailClosed
    });
    Ok(Some(policy))
}

fn parse_event_settings(event_poll_interval_ms: Option<u64>) -> Option<EventSettings> {
    event_poll_interval_ms.map(EventSettings::from_poll_interval_ms)
}

fn parse_hosted_volume_config(summon: SummonApiConfig) -> Result<HostedVolumeConfig> {
    let accelerator_mode = parse_accelerator_mode(summon.accelerator_mode.as_deref());
    let accelerator_fallback_policy = parse_accelerator_fallback_policy(
        summon.accelerator_fallback_policy.as_deref(),
        accelerator_mode,
    );
    let relay_fallback_policy =
        parse_relay_fallback_policy(summon.relay_fallback_policy.as_deref(), accelerator_mode)?;
    Ok(HostedVolumeConfig {
        provider: provider_from_api(&summon.provider),
        bucket: summon.bucket,
        region: summon.region,
        endpoint: summon.endpoint,
        access_key_id: summon.access_key_id,
        secret_access_key: summon.secret_access_key,
        storage_mode: summon.storage_mode,
        accelerator_endpoint: summon.accelerator_endpoint,
        accelerator_mode: Some(accelerator_mode),
        accelerator_fallback_policy: Some(accelerator_fallback_policy),
        relay_fallback_policy,
        event_endpoint: summon.event_endpoint,
        event_settings: parse_event_settings(summon.event_poll_interval_ms),
        accelerator_session_token: summon.accelerator_session_token,
        accelerator_session_expiry: summon.accelerator_session_expiry,
        object_prefix: summon.object_prefix,
        telemetry_object_prefix: summon.telemetry_object_prefix,
    })
}

pub fn append_hosted_storage_args(args: &mut Vec<OsString>, hosted: &HostedVolumeConfig) {
    args.push(OsString::from("--object-provider"));
    args.push(OsString::from(object_provider_name(hosted.provider)));
    args.push(OsString::from("--bucket"));
    args.push(OsString::from(hosted.bucket.clone()));
    if let Some(region) = &hosted.region {
        args.push(OsString::from("--region"));
        args.push(OsString::from(region));
    }
    if let Some(endpoint) = &hosted.endpoint {
        args.push(OsString::from("--endpoint"));
        args.push(OsString::from(endpoint));
    }
    if let Some(object_prefix) = &hosted.object_prefix {
        args.push(OsString::from("--object-prefix"));
        args.push(OsString::from(object_prefix));
    }
}

fn apply_hosted_accelerator_env(command: &mut Command, hosted: &HostedVolumeConfig) {
    if let Some(endpoint) = &hosted.accelerator_endpoint {
        command.env("CLAWFS_ACCELERATOR_ENDPOINT", endpoint);
    }
    if let Some(mode) = hosted.accelerator_mode {
        command.env("CLAWFS_ACCELERATOR_MODE", mode.as_str());
    }
    if let Some(policy) = hosted.accelerator_fallback_policy {
        command.env("CLAWFS_ACCELERATOR_FALLBACK_POLICY", policy.as_str());
    }
    if let Some(policy) = hosted.relay_fallback_policy {
        command.env("CLAWFS_RELAY_FALLBACK_POLICY", policy.as_str());
    } else {
        command.env_remove("CLAWFS_RELAY_FALLBACK_POLICY");
    }
    if let Some(endpoint) = &hosted.event_endpoint {
        command.env("CLAWFS_EVENT_ENDPOINT", endpoint);
    }
    if let Some(settings) = &hosted.event_settings {
        command
            .env(
                "CLAWFS_EVENT_POLL_INTERVAL_MS",
                settings.poll_interval_ms.to_string(),
            )
            .env(
                "CLAWFS_EVENT_RECONNECT_BACKOFF_MS",
                settings.reconnect_backoff_ms.to_string(),
            );
    }
    if let Some(token) = &hosted.accelerator_session_token {
        command.env("CLAWFS_ACCELERATOR_SESSION_TOKEN", token);
    }
    if let Some(expiry) = hosted.accelerator_session_expiry {
        command.env("CLAWFS_ACCELERATOR_SESSION_EXPIRY", expiry.to_string());
    }
}

pub fn object_provider_name(provider: ObjectStoreProvider) -> &'static str {
    match provider {
        ObjectStoreProvider::Local => "local",
        ObjectStoreProvider::Aws => "aws",
        ObjectStoreProvider::Gcs => "gcs",
    }
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
        println!("Run `clawfs login` to sign in.");
    }
    Ok(())
}

fn run_browser_login(args: LoginArgs) -> Result<AuthProfile> {
    let api_base_url = normalize_browser_api_base(args.api_base_url)?;
    let start = start_cli_login(&api_base_url, &args.label)?;

    println!("Open this link to sign in to ClawFS:");
    println!("{}", start.login_url);
    println!(
        "Waiting for authentication to complete (expires at {}).",
        start.expires_at
    );

    let server_poll_interval = Duration::from_secs(start.poll_interval_seconds.max(1));
    poll_cli_login(
        &start.poll_url,
        &api_base_url,
        server_poll_interval.max(CLI_LOGIN_POLL_INTERVAL),
    )
}

fn start_cli_login(api_base_url: &str, label: &str) -> Result<CliLoginStartResponse> {
    let client = reqwest::blocking::Client::new();
    let url = format!("{}/api/cli-login/start", api_base_url);
    let response = client
        .post(&url)
        .json(&serde_json::json!({ "label": label }))
        .send()
        .with_context(|| format!("failed to contact {}", url))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response
            .text()
            .unwrap_or_else(|_| "(no response body)".to_string());
        bail!("failed to start browser login ({}): {}", status, text);
    }

    response
        .json()
        .context("failed to parse CLI login start response")
}

fn poll_cli_login(
    poll_url: &str,
    api_base_url: &str,
    poll_interval: Duration,
) -> Result<AuthProfile> {
    let client = reqwest::blocking::Client::new();
    let deadline = Instant::now() + CLI_LOGIN_TIMEOUT;

    loop {
        if Instant::now() >= deadline {
            bail!("timed out waiting for browser login to complete");
        }

        let response = client
            .get(poll_url)
            .send()
            .with_context(|| format!("failed to poll {}", poll_url))?;

        if response.status() == reqwest::StatusCode::ACCEPTED {
            thread::sleep(poll_interval);
            continue;
        }

        if !response.status().is_success() {
            let status = response.status();
            let text = response
                .text()
                .unwrap_or_else(|_| "(no response body)".to_string());
            bail!("browser login failed ({}): {}", status, text);
        }

        match response
            .json::<CliLoginPollResponse>()
            .context("failed to parse CLI login poll response")?
        {
            CliLoginPollResponse::Pending { expires_at: _ } => {
                thread::sleep(poll_interval);
            }
            CliLoginPollResponse::Complete {
                api_token,
                api_base_url: returned_api_base_url,
                email,
                account_id,
                provider,
            } => {
                let profile = AuthProfile {
                    api_token,
                    api_base_url: if returned_api_base_url.trim().is_empty() {
                        api_base_url.to_string()
                    } else {
                        returned_api_base_url
                    },
                    account_id,
                    email,
                    provider: provider.and_then(parse_auth_provider),
                };
                return Ok(profile);
            }
        }
    }
}

fn print_stored_profile(profile: &AuthProfile) -> Result<()> {
    let path = store_profile(profile)?;
    println!(
        "stored API token for {} at {}",
        profile
            .email
            .as_deref()
            .or(profile.account_id.as_deref())
            .unwrap_or("this ClawFS account"),
        path.display()
    );
    println!("Telemetry is enabled by default. Run `clawfs telemetry disable` to opt out.");
    Ok(())
}

fn normalize_browser_api_base(api_base_url: String) -> Result<String> {
    let trimmed = api_base_url.trim().trim_end_matches('/').to_string();
    if trimmed.is_empty() {
        bail!("API base URL cannot be empty");
    }
    Ok(trimmed)
}

fn parse_auth_provider(value: String) -> Option<crate::auth::AuthProvider> {
    match value.as_str() {
        "email" => Some(crate::auth::AuthProvider::Email),
        "google" => Some(crate::auth::AuthProvider::Google),
        "github" => Some(crate::auth::AuthProvider::Github),
        _ => None,
    }
}

fn handle_telemetry_command(args: &[OsString]) -> Result<()> {
    match args.get(2).and_then(|value| value.to_str()) {
        None | Some("status") => {
            let status = telemetry_status()?;
            println!(
                "telemetry: {}",
                if status.enabled {
                    "enabled"
                } else {
                    "disabled"
                }
            );
            println!("config: {}", status.config_path.display());
            println!(
                "config_present: {}",
                if status.config_exists { "yes" } else { "no" }
            );
            if let Some(client_id) = status.client_id {
                println!("client_id: {client_id}");
            }
            if let Some(env_override) = status.env_override {
                println!(
                    "env_override: {}",
                    if env_override { "enabled" } else { "disabled" }
                );
            }
            Ok(())
        }
        Some("enable") => {
            let status = set_telemetry_enabled(true)?;
            println!(
                "telemetry enabled (config: {}, client_id: {})",
                status.config_path.display(),
                status.client_id.unwrap_or_else(|| "unknown".to_string())
            );
            Ok(())
        }
        Some("disable") => {
            let status = set_telemetry_enabled(false)?;
            println!(
                "telemetry disabled (config: {}, client_id: {})",
                status.config_path.display(),
                status.client_id.unwrap_or_else(|| "unknown".to_string())
            );
            Ok(())
        }
        Some("help") | Some("--help") | Some("-h") => {
            print_telemetry_help();
            Ok(())
        }
        Some(other) => {
            bail!("unknown telemetry subcommand {other:?}; expected status, enable, or disable")
        }
    }
}

fn print_telemetry_help() {
    println!("Usage: clawfs telemetry [status|enable|disable]");
    println!();
    println!("Inspect or update the local ClawFS telemetry preference.");
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
        None => cwd.join(DEFAULT_PREFIX),
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
        nfs_mount_info_path: root.join("state").join("nfs_mount.json"),
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

pub fn resolve_gateway_binary() -> Result<PathBuf> {
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
    use super::{
        manual_cli_hint, object_provider_name, parse_hosted_volume_config, print_version,
        provider_from_api, resolve_mount_path, resolve_prefix_path, sanitize_volume_name,
    };
    use crate::clawfs::{AcceleratorFallbackPolicy, AcceleratorMode};
    use crate::config::ObjectStoreProvider;
    use crate::relay::RelayOutagePolicy;
    use std::ffi::OsString;
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
    fn default_mount_path_matches_default_prefix() {
        let cwd = std::env::current_dir().expect("cwd");
        let path = resolve_mount_path(None).expect("path");
        assert_eq!(path, cwd.join("clawfs"));
    }

    #[test]
    fn manual_cli_hint_catches_top_level_flags() {
        let args = vec![
            OsString::from("clawfs"),
            OsString::from("--object-provider"),
        ];
        assert_eq!(
            manual_cli_hint(&args),
            Some("manual storage configuration moved to `clawfsd`")
        );
    }

    #[test]
    fn provider_aliases_map_to_supported_providers() {
        assert_eq!(provider_from_api("aws"), ObjectStoreProvider::Aws);
        assert_eq!(provider_from_api("gcp"), ObjectStoreProvider::Gcs);
        assert_eq!(provider_from_api("unknown"), ObjectStoreProvider::Local);
        assert_eq!(object_provider_name(ObjectStoreProvider::Aws), "aws");
    }

    #[test]
    fn version_string_comes_from_package_metadata() {
        let version_fn: fn() = print_version;
        let _ = version_fn;
        assert_eq!(env!("CARGO_PKG_VERSION"), super::CLI_VERSION);
    }

    #[test]
    fn summon_config_defaults_to_direct_accelerator_mode() {
        let summon: super::SummonApiConfig = serde_json::from_str(
            r#"{"provider":"aws","bucket":"bucket-a","storage_mode":"byob_paid"}"#,
        )
        .expect("parse summon config");
        let hosted = parse_hosted_volume_config(summon).expect("hosted config");

        assert_eq!(hosted.accelerator_mode, Some(AcceleratorMode::Direct));
        assert_eq!(
            hosted.accelerator_fallback_policy,
            Some(AcceleratorFallbackPolicy::PollAndDirect)
        );
        assert_eq!(hosted.relay_fallback_policy, None);
        assert_eq!(hosted.event_settings, None);
    }

    #[test]
    fn summon_config_propagates_explicit_accelerator_fields() {
        let summon: super::SummonApiConfig = serde_json::from_str(
            r#"{
                "provider":"aws",
                "bucket":"bucket-a",
                "accelerator_endpoint":"https://accelerator.example",
                "accelerator_mode":"relay_write",
                "accelerator_fallback_policy":"fail_closed",
                "event_endpoint":"https://events.example",
                "event_poll_interval_ms":2500,
                "relay_fallback_policy":"direct_write_fallback",
                "accelerator_session_token":"token-123",
                "accelerator_session_expiry":1700000000
            }"#,
        )
        .expect("parse summon config");
        let hosted = parse_hosted_volume_config(summon).expect("hosted config");

        assert_eq!(hosted.accelerator_mode, Some(AcceleratorMode::RelayWrite));
        assert_eq!(
            hosted.accelerator_fallback_policy,
            Some(AcceleratorFallbackPolicy::FailClosed)
        );
        assert_eq!(
            hosted.relay_fallback_policy,
            Some(RelayOutagePolicy::DirectWriteFallback)
        );
        assert_eq!(
            hosted.event_settings,
            Some(super::EventSettings::from_poll_interval_ms(2500))
        );
        assert_eq!(
            hosted.accelerator_session_token.as_deref(),
            Some("token-123")
        );
        assert_eq!(hosted.accelerator_session_expiry, Some(1_700_000_000));
    }

    #[test]
    fn relay_fallback_policy_rejects_non_relay_modes() {
        let summon: super::SummonApiConfig = serde_json::from_str(
            r#"{
                "provider":"aws",
                "bucket":"bucket-a",
                "accelerator_mode":"direct_plus_cache",
                "relay_fallback_policy":"direct_write_fallback"
            }"#,
        )
        .expect("parse summon config");

        let err = parse_hosted_volume_config(summon).expect_err("invalid relay policy");
        assert!(err.to_string().contains("relay_fallback_policy"));
    }

    #[test]
    fn relay_fallback_policy_defaults_to_fail_closed_for_relay_write() {
        let summon: super::SummonApiConfig = serde_json::from_str(
            r#"{
                "provider":"aws",
                "bucket":"bucket-a",
                "accelerator_mode":"relay_write"
            }"#,
        )
        .expect("parse summon config");

        let hosted = parse_hosted_volume_config(summon).expect("hosted config");
        assert_eq!(
            hosted.relay_fallback_policy,
            Some(RelayOutagePolicy::FailClosed)
        );
    }
}
