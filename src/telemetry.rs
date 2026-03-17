use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use crate::auth::user_config_root;
use crate::clawfs;
use crate::config::{Config, ObjectStoreProvider};

pub const TELEMETRY_ENV: &str = "CLAWFS_TELEMETRY";

const TELEMETRY_CONFIG_FILE: &str = "telemetry.json";
const TELEMETRY_QUEUE_CAPACITY: usize = 256;
const TELEMETRY_BATCH_SIZE: usize = 16;
const TELEMETRY_FLUSH_INTERVAL: Duration = Duration::from_secs(2);
const ERRNO_EVENT_COOLDOWN: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TelemetryFile {
    enabled: bool,
    client_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    updated_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TelemetryStatus {
    pub enabled: bool,
    pub client_id: Option<String>,
    pub config_path: PathBuf,
    pub config_exists: bool,
    pub env_override: Option<bool>,
}

#[derive(Clone)]
pub struct TelemetryDestination {
    store: Arc<dyn ObjectStore>,
    root_prefix: String,
    client_id: String,
}

#[derive(Debug, Clone, Serialize)]
struct TelemetryEvent {
    event_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    metadata: Value,
}

#[derive(Debug)]
enum TelemetryCommand {
    Event(TelemetryEvent),
}

pub struct TelemetryClient {
    sender: SyncSender<TelemetryCommand>,
    destination: TelemetryDestination,
    errno_gate: Mutex<HashMap<(String, String, i32), Instant>>,
}

#[derive(Clone)]
struct PanicContext {
    destination: TelemetryDestination,
    component: String,
    session_id: Option<String>,
}

static PANIC_CONTEXT: OnceLock<Mutex<Option<PanicContext>>> = OnceLock::new();
static PANIC_HOOK_ONCE: Once = Once::new();

impl TelemetryClient {
    pub fn from_config(config: &Config) -> Result<Option<Arc<Self>>> {
        let Some(destination) = TelemetryDestination::from_config(config)? else {
            return Ok(None);
        };
        let (sender, receiver) = mpsc::sync_channel(TELEMETRY_QUEUE_CAPACITY);
        let worker_destination = destination.clone();
        thread::Builder::new()
            .name("clawfs-telemetry".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .ok();
                let mut batch = Vec::with_capacity(TELEMETRY_BATCH_SIZE);
                loop {
                    match receiver.recv_timeout(TELEMETRY_FLUSH_INTERVAL) {
                        Ok(TelemetryCommand::Event(event)) => {
                            batch.push(event);
                            if batch.len() >= TELEMETRY_BATCH_SIZE {
                                flush_batch(runtime.as_ref(), &worker_destination, &mut batch);
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            flush_batch(runtime.as_ref(), &worker_destination, &mut batch);
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            flush_batch(runtime.as_ref(), &worker_destination, &mut batch);
                            break;
                        }
                    }
                }
            })
            .context("spawning telemetry worker thread")?;
        Ok(Some(Arc::new(Self {
            sender,
            destination,
            errno_gate: Mutex::new(HashMap::new()),
        })))
    }

    pub fn destination(&self) -> TelemetryDestination {
        self.destination.clone()
    }

    pub fn emit(&self, event_name: &str, session_id: Option<&str>, metadata: Value) {
        let command = TelemetryCommand::Event(TelemetryEvent {
            event_name: event_name.to_string(),
            session_id: session_id.map(ToOwned::to_owned),
            metadata: decorate_metadata(&self.destination.client_id, metadata),
        });
        match self.sender.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    pub fn emit_errno_sampled(&self, layer: &str, op: &str, errno: i32) {
        let key = (layer.to_string(), op.to_string(), errno);
        let now = Instant::now();
        let mut gate = self
            .errno_gate
            .lock()
            .expect("telemetry errno gate poisoned");
        if gate
            .get(&key)
            .is_some_and(|last| now.saturating_duration_since(*last) < ERRNO_EVENT_COOLDOWN)
        {
            return;
        }
        gate.insert(key, now);
        drop(gate);
        self.emit(
            "runtime.fs_errno",
            None,
            json!({
                "layer": layer,
                "operation": op,
                "errno": errno,
                "errno_name": errno_name(errno),
            }),
        );
    }
}

impl TelemetryDestination {
    pub fn from_config(config: &Config) -> Result<Option<Self>> {
        let status = load_effective_settings(true)?;
        if !status.enabled {
            return Ok(None);
        }
        let Some(root_prefix) = config.telemetry_object_prefix.clone() else {
            return Ok(None);
        };
        let client_id = status
            .client_id
            .ok_or_else(|| anyhow::anyhow!("telemetry enabled without client id"))?;
        let store = create_telemetry_store(config)?;
        Ok(Some(Self {
            store,
            root_prefix: normalize_prefix(&root_prefix),
            client_id,
        }))
    }

    pub fn emit_blocking(&self, event_name: &str, session_id: Option<&str>, metadata: Value) {
        let event = TelemetryEvent {
            event_name: event_name.to_string(),
            session_id: session_id.map(ToOwned::to_owned),
            metadata: decorate_metadata(&self.client_id, metadata),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        if let Ok(runtime) = runtime {
            let _ = write_batch(&runtime, self, &[event]);
        }
    }
}

pub fn install_panic_hook() {
    PANIC_HOOK_ONCE.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if let Some(context) = PANIC_CONTEXT
                .get_or_init(|| Mutex::new(None))
                .lock()
                .expect("panic telemetry context poisoned")
                .clone()
            {
                context.destination.emit_blocking(
                    "runtime.panic",
                    context.session_id.as_deref(),
                    json!({
                        "component": context.component,
                        "thread": thread::current().name().unwrap_or("unnamed"),
                        "location": info.location().map(|location| {
                            json!({
                                "file": location.file(),
                                "line": location.line(),
                                "column": location.column(),
                            })
                        }),
                        "message_kind": panic_payload_kind(info),
                    }),
                );
            }
            previous(info);
        }));
    });
}

pub fn set_panic_context(
    destination: Option<TelemetryDestination>,
    component: impl Into<String>,
    session_id: Option<String>,
) {
    let mut guard = PANIC_CONTEXT
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("panic telemetry context poisoned");
    *guard = destination.map(|destination| PanicContext {
        destination,
        component: component.into(),
        session_id,
    });
}

pub fn telemetry_status() -> Result<TelemetryStatus> {
    load_effective_settings(false)
}

pub fn set_telemetry_enabled(enabled: bool) -> Result<TelemetryStatus> {
    let path = telemetry_config_path()?;
    let mut file = read_telemetry_file(&path)?.unwrap_or_else(default_telemetry_file);
    file.enabled = enabled;
    file.updated_at = Some(now_rfc3339());
    write_telemetry_file(&path, &file)?;
    telemetry_status()
}

fn load_effective_settings(create_if_missing: bool) -> Result<TelemetryStatus> {
    let path = telemetry_config_path()?;
    let env_override = env_override();
    let mut file = read_telemetry_file(&path)?;
    if file.is_none() && create_if_missing && env_override != Some(false) {
        let created = default_telemetry_file();
        write_telemetry_file(&path, &created)?;
        file = Some(created);
    }
    let config_exists = file.is_some();
    let enabled =
        env_override.unwrap_or_else(|| file.as_ref().map(|value| value.enabled).unwrap_or(true));
    Ok(TelemetryStatus {
        enabled,
        client_id: file.map(|value| value.client_id),
        config_path: path,
        config_exists,
        env_override,
    })
}

fn read_telemetry_file(path: &PathBuf) -> Result<Option<TelemetryFile>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let file: TelemetryFile =
        serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))?;
    if file.client_id.trim().is_empty() {
        bail!("telemetry client_id cannot be empty");
    }
    Ok(Some(file))
}

fn write_telemetry_file(path: &PathBuf, file: &TelemetryFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let payload = serde_json::to_vec_pretty(file)?;
    fs::write(path, payload).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn telemetry_config_path() -> Result<PathBuf> {
    Ok(user_config_root()?.join(TELEMETRY_CONFIG_FILE))
}

fn default_telemetry_file() -> TelemetryFile {
    TelemetryFile {
        enabled: true,
        client_id: Uuid::new_v4().to_string(),
        updated_at: Some(now_rfc3339()),
    }
}

fn env_override() -> Option<bool> {
    let value = env::var(TELEMETRY_ENV).ok()?;
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn decorate_metadata(client_id: &str, metadata: Value) -> Value {
    let mut map = match metadata {
        Value::Object(map) => map,
        other => {
            let mut map = serde_json::Map::new();
            map.insert("value".to_string(), other);
            map
        }
    };
    map.insert("client_id".to_string(), json!(client_id));
    map.insert(
        "platform".to_string(),
        json!(format!("{}-{}", env::consts::OS, env::consts::ARCH)),
    );
    map.insert("cli_version".to_string(), json!(env!("CARGO_PKG_VERSION")));
    map.insert("ts".to_string(), json!(now_rfc3339()));
    Value::Object(map)
}

fn flush_batch(
    runtime: Option<&tokio::runtime::Runtime>,
    destination: &TelemetryDestination,
    batch: &mut Vec<TelemetryEvent>,
) {
    if batch.is_empty() {
        return;
    }
    let events = batch.split_off(0);
    if let Some(runtime) = runtime {
        let _ = write_batch(runtime, destination, &events);
    }
}

fn write_batch(
    runtime: &tokio::runtime::Runtime,
    destination: &TelemetryDestination,
    events: &[TelemetryEvent],
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let now = OffsetDateTime::now_utc();
    let date = now
        .format(&time::macros::format_description!("[year]-[month]-[day]"))
        .unwrap_or_else(|_| "unknown-date".to_string());
    let hour = now
        .format(&time::macros::format_description!("[hour]"))
        .unwrap_or_else(|_| "00".to_string());
    let object_key = format!(
        "{}/date={}/hour={}/client={}/{}-{}.jsonl",
        destination.root_prefix,
        date,
        hour,
        destination.client_id,
        now.unix_timestamp_nanos(),
        Uuid::new_v4(),
    );
    let mut payload = Vec::new();
    for event in events {
        serde_json::to_writer(&mut payload, event)?;
        payload.push(b'\n');
    }
    let path = ObjectPath::from(object_key);
    runtime.block_on(async {
        destination
            .store
            .put(&path, PutPayload::from(Bytes::from(payload)))
            .await
    })?;
    Ok(())
}

fn create_telemetry_store(config: &Config) -> Result<Arc<dyn ObjectStore>> {
    match config.object_provider {
        ObjectStoreProvider::Local => {
            fs::create_dir_all(&config.store_path).with_context(|| {
                format!("creating telemetry root {}", config.store_path.display())
            })?;
            Ok(
                Arc::new(LocalFileSystem::new_with_prefix(config.store_path.clone())?)
                    as Arc<dyn ObjectStore>,
            )
        }
        ObjectStoreProvider::Aws => {
            let bucket = config
                .bucket
                .clone()
                .context("telemetry requires bucket for AWS provider")?;
            let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);
            let mut region = config
                .region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_string());
            if region == "auto" {
                region = "us-east-1".to_string();
            }
            builder = builder.with_region(&region);
            if let Some(endpoint) = &config.endpoint {
                builder = builder.with_endpoint(endpoint);
                builder = builder.with_metadata_endpoint("http://127.0.0.1:1");
            }
            if let Some(key) = clawfs::aws_access_key_id() {
                builder = builder.with_access_key_id(key);
            }
            if let Some(secret) = clawfs::aws_secret_access_key() {
                builder = builder.with_secret_access_key(secret);
            }
            if let Some(token) = clawfs::aws_session_token() {
                builder = builder.with_token(token);
            }
            if config.aws_allow_http {
                builder = builder.with_allow_http(true);
            }
            if config.aws_force_path_style {
                builder = builder.with_virtual_hosted_style_request(false);
            }
            Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
        }
        ObjectStoreProvider::Gcs => {
            let bucket = config
                .bucket
                .clone()
                .context("telemetry requires bucket for GCS provider")?;
            let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);
            if let Some(sa_path) = &config.gcs_service_account {
                builder = builder.with_service_account_path(sa_path.to_string_lossy().into_owned());
            }
            Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
        }
    }
}

fn normalize_prefix(prefix: &str) -> String {
    prefix
        .trim_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

pub fn errno_name(errno: i32) -> &'static str {
    use crate::compat::*;
    match errno {
        EIO => "EIO",
        ENOENT => "ENOENT",
        EPERM => "EPERM",
        EEXIST => "EEXIST",
        EINVAL => "EINVAL",
        ENOTDIR => "ENOTDIR",
        ENOTEMPTY => "ENOTEMPTY",
        EISDIR => "EISDIR",
        ENAMETOOLONG => "ENAMETOOLONG",
        EFBIG => "EFBIG",
        _ => "OTHER",
    }
}

fn panic_payload_kind(info: &std::panic::PanicHookInfo<'_>) -> &'static str {
    if info.payload().downcast_ref::<&'static str>().is_some() {
        "static_str"
    } else if info.payload().downcast_ref::<String>().is_some() {
        "string"
    } else {
        "other"
    }
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{LazyLock, Mutex};

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[test]
    fn telemetry_status_defaults_enabled_without_file() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        let dir = tempfile::tempdir().expect("tempdir");
        // SAFETY: tests serialize env changes via ENV_LOCK.
        unsafe {
            env::set_var("XDG_CONFIG_HOME", dir.path());
            env::remove_var(TELEMETRY_ENV);
        }
        let status = telemetry_status().expect("status");
        assert!(status.enabled);
        assert!(!status.config_exists);
        assert!(status.client_id.is_none());
        // SAFETY: tests serialize env changes via ENV_LOCK.
        unsafe {
            env::remove_var("XDG_CONFIG_HOME");
        }
    }

    #[test]
    fn set_enabled_persists_client_id() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        let dir = tempfile::tempdir().expect("tempdir");
        // SAFETY: tests serialize env changes via ENV_LOCK.
        unsafe {
            env::set_var("XDG_CONFIG_HOME", dir.path());
            env::remove_var(TELEMETRY_ENV);
        }
        let enabled = set_telemetry_enabled(true).expect("enable");
        let disabled = set_telemetry_enabled(false).expect("disable");
        assert_eq!(enabled.client_id, disabled.client_id);
        assert!(!disabled.enabled);
        assert!(disabled.config_exists);
        // SAFETY: tests serialize env changes via ENV_LOCK.
        unsafe {
            env::remove_var("XDG_CONFIG_HOME");
        }
    }

    #[test]
    fn env_override_wins_over_file() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        let dir = tempfile::tempdir().expect("tempdir");
        // SAFETY: tests serialize env changes via ENV_LOCK.
        unsafe {
            env::set_var("XDG_CONFIG_HOME", dir.path());
            env::set_var(TELEMETRY_ENV, "0");
        }
        set_telemetry_enabled(true).expect("enable");
        let status = telemetry_status().expect("status");
        assert!(!status.enabled);
        assert_eq!(status.env_override, Some(false));
        // SAFETY: tests serialize env changes via ENV_LOCK.
        unsafe {
            env::remove_var(TELEMETRY_ENV);
            env::remove_var("XDG_CONFIG_HOME");
        }
    }

    #[test]
    fn normalize_prefix_trims_slashes() {
        assert_eq!(
            normalize_prefix("/telemetry/orgs/demo/"),
            "telemetry/orgs/demo"
        );
    }
}
