use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectError, ObjectStore};

use crate::clawfs;
use crate::config::{ObjectStoreProvider, SourceStoreConfig};

#[derive(Debug, Clone)]
pub struct SourceObjectMeta {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified_ns: Option<i64>,
}

#[derive(Debug, Clone)]
pub enum DiscoveredEntry {
    File(SourceObjectMeta),
    Directory,
}

pub struct SourceObjectStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl SourceObjectStore {
    pub fn new(config: &SourceStoreConfig) -> Result<Self> {
        let (store, prefix): (Arc<dyn ObjectStore>, String) = match config.object_provider {
            ObjectStoreProvider::Local => {
                let root = config
                    .store_path
                    .clone()
                    .context("--source-store-path is required for local source provider")?;
                std::fs::create_dir_all(&root)
                    .with_context(|| format!("creating source store root {}", root.display()))?;
                let store =
                    Arc::new(LocalFileSystem::new_with_prefix(root)?) as Arc<dyn ObjectStore>;
                (store, normalize_prefix(&config.prefix))
            }
            ObjectStoreProvider::Aws => {
                let bucket = config
                    .bucket
                    .clone()
                    .context("--source-bucket is required for AWS source provider")?;
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
                if let Some(key) = &config.aws_access_key_id {
                    builder = builder.with_access_key_id(key);
                } else if let Some(key) = clawfs::aws_access_key_id() {
                    builder = builder.with_access_key_id(key);
                }
                if let Some(secret) = &config.aws_secret_access_key {
                    builder = builder.with_secret_access_key(secret);
                } else if let Some(secret) = clawfs::aws_secret_access_key() {
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
                let store = Arc::new(builder.build()?) as Arc<dyn ObjectStore>;
                (store, normalize_prefix(&config.prefix))
            }
            ObjectStoreProvider::Gcs => {
                let bucket = config
                    .bucket
                    .clone()
                    .context("--source-bucket is required for GCS source provider")?;
                let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);
                if let Some(sa_path) = &config.gcs_service_account {
                    builder =
                        builder.with_service_account_path(sa_path.to_string_lossy().into_owned());
                }
                let store = Arc::new(builder.build()?) as Arc<dyn ObjectStore>;
                (store, normalize_prefix(&config.prefix))
            }
        };
        Ok(Self { store, prefix })
    }

    pub async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>> {
        if end <= start {
            return Ok(Vec::new());
        }
        let path = self.object_path_for_key(key);
        let bytes = self.store.get_range(&path, start..end).await?;
        Ok(bytes.to_vec())
    }

    pub async fn read_all(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.object_path_for_key(key);
        let bytes = self.store.get(&path).await?.bytes().await?;
        Ok(bytes.to_vec())
    }

    pub async fn lookup_child(
        &self,
        parent_path: &str,
        name: &str,
    ) -> Result<Option<DiscoveredEntry>> {
        let parent_key = Self::path_to_key(parent_path);
        let exact_key = if parent_key.is_empty() {
            name.to_string()
        } else {
            format!("{parent_key}/{name}")
        };
        let exact_path = self.object_path_for_key(&exact_key);
        match self.store.head(&exact_path).await {
            Ok(meta) => {
                return Ok(Some(DiscoveredEntry::File(SourceObjectMeta {
                    key: exact_key,
                    size: meta.size,
                    etag: meta.e_tag,
                    last_modified_ns: meta.last_modified.timestamp_nanos_opt(),
                })));
            }
            Err(err) if is_not_found(&err) => {}
            Err(err) => return Err(err.into()),
        }

        let child_prefix = format!("{exact_key}/");
        let child_prefix_path = self.object_path_for_key(&child_prefix);
        let mut stream = self.store.list(Some(&child_prefix_path));
        if let Some(item) = stream.next().await {
            let _ = item?;
            return Ok(Some(DiscoveredEntry::Directory));
        }
        Ok(None)
    }

    pub async fn list_direct_children(
        &self,
        dir_path: &str,
    ) -> Result<Vec<(String, DiscoveredEntry)>> {
        let dir_key = Self::path_to_key(dir_path);
        let list_prefix = if dir_key.is_empty() {
            String::new()
        } else {
            format!("{dir_key}/")
        };
        let list_path = self.object_path_for_key(&list_prefix);
        let mut stream = self.store.list(Some(&list_path));

        #[derive(Default)]
        struct ChildAgg {
            file: Option<SourceObjectMeta>,
            is_dir: bool,
        }

        let mut out: BTreeMap<String, ChildAgg> = BTreeMap::new();
        while let Some(item) = stream.next().await {
            let meta = item?;
            let Some(rel_key) = self.relative_key(&meta.location) else {
                continue;
            };
            if rel_key.is_empty() {
                continue;
            }
            let suffix = if list_prefix.is_empty() {
                rel_key.as_str()
            } else if let Some(s) = rel_key.strip_prefix(&list_prefix) {
                s
            } else {
                continue;
            };
            if suffix.is_empty() {
                continue;
            }
            let mut parts = suffix.splitn(2, '/');
            let child_name = parts.next().unwrap_or_default();
            if child_name.is_empty() {
                continue;
            }
            let has_more = parts.next().is_some();
            let entry = out.entry(child_name.to_string()).or_default();
            if has_more {
                entry.is_dir = true;
            } else {
                entry.file = Some(SourceObjectMeta {
                    key: rel_key,
                    size: meta.size,
                    etag: meta.e_tag,
                    last_modified_ns: meta.last_modified.timestamp_nanos_opt(),
                });
            }
        }

        Ok(out
            .into_iter()
            .map(|(name, agg)| {
                if let Some(file) = agg.file {
                    (name, DiscoveredEntry::File(file))
                } else {
                    (name, DiscoveredEntry::Directory)
                }
            })
            .collect())
    }

    fn object_path_for_key(&self, key: &str) -> ObjectPath {
        let key = key.trim_start_matches('/');
        if self.prefix.is_empty() {
            ObjectPath::from(key)
        } else if key.is_empty() {
            ObjectPath::from(self.prefix.clone())
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, key))
        }
    }

    fn relative_key(&self, location: &ObjectPath) -> Option<String> {
        let full = location.as_ref();
        if self.prefix.is_empty() {
            return Some(full.to_string());
        }
        if full == self.prefix {
            return Some(String::new());
        }
        let prefix_with_slash = format!("{}/", self.prefix);
        full.strip_prefix(&prefix_with_slash).map(|v| v.to_string())
    }

    fn path_to_key(path: &str) -> String {
        if path == "/" {
            String::new()
        } else {
            path.trim_start_matches('/')
                .trim_end_matches('/')
                .to_string()
        }
    }
}

fn is_not_found(err: &ObjectError) -> bool {
    matches!(err, ObjectError::NotFound { .. })
}

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        trimmed.to_string()
    }
}
