use std::fs;
use std::future::Future;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use crate::config::{Config, ObjectStoreProvider};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentPointer {
    pub segment_id: u64,
    pub generation: u64,
    pub offset: u64,
    pub length: u64,
}

pub struct SegmentEntry {
    pub inode: u64,
    pub path: String,
    pub data: Vec<u8>,
}

pub struct SegmentManager {
    store: Arc<dyn ObjectStore>,
    handle: Handle,
    root_prefix: String,
}

impl SegmentManager {
    pub fn new(config: &Config, handle: Handle) -> Result<Self> {
        let (store, prefix): (Arc<dyn ObjectStore>, String) = match config.object_provider {
            ObjectStoreProvider::Local => {
                fs::create_dir_all(&config.store_path).with_context(|| {
                    format!("creating segment root {}", config.store_path.display())
                })?;
                let store = Arc::new(LocalFileSystem::new_with_prefix(config.store_path.clone())?)
                    as Arc<dyn ObjectStore>;
                (store, segment_prefix(&config.object_prefix))
            }
            ObjectStoreProvider::Aws => {
                let bucket = config
                    .bucket
                    .clone()
                    .context("--bucket is required for AWS provider")?;
                let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);
                let region = config
                    .region
                    .clone()
                    .unwrap_or_else(|| "us-east-1".to_string());
                builder = builder.with_region(&region);
                if let Some(endpoint) = &config.endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                let store = Arc::new(builder.build()?) as Arc<dyn ObjectStore>;
                (store, segment_prefix(&config.object_prefix))
            }
            ObjectStoreProvider::Gcs => {
                let bucket = config
                    .bucket
                    .clone()
                    .context("--bucket is required for GCS provider")?;
                let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);
                if let Some(sa_path) = &config.gcs_service_account {
                    let creds = sa_path.to_string_lossy().into_owned();
                    builder = builder.with_service_account_path(creds);
                }
                let store = Arc::new(builder.build()?) as Arc<dyn ObjectStore>;
                (store, segment_prefix(&config.object_prefix))
            }
        };
        Ok(Self {
            store,
            handle,
            root_prefix: prefix,
        })
    }

    pub fn write_batch(
        &self,
        generation: u64,
        segment_id: u64,
        entries: Vec<SegmentEntry>,
    ) -> Result<Vec<(u64, SegmentPointer)>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut buffer = Vec::new();
        buffer.extend_from_slice(b"OSG1");
        buffer.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        let mut pointers = Vec::with_capacity(entries.len());
        for entry in entries {
            buffer.extend_from_slice(&entry.inode.to_le_bytes());
            let path_bytes = entry.path.as_bytes();
            buffer.extend_from_slice(&(path_bytes.len() as u64).to_le_bytes());
            buffer.extend_from_slice(path_bytes);
            buffer.extend_from_slice(&(entry.data.len() as u64).to_le_bytes());
            let offset = buffer.len() as u64;
            buffer.extend_from_slice(&entry.data);
            pointers.push((
                entry.inode,
                SegmentPointer {
                    segment_id,
                    generation,
                    offset,
                    length: entry.data.len() as u64,
                },
            ));
        }
        let object_path = self.segment_path(generation, segment_id);
        let object_path_clone = object_path.clone();
        let store = self.store.clone();
        let payload = Bytes::from(buffer);
        self.run_store(
            async move {
                store
                    .put(&object_path_clone, PutPayload::from_bytes(payload))
                    .await
                    .map(|_| ())
            },
            || format!("writing segment {}", object_path),
        )?;
        Ok(pointers)
    }

    pub fn read_pointer(&self, pointer: &SegmentPointer) -> Result<Vec<u8>> {
        let path = self.segment_path(pointer.generation, pointer.segment_id);
        let path_clone = path.clone();
        let range = pointer.offset..(pointer.offset + pointer.length);
        let store = self.store.clone();
        let bytes = self.run_store(
            async move { store.get_range(&path_clone, range).await },
            || format!("reading segment {}", path),
        )?;
        Ok(bytes.to_vec())
    }

    fn segment_path(&self, generation: u64, segment_id: u64) -> ObjectPath {
        let base = self.root_prefix.trim_matches('/');
        let dir = if base.is_empty() {
            "segs".to_string()
        } else {
            format!("{}/segs", base)
        };
        ObjectPath::from(format!("{}/s_{generation:020}_{segment_id:020}", dir))
    }

    fn run_store<F, T, C>(&self, fut: F, ctx: C) -> Result<T>
    where
        F: Future<Output = object_store::Result<T>> + Send + 'static,
        C: FnOnce() -> String,
    {
        self.handle
            .block_on(fut)
            .map_err(anyhow::Error::from)
            .with_context(ctx)
    }
}

fn segment_prefix(user_prefix: &str) -> String {
    let trimmed = user_prefix.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, ObjectStoreProvider};
    use std::path::Path;
    use tempfile::tempdir;

    fn build_config(root: &Path) -> Config {
        Config {
            mount_path: root.join("mnt"),
            store_path: root.join("data"),
            inline_threshold: 1024,
            shard_size: 1024,
            inode_batch: 16,
            segment_batch: 32,
            pending_bytes: 1024 * 1024,
            object_provider: ObjectStoreProvider::Local,
            bucket: None,
            region: None,
            endpoint: None,
            object_prefix: "".into(),
            gcs_service_account: None,
            state_path: root.join("state.json"),
            foreground: false,
        }
    }

    #[test]
    fn write_and_read_segment_pointer() {
        let dir = tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();
        let config = build_config(dir.path());
        let manager = SegmentManager::new(&config, handle).unwrap();
        let entries = vec![SegmentEntry {
            inode: 42,
            path: "/foo.txt".into(),
            data: b"hello world".to_vec(),
        }];
        let pointers = manager.write_batch(7, 1, entries).unwrap();
        assert_eq!(pointers.len(), 1);
        let ptr = &pointers[0].1;
        let bytes = manager.read_pointer(ptr).unwrap();
        assert_eq!(bytes, b"hello world");
    }
}
