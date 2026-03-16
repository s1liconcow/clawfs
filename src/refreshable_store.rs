//! A wrapper around `Arc<dyn ObjectStore>` that transparently refreshes
//! credentials on auth failure (403 / PermissionDenied / Unauthenticated)
//! and retries the failed operation once.

use std::env;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use log::{info, warn};
use object_store::path::Path as ObjectPath;
use object_store::{
    Error as ObjError, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parking_lot::RwLock;

use crate::launch::{HostedControlPlane, fetch_credentials_from_api, save_cached_credentials};

/// A factory that can rebuild the underlying `ObjectStore` after credentials
/// have been refreshed in the environment.
pub type StoreBuilder = Arc<dyn Fn() -> anyhow::Result<Arc<dyn ObjectStore>> + Send + Sync>;

/// An `ObjectStore` that detects credential errors and transparently refreshes
/// AWS credentials from the control plane before retrying the failed call.
pub struct RefreshableObjectStore {
    inner: RwLock<Arc<dyn ObjectStore>>,
    builder: StoreBuilder,
    state_path: PathBuf,
    hosted: HostedControlPlane,
    refresh_lock: tokio::sync::Mutex<()>,
}

impl RefreshableObjectStore {
    pub fn new(
        initial_store: Arc<dyn ObjectStore>,
        builder: StoreBuilder,
        state_path: PathBuf,
        hosted: &HostedControlPlane,
    ) -> Self {
        Self {
            inner: RwLock::new(initial_store),
            builder,
            state_path,
            hosted: hosted.clone(),
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn is_credential_error(err: &ObjError) -> bool {
        matches!(
            err,
            ObjError::PermissionDenied { .. } | ObjError::Unauthenticated { .. }
        )
    }

    async fn refresh_credentials(&self) -> Result<(), ObjError> {
        let _guard = self.refresh_lock.lock().await;

        let hosted = self.hosted.clone();
        let state_path = self.state_path.clone();
        let creds = tokio::task::spawn_blocking(move || {
            fetch_credentials_from_api(&hosted).inspect(|c| {
                save_cached_credentials(&state_path, c);
            })
        })
        .await
        .map_err(|e| ObjError::Generic {
            store: "RefreshableObjectStore",
            source: e.into(),
        })?
        .map_err(|e| ObjError::Generic {
            store: "RefreshableObjectStore",
            source: e.into(),
        })?;

        unsafe {
            env::set_var("AWS_ACCESS_KEY_ID", &creds.access_key_id);
            env::set_var("AWS_SECRET_ACCESS_KEY", &creds.secret_access_key);
        }

        info!(
            "Credential refresh: rebuilt store (key={}..., expires={:?})",
            &creds.access_key_id[..creds.access_key_id.len().min(12)],
            creds.expires_at,
        );

        let new_store = (self.builder)().map_err(|e| ObjError::Generic {
            store: "RefreshableObjectStore",
            source: e.into(),
        })?;

        *self.inner.write() = new_store;
        Ok(())
    }

    fn store(&self) -> Arc<dyn ObjectStore> {
        self.inner.read().clone()
    }
}

impl fmt::Debug for RefreshableObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefreshableObjectStore").finish()
    }
}

impl fmt::Display for RefreshableObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RefreshableObjectStore({})", self.store())
    }
}

#[async_trait]
impl ObjectStore for RefreshableObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, ObjError> {
        let payload2 = payload.clone();
        let opts2 = opts.clone();
        let result = self.store().put_opts(location, payload, opts).await;
        match result {
            Err(ref e) if Self::is_credential_error(e) => {
                warn!("Credential error on put, refreshing: {e}");
                self.refresh_credentials().await?;
                self.store().put_opts(location, payload2, opts2).await
            }
            other => other,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, ObjError> {
        let opts2 = opts.clone();
        let result = self.store().put_multipart_opts(location, opts).await;
        match result {
            Err(ref e) if Self::is_credential_error(e) => {
                warn!("Credential error on put_multipart, refreshing: {e}");
                self.refresh_credentials().await?;
                self.store().put_multipart_opts(location, opts2).await
            }
            other => other,
        }
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> Result<GetResult, ObjError> {
        let options2 = options.clone();
        let result = self.store().get_opts(location, options).await;
        match result {
            Err(ref e) if Self::is_credential_error(e) => {
                warn!("Credential error on get, refreshing: {e}");
                self.refresh_credentials().await?;
                self.store().get_opts(location, options2).await
            }
            other => other,
        }
    }

    async fn delete(&self, location: &ObjectPath) -> Result<(), ObjError> {
        let result = self.store().delete(location).await;
        match result {
            Err(ref e) if Self::is_credential_error(e) => {
                warn!("Credential error on delete, refreshing: {e}");
                self.refresh_credentials().await?;
                self.store().delete(location).await
            }
            other => other,
        }
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjError>> {
        self.store().list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> Result<ListResult, ObjError> {
        let result = self.store().list_with_delimiter(prefix).await;
        match result {
            Err(ref e) if Self::is_credential_error(e) => {
                warn!("Credential error on list_with_delimiter, refreshing: {e}");
                self.refresh_credentials().await?;
                self.store().list_with_delimiter(prefix).await
            }
            other => other,
        }
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> Result<(), ObjError> {
        let result = self.store().copy(from, to).await;
        match result {
            Err(ref e) if Self::is_credential_error(e) => {
                warn!("Credential error on copy, refreshing: {e}");
                self.refresh_credentials().await?;
                self.store().copy(from, to).await
            }
            other => other,
        }
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> Result<(), ObjError> {
        let result = self.store().copy_if_not_exists(from, to).await;
        match result {
            Err(ref e) if Self::is_credential_error(e) => {
                warn!("Credential error on copy_if_not_exists, refreshing: {e}");
                self.refresh_credentials().await?;
                self.store().copy_if_not_exists(from, to).await
            }
            other => other,
        }
    }
}
