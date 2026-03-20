use tonic::{Request, Response, Status};
use crate::csi::*;
use std::process::Command;
use std::path::Path;
use log::{debug, error, info};

#[derive(Debug, Clone, Default)]
pub struct ClawFSNodeService {
    pub node_id: String,
}

impl ClawFSNodeService {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    fn ensure_mount_point_exists(&self, path: &str) -> Result<(), Status> {
        if !Path::new(path).exists() {
            std::fs::create_dir_all(path).map_err(|e| {
                error!("Failed to create mount point {}: {}", path, e);
                Status::internal(format!("Failed to create mount point: {}", e))
            })?;
        }
        Ok(())
    }

    fn mount_clawfs(&self, volume_id: &str, target_path: &str, attributes: &std::collections::HashMap<String, String>) -> Result<(), Status> {
        // Parse volume attributes to get connection details
        // Expected attributes:
        // - state-path: local state directory
        // - host: clawfs daemon host (optional, defaults to localhost)
        // - port: clawfs daemon port (optional, defaults to 3049)

        let state_path = attributes.get("state-path")
            .ok_or_else(|| {
                error!("Missing required attribute: state-path");
                Status::invalid_argument("Missing required attribute: state-path")
            })?;

        let host = attributes.get("host").map(|s| s.as_str()).unwrap_or("localhost");
        let port = attributes.get("port").map(|s| s.as_str()).unwrap_or("3049");

        info!("Mounting ClawFS volume {} at {} using daemon at {}:{}", volume_id, target_path, host, port);

        // Call clawfs mount command
        // Example: clawfs mount --volume-id <id> --mount-path <path> --state-path <state-path> --host <host> --port <port>
        let output = Command::new("clawfsd")
            .arg("mount")
            .arg("--volume-id").arg(volume_id)
            .arg("--mount-path").arg(target_path)
            .arg("--state-path").arg(state_path)
            .arg("--host").arg(host)
            .arg("--port").arg(port)
            .output()
            .map_err(|e| {
                error!("Failed to execute clawfsd mount: {}", e);
                Status::internal(format!("Failed to mount volume: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("clawfsd mount failed: {}", stderr);
            return Err(Status::internal(format!("Mount failed: {}", stderr)));
        }

        info!("Successfully mounted ClawFS volume {} at {}", volume_id, target_path);
        Ok(())
    }

    fn unmount_clawfs(&self, target_path: &str) -> Result<(), Status> {
        info!("Unmounting ClawFS volume at {}", target_path);

        // Use umount command (FUSE mounts use fusermount on Linux)
        let output = Command::new("umount")
            .arg(target_path)
            .output()
            .map_err(|e| {
                error!("Failed to execute umount: {}", e);
                Status::internal(format!("Failed to unmount volume: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("umount failed: {}", stderr);
            return Err(Status::internal(format!("Unmount failed: {}", stderr)));
        }

        info!("Successfully unmounted ClawFS volume at {}", target_path);
        Ok(())
    }
}

#[tonic::async_trait]
impl node_server::Node for ClawFSNodeService {
    async fn node_stage_volume(
        &self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        let req = request.into_inner();
        debug!("NodeStageVolume: volume_id={}, staging_target_path={}", req.volume_id, req.staging_target_path);

        self.ensure_mount_point_exists(&req.staging_target_path)?;
        self.mount_clawfs(&req.volume_id, &req.staging_target_path, &req.volume_attributes)?;

        Ok(Response::new(NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let req = request.into_inner();
        debug!("NodeUnstageVolume: volume_id={}, staging_target_path={}", req.volume_id, req.staging_target_path);

        self.unmount_clawfs(&req.staging_target_path)?;

        Ok(Response::new(NodeUnstageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        debug!("NodePublishVolume: volume_id={}, target_path={}", req.volume_id, req.target_path);

        // For a minimal implementation, we can either:
        // 1. Mount directly to target_path (skipping staging)
        // 2. Bind-mount from staging_target_path to target_path

        // We'll do a bind mount from staging path to target path
        if !req.staging_target_path.is_empty() && !req.target_path.is_empty() {
            self.ensure_mount_point_exists(&req.target_path)?;
            
            let output = Command::new("mount")
                .arg("--bind")
                .arg(&req.staging_target_path)
                .arg(&req.target_path)
                .output()
                .map_err(|e| {
                    error!("Failed to bind mount: {}", e);
                    Status::internal(format!("Bind mount failed: {}", e))
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                error!("Bind mount failed: {}", stderr);
                return Err(Status::internal(format!("Bind mount failed: {}", stderr)));
            }

            if req.readonly {
                let output = Command::new("mount")
                    .arg("-o").arg("remount,ro")
                    .arg(&req.target_path)
                    .output()
                    .map_err(|e| {
                        error!("Failed to remount read-only: {}", e);
                        Status::internal(format!("Read-only remount failed: {}", e))
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    error!("Read-only remount failed: {}", stderr);
                }
            }
        } else {
            // If no staging path, mount directly
            self.ensure_mount_point_exists(&req.target_path)?;
            self.mount_clawfs(&req.volume_id, &req.target_path, &req.volume_attributes)?;
        }

        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        debug!("NodeUnpublishVolume: volume_id={}, target_path={}", req.volume_id, req.target_path);

        self.unmount_clawfs(&req.target_path)?;

        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![
                NodeServiceCapability {
                    r#type: Some(node_service_capability::Type::Rpc(
                        node_service_capability::Rpc {
                            r#type: node_service_capability::rpc::Type::StageUnstageVolume as i32,
                        },
                    )),
                },
            ],
        }))
    }

    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_id.clone(),
        }))
    }
}
