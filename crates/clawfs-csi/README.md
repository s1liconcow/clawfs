# ClawFS CSI Driver

A minimal Kubernetes Container Storage Interface (CSI) driver for mounting ClawFS volumes in Kubernetes pods.

## Overview

The ClawFS OSS CSI driver enables Kubernetes to mount existing ClawFS volumes into pods as persistent storage. It implements the CSI Node Service for mount/unmount operations and handles the provisioning of persistent volumes.

Please contact hi@clawfs.dev for access to the CSI driver that enables 'Pro' or 'Enterprise' features such as snapshots
or the hosted accelerator.

### Kubernetes Version Support

- Kubernetes 1.20+

### Features

- **Node mount/unmount operations** - Handles NodeStageVolume, NodePublishVolume, and related operations
- **Read-only mount support** - Can mount volumes as read-only
- **Staging support** - Optional staging for performance optimization
- **Unix socket communication** - Uses standard CSI Unix socket for kubelet integration

### Limitations (Current Minimal Implementation)

- **No controller service** - Does not handle volume provisioning/deletion (assumes volumes exist)
- **No snapshots** - Snapshot support currently not implemented
- **No volume expansion** - Dynamic volume expansion not supported
- **Basic mount operations** - Uses standard `mount` commands via system calls

## Building

Build the CSI driver binary:

```bash
cd crates/clawfs-csi
cargo build --release
```

This produces `target/release/clawfs-csi` (or `.exe` on Windows).

## Docker Image

To create a Docker image for the CSI driver:

```dockerfile
FROM rust:1.75 as builder
WORKDIR /build
COPY . .
RUN cargo build --release -p clawfs-csi

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/clawfs-csi /clawfs-csi
ENTRYPOINT ["/clawfs-csi"]
```

Build and tag:

```bash
docker build -t my-registry/clawfs-csi:0.2.9 -f Dockerfile.csi .
docker push my-registry/clawfs-csi:0.2.9
```

## Kubernetes Deployment

### Via Helm

1. Update `charts/clawfs/values.yaml` with your image registry:
   ```yaml
   csi:
     image:
       repository: my-registry/clawfs-csi
       tag: "0.2.9"
   ```

2. Install the Helm chart:
   ```bash
   helm install clawfs-csi ./charts/clawfs \
     --namespace clawfs \
     --create-namespace
   ```

### Manual Deployment

Apply the generated YAML:

```bash
kubectl apply -f - <<EOF
$(helm template clawfs-csi ./charts/clawfs --namespace clawfs)
EOF
```

## Using ClawFS Volumes

Once the CSI driver is deployed, you can create PVCs using the `clawfs` StorageClass:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: clawfs-volume
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: clawfs
  resources:
    requests:
      storage: 10Gi
```

Mount it in a pod:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: clawfs-pod
spec:
  containers:
    - name: app
      image: ubuntu:24.04
      volumeMounts:
        - name: clawfs-vol
          mountPath: /data
      command:
        - sh
        - -c
        - |
          echo "Data: $(cat /data/example)" || echo "No data yet"
          sleep infinity
  volumes:
    - name: clawfs-vol
      persistentVolumeClaim:
        claimName: clawfs-volume
```

## Architecture

### Components

- **Identity Service** - Provides plugin metadata and capabilities
- **Node Service** - Handles mount/unmount on worker nodes
- **Driver Registrar** - Sidecar that registers the plugin with kubelet

### Mount Flow

1. **NodeStageVolume**: Mounts ClawFS volume to a staging directory on the node
2. **NodePublishVolume**: Bind-mounts from staging directory to the container's target path
3. **NodeUnpublishVolume**: Unmounts from container target path
4. **NodeUnstageVolume**: Unmounts from staging directory

### Socket Communication

The driver communicates via Unix domain socket (default: `/csi/csi.sock`):

```
kubelet → /var/lib/kubelet/plugins/clawfs.dev/csi.sock → clawfs-csi binary
```

## Configuration

### Node Driver Arguments

```bash
clawfs-csi \
  --endpoint=unix:///csi/csi.sock \
  --node-id=$(hostname) \
  --log-level=info
```

### Volume Attributes

When creating PVCs with volume attributes, pass mount configuration:

```yaml
spec:
  storageClassName: clawfs
  parameters:
    state-path: /var/lib/clawfs     # Local state directory on node
    host: localhost                  # ClawFS daemon host
    port: "3049"                     # ClawFS daemon port
```

### Mount Options

Mount options can be passed via CSI requests or StorageClass parameters.

## Troubleshooting

### Check driver status

```bash
# Check if DaemonSet is running
kubectl get daemonset -n clawfs

# Check driver logs
kubectl logs -n clawfs -l app.kubernetes.io/component=csi-node -c csi-node

# Check driver registrar
kubectl logs -n clawfs -l app.kubernetes.io/component=csi-node -c driver-registrar
```

### Verify CSI socket

```bash
# On a worker node
ls -la /var/lib/kubelet/plugins/clawfs.dev/csi.sock
```

### Mount debugging

```bash
# Check mounted volumes on worker
mount | grep clawfs

# Check kubelet events
kubectl describe node <node-name> | grep -i clawfs
```

## Development

### Running Tests

```bash
cargo test -p clawfs-csi
```

### Building with Debug Output

```bash
RUST_LOG=debug cargo build -p clawfs-csi
```

## API Reference

### gRPC Service Endpoints

- **Identity.GetPluginInfo** - Returns plugin metadata
- **Identity.Probe** - Health check
- **Node.NodeStageVolume** - Prepare volume on node
- **Node.NodeUnstageVolume** - Clean up staged volume
- **Node.NodePublishVolume** - Mount volume to pod
- **Node.NodeUnpublishVolume** - Unmount volume from pod
- **Node.NodeGetCapabilities** - Report driver capabilities
- **Node.NodeGetInfo** - Return node information

## Future Enhancements

- [ ] Controller service for dynamic provisioning
- [ ] Snapshot support
- [ ] Volume expansion
- [ ] Health monitoring
- [ ] Advanced quota management
- [ ] Encryption support

## License

Same as ClawFS main project
