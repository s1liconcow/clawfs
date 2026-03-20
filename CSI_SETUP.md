# Kubernetes CSI Plugin Setup Guide

## What Has Been Created

I've set up a **minimal Kubernetes CSI Node Driver** for ClawFS. Here's the structure:

### 1. **CSI Driver Crate** (`crates/clawfs-csi/`)

This is a new Rust crate that implements the CSI Node Service:

- **proto/csi.proto** - gRPC service definitions for CSI
- **src/lib.rs** - Module definitions
- **src/identity.rs** - CSI Identity Service (GetPluginInfo, Probe)
- **src/node.rs** - CSI Node Service (mount/unmount operations)
- **src/main.rs** - gRPC server that listens on Unix socket
- **build.rs** - Proto compilation

### 2. **Kubernetes Deployment** (`charts/clawfs/`)

Helm chart templates for deploying the CSI driver:

- **csi-serviceaccount.yaml** - ServiceAccount for the driver
- **csi-rbac.yaml** - ClusterRole and ClusterRoleBinding
- **csi-daemonset.yaml** - DaemonSet that runs on all nodes
- **storageclass.yaml** - StorageClass for provisioning
- **Chart.yaml** - Chart metadata
- **values.yaml** - Default configuration

### 3. **Documentation**

- **README.md** - Complete setup and usage guide

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Kubernetes                           │
│  Pod wanting to mount ClawFS volume                      │
└──────────┬──────────────────────────────────────────────┘
           │
           │ requests mount via CSI
           ↓
┌─────────────────────────────────────────────────────────┐
│              kubelet (on worker node)                    │
└──────────┬──────────────────────────────────────────────┘
           │
           │ communicates via Unix socket
           ↓
        /var/lib/kubelet/plugins/clawfs.dev/csi.sock
           ↑
           │
┌──────────┴──────────────────────────────────────────────┐
│        clawfs-csi gRPC server (DaemonSet)               │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Identity Service                                 │   │
│  │  - GetPluginInfo                                │   │
│  │  - Probe                                        │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Node Service                                    │   │
│  │  - NodeStageVolume (mount to staging dir)      │   │
│  │  - NodePublishVolume (bind-mount to pod)       │   │
│  │  - NodeUnpublishVolume (unmount from pod)      │   │
│  │  - NodeUnstageVolume (unmount staging)         │   │
│  └─────────────────────────────────────────────────┘   │
└──────────┬──────────────────────────────────────────────┘
           │
           │ uses system mount commands
           ↓
    ┌──────────────┐
    │ mount -bind  │  Connects to ClawFS daemon
    │ umount       │  or mounts volume at target
    └──────────────┘
```

## Next Steps

### 1. Build the CSI Driver Binary

```bash
cd /workspaces/clawfs
cargo build --release -p clawfs-csi
```

This produces: `target/release/clawfs-csi`

### 2. Create Docker Image

```bash
# Create a Dockerfile (example provided in CSI README)
docker build -t my-registry.azurecr.io/clawfs-csi:0.2.9 -f Dockerfile.csi .
docker push my-registry.azurecr.io/clawfs-csi:0.2.9
```

### 3. Deploy to Kubernetes

```bash
# Update values.yaml with your image registry
vim charts/clawfs/values.yaml

# Install the chart
helm install clawfs ./charts/clawfs \
  --namespace clawfs \
  --create-namespace
```

### 4. Create a Test PVC

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: clawfs-test-pvc
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: clawfs
  resources:
    requests:
      storage: 10Gi
EOF
```

### 5. Mount in a Pod

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: clawfs-test-pod
spec:
  containers:
    - name: app
      image: ubuntu:24.04
      volumeMounts:
        - name: clawfs-vol
          mountPath: /data
      command: ["sleep", "infinity"]
  volumes:
    - name: clawfs-vol
      persistentVolumeClaim:
        claimName: clawfs-test-pvc
EOF

# Test the mount
kubectl exec clawfs-test-pod -- ls -la /data
```

## Key Files Overview

| File | Purpose |
|------|---------|
| `crates/clawfs-csi/Cargo.toml` | CSI driver dependencies |
| `crates/clawfs-csi/proto/csi.proto` | gRPC service definitions |
| `crates/clawfs-csi/src/main.rs` | Entry point, gRPC server setup |
| `crates/clawfs-csi/src/node.rs` | Mount/unmount logic |
| `charts/clawfs/templates/csi-daemonset.yaml` | K8s deployment |
| `charts/clawfs/Chart.yaml` | Helm chart metadata |
| `charts/clawfs/values.yaml` | Configuration defaults |

## Configuration Options

### In Helm values.yaml

```yaml
csi:
  image:
    repository: my-registry/clawfs-csi
    tag: "0.2.9"
  
  kubeletRegistrationPath: /var/lib/kubelet/plugins/clawfs.dev/csi.sock
  
  resources:
    limits:
      memory: 256Mi
    requests:
      cpu: 100m
      memory: 64Mi
  
  nodeSelector: {}
  tolerations: []
```

### In StorageClass parameters

```yaml
parameters:
  state-path: /var/lib/clawfs    # Where ClawFS keeps local state
  host: localhost                 # ClawFS daemon hostname
  port: "3049"                    # ClawFS daemon port
```

## What's Next?

1. **Build** the binary locally
2. **Containerize** it in a Docker image
3. **Push** to your registry
4. **Deploy** to your Kubernetes cluster
5. **Test** by creating PVCs and mounting them in pods

## Notes

- The CSI driver is **minimal** (node service only) - no controller/provisioning
- Assumes **ClawFS volumes already exist** on your system
- Uses standard Linux **`mount`** and **`umount`** commands
- Ready for **Kubernetes 1.20+**
- Includes **RBAC** for proper access control

For more details, see:
- [CSI Driver README](../crates/clawfs-csi/README.md)
- [Helm Chart Values](charts/clawfs/values.yaml)
