# Quick Start: Deploy ClawFS CSI Driver

## Prerequisites

- Kubernetes cluster (1.20+)
- Helm 3+
- kubectl configured to access your cluster
- Docker image pushed to a registry (`my-registry/clawfs-csi:0.2.9`)

## Step 1: Update values.yaml

Edit `charts/clawfs/values.yaml` and update the image registry:

```yaml
csi:
  image:
    repository: my-registry/clawfs-csi  # Your registry
    tag: "0.2.9"
    pullPolicy: Always
```

## Step 2: Install Helm Chart

```bash
helm install clawfs ./charts/clawfs \
  --namespace clawfs \
  --create-namespace \
  --values charts/clawfs/values.yaml
```

## Step 3: Verify Installation

```bash
# Check pods
kubectl get pods -n clawfs

# Check DaemonSet
kubectl get daemonset -n clawfs

# Check StorageClass
kubectl get storageclass clawfs

# Check logs
kubectl logs -n clawfs -l app.kubernetes.io/component=csi-node -c csi-node
```

## Step 4: Create a Test Volume

Save this as `test-pvc.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: clawfs-test
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: clawfs
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: clawfs-test-pod
spec:
  containers:
    - name: app
      image: ubuntu:24.04
      volumeMounts:
        - name: data
          mountPath: /data
      command:
        - sh
        - -c
        - |
          echo "ClawFS volume mounted successfully!"
          ls -la /data
          sleep 3600
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: clawfs-test
```

Apply it:

```bash
kubectl apply -f test-pvc.yaml

# Check pod logs
kubectl logs clawfs-test-pod

# Verify mount
kubectl exec clawfs-test-pod -- df -h | grep clawfs
```

## Troubleshooting

### Pods not starting?

```bash
# Check DaemonSet status
kubectl describe daemonset -n clawfs clawfs-csi-node

# Check node scheduling
kubectl describe nodes | grep -A 5 clawfs

# Check events
kubectl get events -n clawfs --sort-by='.lastTimestamp'
```

### Mount failures?

```bash
# Check CSI driver logs
kubectl logs -n clawfs -l app.kubernetes.io/component=csi-node \
  -c csi-node --tail=100

# Check driver registrar
kubectl logs -n clawfs -l app.kubernetes.io/component=csi-node \
  -c driver-registrar --tail=100
```

### On worker node (SSH required):

```bash
# Check if socket exists
ls -la /var/lib/kubelet/plugins/clawfs.dev/

# Check mounts
mount | grep clawfs

# Check CSI driver process
ps aux | grep clawfs-csi
```

## Cleanup

```bash
# Remove test resources
kubectl delete pod clawfs-test-pod
kubectl delete pvc clawfs-test

# Uninstall Helm chart
helm uninstall clawfs -n clawfs

# Remove namespace
kubectl delete namespace clawfs
```

## Next Steps

1. Configure storage parameters for your deployment
2. Create PersistentVolumeClaims with the `clawfs` StorageClass
3. Mount them in your application pods
4. Monitor logs and events for issues

See [README.md](../crates/clawfs-csi/README.md) for detailed documentation.
