# Hosted Accelerator Runbook

This runbook is for operators running managed ClawFS volumes with hosted maintenance, coordination, cache hints, or relay-write enabled.

It is operational, not architectural. For the design model, see `docs/clawfs-hosted-accelerator-codex.md`. For the implementation surfaces, see `src/launch.rs`, `src/maintenance.rs`, `src/coordination.rs`, `src/hosted_cache.rs`, `src/relay.rs`, and `benches/hosted.rs`.

## Deployment Topology

Hosted maintenance should run in the same region as the bucket and close enough to the object store to keep lease-driven cleanup cheap. The maintenance worker is the first hosted component that should be deployed for managed volumes.

The coordination service is optional and advisory. It can run in the same region as the bucket, but it does not become authoritative for generation state. Clients must still be able to read the object store directly if the service is unavailable.

The relay-write executor is only for volumes that explicitly opt in. It should be placed next to the bucket, preferably in the same region and VPC. Relay-write adds a foreground dependency, so it should not be deployed as a generic shared gateway.

Network expectations:

| Path | Expectation |
| --- | --- |
| Client -> object store | Always available for direct and direct_plus_cache modes |
| Client -> coordination | Low-latency preferred, but advisory only |
| Client -> relay executor | Lowest practical latency, because close/fsync depend on it |
| Worker -> object store | Same-region or same-VPC preferred |
| Org worker replica -> replica | Pod-to-pod or VPC-internal; required for HA relay forwarding |

## Normal Operation

### Hosted Maintenance

Healthy hosted maintenance means the worker acquires cleanup leases, clears backlog, and keeps the client-side cleanup worker disabled for managed volumes.

Watch:

| Signal | Healthy target | Notes |
| --- | --- | --- |
| Cleanup lease acquisition | Success rate above 95% over 10 minutes | `cleanup_lease_acquired` should be common, not rare |
| Lease contention | Low and transient | Sustained contention usually means another worker is holding leases too long |
| Delta backlog | Below `2 x DELTA_COMPACT_THRESHOLD` | The current default threshold is 128 files |
| Segment backlog | Stable or falling | The current compaction loop batches 8 candidates and lags by 3 generations |
| Client maintenance bytes | Near zero for hosted clients | Maintenance traffic should move off edge clients |

Current benchmark reference from `benches/hosted.rs` and the local hosted perf artifact:

| Workload | direct p99 | hosted_maintenance_only p99 | Interpretation |
| --- | --- | --- | --- |
| close/fsync latency | 30.123 ms | 31.328 ms | Maintenance hosting should stay within about 10% of direct baseline |
| metadata visibility lag | 9.607 ms | 9.609 ms | Maintenance alone should not materially change freshness |
| small-file metadata-heavy | 11736.172 ms | 10615.133 ms | Small variance is acceptable, but the main win is offloaded maintenance traffic |

### Coordination

Healthy coordination means clients receive invalidation or generation hints and continue to fall back safely if hints stop arriving.

Watch:

| Signal | Healthy target | Notes |
| --- | --- | --- |
| Event lag | Below the configured staleness timeout | Default timeout in code is 60 seconds |
| Subscriber state | `connected`, not `poll_only` | Poll-only is a degraded fallback, not the desired steady state |
| Full refreshes | Rare | Repeated full refreshes usually mean missed events or a bad event endpoint |
| Cache hit rate | Rising on hot metadata workloads | Cache misses should still fall back to the object store cleanly |

Current benchmark reference:

| Workload | direct p99 | direct_plus_cache p99 | Interpretation |
| --- | --- | --- | --- |
| metadata visibility lag | 9.607 ms | 1.626 ms | This is the main direct_plus_cache win |
| hot-read tail latency | 950.293 ms | 13.884 ms | Cache hits should collapse tail latency into the low tens of ms |
| small-file metadata-heavy | 11736.172 ms | 1400.562 ms | Advisory metadata cache should dramatically reduce metadata-heavy latency |

### Relay-Write

Relay-write is healthy only when the volume explicitly opted in, the relay endpoint is reachable, and the client sees `Committed` responses fast enough that the mode is paying for itself.

Watch:

| Signal | Healthy target | Notes |
| --- | --- | --- |
| Commit result | `Committed`, not `Accepted` | Do not clear journal state until committed generation is confirmed |
| Policy state | `Normal` | `Blocked` means fail-closed is engaged |
| Queue depth | Bounded and draining | Queue-and-retry is only acceptable if it stays bounded |
| Retry rate | Low | Repeated retries usually mean the relay is slower than direct flush |

Current local benchmark artifact does not show a relay-write win for close/fsync:

| Workload | direct p99 | relay_write p99 | Interpretation |
| --- | --- | --- | --- |
| close/fsync latency | 30.123 ms | 34.208 ms | In this environment relay-write is slower than direct, so it is not a default rollout candidate |
| metadata visibility lag | 9.607 ms | 9.782 ms | Relay-write does not change freshness; it is a write-path feature |
| small-file metadata-heavy | 11736.172 ms | 10688.274 ms | The workload is still dominated by metadata churn, not relay latency |

Use the latest hosted benchmark report as the rollout gate for relay-write. If the latest report does not beat direct baseline by at least one object-store RTT for the relevant workload, keep the volume off relay-write.

## Failure Diagnosis

### Maintenance Worker Outage

Symptoms:

- Delta backlog rises steadily.
- Segment backlog stops shrinking.
- `cleanup_lease_contention` may disappear entirely because nothing is acquiring leases.
- Clients still read and write, but cleanup debt accumulates.

Checks:

- Look for `cleanup_lease_acquired`, `cleanup_lease_released`, and `compaction_completed`.
- Check worker health output for last lease holder and last compaction result.
- Verify the worker is still in the bucket region and still reaching the object store.

Recovery:

- Restart the maintenance worker.
- Confirm cleanup leases are being acquired again.
- Keep managed clients on `--disable-cleanup` so they do not reclaim the maintenance loop by accident.

Status classification:

- This is safely degraded, not actively blocked.

### Coordination Stream Outage

Symptoms:

- Clients stop receiving generation hints.
- The subscriber enters poll-only mode or repeatedly reconnects.
- Metadata freshness improves only on poll intervals instead of event-driven updates.

Checks:

- Look for `coordination_reconcile_full_refresh`, subscriber health transitions, and poll-only fallback.
- Confirm that `hosted_cache_stale_discarded` still appears when stale cache entries are encountered.
- Verify the coordination endpoint is reachable from the client network path.

Recovery:

- Restore the event endpoint.
- If the stream stays unstable, leave clients on polling fallback until the endpoint is stable again.
- Do not treat the coordination stream as authoritative; the object store remains the source of truth.

Status classification:

- This is safely degraded if clients can still poll and read the object store.

### Relay-Write Outage

Symptoms:

- Close or fsync calls fail or stall on relay-write volumes.
- `relay_write_blocked` appears when fail-closed policy is engaged.
- Journal entries remain uncleared because commit confirmation never arrived.

Checks:

- Confirm the configured relay policy for the volume.
- Check whether the relay endpoint is reachable and whether the relay executor is still committing generations.
- Inspect `relay_status` and `accelerator_status` at startup or in periodic logs.

Recovery:

- If the volume is configured `FailClosed`, restore the relay service or switch the volume out of relay-write mode.
- If the volume is explicitly allowed to `DirectWriteFallback`, switch that policy only after confirming it is permitted for the volume.
- If the volume is `QueueAndRetry`, keep the queue bounded and drain it before considering the incident resolved.

Status classification:

- This is actively blocked when `FailClosed` is engaged.

## Rollout Sequencing

### Phase 1: Hosted Maintenance

Safe to enable broadly for managed volumes.

Roll forward:

- Deploy the maintenance worker near the bucket.
- Disable local cleanup on managed clients.
- Confirm lease acquisition and backlog reduction.

Rollback:

- Stop the hosted worker.
- Re-enable client cleanup only if the volume is no longer managed.

### Phase 2: Coordination And Cache Hints

Enable per volume after the event stream and cache behavior are verified.

Roll forward:

- Enable the coordination endpoint.
- Verify that visibility lag drops and stale cache entries are discarded.
- Confirm poll-only fallback still preserves correctness.

Rollback:

- Disable the event endpoint.
- Leave direct object-store reads and writes intact.

### Phase 3: Relay-Write

Opt-in only.

Roll forward:

- Enable relay-write on one volume first.
- Verify journal replay, duplicate handling, and commit confirmation.
- Keep the policy fail-closed until the deployment proves stable.

Rollback:

- Disable relay-write for the affected volume.
- If direct fallback is allowed for that volume, switch only after confirming policy and latency tradeoffs.
- Do not clear journal state until a committed generation is confirmed.

## Org-Scoped Worker (`clawfs_org_worker`)

The org-scoped worker manages maintenance and relay commit authority for **all**
ClawFS volumes under a single discovery prefix.  It replaces the per-volume
`clawfs_maintenance_worker` and `clawfs_relay_executor` binaries for multi-volume
deployments and discovers volumes dynamically, so no volume list configuration is
required at startup.

### When to use

| Mode | Recommended binary |
| --- | --- |
| Single volume, maintenance only | `clawfs_maintenance_worker` |
| Single volume, relay write | `clawfs_relay_executor` |
| Many volumes under one org prefix | `clawfs_org_worker` |

### Required arguments

| Flag | Description |
| --- | --- |
| `--discovery-prefix` | Org-scoped root the worker scans for volumes (e.g. `orgs/myorg`). The worker NEVER touches paths outside this prefix. |
| `--bucket` / `--region` / `--endpoint` | Object store connection (same as single-volume binaries). |
| `--advertise-url` | Publicly reachable URL of this replica — required when `--enable-relay` is set. Written into relay owner records so peer replicas can forward requests here. |
| `--replica-id` | Stable, unique identity for this replica — required when `--enable-relay` is set. Must survive restarts. Use the pod name or a UUID stored in a ConfigMap. |

### Optional arguments with defaults

| Flag | Default | Description |
| --- | --- | --- |
| `--scan-interval-secs` | `300` | How often to rescan for new or removed volumes. |
| `--max-active-volumes` | `64` | Maximum simultaneously loaded volume contexts. Contexts idle longer than `--idle-eviction-secs` are evicted. |
| `--enable-maintenance` | `true` | Run delta/segment compaction, checkpointing, and lifecycle cleanup. |
| `--maintenance-concurrency` | `4` | Maximum concurrent per-volume maintenance rounds. |
| `--maintenance-poll-secs` | `30` | How often to check for due work. |
| `--enable-relay` | `false` | Enable relay write ownership and forwarding. Requires `--advertise-url` and `--replica-id`. |
| `--relay-listen` | `0.0.0.0:8080` | HTTP server address (relay + health). |
| `--jwt-secret` / `CLAWFS_RELAY_JWT_SECRET` | unset (dev mode) | HMAC-SHA256 secret for relay JWT tokens. Unset accepts all requests. |

### Deployment: container image

The shared `hosted-accelerator` image already includes the binary.  Select it
with `HOSTED_ACCELERATOR_BIN=clawfs_org_worker`:

```yaml
# docker run example
docker run \
  -e HOSTED_ACCELERATOR_BIN=clawfs_org_worker \
  -e CLAWFS_RELAY_JWT_SECRET=<secret> \
  hosted-accelerator \
  --bucket my-org-bucket \
  --region us-east-1 \
  --discovery-prefix orgs/myorg \
  --enable-relay \
  --advertise-url http://$(hostname):8080 \
  --replica-id $(hostname)
```

### Deployment: Kubernetes (HA relay, 2 replicas)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: clawfs-org-worker
spec:
  replicas: 2
  selector:
    matchLabels:
      app: clawfs-org-worker
  template:
    metadata:
      labels:
        app: clawfs-org-worker
    spec:
      containers:
      - name: worker
        image: ghcr.io/yourorg/hosted-accelerator:latest
        env:
        - name: HOSTED_ACCELERATOR_BIN
          value: clawfs_org_worker
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: CLAWFS_RELAY_JWT_SECRET
          valueFrom:
            secretKeyRef:
              name: clawfs-relay-secret
              key: jwt-secret
        args:
        - --bucket=my-org-bucket
        - --region=us-east-1
        - --discovery-prefix=orgs/myorg
        - --enable-relay
        - --replica-id=$(POD_NAME)
        - --advertise-url=http://$(POD_NAME).clawfs-org-worker-headless.default.svc.cluster.local:8080
        ports:
        - containerPort: 8080
---
# Headless service for pod-to-pod forwarding (required for HA relay).
# Replicas forward relay requests to the current lease-owner via its
# advertise-url.  Without backend reachability the forwarding hop fails.
apiVersion: v1
kind: Service
metadata:
  name: clawfs-org-worker-headless
spec:
  clusterIP: None
  selector:
    app: clawfs-org-worker
  ports:
  - port: 8080
```

### Network requirements for HA relay forwarding

The org-scoped worker performs internal request forwarding when it receives a
relay write for a volume whose lease is held by another replica.  This requires
**backend-network reachability between all replicas**.

| Path | Requirement |
| --- | --- |
| Client → any replica | Routable; typically a ClusterIP or load-balancer service |
| Replica → replica (forwarding) | Direct pod-to-pod or VPC-internal; NOT through a public load balancer |
| Replica → object store | Same-region or same-VPC preferred |

**Common failure mode**: all replicas are reachable from clients but not from
each other (e.g., deployed behind a public load balancer with no pod-to-pod
route).  The forwarding hop returns a connection error and the non-owner replica
returns HTTP 502 to the client.  Use a headless service in Kubernetes or a
VPC-internal ALB listener in ECS/Cloud Run.

Max forwarding hops before rejection: **2** (prevents infinite loops).

### Health endpoints

```
GET  /health                    → aggregate health JSON
GET  /health/volumes            → per-volume health list
GET  /health/volumes/<prefix>   → single volume detail (relay_enabled, maintenance_enabled, generation, shard_size)
```

### Per-volume policy override

Place `<volume-prefix>/metadata/accelerator_policy.json` in the bucket to
override defaults for a specific volume.  The worker re-reads the policy file
on each discovery scan; updates take effect without restarting the binary.

```json
{
  "relay_enabled": true,
  "maintenance_enabled": true,
  "checkpoint_enabled": true,
  "checkpoint_interval_secs": 86400,
  "lifecycle_cleanup": false
}
```

Missing fields use defaults (see `src/org_policy.rs`).  An invalid JSON file
marks only that volume as `unhealthy_policy`; other volumes continue normally.

### Rollout guidance

1. Deploy the org-scoped worker alongside existing per-volume workers.
2. Verify discovery via `GET /health/volumes` — all expected volumes should appear within one scan interval.
3. Confirm per-volume health is `healthy_idle` or `healthy_backlogged` (not `unhealthy_init`).
4. Set `relay_enabled: true` in policy files one volume at a time.
5. After relay is stable, decommission per-volume relay executor instances.
6. Finally, decommission per-volume maintenance workers once the org worker confirms sustained healthy maintenance rounds.

Rollback: set `HOSTED_ACCELERATOR_BIN=clawfs_maintenance_worker` (or `clawfs_relay_executor`) to return to per-volume mode.  The org-scoped worker leaves no side effects in the object store beyond the relay owner lease file (`<prefix>/metadata/relay_owner.json`), which expires automatically after `RELAY_OWNER_LEASE_TTL_SECS` (30 seconds).

## Alert Definitions

| Alert | Threshold | Duration | Severity | Response |
| --- | --- | --- | --- | --- |
| `maintenance_backlog_high` | Delta count greater than `2 x DELTA_COMPACT_THRESHOLD` | 1 hour | Warning | Check the worker, the lease owner, and object-store reachability |
| `coordination_disconnected` | No events for longer than `staleness_timeout` | 5 minutes | Warning | Expect poll-only fallback and investigate the event endpoint |
| `relay_write_blocked` | FailClosed policy engaged | 5 minutes | Critical | Restore relay availability or move the volume off relay-write |
| `lease_contention_high` | Lease acquisition failure rate above 50% | 10 minutes | Warning | Look for a stuck worker or too many competing workers |

## What To Check First

When a managed volume looks unhealthy, check these signals in order:

1. `accelerator_status`
2. `cleanup_owner`
3. `coordination_status`
4. `relay_status`
5. Lease acquisition and compaction logs
6. The latest `benches/hosted.rs` report for the workload that regressed

If the system is safely degraded, the object store and superblock generation should still be authoritative. If the system is actively blocked, relay-write policy is usually the first place to look.
