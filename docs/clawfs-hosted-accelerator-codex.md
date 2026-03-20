# ClawFS Hosted Accelerator

## Summary
The ClawFS hosted accelerator is a managed service for organization-owned or organization-managed volumes. Its job is to reduce latency for remote clients and move maintenance work out of edge clients and into regional cloud services that run close to the volume's object storage.

The accelerator should preserve ClawFS's current architecture by default:

- clients keep direct access to the object store
- clients keep local journal and staging behavior
- the object store remains the durability boundary for committed data
- the superblock generation remains the source of truth

The accelerator is therefore an adjunct, not a replacement, for the current ClawFS data path. In v1 it should be off the steady-state read and write path by default, while also supporting an optional managed `relay_write` mode for organizations that want lower close and fsync latency than direct remote object commits can provide.

Current split-prep workspace note:

- `crates/clawfs-core` is the OSS/public facade intended for the future public repo
- `crates/clawfs-private` is the private facade intended for hosted, preload, and gateway consumers
- the hosted worker binaries still live in the root `clawfs` package for now, so container and release packaging continue to build them from the top-level manifest

This design builds on the current repo shape documented in:

- `docs/clawfs-project-reference.md`
- `docs/clawfs-storage-modes.md`
- `docs/CLEANUP_AGENT.md`
- `src/clawfs.rs`
- `src/launch.rs`
- `src/refreshable_store.rs`
- `src/superblock.rs`

## Goals
- Lower latency for remote or cross-region clients, especially metadata visibility delay and close or fsync-sensitive workflows.
- Offload maintenance operations, especially delta compaction and segment compaction, to cloud workers that run near the bucket.
- Keep direct client-to-object-store access viable for both BYOB and hosted volumes.
- Reuse the current runtime-spec and credential-refresh model rather than introducing a separate storage protocol everywhere.
- Provide a clean upgrade path from today's hosted bootstrap to a managed organizational accelerator.

## Non-Goals
- Replacing direct object-store access with a mandatory full read and write proxy.
- Storing raw user payloads permanently in a new control-plane database.
- Changing ClawFS durability semantics so that uncommitted cloud-side state becomes authoritative over the client journal.
- Making BYOB operation depend on a permanently available hosted service.

## Current Architecture Baseline
Today ClawFS is a log-structured filesystem that stages writes locally, writes immutable segments to object storage, stores metadata as immutable inode-map shards plus per-generation delta files, and coordinates commits through a CAS-updated superblock generation.

Important current behaviors:

- Clients stage large writes under the local `segment_stage` area and keep a local close-time journal under `$STORE/journal`.
- `flush_pending()` writes segment objects, emits metadata deltas, rewrites touched shards, and advances the superblock generation.
- Reads already use a combination of metadata caches, local segment cache, and direct object-store range reads.
- Remote changes are discovered by metadata polling, and the poller may prefetch segments for newly observed extents.
- Maintenance is already designed to be separable from foreground clients:
  - cleanup work is coordinated through short leases in the superblock
  - clients can run with `--disable-cleanup`
  - the repo already has a dedicated cleanup-agent design for near-bucket deployment
- Hosted runtime bootstrap already exists:
  - hosted mode can provide bucket, prefix, and short-lived credentials
  - credentials can refresh in place through `RefreshableObjectStore`

This matters because the hosted accelerator should extend the current architecture rather than fight it. The repo already makes one strong product decision: after bootstrap, steady-state data access should remain direct from clients to object storage unless there is a compelling managed mode that opts into a different tradeoff.

## Proposed Modes
The hosted accelerator should support three operating modes for managed organizational volumes.

### `direct`
This is the default managed mode.

- Reads and writes remain direct from the client to the object store.
- Clients keep the current journal, staging, flush, and replay behavior.
- The accelerator handles maintenance, credential issuance, and low-latency coordination only.

This mode gives the lowest architectural risk and the easiest fit with current ClawFS behavior.

### `direct_plus_cache`
This mode keeps the direct object-store path but adds accelerator-backed latency reduction.

- Reads still resolve through the local client cache first.
- The accelerator distributes generation or invalidation hints so clients do not rely only on fixed TTLs and polling intervals.
- The accelerator may maintain a hot metadata cache and a hot segment cache near the bucket.
- Clients still fetch objects directly on cache miss or when direct access is cheaper or simpler.

This mode aims to reduce remote metadata staleness and tail latency without making the accelerator a hard data-path dependency.

### `relay_write`
This mode is optional and should only be enabled for managed organizational volumes that explicitly opt in.

- The client still journals locally before acknowledging success to the caller.
- Instead of writing the flush batch directly to the bucket, the client sends the staged flush payload to the accelerator.
- The accelerator performs the segment upload, delta write, shard rewrite, and superblock commit in-region.
- The client clears its journal only after the accelerator returns the committed generation.

This mode is the main way to reduce close and fsync latency for far-away clients, but it introduces real coupling between foreground writes and accelerator availability. It should not be the default.

### Rejected v1 Mode: Full Proxy
A full read and write proxy is not recommended for v1.

Reasons:

- it conflicts with the current direct-object-store architecture
- it expands the accelerator blast radius from optimization service to mandatory storage gateway
- it makes ClawFS availability too dependent on a new service tier
- it adds migration and rollback complexity that is not required to capture the initial value

## Recommended Architecture
The hosted accelerator should be split into four logical subsystems, with a fifth optional subsystem for the `relay_write` mode.

### 1. Control Plane
Responsibilities:

- authenticate organization users and services
- store volume-level policy
- choose accelerator mode per volume
- issue short-lived prefix-scoped object credentials
- return runtime configuration to the launcher or mount path
- track the accelerator endpoint and mode flags for each volume

This extends the current hosted runtime contract rather than replacing it. The client should still receive the bucket, region or endpoint, object prefix, credential expiry, and capped runtime settings. The new addition is accelerator-specific configuration such as:

- accelerator endpoint
- accelerator mode
- invalidation-stream or event endpoint
- relay-write policy
- fallback policy if relay becomes unavailable

### 2. Coordination Service
Responsibilities:

- publish the latest known committed generation or metadata-head hints
- fan out invalidation events after commits
- reduce client dependence on fixed `metadata_poll_interval_ms`
- expose session-level health and policy hints
- coordinate managed maintenance workers for hosted volumes

This service is advisory, not authoritative. The superblock generation and object-store state remain the source of truth. If a client misses an event, it must still recover by polling and reconciling.

### 3. Maintenance Workers
Responsibilities:

- run delta compaction
- run segment compaction
- trigger or manage checkpoint workflows
- prune or expire hosted prefixes where product policy allows
- emit operational metrics for compaction volume, lease hold time, and backlog

This is the most natural first step because the repo already has:

- cleanup leases in `src/superblock.rs`
- client-side cleanup workers in `src/launch.rs`
- a near-bucket cleanup-agent design in `docs/CLEANUP_AGENT.md`

For managed accelerator-backed volumes, clients should normally run with cleanup disabled and let the hosted workers become the preferred lease holders.

### 4. Optional Near-Bucket Cache Layer
Responsibilities:

- keep hot metadata materialized in memory or local SSD
- optionally cache hot segment extents near the bucket
- serve advisory cache hits or metadata summaries to clients

This layer is optional because direct object fetches may remain faster or simpler in many deployments. It should not change correctness rules. Cache entries are disposable and reconstructible.

### 5. Optional Relay-Write Executor
Responsibilities:

- accept relay-write batches from clients
- guarantee idempotent replay handling
- write segments in-region
- persist metadata deltas and shard rewrites
- commit superblock generation
- return the committed generation back to the client

This subsystem should be isolated enough that volumes not using `relay_write` do not inherit its failure modes.
