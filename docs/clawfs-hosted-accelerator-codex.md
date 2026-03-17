# ClawFS Hosted Accelerator

## Summary
The ClawFS hosted accelerator is a managed service for organization-owned or organization-managed volumes. Its job is to reduce latency for remote clients and move maintenance work out of edge clients and into regional cloud services that run close to the volume's object storage.

The accelerator should preserve ClawFS's current architecture by default:

- clients keep direct access to the object store
- clients keep local journal and staging behavior
- the object store remains the durability boundary for committed data
- the superblock generation remains the source of truth

The accelerator is therefore an adjunct, not a replacement, for the current ClawFS data path. In v1 it should be off the steady-state read and write path by default, while also supporting an optional managed `relay_write` mode for organizations that want lower close and fsync latency than direct remote object commits can provide.

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
- The client clears its journal only after the accelerator returns a committed generation.

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

## End-To-End Workflow

### Bootstrap
1. The administrator enables the hosted accelerator for a managed volume.
2. The control plane stores the selected mode and policy for that volume.
3. The client authenticates through the hosted launcher or mount flow.
4. The client receives:
   - object provider
   - bucket
   - region or endpoint
   - object prefix
   - short-lived scoped object credentials
   - runtime caps where relevant
   - accelerator endpoint
   - accelerator mode
   - event or invalidation settings
   - relay fallback policy if applicable
5. The client mounts ClawFS using the existing runtime bootstrap plus the new accelerator hints.

### Normal Read Flow In `direct` And `direct_plus_cache`
1. The client checks its local metadata and segment caches.
2. The client consults accelerator generation or invalidation hints when available.
3. If the local view is current, the read continues using normal ClawFS paths.
4. If the client detects a newer generation or cache miss, it refreshes metadata and reads objects directly from the bucket.
5. In `direct_plus_cache`, the accelerator may reduce latency by:
   - pushing invalidation hints quickly
   - serving hot metadata summaries
   - redirecting to a regional cache for hot extents when beneficial

The correctness rule remains unchanged: the object store plus superblock generation is authoritative.

### Normal Write Flow In `direct` And `direct_plus_cache`
1. The client stages data locally and records journal state as it does today.
2. The client flushes directly to object storage.
3. The client writes metadata deltas, rewrites touched shards, and commits the new generation.
4. After commit, the accelerator coordination layer fanouts invalidation or generation-advance notifications to other clients.
5. Hosted maintenance workers compact deltas and segments later using leases.

This mode preserves today's durability and recovery model exactly where possible.

### Write Flow In `relay_write`
1. The client stages locally and persists the relevant journal entry before acknowledging the caller.
2. On flush, close, or fsync, the client sends a relay-write batch to the accelerator instead of writing segments and metadata directly.
3. The batch includes:
   - client identity
   - volume identity
   - idempotency key
   - the staged data needed for the flush
   - metadata context needed to commit safely
4. The accelerator performs the full commit in-region:
   - upload segment object or objects
   - emit delta object or objects
   - rewrite touched shard objects
   - advance the superblock generation with CAS
5. The accelerator returns the committed generation.
6. The client clears the corresponding journal state only after receiving a successful committed response.

The main benefit is lower cross-region write latency. The main cost is tighter coupling between foreground writes and the accelerator service.

## Constraints
- The hosted accelerator must preserve prefix-scoped isolation for organizational volumes.
- The accelerator must not require the control plane to store raw filesystem payloads permanently.
- The superblock generation remains the authoritative ordering mechanism.
- The client journal remains authoritative for unacknowledged or uncommitted writes.
- Missed events or stale cache entries must degrade to poll-and-reconcile, not silent corruption.
- BYOB direct operation must remain viable even if the accelerator feature exists.
- The initial design should be provider-neutral first:
  - use portable object-store concepts
  - avoid assuming only AWS-specific primitives
  - allow an AWS-first implementation path without baking AWS-only semantics into the architecture

## Architecture Choices
The following choices should be treated as design decisions, not open questions.

### Keep Direct Object Access As The Default
This aligns with the current storage-modes design and avoids re-architecting ClawFS around a mandatory proxy tier.

### Offload Maintenance First
Hosted maintenance is the cleanest v1 step because:

- the repo already supports `--disable-cleanup`
- lease-based maintenance is already implemented
- near-bucket maintenance has direct bandwidth and latency benefits
- this adds operational value without changing correctness semantics for reads and writes

### Add Event-Driven Coordination Before Deep Protocol Changes
Client-visible staleness today comes partly from polling intervals and TTLs. A coordination service that fans out generation updates should improve multi-client visibility before ClawFS adopts a more invasive metadata protocol.

### Make `relay_write` Opt-In
`relay_write` should be a premium or managed-volume feature for workloads where lower write latency is worth extra service coupling and more complicated recovery logic.

## Tradeoffs

### Off-Path Default
Pros:

- fits the current ClawFS architecture
- preserves client independence from the accelerator
- failures degrade performance and freshness more than correctness
- easier rollout and rollback

Cons:

- only modest improvement for close and fsync latency
- reads still depend mainly on client caches, TTLs, and direct object fetch cost

### `direct_plus_cache`
Pros:

- better metadata freshness and faster remote visibility
- improves hot-read latency without requiring full proxy behavior
- still keeps the object store as the primary data path

Cons:

- cache invalidation and generation hint ordering must be correct
- benefits may vary by workload and region layout
- adds a service tier that clients will want to consult even when correctness does not require it

### `relay_write`
Pros:

- strongest latency improvement for remote write commit paths
- moves segment upload and metadata commit work next to the bucket
- creates a clean managed mode for organizations that want tighter service guarantees

Cons:

- accelerator availability now affects foreground write availability
- journal acknowledgement rules become more complicated
- relay operations must be idempotent and replay-safe
- fallback policy must be explicit to avoid silent semantic changes

### Full Proxy
Pros:

- maximum central control
- easiest place to insert future locking or stronger serialization features

Cons:

- largest blast radius
- biggest architecture departure
- highest operational cost
- unnecessary for the initial hosted-accelerator value proposition

Full proxy is therefore out of scope for v1.

## Failure Handling

### Accelerator Unavailable In `direct`
The volume should continue to operate.

Expected behavior:

- clients continue direct reads and writes to the object store
- clients fall back to normal metadata polling
- maintenance may pause or run from another hosted worker
- latency and freshness degrade, but the volume should not be considered down solely because the accelerator is unavailable

### Accelerator Unavailable In `direct_plus_cache`
The volume should also continue to operate.

Expected behavior:

- clients ignore cache-hint or invalidation outages and fall back to polling
- direct object fetch remains available
- cached responses are advisory only and never the sole source of truth

### Accelerator Unavailable In `relay_write`
The default policy should be fail closed.

Expected behavior:

- new relay-write commits fail rather than silently switching semantics
- the client keeps local journal state intact
- operators may enable a per-volume direct-write fallback policy, but that must be explicit and logged because it changes latency and failure behavior

This default is safer than auto-falling back from managed relay commits to unmanaged direct commits without operator intent.

### Replay And Idempotency
Every relay-write request must include a stable idempotency key derived from the client and its journal or flush identity.

Required behavior:

- duplicate requests must not create duplicate committed generations or duplicate object state
- if a request was already committed, the accelerator should return the committed generation instead of re-applying it blindly
- client retries after timeout must be safe

### Credential Expiry
Hosted volumes already have a credential refresh pattern. The accelerator should extend it, not replace it.

Required behavior:

- direct reads and writes keep using refreshable scoped object credentials
- accelerator session metadata can refresh independently
- credential refresh must not change the logical identity of the volume or its object prefix

### Cache Staleness
Cache entries and event streams are advisory.

Required behavior:

- superblock generation stays authoritative
- missed invalidation events fall back to poll-and-reconcile
- clients never assume a cache hit is correct if generation checks disagree

### Split-Brain Maintenance
Hosted maintenance workers should be the preferred lease holders for managed volumes, but correctness must not depend on only one process ever attempting cleanup.

Required behavior:

- lease acquisition remains the exclusivity mechanism
- expired leases can be taken over
- maintenance operations remain safe across process death and retry

### Crash Recovery
The boundary for journal clearing must stay conservative.

Required behavior:

- direct modes clear journal only after the normal ClawFS commit path succeeds
- `relay_write` clears journal only after the accelerator returns a committed generation
- partially applied relay operations reconcile by idempotent retry, not manual operator repair in the normal case

## Testing Plan

### Unit Tests
- runtime-spec parsing for accelerator endpoint, mode, and fallback policy
- hosted mode clamping and compatibility with existing hosted-free behavior
- idempotency-key generation and duplicate-request handling
- lease ownership and handoff logic for hosted maintenance workers
- invalidation ordering and generation-advance hint handling

### Integration Tests
- managed volume bootstrap returning object config plus accelerator config
- clients mounted with cleanup disabled while hosted workers perform compaction
- multi-client metadata visibility with event fanout and with polling-only fallback
- `direct_plus_cache` reads that still succeed when the coordination channel is unavailable
- `relay_write` close and fsync flows preserving local journal semantics
- credential refresh while the filesystem remains mounted and active

### Failure-Injection Tests
- accelerator outage during read-heavy workloads
- accelerator outage before relay commit starts
- accelerator outage after segment upload but before generation commit
- duplicate relay request delivery after client timeout
- lease expiry in the middle of compaction
- missed invalidation event delivery causing clients to fall back to polling

### Performance Validation
Compare at least three modes:

- baseline direct mode without accelerator
- `direct_plus_cache`
- `relay_write`

Track:

- close latency
- fsync latency
- small-file create and rename latency
- multi-client visibility lag for metadata updates
- remote read tail latency
- maintenance bandwidth removed from edge clients

Prefer existing tooling where possible:

- `src/perf.rs`
- `src/replay.rs`
- `scripts/fio_workloads.sh`
- `scripts/micro_workflows.sh`

## Proposed Phasing

### Phase 1: Hosted Maintenance Offload
- Deploy managed near-bucket maintenance workers.
- Disable cleanup on accelerator-managed clients by default.
- Keep read and write paths direct.
- Add basic accelerator registration and health reporting to hosted runtime config.

This phase captures immediate value with low architectural risk.

### Phase 2: Coordination And Cache Hints
- Add generation and invalidation fanout.
- Reduce dependence on fixed polling intervals for visibility.
- Add optional hot metadata and hot-segment caching near the bucket.

This phase targets remote metadata freshness and tail-latency improvements.

### Phase 3: Optional `relay_write`
- Introduce managed relay-write support for selected organizational volumes.
- Define volume-level policy for fallback behavior.
- Add stronger SLO and observability requirements because foreground writes now depend on the accelerator.

This phase should only ship after idempotent replay and failure handling are proven in test.

## Operational Notes
- Managed volumes should expose accelerator mode and health in tooling so operators can quickly distinguish direct degradation from relay-write outage.
- Hosted workers should emit metrics for:
  - lease acquisition success and contention
  - delta backlog
  - segment-compaction backlog
  - relay-write latency and retry rate
  - invalidation fanout lag
- The accelerator should be regionally colocated with the volume's bucket. Client proximity matters too, but near-bucket placement is the first requirement because both maintenance and relay-write depend on it.

## Assumptions
- This document is an internal engineering design doc, not end-user documentation.
- The initial audience is ClawFS maintainers working on hosted volume support and managed operations.
- The design intentionally preserves the current direct-object-store model for default operation.
- `relay_write` is opt-in for managed organizational volumes, not a required baseline for all ClawFS deployments.
