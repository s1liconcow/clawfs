# ClawFS

## Persistent Volumes for Agents 

> **BETA status**: ClawFS is in BETA and should be used only for non-critical data.
>

> **POSIX compliance**: ClawFS passes the [pjdfstest](https://github.com/pjd/pjdfstest) POSIX filesystem test suite.
>
> **xfstests status**: ClawFS currently passes the xfstests smoke suite we run in CI/sprite (`generic/001,011,023,024,028,029,030,080,084,087,088,095,098`).
>
> **Linux build validation**: Linux source untar plus kernel build (`make defconfig && make -j`) passes successfully on ClawFS.

ClawFS is a persistent shared filesystem for agents, but it also works well for
non-agentic shared-storage workloads such as developer environments, research
pipelines, and general multi-client file access. ClawFS stores metadata and
payloads in a log-structured layout over an object store. Metadata lives in the
bucket under `metadata/imaps` (immutable shard snapshots) and
`metadata/imap_deltas` (per-generation inode deltas), while large payloads are
written to immutable segment objects under `segs/`. Deltas embed a bloom suffix
in their object name so clients can skip unrelated updates with list-only scans.

ClawFS supports access through:
- FUSE mounts on hosts that can expose a normal filesystem mountpoint
- user-mode NFS for networked or remote clients
- a preload shared library for tighter in-process agent integration

The preload/shared-library path is aimed at agent launchers that want normal
file APIs without requiring a host mount. It currently targets Linux-style
`LD_PRELOAD` flows.

For the hosted demo/free journey, the intended product flow is:
- `clawfs login`
- sign in with email, Google, or GitHub
- receive and store a `CLAWFS_API_TOKEN`
- `clawfs summon demo -- <command>`

That auth step exists to tie hosted demo volumes to a real account and reduce
abuse. Purely local usage does not need the hosted auth path.

## Comparison matrix

This matrix is for quick positioning against common alternatives.

| System | Metadata architecture | POSIX / file semantics | Shared multi-writer behavior | Cost + deployment profile |
|-----------|-----------------------|------------------------|------------------------------|---------------------------|
| **ClawFS** | Metadata lives in object storage (`/imaps` + `/imap_deltas`) with no external metadata DB | POSIX-ish, with explicit durability/caching knobs and shared-state reconciliation via generations | Designed as a shared filesystem across clients (generation-based metadata + leases) | Self-managed (local, S3, or GCS), infra cost depends on your object store + compute |
| **JuiceFS** | Requires a separate metadata engine (Redis/MySQL/PostgreSQL/TiKV/etcd/FoundationDB) in addition to object storage [^juicefs-meta] | Strong POSIX target (JuiceFS publishes POSIX compatibility behavior) [^juicefs-posix] | Shared filesystem, but depends on operating and scaling that metadata engine [^juicefs-meta] | Self-managed OSS, but operational footprint includes object store **plus** metadata service |
| **Object-mount OSS adapters (s3fs/goofys/gcsfuse/Mountpoint)** | Typically map object keys directly to files (native object format / object APIs) [^s3fs-native] [^mountpoint-posix] | Trade POSIX completeness for object-store alignment and/or throughput: not fully POSIX or explicitly POSIX-ish [^gcsfuse-posix] [^goofys-posix] [^mountpoint-posix] | Same-file multi-writer semantics are limited: e.g., no client coordination or explicit warnings against concurrent same-object writers [^s3fs-multi] [^gcsfuse-concurrency] | Lightweight OSS clients; good for read-heavy/object-native workflows |
| **Amazon EFS / FSx for Lustre** | Managed AWS file services [^efs-overview] [^fsx-overview] | Strong POSIX/NFS semantics (FSx for Lustre explicitly POSIX-compliant) [^fsx-overview] | Shared multi-client file systems by design | AWS-only managed services with recurring charges across storage, throughput/IOPS, requests, backups, and transfer depending on tier/configuration [^efs-pricing] [^fsx-pricing] |

### Why ClawFS in this landscape

1. Unlike JuiceFS, ClawFS does not require a separate metadata database tier.
2. Unlike most object-mount adapters, ClawFS is built as a shared filesystem first (not just object-key projection).
3. Unlike EFS/FSx, ClawFS is not tied to a single cloud vendor's managed filesystem SKU and pricing model.

[^juicefs-meta]: JuiceFS metadata docs: https://juicefs.com/docs/community/databases_for_metadata/
[^juicefs-posix]: JuiceFS POSIX compatibility: https://juicefs.com/docs/community/posix_compatibility/
[^s3fs-native]: s3fs README ("preserves the native object format for files"): https://github.com/s3fs-fuse/s3fs-fuse
[^s3fs-multi]: s3fs limitations ("no coordination between multiple clients mounting the same bucket"): https://github.com/s3fs-fuse/s3fs-fuse
[^goofys-posix]: goofys README ("performance first and POSIX second", non-POSIX limitations): https://github.com/kahing/goofys
[^mountpoint-posix]: Mountpoint for S3 docs (not full POSIX; limited file operations/locking): https://docs.aws.amazon.com/AmazonS3/latest/userguide/mountpoint.html
[^gcsfuse-posix]: Cloud Storage FUSE overview ("not POSIX compliant"): https://docs.cloud.google.com/storage/docs/cloud-storage-fuse/overview
[^gcsfuse-concurrency]: Cloud Storage FUSE overview (recommendation against multiple sources modifying same object): https://docs.cloud.google.com/storage/docs/cloud-storage-fuse/overview
[^efs-overview]: Amazon EFS overview: https://docs.aws.amazon.com/efs/latest/ug/whatisefs.html
[^efs-pricing]: Amazon EFS pricing: https://aws.amazon.com/efs/pricing/
[^fsx-overview]: FSx for Lustre overview: https://docs.aws.amazon.com/fsx/latest/LustreGuide/what-is.html
[^fsx-pricing]: FSx for Lustre pricing: https://aws.amazon.com/fsx/lustre/pricing/

## High-level architecture

| Component | Responsibility |
|-----------|----------------|
| Superblock (`metadata/superblock.bin`) | Tracks generation counter, next inode/segment ids, shard geometry, filesystem state (CLEAN/DIRTY) and is updated via a compare-and-swap write. Backup formats are versioned via Flexbuffers so upgrades can be detected. |
| Metadata store (`metadata/imaps`, `metadata/imap_deltas`) | Shard snapshots (`i_<gen>_<shard>.bin`) hold current inode state per shard; delta logs (`d_<gen>_<bloom>.bin`) capture per-generation changes. |
| Segment manager (`segs/s_<gen>_<id>` blobs) | Serializes staged file payloads into immutable segment objects and serves extent reads with local cache + range-read support. |
| Local durability layer (`$STORE/journal`, `$STORE/segment_stage`) | Records pending inode state in an append-only WAL and stages large write payload chunks before remote flush. |
| FUSE client (`OsageFs`) | Implements lookup/stat/read/write/rename/link/etc. while translating requests into log-structured object updates. |

### Write path (stage -> flush -> commit)

Normal writes are staged first and committed in batched flushes:

1. `write()` goes through `op_write` and chooses one of three paths:
   inline append (`append_file`), full inline stage (`stage_file`), or sparse
   segment-backed update (`write_large_segments`) for large/partial overwrites.
2. Pending data is stored per inode in memory as either:
   inline bytes, or
   staged segments (`base_extents` + staged chunk overlays).
3. If journaling is enabled, each staged inode is appended to the local WAL
   (`$STORE/journal/wal.bin`).
4. Flush is triggered by explicit `fsync/flush/release` policy, elapsed flush
   interval, or dirty-byte thresholds.
5. `flush_pending` drains pending entries, prepares a dirty generation, writes
   one or more segment objects (batched by size), persists inode shard/delta
   metadata, then atomically commits the new superblock generation via CAS.
6. After commit, staged chunks are released and corresponding journal entries
   are cleared.

### Read path (pending overlay first)

Reads are resolved in this order:

1. If inode data exists in `pending`, serve from staged state.
2. Else if inode is currently in `flushing`, serve from flushing state.
3. Else serve committed storage from metadata:
   inline bytes, or
   segment extents read from immutable segment objects.

For segment-backed files, reads overlay extents in logical order so newer
overwrites win on overlap. When segment compression/encryption is disabled, the
reader uses direct sub-range fetches for lower read amplification.

### Inodes and shards

Each inode record stores ownership, timestamps, file mode, link count, and
either an inline payload (≤1 KiB) or a pointer to a log segment (segment id +
offset + length). Directories carry a `BTreeMap<String, u64>` of child entries.
Records are sharded by a configurable `shard_size`, enabling background
compaction from per-generation delta files (`imap_deltas/d_<generation>_<bloom>.bin`)
into materialized shard blobs (`imaps/i_<generation>_<shard>.bin`).

### ID + file writes & client state

Each client reserves chunks of inode numbers and segment ids (configurable via
`--inode-batch` / `--segment-batch`) to reduce contention on the shared
superblock. When a local pool runs dry, the client atomically advances the
superblock counters and records the new high-water mark in `metadata/superblock.bin`.

* Writes smaller than `--inline-threshold` stay inline inside the inode record.
* Dirty file payloads are serialized into immutable segment objects at flush
  time (path `segs/s_<generation>_<segment_id>`). A flush may emit multiple
  segment objects when the staged batch is large.
* Delta objects inline payloads (≤1 KiB) plus pointers (segment id, offset,
  length) for larger files, keeping metadata compact while still allowing clients
  to reconstruct mutations by replaying deltas. All blobs include a format
  version header, making future migrations safe.
* `fsync()` and close-sync (`--fsync-on-close`) trigger inode-scoped flushes:
  log segments are written first,
  inode-map deltas are applied next, and finally the superblock generation id
  advances once to atomically commit the batch. If staged data exceeds
  `--pending-bytes`, the client flushes eagerly to keep memory bounded.
* On mount, ClawFS ensures a writable home tree (`--home-prefix`, default
  `/home/<user>`), so you always have a place to untar/build even if the bucket
  was previously populated by root-only metadata.
* Each client keeps a local `~/.clawfs/state/client_state.bin` (configurable via
  `--state-path`) that records its `client_id` and remaining pre-allocated inode /
  segment ranges so we never need cross-client coordination for id pools. The
  state file also carries a format version to ease upgrades.

### Directory operations

* **Create/Mkdir**: allocates a fresh inode, persists it, and inserts a directory
  entry under the parent shard.
* **Unlink**: removes the directory entry and either decrements link count or,
  when it reaches zero, tombstones the inode (segments remain immutable log
  entries for recovery).
* **Rename**: moves entries across parents, optionally replacing existing files
  (with `RENAME_NOREPLACE` honored). Directory renames re-base descendant paths
  to keep segment names stable.
* **Link**: adds a hard link by incrementing the file's `link_count` and inserting
  a new directory entry. Directories cannot be linked.
* **Setattr**: updates ownership/permission bits and supports file truncation.

## Usage

```bash
cargo run -- \
  --mount-path /mnt/claw \
  --store-path /var/tmp/clawfs \
  --object-provider local \
  --inline-threshold 1024 \
  --shard-size 2048 \
  --inode-batch 128 \
  --segment-batch 256 \
  --state-path ~/.clawfs/state/client_state.bin \
  --flush-interval-ms 500 \
  --lookup-cache-ttl-ms 5000 \
  --dir-cache-ttl-ms 5000 \
  --metadata-poll-interval-ms 2000 \
  --segment-cache-bytes $((512*1024*1024)) \
  # add --fsync-on-close to force flushes on every release
```

By default the filesystem runs in the background with `AllowRoot` and
`AutoUnmount` enabled. Pass `--foreground` to keep the FUSE session attached to
STDERR for easier debugging. To target a real S3-compatible backend, supply
`--object-provider aws` (or `gcs`) with `--bucket`, plus `--region`/`--endpoint`
for AWS or `--gcs-service-account` for Google Cloud. ClawFS writes everything
into the configured bucket prefix: superblock + metadata JSON live under
`metadata/`, delta logs under `/imap_deltas/`, and immutable segments under
`/segs/`. Keep `--state-path` on local storage so each client maintains its own
`client_id` and id-pool bookkeeping separate from the shared object store.

### Mount Existing Buckets via Source Overlay

ClawFS can mount objects from an existing source bucket while keeping ClawFS
metadata and write path in a separate overlay backend:

* Source files are discovered lazily on `lookup`/`readdir`.
* Reads fetch byte ranges directly from the source object store.
* First write to a source-backed file performs copy-up into ClawFS-managed
  storage; the source object is not modified.

Use source flags to configure an independent source backend (including separate
credentials/provider for multi-cloud layouts):

```bash
cargo run -- \
  --mount-path /mnt/osage \
  --object-provider gcs \
  --bucket my-overlay-bucket \
  --source-object-provider aws \
  --source-bucket commoncrawl \
  --source-prefix crawl-data/CC-MAIN-2025-13/
```

Additional source options:
`--source-store-path` (local provider), `--source-region`,
`--source-endpoint`, `--source-gcs-service-account`,
`--source-aws-access-key-id`, `--source-aws-secret-access-key`,
`--source-aws-allow-http`, `--source-aws-force-path-style`.

Quick demo script:

```bash
OVERLAY_OBJECT_PROVIDER=gcs \
OVERLAY_BUCKET=my-clawfs-overlay \
SOURCE_OBJECT_PROVIDER=aws \
SOURCE_BUCKET=commoncrawl \
SOURCE_PREFIX=crawl-data/CC-MAIN-2025-13/ \
scripts/demo_common_crawl_mount.sh
```

### Superblock checkpoints and restore

Because inode shards, deltas, and segments are immutable objects, you can
checkpoint and restore by saving/restoring only the superblock state.

Create a checkpoint file:

```bash
cargo run --bin clawfs_checkpoint -- create \
  --store-path /tmp/clawfs-store \
  --checkpoint-path /tmp/clawfs-store-checkpoint.bin \
  --note "before migration"
```

Restore a checkpoint:

```bash
cargo run --bin clawfs_checkpoint -- restore \
  --store-path /tmp/clawfs-store \
  --checkpoint-path /tmp/clawfs-store-checkpoint.bin
```

Restore resets `metadata/superblock.bin` to the checkpointed generation and
allocation counters (`next_inode`, `next_segment`).

Offline helper script (recommended):

```bash
scripts/checkpoint.sh create
scripts/checkpoint.sh restore
```

The helper enforces offline safety by default (no mounted filesystem at
`$MOUNT_PATH`, no active `clawfs` process for `$STORE_PATH`) unless you set
`FORCE=1`.

### Performance logging

Pass `--perf-log /path/to/clawfs-perf.jsonl` to emit structured JSONL timing
records. Every staged write produces a `stage_file` entry with the inode id,
payload size, total staged bytes, and whether it triggered an automatic flush.
Each flush generates a `flush_pending` record summarizing how many files were
persisted inline vs. segments, total bytes, individual durations for segment
uploads / metadata persistence / superblock commits, and the next target
generation. The log is append-only, so you can leave it enabled during perf
tests (e.g. `./scripts/run_clawfs.sh`, which now defaults to `$ROOT/clawfs-perf.jsonl`; set `PERF_LOG_PATH=` to disable).

The Linux kernel perf harness (`scripts/linux_kernel_perf.sh`) enables perf
logging by default via `$PERF_LOG_PATH` (set it to an empty string to disable).

For local microbenchmarks and regression tracking, use Criterion:

```bash
CLAWFS_PERF_PROFILE=balanced cargo bench --bench perf_local_criterion
scripts/perf_guard.sh
```

Close-time durability normally relies on the local journal under
`$STORE/journal`. Each staged inode writes a record so a crash can replay pending
data before the mount finishes. For benchmarking you can pass
`--disable-journal` to skip those writes (at the cost of durability).

### Daemon logging

`--log-file` defaults to `clawfs.log` (next to the binary) so every mount
mirrors stderr logs to disk. Pass `--debug-log` when you also want to force the
log level to DEBUG; otherwise the daemon sticks to the `RUST_LOG`/INFO default
even while writing the file.

### Metadata & segment caching

ClawFS keeps NFS-style caches with explicit TTLs:

1. `--lookup-cache-ttl-ms` and `--dir-cache-ttl-ms` govern how long cached attrs
   and directory entries remain valid. Expired entries trigger a shard reload
   before serving FUSE requests.
2. A background poller (`--metadata-poll-interval-ms`) lists `/imap_deltas/`
   and applies any newer generations. Because delta filenames contain bloom
   filters, we can skip unrelated updates by inspecting object names alone.
3. When deltas mention `SegmentPointer`s, the poller asks the `SegmentManager`
   to prefetch them. Segments are staged and cached locally under
   `$STORE_PATH/segment_cache`, bounded by `--segment-cache-bytes`.
4. Large writes stage payload chunks into files under
   `$STORE_PATH/segment_stage/stage_*.bin` during write handling. On flush, the
   active stage file is rotated, staged chunks are serialized into immutable
   segment objects, and old stage files are deleted after references are
   released.
5. Metadata flushes batch up to `--imap-delta-batch` inode records into each
   delta/WAL object and rewrite each shard snapshot at most once per flush. This
   keeps backing-store API calls bounded even when thousands of dentries change
   in a single tar extract or build.

### Background cleanup

Multiple clients share cleanup duties via short-lived leases stored in the
superblock. Each task is coordinated with a compare-and-swap update so that only
one client performs it at a time:

1. **Delta compaction:** when `/imap_deltas` grows beyond `DELTA_COMPACT_THRESHOLD`
   (128 files by default), a client grabs the `DeltaCompaction` lease, prunes
   older deltas (keeping the most recent 32), and releases the lease.
2. **Segment compaction:** opportunistically rewrites older immutable segments
   into a fresh generation so that metadata points to fewer, larger blobs. The
   cleanup worker samples segment pointers from the cached inode shards, writes a
   new consolidated segment, updates the affected inodes, and deletes the old
   remote segments. This keeps read amplification in check and avoids unbounded
   growth in `/segs`.

Both tasks run opportunistically in the background (5 s cadence) and use leases
to avoid duplicate work across clients.

To prevent remote clients from accidentally shipping maintenance data across
regions, pass `--disable-cleanup` when mounting. A separate cleanup agent (see
`docs/CLEANUP_AGENT.md`) can run next to the bucket and take the leases on their
behalf.

### NFS gateway

Run `clawfs-nfs-gateway` when you want to export ClawFS over NFS without
running a FUSE mount. The default `--protocol v3` backend talks to ClawFS
metadata/segments directly and serves them over NFS. It uses the
[`nfsserve`](https://github.com/xetdata/nfsserve) user-mode NFSv3 server so it
works with the Windows built-in client. Example:

```
cargo run --manifest-path clawfs-nfs-gateway/Cargo.toml -- \
  --store-path /tmp/clawfs-store \
  --listen 0.0.0.0:2049
```

Windows clients can mount via `mount -o anon,nolock,vers=3 \\10.0.0.5\\ X:`.
NFSv4 support is part of the Enterprise offering.

### Remote cleanup agents

If you prefer to offload cleanup work to a regional agent (e.g., a WASI module
running on Cloudflare Workers, AWS Lambda, or Cloud Run), see
`docs/CLEANUP_AGENT.md`. That guide explains how to disable cleanup locally and
deploy a tiny agent next to the bucket so cross-region traffic stays minimal.

### Durability knobs

New workloads (especially tar/make) create many tiny files; syncing each close
is overkill. By default ClawFS now batches closes and only forces a flush when:

1. `--pending-bytes` is exceeded (same as before), or
2. `--flush-interval-ms` (default 500 ms) has elapsed since the last flush and a
   new write arrives.

Set `--fsync-on-close` to `true` when you need the previous semantics (every
`release()` flushes). Setting `--flush-interval-ms 0` disables the timer so only
`pending_bytes` and explicit `flush/fsync` calls commit data. Even without a full
flush, large payloads live on disk inside the staging directory, so the next
generation commit only needs to upload the buffered segment.

## Status update

The previous "Future work" items in this README are now implemented:

1. Object-store backends (local, AWS, GCS) are active, and superblock updates
   are compare-and-swap based.
2. Background cleanup includes delta pruning and segment compaction with
   superblock lease coordination.
3. Core FUSE/NFS surfaces for read/write/rename/link/symlink and shared-client
   generation reconciliation are in place.
