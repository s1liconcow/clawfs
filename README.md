# OsageFS

OsageFS is a POSIX-ish shared filesystem that speaks FUSE and stores metadata
and payloads in a log-structured layout over an object store. Metadata now lives
directly inside the bucket as immutable inode-map shards (`/imaps/i_<gen>_<shard>`)
and per-generation delta files (`/imap_deltas/d_<gen>_<bloom>.bin`). Each delta
filename embeds a hex bloom-filter of the inodes it contains so clients can skip
irrelevant updates just by listing objects.

## High-level architecture

| Component | Responsibility |
|-----------|----------------|
| Superblock (`metadata/superblock.bin`) | Tracks generation counter, next inode/segment ids, shard geometry, filesystem state (CLEAN/DIRTY) and is updated via a compare-and-swap write. Backup formats are versioned via Flexbuffers so upgrades can be detected. |
| Metadata store (`/imaps`, `/imap_deltas`) | Shard snapshots (`imaps/i_<gen>_<shard>.bin`) hold the latest inode map per shard, while delta logs (`imap_deltas/d_<gen>_<bloom>.bin`) capture per-generation changes with inline payloads for ≤1 KiB files. Filenames embed a bloom filter so clients can skip unrelated deltas via `list()` alone. |
| Segment manager (`segs/s_<gen>_<id>` blobs) | Flushes >1 KiB payloads into immutable log segments so the filesystem can be reconstructed from the log alone. |
| FUSE client (`OsageFs`) | Implements lookup/stat/read/write/rename/link/etc. while translating requests into log-structured object updates. |

### Superblock + transactions

All metadata mutations take a "generation" snapshot:

1. Stage metadata and file-data mutations locally (inline or in-memory).
2. On `close()` / `fsync()`, flush dirty files into a single `/segs/s_<gen>_<id>`
   blob and emit a delta object `/imap_deltas/d_<gen>_<bloom>.bin` containing all
   touched inode records (with inline payloads if ≤1 KiB). Updated shard
   snapshots for affected shards are written under `/imaps/i_<gen>_<shard>.bin`.
3. Update the superblock generation id **once** after the segment + metadata
   writes succeed using a compare-and-swap write so the commit is atomic even on
   S3/GCS.

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
* All dirty files are combined into a single immutable segment at flush time
  (path `/segs/s_<generation>_<segment_id>`). Each entry stores the inode, path,
  and contents so replaying the log reconstructs the tree.
* Delta objects inline payloads (≤1 KiB) plus pointers (segment id, offset,
  length) for larger files, keeping metadata compact while still allowing clients
  to reconstruct mutations by replaying deltas. All blobs include a format
  version header, making future migrations safe.
* `close()` / `fsync()` triggers a flush: log segments are written first,
  inode-map deltas are applied next, and finally the superblock generation id
  advances once to atomically commit the batch. If staged data exceeds
  `--pending-bytes`, the client flushes eagerly to keep memory bounded.
* On mount, OsageFS ensures a writable home tree (`--home-prefix`, default
  `/home/<user>`), so you always have a place to untar/build even if the bucket
  was previously populated by root-only metadata.
* Each client keeps a local `.osagefs_state.bin` (configurable via
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
  --mount-path /mnt/osage \
  --store-path /var/tmp/osagefs \
  --object-provider local \
  --inline-threshold 1024 \
  --shard-size 2048 \
  --inode-batch 128 \
  --segment-batch 256 \
  --state-path ~/.osagefs_state.bin \
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
for AWS or `--gcs-service-account` for Google Cloud. OsageFS writes everything
into the configured bucket prefix: superblock + metadata JSON live under
`metadata/`, delta logs under `/imap_deltas/`, and immutable segments under
`/segs/`. Keep `--state-path` on local storage so each client maintains its own
`client_id` and id-pool bookkeeping separate from the shared object store.

### Performance logging

Pass `--perf-log /path/to/osagefs-perf.jsonl` to emit structured JSONL timing
records. Every staged write produces a `stage_file` entry with the inode id,
payload size, total staged bytes, and whether it triggered an automatic flush.
Each flush generates a `flush_pending` record summarizing how many files were
persisted inline vs. segments, total bytes, individual durations for segment
uploads / metadata persistence / superblock commits, and the next target
generation. The log is append-only, so you can leave it enabled during perf
tests (e.g. `./scripts/run_osagefs.sh`, which now defaults to `$ROOT/osagefs-perf.jsonl`; set `PERF_LOG_PATH=` to disable).

The Linux kernel perf harness (`scripts/linux_kernel_perf.sh`) enables perf
logging by default via `$PERF_LOG_PATH` (set it to an empty string to disable).

Close-time durability normally relies on the local journal under
`$STORE/journal`. Each staged inode writes a record so a crash can replay pending
data before the mount finishes. For benchmarking you can pass
`--disable-journal` to skip those writes (at the cost of durability).

### Daemon logging

`--log-file` defaults to `osagefs.log` (next to the binary) so every mount
mirrors stderr logs to disk. Pass `--debug-log` when you also want to force the
log level to DEBUG; otherwise the daemon sticks to the `RUST_LOG`/INFO default
even while writing the file.

### Metadata & segment caching

OsageFS keeps NFS-style caches with explicit TTLs:

1. `--lookup-cache-ttl-ms` and `--dir-cache-ttl-ms` govern how long cached attrs
   and directory entries remain valid. Expired entries trigger a shard reload
   before serving FUSE requests.
2. A background poller (`--metadata-poll-interval-ms`) lists `/imap_deltas/`
   and applies any newer generations. Because delta filenames contain bloom
   filters, we can skip unrelated updates by inspecting object names alone.
3. When deltas mention `SegmentPointer`s, the poller asks the `SegmentManager`
   to prefetch them. Segments are staged and cached locally under
   `$STORE_PATH/segment_cache`, bounded by `--segment-cache-bytes`.
4. Large writes append to a staging segment file under
   `$STORE_PATH/segment_stage/stage_*.bin` as soon as `close()` runs so data
   survives crashes even before the next flush. When a flush succeeds the staged
   file is uploaded as an immutable segment and a fresh staging file is opened
   for the next batch. Uploading to S3/GCS only happens when `flush()`/`fsync()`
   runs (or the flush timer fires), which keeps close() latency low.
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

Run `osagefs-nfs-gateway` when you want to export an existing OsageFS mount over
NFS without installing a Windows FUSE driver. By default it uses the
[`nfsserve`](https://github.com/xetdata/nfsserve) user-mode NFSv3 server so it
works with the Windows built-in client. Example:

```
cargo run --manifest-path osagefs-nfs-gateway/Cargo.toml -- \
  --mount-path /tmp/osagefs-mnt \
  --listen 0.0.0.0:2049
```

If you need a compliant NFSv4 endpoint, add `--protocol v4` and point the
gateway at a `ganesha.nfsd` binary. The gateway generates a temporary export
definition rooted at the supplied OsageFS mount and manages the ganesha process
lifecycle so that Windows or Linux clients can mount it like any other v4
export.

```
cargo run --manifest-path osagefs-nfs-gateway/Cargo.toml -- \
  --mount-path /tmp/osagefs-mnt \
  --protocol v4 \
  --ganesha-binary /usr/bin/ganesha.nfsd \
  --listen 10.0.0.5:2049
```

Windows clients can then mount via `mount -o anon,nolock,vers=3 \\10.0.0.5\\ X:`
for the user-mode server, or change `vers=4` when running with ganesha.

### Remote cleanup agents

If you prefer to offload cleanup work to a regional agent (e.g., a WASI module
running on Cloudflare Workers, AWS Lambda, or Cloud Run), see
`docs/CLEANUP_AGENT.md`. That guide explains how to disable cleanup locally and
deploy a tiny agent next to the bucket so cross-region traffic stays minimal.

### Durability knobs

New workloads (especially tar/make) create many tiny files; syncing each close
is overkill. By default OsageFS now batches closes and only forces a flush when:

1. `--pending-bytes` is exceeded (same as before), or
2. `--flush-interval-ms` (default 500 ms) has elapsed since the last flush and a
   new write arrives.

Set `--fsync-on-close` to `true` when you need the previous semantics (every
`release()` flushes). Setting `--flush-interval-ms 0` disables the timer so only
`pending_bytes` and explicit `flush/fsync` calls commit data. Even without a full
flush, large payloads live on disk inside the staging directory, so the next
generation commit only needs to upload the buffered segment.

## Future work

1. Replace the local segment directory with a proper S3/GCS backend that uses
   conditional headers for superblock swaps.
2. Add background compaction for inode delta logs plus a metadata journal that
   batches multi-op transactions.
3. Extend the FUSE surface (e.g., symlink support, extended attributes) and add
   multi-client reconciliation on mount.
