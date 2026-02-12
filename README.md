# OsageFS

OsageFS is a POSIX-ish shared filesystem that speaks FUSE and stores both
metadata and file payloads in a log-structured layout over an object store. The
current prototype uses SlateDB to persist metadata locally while mimicking an
S3-like bucket for blobs and segments.

## High-level architecture

| Component | Responsibility |
|-----------|----------------|
| Superblock (SlateDB key `superblock`) | Tracks generation counter, next inode/segment ids, shard geometry, filesystem state (CLEAN/DIRTY). |
| Metadata store (SlateDB) | Persists inode records, shard snapshots (`imap_shard:*`), delta logs (`imap_delta:*`), and inode tombstones. |
| Segment manager (`segs/s_<gen>_<id>` blobs) | Flushes >1 KiB payloads into immutable log segments so the filesystem can be reconstructed from the log alone. |
| FUSE client (`OsageFs`) | Implements lookup/stat/read/write/rename/link/etc. while translating requests into log-structured object updates. |

### Superblock + transactions

All metadata mutations take a "generation" snapshot:

1. Stage metadata and file-data mutations locally (inline or in-memory).
2. On `close()` / `fsync()`, flush dirty files into a single `/segs/s_<gen>_<id>`
   blob and persist inode-map deltas in SlateDB.
3. Update the superblock generation id **once** after the segment + metadata
   writes succeed, mirroring the atomic swap semantics of
   `x-goog-if-generation-match`.

### Inodes and shards

Each inode record stores ownership, timestamps, file mode, link count, and
either an inline payload (≤1 KiB) or a pointer to a log segment (segment id +
offset + length). Directories carry a `BTreeMap<String, u64>` of child entries.
Records are sharded by a configurable `shard_size`, enabling background
compaction from per-generation delta files (`imap_delta:<shard>:<generation>:
<inode>`) into materialized shard blobs (`imap_shard:<id>`).

### ID + file writes & client state

Each client reserves chunks of inode numbers and segment ids (configurable via
`--inode-batch` / `--segment-batch`) to reduce contention on the shared
superblock. When a local pool runs dry, the client atomically advances the
superblock counters and records the new high-water mark in SlateDB.

* Writes smaller than `--inline-threshold` stay inline inside the inode record.
* All dirty files are combined into a single immutable segment at flush time
  (path `/segs/s_<generation>_<segment_id>`). Each entry stores the inode, path,
  and contents so replaying the log reconstructs the tree.
* SlateDB only stores inline payloads (≤1 KiB) plus pointers (segment id,
  offset, length) for larger files, keeping metadata compact.
* `close()` / `fsync()` triggers a flush: log segments are written first,
  inode-map deltas are applied next, and finally the superblock generation id
  advances once to atomically commit the batch. If staged data exceeds
  `--pending-bytes`, the client flushes eagerly to keep memory bounded.
* Each client keeps a local `.osagefs_state.json` (configurable via
  `--state-path`) that records its `client_id` and remaining pre-allocated inode
  / segment ranges. This prevents per-client bookkeeping from touching the
  shared SlateDB bucket even when mounting against S3/GCS.

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
  --state-path ~/.osagefs_state.json
```

By default the filesystem runs in the background with `AllowRoot` and
`AutoUnmount` enabled. Pass `--foreground` to keep the FUSE session attached to
STDERR for easier debugging. To target a real S3-compatible backend, supply
`--object-provider aws` (or `gcs`) with `--bucket`, plus `--region`/`--endpoint`
for AWS or `--gcs-service-account` for Google Cloud. OsageFS still uses SlateDB
locally for metadata but now PUTs/GETs segment blobs under `/segs/...` in the
configured bucket. Keep `--state-path` on local storage so each client maintains
its own `client_id` and id-pool bookkeeping separate from the shared object
store.

## Future work

1. Replace the local segment directory with a proper S3/GCS backend that uses
   conditional headers for superblock swaps.
2. Add background compaction for inode delta logs plus a metadata journal that
   batches multi-op transactions.
3. Extend the FUSE surface (e.g., symlink support, extended attributes) and add
   multi-client reconciliation on mount.
