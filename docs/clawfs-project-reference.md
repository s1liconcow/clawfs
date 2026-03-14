# ClawFS Project Reference

## Project Overview
- ClawFS is a FUSE-based log-structured filesystem that stages writes locally, flushes batched immutable segments (under `/segs/s_<generation>_<segment_id>`) to an object store (local FS, AWS S3, or GCS), and stores metadata as immutable inode-map shards (`/imaps/i_<generation>_<shard>.bin`) plus per-generation delta logs (`/imap_deltas/d_<generation>_<bloom>.bin`).
- Close-time durability uses a local journal under `$STORE/journal`; disable with `--disable-journal` when benchmarking.
- Inline threshold (`inline_threshold`): payloads <= threshold stay inline in the delta payload; larger payloads use `SegmentPointer`.
- Payload transforms: `--inline-compression` and `--segment-compression` default to `true`; `--inline-encryption-key` and `--segment-encryption-key` enable ChaCha20Poly1305 after compression when beneficial.
- Pending writes are buffered per inode; `flush_pending()` writes a single immutable segment, emits a delta log, rewrites touched shards, and bumps the superblock generation.
- Flush triggers: explicit `flush`, `fsync`, or `release`, the interval timer, or staged bytes exceeding `pending_bytes`. Flush is async via `trigger_async_flush` with coalescing.
- Large writes append to `$STORE/segment_stage/stage_*.bin`. Flush rotates to a fresh stage file; old stage files are deleted via background tasks.
- Metadata and list caches follow NFS-like TTLs. A background poller applies newer generations and prefetches segments. Metadata flushes batch up to `--imap-delta-batch` inodes per delta object.
- Cleanup uses short leases in the superblock for `DeltaCompaction` and `SegmentCompaction`. Use `--disable-cleanup` when far from the bucket.
- Each client maintains a local state file (`--state-path`) for `client_id`, inode pool, and segment pool.
- `clawfs-nfs-gateway/` is a standalone NFSv3 server. NFSv4 is Enterprise-only.

## Key Components
- `src/config.rs`: CLI to `Config` mapping.
- `src/perf.rs`: JSONL perf logger for `stage_file` and `flush_pending` events.
- `src/replay.rs`: compressed replay logger (`.jsonl.gz`) for FUSE and NFS traces.
- `src/state.rs`: `ClientStateManager` for per-client pools.
- `src/metadata.rs`: inode caching, shard snapshots, delta logs, CAS superblock updates. Use `get_cached_inode()` for safe cache-only lookups.
- `src/checkpoint.rs` and `src/bin/clawfs_checkpoint.rs`: checkpoint and restore utilities for superblock snapshots.
- `src/segment.rs`: `SegmentManager::write_batch` serializes immutable segments and chunks large payloads into 4 MiB entries when compression is enabled. `read_pointer` serves cache hits or issues range reads.
- `src/fs/mod.rs`: FS types and constants.
- `src/fs/core.rs`: inode and path handling.
- `src/fs/write_path.rs`: staging, appends, and segment writes.
- `src/fs/flush.rs`: flush, journal, and replay timing.
- `src/fs/ops.rs`: shared transport-agnostic FS operations. Add new FS behavior here first.
- `src/fs/fuse.rs`: FUSE adapter.
- `src/fs/nfs.rs`: NFS adapter.
- `src/bin/clawfs_replay.rs`: direct API replayer with brute-force mode (`--iterations N`, `--seed`, chaos knobs).
- Main scripts: `linux_kernel_perf.sh`, `fio_workloads.sh`, `cleanup.sh`, `stress_e2e.sh`, `micro_workflows.sh`, `checkpoint.sh`, `run_clawfs.sh`, `run_nfs_gateway.sh`, `sprite_validate_parallel.sh`, `common.sh`.

## Architecture Decisions
- Flush uses per-inode `try_lock` for non-blocking draining; contended inodes are skipped for that cycle.
- Flush scans `pending_inodes` in a `DashSet` rather than all active inodes.
- Pending, mutating, and flushing tables use `DashMap`.
- `load_inode_in_memory()` uses a double-check pattern to prevent ENOENT races across maps.
- `append_file` atomically moves the inode to `mutating_inodes`, works on local state, and re-locks only for commit or rollback.
- Pending buffers use `Arc`-backed storage for cheap clone during rollback or handoff.
- `flush_pending` reloads fresh committed extents from the metadata cache rather than stale `base_extents` to avoid extent loss.
- Segment-extent merge falls back to authoritative `metadata.get_inode()` on cache miss.
- `slice_pending_bytes` serves staged-only ranges via overlapped chunk range reads without fully materializing data.
- `op_read` routes through `read_file_range(...)` for efficient segment-backed reads.
- `fsync` and close-sync flush only the target inode and pending ancestors, not every pending inode.
- Journal semantics: `write()` has no durability promise; `fsync` is the explicit local durability boundary.
- Unlinking the last link keeps inode data with `link_count=0` for open file handles.
- `write_large_segments`: if pending is empty and flushing has data, rebuild from flushing payloads before applying the new write.
- `setattr` stages file content once via `stage_file`; there is no separate `stage_inode`, which avoids stale `Inline([])` corruption.
- Huge truncate returns `EFBIG` via checked, fallible resize.
- `NAME_MAX=255` is enforced with `ENAMETOOLONG` on overlong path components.
- `mknod` parameter order is `(mode, umask, rdev)`; swapping `umask` and `rdev` silently compiles.
- FUSE inode generation is a stable constant `1`, not the superblock generation.
- The `io_uring` experiment was removed; the standard write path remains.
