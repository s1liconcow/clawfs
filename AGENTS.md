# OsageFS Assistant Notes

## Project Overview
- OsageFS: a FUSE-based log-structured filesystem that stages writes locally, flushes batched immutable segments (under `/segs/s_<generation>_<segment_id>`) to an object store (local FS, AWS S3, or GCS), and stores metadata as immutable inode-map shards (`/imaps/i_<generation>_<shard>.bin`) plus per-generation delta logs (`/imap_deltas/d_<generation>_<bloom>.bin`).
- Close-time durability uses a local journal under `$STORE/journal`: every staged inode (data or metadata) is serialized with its payload path/bytes so a crash can replay the journal and flush pending writes before the mount finishes. You can disable this (unsafe) path via `--disable-journal` when benchmarking raw throughput.
- Inline threshold (`inline_threshold`): payloads <= threshold stay inline in the delta payload; larger payloads use `SegmentPointer` (segment id, offset, length).
- Pending writes are buffered per inode; `flush_pending()` writes a single immutable segment, emits a delta log, rewrites touched shards, and bumps the superblock generation once. Each large write appends to a staging segment file under `$STORE/segment_stage` so data survives until the next flush. When a flush finishes we drop that file and open a fresh staging file for the next batch. Flush triggers: explicit `flush`/`fsync`/`release`, interval timer, or when staged bytes exceed `pending_bytes`.
- Metadata/list caches follow NFS-like TTLs: `--lookup-cache-ttl-ms` for attrs and `--dir-cache-ttl-ms` for directory entries. A background poller (`--metadata-poll-interval-ms`) lists `/imap_deltas/`, applies newer generations, and kicks off segment prefetches for affected inodes. Immutable segments are cached under `$STORE/segment_cache` up to `--segment-cache-bytes`.
- Metadata/list caches follow NFS-like TTLs: `--lookup-cache-ttl-ms` for attrs and `--dir-cache-ttl-ms` for directory entries. A background poller (`--metadata-poll-interval-ms`) lists `/imap_deltas/`, applies newer generations, and kicks off segment prefetches for affected inodes. Immutable segments are cached under `$STORE/segment_cache` up to `--segment-cache-bytes`. Metadata flushes batch up to `--imap-delta-batch` inodes per delta object and rewrite each shard snapshot only once per flush to limit backing-store API calls.
- Cleanup is coordinated via short leases recorded in the superblock. Clients opportunistically take `DeltaCompaction` leases (prune older `/imap_deltas/`) and `SegmentCompaction` leases (merge older segments and update inode pointers) so maintenance work is shared across mounts.
- Use `--disable-cleanup` when a client is far from the bucket; run a regional cleanup agent (see `docs/CLEANUP_AGENT.md`) to pick up the leases without incurring cross-region traffic.
- Each client maintains a local state file (`--state-path`, default `.osagefs_state.bin`) recording `client_id`, inode pool, and segment pool—no shared metadata writes are needed for ID bookkeeping.

## Key Components
- `src/config.rs`: CLI -> Config mapping. Important fields: `mount_path`, `store_path`, `inline_threshold`, `shard_size`, `inode_batch`, `segment_batch`, `pending_bytes`, `object_provider`, `bucket`, `region`, `endpoint`, `object_prefix`, `gcs_service_account`, `state_path`, `perf_log`, `fsync_on_close`, `flush_interval_ms`, `lookup_cache_ttl_ms`, `dir_cache_ttl_ms`, `metadata_poll_interval_ms`, `segment_cache_bytes`, `foreground`.
- Logging: `--log-file` defaults to `osagefs.log` and mirrors every log (now defaulting to DEBUG when a file is used) so tailing that file shows detailed staging/flush info.
- `src/perf.rs`: JSONL perf logger that captures `stage_file` / `flush_pending` events when `--perf-log` is supplied.
- `src/state.rs`: `ClientStateManager` handles per-client pools, persisted via JSON. Tests ensure persistence across runs.
- `src/metadata.rs`: Handles inode caching, writes shard snapshots under `/imaps`, emits delta logs with bloom-filtered filenames, and performs CAS updates on `metadata/superblock.bin`.
- `src/segment.rs`: `SegmentManager::write_batch` serializes `[inode,path,data]` entries into immutable segments; `read_pointer` uses range reads. Tests verify write/read.
- `src/fs.rs`: `OsageFs` FUSE implementation with pending-staging logic, flush thresholds, multi-client tests, etc. Unit tests cover single-client flush, multi-client independence, and pending-byte/interval stress.
- `scripts/linux_kernel_perf.sh`: End-to-end harness that cleans mounts, downloads Linux tarball (if missing), mounts osagefs, and times untar + kernel build. Handles cleanups and dependencies.
- `scripts/cleanup.sh`: unmounts, deletes store dir, and removes the state file to give perf scripts a clean slate.
- `scripts/stress_e2e.sh`: Light-weight FUSE smoke test that launches the daemon via `run_osagefs.sh`, performs single-file, burst, large-file, offset-write, and chmod checks directly against the mounted filesystem, and tears everything down afterward.

## Testing / Tools
- `cargo test`: runs unit tests (including new integration-like tests in `fs.rs`, `segment.rs`, `state.rs`).
- `scripts/linux_kernel_perf.sh`: Perf test; requires `user_allow_other` in `/etc/fuse.conf`, `flex`, `bison`, `curl`, `python3`, etc. Sets `PERF_LOG_PATH` (default `$ROOT/osagefs-perf.jsonl`) so runs automatically emit JSONL timing data.
- `scripts/run_osagefs.sh`: Convenience launcher; defaults `PERF_LOG_PATH=$ROOT/osagefs-perf.jsonl` which can be disabled via `PERF_LOG_PATH=`.
- `osagefs-nfs-gateway/`: stand-alone crate that exports an existing OsageFS mount over NFS (user-mode NFSv3 via `nfsserve`, or NFSv4 by shelling out to `ganesha.nfsd`).

## Common Issues / Fixes
- FUSE error `allow_other`: ensure `user_allow_other` in `/etc/fuse.conf` when running without sudo.
- Root-owned leftovers from sudo runs: unmount and `sudo rm -rf` mount/store dirs before re-running.
- Kernel build dependencies (flex/bison) needed for `make defconfig`.
- Pending bytes threshold + `--flush-interval-ms` ensure `flush_pending()` runs automatically; staged payloads append to `$STORE/segment_stage/stage_*.bin` and only one of those files exists per in-flight flush. Metadata poller + TTLs keep cached attrs fresh, while cleanup leases prevent `/imap_deltas/` and `/segs/` from growing without bound.

## Useful Commands
- Clean mount/store/state: `fusermount -u /tmp/osagefs-mnt; sudo rm -rf /tmp/osagefs-mnt /tmp/osagefs-store ~/.osagefs_state.bin`.
- Run perf test: `LOG_FILE=$HOME/linux_build_timings.log ./scripts/linux_kernel_perf.sh`.
- `scripts/run_osagefs.sh` defaults to `$ROOT/osagefs-perf.jsonl`; set `PERF_LOG_PATH=` to disable tracing when needed.
- Enable perf tracing for other invocations by exporting `PERF_LOG_PATH=/tmp/osagefs-perf.jsonl` (empty string disables logging when scripts default it). Use `--fsync-on-close` to restore immediate durability, `--flush-interval-ms` to adjust opportunistic flush cadence (0 disables timer), and `--disable-cleanup` when cleanup will be handled by an external agent.
- Export OsageFS via NFS: `cargo run --manifest-path osagefs-nfs-gateway/Cargo.toml -- --mount-path /tmp/osagefs-mnt --listen 0.0.0.0:2049`. Add `--protocol v4 --ganesha-binary /usr/bin/ganesha.nfsd` to delegate to ganesha for a full NFSv4 server.

Keep this file updated with future design decisions, scripts, and troubleshooting steps.
