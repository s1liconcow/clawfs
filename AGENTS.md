# OsageFS Assistant Notes

## Living Document
Please continuously update this document with useful things you figure out that will make future workflows smoother and increase iteration speed.

## Increase Iteration Speed and Token Efficiency
If a new tool (or modifying an existing) can help with your work, propose building it. If you're creating a shell or python command longer than a few lines, consider making a reusable tool instead.

## Post-Merge Hygiene
- After merging a task branch into `master`, automatically clean up temporary git worktrees and local task branches from that effort.
- Standard cleanup sequence: `git worktree list`, `git worktree remove <task-worktree-path>`, `git branch -D <task-branch>`.
- Sandbox note: if a sibling worktree lives outside the current writable root, `git worktree remove` may fail with `Permission denied`; rerun with escalated permissions.

## Project Overview
- OsageFS: a FUSE-based log-structured filesystem that stages writes locally, flushes batched immutable segments (under `/segs/s_<generation>_<segment_id>`) to an object store (local FS, AWS S3, or GCS), and stores metadata as immutable inode-map shards (`/imaps/i_<generation>_<shard>.bin`) plus per-generation delta logs (`/imap_deltas/d_<generation>_<bloom>.bin`).
- Close-time durability uses a local journal under `$STORE/journal`; disable with `--disable-journal` when benchmarking.
- Inline threshold (`inline_threshold`): payloads <= threshold stay inline in the delta payload; larger payloads use `SegmentPointer`.
- Payload transforms: `--inline-compression`/`--segment-compression` default to `true`; `--inline-encryption-key`/`--segment-encryption-key` enable ChaCha20Poly1305 (compression runs first when beneficial).
- Pending writes are buffered per inode; `flush_pending()` writes a single immutable segment, emits a delta log, rewrites touched shards, and bumps the superblock generation. Flush triggers: explicit `flush`/`fsync`/`release`, interval timer, or staged bytes exceeding `pending_bytes`. Flush is async via `trigger_async_flush` with coalescing.
- Staging: large writes append to `$STORE/segment_stage/stage_*.bin`. Flush rotates to a fresh stage file; old stage files are deleted via background tasks.
- Metadata/list caches follow NFS-like TTLs. Background poller applies newer generations and prefetches segments. Metadata flushes batch up to `--imap-delta-batch` inodes per delta object.
- Cleanup via short leases in the superblock: `DeltaCompaction` and `SegmentCompaction`. Use `--disable-cleanup` when far from the bucket.
- Each client maintains a local state file (`--state-path`) for `client_id`, inode pool, and segment pool.
- `osagefs-nfs-gateway/`: stand-alone NFSv3 server (no FUSE mount). NFSv4 is Enterprise-only.

## Key Components
- `src/config.rs`: CLI -> Config mapping.
- `src/perf.rs`: JSONL perf logger for `stage_file`/`flush_pending` events.
- `src/replay.rs`: Compressed replay logger (`.jsonl.gz`) for FUSE/NFS op traces.
- `src/state.rs`: `ClientStateManager` for per-client pools.
- `src/metadata.rs`: Inode caching, shard snapshots, delta logs, CAS superblock updates. `get_cached_inode()` for safe cache-only lookups.
- `src/checkpoint.rs` + `src/bin/osagefs_checkpoint.rs`: checkpoint/restore utility for superblock snapshots.
- `src/segment.rs`: `SegmentManager::write_batch` serializes entries into immutable segments (chunks large payloads into 4 MiB entries when compression is enabled). `read_pointer` serves cache hits or issues range reads.
- `src/fs/`: `mod.rs` (types/constants), `core.rs` (inode/path), `write_path.rs` (staging/appends/segments), `flush.rs` (flush/journal/replay timing), `ops.rs` (shared transport-agnostic ops), `fuse.rs` (FUSE adapter), `nfs.rs` (NFS adapter). Add new FS behavior in `ops.rs` first.
- `src/bin/osagefs_replay.rs`: direct API replayer with brute-force mode (`--iterations N`, `--seed`, chaos knobs).
- Scripts: `linux_kernel_perf.sh`, `fio_workloads.sh`, `cleanup.sh`, `stress_e2e.sh`, `micro_workflows.sh`, `checkpoint.sh`, `run_osagefs.sh`, `run_nfs_gateway.sh`, `sprite_validate_parallel.sh`, `common.sh`.

## Testing / Tools
- Default validation: `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, then `cargo test`.
- `OSAGEFS_RUN_PERF=1 cargo test --test perf_local -- --nocapture`: local performance suite.
- `scripts/fio_workloads.sh` supports `WORKLOADS`, `FAST_REPRO=1`, `HEAPTRACK=1`. Example: `WORKLOADS=smallfiles_sync FAST_REPRO=1 ./scripts/fio_workloads.sh`.
- `scripts/micro_workflows.sh`: `MODE=both BUILD_MODE=check`, `WORKFLOW_PROFILE=quick|realistic|all`. Knobs: `SMALLFILE_COUNT=5000`, `DEV_SCAN_TREE_COPIES=8`, `ETL_ROWS=500000`, etc.
- xfstests (Sprite): use `FUSE_SUBTYP` (not `FUSE_SUBTYPE`), install `/sbin/mount.fuse.osagefs`, set distinct `--fuse-fsname` for TEST/SCRATCH. CLI: options before tests.
- xfstests mount opts: set `TEST_FS_MOUNT_OPTS`/`MOUNT_OPTIONS` with `-o...` (for example `-osource=/tmp/osagefs-test-store,...`); omitting `-o` causes `mount: bad usage` before the helper runs.
- Mount validation: `scripts/common.sh` provides `osage_assert_welcome_file`; tune with `MOUNT_CHECK_TIMEOUT_SEC`.
- `scripts/run_osagefs.sh` auto-rebuilds when source is newer. Defaults `PERF_LOG_PATH=$ROOT/osagefs-perf.jsonl` (set `=` to disable).
- Replay: `REPLAY_LOG_PATH=/path/replay.jsonl.gz` for both FUSE and NFS scripts.
- `scripts/profile_daemon.sh`: Profile the FUSE daemon during a workload. Knobs: `WORKLOAD=untar_compile|untar_only|custom`, `PERF_CALLGRAPH=fp|dwarf`, `DISABLE_PERF=1`, `EXTRA_OSAGEFS_ARGS`. Outputs timings, flush analysis, and optional perf reports to `RESULTS_DIR`. Use `PERF_CALLGRAPH=fp` (default) to avoid huge perf.data files.
- Pre-commit hook: `.githooks/pre-commit` (enable with `git config core.hooksPath .githooks`).
- When CLI flags/config defaults change, update `scripts/` launchers in the same PR.

## Architecture Decisions (current state)
- Flush uses per-inode `try_lock` (non-blocking drain); contended inodes skipped that cycle.
- Flush scans `pending_inodes` DashSet (not all active inodes).
- Pending/mutating/flushing tables use `DashMap`. `load_inode_in_memory()` with double-check pattern prevents ENOENT races across maps.
- `append_file` atomically moves inode to `mutating_inodes`, works on local state, re-locks only for commit/rollback.
- Pending buffers use `Arc`-backed storage for cheap clone on rollback/handoff.
- `flush_pending` retrieves fresh committed extents from metadata cache (not stale `base_extents`) to prevent extent loss.
- Segment-extent merge falls back to authoritative `metadata.get_inode()` on cache miss.
- `slice_pending_bytes` serves staged-only ranges via overlapped chunk range reads (no full materialization).
- `op_read` routes through `read_file_range(...)` for efficient range reads on segment-backed files.
- fsync/close-sync flushes only the target inode + pending ancestors (not all pending inodes).
- Journal: `write()` has no durability promise; `fsync` is the explicit local durability boundary.
- Unlink last link keeps inode data with `link_count=0` for open file handles.
- `write_large_segments`: if pending is empty and flushing has data, rebuild from flushing payloads before applying new write.
- `setattr` stages file content once via `stage_file`; no separate `stage_inode` (prevents stale `Inline([])` corruption).
- Huge truncate returns `EFBIG` (checked/fallible resize).
- `NAME_MAX=255` enforced; `ENAMETOOLONG` on overlong components.
- `mknod` parameter order: `(mode, umask, rdev)` — mixing `umask`/`rdev` silently compiles.
- Stable FUSE inode generation constant (`1`), not superblock generation.
- `io_uring` experiment removed; standard write path retained.

## Common Issues / Fixes
- FUSE `allow_other`: ensure `user_allow_other` in `/etc/fuse.conf`. CLI: `--allow-other`, scripts: `ALLOW_OTHER=1`.
- Root-owned leftovers: unmount and `sudo rm -rf` mount/store dirs before re-running.
- `scripts/stress_e2e.sh` runs cleanup on exit (removes logs); copy logs before if needed.
- `scripts/micro_workflows.sh` preserves caller-provided `PERF_LOG_PATH` on exit; perf logging is opt-in.
- Troubleshooting policy: reproduce with a failing automated test in `src/fs/tests/mod.rs` before fixing.
- Sprite quirks: `fusermount -u` may fail without `/etc/mtab` (fallback: `umount -l`); `cd /tmp` before daemon shutdown to avoid stale-mount errors.
- Linux kernel build deps in sprite: `flex`, `bison`, `libelf-dev`, `dwarves`.
- `scripts/linux_kernel_perf.sh` fails fast on stale/inaccessible mount or existing daemon.
- `scripts/checkpoint.sh` process guard can false-positive match; run checkpoint steps in separate shells.
- Checkpoints snapshot only the superblock pointer, not data objects.
- EIO during builds: verify daemon liveness and mount health before chasing data-path logic.

## Useful Commands
- Clean: `fusermount -u /tmp/osagefs-mnt; sudo rm -rf /tmp/osagefs-mnt /tmp/osagefs-store ~/.osagefs_state.bin`
- Perf test: `LOG_FILE=$HOME/linux_build_timings.log ./scripts/linux_kernel_perf.sh`
- Fio sweep: `RESULTS_DIR=/work/osagefs/fio-results LOG_FILE=/work/osagefs/osagefs.log PERF_LOG_PATH=/work/osagefs/osagefs-perf.jsonl ./scripts/fio_workloads.sh`
- Analyze perf: `./scripts/analyze_perf_log.py --log perf-log.jsonl --event flush_pending --top 10`
- Replay capture: `REPLAY_LOG_PATH=/work/osagefs/replay.jsonl.gz ./scripts/run_osagefs.sh`
- Direct replay: `cargo run --release --bin osagefs_replay -- --trace-path replay.jsonl.gz --store-path /tmp/osagefs-replay-store --local-cache-path /tmp/osagefs-replay-cache --state-path /tmp/osagefs-replay-state.bin --layer fuse --speed 1.0`
- Checkpoint create/restore: `cargo run --bin osagefs_checkpoint -- create/restore --store-path ... --checkpoint-path ...`
- NFS export: `cargo run --manifest-path osagefs-nfs-gateway/Cargo.toml -- --store-path /tmp/osagefs-store --listen 0.0.0.0:2049`

# Using Sprites

## Goal
Privileged execution must happen inside a Fly.io Sprite VM, NOT locally.

## Requirements
- `sprite` CLI on PATH, `SPRITES_TOKEN` set.

## Sprite bootstrap (Debian/Ubuntu)
```
sprite exec -s "$SPRITE_NAME" -- bash -lc 'sudo apt-get update && sudo apt-get install -y python3 fio fuse3 util-linux coreutils findutils procps psmisc tar gzip ripgrep strace'
```

## Hard rules
- No privileged commands locally. No local FUSE/NFS mounts.
- All privileged commands via `sprite exec -s <sprite> -- bash -lc '<command>'`.
- Prefer idempotent steps.
- Run `cargo fmt --all --check`, `cargo clippy ...`, `cargo test` before handing off.

## Sprite naming (<=55 chars)
```
REPO_SLUG="$(basename "$(git rev-parse --show-toplevel)" | tr -cs 'a-zA-Z0-9' '-' | tr '[:upper:]' '[:lower:]' | sed 's/^-*//; s/-*$//')"
BRANCH_SLUG="$(git rev-parse --abbrev-ref HEAD | tr -cs 'a-zA-Z0-9' '-' | tr '[:upper:]' '[:lower:]' | sed 's/^-*//; s/-*$//')"
RAW_BASE="${REPO_SLUG}-${BRANCH_SLUG}"
BASE_HASH="$(printf '%s' "$RAW_BASE" | sha1sum | cut -c1-8)"
SPRITE_BASENAME="$(printf '%s' "$RAW_BASE" | cut -c1-46 | sed 's/-$//')-${BASE_HASH}"
```
For multi-sprite (role suffix): append `-${ROLE_SLUG}-${NAME_HASH}` (roles: `build`, `stress`, `perf-a`, `cleanup`).

After task completion, destroy sprites: `sprite destroy --force "$SPRITE_NAME"`.

## Getting code into the sprite
```
tar -czf - . | sprite exec -s "$SPRITE_NAME" -- bash -lc 'rm -rf /work/osagefs && mkdir -p /work/osagefs && tar -xzf - -C /work/osagefs'
```

## Build + test
- Smoke: `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cd /work/osagefs && cargo build --release && ./target/release/osagefs --help >/dev/null'`
- E2E: `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cd /work/osagefs && ./scripts/stress_e2e.sh'`
- Fio: `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cd /work/osagefs && RESULTS_DIR=/work/osagefs/fio-results/compact LOG_FILE=/work/osagefs/osagefs.log PERF_LOG_PATH=/work/osagefs/osagefs-perf.jsonl RUNTIME_SEC=5 SEQ_SIZE=64M RAND_SIZE=64M RAND_NUMJOBS=2 RAND_IODEPTH=8 SMALLFILE_COUNT=200 SMALLFILE_NUMJOBS=4 SMALLFILE_SIZE=8k ./scripts/fio_workloads.sh'`

## Troubleshooting in sprites
Collect on failure:
```
sprite exec -s "$SPRITE_NAME" -- bash -lc 'tail -n 200 /work/osagefs/osagefs.log || true'
sprite exec -s "$SPRITE_NAME" -- bash -lc 'ps -ef | rg osagefs || true; mount | rg /tmp/osagefs-mnt || true'
sprite exec -s "$SPRITE_NAME" -- bash -lc 'dmesg | tail -200 || true'
```

Fix iteration order: reproduce in sprite -> add regression test in `src/fs/tests/mod.rs` -> fix -> rerun.

## Sprite Checkpoints
```
scripts/sprite_perf_loop.sh --sprite $SPRITE_NAME --mode cold --create-checkpoint --comment "linux tree prepared"
scripts/sprite_perf_loop.sh --sprite $SPRITE_NAME --restore v2 --mode fast
scripts/sprite_perf_loop.sh --ephemeral --mode fast  # auto-clean; add --keep-sprite to preserve
```

Keep this file updated with future design decisions, scripts, and troubleshooting steps.
