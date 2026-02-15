# OsageFS Assistant Notes

## Living Document
Please continuously update this document with useful things you figure out that will make future workflows smoother and increase iteration speed.  

## Post-Merge Hygiene
- After merging a task branch into `master`, automatically clean up temporary git worktrees and local task branches from that effort.
- Standard cleanup sequence:
  - `git worktree list`
  - `git worktree remove <task-worktree-path>`
  - `git branch -D <task-branch>`
- If multiple task branches/worktrees were used for the same change, remove all of them once the merge is complete and verified.

## Project Overview
- OsageFS: a FUSE-based log-structured filesystem that stages writes locally, flushes batched immutable segments (under `/segs/s_<generation>_<segment_id>`) to an object store (local FS, AWS S3, or GCS), and stores metadata as immutable inode-map shards (`/imaps/i_<generation>_<shard>.bin`) plus per-generation delta logs (`/imap_deltas/d_<generation>_<bloom>.bin`).
- Close-time durability uses a local journal under `$STORE/journal`: every staged inode (data or metadata) is serialized with its payload path/bytes so a crash can replay the journal and flush pending writes before the mount finishes. You can disable this (unsafe) path via `--disable-journal` when benchmarking raw throughput.
- Inline threshold (`inline_threshold`): payloads <= threshold stay inline in the delta payload; larger payloads use `SegmentPointer` (segment id, offset, length).
- Pending writes are buffered per inode; `flush_pending()` writes a single immutable segment, emits a delta log, rewrites touched shards, and bumps the superblock generation once. Each large write appends to a staging segment file under `$STORE/segment_stage` so data survives until the next flush. When a flush finishes we drop that file and open a fresh staging file for the next batch. Flush triggers: explicit `flush`/`fsync`/`release`, interval timer, or when staged bytes exceed `pending_bytes`.
- Adaptive large-write flush policy: when the journal is enabled, append-heavy large writes can defer interval-driven flushes and use a higher pending-byte watermark (capped) to reduce write amplification; deferral is still bounded (currently 5s max) so metadata visibility remains bounded while preserving crash replay durability.
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
- `src/segment.rs`: `SegmentManager::write_batch` accepts in-memory bytes or staged chunks; flushing can stream staged payloads directly into immutable segments to reduce extra copy overhead in `flush_pending`.
- `src/fs.rs`: `OsageFs` FUSE implementation with pending-staging logic, flush thresholds, multi-client tests, etc. Unit tests cover single-client flush, multi-client independence, and pending-byte/interval stress.
- `scripts/linux_kernel_perf.sh`: End-to-end harness that cleans mounts, downloads Linux tarball (if missing), mounts osagefs, and times untar + kernel build. Handles cleanups and dependencies.
- `scripts/fio_workloads.sh`: Starts OsageFS, runs mixed fio profiles (sequential, random, mixed, small-file sync), and writes per-workload JSON plus `summary.md` under `fio-results-*`.
- `scripts/cleanup.sh`: unmounts, deletes store dir, and removes the state file to give perf scripts a clean slate.
- `scripts/stress_e2e.sh`: Light-weight FUSE smoke test that launches the daemon via `run_osagefs.sh`, performs single-file, burst, large-file, offset-write, and chmod checks directly against the mounted filesystem, and tears everything down afterward.

## Testing / Tools
- `cargo test`: runs unit tests (including new integration-like tests in `fs.rs`, `segment.rs`, `state.rs`).
- `OSAGEFS_RUN_PERF=1 cargo test --test perf_local -- --nocapture`: local performance suite includes stage throughput, batched segment throughput, and small-file IOPS checks.
- `scripts/linux_kernel_perf.sh`: Perf test; requires `user_allow_other` in `/etc/fuse.conf`, `flex`, `bison`, `curl`, `python3`, etc. Sets `PERF_LOG_PATH` (default `$ROOT/osagefs-perf.jsonl`) so runs automatically emit JSONL timing data.
- `scripts/fio_workloads.sh`: fio benchmark harness; requires `fio`, `python3`, and FUSE support in the runtime environment (prefer sprite execution for mount tests).
- `scripts/run_osagefs.sh`: Convenience launcher; defaults `PERF_LOG_PATH=$ROOT/osagefs-perf.jsonl` which can be disabled via `PERF_LOG_PATH=`.
- `osagefs-nfs-gateway/`: stand-alone crate that serves OsageFS directly over NFSv3 (`nfsserve`, no FUSE mount), or exports an existing path for NFSv4 via `ganesha.nfsd` with `--use-existing-mount`.
- When CLI flags/config defaults change, double-check and update `scripts/` launchers in the same PR so script argument sets stay in sync with binaries.

## Common Issues / Fixes
- FUSE error `allow_other`: ensure `user_allow_other` in `/etc/fuse.conf` when running without sudo.
- Root-owned leftovers from sudo runs: unmount and `sudo rm -rf` mount/store dirs before re-running.
- Kernel build dependencies (flex/bison) needed for `make defconfig`.
- `scripts/fio_workloads.sh` generates timestamped `fio-results-*` directories and does not call `scripts/cleanup.sh` at exit, so logs/perf traces are preserved by default for analysis.
- Pending bytes threshold + `--flush-interval-ms` ensure `flush_pending()` runs automatically; staged payloads append to `$STORE/segment_stage/stage_*.bin` and only one of those files exists per in-flight flush. Metadata poller + TTLs keep cached attrs fresh, while cleanup leases prevent `/imap_deltas/` and `/segs/` from growing without bound.
- Troubleshooting policy: reproduce with a failing automated test before fixing. For NFS/data-path issues, first capture logs (`write error`, `load_inode miss`, etc.), then codify the failure as a focused regression test in `src/fs.rs`; only after the test fails should code changes be made. Keep the test to prevent regressions.
- `scripts/stress_e2e.sh` always runs `scripts/cleanup.sh` on exit, which removes both `LOG_FILE` and `PERF_LOG_PATH`; when you need post-run perf analysis, run an equivalent custom workload (or copy logs elsewhere) before cleanup.
- In Sprite VMs, long `apt-get install` runs can sometimes end with `Error: connection closed` / non-zero transport exit even after package `Setting up ...` lines complete; verify required tools explicitly (`fusermount3`, `rg`, `strace`) before retrying full bootstrap.

## Useful Commands
- Clean mount/store/state: `fusermount -u /tmp/osagefs-mnt; sudo rm -rf /tmp/osagefs-mnt /tmp/osagefs-store ~/.osagefs_state.bin`.
- Run perf test: `LOG_FILE=$HOME/linux_build_timings.log ./scripts/linux_kernel_perf.sh`.
- Run fio workload sweep: `RESULTS_DIR=/work/osagefs/fio-results LOG_FILE=/work/osagefs/osagefs.log PERF_LOG_PATH=/work/osagefs/osagefs-perf.jsonl ./scripts/fio_workloads.sh`.
- `scripts/run_osagefs.sh` defaults to `$ROOT/osagefs-perf.jsonl`; set `PERF_LOG_PATH=` to disable tracing when needed.
- Enable perf tracing for other invocations by exporting `PERF_LOG_PATH=/tmp/osagefs-perf.jsonl` (empty string disables logging when scripts default it). Use `--fsync-on-close` to restore immediate durability, `--flush-interval-ms` to adjust opportunistic flush cadence (0 disables timer), and `--disable-cleanup` when cleanup will be handled by an external agent.
- Export OsageFS via NFS: `cargo run --manifest-path osagefs-nfs-gateway/Cargo.toml -- --store-path /tmp/osagefs-store --listen 0.0.0.0:2049`. Add `--protocol v4 --use-existing-mount --mount-path /tmp/osagefs-mnt --ganesha-binary /usr/bin/ganesha.nfsd` for a full NFSv4 server.

Keep this file updated with future design decisions, scripts, and troubleshooting steps.

# Using Sprites

## Goal
Privileged execution must happen inside a Fly.io Sprite VM (Linux sandbox),
NOT on the developer machine. Use the `sprite` CLI to run commands remotely.

## Requirements
- `sprite` CLI installed and available on PATH
- `SPRITES_TOKEN` set in the environment
- A sprite name to use (default: `iotest`)

## Sprite bootstrap dependencies
On a fresh sprite, install the tools used by build/stress/debug flows before running tests:
- Rust toolchain (`cargo`, `rustc`)
- `python3`
- `fio`
- `fuse3` userspace tools (`fusermount`)
- `util-linux` (`mount`, `umount`, `findmnt`, `mountpoint`)
- `coreutils` (`dd`, `truncate`, `stat`, `tail`, `sync`, `timeout`)
- `findutils` (`find`)
- `procps` + `psmisc` (`ps`, `pkill`)
- `tar`, `gzip`
- `ripgrep` (`rg`) for diagnostics
- `strace` for syscall-level failure triage

Example bootstrap (Debian/Ubuntu sprite):
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'sudo apt-get update && sudo apt-get install -y python3 fio fuse3 util-linux coreutils findutils procps psmisc tar gzip ripgrep strace'`

## Hard rules for agents
- Do not run privileged commands locally.
- Do not mount FUSE or run NFS services locally.
- Privileged commands MUST be executed in the sprite via `sprite exec -s <sprite> -- <cmd...>`.
- Prefer idempotent steps: the sprite may be fresh or may contain old state.

## Sprite name
Use deterministic sprite names per worktree/task so repeated runs reuse VM state.
One task may use multiple sprites (for example: build, stress, perf, cleanup-agent), as long as each sprite name is deterministic.
Sprite names must be `<=55` characters.

Recommended naming rule (55-char safe):
- `REPO_SLUG="$(basename "$(git rev-parse --show-toplevel)" | tr -cs 'a-zA-Z0-9' '-' | tr '[:upper:]' '[:lower:]' | sed 's/^-*//; s/-*$//')"`
- `BRANCH_SLUG="$(git rev-parse --abbrev-ref HEAD | tr -cs 'a-zA-Z0-9' '-' | tr '[:upper:]' '[:lower:]' | sed 's/^-*//; s/-*$//')"`
- `RAW_BASE="iotest-${REPO_SLUG}-${BRANCH_SLUG}"`
- `BASE_HASH="$(printf '%s' "$RAW_BASE" | sha1sum | cut -c1-8)"`
- `SPRITE_BASENAME="$(printf '%s' "$RAW_BASE" | cut -c1-46 | sed 's/-$//')-${BASE_HASH}"`  (always `<=55`)

Multi-sprite naming rule (recommended, 55-char safe):
- `ROLE_SLUG="$(printf '%s' "$ROLE" | tr -cs 'a-zA-Z0-9' '-' | tr '[:upper:]' '[:lower:]' | sed 's/^-*//; s/-*$//' | cut -c1-12)"`
- `RAW_NAME="${RAW_BASE}-${ROLE_SLUG}"`
- `NAME_HASH="$(printf '%s' "$RAW_NAME" | sha1sum | cut -c1-8)"`
- `MAX_BASE_LEN=$((55 - 1 - ${#ROLE_SLUG} - 1 - 8))`
- `BASE_TRIMMED="$(printf '%s' "$RAW_BASE" | cut -c1-"$MAX_BASE_LEN" | sed 's/-$//')"`
- `SPRITE_NAME="${BASE_TRIMMED}-${ROLE_SLUG}-${NAME_HASH}"` where `ROLE` is stable (`build`, `stress`, `perf-a`, `perf-b`, `cleanup`).

Operational policy:
- For a new worktree or task branch, derive a stable base name and reuse it.
- You may allocate multiple sprites for the same task, but each must use a stable role suffix and deterministic name.
- Keep `iotest` as a shared fallback only when you explicitly need a common long-lived sprite.
- After task completion, destroy all task sprites (`sprite destroy --force "$SPRITE_NAME"`), unless you intentionally keep specific ones for checkpoints.

If the deterministic sprite does not exist, create it:
- `sprite create "$SPRITE_NAME"`

(If create fails because it already exists, continue.)

## How to run commands
Always wrap commands like this:

- `sprite exec -s "$SPRITE_NAME" -- bash -lc '<command>'`

Use `bash -lc` so PATH, login-shell behavior, and chaining works.

## Getting code into the sprite
Assume the sprite does NOT automatically have the repo.
Use one of these strategies, in order:

### Strategy: Tar stream from local into sprite (works for local-only repos)
Create a tarball locally and extract in the sprite:

- `tar -czf - . | sprite exec -s "$SPRITE_NAME" -- bash -lc 'rm -rf /work/osagefs && mkdir -p /work/osagefs && tar -xzf - -C /work/osagefs'`

Do not include giant build artifacts if possible (respect .gitignore).

## Build + test entrypoints
### Minimal “smoke” (no mounts)
Use this first to validate toolchains:
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cd /work/osagefs && cargo build --release && ./target/release/osagefs --help >/dev/null'`


### Integration tests that require mounting (FUSE/NFS)
If integration tests need mounts/services, they must run inside the sprite.
Use the repo script that already handles launch, workload, verification, and cleanup:

- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cd /work/osagefs && ./scripts/stress_e2e.sh'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cd /work/osagefs && RESULTS_DIR=/work/osagefs/fio-results/compact LOG_FILE=/work/osagefs/osagefs.log PERF_LOG_PATH=/work/osagefs/osagefs-perf.jsonl RUNTIME_SEC=5 SEQ_SIZE=64M RAND_SIZE=64M RAND_NUMJOBS=2 RAND_IODEPTH=8 SMALLFILE_COUNT=200 SMALLFILE_NUMJOBS=4 SMALLFILE_SIZE=8k ./scripts/fio_workloads.sh'`

Useful knobs for repeated runs:
- `LOG_FILE=/work/osagefs/osagefs.log`
- `PERF_LOG_PATH=/work/osagefs/osagefs-perf.jsonl`
- `PID_FILE=/tmp/osagefs-stress.pid`
- `MOUNT_PATH=/tmp/osagefs-mnt`
- `STORE_PATH=/tmp/osagefs-store`
- `LOCAL_CACHE_PATH=/tmp/osagefs-cache`
- `STATE_PATH=$HOME/.osagefs/state/client_state.bin`

Example with explicit paths:
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cd /work/osagefs && LOG_FILE=/work/osagefs/osagefs.log PERF_LOG_PATH=/work/osagefs/osagefs-perf.jsonl ./scripts/stress_e2e.sh'`

## Sprite FUSE Troubleshooting
If stress fails with `Input/output error` (for example during `cp` in the attribute test), collect these immediately:
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'tail -n 200 /work/osagefs/osagefs.log || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'tail -n 200 /work/osagefs/osagefs-perf.jsonl || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cat /tmp/osagefs-stress.pid 2>/dev/null; ps -ef | rg osagefs || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'mount | rg /tmp/osagefs-mnt || true; ls -la /tmp/osagefs-mnt || true'`

When iterating on fixes, follow this order:
1) Reproduce with `./scripts/stress_e2e.sh` in sprite.
2) Add/adjust a focused regression test in `src/fs.rs` for the failing behavior.
3) Implement the fix and rerun the same sprite stress command.

## Sprite Checkpoints
Use sprite checkpoints to avoid repeated Linux untar/build setup cost.

Recommended loop:
1) Seed a reusable checkpoint once (cold run):
- `scripts/sprite_perf_loop.sh --sprite iotest --mode cold --create-checkpoint --comment "linux tree prepared"`
2) Iterate quickly from that checkpoint (restore + sync + perf reuse):
- `scripts/sprite_perf_loop.sh --sprite iotest --restore v2 --mode fast`
3) For one-off perf experiments, use ephemeral sprites that auto-clean on exit:
- `scripts/sprite_perf_loop.sh --ephemeral --mode fast`
- Add `--keep-sprite` to preserve the ephemeral sprite, or `--cleanup-stale` to delete other leftover `iotest-perf-*` sprites automatically.

Direct checkpoint commands:
- `sprite checkpoint list -s iotest`
- `sprite checkpoint create -s iotest --comment "note"`
- `sprite restore -s iotest v2`

Notes:
- `--mode fast` adds `--reuse_tree` to `scripts/linux_kernel_perf.sh` so it skips cleanup + extract.
- Keep tarball + extracted kernel tree in the checkpoint you restore from, then only sync code deltas each run.

FIO note:
- In sprite runs, `smallfiles_sync` may return `Input/output error` during high-concurrency open/create (`filesetup.c:open(...)`) while larger sequential/random workloads still complete. Keep the workload in the matrix for regression tracking, but do not let that single failure abort summary generation.

## Artifact capture
If tests fail, capture logs from the sprite:
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'dmesg | tail -200 || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'journalctl -n 200 --no-pager || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'ls -la /tmp/osagefs-it || true'`
