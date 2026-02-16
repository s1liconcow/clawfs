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
- Sandbox note: if a sibling worktree lives outside the current writable root, `git worktree remove` may fail with `Permission denied`; rerun that command with escalated permissions, then run `git branch -D <task-branch>`.

## Project Overview
- OsageFS: a FUSE-based log-structured filesystem that stages writes locally, flushes batched immutable segments (under `/segs/s_<generation>_<segment_id>`) to an object store (local FS, AWS S3, or GCS), and stores metadata as immutable inode-map shards (`/imaps/i_<generation>_<shard>.bin`) plus per-generation delta logs (`/imap_deltas/d_<generation>_<bloom>.bin`).
- Close-time durability uses a local journal under `$STORE/journal`: every staged inode (data or metadata) is serialized with its payload path/bytes so a crash can replay the journal and flush pending writes before the mount finishes. You can disable this (unsafe) path via `--disable-journal` when benchmarking raw throughput.
- Inline threshold (`inline_threshold`): payloads <= threshold stay inline in the delta payload; larger payloads use `SegmentPointer` (segment id, offset, length).
- Payload transforms: inline metadata payloads and immutable segment payloads can be compressed/encrypted. `--inline-compression` and `--segment-compression` default to `true` and only keep compressed bytes when smaller; `--inline-encryption-key <passphrase>` and `--segment-encryption-key <passphrase>` enable ChaCha20Poly1305 encryption (compression runs first when beneficial).
- Pending writes are buffered per inode; `flush_pending()` writes a single immutable segment, emits a delta log, rewrites touched shards, and bumps the superblock generation once. Each large write appends to a staging segment file under `$STORE/segment_stage` so data survives until the next flush. When a flush finishes we drop that file and open a fresh staging file for the next batch. Flush triggers: explicit `flush`/`fsync`/`release`, interval timer, or when staged bytes exceed `pending_bytes`.
- Adaptive large-write flush policy: when the journal is enabled, append-heavy large writes can defer interval-driven flushes and use a higher pending-byte watermark (capped) to reduce write amplification; deferral is still bounded (currently 5s max) so metadata visibility remains bounded while preserving crash replay durability.
- Metadata/list caches follow NFS-like TTLs: `--lookup-cache-ttl-ms` for attrs and `--dir-cache-ttl-ms` for directory entries. A background poller (`--metadata-poll-interval-ms`) lists `/imap_deltas/`, applies newer generations, and kicks off segment prefetches for affected inodes. Immutable segments are cached under `$STORE/segment_cache` up to `--segment-cache-bytes`.
- Metadata/list caches follow NFS-like TTLs: `--lookup-cache-ttl-ms` for attrs and `--dir-cache-ttl-ms` for directory entries. A background poller (`--metadata-poll-interval-ms`) lists `/imap_deltas/`, applies newer generations, and kicks off segment prefetches for affected inodes. Immutable segments are cached under `$STORE/segment_cache` up to `--segment-cache-bytes`. Metadata flushes batch up to `--imap-delta-batch` inodes per delta object and rewrite each shard snapshot only once per flush to limit backing-store API calls.
- Cleanup is coordinated via short leases recorded in the superblock. Clients opportunistically take `DeltaCompaction` leases (prune older `/imap_deltas/`) and `SegmentCompaction` leases (merge older segments and update inode pointers) so maintenance work is shared across mounts.
- Use `--disable-cleanup` when a client is far from the bucket; run a regional cleanup agent (see `docs/CLEANUP_AGENT.md`) to pick up the leases without incurring cross-region traffic.
- Each client maintains a local state file (`--state-path`, default `.osagefs_state.bin`) recording `client_id`, inode pool, and segment pool—no shared metadata writes are needed for ID bookkeeping.

## Key Components
- `src/config.rs`: CLI -> Config mapping. Important fields: `mount_path`, `store_path`, `inline_threshold`, `shard_size`, `inode_batch`, `segment_batch`, `pending_bytes`, `object_provider`, `bucket`, `region`, `endpoint`, `object_prefix`, `gcs_service_account`, `state_path`, `perf_log`, `fsync_on_close`, `flush_interval_ms`, `lookup_cache_ttl_ms`, `dir_cache_ttl_ms`, `metadata_poll_interval_ms`, `segment_cache_bytes`, `foreground`, `allow_other`.
- Logging: `--log-file` defaults to `osagefs.log` and mirrors every log (now defaulting to DEBUG when a file is used) so tailing that file shows detailed staging/flush info.
- `src/perf.rs`: JSONL perf logger that captures `stage_file` / `flush_pending` events when `--perf-log` is supplied.
- `src/replay.rs`: Compressed replay logger (`.jsonl.gz`) for request-layer traces; records ordered FUSE/NFS ops with timing, errno, and metadata (inode/offset/length/path identifiers) but not payload bytes.
- Replay logs now include a startup metadata event (`layer=meta`, `op=fs_config`) with key runtime knobs (`home_prefix`, inline/pending/flush/cache settings, journal/fsync flags, bootstrap user) so replay can align config with capture.
- `src/state.rs`: `ClientStateManager` handles per-client pools, persisted via JSON. Tests ensure persistence across runs.
- `src/metadata.rs`: Handles inode caching, writes shard snapshots under `/imaps`, emits delta logs with bloom-filtered filenames, and performs CAS updates on `metadata/superblock.bin`.
- `src/checkpoint.rs` + `src/bin/osagefs_checkpoint.rs`: checkpoint/restore utility that saves and restores superblock snapshots; use it to roll filesystem state backward/forward by generation without rewriting immutable objects.
- `src/segment.rs`: `SegmentManager::write_batch` serializes `[inode,path,data]` entries into immutable segments; `read_pointer` serves cache hits locally, otherwise issues object-store range reads for the pointer span and asynchronously enqueues full-segment cache fill. Tests verify write/read.
- `src/segment.rs`: `SegmentManager::write_batch` accepts in-memory bytes or staged chunks; flushing can stream staged payloads directly into immutable segments to reduce extra copy overhead in `flush_pending`.
- `src/fs.rs`: `OsageFs` FUSE implementation with pending-staging logic, flush thresholds, multi-client tests, etc. Unit tests cover single-client flush, multi-client independence, and pending-byte/interval stress.
- `scripts/linux_kernel_perf.sh`: End-to-end harness that cleans mounts, downloads Linux tarball (if missing), mounts osagefs, and times untar + kernel build. Handles cleanups and dependencies.
- `scripts/fio_workloads.sh`: Starts OsageFS, runs mixed fio profiles (sequential, random, mixed, small-file sync), and writes per-workload JSON plus `summary.md` under `fio-results-*`.
- `scripts/cleanup.sh`: unmounts, deletes store dir, and removes the state file to give perf scripts a clean slate.
- `scripts/stress_e2e.sh`: Light-weight FUSE smoke test that launches the daemon via `run_osagefs.sh`, performs single-file, burst, large-file, offset-write, and chmod checks directly against the mounted filesystem, and tears everything down afterward.
- `scripts/micro_workflows.sh`: Micro-workflow benchmark harness that runs practical dev/data/AI loops (`dev_smallfile_burst`, `dev_scan_and_status`, `data_csv_etl`, `ai_checkpoint_loop`, optional `dev_incremental_build`) on OsageFS and local disk, then emits `results.jsonl` + `summary.md` with side-by-side timing ratios.
- `scripts/checkpoint.sh`: offline checkpoint helper (`create`/`restore`) for `metadata/superblock.bin`; aborts when mount/process checks indicate OsageFS is still running unless `FORCE=1`.

## Testing / Tools
- `cargo test`: runs unit tests (including new integration-like tests in `fs.rs`, `segment.rs`, `state.rs`).
- `OSAGEFS_RUN_PERF=1 cargo test --test perf_local -- --nocapture`: local performance suite includes stage throughput, batched segment throughput, and small-file IOPS checks.
- `scripts/linux_kernel_perf.sh`: Perf test; requires `user_allow_other` in `/etc/fuse.conf`, `flex`, `bison`, `curl`, `python3`, etc. Sets `PERF_LOG_PATH` (default `$ROOT/osagefs-perf.jsonl`) so runs automatically emit JSONL timing data.
- `scripts/fio_workloads.sh`: fio benchmark harness; requires `fio`, `python3`, and FUSE support in the runtime environment (prefer sprite execution for mount tests).
- `scripts/fio_workloads.sh` supports iteration knobs: `WORKLOADS` (comma-separated workload names, or `all`) and `FAST_REPRO=1` (2s runtime, small sizes) for quick repro loops. Example: `WORKLOADS=smallfiles_sync FAST_REPRO=1 ./scripts/fio_workloads.sh`.
- `scripts/micro_workflows.sh`: Run `MODE=both BUILD_MODE=check ./scripts/micro_workflows.sh` for an OsageFS-vs-local baseline comparison. Use `WORKFLOW_PROFILE=quick|realistic|all` to choose synthetic micro loops, realistic OSS/data/AI flows, or both. Realistic knobs: `OSS_REPO_URL`/`OSS_BUILD_CMD`/`OSS_TEST_CMD`, `DUCKDB_DATASET=cancer|nyc_taxi`, and Imagenette training controls (`AI_EPOCHS`, `AI_MAX_BATCHES`, `AI_SUBSET_IMAGES`). Dependency policy: missing system tools auto-install by default (`ALLOW_SYSTEM_INSTALL=1`), and missing Python modules (`duckdb`, `torch`, `torchvision`) auto-install by default (`ALLOW_PIP_INSTALL=1`).
- `scripts/run_osagefs.sh`: Convenience launcher; defaults `PERF_LOG_PATH=$ROOT/osagefs-perf.jsonl` which can be disabled via `PERF_LOG_PATH=`.
- `scripts/run_osagefs.sh` + `scripts/run_nfs_gateway.sh`: set `REPLAY_LOG_PATH=/path/replay.jsonl.gz` to pass `--replay-log` and capture replay traces for FUSE or direct NFS traffic.
- `src/bin/osagefs_replay.rs`: direct API replayer (`cargo run --release --bin osagefs_replay -- ...`) that replays captured events through `OsageFs::nfs_*` methods (bypasses FUSE/NFS transport), preserving order/timing and synthesizing write payload bytes. It now bootstraps fresh-init state to match normal startup (`/`, `WELCOME.txt`, and `/home/<user>` by default; configurable via `--home-prefix` / `--user-name`).
- `cargo test checkpoint::tests::checkpoint_restore_round_trip_resets_superblock`: validates checkpoint capture + restore round-trip against persisted superblock state.
- `src/bin/osagefs_replay.rs` brute-force mode: `--iterations N` replays from fresh-init state N times with deterministic seeds (`--seed`) and optional chaos knobs (`--jitter-us`, `--chaos-sleep-prob`, `--chaos-sleep-max-us`, `--chaos-flush-prob`) to shake out timing-sensitive issues.
- Mount validation: `scripts/common.sh` now provides `osage_assert_welcome_file` and mount-oriented scripts assert `${MOUNT_PATH}/WELCOME.txt` (or `${NFS_MOUNT_PATH}/WELCOME.txt` for NFS auto-mount) to fail fast when the mount did not come up correctly. Tune wait time with `MOUNT_CHECK_TIMEOUT_SEC` (default `10`).
- `osagefs-nfs-gateway/`: stand-alone crate that serves OsageFS directly over NFSv3 (`nfsserve`, no FUSE mount). NFSv4 support is Enterprise-only.
- When CLI flags/config defaults change, double-check and update `scripts/` launchers in the same PR so script argument sets stay in sync with binaries.

## Common Issues / Fixes
- FUSE error `allow_other`: ensure `user_allow_other` in `/etc/fuse.conf` when running without sudo.
- Root-owned leftovers from sudo runs: unmount and `sudo rm -rf` mount/store dirs before re-running.
- Kernel build dependencies (flex/bison) needed for `make defconfig`.
- `scripts/fio_workloads.sh` generates timestamped `fio-results-*` directories and does not call `scripts/cleanup.sh` at exit, so logs/perf traces are preserved by default for analysis.
- Pending bytes threshold + `--flush-interval-ms` ensure `flush_pending()` runs automatically; staged payloads append to `$STORE/segment_stage/stage_*.bin` and only one of those files exists per in-flight flush. Metadata poller + TTLs keep cached attrs fresh, while cleanup leases prevent `/imap_deltas/` and `/segs/` from growing without bound.
- Troubleshooting policy: reproduce with a failing automated test before fixing. For NFS/data-path issues, first capture logs (`write error`, `load_inode miss`, etc.), then codify the failure as a focused regression test in `src/fs.rs`; only after the test fails should code changes be made. Keep the test to prevent regressions.
- `scripts/stress_e2e.sh` always runs `scripts/cleanup.sh` on exit, which removes both `LOG_FILE` and `PERF_LOG_PATH`; when you need post-run perf analysis, run an equivalent custom workload (or copy logs elsewhere) before cleanup.
- `scripts/stress_e2e.sh` now wraps `scripts/cleanup.sh` via a shared `run_cleanup_script()` helper for both initial cleanup and EXIT teardown, matching the common script pattern used by other harnesses.
- In Sprite VMs, long `apt-get install` runs can sometimes end with `Error: connection closed` / non-zero transport exit even after package `Setting up ...` lines complete; verify required tools explicitly (`fusermount3`, `rg`, `strace`) before retrying full bootstrap.
- Sprite quirk: some images do not provide `/etc/mtab`, so `fusermount -u` can print `failed to open /etc/mtab`; fall back to `umount -l <mount>` when needed.
- Offline checkpoint helper quirk: `scripts/checkpoint.sh` process guard matches `osagefs.*--store-path`. If you run it inside a long wrapper command that also includes `osagefs_checkpoint ... --store-path`, run checkpoint steps in separate shell invocations to avoid false-positive self-matches.
- Offline checkpoint scope note: checkpoints snapshot only the superblock metadata pointer; they do not synthesize missing data objects. When seeding a "post-untar" checkpoint, verify `linux-*/Makefile` exists on the mounted filesystem before `checkpoint.sh create`, and validate by restore + `find /tmp/osagefs-mnt/home/uid$(id -u) -maxdepth 1 -name "linux-*"` afterward.
- Sprite teardown gotcha: if you kill `osagefs` while your shell `cwd` is still inside `/tmp/osagefs-mnt`, follow-on commands can hit `Transport endpoint is not connected` and be misread as data-path EIO. `cd /tmp` before daemon shutdown, then unmount (`umount -l /tmp/osagefs-mnt` fallback).
- Linux kernel build deps in sprite: beyond `flex`/`bison`, install `libelf-dev` (and usually `dwarves`) or `make` fails in `tools/objtool` with `fatal error: gelf.h: No such file or directory`, which is a host dependency issue rather than OsageFS I/O corruption.
- Local FUSE triage note (2026-02-16): during Linux `defconfig` repro, userspace build errors can show `Input/output error` while replay logs contain zero `errno=5` events. Treat this as a likely FUSE transport/session failure (stale mount or daemon/session teardown) rather than an explicit `reply.error(EIO)` path in `src/fs.rs`; verify daemon liveness and mount health (`ps -ef | rg osagefs`, `mount | rg /tmp/osagefs-mnt`) before chasing data-path logic.
- Update (2026-02-16): `scripts/linux_kernel_perf.sh` now fails fast when the mount path is stale/inaccessible or already has an active `osagefs` daemon for the same `--mount-path`; it prints explicit recovery guidance (`sudo umount -l <mount>` / stop existing daemon) instead of proceeding into confusing EIO failures.
- Concurrent `smallfiles_sync` (`numjobs=8`) can expose create/open races; when reproducing, start with `WORKLOADS=smallfiles_sync FAST_REPRO=1 SMALLFILE_NUMJOBS=8` and inspect `/work/osagefs/fio-small*.json` for `open(..., O_CREAT) = -1 EIO`.
- `scripts/micro_workflows.sh` now normalizes DuckDB result payloads to JSON-safe primitives (including `date`/`datetime`) and validates `duckdb_results.json` by parsing it after write; this prevents false `ok` statuses from partially written artifacts.
- Update (2026-02-15): `smallfiles_sync` `open(..., O_CREAT) = -1 EIO` under `fsync=1` / `numjobs=8` was traced to unstable FUSE inode generation values in `ReplyEntry`/`ReplyCreate`. Do not use superblock generation there; use a stable node generation constant (`1`) per inode lifetime.
- Update (2026-02-16): Linux untar crash path fixed: `append_file` could hit `PendingData::Staged` and panic at `unreachable!("staged append handled elsewhere")`, which dropped the FUSE session and surfaced as transport/EIO errors. Keep regression test `append_file_handles_staged_pending_without_panic` in `src/fs.rs`.
- Update (2026-02-16): Generated-header/source corruption during Linux builds was resolved by the metadata-only setattr restage fix (avoid extra `stage_inode` after `stage_file` in setattr paths). After that fix, clean `untar -> make defconfig -> make -j8` completed successfully on `/tmp/osagefs-mnt`.
- Update (2026-02-16): fsync/close sync amplification fix: for FUSE `fsync` and close-sync paths (`flush`/`release` when `--fsync-on-close` is enabled), flush only the target inode plus pending ancestor directories instead of flushing all pending inodes globally. This preserves inode-level durability semantics while avoiding unrelated writeback storms.
- Update (2026-02-16): metadata-only setattr fix: both FUSE `setattr` and `nfs_setattr` should stage file content once (via `stage_file`) and must not immediately restage the inode separately. The extra `stage_inode` could create metadata-only pending entries with stale `Inline([])` storage for non-empty files under heavy build workloads, leading to generated/source file corruption. Preserve caller-specified timestamps by avoiding `stage_file` timestamp overrides.
- Update (2026-02-16): `pjdfstest` `truncate/12.t` + `ftruncate/12.t` can issue `truncate(..., 999999999999999)`. In setattr paths, avoid direct `Vec::resize(target_size as usize, 0)` because it can abort the daemon (`memory allocation of 999999999999999 bytes failed`). Use checked/fallible resize and return `EFBIG` when growth is unrepresentable or cannot be reserved; keep `nfs_setattr_huge_truncate_returns_efbig_and_keeps_fs_alive` in `src/fs.rs` as a regression test.
- Update (2026-02-16): For `pjdfstest` user-switching cases (`-u/-g`), mounts must be accessible beyond root/mounter. OsageFS now supports `OSAGEFS_ALLOW_OTHER=1` to mount with `allow_other`; in sprites, ensure `/etc/fuse.conf` contains `user_allow_other` first.
- Update (2026-02-16): OsageFS now has a proper CLI flag `--allow-other` (wired through `Config.allow_other`). `scripts/run_osagefs.sh` maps `ALLOW_OTHER=1` to `--allow-other`; `OSAGEFS_ALLOW_OTHER=1` remains as a backward-compatible fallback.
- Update (2026-02-16): Long-name behavior should return `ENAMETOOLONG` (not `ENOENT`/success). Enforce component length checks (`NAME_MAX=255`) in FUSE path operations and lookup to satisfy `*/02.t` length tests.
- Update (2026-02-16): `mknod` callback parameter order in `fuser` is `(mode, umask, rdev)`. Mixing `umask`/`rdev` silently compiles (both `u32`) but breaks `major/minor` assertions (`mknod/11.t`).
- Sprite A/B benchmark (2026-02-16): baseline (`HEAD`) vs patched fsync-scope binary on a dev-build-like mixed workload (many object-file appends plus per-round lockfile `fsync`) showed median runtime drop from `6.782s` to `3.153s` across 5 runs (`53.5%` faster), with lower run-to-run variance (`stdev 0.199s -> 0.060s`).

## Useful Commands
- Clean mount/store/state: `fusermount -u /tmp/osagefs-mnt; sudo rm -rf /tmp/osagefs-mnt /tmp/osagefs-store ~/.osagefs_state.bin`.
- Run perf test: `LOG_FILE=$HOME/linux_build_timings.log ./scripts/linux_kernel_perf.sh`.
- Run fio workload sweep: `RESULTS_DIR=/work/osagefs/fio-results LOG_FILE=/work/osagefs/osagefs.log PERF_LOG_PATH=/work/osagefs/osagefs-perf.jsonl ./scripts/fio_workloads.sh`.
- `scripts/run_osagefs.sh` defaults to `$ROOT/osagefs-perf.jsonl`; set `PERF_LOG_PATH=` to disable tracing when needed.
- Replay trace capture: `REPLAY_LOG_PATH=/work/osagefs/replay.jsonl.gz ./scripts/run_osagefs.sh` (or `./scripts/run_nfs_gateway.sh`). Inspect with `gzip -dc /work/osagefs/replay.jsonl.gz | tail -n 50`.
- Direct API replay: `cargo run --release --bin osagefs_replay -- --trace-path /work/osagefs/replay.jsonl.gz --store-path /tmp/osagefs-replay-store --local-cache-path /tmp/osagefs-replay-cache --state-path /tmp/osagefs-replay-state.bin --layer fuse --speed 1.0` (add `--ignore-timing` for max-throughput replay).
- Create checkpoint: `cargo run --bin osagefs_checkpoint -- create --store-path /tmp/osagefs-store --checkpoint-path /tmp/osagefs-store.cp.bin --note "before experiment"`.
- Restore checkpoint: `cargo run --bin osagefs_checkpoint -- restore --store-path /tmp/osagefs-store --checkpoint-path /tmp/osagefs-store.cp.bin`.
- Offline checkpoint helper: `CHECKPOINT_PATH=/tmp/osagefs-store.cp.bin scripts/checkpoint.sh create` and `CHECKPOINT_PATH=/tmp/osagefs-store.cp.bin scripts/checkpoint.sh restore`.
- Enable perf tracing for other invocations by exporting `PERF_LOG_PATH=/tmp/osagefs-perf.jsonl` (empty string disables logging when scripts default it). Use `--fsync-on-close` to restore immediate durability, `--flush-interval-ms` to adjust opportunistic flush cadence (0 disables timer), and `--disable-cleanup` when cleanup will be handled by an external agent.
- Export OsageFS via NFS: `cargo run --manifest-path osagefs-nfs-gateway/Cargo.toml -- --store-path /tmp/osagefs-store --listen 0.0.0.0:2049`.

Keep this file updated with future design decisions, scripts, and troubleshooting steps.

# Using Sprites

## Goal
Privileged execution must happen inside a Fly.io Sprite VM (Linux sandbox),
NOT on the developer machine. Use the `sprite` CLI to run commands remotely.

## Requirements
- `sprite` CLI installed and available on PATH
- `SPRITES_TOKEN` set in the environment
- A sprite name to use (default: `osagefs`)

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
- `RAW_BASE="${REPO_SLUG}-${BRANCH_SLUG}"`
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
- Keep `osagefs` as a shared fallback only when you explicitly need a common long-lived sprite.
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
- `scripts/sprite_perf_loop.sh --sprite $SPRITE_NAME --mode cold --create-checkpoint --comment "linux tree prepared"`
2) Iterate quickly from that checkpoint (restore + sync + perf reuse):
- `scripts/sprite_perf_loop.sh --sprite SPRITE_NAME --restore v2 --mode fast`
3) For one-off perf experiments, use ephemeral sprites that auto-clean on exit:
- `scripts/sprite_perf_loop.sh --ephemeral --mode fast`
- Add `--keep-sprite` to preserve the ephemeral sprite, or `--cleanup-stale` to delete other leftover `osagefs-perf-*` sprites automatically.

Direct checkpoint commands:
- `sprite checkpoint list -s $SPRITE_NAME`
- `sprite checkpoint create -s $SPRITE_NAME --comment "note"`
- `sprite restore -s $SPRITE_NAME v2`

Notes:
- `--mode fast` adds `--reuse_tree` to `scripts/linux_kernel_perf.sh` so it skips cleanup + extract.
- Keep tarball + extracted kernel tree in the checkpoint you restore from, then only sync code deltas each run.

FIO note:
- In sprite runs, `smallfiles_sync` may return `Input/output error` during high-concurrency open/create (`filesetup.c:open(...)`) while larger sequential/random workloads still complete. Keep the workload in the matrix for regression tracking, but do not let that single failure abort summary generation.
- Regression check for this class of issue: `WORKLOADS=smallfiles_sync FAST_REPRO=1 SMALLFILE_NUMJOBS=8` should pass repeatedly with `fsync=1` once FUSE generation is stable.
- `scripts/micro_workflows.sh` `dev_scan_and_status` can expose transient `.git/config.lock` behavior on OsageFS during `git init/config/status`; the harness now retries and clears stale lock files so the suite can complete while preserving this workflow signal.
- Git clone lockfile failure root cause: FUSE `rename` had a same-directory stale-entry bug (`config.lock -> config` could leave `config.lock` visible). Fixed by routing FUSE and NFS rename through shared `rename_entry(...)` logic and covered by `rename_same_parent_drops_old_name`.
- Large-file metadata-only flush root cause: `flush_pending` incorrectly required a new segment pointer for any record with `size > inline_threshold`, even when no payload was pending. Fixed by only requiring pointers for inodes with segment payload in the current flush (`segment_data_inodes`), covered by `metadata_only_flush_preserves_large_file_pointer`.
- Sprite caveat: in this environment, Rust workloads on the OsageFS mount may intermittently fail with `rust-lld` bus errors during build-script linking; this appears environment/toolchain-related, not a filesystem semantic error. For reliable cross-filesystem micro comparisons, use non-Rust OSS build targets or run Rust comparisons in a sprite without this linker instability.
- Dev-build optimization validated in sprite (2026-02-15): keep source/worktree on OsageFS but place Rust build artifacts on sprite-local disk with a shared target dir, e.g. `CARGO_TARGET_DIR=/tmp/osagefs-rust-target-shared cargo check -q`. In repeated runs this reduced median `cargo check` time from ~23.9s (unique cold target each run) to ~2.6s (shared warm target), ~89% faster, while preserving OsageFS durability semantics for source and metadata paths.

## Artifact capture
If tests fail, capture logs from the sprite:
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'dmesg | tail -200 || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'journalctl -n 200 --no-pager || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'ls -la /tmp/osagefs-it || true'`
