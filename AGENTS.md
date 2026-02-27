# OsageFS Assistant Notes

## Living Document
Please continuously update this document with useful things you figure out that will make future workflows smoother and increase iteration speed.  

## Increase Iteration Speed and Token Efficiency
If a new tool (or modifying an existing) can help with your work, propose building it.  If you're creating a shell or python command longer than a few lines, consider making a reusable tool instead.

## Post-Merge Hygiene
- After merging a task branch into `master`, automatically clean up temporary git worktrees and local task branches from that effort.
- Standard cleanup sequence: `git worktree list`, `git worktree remove <task-worktree-path>`, `git branch -D <task-branch>`.
- If multiple task branches/worktrees were used for the same change, remove all of them once the merge is complete and verified.
- Sandbox note: if a sibling worktree lives outside the current writable root, `git worktree remove` may fail with `Permission denied`; rerun that command with escalated permissions, then run `git branch -D <task-branch>`.

## Project Overview
- OsageFS: a FUSE-based log-structured filesystem that stages writes locally, flushes batched immutable segments (under `/segs/s_<generation>_<segment_id>`) to an object store (local FS, AWS S3, or GCS), and stores metadata as immutable inode-map shards (`/imaps/i_<generation>_<shard>.bin`) plus per-generation delta logs (`/imap_deltas/d_<generation>_<bloom>.bin`).
- Close-time durability uses a local journal under `$STORE/journal`: every staged inode (data or metadata) is serialized with its payload path/bytes so a crash can replay the journal and flush pending writes before the mount finishes. You can disable this (unsafe) path via `--disable-journal` when benchmarking raw throughput.
- Inline threshold (`inline_threshold`): payloads <= threshold stay inline in the delta payload; larger payloads use `SegmentPointer` (segment id, offset, length).
- Payload transforms: inline metadata payloads and immutable segment payloads can be compressed/encrypted. `--inline-compression` and `--segment-compression` default to `true` and only keep compressed bytes when smaller; `--inline-encryption-key <passphrase>` and `--segment-encryption-key <passphrase>` enable ChaCha20Poly1305 encryption (compression runs first when beneficial).
- Pending writes are buffered per inode; `flush_pending()` writes a single immutable segment, emits a delta log, rewrites touched shards, and bumps the superblock generation once. Each large write appends to a staging segment file under `$STORE/segment_stage` so data survives until the next flush. When a flush finishes we drop that file and open a fresh staging file for the next batch. Flush triggers: explicit `flush`/`fsync`/`release`, interval timer, or when staged bytes exceed `pending_bytes`.
- Adaptive large-write flush policy: append-heavy large writes can use a higher pending-byte watermark (capped) to reduce write amplification. `flush_interval_ms` is treated as the hard max dirty-data wait time for interval flushing (no extra adaptive defer window).
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
- `src/segment.rs`: `SegmentManager::write_batch` serializes `[inode,path,data]` entries (in-memory or staged chunks) into immutable segments; `flush_pending` can stream staged payloads directly to avoid extra copies. `read_pointer` serves cache hits locally, otherwise issues object-store range reads and asynchronously enqueues full-segment cache fill. Tests cover write/read paths.
- `src/fs/`: split filesystem module. `mod.rs` holds shared types/constants, `core.rs` handles inode/path primitives, `write_path.rs` handles staging/appends/segment writes, `flush.rs` handles flush/journal/replay timing, `ops.rs` provides shared transport-agnostic `op_*` methods, `fuse.rs` contains FUSE adapter wiring, and `nfs.rs` contains NFS adapter wiring.
- `scripts/linux_kernel_perf.sh`: End-to-end harness that cleans mounts, downloads Linux tarball (if missing), mounts osagefs, and times untar + kernel build. Handles cleanups and dependencies.
- `scripts/fio_workloads.sh`: Starts OsageFS, runs mixed fio profiles (sequential, random, mixed, small-file sync), and writes per-workload JSON plus `summary.md` under `fio-results-*`.
- `scripts/cleanup.sh`: unmounts, deletes store dir, and removes the state file to give perf scripts a clean slate.
- `scripts/stress_e2e.sh`: Light-weight FUSE smoke test that launches the daemon via `run_osagefs.sh`, performs single-file, burst, large-file, offset-write, and chmod checks directly against the mounted filesystem, and tears everything down afterward.
- `scripts/micro_workflows.sh`: Micro-workflow benchmark harness that runs practical dev/data/AI loops (`dev_smallfile_burst`, `dev_scan_and_status`, `data_csv_etl`, `ai_checkpoint_loop`, optional `dev_incremental_build`) on OsageFS and local disk, then emits `results.jsonl` + `summary.md` with side-by-side timing ratios.
- `scripts/checkpoint.sh`: offline checkpoint helper (`create`/`restore`) for `metadata/superblock.bin`; aborts when mount/process checks indicate OsageFS is still running unless `FORCE=1`.

## Testing / Tools
- Default code-change validation sequence for agent runs: `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, then `cargo test` (or a clearly documented narrower test target when full test suite is not feasible).
- `cargo test`: runs unit tests (including new integration-like tests in `src/fs/tests/mod.rs`, `segment.rs`, `state.rs`).
- `OSAGEFS_RUN_PERF=1 cargo test --test perf_local -- --nocapture`: local performance suite includes stage throughput, batched segment throughput, and small-file IOPS checks.
- `scripts/linux_kernel_perf.sh`: Perf test; requires `user_allow_other` in `/etc/fuse.conf`, `flex`, `bison`, `curl`, `python3`, etc. Sets `PERF_LOG_PATH` (default `$ROOT/osagefs-perf.jsonl`) so runs automatically emit JSONL timing data.
- `scripts/fio_workloads.sh`: fio benchmark harness; requires `fio`, `python3`, and FUSE support in the runtime environment (prefer sprite execution for mount tests).
- `scripts/sprite_validate_parallel.sh`: launches dedicated role-based sprites in parallel for `xfstests`, Linux kernel compile (`linux_kernel_perf.sh`), and `pjdfstest`, with per-task logs and summary exit status.
- xfstests (Sprite): upstream xfstests fuse config expects `FUSE_SUBTYP` (not `FUSE_SUBTYPE`) and a `mount.fuse.<subtype>` helper. For OsageFS in sprites, install `/sbin/mount.fuse.osagefs`, set `FSTYP=fuse`, `FUSE_SUBTYP=.osagefs`, and pass store paths via `MOUNT_OPTIONS` / `TEST_FS_MOUNT_OPTS` (`-osource=/tmp/osagefs-...`).
- xfstests caveat: for TEST/SCRATCH dual-mount flows, set distinct `--fuse-fsname` values so xfstests source/device matching can distinguish mounts reliably.
- xfstests CLI ordering: pass options before tests (`./check -E excludes generic/001 ...`), otherwise xfstests exits with `Arguments before tests, please!`.
- `scripts/fio_workloads.sh` supports iteration knobs: `WORKLOADS` (comma-separated workload names, or `all`) and `FAST_REPRO=1` (2s runtime, small sizes) for quick repro loops. Example: `WORKLOADS=smallfiles_sync FAST_REPRO=1 ./scripts/fio_workloads.sh`.
- Update (2026-02-22): heap profiling support was added to `scripts/run_osagefs.sh`/`scripts/fio_workloads.sh` via `HEAPTRACK=1` (optional `HEAPTRACK_OUTPUT`, `HEAPTRACK_RAW=1`, `HEAPTRACK_EXTRA_ARGS`). Example focused OOM repro: `HEAPTRACK=1 WORKLOADS=randwrite_4k,smallfiles_sync FAST_REPRO=1 ./scripts/fio_workloads.sh`, then inspect with `heaptrack_print -f <heaptrack.gz> -p -a -n 30 -s 10`.
- `scripts/micro_workflows.sh`: Run `MODE=both BUILD_MODE=check ./scripts/micro_workflows.sh` for an OsageFS-vs-local baseline comparison. Use `WORKFLOW_PROFILE=quick|realistic|all` to choose synthetic micro loops, realistic OSS/data/AI flows, or both. Realistic knobs: `OSS_REPO_URL`/`OSS_BUILD_CMD`/`OSS_TEST_CMD`, `DUCKDB_DATASET=cancer|nyc_taxi`, and Imagenette training controls (`AI_EPOCHS`, `AI_MAX_BATCHES`, `AI_SUBSET_IMAGES`). Dependency policy: missing system tools auto-install by default (`ALLOW_SYSTEM_INSTALL=1`), and missing Python modules (`duckdb`, `torch`, `torchvision`) auto-install by default (`ALLOW_PIP_INSTALL=1`).
- Update (2026-02-17): quick-profile non-AI loops now target multi-second runtime for clearer signal. Current defaults: `SMALLFILE_COUNT=5000`, `SMALLFILE_SIZE=2048`, `DEV_SCAN_TREE_COPIES=8`, `DEV_SCAN_EDIT_FILES=3000`, `DEV_SCAN_RG_PASSES=80`, `DEV_SCAN_STATUS_PASSES=300`, `DEV_SCAN_MUTATION_ROUNDS=60`, `ETL_ROWS=500000`, `ETL_COLS=10`, `ETL_PASSES=3`. Override env vars for faster smoke runs.
- Update (2026-02-17): OsageFS now emits a startup `info` log on boot (`target=startup`, message prefix `fs_boot_config`) containing effective runtime filesystem config (sanitized: encryption keys reported as enabled/disabled only), including resolved `allow_other` behavior.
- Update (2026-02-17): `osagefs-nfs-gateway` journal control is now explicitly toggleable (`--disable-journal` / `--enable-journal`), and gateway `imap_delta_batch` now matches core default `512`. `scripts/run_nfs_gateway.sh` now supports `ENABLE_JOURNAL=1` and rejects conflicting `DISABLE_JOURNAL=1` + `ENABLE_JOURNAL=1`.
- `scripts/run_osagefs.sh`: Convenience launcher; defaults `PERF_LOG_PATH=$ROOT/osagefs-perf.jsonl` which can be disabled via `PERF_LOG_PATH=`.
- `scripts/run_osagefs.sh` now rebuilds `target/release/osagefs` automatically when missing or older than `Cargo.toml`/`Cargo.lock`/`src`/`fuser-mt` inputs, so normal launches pick up fresh code without a manual build step.
- `scripts/run_osagefs.sh` + `scripts/run_nfs_gateway.sh`: set `REPLAY_LOG_PATH=/path/replay.jsonl.gz` to pass `--replay-log` and capture replay traces for FUSE or direct NFS traffic.
- Update (2026-02-27): repo-local pre-commit hook now lives at `.githooks/pre-commit` and runs `cargo fmt --all --check` plus `cargo clippy --all-targets --all-features -- -D warnings`; enable with `git config core.hooksPath .githooks`.
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
- Troubleshooting policy: reproduce with a failing automated test before fixing. For NFS/data-path issues, first capture logs (`write error`, `load_inode miss`, etc.), then codify the failure as a focused regression test in `src/fs/tests/mod.rs`; only after the test fails should code changes be made. Keep the test to prevent regressions.
- `scripts/stress_e2e.sh` always runs `scripts/cleanup.sh` on exit, which removes both `LOG_FILE` and `PERF_LOG_PATH`; when you need post-run perf analysis, run an equivalent custom workload (or copy logs elsewhere) before cleanup.
- Update (2026-02-20): `scripts/micro_workflows.sh` preserves caller-provided `PERF_LOG_PATH` on EXIT cleanup and perf logging is now opt-in; pass `PERF_LOG_PATH=/path/to/osagefs-perf.jsonl` explicitly when you want traces.
- `scripts/stress_e2e.sh` now wraps `scripts/cleanup.sh` via a shared `run_cleanup_script()` helper for both initial cleanup and EXIT teardown, matching the common script pattern used by other harnesses.
- In Sprite VMs, long `apt-get install` runs can sometimes end with `Error: connection closed` / non-zero transport exit even after package `Setting up ...` lines complete; verify required tools explicitly (`fusermount3`, `rg`, `strace`) before retrying full bootstrap.
- Sprite quirk: some images do not provide `/etc/mtab`, so `fusermount -u` can print `failed to open /etc/mtab`; fall back to `umount -l <mount>` when needed.
- Offline checkpoint helper quirk: `scripts/checkpoint.sh` process guard matches `osagefs.*--store-path`. If you run it inside a long wrapper command that also includes `osagefs_checkpoint ... --store-path`, run checkpoint steps in separate shell invocations to avoid false-positive self-matches.
- Offline checkpoint scope note: checkpoints snapshot only the superblock metadata pointer; they do not synthesize missing data objects. When seeding a "post-untar" checkpoint, verify `linux-*/Makefile` exists on the mounted filesystem before `checkpoint.sh create`, and validate by restore + `find /tmp/osagefs-mnt/home/uid$(id -u) -maxdepth 1 -name "linux-*"` afterward.
- Sprite teardown gotcha: if you kill `osagefs` while your shell `cwd` is still inside `/tmp/osagefs-mnt`, follow-on commands can hit `Transport endpoint is not connected` and be misread as data-path EIO. `cd /tmp` before daemon shutdown, then unmount (`umount -l /tmp/osagefs-mnt` fallback).
- Linux kernel build deps in sprite: beyond `flex`/`bison`, install `libelf-dev` (and usually `dwarves`) or `make` fails in `tools/objtool` with `fatal error: gelf.h: No such file or directory`, which is a host dependency issue rather than OsageFS I/O corruption.
- Local FUSE triage note (2026-02-16): during Linux `defconfig` repro, userspace build errors can show `Input/output error` while replay logs contain zero `errno=5` events. Treat this as a likely FUSE transport/session failure (stale mount or daemon/session teardown) rather than an explicit `reply.error(EIO)` path in `src/fs/fuse.rs`; verify daemon liveness and mount health (`ps -ef | rg osagefs`, `mount | rg /tmp/osagefs-mnt`) before chasing data-path logic.
- Update (2026-02-16): `scripts/linux_kernel_perf.sh` now fails fast when the mount path is stale/inaccessible or already has an active `osagefs` daemon for the same `--mount-path`; it prints explicit recovery guidance (`sudo umount -l <mount>` / stop existing daemon) instead of proceeding into confusing EIO failures.
- Concurrent `smallfiles_sync` (`numjobs=8`) can expose create/open races; when reproducing, start with `WORKLOADS=smallfiles_sync FAST_REPRO=1 SMALLFILE_NUMJOBS=8` and inspect `/work/osagefs/fio-small*.json` for `open(..., O_CREAT) = -1 EIO`.
- `scripts/micro_workflows.sh` now normalizes DuckDB result payloads to JSON-safe primitives (including `date`/`datetime`) and validates `duckdb_results.json` by parsing it after write; this prevents false `ok` statuses from partially written artifacts.
- Update (2026-02-15): `smallfiles_sync` `open(..., O_CREAT) = -1 EIO` under `fsync=1` / `numjobs=8` was traced to unstable FUSE inode generation values in `ReplyEntry`/`ReplyCreate`. Do not use superblock generation there; use a stable node generation constant (`1`) per inode lifetime.
- Update (2026-02-16): Linux untar crash path fixed: `append_file` could hit `PendingData::Staged` and panic at `unreachable!("staged append handled elsewhere")`, which dropped the FUSE session and surfaced as transport/EIO errors. Keep regression test `append_file_handles_staged_pending_without_panic` in `src/fs/tests/mod.rs`.
- Update (2026-02-27): append-path stale-size race fix: when rebuilding from `state.flushing`, use flushing data length (not caller `record.size`), and never shrink inline pending buffers to a stale record size during append. This prevents truncating in-flight bytes under flush races (regression: `append_while_flushing_uses_flushing_data_len_not_stale_record_size`).
- Update (2026-02-16): Generated-header/source corruption during Linux builds was resolved by the metadata-only setattr restage fix (avoid extra `stage_inode` after `stage_file` in setattr paths). After that fix, clean `untar -> make defconfig -> make -j8` completed successfully on `/tmp/osagefs-mnt`.
- Update (2026-02-16): fsync/close sync amplification fix: for FUSE `fsync` and close-sync paths (`flush`/`release` when `--fsync-on-close` is enabled), flush only the target inode plus pending ancestor directories instead of flushing all pending inodes globally. This preserves inode-level durability semantics while avoiding unrelated writeback storms.
- Update (2026-02-17): journal durability semantics now align better with ext4-style writeback expectations for local state: mutation-time journal writes no longer force `sync_all` per syscall, while inode `fsync`/close-sync paths perform explicit local sync of staged payload files plus relevant journal entries before flush. This keeps `write()` fast/no-durability-promise semantics and makes `fsync` the explicit local durability boundary.
- Update (2026-02-22): `flush_pending` perf telemetry now includes step timings (`flush_lock_wait_ms`, `drain_select_ms`, `prepare_generation_ms`, `classify_records_ms`, `merge_segment_extents_ms`, `metadata_persist_only_ms`, `metadata_sync_only_ms`, `finalize_ms`) to isolate multi-second overhead beyond `segment_write_ms`.
- Update (2026-02-22): `flush_pending` drain now uses per-inode `try_lock` (non-blocking) when moving `pending -> flushing`; contended inodes are skipped for that cycle (`drain_skipped_locked` in perf details) instead of stalling flush behind active writers.
- Update (2026-02-22): flush drain now scans `pending_inodes` (dirty-index `DashSet`) instead of all `active_inodes`; pending stage/recovery paths maintain this index and flush removes entries once pending is cleared. This avoids O(total-active-inodes) scans when only a few inodes are dirty.
- Update (2026-02-27): in `flush_pending` segment-extent merge, cache miss on `get_cached_inode()` now falls back to authoritative `metadata.get_inode()` before combining `base_extents` + new extents. This avoids stale-base fallback under heavy churn that could otherwise drop recently committed extents.
- Update (2026-02-27): unlink semantics fix for `pjdfstest` `unlink/14.t`: on last unlink (`link_count == 1`), keep inode data with `link_count=0` instead of immediate tombstone so open file handles continue to support `fstat`/I/O after path removal. Regression test: `unlink_last_link_keeps_open_inode_readable_and_writable`.
- Update (2026-02-22): `SegmentManager::rotate_stage_file()` no longer calls `sync_data()` on the active stage file during flush rotation. This split-point now only hands new writes to a fresh stage file; explicit durability boundaries still use `sync_staged_chunks()` (`fsync`/close-sync paths). This removes multi-second `rotate_stage_file_ms` stalls from flush.
- Update (2026-02-22): staged chunk cleanup now batches per-path release (`SegmentManager::release_staged_chunks`) so flush finalize no longer acquires `stage_state` lock and performs file operations once per chunk; `release_pending_data` now uses the batched API.
- Update (2026-02-22): staged chunk release for the active stage file now rotates to a fresh stage file instead of synchronous in-place truncate/reset, and staged file deletes are dispatched via background blocking tasks. This removes foreground flush finalize stalls from large stage-file cleanup.
- Update (2026-02-27): Linux object corruption (`ld: ... nl80211.o: corrupt symbol table`) can come from writes arriving while the inode is in `flushing` state. In `write_large_segments`, correctness should take priority: if `pending` is empty and `flushing` has data, rebuild pending bytes from flushing payloads before applying the new write so in-flight staged chunks are not dropped.
- Update (2026-02-16): metadata-only setattr fix: both FUSE `setattr` and `nfs_setattr` should stage file content once (via `stage_file`) and must not immediately restage the inode separately. The extra `stage_inode` could create metadata-only pending entries with stale `Inline([])` storage for non-empty files under heavy build workloads, leading to generated/source file corruption. Preserve caller-specified timestamps by avoiding `stage_file` timestamp overrides.
- Update (2026-02-16): `pjdfstest` `truncate/12.t` + `ftruncate/12.t` can issue `truncate(..., 999999999999999)`. In setattr paths, avoid direct `Vec::resize(target_size as usize, 0)` because it can abort the daemon (`memory allocation of 999999999999999 bytes failed`). Use checked/fallible resize and return `EFBIG` when growth is unrepresentable or cannot be reserved; keep `nfs_setattr_huge_truncate_returns_efbig_and_keeps_fs_alive` in `src/fs/tests/mod.rs` as a regression test.
- Update (2026-02-16): `src/fs.rs` was modularized into `src/fs/` with a shared `ops.rs` layer. Add new filesystem behavior in `ops.rs` first, and keep `fuse.rs`/`nfs.rs` focused on transport-level validation, replay logging, and response mapping.
- Update (2026-02-17): `op_read` now routes through `read_file_range(...)` so large segment-backed files can satisfy read windows without always materializing the entire file in memory first.
- Update (2026-02-16): For `pjdfstest` user-switching cases (`-u/-g`), mounts must be accessible beyond root/mounter. OsageFS now supports `OSAGEFS_ALLOW_OTHER=1` to mount with `allow_other`; in sprites, ensure `/etc/fuse.conf` contains `user_allow_other` first.
- Update (2026-02-16): OsageFS now has a proper CLI flag `--allow-other` (wired through `Config.allow_other`). `scripts/run_osagefs.sh` maps `ALLOW_OTHER=1` to `--allow-other`; `OSAGEFS_ALLOW_OTHER=1` remains as a backward-compatible fallback.
- Update (2026-02-16): Long-name behavior should return `ENAMETOOLONG` (not `ENOENT`/success). Enforce component length checks (`NAME_MAX=255`) in FUSE path operations and lookup to satisfy `*/02.t` length tests.
- Update (2026-02-16): `mknod` callback parameter order in `fuser` is `(mode, umask, rdev)`. Mixing `umask`/`rdev` silently compiles (both `u32`) but breaks `major/minor` assertions (`mknod/11.t`).
- Performance summary (2026-02-16): fsync-scope and segment-write-path optimizations materially improved dev-loop latency and local `perf_local` throughput; keep current flush-scope and segment serialization behavior unless a targeted regression test/perf run indicates otherwise.
- Update (2026-02-16): `io_uring` local segment write experiment was removed after `perf_local` did not show consistent wins versus the standard write path; keep default object-store/local write flow until a better async write design is ready.

## Useful Commands
- Clean mount/store/state: `fusermount -u /tmp/osagefs-mnt; sudo rm -rf /tmp/osagefs-mnt /tmp/osagefs-store ~/.osagefs_state.bin`.
- Run perf test: `LOG_FILE=$HOME/linux_build_timings.log ./scripts/linux_kernel_perf.sh`.
- Run fio workload sweep: `RESULTS_DIR=/work/osagefs/fio-results LOG_FILE=/work/osagefs/osagefs.log PERF_LOG_PATH=/work/osagefs/osagefs-perf.jsonl ./scripts/fio_workloads.sh`.
- Analyze perf logs: `./scripts/analyze_perf_log.py --log perf-log.jsonl --event flush_pending --top 10` (or point `--log` at `osagefs-perf.jsonl`) to summarize duration percentiles and top slow flushes with step breakdown.
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
- For every code-changing task, run and report `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and tests (`cargo test` by default) before handing off.

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

## Sprite FUSE Troubleshooting
If stress fails with `Input/output error` (for example during `cp` in the attribute test), collect these immediately:
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'tail -n 200 /work/osagefs/osagefs.log || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'tail -n 200 /work/osagefs/osagefs-perf.jsonl || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cat /tmp/osagefs-stress.pid 2>/dev/null; ps -ef | rg osagefs || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'mount | rg /tmp/osagefs-mnt || true; ls -la /tmp/osagefs-mnt || true'`

When iterating on fixes, follow this order:
1) Reproduce with `./scripts/stress_e2e.sh` in sprite.
2) Add/adjust a focused regression test in `src/fs/tests/mod.rs` for the failing behavior.
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

## Artifact capture
If tests fail, capture logs from the sprite:
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'dmesg | tail -200 || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'journalctl -n 200 --no-pager || true'`
- `sprite exec -s "$SPRITE_NAME" -- bash -lc 'ls -la /tmp/osagefs-it || true'`

- Update (2026-02-19): write-path contention hardening: `append_file` no longer panics when a pending entry disappears during concurrent mutation (returns `EIO`), and mutation handoff for `stage_file`/`write_large_segments` now avoids cloning payload bytes while still preserving inode visibility via `mutating_inodes` handoff under lock.
- Update (2026-02-19): synchronous threshold-triggered write-path flushes are now deferred to the interval/explicit flush path (non-blocking write syscall path), reducing foreground write latency spikes under heavy append/segment traffic.
- Update (2026-02-19): pending-byte threshold and interval flush triggers now schedule `flush_pending()` on a background worker (`trigger_async_flush`) with coalescing (`flush_scheduled`) so foreground writes still trigger flush promptly without blocking syscall latency.
- Update (2026-02-19): pending inline/staged buffers now use `Arc`-backed storage (`PendingData::Inline(Arc<Vec<u8>>`, `PendingSegments.chunks: Arc<Vec<StagedChunk>>`) so rollback/hand-off paths can clone handles without deep-copying payload bytes.
- Update (2026-02-19): `append_file` now atomically moves the inode from `pending_inodes` to `mutating_inodes` up front, performs read/stage work on local state, and only re-locks maps for final commit/rollback; non-I/O races no longer surface as `EIO`.
- Update (2026-02-21): `SegmentManager::write_batch` now chunks large payloads into 4 MiB segment entries when segment compression is enabled, and flush paths persist all returned extents per inode. This caps per-entry compression working set (helps prevent OOM during large fio writes) while preserving logical offsets for reads.
- Update (2026-02-19): pending/mutating/flushing inode tables now use `DashMap` instead of `Mutex<HashMap<...>>` for concurrent access; flush selection snapshots keys then removes entries explicitly before selector/commit to preserve rollback behavior.
- Update (2026-02-21): fixed stale-base-extents race in `flush_pending`: when an async flush (flush #N) is running for an inode while a new write arrives and creates a fresh pending entry with stale `base_extents`, flush #(N+1) would build `all_extents = stale_base + new_extent` and overwrite the metadata record from flush #N, losing the extent that flush #N committed. Fix: at flush time, retrieve fresh committed extents via `metadata.get_cached_inode()` (cache-only, no shard reload, no negative-cache side effects) instead of relying on the potentially stale `base_extents` captured when the pending entry was created. Flushes are serialized by `flush_lock`, so the metadata cache is always authoritative at this point. `MetadataStore::get_cached_inode()` was added for safe synchronous-context cache lookups. Regression test: `prefill_seq_then_overwrite_then_read_is_consistent`.
- Update (2026-02-21): fixed `load_inode` ENOENT race: three sequential DashMap lookups (pending → mutating → flushing) in `load_inode` are not collectively atomic — an inode moving between maps between checks causes a spurious ENOENT. Fixed with `load_inode_in_memory()` helper + double-check pattern (re-check after the slow metadata lookup, which acts as a natural delay).  Regression test: `load_inode_visible_during_large_segment_mutation`.
- Update (2026-02-22): heaptrack OOM triage on `scripts/fio_workloads.sh` (`WORKLOADS=randwrite_4k,smallfiles_sync` without `FAST_REPRO`) showed multi-GiB peaks from pending staged range reads + flush serialization (`slice_pending_bytes`/`read_staged_chunks` and `SegmentManager::write_batch`). `slice_pending_bytes` now serves staged-only ranges via overlapped chunk range reads (`read_staged_chunk_range`) instead of materializing full pending payloads.
