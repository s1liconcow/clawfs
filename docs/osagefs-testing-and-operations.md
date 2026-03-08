# OsageFS Testing And Operations

## Validation And Benchmarking
- Default validation: `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, then `cargo test`.
- Local performance suite: `OSAGEFS_PERF_PROFILE=balanced cargo bench --bench perf_local_criterion`.
- `OSAGEFS_PERF_PROFILE=fast` in `benches/perf_local_criterion.rs` keeps `sample_size=20` and increases `measurement_time` to 13s to avoid Criterion sample warnings on slower hosts.
- Perf guard uses `scripts/perf_guard.sh` together with `OSAGEFS_BENCH_METRICS_FILE`, which is emitted as JSONL by `benches/perf_local_criterion.rs`.
- `scripts/perf_guard.sh` copies Criterion HTML reports from `target/criterion/report` into `bench-artifacts/perf_guard_graphs/<commit5>/` by default. Override with `PERF_GUARD_GRAPH_ROOT` or `PERF_GUARD_GRAPH_DIR`.
- Hook runs can skip graph artifact copies with `PERF_GUARD_WRITE_GRAPHS=0`. `.githooks/pre-push` uses this to avoid staging large report trees.
- `.github/workflows/perf-reports-pages.yml` publishes `bench-artifacts/perf_guard_graphs` and creates a root index linking each `<date>-<commit5>/report/index.html`.
- CI note: Criterion `html_reports` requires native `fontconfig`; keep `pkg-config` and `libfontconfig1-dev` installed in Rust CI jobs that build tests or benches.
- `segment_sequential_read_throughput` in `benches/perf_local_criterion.rs` must treat `write_batch` output as a logical extent list sorted by `logical_offset`, not a single pointer, because large payloads are chunked into 4 MiB extents.

## Script Notes
- `scripts/fio_workloads.sh` supports `WORKLOADS`, `FAST_REPRO=1`, and `HEAPTRACK=1`. Example: `WORKLOADS=smallfiles_sync FAST_REPRO=1 ./scripts/fio_workloads.sh`.
- `scripts/micro_workflows.sh` supports `MODE=both`, `BUILD_MODE=check`, and `WORKFLOW_PROFILE=quick|realistic|all`. Common knobs include `SMALLFILE_COUNT=5000`, `DEV_SCAN_TREE_COPIES=8`, and `ETL_ROWS=500000`.
- `scripts/run_osagefs.sh` auto-rebuilds when source files are newer. It defaults `PERF_LOG_PATH=$ROOT/osagefs-perf.jsonl`; set `PERF_LOG_PATH=` to disable logging.
- `REPLAY_LOG_PATH=/path/replay.jsonl.gz` works for both FUSE and NFS launchers.
- `scripts/profile_daemon.sh` profiles the FUSE daemon during a workload. Main knobs: `WORKLOAD=untar_compile|untar_only|custom`, `PERF_CALLGRAPH=fp|dwarf`, `DISABLE_PERF=1`, and `EXTRA_OSAGEFS_ARGS`. It writes timings, flush analysis, and optional perf reports to `RESULTS_DIR`.
- Use `PERF_CALLGRAPH=fp` by default to avoid oversized `perf.data` files.
- Pre-commit hook: `.githooks/pre-commit` and enable with `git config core.hooksPath .githooks`.

## xfstests Notes
- In Sprite xfstests, use `FUSE_SUBTYP`, not `FUSE_SUBTYPE`.
- Install `/sbin/mount.fuse.osagefs` and use distinct `--fuse-fsname` values for TEST and SCRATCH.
- Pass CLI options before test names.
- Set `TEST_FS_MOUNT_OPTS` and `MOUNT_OPTIONS` with `-o` included. Recommended: `-o source=/tmp/osagefs-test-store,allow_other,default_permissions`.
- Keep `.github/workflows/xfstests.yml` and `scripts/sprite_validate_parallel.sh` mount-option formatting in sync. Drift here reintroduces `mount: bad usage` in `generic/084` and cascades into `generic/088`.
- In shared sprites, clear and recreate `/tmp/osagefs-{test,scratch}-{mnt,store}` and ensure writable permissions before running `./check`; cross-user residue can break mounts.
- `scripts/common.sh` provides `osage_assert_welcome_file` for mount validation. Tune with `MOUNT_CHECK_TIMEOUT_SEC`.

## Common Issues And Fixes
- FUSE `allow_other`: ensure `user_allow_other` exists in `/etc/fuse.conf`. CLI flag: `--allow-other`. Scripts: `ALLOW_OTHER=1`.
- Root-owned leftovers: unmount and `sudo rm -rf` mount and store directories before rerunning.
- `scripts/stress_e2e.sh` cleans up on exit and removes logs; copy logs first if needed.
- `scripts/micro_workflows.sh` preserves a caller-provided `PERF_LOG_PATH` on exit; perf logging is opt-in.
- Troubleshooting policy: reproduce with a failing automated test in `src/fs/tests/mod.rs` before fixing.
- Sprite quirk: `fusermount -u` may fail without `/etc/mtab`; fall back to `umount -l`.
- When shutting down a daemon in a sprite, `cd /tmp` first to avoid stale-mount errors.
- Sprite xfstests may print `System has not been booted with systemd...` from `systemd-run`; treat it as noise unless `./check` fails or exits non-zero.
- Linux kernel build dependencies in sprites: `flex`, `bison`, `libelf-dev`, `dwarves`.
- `scripts/linux_kernel_perf.sh` fails fast on stale or inaccessible mounts and on an already-running daemon.
- `scripts/checkpoint.sh` process guards can false-positive match; run checkpoint phases in separate shells.
- Checkpoints snapshot only the superblock pointer, not data objects.
- EIO during builds usually means daemon liveness or mount health is broken; verify those before debugging the data path.

## Useful Commands
- Clean: `fusermount -u /tmp/osagefs-mnt; sudo rm -rf /tmp/osagefs-mnt /tmp/osagefs-store ~/.osagefs_state.bin`
- Perf test: `LOG_FILE=$HOME/linux_build_timings.log ./scripts/linux_kernel_perf.sh`
- Fio sweep: `RESULTS_DIR=/work/osagefs/fio-results LOG_FILE=/work/osagefs/osagefs.log PERF_LOG_PATH=/work/osagefs/osagefs-perf.jsonl ./scripts/fio_workloads.sh`
- Analyze perf: `./scripts/analyze_perf_log.py --log perf-log.jsonl --event flush_pending --top 10`
- Replay capture: `REPLAY_LOG_PATH=/work/osagefs/replay.jsonl.gz ./scripts/run_osagefs.sh`
- Direct replay: `cargo run --release --bin osagefs_replay -- --trace-path replay.jsonl.gz --store-path /tmp/osagefs-replay-store --local-cache-path /tmp/osagefs-replay-cache --state-path /tmp/osagefs-replay-state.bin --layer fuse --speed 1.0`
- Checkpoint create or restore: `cargo run --bin osagefs_checkpoint -- create/restore --store-path ... --checkpoint-path ...`
- NFS export: `cargo run --manifest-path osagefs-nfs-gateway/Cargo.toml -- --store-path /tmp/osagefs-store --listen 0.0.0.0:2049`
