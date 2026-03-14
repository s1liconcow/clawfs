# ClawFS Sprites

## Goal
Privileged execution must happen inside a Fly.io Sprite VM, not locally.

## Requirements
- `sprite` must be on `PATH`.
- Authenticate with `SPRITES_TOKEN` or `sprite login` before use.

## Hard Rules
- Do not run privileged commands locally.
- Do not create local FUSE or NFS mounts.
- Run privileged commands with `sprite exec -s <sprite> -- bash -lc '<command>'`.
- Prefer idempotent setup and cleanup steps.
- Run `cargo fmt --all --check`, `cargo clippy ...`, and `cargo test` before handoff.

## Bootstrap
```bash
sprite exec -s "$SPRITE_NAME" -- bash -lc 'sudo apt-get update && sudo apt-get install -y python3 fio fuse3 util-linux coreutils findutils procps psmisc tar gzip ripgrep strace'
```

## Naming
```bash
REPO_SLUG="$(basename "$(git rev-parse --show-toplevel)" | tr -cs 'a-zA-Z0-9' '-' | tr '[:upper:]' '[:lower:]' | sed 's/^-*//; s/-*$//')"
BRANCH_SLUG="$(git rev-parse --abbrev-ref HEAD | tr -cs 'a-zA-Z0-9' '-' | tr '[:upper:]' '[:lower:]' | sed 's/^-*//; s/-*$//')"
RAW_BASE="${REPO_SLUG}-${BRANCH_SLUG}"
BASE_HASH="$(printf '%s' "$RAW_BASE" | sha1sum | cut -c1-8)"
SPRITE_BASENAME="$(printf '%s' "$RAW_BASE" | cut -c1-46 | sed 's/-$//')-${BASE_HASH}"
```

For multi-sprite setups, append `-${ROLE_SLUG}-${NAME_HASH}`. Suggested role slugs: `build`, `stress`, `perf-a`, `cleanup`.

## Cleanup
- After task completion, destroy sprites with `sprite destroy --force "$SPRITE_NAME"`.

## Sync Code Into A Sprite
```bash
tar -czf - . | sprite exec -s "$SPRITE_NAME" -- bash -lc 'rm -rf /work/clawfs && mkdir -p /work/clawfs && tar -xzf - -C /work/clawfs'
```

## Common Workflows
- Smoke build: `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cd /work/clawfs && cargo build --release && ./target/release/clawfs --help >/dev/null'`
- E2E: `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cd /work/clawfs && ./scripts/stress_e2e.sh'`
- Fio: `sprite exec -s "$SPRITE_NAME" -- bash -lc 'cd /work/clawfs && RESULTS_DIR=/work/clawfs/fio-results/compact LOG_FILE=/work/clawfs/clawfs.log PERF_LOG_PATH=/work/clawfs/clawfs-perf.jsonl RUNTIME_SEC=5 SEQ_SIZE=64M RAND_SIZE=64M RAND_NUMJOBS=2 RAND_IODEPTH=8 SMALLFILE_COUNT=200 SMALLFILE_NUMJOBS=4 SMALLFILE_SIZE=8k ./scripts/fio_workloads.sh'`

## Troubleshooting
Collect these on failure:

```bash
sprite exec -s "$SPRITE_NAME" -- bash -lc 'tail -n 200 /work/clawfs/clawfs.log || true'
sprite exec -s "$SPRITE_NAME" -- bash -lc 'ps -ef | rg clawfs || true; mount | rg /tmp/clawfs-mnt || true'
sprite exec -s "$SPRITE_NAME" -- bash -lc 'dmesg | tail -200 || true'
```

Fix order for sprite-only bugs:
1. Reproduce in a sprite.
2. Add a regression test in `src/fs/tests/mod.rs`.
3. Implement the fix.
4. Rerun the validation in the sprite.

## Sprite Checkpoints
```bash
scripts/sprite_perf_loop.sh --sprite $SPRITE_NAME --mode cold --create-checkpoint --comment "linux tree prepared"
scripts/sprite_perf_loop.sh --sprite $SPRITE_NAME --restore v2 --mode fast
scripts/sprite_perf_loop.sh --ephemeral --mode fast
```

Add `--keep-sprite` to preserve an ephemeral run for inspection.
