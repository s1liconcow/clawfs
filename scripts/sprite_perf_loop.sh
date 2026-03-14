#!/usr/bin/env bash
set -euo pipefail

SPRITE_NAME="${SPRITE_NAME:-iotest}"
RESTORE_CHECKPOINT="${RESTORE_CHECKPOINT:-}"
CREATE_CHECKPOINT=0
CHECKPOINT_COMMENT="${CHECKPOINT_COMMENT:-}"
SYNC_REPO=1
MODE="${MODE:-fast}"
SPRITE_GC_PREFIX="${SPRITE_GC_PREFIX:-iotest-perf-}"
EPHEMERAL=0
KEEP_SPRITE=0
CLEANUP_STALE=0
CREATED_SPRITE=0
PERF_ARGS=()

usage() {
  cat <<'USAGE'
Usage: scripts/sprite_perf_loop.sh [options] [-- <linux_kernel_perf.sh args...>]

Options:
  --sprite <name>               Sprite name (default: iotest)
  --ephemeral                   Create a temporary sprite name and auto-destroy it on exit
  --keep-sprite                 Do not auto-destroy the ephemeral sprite on exit
  --cleanup-stale               Destroy other sprites matching SPRITE_GC_PREFIX (except active)
  --restore <checkpoint-id>     Restore checkpoint before syncing/running
  --create-checkpoint           Create checkpoint after successful run
  --comment <text>              Comment for created checkpoint
  --mode <fast|cold>            fast => add --reuse_tree, cold => full run (default: fast)
  --no-sync                     Skip tar sync; assume /work/clawfs already updated
  -h, --help                    Show this help

Examples:
  scripts/sprite_perf_loop.sh --restore v2
  scripts/sprite_perf_loop.sh --ephemeral --mode cold
  scripts/sprite_perf_loop.sh --mode cold --create-checkpoint --comment "linux-6.18 extracted"
  scripts/sprite_perf_loop.sh --restore v2 -- --skip_extract
USAGE
}

destroy_sprite() {
  local name="$1"
  echo "Destroying sprite '$name'..."
  sprite destroy --force "$name" >/dev/null
}

cleanup_stale_sprites() {
  local active="$1"
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    [[ "$name" == "$active" ]] && continue
    if [[ "$name" == "$SPRITE_GC_PREFIX"* ]]; then
      destroy_sprite "$name" || true
    fi
  done < <(sprite list 2>/dev/null || true)
}

on_exit() {
  local rc=$?
  set +e
  if [[ $KEEP_SPRITE -eq 0 && $EPHEMERAL -eq 1 ]]; then
    destroy_sprite "$SPRITE_NAME" || true
  fi
  if [[ $CLEANUP_STALE -eq 1 ]]; then
    cleanup_stale_sprites "$SPRITE_NAME" || true
  fi
  exit "$rc"
}

trap on_exit EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sprite)
      SPRITE_NAME="$2"
      shift 2
      ;;
    --ephemeral)
      EPHEMERAL=1
      shift
      ;;
    --keep-sprite)
      KEEP_SPRITE=1
      shift
      ;;
    --cleanup-stale)
      CLEANUP_STALE=1
      shift
      ;;
    --restore)
      RESTORE_CHECKPOINT="$2"
      shift 2
      ;;
    --create-checkpoint)
      CREATE_CHECKPOINT=1
      shift
      ;;
    --comment)
      CHECKPOINT_COMMENT="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --no-sync)
      SYNC_REPO=0
      shift
      ;;
    --)
      shift
      PERF_ARGS+=("$@")
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$MODE" != "fast" && "$MODE" != "cold" ]]; then
  echo "Invalid mode: $MODE (expected fast|cold)" >&2
  exit 1
fi

if [[ $EPHEMERAL -eq 1 ]]; then
  SPRITE_NAME="${SPRITE_GC_PREFIX}$(date +%Y%m%d-%H%M%S)"
  echo "Using ephemeral sprite '$SPRITE_NAME'"
fi

if ! sprite list | rg -qx "$SPRITE_NAME"; then
  echo "Creating sprite '$SPRITE_NAME'..."
  sprite create "$SPRITE_NAME" >/dev/null
  CREATED_SPRITE=1
fi

if [[ -n "$RESTORE_CHECKPOINT" ]]; then
  echo "Restoring sprite '$SPRITE_NAME' from checkpoint '$RESTORE_CHECKPOINT'..."
  sprite restore -s "$SPRITE_NAME" "$RESTORE_CHECKPOINT"
fi

if [[ $SYNC_REPO -eq 1 ]]; then
  echo "Syncing repository to sprite '$SPRITE_NAME'..."
  tar \
    --exclude='.git' \
    --exclude='target' \
    --exclude='clawfs.log' \
    --exclude='clawfs-perf.jsonl' \
    -czf - . \
    | sprite exec -s "$SPRITE_NAME" -- bash -lc 'rm -rf /work/clawfs && mkdir -p /work/clawfs && tar -xzf - -C /work/clawfs'
fi

REMOTE_ARGS=("${PERF_ARGS[@]}")
if [[ "$MODE" == "fast" ]]; then
  REMOTE_ARGS=(--reuse_tree "${REMOTE_ARGS[@]}")
fi

printf -v REMOTE_ARG_STR "%q " "${REMOTE_ARGS[@]}"
REMOTE_CMD="cd /work/clawfs && LOG_FILE=/work/clawfs/linux_build_timings.log PERF_LOG_PATH=/work/clawfs/clawfs-perf.jsonl ./scripts/linux_kernel_perf.sh ${REMOTE_ARG_STR}"

echo "Running perf on sprite '$SPRITE_NAME' (mode=$MODE)..."
sprite exec -s "$SPRITE_NAME" -- bash -lc "$REMOTE_CMD"

if [[ $CREATE_CHECKPOINT -eq 1 ]]; then
  echo "Creating checkpoint on sprite '$SPRITE_NAME'..."
  if [[ -n "$CHECKPOINT_COMMENT" ]]; then
    sprite checkpoint create -s "$SPRITE_NAME" --comment "$CHECKPOINT_COMMENT"
  else
    sprite checkpoint create -s "$SPRITE_NAME"
  fi
fi

echo "Done."
