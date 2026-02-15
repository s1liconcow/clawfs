#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

usage() {
  cat <<'EOF'
Usage:
  scripts/checkpoint.sh create
  scripts/checkpoint.sh restore

Environment:
  STORE_PATH        Backing store root (default: /tmp/osagefs-store)
  MOUNT_PATH        Mount path to check for active mount (default: /tmp/osagefs-mnt)
  CHECKPOINT_PATH   Checkpoint file path (default: $STORE_PATH/checkpoints/latest.bin)
  SHARD_SIZE        Metadata shard size for store open (default: 2048)
  LOG_STORAGE_IO    1/true to enable metadata backing IO logs during operation
  NOTE              Optional note, used for create only
  FORCE             1/true to skip offline safety checks
EOF
}

MODE="${1:-}"
case "$MODE" in
  create|restore) ;;
  -h|--help|"")
    usage
    exit 0
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    usage >&2
    exit 1
    ;;
esac

CHECKPOINT_PATH="${CHECKPOINT_PATH:-$STORE_PATH/checkpoints/latest.bin}"
SHARD_SIZE="${SHARD_SIZE:-2048}"
LOG_STORAGE_IO="${LOG_STORAGE_IO:-0}"
FORCE="${FORCE:-0}"
CHECKPOINT_BIN="$ROOT_DIR/target/release/osagefs_checkpoint"

if ! osage_is_true "$FORCE"; then
  if command -v mountpoint >/dev/null 2>&1 && mountpoint -q "$MOUNT_PATH"; then
    echo "Refusing to run while mount is active at $MOUNT_PATH" >&2
    echo "Unmount first, or set FORCE=1 to bypass." >&2
    exit 1
  fi

  if ps -ef | rg -q "[o]sagefs.*--store-path[[:space:]]+$STORE_PATH"; then
    echo "Refusing to run while an osagefs process appears to use STORE_PATH=$STORE_PATH" >&2
    echo "Stop osagefs first, or set FORCE=1 to bypass." >&2
    exit 1
  fi
fi

osage_require_cmd cargo
osage_require_cmd rg
osage_ensure_release_checkpoint_binary "$CHECKPOINT_BIN"

mkdir -p "$(dirname "$CHECKPOINT_PATH")"

CMD=(
  "$CHECKPOINT_BIN"
  "$MODE"
  --store-path "$STORE_PATH"
  --checkpoint-path "$CHECKPOINT_PATH"
  --shard-size "$SHARD_SIZE"
)

if osage_is_true "$LOG_STORAGE_IO"; then
  CMD+=(--log-storage-io)
fi

if [[ "$MODE" == "create" && -n "${NOTE:-}" ]]; then
  CMD+=(--note "$NOTE")
fi

echo "Running offline checkpoint command: mode=$MODE store=$STORE_PATH checkpoint=$CHECKPOINT_PATH"
"${CMD[@]}"
