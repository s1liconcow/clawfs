#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "$0")/.." && pwd)
OSAGE_BIN="$ROOT_DIR/target/release/osagefs"

MOUNT_PATH="${MOUNT_PATH:-/tmp/osagefs-mnt}"
STORE_PATH="${STORE_PATH:-/tmp/osagefs-store}"
LOCAL_CACHE_PATH="${LOCAL_CACHE_PATH:-$HOME/.osagefs/cache}"
STATE_PATH="${STATE_PATH:-$HOME/.osagefs/state/client_state.bin}"
LOG_FILE="${LOG_FILE:-$ROOT_DIR/osagefs.log}"
PID_FILE="${PID_FILE:-/tmp/osagefs.pid}"
PERF_LOG_PATH="${PERF_LOG_PATH:-$ROOT_DIR/osagefs-perf.jsonl}"
FOREGROUND="${FOREGROUND:-0}"
DISABLE_JOURNAL="${DISABLE_JOURNAL:-1}"
DEBUG_LOG="${DEBUG_LOG:-0}"

is_true() {
  case "$1" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

if [[ ! -x "$OSAGE_BIN" ]]; then
  echo "Building osagefs --release ..."
  (cd "$ROOT_DIR" && cargo build --release)
fi

mkdir -p "$MOUNT_PATH" "$STORE_PATH" "$LOCAL_CACHE_PATH" "$(dirname "$STATE_PATH")"

CMD=(
  "$OSAGE_BIN"
  --mount-path "$MOUNT_PATH"
  --store-path "$STORE_PATH"
  --local-cache-path "$LOCAL_CACHE_PATH"
  --state-path "$STATE_PATH"
  --object-provider local
  --home-prefix /home
  --disable-cleanup
  --log-file "$LOG_FILE"
  --foreground
)

if is_true "$DISABLE_JOURNAL"; then
  CMD+=(--disable-journal)
fi
if is_true "$DEBUG_LOG"; then
  CMD+=(--debug-log)
fi
if [[ -n "$PERF_LOG_PATH" ]]; then
  CMD+=(--perf-log "$PERF_LOG_PATH")
fi

if is_true "$FOREGROUND"; then
  exec "${CMD[@]}"
fi

echo "Starting osagefs in background..."
mkdir -p "$(dirname "$LOG_FILE")"
nohup "${CMD[@]}" >"$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"
echo "osagefs running as PID $PID (log: $LOG_FILE, pid file: $PID_FILE)"
