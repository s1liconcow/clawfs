#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

OSAGE_BIN="$ROOT_DIR/target/release/osagefs"

PID_FILE="${PID_FILE:-/tmp/osagefs.pid}"
FOREGROUND="${FOREGROUND:-0}"
DISABLE_JOURNAL="${DISABLE_JOURNAL:-1}"
DEBUG_LOG="${DEBUG_LOG:-0}"

osage_ensure_release_binary "$OSAGE_BIN"

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

if osage_is_true "$DISABLE_JOURNAL"; then
  CMD+=(--disable-journal)
fi
if osage_is_true "$DEBUG_LOG"; then
  CMD+=(--debug-log)
fi
if [[ -n "$PERF_LOG_PATH" ]]; then
  CMD+=(--perf-log "$PERF_LOG_PATH")
fi

if osage_is_true "$FOREGROUND"; then
  exec "${CMD[@]}"
fi

echo "Starting osagefs in background..."
mkdir -p "$(dirname "$LOG_FILE")"
nohup "${CMD[@]}" >"$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"
echo "osagefs running as PID $PID (log: $LOG_FILE, pid file: $PID_FILE)"
