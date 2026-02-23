#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

OSAGE_BIN="$ROOT_DIR/target/release/osagefs"

PID_FILE="${PID_FILE:-/tmp/osagefs.pid}"
FOREGROUND="${FOREGROUND:-0}"
DISABLE_JOURNAL="${DISABLE_JOURNAL:-1}"
DEBUG_LOG="${DEBUG_LOG:-0}"
MOUNT_CHECK_TIMEOUT_SEC="${MOUNT_CHECK_TIMEOUT_SEC:-10}"
REPLAY_LOG_PATH="${REPLAY_LOG_PATH:-}"
ALLOW_OTHER="${ALLOW_OTHER:-0}"
SEGMENT_COMPRESSION="${SEGMENT_COMPRESSION:-true}"
INLINE_COMPRESSION="${INLINE_COMPRESSION:-true}"
HEAPTRACK="${HEAPTRACK:-0}"
HEAPTRACK_OUTPUT="${HEAPTRACK_OUTPUT:-$ROOT_DIR/heaptrack/heaptrack.osagefs.%p}"
HEAPTRACK_RAW="${HEAPTRACK_RAW:-0}"
HEAPTRACK_EXTRA_ARGS="${HEAPTRACK_EXTRA_ARGS:-}"

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
  --segment-compression "$SEGMENT_COMPRESSION"
  --inline-compression "$INLINE_COMPRESSION"
  --log-storage-io
  --debug-log
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
if [[ -n "$REPLAY_LOG_PATH" ]]; then
  CMD+=(--replay-log "$REPLAY_LOG_PATH")
fi
if osage_is_true "$ALLOW_OTHER"; then
  CMD+=(--allow-other)
fi
# Allow callers to inject additional CLI flags (e.g. --pending-bytes, --flush-interval-ms).
if [[ -n "${OSAGEFS_EXTRA_ARGS:-}" ]]; then
  read -ra _extra <<< "$OSAGEFS_EXTRA_ARGS"
  CMD+=("${_extra[@]}")
fi

LAUNCH_CMD=("${CMD[@]}")
if osage_is_true "$HEAPTRACK"; then
  osage_require_cmd heaptrack
  mkdir -p "$(dirname "$HEAPTRACK_OUTPUT")"
  LAUNCH_CMD=(heaptrack -o "$HEAPTRACK_OUTPUT")
  if osage_is_true "$HEAPTRACK_RAW"; then
    LAUNCH_CMD+=(-r)
  fi
  if [[ -n "$HEAPTRACK_EXTRA_ARGS" ]]; then
    read -ra _heap_extra <<< "$HEAPTRACK_EXTRA_ARGS"
    LAUNCH_CMD+=("${_heap_extra[@]}")
  fi
  LAUNCH_CMD+=("${CMD[@]}")
  echo "Heaptrack enabled (output pattern: $HEAPTRACK_OUTPUT)"
fi

if osage_is_true "$FOREGROUND"; then
  exec "${LAUNCH_CMD[@]}"
fi

echo "Starting osagefs in background..."
mkdir -p "$(dirname "$LOG_FILE")"
nohup "${LAUNCH_CMD[@]}" >"$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"
if ! osage_assert_welcome_file "$MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"; then
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID" >/dev/null 2>&1 || true
    wait "$PID" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
  exit 1
fi
echo "osagefs running as PID $PID (log: $LOG_FILE, pid file: $PID_FILE)"
