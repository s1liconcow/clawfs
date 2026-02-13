#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "$0")/.." && pwd)
TARGET_DIR="$ROOT_DIR/target/release"
OSAGE_BIN="$TARGET_DIR/osagefs"

MOUNT_PATH="${MOUNT_PATH:-/tmp/osagefs-mnt}"
STORE_PATH="${STORE_PATH:-/tmp/osagefs-store}"
STATE_PATH="${STATE_PATH:-$HOME/.osagefs_state.bin}"
PENDING_BYTES="${PENDING_BYTES:-$((64*1024*1024))}"
INLINE_THRESHOLD="${INLINE_THRESHOLD:-1024}"
HOME_PREFIX="${HOME_PREFIX:-/home}"
OBJECT_PROVIDER="${OBJECT_PROVIDER:-local}"
LOG_FILE="${LOG_FILE:-$ROOT_DIR/osagefs.log}"
PID_FILE="${PID_FILE:-/tmp/osagefs.pid}"
PERF_LOG_PATH="${PERF_LOG_PATH:-$ROOT_DIR/osagefs-perf.jsonl}"
#export RUST_LOG="${RUST_LOG:-osagefs=trace,fuser=debug}"

if [[ ! -x "$OSAGE_BIN" ]]; then
  echo "Building osagefs --release ..."
  (cd "$ROOT_DIR" && cargo build --release)
fi

mkdir -p "$MOUNT_PATH" "$STORE_PATH"

CMD=(
  "$OSAGE_BIN"
  --disable-cleanup
  --disable-journal
  --mount-path "$MOUNT_PATH"
  --store-path "$STORE_PATH"
  --state-path "$STATE_PATH"
  --inline-threshold "$INLINE_THRESHOLD"
  --pending-bytes "$PENDING_BYTES"
  --home-prefix "$HOME_PREFIX"
  --object-provider "$OBJECT_PROVIDER"
  --log-file "$LOG_FILE"
  --foreground
)

if [[ -n "$PERF_LOG_PATH" ]]; then
  CMD+=(--perf-log "$PERF_LOG_PATH")
fi

echo "Starting osagefs in background..."
mkdir -p "$(dirname "$LOG_FILE")"
nohup "${CMD[@]}" >"$LOG_FILE" 2>&1 &
PID=$!
echo $PID > "$PID_FILE"
if [[ -n "$PERF_LOG_PATH" ]]; then
  echo "perf log: $PERF_LOG_PATH"
fi
echo "osagefs running as PID $PID (log: $LOG_FILE, pid file: $PID_FILE)"
