#!/usr/bin/env bash
set -eEuo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

PID_FILE="${PID_FILE:-/tmp/clawfs-metadata.pid}"
OSAGE_BIN="${OSAGE_BIN:-$ROOT_DIR/scripts/run_clawfs.sh}"
CLEANUP_SCRIPT="$ROOT_DIR/scripts/cleanup.sh"
MOUNT_CHECK_TIMEOUT_SEC="${MOUNT_CHECK_TIMEOUT_SEC:-15}"
WORKDIR="$MOUNT_PATH/home/${USER:-$(whoami)}/metadata-consistency"
REF_DIR="${REF_DIR:-$(mktemp -d /tmp/clawfs-metadata-ref.XXXXXX)}"
ROUNDS="${ROUNDS:-2}"

run_cleanup_script() {
  LOG_FILE="$LOG_FILE" \
  PERF_LOG_PATH="$PERF_LOG_PATH" \
  MOUNT_PATH="$MOUNT_PATH" \
  STORE_PATH="$STORE_PATH" \
  LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" \
  STATE_PATH="$STATE_PATH" \
  "$CLEANUP_SCRIPT"
}

ensure_daemon_alive() {
  local phase=$1
  if [[ ! -s "$PID_FILE" ]]; then
    echo "PID file missing during $phase" >&2
    exit 1
  fi
  local pid
  pid=$(cat "$PID_FILE")
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "clawfs PID $pid not running during $phase" >&2
    exit 1
  fi
}

start_daemon() {
  LOG_FILE="$LOG_FILE" \
  PERF_LOG_PATH="$PERF_LOG_PATH" \
  PID_FILE="$PID_FILE" \
  MOUNT_PATH="$MOUNT_PATH" \
  STORE_PATH="$STORE_PATH" \
  LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" \
  STATE_PATH="$STATE_PATH" \
  "$OSAGE_BIN"
  osage_assert_welcome_file "$MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"
  ensure_daemon_alive "start"
}

stop_daemon_preserve_store() {
  if [[ ! -f "$PID_FILE" ]]; then
    return
  fi
  local pid
  pid=$(cat "$PID_FILE")
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" || true
    wait "$pid" 2>/dev/null || true
  fi
  if mountpoint -q "$MOUNT_PATH" 2>/dev/null; then
    fusermount -u "$MOUNT_PATH" 2>/dev/null || umount "$MOUNT_PATH" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
}

cleanup() {
  set +e
  stop_daemon_preserve_store
  if [[ -d "$REF_DIR" ]]; then
    rm -rf "$REF_DIR"
  fi
  run_cleanup_script >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'echo "Metadata consistency E2E failed at line $LINENO" >&2' ERR

run_cleanup_script >/dev/null 2>&1 || true

start_daemon

python3 "$ROOT_DIR/scripts/metadata_consistency_e2e.py" \
  --mount-base "$WORKDIR" \
  --ref-base "$REF_DIR" \
  --rounds "$ROUNDS" \
  --mode run

sync
ensure_daemon_alive "post-run"

stop_daemon_preserve_store
mkdir -p "$MOUNT_PATH"
start_daemon

python3 "$ROOT_DIR/scripts/metadata_consistency_e2e.py" \
  --mount-base "$WORKDIR" \
  --ref-base "$REF_DIR" \
  --rounds "$ROUNDS" \
  --mode verify

diff -ruN "$REF_DIR" "$WORKDIR"

echo "Metadata consistency E2E completed successfully."
