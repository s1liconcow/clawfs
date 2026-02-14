#!/usr/bin/env bash
set -euo pipefail

MOUNT_PATH="${MOUNT_PATH:-/tmp/osagefs-mnt}"
STORE_PATH="${STORE_PATH:-/tmp/osagefs-store}"
LOCAL_CACHE_PATH="${LOCAL_CACHE_PATH:-/tmp/osagefs-cache}"
STATE_PATH="${STATE_PATH:-$HOME/.osagefs_state.bin}"
LOG_FILE="${LOG_FILE:-$PWD/osagefs.log}"
PERF_LOG_PATH="${PERF_LOG_PATH:-$PWD/osagefs-perf.jsonl}"

is_mounted() {
  local path="$1"
  if mountpoint -q "$path" 2>/dev/null; then
    return 0
  fi
  if findmnt -rn "$path" >/dev/null 2>&1; then
    return 0
  fi
  if mount | grep -q " on ${path//\//\\/} "; then
    return 0
  fi
  return 1
}

if is_mounted "$MOUNT_PATH"; then
  echo "Unmounting $MOUNT_PATH ..."
  if ! fusermount -u "$MOUNT_PATH" 2>/dev/null; then
    fusermount -u "$MOUNT_PATH"
  fi
fi

for dir in "$STORE_PATH" "$LOCAL_CACHE_PATH"; do
  if [[ -d "$dir" ]]; then
    echo "Removing store dir $dir ..."
    rm -rf "$dir"
  fi
done

if [[ -f "$STATE_PATH" ]]; then
  echo "Removing state file $STATE_PATH ..."
  rm -f "$STATE_PATH"
fi

if [[ -d "$MOUNT_PATH" ]]; then
  echo "Removing mount dir $MOUNT_PATH ..."
  rm -rf "$MOUNT_PATH"
fi

if [[ -f "$LOG_FILE" ]]; then
  echo "Removing log file $LOG_FILE ..."
  rm -f "$LOG_FILE"
fi

if [[ -f "$PERF_LOG_PATH" ]]; then
  echo "Removing perf log $PERF_LOG_PATH ..."
  rm -f "$PERF_LOG_PATH"
fi

echo "Cleanup complete."
