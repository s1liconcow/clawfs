#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

NFS_LOG_FILE="${NFS_LOG_FILE:-$PWD/osagefs-nfs-gateway.log}"
NFS_GATEWAY_PID_FILE="${NFS_GATEWAY_PID_FILE:-/tmp/osagefs-nfs-gateway.pid}"

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

mount_fstype() {
  local path="$1"
  findmnt -n -o FSTYPE --target "$path" 2>/dev/null || true
}

run_umount() {
  if command -v timeout >/dev/null 2>&1; then
    timeout 5 "$@"
  else
    "$@"
  fi
}

kill_pidfile_process() {
  local pid_file="$1"
  local label="$2"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "Stopping $label PID $pid ..."
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do
        if ! kill -0 "$pid" 2>/dev/null; then
          break
        fi
        sleep 0.1
      done
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
      fi
    fi
    rm -f "$pid_file"
  fi
}

kill_pidfile_process "$NFS_GATEWAY_PID_FILE" "osagefs-nfs-gateway"
pkill -f osagefs-nfs-gateway 2>/dev/null || true

if is_mounted "$MOUNT_PATH"; then
  echo "Unmounting $MOUNT_PATH ..."
  fstype="$(mount_fstype "$MOUNT_PATH")"
  if [[ "$fstype" == fuse* ]]; then
    if ! run_umount fusermount -u "$MOUNT_PATH" 2>/dev/null; then
      run_umount umount "$MOUNT_PATH" 2>/dev/null \
        || run_umount sudo umount "$MOUNT_PATH" 2>/dev/null \
        || run_umount umount -l "$MOUNT_PATH" 2>/dev/null \
        || run_umount sudo umount -l "$MOUNT_PATH" 2>/dev/null \
        || true
    fi
  else
    run_umount umount "$MOUNT_PATH" 2>/dev/null \
      || run_umount sudo umount "$MOUNT_PATH" 2>/dev/null \
      || run_umount umount -l "$MOUNT_PATH" 2>/dev/null \
      || run_umount sudo umount -l "$MOUNT_PATH" 2>/dev/null \
      || true
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

if [[ -f "$NFS_LOG_FILE" ]]; then
  echo "Removing NFS log files $NFS_LOG_FILE ..."
  rm -f "$NFS_LOG_FILE"
fi

if [[ -f "$PERF_LOG_PATH" ]]; then
  echo "Removing perf log $PERF_LOG_PATH ..."
  rm -f "$PERF_LOG_PATH"
fi

echo "Cleanup complete."
