#!/usr/bin/env bash
set -eEuo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

MOUNT_PATH="${MOUNT_PATH:-/tmp/clawfs-mnt}"
STORE_PATH="${STORE_PATH:-/tmp/clawfs-store}"
LOCAL_CACHE_PATH="${LOCAL_CACHE_PATH:-/tmp/clawfs-cache}"
STATE_PATH="${STATE_PATH:-/tmp/clawfs-state.bin}"
PID_FILE="${PID_FILE:-/tmp/clawfs.pid}"
LOG_FILE="${LOG_FILE:-/tmp/clawfs-mount.log}"
STRESS_LOG="${STRESS_LOG:-/tmp/clawfs-stress.out}"
BUILD_RELEASE="${BUILD_RELEASE:-1}"
MOUNT_WAIT_SEC="${MOUNT_WAIT_SEC:-30}"
WORKLOAD_TIMEOUT_SEC="${WORKLOAD_TIMEOUT_SEC:-90}"
STRESS_TIMEOUT_SEC="${STRESS_TIMEOUT_SEC:-60}"
KILL_AFTER_SEC="${KILL_AFTER_SEC:-10}"

cleanup() {
  set +e
  cd /tmp || true
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null || true)
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "Stopping clawfsd PID $pid"
      kill "$pid" 2>/dev/null || true
      sleep 1
    fi
    rm -f "$PID_FILE"
  fi
  sudo umount -l "$MOUNT_PATH" 2>/dev/null || fusermount -u "$MOUNT_PATH" 2>/dev/null || true
}
trap cleanup EXIT

osage_require_cmd stress-ng
osage_require_cmd mountpoint
osage_require_cmd timeout

if osage_is_true "$BUILD_RELEASE"; then
  osage_ensure_release_binary
fi

if ! grep -qx 'user_allow_other' /etc/fuse.conf 2>/dev/null; then
  echo "Missing user_allow_other in /etc/fuse.conf" >&2
  echo "Add it first or rerun with enough privileges to update /etc/fuse.conf." >&2
  exit 1
fi

rm -rf "$MOUNT_PATH" "$STORE_PATH" "$LOCAL_CACHE_PATH" "$STATE_PATH" "$LOG_FILE" "$STRESS_LOG"
mkdir -p "$MOUNT_PATH" "$STORE_PATH" "$LOCAL_CACHE_PATH"

echo "Starting clawfsd..."
"$ROOT_DIR/target/release/clawfsd" \
  --mount-path "$MOUNT_PATH" \
  --store-path "$STORE_PATH" \
  --local-cache-path "$LOCAL_CACHE_PATH" \
  --state-path "$STATE_PATH" \
  --object-provider local \
  --log-file "$LOG_FILE" \
  --allow-other \
  --disable-journal \
  --disable-cleanup \
  --foreground >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"

echo "Waiting for mount at $MOUNT_PATH..."
for _ in $(seq 1 "$MOUNT_WAIT_SEC"); do
  if mountpoint -q "$MOUNT_PATH" 2>/dev/null; then
    break
  fi
  sleep 1
done

if ! mountpoint -q "$MOUNT_PATH" 2>/dev/null; then
  echo "ClawFS failed to mount" >&2
  tail -n 200 "$LOG_FILE" >&2 || true
  exit 1
fi

echo "Mounted. Running stress-ng..."
cd "$MOUNT_PATH"
set +e
timeout -k "${KILL_AFTER_SEC}s" "${WORKLOAD_TIMEOUT_SEC}s" \
  stress-ng \
    --access 1 \
    --chdir 1 \
    --chmod 1 \
    --chown 1 \
    --dentry 1 \
    --dir 1 \
    --dirdeep 1 \
    --dirmany 1 \
    --dup 1 \
    --eventfd 1 \
    --fd-fork 1 \
    --filename 1 \
    --fstat 1 \
    --getdent 1 \
    --link 1 \
    --metamix 1 \
    --open 1 \
    --rename 1 \
    --symlink 1 \
    --touch 1 \
    --utime 1 \
    --timeout "${STRESS_TIMEOUT_SEC}s" 2>&1 | tee "$STRESS_LOG"
status=${PIPESTATUS[0]}
set -e

echo
echo "stress-ng exit status: $status"
echo "Mount log: $LOG_FILE"
echo "Stress log: $STRESS_LOG"
echo
echo "--- tail $LOG_FILE ---"
tail -n 200 "$LOG_FILE" || true

exit "$status"
