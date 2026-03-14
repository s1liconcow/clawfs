#!/usr/bin/env bash
set -eEuo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

PID_FILE="${PID_FILE:-/tmp/clawfs-stress.pid}"
OSAGE_BIN="${OSAGE_BIN:-$ROOT_DIR/scripts/run_clawfs.sh}"
CLEANUP_SCRIPT="$ROOT_DIR/scripts/cleanup.sh"
MOUNT_CHECK_TIMEOUT_SEC="${MOUNT_CHECK_TIMEOUT_SEC:-10}"
REF_DIR=""

run_cleanup_script() {
  LOG_FILE="$LOG_FILE" \
  PERF_LOG_PATH="$PERF_LOG_PATH" \
  MOUNT_PATH="$MOUNT_PATH" \
  STORE_PATH="$STORE_PATH" \
  LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" \
  STATE_PATH="$STATE_PATH" \
  "$CLEANUP_SCRIPT"
}

write_payload() {
  local file=$1 size=$2
  python3 - "$file" "$size" <<'PY'
import sys
path = sys.argv[1]
size = int(sys.argv[2])
with open(path, "wb") as f:
    remaining = size
    value = 0
    while remaining > 0:
        span = min(4096, remaining)
        data = bytes(((value + i) & 0xFF) for i in range(span))
        f.write(data)
        remaining -= span
        value = (value + span) & 0xFF
PY
}

ensure_daemon_alive() {
  local phase=$1
  if [[ ! -f "$PID_FILE" ]]; then
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

on_error() {
  local code=$1 line=$2
  echo "Stress test failed at line $line (exit $code)" >&2
}
trap 'on_error $? $LINENO' ERR

assert_eq() {
  local lhs=$1 rhs=$2 msg=${3:-"expected $lhs == $rhs"}
  if [[ "$lhs" != "$rhs" ]]; then
    echo "[FAIL] $msg" >&2
    exit 1
  fi
}

cleanup() {
  set +e
  if [[ -n "${REF_DIR:-}" && -d "$REF_DIR" ]]; then
    rm -rf "$REF_DIR"
  fi
  if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
      echo "Stopping clawfs PID $PID"
      kill "$PID"
      wait "$PID" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  run_cleanup_script >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Ensure clean slate unless user wants to keep existing mount/store
if [[ -z "${SKIP_CLEANUP:-}" ]]; then
  run_cleanup_script >/dev/null 2>&1 || true
fi

mkdir -p "$ROOT_DIR"
if [[ -z "${SKIP_OSAGE:-}" ]]; then
  LOG_FILE="$LOG_FILE" PERF_LOG_PATH="$PERF_LOG_PATH" PID_FILE="$PID_FILE" MOUNT_PATH="$MOUNT_PATH" STORE_PATH="$STORE_PATH" LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" STATE_PATH="$STATE_PATH" "$OSAGE_BIN"
  sleep 2
else
  echo "SKIP_OSAGE set; assuming daemon already running and PID_FILE populated."
fi

if [[ -s "$PID_FILE" ]]; then
  PID=$(cat "$PID_FILE")
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "clawfs PID $PID not running" >&2
    exit 1
  fi
else
  echo "Warning: PID file $PID_FILE missing; continue if you started clawfs manually."
fi

osage_assert_welcome_file "$MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"

run_workload() {
  local dir=$1
  local interactive=${2:-0}
  rm -rf "$dir"
  mkdir -p "$dir"
  pushd "$dir" >/dev/null

  # 1. Single file create/read
  printf "alpha" > single.txt
  cmp <(printf "alpha") single.txt

  # 2. Burst of 1000 files
  for i in $(seq 1 1000); do
    printf "file-%04d" "$i" > "bulk_$i.txt"
  done
  local count
  count=$(
    cd / &&
      find "$dir" -maxdepth 1 -type f -print | wc -l
  )
  assert_eq "$count" 1001 "expected 1001 files after bulk create in $dir"
  #ensure_daemon_alive "post-bulk-create"

  echo "Small file test successful"

  echo "Checking varied sized payloads.."

  # 3. Varied size payloads (deterministic contents so diff works)
  write_payload small.bin 512
  write_payload medium.bin 65536
  write_payload large.bin 2097152
  stat -c %s small.bin | grep -qx 512
  stat -c %s medium.bin | grep -qx 65536
  stat -c %s large.bin | grep -qx 2097152

  # 4. Offset writes
  truncate -s 4096 offsets.bin
  echo -n START | dd of=offsets.bin bs=1 seek=0 conv=notrunc status=none
  echo -n MIDDLE | dd of=offsets.bin bs=1 seek=2048 conv=notrunc status=none
  echo -n TAIL | dd of=offsets.bin bs=1 seek=4096 conv=notrunc status=none
  [[ $(dd if=offsets.bin bs=5 count=1 status=none) == START ]]
  [[ $(dd if=offsets.bin bs=1 skip=2048 count=6 status=none) == MIDDLE ]]
  [[ $(tail -c 4 offsets.bin) == TAIL ]]

  # 5. Attribute changes
  cp single.txt attrs.txt
  chmod 700 attrs.txt
  chown $(id -u):$(id -g) attrs.txt
  local mode
  mode=$(stat -c %a attrs.txt)
  assert_eq "$mode" 700 "chmod mismatch in $dir"

  popd >/dev/null
}

WORKDIR="$MOUNT_PATH/home/${USER:-$(whoami)}/stress"
run_workload "$WORKDIR" 1

REF_DIR=$(mktemp -d /tmp/clawfs-stress-ref.XXXXXX)
run_workload "$REF_DIR" 0

diff -ruN "$REF_DIR" "$WORKDIR"

sync

echo "Stress test completed successfully."
