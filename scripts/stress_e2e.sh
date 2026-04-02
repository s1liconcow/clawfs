#!/usr/bin/env bash
set -eEuo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

PID_FILE="${PID_FILE:-/tmp/clawfs-stress.pid}"
OSAGE_BIN="${OSAGE_BIN:-$ROOT_DIR/scripts/run_clawfs.sh}"
CLEANUP_SCRIPT="$ROOT_DIR/scripts/cleanup.sh"
MOUNT_CHECK_TIMEOUT_SEC="${MOUNT_CHECK_TIMEOUT_SEC:-10}"
REF_DIR=""
METADATA_REF_DIR=""
METADATA_ROUNDS="${METADATA_ROUNDS:-2}"
SHARED_STATE_MULTI_CLIENT="${SHARED_STATE_MULTI_CLIENT:-0}"
CLIENT_B_PID_FILE="${CLIENT_B_PID_FILE:-/tmp/clawfs-stress-b.pid}"
CLIENT_B_MOUNT_PATH="${CLIENT_B_MOUNT_PATH:-/tmp/clawfs-mnt-b}"
CLIENT_B_LOCAL_CACHE_PATH="${CLIENT_B_LOCAL_CACHE_PATH:-$HOME/.clawfs/cache-b}"
CLIENT_B_STATE_PATH="${CLIENT_B_STATE_PATH:-$HOME/.clawfs/state/client_state_b.bin}"
CLIENT_B_LOG_FILE="${CLIENT_B_LOG_FILE:-$ROOT_DIR/clawfs-b.log}"
CLIENT_B_PERF_LOG_PATH="${CLIENT_B_PERF_LOG_PATH:-}"
TAIL_PID=""
MULTI_CLIENT_CLAWFS_EXTRA_ARGS="${MULTI_CLIENT_CLAWFS_EXTRA_ARGS:---lookup-cache-ttl-ms 0 --dir-cache-ttl-ms 0 --metadata-poll-interval-ms 250 --entry-ttl-secs 0 --flush-interval-ms 250 --fsync-on-close}"

run_cleanup_script() {
  LOG_FILE="$LOG_FILE" \
  PERF_LOG_PATH="$PERF_LOG_PATH" \
  MOUNT_PATH="$MOUNT_PATH" \
  STORE_PATH="$STORE_PATH" \
  LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" \
  STATE_PATH="$STATE_PATH" \
  "$CLEANUP_SCRIPT"
}

cleanup_client_b_artifacts() {
  if [[ -f "$CLIENT_B_PID_FILE" ]]; then
    local client_b_pid=""
    client_b_pid=$(cat "$CLIENT_B_PID_FILE" 2>/dev/null || true)
    if [[ -n "$client_b_pid" ]] && kill -0 "$client_b_pid" 2>/dev/null; then
      kill "$client_b_pid" 2>/dev/null || true
      wait "$client_b_pid" 2>/dev/null || true
    fi
    rm -f "$CLIENT_B_PID_FILE"
  fi
  if mountpoint -q "$CLIENT_B_MOUNT_PATH" 2>/dev/null; then
    fusermount -u "$CLIENT_B_MOUNT_PATH" 2>/dev/null \
      || umount "$CLIENT_B_MOUNT_PATH" 2>/dev/null \
      || umount -l "$CLIENT_B_MOUNT_PATH" 2>/dev/null \
      || true
  fi
  if [[ -d "$CLIENT_B_LOCAL_CACHE_PATH" ]]; then
    rm -rf "$CLIENT_B_LOCAL_CACHE_PATH"
  fi
  if [[ -f "$CLIENT_B_STATE_PATH" ]]; then
    rm -f "$CLIENT_B_STATE_PATH"
  fi
  if [[ -d "$CLIENT_B_MOUNT_PATH" ]]; then
    rm -rf "$CLIENT_B_MOUNT_PATH"
  fi
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

wait_for_condition() {
  local timeout_sec=$1
  shift
  local start=$SECONDS
  while (( SECONDS - start < timeout_sec )); do
    if "$@"; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

assert_file_contains_line() {
  local path=$1 line=$2
  stat "$path" >/dev/null 2>&1 || return 1
  grep -Fxq "$line" "$path"
}

cleanup() {
  set +e
  if [[ -n "${TAIL_PID:-}" ]] && kill -0 "$TAIL_PID" 2>/dev/null; then
    kill "$TAIL_PID" 2>/dev/null || true
    wait "$TAIL_PID" 2>/dev/null || true
  fi
  if [[ -n "${REF_DIR:-}" && -d "$REF_DIR" ]]; then
    rm -rf "$REF_DIR"
  fi
  if [[ -n "${METADATA_REF_DIR:-}" && -d "$METADATA_REF_DIR" ]]; then
    rm -rf "$METADATA_REF_DIR"
  fi
  if [[ -f "$CLIENT_B_PID_FILE" ]]; then
    CLIENT_B_PID=$(cat "$CLIENT_B_PID_FILE")
    if kill -0 "$CLIENT_B_PID" 2>/dev/null; then
      echo "Stopping clawfs client B PID $CLIENT_B_PID"
      kill "$CLIENT_B_PID"
      wait "$CLIENT_B_PID" 2>/dev/null || true
    fi
    rm -f "$CLIENT_B_PID_FILE"
  fi
  cleanup_client_b_artifacts >/dev/null 2>&1 || true
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

start_daemon() {
  local extra_args="${CLAWFS_EXTRA_ARGS:-}"
  if osage_is_true "$SHARED_STATE_MULTI_CLIENT"; then
    extra_args="${extra_args:+$extra_args }$MULTI_CLIENT_CLAWFS_EXTRA_ARGS"
  fi
  LOG_FILE="$LOG_FILE" PERF_LOG_PATH="$PERF_LOG_PATH" PID_FILE="$PID_FILE" MOUNT_PATH="$MOUNT_PATH" STORE_PATH="$STORE_PATH" LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" STATE_PATH="$STATE_PATH" CLAWFS_EXTRA_ARGS="$extra_args" "$OSAGE_BIN"
  sleep 2
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
}

start_daemon_client_b() {
  local extra_args="${CLAWFS_EXTRA_ARGS:-}"
  extra_args="${extra_args:+$extra_args }$MULTI_CLIENT_CLAWFS_EXTRA_ARGS"
  LOG_FILE="$CLIENT_B_LOG_FILE" \
  PERF_LOG_PATH="$CLIENT_B_PERF_LOG_PATH" \
  PID_FILE="$CLIENT_B_PID_FILE" \
  MOUNT_PATH="$CLIENT_B_MOUNT_PATH" \
  STORE_PATH="$STORE_PATH" \
  LOCAL_CACHE_PATH="$CLIENT_B_LOCAL_CACHE_PATH" \
  STATE_PATH="$CLIENT_B_STATE_PATH" \
  CLAWFS_EXTRA_ARGS="$extra_args" \
  "$OSAGE_BIN"
  sleep 2
  if [[ -s "$CLIENT_B_PID_FILE" ]]; then
    CLIENT_B_PID=$(cat "$CLIENT_B_PID_FILE")
    if ! kill -0 "$CLIENT_B_PID" 2>/dev/null; then
      echo "clawfs client B PID $CLIENT_B_PID not running" >&2
      exit 1
    fi
  else
    echo "Warning: client B PID file $CLIENT_B_PID_FILE missing." >&2
  fi
  osage_assert_welcome_file "$CLIENT_B_MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"
}

run_multi_client_smoke() {
  local shared_dir_rel="home/${USER:-$(whoami)}/multi-client-smoke"
  local dir_a="$MOUNT_PATH/$shared_dir_rel"
  local dir_b="$CLIENT_B_MOUNT_PATH/$shared_dir_rel"
  local file_a="$dir_a/shared.log"
  local file_b="$dir_b/shared.log"
  local tail_capture
  tail_capture=$(mktemp /tmp/clawfs-tail-b.XXXXXX)

  mkdir -p "$dir_a"
  if ! wait_for_condition 10 test -d "$dir_b"; then
    echo "client B did not observe directory creation from client A" >&2
    exit 1
  fi

  printf "writer-a:hello\n" > "$file_a"
  sync
  if ! wait_for_condition 10 assert_file_contains_line "$file_b" "writer-a:hello"; then
    echo "client B did not observe initial file content from client A" >&2
    exit 1
  fi

  stdbuf -oL tail -n 0 -f "$file_b" >"$tail_capture" &
  TAIL_PID=$!
  sleep 1

  printf "writer-a:tail-1\n" >> "$file_a"
  sync
  if ! wait_for_condition 10 assert_file_contains_line "$tail_capture" "writer-a:tail-1"; then
    echo "client B tail -f did not observe append from client A" >&2
    echo "--- tail capture ---" >&2
    cat "$tail_capture" >&2 || true
    exit 1
  fi

  printf "writer-b:append-1\n" >> "$file_b"
  sync
  if ! wait_for_condition 10 assert_file_contains_line "$file_a" "writer-b:append-1"; then
    echo "client A did not observe append from client B" >&2
    exit 1
  fi

  if ! wait_for_condition 10 assert_file_contains_line "$tail_capture" "writer-b:append-1"; then
    echo "client B tail output did not include its local append" >&2
    exit 1
  fi

  local expected=$'writer-a:hello\nwriter-a:tail-1\nwriter-b:append-1'
  assert_eq "$(cat "$file_a")" "$expected" "client A final shared log contents mismatch"
  assert_eq "$(cat "$file_b")" "$expected" "client B final shared log contents mismatch"

  kill "$TAIL_PID" 2>/dev/null || true
  wait "$TAIL_PID" 2>/dev/null || true
  TAIL_PID=""
  rm -f "$tail_capture"
  echo "Two-client smoke test completed successfully."
}

stop_daemon_preserve_store() {
  set +e
  local pid=""
  if [[ -f "$PID_FILE" ]]; then
    pid=$(cat "$PID_FILE")
  fi

  if mountpoint -q "$MOUNT_PATH" 2>/dev/null; then
    fusermount -u "$MOUNT_PATH" 2>/dev/null || umount "$MOUNT_PATH" 2>/dev/null || true
  fi

  if [[ -f "$PID_FILE" ]]; then
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "Waiting for clawfs PID $pid to exit after unmount"
      for _ in $(seq 1 50); do
        if ! kill -0 "$pid" 2>/dev/null; then
          break
        fi
        sleep 0.1
      done
    fi
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "Stopping clawfs PID $pid for restart"
      kill "$pid"
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  mkdir -p "$MOUNT_PATH"
  set -e
}

run_metadata_consistency_workload() {
  local workdir="$MOUNT_PATH/home/${USER:-$(whoami)}/metadata-consistency"
  METADATA_REF_DIR=$(mktemp -d /tmp/clawfs-metadata-ref.XXXXXX)

  python3 "$ROOT_DIR/scripts/metadata_consistency_e2e.py" \
    --mount-base "$workdir" \
    --ref-base "$METADATA_REF_DIR" \
    --rounds "$METADATA_ROUNDS" \
    --mode run

  sync
  ensure_daemon_alive "metadata-consistency-run"

  stop_daemon_preserve_store
  start_daemon

  python3 "$ROOT_DIR/scripts/metadata_consistency_e2e.py" \
    --mount-base "$workdir" \
    --ref-base "$METADATA_REF_DIR" \
    --rounds "$METADATA_ROUNDS" \
    --mode verify

  diff -ruN "$METADATA_REF_DIR" "$workdir"
  echo "Metadata consistency E2E completed successfully."
}

# Ensure clean slate unless user wants to keep existing mount/store
if [[ -z "${SKIP_CLEANUP:-}" ]]; then
  run_cleanup_script >/dev/null 2>&1 || true
  cleanup_client_b_artifacts >/dev/null 2>&1 || true
fi

mkdir -p "$ROOT_DIR"
if [[ -z "${SKIP_OSAGE:-}" ]]; then
  start_daemon
else
  echo "SKIP_OSAGE set; assuming daemon already running and PID_FILE populated."
  osage_assert_welcome_file "$MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"
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

if osage_is_true "$SHARED_STATE_MULTI_CLIENT"; then
  start_daemon_client_b
  run_multi_client_smoke
fi

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
ensure_daemon_alive "post-stress-workload"

run_metadata_consistency_workload

echo "Stress test completed successfully."
