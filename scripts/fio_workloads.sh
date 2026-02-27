#!/usr/bin/env bash
set -eEuo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

PID_FILE="${PID_FILE:-/tmp/osagefs-fio.pid}"
OSAGE_BIN="${OSAGE_BIN:-$ROOT_DIR/scripts/run_osagefs.sh}"
CLEANUP_SCRIPT="$ROOT_DIR/scripts/cleanup.sh"

DISABLE_JOURNAL="${DISABLE_JOURNAL:-1}"
DEBUG_LOG="${DEBUG_LOG:--1}"
SKIP_OSAGE="${SKIP_OSAGE:-0}"
SKIP_CLEANUP="${SKIP_CLEANUP:-0}"
KEEP_MOUNT="${KEEP_MOUNT:-0}"
MOUNT_CHECK_TIMEOUT_SEC="${MOUNT_CHECK_TIMEOUT_SEC:-10}"
DIRECT_IO="${DIRECT_IO:-0}"
RUNTIME_SEC="${RUNTIME_SEC:-30}"
SEQ_SIZE="${SEQ_SIZE:-1G}"
RAND_SIZE="${RAND_SIZE:-2G}"
RAND_NUMJOBS="${RAND_NUMJOBS:-4}"
RAND_IODEPTH="${RAND_IODEPTH:-32}"
SMALLFILE_COUNT="${SMALLFILE_COUNT:-4000}"
SMALLFILE_SIZE="${SMALLFILE_SIZE:-16k}"
SMALLFILE_NUMJOBS="${SMALLFILE_NUMJOBS:-8}"
WORKLOADS="${WORKLOADS:-all}"
 # OsageFS tuning for benchmark workloads.
PENDING_BYTES="${PENDING_BYTES:-$((1024*1024*128))}"       # 128 MiB — fewer flushes, fewer extents
FLUSH_INTERVAL_MS="${FLUSH_INTERVAL_MS:-0}"  # Disable for testing
RESULTS_DIR="${RESULTS_DIR:-$ROOT_DIR/fio-results-$(date +%Y%m%d-%H%M%S)}"
HEAPTRACK="${HEAPTRACK:-0}"
HEAPTRACK_OUTPUT="${HEAPTRACK_OUTPUT:-$RESULTS_DIR/heaptrack/heaptrack.osagefs.%p}"
HEAPTRACK_RAW="${HEAPTRACK_RAW:-0}"
HEAPTRACK_EXTRA_ARGS="${HEAPTRACK_EXTRA_ARGS:-}"

WORK_ROOT="$MOUNT_PATH/home/${USER:-$(whoami)}/fio"
RAW_DIR="$RESULTS_DIR/raw"
SUMMARY_MD="$RESULTS_DIR/summary.md"
RUN_STATUS=0
FAILED_WORKLOADS=()

osage_require_cmd fio
osage_require_cmd python3

workload_enabled() {
  local name=$1
  if [[ "$WORKLOADS" == "all" ]]; then
    return 0
  fi
  [[ ",$WORKLOADS," == *",$name,"* ]]
}

cleanup_daemon() {
  set +e
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null)
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid"
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  if ! osage_is_true "$KEEP_MOUNT"; then
    fusermount -u "$MOUNT_PATH" >/dev/null 2>&1 || true
  fi
}

on_exit() {
  local status=$?
  cleanup_daemon
  if [[ "$status" -ne 0 ]]; then
    echo "fio workload run failed with status $status" >&2
  fi
}
trap on_exit EXIT

if ! osage_is_true "$SKIP_CLEANUP"; then
  LOG_FILE="$LOG_FILE" \
  PERF_LOG_PATH="$PERF_LOG_PATH" \
  MOUNT_PATH="$MOUNT_PATH" \
  STORE_PATH="$STORE_PATH" \
  LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" \
  STATE_PATH="$STATE_PATH" \
  "$CLEANUP_SCRIPT" >/dev/null 2>&1 || true
fi

if ! osage_is_true "$SKIP_OSAGE"; then
  LOG_FILE="$LOG_FILE" \
  PERF_LOG_PATH="$PERF_LOG_PATH" \
  PID_FILE="$PID_FILE" \
  MOUNT_PATH="$MOUNT_PATH" \
  STORE_PATH="$STORE_PATH" \
  LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" \
  STATE_PATH="$STATE_PATH" \
  DISABLE_JOURNAL="$DISABLE_JOURNAL" \
  DEBUG_LOG="$DEBUG_LOG" \
  HEAPTRACK="$HEAPTRACK" \
  HEAPTRACK_OUTPUT="$HEAPTRACK_OUTPUT" \
  HEAPTRACK_RAW="$HEAPTRACK_RAW" \
  HEAPTRACK_EXTRA_ARGS="$HEAPTRACK_EXTRA_ARGS" \
  SEGMENT_COMPRESSION=false \
  INLINE_COMPRESSION=false \
  OSAGEFS_EXTRA_ARGS="--pending-bytes $PENDING_BYTES --flush-interval-ms $FLUSH_INTERVAL_MS" \
  "$OSAGE_BIN"
  sleep 2
fi

osage_assert_welcome_file "$MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"

mkdir -p "$WORK_ROOT" "$RAW_DIR"

run_fio() {
  local name=$1
  shift
  local output="$RAW_DIR/${name}.json"
  echo "Running workload: $name"
  local time_args=()
  if [[ "$name" != prefill_* ]]; then
    time_args=(--time_based=1 --runtime="$RUNTIME_SEC")
  fi

  set +e
  fio \
    --name="$name" \
    --thread=1 \
    "${time_args[@]}" \
    --group_reporting=1 \
    --output-format=json \
    --output="$output" \
    "$@"
  local status=$?
  set -e
  if [[ "$status" -ne 0 ]]; then
    echo "Workload failed: $name (exit $status)" >&2
    RUN_STATUS=1
    FAILED_WORKLOADS+=("$name")
  fi
}

fsync_prefill_file() {
  local file_path=$1
  if [[ ! -f "$file_path" ]]; then
    echo "prefill file missing for fsync: $file_path" >&2
    return 1
  fi
  python3 - "$file_path" <<'PY'
import os
import sys

path = sys.argv[1]
fd = os.open(path, os.O_RDONLY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
PY
}

if workload_enabled "seq_read_1m"; then
  run_fio prefill_seq \
    --filename="$WORK_ROOT/seq.bin" \
    --rw=write \
    --bs=1m \
    --size="$SEQ_SIZE" \
    --ioengine=sync \
    --direct="$DIRECT_IO"
fi

if workload_enabled "randread_4k" || workload_enabled "randrw_70r_30w_4k"; then
  run_fio prefill_rand \
    --filename="$WORK_ROOT/rand.bin" \
    --rw=write \
    --bs=4k \
    --size="$RAND_SIZE" \
    --ioengine=libaio \
    --iodepth="$RAND_IODEPTH" \
    --numjobs="$RAND_NUMJOBS" \
    --direct="$DIRECT_IO" 
  if [[ "$RUN_STATUS" -eq 0 ]]; then
    echo "Syncing prefill file before read workload"
    fsync_prefill_file "$WORK_ROOT/rand.bin" || {
      echo "prefill fsync failed for $WORK_ROOT/rand.bin" >&2
      RUN_STATUS=1
      FAILED_WORKLOADS+=("prefill_rand_fsync")
    }
    echo "Synced"
  fi
fi

if workload_enabled "seq_write_1m"; then
  run_fio seq_write_1m \
    --filename="$WORK_ROOT/seq.bin" \
    --rw=write \
    --bs=1m \
    --size="$SEQ_SIZE" \
    --ioengine=sync \
    --direct="$DIRECT_IO"
fi

if workload_enabled "seq_read_1m"; then
  run_fio seq_read_1m \
    --filename="$WORK_ROOT/seq.bin" \
    --rw=read \
    --bs=1m \
    --size="$SEQ_SIZE" \
    --ioengine=sync \
    --direct="$DIRECT_IO"
fi

if workload_enabled "randread_4k"; then
  run_fio randread_4k \
    --filename="$WORK_ROOT/rand.bin" \
    --rw=randread \
    --bs=4k \
    --size="$RAND_SIZE" \
    --ioengine=libaio \
    --iodepth="$RAND_IODEPTH" \
    --numjobs="$RAND_NUMJOBS" \
    --direct="$DIRECT_IO"
fi

if workload_enabled "randwrite_4k"; then
  run_fio randwrite_4k \
    --filename="$WORK_ROOT/rand.bin" \
    --rw=randwrite \
    --bs=4k \
    --size="$RAND_SIZE" \
    --ioengine=libaio \
    --iodepth="$RAND_IODEPTH" \
    --numjobs="$RAND_NUMJOBS" \
    --direct="$DIRECT_IO"
fi

if workload_enabled "randrw_70r_30w_4k"; then
  run_fio randrw_70r_30w_4k \
    --filename="$WORK_ROOT/rand.bin" \
    --rw=randrw \
    --rwmixread=70 \
    --bs=4k \
    --size="$RAND_SIZE" \
    --ioengine=libaio \
    --iodepth="$RAND_IODEPTH" \
    --numjobs="$RAND_NUMJOBS" \
    --direct="$DIRECT_IO"
fi

mkdir -p "$WORK_ROOT/smallfiles"
if workload_enabled "smallfiles_sync"; then
  run_fio smallfiles_sync \
    --directory="$WORK_ROOT/smallfiles" \
    --rw=randwrite \
    --numjobs="$SMALLFILE_NUMJOBS" \
    --nrfiles="$SMALLFILE_COUNT" \
    --filesize="$SMALLFILE_SIZE" \
    --openfiles=128 \
    --file_service_type=random \
    --create_on_open=1 \
    --ioengine=sync \
    --fsync=1 \
    --direct=0
fi

python3 - "$RAW_DIR" "$SUMMARY_MD" <<'PY'
import json
import pathlib
import sys

raw_dir = pathlib.Path(sys.argv[1])
summary_path = pathlib.Path(sys.argv[2])


def aggregate(jobset):
    total_read_bw = 0.0
    total_write_bw = 0.0
    total_read_iops = 0.0
    total_write_iops = 0.0
    total_ios = 0
    weighted_clat_ns = 0.0

    for job in jobset:
        read = job.get("read", {})
        write = job.get("write", {})
        total_read_bw += float(read.get("bw_bytes", 0))
        total_write_bw += float(write.get("bw_bytes", 0))
        total_read_iops += float(read.get("iops", 0))
        total_write_iops += float(write.get("iops", 0))

        read_ios = int(read.get("total_ios", 0) or 0)
        write_ios = int(write.get("total_ios", 0) or 0)
        ios = read_ios + write_ios
        clat = 0.0

        if read_ios > 0:
            clat += float(read.get("clat_ns", {}).get("mean", 0.0)) * read_ios
        if write_ios > 0:
            clat += float(write.get("clat_ns", {}).get("mean", 0.0)) * write_ios

        weighted_clat_ns += clat
        total_ios += ios

    total_bw_mib = (total_read_bw + total_write_bw) / (1024 * 1024)
    avg_clat_ms = (weighted_clat_ns / total_ios / 1e6) if total_ios > 0 else 0.0

    return {
        "bw_mib_s": total_bw_mib,
        "read_iops": total_read_iops,
        "write_iops": total_write_iops,
        "avg_clat_ms": avg_clat_ms,
    }

rows = []
skipped = []
for path in sorted(raw_dir.glob("*.json")):
    if path.stem.startswith("prefill_"):
        continue
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:
        skipped.append(path.stem)
        continue
    rows.append((path.stem, aggregate(payload.get("jobs", []))))

lines = [
    "# FIO Workload Summary",
    "",
    "| Workload | Total BW (MiB/s) | Read IOPS | Write IOPS | Avg Completion Latency (ms) |",
    "|---|---:|---:|---:|---:|",
]

for name, data in rows:
    lines.append(
        f"| {name} | {data['bw_mib_s']:.2f} | {data['read_iops']:.0f} | {data['write_iops']:.0f} | {data['avg_clat_ms']:.3f} |"
    )

if skipped:
    lines.extend(["", "Skipped invalid result files: " + ", ".join(sorted(skipped))])

summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(summary_path)
PY

echo "FIO run complete."
echo "- Raw JSON: $RAW_DIR"
echo "- Summary:  $SUMMARY_MD"
if osage_is_true "$HEAPTRACK"; then
  HEAPTRACK_DIR=$(dirname "$HEAPTRACK_OUTPUT")
  echo "- Heaptrack output pattern: $HEAPTRACK_OUTPUT"
  if [[ -d "$HEAPTRACK_DIR" ]] && compgen -G "$HEAPTRACK_DIR/heaptrack.*" > /dev/null; then
    LATEST_HEAPTRACK=$(ls -1t "$HEAPTRACK_DIR"/heaptrack.* | head -n 1)
    echo "- Heaptrack latest: $LATEST_HEAPTRACK"
    echo "- Heaptrack analyze: heaptrack_print -f \"$LATEST_HEAPTRACK\" -p -a -n 30 -s 10"
  fi
fi
if [[ "${#FAILED_WORKLOADS[@]}" -gt 0 ]]; then
  echo "- Failed workloads: ${FAILED_WORKLOADS[*]}" >&2
fi
exit "$RUN_STATUS"
