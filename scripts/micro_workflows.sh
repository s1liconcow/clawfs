#!/usr/bin/env bash
set -x
set -eEuo pipefail

PERF_LOG_PATH_FROM_ENV="${PERF_LOG_PATH-}"
PERF_LOG_PATH_WAS_SET=0
if [[ "${PERF_LOG_PATH+x}" == "x" ]]; then
  PERF_LOG_PATH_WAS_SET=1
fi

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

PID_FILE="${PID_FILE:-/tmp/osagefs-micro.pid}"
TRANSPORT="${TRANSPORT:-fuse}" # fuse|nfs
if [[ "$TRANSPORT" == "nfs" ]]; then
  OSAGE_RUNNER="${OSAGE_RUNNER:-$ROOT_DIR/scripts/run_nfs_gateway.sh}"
else
  OSAGE_RUNNER="${OSAGE_RUNNER:-$ROOT_DIR/scripts/run_osagefs.sh}"
fi
CLEANUP_SCRIPT="$ROOT_DIR/scripts/cleanup.sh"
RESULTS_DIR="${RESULTS_DIR:-$ROOT_DIR/micro-results-$(date +%Y%m%d-%H%M%S)}"
LOCAL_BASE="${LOCAL_BASE:-/tmp/osagefs-micro-local}"
OSAGE_BASE_SUBDIR="${OSAGE_BASE_SUBDIR:-home/${USER:-$(whoami)}/micro}"
MOUNT_CHECK_TIMEOUT_SEC="${MOUNT_CHECK_TIMEOUT_SEC:-10}"
MODE="${MODE:-both}" # both|osage|local
WORKFLOW_PROFILE="${WORKFLOW_PROFILE:-quick}" # quick|realistic|all
TEST_FILTER="${TEST_FILTER:-}" # comma-separated test names from TEST_NAMES

# NFS transport knobs
NFS_LISTEN="${NFS_LISTEN:-0.0.0.0:2049}"
NFS_MOUNT_PATH="${NFS_MOUNT_PATH:-$MOUNT_PATH}"
NFS_LOG_FILE="${NFS_LOG_FILE:-$ROOT_DIR/osagefs-nfs-gateway.log}"

# Workload knobs (default to moderate intensity for clearer perf signal)
SMALLFILE_COUNT="${SMALLFILE_COUNT:-5000}"
SMALLFILE_SIZE="${SMALLFILE_SIZE:-2048}"
DEV_SCAN_EDIT_FILES="${DEV_SCAN_EDIT_FILES:-3000}"
DEV_SCAN_RG_PASSES="${DEV_SCAN_RG_PASSES:-80}"
DEV_SCAN_STATUS_PASSES="${DEV_SCAN_STATUS_PASSES:-300}"
DEV_SCAN_TREE_COPIES="${DEV_SCAN_TREE_COPIES:-8}"
DEV_SCAN_MUTATION_ROUNDS="${DEV_SCAN_MUTATION_ROUNDS:-60}"
CHECKPOINT_MB="${CHECKPOINT_MB:-128}"
CHECKPOINT_ITERS="${CHECKPOINT_ITERS:-3}"
ETL_ROWS="${ETL_ROWS:-500000}"
ETL_COLS="${ETL_COLS:-10}"
ETL_PASSES="${ETL_PASSES:-3}"
BUILD_MODE="${BUILD_MODE:-check}" # check|none
DEV_BUILD_TARGET_DIR="${DEV_BUILD_TARGET_DIR:-}" # optional override for cargo target dir
DEV_BUILD_CARGO_JOBS="${DEV_BUILD_CARGO_JOBS:-}" # optional override for cargo parallelism
DEV_BUILD_INCREMENTAL="${DEV_BUILD_INCREMENTAL:-}" # optional override for CARGO_INCREMENTAL
DEV_BUILD_SPRITE_WORKAROUND="${DEV_BUILD_SPRITE_WORKAROUND:-1}" # set 0 to disable auto sprite workaround
ALLOW_PIP_INSTALL="${ALLOW_PIP_INSTALL:-1}"
ALLOW_SYSTEM_INSTALL="${ALLOW_SYSTEM_INSTALL:-1}"

# Realistic workflow knobs
OSS_REPO_URL="${OSS_REPO_URL:-https://github.com/BurntSushi/ripgrep.git}"
OSS_REPO_REF="${OSS_REPO_REF:-}"
OSS_BUILD_CMD="${OSS_BUILD_CMD:-cargo build -q}"
OSS_TEST_CMD="${OSS_TEST_CMD:-cargo test -q --lib}"
DUCKDB_DATASET="${DUCKDB_DATASET:-cancer}" # cancer|nyc_taxi
DUCKDB_CANCER_URL="${DUCKDB_CANCER_URL:-https://raw.githubusercontent.com/selva86/datasets/master/BreastCancer.csv}"
DUCKDB_NYC_URL="${DUCKDB_NYC_URL:-https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet}"
AI_IMAGENETTE_URL="${AI_IMAGENETTE_URL:-https://s3.amazonaws.com/fast-ai-imageclas/imagenette2-160.tgz}"
AI_EPOCHS="${AI_EPOCHS:-1}"
AI_MAX_BATCHES="${AI_MAX_BATCHES:-30}"
AI_BATCH_SIZE="${AI_BATCH_SIZE:-32}"
AI_NUM_WORKERS="${AI_NUM_WORKERS:-2}"
AI_SUBSET_IMAGES="${AI_SUBSET_IMAGES:-4000}"
AI_DATASET_CACHE_DIR="${AI_DATASET_CACHE_DIR:-/tmp/osagefs-imagenette-cache}"

QUICK_TEST_NAMES=(
  dev_smallfile_burst
  dev_scan_and_status
  data_csv_etl
  ai_checkpoint_loop
)

REALISTIC_TEST_NAMES=(
  dev_oss_clone_build_test
  data_duckdb_real_dataset
  ai_imagenette_train
)

TEST_NAMES=()
case "$WORKFLOW_PROFILE" in
  quick) TEST_NAMES=("${QUICK_TEST_NAMES[@]}") ;;
  realistic) TEST_NAMES=("${REALISTIC_TEST_NAMES[@]}") ;;
  all) TEST_NAMES=("${QUICK_TEST_NAMES[@]}" "${REALISTIC_TEST_NAMES[@]}") ;;
  *)
    echo "Invalid WORKFLOW_PROFILE=$WORKFLOW_PROFILE (expected quick|realistic|all)" >&2
    exit 1
    ;;
esac

if [[ -n "$TEST_FILTER" ]]; then
  IFS=',' read -r -a _requested_tests <<<"$TEST_FILTER"
  _filtered_tests=()
  for _candidate in "${TEST_NAMES[@]}"; do
    for _requested in "${_requested_tests[@]}"; do
      if [[ "$_candidate" == "$_requested" ]]; then
        _filtered_tests+=("$_candidate")
        break
      fi
    done
  done
  TEST_NAMES=("${_filtered_tests[@]}")
fi

mkdir -p "$RESULTS_DIR"
RESULTS_JSON="$RESULTS_DIR/results.jsonl"
SUMMARY_MD="$RESULTS_DIR/summary.md"
CASE_SKIP_REASON=""
RUN_FAILURES=0

run_cleanup_script() {
  local cleanup_perf_log="${1:-1}"
  local perf_log_arg="$PERF_LOG_PATH"
  if [[ "$cleanup_perf_log" != "1" ]]; then
    perf_log_arg=""
  fi
  local nfs_pid_arg=""
  local nfs_log_arg=""
  if [[ "$TRANSPORT" == "nfs" ]]; then
    nfs_pid_arg="$PID_FILE"
    nfs_log_arg="$NFS_LOG_FILE"
    # When NFS mount path differs from MOUNT_PATH, unmount it first.
    if [[ "$NFS_MOUNT_PATH" != "$MOUNT_PATH" ]] && mountpoint -q "$NFS_MOUNT_PATH" 2>/dev/null; then
      umount "$NFS_MOUNT_PATH" 2>/dev/null \
        || sudo umount -l "$NFS_MOUNT_PATH" 2>/dev/null \
        || true
    fi
  fi
  NFS_GATEWAY_PID_FILE="$nfs_pid_arg" \
  NFS_LOG_FILE="$nfs_log_arg" \
  LOG_FILE="$LOG_FILE" \
  PERF_LOG_PATH="$perf_log_arg" \
  MOUNT_PATH="$MOUNT_PATH" \
  STORE_PATH="$STORE_PATH" \
  LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" \
  STATE_PATH="$STATE_PATH" \
  "$CLEANUP_SCRIPT"
}

cleanup() {
  set +e
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid=$(cat "$PID_FILE" 2>/dev/null || true)
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  # Unmount NFS before general cleanup when mount paths differ.
  if [[ "$TRANSPORT" == "nfs" && "$NFS_MOUNT_PATH" != "$MOUNT_PATH" ]]; then
    umount "$NFS_MOUNT_PATH" 2>/dev/null \
      || sudo umount -l "$NFS_MOUNT_PATH" 2>/dev/null \
      || true
  fi
  run_cleanup_script 0 >/dev/null 2>&1 || true
}

trap cleanup EXIT

apt_install_packages() {
  local packages=("$@")
  [[ "${#packages[@]}" -eq 0 ]] && return 0
  if [[ "$ALLOW_SYSTEM_INSTALL" != "1" ]]; then
    return 1
  fi

  if command -v sudo >/dev/null 2>&1; then
    sudo env DEBIAN_FRONTEND=noninteractive apt-get update >/dev/null
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "${packages[@]}" >/dev/null
  else
    DEBIAN_FRONTEND=noninteractive apt-get update >/dev/null
    DEBIAN_FRONTEND=noninteractive apt-get install -y "${packages[@]}" >/dev/null
  fi
}

ensure_cmd() {
  local cmd=$1
  shift
  if command -v "$cmd" >/dev/null 2>&1; then
    return 0
  fi
  if apt_install_packages "$@"; then
    command -v "$cmd" >/dev/null 2>&1
    return $?
  fi
  return 1
}

require_tools() {
  ensure_cmd python3 python3 || { echo "Missing dependency: python3" >&2; exit 1; }
  ensure_cmd rg ripgrep || { echo "Missing dependency: rg/ripgrep" >&2; exit 1; }
  ensure_cmd sha256sum coreutils || { echo "Missing dependency: sha256sum/coreutils" >&2; exit 1; }
  ensure_cmd dd coreutils || { echo "Missing dependency: dd/coreutils" >&2; exit 1; }
  ensure_cmd find findutils || { echo "Missing dependency: find/findutils" >&2; exit 1; }
  ensure_cmd git git || { echo "Missing dependency: git" >&2; exit 1; }
  ensure_cmd cargo cargo || { echo "Missing dependency: cargo" >&2; exit 1; }
  ensure_cmd rsync rsync || { echo "Missing dependency: rsync" >&2; exit 1; }
  ensure_cmd curl curl || { echo "Missing dependency: curl" >&2; exit 1; }
  ensure_cmd tar tar || { echo "Missing dependency: tar" >&2; exit 1; }

  if [[ "$WORKFLOW_PROFILE" == "realistic" || "$WORKFLOW_PROFILE" == "all" ]]; then
    ensure_cmd pip3 python3-pip || { echo "Missing dependency: pip3/python3-pip" >&2; exit 1; }
  fi

  # Only required when mounting OsageFS.
  if [[ "$MODE" == "both" || "$MODE" == "osage" ]]; then
    if [[ "$TRANSPORT" == "nfs" ]]; then
      # NFS transport needs mount.nfs (nfs-common on Debian/Ubuntu).
      if ! command -v mount.nfs >/dev/null 2>&1 \
         && ! [[ -x /sbin/mount.nfs ]] \
         && ! [[ -x /usr/sbin/mount.nfs ]]; then
        ensure_cmd mount.nfs nfs-common || { echo "Missing dependency: mount.nfs/nfs-common" >&2; exit 1; }
      fi
    else
      ensure_cmd fusermount3 fuse3 || { echo "Missing dependency: fusermount3/fuse3" >&2; exit 1; }
    fi
  fi
}

start_osage_if_needed() {
  [[ "$MODE" == "local" ]] && return

  run_cleanup_script >/dev/null 2>&1 || true
  if [[ "$TRANSPORT" == "nfs" ]]; then
    LOG_FILE="$NFS_LOG_FILE" PID_FILE="$PID_FILE" \
      MOUNT_PATH="$MOUNT_PATH" STORE_PATH="$STORE_PATH" LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" \
      STATE_PATH="$STATE_PATH" LISTEN="$NFS_LISTEN" NFS_MOUNT_PATH="$NFS_MOUNT_PATH" \
      AUTO_MOUNT_NFS=1 FOREGROUND=0 SKIP_BUILD=0 \
      MOUNT_CHECK_TIMEOUT_SEC="$MOUNT_CHECK_TIMEOUT_SEC" \
      "$OSAGE_RUNNER"
  else
    LOG_FILE="$LOG_FILE" PERF_LOG_PATH="$PERF_LOG_PATH" PID_FILE="$PID_FILE" \
      MOUNT_PATH="$MOUNT_PATH" STORE_PATH="$STORE_PATH" LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" \
      STATE_PATH="$STATE_PATH" "$OSAGE_RUNNER"
    osage_assert_welcome_file "$MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"
  fi
}

emit_result() {
  local fs_type=$1
  local test_name=$2
  local seconds=$3
  local status=$4
  local detail=$5
  python3 - "$RESULTS_JSON" "$fs_type" "$test_name" "$seconds" "$status" "$detail" <<'PY'
import json
import sys
from datetime import datetime, timezone

path, fs_type, test_name, seconds, status, detail = sys.argv[1:]
record = {
    "ts": datetime.now(timezone.utc).isoformat(),
    "fs_type": fs_type,
    "test": test_name,
    "seconds": float(seconds),
    "status": status,
    "detail": detail,
}
with open(path, "a", encoding="utf-8") as f:
    f.write(json.dumps(record, sort_keys=True) + "\n")
PY
}

run_timed_case() {
  local fs_type=$1
  local test_name=$2
  local base_dir=$3
  local elapsed
  local start_ns end_ns

  CASE_SKIP_REASON=""
  start_ns=$(date +%s%N)
  if "$test_name" "$base_dir"; then
    end_ns=$(date +%s%N)
    elapsed=$(python3 - "$start_ns" "$end_ns" <<'PY'
import sys
start_ns, end_ns = map(int, sys.argv[1:])
print(f"{(end_ns - start_ns)/1e9:.6f}")
PY
)
    if [[ -n "$CASE_SKIP_REASON" ]]; then
      emit_result "$fs_type" "$test_name" "$elapsed" "skip" "$CASE_SKIP_REASON"
      printf '[%s] %-24s SKIP %ss (%s)\n' "$fs_type" "$test_name" "$elapsed" "$CASE_SKIP_REASON"
      return 0
    fi
    emit_result "$fs_type" "$test_name" "$elapsed" "ok" ""
    printf '[%s] %-24s ok  %ss\n' "$fs_type" "$test_name" "$elapsed"
  else
    end_ns=$(date +%s%N)
    elapsed=$(python3 - "$start_ns" "$end_ns" <<'PY'
import sys
start_ns, end_ns = map(int, sys.argv[1:])
print(f"{(end_ns - start_ns)/1e9:.6f}")
PY
)
    emit_result "$fs_type" "$test_name" "$elapsed" "fail" "non-zero exit"
    printf '[%s] %-24s FAIL %ss\n' "$fs_type" "$test_name" "$elapsed" >&2
    return 1
  fi
}

mark_skip() {
  CASE_SKIP_REASON="$1"
  return 0
}

dev_smallfile_burst() {
  local root=$1
  local d="$root/dev_smallfiles"
  rm -rf "$d"
  mkdir -p "$d"

  python3 - "$d" "$SMALLFILE_COUNT" "$SMALLFILE_SIZE" <<'PY'
import os
import sys

root = sys.argv[1]
count = int(sys.argv[2])
size = int(sys.argv[3])
payload = (b"abcdefghijklmnopqrstuvwxyz012345" * ((size // 32) + 1))[:size]
for i in range(count):
    p = os.path.join(root, f"file_{i:06d}.txt")
    with open(p, "wb") as f:
      f.write(payload)
PY

  local file_count
  file_count=$(find "$d" -maxdepth 1 -type f | wc -l | tr -d ' ')
  [[ "$file_count" == "$SMALLFILE_COUNT" ]]
}

dev_scan_and_status() {
  local root=$1
  local d="$root/dev_scan"
  rm -rf "$d"
  mkdir -p "$d"

  rsync -a --delete \
    --exclude .git \
    --exclude target \
    --exclude fio-results-* \
    "$ROOT_DIR/src" "$ROOT_DIR/scripts" "$ROOT_DIR/Cargo.toml" "$ROOT_DIR/Cargo.lock" "$d/"

  local copy_idx
  for copy_idx in $(seq 1 "$DEV_SCAN_TREE_COPIES"); do
    cp -a "$d/src" "$d/src_copy_${copy_idx}"
  done

  (
    cd "$d"
    git init -q
    # Retry around transient lock artifacts seen on slower metadata backends.
    for _ in 1 2 3 4 5; do
      rm -f .git/config.lock .git/index.lock
      git config user.email "micro@example.com" && break
      sleep 0.1
    done
    for _ in 1 2 3 4 5; do
      rm -f .git/config.lock .git/index.lock
      git config user.name "micro" && break
      sleep 0.1
    done
    for _ in 1 2 3 4 5; do
      rm -f .git/config.lock .git/index.lock
      git add . && break
      sleep 0.1
    done
    for _ in 1 2 3 4 5; do
      rm -f .git/config.lock .git/index.lock
      git commit -qm "snapshot" && break
      sleep 0.1
    done

    local pass
    for pass in $(seq 1 "$DEV_SCAN_RG_PASSES"); do
      rg "fn |pub struct|impl " src scripts src_copy_* >/dev/null
    done

    python3 - "$DEV_SCAN_EDIT_FILES" <<'PY'
import sys
from pathlib import Path
target_edits = int(sys.argv[1])
count = 0
for p in Path(".").rglob("*.rs"):
    if ".git" in p.parts or "target" in p.parts:
        continue
    txt = p.read_text(encoding="utf-8")
    p.write_text(txt + "\n// micro workflow edit\n", encoding="utf-8")
    count += 1
    if count >= target_edits:
        break
PY

    # Status is metadata heavy and representative of inner-loop developer ops.
    for pass in $(seq 1 "$DEV_SCAN_STATUS_PASSES"); do
      for _ in 1 2 3 4 5; do
        rm -f .git/config.lock .git/index.lock
        git status --porcelain >/dev/null && break
        sleep 0.1
      done
    done

    # Repeated index churn better approximates active dev loops than status alone.
    for pass in $(seq 1 "$DEV_SCAN_MUTATION_ROUNDS"); do
      git add -A
      git diff --cached --name-only >/dev/null
      git reset -q
    done
  )
}

data_csv_etl() {
  local root=$1
  local d="$root/data_etl"
  rm -rf "$d"
  mkdir -p "$d"

  python3 - "$d" "$ETL_ROWS" "$ETL_COLS" "$ETL_PASSES" <<'PY'
import csv
import os
import random
import sys
from collections import defaultdict

root = sys.argv[1]
rows = int(sys.argv[2])
cols = int(sys.argv[3])
passes = int(sys.argv[4])
random.seed(42)
in_csv = os.path.join(root, "input.csv")
out_dir = os.path.join(root, "partitions")
os.makedirs(out_dir, exist_ok=True)

with open(in_csv, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    header = ["bucket"] + [f"c{i}" for i in range(cols)]
    w.writerow(header)
    for i in range(rows):
        bucket = i % 16
        vals = [bucket] + [random.randint(0, 1000) for _ in range(cols)]
        w.writerow(vals)

for p in range(passes):
    agg = defaultdict(int)
    with open(in_csv, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            b = int(row["bucket"])
            agg[b] += int(row["c0"])
            if p > 0:
                agg[b] += int(row["c1"]) % 7

    for b in range(16):
        part = os.path.join(out_dir, f"pass={p:02d}_bucket={b:02d}.csv")
        with open(part, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["bucket", "sum_c0"])
            w.writerow([b, agg[b]])
PY

  [[ -f "$d/partitions/pass=00_bucket=00.csv" ]]
  [[ -f "$d/partitions/pass=$(printf '%02d' $((ETL_PASSES - 1)))_bucket=15.csv" ]]
}

ai_checkpoint_loop() {
  local root=$1
  local d="$root/ai_ckpt"
  rm -rf "$d"
  mkdir -p "$d"

  local bytes=$((CHECKPOINT_MB * 1024 * 1024))
  local i
  for i in $(seq 1 "$CHECKPOINT_ITERS"); do
    dd if=/dev/zero of="$d/checkpoint_${i}.bin" bs=1M count="$CHECKPOINT_MB" conv=fsync status=none
    sha256sum "$d/checkpoint_${i}.bin" > "$d/checkpoint_${i}.sha256"
    dd if="$d/checkpoint_${i}.bin" of=/dev/null bs=1M status=none

    python3 - "$d/train.log" "$i" "$bytes" <<'PY'
import sys
path, step, size = sys.argv[1:]
with open(path, "a", encoding="utf-8") as f:
    f.write(f"step={step} checkpoint_bytes={size}\n")
PY
  done

  [[ -f "$d/checkpoint_${CHECKPOINT_ITERS}.sha256" ]]
}

dev_oss_clone_build_test() {
  local root=$1
  local d="$root/dev_oss"
  local repo_dir="$d/repo"
  rm -rf "$d"
  mkdir -p "$d"

  git clone --depth 1 "$OSS_REPO_URL" "$repo_dir" >/dev/null 2>&1
  (
    cd "$repo_dir"
    if [[ -n "$OSS_REPO_REF" ]]; then
      git fetch --depth 1 origin "$OSS_REPO_REF" >/dev/null 2>&1
      git checkout -q FETCH_HEAD
    fi
    bash -lc "$OSS_BUILD_CMD"
    bash -lc "$OSS_TEST_CMD"
  )
}

data_duckdb_real_dataset() {
  local root=$1
  local d="$root/data_duckdb_real"
  rm -rf "$d"
  mkdir -p "$d"

  python3 - "$d" "$DUCKDB_DATASET" "$DUCKDB_CANCER_URL" "$DUCKDB_NYC_URL" "$ALLOW_PIP_INSTALL" <<'PY'
import json
import os
import subprocess
import sys
import urllib.request
from datetime import date, datetime

out_dir, dataset, cancer_url, nyc_url, allow_pip = sys.argv[1:]
allow_pip = allow_pip == "1"

def ensure_duckdb():
    try:
        import duckdb  # noqa: F401
        return True, ""
    except Exception:
        if not allow_pip:
            return False, "python duckdb module missing (set ALLOW_PIP_INSTALL=1 to auto-install)"
        cmd = [sys.executable, "-m", "pip", "install", "duckdb"]
        subprocess.check_call(cmd)
        import duckdb  # noqa: F401
        return True, ""

ok, reason = ensure_duckdb()
if not ok:
    skip_path = os.path.join(out_dir, "SKIP")
    with open(skip_path, "w", encoding="utf-8") as f:
        f.write(reason + "\n")
    print(f"SKIP:{reason}")
    sys.exit(0)

import duckdb

def json_safe(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    return value
con = duckdb.connect(os.path.join(out_dir, "analysis.duckdb"))

if dataset == "cancer":
    csv_path = os.path.join(out_dir, "cancer.csv")
    urllib.request.urlretrieve(cancer_url, csv_path)
    con.execute("CREATE TABLE cancer AS SELECT * FROM read_csv_auto(?)", [csv_path])
    info = con.execute("PRAGMA table_info('cancer')").fetchall()
    cols = [row[1] for row in info]
    types = {row[1]: str(row[2]).upper() for row in info}
    class_col = next((c for c in ["diagnosis", "Diagnosis", "target", "class"] if c in cols), cols[0])
    numeric_cols = [c for c in cols if any(t in types.get(c, "") for t in ["INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL"])]
    metric_col = numeric_cols[0] if numeric_cols else cols[-1]
    metric_col2 = numeric_cols[1] if len(numeric_cols) > 1 else metric_col
    summary = con.execute(
        f"""
        SELECT "{class_col}" AS klass,
               COUNT(*) AS n,
               AVG(try_cast("{metric_col}" AS DOUBLE)) AS avg_metric1,
               AVG(try_cast("{metric_col2}" AS DOUBLE)) AS avg_metric2
        FROM cancer
        GROUP BY klass
        ORDER BY klass
        """
    ).fetchall()
    top = con.execute(
        f"""
        SELECT "{class_col}" AS klass,
               try_cast("{metric_col}" AS DOUBLE) AS metric1,
               try_cast("{metric_col2}" AS DOUBLE) AS metric2
        FROM cancer
        ORDER BY metric1 DESC NULLS LAST
        LIMIT 20
        """
    ).fetchall()
    payload = {"dataset": dataset, "class_col": class_col, "metric_cols": [metric_col, metric_col2], "summary": summary, "top_metric1": top}
else:
    # Query a real NYC taxi parquet directly from URL; keep output bounded.
    q1 = con.execute(
        """
        SELECT passenger_count,
               COUNT(*) AS trips,
               AVG(total_amount) AS avg_total
        FROM read_parquet(?)
        WHERE trip_distance > 0 AND total_amount > 0
        GROUP BY passenger_count
        ORDER BY trips DESC
        LIMIT 20
        """,
        [nyc_url],
    ).fetchall()
    q2 = con.execute(
        """
        SELECT date_trunc('day', tpep_pickup_datetime) AS day,
               COUNT(*) AS trips,
               AVG(trip_distance) AS avg_dist
        FROM read_parquet(?)
        GROUP BY day
        ORDER BY day
        LIMIT 31
        """,
        [nyc_url],
    ).fetchall()
    payload = {"dataset": dataset, "passenger_mix": q1, "daily": q2}

out_path = os.path.join(out_dir, "duckdb_results.json")
payload = json_safe(payload)
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)
# Validate the artifact is well-formed JSON; fail fast if partially written/corrupt.
with open(out_path, "r", encoding="utf-8") as f:
    json.load(f)
print(out_path)
PY

  if [[ -f "$d/SKIP" ]]; then
    mark_skip "$(cat "$d/SKIP")"
    return 0
  fi
  [[ -s "$d/duckdb_results.json" ]]
  python3 - "$d/duckdb_results.json" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    json.load(f)
PY
}

ai_imagenette_train() {
  local root=$1
  local d="$root/ai_imagenette"
  local tarball="$d/imagenette2-160.tgz"
  local extract_dir="$d/imagenette2-160"
  rm -rf "$d"
  mkdir -p "$d"

  python3 - "$d" "$ALLOW_PIP_INSTALL" <<'PY'
import os
import subprocess
import sys

work_dir, allow_pip = sys.argv[1:]
allow_pip = allow_pip == "1"

def ensure_torch():
    try:
        import torch  # noqa: F401
        import torchvision  # noqa: F401
        return True, ""
    except Exception:
        if not allow_pip:
            return False, "torch/torchvision missing (set ALLOW_PIP_INSTALL=1 to auto-install)"
        subprocess.check_call([
            sys.executable, "-m", "pip", "install",
            "--index-url", "https://download.pytorch.org/whl/cpu",
            "torch", "pillow", "numpy"
        ])
        import torch  # noqa: F401
        return True, ""

ok, reason = ensure_torch()
if not ok:
    with open(os.path.join(work_dir, "SKIP"), "w", encoding="utf-8") as f:
        f.write(reason + "\n")
    print(f"SKIP:{reason}")
PY

  if [[ -f "$d/SKIP" ]]; then
    mark_skip "$(cat "$d/SKIP")"
    return 0
  fi

  local cache_root="$AI_DATASET_CACHE_DIR"
  local cache_tar="$cache_root/imagenette2-160.tgz"
  local cache_extract="$cache_root/imagenette2-160"
  mkdir -p "$cache_root"
  if [[ ! -f "$cache_tar" ]]; then
    curl -L --fail "$AI_IMAGENETTE_URL" -o "$cache_tar"
  fi
  if [[ ! -d "$cache_extract/train" ]]; then
    rm -rf "$cache_extract"
    tar -xzf "$cache_tar" -C "$cache_root"
  fi
  rsync -a --delete "$cache_extract/" "$extract_dir/"

  python3 - \
    "$extract_dir" "$d/metrics.json" "$AI_EPOCHS" "$AI_MAX_BATCHES" "$AI_BATCH_SIZE" "$AI_NUM_WORKERS" "$AI_SUBSET_IMAGES" <<'PY'
import json
import os
from pathlib import Path
import sys

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset

data_dir, out_metrics, epochs, max_batches, batch_size, num_workers, subset_images = sys.argv[1:]
epochs = int(epochs)
max_batches = int(max_batches)
batch_size = int(batch_size)
num_workers = int(num_workers)
subset_images = int(subset_images)

train_dir = f"{data_dir}/train"
val_dir = f"{data_dir}/val"

from PIL import Image
import numpy as np

class SimpleImageFolder(Dataset):
    def __init__(self, root, limit=None):
        root_p = Path(root)
        self.class_names = sorted([p.name for p in root_p.iterdir() if p.is_dir()])
        self.class_to_idx = {c: i for i, c in enumerate(self.class_names)}
        self.samples = []
        for c in self.class_names:
            for img in (root_p / c).rglob("*"):
                if img.suffix.lower() in (".jpeg", ".jpg", ".png"):
                    self.samples.append((img, self.class_to_idx[c]))
        self.samples.sort(key=lambda x: str(x[0]))
        if limit is not None:
            self.samples = self.samples[:limit]

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        path, label = self.samples[idx]
        with Image.open(path) as im:
            im = im.convert("RGB").resize((160, 160))
            arr = np.asarray(im, dtype=np.float32) / 255.0
            arr = np.transpose(arr, (2, 0, 1))
            x = torch.from_numpy(arr)
        y = torch.tensor(label, dtype=torch.long)
        return x, y

train_ds = SimpleImageFolder(train_dir, limit=subset_images)
val_subset_n = min(max(500, subset_images // 5), 2000)
val_ds = SimpleImageFolder(val_dir, limit=val_subset_n)
classes = len(train_ds.class_names)
subset_n = len(train_ds)

loader_train = DataLoader(train_ds, batch_size=batch_size, shuffle=True, num_workers=num_workers)
loader_val = DataLoader(val_ds, batch_size=batch_size, shuffle=False, num_workers=num_workers)

device = "cuda" if torch.cuda.is_available() else "cpu"
# Lightweight ImageNet-style CNN (custom, no torchvision dependency).
model = nn.Sequential(
    nn.Conv2d(3, 32, kernel_size=3, stride=2, padding=1),
    nn.ReLU(inplace=True),
    nn.Conv2d(32, 64, kernel_size=3, stride=2, padding=1),
    nn.ReLU(inplace=True),
    nn.Conv2d(64, 128, kernel_size=3, stride=2, padding=1),
    nn.ReLU(inplace=True),
    nn.AdaptiveAvgPool2d((1, 1)),
    nn.Flatten(),
    nn.Linear(128, classes),
).to(device)

criterion = nn.CrossEntropyLoss()
optimizer = optim.SGD(model.parameters(), lr=0.01, momentum=0.9)

model.train()
seen_batches = 0
final_loss = None
for _ in range(epochs):
    for i, (x, y) in enumerate(loader_train):
        x = x.to(device)
        y = y.to(device)
        optimizer.zero_grad()
        out = model(x)
        loss = criterion(out, y)
        loss.backward()
        optimizer.step()
        final_loss = float(loss.detach().cpu().item())
        seen_batches += 1
        if seen_batches >= max_batches:
            break
    if seen_batches >= max_batches:
        break

model.eval()
correct = 0
total = 0
with torch.no_grad():
    for i, (x, y) in enumerate(loader_val):
        x = x.to(device)
        y = y.to(device)
        out = model(x)
        pred = out.argmax(dim=1)
        correct += int((pred == y).sum().item())
        total += int(y.numel())
        if i >= 20:
            break

acc = (correct / total) if total else 0.0
metrics = {
    "device": device,
    "classes": classes,
    "train_images": subset_n,
    "val_images": total,
    "batches_trained": seen_batches,
    "final_loss": final_loss,
    "val_acc": acc,
}
with open(out_metrics, "w", encoding="utf-8") as f:
    json.dump(metrics, f, indent=2)
print(json.dumps(metrics))
PY

  [[ -f "$d/metrics.json" ]]
}

optional_build_probe() {
  local fs_type=$1
  local base_dir=$2
  [[ "$BUILD_MODE" == "none" ]] && return 0

  local d="$base_dir/dev_build"
  rm -rf "$d"
  mkdir -p "$d"
  rsync -a --delete --exclude target --exclude .git "$ROOT_DIR/" "$d/"

  local target_dir="$DEV_BUILD_TARGET_DIR"
  local jobs="$DEV_BUILD_CARGO_JOBS"
  local incremental="$DEV_BUILD_INCREMENTAL"
  if [[ -z "$target_dir" && "$fs_type" == "osagefs" && "$DEV_BUILD_SPRITE_WORKAROUND" == "1" ]]; then
    # rustc mmap's .rmeta files; FUSE mmap semantics are broken and cause SIGBUS.
    # Redirect build artifacts off the FUSE mount regardless of environment.
    target_dir="/tmp/osagefs-rust-target-shared"
  fi
  if [[ -z "$jobs" && "$target_dir" == /tmp/* ]]; then
    jobs=1
  fi
  if [[ -z "$incremental" && "$target_dir" == /tmp/* ]]; then
    incremental=0
  fi

  local start_ns end_ns elapsed
  start_ns=$(date +%s%N)
  if ! (
    cd "$d" &&
    if [[ -n "$target_dir" ]]; then export CARGO_TARGET_DIR="$target_dir"; fi &&
    if [[ -n "$jobs" ]]; then export CARGO_BUILD_JOBS="$jobs"; fi &&
    if [[ -n "$incremental" ]]; then export CARGO_INCREMENTAL="$incremental"; fi &&
    cargo check -q
  ); then
    end_ns=$(date +%s%N)
    elapsed=$(python3 - "$start_ns" "$end_ns" <<'PY'
import sys
start_ns, end_ns = map(int, sys.argv[1:])
print(f"{(end_ns - start_ns)/1e9:.6f}")
PY
)
    local detail="cargo check failed"
    if [[ -n "$target_dir" ]]; then
      detail="$detail (target_dir=$target_dir)"
    fi
    emit_result "$fs_type" "dev_incremental_build" "$elapsed" "fail" "$detail"
    printf '[%s] %-24s FAIL %ss\n' "$fs_type" "dev_incremental_build" "$elapsed" >&2
    return 1
  fi
  end_ns=$(date +%s%N)
  elapsed=$(python3 - "$start_ns" "$end_ns" <<'PY'
import sys
start_ns, end_ns = map(int, sys.argv[1:])
print(f"{(end_ns - start_ns)/1e9:.6f}")
PY
)
  local detail="cargo check"
  if [[ -n "$target_dir" ]]; then
    detail="$detail (target_dir=$target_dir)"
  fi
  emit_result "$fs_type" "dev_incremental_build" "$elapsed" "ok" "$detail"
  printf '[%s] %-24s ok  %ss\n' "$fs_type" "dev_incremental_build" "$elapsed"
}

run_suite_for_fs() {
  local fs_type=$1
  local base_dir=$2
  mkdir -p "$base_dir"

  local t
  for t in "${TEST_NAMES[@]}"; do
    if ! run_timed_case "$fs_type" "$t" "$base_dir"; then
      RUN_FAILURES=1
    fi
  done
  if ! optional_build_probe "$fs_type" "$base_dir"; then
    RUN_FAILURES=1
  fi
}

write_summary() {
  python3 - "$RESULTS_JSON" "$SUMMARY_MD" <<'PY'
import json
import sys
from collections import defaultdict

results_path, summary_path = sys.argv[1:]
rows = []
with open(results_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            rows.append(json.loads(line))

by_test = defaultdict(dict)
bad = []
for r in rows:
    if r.get("status") == "ok":
        by_test[r["test"]][r["fs_type"]] = r["seconds"]
    else:
        bad.append(r)

lines = []
lines.append("# OsageFS Micro Workflow Results")
lines.append("")
lines.append("| test | osagefs_s | local_s | local/osagefs |")
lines.append("|---|---:|---:|---:|")
for test in sorted(by_test):
    osage = by_test[test].get("osagefs")
    local = by_test[test].get("local")
    if osage is None and local is None:
        continue
    osage_s = f"{osage:.3f}" if osage is not None else "-"
    local_s = f"{local:.3f}" if local is not None else "-"
    ratio = "-"
    if osage is not None and local is not None and osage > 0:
        ratio = f"{(local / osage):.3f}x"
    lines.append(f"| {test} | {osage_s} | {local_s} | {ratio} |")

if bad:
    lines.append("")
    lines.append("## Non-OK Results")
    lines.append("")
    lines.append("| fs | test | status | detail |")
    lines.append("|---|---|---|---|")
    for r in bad:
        lines.append(f"| {r['fs_type']} | {r['test']} | {r['status']} | {r.get('detail','')} |")

with open(summary_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")
print("\n".join(lines))
PY
}

main() {
  require_tools
  rm -f "$RESULTS_JSON"

  start_osage_if_needed

  local effective_mount="$MOUNT_PATH"
  if [[ "$TRANSPORT" == "nfs" ]]; then
    effective_mount="$NFS_MOUNT_PATH"
  fi

  if [[ "$MODE" == "both" || "$MODE" == "osage" ]]; then
    run_suite_for_fs "osagefs" "$effective_mount/$OSAGE_BASE_SUBDIR"
  fi

  if [[ "$MODE" == "both" || "$MODE" == "local" ]]; then
    rm -rf "$LOCAL_BASE"
    mkdir -p "$LOCAL_BASE"
    run_suite_for_fs "local" "$LOCAL_BASE"
  fi

  write_summary
  echo "Results: $RESULTS_JSON"
  echo "Summary: $SUMMARY_MD"

  if [[ "$RUN_FAILURES" -ne 0 ]]; then
    echo "One or more workloads failed. See $RESULTS_JSON for details." >&2
    return 1
  fi
}

main "$@"
