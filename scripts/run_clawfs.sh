#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

OSAGE_BIN="$ROOT_DIR/target/release/clawfs"

PID_FILE="${PID_FILE:-/tmp/clawfs.pid}"
FOREGROUND="${FOREGROUND:-0}"
DISABLE_JOURNAL="${DISABLE_JOURNAL:-1}"
DEBUG_LOG="${DEBUG_LOG:-0}"
MOUNT_CHECK_TIMEOUT_SEC="${MOUNT_CHECK_TIMEOUT_SEC:-10}"
REPLAY_LOG_PATH="${REPLAY_LOG_PATH:-}"
ALLOW_OTHER="${ALLOW_OTHER:-0}"
SEGMENT_COMPRESSION="${SEGMENT_COMPRESSION:-true}"
INLINE_COMPRESSION="${INLINE_COMPRESSION:-true}"
FUSE_THREADS="${FUSE_THREADS:-4}"
NO_WRITEBACK_CACHE="${NO_WRITEBACK_CACHE:-0}"
HEAPTRACK="${HEAPTRACK:-0}"
HEAPTRACK_OUTPUT="${HEAPTRACK_OUTPUT:-$ROOT_DIR/heaptrack/heaptrack.clawfs.%p}"
HEAPTRACK_RAW="${HEAPTRACK_RAW:-0}"
HEAPTRACK_EXTRA_ARGS="${HEAPTRACK_EXTRA_ARGS:-}"

echo "Ensuring clawfs binary is up to date..."
(cd "$ROOT_DIR" && cargo build --release --bin clawfs)
OSAGE_BIN="$(cd "$(dirname "$OSAGE_BIN")" && pwd)/clawfs"
echo "Using binary: $OSAGE_BIN"

mkdir -p "$MOUNT_PATH" "$STORE_PATH" "$LOCAL_CACHE_PATH" "$(dirname "$STATE_PATH")"

if [[ ! -x "$OSAGE_BIN" ]]; then
  echo "ERROR: Binary not found or not executable: $OSAGE_BIN" >&2
  exit 1
fi

echo "Verifying binary execution..."
"$OSAGE_BIN" --help >/dev/null || { echo "ERROR: Binary failed to execute --help" >&2; exit 1; }

CMD=(
  "$OSAGE_BIN"
  --mount-path "$MOUNT_PATH"
  --store-path "$STORE_PATH"
  --local-cache-path "$LOCAL_CACHE_PATH"
  --state-path "$STATE_PATH"
  --home-prefix /home
  --disable-cleanup
  --log-file "$LOG_FILE"
  --segment-compression "$SEGMENT_COMPRESSION"
  --inline-compression "$INLINE_COMPRESSION"
  --foreground
)

OBJECT_PROVIDER="${OBJECT_PROVIDER:-local}"

if [[ "$OBJECT_PROVIDER" == "aws" ]]; then
  CMD+=(--object-provider aws)
  if [[ -n "${AWS_ENDPOINT_URL_S3:-}" ]]; then
    CMD+=(--endpoint "$AWS_ENDPOINT_URL_S3")
    # Custom endpoints (MinIO, R2, local clones) typically require path-style access.
    CMD+=(--aws-force-path-style)
    # If using HTTP, explicitly allow it.
    if [[ "$AWS_ENDPOINT_URL_S3" == http://* ]]; then
      CMD+=(--aws-allow-http)
    fi
  fi
  if [[ -n "${AWS_REGION:-}" ]]; then
    CMD+=(--region "$AWS_REGION")
  fi
  # Check AWS_BUCKET env var, then fallback to clawfs-bucket if not passed in EXTRA_ARGS.
  if [[ ! " ${CLAWFS_EXTRA_ARGS:-} " =~ " --bucket " ]]; then
      _BUCKET="${AWS_BUCKET:-clawfs-bucket}"
      CMD+=(--bucket "$_BUCKET")
  fi
else
  CMD+=(--object-provider local)
fi

# Auto-create bucket if using a custom AWS/S3 endpoint
if [[ "$OBJECT_PROVIDER" == "aws" ]] && [[ -n "${AWS_ENDPOINT_URL_S3:-}" ]]; then
  # Extract bucket from CMD if not explicitly set in _BUCKET
  if [[ -z "${_BUCKET:-}" ]]; then
      for ((i=0; i<${#CMD[@]}; i++)); do
          if [[ "${CMD[i]}" == "--bucket" ]]; then
              _BUCKET="${CMD[i+1]}"
              break
          fi
      done
  fi
  _BUCKET="${_BUCKET:-clawfs-bucket}"


  echo "Ensuring S3 bucket '$_BUCKET' exists at $AWS_ENDPOINT_URL_S3 ..."
  AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" \
  AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
  _BUCKET="$_BUCKET" \
  AWS_ENDPOINT_URL_S3="$AWS_ENDPOINT_URL_S3" \
  AWS_REGION="${AWS_REGION:-}" \
  python3 - <<'PY'
import os
import urllib.request
import sys

bucket = os.environ.get("_BUCKET", "clawfs-bucket")
endpoint = os.environ.get("AWS_ENDPOINT_URL_S3")
access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

if not endpoint:
    sys.exit(0)

try:
    import boto3
    from botocore.client import Config
    s3 = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name=os.environ.get("AWS_REGION", "us-east-1"))
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' ensured via boto3")
    except Exception as e:
        if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
            pass
        else:
            print(f"Note: boto3 bucket creation info: {e}")
except ImportError:
    # Fallback to simple PUT for local development environments
    try:
        req = urllib.request.Request(f"{endpoint.rstrip('/')}/{bucket}", method="PUT")
        with urllib.request.urlopen(req, timeout=5) as f:
            print(f"Bucket '{bucket}' created (status: {f.status})")
    except Exception as e:
        print(f"Note: Could not verify/create bucket via simple PUT: {e}")
PY
fi

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
CMD+=(--fuse-threads "$FUSE_THREADS")
if osage_is_true "${WRITEBACK_CACHE:-}"; then
  CMD+=(--writeback-cache)
fi
# Allow callers to inject additional CLI flags (e.g. --pending-bytes, --flush-interval-ms).
if [[ -n "${CLAWFS_EXTRA_ARGS:-}" ]]; then
  read -ra _extra <<< "$CLAWFS_EXTRA_ARGS"
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

echo "Running: ${LAUNCH_CMD[*]}"
echo "Log file: $LOG_FILE"
mkdir -p "$(dirname "$LOG_FILE")"
nohup "${LAUNCH_CMD[@]}" >"$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"
if ! osage_assert_welcome_file "$MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"; then
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID" >/dev/null 2>&1 || true
    wait "$PID" 2>/dev/null || true
  fi
  echo "--- LAST 20 LINES OF $LOG_FILE ---" >&2
  tail -n 20 "$LOG_FILE" >&2 || true
  rm -f "$PID_FILE"
  exit 1
fi
echo "clawfs running as PID $PID (log: $LOG_FILE, pid file: $PID_FILE)"
