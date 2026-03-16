#!/usr/bin/env bash
set -euo pipefail

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

MOUNT_PATH="${MOUNT_PATH:-/tmp/clawfs-commoncrawl-mnt}"
STORE_PATH="${STORE_PATH:-/tmp/clawfs-overlay-store}"
LOCAL_CACHE_PATH="${LOCAL_CACHE_PATH:-$HOME/.clawfs/commoncrawl-cache}"
STATE_PATH="${STATE_PATH:-$HOME/.clawfs/state/commoncrawl-client-state.bin}"
LOG_FILE="${LOG_FILE:-$ROOT_DIR/clawfs-commoncrawl.log}"
PID_FILE="${PID_FILE:-/tmp/clawfs-commoncrawl.pid}"
FOREGROUND="${FOREGROUND:-0}"
MOUNT_CHECK_TIMEOUT_SEC="${MOUNT_CHECK_TIMEOUT_SEC:-15}"

# Overlay backend (ClawFS-owned metadata + write path)
OVERLAY_OBJECT_PROVIDER="${OVERLAY_OBJECT_PROVIDER:-local}"
OVERLAY_BUCKET="${OVERLAY_BUCKET:-}"
OVERLAY_REGION="${OVERLAY_REGION:-}"
OVERLAY_ENDPOINT="${OVERLAY_ENDPOINT:-}"
OVERLAY_PREFIX="${OVERLAY_PREFIX:-clawfs-overlay/commoncrawl-demo}"
OVERLAY_GCS_SERVICE_ACCOUNT="${OVERLAY_GCS_SERVICE_ACCOUNT:-}"
OVERLAY_AWS_ALLOW_HTTP="${OVERLAY_AWS_ALLOW_HTTP:-0}"
OVERLAY_AWS_FORCE_PATH_STYLE="${OVERLAY_AWS_FORCE_PATH_STYLE:-0}"

# Source backend (existing dataset bucket)
SOURCE_OBJECT_PROVIDER="${SOURCE_OBJECT_PROVIDER:-aws}"
SOURCE_BUCKET="${SOURCE_BUCKET:-commoncrawl}"
SOURCE_PREFIX="${SOURCE_PREFIX:-crawl-data/CC-MAIN-2025-13/}"
SOURCE_STORE_PATH="${SOURCE_STORE_PATH:-}"
SOURCE_REGION="${SOURCE_REGION:-us-east-1}"
SOURCE_ENDPOINT="${SOURCE_ENDPOINT:-}"
SOURCE_GCS_SERVICE_ACCOUNT="${SOURCE_GCS_SERVICE_ACCOUNT:-}"
SOURCE_AWS_ACCESS_KEY_ID="${SOURCE_AWS_ACCESS_KEY_ID:-}"
SOURCE_AWS_SECRET_ACCESS_KEY="${SOURCE_AWS_SECRET_ACCESS_KEY:-}"
SOURCE_AWS_ALLOW_HTTP="${SOURCE_AWS_ALLOW_HTTP:-0}"
SOURCE_AWS_FORCE_PATH_STYLE="${SOURCE_AWS_FORCE_PATH_STYLE:-0}"

osage_ensure_release_binary
OSAGE_BIN="$ROOT_DIR/target/release/clawfsd"
osage_require_path "$OSAGE_BIN"

mkdir -p "$MOUNT_PATH" "$STORE_PATH" "$LOCAL_CACHE_PATH" "$(dirname "$STATE_PATH")"

CMD=(
  "$OSAGE_BIN"
  --mount-path "$MOUNT_PATH"
  --store-path "$STORE_PATH"
  --local-cache-path "$LOCAL_CACHE_PATH"
  --state-path "$STATE_PATH"
  --home-prefix /home
  --disable-cleanup
  --log-file "$LOG_FILE"
  --foreground
  --object-prefix "$OVERLAY_PREFIX"
  --source-object-provider "$SOURCE_OBJECT_PROVIDER"
  --source-prefix "$SOURCE_PREFIX"
)

case "$OVERLAY_OBJECT_PROVIDER" in
  local)
    CMD+=(--object-provider local)
    ;;
  aws)
    CMD+=(--object-provider aws)
    [[ -n "$OVERLAY_BUCKET" ]] || { echo "OVERLAY_BUCKET is required for OVERLAY_OBJECT_PROVIDER=aws" >&2; exit 1; }
    CMD+=(--bucket "$OVERLAY_BUCKET")
    [[ -n "$OVERLAY_REGION" ]] && CMD+=(--region "$OVERLAY_REGION")
    [[ -n "$OVERLAY_ENDPOINT" ]] && CMD+=(--endpoint "$OVERLAY_ENDPOINT")
    if osage_is_true "$OVERLAY_AWS_ALLOW_HTTP"; then
      CMD+=(--aws-allow-http)
    fi
    if osage_is_true "$OVERLAY_AWS_FORCE_PATH_STYLE"; then
      CMD+=(--aws-force-path-style)
    fi
    ;;
  gcs)
    CMD+=(--object-provider gcs)
    [[ -n "$OVERLAY_BUCKET" ]] || { echo "OVERLAY_BUCKET is required for OVERLAY_OBJECT_PROVIDER=gcs" >&2; exit 1; }
    CMD+=(--bucket "$OVERLAY_BUCKET")
    [[ -n "$OVERLAY_GCS_SERVICE_ACCOUNT" ]] && CMD+=(--gcs-service-account "$OVERLAY_GCS_SERVICE_ACCOUNT")
    ;;
  *)
    echo "Unsupported OVERLAY_OBJECT_PROVIDER: $OVERLAY_OBJECT_PROVIDER" >&2
    exit 1
    ;;
esac

case "$SOURCE_OBJECT_PROVIDER" in
  local)
    [[ -n "$SOURCE_STORE_PATH" ]] || { echo "SOURCE_STORE_PATH is required for SOURCE_OBJECT_PROVIDER=local" >&2; exit 1; }
    CMD+=(--source-store-path "$SOURCE_STORE_PATH")
    ;;
  aws)
    [[ -n "$SOURCE_BUCKET" ]] || { echo "SOURCE_BUCKET is required for SOURCE_OBJECT_PROVIDER=aws" >&2; exit 1; }
    CMD+=(--source-bucket "$SOURCE_BUCKET")
    [[ -n "$SOURCE_REGION" ]] && CMD+=(--source-region "$SOURCE_REGION")
    [[ -n "$SOURCE_ENDPOINT" ]] && CMD+=(--source-endpoint "$SOURCE_ENDPOINT")
    [[ -n "$SOURCE_AWS_ACCESS_KEY_ID" ]] && CMD+=(--source-aws-access-key-id "$SOURCE_AWS_ACCESS_KEY_ID")
    [[ -n "$SOURCE_AWS_SECRET_ACCESS_KEY" ]] && CMD+=(--source-aws-secret-access-key "$SOURCE_AWS_SECRET_ACCESS_KEY")
    if osage_is_true "$SOURCE_AWS_ALLOW_HTTP"; then
      CMD+=(--source-aws-allow-http)
    fi
    if osage_is_true "$SOURCE_AWS_FORCE_PATH_STYLE"; then
      CMD+=(--source-aws-force-path-style)
    fi
    ;;
  gcs)
    [[ -n "$SOURCE_BUCKET" ]] || { echo "SOURCE_BUCKET is required for SOURCE_OBJECT_PROVIDER=gcs" >&2; exit 1; }
    CMD+=(--source-bucket "$SOURCE_BUCKET")
    [[ -n "$SOURCE_GCS_SERVICE_ACCOUNT" ]] && CMD+=(--source-gcs-service-account "$SOURCE_GCS_SERVICE_ACCOUNT")
    ;;
  *)
    echo "Unsupported SOURCE_OBJECT_PROVIDER: $SOURCE_OBJECT_PROVIDER" >&2
    exit 1
    ;;
esac

if [[ -n "${CLAWFS_EXTRA_ARGS:-}" ]]; then
  read -ra _extra <<< "$CLAWFS_EXTRA_ARGS"
  CMD+=("${_extra[@]}")
fi

if osage_is_true "$FOREGROUND"; then
  echo "Running in foreground: ${CMD[*]}"
  exec "${CMD[@]}"
fi

echo "Running: ${CMD[*]}"
nohup "${CMD[@]}" >"$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"

if ! osage_assert_welcome_file "$MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"; then
  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID" >/dev/null 2>&1 || true
    wait "$PID" 2>/dev/null || true
  fi
  echo "--- LAST 40 LINES OF $LOG_FILE ---" >&2
  tail -n 40 "$LOG_FILE" >&2 || true
  rm -f "$PID_FILE"
  exit 1
fi

cat <<EOF
Mounted Common Crawl source overlay.
  PID: $PID
  Mount: $MOUNT_PATH
  Log: $LOG_FILE
  PID file: $PID_FILE

Try:
  ls "$MOUNT_PATH"
  find "$MOUNT_PATH" -maxdepth 2 | head
EOF
