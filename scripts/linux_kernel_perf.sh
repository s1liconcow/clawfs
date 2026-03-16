#!/usr/bin/env bash
set -euo pipefail
set -x

source "$(cd -- "$(dirname -- "$0")" && pwd)/common.sh"
osage_set_defaults

TARGET_DIR="$ROOT_DIR/target/release"
OSAGE_BIN="$TARGET_DIR/clawfsd"

CACHE_DIR="${LINUX_CACHE:-$HOME/.cache/linux-tarballs}"
LOG_FILE="${LOG_FILE:-$ROOT_DIR/linux_build_timings.log}"
LINUX_VERSION="${LINUX_VERSION:-6.11}"
HOME_PREFIX="${HOME_PREFIX:-/home}"
DO_CLEANUP=1
SKIP_EXTRACT=0
REUSE_TREE=0
EXTRA_FLAGS=(--disable-cleanup --disable-journal)
RUN_CLEANUP="$ROOT_DIR/scripts/cleanup.sh"
MOUNT_CHECK_TIMEOUT_SEC="${MOUNT_CHECK_TIMEOUT_SEC:-10}"
DEBUG_LOG="${DEBUG_LOG:-}"

usage() {
  cat <<USAGE
Usage: ${0##*/} [--no_cleanup]

  --no_cleanup   Leave ClawFS mounted and running after the script finishes.
  --skip_extract Skip tar extraction and reuse linux-\$LINUX_VERSION already in WORKDIR.
  --reuse_tree   Reuse existing mount/store and extracted tree for faster reruns.
USAGE
}

while [[ ${1:-} != "" ]]; do
  case "$1" in
    --no_cleanup)
      DO_CLEANUP=0
      shift
      ;;
    --skip_extract)
      SKIP_EXTRACT=1
      shift
      ;;
    --reuse_tree)
      REUSE_TREE=1
      DO_CLEANUP=0
      SKIP_EXTRACT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done
HOME_PREFIX_TRIMMED="${HOME_PREFIX#/}"
HOME_PREFIX_TRIMMED="${HOME_PREFIX_TRIMMED%/}"

cleanup_mounts() {
  set +e
  MOUNT_PATH="$MOUNT_PATH" \
    STORE_PATH="$STORE_PATH" \
    LOCAL_CACHE_PATH="$LOCAL_CACHE_PATH" \
    STATE_PATH="$STATE_PATH" \
    LOG_FILE="$ROOT_DIR/clawfs.log" \
    PERF_LOG_PATH="$PERF_LOG_PATH" \
    "$RUN_CLEANUP" >/dev/null 2>&1 || true
  mkdir -p "$MOUNT_PATH" "$STORE_PATH" "$LOCAL_CACHE_PATH"
}

if [[ $REUSE_TREE -eq 0 ]]; then
  cleanup_mounts
else
  mkdir -p "$MOUNT_PATH" "$STORE_PATH" "$LOCAL_CACHE_PATH"
fi
osage_assert_mount_accessible "$MOUNT_PATH"
if [[ $DO_CLEANUP -eq 1 ]]; then
  trap cleanup_mounts EXIT
fi

mkdir -p "$CACHE_DIR" "$(dirname "$LOG_FILE")"

osage_require_cmd curl
osage_require_cmd python3
osage_require_cmd pv
osage_require_cmd tar
osage_require_cmd xz
osage_require_cmd make
osage_require_cmd gcc
osage_require_cmd ld
osage_require_cmd bc
osage_require_cmd bison
osage_require_cmd flex
osage_require_cmd perl
osage_require_cmd rsync
osage_require_cmd cpio
osage_require_path /usr/bin/time
if ! command -v fusermount >/dev/null 2>&1 && ! command -v fusermount3 >/dev/null 2>&1; then
  osage_require_cmd umount
fi
if ps -ef | grep -E "[o]sagefs(.| )*--mount-path[[:space:]]+$MOUNT_PATH" >/dev/null 2>&1; then
  echo "clawfs already running for mount $MOUNT_PATH" >&2
  echo "Stop existing daemon first, or use a different MOUNT_PATH." >&2
  exit 1
fi

latest_kernel_version() {
  python3 - "$@" <<'PY'
import json
from urllib.request import urlopen
url = "https://www.kernel.org/releases.json"
data = json.load(urlopen(url))
stable = next((rel for rel in data["releases"] if rel.get("moniker") == "stable"), None)
print(stable["version"] if stable else "6.12.0")
PY
}

if [[ -z "$LINUX_VERSION" ]]; then
  echo "Fetching latest Linux version metadata..."
  LINUX_VERSION=$(latest_kernel_version)
fi

tarball="linux-${LINUX_VERSION}.tar.xz"
CACHE_TARBALL="$CACHE_DIR/$tarball"
if [[ ! -f "$CACHE_TARBALL" ]]; then
  echo "Downloading $tarball ..."
  curl -fSLo "$CACHE_TARBALL" "https://cdn.kernel.org/pub/linux/kernel/v$(echo "$LINUX_VERSION" | cut -d. -f1).x/$tarball"
else
  echo "Using cached tarball $CACHE_TARBALL"
fi

osage_ensure_release_binary "$OSAGE_BIN"

echo "Starting ClawFS..."
CMD=(
  "$OSAGE_BIN"
  --mount-path "$MOUNT_PATH"
  --store-path "$STORE_PATH"
  --local-cache-path "$LOCAL_CACHE_PATH"
  --state-path "$STATE_PATH"
  --home-prefix "$HOME_PREFIX"
  --metadata-poll-interval-ms 0
  --foreground
  "${EXTRA_FLAGS[@]}"
)

OBJECT_PROVIDER="${OBJECT_PROVIDER:-local}"
if [[ "$OBJECT_PROVIDER" == "aws" ]]; then
  CMD+=(--object-provider aws)
  if [[ -n "${AWS_ENDPOINT_URL_S3:-}" ]]; then
    CMD+=(--endpoint "$AWS_ENDPOINT_URL_S3")
    if [[ "$AWS_ENDPOINT_URL_S3" == http://* ]]; then
      CMD+=(--aws-allow-http --aws-force-path-style)
    fi
  fi
  if [[ -n "${AWS_REGION:-}" ]]; then
    CMD+=(--region "$AWS_REGION")
  fi
  if [[ ! " ${EXTRA_FLAGS[*]:-} " =~ " --bucket " ]]; then
      CMD+=(--bucket "${AWS_BUCKET:-clawfs-bucket}")
  fi
else
  CMD+=(--object-provider local)
fi
if [[ -n "$PERF_LOG_PATH" ]]; then
  CMD+=(--perf-log "$PERF_LOG_PATH")
  echo "Perf trace -> $PERF_LOG_PATH"
fi
if [[ -n "$DEBUG_LOG" ]]; then
  CMD+=(--debug-log)
  echo "Debug logging enabled."
fi
"${CMD[@]}" &
OSAGE_PID=$!
sleep 2
if ! kill -0 "$OSAGE_PID" >/dev/null 2>&1; then
  echo "clawfs exited during startup; see log at $ROOT_DIR/clawfs.log" >&2
  exit 1
fi
osage_assert_mount_accessible "$MOUNT_PATH"
osage_assert_welcome_file "$MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"

if [[ $EUID -eq 0 ]]; then
  WORKDIR="$MOUNT_PATH"
else
  USER_NAME="${USER:-uid$EUID}"
  if [[ -n "$HOME_PREFIX_TRIMMED" ]]; then
    WORKDIR="${MOUNT_PATH%/}/${HOME_PREFIX_TRIMMED}/${USER_NAME}"
  else
    WORKDIR="${MOUNT_PATH%/}/${USER_NAME}"
  fi
fi
mkdir -p "$WORKDIR"

pushd "$WORKDIR" >/dev/null
if [[ $SKIP_EXTRACT -eq 1 && -d "linux-$LINUX_VERSION" ]]; then
  echo "Skipping extract: reusing $(pwd)/linux-$LINUX_VERSION"
else
  if [[ $SKIP_EXTRACT -eq 1 ]]; then
    echo "--skip_extract requested but linux-$LINUX_VERSION not present; extracting once."
  fi
  echo "Extracting $tarball (progress shown via pv)..."
  /usr/bin/time -f "extract elapsed %E" -o "$LOG_FILE" -a \
    bash -c 'pv -ptebar "$1" | tar xJf -' bash "$CACHE_TARBALL"
fi
cd "linux-$LINUX_VERSION"
/usr/bin/time -f "defconfig elapsed %E" -o "$LOG_FILE" -a make defconfig >/dev/null
/usr/bin/time -f "build elapsed %E" -o "$LOG_FILE" -a make -j"$(nproc)"
popd >/dev/null

echo "Build timings recorded in $LOG_FILE"
if [[ $DO_CLEANUP -eq 1 ]]; then
  kill "$OSAGE_PID" >/dev/null 2>&1 || true
  cleanup_mounts
else
  echo "Leaving ClawFS running (PID $OSAGE_PID). Mount available at $MOUNT_PATH"
  echo "To stop it manually: kill $OSAGE_PID && fusermount -u $MOUNT_PATH"
fi
