#!/usr/bin/env bash
set -euo pipefail
set -x

ROOT_DIR=$(cd -- "$(dirname -- "$0")/.." && pwd)
TARGET_DIR="$ROOT_DIR/target/release"
OSAGE_BIN="$TARGET_DIR/osagefs"

MOUNT_PATH="${MOUNT_PATH:-/tmp/osagefs-mnt}"
STORE_PATH="${STORE_PATH:-/tmp/osagefs-store}"
LOCAL_CACHE_PATH="${LOCAL_CACHE_PATH:-/tmp/osagefs-cache}"
STATE_PATH="${STATE_PATH:-$HOME/.osagefs_state.bin}"
CACHE_DIR="${LINUX_CACHE:-$HOME/.cache/linux-tarballs}"
LOG_FILE="${LOG_FILE:-$ROOT_DIR/linux_build_timings.log}"
LINUX_VERSION="${LINUX_VERSION:-}"
HOME_PREFIX="${HOME_PREFIX:-/home}"
PERF_LOG_PATH="${PERF_LOG_PATH:-$ROOT_DIR/osagefs-perf.jsonl}"
DO_CLEANUP=1
EXTRA_FLAGS=(--disable-cleanup --disable-journal)

usage() {
  cat <<USAGE
Usage: ${0##*/} [--no_cleanup]

  --no_cleanup   Leave OsageFS mounted and running after the script finishes.
USAGE
}

while [[ ${1:-} != "" ]]; do
  case "$1" in
    --no_cleanup)
      DO_CLEANUP=0
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
  if mountpoint -q "$MOUNT_PATH"; then
    fusermount -u "$MOUNT_PATH" 2>/dev/null || sudo fusermount -u "$MOUNT_PATH"
  fi
  rm -rf "$MOUNT_PATH" "$STORE_PATH" "$LOCAL_CACHE_PATH"
  mkdir -p "$MOUNT_PATH" "$STORE_PATH" "$LOCAL_CACHE_PATH"
}

cleanup_mounts
if [[ $DO_CLEANUP -eq 1 ]]; then
  trap cleanup_mounts EXIT
fi

mkdir -p "$CACHE_DIR" "$(dirname "$LOG_FILE")"

require() {
  if ! command -v "$1" >/dev/null; then
    echo "Missing dependency: $1" >&2
    exit 1
  fi
}

require_path() {
  if [[ ! -x "$1" ]]; then
    echo "Missing dependency: $1" >&2
    exit 1
  fi
}

require curl
require python3
require pv
require tar
require xz
require make
require gcc
require ld
require bc
require bison
require flex
require perl
require rsync
require cpio
require_path /usr/bin/time

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

if [[ ! -x "$OSAGE_BIN" ]]; then
  echo "Building osagefs --release ..."
  (cd "$ROOT_DIR" && cargo build --release)
fi

echo "Starting OsageFS..."
CMD=(
  "$OSAGE_BIN"
  --mount-path "$MOUNT_PATH"
  --store-path "$STORE_PATH"
  --local-cache-path "$LOCAL_CACHE_PATH"
  --object-provider local
  --state-path "$STATE_PATH"
  --home-prefix "$HOME_PREFIX"
  --foreground
  "${EXTRA_FLAGS[@]}"
)
if [[ -n "$PERF_LOG_PATH" ]]; then
  CMD+=(--perf-log "$PERF_LOG_PATH")
  echo "Perf trace -> $PERF_LOG_PATH"
fi
"${CMD[@]}" &
OSAGE_PID=$!
sleep 2

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
echo "Extracting $tarball (progress shown via pv)..."
/usr/bin/time -f "extract elapsed %E" -o "$LOG_FILE" -a \
  bash -c 'pv -ptebar "$1" | tar xJf -' bash "$CACHE_TARBALL"
cd "linux-$LINUX_VERSION"
/usr/bin/time -f "defconfig elapsed %E" -o "$LOG_FILE" -a make defconfig >/dev/null
/usr/bin/time -f "build elapsed %E" -o "$LOG_FILE" -a make -j"$(nproc)"
popd >/dev/null

echo "Build timings recorded in $LOG_FILE"
if [[ $DO_CLEANUP -eq 1 ]]; then
  kill "$OSAGE_PID" >/dev/null 2>&1 || true
  cleanup_mounts
else
  echo "Leaving OsageFS running (PID $OSAGE_PID). Mount available at $MOUNT_PATH"
  echo "To stop it manually: kill $OSAGE_PID && fusermount -u $MOUNT_PATH"
fi
