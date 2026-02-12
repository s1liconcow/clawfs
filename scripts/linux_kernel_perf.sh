#!/usr/bin/env bash
set -euox pipefail

ROOT_DIR=$(cd -- "$(dirname -- "$0")/.." && pwd)
TARGET_DIR="$ROOT_DIR/target/release"
OSAGE_BIN="$TARGET_DIR/osagefs"

MOUNT_PATH="${MOUNT_PATH:-/tmp/osagefs-mnt}"
STORE_PATH="${STORE_PATH:-/tmp/osagefs-store}"
STATE_PATH="${STATE_PATH:-$HOME/.osagefs_state.json}"
CACHE_DIR="${LINUX_CACHE:-$HOME/.cache/linux-tarballs}"
LOG_FILE="${LOG_FILE:-$ROOT_DIR/linux_build_timings.log}"
PENDING_BYTES="${PENDING_BYTES:-$((64*1024*1024))}"
INLINE_THRESHOLD="${INLINE_THRESHOLD:-1024}"
LINUX_VERSION="${LINUX_VERSION:-}"

cleanup_mounts() {
  set +e
  if mountpoint -q "$MOUNT_PATH"; then
    fusermount -u "$MOUNT_PATH" 2>/dev/null || sudo fusermount -u "$MOUNT_PATH"
  fi
  sudo rm -rf "$MOUNT_PATH" "$STORE_PATH"
  mkdir -p "$MOUNT_PATH" "$STORE_PATH"
  [[ -f "$STATE_PATH" ]] && sudo rm -f "$STATE_PATH"
  set -e
}

cleanup_mounts
trap cleanup_mounts EXIT

mkdir -p "$CACHE_DIR" "$(dirname "$LOG_FILE")"

require() {
  if ! command -v "$1" >/dev/null; then
    echo "Missing dependency: $1" >&2
    exit 1
  fi
}

require curl
require python3

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
"$OSAGE_BIN" \
  --mount-path "$MOUNT_PATH" \
  --store-path "$STORE_PATH" \
  --inline-threshold "$INLINE_THRESHOLD" \
  --pending-bytes "$PENDING_BYTES" \
  --object-provider local \
  --state-path "$STATE_PATH" \
  --foreground &
OSAGE_PID=$!
sleep 2

pushd "$MOUNT_PATH" >/dev/null
/usr/bin/time -f "extract elapsed %E" -o "$LOG_FILE" -a tar xf "$CACHE_TARBALL"
cd "linux-$LINUX_VERSION"
/usr/bin/time -f "defconfig elapsed %E" -o "$LOG_FILE" -a make defconfig >/dev/null
/usr/bin/time -f "build elapsed %E" -o "$LOG_FILE" -a make -j"$(nproc)"
popd >/dev/null

echo "Build timings recorded in $LOG_FILE"
kill "$OSAGE_PID" >/dev/null 2>&1 || true
cleanup_mounts
