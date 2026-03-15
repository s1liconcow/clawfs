#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "$0")/.." && pwd)
GATEWAY_MANIFEST="$ROOT_DIR/clawfs-nfs-gateway/Cargo.toml"
GATEWAY_BIN="$ROOT_DIR/target/release/clawfs-nfs-gateway"

MOUNT_PATH="${MOUNT_PATH:-/tmp/clawfs-mnt}"
STORE_PATH="${STORE_PATH:-/tmp/clawfs-store}"
LOCAL_CACHE_PATH="${LOCAL_CACHE_PATH:-$HOME/.clawfs/cache}"
STATE_PATH="${STATE_PATH:-$HOME/.clawfs/state/nfs_gateway_state.bin}"
LISTEN="${LISTEN:-0.0.0.0:2049}"
PROTOCOL="${PROTOCOL:-v3}"
USE_EXISTING_MOUNT="${USE_EXISTING_MOUNT:-0}"
DISABLE_JOURNAL="${DISABLE_JOURNAL:-1}"
ENABLE_JOURNAL="${ENABLE_JOURNAL:-0}"
DEBUG_LOG="${DEBUG_LOG:-0}"
FOREGROUND="${FOREGROUND:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"
PID_FILE="${PID_FILE:-/tmp/clawfs-nfs-gateway.pid}"
LOG_FILE="${LOG_FILE:-$ROOT_DIR/clawfs-nfs-gateway.log}"
REPLAY_LOG_PATH="${REPLAY_LOG_PATH:-}"

AUTO_MOUNT_NFS="${AUTO_MOUNT_NFS:-1}"
NFS_MOUNT_PATH="${NFS_MOUNT_PATH:-/tmp/clawfs-mnt}"
NFS_MOUNT_HOST="${NFS_MOUNT_HOST:-127.0.0.1}"
NFS_MOUNT_EXPORT="${NFS_MOUNT_EXPORT:-/}"
MOUNT_WITH_SUDO="${MOUNT_WITH_SUDO:-1}"
MOUNT_CHECK_TIMEOUT_SEC="${MOUNT_CHECK_TIMEOUT_SEC:-10}"

GANESHA_BINARY="${GANESHA_BINARY:-}"
GANESHA_LOG="${GANESHA_LOG:-}"

is_true() {
  case "$1" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

assert_welcome_file() {
  local mount_path=$1
  local timeout_sec=${2:-10}
  local welcome_path="${mount_path%/}/WELCOME.txt"
  local start=$SECONDS

  while (( SECONDS - start < timeout_sec )); do
    if [[ -f "$welcome_path" ]]; then
      return 0
    fi
    sleep 0.2
  done

  echo "Mount validation failed: expected preset file $welcome_path" >&2
  if [[ -d "$mount_path" ]]; then
    ls -la "$mount_path" >&2 || true
  fi
  return 1
}

if ! is_true "$SKIP_BUILD"; then
  echo "Building clawfs-nfs-gateway --release ..."
  (cd "$ROOT_DIR" && cargo build --release --manifest-path "$GATEWAY_MANIFEST")
elif [[ ! -x "$GATEWAY_BIN" ]]; then
  echo "Gateway binary missing at $GATEWAY_BIN and SKIP_BUILD=1" >&2
  exit 1
fi

mkdir -p "$STORE_PATH" "$LOCAL_CACHE_PATH" "$(dirname "$STATE_PATH")"

LISTEN_PORT="${LISTEN##*:}"
mount_prefix=()
if is_true "$MOUNT_WITH_SUDO" && [[ "${EUID:-$(id -u)}" -ne 0 ]] && command -v sudo >/dev/null 2>&1; then
  mount_prefix=(sudo)
fi

CMD=(
  "$GATEWAY_BIN"
  --mount-path "$MOUNT_PATH"
  --store-path "$STORE_PATH"
  --local-cache-path "$LOCAL_CACHE_PATH"
  --state-path "$STATE_PATH"
  --listen "$LISTEN"
  --protocol "$PROTOCOL"
)

if is_true "$USE_EXISTING_MOUNT"; then
  CMD+=(--use-existing-mount)
fi
if is_true "$DISABLE_JOURNAL" && is_true "$ENABLE_JOURNAL"; then
  echo "Conflicting settings: DISABLE_JOURNAL=1 and ENABLE_JOURNAL=1" >&2
  exit 1
fi
if is_true "$ENABLE_JOURNAL"; then
  CMD+=(--enable-journal)
elif is_true "$DISABLE_JOURNAL"; then
  CMD+=(--disable-journal)
fi
if [[ -n "$GANESHA_BINARY" ]]; then
  CMD+=(--ganesha-binary "$GANESHA_BINARY")
fi
if [[ -n "$GANESHA_LOG" ]]; then
  CMD+=(--ganesha-log "$GANESHA_LOG")
fi
if [[ -n "$REPLAY_LOG_PATH" ]]; then
  CMD+=(--replay-log "$REPLAY_LOG_PATH")
fi

if is_true "$DEBUG_LOG"; then
  export RUST_LOG="${RUST_LOG:-debug}"
fi

if is_true "$FOREGROUND"; then
  echo "Starting clawfs-nfs-gateway in foreground..."
  "${CMD[@]}" &
  PID=$!
  echo "$PID" >"$PID_FILE"
else
  echo "Starting clawfs-nfs-gateway in background..."
  mkdir -p "$(dirname "$LOG_FILE")"
  nohup "${CMD[@]}" >"$LOG_FILE" 2>&1 &
  PID=$!
  echo "$PID" >"$PID_FILE"
  echo "clawfs-nfs-gateway running as PID $PID (log: $LOG_FILE, pid file: $PID_FILE)"
fi

if is_true "$AUTO_MOUNT_NFS"; then
  nfs_mount_helper=""
  if command -v mount.nfs >/dev/null 2>&1; then
    nfs_mount_helper="$(command -v mount.nfs)"
  elif [[ -x /sbin/mount.nfs ]]; then
    nfs_mount_helper="/sbin/mount.nfs"
  elif [[ -x /usr/sbin/mount.nfs ]]; then
    nfs_mount_helper="/usr/sbin/mount.nfs"
  fi
  if [[ -z "$nfs_mount_helper" ]]; then
    echo "NFS client helper not found (mount.nfs)." >&2
    echo "Install nfs client tools (Ubuntu/Debian: sudo apt-get install -y nfs-common)." >&2
    exit 1
  fi

  sleep 1
  mountinfo_path="${NFS_MOUNT_PATH//\\/\\\\}"
  mountinfo_path="${mountinfo_path// /\\040}"
  if ! grep -q " ${mountinfo_path} " /proc/self/mountinfo 2>/dev/null; then
    if [[ ! -e "$NFS_MOUNT_PATH" ]]; then
      mkdir -p "$NFS_MOUNT_PATH"
    fi
  fi
  if mountpoint -q "$NFS_MOUNT_PATH"; then
    "${mount_prefix[@]}" umount "$NFS_MOUNT_PATH" || true
  fi

  echo "Mounting NFS ${NFS_MOUNT_HOST}:${NFS_MOUNT_EXPORT} -> $NFS_MOUNT_PATH"
  "${mount_prefix[@]}" "$nfs_mount_helper" \
    -o "vers=3,nolock,proto=tcp,port=${LISTEN_PORT},mountport=${LISTEN_PORT}" \
    "${NFS_MOUNT_HOST}:${NFS_MOUNT_EXPORT}" \
    "$NFS_MOUNT_PATH"
  echo "Mounted NFS at $NFS_MOUNT_PATH"
  assert_welcome_file "$NFS_MOUNT_PATH" "$MOUNT_CHECK_TIMEOUT_SEC"
fi

if is_true "$FOREGROUND"; then
  wait "$PID"
fi
