#!/usr/bin/env bash

# Shared script helpers for ClawFS shell entrypoints.

osage_set_defaults() {
  local common_dir
  common_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
  ROOT_DIR="${ROOT_DIR:-$(cd -- "$common_dir/.." && pwd)}"

  MOUNT_PATH="${MOUNT_PATH:-/tmp/clawfs-mnt}"
  STORE_PATH="${STORE_PATH:-/tmp/clawfs-store}"
  LOCAL_CACHE_PATH="${LOCAL_CACHE_PATH:-$HOME/.clawfs/cache}"
  STATE_PATH="${STATE_PATH:-$HOME/.clawfs/state/client_state.bin}"
  LOG_FILE="${LOG_FILE:-$ROOT_DIR/clawfs.log}"
  PERF_LOG_PATH="${PERF_LOG_PATH:-}"
}

osage_is_true() {
  case "$1" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

osage_require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing dependency: $1" >&2
    exit 1
  fi
}

osage_require_path() {
  if [[ ! -x "$1" ]]; then
    echo "Missing dependency: $1" >&2
    exit 1
  fi
}

osage_ensure_release_binary() {
  local osage_bin=${1:-"$ROOT_DIR/target/release/clawfsd"}
  local needs_build=0

  if [[ ! -x "$osage_bin" ]]; then
    needs_build=1
  elif [[ "$ROOT_DIR/Cargo.toml" -nt "$osage_bin" ]] || [[ "$ROOT_DIR/Cargo.lock" -nt "$osage_bin" ]]; then
    needs_build=1
  elif find "$ROOT_DIR/src" "$ROOT_DIR/fuser-mt" -type f -newer "$osage_bin" -print -quit 2>/dev/null | grep -q .; then
    needs_build=1
  fi

  if [[ $needs_build -eq 1 ]]; then
    echo "Building clawfsd --release ..."
    (cd "$ROOT_DIR" && cargo build --release --bin clawfsd)
  fi
}

osage_ensure_release_checkpoint_binary() {
  local checkpoint_bin=${1:-"$ROOT_DIR/target/release/clawfs_checkpoint"}
  if [[ ! -x "$checkpoint_bin" ]]; then
    echo "Building clawfs_checkpoint --release ..."
    (cd "$ROOT_DIR" && cargo build --release --bin clawfs_checkpoint)
  fi
}

osage_assert_welcome_file() {
  local mount_path=${1:-"$MOUNT_PATH"}
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

osage_assert_mount_accessible() {
  local mount_path=${1:-"$MOUNT_PATH"}
  local mounted=1

  if command -v mountpoint >/dev/null 2>&1; then
    if mountpoint -q "$mount_path"; then
      mounted=0
    fi
  else
    if mount | grep -F " on $mount_path " >/dev/null 2>&1; then
      mounted=0
    fi
  fi

  if [[ $mounted -eq 0 ]]; then
    if ! ls "$mount_path" >/dev/null 2>&1; then
      echo "Mount appears stale or inaccessible at $mount_path" >&2
      echo "Try: sudo umount -l $mount_path" >&2
      return 1
    fi
  fi

  return 0
}
