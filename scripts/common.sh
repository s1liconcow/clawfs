#!/usr/bin/env bash

# Shared script helpers for OsageFS shell entrypoints.

osage_set_defaults() {
  local common_dir
  common_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
  ROOT_DIR="${ROOT_DIR:-$(cd -- "$common_dir/.." && pwd)}"

  MOUNT_PATH="${MOUNT_PATH:-/tmp/osagefs-mnt}"
  STORE_PATH="${STORE_PATH:-/tmp/osagefs-store}"
  LOCAL_CACHE_PATH="${LOCAL_CACHE_PATH:-$HOME/.osagefs/cache}"
  STATE_PATH="${STATE_PATH:-$HOME/.osagefs/state/client_state.bin}"
  LOG_FILE="${LOG_FILE:-$ROOT_DIR/osagefs.log}"
  PERF_LOG_PATH="${PERF_LOG_PATH:-$ROOT_DIR/osagefs-perf.jsonl}"
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
  local osage_bin=${1:-"$ROOT_DIR/target/release/osagefs"}
  if [[ ! -x "$osage_bin" ]]; then
    echo "Building osagefs --release ..."
    (cd "$ROOT_DIR" && cargo build --release)
  fi
}

osage_ensure_release_checkpoint_binary() {
  local checkpoint_bin=${1:-"$ROOT_DIR/target/release/osagefs_checkpoint"}
  if [[ ! -x "$checkpoint_bin" ]]; then
    echo "Building osagefs_checkpoint --release ..."
    (cd "$ROOT_DIR" && cargo build --release --bin osagefs_checkpoint)
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
