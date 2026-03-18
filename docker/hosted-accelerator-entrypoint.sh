#!/bin/sh
set -eu

BIN="${HOSTED_ACCELERATOR_BIN:-clawfs_maintenance_worker}"

case "$BIN" in
  clawfs_maintenance_worker|clawfs_relay_executor)
    exec "/usr/local/bin/$BIN" "$@"
    ;;
  *)
    echo "unsupported HOSTED_ACCELERATOR_BIN: $BIN" >&2
    exit 64
    ;;
esac
