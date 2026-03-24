#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
template_path="${ROOT_DIR}/scripts/mount.fuse.clawfs"
mount_helper_path="${MOUNT_HELPER_PATH:-/sbin/mount.fuse.clawfs}"
clawfsd_bin="${CLAWFSD_BIN:-}"

if [[ -z "$clawfsd_bin" ]]; then
  echo "CLAWFSD_BIN must point at the clawfsd binary" >&2
  exit 1
fi

if [[ ! -x "$clawfsd_bin" ]]; then
  echo "CLAWFSD_BIN is not executable: $clawfsd_bin" >&2
  exit 1
fi

tmp_path="$(mktemp)"
trap 'rm -f "$tmp_path"' EXIT

sed "s|__CLAWFSD_BIN__|$clawfsd_bin|g" "$template_path" >"$tmp_path"
install -m 0755 "$tmp_path" "$mount_helper_path"

if ! grep -q '^user_allow_other$' /etc/fuse.conf 2>/dev/null; then
  echo "user_allow_other" >> /etc/fuse.conf
fi
