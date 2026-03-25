#!/usr/bin/env bash
set -euo pipefail

xfstests_dir="${1:-/tmp/xfstests}"
rc_path="${xfstests_dir}/common/rc"
config_path="${xfstests_dir}/common/config"

if [[ ! -f "$rc_path" ]]; then
  echo "xfstests common/rc not found: $rc_path" >&2
  exit 1
fi

if [[ ! -f "$config_path" ]]; then
  echo "xfstests common/config not found: $config_path" >&2
  exit 1
fi

python3 - "$rc_path" "$config_path" <<'PY'
from pathlib import Path
import sys

rc_path = Path(sys.argv[1])
config_path = Path(sys.argv[2])
rc_text = rc_path.read_text()
config_text = config_path.read_text()

needle = (
    "        sed -e 's/nfs4/nfs/' -e 's/fuse.glusterfs/glusterfs/' \\\n"
    "            -e 's/fuse.ceph-fuse/ceph-fuse/'"
)
replacement = needle + " \\\n            -e 's/fuse\\..*/fuse/'"

if "fuse\\..*/fuse" not in rc_text:
    if needle not in rc_text:
        raise SystemExit(f"expected xfstests _fs_type() normalization block in {rc_path}")
    rc_text = rc_text.replace(needle, replacement, 1)

config_needle = """_canonicalize_mountpoint()\n{\n\tlocal name=$1\n\tlocal dir=$2\n\n\tif [ -d \"$dir\" ]; then\n\t\t# this follows symlinks and removes all trailing \"/\"s\n\t\treadlink -e \"$dir\"\n\t\treturn 0\n\tfi\n"""
config_replacement = """_canonicalize_mountpoint()\n{\n\tlocal name=$1\n\tlocal dir=$2\n\n\tif [ \"$FSTYP\" = \"fuse\" ] && mountpoint -q \"$dir\" && ! ls \"$dir\" >/dev/null 2>&1; then\n\t\tumount -l \"$dir\" >/dev/null 2>&1 || fusermount3 -u \"$dir\" >/dev/null 2>&1 || true\n\t\tif ! mountpoint -q \"$dir\"; then\n\t\t\tmkdir -p \"$dir\" >/dev/null 2>&1 || true\n\t\tfi\n\tfi\n\n\tif [ -d \"$dir\" ]; then\n\t\t# this follows symlinks and removes all trailing \"/\"s\n\t\treadlink -e \"$dir\"\n\t\treturn 0\n\tfi\n"""

if "mountpoint -q \"$dir\" && ! ls \"$dir\"" not in config_text:
    if config_needle not in config_text:
        raise SystemExit(f"expected xfstests _canonicalize_mountpoint() block in {config_path}")
    config_text = config_text.replace(config_needle, config_replacement, 1)

rc_path.write_text(rc_text)
config_path.write_text(config_text)
PY
