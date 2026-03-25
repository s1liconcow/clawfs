#!/usr/bin/env bash
set -euo pipefail

xfstests_dir="${1:-/tmp/xfstests}"
rc_path="${xfstests_dir}/common/rc"

if [[ ! -f "$rc_path" ]]; then
  echo "xfstests common/rc not found: $rc_path" >&2
  exit 1
fi

python3 - "$rc_path" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text()

needle = (
    "        sed -e 's/nfs4/nfs/' -e 's/fuse.glusterfs/glusterfs/' \\\n"
    "            -e 's/fuse.ceph-fuse/ceph-fuse/'"
)
replacement = needle + " \\\n            -e 's/fuse\\..*/fuse/'"

if "fuse\\..*/fuse" in text:
    sys.exit(0)

if needle not in text:
    raise SystemExit(f"expected xfstests _fs_type() normalization block in {path}")

path.write_text(text.replace(needle, replacement, 1))
PY
