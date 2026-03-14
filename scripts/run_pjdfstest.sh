#!/usr/bin/env bash
set -euo pipefail
set -x

# ---------- config ----------
PJDFSTEST_DIR="${PJDFSTEST_DIR:-$(pwd)/../pjdfstest_nfs}"   # change if needed
TESTDIR="${TESTDIR:-/tmp/clawfs-mnt}"                   # your FUSE mountpoint
JOBS="${JOBS:-8}"

# Run
echo "Starting clawfs..."
./scripts/cleanup.sh
ALLOW_OTHER=1 ./scripts/run_clawfs.sh
cd $TESTDIR && sudo /usr/bin/prove -r -j"$JOBS" "${PJDFSTEST_DIR}/tests"
./scripts/cleanup.sh
echo "pre-push: OK"
