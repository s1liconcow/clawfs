#!/usr/bin/env bash
set -euo pipefail
set -x

# ---------- config ----------
PJDFSTEST_DIR="${PJDFSTEST_DIR:-/home/david/projects/osagefs/pjdfstest_nfs}"   # change if needed
TESTDIR="${TESTDIR:-/tmp/osagefs-mnt}"                   # your FUSE mountpoint
JOBS="${JOBS:-8}"

# Run
echo "Starting osagefs..."
./scripts/cleanup.sh
./scripts/run_osagefs.sh
cd $TESTDIR && sudo /usr/bin/prove -r -j"$JOBS" "${PJDFSTEST_DIR}/tests"
./scripts/cleanup.sh
echo "pre-push: OK"
