#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "$0")/.." && pwd)"
WORKDIR_REMOTE="${WORKDIR_REMOTE:-/work/clawfs}"
SYNC_REPO=1
SKIP_BOOTSTRAP=0
TASKS_CSV="xfstests,linux,pjdfstest"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/validation-results-$(date +%Y%m%d-%H%M%S)}"

usage() {
  cat <<'USAGE'
Usage: scripts/sprite_validate_parallel.sh [options]

Run validation suites in parallel on dedicated sprites:
  - xfstests
  - linux kernel compile perf script
  - pjdfstest

Options:
  --tasks <csv>        Comma-separated list: xfstests,linux,pjdfstest
  --no-sync            Skip tar sync into sprites (assume /work/clawfs is current)
  --skip-bootstrap     Skip apt/bootstrap steps inside sprites
  --log-dir <path>     Local directory for per-task logs
  -h, --help           Show help

Examples:
  scripts/sprite_validate_parallel.sh
  scripts/sprite_validate_parallel.sh --tasks xfstests,linux
  scripts/sprite_validate_parallel.sh --no-sync --skip-bootstrap
USAGE
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

slugify() {
  printf '%s' "$1" \
    | tr -cs 'a-zA-Z0-9' '-' \
    | tr '[:upper:]' '[:lower:]' \
    | sed 's/^-*//; s/-*$//'
}

derive_raw_base() {
  local repo_slug branch_slug
  repo_slug="$(slugify "$(basename "$(git -C "$ROOT_DIR" rev-parse --show-toplevel)")")"
  branch_slug="$(slugify "$(git -C "$ROOT_DIR" rev-parse --abbrev-ref HEAD)")"
  printf '%s-%s' "$repo_slug" "$branch_slug"
}

derive_sprite_name() {
  local raw_base="$1"
  local role="$2"
  local role_slug raw_name name_hash max_base_len base_trimmed
  role_slug="$(slugify "$role" | cut -c1-12)"
  raw_name="${raw_base}-${role_slug}"
  name_hash="$(printf '%s' "$raw_name" | sha1sum | cut -c1-8)"
  max_base_len=$((55 - 1 - ${#role_slug} - 1 - 8))
  base_trimmed="$(printf '%s' "$raw_base" | cut -c1-"$max_base_len" | sed 's/-$//')"
  printf '%s-%s-%s' "$base_trimmed" "$role_slug" "$name_hash"
}

contains_task() {
  local want="$1"
  local t
  IFS=',' read -r -a _tasks <<< "$TASKS_CSV"
  for t in "${_tasks[@]}"; do
    if [[ "$t" == "$want" ]]; then
      return 0
    fi
  done
  return 1
}

ensure_sprite() {
  local sprite="$1"
  if ! sprite list | rg -qx "$sprite"; then
    echo "Creating sprite $sprite"
    sprite create "$sprite" >/dev/null
  fi
}

sync_repo_to_sprite() {
  local sprite="$1"
  tar \
    --exclude='.git' \
    --exclude='target' \
    --exclude='linux-local' \
    --exclude='out' \
    --exclude='fio-results-*' \
    --exclude='*.log' \
    -czf - . \
    | sprite exec -s "$sprite" -- bash -lc "rm -rf '$WORKDIR_REMOTE' && mkdir -p '$WORKDIR_REMOTE' && tar -xzf - -C '$WORKDIR_REMOTE'"
}

bootstrap_common_cmd() {
  cat <<'EOF'
set -euo pipefail
sudo apt-get update
sudo apt-get install -y \
  bash coreutils findutils procps psmisc tar gzip xz-utils curl git python3 \
  make gcc g++ binutils bc bison flex perl rsync cpio pv time ripgrep strace \
  fio fuse3 util-linux sudo
echo "user_allow_other" | sudo tee -a /etc/fuse.conf >/dev/null || true
EOF
}

run_xfstests_cmd() {
  cat <<EOF
set -euo pipefail
cd '$WORKDIR_REMOTE'
if [[ "$SKIP_BOOTSTRAP" -eq 0 ]]; then
  $(bootstrap_common_cmd)
  sudo apt-get install -y \
    acl attr automake dbench dump e2fsprogs gawk indent \
    libacl1-dev libaio-dev libcap-dev libgdbm-dev libtool libtool-bin \
    liburing-dev libuuid1 lvm2 quota sed uuid-dev uuid-runtime xfsprogs \
    sqlite3 libgdbm-compat-dev systemd-coredump systemd jq pkg-config \
    nfs-common \
    exfatprogs f2fs-tools ocfs2-tools udftools xfsdump xfslibs-dev || true
fi

if [[ ! -d /tmp/xfstests ]]; then
  git clone https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git /tmp/xfstests
  (cd /tmp/xfstests && make && sudo make install)
fi

(cd clawfs-nfs-gateway && cargo build --release)

# Clean up from prior runs.
sudo rm -rf /tmp/clawfs-test-store /tmp/clawfs-scratch-store
sudo rm -rf /tmp/clawfs-test-cache /tmp/clawfs-scratch-cache
mkdir -p /tmp/clawfs-test-store /tmp/clawfs-scratch-store
mkdir -p /tmp/clawfs-test-cache /tmp/clawfs-scratch-cache
sudo mkdir -p /mnt/test /mnt/scratch

# Start ClawFS NFS gateway (TEST instance)
target/release/clawfs-nfs-gateway \
  --store-path /tmp/clawfs-test-store \
  --local-cache-path /tmp/clawfs-test-cache \
  --state-path /tmp/clawfs-test-state.bin \
  --object-provider local \
  --disable-journal \
  --listen 127.0.0.1:2049 &
echo \$! > /tmp/clawfs-test.pid

for _ in \$(seq 1 30); do
  nc -z 127.0.0.1 2049 && break
  sleep 1
done
nc -z 127.0.0.1 2049 || { echo "TEST NFS instance failed to start"; exit 1; }

# Start ClawFS NFS gateway (SCRATCH instance)
target/release/clawfs-nfs-gateway \
  --store-path /tmp/clawfs-scratch-store \
  --local-cache-path /tmp/clawfs-scratch-cache \
  --state-path /tmp/clawfs-scratch-state.bin \
  --object-provider local \
  --disable-journal \
  --listen 127.0.0.1:20499 &
echo \$! > /tmp/clawfs-scratch.pid

for _ in \$(seq 1 30); do
  nc -z 127.0.0.1 20499 && break
  sleep 1
done
nc -z 127.0.0.1 20499 || { echo "SCRATCH NFS instance failed to start"; exit 1; }

cd /tmp/xfstests
cat > local.config <<'LOCALCFG'
export FSTYP=nfs

export TEST_DEV=localhost:/
export TEST_DIR=/mnt/test
export SCRATCH_DEV=127.0.0.1:/
export SCRATCH_MNT=/mnt/scratch

export TEST_FS_MOUNT_OPTS="-o nolock,tcp,port=2049,mountport=2049,vers=3,hard"
export NFS_MOUNT_OPTIONS="-o nolock,tcp,port=20499,mountport=20499,vers=3,hard"
LOCALCFG

cat > excludes <<'EX'
generic/003
generic/013
generic/035
generic/069
generic/075
generic/091
generic/112
generic/113
generic/126
generic/169
generic/184
generic/394
generic/467
generic/263
generic/759
generic/760
generic/477
generic/564
generic/633
generic/696
generic/591
generic/615
generic/087
EX

sudo ./check -E excludes \
  generic/001 generic/011 generic/023 generic/024 generic/028 generic/029 \
  generic/030 generic/080 generic/084 generic/087 generic/088 generic/095 generic/098

# Cleanup NFS instances
sudo umount /mnt/test || true
sudo umount /mnt/scratch || true
kill \$(cat /tmp/clawfs-test.pid) || true
kill \$(cat /tmp/clawfs-scratch.pid) || true
EOF
}

run_linux_cmd() {
  cat <<EOF
set -euo pipefail
cd '$WORKDIR_REMOTE'
if [[ "$SKIP_BOOTSTRAP" -eq 0 ]]; then
  $(bootstrap_common_cmd)
  sudo apt-get install -y libelf-dev dwarves
fi
LOG_FILE='$WORKDIR_REMOTE/linux_build_timings.log' \
PERF_LOG_PATH='$WORKDIR_REMOTE/clawfs-perf.jsonl' \
./scripts/linux_kernel_perf.sh
EOF
}

run_pjdfstest_cmd() {
  cat <<EOF
set -euo pipefail
cd '$WORKDIR_REMOTE'
if [[ "$SKIP_BOOTSTRAP" -eq 0 ]]; then
  $(bootstrap_common_cmd)
  sudo apt-get install -y prove libtest2-suite-perl || true
fi
if [[ ! -d /tmp/pjdfstest ]]; then
  git clone https://github.com/pjd/pjdfstest.git /tmp/pjdfstest
fi
cargo build --release --bin clawfs
PJDFSTEST_DIR=/tmp/pjdfstest TESTDIR=/tmp/clawfs-mnt JOBS=8 ./scripts/run_pjdfstest.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tasks)
      TASKS_CSV="$2"
      shift 2
      ;;
    --no-sync)
      SYNC_REPO=0
      shift
      ;;
    --skip-bootstrap)
      SKIP_BOOTSTRAP=1
      shift
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_cmd sprite
require_cmd rg
require_cmd tar
require_cmd sha1sum
require_cmd git

# Support both explicit token auth and pre-authenticated local sprite sessions.
if ! sprite list >/dev/null 2>&1; then
  echo "sprite is not authenticated. Set SPRITES_TOKEN or run 'sprite login' first." >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
RAW_BASE="$(derive_raw_base)"
echo "Validation log dir: $LOG_DIR"

declare -A SPRITE_BY_TASK
declare -A PID_BY_TASK
declare -A LOG_BY_TASK

for task in xfstests linux pjdfstest; do
  if ! contains_task "$task"; then
    continue
  fi
  sprite_name="$(derive_sprite_name "$RAW_BASE" "validate-$task")"
  SPRITE_BY_TASK["$task"]="$sprite_name"
  LOG_BY_TASK["$task"]="$LOG_DIR/${task}.log"
  ensure_sprite "$sprite_name"
done

if [[ "$SYNC_REPO" -eq 1 ]]; then
  for task in "${!SPRITE_BY_TASK[@]}"; do
    echo "Syncing repo to ${SPRITE_BY_TASK[$task]} ($task)"
    sync_repo_to_sprite "${SPRITE_BY_TASK[$task]}"
  done
fi

launch_task() {
  local task="$1"
  local sprite="$2"
  local log_path="$3"
  local cmd=""
  case "$task" in
    xfstests) cmd="$(run_xfstests_cmd)" ;;
    linux) cmd="$(run_linux_cmd)" ;;
    pjdfstest) cmd="$(run_pjdfstest_cmd)" ;;
    *) echo "Unknown task $task" >&2; return 1 ;;
  esac

  (
    echo "[$task] sprite=$sprite"
    sprite exec -s "$sprite" -- bash -lc "$cmd"
  ) >"$log_path" 2>&1 &
  PID_BY_TASK["$task"]=$!
}

for task in "${!SPRITE_BY_TASK[@]}"; do
  launch_task "$task" "${SPRITE_BY_TASK[$task]}" "${LOG_BY_TASK[$task]}"
done

overall_rc=0
for task in "${!PID_BY_TASK[@]}"; do
  pid="${PID_BY_TASK[$task]}"
  if wait "$pid"; then
    echo "[PASS] $task (log: ${LOG_BY_TASK[$task]})"
  else
    rc=$?
    overall_rc=1
    echo "[FAIL] $task rc=$rc (log: ${LOG_BY_TASK[$task]})"
  fi
done

echo
echo "Sprite assignment:"
for task in "${!SPRITE_BY_TASK[@]}"; do
  echo "  $task -> ${SPRITE_BY_TASK[$task]}"
done

exit "$overall_rc"
