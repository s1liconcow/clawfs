#!/usr/bin/env bash
set -x
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "$0")/.." && pwd)"
WORKDIR_REMOTE="${WORKDIR_REMOTE:-/work/clawfs}"
SYNC_REPO=1
SKIP_BOOTSTRAP=0
TASKS_CSV="linux,pjdfstest,stress-ng"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/validation-results-$(date +%Y%m%d-%H%M%S)}"

usage() {
  cat <<'USAGE'
Usage: scripts/sprite_validate_parallel.sh [options]

Run validation suites in parallel on dedicated sprites:
  - linux kernel compile perf script
  - pjdfstest
  - stress-ng CI repro

Options:
  --tasks <csv>        Comma-separated list: linux,pjdfstest,stress-ng
  --no-sync            Skip tar sync into sprites (assume /work/clawfs is current)
  --skip-bootstrap     Skip apt/bootstrap steps inside sprites
  --log-dir <path>     Local directory for per-task logs
  -h, --help           Show help

Examples:
  scripts/sprite_validate_parallel.sh
  scripts/sprite_validate_parallel.sh --tasks linux,pjdfstest,stress-ng
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
    sprite create --skip-console "$sprite" >/dev/null
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
cargo build --release --bin clawfsd
PJDFSTEST_DIR=/tmp/pjdfstest TESTDIR=/tmp/clawfs-mnt JOBS=8 ./scripts/run_pjdfstest.sh
EOF
}

run_stress_ng_cmd() {
  cat <<EOF
set -euo pipefail
cd '$WORKDIR_REMOTE'
if [[ "$SKIP_BOOTSTRAP" -eq 0 ]]; then
  $(bootstrap_common_cmd)
  sudo apt-get install -y stress-ng
fi
./scripts/repro_stress_ng_ci.sh
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

for task in linux pjdfstest stress-ng; do
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
    linux) cmd="$(run_linux_cmd)" ;;
    pjdfstest) cmd="$(run_pjdfstest_cmd)" ;;
    stress-ng) cmd="$(run_stress_ng_cmd)" ;;
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
