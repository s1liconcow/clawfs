#!/usr/bin/env bash
set -x
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "$0")/.." && pwd)"
BASELINE="${BASELINE:-$ROOT/bench-artifacts/perf_guard_baseline.json}"
METRICS_FILE="${METRICS_FILE:-$ROOT/bench-artifacts/perf_metrics.current.jsonl}"
COMMIT_SHORT="$(git -C "$ROOT" rev-parse --short=5 HEAD)"
GRAPH_ROOT="${PERF_GUARD_GRAPH_ROOT:-$ROOT/bench-artifacts/perf_guard_graphs}"
RUN_DATE="$(date +%Y%m%d)"
GRAPH_DIR="${PERF_GUARD_GRAPH_DIR:-$GRAPH_ROOT/${RUN_DATE}-${COMMIT_SHORT}}"
PERF_GUARD_MODE="${PERF_GUARD_MODE:-sprite}" # sprite|local
WORKDIR_REMOTE="${WORKDIR_REMOTE:-/work/clawfs}"
PERF_GUARD_SKIP_BOOTSTRAP="${PERF_GUARD_SKIP_BOOTSTRAP:-0}"
PERF_GUARD_SPRITE_SYNC="${PERF_GUARD_SPRITE_SYNC:-1}"
PERF_GUARD_SPRITE_NAME="${PERF_GUARD_SPRITE_NAME:-}"
REMOTE_TARGET_DIR="${PERF_GUARD_REMOTE_TARGET_DIR:-$WORKDIR_REMOTE/target}"
REMOTE_METRICS_FILE="${PERF_GUARD_REMOTE_METRICS_FILE:-$WORKDIR_REMOTE/bench-artifacts/perf_metrics.current.jsonl}"
PERF_GUARD_PROFILE="${PERF_GUARD_PROFILE:-release}" # release|debug
CLAWFS_PERF_PROFILE="${CLAWFS_PERF_PROFILE:-fast}" # fast|balanced|thorough
CLAWFS_PERF_SUITE="${CLAWFS_PERF_SUITE:-core}" # core|full
PERF_GUARD_WRITE_GRAPHS="${PERF_GUARD_WRITE_GRAPHS:-1}"

if [[ -n "${CARGO_TARGET_DIR:-}" && "$CARGO_TARGET_DIR" = /* ]]; then
  TARGET_DIR="$CARGO_TARGET_DIR"
else
  TARGET_DIR="$ROOT/${CARGO_TARGET_DIR:-target}"
fi
CRITERION_DIR="$TARGET_DIR/criterion"
CRITERION_REPORT_INDEX="$CRITERION_DIR/report/index.html"

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
  repo_slug="$(slugify "$(basename "$(git -C "$ROOT" rev-parse --show-toplevel)")")"
  branch_slug="$(slugify "$(git -C "$ROOT" rev-parse --abbrev-ref HEAD)")"
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

ensure_sprite() {
  local sprite_name="$1"
  if ! sprite list | rg -qx "$sprite_name"; then
    echo "[perf-guard] creating sprite $sprite_name"
    sprite create --skip-console "$sprite_name" >/dev/null
  fi
}

sync_repo_to_sprite() {
  local sprite_name="$1"
  tar \
    --exclude='.git' \
    --exclude='target' \
    --exclude='linux-local' \
    --exclude='out' \
    --exclude='fio-results-*' \
    --exclude='bench-artifacts/perf_guard_graphs' \
    --exclude='*.log' \
    -czf - . \
    | sprite exec -s "$sprite_name" -- bash -lc "rm -rf '$WORKDIR_REMOTE' && mkdir -p '$WORKDIR_REMOTE' && tar -xzf - -C '$WORKDIR_REMOTE'"
}

bootstrap_perf_cmd() {
  cat <<'EOF'
set -euo pipefail
sudo apt-get update
sudo apt-get install -y \
  bash coreutils findutils procps psmisc tar gzip xz-utils curl git python3 \
  pkg-config libfontconfig1-dev
EOF
}

run_local_bench() {
  (
    cd "$ROOT"
    if [[ "$PERF_GUARD_PROFILE" == "release" ]]; then
      CLAWFS_PERF_PROFILE="$CLAWFS_PERF_PROFILE" \
      CLAWFS_PERF_SUITE="$CLAWFS_PERF_SUITE" \
      CLAWFS_BENCH_METRICS_FILE="$METRICS_FILE" \
      cargo bench --bench perf_local_criterion
    else
      CLAWFS_PERF_PROFILE="$CLAWFS_PERF_PROFILE" \
      CLAWFS_PERF_SUITE="$CLAWFS_PERF_SUITE" \
      CLAWFS_BENCH_METRICS_FILE="$METRICS_FILE" \
      cargo bench --profile dev --bench perf_local_criterion
    fi
  )
}

run_sprite_bench() {
  local sprite_name="$1"
  local remote_cmd
  remote_cmd="$(cat <<EOF
set -euo pipefail
cd '$WORKDIR_REMOTE'
if [[ '$PERF_GUARD_SKIP_BOOTSTRAP' != '1' ]]; then
$(bootstrap_perf_cmd)
fi
mkdir -p '$(dirname "$REMOTE_METRICS_FILE")'
rm -f '$REMOTE_METRICS_FILE'
rm -rf '$REMOTE_TARGET_DIR/criterion'
if [[ '$PERF_GUARD_PROFILE' == 'release' ]]; then
  CLAWFS_PERF_PROFILE='$CLAWFS_PERF_PROFILE' \
  CLAWFS_PERF_SUITE='$CLAWFS_PERF_SUITE' \
  CLAWFS_BENCH_METRICS_FILE='$REMOTE_METRICS_FILE' \
  CARGO_TARGET_DIR='$REMOTE_TARGET_DIR' \
  cargo bench --bench perf_local_criterion
else
  CLAWFS_PERF_PROFILE='$CLAWFS_PERF_PROFILE' \
  CLAWFS_PERF_SUITE='$CLAWFS_PERF_SUITE' \
  CLAWFS_BENCH_METRICS_FILE='$REMOTE_METRICS_FILE' \
  CARGO_TARGET_DIR='$REMOTE_TARGET_DIR' \
  cargo bench --profile dev --bench perf_local_criterion
fi
EOF
)"
  sprite exec -s "$sprite_name" -- bash -lc "$remote_cmd"
}

fetch_file_from_sprite() {
  local sprite_name="$1"
  local remote_path="$2"
  local local_path="$3"
  mkdir -p "$(dirname "$local_path")"
  sprite exec -s "$sprite_name" -- bash -lc "cat '$remote_path'" >"$local_path"
}

copy_criterion_from_sprite() {
  local sprite_name="$1"
  local remote_dir="$2"
  local local_dir="$3"
  if sprite exec -s "$sprite_name" -- bash -lc "[ -d '$remote_dir' ]"; then
    rm -rf "$local_dir"
    mkdir -p "$local_dir"
    sprite exec -s "$sprite_name" -- bash -lc "cd '$remote_dir' && tar -czf - ." \
      | tar -xzf - -C "$local_dir"
    echo "[perf-guard] criterion html report -> $local_dir/report/index.html"
  else
    echo "[perf-guard] warning: criterion report not found in sprite: $remote_dir" >&2
  fi
}

rm -f "$METRICS_FILE"

echo "[perf-guard] collecting perf metrics (mode=$PERF_GUARD_MODE, profile=$PERF_GUARD_PROFILE, rigor=$CLAWFS_PERF_PROFILE, suite=$CLAWFS_PERF_SUITE)"

if [[ "$PERF_GUARD_MODE" == "local" ]]; then
  run_local_bench
elif [[ "$PERF_GUARD_MODE" == "sprite" ]]; then
  require_cmd sprite
  require_cmd rg
  require_cmd tar
  require_cmd sha1sum

  if ! sprite list >/dev/null 2>&1; then
    echo "sprite is not authenticated. Set SPRITES_TOKEN or run 'sprite login' first." >&2
    exit 1
  fi

  if [[ -z "$PERF_GUARD_SPRITE_NAME" ]]; then
    PERF_GUARD_SPRITE_NAME="$(derive_sprite_name "$(derive_raw_base)" "perf-guard")"
  fi

  echo "[perf-guard] sprite=$PERF_GUARD_SPRITE_NAME workdir=$WORKDIR_REMOTE"
  ensure_sprite "$PERF_GUARD_SPRITE_NAME"
  if [[ "$PERF_GUARD_SPRITE_SYNC" == "1" ]]; then
    echo "[perf-guard] syncing repo to sprite"
    sync_repo_to_sprite "$PERF_GUARD_SPRITE_NAME"
  fi
  run_sprite_bench "$PERF_GUARD_SPRITE_NAME"
  fetch_file_from_sprite "$PERF_GUARD_SPRITE_NAME" "$REMOTE_METRICS_FILE" "$METRICS_FILE"
else
  echo "invalid PERF_GUARD_MODE=$PERF_GUARD_MODE (expected sprite|local)" >&2
  exit 1
fi

if [[ "$PERF_GUARD_WRITE_GRAPHS" == "1" ]] && [[ "$PERF_GUARD_MODE" == "sprite" ]]; then
  copy_criterion_from_sprite "$PERF_GUARD_SPRITE_NAME" "$REMOTE_TARGET_DIR/criterion" "$GRAPH_DIR"
elif [[ "$PERF_GUARD_WRITE_GRAPHS" == "1" ]] && [[ -f "$CRITERION_REPORT_INDEX" ]]; then
  rm -rf "$GRAPH_DIR"
  mkdir -p "$GRAPH_DIR"
  cp -a "$CRITERION_DIR"/. "$GRAPH_DIR"/
  echo "[perf-guard] criterion html report -> $GRAPH_DIR/report/index.html"
elif [[ "$PERF_GUARD_WRITE_GRAPHS" != "1" ]]; then
  echo "[perf-guard] skipping graph copy (PERF_GUARD_WRITE_GRAPHS=$PERF_GUARD_WRITE_GRAPHS)"
else
  echo "[perf-guard] warning: criterion report not found: $CRITERION_REPORT_INDEX"
fi

echo "[perf-guard] checking baseline -> $BASELINE"
python3 "$ROOT/scripts/check_perf_guard.py" \
  --baseline "$BASELINE" \
  --metrics "$METRICS_FILE"
