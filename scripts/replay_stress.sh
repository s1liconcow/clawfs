#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "$0")/.." && pwd)

CHAOS_MODE=0
EXTRA_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --chaos)
      CHAOS_MODE=1
      ;;
    --help|-h)
      cat <<'USAGE'
Usage: ./scripts/replay_stress.sh [--chaos] [-- <extra osagefs_replay args>]

Environment overrides:
  TRACE_PATH STORE_PATH LOCAL_CACHE_PATH STATE_PATH
  LAYER ITERATIONS SEED IGNORE_TIMING
  JITTER_US CHAOS_SLEEP_PROB CHAOS_SLEEP_MAX_US CHAOS_FLUSH_PROB
  MAX_MISMATCH_LOGS HOME_PREFIX USER_NAME USE_TRACE_CONFIG

`--chaos` preset (if vars are not already set):
  ITERATIONS=20
  IGNORE_TIMING=0
  JITTER_US=5000
  CHAOS_SLEEP_PROB=0.05
  CHAOS_SLEEP_MAX_US=20000
  CHAOS_FLUSH_PROB=0.03
USAGE
      exit 0
      ;;
    *)
      EXTRA_ARGS+=("$arg")
      ;;
  esac
done

TRACE_PATH="${TRACE_PATH:-}"
if [[ -z "$TRACE_PATH" ]]; then
  TRACE_PATH=$(ls -1t "$ROOT_DIR"/replays/*.jsonl.gz 2>/dev/null | head -n1 || true)
fi
if [[ -z "$TRACE_PATH" ]]; then
  TRACE_PATH=$(ls -1t "$ROOT_DIR"/replays/*.partial.jsonl 2>/dev/null | head -n1 || true)
fi
if [[ -z "$TRACE_PATH" || ! -f "$TRACE_PATH" ]]; then
  echo "No replay trace found. Set TRACE_PATH=/path/to/trace.{jsonl.gz,partial.jsonl}" >&2
  exit 1
fi

STORE_PATH="${STORE_PATH:-/tmp/osagefs-replay-store}"
LOCAL_CACHE_PATH="${LOCAL_CACHE_PATH:-/tmp/osagefs-replay-cache}"
STATE_PATH="${STATE_PATH:-/tmp/osagefs-replay-state.bin}"
LAYER="${LAYER:-fuse}"
ITERATIONS_SET="${ITERATIONS+x}"
IGNORE_TIMING_SET="${IGNORE_TIMING+x}"
JITTER_US_SET="${JITTER_US+x}"
CHAOS_SLEEP_PROB_SET="${CHAOS_SLEEP_PROB+x}"
CHAOS_SLEEP_MAX_US_SET="${CHAOS_SLEEP_MAX_US+x}"
CHAOS_FLUSH_PROB_SET="${CHAOS_FLUSH_PROB+x}"
ITERATIONS="${ITERATIONS:-1}"
SEED="${SEED:-}"
IGNORE_TIMING="${IGNORE_TIMING:-1}"
JITTER_US="${JITTER_US:-0}"
CHAOS_SLEEP_PROB="${CHAOS_SLEEP_PROB:-0.0}"
CHAOS_SLEEP_MAX_US="${CHAOS_SLEEP_MAX_US:-0}"
CHAOS_FLUSH_PROB="${CHAOS_FLUSH_PROB:-0.0}"
MAX_MISMATCH_LOGS="${MAX_MISMATCH_LOGS:-200}"
HOME_PREFIX="${HOME_PREFIX:-}"
USER_NAME="${USER_NAME:-}"
USE_TRACE_CONFIG="${USE_TRACE_CONFIG:-1}"

if [[ "$CHAOS_MODE" == "1" ]]; then
  [[ -z "$ITERATIONS_SET" ]] && ITERATIONS=20
  [[ -z "$IGNORE_TIMING_SET" ]] && IGNORE_TIMING=0
  [[ -z "$JITTER_US_SET" ]] && JITTER_US=5000
  [[ -z "$CHAOS_SLEEP_PROB_SET" ]] && CHAOS_SLEEP_PROB=0.05
  [[ -z "$CHAOS_SLEEP_MAX_US_SET" ]] && CHAOS_SLEEP_MAX_US=20000
  [[ -z "$CHAOS_FLUSH_PROB_SET" ]] && CHAOS_FLUSH_PROB=0.03
fi

CMD=(
  cargo run --release --bin osagefs_replay -- --trace-path "$TRACE_PATH"
  --store-path "$STORE_PATH"
  --local-cache-path "$LOCAL_CACHE_PATH"
  --state-path "$STATE_PATH"
  --layer "$LAYER"
  --iterations "$ITERATIONS"
  --jitter-us "$JITTER_US"
  --chaos-sleep-prob "$CHAOS_SLEEP_PROB"
  --chaos-sleep-max-us "$CHAOS_SLEEP_MAX_US"
  --chaos-flush-prob "$CHAOS_FLUSH_PROB"
  --max-mismatch-logs "$MAX_MISMATCH_LOGS"
)

if [[ "$IGNORE_TIMING" == "1" ]]; then
  CMD+=(--ignore-timing)
fi
if [[ "$USE_TRACE_CONFIG" == "1" ]]; then
  CMD+=(--use-trace-config)
fi
if [[ -n "$SEED" ]]; then
  CMD+=(--seed "$SEED")
fi
if [[ -n "$HOME_PREFIX" ]]; then
  CMD+=(--home-prefix "$HOME_PREFIX")
fi
if [[ -n "$USER_NAME" ]]; then
  CMD+=(--user-name "$USER_NAME")
fi

echo "Replay trace: $TRACE_PATH"
if [[ "$CHAOS_MODE" == "1" ]]; then
  echo "Chaos preset enabled"
fi
echo "Running: ${CMD[*]}"
cd "$ROOT_DIR"
"${CMD[@]}" "${EXTRA_ARGS[@]}"
