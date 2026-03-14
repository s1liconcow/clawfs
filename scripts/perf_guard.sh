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
if [[ -n "${CARGO_TARGET_DIR:-}" && "$CARGO_TARGET_DIR" = /* ]]; then
  TARGET_DIR="$CARGO_TARGET_DIR"
else
  TARGET_DIR="$ROOT/${CARGO_TARGET_DIR:-target}"
fi
CRITERION_DIR="$TARGET_DIR/criterion"
CRITERION_REPORT_INDEX="$CRITERION_DIR/report/index.html"
PERF_GUARD_PROFILE="${PERF_GUARD_PROFILE:-release}" # release|debug
CLAWFS_PERF_PROFILE="${CLAWFS_PERF_PROFILE:-fast}" # fast|balanced|thorough
PERF_GUARD_WRITE_GRAPHS="${PERF_GUARD_WRITE_GRAPHS:-1}"

rm -f "$METRICS_FILE"

echo "[perf-guard] collecting perf metrics (profile=$PERF_GUARD_PROFILE, rigor=$CLAWFS_PERF_PROFILE)"

(
  cd "$ROOT"
  if [[ "$PERF_GUARD_PROFILE" == "release" ]]; then
    CLAWFS_PERF_PROFILE="$CLAWFS_PERF_PROFILE" CLAWFS_BENCH_METRICS_FILE="$METRICS_FILE" \
      cargo bench --bench perf_local_criterion
  else
    CLAWFS_PERF_PROFILE="$CLAWFS_PERF_PROFILE" CLAWFS_BENCH_METRICS_FILE="$METRICS_FILE" \
      cargo bench --profile dev --bench perf_local_criterion
  fi
)

if [[ "$PERF_GUARD_WRITE_GRAPHS" == "1" ]] && [[ -f "$CRITERION_REPORT_INDEX" ]]; then
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
