#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "$0")/.." && pwd)"
BASELINE="${BASELINE:-$ROOT/bench-artifacts/perf_guard_baseline.json}"
METRICS_FILE="${METRICS_FILE:-$ROOT/bench-artifacts/perf_metrics.current.jsonl}"
PERF_GUARD_ITERS="${PERF_GUARD_ITERS:-3}"
PERF_GUARD_PROFILE="${PERF_GUARD_PROFILE:-release}" # release|debug
PERF_GUARD_WARMUP="${PERF_GUARD_WARMUP:-1}"

rm -f "$METRICS_FILE"
for i in $(seq 1 "$PERF_GUARD_ITERS"); do
  rm -f "${METRICS_FILE%.jsonl}.iter${i}.jsonl"
done

echo "[perf-guard] collecting perf metrics ($PERF_GUARD_ITERS iterations, profile=$PERF_GUARD_PROFILE, warmup=$PERF_GUARD_WARMUP)"
if [[ "$PERF_GUARD_WARMUP" -gt 0 ]]; then
  echo "[perf-guard] warmup run (discarded)"
  (
    cd "$ROOT"
    if [[ "$PERF_GUARD_PROFILE" == "release" ]]; then
      OSAGEFS_RUN_PERF=1 cargo test --release --test perf_local -- --nocapture --skip segment_sequential_read_throughput >/dev/null
    else
      OSAGEFS_RUN_PERF=1 cargo test --test perf_local -- --nocapture --skip segment_sequential_read_throughput >/dev/null
    fi
  )
fi
for i in $(seq 1 "$PERF_GUARD_ITERS"); do
  iter_file="${METRICS_FILE%.jsonl}.iter${i}.jsonl"
  echo "[perf-guard] iteration $i/$PERF_GUARD_ITERS -> $iter_file"
  (
    cd "$ROOT"
    if [[ "$PERF_GUARD_PROFILE" == "release" ]]; then
      OSAGEFS_RUN_PERF=1 OSAGEFS_PERF_METRICS_FILE="$iter_file" \
        cargo test --release --test perf_local -- --nocapture --skip segment_sequential_read_throughput
    else
      OSAGEFS_RUN_PERF=1 OSAGEFS_PERF_METRICS_FILE="$iter_file" \
        cargo test --test perf_local -- --nocapture --skip segment_sequential_read_throughput
    fi
  )
done

python3 "$ROOT/scripts/aggregate_perf_metrics.py" \
  --out "$METRICS_FILE" \
  "${METRICS_FILE%.jsonl}".iter*.jsonl

echo "[perf-guard] checking baseline -> $BASELINE"
python3 "$ROOT/scripts/check_perf_guard.py" \
  --baseline "$BASELINE" \
  --metrics "$METRICS_FILE"
