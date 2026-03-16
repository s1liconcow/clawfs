#!/usr/bin/env bash
set -euo pipefail

# profile_daemon.sh — Profile the ClawFS FUSE daemon during a workload.
#
# Starts ClawFS with --perf-log, runs a user-specified workload while
# perf-recording the daemon PID, then generates analysis reports.
#
# Usage:
#   ./scripts/profile_daemon.sh                          # default: linux untar+compile
#   WORKLOAD=untar_only ./scripts/profile_daemon.sh      # just untar (skip compile)
#   WORKLOAD=custom CUSTOM_CMD="fio ..." ./scripts/profile_daemon.sh
#   PERF_FREQ=499 PERF_CALLGRAPH=fp ./scripts/profile_daemon.sh
#
# Environment knobs:
#   WORKLOAD          untar_compile (default) | untar_only | custom
#   CUSTOM_CMD        shell command to run when WORKLOAD=custom (cwd = mount)
#   RESULTS_DIR       where reports go (default: $ROOT/profile-results-<timestamp>)
#   PERF_FREQ         sampling frequency (default: 997)
#   PERF_CALLGRAPH    fp | dwarf (default: fp — much smaller perf.data)
#   PERF_DWARF_SIZE   dwarf stack dump size (default: 16384, only when callgraph=dwarf)
#   DISABLE_PERF      1 to skip perf record entirely (still get JSONL perf log)
#   LINUX_TARBALL     path to pre-downloaded linux tarball
#   LINUX_VERSION     e.g. "6.1" (default)
#   MAKE_JOBS         parallelism for kernel build (default: nproc)
#   EXTRA_CLAWFS_ARGS  extra flags for clawfs daemon
#   DEBUG_SYMBOLS     1 to build with CARGO_PROFILE_RELEASE_DEBUG=2 (default: 1)

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/common.sh"
osage_set_defaults

# ── Configuration ──────────────────────────────────────────────────────────────

WORKLOAD="${WORKLOAD:-untar_compile}"
CUSTOM_CMD="${CUSTOM_CMD:-}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RESULTS_DIR="${RESULTS_DIR:-$ROOT_DIR/profile-results-$TIMESTAMP}"
PERF_FREQ="${PERF_FREQ:-997}"
PERF_CALLGRAPH="${PERF_CALLGRAPH:-fp}"
PERF_DWARF_SIZE="${PERF_DWARF_SIZE:-16384}"
DISABLE_PERF="${DISABLE_PERF:-0}"
LINUX_VERSION="${LINUX_VERSION:-6.1}"
LINUX_TARBALL="${LINUX_TARBALL:-/tmp/linux-${LINUX_VERSION}.tar.gz}"
MAKE_JOBS="${MAKE_JOBS:-$(nproc)}"
EXTRA_CLAWFS_ARGS="${EXTRA_CLAWFS_ARGS:-}"
DEBUG_SYMBOLS="${DEBUG_SYMBOLS:-1}"

PERF_DATA="$RESULTS_DIR/perf.data"
PERF_LOG="$RESULTS_DIR/clawfs-perf.jsonl"
DAEMON_LOG="$RESULTS_DIR/clawfs.log"

OSAGE_PID=""
PERF_PID=""

# ── Helpers ────────────────────────────────────────────────────────────────────

cleanup() {
  cd /tmp
  [[ -n "$PERF_PID" ]] && kill -INT "$PERF_PID" 2>/dev/null && wait "$PERF_PID" 2>/dev/null || true
  [[ -n "$OSAGE_PID" ]] && kill "$OSAGE_PID" 2>/dev/null && wait "$OSAGE_PID" 2>/dev/null || true
  fusermount -u "$MOUNT_PATH" 2>/dev/null || umount -l "$MOUNT_PATH" 2>/dev/null || true
}
trap cleanup EXIT

log() { echo "=== $* ===" >&2; }

record_timing() {
  local label="$1" ms="$2"
  echo "${label}_ms=${ms}" >> "$RESULTS_DIR/timings.txt"
  local secs
  secs=$(awk "BEGIN{printf \"%.1f\", $ms/1000}")
  log "$label: ${secs}s (${ms}ms)"
}

ns_now() { date +%s%N; }
elapsed_ms() { echo $(( ($1 - $2) / 1000000 )); }

# ── Build ──────────────────────────────────────────────────────────────────────

log "Building ClawFS"
if [[ "$DEBUG_SYMBOLS" == "1" ]]; then
  export CARGO_PROFILE_RELEASE_DEBUG=2
fi
osage_ensure_release_binary

# ── Setup ──────────────────────────────────────────────────────────────────────

mkdir -p "$RESULTS_DIR"
log "Results will be written to $RESULTS_DIR"

# Clean previous state
cd /tmp
fusermount -u "$MOUNT_PATH" 2>/dev/null || umount -l "$MOUNT_PATH" 2>/dev/null || true
pkill -f "clawfsd.*--mount-path $MOUNT_PATH" 2>/dev/null || true
sleep 1
rm -rf "$MOUNT_PATH" "$STORE_PATH" "${LOCAL_CACHE_PATH}" "${STATE_PATH}"
mkdir -p "$MOUNT_PATH" "$STORE_PATH"

# Ensure /etc/fuse.conf allows allow_other
if ! grep -q user_allow_other /etc/fuse.conf 2>/dev/null; then
  echo "user_allow_other" | sudo tee -a /etc/fuse.conf >/dev/null
fi

# ── Start daemon ───────────────────────────────────────────────────────────────

log "Starting ClawFS daemon"
# shellcheck disable=SC2086
"$ROOT_DIR/target/release/clawfsd" \
  --mount-path "$MOUNT_PATH" \
  --store-path "$STORE_PATH" \
  --local-cache-path "$LOCAL_CACHE_PATH" \
  --state-path "$STATE_PATH" \
  --perf-log "$PERF_LOG" \
  --log-file "$DAEMON_LOG" \
  --allow-other \
  --disable-cleanup \
  --foreground \
  $EXTRA_CLAWFS_ARGS &
OSAGE_PID=$!

for i in $(seq 1 30); do
  [[ -f "$MOUNT_PATH/WELCOME.txt" ]] && break
  sleep 1
done
if [[ ! -f "$MOUNT_PATH/WELCOME.txt" ]]; then
  echo "FATAL: mount did not come up after 30s" >&2
  exit 1
fi
log "Mount ready (PID=$OSAGE_PID)"

# ── Start perf ─────────────────────────────────────────────────────────────────

if [[ "$DISABLE_PERF" != "1" ]] && command -v perf >/dev/null 2>&1; then
  log "Starting perf record (freq=$PERF_FREQ, callgraph=$PERF_CALLGRAPH)"
  perf_cg_args=()
  if [[ "$PERF_CALLGRAPH" == "dwarf" ]]; then
    perf_cg_args=(--call-graph "dwarf,$PERF_DWARF_SIZE")
  else
    perf_cg_args=(--call-graph fp)
  fi
  perf record -g -F "$PERF_FREQ" "${perf_cg_args[@]}" \
    -p "$OSAGE_PID" -o "$PERF_DATA" &
  PERF_PID=$!
else
  log "perf not available or disabled; skipping CPU profiling (JSONL perf log still active)"
fi

# ── Workload ───────────────────────────────────────────────────────────────────

run_untar() {
  if [[ ! -f "$LINUX_TARBALL" ]]; then
    log "Downloading linux-${LINUX_VERSION} tarball"
    curl -sL "https://cdn.kernel.org/pub/linux/kernel/v${LINUX_VERSION%%.*}.x/linux-${LINUX_VERSION}.tar.gz" \
      -o "$LINUX_TARBALL"
  fi
  log "Phase: untar ($(du -h "$LINUX_TARBALL" | cut -f1))"
  local t0; t0=$(ns_now)
  cd "$MOUNT_PATH"
  tar -xzf "$LINUX_TARBALL"
  local t1; t1=$(ns_now)
  record_timing "untar" "$(elapsed_ms "$t1" "$t0")"
  LINUX_DIR=$(find "$MOUNT_PATH" -maxdepth 1 -name "linux-*" -type d | head -1)
}

run_defconfig() {
  log "Phase: make defconfig"
  local t0; t0=$(ns_now)
  cd "$LINUX_DIR"
  make defconfig 2>&1 | tail -1
  local t1; t1=$(ns_now)
  record_timing "defconfig" "$(elapsed_ms "$t1" "$t0")"
}

run_build() {
  log "Phase: make -j${MAKE_JOBS}"
  local t0; t0=$(ns_now)
  cd "$LINUX_DIR"
  make -j"$MAKE_JOBS" 2>&1 | tail -3
  local rc=$?
  local t1; t1=$(ns_now)
  record_timing "build" "$(elapsed_ms "$t1" "$t0")"
  echo "build_exit=$rc" >> "$RESULTS_DIR/timings.txt"
  echo "make_jobs=$MAKE_JOBS" >> "$RESULTS_DIR/timings.txt"
}

case "$WORKLOAD" in
  untar_compile)
    run_untar
    run_defconfig
    run_build
    ;;
  untar_only)
    run_untar
    ;;
  custom)
    if [[ -z "$CUSTOM_CMD" ]]; then
      echo "CUSTOM_CMD is required when WORKLOAD=custom" >&2
      exit 1
    fi
    log "Phase: custom workload"
    local t0; t0=$(ns_now)
    cd "$MOUNT_PATH"
    eval "$CUSTOM_CMD"
    local t1; t1=$(ns_now)
    record_timing "custom" "$(elapsed_ms "$t1" "$t0")"
    ;;
  *)
    echo "Unknown WORKLOAD=$WORKLOAD (expected: untar_compile, untar_only, custom)" >&2
    exit 1
    ;;
esac

# ── Stop profiling ─────────────────────────────────────────────────────────────

cd /tmp

if [[ -n "$PERF_PID" ]]; then
  log "Stopping perf"
  kill -INT "$PERF_PID" 2>/dev/null
  wait "$PERF_PID" 2>/dev/null || true
  PERF_PID=""
fi

# ── Stop daemon ────────────────────────────────────────────────────────────────

log "Stopping ClawFS"
kill "$OSAGE_PID" 2>/dev/null
wait "$OSAGE_PID" 2>/dev/null || true
OSAGE_PID=""
fusermount -u "$MOUNT_PATH" 2>/dev/null || umount -l "$MOUNT_PATH" 2>/dev/null || true

# ── Generate reports ───────────────────────────────────────────────────────────

log "Generating analysis reports"

# 1. Perf reports (if perf.data exists and is non-trivial)
if [[ -f "$PERF_DATA" ]] && [[ "$(stat -c%s "$PERF_DATA" 2>/dev/null || echo 0)" -gt 1000 ]]; then
  perf_size=$(du -h "$PERF_DATA" | cut -f1)
  echo "perf_data_size=$perf_size" >> "$RESULTS_DIR/timings.txt"

  perf report -i "$PERF_DATA" --stdio --no-children -g none --percent-limit 0.5 \
    > "$RESULTS_DIR/perf_flat.txt" 2>/dev/null || true

  perf report -i "$PERF_DATA" --stdio --no-children -g fractal,5 --percent-limit 2.0 \
    > "$RESULTS_DIR/perf_callgraph.txt" 2>/dev/null || true

  perf report -i "$PERF_DATA" --stdio --no-children --sort=dso --percent-limit 0.1 \
    > "$RESULTS_DIR/perf_by_dso.txt" 2>/dev/null || true
fi

# 2. JSONL perf log analysis
if [[ -f "$PERF_LOG" ]]; then
  python3 - "$PERF_LOG" "$RESULTS_DIR/flush_analysis.txt" << 'PYEOF'
import sys, json

perf_log = sys.argv[1]
out_path = sys.argv[2]

flush_events = []
stage_durations = []
for line in open(perf_log):
    try:
        e = json.loads(line)
    except Exception:
        continue
    ev = e.get("event")
    if ev == "flush_pending":
        flush_events.append(e)
    elif ev == "stage_file":
        stage_durations.append(e.get("duration_ms", 0))

lines = []
def p(s=""): lines.append(s)

# ── flush_pending analysis ──
if flush_events:
    durations = sorted(e.get("duration_ms", 0) for e in flush_events)
    n = len(durations)
    p(f"flush_pending: {n} events")
    p(f"  min={durations[0]:.0f}ms  median={durations[n//2]:.0f}ms  "
      f"p95={durations[int(n*0.95)]:.0f}ms  p99={durations[int(n*0.99)]:.0f}ms  "
      f"max={durations[-1]:.0f}ms")
    p(f"  total_wall={sum(durations)/1000:.1f}s")
    p()

    # Step breakdown (averages and maxes)
    step_data = {}
    for e in flush_events:
        d = e.get("details", {})
        for k, v in d.items():
            if k.endswith("_ms"):
                step_data.setdefault(k, []).append(v)
    p("  Step breakdown (avg / max / total):")
    for k in sorted(step_data, key=lambda x: -sum(step_data[x])):
        vals = step_data[k]
        total = sum(vals)
        if total < 1:
            continue
        p(f"    {k:40s}  avg={total/len(vals):8.1f}ms  "
          f"max={max(vals):8.0f}ms  total={total/1000:8.1f}s")
    p()

    # Top 5 slowest flushes
    ranked = sorted(flush_events, key=lambda e: -e.get("duration_ms", 0))
    p("  Top 5 slowest flushes:")
    for e in ranked[:5]:
        dur = e.get("duration_ms", 0)
        d = e.get("details", {})
        top_steps = sorted(
            ((k, v) for k, v in d.items() if k.endswith("_ms") and v > 1),
            key=lambda x: -x[1]
        )[:5]
        step_str = ", ".join(f"{k}={v:.0f}" for k, v in top_steps)
        p(f"    {dur:.0f}ms: {step_str}")
    p()

# ── stage_file analysis ──
if stage_durations:
    stage_durations.sort()
    n = len(stage_durations)
    p(f"stage_file: {n} events")
    p(f"  min={stage_durations[0]:.1f}ms  median={stage_durations[n//2]:.1f}ms  "
      f"p95={stage_durations[int(n*0.95)]:.1f}ms  p99={stage_durations[int(n*0.99)]:.1f}ms  "
      f"max={stage_durations[-1]:.0f}ms")
    p(f"  total_wall={sum(stage_durations)/1000:.1f}s")

with open(out_path, "w") as f:
    f.write("\n".join(lines) + "\n")
print(f"Wrote {out_path}")
PYEOF
fi

# 3. Compaction error count from daemon log
if [[ -f "$DAEMON_LOG" ]]; then
  compact_errors=$(grep -c "segment compaction failed" "$DAEMON_LOG" 2>/dev/null || echo 0)
  echo "segment_compaction_errors=$compact_errors" >> "$RESULTS_DIR/timings.txt"
fi

# ── Summary ────────────────────────────────────────────────────────────────────

log "Profile complete"
echo ""
echo "Results directory: $RESULTS_DIR"
echo ""
cat "$RESULTS_DIR/timings.txt"
echo ""
if [[ -f "$RESULTS_DIR/flush_analysis.txt" ]]; then
  echo "--- Flush Analysis ---"
  cat "$RESULTS_DIR/flush_analysis.txt"
fi
echo ""
echo "Files:"
ls -lh "$RESULTS_DIR/"
