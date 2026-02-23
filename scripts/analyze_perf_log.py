#!/usr/bin/env python3
"""
Analyze OsageFS perf JSONL logs.

Focuses on `flush_pending` by default and reports:
- basic duration stats
- top-N slowest events
- step-level contribution summary for instrumented flush fields
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any


FLUSH_STEP_KEYS = [
    "flush_lock_wait_ms",
    "drain_select_ms",
    "rotate_stage_file_ms",
    "prepare_generation_ms",
    "classify_records_ms",
    "segment_write_ms",
    "merge_segment_extents_ms",
    "metadata_persist_only_ms",
    "metadata_sync_only_ms",
    "commit_ms",
    "finalize_ms",
]

FINALIZE_STEP_KEYS = [
    "finalize_pending_bytes_ms",
    "finalize_clear_flushing_ms",
    "finalize_clear_flushing_lock_wait_ms",
    "finalize_release_pending_data_ms",
    "finalize_journal_phase_ms",
    "finalize_journal_clear_ms",
]


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if p <= 0:
        return sorted_values[0]
    if p >= 100:
        return sorted_values[-1]
    rank = (p / 100.0) * (len(sorted_values) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return sorted_values[low]
    weight = rank - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def load_events(path: Path, event_name: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("event") != event_name:
                continue
            if "duration_ms" not in obj:
                continue
            obj["_line_no"] = line_no
            events.append(obj)
    return events


def fmt_ms(v: float) -> str:
    return f"{v:,.3f} ms"


def summarize_durations(events: list[dict[str, Any]]) -> None:
    durations = [float(e["duration_ms"]) for e in events]
    d_sorted = sorted(durations)
    print("Duration Summary")
    print(f"- count: {len(durations)}")
    print(f"- mean: {fmt_ms(statistics.fmean(durations))}")
    print(f"- p50 : {fmt_ms(percentile(d_sorted, 50))}")
    print(f"- p90 : {fmt_ms(percentile(d_sorted, 90))}")
    print(f"- p99 : {fmt_ms(percentile(d_sorted, 99))}")
    print(f"- max : {fmt_ms(max(durations))}")


def summarize_flush_steps(events: list[dict[str, Any]]) -> None:
    present = [k for k in FLUSH_STEP_KEYS if any(k in (e.get("details") or {}) for e in events)]
    if not present:
        print("\nNo instrumented flush step fields found in this log.")
        return

    step_totals: dict[str, float] = {k: 0.0 for k in present}
    total_duration = 0.0
    for e in events:
        details = e.get("details") or {}
        duration = float(e["duration_ms"])
        total_duration += duration
        for k in present:
            v = details.get(k)
            if isinstance(v, (int, float)):
                step_totals[k] += float(v)

    print("\nFlush Step Contribution (sum across events)")
    by_total = sorted(step_totals.items(), key=lambda kv: kv[1], reverse=True)
    for k, v in by_total:
        pct = (v / total_duration * 100.0) if total_duration > 0 else 0.0
        print(f"- {k}: {fmt_ms(v)} ({pct:.1f}% of total duration)")


def summarize_finalize_breakdown(events: list[dict[str, Any]]) -> None:
    finalize_values = [
        float((e.get("details") or {}).get("finalize_ms", 0.0))
        for e in events
        if isinstance((e.get("details") or {}).get("finalize_ms"), (int, float))
    ]
    if not finalize_values:
        return

    finalize_total = sum(finalize_values)
    if finalize_total <= 0:
        return

    present = [
        k
        for k in FINALIZE_STEP_KEYS
        if any(isinstance((e.get("details") or {}).get(k), (int, float)) for e in events)
    ]
    if not present:
        return

    totals: dict[str, float] = {k: 0.0 for k in present}
    for e in events:
        details = e.get("details") or {}
        for k in present:
            v = details.get(k)
            if isinstance(v, (int, float)):
                totals[k] += float(v)

    print("\nFinalize Breakdown (sum across events)")
    print(f"- finalize_total_ms: {fmt_ms(finalize_total)}")
    by_total = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)
    for k, v in by_total:
        pct_finalize = (v / finalize_total * 100.0) if finalize_total > 0 else 0.0
        print(f"- {k}: {fmt_ms(v)} ({pct_finalize:.1f}% of finalize)")


def show_top_events(events: list[dict[str, Any]], top_n: int) -> None:
    top = sorted(events, key=lambda e: float(e["duration_ms"]), reverse=True)[:top_n]
    print(f"\nTop {len(top)} Slowest Events")
    for idx, e in enumerate(top, 1):
        details = e.get("details") or {}
        duration = float(e["duration_ms"])
        ts = e.get("ts", "?")
        line_no = e.get("_line_no", "?")
        print(f"{idx}. ts={ts} duration={fmt_ms(duration)} line={line_no}")

        step_values: list[tuple[str, float]] = []
        for k in FLUSH_STEP_KEYS:
            v = details.get(k)
            if isinstance(v, (int, float)):
                step_values.append((k, float(v)))
        if step_values:
            step_values.sort(key=lambda kv: kv[1], reverse=True)
            step_sum = sum(v for _, v in step_values)
            unaccounted = max(0.0, duration - step_sum)
            top_steps = ", ".join(f"{k}={v:.2f}" for k, v in step_values[:4])
            print(f"   top_steps: {top_steps}")
            print(f"   accounted={fmt_ms(step_sum)} unaccounted={fmt_ms(unaccounted)}")

        seg_write = details.get("segment_write_ms")
        metadata = details.get("metadata_ms")
        commit = details.get("commit_ms")
        if isinstance(seg_write, (int, float)) and isinstance(metadata, (int, float)) and isinstance(commit, (int, float)):
            overhead = duration - float(seg_write) - float(metadata) - float(commit)
            print(
                "   coarse: "
                f"segment_write_ms={float(seg_write):.2f}, "
                f"metadata_ms={float(metadata):.2f}, "
                f"commit_ms={float(commit):.2f}, "
                f"overhead_ms={overhead:.2f}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze OsageFS perf JSONL logs")
    parser.add_argument(
        "--log",
        default="perf-log.jsonl",
        help="path to perf log JSONL (default: perf-log.jsonl)",
    )
    parser.add_argument(
        "--event",
        default="flush_pending",
        help="event name to analyze (default: flush_pending)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="number of slowest events to print (default: 10)",
    )
    args = parser.parse_args()

    path = Path(args.log)
    if not path.exists():
        print(f"error: log file not found: {path}", file=sys.stderr)
        return 2

    events = load_events(path, args.event)
    if not events:
        print(f"no events named '{args.event}' in {path}")
        return 1

    print(f"Analyzing {len(events)} '{args.event}' events from {path}")
    summarize_durations(events)
    if args.event == "flush_pending":
        summarize_flush_steps(events)
        summarize_finalize_breakdown(events)
    show_top_events(events, args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
