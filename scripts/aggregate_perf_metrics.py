#!/usr/bin/env python3
import argparse
import json
import statistics
from pathlib import Path


def read_jsonl(path: Path) -> dict[str, float]:
    out: dict[str, float] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            out[rec["metric"]] = float(rec["value"])
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Aggregate perf metric JSONL files.")
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("inputs", nargs="+", type=Path)
    args = p.parse_args()

    series: dict[str, list[float]] = {}
    for path in args.inputs:
        current = read_jsonl(path)
        for k, v in current.items():
            series.setdefault(k, []).append(v)

    with args.out.open("w", encoding="utf-8") as out:
        for metric, values in sorted(series.items()):
            rec = {
                "metric": metric,
                "value": statistics.median(values),
                "samples": values,
                "n": len(values),
            }
            out.write(json.dumps(rec))
            out.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

