#!/usr/bin/env python3
import argparse
import json
import math
from pathlib import Path


def load_metrics(path: Path) -> dict[str, float]:
    metrics: dict[str, float] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            metrics[rec["metric"]] = float(rec["value"])
    return metrics


def main() -> int:
    p = argparse.ArgumentParser(description="Check perf metrics against baseline bounds.")
    p.add_argument("--baseline", required=True, type=Path)
    p.add_argument("--metrics", required=True, type=Path)
    args = p.parse_args()

    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    observed = load_metrics(args.metrics)
    spec = baseline["metrics"]

    failures: list[str] = []
    lines: list[str] = []
    for name, cfg in spec.items():
        if name not in observed:
            failures.append(f"missing metric: {name}")
            continue
        val = observed[name]
        base = float(cfg["value"])
        reg = float(cfg.get("max_regression_pct", 20.0)) / 100.0
        direction = cfg["direction"]
        if direction == "higher":
            floor = base * (1.0 - reg)
            ok = val >= floor
            lines.append(
                f"{name}: observed={val:.3f} baseline={base:.3f} min={floor:.3f} dir=higher"
            )
        elif direction == "lower":
            ceil = base * (1.0 + reg)
            ok = val <= ceil
            lines.append(
                f"{name}: observed={val:.3f} baseline={base:.3f} max={ceil:.3f} dir=lower"
            )
        else:
            failures.append(f"{name}: invalid direction {direction}")
            continue
        if not ok:
            failures.append(f"regression: {lines[-1]}")

    print("\n".join(lines))
    if failures:
        print("\nFAILURES:")
        print("\n".join(failures))
        return 1
    print("\nperf guard: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

