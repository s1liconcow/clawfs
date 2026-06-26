# Performance Budgets

Run-local budget mirror from `bench-artifacts/perf_guard_baseline.json`.

| Surface | Budget | Source |
|---------|-------:|--------|
| Untar flush latency | <= 12.855036 ms | `perf_guard_baseline.json` |
| Large staged segment flush | >= 564.051276 MiB/s | `perf_guard_baseline.json` |
| Segment sequential read | >= 1077743.474600 MiB/s | `perf_guard_baseline.json` |
| Preload create/write/close with flush | >= 93.116101 IOPS | `perf_guard_baseline.json` |

## Update Policy

Tighten these budgets only after a same-environment fresh perf guard run proves
a real improvement. Do not relax them without a written reason and a replacement
baseline artifact.
