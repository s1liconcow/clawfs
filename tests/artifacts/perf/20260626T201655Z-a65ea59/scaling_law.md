# Scaling Law - 20260626T201655Z-a65ea59

This pass did not execute a fresh scaling sweep. Existing evidence gives one
limited axis:

Axis: preload create/write/close, 100 operations, flush policy.

| Mode | Mean per 100-op iteration | Relative |
|------|--------------------------:|---------:|
| no flush, prior full-suite artifact | 145.863570 ms | 1.00x |
| with flush, prior full-suite artifact | 369.311863 ms | 2.53x slower |
| with flush, current core artifact | 326.574380 ms | 2.24x slower vs prior no-flush artifact |

## Limitations

These values come from checked-in artifacts from different perf guard dates.
They are good enough to prioritize a profiling target, but not enough to claim a
new improvement or regression.

## Next Sweep

For a fresh scaling law, run the same harness across:

| Axis | Values |
|------|--------|
| operation count | 1, 10, 50, 100, 500, 1000 |
| flush policy | no flush, flush every op, flush every 10 ops, final flush only |
| payload size | 1 KiB, 4 KiB, 64 KiB, 1 MiB |

Report p50, p95, p99, throughput, peak RSS, and CPU profile for the chosen
optimization target.
