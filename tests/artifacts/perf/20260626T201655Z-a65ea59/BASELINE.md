# Baseline - perf guard baseline triage - 2026-06-26 - a65ea59

This pass reuses the committed sprite baseline refreshed on 2026-04-10.
It does not contain a fresh local benchmark run.

| Surface | Guard metric | Baseline value | Supporting Criterion mean |
|---------|--------------|---------------:|--------------------------:|
| Untar flush latency | `untar_flush_latency_ms` | 7.561786 ms | 8.826359 ms |
| Large staged segment flush | `large_segment_staged_flush_mib_per_s` | 1880.170920 MiB/s | 76.111356 ms per 128 MiB iteration |
| Segment sequential read | `segment_sequential_read_mib_per_s` | 3592478.248667 MiB/s | 0.036924 ms per 128 MiB iteration |
| Preload create/write/close with flush | `preload_create_write_close_iops_with_flush` | 310.387004 IOPS | 326.574380 ms per 100-op iteration |

## Run command

No fresh `cargo bench` command was run. Commands used:

```bash
bash /Users/david/.codex/skills/profiling-software-performance/scripts/env_fingerprint.sh --run-id 20260626T201655Z-a65ea59 --build-profile release > tests/artifacts/perf/20260626T201655Z-a65ea59/fingerprint.json
cp bench-artifacts/perf_guard_baseline.json tests/artifacts/perf/20260626T201655Z-a65ea59/perf_guard_baseline.json
```

## Evidence

- `perf_guard_baseline.json`
- `evidence/criterion-20260410/untar_flush_latency_estimates.json`
- `evidence/criterion-20260410/large_segment_staged_flush_latency_estimates.json`
- `evidence/criterion-20260410/segment_sequential_read_throughput_estimates.json`
- `evidence/criterion-20260410/create_write_close_iops_with_flush_estimates.json`

## Missing metrics

Fresh p50, p95, p99, peak RSS, CPU percent, heap high-watermark, and off-CPU
wait data were not collected because this pass did not run a new workload.
