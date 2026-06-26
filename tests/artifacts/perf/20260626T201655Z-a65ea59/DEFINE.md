# DEFINE - perf guard baseline triage

## Scenario
Profile the current ClawFS performance surfaces by reusing the project-approved
sprite perf guard baseline from `bench-artifacts/perf_guard_baseline.json`
instead of running a fresh Criterion benchmark. The user asked to continue after
the profiling skill was selected, but AGENTS.md explicitly requires user
authorization before `cargo bench`, so this pass is evidence triage only.

## Metric
Core guard metrics:
- `untar_flush_latency_ms`
- `large_segment_staged_flush_mib_per_s`
- `segment_sequential_read_mib_per_s`
- `preload_create_write_close_iops_with_flush`

Supporting timing context comes from checked-in Criterion `estimates.json`
artifacts copied into `evidence/criterion-20260410/`.

## Budget
Use the checked-in 2026-04-10 guardrails:

| Metric | Direction | Baseline | Guard |
|--------|-----------|---------:|------:|
| `untar_flush_latency_ms` | lower | 7.561786 ms | <= 12.855036 ms |
| `large_segment_staged_flush_mib_per_s` | higher | 1880.170920 MiB/s | >= 564.051276 MiB/s |
| `segment_sequential_read_mib_per_s` | higher | 3592478.248667 MiB/s | >= 1077743.474600 MiB/s |
| `preload_create_write_close_iops_with_flush` | higher | 310.387004 IOPS | >= 93.116101 IOPS |

## Golden output
No fresh workload output was produced in this pass. The benchmark harness
contains internal assertions for the measured paths, and this artifact reuses
the committed guard baseline.

## Scope boundary
Out of scope:
- Fresh `cargo bench` run.
- OS tuning or sudo changes.
- FUSE daemon profile.
- CPU flamegraph or heap profile collection.
- Optimization/code changes.

## Variance envelope
- <= 10% drift vs prior same-host run: noise.
- > 10%: investigate.
- > 20%, or 3 consecutive > 10%: escalate.

## Stakeholder / requester
Requester: user invoked `$profiling-software-performance` and then asked to
continue. This run prepares a ranked, evidence-backed target list for a later
optimization pass.
