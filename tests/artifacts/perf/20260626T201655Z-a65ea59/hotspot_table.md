# Hotspot Table - 20260626T201655Z-a65ea59

Rows are ranked by likely optimization leverage for the current core suite, not
by regression severity. Evidence is from committed perf artifacts and benchmark
source mapping; no fresh flamegraph was collected.

| Rank | Location | Metric | Value | Category | Evidence |
|-----:|----------|--------|------:|----------|----------|
| 1 | `run_preload_create_write_close_iops_once`: `nfs_create` -> `nfs_write` -> per-file `nfs_flush` | Throughput | 310.387004 IOPS; 326.574380 ms per 100-op iteration | metadata + flush I/O | `perf_guard_baseline.json`; `evidence/criterion-20260410/create_write_close_iops_with_flush_estimates.json`; `benches/perf_local_criterion.rs:590` |
| 2 | `run_untar_flush_latency_once`: inline encode + `persist_inodes_batch` + `sync_metadata_writes` + `commit_generation` | Latency | 7.561786 ms guard baseline; 8.826359 ms Criterion mean | CPU + metadata I/O | `perf_guard_baseline.json`; `evidence/criterion-20260410/untar_flush_latency_estimates.json`; `benches/perf_local_criterion.rs:161` |
| 3 | `run_large_segment_staged_flush_latency_once`: staged 128 MiB segment `write_batch` | Throughput | 1880.170920 MiB/s; 76.111356 ms per 128 MiB iteration | segment I/O | `perf_guard_baseline.json`; `evidence/criterion-20260410/large_segment_staged_flush_latency_estimates.json`; `benches/perf_local_criterion.rs:277` |
| 4 | `run_flush_metadata_batch_latency_once`: 5,000 inode batch persist/sync/commit | Latency | 5.593405 ms Criterion mean, supporting full-suite artifact | metadata I/O | `evidence/criterion-20260410/flush_metadata_batch_latency_20260404_estimates.json`; `benches/perf_local_criterion.rs:226` |
| 5 | `run_segment_sequential_read_mib_per_s_once`: cached extent reads via `read_pointer_arc` | Throughput | 3592478.248667 MiB/s; 0.036924 ms per 128 MiB iteration | cache/memory | `perf_guard_baseline.json`; `evidence/criterion-20260410/segment_sequential_read_throughput_estimates.json`; `benches/perf_local_criterion.rs:439` |

## Interpretation

The top target is the preload/NFS create-write-close path because it exercises a
user-visible small-write workflow and is far lower throughput than the cached
read and bulk segment paths. The second and fourth rows point at metadata flush
work as the next root-cause surface to isolate with spans or a profiler.

The fifth row is included as a counterexample: sequential cached reads are not
the first optimization target in the current evidence set.
