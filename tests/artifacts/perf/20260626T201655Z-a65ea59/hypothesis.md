# Hypothesis Ledger - 20260626T201655Z-a65ea59

| Hypothesis | Verdict | Evidence |
|------------|---------|----------|
| Per-file flush cost is the main visible bottleneck in create/write/close workflows. | supports | With-flush core baseline is 310.387004 IOPS and 326.574380 ms per 100-op iteration. Prior full-suite no-flush timing was 145.863570 ms per 100-op iteration, while same-suite with-flush was 369.311863 ms. Evidence: `perf_guard_baseline.json`, `evidence/criterion-20260410/create_write_close_iops_with_flush_estimates.json`, `evidence/criterion-20260410/create_write_close_iops_no_flush_20260404_estimates.json`. |
| Untar flush latency is mostly metadata batch persistence after inline encoding. | supports | Benchmark source explicitly models inline encoding plus `persist_inodes_batch`, `sync_metadata_writes`, and `commit_generation`; full-suite `flush_metadata_batch_latency` is 5.593405 ms mean versus core untar flush 8.826359 ms mean. |
| Large staged segment writes are the highest-priority current bottleneck. | mixed | The 128 MiB staged write path is meaningful, but the core baseline is still 1880.170920 MiB/s. It should be profiled if large-checkpoint workloads are the user pain point, but current evidence ranks small flushed writes higher. |
| Sequential cached reads should be optimized first. | rejects | Current core baseline is 3592478.248667 MiB/s and 0.036924 ms per 128 MiB Criterion iteration. That evidence is not consistent with a current read-path bottleneck. |
| A code optimization is justified immediately from these artifacts alone. | rejects | This pass has no fresh stack profile, allocation profile, or same-host variance envelope. The next step should collect CPU/alloc/wait evidence for rank 1 or rank 2 before changing code. |
