# FIO Workloads and Optimization Ideas

This note maps `scripts/fio_workloads.sh` results to likely ClawFS bottlenecks and practical tuning knobs.

## Workload intent

- `seq_write_1m`: append-heavy large writes, tests flush batching and segment upload efficiency.
- `seq_read_1m`: large streaming reads, tests range-read throughput and segment cache effectiveness.
- `randread_4k`: random read latency and metadata lookup path.
- `randwrite_4k`: random small write amplification, journal/staging overhead, metadata rewrite cost.
- `randrw_70r_30w_4k`: mixed OLTP-like profile, contention between read cache and write flushes.
- `smallfiles_sync`: metadata-heavy sync writes across many files.

## Optimization suggestions

1. Raise write coalescing for random/small writes.
- Increase `--pending-bytes` and keep `--flush-interval-ms` non-zero to batch more inode updates per generation.
- Keep journal enabled for safety, but allow adaptive defer window to absorb bursts.

2. Tune inline data threshold for small files.
- For `smallfiles_sync` and random 4k writes, increasing `--inline-threshold` can avoid segment indirection.
- Re-check memory and delta object growth after changing this value.

3. Reduce metadata rewrite fanout.
- Increase `--imap-delta-batch` so each flush emits fewer delta objects.
- Ensure shard size is not too small; overly small shards increase per-flush snapshot churn.

4. Improve read-side cache hit rate.
- Increase `--segment-cache-bytes` for `seq_read_1m` and `randread_4k` if working set fits.
- Lower metadata poll interval only when staleness requirements demand it; aggressive polling can add background overhead.

5. Separate durability and throughput profiles.
- Throughput profile: `--disable-journal` for controlled benchmarking only.
- Durable profile: journal on, higher pending bytes, and tuned flush interval to avoid per-close commits.

6. Watch object-store API pressure.
- If bandwidth is good but latency is high, focus on reducing object count per generation (larger batches, fewer tiny flushes).
- Run cleanup agent near the bucket so compaction work does not compete with client foreground IO.

## Practical experiment matrix

- Baseline: default config with journal on.
- Batching test: `--pending-bytes` x2/x4 and `--flush-interval-ms` 500/1000.
- Inline threshold test: 1 KiB vs 4 KiB vs 16 KiB.
- Cache test: `--segment-cache-bytes` 512 MiB vs 2 GiB.
- API pressure test: vary `--imap-delta-batch` and compare object write count per fio runtime.

Collect with:
- fio summary (`scripts/fio_workloads.sh` output)
- `--perf-log` JSONL (`stage_file`, `flush_pending` durations)
- object-store request counts/latencies

