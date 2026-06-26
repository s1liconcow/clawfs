# Hand-off - 20260626T201655Z-a65ea59

Baseline captured from the approved checked-in perf guard. Top targets are
ranked with evidence in this directory.

Recommended next profiling target:

1. `run_preload_create_write_close_iops_once` / NFS create-write-flush path.
2. Collect a fresh same-host or sprite run with CPU stacks, allocation profile,
   and stage-level spans before changing code.
3. Only after that, hand to `extreme-software-optimization` for one-lever
   changes scored by impact, confidence, and effort.

No optimization changes were made in this pass.
