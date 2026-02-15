# Verified Lift Summary (Concurrent fsync workload)

Candidate variant:
- `src/fs.rs`: remove global `mutation_lock` from FUSE `flush/fsync/release` paths.
- keep global `flush_pending()` semantics for these sync operations.
- keep durability/consistency behavior (sync calls still flush and wait for commit before returning).

Test protocol:
- Sprite: `osagefs-duckdb-realistic`
- Baseline: `/work/osagefs-baseline` (`main`)
- Candidate: `/work/osagefs-candidate3`
- Interleaved sequence per run-set: `baseline, candidate, candidate, baseline` (2 blocks => 8 total runs)
- Workload: 4 processes, each 300 iterations of `write(4KiB)+fsync`, file size checks enforced.

Run set A (`interleaved-long-v2-n4.tsv`):
- baseline mean 8.213396s
- candidate mean 5.940270s
- lift mean +27.676%
- baseline median 8.279462s
- candidate median 5.964837s
- lift median +27.956%

Run set B (`interleaved-long-v3-n4.tsv`):
- baseline mean 7.212389s
- candidate mean 7.587454s
- lift mean -5.200%
- baseline median 6.957069s
- candidate median 8.158316s
- lift median -17.267%

Combined (A+B, 16 total samples):
- baseline mean 7.712892s
- candidate mean 6.763862s
- lift mean +12.304%
- baseline median 8.199251s
- candidate median 6.381348s
- lift median +22.172%

Conclusion:
- In the combined long-run NPROC=4 fsync profile, candidate shows net positive lift.
- Variance remains non-trivial across individual run sets; further isolation on dedicated hardware is recommended for tighter confidence bounds.
