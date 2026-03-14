# ClawFS Assistant Notes

## Purpose
This is the top-level guide for working in the ClawFS repo. Keep this file concise: retain high-level directives, a compact project summary, and links to detailed reference docs in `docs/`.

## Living Document
Continuously update this guide and the linked docs with anything that will improve future iteration speed, reduce repeated investigation, or clarify current project behavior.

## High-Level Directives
- Prefer reusable tools or scripts over repeatedly writing long ad hoc shell or Python commands.
- After merging a task branch into `master`, clean up temporary worktrees and local task branches.
- Standard post-merge cleanup sequence: `git worktree list`, `git worktree remove <task-worktree-path>`, `git branch -D <task-branch>`.
- If a sibling worktree is outside the writable root and `git worktree remove` fails with `Permission denied`, rerun with escalated permissions.
- When CLI flags or config defaults change, ensure you update all the places Config is used (tests, NFS gateway, etc) and update the related `scripts/` launchers in the same PR.
- Before handing work off, run `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test` unless the task explicitly prevents it.
- Troubleshooting policy: reproduce bugs with an automated test in `src/fs/tests/mod.rs` before fixing when practical.

## ClawFS Summary
ClawFS is a FUSE-based log-structured filesystem that stages writes locally, flushes immutable data segments to object storage, and stores metadata as immutable inode-map shards plus per-generation delta logs.

Close-time durability uses a local journal under `$STORE/journal`. Pending writes are buffered per inode and flushed asynchronously with coalescing; a flush writes a segment, emits delta logs, rewrites touched shards, and advances the superblock generation. Large writes stage under `$STORE/segment_stage`, metadata caches follow NFS-like TTLs, and each client keeps local identity and allocation state via `--state-path`.

The repository also includes `clawfs-nfs-gateway/`, a standalone NFSv3 server that serves the same storage model without a FUSE mount.

## Table Of Contents
- [Project internals and architecture](docs/clawfs-project-reference.md)
- [Shared-library / preload design](docs/clawfs-shared-library-design.md)
- [ClawFS storage modes and hosted free-tier model](docs/clawfs-storage-modes.md)
- [Testing, tooling, troubleshooting, and common commands](docs/clawfs-testing-and-operations.md)
- [Sprite workflow and privileged execution rules](docs/clawfs-sprites.md)

## Quick Pointers
- Add new filesystem behavior in `src/fs/ops.rs` first, then adapt transport-specific layers in `src/fs/fuse.rs` or `src/fs/nfs.rs` if needed.
- `src/config.rs` maps CLI flags into runtime config.
- `src/metadata.rs` owns inode caching, shard snapshots, delta logs, and superblock CAS updates.
- `src/segment.rs` owns immutable segment serialization and range reads.
- `src/perf.rs` and `src/replay.rs` provide structured performance and replay logging.

Keep this file focused. Put detailed implementation notes, workflow specifics, and troubleshooting expansions in the linked `docs/` files.
