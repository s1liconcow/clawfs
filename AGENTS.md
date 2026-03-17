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

## MCP Agent Mail: coordination for multi-agent workflows

What it is
- A mail-like layer that lets coding agents coordinate asynchronously via MCP tools and resources.
- Provides identities, inbox/outbox, searchable threads, and advisory file reservations, with human-auditable artifacts in Git.

Why it's useful
- Prevents agents from stepping on each other with explicit file reservations (leases) for files/globs.
- Keeps communication out of your token budget by storing messages in a per-project archive.
- Offers quick reads (`resource://inbox/...`, `resource://thread/...`) and macros that bundle common flows.

How to use effectively
1) Same repository
   - Register an identity: call `ensure_project`, then `register_agent` using this repo's absolute path as `project_key`.
   - Reserve files before you edit: `file_reservation_paths(project_key, agent_name, ["src/**"], ttl_seconds=3600, exclusive=true)` to signal intent and avoid conflict.
   - Communicate with threads: use `send_message(..., thread_id="FEAT-123")`; check inbox with `fetch_inbox` and acknowledge with `acknowledge_message`.
   - Read fast: `resource://inbox/{Agent}?project=<abs-path>&limit=20` or `resource://thread/{id}?project=<abs-path>&include_bodies=true`.
   - Tip: set `AGENT_NAME` in your environment so the pre-commit guard can block commits that conflict with others' active exclusive file reservations.

2) Across different repos in one project (e.g., Next.js frontend + FastAPI backend)
   - Option A (single project bus): register both sides under the same `project_key` (shared key/path). Keep reservation patterns specific (e.g., `frontend/**` vs `backend/**`).
   - Option B (separate projects): each repo has its own `project_key`; use `macro_contact_handshake` or `request_contact`/`respond_contact` to link agents, then message directly. Keep a shared `thread_id` (e.g., ticket key) across repos for clean summaries/audits.

Macros vs granular tools
- Prefer macros when you want speed or are on a smaller model: `macro_start_session`, `macro_prepare_thread`, `macro_file_reservation_cycle`, `macro_contact_handshake`.
- Use granular tools when you need control: `register_agent`, `file_reservation_paths`, `send_message`, `fetch_inbox`, `acknowledge_message`.

Common pitfalls
- "from_agent not registered": always `register_agent` in the correct `project_key` first.
- "FILE_RESERVATION_CONFLICT": adjust patterns, wait for expiry, or use a non-exclusive reservation when appropriate.
- Auth errors: if JWT+JWKS is enabled, include a bearer token with a `kid` that matches server JWKS; static bearer is used only when JWT is disabled.


## Integrating with Beads (dependency-aware task planning)

Beads provides a lightweight, dependency-aware issue database and a CLI (`bd`) for selecting "ready work," setting priorities, and tracking status. It complements MCP Agent Mail's messaging, audit trail, and file-reservation signals. Project: [steveyegge/beads](https://github.com/steveyegge/beads)

Recommended conventions
- **Single source of truth**: Use **Beads** for task status/priority/dependencies; use **Agent Mail** for conversation, decisions, and attachments (audit).
- **Shared identifiers**: Use the Beads issue id (e.g., `bd-123`) as the Mail `thread_id` and prefix message subjects with `[bd-123]`.
- **Reservations**: When starting a `bd-###` task, call `file_reservation_paths(...)` for the affected paths; include the issue id in the `reason` and release on completion.

Typical flow (agents)
1) **Pick ready work** (Beads)
   - `bd ready --json` → choose one item (highest priority, no blockers)
2) **Reserve edit surface** (Mail)
   - `file_reservation_paths(project_key, agent_name, ["src/**"], ttl_seconds=3600, exclusive=true, reason="bd-123")`
3) **Announce start** (Mail)
   - `send_message(..., thread_id="bd-123", subject="[bd-123] Start: <short title>", ack_required=true)`
4) **Work and update**
   - Reply in-thread with progress and attach artifacts/images; keep the discussion in one thread per issue id
5) **Complete and release**
   - `bd close bd-123 --reason "Completed"` (Beads is status authority)
   - `release_file_reservations(project_key, agent_name, paths=["src/**"])`
   - Final Mail reply: `[bd-123] Completed` with summary and links

Mapping cheat-sheet
- **Mail `thread_id`** ↔ `bd-###`
- **Mail subject**: `[bd-###] …`
- **File reservation `reason`**: `bd-###`
- **Commit messages (optional)**: include `bd-###` for traceability

Event mirroring (optional automation)
- On `bd update --status blocked`, send a high-importance Mail message in thread `bd-###` describing the blocker.
- On Mail "ACK overdue" for a critical decision, add a Beads label (e.g., `needs-ack`) or bump priority to surface it in `bd ready`.

Pitfalls to avoid
- Don't create or manage tasks in Mail; treat Beads as the single task queue.
- Always include `bd-###` in message `thread_id` to avoid ID drift across tools.
