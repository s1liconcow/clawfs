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
- Before handing work off, run `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test` unless the task explicitly prevents it.
- Tests and Troubleshooting policy: always validate your change with unit and integration tests where possible, reproduce bugs with tests before solving them
- Performance improvements: always try to quantify the lift of performance improvements.
- Perf workflow: the current core-suite sprite baseline is checked in at `bench-artifacts/perf_guard_baseline.json` (refreshed 2026-04-10). Reuse that baseline for follow-on perf work unless the benchmark suite or sprite environment changes materially.
- Duplicate any new config added to private/src/config.rs
- *important* After compaction, agents must re-read AGENTS.md
- *important* do not run cargo bench without the user's authorization

## ClawFS Summary
ClawFS is a FUSE-based log-structured filesystem that stages writes locally, flushes immutable data segments to object storage, and stores metadata as immutable inode-map shards plus per-generation delta logs.

Close-time durability uses a local journal under `$STORE/journal`. Pending writes are buffered per inode and flushed asynchronously with coalescing; a flush writes a segment, emits delta logs, rewrites touched shards, and advances the superblock generation. Large writes stage under `$STORE/segment_stage`, metadata caches follow NFS-like TTLs, and each client keeps local identity and allocation state via `--state-path`.

## Table Of Contents
- [Project internals and architecture](docs/clawfs-project-reference.md)
- [ClawFS storage modes and hosted free-tier model](docs/clawfs-storage-modes.md)
- [Testing, tooling, troubleshooting, and common commands](docs/clawfs-testing-and-operations.md)
- [Sprite workflow and privileged execution rules](docs/clawfs-sprites.md)

## Quick Pointers
- Repo split scaffolding:
  `crates/clawfs-core` is the public/OSS facade crate used for the public repository split.
- Add new filesystem behavior in `src/fs/ops.rs` first, then adapt transport-specific layers in `src/fs/fuse.rs` or `src/fs/nfs.rs` if needed.
- `src/config.rs` maps CLI flags into runtime config.
- `src/metadata.rs` owns inode caching, shard snapshots, delta logs, and superblock CAS updates.
- `src/segment.rs` owns immutable segment serialization and range reads.
- `src/perf.rs` and `src/replay.rs` provide structured performance and replay logging.

Private repo containing the nfs-gateway, preload library, and hosted CLI is under private/

Keep this file focused. Put detailed implementation notes, workflow specifics, and troubleshooting expansions in the linked `docs/` files.

## Project Structure (key files by responsibility)

| File | Lines (approx) | Responsibility |
|------|----------------|----------------|
| `src/config.rs` | ~470 | CLI/env config parsing. `Cli` struct (line 11), `Config` struct (line 232), `ObjectStoreProvider` enum (line 468) |
| `src/clawfs.rs` | ~330 | Storage modes, runtime spec. `StorageMode` enum (line 27), `RuntimeSpec` (line 56), `apply_env_runtime_spec()` (line 105) |
| `src/launch.rs` | ~700 | Public mount bootstrap, logging, root initialization, metadata polling, and local cleanup-worker startup |
| `src/superblock.rs` | ~258 | Superblock, leases, generation. `Superblock` (line 18), `CleanupTaskKind` enum (line 42), `CleanupLease` (line 48), `try_acquire_cleanup()` (line 225), `complete_cleanup()` (line 248) |
| `src/metadata.rs` | ~1400+ | Inode caching, shards, deltas. `get_inode_with_ttl()` (line 809), `persist_inodes_batch()` (line 943), `store_superblock_conditional()` (line 679), `apply_external_deltas()` (line 1350), `delta_file_count()` (line 1354), `segment_candidates()` (line 1394) |
| `src/segment.rs` | ~400+ | Immutable segment I/O. `SegmentPointer` (line 31), `SegmentEntry` (line 38), `SegmentManager` (line 65), `write_batch()`, `read_pointer()`, `delete_segment()` |
| `src/journal.rs` | ~300+ | Write-ahead log. `JournalPayload` enum (line 53), `JournalEntry` (line 61), `JournalManager` (line 78), `persist_entry()` (line 102), `sync_entries()` (line 127) |
| `src/fs/flush.rs` | ~400+ | Flush pipeline. `flush_pending()` (line 18), `flush_pending_selected()` (line 96) — drain pending → write segments → create deltas → commit superblock → clear journal |
| `src/fs/ops.rs` | | Filesystem operations (new behavior starts here) |
| `src/fs/fuse.rs` | | FUSE transport layer |
| `src/fs/nfs.rs` | | NFS transport layer |
| `src/perf.rs` | | Structured performance logging |
| `src/replay.rs` | | Replay capture |

**Cleanup constants** (src/launch.rs): `DELTA_COMPACT_THRESHOLD=128` (line 39), `DELTA_COMPACT_KEEP=32` (line 40), `SEGMENT_COMPACT_BATCH=8` (line 41), `SEGMENT_COMPACT_LAG=3` (line 42), lease TTL=30s.

**Key design docs:**
- `docs/clawfs-project-reference.md` — Architecture internals

## Writing Good Beads (Task Descriptions)

When creating or updating task beads, follow this structure so an agent can implement the task without guidance:

```
<title>: Short imperative description

<description body>:
Context:
Part of EPIC: <parent epic name and ID>. Depends on <dependencies with IDs>.
Brief explanation of why this task exists and what it enables.

What to Do:
1. Step-by-step implementation instructions
2. Reference specific files, structs, functions, and line numbers
3. Name the types/functions to create or modify
4. Specify defaults and edge cases

Acceptance Criteria:
- Testable, concrete outcomes
- What behavior must exist
- What must NOT change (regression safety)

Files to Modify:
- path/to/file.rs: What to change and where (~line N)
```

Key rules:
- Always include specific file paths and approximate line numbers
- Name the structs, enums, and functions to create or modify
- Include defaults and edge-case behavior
- Specify what must NOT change (existing behavior preservation)
- Reference the Project Structure table above for current locations
- Use `br update <id> --description "..."` to update descriptions
- Use `br update <id> --acceptance-criteria "..."` for acceptance criteria (separate field)
- Use `br comments add <id> "..."` for additional context or implementation notes

# Agent Tools

# Beads Workflow Integration

This project uses [beads_rust](https://github.com/Dicklesworthstone/beads_rust) for issue tracking. Issues are stored in `.beads/` and tracked in git.

**Note:** `br` (beads_rust) is non-invasive and never executes git commands directly. After running `br sync --flush-only`, you must manually run git commands to commit and push changes.

### Essential Commands

```bash
# View issues (launches TUI - avoid in automated sessions)
bv

# CLI commands for agents (use these instead)
br ready              # Show issues ready to work (no blockers)
br list --status=open # All open issues
br show <id>          # Full issue details with dependencies
br create --title="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason="Completed"
br close <id1> <id2>  # Close multiple issues at once
br sync --flush-only  # Export to JSONL (does NOT run git commands)
git add .beads/ && git commit -m "Update beads" && git push  # Manual git steps
```

### Workflow Pattern

1. **Start**: Run `br ready` to find actionable work
2. **Claim**: Use `br update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `br close <id>`
5. **Sync**: Run `br sync --flush-only`, then manually `git add .beads/ && git commit && git push`

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

<!-- bv-agent-instructions-v2 -->

---

## Beads Workflow Integration

This project uses [beads_rust](https://github.com/Dicklesworthstone/beads_rust) (`br`) for issue tracking and [beads_viewer](https://github.com/Dicklesworthstone/beads_viewer) (`bv`) for graph-aware triage. Issues are stored in `.beads/` and tracked in git.

### Using bv as an AI sidecar

bv is a graph-aware triage engine for Beads projects (.beads/beads.jsonl). Instead of parsing JSONL or hallucinating graph traversal, use robot flags for deterministic, dependency-aware outputs with precomputed metrics (PageRank, betweenness, critical path, cycles, HITS, eigenvector, k-core).

**Scope boundary:** bv handles *what to work on* (triage, priority, planning). `br` handles creating, modifying, and closing beads.

**CRITICAL: Use ONLY --robot-* flags. Bare bv launches an interactive TUI that blocks your session.**

#### The Workflow: Start With Triage

**`bv --robot-triage` is your single entry point.** It returns everything you need in one call:
- `quick_ref`: at-a-glance counts + top 3 picks
- `recommendations`: ranked actionable items with scores, reasons, unblock info
- `quick_wins`: low-effort high-impact items
- `blockers_to_clear`: items that unblock the most downstream work
- `project_health`: status/type/priority distributions, graph metrics
- `commands`: copy-paste shell commands for next steps

```bash
bv --robot-triage        # THE MEGA-COMMAND: start here
bv --robot-next          # Minimal: just the single top pick + claim command

# Token-optimized output (TOON) for lower LLM context usage:
bv --robot-triage --format toon
```

#### Other bv Commands

| Command | Returns |
|---------|---------|
| `--robot-plan` | Parallel execution tracks with unblocks lists |
| `--robot-priority` | Priority misalignment detection with confidence |
| `--robot-insights` | Full metrics: PageRank, betweenness, HITS, eigenvector, critical path, cycles, k-core |
| `--robot-alerts` | Stale issues, blocking cascades, priority mismatches |
| `--robot-suggest` | Hygiene: duplicates, missing deps, label suggestions, cycle breaks |
| `--robot-diff --diff-since <ref>` | Changes since ref: new/closed/modified issues |
| `--robot-graph [--graph-format=json\|dot\|mermaid]` | Dependency graph export |

#### Scoping & Filtering

```bash
bv --robot-plan --label backend              # Scope to label's subgraph
bv --robot-insights --as-of HEAD~30          # Historical point-in-time
bv --recipe actionable --robot-plan          # Pre-filter: ready to work (no blockers)
bv --recipe high-impact --robot-triage       # Pre-filter: top PageRank scores
```

### br Commands for Issue Management

```bash
br ready              # Show issues ready to work (no blockers)
br list --status=open # All open issues
br show <id>          # Full issue details with dependencies
br create --title="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason="Completed"
br close <id1> <id2>  # Close multiple issues at once
br sync --flush-only  # Export DB to JSONL
```

### Workflow Pattern

1. **Triage**: Run `bv --robot-triage` to find the highest-impact actionable work
2. **Claim**: Use `br update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `br close <id>`
5. **Sync**: Always run `br sync --flush-only` at session end

### Key Concepts

- **Dependencies**: Issues can block other issues. `br ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers 0-4, not words)
- **Types**: task, bug, feature, epic, chore, docs, question
- **Blocking**: `br dep add <issue> <depends-on>` to add dependencies

### Session Protocol

```bash
git status              # Check what changed
git add <files>         # Stage code changes
br sync --flush-only    # Export beads changes to JSONL
git commit -m "..."     # Commit everything
git push                # Push to remote
```

<!-- end-bv-agent-instructions -->
---

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests `cargo test`, linters, and `cargo build --bin clawfsd`
3. **Smoke tests** run `./scripts/stress_e2e.sh` for e2e smoke tests.
4. **Fresh eyes review** Carefully read over all of the new code you just wrote and other existing code you just modified with "fresh eyes" looking super carefully for any obvious bugs, errors, problems, issues, confusion, etc. Carefully fix anything you uncover. Use ultrathink.
5. **Update issue status** - Close finished work, update in-progress items
6. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   br sync --flush-only
   git add .beads/
   git commit -m "Update beads"
   git push
   git status  # MUST show "up to date with origin"
   ```
7. **Clean up** - Clear stashes, prune remote branches
8. **Verify** - All changes committed AND pushed
9. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

---
