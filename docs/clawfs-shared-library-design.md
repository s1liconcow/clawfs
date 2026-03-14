# ClawFS Shared Library Design

## Summary
- Goal: run ClawFS as an in-process shared library that intercepts filesystem calls for selected path prefixes and redirects them into ClawFS core logic.
- Main use case: agent launchers that want normal file APIs and POSIX-ish semantics without requiring a host-level FUSE mount.
- Primary targets: Linux first, macOS second. Dynamic processes only. Static binaries and privileged system binaries are out of scope.
- This design complements FUSE and NFS. It does not replace them.

## Why This Exists
- Agent runtimes often want a normal filesystem view but cannot rely on host mount privileges.
- NFS solves the server side in userspace, but the client side is still usually kernel-mediated.
- A preload/shared-library path makes ClawFS a runtime primitive:
  - no explicit mount required inside the agent process
  - existing tools keep using `open`, `read`, `write`, `rename`, `readdir`, and friends
  - ClawFS path prefixes can behave like real workspaces instead of API-backed scratch areas

## Product Shape
- Launcher injects a preload library into the agent process:
  - Linux: `LD_PRELOAD=libclawfs_preload.so`
  - macOS: `DYLD_INSERT_LIBRARIES=libclawfs_preload.dylib` where allowed
- Launcher also sets runtime config:
  - `CLAWFS_PREFIXES=/claw,/agent-data`
  - storage mode and object config env vars
  - local cache and state paths
  - optional hosted-free caps
- Any filesystem operation under a configured prefix is handled by ClawFS instead of the host filesystem.
- Any path outside configured prefixes is passed through untouched to libc and the host kernel.

## Goals
- Support path-transparent ClawFS access for dynamically linked agent processes.
- Reuse the existing ClawFS object-store and metadata core rather than building a separate daemon protocol.
- Preserve familiar file APIs for common agent workflows:
  - create, read, write, truncate, rename, delete, directory iteration, symlinks, metadata lookup
- Keep the boundary explicit:
  - redirected paths are fully handled by ClawFS
  - non-redirected paths are fully handled by the host
- Fail clearly for unsupported cases instead of silently falling back in ways that corrupt semantics.

## Non-Goals
- Supporting every libc edge case in v1.
- Injecting into system binaries, setuid binaries, or statically linked binaries.
- Cross-boundary rename or hard-link behavior between ClawFS paths and host filesystem paths.
- Full `mmap`, file locking, device-node, or special-file support in v1.
- Replacing the need for FUSE or NFS in every deployment.

## Architecture

### Layers
1. Interception layer
   - Hooks libc-visible filesystem entry points.
   - Decides whether a call targets ClawFS or the host filesystem.

2. Path routing and cwd layer
   - Resolves absolute and relative paths.
   - Tracks redirected current-working-directory state when the process `chdir`s into a ClawFS path.

3. FD virtualization layer
   - Maintains synthetic ClawFS file descriptors and directory handles.
   - Distinguishes them from real OS file descriptors.

4. ClawFS runtime bootstrap
   - Initializes `Config`, `MetadataStore`, `SegmentManager`, `SuperblockManager`, and `ClientStateManager` in-process.
   - Applies env-based storage mode overrides and hosted-free caps from `src/clawfs.rs`.

5. Core dispatch layer
   - Translates intercepted operations into ClawFS core methods in `src/fs/ops.rs` and related modules.

### Runtime Model
- One lazy-initialized process-global ClawFS runtime per injected process.
- Shared across threads.
- Backed by the same object-store, cache, and state configuration as the FUSE/NFS paths.
- Best-effort process teardown only. Correctness must not depend on destructor order.

## Interception Surface

### v1 Required Calls
- File open and close:
  - `open`, `open64`, `openat`, `close`
- Read and write:
  - `read`, `pread`, `write`, `pwrite`
- Offsets and metadata:
  - `lseek`
  - `stat`, `lstat`, `fstat`, `fstatat`
  - `access`
- Namespace changes:
  - `mkdir`, `mkdirat`
  - `rmdir`
  - `unlink`, `unlinkat`
  - `rename`, `renameat`
  - `symlink`, `readlink`
- Size and sync:
  - `truncate`, `ftruncate`
  - `fsync`, `fdatasync`
- Directory APIs:
  - `opendir`, `fdopendir`, `readdir`, `readdir64`, `closedir`
- Descriptor duplication:
  - `dup`, `dup2`, `dup3`
- Process path state:
  - `chdir`, `getcwd`

### v1 Deferred or Unsupported
- `mmap`
- advisory and mandatory locking
- `ioctl`
- `sendfile`
- `copy_file_range`
- `splice`
- hard links if not explicitly implemented
- device nodes, fifos, sockets
- extended attributes unless later required by workloads

Unsupported operations on redirected paths should fail with a deliberate errno such as `ENOTSUP`, `EOPNOTSUPP`, or `EXDEV`.

## Path Routing Rules
- Prefix matching is done after path normalization.
- Relative paths are resolved against either:
  - the host cwd when cwd is on the host
  - the redirected ClawFS cwd when the process has entered a ClawFS path
- Once a path is classified as ClawFS-managed for a given call, there is no fallback to the host filesystem for that call.
- Cross-boundary operations fail clearly:
  - rename host -> ClawFS: `EXDEV`
  - rename ClawFS -> host: `EXDEV`
  - link across boundary: `EXDEV` or `ENOTSUP`

## FD Virtualization

### Synthetic FD Table
- Reserve a synthetic fd range or maintain a tagged table that maps process-visible fds to:
  - ClawFS file handle state
  - directory cursor state
  - open flags
  - logical offset
  - inode identity
- The table must support:
  - `dup` and friends
  - `fstat`
  - `lseek`
  - correct shared offset behavior where expected

### Directory Handles
- Directory iteration cannot rely on host `DIR*` semantics.
- Maintain an internal directory cursor and expose stable `readdir` behavior for intercepted directories.
- Directory iteration state should tolerate concurrent metadata refreshes without crashing or duplicating entries unexpectedly.

## ClawFS Core Integration

### Reuse Strategy
- Reuse existing config and runtime bootstrap logic.
- Reuse `src/clawfs.rs` env-driven runtime spec for:
  - `hosted_free`
  - `byob_paid`
  - scoped credentials
  - hosted-free cache and pending-byte caps
- Reuse `src/fs/ops.rs` and lower layers as the source of filesystem behavior.

### Required Refactors
- Extract a library-friendly runtime bootstrap from the FUSE and NFS entrypoints.
- Add handle-oriented helpers where current APIs are too transport-specific.
- Keep transport wrappers thin:
  - FUSE wrapper
  - NFS wrapper
  - preload/shared-library wrapper

## Configuration Contract

### Required Env Vars
- `CLAWFS_PREFIXES`
- `CLAWFS_STORAGE_MODE`
- `CLAWFS_OBJECT_PROVIDER`
- `CLAWFS_BUCKET`
- `CLAWFS_REGION`
- `CLAWFS_ENDPOINT`
- `CLAWFS_OBJECT_PREFIX`
- `CLAWFS_LOCAL_CACHE_PATH`
- `CLAWFS_STATE_PATH`

### Hosted Free-Tier Env Support
- `CLAWFS_MAX_PENDING_BYTES`
- `CLAWFS_MAX_SEGMENT_CACHE_BYTES`
- `CLAWFS_MAX_LOGICAL_BYTES`
- `CLAWFS_MAX_CHECKPOINTS`
- `CLAWFS_IDLE_EXPIRY_SECS`

### Scoped Credential Env Support
- `CLAWFS_AWS_ACCESS_KEY_ID`
- `CLAWFS_AWS_SECRET_ACCESS_KEY`
- `CLAWFS_AWS_SESSION_TOKEN`

## Semantics and Error Model
- Redirected paths should behave like ClawFS, not like a host overlay shim.
- Operations outside configured prefixes should be invisible to the library.
- Unsupported redirected operations should fail explicitly, not fall back to host paths or temporary files.
- Hosted-free caps are enforced at runtime config level where already supported:
  - `pending_bytes`
  - `segment_cache_bytes`
- Larger hosted-free resource enforcement still belongs in the future control plane, not in this library.

## Platform Notes

### Linux
- First implementation target.
- Intercept glibc entry points.
- Dynamic processes only.
- Static binaries and setuid flows are out of scope.

### macOS
- Second target.
- Depends on `DYLD_INSERT_LIBRARIES` being honored for the target process.
- SIP and system binary restrictions make coverage narrower than Linux.

## Testing Plan

### Unit Tests
- Prefix classification
- path normalization
- relative path resolution
- synthetic fd table correctness
- duplicate fd behavior
- hosted-free env parsing and clamping

### Integration Tests
- Launch child processes with the preload library enabled and exercise:
  - create/write/read/truncate/unlink
  - rename and directory traversal
  - symlink creation and lookup
  - cwd changes into ClawFS paths
  - mixed host and ClawFS path access in the same process
  - unsupported redirected operations returning expected errno

### Stress and Concurrency
- multi-threaded writes in one process
- multiple injected processes against the same backing store
- crash/restart with journal replay where applicable

## Rollout Plan

### Phase 1
- Linux-only prototype
- path interception for a small syscall set
- basic file and directory support
- local object provider first

### Phase 2
- AWS-backed and hosted-free env integration
- broader syscall coverage
- better error fidelity

### Phase 3
- macOS backend
- launcher integration
- workload validation against real coding-agent loops

## Main Risks
- libc interception is brittle and platform-specific.
- File descriptor emulation bugs can be subtle and dangerous.
- `readdir` and cwd semantics are easy to get almost right and still break tools.
- Silent fallback across the host/ClawFS boundary would create correctness bugs.
- `mmap` and locking semantics may block some workloads even after v1 ships.

## Repo Notes
- Current shipped modes are FUSE and user-mode NFS.
- `src/clawfs.rs` already provides the shared env/runtime substrate for hosted-free and BYOB storage config.
- This repo does not yet include the preload library crate or the syscall interception layer.
- This document is the source of truth for that planned implementation.
