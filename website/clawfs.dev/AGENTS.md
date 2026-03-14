# ClawFS Shared Directory Guide

## /clawfs - Shared Collaboration Directory

When using ClawFS, the path `/clawfs` (or your configured prefix) represents a **shared persistent workspace** that persists across agent sessions and enables collaboration between agents.

### What is /clawfs?

`/clawfs` is a shared directory where agents can:
- Store files that persist across restarts and sessions
- Share data and checkpoints with other agents
- Collaborate on tasks using a common filesystem namespace
- Read and write files as if they were on a local filesystem

### How It Works

By default, ClawFS uses the prefix `clawfs` relative to the current working directory. This means:
- `./clawfs/` in your working directory is intercepted and redirected to ClawFS
- All file operations within this directory are persisted and synchronized
- Multiple agents can access the same volume simultaneously

### Usage Examples

```bash
# Using up (default path is 'clawfs')
clawfs up -- your-agent

# Using up with explicit path
clawfs up --path clawfs -- your-agent
```

### Configuration

You can customize the prefix path:
- `--path clawfs` (relative to current directory - default)
- `--path /tmp/clawfs-mnt` (absolute path)
- `--path shared-vol` (custom name)

### Best Practices

1. **Use consistent paths** across your team when sharing volumes
2. **Name volumes meaningfully** with `--volume` for different projects
3. **Checkpoint important state** before ending agent sessions
4. **Clean up temporary files** to keep shared volumes organized

### Collaboration Model

ClawFS enables multi-agent collaboration through:
- **Shared volumes** accessed by name (`--volume team-project`)
- **Persistent storage** that survives agent restarts
- **Concurrent access** with automatic synchronization
- **Checkpoints** for saving and restoring agent state

---

For more information, see [Getting Started](https://clawfs.dev/getting-started.html) and [Documentation](https://clawfs.dev/docs).
