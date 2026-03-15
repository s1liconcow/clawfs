mod cwd;
mod dispatch;
mod errno;
mod fd_table;
mod inotify;
mod intercept;
mod prefix;
mod runtime;

/// Library constructor — runs before `main()` when loaded via LD_PRELOAD.
/// Initializes the ClawFS runtime if CLAWFS_PREFIXES is set.
#[ctor::ctor]
fn init() {
    // Initialize env_logger for diagnostic output (controlled by RUST_LOG).
    let _ = env_logger::try_init();

    match runtime::ClawfsRuntime::init() {
        Ok(true) => {
            log::debug!("clawfs-preload: runtime initialized");
        }
        Ok(false) => {
            // CLAWFS_PREFIXES not set — library is a no-op.
        }
        Err(e) => {
            eprintln!("clawfs-preload: initialization failed: {e:#}");
            // Don't abort — fall through to real libc for all calls.
        }
    }
}
