use parking_lot::Mutex;

/// Tracks the current working directory, which may be inside a ClawFS volume.
pub struct CwdTracker {
    state: Mutex<CwdState>,
}

#[derive(Clone, Debug)]
enum CwdState {
    /// Cwd is on the host filesystem — delegate to real getcwd.
    Host,
    /// Cwd is inside a ClawFS volume.
    ClawFs {
        /// The full path as seen by the process (e.g. "/claw/subdir").
        full_path: String,
        /// The inner path within the volume (e.g. "/subdir").
        inner_path: String,
        /// Inode of the current directory.
        inode: u64,
    },
}

impl CwdTracker {
    pub fn new() -> Self {
        CwdTracker {
            state: Mutex::new(CwdState::Host),
        }
    }

    /// Set cwd to a ClawFS directory.
    pub fn set_clawfs(&self, full_path: String, inner_path: String, inode: u64) {
        *self.state.lock() = CwdState::ClawFs {
            full_path,
            inner_path,
            inode,
        };
    }

    /// Set cwd to the host filesystem.
    pub fn set_host(&self) {
        *self.state.lock() = CwdState::Host;
    }

    /// Returns `Some((full_path, inner_path, inode))` if cwd is in ClawFS.
    pub fn get_clawfs(&self) -> Option<(String, String, u64)> {
        match &*self.state.lock() {
            CwdState::Host => None,
            CwdState::ClawFs {
                full_path,
                inner_path,
                inode,
            } => Some((full_path.clone(), inner_path.clone(), *inode)),
        }
    }

    /// Returns true if cwd is inside a ClawFS volume.
    #[allow(dead_code)]
    pub fn is_clawfs(&self) -> bool {
        matches!(&*self.state.lock(), CwdState::ClawFs { .. })
    }
}
