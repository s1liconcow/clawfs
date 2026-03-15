use std::path::{Component, PathBuf};

/// Result of classifying a path against configured ClawFS prefixes.
#[derive(Debug, Clone)]
pub struct ClawfsPath {
    /// The inner path within the ClawFS volume (e.g. "/foo/bar" when the prefix
    /// is "/claw" and the full path is "/claw/foo/bar"). Always starts with "/".
    pub inner: String,
}

/// Routes absolute paths to ClawFS volumes based on configured prefixes.
pub struct PrefixRouter {
    /// Prefixes sorted longest-first so the most specific match wins.
    prefixes: Vec<PathBuf>,
}

impl PrefixRouter {
    /// Build a router from a comma-separated list of path prefixes.
    /// Each prefix is normalized and must be absolute.
    pub fn new(prefixes_str: &str) -> Self {
        let mut prefixes: Vec<PathBuf> = prefixes_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(normalize_path)
            .filter(|p| p.is_absolute())
            .collect();
        // Sort longest-first so more specific prefixes match first.
        prefixes.sort_by_key(|p| std::cmp::Reverse(p.as_os_str().len()));
        PrefixRouter { prefixes }
    }

    /// Check whether the given absolute path falls under a ClawFS prefix.
    /// Returns the inner path suffix if it does.
    pub fn classify(&self, path: &str) -> Option<ClawfsPath> {
        let normalized = normalize_path(path);
        let norm_str = normalized.to_str()?;

        for prefix in &self.prefixes {
            let prefix_str = prefix.to_str()?;
            if norm_str == prefix_str {
                return Some(ClawfsPath {
                    inner: "/".to_string(),
                });
            }
            if let Some(suffix) = norm_str.strip_prefix(prefix_str) {
                if suffix.starts_with('/') {
                    return Some(ClawfsPath {
                        inner: suffix.to_string(),
                    });
                }
            }
        }
        None
    }

    /// Returns true if this router has no prefixes configured (library is a no-op).
    pub fn is_empty(&self) -> bool {
        self.prefixes.is_empty()
    }

    /// Reconstruct the full external path for an inner volume path.
    /// Uses the first (most-specific) configured prefix.
    pub fn full_path(&self, inner: &str) -> Option<String> {
        let prefix = self.prefixes.first()?;
        let prefix_str = prefix.to_str()?;
        if inner == "/" || inner.is_empty() {
            Some(prefix_str.to_string())
        } else {
            Some(format!("{}{}", prefix_str.trim_end_matches('/'), inner))
        }
    }
}

/// Normalize a path: resolve `.` and `..` lexically, collapse repeated `/`.
fn normalize_path(path: &str) -> PathBuf {
    let mut result = PathBuf::new();
    let p = PathBuf::from(path);
    for component in p.components() {
        match component {
            Component::RootDir => {
                result.push("/");
            }
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            Component::Normal(c) => {
                result.push(c);
            }
            Component::Prefix(_) => {}
        }
    }
    if result.as_os_str().is_empty() {
        PathBuf::from("/")
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_exact_prefix() {
        let router = PrefixRouter::new("/claw");
        let result = router.classify("/claw").unwrap();
        assert_eq!(result.inner, "/");
    }

    #[test]
    fn classify_subpath() {
        let router = PrefixRouter::new("/claw");
        let result = router.classify("/claw/foo/bar").unwrap();
        assert_eq!(result.inner, "/foo/bar");
    }

    #[test]
    fn classify_no_match() {
        let router = PrefixRouter::new("/claw");
        assert!(router.classify("/other/path").is_none());
    }

    #[test]
    fn classify_partial_prefix_no_match() {
        let router = PrefixRouter::new("/claw");
        // "/clawmore" should NOT match prefix "/claw"
        assert!(router.classify("/clawmore").is_none());
    }

    #[test]
    fn classify_multiple_prefixes() {
        let router = PrefixRouter::new("/claw, /agent-data");
        assert!(router.classify("/claw/x").is_some());
        assert!(router.classify("/agent-data/y").is_some());
        assert!(router.classify("/other").is_none());
    }

    #[test]
    fn classify_longest_prefix_wins() {
        let router = PrefixRouter::new("/data, /data/special");
        let result = router.classify("/data/special/file").unwrap();
        assert_eq!(result.inner, "/file");
    }

    #[test]
    fn normalize_dotdot() {
        let router = PrefixRouter::new("/claw");
        let result = router.classify("/claw/foo/../bar").unwrap();
        assert_eq!(result.inner, "/bar");
    }

    #[test]
    fn normalize_dot() {
        let router = PrefixRouter::new("/claw");
        let result = router.classify("/claw/./foo").unwrap();
        assert_eq!(result.inner, "/foo");
    }

    #[test]
    fn empty_prefixes() {
        let router = PrefixRouter::new("");
        assert!(router.is_empty());
        assert!(router.classify("/anything").is_none());
    }
}
