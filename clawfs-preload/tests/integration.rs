//! Integration test: spawns a child process with LD_PRELOAD set,
//! the child performs file I/O under a ClawFS prefix and validates results.

use std::path::PathBuf;
use std::process::Command;

fn preload_lib_path() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    let candidates = [
        exe.parent().unwrap().join("libclawfs_preload.so"),
        exe.parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("libclawfs_preload.so"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("debug")
            .join("libclawfs_preload.so"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("target")
            .join("debug")
            .join("libclawfs_preload.so"),
    ];
    for candidate in candidates {
        if candidate.exists() {
            return candidate;
        }
    }
    panic!("libclawfs_preload.so not found. Build with `cargo build --manifest-path clawfs-preload/Cargo.toml` first.");
}

struct TestEnv {
    _tmp: tempfile::TempDir,
    store_path: PathBuf,
    cache_path: PathBuf,
    state_path: PathBuf,
    prefix: String,
    lib: PathBuf,
}

impl TestEnv {
    fn new(prefix: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        Self {
            _tmp: tmp,
            store_path: root.join("store"),
            cache_path: root.join("cache"),
            state_path: root.join("state").join("preload_state.bin"),
            prefix: prefix.to_string(),
            lib: preload_lib_path(),
        }
    }

    fn command(&self, program: &str) -> Command {
        let mut command = Command::new(program);
        command
            .env("LD_PRELOAD", &self.lib)
            .env("CLAWFS_PREFIXES", &self.prefix)
            .env("CLAWFS_STORE_PATH", &self.store_path)
            .env("CLAWFS_LOCAL_CACHE_PATH", &self.cache_path)
            .env("CLAWFS_STATE_PATH", &self.state_path)
            .env("CLAWFS_STORAGE_MODE", "byob_paid")
            .env("CLAWFS_OBJECT_PROVIDER", "local")
            .env("RUST_LOG", "error");
        command
    }
}

fn read_file_via_preload(env: &TestEnv, path: &str) -> Vec<u8> {
    let script = r#"
import os, sys
with open(sys.argv[1], "rb") as f:
    sys.stdout.buffer.write(f.read())
"#;

    let output = env
        .command("python3")
        .arg("-c")
        .arg(script)
        .arg(path)
        .output()
        .expect("failed to run python3");

    assert!(
        output.status.success(),
        "python read failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    output.stdout
}

#[test]
fn preload_basic_file_io() {
    let env = TestEnv::new("/clawfs-test-prefix");
    let prefix = &env.prefix;

    // Write a small Python script that exercises basic file I/O under the prefix.
    // We use Python because it's universally available and uses libc under the hood.
    let script = format!(
        r#"
import os, sys, errno

prefix = "{prefix}"

# 1. Create a file under the prefix and write to it.
path = prefix + "/hello.txt"
fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o644)
assert fd > 0, f"open returned {{fd}}"
n = os.write(fd, b"Hello, ClawFS!")
assert n == 14, f"write returned {{n}}"
os.close(fd)

# 2. Open the file for reading and verify contents.
fd = os.open(path, os.O_RDONLY)
data = os.read(fd, 1024)
assert data == b"Hello, ClawFS!", f"read returned {{data!r}}"
os.close(fd)

# 3. Stat the file and verify size.
st = os.stat(path)
assert st.st_size == 14, f"stat size = {{st.st_size}}"

# 4. Create a subdirectory.
dirpath = prefix + "/subdir"
os.mkdir(dirpath, 0o755)

# 5. Create a file inside the subdirectory.
inner_path = dirpath + "/inner.txt"
fd = os.open(inner_path, os.O_CREAT | os.O_WRONLY, 0o644)
os.write(fd, b"nested")
os.close(fd)

# 6. Stat the inner file.
st = os.stat(inner_path)
assert st.st_size == 6, f"inner stat size = {{st.st_size}}"

# 7. Rename the file.
renamed_path = prefix + "/renamed.txt"
os.rename(path, renamed_path)

# Verify renamed file exists.
st = os.stat(renamed_path)
assert st.st_size == 14

# Verify original is gone.
try:
    os.stat(path)
    assert False, "stat should have failed for deleted file"
except FileNotFoundError:
    pass

# 8. Unlink files and rmdir.
os.unlink(inner_path)
os.rmdir(dirpath)
os.unlink(renamed_path)

# 9. Verify everything is cleaned up.
try:
    os.stat(renamed_path)
    assert False, "should be gone"
except FileNotFoundError:
    pass

# 10. Verify access() works for the prefix root.
assert os.access(prefix, os.F_OK)

# ================================================================
# Phase 2 tests
# ================================================================

# 11. pread/pwrite — write at explicit offset, read back.
ppath = prefix + "/preadwrite.txt"
fd = os.open(ppath, os.O_CREAT | os.O_RDWR, 0o644)
os.pwrite(fd, b"AAABBBCCC", 0)
# pread from offset 3 should return "BBBCCC".
data = os.pread(fd, 6, 3)
assert data == b"BBBCCC", f"pread returned {{data!r}}"
# pwrite at offset 3 to overwrite.
os.pwrite(fd, b"XXX", 3)
data = os.pread(fd, 9, 0)
assert data == b"AAAXXXCCC", f"after pwrite: {{data!r}}"
os.close(fd)

# 12. truncate/ftruncate.
fd = os.open(ppath, os.O_RDWR)
os.ftruncate(fd, 5)
st = os.fstat(fd)
assert st.st_size == 5, f"ftruncate size = {{st.st_size}}"
os.close(fd)
os.truncate(ppath, 3)
st = os.stat(ppath)
assert st.st_size == 3, f"truncate size = {{st.st_size}}"

# 13. fsync (just verify it doesn't error).
fd = os.open(ppath, os.O_RDWR)
os.write(fd, b"sync-test")
os.fsync(fd)
os.close(fd)

# 14. dup — verify dup'd fd reads same data.
fd = os.open(ppath, os.O_RDONLY)
fd2 = os.dup(fd)
data1 = os.read(fd, 4)
# fd2 shares offset with fd, so it should continue from where fd left off.
data2 = os.read(fd2, 4)
os.close(fd)
os.close(fd2)

# 15. symlink/readlink.
link_path = prefix + "/mylink"
os.symlink("preadwrite.txt", link_path)
target = os.readlink(link_path)
assert target == "preadwrite.txt", f"readlink returned {{target!r}}"
os.unlink(link_path)

# 16. opendir/readdir/closedir.
os.mkdir(prefix + "/dirtest", 0o755)
for name in ["aa.txt", "bb.txt", "cc.txt"]:
    fd = os.open(prefix + "/dirtest/" + name, os.O_CREAT | os.O_WRONLY, 0o644)
    os.close(fd)
entries = os.listdir(prefix + "/dirtest")
assert sorted(entries) == ["aa.txt", "bb.txt", "cc.txt"], f"listdir = {{entries}}"
for name in ["aa.txt", "bb.txt", "cc.txt"]:
    os.unlink(prefix + "/dirtest/" + name)
os.rmdir(prefix + "/dirtest")

# 17. chdir/getcwd.
os.mkdir(prefix + "/cwdtest", 0o755)
saved_cwd = os.getcwd()
os.chdir(prefix + "/cwdtest")
cwd = os.getcwd()
assert cwd == prefix + "/cwdtest", f"getcwd = {{cwd!r}}"
os.chdir(saved_cwd)  # restore

# Clean up.
os.rmdir(prefix + "/cwdtest")
os.unlink(ppath)

print("PRELOAD_TEST_PASSED")
"#
    );

    let output = env
        .command("python3")
        .arg("-c")
        .arg(&script)
        .output()
        .expect("failed to run python3");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        panic!(
            "Child process failed with status {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
            output.status,
        );
    }

    assert!(
        stdout.contains("PRELOAD_TEST_PASSED"),
        "Test marker not found in stdout.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
fn preload_bash_redirection_writes_file() {
    let env = TestEnv::new("/clawfs-shell-prefix");
    let path = format!("{}/testing", env.prefix);

    let output = env
        .command("bash")
        .arg("-lc")
        .arg(format!("echo one > {path}"))
        .output()
        .expect("failed to run bash");

    assert!(
        output.status.success(),
        "bash redirect failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "redirected output leaked to stdout: {:?}",
        String::from_utf8_lossy(&output.stdout)
    );

    let data = read_file_via_preload(&env, &path);
    assert_eq!(data, b"one\n");
}

#[test]
fn preload_bash_append_redirection_preserves_contents() {
    let env = TestEnv::new("/clawfs-shell-append-prefix");
    let path = format!("{}/append.txt", env.prefix);

    let output = env
        .command("bash")
        .arg("-lc")
        .arg(format!("echo one > {path}; echo two >> {path}"))
        .output()
        .expect("failed to run bash");

    assert!(
        output.status.success(),
        "bash append redirect failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "redirected append leaked to stdout: {:?}",
        String::from_utf8_lossy(&output.stdout)
    );

    let data = read_file_via_preload(&env, &path);
    assert_eq!(data, b"one\ntwo\n");
}

#[test]
fn preload_rm_persists_unlink_across_processes() {
    let env = TestEnv::new("/clawfs-rm-prefix");
    let path = format!("{}/testing", env.prefix);

    let create = env
        .command("bash")
        .arg("-lc")
        .arg(format!("echo test > {path}"))
        .output()
        .expect("failed to create test file");
    assert!(
        create.status.success(),
        "create failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        create.status,
        String::from_utf8_lossy(&create.stdout),
        String::from_utf8_lossy(&create.stderr)
    );

    let remove = env
        .command("rm")
        .arg(&path)
        .output()
        .expect("failed to run rm");
    assert!(
        remove.status.success(),
        "rm failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        remove.status,
        String::from_utf8_lossy(&remove.stdout),
        String::from_utf8_lossy(&remove.stderr)
    );

    let stat = env
        .command("python3")
        .arg("-c")
        .arg("import os, sys; sys.exit(0 if os.path.exists(sys.argv[1]) else 1)")
        .arg(&path)
        .status()
        .expect("failed to stat removed file");
    assert_eq!(stat.code(), Some(1), "file still exists after rm");
}

#[test]
fn preload_relative_rm_from_host_cwd_succeeds() {
    let host_root = tempfile::tempdir().unwrap();
    let host_cwd = host_root.path().to_path_buf();
    let env = TestEnv::new(host_cwd.join("clawfs").to_str().unwrap());

    let create = env
        .command("bash")
        .current_dir(&host_cwd)
        .arg("-lc")
        .arg("echo test > clawfs/testing")
        .output()
        .expect("failed to create relative test file");
    assert!(
        create.status.success(),
        "relative create failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        create.status,
        String::from_utf8_lossy(&create.stdout),
        String::from_utf8_lossy(&create.stderr)
    );

    let remove = env
        .command("rm")
        .current_dir(&host_cwd)
        .arg("clawfs/testing")
        .output()
        .expect("failed to run relative rm");
    assert!(
        remove.status.success(),
        "relative rm failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        remove.status,
        String::from_utf8_lossy(&remove.stdout),
        String::from_utf8_lossy(&remove.stderr)
    );

    let stat = env
        .command("python3")
        .current_dir(&host_cwd)
        .arg("-c")
        .arg("import os, sys; sys.exit(0 if os.path.exists('clawfs/testing') else 1)")
        .status()
        .expect("failed to stat relative removed file");
    assert_eq!(stat.code(), Some(1), "relative file still exists after rm");
}

#[test]
fn preload_mkdir_persists_across_processes() {
    let env = TestEnv::new("/clawfs-mkdir-prefix");
    let path = format!("{}/dir1", env.prefix);

    let create = env
        .command("mkdir")
        .arg(&path)
        .output()
        .expect("failed to run mkdir");
    assert!(
        create.status.success(),
        "mkdir failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        create.status,
        String::from_utf8_lossy(&create.stdout),
        String::from_utf8_lossy(&create.stderr)
    );

    let stat = env
        .command("python3")
        .arg("-c")
        .arg("import os, sys; sys.exit(0 if os.path.isdir(sys.argv[1]) else 1)")
        .arg(&path)
        .status()
        .expect("failed to stat created directory");
    assert_eq!(
        stat.code(),
        Some(0),
        "directory did not persist across processes"
    );
}

#[test]
fn preload_recursive_rm_directory_succeeds() {
    let env = TestEnv::new("/clawfs-rmrf-prefix");
    let root = format!("{}/dir1", env.prefix);

    let create = env
        .command("python3")
        .arg("-c")
        .arg(
            r#"
import os, sys
root = sys.argv[1]
os.makedirs(root + "/sub", exist_ok=True)
for i in range(50):
    with open(f"{root}/batch_{i}.txt", "w") as f:
        f.write("x")
with open(root + "/sub/nested.txt", "w") as f:
    f.write("nested")
"#,
        )
        .arg(&root)
        .output()
        .expect("failed to create directory tree");
    assert!(
        create.status.success(),
        "tree setup failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        create.status,
        String::from_utf8_lossy(&create.stdout),
        String::from_utf8_lossy(&create.stderr)
    );

    let remove = env
        .command("rm")
        .arg("-rf")
        .arg(&root)
        .output()
        .expect("failed to run rm -rf");
    assert!(
        remove.status.success(),
        "rm -rf failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        remove.status,
        String::from_utf8_lossy(&remove.stdout),
        String::from_utf8_lossy(&remove.stderr)
    );

    let stat = env
        .command("python3")
        .arg("-c")
        .arg("import os, sys; sys.exit(0 if os.path.exists(sys.argv[1]) else 1)")
        .arg(&root)
        .status()
        .expect("failed to stat removed directory");
    assert_eq!(stat.code(), Some(1), "directory still exists after rm -rf");
}
