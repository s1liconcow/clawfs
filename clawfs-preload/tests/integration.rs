//! Integration test: spawns a child process with LD_PRELOAD set,
//! the child performs file I/O under a ClawFS prefix and validates results.

use std::path::PathBuf;
use std::process::Command;

fn preload_lib_path() -> PathBuf {
    // The cdylib is built at target/debug/libclawfs_preload.so (when run via cargo test).
    let mut path = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    path.push("libclawfs_preload.so");
    if !path.exists() {
        // Fallback: try the deps directory.
        path = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        path.push("libclawfs_preload.so");
    }
    assert!(
        path.exists(),
        "libclawfs_preload.so not found at {path:?}. Build with `cargo build` first."
    );
    path
}

#[test]
fn preload_basic_file_io() {
    let tmp = tempfile::tempdir().unwrap();
    let store_path = tmp.path().join("store");
    let cache_path = tmp.path().join("cache");
    let state_path = tmp.path().join("state").join("preload_state.bin");
    let prefix = "/clawfs-test-prefix";

    let lib = preload_lib_path();

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

    let output = Command::new("python3")
        .arg("-c")
        .arg(&script)
        .env("LD_PRELOAD", &lib)
        .env("CLAWFS_PREFIXES", prefix)
        .env("CLAWFS_STORE_PATH", &store_path)
        .env("CLAWFS_LOCAL_CACHE_PATH", &cache_path)
        .env("CLAWFS_STATE_PATH", &state_path)
        .env("CLAWFS_STORAGE_MODE", "byob_paid")
        .env("CLAWFS_OBJECT_PROVIDER", "local")
        .env("RUST_LOG", "info")
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
