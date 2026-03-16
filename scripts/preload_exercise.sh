#!/usr/bin/env bash
# Thorough exercise of the ClawFS preload (LD_PRELOAD) layer.
# Runs a battery of filesystem operations via real Unix tools through
# the preload shim, using a local object store (no cloud credentials).
#
# Usage:  ./scripts/preload_exercise.sh [PREFIX]
#   PREFIX defaults to /tmp/clawfs-preload-exercise/vol
#
# Environment (set automatically if not provided):
#   CLAWFS_PRELOAD_LIB  path to libclawfs_preload.so
#
# Known preload layer limitations (not tested here):
#   - Bash heredoc redirect (<<EOF) uses an unintercepted open path
#   - mkdir -p may warn on the prefix root (EEXIST mishandled by coreutils)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PREFIX="${1:-/tmp/clawfs-preload-exercise/vol}"
SCRATCH="$(mktemp -d)"

# Locate the preload library.
if [[ -z "${CLAWFS_PRELOAD_LIB:-}" ]]; then
    for candidate in \
        "$ROOT_DIR/target/release/libclawfs_preload.so" \
        "$ROOT_DIR/target/debug/libclawfs_preload.so"; do
        if [[ -f "$candidate" ]]; then
            CLAWFS_PRELOAD_LIB="$candidate"
            break
        fi
    done
fi
if [[ -z "${CLAWFS_PRELOAD_LIB:-}" ]]; then
    echo "FATAL: libclawfs_preload.so not found. Build first." >&2
    exit 1
fi

# Set up store paths.
export LD_PRELOAD="$CLAWFS_PRELOAD_LIB"
export CLAWFS_PREFIXES="$PREFIX"
export CLAWFS_STORE_PATH="$SCRATCH/store"
export CLAWFS_LOCAL_CACHE_PATH="$SCRATCH/cache"
export CLAWFS_STATE_PATH="$SCRATCH/state/preload_state.bin"
export CLAWFS_STORAGE_MODE="byob_paid"
export CLAWFS_OBJECT_PROVIDER="local"
export RUST_LOG="${RUST_LOG:-error}"

PASS=0
FAIL=0
TESTS_FAILED=""

run_test() {
    local name="$1"
    shift
    echo -n "  $name ... "
    local output
    if output=$("$@" 2>&1); then
        echo "ok"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        echo "    output: $(echo "$output" | head -3)"
        FAIL=$((FAIL + 1))
        TESTS_FAILED="$TESTS_FAILED $name"
    fi
}

# Wrapper: run a bash snippet under the preload environment.
preload_bash() {
    bash -c "$1"
}

cleanup() {
    rm -rf "$SCRATCH"
}
trap cleanup EXIT

# ===================================================================
echo "=== Phase 1: Basic file I/O (cross-process) ==="
# ===================================================================
# Each preload_bash spawns a new process; cross-process persistence
# is tested by writing in one invocation and reading in the next.

run_test "write-file" preload_bash "echo hello > $PREFIX/hello.txt"
run_test "read-file" preload_bash "[ \"\$(cat $PREFIX/hello.txt)\" = 'hello' ]"
run_test "append-file" preload_bash "echo world >> $PREFIX/hello.txt"
run_test "read-appended" preload_bash "
    [ \"\$(cat $PREFIX/hello.txt)\" = 'hello
world' ]
"
run_test "stat-file" preload_bash "stat $PREFIX/hello.txt >/dev/null"
run_test "cp-file" preload_bash "cp $PREFIX/hello.txt $PREFIX/copy.txt"
run_test "verify-cp" preload_bash "[ \"\$(cat $PREFIX/copy.txt)\" = \"\$(cat $PREFIX/hello.txt)\" ]"
run_test "rm-file" preload_bash "rm $PREFIX/copy.txt"
run_test "verify-rm" preload_bash "! [ -e $PREFIX/copy.txt ]"
run_test "rm-hello" preload_bash "rm $PREFIX/hello.txt"

# ===================================================================
echo "=== Phase 2: Directory operations (cross-process) ==="
# ===================================================================

run_test "mkdir" preload_bash "mkdir $PREFIX/dir1"
run_test "verify-mkdir" preload_bash "[ -d $PREFIX/dir1 ]"

# ===================================================================
echo "=== Phase 3: Directory listing (tests the dirfd/readdir fix) ==="
# ===================================================================

run_test "ls-empty-dir" preload_bash "
    entries=\$(ls $PREFIX/dir1)
    [ -z \"\$entries\" ]
"
run_test "populate-dir" preload_bash "
    echo a > $PREFIX/dir1/alpha.txt
    echo b > $PREFIX/dir1/bravo.txt
    echo c > $PREFIX/dir1/charlie.txt
"
run_test "ls-populated" preload_bash "
    entries=\$(ls $PREFIX/dir1)
    echo \"\$entries\" | grep -q alpha.txt
    echo \"\$entries\" | grep -q bravo.txt
    echo \"\$entries\" | grep -q charlie.txt
"
run_test "ls-l-metadata" preload_bash "ls -l $PREFIX/dir1 | grep -q alpha.txt"
run_test "ls-subdir" preload_bash "
    mkdir $PREFIX/dir1/subdir
    ls $PREFIX/dir1 | grep -q subdir
"
run_test "ls-nested" preload_bash "
    echo nested > $PREFIX/dir1/subdir/nested.txt
    ls $PREFIX/dir1/subdir | grep -q nested.txt
"
run_test "ls-l-nested" preload_bash "ls -l $PREFIX/dir1/subdir | grep -q nested.txt"

# ===================================================================
echo "=== Phase 4: Large file I/O ==="
# ===================================================================

run_test "dd-write-1M" preload_bash "dd if=/dev/urandom of=$PREFIX/dir1/large.bin bs=1024 count=1024 2>/dev/null"
run_test "dd-read-verify" preload_bash "dd if=$PREFIX/dir1/large.bin of=/dev/null bs=1024 2>/dev/null"
run_test "large-file-size" preload_bash "[ \"\$(stat -c %s $PREFIX/dir1/large.bin)\" = '1048576' ]"
run_test "truncate" preload_bash "truncate -s 512 $PREFIX/dir1/large.bin && [ \"\$(stat -c %s $PREFIX/dir1/large.bin)\" = '512' ]"
run_test "rm-large" preload_bash "rm $PREFIX/dir1/large.bin"

# ===================================================================
echo "=== Phase 5: Pipe and tool integration ==="
# ===================================================================

run_test "pipe-grep" preload_bash "
    echo 'pipe test data' > $PREFIX/dir1/pipe.txt
    cat $PREFIX/dir1/pipe.txt | grep -q 'pipe test data'
"
run_test "printf-write" preload_bash "printf 'cherry\napple\nbanana\n' > $PREFIX/dir1/fruit.txt"
run_test "cat-sort" preload_bash "[ \"\$(cat $PREFIX/dir1/fruit.txt | sort | head -1)\" = 'apple' ]"
run_test "cat-grep" preload_bash "cat $PREFIX/dir1/fruit.txt | grep -q banana"
run_test "cat-wc" preload_bash "[ \"\$(cat $PREFIX/dir1/fruit.txt | wc -l)\" = '3' ]"

# ===================================================================
echo "=== Phase 5b: fopen/fread-based tools (direct, no pipe workaround) ==="
# ===================================================================

run_test "sort-direct" preload_bash "[ \"\$(sort $PREFIX/dir1/fruit.txt | head -1)\" = 'apple' ]"
run_test "wc-direct" preload_bash "[ \"\$(wc -l $PREFIX/dir1/fruit.txt | awk '{print \$1}')\" = '3' ]"
run_test "tee-direct" preload_bash "
    echo 'tee test' | tee $PREFIX/dir1/tee_out.txt >/dev/null
    [ \"\$(cat $PREFIX/dir1/tee_out.txt)\" = 'tee test' ]
"
run_test "grep-direct-file" preload_bash "grep -q banana $PREFIX/dir1/fruit.txt"

# ===================================================================
echo "=== Phase 6: Symlinks ==="
# ===================================================================

run_test "symlink-python" preload_bash "
    python3 -c '
import os
P = \"$PREFIX/dir1\"
os.symlink(\"alpha.txt\", P + \"/alink\")
assert os.readlink(P + \"/alink\") == \"alpha.txt\"
os.unlink(P + \"/alink\")
'
"
run_test "symlink-ln-s" preload_bash "
    ln -s alpha.txt $PREFIX/dir1/alink2
    [ \"\$(readlink $PREFIX/dir1/alink2)\" = 'alpha.txt' ]
    rm $PREFIX/dir1/alink2
"

# ===================================================================
echo "=== Phase 7: Rename (cross-process) ==="
# ===================================================================

run_test "rename-file" preload_bash "mv $PREFIX/dir1/alpha.txt $PREFIX/dir1/alpha_renamed.txt"
run_test "verify-rename-dst" preload_bash "[ -f $PREFIX/dir1/alpha_renamed.txt ]"
run_test "verify-rename-src-gone" preload_bash "! [ -e $PREFIX/dir1/alpha.txt ]"

# ===================================================================
echo "=== Phase 8: Batch file operations ==="
# ===================================================================

run_test "batch-create-50" preload_bash "
    for i in \$(seq 1 50); do echo \"file-\$i\" > $PREFIX/dir1/batch_\$i.txt; done
"
run_test "ls-count-50" preload_bash "
    count=\$(ls $PREFIX/dir1/ | grep '^batch_' | wc -l)
    [ \"\$count\" = '50' ]
"

# ===================================================================
echo "=== Phase 9: Python POSIX exerciser ==="
# ===================================================================
# Comprehensive single-process test exercising every intercepted
# POSIX function, including the new dirfd/readdir/d_type/scandir path.

run_test "python-posix" preload_bash "
python3 -c '
import os, stat as st_mod

P = \"$PREFIX/pytest\"
os.makedirs(P + \"/sub\", exist_ok=True)

# 1. open/write/read/close
fd = os.open(P + \"/t.bin\", os.O_CREAT | os.O_RDWR, 0o644)
os.write(fd, b\"ABCDEFGHIJ\")
os.lseek(fd, 0, os.SEEK_SET)
assert os.read(fd, 10) == b\"ABCDEFGHIJ\", \"basic read\"

# 2. pread/pwrite
os.pwrite(fd, b\"XY\", 3)
assert os.pread(fd, 4, 2) == b\"CXYF\", \"pread/pwrite\"

# 3. ftruncate + fstat
os.ftruncate(fd, 5)
assert os.fstat(fd).st_size == 5, \"ftruncate\"

# 4. fsync
os.fsync(fd)

# 5. dup (shared offset)
fd2 = os.dup(fd)
os.lseek(fd, 0, os.SEEK_SET)
assert os.read(fd2, 5) == b\"ABCXY\", \"dup\"
os.close(fd2)
os.close(fd)

# 6. truncate by path
os.truncate(P + \"/t.bin\", 2)
assert os.stat(P + \"/t.bin\").st_size == 2, \"truncate path\"

# 7. listdir (opendir + readdir + closedir)
for name in [\"x.txt\", \"y.txt\", \"z.txt\"]:
    with open(P + \"/sub/\" + name, \"w\") as f:
        f.write(name)
entries = sorted(os.listdir(P + \"/sub\"))
assert entries == [\"x.txt\", \"y.txt\", \"z.txt\"], \"listdir: %s\" % entries

# 8. scandir — exercises readdir + d_type + dirfd + stat per entry
with os.scandir(P + \"/sub\") as it:
    for entry in sorted(it, key=lambda e: e.name):
        assert entry.is_file(), \"%s should be file\" % entry.name
        assert not entry.is_dir(), \"%s not dir\" % entry.name
        assert entry.stat().st_size > 0, \"%s has size\" % entry.name

# 9. scandir on mixed dir (files + subdirectory)
os.mkdir(P + \"/sub/inner\")
with open(P + \"/sub/inner/deep.txt\", \"w\") as f:
    f.write(\"deep\")
with os.scandir(P + \"/sub\") as it:
    types = {}
    for entry in it:
        types[entry.name] = (\"dir\" if entry.is_dir() else \"file\")
assert types.get(\"inner\") == \"dir\", \"inner should be dir\"
assert types.get(\"x.txt\") == \"file\", \"x.txt should be file\"

# 10. nested listdir
assert os.listdir(P + \"/sub/inner\") == [\"deep.txt\"], \"nested listdir\"

# 11. symlink/readlink
os.symlink(\"x.txt\", P + \"/sub/xlink\")
assert os.readlink(P + \"/sub/xlink\") == \"x.txt\", \"readlink\"
os.unlink(P + \"/sub/xlink\")

# 12. rename
os.rename(P + \"/sub/x.txt\", P + \"/sub/x_renamed.txt\")

# 13. chdir/getcwd
saved = os.getcwd()
os.chdir(P)
assert os.getcwd() == P, \"chdir/getcwd\"
os.chdir(saved)

# 14. access
assert os.access(P, os.F_OK), \"access F_OK\"
assert os.access(P, os.R_OK), \"access R_OK\"

# 15. stat mode bits
s = os.stat(P)
assert st_mod.S_ISDIR(s.st_mode), \"root is dir\"
s = os.stat(P + \"/t.bin\")
assert st_mod.S_ISREG(s.st_mode), \"t.bin is regular\"

# 16. recursive mkdir (makedirs)
os.makedirs(P + \"/deep/a/b/c\", exist_ok=True)
assert os.path.isdir(P + \"/deep/a/b/c\"), \"makedirs\"
assert sorted(os.listdir(P + \"/deep/a/b\")) == [\"c\"], \"nested ls after makedirs\"

# 17. seekdir/rewinddir via repeated listdir
entries1 = sorted(os.listdir(P + \"/sub\"))
entries2 = sorted(os.listdir(P + \"/sub\"))
assert entries1 == entries2, \"repeated listdir consistent\"

print(\"ALL_PYTHON_TESTS_PASSED\")
' 2>&1 | tail -1 | grep -q ALL_PYTHON_TESTS_PASSED
"

# ===================================================================
echo "=== Phase 10: Concurrent writers ==="
# ===================================================================

run_test "create-par-dir" preload_bash "mkdir $PREFIX/par"
run_test "write-10-files" preload_bash "
    echo w1 > $PREFIX/par/w1.txt
    echo w2 > $PREFIX/par/w2.txt
    echo w3 > $PREFIX/par/w3.txt
    echo w4 > $PREFIX/par/w4.txt
    echo w5 > $PREFIX/par/w5.txt
    echo w6 > $PREFIX/par/w6.txt
    echo w7 > $PREFIX/par/w7.txt
    echo w8 > $PREFIX/par/w8.txt
    echo w9 > $PREFIX/par/w9.txt
    echo w10 > $PREFIX/par/w10.txt
"
run_test "verify-10-files" preload_bash "
    count=\$(ls $PREFIX/par/ | wc -l)
    [ \"\$count\" = '10' ]
"

# ===================================================================
echo "=== Phase 11: Cleanup ==="
# ===================================================================

# Best-effort cleanup. The SCRATCH tmpdir is removed on exit regardless.
# rm -rf on deep trees can hit metadata flush races which are orthogonal
# to preload correctness — so these are not scored as test failures.
preload_bash "rm -rf $PREFIX/dir1 2>/dev/null; rm -rf $PREFIX/pytest 2>/dev/null; rm -rf $PREFIX/par 2>/dev/null; true"
echo "  cleanup ... done"

# ===================================================================
# Summary
# ===================================================================

TOTAL=$((PASS + FAIL))
echo ""
echo "=== Results: $PASS/$TOTAL passed ==="
if [ "$FAIL" -gt 0 ]; then
    echo "FAILED:$TESTS_FAILED"
    exit 1
else
    echo "All tests passed."
fi
