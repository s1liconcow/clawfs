#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import random
import shutil
import stat
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def deterministic_bytes(seed: str, size: int) -> bytes:
    output = bytearray()
    counter = 0
    while len(output) < size:
        output.extend(hashlib.sha256(f"{seed}:{counter}".encode()).digest())
        counter += 1
    return bytes(output[:size])


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def ensure_missing(path: Path) -> None:
    try:
        os.lstat(path)
    except FileNotFoundError:
        return
    raise AssertionError(f"expected {path} to be absent")


def write_file(path: Path, payload: bytes, do_fsync: bool) -> None:
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    try:
        written = 0
        while written < len(payload):
            written += os.write(fd, payload[written:])
        if do_fsync:
            os.fsync(fd)
    finally:
        os.close(fd)


def read_exact(path: Path) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def compare_trees(expected_root: Path, actual_root: Path) -> None:
    for root, dirs, files in os.walk(expected_root):
        rel = Path(root).relative_to(expected_root)
        actual_dir = actual_root / rel
        if not actual_dir.is_dir():
            raise AssertionError(f"missing directory {actual_dir}")

        expected_entries = sorted(dirs + files)
        actual_entries = sorted(entry.name for entry in actual_dir.iterdir())
        if expected_entries != actual_entries:
            raise AssertionError(
                f"directory mismatch at {actual_dir}: expected {expected_entries}, got {actual_entries}"
            )

        for filename in files:
            expected_path = Path(root) / filename
            actual_path = actual_dir / filename
            if not actual_path.is_file():
                raise AssertionError(f"missing file {actual_path}")
            expected_data = read_exact(expected_path)
            actual_data = read_exact(actual_path)
            if actual_data != expected_data:
                raise AssertionError(f"content mismatch for {actual_path}")
            expected_mode = stat.S_IMODE(os.lstat(expected_path).st_mode)
            actual_mode = stat.S_IMODE(os.lstat(actual_path).st_mode)
            if actual_mode != expected_mode:
                raise AssertionError(
                    f"mode mismatch for {actual_path}: expected {oct(expected_mode)}, got {oct(actual_mode)}"
                )

    for root, dirs, files in os.walk(actual_root):
        rel = Path(root).relative_to(actual_root)
        expected_dir = expected_root / rel
        if not expected_dir.is_dir():
            raise AssertionError(f"unexpected directory {root}")
        expected_entries = sorted(entry.name for entry in expected_dir.iterdir())
        actual_entries = sorted(dirs + files)
        if expected_entries != actual_entries:
            raise AssertionError(
                f"directory mismatch at {root}: expected {expected_entries}, got {actual_entries}"
            )


def workload_round(mount_base: Path, ref_base: Path, round_idx: int) -> None:
    shared_mount = mount_base / "shared"
    shared_ref = ref_base / "shared"
    nested_mount = mount_base / "nested" / f"round_{round_idx}"
    nested_ref = ref_base / "nested" / f"round_{round_idx}"
    retained_mount = mount_base / "retained" / f"round_{round_idx}"
    retained_ref = ref_base / "retained" / f"round_{round_idx}"

    for path in (shared_mount, shared_ref, nested_mount, nested_ref, retained_mount, retained_ref):
        path.mkdir(parents=True, exist_ok=True)

    shared_mount_lock = threading.Lock()
    shared_ref_lock = threading.Lock()

    def worker(worker_id: int) -> None:
        rng = random.Random(round_idx * 1000 + worker_id)
        for iteration in range(40):
            stem = f"r{round_idx:02d}_w{worker_id:02d}_i{iteration:03d}"
            payload = deterministic_bytes(stem, 256 + ((worker_id + iteration) % 7) * 173)
            temp_name = f"{stem}.tmp"
            final_name = f"{stem}.bin"
            renamed_name = f"{stem}.live"
            keep_file = iteration % 5 == 0

            for root in (shared_mount, shared_ref):
                temp_path = root / temp_name
                final_path = root / final_name
                renamed_path = root / renamed_name
                write_file(temp_path, payload, do_fsync=True)
                os.lstat(temp_path)
                os.rename(temp_path, final_path)
                os.lstat(final_path)
                if read_exact(final_path) != payload:
                    raise AssertionError(f"read-after-create mismatch for {final_path}")
                os.rename(final_path, renamed_path)
                os.lstat(renamed_path)

            with shared_mount_lock:
                fsync_dir(shared_mount)
            with shared_ref_lock:
                fsync_dir(shared_ref)

            renamed_mount = shared_mount / renamed_name
            renamed_ref = shared_ref / renamed_name
            if keep_file:
                retained_name = f"keep_{stem}.bin"
                target_mount = retained_mount / retained_name
                target_ref = retained_ref / retained_name
                os.rename(renamed_mount, target_mount)
                os.rename(renamed_ref, target_ref)
                mode = 0o640 | (worker_id & 0o7)
                os.chmod(target_mount, mode)
                os.chmod(target_ref, mode)
                if iteration % 10 == 0:
                    trailer = deterministic_bytes(f"{stem}:tail", 64)
                    fd = os.open(target_mount, os.O_RDWR)
                    try:
                        os.lseek(fd, len(payload), os.SEEK_SET)
                        os.write(fd, trailer)
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    with open(target_ref, "ab") as handle:
                        handle.write(trailer)
                        handle.flush()
                        os.fsync(handle.fileno())
                if read_exact(target_mount) != read_exact(target_ref):
                    raise AssertionError(f"retained payload mismatch for {target_mount}")
            else:
                os.unlink(renamed_mount)
                os.unlink(renamed_ref)
                ensure_missing(renamed_mount)
                ensure_missing(renamed_ref)

            if rng.randrange(3) == 0:
                scratch_mount = nested_mount / f"scratch_{worker_id}_{iteration}"
                scratch_ref = nested_ref / f"scratch_{worker_id}_{iteration}"
                for root in (scratch_mount, scratch_ref):
                    root.mkdir(parents=True, exist_ok=True)
                    probe = root / "probe.txt"
                    write_file(probe, deterministic_bytes(stem + ":probe", 128), do_fsync=True)
                    os.lstat(probe)
                    os.unlink(probe)
                    ensure_missing(probe)
                    os.rmdir(root)

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(worker, worker_id) for worker_id in range(6)]
        for future in futures:
            future.result()

    deep_mount = mount_base / "deep" / f"round_{round_idx}" / "a" / "b" / "c"
    deep_ref = ref_base / "deep" / f"round_{round_idx}" / "a" / "b" / "c"
    deep_mount.mkdir(parents=True, exist_ok=True)
    deep_ref.mkdir(parents=True, exist_ok=True)
    for iteration in range(24):
        name = f"deep_{round_idx:02d}_{iteration:03d}.dat"
        payload = deterministic_bytes(name, 1024 + iteration * 19)
        mount_path = deep_mount / name
        ref_path = deep_ref / name
        write_file(mount_path, payload, do_fsync=True)
        write_file(ref_path, payload, do_fsync=True)
        if read_exact(mount_path) != payload:
            raise AssertionError(f"deep read-back mismatch for {mount_path}")

    compare_trees(ref_base, mount_base)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mount-base", required=True)
    parser.add_argument("--ref-base", required=True)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--mode", choices=("run", "verify"), default="run")
    args = parser.parse_args()

    mount_base = Path(args.mount_base)
    ref_base = Path(args.ref_base)

    if args.mode == "run":
        reset_dir(mount_base)
        reset_dir(ref_base)
        for round_idx in range(args.rounds):
            workload_round(mount_base, ref_base, round_idx)
    else:
        compare_trees(ref_base, mount_base)

    print(
        json.dumps(
            {
                "mode": args.mode,
                "mount_base": str(mount_base),
                "ref_base": str(ref_base),
                "rounds": args.rounds,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
