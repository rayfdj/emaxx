#!/usr/bin/env python3
"""Measure Linux crash-handler time with different guarded memory reservations."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import statistics
import subprocess
import sys
import time


CHILD = r'''import ctypes, json, mmap, os, resource, sys
from pathlib import Path

# Match the existing syscall diagnostic's resource limit. Linux pipe-based
# core handlers can still run; preserve and record the host's actual policy.
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
size = int(sys.argv[1]) * 1024 * 1024
if size:
    page = os.sysconf("SC_PAGE_SIZE")
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                         ctypes.c_int, ctypes.c_int, ctypes.c_long]
    libc.mmap.restype = ctypes.c_void_p
    libc.mprotect.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
    libc.mprotect.restype = ctypes.c_int
    address = libc.mmap(None, size + page, 0, mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS, -1, 0)
    if address == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_errno(), "mmap")
    if libc.mprotect(address + page, size, mmap.PROT_READ | mmap.PROT_WRITE):
        raise OSError(ctypes.get_errno(), "mprotect")
    ctypes.c_ubyte.from_address(address + page + size - 1).value = 1
print(json.dumps({"reserved_bytes": size,
                  "core_limit": list(resource.getrlimit(resource.RLIMIT_CORE)),
                  "coredump_filter": Path("/proc/self/coredump_filter").read_text().strip(),
                  "maps": Path("/proc/self/maps").read_text()}), flush=True)
os.abort()
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("this diagnostic measures the Linux crash handler")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    script = output / "core-child.py"
    script.write_text(CHILD)
    executable = Path(sys.executable).resolve(strict=True)
    environment = {"LANG": "C", "PATH": os.defpath}
    evidence = {
        "kind": "diagnostic_only",
        "host": platform.uname()._asdict(),
        "core_pattern": Path("/proc/sys/kernel/core_pattern").read_text().strip(),
        "python_sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
        "script_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
        "environment": environment,
        "runs": [],
    }
    for round_index, sizes in enumerate([(0, 128, 8192), (8192, 128, 0)], 1):
        for size in sizes:
            stem = str(round_index) + "-" + str(size)
            stdout = output / (stem + ".stdout")
            stderr = output / (stem + ".stderr")
            command = [str(executable), str(script), str(size)]
            start = time.monotonic()
            with stdout.open("wb") as out, stderr.open("wb") as err:
                child = subprocess.run(command, env=environment, cwd=output,
                                       stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                       timeout=90)
            elapsed = time.monotonic() - start
            record = {
                "round": round_index, "reserved_mib": size,
                "command": command, "exit_code": child.returncode,
                "seconds": elapsed,
                "stdout_sha256": hashlib.sha256(stdout.read_bytes()).hexdigest(),
                "stderr_sha256": hashlib.sha256(stderr.read_bytes()).hexdigest(),
            }
            evidence["runs"].append(record)
            (output / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
            if child.returncode != -signal.SIGABRT:
                raise RuntimeError("child did not reach the intentional abort: " + stem)
            metadata = json.loads(stdout.read_text())
            if metadata["reserved_bytes"] != size * 1024 * 1024:
                raise RuntimeError("child memory reservation differs: " + stem)
            print(size, "MiB", round(elapsed, 6), "seconds", flush=True)
    medians = {
        str(size): statistics.median(row["seconds"] for row in evidence["runs"]
                                    if row["reserved_mib"] == size)
        for size in (0, 128, 8192)
    }
    (output / "medians.json").write_text(json.dumps(medians, indent=2) + "\n")
    print("Diagnostic only: intentional aborts, no frozen outcomes certified.")


if __name__ == "__main__":
    main()
