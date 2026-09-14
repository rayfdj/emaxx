#!/usr/bin/env python3
"""Collect real Linux sandbox failures; this is not a frozen certificate."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import shlex
import shutil
import signal
import subprocess
import sys
import time


def no_core_dump():
    # Diagnostic traces need the terminating syscall, not a multi-GB dump.
    # This applies only to these diagnostic children, never to frozen runs.
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-source", type=Path, required=True)
    parser.add_argument("--emaxx", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("requires Linux syscall tracing")
    source = args.oracle_source.resolve(strict=True)
    binaries = {"oracle": source / "src/emacs", "emaxx": args.emaxx.resolve(strict=True)}
    filters = [source / "lib-src" / name for name in
               ("seccomp-filter.bpf", "seccomp-filter-exec.bpf")]
    tools = {name: shutil.which(name) for name in ("strace", "bash", "bwrap")}
    if not all(tools.values()):
        parser.error("requires strace, bash and bubblewrap")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    inputs = list(binaries.values()) + filters
    inputs += [binary.with_suffix(".pdmp") for binary in binaries.values()]
    inputs += [Path(path) for path in tools.values()]
    evidence = {
        "kind": "diagnostic_only",
        "environment": {},
        "core_limit": 0,
        "inputs_sha256": {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in inputs},
        "kernel": list(os.uname()),
        "kernel_settings": {},
        "commands": [],
    }
    for name in ("kernel/apparmor_restrict_unprivileged_userns",
                 "kernel/unprivileged_userns_clone", "user/max_user_namespaces",
                 "kernel/core_pattern", "fs/suid_dumpable"):
        path = Path("/proc/sys") / name
        if path.is_file():
            evidence["kernel_settings"][name] = path.read_text().strip()

    def run(label, command, trace=False):
        if trace:
            command = [tools["strace"], "-ff", "-tt", "-T", "-s", "160",
                       "-o", str(output / (label + ".strace")), *command]
        started = time.monotonic()
        timed_out = False
        with (output / (label + ".stdout")).open("wb") as stdout, \
                (output / (label + ".stderr")).open("wb") as stderr:
            with subprocess.Popen(command, env={}, cwd=output, stdout=stdout,
                                  stderr=stderr, preexec_fn=no_core_dump,
                                  start_new_session=True) as process:
                try:
                    process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    # The group owns strace and its actual traced descendants.
                    # Do not leave a crashed editor behind or lose later probes.
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
        evidence["commands"].append(dict(label=label, command=command,
                                         exit_code=process.returncode,
                                         timed_out=timed_out,
                                         seconds=time.monotonic() - started))
        (output / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(label, "exit", process.returncode, flush=True)

    run("bubblewrap-namespace", [tools["bwrap"], "--ro-bind", "/", "/", "--", "/bin/true"])
    for who, binary in binaries.items():
        editor = [str(binary), "--quick", "--batch"]
        expression = ['--eval=(message "Hi")']
        run(who + "-baseline", editor + expression)
        run(who + "-seccomp", editor + ["--seccomp=" + str(filters[0])] + expression, trace=True)
        wrapped = [tools["bwrap"], "--ro-bind", "/", "/", "--seccomp", "20", "--",
                   *editor, *expression]
        shell = shlex.join(wrapped) + " 20< " + shlex.quote(str(filters[1]))
        run(who + "-bubblewrap", [tools["bash"], "-c", shell], trace=True)
    for path, digest in evidence["inputs_sha256"].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest:
            raise RuntimeError("Diagnostic input changed: " + path)
    evidence["outputs_sha256"] = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(output.iterdir()) if path.name != "evidence.json"
    }
    (output / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("DIAGNOSTIC ONLY: no compatibility outcomes are certified.", flush=True)
    return int(any(command["exit_code"] for command in evidence["commands"]))


if __name__ == "__main__":
    raise SystemExit(main())
