#!/usr/bin/env python3
"""Replay a selected Rust gate inventory under GDB; never certify a full gate."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

import serial_grouped_gate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filter", required=True)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    if not arguments.filter.strip():
        parser.error("--filter must select an explicit nonempty test inventory")

    gate = serial_grouped_gate.gate
    output = arguments.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = gate.gate_environment(True)
    summary = {
        "scope": "Selected library tests under GDB; not a full gate or performance run",
        "git": gate.git_state(),
        "filter": arguments.filter,
        "timeout_seconds": gate.DEFAULT_TIMEOUT_SECONDS,
        "environment": {
            key: environment.get(key)
            for key in (
                "LANG", "LC_ALL", "RUST_MIN_STACK", "RUST_TEST_THREADS",
                "RUST_BACKTRACE", "EMAXX_IMAGE_TEMPLATE", "EMAXX_FIXTURE_IMAGE_DIR",
                "CARGO_PROFILE_GATE_DEBUG", "EMAXX_GC_VERIFY",
            )
        },
        "status": "building",
    }

    def save() -> None:
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    save()
    try:
        binary = gate.discover_test_binary("gate", output)
        summary["test_binary"] = {
            "path": str(binary), "sha256": gate.sha256_file(binary),
        }

        def inventory(label: str, selection: list[str], *, allow_empty: bool = False) -> list[str]:
            completed = subprocess.run(
                [str(binary), *selection, "--list"], cwd=gate.PROJECT_ROOT,
                env=environment, capture_output=True, text=True, timeout=60,
            )
            (output / f"{label}.log").write_text(completed.stdout + completed.stderr)
            if completed.returncode:
                raise gate.GateError(f"{label} exited with {completed.returncode}")
            return gate.parse_inventory(completed.stdout, allow_empty=allow_empty)

        all_tests = inventory("full-inventory", [])
        selected = inventory("selected-inventory", [arguments.filter])
        ignored = inventory("ignored-inventory", [arguments.filter, "--ignored"], allow_empty=True)
        assert set(ignored) <= set(selected) <= set(all_tests)
        summary.update(expected_tests=selected, ignored_tests=ignored, total_tests=len(all_tests))
        test_command = [str(binary), arguments.filter, "--test-threads", "1"]
        command = [
            "gdb", "--batch", "--return-child-result",
            "-ex", "set pagination off", "-ex", "set print thread-events off",
            "-ex", "run",
            "-ex", "python gdb.execute('thread apply all bt') if gdb.selected_inferior().pid else None",
            "-ex", "python gdb.execute('info registers') if gdb.selected_inferior().pid else None",
            "-ex", "python gdb.execute('x/12i $pc-24') if gdb.selected_inferior().pid else None",
            "--args", *test_command,
        ]
        summary.update(status="running", command=command, test_command=test_command)
        save()
        started = time.monotonic()
        log = output / "backtrace.log"
        with log.open("wb") as stream:
            process = subprocess.Popen(
                command, cwd=gate.PROJECT_ROOT, env=environment,
                stdout=stream, stderr=subprocess.STDOUT, start_new_session=True,
            )
            timed_out = False
            try:
                process.wait(timeout=gate.DEFAULT_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                timed_out = True
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
        text = log.read_text(errors="replace")
        executed = re.findall(r"^test (\S+) \.\.\.", text, re.MULTILINE)
        summary.update(
            exit_code=process.returncode, timed_out=timed_out,
            elapsed_seconds=time.monotonic() - started, executed_tests=executed,
            source_after=gate.git_state(),
        )
        save()
        if timed_out or process.returncode:
            raise gate.GateError(f"debugger exited with {process.returncode}; timed_out={timed_out}")
        result = gate.parse_test_result(text)
        summary["result"] = result
        gate.validate_test_result(result, len(selected), len(all_tests), len(ignored))
        if sorted(executed) != selected:
            raise gate.GateError("executed test names differ from the selected inventory")
        if summary["source_after"] != summary["git"]:
            raise gate.GateError("source state changed during the diagnostic")
        summary["status"] = "selected diagnostic passed"
        return 0
    except BaseException as error:
        summary.update(status="failed", error=repr(error))
        raise
    finally:
        save()


if __name__ == "__main__":
    raise SystemExit(main())
