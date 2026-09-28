#!/usr/bin/env python3
"""Replay a selected Rust gate inventory, optionally under GDB; not a full gate."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time

import serial_grouped_gate


def execute(command: list[str], environment: dict[str, str], log: Path) -> dict:
    """Keep the original process timeout and raw output for each separate run."""
    gate = serial_grouped_gate.gate
    started = time.monotonic()
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
    return dict(
        command=command, exit_code=process.returncode, timed_out=timed_out,
        elapsed_seconds=time.monotonic() - started,
        executed_tests=re.findall(r"^test (\S+) \.\.\.", text, re.MULTILINE),
    )


def validate_execution(record: dict, text: str, selected: list[str],
                       all_tests: list[str], ignored: list[str]) -> None:
    gate = serial_grouped_gate.gate
    if record["timed_out"] or record["exit_code"]:
        raise gate.GateError(
            f"diagnostic exited with {record['exit_code']}; timed_out={record['timed_out']}"
        )
    result = gate.parse_test_result(text)
    record["result"] = result
    gate.validate_test_result(result, len(selected), len(all_tests), len(ignored))
    if sorted(record["executed_tests"]) != selected:
        raise gate.GateError("executed test names differ from the selected inventory")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filter", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--prelude-filter",
        help="run this original inventory first in a separate process sharing the fixture image",
    )
    parser.add_argument(
        "--plain", action="store_true",
        help="execute the recorded test binary directly, without a debugger",
    )
    arguments = parser.parse_args()
    if not arguments.filter.strip():
        parser.error("--filter must select an explicit nonempty test inventory")
    if arguments.prelude_filter is not None and not arguments.prelude_filter.strip():
        parser.error("--prelude-filter must select an explicit nonempty test inventory")

    gate = serial_grouped_gate.gate
    output = arguments.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = gate.gate_environment(True)
    summary = {
        "scope": (
            "Selected library tests directly; not a full gate or performance run"
            if arguments.plain else
            "Selected library tests under GDB; not a full gate or performance run"
        ),
        "git": gate.git_state(),
        "filter": arguments.filter,
        "prelude_filter": arguments.prelude_filter,
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
        "fixture_images_before": [
            {"path": str(path), "sha256": gate.sha256_file(path), "bytes": path.stat().st_size}
            for path in sorted(Path(environment["EMAXX_FIXTURE_IMAGE_DIR"]).glob("*.pdmp"))
        ],
    }

    def save() -> None:
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    save()
    try:
        binary = gate.discover_test_binary("gate", output)
        summary["test_binary"] = {
            "path": str(binary), "sha256": gate.sha256_file(binary),
        }
        # Preserve the exact executable behind stack addresses so compiler
        # spills and native frames can be inspected after the CI VM exits.
        # Execute the original path below: relocating it can change tests'
        # invocation-directory and fixture lookup behavior.
        saved_binary = output / "libtest"
        shutil.copy2(binary, saved_binary)
        if gate.sha256_file(saved_binary) != summary["test_binary"]["sha256"]:
            raise gate.GateError("diagnostic executable copy differs from the tested binary")
        summary["test_binary"]["artifact"] = saved_binary.name

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
        if arguments.prelude_filter:
            selection = [arguments.prelude_filter]
            prelude_selected = inventory("prelude-inventory", selection)
            prelude_ignored = inventory(
                "prelude-ignored-inventory", [*selection, "--ignored"], allow_empty=True,
            )
            assert set(prelude_ignored) <= set(prelude_selected) <= set(all_tests)
            prelude_command = [str(binary), *selection, "--test-threads", "1"]
            summary.update(status="running prelude", prelude={
                "command": prelude_command, "expected_tests": prelude_selected,
                "ignored_tests": prelude_ignored,
            })
            save()
            prelude_log = output / "prelude.log"
            summary["prelude"].update(execute(prelude_command, environment, prelude_log))
            save()
            validate_execution(summary["prelude"], prelude_log.read_text(errors="replace"),
                               prelude_selected, all_tests, prelude_ignored)
            summary["prelude"]["status"] = "passed"
            save()
        test_command = [str(binary), arguments.filter, "--test-threads", "1"]
        command = test_command if arguments.plain else [
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
        log = output / ("replay.log" if arguments.plain else "backtrace.log")
        summary.update(execute(command, environment, log), source_after=gate.git_state())
        save()
        validate_execution(summary, log.read_text(errors="replace"), selected, all_tests, ignored)
        if summary["source_after"] != summary["git"]:
            raise gate.GateError("source state changed during the diagnostic")
        summary["status"] = "selected diagnostic passed"
        return 0
    except BaseException as error:
        summary.update(status="failed", error=repr(error))
        raise
    finally:
        # These are inputs to subsequent processes, not merely a speed cache.
        # Keep the actual image for the startup-history diagnosis even on failure.
        try:
            fixture_images = Path(environment["EMAXX_FIXTURE_IMAGE_DIR"])
            summary["fixture_images"] = []
            for source in sorted(fixture_images.glob("*.pdmp")):
                target = output / "fixture-images" / source.name
                target.parent.mkdir(exist_ok=True)
                shutil.copy2(source, target)
                digest = gate.sha256_file(source)
                if gate.sha256_file(target) != digest:
                    raise gate.GateError("saved fixture image differs from the executed input")
                summary["fixture_images"].append({
                    "source": str(source), "artifact": str(target.relative_to(output)),
                    "sha256": digest, "bytes": source.stat().st_size,
                })
        except BaseException as error:
            summary.update(status="failed", image_preservation_error=repr(error))
            raise
        finally:
            save()


if __name__ == "__main__":
    raise SystemExit(main())
