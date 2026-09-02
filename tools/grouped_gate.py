#!/usr/bin/env python3
"""Run the Rust gate in resource-isolated, coverage-checked groups.

The expensive GNU-image consumers run serially inside separate processes so
each process may safely reuse EMAXX_IMAGE_TEMPLATE.  Only measured pairs run
at the same time.  The script discovers the libtest inventory from the exact
binary it built and refuses missing, duplicate, or newly-unclassified eval
groups.  It never retries a failed test.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from typing import BinaryIO, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TIMEOUT_SECONDS = 3600

EVAL_PREFIXES = tuple(
    f"lisp::eval::tests::eval_0{number}::" for number in range(1, 6)
)
PRIMITIVES_PREFIX = "lisp::primitives::tests::"
COMPAT_RUNTIME_PREFIX = "lisp::primitives::compat_runtime_tests::"
TTY_PREFIX = "tty::"
BATCH_PREFIX = "batch::tests::"
CLASSIFIED_PREFIXES = EVAL_PREFIXES + (
    PRIMITIVES_PREFIX,
    COMPAT_RUNTIME_PREFIX,
    TTY_PREFIX,
    BATCH_PREFIX,
)
ALLOWED_IGNORED_TESTS = frozenset(
    {
        "lisp::eval::tests::eval_04::"
        "decode_coding_region_rewrites_dos_eol_in_place",
        "lisp::eval::tests::eval_04::"
        "encode_coding_string_substitutes_unencodable_ascii_and_latin1_chars",
        "tty::tty_differential_end_to_end",
        "tty::tty_smoke_end_to_end",
    }
)

RESULT_RE = re.compile(
    r"test result: (?P<status>ok|FAILED)\. "
    r"(?P<passed>\d+) passed; (?P<failed>\d+) failed; "
    r"(?P<ignored>\d+) ignored; (?P<measured>\d+) measured; "
    r"(?P<filtered>\d+) filtered out; finished in "
    r"(?P<reported_seconds>[0-9.]+)s"
)


class GateError(RuntimeError):
    """A fail-closed grouped-gate error."""


@dataclass(frozen=True)
class GroupSpec:
    name: str
    prefix: str | None
    template: bool
    test_threads: int

    def selected_names(
        self, inventory: Sequence[str], classified: set[str]
    ) -> list[str]:
        if self.prefix is None:
            return [name for name in inventory if name not in classified]
        return [name for name in inventory if name.startswith(self.prefix)]

    def command(self, test_binary: Path) -> list[str]:
        command = [str(test_binary)]
        if self.prefix is not None:
            command.append(self.prefix)
        else:
            for prefix in CLASSIFIED_PREFIXES:
                command.extend(("--skip", prefix))
        command.extend(("--test-threads", str(self.test_threads), "--quiet"))
        return command


EVAL_GROUPS = tuple(
    GroupSpec(f"eval_0{number}", prefix, True, 1)
    for number, prefix in enumerate(EVAL_PREFIXES, start=1)
)
PRIMITIVES_GROUP = GroupSpec("primitives", PRIMITIVES_PREFIX, True, 1)
COMPAT_RUNTIME_GROUP = GroupSpec(
    "compat_runtime", COMPAT_RUNTIME_PREFIX, True, 1
)
TTY_GROUP = GroupSpec("tty", TTY_PREFIX, True, 1)
BATCH_GROUP = GroupSpec("batch", BATCH_PREFIX, False, 2)
LIGHTWEIGHT_GROUP = GroupSpec("lightweight", None, False, 2)

EVAL_PHASES = (
    (EVAL_GROUPS[0], EVAL_GROUPS[1]),
    (EVAL_GROUPS[2], EVAL_GROUPS[3]),
    (EVAL_GROUPS[4],),
)
LIB_PHASES = EVAL_PHASES + (
    (PRIMITIVES_GROUP,),
    (COMPAT_RUNTIME_GROUP, TTY_GROUP),
    (BATCH_GROUP,),
    (LIGHTWEIGHT_GROUP,),
)


def parse_inventory(output: str, *, allow_empty: bool = False) -> list[str]:
    tests = []
    for line in output.splitlines():
        if line.endswith(": test"):
            tests.append(line[: -len(": test")])
    if not tests and not allow_empty:
        raise GateError("libtest --list returned no tests")
    if len(tests) != len(set(tests)):
        duplicates = sorted(name for name in set(tests) if tests.count(name) > 1)
        raise GateError(f"libtest inventory contains duplicate names: {duplicates}")
    return sorted(tests)


def classify_inventory(
    inventory: Sequence[str], scope: str
) -> tuple[dict[str, list[str]], tuple[tuple[GroupSpec, ...], ...]]:
    unknown_eval = [
        name
        for name in inventory
        if name.startswith("lisp::eval::tests::eval_")
        and not name.startswith(EVAL_PREFIXES)
    ]
    if unknown_eval:
        raise GateError(
            "new eval groups require an explicit resource classification: "
            + ", ".join(unknown_eval)
        )

    phases = EVAL_PHASES if scope == "eval" else LIB_PHASES
    specs = tuple(spec for phase in phases for spec in phase)
    classified = {
        name
        for name in inventory
        if any(name.startswith(prefix) for prefix in CLASSIFIED_PREFIXES)
    }
    groups = {
        spec.name: spec.selected_names(inventory, classified) for spec in specs
    }
    empty = [name for name, names in groups.items() if not names]
    if empty:
        raise GateError(f"scheduled groups selected no tests: {empty}")

    scheduled = [name for names in groups.values() for name in names]
    expected = (
        [name for name in inventory if name.startswith(EVAL_PREFIXES)]
        if scope == "eval"
        else list(inventory)
    )
    if len(scheduled) != len(set(scheduled)):
        raise GateError("the grouped schedule selects at least one test more than once")
    if set(scheduled) != set(expected):
        missing = sorted(set(expected) - set(scheduled))
        added = sorted(set(scheduled) - set(expected))
        raise GateError(
            "grouped schedule does not cover its inventory; "
            f"missing={missing}, added={added}"
        )
    return groups, phases


def parse_test_result(output: str) -> dict[str, int | float | str]:
    matches = list(RESULT_RE.finditer(output))
    if len(matches) != 1:
        raise GateError(
            f"expected exactly one libtest result line, found {len(matches)}"
        )
    match = matches[0]
    return {
        "status": match.group("status"),
        "passed": int(match.group("passed")),
        "failed": int(match.group("failed")),
        "ignored": int(match.group("ignored")),
        "measured": int(match.group("measured")),
        "filtered_out": int(match.group("filtered")),
        "reported_seconds": float(match.group("reported_seconds")),
    }


def validate_test_result(
    result: dict[str, int | float | str],
    expected: int,
    total: int,
    expected_ignored: int,
) -> None:
    observed = sum(int(result[key]) for key in ("passed", "failed", "ignored"))
    if observed != expected:
        raise GateError(
            f"group reported {observed} test outcomes but its inventory "
            f"contains {expected}"
        )
    if int(result["filtered_out"]) != total - expected:
        raise GateError(
            "group filtered-out count does not complement the complete inventory: "
            f"{result['filtered_out']} != {total - expected}"
        )
    if int(result["ignored"]) != expected_ignored:
        raise GateError(
            f"group reported {result['ignored']} ignored tests but its exact "
            f"ignored inventory contains {expected_ignored}"
        )
    if int(result["measured"]) != 0:
        raise GateError(f"group unexpectedly reported benchmarks: {result}")
    if result["status"] != "ok" or int(result["failed"]) != 0:
        raise GateError(f"group did not pass: {result}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def git_state() -> dict[str, str | bool | None]:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return {
        "head": head.stdout.strip() if head.returncode == 0 else None,
        "dirty": bool(status.stdout) if status.returncode == 0 else None,
    }


def gate_environment(template: bool) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "LANG": "C",
            "LC_ALL": "C",
            "RUST_MIN_STACK": "134217728",
            "RUST_TEST_THREADS": "2",
        }
    )
    if template:
        environment["EMAXX_IMAGE_TEMPLATE"] = "1"
    else:
        environment.pop("EMAXX_IMAGE_TEMPLATE", None)
    return environment


def discover_test_binary(profile: str, artifact_root: Path) -> Path:
    command = [
        "cargo",
        "test",
        "--profile",
        profile,
        "--lib",
        "--no-run",
        "--message-format=json-render-diagnostics",
    ]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=gate_environment(False),
        text=True,
        capture_output=True,
        check=False,
    )
    (artifact_root / "build.log").write_text(
        completed.stdout + "\n--- cargo stderr ---\n" + completed.stderr
    )
    if completed.returncode != 0:
        raise GateError(
            f"building the {profile} library test binary failed with "
            f"{completed.returncode}"
        )

    manifest = str((PROJECT_ROOT / "Cargo.toml").resolve())
    executables = []
    for line in completed.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            message.get("reason") == "compiler-artifact"
            and message.get("manifest_path") == manifest
            and message.get("target", {}).get("name") == "emaxx"
            and message.get("target", {}).get("kind") == ["lib"]
            and message.get("profile", {}).get("test") is True
            and message.get("executable")
        ):
            executables.append(Path(message["executable"]).resolve())
    if len(executables) != 1:
        raise GateError(
            f"expected one project library test executable, found {executables}"
        )
    if not executables[0].is_file():
        raise GateError(f"library test executable is missing: {executables[0]}")
    return executables[0]


def parse_cargo_test_targets(metadata: dict[str, object]) -> dict[str, list[str]]:
    raw_packages = metadata.get("packages")
    if not isinstance(raw_packages, list) or not all(
        isinstance(package, dict) for package in raw_packages
    ):
        raise GateError("Cargo metadata has no valid packages array")
    packages = [
        package
        for package in raw_packages
        if package.get("manifest_path")
        == str((PROJECT_ROOT / "Cargo.toml").resolve())
    ]
    if len(packages) != 1:
        raise GateError(f"expected one root Cargo package, found {len(packages)}")
    raw_targets = packages[0].get("targets")
    if not isinstance(raw_targets, list) or not all(
        isinstance(target, dict) for target in raw_targets
    ):
        raise GateError("Cargo metadata has no valid root-package targets array")
    groups = {}
    for label, kind in (("bins", "bin"), ("integrations", "test")):
        names = sorted(
            target["name"]
            for target in raw_targets
            if target.get("kind") == [kind]
            and target.get("test") is True
            and isinstance(target.get("name"), str)
        )
        if not names:
            raise GateError(f"Cargo metadata reported no {label} test targets")
        if len(names) != len(set(names)):
            raise GateError(f"Cargo metadata contains duplicate {label}: {names}")
        groups[label] = names
    return groups


def discover_cargo_test_targets(artifact_root: Path) -> dict[str, list[str]]:
    command = ["cargo", "metadata", "--no-deps", "--format-version=1"]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=gate_environment(False),
        text=True,
        capture_output=True,
        check=False,
    )
    (artifact_root / "metadata.log").write_text(
        completed.stdout + "\n--- cargo stderr ---\n" + completed.stderr
    )
    if completed.returncode != 0:
        raise GateError(f"Cargo metadata failed with {completed.returncode}")
    try:
        metadata = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise GateError(f"Cargo metadata returned invalid JSON: {error}") from error
    return parse_cargo_test_targets(metadata)


@dataclass
class RunningGroup:
    spec: GroupSpec
    expected: int
    expected_ignored: int
    command: list[str]
    log_path: Path
    log_handle: BinaryIO
    process: subprocess.Popen[bytes]
    started: float


def start_group(
    spec: GroupSpec,
    expected: int,
    expected_ignored: int,
    test_binary: Path,
    artifact_root: Path,
    repetition: int,
) -> RunningGroup:
    command = spec.command(test_binary)
    log_path = artifact_root / f"repeat-{repetition:02d}-{spec.name}.log"
    log_handle = log_path.open("wb")
    try:
        process = subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            env=gate_environment(spec.template),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=os.name == "posix",
        )
    except BaseException:
        log_handle.close()
        raise
    return RunningGroup(
        spec,
        expected,
        expected_ignored,
        command,
        log_path,
        log_handle,
        process,
        time.monotonic(),
    )


def terminate_group(running: RunningGroup) -> None:
    if running.process.poll() is not None:
        return
    if os.name == "posix":
        os.killpg(running.process.pid, signal.SIGTERM)
    else:
        running.process.terminate()
    try:
        running.process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            os.killpg(running.process.pid, signal.SIGKILL)
        else:
            running.process.kill()
        running.process.wait()


def finish_group(
    running: RunningGroup, total: int, timeout_seconds: int
) -> dict[str, object]:
    remaining = timeout_seconds - (time.monotonic() - running.started)
    timed_out = False
    if running.process.poll() is None and remaining <= 0:
        timed_out = True
    elif running.process.poll() is None:
        try:
            running.process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            timed_out = True
    if timed_out:
        terminate_group(running)
    elapsed = time.monotonic() - running.started
    running.log_handle.close()
    output = running.log_path.read_text(errors="replace")
    record: dict[str, object] = {
        "group": running.spec.name,
        "command": running.command,
        "expected_tests": running.expected,
        "expected_ignored": running.expected_ignored,
        "template": running.spec.template,
        "test_threads": running.spec.test_threads,
        "elapsed_seconds": elapsed,
        "exit_code": running.process.returncode,
        "timed_out": timed_out,
        "log": str(running.log_path),
    }
    if timed_out:
        raise GateError(
            f"{running.spec.name} exceeded its {timeout_seconds}s group timeout"
        )
    result = parse_test_result(output)
    record["result"] = result
    validate_test_result(
        result,
        running.expected,
        total,
        running.expected_ignored,
    )
    if running.process.returncode != 0:
        raise GateError(
            f"{running.spec.name} exited with {running.process.returncode} "
            "despite its result"
        )
    return record


def run_phase(
    phase: Sequence[GroupSpec],
    groups: dict[str, list[str]],
    test_binary: Path,
    artifact_root: Path,
    repetition: int,
    total: int,
    timeout_seconds: int,
    ignored_names: frozenset[str],
) -> list[dict[str, object]]:
    active = []
    records = []
    try:
        for spec in phase:
            active.append(
                start_group(
                    spec,
                    len(groups[spec.name]),
                    len(ignored_names.intersection(groups[spec.name])),
                    test_binary,
                    artifact_root,
                    repetition,
                )
            )
        print("START " + " + ".join(spec.name for spec in phase), flush=True)
        pending = list(active)
        while pending:
            ready = [
                running
                for running in pending
                if running.process.poll() is not None
                or time.monotonic() - running.started >= timeout_seconds
            ]
            if not ready:
                time.sleep(0.05)
                continue
            for running in ready:
                record = finish_group(running, total, timeout_seconds)
                records.append(record)
                pending.remove(running)
                result = record["result"]
                print(
                    f"PASS {running.spec.name}: "
                    f"{result['passed']} passed, {result['ignored']} ignored, "
                    f"{record['elapsed_seconds']:.2f}s",
                    flush=True,
                )
    except BaseException:
        for running in active:
            terminate_group(running)
            try:
                running.log_handle.close()
            except Exception:
                pass
        raise
    return records


def run_cargo_stage(
    name: str,
    arguments: Sequence[str],
    profile: str,
    artifact_root: Path,
    expected_results: int,
) -> dict[str, object]:
    command = ["cargo", "test", "--profile", profile, *arguments]
    log_path = artifact_root / f"{name}.log"
    started = time.monotonic()
    with log_path.open("wb") as log:
        completed = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=gate_environment(False),
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    elapsed = time.monotonic() - started
    output = log_path.read_text(errors="replace")
    results = [
        {
            "status": match.group("status"),
            "passed": int(match.group("passed")),
            "failed": int(match.group("failed")),
            "ignored": int(match.group("ignored")),
            "measured": int(match.group("measured")),
            "filtered_out": int(match.group("filtered")),
            "reported_seconds": float(match.group("reported_seconds")),
        }
        for match in RESULT_RE.finditer(output)
    ]
    if completed.returncode != 0 or len(results) != expected_results or any(
        result["status"] != "ok"
        or result["failed"] != 0
        or result["ignored"] != 0
        or result["measured"] != 0
        for result in results
    ):
        raise GateError(
            f"Cargo stage {name} failed; exit={completed.returncode}, "
            f"expected_results={expected_results}, results={results}"
        )
    print(f"PASS {name}: {elapsed:.2f}s", flush=True)
    return {
        "stage": name,
        "command": command,
        "elapsed_seconds": elapsed,
        "exit_code": completed.returncode,
        "results": results,
        "log": str(log_path),
    }


def write_summary(path: Path, summary: dict[str, object]) -> None:
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope",
        choices=("eval", "lib", "full"),
        default="full",
        help="eval groups only, the complete library, or the complete publication gate",
    )
    parser.add_argument("--profile", default="gate")
    parser.add_argument("--repetitions", type=positive_integer, default=1)
    parser.add_argument(
        "--timeout-seconds",
        type=positive_integer,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="per grouped worker; timeout is a failure and is never retried",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        help="new output directory (default: target/grouped-gate/run-...)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    artifact_root = args.artifact_root or (
        PROJECT_ROOT
        / "target"
        / "grouped-gate"
        / f"run-{time.time_ns()}-{os.getpid()}"
    )
    artifact_root = artifact_root.resolve()
    try:
        artifact_root.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        print(f"artifact root already exists: {artifact_root}", file=sys.stderr)
        return 2

    summary: dict[str, object] = {
        "format_version": 1,
        "scope": args.scope,
        "profile": args.profile,
        "repetitions": args.repetitions,
        "timeout_seconds": args.timeout_seconds,
        "artifact_root": str(artifact_root),
        "git": git_state(),
        "environment": {
            "LANG": "C",
            "LC_ALL": "C",
            "RUST_MIN_STACK": "134217728",
            "RUST_TEST_THREADS": "2",
        },
        "runs": [],
        "cargo_stages": [],
        "status": "running",
    }
    summary_path = artifact_root / "summary.json"
    write_summary(summary_path, summary)
    print(f"ARTIFACT {artifact_root}", flush=True)

    try:
        test_binary = discover_test_binary(args.profile, artifact_root)
        listing = subprocess.run(
            [str(test_binary), "--list", "--format=terse"],
            cwd=PROJECT_ROOT,
            env=gate_environment(False),
            text=True,
            capture_output=True,
            check=False,
        )
        (artifact_root / "inventory.txt").write_text(listing.stdout)
        if listing.returncode != 0:
            raise GateError(f"listing tests failed with {listing.returncode}")
        inventory = parse_inventory(listing.stdout)
        ignored_listing = subprocess.run(
            [str(test_binary), "--list", "--ignored", "--format=terse"],
            cwd=PROJECT_ROOT,
            env=gate_environment(False),
            text=True,
            capture_output=True,
            check=False,
        )
        (artifact_root / "ignored-inventory.txt").write_text(
            ignored_listing.stdout
        )
        if ignored_listing.returncode != 0:
            raise GateError(
                f"listing ignored tests failed with {ignored_listing.returncode}"
            )
        ignored_names = frozenset(
            parse_inventory(ignored_listing.stdout, allow_empty=True)
        )
        unknown_ignored = sorted(ignored_names - ALLOWED_IGNORED_TESTS)
        if unknown_ignored:
            raise GateError(
                "new ignored tests require explicit review: "
                + ", ".join(unknown_ignored)
            )
        missing_ignored = sorted(ignored_names - set(inventory))
        if missing_ignored:
            raise GateError(
                "ignored tests are absent from the complete inventory: "
                + ", ".join(missing_ignored)
            )
        groups, phases = classify_inventory(inventory, args.scope)
        inventory_digest = hashlib.sha256(
            ("\n".join(inventory) + "\n").encode()
        ).hexdigest()
        summary["test_binary"] = {
            "path": str(test_binary),
            "sha256": sha256_file(test_binary),
        }
        summary["inventory"] = {
            "total_tests": len(inventory),
            "sha256": inventory_digest,
            "scheduled_tests": sum(len(names) for names in groups.values()),
            "groups": {name: len(names) for name, names in groups.items()},
            "ignored_tests": sorted(ignored_names),
        }
        summary["schedule"] = [
            [spec.name for spec in phase] for phase in phases
        ]
        write_summary(summary_path, summary)

        for repetition in range(1, args.repetitions + 1):
            print(f"REPETITION {repetition}/{args.repetitions}", flush=True)
            records = []
            for phase in phases:
                records.extend(
                    run_phase(
                        phase,
                        groups,
                        test_binary,
                        artifact_root,
                        repetition,
                        len(inventory),
                        args.timeout_seconds,
                        ignored_names,
                    )
                )
            observed = sum(
                int(record["result"][key])
                for record in records
                for key in ("passed", "failed", "ignored")
            )
            expected = sum(len(names) for names in groups.values())
            if observed != expected:
                raise GateError(
                    f"repetition covered {observed} outcomes, expected {expected}"
                )
            summary["runs"].append(
                {
                    "repetition": repetition,
                    "expected_tests": expected,
                    "observed_outcomes": observed,
                    "groups": records,
                }
            )
            write_summary(summary_path, summary)

        if args.scope == "full":
            cargo_targets = discover_cargo_test_targets(artifact_root)
            summary["cargo_test_targets"] = cargo_targets
            write_summary(summary_path, summary)
            integration_arguments = tuple(
                argument
                for target in cargo_targets["integrations"]
                for argument in ("--test", target)
            )
            for name, arguments, expected_results in (
                ("bins", ("--bins",), len(cargo_targets["bins"])),
                (
                    "integration",
                    integration_arguments,
                    len(cargo_targets["integrations"]),
                ),
            ):
                summary["cargo_stages"].append(
                    run_cargo_stage(
                        name,
                        arguments,
                        args.profile,
                        artifact_root,
                        expected_results,
                    )
                )
                write_summary(summary_path, summary)

        summary["status"] = "passed"
        write_summary(summary_path, summary)
        print(f"GROUPED GATE PASSED: {artifact_root}")
        return 0
    except (GateError, OSError, subprocess.SubprocessError) as error:
        summary["status"] = "failed"
        summary["error"] = str(error)
        write_summary(summary_path, summary)
        print(f"GROUPED GATE FAILED: {error}", file=sys.stderr)
        print(f"ARTIFACT {artifact_root}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        summary["status"] = "interrupted"
        summary["error"] = "interrupted by user"
        write_summary(summary_path, summary)
        print("GROUPED GATE INTERRUPTED", file=sys.stderr)
        print(f"ARTIFACT {artifact_root}", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
