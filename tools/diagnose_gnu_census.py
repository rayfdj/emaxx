#!/usr/bin/env python3
"""Separate GNU census environment variation from Rust test-order effects.

Run the original fixture and Rust assertions unchanged, preserving every failure.
This is a bounded diagnostic, not a replacement for the complete Rust gate.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess

import diagnose_rust_gate
from diagnose_weak_roots import compiled_inputs
import serial_grouped_gate


CONTROL = "lisp::primitives::tests::native_vector_and_closure_census_matches_gnu_word_layout"
GROUP = "lisp::primitives::tests::"
VARIANTS = (
    ("full", "rust", ""),
    ("single", "rust-replay", CONTROL),
    ("group", "rust-replay", GROUP),
)
SAFE_ENVIRONMENT = (
    "LANG", "LC_ALL", "RUST_MIN_STACK", "RUST_TEST_THREADS", "RUST_BACKTRACE",
    "EMAXX_IMAGE_TEMPLATE", "EMAXX_FIXTURE_IMAGE_DIR", "CARGO_BUILD_JOBS",
    "VALIDATION", "VALIDATION_RUST_FILTER", "VALIDATION_RUST_PRELUDE",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--oracle-source", type=Path)
    parser.add_argument("--gnu-only", action="store_true",
                        help="run only the nine initial GNU probes; no Rust test verdict")
    args = parser.parse_args()
    gate = serial_grouped_gate.gate
    root = gate.PROJECT_ROOT
    oracle = (args.oracle_source or root.parent / "emacs").resolve()
    binary = oracle / "src/emacs"
    image = oracle / "src/emacs.pdmp"
    if not binary.is_file() or not image.is_file():
        parser.error("--oracle-source must contain the ordinary GNU executable and image")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    fixture = root / "tests/fixtures/vector-closure-rounded-census.el"
    expected_file = fixture.with_suffix(".expected")
    fixture_text = fixture.read_text()
    # The Rust oracle helper wraps this same ASCII fixture with prin1.
    fixture_text.encode("ascii")
    expected = expected_file.read_bytes().strip()
    environment = gate.gate_environment(True)
    environment.update(VALIDATION="rust", VALIDATION_RUST_FILTER="")
    before = compiled_inputs(root)
    (output / "source-before.json").write_text(json.dumps(before, indent=2) + "\n")
    inputs = {
        str(path): gate.sha256_file(path)
        for path in (binary, image, fixture, expected_file, Path(__file__),
                     Path(diagnose_rust_gate.__file__), Path(gate.__file__),
                     Path(serial_grouped_gate.__file__))
    }
    summary = {
        "status": "preparing", "git": gate.git_state(), "inputs": inputs,
        "scope": "GNU-only probes" if args.gnu_only else "GNU probes and unchanged Rust controls",
        "qualification": (
            "Three interleaved rounds vary only the recorded workflow selector variables. "
            "The original Rust single/group/single runs all use the same full-run selector "
            "environment. Other environment, process and filesystem effects remain possible. "
            "No warm-up GC, expectation, fixture, test selector or timeout is changed. "
            "This diagnosis cannot certify a complete gate or erase an earlier failure."
        ),
        "environment": {key: environment.get(key) for key in SAFE_ENVIRONMENT},
        "timeout_seconds": gate.DEFAULT_TIMEOUT_SECONDS,
        "gnu_runs": [], "rust_runs": [], "retained_files": [],
    }

    def save() -> None:
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    def retain(source: Path, relative: Path) -> None:
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = gate.sha256_file(source)
        shutil.copy2(source, target)
        if gate.sha256_file(target) != digest:
            raise gate.GateError(f"retained input differs: {source}")
        summary["retained_files"].append({
            "source": str(source), "artifact": str(relative),
            "sha256": digest, "bytes": source.stat().st_size,
        })

    def retain_images(stage: str) -> None:
        for source in sorted(Path(environment["EMAXX_FIXTURE_IMAGE_DIR"]).glob("*.pdmp")):
            retain(source, Path("fixture-images") / stage / source.name)

    def matrix(stage: str) -> None:
        for repetition in range(1, 4):
            variants = VARIANTS if repetition != 2 else tuple(reversed(VARIANTS))
            for label, validation, selection in variants:
                stem = f"gnu-{stage}-{repetition}-{label}"
                overrides = {"VALIDATION": validation, "VALIDATION_RUST_FILTER": selection}
                command = [str(binary), "--batch", "-Q", "--eval", f"(prin1 {fixture_text})"]
                record = {"stage": stage, "repetition": repetition, "variant": label,
                          "command": command, "environment_overrides": overrides}
                summary["gnu_runs"].append(record)
                summary["status"] = f"running {stem}"
                save()
                try:
                    result = subprocess.run(
                        command, cwd=root, env={**environment, **overrides},
                        stdin=subprocess.DEVNULL, capture_output=True,
                        timeout=gate.DEFAULT_TIMEOUT_SECONDS,
                    )
                    stdout, stderr = result.stdout, result.stderr
                    record.update(exit_code=result.returncode, timed_out=False)
                except subprocess.TimeoutExpired as error:
                    stdout, stderr = error.stdout or b"", error.stderr or b""
                    record.update(exit_code=None, timed_out=True)
                stdout_path, stderr_path = output / f"{stem}.stdout", output / f"{stem}.stderr"
                stdout_path.write_bytes(stdout)
                stderr_path.write_bytes(stderr)
                record.update(
                    stdout_file=stdout_path.name, stderr_file=stderr_path.name,
                    stdout_sha256=gate.sha256_file(stdout_path),
                    stderr_sha256=gate.sha256_file(stderr_path),
                    expected_output=(stdout == expected),
                    passed=(record["exit_code"] == 0 and not record["timed_out"]
                            and stdout == expected and not stderr),
                )
                save()

    save()
    try:
        summary["oracle_source_commit"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=oracle, text=True,
        ).strip()
        retain(binary, Path("gnu-inputs/emacs"))
        retain(image, Path("gnu-inputs/emacs.pdmp"))
        for name in ("src/config.h", "src/Makefile"):
            retain(oracle / name, Path("gnu-inputs") / Path(name).name)
        matrix("before")
        if not args.gnu_only:
            rust_binary = gate.discover_test_binary("gate", output)
            summary["test_binary"] = {"path": str(rust_binary), "sha256": gate.sha256_file(rust_binary)}
            retain(rust_binary, Path("libtest"))

            def inventory(label: str, selection: list[str], allow_empty: bool = False) -> list[str]:
                completed = subprocess.run(
                    [str(rust_binary), *selection, "--list"], cwd=root, env=environment,
                    capture_output=True, text=True, timeout=60,
                )
                (output / f"{label}.log").write_text(completed.stdout + completed.stderr)
                if completed.returncode:
                    raise gate.GateError(f"inventory failed: {label}")
                return gate.parse_inventory(completed.stdout, allow_empty=allow_empty)

            all_tests = inventory("full-inventory", [])
            selected = inventory("single-inventory", [CONTROL])
            selected_ignored = inventory("single-ignored", [CONTROL, "--ignored"], True)
            group = inventory("group-inventory", [GROUP])
            group_ignored = inventory("group-ignored", [GROUP, "--ignored"], True)
            if selected != [CONTROL] or not set(selected) <= set(group) <= set(all_tests):
                raise gate.GateError("original control/group absent from the complete inventory")
            summary["inventory"] = {"all": all_tests, "single": selected, "group": group,
                                    "single_ignored": selected_ignored, "group_ignored": group_ignored}
            retain_images("before-rust")
            for label, selection, names, ignored in (
                ("single-before", [CONTROL], selected, selected_ignored),
                ("group", [GROUP], group, group_ignored),
                ("single-after", [CONTROL], selected, selected_ignored),
            ):
                summary["status"] = f"running Rust {label}"
                save()
                log = output / f"rust-{label}.log"
                record = diagnose_rust_gate.execute(
                    [str(rust_binary), *selection, "--test-threads", "1"], environment, log,
                )
                record.update(label=label, log_file=log.name, log_sha256=gate.sha256_file(log))
                summary["rust_runs"].append(record)
                text = log.read_text(errors="replace")
                # Retain failed summaries and continue to the after-controls.
                # A failed prelude must not hide the original failure or prevent diagnosis.
                try:
                    record["result"] = gate.parse_test_result(text)
                    diagnose_rust_gate.validate_execution(record, text, names, all_tests, ignored)
                    record["passed"] = True
                except gate.GateError as error:
                    record.update(passed=False, validation_error=str(error))
                retain_images(label)
                save()
            matrix("after")
            if gate.sha256_file(rust_binary) != summary["test_binary"]["sha256"]:
                raise gate.GateError("executed Rust binary changed during the diagnostic")
        summary["source_after"] = gate.git_state()
        summary["compiled_inputs_unchanged"] = compiled_inputs(root) == before
        summary["inputs_unchanged"] = all(gate.sha256_file(Path(path)) == digest for path, digest in inputs.items())
        if not summary["compiled_inputs_unchanged"] or not summary["inputs_unchanged"]:
            raise gate.GateError("diagnostic input changed during execution")
        failures = sum(not record["passed"] for record in summary["gnu_runs"] + summary["rust_runs"])
        summary.update(status="diagnostic completed", failed_processes=failures)
        return 1 if failures else 0
    except BaseException as error:
        summary.update(status="diagnostic error", error=repr(error))
        raise
    finally:
        save()


if __name__ == "__main__":
    raise SystemExit(main())
