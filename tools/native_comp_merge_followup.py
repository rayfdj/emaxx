#!/usr/bin/env python3
"""Continue the September native-comp merge gate from its recorded results.

Retain completed tests from c7811e0, rerun the corrected image test and TLS
controls, and execute every missing group and Cargo target. The only permitted
runtime delta is the reviewed TLS read correction. Earlier failures remain in
the evidence; publication requires successful corrected and remaining tests.
"""
import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

import serial_grouped_gate

gate = serial_grouped_gate.gate
SOURCE = "c7811e021c31e31613a11d15b2befefbb7867a5a"
TLS_PATCH = "e8080113aa3b66eec59b1da2ba19574e0890afb1f4a99f02524a26727e3d02fd"
NEW_TEST = "lisp::eval::tests::eval_01::tls_reads_wait_for_the_event_loop_to_finish_negotiation"
IMAGE_TEST = "lisp::primitives::tests::fixture_image_directory_dumps_once_and_starts_every_later_boot_from_it"
TLS_TEST = "lisp::primitives::tests::native_gnutls_session_encrypts_process_io_and_closes_the_same_transport"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    previous_root = args.previous.resolve()
    previous = json.loads((previous_root / "summary.json").read_text())
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    patch = subprocess.check_output([
        "git", "diff", "--no-ext-diff", "--no-renames", SOURCE, "--", "src/lisp/eval.rs",
    ])
    assert hashlib.sha256(patch).hexdigest() == TLS_PATCH, "runtime delta changed"
    changed = set(subprocess.check_output([
        "git", "diff", "--name-only", SOURCE, "--", "src", "tests", "Cargo.toml",
        "Cargo.lock", "build.rs", ".cargo", "compat", "tools/grouped_gate.py",
        "tools/serial_grouped_gate.py",
    ], text=True).splitlines())
    assert changed == {
        "src/lisp/eval.rs", "src/lisp/eval/tests/eval_01.rs", "src/lisp/primitives/tests.rs",
    }, changed
    (output / "runtime-delta.diff").write_bytes(patch)
    environment = gate.gate_environment(False)
    report = {
        "status": "running", "retained_source_commit": SOURCE,
        "environment": {key: environment[key] for key in
                        ("LANG", "LC_ALL", "RUST_MIN_STACK", "RUST_TEST_THREADS")},
        "current_git": gate.git_state(), "runtime_delta_sha256": TLS_PATCH,
        "previous_summary": previous, "retained_groups": [],
        "corrected_controls": [], "remaining_groups": [], "cargo_stages": [],
        "errors": [],
    }

    def save():
        gate.write_summary(output / "summary.json", report)

    save()
    binary = gate.discover_test_binary("gate", output)
    inventory_text = subprocess.check_output(
        [str(binary), "--list", "--format=terse"], env=environment, text=True,
    )
    (output / "inventory.txt").write_text(inventory_text)
    inventory = gate.parse_inventory(inventory_text)
    old_inventory = gate.parse_inventory((previous_root / "inventory.txt").read_text())
    assert set(inventory) == set(old_inventory) | {NEW_TEST}
    assert len(inventory) == len(old_inventory) + 1
    ignored_text = subprocess.check_output(
        [str(binary), "--list", "--ignored", "--format=terse"], env=environment, text=True,
    )
    (output / "ignored-inventory.txt").write_text(ignored_text)
    ignored = frozenset(gate.parse_inventory(ignored_text, allow_empty=True))
    assert ignored == frozenset(previous["inventory"]["ignored_tests"])
    report["inventory"] = {"total_tests": len(inventory), "ignored_tests": sorted(ignored)}
    report["test_binary_sha256"] = gate.sha256_file(binary)
    groups, phases = gate.classify_inventory(inventory, "full")
    old_groups, _ = gate.classify_inventory(old_inventory, "full")
    covered = set()
    missing = []
    for phase in phases:
        spec, = phase
        path = previous_root / f"repeat-01-{spec.name}.log"
        if not path.exists():
            missing.append(phase)
            continue
        text = path.read_text()
        result = gate.parse_test_result(text)
        failures = set(re.findall(r"^---- (.*?) stdout ----$", text, re.M))
        expected = len(old_groups[spec.name])
        if failures:
            assert failures <= {IMAGE_TEST, TLS_TEST}, failures
            assert result["status"] == "FAILED" and result["failed"] == len(failures)
            assert result["passed"] + result["failed"] == expected
            assert result["ignored"] == result["measured"] == 0
            assert result["filtered_out"] == len(old_inventory) - expected
        else:
            gate.validate_test_result(
                result, expected, len(old_inventory), len(ignored.intersection(old_groups[spec.name])),
            )
        covered.update(set(old_groups[spec.name]) - failures)
        report["retained_groups"].append({
            "group": spec.name, "result": result, "failures": sorted(failures),
            "log_sha256": gate.sha256_file(path), "log": str(path),
        })
        print("RETAIN", spec.name, result, flush=True)
    save()

    controls = (
        gate.GroupSpec("tls_read_control", NEW_TEST, False, 1),
        gate.GroupSpec("fixture_image_control", IMAGE_TEST, True, 1),
        gate.GroupSpec("tls_controls", "lisp::primitives::tests::native_gnutls_", True, 1),
    )
    for spec in controls:
        names = [name for name in inventory if name.startswith(spec.prefix)]
        assert names
        try:
            report["corrected_controls"].extend(gate.run_phase(
                (spec,), {spec.name: names}, binary, output, 1, len(inventory), 3600, ignored,
            ))
            covered.update(names)
        except gate.GateError as error:
            report["errors"].append(str(error))
        save()
    for phase in missing:
        try:
            report["remaining_groups"].extend(gate.run_phase(
                phase, groups, binary, output, 1, len(inventory), 3600, ignored,
            ))
            covered.update(groups[phase[0].name])
        except gate.GateError as error:
            report["errors"].append(str(error))
        save()

    targets = gate.discover_cargo_test_targets(output)
    report["cargo_test_targets"] = targets
    for kind, flag in (("bins", "--bin"), ("integrations", "--test")):
        for target in targets[kind]:
            try:
                report["cargo_stages"].append(gate.run_cargo_stage(
                    f"{kind}-{target}", (flag, target), "gate", output, 1,
                ))
            except gate.GateError as error:
                report["errors"].append(str(error))
            save()
    report["covered_library_tests"] = len(covered)
    report["uncovered_library_tests"] = sorted(set(inventory) - covered)
    report["status"] = "passed" if covered == set(inventory) and not report["errors"] else "failed"
    save()
    print("CONTINUED GATE", report["status"], flush=True)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
