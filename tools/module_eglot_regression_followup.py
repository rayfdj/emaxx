#!/usr/bin/env python3
"""Finish the stopped 9a44331 inventory and reproduce its failure on its base.

This is evidence collection, not a replacement success gate: the original
failure remains in the combined result and this command returns nonzero.
"""
import argparse
import json
import re
import subprocess
from pathlib import Path

import serial_grouped_gate

gate = serial_grouped_gate.gate
SUBJECT = "9a44331edc7517fc30065ddc0297f1f6f13ba817"
BASE = "1f54e9797933c6bf5249c347fc71b0fffc295b41"
FAILURE = (
    "lisp::primitives::tests::"
    "fixture_image_directory_dumps_once_and_starts_every_later_boot_from_it"
)
REASON = "is already loaded in this process"


def require_image_failure(output, expected, total):
    result = gate.parse_test_result(output)
    assert result["status"] == "FAILED" and result["failed"] == 1, result
    assert result["passed"] == expected - 1 and result["ignored"] == 0, result
    assert result["filtered_out"] == total - expected, result
    assert result["measured"] == 0, result
    assert re.findall(r"^---- (.*?) stdout ----$", output, re.M) == [FAILURE]
    assert "dumped compilation unit " in output and REASON in output
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    prior = list(args.previous.rglob("module-eglot-rust-gate/summary.json"))
    assert len(prior) == 1, prior
    prior_root = prior[0].parent
    previous = json.loads(prior[0].read_text())
    assert previous["git"] == {"head": SUBJECT, "dirty": False}
    assert previous["status"] == "failed"
    assert {p.name for p in prior_root.glob("repeat-01-*.log")} == {
        *(f"repeat-01-eval_0{n}.log" for n in range(1, 6)),
        "repeat-01-primitives.log",
    }
    # Only validation tooling may differ from the previously tested revision.
    subprocess.run(
        ["git", "diff", "--exit-code", SUBJECT, "HEAD", "--", "src", "tests",
         "Cargo.toml", "Cargo.lock", "build.rs", ".cargo",
         "tools/grouped_gate.py", "tools/serial_grouped_gate.py"], check=True,
    )
    baseline = args.baseline.resolve()
    assert subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=baseline, text=True
    ).strip() == BASE
    report = {
        "source_commit": SUBJECT, "validation_commit": gate.git_state(),
        "baseline_commit": BASE, "original_failure": FAILURE,
        "full_gate_status": "failed", "original_groups": [],
        "remaining_groups": [], "cargo_stages": [], "errors": [],
    }

    def save():
        gate.write_summary(output / "summary.json", report)

    save()
    binary = gate.discover_test_binary("gate", output)
    inventory_text = subprocess.check_output(
        [str(binary), "--list", "--format=terse"], text=True,
        env=gate.gate_environment(False),
    )
    (output / "inventory.txt").write_text(inventory_text)
    inventory = gate.parse_inventory(inventory_text)
    assert inventory == gate.parse_inventory((prior_root / "inventory.txt").read_text())
    report["test_binary_sha256"] = gate.sha256_file(binary)
    ignored_text = subprocess.check_output(
        [str(binary), "--list", "--ignored", "--format=terse"], text=True,
        env=gate.gate_environment(False),
    )
    ignored = frozenset(gate.parse_inventory(ignored_text, allow_empty=True))
    assert ignored == frozenset(previous["inventory"]["ignored_tests"])
    groups, phases = gate.classify_inventory(inventory, "full")
    for phase in phases:
        assert len(phase) == 1
        spec = phase[0]
        log = prior_root / f"repeat-01-{spec.name}.log"
        if log.exists():
            text = log.read_text()
            result = gate.parse_test_result(text)
            if spec.name == "primitives":
                require_image_failure(text, len(groups[spec.name]), len(inventory))
            else:
                gate.validate_test_result(
                    result, len(groups[spec.name]), len(inventory),
                    len(ignored.intersection(groups[spec.name])),
                )
            report["original_groups"].append({"name": spec.name, "result": result})
        else:
            try:
                report["remaining_groups"].extend(gate.run_phase(
                    phase, groups, binary, output, 1, len(inventory), 3600, ignored,
                ))
            except gate.GateError as error:
                report["errors"].append(str(error))
        save()

    # Run each Cargo target independently so one failure cannot hide later ones.
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

    # Reuse dependency artifacts, but rebuild the baseline package from source.
    environment = gate.gate_environment(True)
    environment["CARGO_TARGET_DIR"] = str(gate.PROJECT_ROOT / "target")
    environment["EMAXX_FIXTURE_IMAGE_DIR"] = str(output / "baseline-fixture-images")
    with (output / "baseline.log").open("w") as log:
        subprocess.run(
            ["cargo", "clean", "--package", "emaxx", "--profile", "gate"],
            cwd=baseline, env=environment, stdout=log, stderr=subprocess.STDOUT,
            check=True,
        )
        completed = subprocess.run(
            ["cargo", "test", "--locked", "--profile", "gate", "--lib", "-j2",
             FAILURE, "--", "--exact", "--test-threads=1"],
            cwd=baseline, env=environment, stdout=log, stderr=subprocess.STDOUT,
            check=False,
        )
    baseline_text = (output / "baseline.log").read_text()
    result = gate.parse_test_result(baseline_text)
    report["baseline_result"] = result
    report["baseline_exit_code"] = completed.returncode
    try:
        assert completed.returncode != 0
        require_image_failure(baseline_text, 1, int(result["filtered_out"]) + 1)
        report["baseline_reproduced"] = True
    except AssertionError:
        report["baseline_reproduced"] = False
        report["errors"].append("The image failure did not reproduce on the unchanged base")
    observed = sum(
        int(entry["result"][key])
        for entry in report["original_groups"] + report["remaining_groups"]
        for key in ("passed", "failed", "ignored")
    )
    report["library_outcomes"] = observed
    report["library_inventory"] = len(inventory)
    report["diagnostic_complete"] = observed == len(inventory) and not report["errors"]
    save()
    print(json.dumps(report, indent=2), flush=True)
    print("The combined full Rust gate retains the reproduced baseline failure.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
