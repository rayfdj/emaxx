#!/usr/bin/env python3
"""Offline unit tests for grouped_gate.py's fail-closed scheduler."""

from pathlib import Path
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import grouped_gate as gate


class GroupedGateTests(unittest.TestCase):
    def inventory(self):
        names = [
            *(prefix + "case" for prefix in gate.EVAL_PREFIXES),
            gate.PRIMITIVES_PREFIX + "case",
            gate.COMPAT_RUNTIME_PREFIX + "case",
            gate.TTY_PREFIX + "case",
            gate.BATCH_PREFIX + "case",
            "buffer::tests::case",
        ]
        return sorted(names)

    def test_inventory_parser_rejects_duplicates(self):
        with self.assertRaisesRegex(gate.GateError, "duplicate"):
            gate.parse_inventory("same: test\nsame: test\n")

    def test_complete_library_partition_is_exact_and_disjoint(self):
        inventory = self.inventory()
        groups, _ = gate.classify_inventory(inventory, "lib")
        scheduled = [name for names in groups.values() for name in names]
        self.assertEqual(sorted(scheduled), inventory)
        self.assertEqual(len(scheduled), len(set(scheduled)))
        self.assertEqual(groups["lightweight"], ["buffer::tests::case"])

    def test_eval_scope_selects_only_the_five_known_eval_groups(self):
        inventory = self.inventory()
        groups, phases = gate.classify_inventory(inventory, "eval")
        self.assertEqual(sum(map(len, groups.values())), 5)
        self.assertEqual(
            [[spec.name for spec in phase] for phase in phases],
            [["eval_01", "eval_02"], ["eval_03", "eval_04"], ["eval_05"]],
        )

    def test_unknown_eval_group_fails_closed(self):
        with self.assertRaisesRegex(gate.GateError, "explicit resource classification"):
            gate.classify_inventory(
                self.inventory() + ["lisp::eval::tests::eval_06::case"], "lib"
            )

    def test_result_validation_checks_inventory_complement(self):
        result = gate.parse_test_result(
            "test result: ok. 3 passed; 0 failed; 1 ignored; 0 measured; "
            "6 filtered out; finished in 1.25s"
        )
        gate.validate_test_result(
            result, expected=4, total=10, expected_ignored=1
        )
        with self.assertRaisesRegex(gate.GateError, "complement"):
            gate.validate_test_result(
                result, expected=4, total=11, expected_ignored=1
            )

    def test_failed_result_is_never_accepted(self):
        result = gate.parse_test_result(
            "test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; "
            "6 filtered out; finished in 1.25s"
        )
        with self.assertRaisesRegex(gate.GateError, "did not pass"):
            gate.validate_test_result(
                result, expected=4, total=10, expected_ignored=0
            )

    def test_new_ignored_outcome_is_never_accepted(self):
        result = gate.parse_test_result(
            "test result: ok. 3 passed; 0 failed; 1 ignored; 0 measured; "
            "6 filtered out; finished in 1.25s"
        )
        with self.assertRaisesRegex(gate.GateError, "exact ignored inventory"):
            gate.validate_test_result(
                result, expected=4, total=10, expected_ignored=0
            )

    def test_template_groups_are_forced_to_one_test_thread(self):
        for group in (
            *gate.EVAL_GROUPS,
            gate.PRIMITIVES_GROUP,
            gate.COMPAT_RUNTIME_GROUP,
            gate.TTY_GROUP,
        ):
            self.assertTrue(group.template)
            self.assertEqual(group.test_threads, 1)
            self.assertIn("1", group.command(Path("/tmp/libtests")))

    def test_lightweight_command_skips_every_classified_prefix(self):
        command = gate.LIGHTWEIGHT_GROUP.command(Path("/tmp/libtests"))
        for prefix in gate.CLASSIFIED_PREFIXES:
            self.assertIn(prefix, command)

    def test_test_targets_come_from_cargo_metadata(self):
        manifest = str((gate.PROJECT_ROOT / "Cargo.toml").resolve())
        metadata = {
            "packages": [
                {
                    "manifest_path": manifest,
                    "targets": [
                        {"name": "emaxx", "kind": ["lib"], "test": True},
                        {"name": "emaxx", "kind": ["bin"], "test": True},
                        {
                            "name": "compat-harness",
                            "kind": ["bin"],
                            "test": True,
                        },
                        {"name": "cli", "kind": ["test"], "test": True},
                        {
                            "name": "package_lifecycle",
                            "kind": ["test"],
                            "test": True,
                        },
                    ],
                }
            ]
        }
        self.assertEqual(
            gate.parse_cargo_test_targets(metadata),
            {
                "bins": ["compat-harness", "emaxx"],
                "integrations": ["cli", "package_lifecycle"],
            },
        )

    def test_phase_cleans_up_first_worker_if_second_launch_fails(self):
        first = mock.Mock()
        first.spec.name = "eval_01"
        with (
            mock.patch.object(
                gate,
                "start_group",
                side_effect=[first, gate.GateError("launch failed")],
            ),
            mock.patch.object(gate, "terminate_group") as terminate,
        ):
            with self.assertRaisesRegex(gate.GateError, "launch failed"):
                gate.run_phase(
                    gate.EVAL_PHASES[0],
                    {"eval_01": ["one"], "eval_02": ["two"]},
                    Path("/tmp/libtests"),
                    Path("/tmp"),
                    repetition=1,
                    total=2,
                    timeout_seconds=1,
                    ignored_names=frozenset(),
                )
        terminate.assert_called_once_with(first)
        first.log_handle.close.assert_called_once()

    def test_gate_environment_overrides_unsafe_thread_count(self):
        with mock.patch.dict(os.environ, {"RUST_TEST_THREADS": "99"}):
            environment = gate.gate_environment(template=True)
        self.assertEqual(environment["RUST_TEST_THREADS"], "2")
        self.assertEqual(environment["EMAXX_IMAGE_TEMPLATE"], "1")


if __name__ == "__main__":
    unittest.main()
