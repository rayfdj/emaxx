"""Acceptance must require real success from both editors, never matching failure."""
import unittest
from openpgp_gate import TESTS, gate_succeeded


def reports(status="passed"):
    return {editor: {"exit_code": 0, "outcomes": {name: status for name in TESTS}}
            for editor in ["gnu", "emaxx"]}


class OpenPGPGateTests(unittest.TestCase):
    def test_both_editors_must_pass_all_four(self):
        self.assertTrue(gate_succeeded(reports()))

    def test_matching_failures_are_not_success(self):
        self.assertFalse(gate_succeeded(reports("failed")))

    def test_matching_skips_are_not_success(self):
        self.assertFalse(gate_succeeded(reports("skipped")))

    def test_missing_editor_or_empty_report_is_not_success(self):
        self.assertFalse(gate_succeeded({}))
        self.assertFalse(gate_succeeded({"gnu": reports()["gnu"]}))

    def test_incomplete_test_report_is_not_success(self):
        result = reports()
        del result["emaxx"]["outcomes"][TESTS[-1]]
        self.assertFalse(gate_succeeded(result))
        result["emaxx"]["outcomes"] = None
        self.assertFalse(gate_succeeded(result))

    def test_nonzero_exit_is_not_success_even_with_passing_report(self):
        result = reports()
        result["emaxx"]["exit_code"] = 1
        self.assertFalse(gate_succeeded(result))


if __name__ == "__main__":
    unittest.main()
