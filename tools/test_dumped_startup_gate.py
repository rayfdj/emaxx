"""Reject apparent success without saved-image startup and all original tests."""
import unittest
from dumped_startup_gate import SUITES, gate_succeeded


def reports():
    return {editor: {"image_built": True, "startup_probe_passed": True,
                     "test_exit_codes": [0, 0],
                     "outcomes": {name: "passed" for names in SUITES.values() for name in names}}
            for editor in ["gnu", "emaxx"]}


class DumpedStartupGateTests(unittest.TestCase):
    def test_both_editors_must_restore_and_pass_all_three(self):
        self.assertTrue(gate_succeeded(reports()))

    def test_matching_failure_or_skip_is_not_success(self):
        for status in ["FAILED", "skipped"]:
            result = reports()
            for editor in result.values():
                editor["outcomes"] = dict.fromkeys(editor["outcomes"], status)
            self.assertFalse(gate_succeeded(result))

    def test_missing_or_extra_editor_is_not_success(self):
        self.assertFalse(gate_succeeded({}))
        self.assertFalse(gate_succeeded({"gnu": reports()["gnu"]}))
        result = reports()
        result["unexpected"] = result["gnu"]
        self.assertFalse(gate_succeeded(result))

    def test_incomplete_or_extra_test_report_is_not_success(self):
        result = reports()
        result["emaxx"]["outcomes"].pop(next(iter(result["emaxx"]["outcomes"])))
        self.assertFalse(gate_succeeded(result))
        result = reports()
        result["emaxx"]["outcomes"]["unexpected"] = "passed"
        self.assertFalse(gate_succeeded(result))

    def test_nonzero_exit_is_not_success_even_with_passing_report(self):
        for status in [1, 124, -9]:
            result = reports()
            result["emaxx"]["test_exit_codes"] = [0, status]
            self.assertFalse(gate_succeeded(result))

    def test_no_image_or_unverified_startup_is_not_success(self):
        for editor in ["gnu", "emaxx"]:
            for field in ["image_built", "startup_probe_passed"]:
                result = reports()
                result[editor][field] = False
                self.assertFalse(gate_succeeded(result))


if __name__ == "__main__":
    unittest.main()
