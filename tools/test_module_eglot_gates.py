"""Shared failures and accidental omissions must never satisfy these gates."""
import unittest

from emacs_module_gate import gate_succeeded as module_succeeded
from eglot_gate import gate_succeeded as eglot_succeeded


def reports(count):
    return {editor: {"exit_code": 0, "report": {"file_status": "loaded", "results": [
        {"name": f"test-{i}", "status": "passed"} for i in range(count)]}}
        for editor in ("gnu", "emaxx")}


class SuccessGates(unittest.TestCase):
    def test_complete_success(self):
        self.assertTrue(module_succeeded(reports(38), "darwin"))
        self.assertTrue(eglot_succeeded(reports(52)))

    def test_matching_failure_or_skip_is_not_success(self):
        for count, check in [(38, lambda r: module_succeeded(r, "darwin")), (52, eglot_succeeded)]:
            for status in ("failed", "skipped"):
                result = reports(count)
                for editor in result.values():
                    editor["report"]["results"][0]["status"] = status
                self.assertFalse(check(result))

    def test_missing_results_or_process_failure_is_not_success(self):
        for count, check in [(38, lambda r: module_succeeded(r, "darwin")), (52, eglot_succeeded)]:
            self.assertFalse(check({}))
            for field, value in [("report", None), ("exit_code", 1)]:
                result = reports(count)
                result["emaxx"][field] = value
                self.assertFalse(check(result))
            self.assertFalse(check(reports(count - 1)))

    def test_only_platform_module_skip_is_allowed(self):
        result = reports(38)
        for editor in result.values():
            editor["report"]["results"][0] = {
                "name": "module-darwin-secondary-suffix", "status": "skipped"}
        self.assertTrue(module_succeeded(result, "linux"))
        self.assertFalse(module_succeeded(result, "darwin"))

    def test_requires_the_same_unique_selected_outcomes(self):
        for count, check in [(38, lambda r: module_succeeded(r, "darwin")), (52, eglot_succeeded)]:
            result = reports(count)
            result["emaxx"]["report"]["results"][0]["name"] = "different-test"
            self.assertFalse(check(result))
            result = reports(count)
            for editor in result.values():
                editor["report"]["results"].append(editor["report"]["results"][0])
            self.assertFalse(check(result))


if __name__ == "__main__":
    unittest.main()
