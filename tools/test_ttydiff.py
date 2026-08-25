#!/usr/bin/env python3
"""Regression tests for the terminal stream decoder used by ttydiff."""

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from ttydiff import (
    COLS,
    FIELDNOTES_FIXTURE_PATH,
    FIELDNOTES_SCENARIO_NAMES,
    SCENARIOS,
    Vt100Screen,
    gnu_no_window_setup,
    select_scenarios,
    terminal_environment,
)


class Vt100ScreenTests(unittest.TestCase):
    def assert_chunk_invariant(self, stream: bytes) -> None:
        whole = Vt100Screen()
        whole.feed(stream)

        bytewise = Vt100Screen()
        for byte in stream:
            bytewise.feed(bytes([byte]))

        self.assertEqual(bytewise.lines(), whole.lines())
        self.assertEqual((bytewise.row, bytewise.col), (whole.row, whole.col))

    def test_escape_and_utf8_boundaries_are_streaming(self) -> None:
        self.assert_chunk_invariant(
            b"alpha"
            b"\x1b[?12;25h"
            b"\x1b[38;5;191m"
            + " λ ".encode()
            + b"\x1b[K"
            b"\x1b]0;emaxx terminal\x07"
            b"omega"
        )

    def test_designation_escape_is_not_rendered(self) -> None:
        self.assert_chunk_invariant(b"before\x1b(Bafter")
        screen = Vt100Screen()
        screen.feed(b"before\x1b(Bafter")
        self.assertEqual(screen.lines()[0], "beforeafter")

    def test_scenario_selection_defaults_to_all(self) -> None:
        self.assertIs(select_scenarios([]), SCENARIOS)

    def test_scenario_selection_preserves_requested_order(self) -> None:
        selected = select_scenarios(["page-past-end-error-echo", "type-one-line"])
        self.assertEqual(
            [entry[0] for entry in selected],
            ["page-past-end-error-echo", "type-one-line"],
        )

    def test_scenario_names_are_unique(self) -> None:
        names = [entry[0] for entry in SCENARIOS]
        self.assertEqual(len(names), len(set(names)))

    def test_scenario_selection_rejects_unknown_names(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown scenario.*not-a-scenario"):
            select_scenarios(["not-a-scenario"])

    def test_fieldnotes_regressions_stay_in_default_gate(self) -> None:
        expected_names = (
            "org-overview-open",
            "org-backtab-cycle",
            "org-tab-children",
            "org-done-face",
            "org-occur-wraps",
        )
        self.assertEqual(FIELDNOTES_SCENARIO_NAMES, expected_names)

        fixture = FIELDNOTES_FIXTURE_PATH.read_text(encoding="utf-8")
        self.assertIn("#+STARTUP: overview", fixture)
        self.assertIn("* DONE ", fixture)
        # The 79-column tagged heading exceeds the usable text body once
        # GNU reserves its continuation/truncation cell on an 80-column tty.
        self.assertGreaterEqual(max(map(len, fixture.splitlines())), COLS - 1)
        self.assertGreaterEqual(len(fixture.splitlines()), 60)

        scenarios = {entry[0]: entry for entry in SCENARIOS}
        for name in expected_names:
            self.assertIn(name, scenarios)
            self.assertEqual(scenarios[name][1], fixture)
            self.assertEqual(scenarios[name][3], ".org")

    def test_terminal_environment_has_one_deterministic_color_contract(self) -> None:
        env = terminal_environment(
            {
                "COLORTERM": "truecolor",
                "TERM_PROGRAM": "ambient-terminal",
                "COLORFGBG": "15;0",
                "EMAXX_TEST_SENTINEL": "kept",
            }
        )
        self.assertEqual(env["TERM"], "xterm")
        self.assertEqual(env["EMAXX_TEST_SENTINEL"], "kept")
        for name in ("COLORTERM", "TERM_PROGRAM", "COLORFGBG"):
            self.assertNotIn(name, env)

    def test_gnu_oracle_setup_removes_ns_only_menu_state(self) -> None:
        setup = gnu_no_window_setup("/tmp/emacs lisp")
        self.assertIn("(delq 'ns features)", setup)
        self.assertIn("[?\\s-c]", setup)
        self.assertIn("[?\\s-u]", setup)
        self.assertIn('"/tmp/emacs lisp/menu-bar.el"', setup)


if __name__ == "__main__":
    unittest.main()
