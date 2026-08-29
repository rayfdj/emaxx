#!/usr/bin/env python3
"""Regression tests for the terminal stream decoder used by ttydiff."""

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from ttydiff import (
    Action,
    ADVERSARIAL_COMMAND_SCENARIO_NAMES,
    COLS,
    CORE_FREQUENCY_SCENARIO_NAMES,
    DIRED_BATCH_SCENARIO_NAMES,
    FIELDNOTES_FIXTURE_PATH,
    FIELDNOTES_ADVANCED_SCENARIO_NAMES,
    FIELDNOTES_SCENARIO_NAMES,
    FILE_LIFECYCLE_SCENARIO_NAMES,
    GLYPHLESS_DISPLAY_SCENARIO_NAMES,
    HELP_FILE_DIRED_SCENARIO_NAMES,
    HIGH_VALUE_COMMAND_SCENARIO_NAMES,
    PACKAGE_MENU_SCENARIO_NAMES,
    SCENARIOS,
    REGEXP_SEARCH_REPLACE_SCENARIO_NAMES,
    SEEDED_SAFE_SCENARIO_NAMES,
    UNDO_KILL_RING_SCENARIO_NAMES,
    Vt100Screen,
    action_timing,
    command_dispatch_minimum,
    create_scenario_target_pair,
    filesystem_snapshot,
    gnu_no_window_setup,
    normalize_action,
    remove_scenario_target,
    screen_divergences,
    seeded_safe_actions,
    select_scenarios,
    terminal_environment,
)
from ttydiff_explore import minimize_divergence


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

    def test_named_actions_checkpoint_but_legacy_chunks_do_not(self) -> None:
        named = Action("forward-char", b"\x06", filesystem=True)
        self.assertIs(normalize_action(named, 0), named)
        self.assertTrue(named.filesystem)
        legacy = normalize_action(b"old scenario", 2)
        self.assertEqual(legacy.name, "step-3")
        self.assertFalse(legacy.checkpoint)

    def test_command_dispatch_floor_scales_with_complete_input(self) -> None:
        self.assertEqual(command_dispatch_minimum(b"x", 1.0), 0.35)
        self.assertEqual(command_dispatch_minimum(b"x" * 20, 2.0), 1.0)
        self.assertEqual(command_dispatch_minimum(b"x" * 100, 2.0), 2.0)
        self.assertEqual(command_dispatch_minimum(b"x", 2.0, True), 2.0)

    def test_legacy_final_checkpoint_gets_complete_dispatch_window(self) -> None:
        legacy = normalize_action(b"complete command", 0)
        self.assertEqual(action_timing("legacy", 0, True, legacy), (3.0, 0.5))

    def test_screen_contract_detects_text_attributes_and_cursor(self) -> None:
        expected = Vt100Screen()
        actual = Vt100Screen()
        expected.feed(b"same")
        actual.feed(b"same")
        self.assertEqual(screen_divergences(expected, actual)[0], [])

        text_mismatch = Vt100Screen()
        text_mismatch.feed(b"else")
        divergences, _ = screen_divergences(expected, text_mismatch)
        self.assertTrue(any(offset == 0 for offset, _, _ in divergences))

        attr_mismatch = Vt100Screen()
        attr_mismatch.feed(b"\x1b[1msame")
        divergences, _ = screen_divergences(expected, attr_mismatch)
        self.assertTrue(any("attrs" in str(offset) for offset, _, _ in divergences))

        cursor_mismatch = Vt100Screen()
        cursor_mismatch.feed(b"same\x1b[D")
        divergences, _ = screen_divergences(expected, cursor_mismatch)
        self.assertTrue(any(offset == "cursor" for offset, _, _ in divergences))

    def test_new_journeys_are_per_command_default_scenarios(self) -> None:
        by_name = {entry[0]: entry for entry in SCENARIOS}
        groups = (
            CORE_FREQUENCY_SCENARIO_NAMES,
            GLYPHLESS_DISPLAY_SCENARIO_NAMES,
            HELP_FILE_DIRED_SCENARIO_NAMES,
            HIGH_VALUE_COMMAND_SCENARIO_NAMES,
            ADVERSARIAL_COMMAND_SCENARIO_NAMES,
            FIELDNOTES_ADVANCED_SCENARIO_NAMES,
            UNDO_KILL_RING_SCENARIO_NAMES,
            REGEXP_SEARCH_REPLACE_SCENARIO_NAMES,
            FILE_LIFECYCLE_SCENARIO_NAMES,
            DIRED_BATCH_SCENARIO_NAMES,
            PACKAGE_MENU_SCENARIO_NAMES,
            SEEDED_SAFE_SCENARIO_NAMES,
        )
        for group in groups:
            for name in group:
                self.assertIn(name, by_name)
                self.assertTrue(by_name[name][2])
                self.assertTrue(all(isinstance(item, Action) for item in by_name[name][2]))
                self.assertTrue(any(item.checkpoint for item in by_name[name][2]))

    def test_glyphless_journey_keeps_real_scalars_and_per_character_motion(self) -> None:
        self.assertEqual(
            GLYPHLESS_DISPLAY_SCENARIO_NAMES,
            (
                "glyphless-unencodable-motion",
                "glyphless-unencodable-wrap",
                "glyphless-unencodable-hscroll",
                "glyphless-unencodable-hscroll-line-numbers",
            ),
        )
        scenarios = {entry[0]: entry for entry in SCENARIOS}
        scenario = scenarios["glyphless-unencodable-motion"]
        self.assertEqual(scenario[1], "AöB€CλD\n")
        self.assertEqual(len(scenario[2]), 7)
        self.assertTrue(all(action.keys == b"\x06" for action in scenario[2]))
        self.assertEqual(
            scenarios["glyphless-unencodable-wrap"][1],
            "x" * 76 + "öZ\n",
        )
        self.assertIn(
            b"set-window-hscroll nil 3",
            scenarios["glyphless-unencodable-hscroll"][2][0].keys,
        )
        self.assertIn(
            b"display-line-numbers t",
            scenarios["glyphless-unencodable-hscroll-line-numbers"][2][0].keys,
        )

    def test_seeded_safe_actions_are_reproducible_complete_commands(self) -> None:
        first = seeded_safe_actions(7595, 24)
        second = seeded_safe_actions(7595, 24)
        different = seeded_safe_actions(7596, 24)
        self.assertEqual(first, second)
        self.assertNotEqual(first, different)
        self.assertEqual(len(first), 24)
        self.assertTrue(all(command.keys for command in first))

    def test_seeded_divergence_minimizer_keeps_the_triggering_command(self) -> None:
        commands = [
            Action("one", b"1"),
            Action("trigger", b"!"),
            Action("two", b"2"),
            Action("three", b"3"),
        ]

        def runner(candidate):
            return all(command.keys != b"!" for command in candidate)

        self.assertEqual(minimize_divergence(commands, runner), [commands[1]])

    def test_mutating_scenarios_get_isolated_same_named_files(self) -> None:
        (gnu_path, emaxx_path), cleanup = create_scenario_target_pair(
            "save-contract",
            "initial\n",
            ".txt",
            {
                "separate_targets": True,
                "extra_files": {"sibling.txt": "sibling\n"},
            },
        )
        try:
            self.assertNotEqual(Path(gnu_path).parent, Path(emaxx_path).parent)
            self.assertEqual(Path(gnu_path).name, Path(emaxx_path).name)
            self.assertEqual(Path(gnu_path).read_text(), "initial\n")
            self.assertEqual(Path(emaxx_path).read_text(), "initial\n")
            self.assertEqual(filesystem_snapshot(gnu_path), filesystem_snapshot(emaxx_path))
            (Path(gnu_path).parent / "sibling.txt").write_text("changed\n")
            self.assertNotEqual(filesystem_snapshot(gnu_path), filesystem_snapshot(emaxx_path))
        finally:
            for target in cleanup:
                remove_scenario_target(target)

    def test_mutating_dired_scenarios_get_isolated_same_named_directories(
        self,
    ) -> None:
        (gnu_path, emaxx_path), cleanup = create_scenario_target_pair(
            "dired-contract",
            "",
            ".dat",
            {
                "target": "directory",
                "separate_targets": True,
                "padding_entries": 3,
                "extra_files": {"subdir/nested.txt": "nested\n"},
                "extra_directories": ("copy-dest",),
                "modes": {"copy-dest": 0o500},
            },
        )
        try:
            gnu = Path(gnu_path)
            emaxx = Path(emaxx_path)
            self.assertNotEqual(gnu.parent, emaxx.parent)
            self.assertEqual(gnu.name, emaxx.name)
            self.assertEqual(
                sorted(path.name for path in gnu.iterdir()),
                sorted(path.name for path in emaxx.iterdir()),
            )
            self.assertTrue((gnu / "00-padding-02.txt").is_file())
            self.assertEqual((gnu / "subdir/nested.txt").read_text(), "nested\n")
            self.assertEqual((gnu / "copy-dest").stat().st_mode & 0o777, 0o500)
            self.assertEqual(gnu.stat().st_mtime_ns, emaxx.stat().st_mtime_ns)
            self.assertEqual(
                (gnu / "alpha.txt").stat().st_mtime_ns,
                (emaxx / "alpha.txt").stat().st_mtime_ns,
            )
            (gnu / "alpha.txt").write_text("GNU-only mutation\n")
            self.assertEqual((emaxx / "alpha.txt").read_text(), "alpha file\nsecond line\n")
        finally:
            for target in cleanup:
                remove_scenario_target(target)

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
        for name in expected_names + FIELDNOTES_ADVANCED_SCENARIO_NAMES:
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
