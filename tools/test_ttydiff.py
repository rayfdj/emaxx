#!/usr/bin/env python3
"""Regression tests for the terminal stream decoder used by ttydiff."""

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from ttydiff import Vt100Screen


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


if __name__ == "__main__":
    unittest.main()
