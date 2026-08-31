#!/usr/bin/env python3
"""Deterministic command-line checker used by the Flycheck parity gate.

The checker deliberately has no dependency on a compiler or language runtime
beyond Python.  Its modes let the gate exercise successful empty checks,
parsed diagnostics, malformed output, and cancellation of a live process.
"""

from __future__ import annotations

from pathlib import Path
import signal
import sys
from typing import NoReturn


def fail(message: str) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(2)


def content_diagnostics(source: Path) -> int:
    levels = {
        "INFO": ("info", "I100", "deterministic information"),
        "WARN": ("warning", "W200", "deterministic warning"),
        "ERROR": ("error", "E300", "deterministic error"),
    }
    found = False
    for line_number, line in enumerate(source.read_text(encoding="utf-8").splitlines(), 1):
        for token, (level, identifier, message) in levels.items():
            column = line.find(token)
            if column >= 0:
                found = True
                print(
                    "%d:%d: %s %s: %s"
                    % (line_number, column + 1, level, identifier, message)
                )
    return 1 if found else 0


def wait_for_cancellation() -> NoReturn:
    def stop(_signum: int, _frame: object) -> NoReturn:
        raise SystemExit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    print("READY", flush=True)
    while True:
        signal.pause()


def main() -> int:
    if len(sys.argv) != 3:
        fail("usage: flycheck_fixture_checker.py MODE SOURCE")
    mode = sys.argv[1]
    source = Path(sys.argv[2])
    if not source.is_file():
        fail("source is not a file: %s" % source)
    if mode == "content":
        return content_diagnostics(source)
    if mode == "clean":
        return 0
    if mode == "malformed":
        print("this output deliberately has no location or severity")
        return 1
    if mode == "wait":
        wait_for_cancellation()
    fail("unknown mode: %s" % mode)


if __name__ == "__main__":
    raise SystemExit(main())
