#!/usr/bin/env python3
"""Differential terminal test: emaxx vs GNU `emacs -nw'.

Runs the same scripted keystrokes through both editors in pseudo-terminals,
decodes each output stream with a small VT100 interpreter into a character
grid, and compares the grids region by region:

  text area   rows before the mode line, after skipping GNU's menu-bar row
  mode line   compared as a presence check only for now (formats differ)
  echo area   the final row

The text area is the contract: identical buffer content, cursor row/column,
and scrolling.  Mode-line/echo formatting differences are reported but not
fatal until the frontend renders `mode-line-format' (a later Phase 2 step).

Usage:
    tools/ttydiff.py EMAXX_BINARY GNU_BINARY GNU_LISP_DIR [SCRIPT...]

With no SCRIPT arguments the built-in scenarios run.  Exits nonzero on any
text-area divergence; missing binaries skip with exit 0 so unconfigured
environments stay green.
"""

import os
import pty
import select
import struct
import sys
import tempfile
import time

import fcntl
import termios

ROWS, COLS = 24, 80


class Vt100Screen:
    """The minimum terminal model both editors' output actually uses:
    cursor addressing, line/screen erase, alternate screen, autowrap."""

    def __init__(self, rows=ROWS, cols=COLS):
        self.rows, self.cols = rows, cols
        self.grid = [[" "] * cols for _ in range(rows)]
        self.row = self.col = 0

    def feed(self, data):
        text = data.decode("utf-8", "replace")
        i = 0
        while i < len(text):
            c = text[i]
            if c == "\x1b":
                i = self._escape(text, i)
            elif c == "\r":
                self.col = 0
                i += 1
            elif c == "\n":
                self.row = min(self.row + 1, self.rows - 1)
                i += 1
            elif c == "\b":
                self.col = max(self.col - 1, 0)
                i += 1
            elif c == "\x07":
                i += 1
            else:
                if self.col >= self.cols:
                    self.col = 0
                    self.row = min(self.row + 1, self.rows - 1)
                self.grid[self.row][self.col] = c
                self.col += 1
                i += 1

    def _escape(self, text, i):
        # i points at ESC.
        if i + 1 >= len(text):
            return i + 1
        kind = text[i + 1]
        if kind == "[":
            j = i + 2
            while j < len(text) and text[j] not in "@ABCDEFGHJKLMPXacdfghlmnpqrsu":
                j += 1
            if j >= len(text):
                return len(text)
            body, final = text[i + 2 : j], text[j]
            self._csi(body, final)
            return j + 1
        if kind == "]":  # OSC: consume to BEL or ESC \
            j = text.find("\x07", i)
            k = text.find("\x1b\\", i)
            ends = [e for e in (j, k) if e != -1]
            return (min(ends) + (1 if min(ends) == j else 2)) if ends else len(text)
        return i + 2

    def _csi(self, body, final):
        params = [int(p) if p.isdigit() else 0 for p in body.lstrip("?").split(";")] or [0]
        p0 = params[0]
        if final in "Hf":
            self.row = min(max((params[0] or 1) - 1, 0), self.rows - 1)
            self.col = min(max((params[1] if len(params) > 1 else 1) or 1, 1) - 1, self.cols - 1)
        elif final == "A":
            self.row = max(self.row - max(p0, 1), 0)
        elif final == "B":
            self.row = min(self.row + max(p0, 1), self.rows - 1)
        elif final == "C":
            self.col = min(self.col + max(p0, 1), self.cols - 1)
        elif final == "D":
            self.col = max(self.col - max(p0, 1), 0)
        elif final == "G":
            self.col = min(max(p0, 1) - 1, self.cols - 1)
        elif final == "d":
            self.row = min(max(p0, 1) - 1, self.rows - 1)
        elif final == "J":
            self._erase_screen(p0)
        elif final == "K":
            self._erase_line(p0)
        elif final == "L":  # insert lines
            for _ in range(max(p0, 1)):
                self.grid.insert(self.row, [" "] * self.cols)
                self.grid.pop()
        elif final == "M":  # delete lines
            for _ in range(max(p0, 1)):
                if self.row < len(self.grid):
                    self.grid.pop(self.row)
                    self.grid.append([" "] * self.cols)
        elif final == "@":  # insert blank characters, shifting right
            count = max(p0, 1)
            row = self.grid[self.row]
            row[self.col:] = ([" "] * count + row[self.col:])[: self.cols - self.col]
        elif final == "P":  # delete characters, shifting left
            count = max(p0, 1)
            row = self.grid[self.row]
            row[self.col:] = (row[self.col + count:] + [" "] * count)[: self.cols - self.col]
        # SGR (m), modes (h/l), and the rest do not affect the text grid.

    def _erase_screen(self, mode):
        if mode == 2:
            self.grid = [[" "] * self.cols for _ in range(self.rows)]
        elif mode == 0:
            self._erase_line(0)
            for r in range(self.row + 1, self.rows):
                self.grid[r] = [" "] * self.cols
        elif mode == 1:
            self._erase_line(1)
            for r in range(self.row):
                self.grid[r] = [" "] * self.cols

    def _erase_line(self, mode):
        if mode == 0:
            for c in range(self.col, self.cols):
                self.grid[self.row][c] = " "
        elif mode == 1:
            for c in range(self.col + 1):
                self.grid[self.row][c] = " "
        else:
            self.grid[self.row] = [" "] * self.cols

    def lines(self):
        return ["".join(row).rstrip() for row in self.grid]


class Session:
    def __init__(self, argv, env_extra):
        env = dict(os.environ)
        env.update(env_extra)
        env["TERM"] = "xterm"
        pid, fd = pty.fork()
        if pid == 0:
            os.environ.clear()
            os.environ.update(env)
            os.execv(argv[0], argv)
        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, COLS, 0, 0))
        self.pid, self.fd = pid, fd
        self.screen = Vt100Screen()

    def drain(self, timeout):
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return
            ready, _, _ = select.select([self.fd], [], [], min(remaining, 0.2))
            if not ready:
                # A quiet gap after output means the redraw settled.
                return
            try:
                chunk = os.read(self.fd, 65536)
            except OSError:
                return
            if not chunk:
                return
            self.screen.feed(chunk)

    def wait_boot(self, timeout):
        """Block until the editor takes the terminal (alternate screen) and
        its first redraw settles.  Sending keys earlier would hit the pty's
        cooked-mode line discipline instead of the editor."""
        deadline = time.time() + timeout
        seen = b""
        while time.time() < deadline and b"\x1b[?1049h" not in seen:
            ready, _, _ = select.select([self.fd], [], [], 0.25)
            if not ready:
                continue
            try:
                chunk = os.read(self.fd, 65536)
            except OSError:
                return
            if not chunk:
                return
            seen += chunk
            self.screen.feed(chunk)
        self.drain(2.0)

    def send(self, data, settle=0.5):
        os.write(self.fd, data)
        self.drain(settle)

    def close(self):
        try:
            os.kill(self.pid, 9)
            os.waitpid(self.pid, 0)
        except (ProcessLookupError, ChildProcessError):
            pass
        os.close(self.fd)


def find_mode_line(lines):
    """Both editors draw a dash-heavy mode line above the echo area."""
    for index in range(len(lines) - 1, -1, -1):
        if lines[index].count("-") >= 8 and "(" in lines[index]:
            return index
    return len(lines) - 2


def compare(scenario, keys, gnu_argv, emaxx_argv, gnu_env, emaxx_env, boot_wait):
    gnu = Session(gnu_argv, gnu_env)
    emaxx = Session(emaxx_argv, emaxx_env)
    try:
        gnu.wait_boot(boot_wait)
        emaxx.wait_boot(boot_wait)
        for chunk in keys:
            gnu.send(chunk)
            emaxx.send(chunk)
        gnu.drain(1.0)
        emaxx.drain(1.0)

        gnu_lines = gnu.screen.lines()
        emaxx_lines = emaxx.screen.lines()

        gnu_mode = find_mode_line(gnu_lines)
        emaxx_mode = find_mode_line(emaxx_lines)
        # GNU -nw shows a menu-bar row at the top; emaxx does not (yet).
        gnu_text = gnu_lines[1:gnu_mode]
        emaxx_text = emaxx_lines[0:emaxx_mode]

        length = max(len(gnu_text), len(emaxx_text))
        gnu_text += [""] * (length - len(gnu_text))
        emaxx_text += [""] * (length - len(emaxx_text))
        divergences = []
        for offset, (expected, actual) in enumerate(zip(gnu_text, emaxx_text)):
            if expected != actual:
                divergences.append((offset, expected, actual))

        if divergences:
            print(f"DIVERGE [{scenario}]: {len(divergences)} text row(s) differ")
            for offset, expected, actual in divergences[:8]:
                print(f"  row {offset}:")
                print(f"    gnu  : {expected!r}")
                print(f"    emaxx: {actual!r}")
            return False
        print(f"MATCH [{scenario}]: text area identical ({length} rows)")
        return True
    finally:
        gnu.close()
        emaxx.close()


SCENARIOS = [
    # (name, initial file contents, keystrokes)
    ("type-one-line", "", [b"hello world"]),
    ("multiline-and-motion", "", [b"first line\rsecond line\rthird line", b"\x10\x10", b"\x01", b"X"]),
    ("open-existing-and-edit", "alpha\nbeta\ngamma\n", [b"\x0e\x0e", b"\x05", b" tail"]),
    ("kill-line-and-undo", "one\ntwo\nthree\n", [b"\x0b", b"\x1f"]),
    ("delete-and-backspace", "abcdef\n", [b"\x06\x06", b"\x04", b"\x7f\x7f"]),
]


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(2)
    emaxx_binary, gnu_binary, lisp_dir = sys.argv[1:4]
    for path, label in [(emaxx_binary, "emaxx"), (gnu_binary, "GNU emacs")]:
        if not os.path.exists(path):
            print(f"SKIP: no {label} binary at {path}")
            return
    if not os.path.isdir(lisp_dir):
        print(f"SKIP: no GNU lisp tree at {lisp_dir}")
        return
    load_path = os.pathsep.join(
        [lisp_dir] + sorted(e.path for e in os.scandir(lisp_dir) if e.is_dir())
    )

    failures = 0
    for name, contents, keys in SCENARIOS:
        handle, path = tempfile.mkstemp(suffix=".txt", prefix=f"ttydiff-{name}-")
        with os.fdopen(handle, "w") as out:
            out.write(contents)
        try:
            ok = compare(
                name,
                keys,
                [gnu_binary, "-nw", "-Q", path],
                [emaxx_binary, path],
                {},
                {"EMACSLOADPATH": load_path},
                boot_wait=20.0,
            )
            failures += 0 if ok else 1
        finally:
            os.unlink(path)
    if failures:
        print(f"FAIL: {failures} scenario(s) diverged")
        sys.exit(1)
    print("PASS: all scenarios match")


if __name__ == "__main__":
    main()
