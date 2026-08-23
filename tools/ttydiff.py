#!/usr/bin/env python3
"""Differential terminal test: emaxx vs GNU `emacs -nw'.

Runs the same scripted keystrokes through both editors in pseudo-terminals,
decodes each output stream with a small VT100 interpreter into a character
grid, and compares the grids region by region:

  text area   rows before the mode line, after skipping GNU's menu-bar row
  mode line   exact characters and padding
  echo area   the final row

The contract is identical buffer content, cursor row/column, scrolling,
mode-line rendering, and echo-area rendering.

Usage:
    tools/ttydiff.py EMAXX_BINARY GNU_BINARY GNU_LISP_DIR [SCRIPT...]

With no SCRIPT arguments the built-in scenarios run.  Exits nonzero on any
text-area divergence; missing binaries skip with exit 0 so unconfigured
environments stay green.
"""

import codecs
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
FIXTURE_PATH = "/tmp/emaxxff-fixture.dat"
# A directory whose listing both editors complete over: two names sharing
# the ambiguous prefix the *Completions* scenarios TAB on.
COMPLETIONS_DIR_NAME = "emaxxffcomp"
COMPLETIONS_DIR = f"/tmp/{COMPLETIONS_DIR_NAME}"


# (fg, bg, bold, underline, reverse): fg/bg are ANSI indexes or None for
# the terminal default.  Erased cells always carry DEFAULT_ATTR — only
# explicitly painted cells hold face attributes, on both editors alike.
DEFAULT_ATTR = (None, None, False, False, False)


class Vt100Screen:
    """The minimum terminal model both editors' output actually uses:
    cursor addressing, line/screen erase, alternate screen, autowrap —
    plus per-cell SGR attributes, the face layer of the contract."""

    def __init__(self, rows=ROWS, cols=COLS):
        self.rows, self.cols = rows, cols
        self.grid = [[" "] * cols for _ in range(rows)]
        self.attrs = [[DEFAULT_ATTR] * cols for _ in range(rows)]
        self.attr = DEFAULT_ATTR
        self.row = self.col = 0
        self.top_margin, self.bottom_margin = 0, rows - 1
        self.saved_cursor = (0, 0)
        self._decoder = codecs.getincrementaldecoder("utf-8")("replace")
        self._pending = ""

    def feed(self, data):
        # PTYs may split both UTF-8 characters and terminal escape sequences
        # at any byte.  Preserve incomplete input across reads: otherwise the
        # tail of (for example) ESC[?25h becomes literal screen text and makes
        # the differential result depend on kernel scheduling.
        text = self._pending + self._decoder.decode(data)
        self._pending = ""
        i = 0
        while i < len(text):
            c = text[i]
            if c == "\x1b":
                next_index = self._escape(text, i)
                if next_index is None:
                    self._pending = text[i:]
                    break
                i = next_index
            elif c == "\r":
                self.col = 0
                i += 1
            elif c == "\n":
                self._linefeed()
                i += 1
            elif c == "\t":
                self.col = min((self.col // 8 + 1) * 8, self.cols - 1)
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
                self.attrs[self.row][self.col] = self.attr
                self.col += 1
                i += 1

    def _escape(self, text, i):
        # i points at ESC.
        if i + 1 >= len(text):
            return None
        kind = text[i + 1]
        if kind == "[":
            j = i + 2
            # ECMA-48 defines every byte from 0x40 through 0x7e as a CSI
            # final byte.  An allowlist silently leaked valid GNU sequences.
            while j < len(text) and not ("@" <= text[j] <= "~"):
                j += 1
            if j >= len(text):
                return None
            body, final = text[i + 2 : j], text[j]
            self._csi(body, final)
            return j + 1
        if kind == "]":  # OSC: consume to BEL or ESC \
            j = text.find("\x07", i)
            k = text.find("\x1b\\", i)
            ends = [e for e in (j, k) if e != -1]
            return (min(ends) + (1 if min(ends) == j else 2)) if ends else None
        if kind in "()#%":
            # Character-set/designation escapes carry one more byte.
            return i + 3 if i + 2 < len(text) else None
        if kind == "D":  # IND: index down, scrolling at the region bottom
            self._linefeed()
        elif kind == "M":  # RI: reverse index, scrolling at the region top
            if self.row == self.top_margin:
                self._scroll_down(1)
            else:
                self.row = max(self.row - 1, 0)
        elif kind == "E":  # NEL
            self.col = 0
            self._linefeed()
        elif kind == "7":  # DECSC
            self.saved_cursor = (self.row, self.col)
        elif kind == "8":  # DECRC
            self.row, self.col = getattr(self, "saved_cursor", (0, 0))
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
        elif final in "G`":
            self.col = min(max(p0, 1) - 1, self.cols - 1)
        elif final == "d":
            self.row = min(max(p0, 1) - 1, self.rows - 1)
        elif final == "J":
            self._erase_screen(p0)
        elif final == "K":
            self._erase_line(p0)
        elif final == "L":  # insert lines within the scroll region
            if self.top_margin <= self.row <= self.bottom_margin:
                for _ in range(max(p0, 1)):
                    self.grid.pop(self.bottom_margin)
                    self.grid.insert(self.row, [" "] * self.cols)
                    self.attrs.pop(self.bottom_margin)
                    self.attrs.insert(self.row, [DEFAULT_ATTR] * self.cols)
        elif final == "M":  # delete lines within the scroll region
            if self.top_margin <= self.row <= self.bottom_margin:
                for _ in range(max(p0, 1)):
                    self.grid.pop(self.row)
                    self.grid.insert(self.bottom_margin, [" "] * self.cols)
                    self.attrs.pop(self.row)
                    self.attrs.insert(self.bottom_margin, [DEFAULT_ATTR] * self.cols)
        elif final == "S":  # scroll region up
            self._scroll_up(max(p0, 1))
        elif final == "T":  # scroll region down
            self._scroll_down(max(p0, 1))
        elif final == "r":  # DECSTBM: set scroll region, home the cursor
            top = (params[0] or 1) - 1
            bottom = (params[1] if len(params) > 1 and params[1] else self.rows) - 1
            if 0 <= top < bottom < self.rows:
                self.top_margin, self.bottom_margin = top, bottom
                self.row = self.col = 0
        elif final == "@":  # insert blank characters, shifting right
            count = max(p0, 1)
            row = self.grid[self.row]
            row[self.col:] = ([" "] * count + row[self.col:])[: self.cols - self.col]
            attrs = self.attrs[self.row]
            attrs[self.col:] = ([DEFAULT_ATTR] * count + attrs[self.col:])[: self.cols - self.col]
        elif final == "P":  # delete characters, shifting left
            count = max(p0, 1)
            row = self.grid[self.row]
            row[self.col:] = (row[self.col + count:] + [" "] * count)[: self.cols - self.col]
            attrs = self.attrs[self.row]
            attrs[self.col:] = (attrs[self.col + count:] + [DEFAULT_ATTR] * count)[: self.cols - self.col]
        elif final == "m":
            self._sgr(params if body else [0])
        # Modes (h/l) and the rest do not affect the text grid.

    def _sgr(self, params):
        fg, bg, bold, underline, reverse = self.attr
        i = 0
        while i < len(params):
            p = params[i]
            if p == 0:
                fg, bg, bold, underline, reverse = DEFAULT_ATTR
            elif p == 1:
                bold = True
            elif p == 22:
                bold = False
            elif p == 4:
                underline = True
            elif p == 24:
                underline = False
            elif p == 7:
                reverse = True
            elif p == 27:
                reverse = False
            elif 30 <= p <= 37:
                fg = p - 30
            elif p == 39:
                fg = None
            elif 40 <= p <= 47:
                bg = p - 40
            elif p == 49:
                bg = None
            elif 90 <= p <= 97:
                fg = p - 90 + 8
            elif 100 <= p <= 107:
                bg = p - 100 + 8
            elif p in (38, 48) and i + 2 < len(params) and params[i + 1] == 5:
                if p == 38:
                    fg = params[i + 2]
                else:
                    bg = params[i + 2]
                i += 2
            i += 1
        self.attr = (fg, bg, bold, underline, reverse)

    def _linefeed(self):
        if self.row == self.bottom_margin:
            self._scroll_up(1)
        else:
            self.row = min(self.row + 1, self.rows - 1)

    def _scroll_up(self, count):
        for _ in range(count):
            self.grid.pop(self.top_margin)
            self.grid.insert(self.bottom_margin, [" "] * self.cols)
            self.attrs.pop(self.top_margin)
            self.attrs.insert(self.bottom_margin, [DEFAULT_ATTR] * self.cols)

    def _scroll_down(self, count):
        for _ in range(count):
            self.grid.pop(self.bottom_margin)
            self.grid.insert(self.top_margin, [" "] * self.cols)
            self.attrs.pop(self.bottom_margin)
            self.attrs.insert(self.top_margin, [DEFAULT_ATTR] * self.cols)

    def _erase_screen(self, mode):
        if mode == 2:
            self.grid = [[" "] * self.cols for _ in range(self.rows)]
            self.attrs = [[DEFAULT_ATTR] * self.cols for _ in range(self.rows)]
        elif mode == 0:
            self._erase_line(0)
            for r in range(self.row + 1, self.rows):
                self.grid[r] = [" "] * self.cols
                self.attrs[r] = [DEFAULT_ATTR] * self.cols
        elif mode == 1:
            self._erase_line(1)
            for r in range(self.row):
                self.grid[r] = [" "] * self.cols
                self.attrs[r] = [DEFAULT_ATTR] * self.cols

    def _erase_line(self, mode):
        if mode == 0:
            for c in range(self.col, self.cols):
                self.grid[self.row][c] = " "
                self.attrs[self.row][c] = DEFAULT_ATTR
        elif mode == 1:
            for c in range(self.col + 1):
                self.grid[self.row][c] = " "
                self.attrs[self.row][c] = DEFAULT_ATTR
        else:
            self.grid[self.row] = [" "] * self.cols
            self.attrs[self.row] = [DEFAULT_ATTR] * self.cols

    def lines(self):
        return ["".join(row).rstrip() for row in self.grid]

    def attr_rows(self):
        """Per-row cell attributes, full width — the face layer of the
        comparison contract."""
        return [list(row) for row in self.attrs]


def describe_attr_row(attrs):
    """Compact human-readable runs for divergence messages:
    \"[0-13 rv][14-25 rv+b]\" — default-attribute runs are omitted."""
    parts = []
    start = 0
    while start < len(attrs):
        end = start
        while end < len(attrs) and attrs[end] == attrs[start]:
            end += 1
        if attrs[start] != DEFAULT_ATTR:
            fg, bg, bold, underline, reverse = attrs[start]
            bits = [
                item
                for item in (
                    f"fg{fg}" if fg is not None else "",
                    f"bg{bg}" if bg is not None else "",
                    "b" if bold else "",
                    "u" if underline else "",
                    "rv" if reverse else "",
                )
                if item
            ]
            parts.append(f"[{start}-{end - 1} {'+'.join(bits)}]")
        start = end
    return "".join(parts) or "[default]"


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
        gnu_attrs = gnu.screen.attr_rows()
        emaxx_attrs = emaxx.screen.attr_rows()

        gnu_mode = find_mode_line(gnu_lines)
        emaxx_mode = find_mode_line(emaxx_lines)
        # Both editors show the default menu bar on row 0 and work a
        # 21-row text window under it; every row — menu captions and
        # scroll positions included — must agree exactly.
        gnu_text = gnu_lines[0:gnu_mode]
        emaxx_text = emaxx_lines[0:emaxx_mode]

        # Faces are part of the contract: every cell's SGR attributes must
        # agree, on text rows, mode lines, and the echo area alike.
        compare_attrs = os.environ.get("EMAXX_TTYDIFF_TEXT_ONLY") is None

        length = max(len(gnu_text), len(emaxx_text))
        gnu_text += [""] * (length - len(gnu_text))
        emaxx_text += [""] * (length - len(emaxx_text))
        divergences = []
        for offset, (expected, actual) in enumerate(zip(gnu_text, emaxx_text)):
            if expected != actual:
                divergences.append((offset, expected, actual))
            elif compare_attrs and gnu_attrs[offset] != emaxx_attrs[offset]:
                divergences.append(
                    (
                        f"{offset} (attrs)",
                        describe_attr_row(gnu_attrs[offset]),
                        describe_attr_row(emaxx_attrs[offset]),
                    )
                )
        # The mode line is part of the contract: same characters, same
        # padding, same percent/line indicators.
        if gnu_lines[gnu_mode] != emaxx_lines[emaxx_mode]:
            divergences.append(("mode-line", gnu_lines[gnu_mode], emaxx_lines[emaxx_mode]))
        elif compare_attrs and gnu_attrs[gnu_mode] != emaxx_attrs[emaxx_mode]:
            divergences.append(
                (
                    "mode-line (attrs)",
                    describe_attr_row(gnu_attrs[gnu_mode]),
                    describe_attr_row(emaxx_attrs[emaxx_mode]),
                )
            )
        # So is the echo area: the same final message (or its absence).
        if gnu_lines[-1] != emaxx_lines[-1]:
            divergences.append(("echo", gnu_lines[-1], emaxx_lines[-1]))
        elif compare_attrs and gnu_attrs[len(gnu_lines) - 1] != emaxx_attrs[len(emaxx_lines) - 1]:
            divergences.append(
                (
                    "echo (attrs)",
                    describe_attr_row(gnu_attrs[len(gnu_lines) - 1]),
                    describe_attr_row(emaxx_attrs[len(emaxx_lines) - 1]),
                )
            )

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


WIDE_SAMPLE = "left-margin " + "wide" * 40 + " right-end\nsecond line\nthird line\n"

ELISP_SAMPLE = """;; A comment line for font-lock.
(defun demo-function (arg)
  "Documentation string here."
  (let ((value (concat "literal" arg)))
    (if value
        (message "%s" value)
      nil)))

(defvar demo-variable 42
  "Another doc string.")
"""

SCENARIOS = [
    # (name, initial file contents, keystrokes[, file suffix])
    ("type-one-line", "", [b"hello world"]),
    # Opening a Lisp source file engages lisp-mode via auto-mode-alist
    # and font-lock paints the visible text: keywords, strings, comments
    # and names must carry GNU's colors on the glass.
    ("fontify-elisp", ELISP_SAMPLE, [b"\x0e\x0e"], ".el"),
    # Editing refontifies: breaking a string open recolors the tail of
    # the buffer through jit-lock's after-change machinery.
    ("fontify-edit", ELISP_SAMPLE, [b"\x0e\x0e\x0e", b"\x05", b" ;; tail comment"], ".el"),
    ("multiline-and-motion", "", [b"first line\rsecond line\rthird line", b"\x10\x10", b"\x01", b"X"]),
    ("open-existing-and-edit", "alpha\nbeta\ngamma\n", [b"\x0e\x0e", b"\x05", b" tail"]),
    ("kill-line-and-undo", "one\ntwo\nthree\n", [b"\x0b", b"\x1f"]),
    # A run of self-inserts amalgamates into shared undo groups
    # (simple.el's undo-auto machinery, driven from the command loop and
    # cmds.c's amalgamation calls): one undo removes the whole tail
    # group, not one character.
    ("undo-amalgamation", "", [b"abcdefghijklmnopqrstuvwxy", b"\x1f"]),
    # truncate-lines: the too-wide line ends in the `$' truncation glyph;
    # with point at its end, auto-hscroll (xdisp.c hscroll_window_tree)
    # scrolls the window so point is visible, marking every line's
    # scrolled-off text with `$' in column zero.
    ("truncate-long", WIDE_SAMPLE, [b"\x1b:(setq truncate-lines t)\r", b"\x05"], ".dat"),
    # Point moving back to a short column releases the auto-hscroll: the
    # recomputed hscroll returns to zero and the plain truncated view.
    (
        "truncate-motion",
        WIDE_SAMPLE,
        [b"\x1b:(setq truncate-lines t)\r", b"\x05", b"\x0e", b"\x01"],
        ".dat",
    ),
    # C-x < invokes scroll-left, a command bindings.el marks disabled:
    # novice.el's disabled-command-function shows its help window through
    # read-multiple-choice, and the two-line prompt grows the mini window
    # (GNU's resize_mini_window, grow-only), wrapping with `\\'.
    ("hscroll-disabled", WIDE_SAMPLE, [b"\x1b:(setq truncate-lines t)\r", b"\x18<"], ".dat"),
    # Explicit scroll-left hscrolls the window and suspends auto-hscroll:
    # `$' at both edges of the wide line, a lone `$' on the lines
    # scrolled entirely off.
    (
        "hscroll-explicit",
        WIDE_SAMPLE,
        [b"\x1b:(setq truncate-lines t)\r", b"\x1b:(scroll-left 20)\r"],
        ".dat",
    ),
    # header-line-format carves the window's first row: the header text
    # in the header-line face, the body shifted down one row.
    (
        "header-line",
        "alpha\nbeta\ngamma\n",
        [b'\x1b:(setq header-line-format "HEADER text here")\r', b"\x0e\x0e"],
        ".dat",
    ),
    # The header renders through the mode-line machinery: %-constructs
    # (%b, %l) expand in the window's own buffer context.
    (
        "header-line-percent",
        "alpha\nbeta\ngamma\n",
        [b'\x1b:(setq header-line-format "buf %b line %l")\r', b"\x0e"],
        ".dat",
    ),
    # Subprocess output reaches the glass between keystrokes: the command
    # loop's wait pumps process output through filters and sentinels
    # (wait_reading_process_output), the popped window shows the output,
    # and after exit the mode line's %s construct answers "no process".
    ("async-shell", "sample text\n", [b"\x1b&echo hi\r", b"\x0c"]),
    # M-x shell end to end: comint spawns $SHELL on a pty, the prompt
    # arrives propertized (comint-highlight-prompt via the font-lock-face
    # alias), typed input echoes bold, RET sends the line, and the
    # command's output plus the next prompt land like GNU paints them --
    # mode-line process status, In/Out and Signals menus included.
    ("mx-shell", "sample text\n", [b"\x1bxshell\r", b"echo hi\r", b"\x0c"]),
    # A raw make-process with the default filter: output inserts at the
    # process mark in the named buffer while the loop waits.
    (
        "make-process",
        "sample text\n",
        [
            b"\x1b:(make-process :name \"p\" :command (list \"echo\" \"hi\") :buffer \"*out*\")\r",
            b"\x18b*out*\r",
        ],
    ),
    ("delete-and-backspace", "abcdef\n", [b"\x06\x06", b"\x04", b"\x7f\x7f"]),
    # A logical line wider than the window: GNU wraps it onto continuation
    # rows (with a trailing "\" marker); motion afterwards must land on the
    # same visual row/column.
    (
        "long-line-wrap",
        "short before\n",
        [bytes("wide" * 50, "ascii"), b"\x01", b"\x06" * 5],
    ),
    # Enough lines to push point past the window bottom: both editors must
    # pick the same new window start when they scroll.
    (
        "scroll-through-file",
        "".join(f"line {n:03}\n" for n in range(60)),
        [b"\x0e" * 30, b"X"],
    ),
    # Universal argument: C-u multiplies self-insert and motion counts.
    (
        "prefix-arguments",
        "abcdefghijklmnop\n",
        [b"\x15x", b"\x15" + b"8" + b"y", b"\x01", b"\x15\x06", b"Z"],
    ),
    # M-x round trip through the minibuffer.
    (
        "m-x-round-trip",
        "abcdef\n",
        [b"\x1bxforward-char\r", b"Q"],
    ),
    # Scrolling back above the window top recenters upward, clamped at
    # the first line.
    (
        "scroll-back-up",
        "".join(f"line {n:03}\n" for n in range(60)),
        [b"\x0e" * 30, b"\x10" * 25, b"X"],
    ),
    # A jump straight to the end of the buffer picks the same window
    # start as GNU's recentering.
    (
        "jump-to-end",
        "".join(f"line {n:03}\n" for n in range(60)),
        [b"\x1b>", b"END"],
    ),
    # next-line moves by visual rows on a wrapped line (line-move-visual).
    (
        "wrapped-line-vertical-motion",
        "top line\n" + "wide" * 50 + "\nbottom line\n",
        [b"\x0e" * 2, b"\x06" * 3, b"\x0e", b"*"],
    ),
    # C-v pages a near-full window forward; the typed X pins point.
    (
        "page-down",
        "".join(f"line {n:03}\n" for n in range(60)),
        [b"\x16", b"\x16", b"X"],
    ),
    # M-v pages back after paging forward.
    (
        "page-up-after-down",
        "".join(f"line {n:03}\n" for n in range(60)),
        [b"\x16", b"\x16", b"\x1bv", b"Y"],
    ),
    # Paging past the end signals; the screen keeps its last good state.
    (
        "page-past-end",
        "".join(f"line {n:03}\n" for n in range(30)),
        [b"\x16", b"\x16", b"Z"],
    ),
    # C-u 4 C-v scrolls exactly four lines.
    (
        "page-by-arg",
        "".join(f"line {n:03}\n" for n in range(60)),
        [b"\x15" + b"4" + b"\x16", b"X"],
    ),
    # C-l cycles middle, top, bottom across consecutive presses.
    (
        "recenter-cycle-top",
        "".join(f"line {n:03}\n" for n in range(60)),
        [b"\x0e" * 30, b"\x0c", b"\x0c", b"X"],
    ),
    (
        "recenter-cycle-bottom",
        "".join(f"line {n:03}\n" for n in range(60)),
        [b"\x0e" * 30, b"\x0c", b"\x0c", b"\x0c", b"Y"],
    ),
    # Paging steps whole screen lines over wrapped text.
    (
        "page-down-wrapped",
        ("wide" * 30 + "\n") * 12,
        [b"\x16", b"*"],
    ),
    # C-g quits with its echo message.
    (
        "quit-key",
        "alpha\nbeta\n",
        [b"\x06", b"\x07"],
    ),
    # M-: shows the value with eval-expression-print-format in the echo.
    (
        "eval-expression",
        "alpha\nbeta\n",
        [b"\x1b:", b"(+ 1 2)", b"\r"],
    ),
    # Kill a region and yank it back: C-SPC, C-k lines, C-y.
    (
        "kill-yank",
        "alpha one\nbeta two\ngamma three\ndelta four\n",
        [b"\x00", b"\x0e\x0e", b"\x17", b"\x1b>", b"\x19"],
    ),
    # Copy with M-w, move, yank: the region stays put, the copy lands.
    (
        "copy-yank",
        "alpha one\nbeta two\ngamma three\n",
        [b"\x00", b"\x0e", b"\x05", b"\x1bw", b"\x1b>", b"\x19"],
    ),
    # C-x C-x swaps point and mark and reactivates the region.
    (
        "exchange-point-mark",
        "alpha one\nbeta two\ngamma three\n",
        [b"\x0e", b"\x00", b"\x0e", b"\x06\x06", b"\x18\x18"],
    ),
    # An unbound key reports itself in the echo area.
    (
        "undefined-key",
        "alpha\nbeta\n",
        [b"\x18", b"j"],
    ),
    # Motion at the buffer edge signals, and the error message shows.
    (
        "edge-error-echo",
        "alpha\nbeta\n",
        [b"\x10"],
    ),
    # A command message clears when the next command runs.
    (
        "message-then-motion",
        "alpha\nbeta\ngamma\n",
        [b"\x1b>", b"\x10"],
    ),
    # M-x completes a unique command prefix with TAB.
    (
        "mx-tab-completion",
        "alpha\n",
        [b"\x1bxforward-ch\t\r", b"X"],
    ),
    # M-p in M-x recalls the previously executed command.
    (
        "mx-history-recall",
        "alpha\n",
        [b"\x1bxforward-char\r", b"\x1bx\x1bp\r", b"Z"],
    ),
    # C-x C-f opens a typed absolute path.
    (
        "find-file-typed",
        "original\n",
        [b"\x18\x06", FIXTURE_PATH.encode() + b"\r"],
    ),
    # TAB completes the fixture's file name in the C-x C-f prompt.
    (
        "find-file-tab",
        "original\n",
        [b"\x18\x06", FIXTURE_PATH[:-7].encode() + b"\t\r"],
    ),
    # The C-x C-f prompt itself: GNU preloads the default directory.
    (
        "find-file-prompt",
        "original\n",
        [b"\x18\x06"],
    ),
    # C-s live search: the echo shows the accumulating search string.
    (
        "isearch-enter",
        "alpha one\nbeta word two\ngamma word three\n",
        [b"\x13", b"wor"],
    ),
    # RET exits the search at the match end.
    (
        "isearch-exit-point",
        "alpha one\nbeta word two\ngamma word three\n",
        [b"\x13", b"wor", b"\r", b"X"],
    ),
    # C-s repeats to the next match.
    (
        "isearch-repeat",
        "alpha one\nbeta word two\ngamma word three\n",
        [b"\x13", b"wor", b"\x13", b"\r", b"Y"],
    ),
    # A failing search reports itself and leaves point at the origin.
    (
        "isearch-fail",
        "alpha one\nbeta word two\ngamma word three\n",
        [b"\x13", b"zzq"],
    ),
    # Repeating past the last match fails, and one more C-s wraps.
    (
        "isearch-wrap",
        "alpha one\nbeta word two\ngamma word three\n",
        [b"\x13", b"wor", b"\x13", b"\x13", b"\x13"],
    ),
    # C-g during a successful search cancels back to the origin.
    (
        "isearch-cancel",
        "alpha one\nbeta word two\ngamma word three\n",
        [b"\x13", b"wor", b"\x07", b"Z"],
    ),
    # C-r searches backward from the end of the buffer.
    (
        "isearch-backward",
        "alpha one\nbeta word two\ngamma word three\n",
        [b"\x1b>", b"\x12", b"wor", b"\r", b"B"],
    ),
    # A key outside the search map exits isearch and then runs.
    (
        "isearch-other-key-exit",
        "alpha one\nbeta word two\ngamma word three\n",
        [b"\x13", b"wor", b"\x01", b"Q"],
    ),
    # DEL edits the search string.
    (
        "isearch-del-edits",
        "alpha one\nbeta word two\ngamma word three\n",
        [b"\x13", b"worz", b"\x7f", b"\r", b"D"],
    ),
    # A match beyond the window scrolls it into view.
    (
        "isearch-scroll",
        "".join(f"line {n:03}\n" for n in range(50)) + "needle here\n",
        [b"\x13", b"needle", b"\r", b"N"],
    ),
    # C-x 2: two stacked windows on the same buffer, a mode line each
    # (the root's 23 lines split 12/11, upper keeps the extra row).
    (
        "split-below-two-windows",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x182"],
    ),
    # Motion after C-x 2 moves point in the upper (selected) window only;
    # the two mode lines disagree on L.
    (
        "split-below-motion",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x182", b"\x0e\x0e\x0e"],
    ),
    # C-x o selects the lower window; motion there leaves the upper
    # window's point alone.
    (
        "other-window-motion",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x182", b"\x18o", b"\x0e" * 5],
    ),
    # C-x 3: side-by-side windows with the vertical border column and
    # per-window mode lines truncated to each body width.
    (
        "split-right-vertical",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x183"],
    ),
    # Windows narrower than truncate-partial-width-windows truncate long
    # lines with the `$' marker instead of wrapping them.
    (
        "split-right-truncated",
        "short one\n" + "W" * 100 + "\n" + "".join(f"line {n:02} alpha\n" for n in range(3, 40)),
        [b"\x183"],
    ),
    # C-x o then motion in the right-hand window.
    (
        "split-right-other",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x183", b"\x18o", b"\x0e\x0e"],
    ),
    # C-x 0 gives the deleted window's rows back to its sibling.
    (
        "delete-window-restores",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x182", b"\x180"],
    ),
    # C-x 1 from the lower window makes it fill the frame.
    (
        "delete-other-windows",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x182", b"\x18o", b"\x181"],
    ),
    # C-x 2 then C-x 3 splits only the upper window.
    (
        "three-way-split",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x182", b"\x183"],
    ),
    # C-x o cycles in tree order: upper-left, upper-right, bottom.
    (
        "three-way-cycle",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x182", b"\x183", b"\x18o", b"X"],
    ),
    # C-v in the lower window scrolls it by its own page size; the upper
    # window must not move.
    (
        "split-scroll-independent",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x182", b"\x18o", b"\x16"],
    ),
    # C-v in the upper window: page size follows its 11 text rows.
    (
        "split-scroll-upper",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x182", b"\x16"],
    ),
    # M-> in the lower window recenters around the buffer end there.
    (
        "split-jump-end",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x182", b"\x18o", b"\x1b>"],
    ),
    # An ambiguous TAB in the C-x C-f prompt pops *Completions* at the
    # frame bottom, sized to its candidate list.
    (
        "completions-pop-up",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x18\x06", COMPLETIONS_DIR_NAME.encode() + b"/am", b"\t", b"\t"],
    ),
    # Finishing the file name removes the *Completions* window again.
    (
        "completions-dismiss",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 61)),
        [b"\x18\x06", COMPLETIONS_DIR_NAME.encode() + b"/am", b"\t", b"\t", b"1.dat\r"],
    ),
    # C-SPC then motion: the active region shows in the region face.
    (
        "region-highlight",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 30)),
        [b"\x00", b"\x0e\x0e", b"\x06\x06\x06"],
    ),
    # A region marked backward (point before mark) highlights the same.
    (
        "region-backward",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 30)),
        [b"\x0e\x0e\x0e", b"\x00", b"\x10\x10"],
    ),
    # C-g deactivates the mark and the highlight disappears.
    (
        "region-deactivate",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 30)),
        [b"\x00", b"\x0e\x0e", b"\x07", b"\x0e"],
    ),
    # C-x C-x re-activates the region and swaps point with mark.
    (
        "exchange-point-mark",
        "alpha one\nbeta two\ngamma three\n",
        [b"\x00", b"\x0e\x0e", b"\x18\x18"],
    ),
    # M-y replaces the just-yanked text with the previous kill.
    (
        "yank-pop",
        "alpha one\nbeta two\ngamma three\n",
        [b"\x0b", b"\x0e", b"\x0b", b"\x1b>", b"\x19", b"\x1by"],
    ),
    # The file-name prompt shadows the ignored prefix; a tty without a
    # displayable shadow face brackets it instead (rfn-eshadow).
    (
        "filename-shadow",
        "fixture\n",
        [b"\x18\x06", b"/etc", b"\x07"],
    ),
    # TAB with no completion shows minibuffer-message's transient.
    (
        "completion-no-match",
        "fixture\n",
        [b"\x18\x06", b"/nonexistent-zz", b"\t"],
    ),
    # F10 drops the File menu; down-arrow moves the selection.
    (
        "f10-open",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b[21~", b"\x1b[B"],
    ),
    # Right-arrow closes File and opens Edit at its bar column.
    (
        "f10-cycle",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b[21~", b"\x1b[C"],
    ),
    # RET on "Visit New File..." runs find-file: the prompt appears.
    (
        "f10-select",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b[21~", b"\x1b[B", b"\r"],
    ),
    # C-g dismisses the menu and restores the glass behind it.
    (
        "f10-dismiss",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b[21~", b"\x07", b"\x0e"],
    ),
    # RET on Edit's "Search >" descends into the submenu: popup-menu's
    # loop reopens x-popup-menu with the sub-keymap at the same spot.
    (
        "submenu-open",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b[21~", b"\x1b[C"] + [b"\x1b[B"] * 9 + [b"\r"],
    ),
    # Selecting "String Backwards..." runs an `(interactive "s...")'
    # command: the prompt reads through the real minibuffer and the bar
    # gains its Minibuf entry.
    (
        "submenu-string-search",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b[21~", b"\x1b[C"] + [b"\x1b[B"] * 9 + [b"\r", b"\x1b[B", b"\r"],
    ),
    # Right-arrow inside a submenu cycles to the next menu-bar menu;
    # Options draws its checkboxes ([X] Blink Cursor needs the delayed
    # Custom replay to run in interactive session mode).
    (
        "submenu-cycle-options",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b[21~", b"\x1b[C"] + [b"\x1b[B"] * 9 + [b"\r", b"\x1b[C"],
    ),
    # Two levels down: Tools, then its Shell Commands submenu.
    (
        "submenu-nested",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b[21~"] + [b"\x1b[C"] * 4 + [b"\x1b[B"] * 2 + [b"\r"],
    ),
    # The File pane is taller than the glass: Up at the top wraps to the
    # menu's last window with the final item selected (MI_SCROLL_BACK).
    (
        "menu-scroll-wrap-up",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b[21~", b"\x1b[A"],
    ),
    # Down past the last visible row advances the window one item at a
    # time (MI_SCROLL_FORWARD) while the selection rides the bottom row.
    (
        "menu-scroll-forward",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b[21~"] + [b"\x1b[B"] * 24,
    ),
    # M-` runs the real tmm.el: split window, shortcut-lettered
    # *Completions*, and the Menu bar prompt.
    (
        "tmm-open",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b`"],
    ),
    # A shortcut letter descends into that menu's own tmm level, key
    # hints and :enable states computed after the first level tore down.
    (
        "tmm-pick-file",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b`", b"f"],
    ),
    # C-g aborts: the split heals and the pre-read window layout returns.
    (
        "tmm-cancel",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1b`", b"\x07"],
    ),
    # SGR mouse press+release on the menu bar (xterm-mouse-mode decodes
    # in GNU; the frontend's terminal layer cooks the same events):
    # [menu-bar mouse-1] finds menu-bar-open-mouse, Edit's pane drops.
    (
        "mouse-bar-click",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1bxxterm-mouse-mode\r", b"\x1b[<0;6;1M", b"\x1b[<0;6;1m"],
    ),
    # The clicked menu dismisses with C-g and the glass heals.
    (
        "mouse-bar-click-dismiss",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1bxxterm-mouse-mode\r", b"\x1b[<0;6;1M", b"\x1b[<0;6;1m", b"\x07", b"\x0e"],
    ),
    # C-mouse-3's menu-item filter yields the menu-bar keymap; a keymap
    # bound to a click pops up at the click point with the pending-keys
    # echo ("C-down-mouse-3- (C-h for help)") under it.
    (
        "mouse-cmenu",
        "".join(f"line {n:02} alpha beta gamma\n" for n in range(1, 25)),
        [b"\x1bxxterm-mouse-mode\r", b"\x1b[<18;10;5M", b"\x1b[<18;10;5m"],
    ),
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

    with open(FIXTURE_PATH, "w") as fixture:
        fixture.write("fixture line one\nfixture line two\n")
    os.makedirs(COMPLETIONS_DIR, exist_ok=True)
    for entry in os.listdir(COMPLETIONS_DIR):
        os.unlink(os.path.join(COMPLETIONS_DIR, entry))
    for name in ("ambig1.dat", "ambig2.dat"):
        with open(os.path.join(COMPLETIONS_DIR, name), "w"):
            pass
    failures = 0
    for entry in SCENARIOS:
        name, contents, keys = entry[0], entry[1], entry[2]
        # A scenario may carry a file suffix; `.el' engages lisp-mode and
        # font-lock through the ordinary auto-mode-alist path.
        suffix = entry[3] if len(entry) > 3 else ".dat"
        handle, path = tempfile.mkstemp(suffix=suffix, prefix=f"ttydiff-{name}-")
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
