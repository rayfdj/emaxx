#!/usr/bin/env python3
"""Differential terminal test: emaxx vs GNU `emacs -nw'.

Runs the same scripted keystrokes through both editors in pseudo-terminals,
decodes each output stream with a small VT100 interpreter into a character
grid, and compares the grids region by region:

  text area   rows before the mode line, after skipping GNU's menu-bar row
  mode line   exact characters and padding
  echo area   the final row

The contract is identical buffer content, cursor row/column, scrolling,
mode-line rendering, and echo-area rendering.  Named ``Action`` entries are
compared after every complete command; legacy byte chunks retain their
historical final-screen-only behavior.

Usage:
    tools/ttydiff.py EMAXX_BINARY GNU_BINARY GNU_LISP_DIR [SCENARIO...]

With no SCENARIO arguments all built-in scenarios run.  Otherwise each
argument names one built-in scenario to run.  Exits nonzero on any screen
divergence; missing binaries skip with exit 0 so unconfigured environments
stay green.

For reproducible generated journeys and delta-debugged failures, see
``tools/ttydiff_explore.py``.
"""

import codecs
import json
import os
import pty
import random
import select
import shutil
import struct
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import fcntl
import termios

ROWS, COLS = 24, 80
STARTUP_WAIT_SECONDS = 120.0
FIXTURE_PATH = "/tmp/emaxxff-fixture.dat"
# A directory whose listing both editors complete over: two names sharing
# the ambiguous prefix the *Completions* scenarios TAB on.
COMPLETIONS_DIR_NAME = "emaxxffcomp"
COMPLETIONS_DIR = f"/tmp/{COMPLETIONS_DIR_NAME}"
FIELDNOTES_FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "fieldnotes.org"


# (fg, bg, bold, underline, reverse): fg/bg are ANSI indexes or None for
# the terminal default.  Erased cells always carry DEFAULT_ATTR — only
# explicitly painted cells hold face attributes, on both editors alike.
DEFAULT_ATTR = (None, None, False, False, False)


@dataclass(frozen=True)
class Action:
    """One complete user command in a differential editing journey.

    Named actions make a checkpoint failure reproducible without treating
    arbitrary bytes inside a multi-key command as stable UI states.  Legacy
    scenarios may continue to use raw ``bytes`` chunks and are compared only
    at their final screen.
    """

    name: str
    keys: bytes
    checkpoint: bool = True
    settle: Optional[float] = None
    quiet: Optional[float] = None


def action(name, keys, *, checkpoint=True, settle=None, quiet=None):
    """Short spelling for declarative scenario entries."""
    return Action(name, keys, checkpoint, settle, quiet)


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
        env = terminal_environment(env_extra)
        pid, fd = pty.fork()
        if pid == 0:
            os.environ.clear()
            os.environ.update(env)
            os.execv(argv[0], argv)
        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, COLS, 0, 0))
        self.pid, self.fd = pid, fd
        self.screen = Vt100Screen()

    def drain(self, timeout, quiet=0.2, minimum=0.0):
        started = time.time()
        deadline = started + timeout
        quiet_deadline = started + quiet
        not_before = started + minimum
        while True:
            now = time.time()
            remaining = deadline - now
            quiet_remaining = quiet_deadline - now
            minimum_remaining = not_before - now
            if remaining <= 0 or (quiet_remaining <= 0 and minimum_remaining <= 0):
                return
            wait = min(remaining, max(quiet_remaining, minimum_remaining))
            ready, _, _ = select.select([self.fd], [], [], wait)
            if not ready:
                # A quiet interval is only stable after the editor has had a
                # minimum command-dispatch window.  Without that lower bound,
                # a busy GNU process can produce no bytes for 200 ms and be
                # snapshotted before it has consumed the key at all.
                continue
            try:
                chunk = os.read(self.fd, 65536)
            except OSError:
                return
            if not chunk:
                return
            self.screen.feed(chunk)
            quiet_deadline = time.time() + quiet

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

    def wait_for_screen_text(self, text, timeout, minimum=0.5):
        """Wait until startup has displayed the requested visited target.

        Taking the alternate screen is earlier than completing command-line
        file visitation.  In particular, a cold mode load can go quiet long
        enough to look settled while startup still owns the command loop.
        Do not send the first test command until the target is visibly live.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(text in line for line in self.screen.lines()):
                self.drain(max(3.0, minimum), quiet=0.5, minimum=minimum)
                return
            ready, _, _ = select.select([self.fd], [], [], 0.25)
            if not ready:
                continue
            try:
                chunk = os.read(self.fd, 65536)
            except OSError as error:
                raise RuntimeError(
                    f"editor terminal closed before displaying {text!r}"
                ) from error
            if not chunk:
                break
            self.screen.feed(chunk)
        visible = [line for line in self.screen.lines() if line]
        raise RuntimeError(
            f"editor did not display startup target {text!r}; "
            f"visible screen: {visible!r}"
        )

    def send(self, data, settle=1.0, quiet=0.2, explicit_settle=False):
        os.write(self.fd, data)
        self.drain(
            settle,
            quiet,
            minimum=command_dispatch_minimum(data, settle, explicit_settle),
        )

    def close(self):
        try:
            os.kill(self.pid, 9)
        except ProcessLookupError:
            pass
        deadline = time.time() + 1.0
        while time.time() < deadline:
            try:
                child, _ = os.waitpid(self.pid, os.WNOHANG)
            except ChildProcessError:
                break
            if child == self.pid:
                break
            time.sleep(0.01)
        try:
            os.close(self.fd)
        except OSError:
            pass


def terminal_environment(env_extra):
    """Build the deterministic 8-color xterm environment under test."""
    env = dict(os.environ)
    env.update(env_extra)
    env["TERM"] = "xterm"
    # GNU consults COLORTERM even when TERM names an 8-color terminal and
    # emits 24-bit SGR in a truecolor parent shell.  Emaxx intentionally
    # models TERM's terminfo class, and the VT decoder's face contract is
    # likewise ANSI-indexed.  Do not let the invoking terminal silently
    # change the oracle both editors are meant to share.
    for name in ("COLORTERM", "TERM_PROGRAM", "COLORFGBG"):
        env.pop(name, None)
    return env


def command_dispatch_minimum(data, settle, explicit_settle=False):
    """Lower bound for consuming a complete terminal command.

    Emaxx dispatches terminal events through its command loop one by one.
    A long minibuffer expression can therefore finish painting its text and
    go quiet before the trailing RET has executed.  Scale the readiness floor
    with the event count, without exceeding the action's declared timeout.
    An explicitly timed action declares that its entire settle window is a
    required dispatch floor, rather than merely a maximum drain deadline.
    """
    if explicit_settle:
        return settle
    return min(settle, max(0.35, len(data) * 0.05))


def gnu_no_window_setup(lisp_dir):
    """Normalize an NS-built GNU oracle to Emaxx's no-window-system model."""
    menu_bar = json.dumps(os.path.abspath(os.path.join(lisp_dir, "menu-bar.el")))
    return (
        "(progn "
        "(setq features (delq 'ns features)) "
        "(define-key global-map [?\\s-c] nil) "
        "(define-key global-map [?\\s-u] nil) "
        "(makunbound 'menu-bar-edit-menu) "
        f"(load {menu_bar} nil t t))"
    )


def find_mode_line(lines):
    """Both editors draw a dash-heavy mode line above the echo area."""
    for index in range(len(lines) - 1, -1, -1):
        if lines[index].count("-") >= 8 and "(" in lines[index]:
            return index
    return len(lines) - 2


def screen_divergences(gnu_screen, emaxx_screen):
    """Return all observable terminal differences between two snapshots."""
    gnu_lines = gnu_screen.lines()
    emaxx_lines = emaxx_screen.lines()
    gnu_attrs = gnu_screen.attr_rows()
    emaxx_attrs = emaxx_screen.attr_rows()

    gnu_mode = find_mode_line(gnu_lines)
    emaxx_mode = find_mode_line(emaxx_lines)
    # Both editors show the default menu bar on row 0 and work a
    # 21-row text window under it; every row -- menu captions and
    # scroll positions included -- must agree exactly.
    gnu_text = gnu_lines[0:gnu_mode]
    emaxx_text = emaxx_lines[0:emaxx_mode]
    length = max(len(gnu_text), len(emaxx_text))
    gnu_text += [""] * (length - len(gnu_text))
    emaxx_text += [""] * (length - len(emaxx_text))

    divergences = []
    for offset, (expected, actual) in enumerate(zip(gnu_text, emaxx_text)):
        if expected != actual:
            divergences.append((offset, expected, actual))
        elif gnu_attrs[offset] != emaxx_attrs[offset]:
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
    elif gnu_attrs[gnu_mode] != emaxx_attrs[emaxx_mode]:
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
    elif gnu_attrs[-1] != emaxx_attrs[-1]:
        divergences.append(
            (
                "echo (attrs)",
                describe_attr_row(gnu_attrs[-1]),
                describe_attr_row(emaxx_attrs[-1]),
            )
        )
    # Cursor placement is observable terminal state too.  A screen can
    # have identical cells while point, minibuffer input, or redisplay
    # leaves the hardware cursor on a different row or column.
    gnu_cursor = (gnu_screen.row, gnu_screen.col)
    emaxx_cursor = (emaxx_screen.row, emaxx_screen.col)
    if gnu_cursor != emaxx_cursor:
        divergences.append(
            (
                "cursor",
                f"{gnu_cursor} on {gnu_lines[gnu_cursor[0]]!r}",
                f"{emaxx_cursor} on {emaxx_lines[emaxx_cursor[0]]!r}",
            )
        )
    return divergences, length


def report_comparison(label, gnu_screen, emaxx_screen):
    """Compare and print one named checkpoint; return whether it matched."""
    divergences, length = screen_divergences(gnu_screen, emaxx_screen)
    if divergences:
        print(f"DIVERGE [{label}]: {len(divergences)} terminal difference(s)")
        for offset, expected, actual in divergences[:8]:
            print(f"  row {offset}:")
            print(f"    gnu  : {expected!r}")
            print(f"    emaxx: {actual!r}")
        return False
    print(f"MATCH [{label}]: text area identical ({length} rows)")
    return True


def normalize_action(item, index):
    """Accept old byte chunks alongside named checkpoint actions."""
    if isinstance(item, Action):
        return item
    if not isinstance(item, bytes):
        raise TypeError(f"action {index + 1} must be bytes or Action, got {type(item)!r}")
    return Action(f"step-{index + 1}", item, checkpoint=False)


def action_timing(scenario, index, final, command):
    """Choose deterministic redraw quiescence for one command."""
    if command.settle is not None or command.quiet is not None:
        return command.settle or 1.0, command.quiet or 0.2
    if scenario == "completions-pop-up" and final:
        return 4.0, 2.5
    if scenario == "completions-dismiss" and final:
        return 3.0, 1.0
    if scenario in {"grep-null", "grep-next-error"} and index >= 1:
        # The grep child can have a quiet process-startup interval longer
        # than the ordinary key-settle window.  Wait through its launch and
        # later navigation so the sentinel's summary and parsed hits exist.
        return 4.0, 2.5
    if scenario.startswith("compile-") and index == 3:
        # Command submission, process output, and the compilation sentinel
        # are separate events.  Wait for the sentinel before the following
        # C-l so it clears the same completed message in both editors.
        return 4.0, 1.5
    if scenario == "org-fold-motion" and final:
        # A cold Org redisplay can pause after accepting the final
        # self-insert but before painting it.  A 200 ms quiet window
        # occasionally captured GNU's preceding C-n frame instead.
        return 3.0, 1.0
    if scenario == "mx-shell":
        # Shell startup, the pty echo, command output, and the next prompt
        # are separate process events.  Wait for the initial prompt before
        # typing too, and do not let a gap between later events masquerade
        # as a stable final screen.
        return 4.0, 1.5
    if not command.checkpoint and final:
        # Legacy byte chunks compare only after their final chunk.  Give that
        # final complete gesture a real dispatch floor so a busy editor cannot
        # be snapshotted at an intermediate minibuffer or prefix-key screen.
        return 3.0, 0.5
    return 1.0, 0.2


def compare(scenario, keys, gnu_argv, emaxx_argv, gnu_env, emaxx_env, boot_wait):
    gnu = Session(gnu_argv, gnu_env)
    emaxx = Session(emaxx_argv, emaxx_env)
    try:
        gnu.wait_boot(boot_wait)
        emaxx.wait_boot(boot_wait)
        # A target basename is rendered in the mode line only after startup
        # has visited it and established its major mode.  A prefix survives
        # mode-line truncation while remaining unique to this disposable
        # scenario target.
        gnu_target = gnu_argv[-1]
        emaxx_target = emaxx_argv[-1]
        gnu.wait_for_screen_text(
            os.path.basename(gnu_target)[:16],
            boot_wait,
            minimum=2.0 if os.path.isdir(gnu_target) else 0.5,
        )
        emaxx.wait_for_screen_text(
            os.path.basename(emaxx_target)[:16],
            boot_wait,
            minimum=2.0 if os.path.isdir(emaxx_target) else 0.5,
        )
        # Keep multi-key gestures close together: mouse press/release pairs
        # must stay a click, and the second completion TAB must reach the
        # first TAB's `sit-for'.  Only the final action needs a long quiet
        # window: popup capture waits past minibuffer-message's two-second
        # transient, while dismissal waits for its post-RET frame redraw.
        commands = [normalize_action(item, index) for index, item in enumerate(keys)]
        final_label = scenario
        for index, command in enumerate(commands):
            final = index + 1 == len(keys)
            settle, quiet = action_timing(scenario, index, final, command)
            explicit_settle = command.settle is not None or (
                not command.checkpoint and final
            )
            gnu.send(
                command.keys,
                settle=settle,
                quiet=quiet,
                explicit_settle=explicit_settle,
            )
            emaxx.send(
                command.keys,
                settle=settle,
                quiet=quiet,
                explicit_settle=explicit_settle,
            )
            if command.checkpoint:
                checkpoint_label = f"{scenario}::{index + 1}:{command.name}"
                if final:
                    # The final drain below is part of this command's
                    # readiness contract; report it once, with its name.
                    final_label = checkpoint_label
                elif not report_comparison(checkpoint_label, gnu.screen, emaxx.screen):
                    return False
        gnu.drain(1.0)
        emaxx.drain(1.0)
        return report_comparison(final_label, gnu.screen, emaxx.screen)
    finally:
        gnu.close()
        emaxx.close()


WIDE_SAMPLE = "left-margin " + "wide" * 40 + " right-end\nsecond line\nthird line\n"

SEARCH_SAMPLE = "alpha beta gamma\nbeta delta beta\ngamma alpha beta\nlast line here\n"

# The checked-in real-usage Org fixture: #+STARTUP folding,
# sections long enough that raw and display row counts disagree, tables,
# source blocks, tagged headlines, and TODO/DONE faces.
FOLD_SAMPLE = FIELDNOTES_FIXTURE_PATH.read_text(encoding="utf-8")

FIELDNOTES_SCENARIO_NAMES = (
    "org-overview-open",
    "org-backtab-cycle",
    "org-tab-children",
    "org-done-face",
    "org-occur-wraps",
)

# Compilation timestamps can never agree between two processes; both
# editors run the same defaliases, so the pinned text compares strictly.
# suggest-key-bindings goes off because its 2-second suggestion timer
# races the capture window.
TIME_PIN = (
    b"\x1b:(progn (defalias (quote current-time-string) (lambda (&rest _) "
    b"\"Mon Jan  1 00:00:00 2026\")) (defalias (quote float-time) "
    b"(lambda (&rest _) 0.0)) (setq suggest-key-bindings nil))\r"
)

ORG_SAMPLE = """* Head one
body line one
body line two
** Sub head
sub body
* Head two
body b
"""

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

CORE_EDIT_SAMPLE = """alpha beta gamma delta
second line has several words
third line is here

This paragraph is deliberately long enough that filling it at the ordinary
seventy-column boundary changes its visible shape in a deterministic way.

last paragraph ends here
"""

# A conservative grammar for deterministic stateful exploration.  Every item
# is one complete command, touches only the disposable scenario buffer, and
# cannot launch a subprocess or visit an external path.  Repetition in this
# tuple is intentional weighting toward the commands used most often during
# ordinary editing.
SAFE_EDIT_ACTIONS = (
    ("forward-char", b"\x06"),
    ("forward-char", b"\x06"),
    ("backward-char", b"\x02"),
    ("backward-char", b"\x02"),
    ("next-line", b"\x0e"),
    ("next-line", b"\x0e"),
    ("previous-line", b"\x10"),
    ("beginning-of-line", b"\x01"),
    ("end-of-line", b"\x05"),
    ("forward-word", b"\x1bf"),
    ("backward-word", b"\x1bb"),
    ("self-insert-x", b"x"),
    ("self-insert-space", b" "),
    ("delete-char", b"\x04"),
    ("backward-delete-char", b"\x7f"),
    ("open-line", b"\x0f"),
    ("transpose-chars", b"\x14"),
    ("kill-line", b"\x0b"),
    ("yank", b"\x19"),
    ("undo", b"\x1f"),
    ("set-mark", b"\x00"),
    ("exchange-point-and-mark", b"\x18\x18"),
)

SEEDED_SAFE_RUNS = ((17, 14), (2309, 18), (7595, 22))


def seeded_safe_actions(seed, steps):
    """Generate a reproducible weighted sequence of safe editor commands."""
    generator = random.Random(seed)
    commands = []
    for index in range(steps):
        name, keys = generator.choice(SAFE_EDIT_ACTIONS)
        commands.append(action(f"{index + 1:02}-{name}", keys))
    return commands

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
    # Opening an .org file engages org-mode: outline faces (org-level-N)
    # paint the headlines through font-lock.
    ("org-open", ORG_SAMPLE, [b"\x0e"], ".org"),
    # TAB on a headline folds its subtree: org 9.7 hides the body with
    # overlay `invisible' properties, the hidden newlines join onto the
    # headline row, and the display-table ellipsis "..." shows in the
    # headline's face.
    ("org-fold", ORG_SAMPLE, [b"\t"], ".org"),
    # Motion skips folded text: C-n from the folded headline lands on
    # the next visible line (vertical-motion walks display lines), and
    # the typed character lands there.
    ("org-fold-motion", ORG_SAMPLE, [b"\t", b"\x0e", b"x"], ".org"),
    # A second TAB cycles FOLDED -> CHILDREN: sub-headlines reappear
    # with their own ellipses, bodies stay hidden.
    ("org-cycle-children", ORG_SAMPLE, [b"\t", b"\t"], ".org"),
    # M-x org-todo: the TODO keyword lands on the headline, and
    # execute-extended-command's suggestion timer (guarded by
    # real-last-command) offers the C-c C-t binding in the echo area.
    ("org-todo", ORG_SAMPLE, [b"\x1bxorg-todo\r"], ".org"),
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
    # `auto-hscroll-mode' `current-line': only the row showing point
    # hscrolls; the other rows keep their columns (min-hscroll zero).
    (
        "hscroll-current-line",
        WIDE_SAMPLE,
        [b"\x1b:(setq truncate-lines t auto-hscroll-mode (quote current-line))\r", b"\x05"],
        ".dat",
    ),
    # A message wider than the frame grows the mini window immediately
    # and wraps with the `\\' marker; the window tree above shrinks by
    # the same rows (grow_mini_window resizes the real tree).
    (
        "long-message",
        "sample\n",
        [b"\x1b:(message "
         b"\"first part of a very long message that certainly wraps beyond the eighty column frame edge\")\r"],
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
        "page-past-end-preserves-screen",
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
        "exchange-point-mark-motion",
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
        "exchange-point-mark-reactivate",
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
    # display-line-numbers t: the number column's width follows the
    # window (digits of first line + body rows - 1), numbers sit
    # right-justified before a blank separator in the line-number face,
    # and every row at or past ZV keeps a blank prefix in that face.
    (
        "lnum-basic",
        "".join(f"L{n}\n" for n in range(1, 13)),
        [b"\x1b:(setq display-line-numbers t)\r"],
    ),
    # Three-digit numbers at the end of a 200-line buffer; the width
    # grows with the window's first line as M-> scrolls.
    (
        "lnum-3digit",
        "".join(f"line {n}\n" for n in range(1, 201)),
        [b"\x1b:(setq display-line-numbers t)\r", b"\x1b>"],
    ),
    # A wrapped line: continuation rows show a blank prefix, and the
    # text wraps at the narrowed width (the column eats text area).
    ("lnum-wrap", WIDE_SAMPLE, [b"\x1b:(setq display-line-numbers t)\r", b"\x05\x05"]),
    # display-line-numbers-mode, the user-facing wrapper, through M-x.
    (
        "lnum-mode",
        "".join(f"L{n}\n" for n in range(1, 13)),
        [b"\x1bxdisplay-line-numbers-mode\r", b"\x0e\x0e"],
    ),
    # Truncation plus auto-hscroll with the column present: the left `$'
    # replaces the column's first cell, the numbers stay put while the
    # text hscrolls, and the recentering step counts the column's width.
    (
        "lnum-hscroll",
        WIDE_SAMPLE,
        [b"\x1b:(setq truncate-lines t display-line-numbers t)\r", b"\x05"],
    ),
    # `relative': distances from point's line, the absolute number on
    # the current line (display-line-numbers-current-absolute t).
    (
        "lnum-relative",
        "".join(f"L{n}\n" for n in range(1, 13)),
        [b"\x1b:(setq display-line-numbers (quote relative))\r",
         b"\x1b:(forward-line 4)\r"],
    ),
    # `visual': screen-line distances — continuation rows count and get
    # numbers of their own.
    (
        "lnum-visual",
        WIDE_SAMPLE,
        [b"\x1b:(setq display-line-numbers (quote visual))\r",
         b"\x1b:(forward-line 2)\r"],
    ),
    # An explicit display-line-numbers-width wider than needed.
    (
        "lnum-width",
        "".join(f"L{n}\n" for n in range(1, 13)),
        [b"\x1b:(setq display-line-numbers-width 5 display-line-numbers t)\r"],
    ),
    # Screen-line motion under the narrowed text area: C-n through a
    # wrapped line must land on the same visual row and column as GNU's
    # iterator, whose lnum_width is primed before the walk.
    (
        "lnum-motion",
        WIDE_SAMPLE,
        [b"\x1b:(setq display-line-numbers t)\r", b"\x0e", b"\x0e", b"\x06\x06\x06"],
    ),
    # Folded org with line numbers: hidden lines keep their numbers, so
    # the rows after a fold jump exactly as GNU's counter does.
    (
        "lnum-org-fold",
        ORG_SAMPLE,
        [b"\x1b:(setq display-line-numbers t)\r", b"\x1bxorg-cycle\r", b"\x0e\x0e"],
        ".org",
    ),
    # M-x compile end to end: compilation-mode buffer, filter output,
    # sentinel annotations fontified by jit-lock in the non-selected
    # window.  Wall-clock text pins through the Lisp-visible time
    # functions, identically in both editors.
    (
        "compile-run",
        "sample\n",
        [TIME_PIN, b"\x1bxcompile\r", b"\x01\x0b", b"echo hi\r", b"\x0c"],
    ),
    # A file:line: message parsed by compilation-mode: the locus gets the
    # error faces, "finished" its info face, and the mode line the exit
    # status.
    (
        "compile-parse",
        "sample\n",
        [TIME_PIN, b"\x1bxcompile\r", b"\x01\x0b",
         b"echo " + FIXTURE_PATH.encode() + b":2: boom\r", b"\x0c"],
    ),
    # next-error, then C-x ` for the next locus: the source file pops in
    # the other window, the compilation window scrolls to the message,
    # and the echo area names the locus buffer.
    (
        "compile-next-error",
        "sample\n",
        [TIME_PIN, b"\x1bxcompile\r", b"\x01\x0b",
         b"echo " + FIXTURE_PATH.encode() + b":1: a; echo "
         + FIXTURE_PATH.encode() + b":2: b\r",
         b"\x0c", b"\x1bxnext-error\r", b"\x18`"],
    ),
    # M-x grep with the --null separator: the NUL renders as ":" through
    # its `display' string property, and grep's SGR match highlight
    # lands as the match face via replace-match's propertized insert.
    (
        "grep-null",
        "sample\n",
        [TIME_PIN, b"\x1bxgrep\r",
         b"fixture " + FIXTURE_PATH.encode() + b"\r", b"\x0c"],
    ),
    # C-x ` from a grep: grep-mode is a compilation mode, so next-error
    # jumps to the first hit.
    (
        "grep-next-error",
        "sample\n",
        [TIME_PIN, b"\x1bxgrep\r",
         b"fixture " + FIXTURE_PATH.encode() + b"\r", b"\x0c", b"\x18`"],
    ),
    # Filename completion in M-x shell: comint's TAB completes the
    # fixture path in place, and RET runs the completed command.
    (
        "shell-tab-complete",
        "sample\n",
        [b"\x1bxshell\r", b"cat " + FIXTURE_PATH.encode()[:-7], b"\t", b"\r", b"\x0c"],
    ),
    # Repeating C-s past the last match fails, and one more C-s wraps:
    # the echo walks I-search -> Failing -> Wrapped with the overwrapped
    # highlight states.
    (
        "isearch-fail-wrap",
        SEARCH_SAMPLE,
        [b"\x13beta", b"\x13\x13\x13\x13", b"\x13"],
    ),
    # M-s w toggles word search inside isearch.
    ("isearch-word", SEARCH_SAMPLE, [b"\x1bsw", b"beta", b"\x13"]),
    # RET ends the search storing it on the ring; a later bare C-s C-s
    # resumes the ring's head from point.
    ("isearch-ring", SEARCH_SAMPLE, [b"\x13beta\r", b"\x1b<", b"\x13\x13"]),
    # query-replace through the y/n/! answers: two spot replacements,
    # one skip, then replace-all, with the summary message.
    (
        "query-replace",
        SEARCH_SAMPLE,
        [b"\x1b%beta\rBETA\r", b"y", b"n", b"y", b"!"],
    ),
    # M-s o from isearch: the pending search becomes an occur, its
    # prefix column in the shadow face (face-differs-from-default-p
    # gates it through tty_supports_face_attributes_p), the match face
    # on each hit, and the copied coding system's mode-line mnemonic.
    ("occur-isearch", SEARCH_SAMPLE, [b"\x13beta", b"\x1bso"]),
    # occur-mode-goto-occurrence: RET on an entry jumps to the buffer
    # locus and the tty overlay arrow (=>) marks the entry row.
    (
        "occur-goto",
        SEARCH_SAMPLE,
        [b"\x1bsobeta\r", b"\x18o", b"\x0e\x0e", b"\r"],
    ),
    # replace-string end to end, with its echo summary.
    (
        "replace-string",
        SEARCH_SAMPLE,
        [b"\x1bxreplace-string\rbeta\rBETA\r"],
    ),
    # #+STARTUP: overview folds on open; the fontification pass must
    # cover the planned window (folds push its end far past any
    # cell-count estimate) while never fontifying hidden stretches.
    ("org-overview-open", FOLD_SAMPLE, [b"\x0e\x0e"], ".org"),
    # S-TAB arrives as `\e[Z' (the backtab function key) and cycles the
    # global visibility states, echoing CONTENTS on the repeat — the
    # repeat only advances because last-command carries over.
    ("org-backtab-cycle", FOLD_SAMPLE, [b"\x1b[Z", b"\x1b[Z"], ".org"),
    # TAB on a headline whose folded subtree spans more raw lines than
    # the window has rows: pos-visible-in-window-p counts DISPLAY rows,
    # so org-cycle's optimize hook must not recenter.
    ("org-tab-children", FOLD_SAMPLE, [b"\x0e\x0e\x0e", b"\t"], ".org"),
    # The DONE headline: org-headline-done's 8-color spec turns bold
    # OFF over the level face under it — the face-list merge honors an
    # explicit nil, and the truncated tagged headline's `$' cell keeps
    # the default face.
    ("org-done-face", FOLD_SAMPLE, [b"\x1b[Z", b"\x1b[Z"], ".org"),
    # M-x occur from an org buffer: org sets truncate-lines locally,
    # but the *Occur* window follows its own buffer (default nil) and
    # wraps its long entries.
    ("org-occur-wraps", FOLD_SAMPLE, [b"\x1bsosection\r"], ".org"),
    # Paging past the end: scroll-up signals (end-of-buffer) with nil
    # DATA, so the echo reads "End of buffer" with no ": nil" tail.
    ("page-past-end-error-echo", "only\nthree\nlines\n", [b"\x16", b"\x16"]),
]

# These journeys are deliberately command-shaped rather than end-state-only.
# They put the highest-frequency editing vocabulary first and compare the
# complete terminal after every command, so an early mismatch cannot be
# hidden by a later redraw, undo, or cursor movement.
CORE_FREQUENCY_SCENARIO_NAMES = (
    "core-character-motion",
    "core-word-motion",
    "core-buffer-motion",
    "core-word-editing",
    "core-line-editing",
    "core-transpose",
    "core-case-editing",
    "core-paragraph-editing",
    "core-mark-kill-yank",
    "core-prefix-and-undo",
)

SCENARIOS += [
    (
        "core-character-motion",
        CORE_EDIT_SAMPLE,
        [
            action("forward-char", b"\x06"),
            action("forward-char-again", b"\x06"),
            action("backward-char", b"\x02"),
            action("end-of-line", b"\x05"),
            action("beginning-of-line", b"\x01"),
            action("next-line", b"\x0e"),
            action("previous-line", b"\x10"),
        ],
    ),
    (
        "core-word-motion",
        CORE_EDIT_SAMPLE,
        [
            action("forward-word", b"\x1bf"),
            action("forward-word-again", b"\x1bf"),
            action("backward-word", b"\x1bb"),
            action("forward-sentence", b"\x1be"),
            action("backward-sentence", b"\x1ba"),
        ],
    ),
    (
        "core-buffer-motion",
        CORE_EDIT_SAMPLE,
        [
            action("end-of-buffer", b"\x1b>"),
            action("backward-paragraph", b"\x1b{"),
            action("forward-paragraph", b"\x1b}"),
            action("beginning-of-buffer", b"\x1b<"),
        ],
    ),
    (
        "core-word-editing",
        CORE_EDIT_SAMPLE,
        [
            action("forward-word", b"\x1bf"),
            action("kill-word", b"\x1bd"),
            action("backward-kill-word", b"\x1b\x7f"),
            action("yank", b"\x19"),
        ],
    ),
    (
        "core-line-editing",
        CORE_EDIT_SAMPLE,
        [
            action("end-of-line", b"\x05"),
            action("open-line", b"\x0f"),
            action("self-insert", b"inserted"),
            action("kill-line", b"\x0b"),
            action("yank", b"\x19"),
            action("join-line", b"\x1b^"),
        ],
    ),
    (
        "core-transpose",
        CORE_EDIT_SAMPLE,
        [
            action("forward-char", b"\x06"),
            action("transpose-chars", b"\x14"),
            action("forward-word", b"\x1bf"),
            action("transpose-words", b"\x1bt"),
        ],
    ),
    (
        "core-case-editing",
        CORE_EDIT_SAMPLE,
        [
            action("upcase-word", b"\x1bu"),
            action("downcase-word", b"\x1bl"),
            action("capitalize-word", b"\x1bc"),
        ],
    ),
    (
        "core-paragraph-editing",
        CORE_EDIT_SAMPLE,
        [
            action("forward-paragraph", b"\x1b}"),
            action("forward-paragraph-again", b"\x1b}"),
            action("fill-paragraph", b"\x1bq", settle=2.0, quiet=0.5),
            action("backward-paragraph", b"\x1b{"),
        ],
    ),
    (
        "core-mark-kill-yank",
        CORE_EDIT_SAMPLE,
        [
            action("set-mark", b"\x00"),
            action("next-line", b"\x0e"),
            action("end-of-line", b"\x05"),
            action("kill-region", b"\x17"),
            action("end-of-buffer", b"\x1b>"),
            action("yank", b"\x19"),
            action("exchange-point-mark", b"\x18\x18"),
        ],
    ),
    (
        "core-prefix-and-undo",
        CORE_EDIT_SAMPLE,
        [
            action("universal-forward", b"\x154\x06"),
            action("negative-forward", b"\x1b-2\x06"),
            action("insert-run", b"xyz"),
            action("undo", b"\x1f"),
            action("undo-via-c-x-u", b"\x18u"),
        ],
    ),
]

HELP_FILE_DIRED_SCENARIO_NAMES = (
    "help-key-then-quit",
    "help-function-then-quit",
    "file-save-buffer",
    "buffer-switch-scratch",
    "buffer-list",
    "dired-motion-mark-sort",
    "dired-open-and-return",
)

SCENARIOS += [
    (
        "help-key-then-quit",
        CORE_EDIT_SAMPLE,
        [
            action("describe-forward-char-key", b"\x08k\x06", settle=2.0, quiet=0.5),
            action("quit-help", b"q"),
        ],
    ),
    (
        "help-function-then-quit",
        CORE_EDIT_SAMPLE,
        [
            action(
                "describe-forward-word",
                b"\x08fforward-word\r",
                settle=2.0,
                quiet=0.5,
            ),
            action("quit-help", b"q"),
        ],
    ),
    (
        "file-save-buffer",
        CORE_EDIT_SAMPLE,
        [
            action("insert-change", b"saved "),
            # The isolated files share a basename but not a parent directory,
            # and GNU's transient `Wrote ...' echo includes that parent.  Defer
            # only that path-bearing frame; the next strict checkpoint checks
            # the post-save buffer, mode-line modified flag, echo, and cursor.
            action(
                "save-buffer",
                b"\x18\x13",
                checkpoint=False,
                settle=2.0,
                quiet=0.5,
            ),
            action("clear-save-message", b"\x06"),
        ],
        ".dat",
        {"separate_targets": True},
    ),
    (
        "buffer-switch-scratch",
        CORE_EDIT_SAMPLE,
        [
            action("switch-to-scratch", b"\x18b*scratch*\r"),
            action("insert-in-scratch", b"scratch text"),
            action("switch-back", b"\x18b\r"),
        ],
    ),
    (
        "buffer-list",
        CORE_EDIT_SAMPLE,
        [
            action("list-buffers", b"\x18\x02", settle=2.0, quiet=0.5),
            action("other-window", b"\x18o"),
            action("next-buffer-row", b"\x0e"),
        ],
    ),
    (
        "dired-motion-mark-sort",
        "",
        [
            action(
                "stable-listing",
                b'\x1b:(dired-sort-other "-Al")\r',
                settle=2.0,
                quiet=0.5,
            ),
            action("next-line", b"n"),
            action("previous-line", b"p"),
            action("mark", b"m"),
            action("unmark", b"u"),
            action("toggle-sort", b"s", settle=2.0, quiet=0.5),
            action("revert", b"g", settle=2.0, quiet=0.5),
        ],
        ".dat",
        {"target": "directory"},
    ),
    (
        "dired-open-and-return",
        "",
        [
            action(
                "stable-listing",
                b'\x1b:(dired-sort-other "-Al")\r',
                settle=2.0,
                quiet=0.5,
            ),
            action("next-line", b"n"),
            action("open-entry", b"\r", settle=2.0, quiet=0.5),
            action("return-to-dired", b"\x18b\r", settle=2.0, quiet=0.5),
        ],
        ".dat",
        {"target": "directory"},
    ),
]

HIGH_VALUE_COMMAND_SCENARIO_NAMES = (
    "minibuffer-edit-abort",
    "kill-buffer-confirm",
    "keyboard-macro-repeat",
    "register-text-and-point",
    "rectangle-kill-yank",
    "query-replace-step-and-undo",
    "bookmark-set-and-jump",
    "revert-buffer-confirm",
)

SCENARIOS += [
    (
        "minibuffer-edit-abort",
        CORE_EDIT_SAMPLE,
        [
            action("open-m-x", b"\x1bx"),
            action("type-misspelled-command", b"forward-wrod"),
            action("backward-word", b"\x1bb"),
            action("kill-word", b"\x1bd"),
            action("insert-correction", b"word"),
            action("abort-minibuffer", b"\x07"),
            action("open-m-x-again", b"\x1bx"),
            action("type-command", b"forward-word"),
            action("execute-command", b"\r"),
        ],
    ),
    (
        "kill-buffer-confirm",
        CORE_EDIT_SAMPLE,
        [
            action("modify-visited-file", b"temporary edits"),
            action("open-kill-buffer-prompt", b"\x18k"),
            action("choose-current-buffer", b"\r", settle=2.0, quiet=0.5),
            action("kill-without-saving", b"yes\r", settle=2.0, quiet=0.5),
        ],
    ),
    (
        "keyboard-macro-repeat",
        CORE_EDIT_SAMPLE,
        [
            action("start-kbd-macro", b"\x18("),
            action("end-of-line", b"\x05"),
            action("insert-bang", b"!"),
            action("next-line", b"\x0e"),
            action("beginning-of-line", b"\x01"),
            action("end-kbd-macro", b"\x18)"),
            action("execute-kbd-macro", b"\x18e", settle=2.0, quiet=0.5),
        ],
    ),
    (
        "register-text-and-point",
        CORE_EDIT_SAMPLE,
        [
            action("set-mark", b"\x00"),
            action("end-of-line", b"\x05"),
            action("copy-to-register-a", b"\x18rsa"),
            action("end-of-buffer", b"\x1b>"),
            action("insert-register-a", b"\x18ria"),
            action("beginning-of-buffer", b"\x1b<"),
            action("forward-word", b"\x1bf"),
            action("point-to-register-p", b"\x18r p"),
            action("end-of-buffer-again", b"\x1b>"),
            action("jump-to-register-p", b"\x18rjp"),
        ],
    ),
    (
        "rectangle-kill-yank",
        "abcd 1111\nabcd 2222\nabcd 3333\nlast line\n",
        [
            action("forward-char", b"\x06"),
            action("set-mark", b"\x00"),
            action("forward-char-again", b"\x06"),
            action("forward-char-third-column", b"\x06"),
            action("next-line", b"\x0e"),
            action("next-line-again", b"\x0e"),
            action("kill-rectangle", b"\x18rk", settle=2.0, quiet=0.5),
            action("end-of-buffer", b"\x1b>"),
            action("yank-rectangle", b"\x18ry", settle=2.0, quiet=0.5),
        ],
    ),
    (
        "query-replace-step-and-undo",
        "alpha one alpha\nalpha two\nlast alpha\n",
        [
            action("open-query-replace", b"\x1b%"),
            action("old-text", b"alpha\r"),
            action("new-text", b"omega\r", settle=2.0, quiet=0.5),
            action("replace-one", b"y"),
            action("skip-one", b"n"),
            action("replace-rest", b"!", settle=2.0, quiet=0.5),
            action("undo-replacement", b"\x1f"),
            action("undo-replacement-again", b"\x1f"),
        ],
    ),
    (
        "bookmark-set-and-jump",
        CORE_EDIT_SAMPLE,
        [
            action(
                "disable-bookmark-persistence",
                b"\x1b:(setq bookmark-save-flag nil)\r",
            ),
            action("forward-word", b"\x1bf"),
            action("open-bookmark-set", b"\x18rm"),
            action("name-bookmark", b"tty-spot\r", settle=2.0, quiet=0.5),
            action("end-of-buffer", b"\x1b>"),
            action("open-bookmark-jump", b"\x18rb"),
            action("choose-bookmark", b"tty-spot\r", settle=2.0, quiet=0.5),
        ],
    ),
    (
        "revert-buffer-confirm",
        CORE_EDIT_SAMPLE,
        [
            action("insert-change", b"changed "),
            action(
                "request-revert-buffer",
                b"\x1bxrevert-buffer\r",
                settle=2.0,
                quiet=0.5,
            ),
            action("confirm-revert-buffer", b"yes\r", settle=2.0, quiet=0.5),
        ],
    ),
]

ADVERSARIAL_COMMAND_SCENARIO_NAMES = (
    "kill-buffer-cancel-save",
    "revert-buffer-decline",
    "write-file-save-as",
    "keyboard-macro-abort-append",
    "keyboard-macro-read-char",
    "dired-copy-rename-delete",
)

SCENARIOS += [
    (
        "kill-buffer-cancel-save",
        CORE_EDIT_SAMPLE,
        [
            action("modify-visited-file", b"unsaved "),
            action("open-kill-buffer-prompt", b"\x18k"),
            action("choose-current-buffer", b"\r", settle=2.0, quiet=0.5),
            action("cancel-kill", b"no\r", settle=2.0, quiet=0.5),
            action(
                "verify-cancelled-state",
                b'\x1b:(list (buffer-name) (and buffer-file-name t) '
                b'(buffer-modified-p))\r',
                settle=2.0,
                quiet=0.5,
            ),
            # GNU's save confirmation names the isolated parent directory.
            # The following motion strictly checks the saved mode line and
            # buffer contents after clearing only that path-bearing echo.
            action(
                "save-buffer",
                b"\x18\x13",
                checkpoint=False,
                settle=2.0,
                quiet=0.5,
            ),
            action("clear-save-message", b"\x01"),
            action("open-kill-after-save", b"\x18k"),
            action("kill-saved-buffer", b"\r", settle=2.0, quiet=0.5),
        ],
        ".dat",
        {"separate_targets": True},
    ),
    (
        "revert-buffer-decline",
        CORE_EDIT_SAMPLE,
        [
            action("insert-change", b"changed "),
            action(
                "request-revert-buffer",
                b"\x1bxrevert-buffer\r",
                settle=2.0,
                quiet=0.5,
            ),
            # GNU's delayed suggest-key-bindings hint is timing-dependent;
            # the next strict motion verifies that declining preserved the
            # modified buffer, mode line, and cursor after clearing it.
            action(
                "decline-revert-buffer",
                b"no\r",
                checkpoint=False,
                settle=2.0,
                quiet=0.5,
            ),
            action("verify-declined-buffer", b"\x06"),
        ],
    ),
    (
        "write-file-save-as",
        CORE_EDIT_SAMPLE,
        [
            action("insert-change", b"saved-as "),
            # The initial file-name minibuffer contains each target's
            # isolated parent.  Replace it before comparing the prompt.
            action("open-write-file", b"\x18\x17", checkpoint=False),
            action("replace-destination", b"\x01\x0bsaved-copy.dat"),
            # `Wrote /isolated/parent/saved-copy.dat' differs only by the
            # intentionally distinct parent.  The next strict command checks
            # the visited basename, clean mode line, contents, and cursor.
            action(
                "write-copy",
                b"\r",
                checkpoint=False,
                settle=2.0,
                quiet=0.5,
            ),
            action("clear-write-message", b"\x06"),
        ],
        ".dat",
        {"separate_targets": True},
    ),
    (
        "keyboard-macro-abort-append",
        CORE_EDIT_SAMPLE,
        [
            action("start-aborted-macro", b"\x18("),
            action("end-of-line", b"\x05"),
            action("insert-aborted-bang", b"!"),
            action("abort-macro", b"\x07", settle=2.0, quiet=0.5),
            action("undo-aborted-edit", b"\x1f"),
            action("start-fresh-macro", b"\x18("),
            action("fresh-end-of-line", b"\x05"),
            action("fresh-insert-bang", b"!"),
            action("end-fresh-macro", b"\x18)"),
            action("append-without-replay", b"\x15\x15\x18("),
            action("append-next-line", b"\x0e"),
            action("append-beginning-of-line", b"\x01"),
            action("append-insert-marker", b">"),
            action("end-appended-macro", b"\x18)"),
            action("beginning-of-buffer", b"\x1b<"),
            action("execute-appended-macro", b"\x18e", settle=2.0, quiet=0.5),
        ],
    ),
    (
        "keyboard-macro-read-char",
        CORE_EDIT_SAMPLE,
        [
            action("start-kbd-macro", b"\x18("),
            # `point-to-register' reads the register with `read-char' after
            # its key binding has dispatched.  The nested `a' must become
            # part of the macro, not turn into a replay-time prompt.
            action("point-to-register-a", b"\x18r a"),
            action("forward-word", b"\x1bf"),
            action("end-kbd-macro", b"\x18)"),
            action("beginning-of-buffer", b"\x1b<"),
            action("execute-read-char-macro", b"\x18e", settle=2.0, quiet=0.5),
        ],
    ),
    (
        "dired-copy-rename-delete",
        "",
        [
            action(
                "stable-listing",
                b'\x1b:(dired-sort-other "-Al")\r',
                checkpoint=False,
                settle=2.0,
                quiet=0.5,
            ),
            # The absolute fixture roots must differ so each editor mutates
            # independent state.  Move the real entry to mid-window before
            # the first strict comparison, leaving only listing data visible.
            action("find-alpha", b"\x13alpha.txt\r", checkpoint=False),
            action("center-alpha", b"\x0c"),
            action("open-copy-prompt", b"C", checkpoint=False),
            action("name-copy", b"\x01\x0balpha-copy.txt"),
            action(
                "finish-copy",
                b"\r",
                checkpoint=False,
                settle=2.0,
                quiet=0.5,
            ),
            action("clear-copy-message", b"\x0c", settle=2.0, quiet=0.5),
            action("find-beta", b"\x13beta.txt\r"),
            action("open-rename-prompt", b"R", checkpoint=False),
            action("name-rename", b"\x01\x0bbeta-renamed.txt"),
            action(
                "finish-rename",
                b"\r",
                checkpoint=False,
                settle=2.0,
                quiet=0.5,
            ),
            action("clear-rename-message", b"\x0c", settle=2.0, quiet=0.5),
            action("find-notes", b"\x13notes.org\r"),
            action("open-delete-prompt", b"D", checkpoint=False),
            action(
                "confirm-delete",
                b"yes\r",
                checkpoint=False,
                settle=2.0,
                quiet=0.5,
            ),
            action("clear-delete-message", b"\x0c", settle=2.0, quiet=0.5),
        ],
        ".dat",
        {"target": "directory", "separate_targets": True, "padding_entries": 32},
    ),
]

FIELDNOTES_ADVANCED_SCENARIO_NAMES = (
    "org-fieldnotes-todo-cycle",
    "org-fieldnotes-priority",
    "org-fieldnotes-table-motion",
    "org-fieldnotes-heading-insert",
    "org-fieldnotes-narrow-widen",
    "org-fieldnotes-heading-motion",
)

SCENARIOS += [
    (
        "org-fieldnotes-todo-cycle",
        FOLD_SAMPLE,
        [
            action("open-contents", b"\x1b[Z"),
            action("search-workshop-heading", b"\x13Rebuild the workshop\r"),
            action("cycle-todo", b"\x03\x14", settle=2.0, quiet=0.5),
            action("cycle-todo-again", b"\x03\x14", settle=2.0, quiet=0.5),
        ],
        ".org",
    ),
    (
        "org-fieldnotes-priority",
        FOLD_SAMPLE,
        [
            action("open-contents", b"\x1b[Z"),
            action("search-rust-heading", b"\x13Learn enough Rust\r"),
            action("set-priority-c", b"\x03,C", settle=2.0, quiet=0.5),
            action("remove-priority", b"\x03, ", settle=2.0, quiet=0.5),
        ],
        ".org",
    ),
    (
        "org-fieldnotes-table-motion",
        FOLD_SAMPLE,
        [
            action("open-contents", b"\x1b[Z"),
            action("search-table-row", b"\x13Piranesi\r"),
            action("next-table-field", b"\t"),
            # Org realigns the table before moving.  On a cold process that
            # work can cross the generic 200 ms quiet window, so wait for the
            # completed command while preserving the same strict frame.
            action("next-table-row", b"\r", settle=2.0, quiet=0.5),
            action("previous-table-field", b"\x1b[Z"),
        ],
        ".org",
    ),
    (
        "org-fieldnotes-heading-insert",
        FOLD_SAMPLE,
        [
            action("open-contents", b"\x1b[Z"),
            action("search-heading", b"\x13Buy timber\r"),
            action("new-heading", b"\x1b\r", settle=2.0, quiet=0.5),
            action("heading-text", b"Order delivery"),
        ],
        ".org",
    ),
    (
        "org-fieldnotes-narrow-widen",
        FOLD_SAMPLE,
        [
            action("open-contents", b"\x1b[Z"),
            action("search-heading", b"\x13Learn enough Rust\r"),
            action("narrow-to-subtree", b"\x18ns", settle=2.0, quiet=0.5),
            action("end-of-buffer", b"\x1b>"),
            action("widen", b"\x18nw", settle=2.0, quiet=0.5),
        ],
        ".org",
    ),
    (
        "org-fieldnotes-heading-motion",
        FOLD_SAMPLE,
        [
            action("open-contents", b"\x1b[Z"),
            action("next-visible-heading", b"\x03\x0e"),
            action("next-visible-heading-again", b"\x03\x0e"),
            action("previous-visible-heading", b"\x03\x10"),
            action("forward-same-level", b"\x03\x06"),
            action("backward-same-level", b"\x03\x02"),
        ],
        ".org",
    ),
]

UNDO_KILL_RING_SCENARIO_NAMES = (
    "undo-kill-line-coalescing",
    "undo-kill-line-boundaries",
    "undo-word-kill-order",
    "undo-yank-pop-cycle",
    "undo-yank-pop-invalid",
    "undo-redo-divergent",
    "undo-keyboard-macro-boundaries",
    "undo-region-limited",
    "undo-repeat-c-slash",
    "undo-repeat-c-x-u",
)

SCENARIOS += [
    # Consecutive C-k commands are one kill-ring entry.  The final yank
    # distinguishes GNU-style coalescing from merely deleting the same text.
    (
        "undo-kill-line-coalescing",
        "alpha\nbeta\ngamma\n",
        [
            action("kill-alpha", b"\x0b"),
            action("kill-first-newline", b"\x0b"),
            action("kill-beta", b"\x0b"),
            action("kill-second-newline", b"\x0b"),
            action("end-of-buffer", b"\x1b>"),
            action("yank-coalesced-kill", b"\x19"),
        ],
    ),
    # C-k alternates between text and newline deletion, handles an empty
    # line, kills unterminated final text, and then reports the EOB error.
    (
        "undo-kill-line-boundaries",
        "one\n\nthree",
        [
            action("kill-first-text", b"\x0b"),
            action("kill-first-newline", b"\x0b"),
            action("kill-empty-line", b"\x0b"),
            action("kill-final-text", b"\x0b"),
            action("kill-at-end-of-buffer", b"\x0b"),
            action("yank-boundary-kill", b"\x19"),
        ],
    ),
    # Forward word kills append while a backward word kill prepends.  Yanking
    # the resulting entry exposes both direction and ordering mistakes.
    (
        "undo-word-kill-order",
        "alpha beta gamma delta\n",
        [
            action("forward-word", b"\x1bf"),
            action("kill-next-word", b"\x1bd"),
            action("kill-following-word", b"\x1bd"),
            action("backward-kill-word", b"\x1b\x7f"),
            action("end-of-buffer", b"\x1b>"),
            action("yank-ordered-entry", b"\x19"),
        ],
    ),
    # Three non-consecutive kills make three entries.  M-y must replace the
    # exact yank span, cycle through the older entries, and wrap to newest;
    # C-x C-x then makes point/mark placement visible on the glass.
    (
        "undo-yank-pop-cycle",
        "one\ntwo\nthree\nanchor\n",
        [
            action("kill-one", b"\x0b"),
            action("move-to-two", b"\x0e"),
            action("kill-two", b"\x0b"),
            action("move-to-three", b"\x0e"),
            action("kill-three", b"\x0b"),
            action("end-of-buffer", b"\x1b>"),
            action("yank-newest", b"\x19"),
            action("yank-pop-two", b"\x1by"),
            action("yank-pop-one", b"\x1by"),
            action("yank-pop-wrap", b"\x1by"),
            action("exchange-yank-point-and-mark", b"\x18\x18"),
        ],
    ),
    # yank-pop is invalid unless the previous command was a yank/yank-pop.
    # The strict frame proves both the GNU error and unchanged buffer text.
    (
        "undo-yank-pop-invalid",
        "stable text\n",
        [
            action("forward-char", b"\x06"),
            action("invalid-yank-pop", b"\x1by"),
        ],
    ),
    # Exercise both sides of the modern undo state machine, then fork the
    # history and prove redo cannot resurrect the abandoned branch.
    (
        "undo-redo-divergent",
        "base\n",
        [
            action("insert-alpha", b"alpha"),
            action("undo-alpha", b"\x1f"),
            action(
                "redo-alpha",
                b"\x1bxundo-redo\r",
                settle=2.0,
                quiet=0.5,
            ),
            action("undo-redone-alpha", b"\x1f"),
            action("insert-divergent-branch", b"branch"),
            action(
                "reject-redo-after-divergence",
                b"\x1bxundo-redo\r",
                settle=2.0,
                quiet=0.5,
            ),
            action("undo-divergent-branch", b"\x1f"),
        ],
    ),
    # Recording a macro performs ordinary edits; replay is a compound edit.
    # Repeated undo must cross the same inner and outer boundaries as GNU.
    (
        "undo-keyboard-macro-boundaries",
        "abcd\n",
        [
            action("start-macro", b"\x18("),
            action("macro-insert-x", b"X"),
            action("macro-forward-char", b"\x06"),
            action("macro-insert-y", b"Y"),
            action("end-macro", b"\x18)"),
            action("undo-recorded-y", b"\x1f"),
            action("undo-recorded-x", b"\x1f"),
            action("beginning-of-buffer", b"\x1b<"),
            action("execute-macro", b"\x18e", settle=2.0, quiet=0.5),
            action("undo-macro-edit", b"\x1f"),
            action("undo-macro-edit-again", b"\x1f"),
        ],
    ),
    # The newest edit is outside the active region.  Region-limited undo must
    # select the older in-region group while preserving the final Z edit.
    (
        "undo-region-limited",
        "aaa\nbbb\nccc\n",
        [
            action("insert-in-first-line", b"X"),
            action("end-of-buffer", b"\x1b>"),
            action("insert-outside-region", b"Z"),
            action("beginning-of-buffer", b"\x1b<"),
            action("set-region-mark", b"\x00"),
            action("select-first-line", b"\x05"),
            action("undo-in-region", b"\x1f"),
            action("verify-outside-edit", b"\x1b>"),
        ],
    ),
    (
        "undo-repeat-c-slash",
        "base\n",
        [
            action("insert-at-start", b"A"),
            action("end-of-buffer", b"\x1b>"),
            action("insert-at-end", b"Z"),
            action("undo-end-edit", b"\x1f"),
            action("undo-start-edit", b"\x1f"),
        ],
    ),
    (
        "undo-repeat-c-x-u",
        "base\n",
        [
            action("insert-at-start", b"A"),
            action("end-of-buffer", b"\x1b>"),
            action("insert-at-end", b"Z"),
            action("undo-end-edit", b"\x18u"),
            action("undo-start-edit", b"\x18u"),
        ],
    ),
]

SEEDED_SAFE_SCENARIO_NAMES = tuple(f"seeded-safe-{seed}" for seed, _ in SEEDED_SAFE_RUNS)
SCENARIOS += [
    (
        f"seeded-safe-{seed}",
        CORE_EDIT_SAMPLE,
        seeded_safe_actions(seed, steps),
    )
    for seed, steps in SEEDED_SAFE_RUNS
]


def select_scenarios(names):
    """Return all scenarios, or the named subset in command-line order."""
    if not names:
        return SCENARIOS
    by_name = {entry[0]: entry for entry in SCENARIOS}
    unknown = [name for name in names if name not in by_name]
    if unknown:
        available = ", ".join(by_name)
        raise ValueError(
            f"unknown scenario(s): {', '.join(unknown)}; available: {available}"
        )
    return [by_name[name] for name in names]


def populate_scenario_directory(path, padding_entries=0):
    """Populate one deterministic Dired fixture directory."""
    for index in range(padding_entries):
        with open(os.path.join(path, f"00-padding-{index:02}.txt"), "w") as out:
            out.write(f"padding file {index:02}\n")
    for filename, body in (
        ("alpha.txt", "alpha file\nsecond line\n"),
        ("beta.txt", "beta file\n"),
        ("notes.org", "* Dired fixture\nbody\n"),
    ):
        with open(os.path.join(path, filename), "w") as out:
            out.write(body)
    os.mkdir(os.path.join(path, "subdir"))


def create_scenario_target(name, contents, suffix=".dat", options=None):
    """Create the disposable file or directory visited by both editors."""
    options = options or {}
    if options.get("target") == "directory":
        path = tempfile.mkdtemp(prefix=f"ttydiff-{name}-")
        populate_scenario_directory(path, options.get("padding_entries", 0))
        return path

    handle, path = tempfile.mkstemp(suffix=suffix, prefix=f"ttydiff-{name}-")
    with os.fdopen(handle, "w") as out:
        out.write(contents)
    return path


def remove_scenario_target(path):
    """Remove a target created by :func:`create_scenario_target`."""
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.unlink(path)


def create_scenario_target_pair(name, contents, suffix=".dat", options=None):
    """Return GNU/Emaxx targets plus the paths that own their cleanup.

    Most read-only journeys intentionally share one target.  A journey that
    writes its visited file needs isolated copies: otherwise GNU saves first
    and Emaxx correctly detects an external modification.  The isolated files
    keep the same basename so the screen contract remains exact.
    """
    options = options or {}
    if not options.get("separate_targets"):
        path = create_scenario_target(name, contents, suffix, options)
        return (path, path), [path]

    roots = [
        tempfile.mkdtemp(prefix=f"ttydiff-{name}-gnu-"),
        tempfile.mkdtemp(prefix=f"ttydiff-{name}-emaxx-"),
    ]
    if options.get("target") == "directory":
        basename = f"ttydiff-{name}"
        targets = []
        for root in roots:
            path = os.path.join(root, basename)
            os.mkdir(path)
            populate_scenario_directory(path, options.get("padding_entries", 0))
            targets.append(path)
        return tuple(targets), roots

    basename = f"ttydiff-{name}{suffix}"
    targets = []
    for root in roots:
        path = os.path.join(root, basename)
        with open(path, "w") as out:
            out.write(contents)
        targets.append(path)
    return tuple(targets), roots


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(2)
    emaxx_binary, gnu_binary, lisp_dir = sys.argv[1:4]
    for path, label in [(emaxx_binary, "emaxx"), (gnu_binary, "GNU emacs")]:
        if not os.path.exists(path):
            message = f"no {label} binary at {path}"
            if os.environ.get("EMAXX_TTYDIFF_REQUIRE") == "1":
                print(f"ERROR: {message}", file=sys.stderr)
                sys.exit(2)
            print(f"SKIP: {message}")
            return
    if not os.path.isdir(lisp_dir):
        message = f"no GNU lisp tree at {lisp_dir}"
        if os.environ.get("EMAXX_TTYDIFF_REQUIRE") == "1":
            print(f"ERROR: {message}", file=sys.stderr)
            sys.exit(2)
        print(f"SKIP: {message}")
        return
    load_path = os.pathsep.join(
        [lisp_dir] + sorted(e.path for e in os.scandir(lisp_dir) if e.is_dir())
    )
    gnu_setup = gnu_no_window_setup(lisp_dir)

    with open(FIXTURE_PATH, "w") as fixture:
        fixture.write("fixture line one\nfixture line two\n")
    os.makedirs(COMPLETIONS_DIR, exist_ok=True)
    for entry in os.listdir(COMPLETIONS_DIR):
        os.unlink(os.path.join(COMPLETIONS_DIR, entry))
    for name in ("ambig1.dat", "ambig2.dat"):
        with open(os.path.join(COMPLETIONS_DIR, name), "w"):
            pass
    try:
        scenarios = select_scenarios(sys.argv[4:])
    except ValueError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(2)

    failures = 0
    for entry in scenarios:
        name, contents, keys = entry[0], entry[1], entry[2]
        # A scenario may carry a file suffix; `.el' engages lisp-mode and
        # font-lock through the ordinary auto-mode-alist path.
        suffix = entry[3] if len(entry) > 3 else ".dat"
        options = entry[4] if len(entry) > 4 else {}
        (gnu_path, emaxx_path), cleanup_targets = create_scenario_target_pair(
            name, contents, suffix, options
        )
        try:
            ok = compare(
                name,
                keys,
                [gnu_binary, "-nw", "-Q", "--eval", gnu_setup, gnu_path],
                [emaxx_binary, emaxx_path],
                {},
                {"EMACSLOADPATH": load_path},
                # Cold Lisp loading can exceed twenty seconds on a busy CI
                # host.  This is only a readiness deadline: comparisons and
                # per-command settle windows remain strict and unchanged.
                boot_wait=STARTUP_WAIT_SECONDS,
            )
            failures += 0 if ok else 1
        finally:
            for target in cleanup_targets:
                remove_scenario_target(target)
    if failures:
        print(f"FAIL: {failures} scenario(s) diverged")
        sys.exit(1)
    print("PASS: all scenarios match")


if __name__ == "__main__":
    main()
