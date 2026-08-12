#!/usr/bin/env python3
"""End-to-end smoke test for the emaxx terminal frontend.

Drives the real binary through a pseudo-terminal: types text, saves with
C-x C-s, and exits with C-x C-c, asserting the screen contents, the saved
file, and the exit status at each step.

Usage:
    tools/tty-smoke.py [EMAXX_BINARY] [GNU_LISP_DIR]

EMAXX_BINARY defaults to target/release/emaxx.  GNU_LISP_DIR is a GNU
Emacs `lisp/' tree used to build EMACSLOADPATH (interactive commands
autoload their dumped Lisp owners); when omitted or missing the test
exits 0 with a SKIP notice so unconfigured environments stay green.
"""

import os
import pty
import re
import select
import struct
import sys
import tempfile
import time

import fcntl
import termios


def fail(message):
    print(f"FAIL: {message}")
    sys.exit(1)


def main():
    binary = sys.argv[1] if len(sys.argv) > 1 else "target/release/emaxx"
    lisp_dir = sys.argv[2] if len(sys.argv) > 2 else "../emacs/lisp"
    if not os.path.exists(binary):
        print(f"SKIP: no emaxx binary at {binary}")
        return
    if not os.path.isdir(lisp_dir):
        print(f"SKIP: no GNU lisp tree at {lisp_dir}")
        return
    load_path = os.pathsep.join(
        [lisp_dir]
        + sorted(
            entry.path
            for entry in os.scandir(lisp_dir)
            if entry.is_dir()
        )
    )

    handle, test_file = tempfile.mkstemp(suffix=".txt", prefix="emaxx-tty-smoke-")
    os.close(handle)
    os.unlink(test_file)

    pid, fd = pty.fork()
    if pid == 0:
        os.environ["TERM"] = "xterm-256color"
        os.environ["EMACSLOADPATH"] = load_path
        os.execv(binary, [binary, test_file])
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))

    def drain(timeout):
        out = b""
        while True:
            ready, _, _ = select.select([fd], [], [], timeout)
            if not ready:
                return out
            try:
                chunk = os.read(fd, 65536)
            except OSError:
                return out
            if not chunk:
                return out
            out += chunk

    def visible(raw):
        text = raw.decode("utf-8", "replace")
        text = re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", text)
        return re.sub(r"\x1b.", "", text)

    try:
        boot = drain(30.0)
        if b"\x1b[?1049h" not in boot:
            fail("alternate screen never entered")
        if os.path.basename(test_file).encode() not in boot:
            fail("mode line does not name the visited file")

        os.write(fd, b"hello")
        typed = visible(drain(2.0))
        if "hello" not in typed:
            fail(f"typed text missing from redraw: {typed!r}")
        if "**" not in typed:
            fail("modified flag missing after insertion")

        os.write(fd, b"\x18\x13")  # C-x C-s
        saved = visible(drain(4.0))
        if not os.path.exists(test_file):
            fail(f"C-x C-s did not write the file; screen: {saved!r}")
        contents = open(test_file).read()
        # GNU's basic-save-buffer supplies the final newline (verified
        # against the oracle: `emacs --batch -Q' saving "hello" writes
        # "hello\n"), so the session save must match it byte for byte.
        if contents != "hello\n":
            fail(f"saved contents wrong: {contents!r}")

        os.write(fd, b"\x18\x03")  # C-x C-c
        deadline = time.time() + 10.0
        status = None
        while time.time() < deadline:
            done, wait_status = os.waitpid(pid, os.WNOHANG)
            if done:
                status = wait_status
                break
            time.sleep(0.2)
        if status is None:
            fail("C-x C-c did not exit the session")
        if os.waitstatus_to_exitcode(status) != 0:
            fail(f"session exited with {status}")
        print("PASS: tty smoke test")
    finally:
        try:
            os.kill(pid, 9)
        except ProcessLookupError:
            pass
        if os.path.exists(test_file):
            os.unlink(test_file)


if __name__ == "__main__":
    main()
