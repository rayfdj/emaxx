#!/usr/bin/env python3
"""Exercise real emacsclient TTYs, serially, against the pinned GNU oracle.

No server Lisp is replaced: both editors load their ordinary server.el and
the GNU emacsclient connects over a private local socket. Requires PTYs and
local Unix sockets. Logs/screens remain in --output for diagnosis.
"""
import argparse
import json
import os
from pathlib import Path
import select
import subprocess
import tempfile
import time

from ttydiff import Session


def exercise(editor, client, root, terminal_options):
    root.mkdir(parents=True, mode=0o700)
    target = root / "client.txt"
    target.write_text("")
    socket = root / "server"
    env = {"HOME": str(root), "TMPDIR": str(root), "TERM": "xterm"}
    sessions = []
    transcripts = []

    def drain(seconds):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            readable, _, _ = select.select([s.fd for s in sessions if s.fd >= 0], [], [], .05)
            for session, transcript in zip(sessions, transcripts):
                if session.fd in readable:
                    try:
                        data = os.read(session.fd, 65536)
                    except OSError:
                        continue
                    transcript.extend(data)
                    with (root / f"terminal-{sessions.index(session)}.raw").open("ab") as log:
                        log.write(data)
                    session.screen.feed(data)

    def evaluate(form):
        process = subprocess.Popen([str(client), "--socket-name", str(socket), "--eval", form],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        until = time.monotonic() + 30
        while process.poll() is None and time.monotonic() < until:
            drain(.1)
        if process.poll() is None:
            process.kill()
            raise RuntimeError(f"server evaluation timed out: {form}")
        out, error = process.communicate()
        if process.returncode:
            raise RuntimeError(error.decode(errors="replace"))
        return out.decode().strip()

    def wait_value(form, expected, timeout=30):
        until = time.monotonic() + timeout
        actual = None
        while time.monotonic() < until:
            actual = evaluate(form)
            if actual == expected:
                return actual
            drain(.2)
        raise AssertionError(f"{form}: expected {expected!r}, got {actual!r}")

    try:
        primary = Session([str(editor), "-Q", *terminal_options, "--eval",
                           f"(progn (require 'server) (setq server-name {json.dumps(str(socket))}) (server-start))"], env)
        sessions.append(primary)
        transcripts.append(bytearray())
        until = time.monotonic() + 90
        while not socket.exists() and time.monotonic() < until:
            drain(.2)
        if not socket.exists():
            raise RuntimeError("editor did not create server socket")
        wait_value("(length (frame-list))", "1")
        evaluate("(setq terminal-gate-primary (selected-frame))")
        secondary = Session([str(client), "--socket-name", str(socket), "-t", str(target)], env)
        sessions.append(secondary)
        transcripts.append(bytearray())
        wait_value("(length (frame-list))", "2")
        buffer = f"(get-file-buffer {json.dumps(str(target))})"
        wait_value(f"(buffer-live-p {buffer})", "t")
        evaluate('(with-current-buffer "*scratch*" (erase-buffer))')
        os.write(primary.fd, b"P")
        wait_value('(with-current-buffer "*scratch*" (buffer-substring-no-properties (point-min) (point-max)))', '"P"')
        drain(1)
        # Split both a termcap key sequence and UTF-8 across device reads.
        for keys in [b"abc", b"\x1bO", b"D", b"X\xc3", b"\xa9"]:
            os.write(secondary.fd, keys)
            drain(1)
        edited = wait_value(f"(with-current-buffer {buffer} (buffer-string))", '"abXéc"')
        drain(.5)
        for n, session in enumerate(sessions):
            (root / f"editing-{n}.txt").write_text("\n".join(session.screen.lines()))
        assert any("abXéc" in line for line in secondary.screen.lines())
        assert not any("abXéc" in line for line in primary.screen.lines())
        os.write(secondary.fd, b"\x18\x13")
        wait_value(f"(with-current-buffer {buffer} (buffer-modified-p))", "nil")
        assert target.read_text() == "abXéc\n", repr(target.read_text())
        os.write(secondary.fd, b"\x18\x35\x30")
        wait_value("(length (frame-list))", "1")
        primary_live = evaluate("(and (frame-live-p terminal-gate-primary) (eq (selected-frame) terminal-gate-primary))")
        assert primary_live == "t", primary_live
        drain(.5)
        pid, status = os.waitpid(secondary.pid, os.WNOHANG)
        assert pid == secondary.pid and os.waitstatus_to_exitcode(status) == 0, (pid, status)
        os.close(secondary.fd)
        secondary.fd = -1
        disconnected = Session([str(client), "--socket-name", str(socket), "-t", str(target)], env)
        sessions.append(disconnected)
        transcripts.append(bytearray())
        wait_value("(length (frame-list))", "2")
        disconnected.close()
        disconnected.fd = -1
        wait_value("(length (frame-list))", "1")
        after_disconnect = evaluate("(and (frame-live-p terminal-gate-primary) (eq (selected-frame) terminal-gate-primary))")
        assert after_disconnect == "t", after_disconnect
        result = {"edited": edited, "saved": target.read_text(), "primary_preserved": primary_live, "client_exit": 0, "disconnect_preserved": after_disconnect}
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        return result
    finally:
        for n, (session, transcript) in enumerate(zip(sessions, transcripts)):
            (root / f"terminal-{n}.raw").write_bytes(transcript)
            (root / f"terminal-{n}.txt").write_text("\n".join(session.screen.lines()))
            if session.fd >= 0:
                session.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle", type=Path, default=Path("../emacs/src/emacs"))
    parser.add_argument("--subject", type=Path, default=Path("target/gate/emaxx"))
    parser.add_argument("--client", type=Path, default=Path("../emacs/lib-src/emacsclient"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or Path(tempfile.mkdtemp(prefix="emaxx-terminal-gate-", dir="/tmp"))
    print(f"Artifacts: {output}", flush=True)
    oracle = exercise(args.oracle.resolve(), args.client.resolve(), output / "oracle", ["-nw"])
    print(f"GNU: {oracle}", flush=True)
    # Emaxx's interactive frontend is already a TTY; its CLI doesn't expose
    # GNU's window-system switch yet.
    subject = exercise(args.subject.resolve(), args.client.resolve(), output / "subject", [])
    assert subject == oracle, (oracle, subject)
    print(f"Emaxx: {subject}", flush=True)


if __name__ == "__main__":
    main()
