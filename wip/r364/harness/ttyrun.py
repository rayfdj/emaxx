#!/usr/bin/env python3
"""ttyrun.py both TRACE OUT BIN args...: run BIN under strace (writes only)
with a pty as its stdout and stderr (stdin /dev/null); what the pty
received goes to OUT."""
import os, pty, sys, select, subprocess
mode, trace, out, argv = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
m, s = pty.openpty()
cmd = ['timeout', '60', 'strace', '-f', '-o', trace, '-e', 'trace=write', '-e', 'signal=none', '-s', '24'] + argv
p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=s, stderr=s)
os.close(s)
data = b''
while True:
    r, _, _ = select.select([m], [], [], 0.2)
    if r:
        try:
            d = os.read(m, 65536)
        except OSError:
            break
        if not d:
            break
        data += d
    elif p.poll() is not None:
        # drain what is left
        try:
            while True:
                r, _, _ = select.select([m], [], [], 0.2)
                if not r: break
                d = os.read(m, 65536)
                if not d: break
                data += d
        except OSError:
            pass
        break
p.wait()
open(out, 'wb').write(data.replace(b'\r\n', b'\n'))
