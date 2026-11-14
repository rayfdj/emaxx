#!/usr/bin/env python3
"""run.py BIN EL OUT [KEYS] : run BIN -Q -nw -l EL on a 24x80 pty; type KEYS
(a python literal list of (delay_seconds, bytes)) and save the raw terminal output to OUT.raw.
The elisp file is expected to write its own dump to the file named by $DUMP and kill emacs."""
import os, pty, sys, select, time, subprocess, signal, struct, fcntl, termios, ast
binary, el, out = sys.argv[1], sys.argv[2], sys.argv[3]
keys = ast.literal_eval(open(sys.argv[4]).read()) if len(sys.argv) > 4 else []
m, s = pty.openpty()
fcntl.ioctl(s, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 80, 0, 0))
env = dict(os.environ, TERM='xterm', DUMP=out + '.dump', LC_ALL='C.UTF-8')
def ctty():
    os.setsid()
    fcntl.ioctl(0, termios.TIOCSCTTY, 0)
p = subprocess.Popen([binary, '-Q', '-nw', '-l', el], stdin=s, stdout=s, stderr=s, preexec_fn=ctty, env=env)
os.close(s)
data = b''
def drain(t):
    global data
    end = time.time() + t
    while True:
        left = end - time.time()
        if left <= 0: return True
        r, _, _ = select.select([m], [], [], left)
        if r:
            try: d = os.read(m, 65536)
            except OSError: return False
            if not d: return False
            data += d
alive = True
for delay, k in keys:
    alive = drain(delay)
    if not alive: break
    os.write(m, k)
if alive: drain(float(os.environ.get('TTY_WAIT', '8')))
try: rc = p.wait(timeout=3)
except subprocess.TimeoutExpired:
    os.killpg(p.pid, signal.SIGKILL); rc = 'killed'
open(out + '.raw', 'wb').write(data)
print('rc=%s raw=%d' % (rc, len(data)))
