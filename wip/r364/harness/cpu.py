#!/usr/bin/env python3
"""cpu.py RUNS FILE.el BIN...: least user+system CPU seconds of RUNS runs of
BIN -Q --batch -l FILE.el, for each BIN."""
import os, subprocess, sys
runs = int(sys.argv[1]); prog = sys.argv[2]
out = []
for b in sys.argv[3:]:
    best = None
    for _ in range(runs):
        pid = os.fork()
        if pid == 0:
            null = os.open(os.devnull, os.O_RDWR)
            os.dup2(null, 0); os.dup2(null, 1); os.dup2(null, 2)
            os.execv(b, [b, "-Q", "--batch", "-l", prog])
        _, status, ru = os.wait4(pid, 0)
        t = ru.ru_utime + ru.ru_stime
        best = t if best is None or t < best else best
    out.append("%.3f" % best)
print(os.path.basename(prog), " ".join(out))
