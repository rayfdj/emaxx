#!/usr/bin/env python3
"""Instructions a unit of the probes in tools/perf/probes, in each editor.

    tools/perf/cg-table.py LABEL=BINARY... -- PROBE:N1:N2 ...

Each probe is a Lisp file with the token @N@ for its iteration count.  It
is run under callgrind at the two counts N1 and N2 in every BINARY (an
Emacs: GNU's, or Emaxx with its image beside it) as `BINARY -Q --batch
-l FILE'; the difference of the two totals, over N2 - N1 and the probe's
units an iteration (its `DIV n' line), is the cost of a unit with the
boot and any work outside the loop cancelled.  A probe signals, and its
row reads ERR, when its own result check fails; a `RESULT x' line is
printed beside the row.  CG_JOBS sets the number of runs at a time
(default 2).

The counts used for HANDOVER's tables:

    lex-rec:2000:6000 dyn-rec:2000:6000 looking-at:200000:600000
    string-match:200000:600000 make-symbol:200000:600000
    intern-new:20000:60000 priv-intern:20000:60000 read-gensym:100000:300000
    mapatoms-bc:20:60 mapatoms-closure:20:60 maphash-bc:200:600
    mapc-bc:200:600 mapc-interp:200:600 interp-ignore:200000:600000
    princ-interp:50000:150000 windows:100000:300000 ucs-names:1:3
    read-file:2:6 mapc-bc-bc:200:600

and for a subr called from native code, each at 100000:300000
(native-base is the loop they share, with no call in it):

    native-base native-vectorp native-symbol-with-pos-p native-recordp
    native-bare-symbol native-keywordp native-nth native-nth-var
    native-point native-plus native-max native-symbol-name native-delq
    native-assoc native-logand native-cons native-eqlsign native-geq
    native-purecopy native-current-buffer
"""
import concurrent.futures
import os
import re
import subprocess
import sys
import tempfile


def run(probes_dir, label, binary, probe, count):
    source = open(os.path.join(probes_dir, probe + ".el")).read()
    work = tempfile.mkdtemp(prefix="cg-table-")
    file = os.path.join(work, "probe.el")
    with open(file, "w") as out:
        out.write(source.replace("@N@", str(count)))
    profile = os.path.join(work, "callgrind.out")
    done = subprocess.run(
        ["valgrind", "--tool=callgrind", "--callgrind-out-file=" + profile,
         binary, "-Q", "--batch", "-l", file],
        capture_output=True, text=True)
    total = None
    if done.returncode == 0:
        with open(profile) as lines:
            for line in lines:
                if line.startswith("summary:"):
                    total = int(line.split()[1])
                    break
    units = re.search(r"DIV (\d+)", done.stdout)
    result = re.search(r"RESULT (\S+)", done.stdout)
    for name in (file, profile):
        if os.path.exists(name):
            os.remove(name)
    os.rmdir(work)
    if total is None or units is None:
        return label, probe, count, None, None, done.stderr.strip()[-200:]
    return label, probe, count, total, int(units.group(1)), result.group(1) if result else ""


def main():
    arguments = sys.argv[1:]
    if "--" not in arguments:
        sys.exit(__doc__)
    split = arguments.index("--")
    binaries = [argument.split("=", 1) for argument in arguments[:split]]
    probes = [argument.split(":") for argument in arguments[split + 1:]]
    probes_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "probes")
    jobs = []
    with concurrent.futures.ThreadPoolExecutor(int(os.environ.get("CG_JOBS", "2"))) as pool:
        for probe, first, second in probes:
            for label, binary in binaries:
                for count in (int(first), int(second)):
                    jobs.append(pool.submit(run, probes_dir, label, binary, probe, count))
        runs = [job.result() for job in jobs]
    table = {}
    for label, probe, count, total, units, note in runs:
        table.setdefault((probe, label), []).append((count, total, units, note))
    labels = [label for label, _ in binaries]
    print("%-18s" % "probe" + "".join("%16s" % label for label in labels))
    failed = False
    for probe, _, _ in probes:
        cells, notes = [], []
        for label in labels:
            (n1, t1, units, note1), (n2, t2, _, note2) = sorted(table[(probe, label)])
            if t1 is None or t2 is None:
                failed = True
                cells.append("%16s" % "ERR")
                notes.append(note1 if t1 is None else note2)
            else:
                cells.append("%16.1f" % ((t2 - t1) / (n2 - n1) / units))
                notes.append(note1)
        print("%-18s" % probe + "".join(cells) + "  " + " ".join(note for note in notes if note))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
