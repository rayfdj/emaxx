#!/bin/sh
# Instructions per iteration of a Lisp loop, with the boot and any
# collections cancelled out: callgrind on two run lengths, differenced.
#
#   tools/perf/callgrind-diff.sh BINARY FILE.el N1 N2
#
# FILE.el must contain the literal N1 as its iteration count; the script
# rewrites it to N2 for the second run.  BINARY is an Emacs (GNU's or
# Emaxx with its image beside it).  Example, as used for the ledger's
# call-loop and get-loop numbers:
#
#   tools/perf/callgrind-diff.sh target/release/emaxx tools/perf/call-loop.el 200000 600000
#   tools/perf/callgrind-diff.sh ../emacs/src/emacs tools/perf/call-loop.el 200000 600000
set -eu
binary=$1; file=$2; n1=$3; n2=$4
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cp "$file" "$work/a.el"
sed "s/$n1/$n2/" "$file" > "$work/b.el"
for run in a b; do
    valgrind --tool=callgrind --callgrind-out-file="$work/$run.out" \
        "$binary" -Q --batch -l "$work/$run.el" > /dev/null 2>&1
done
a=$(grep -m1 '^summary:' "$work/a.out" | awk '{print $2}')
b=$(grep -m1 '^summary:' "$work/b.out" | awk '{print $2}')
echo "$(( (b - a) / (n2 - n1) )) instructions an iteration ($binary, $file)"
