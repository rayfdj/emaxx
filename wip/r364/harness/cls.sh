#!/bin/sh
# cls.sh EMAXX FILE...: each differing program again with prstack grown
# beforehand (a print of an 80-deep list), so that GNU's print stack is
# never reallocated under a print that Lisp interrupts.  SAME: the
# difference was the realloc's; DIFF: something else.
E=$1; shift
G=/home/user/emacs/src/emacs
O=$(dirname "$0")/cls
for f in "$@"; do
  n=$(echo "$f" | tr '/' '_')
  { echo ';; -*- lexical-binding: t -*-'; echo '(let ((x nil)) (dotimes (_ 80) (setq x (list x))) (prin1-to-string x))'; cat "$f"; } > $O/$n
  timeout 30 $G -Q --batch -l $O/$n > $O/$n.g.o 2> $O/$n.g.e < /dev/null; gs=$?
  timeout 30 $G -Q --batch -l $O/$n > $O/$n.g2.o 2> $O/$n.g2.e < /dev/null; gs2=$?
  timeout 30 $E -Q --batch -l $O/$n > $O/$n.e.o 2> $O/$n.e.e < /dev/null; es=$?
  r=SAME
  if [ $gs != $es ] || ! cmp -s $O/$n.g.o $O/$n.e.o || ! cmp -s $O/$n.g.e $O/$n.e.e; then r=DIFF; fi
  if [ $gs != $gs2 ] || ! cmp -s $O/$n.g.o $O/$n.g2.o; then r="$r GNU-UNSTABLE"; fi
  echo "$r $f ($gs/$es)"
done
