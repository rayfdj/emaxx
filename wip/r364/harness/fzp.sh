#!/bin/sh
# fzp.sh EMAXX DIR: every program of DIR GNU answers the same twice
# (stable.sh), on GNU and EMAXX, after a print of an 80-deep list: GNU's
# print stack is then never reallocated under a print that Lisp
# interrupts (its use after free).  Names of those that differ.
E=$1; D=$2
G=/home/user/emacs/src/emacs
P=$D/p; mkdir -p $P
bad=0; n=0
for f in $D/z*.el; do
  [ -f $D/unstable.txt ] && grep -qx "$f" $D/unstable.txt && continue
  n=$((n+1))
  b=$P/$(basename $f .el)
  { echo ';; -*- lexical-binding: t -*-'; echo '(let ((x nil)) (dotimes (_ 80) (setq x (list x))) (prin1-to-string x))'; cat $f; } > $b.el
  timeout 30 $G -Q --batch -l $b.el > $b.g.o 2> $b.g.e < /dev/null; gs=$?
  timeout 30 $E -Q --batch -l $b.el > $b.e.o 2> $b.e.e < /dev/null; es=$?
  if [ $gs != $es ] || ! cmp -s $b.g.o $b.e.o || ! cmp -s $b.g.e $b.e.e; then
    timeout 30 $G -Q --batch -l $b.el > $b.g2.o 2> $b.g2.e < /dev/null; gs2=$?
    if [ $gs != $gs2 ] || ! cmp -s $b.g.o $b.g2.o || ! cmp -s $b.g.e $b.g2.e; then echo "UNSTABLE $f"; else bad=$((bad+1)); echo "DIFF $f ($gs/$es)"; fi
  fi
done
echo "programs $n differing $bad"
