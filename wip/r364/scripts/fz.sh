#!/bin/sh
# fz.sh EMAXX DIR: every program of DIR on GNU and EMAXX; names of those that differ.
E=$1; D=$2
G=/home/user/emacs-mirror/emacs/src/emacs
bad=0; n=0
for f in $D/z*.el; do
  n=$((n+1))
  b=${f%.el}
  timeout 30 $G -Q --batch -l $f > $b.g.o 2> $b.g.e < /dev/null; gs=$?
  timeout 30 $E -Q --batch -l $f > $b.e.o 2> $b.e.e < /dev/null; es=$?
  if [ $gs != $es ] || ! cmp -s $b.g.o $b.e.o || ! cmp -s $b.g.e $b.e.e; then bad=$((bad+1)); echo "DIFF $f ($gs/$es)"; fi
done
echo "programs $n differing $bad"
