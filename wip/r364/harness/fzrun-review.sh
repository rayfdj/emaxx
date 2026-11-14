#!/bin/bash
G=/home/user/emacs-mirror/emacs/src/emacs
E=${1:-/home/user/emaxx/target/prof/release/emaxx}
D=/home/user/emaxx-work/review364/fz
same=0; diff=0; unstable=0
norm() { sed -E 's/0x[0-9a-f]+/0xP/g; s/n=[0-9]+>/n=N>/g'; }
for f in $D/*.el; do
  n=$(basename $f .el)
  timeout 30 $G -Q --batch -l $f > $D/$n.g.o 2> $D/$n.g.e < /dev/null; gs=$?
  timeout 30 $E -Q --batch -l $f > $D/$n.e.o 2> $D/$n.e.e < /dev/null; es=$?
  if [ $gs = $es ] && cmp -s <(norm < $D/$n.g.o) <(norm < $D/$n.e.o) && cmp -s <(norm < $D/$n.g.e) <(norm < $D/$n.e.e); then same=$((same+1)); rm -f $D/$n.g.o $D/$n.g.e $D/$n.e.o $D/$n.e.e
  else
    timeout 30 $G -Q --batch -l $f > $D/$n.g2.o 2> $D/$n.g2.e < /dev/null; gs2=$?
    if [ $gs != $gs2 ] || ! cmp -s $D/$n.g.o $D/$n.g2.o || ! cmp -s $D/$n.g.e $D/$n.g2.e || [ $gs = 134 ] || [ $gs = 139 ]; then unstable=$((unstable+1)); echo "$n: gnu unstable/abort ($gs)"; else diff=$((diff+1)); echo "$n: DIFF status $gs/$es"; fi
  fi
done
echo "same $same, different $diff, gnu-unstable $unstable"
