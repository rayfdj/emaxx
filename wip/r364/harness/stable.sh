#!/bin/sh
# stable.sh DIR: programs GNU answers differently on two runs -> DIR/unstable.txt
G=/home/user/emacs/src/emacs; D=$1; : > $D/unstable.txt
for f in $D/z*.el; do b=${f%.el}
  timeout 30 $G -Q --batch -l $f > $b.s1.o 2> $b.s1.e < /dev/null; r1=$?
  timeout 30 $G -Q --batch -l $f > $b.s2.o 2> $b.s2.e < /dev/null; r2=$?
  if [ $r1 != $r2 ] || ! cmp -s $b.s1.o $b.s2.o || ! cmp -s $b.s1.e $b.s2.e; then echo $f >> $D/unstable.txt; fi
  rm -f $b.s1.o $b.s1.e $b.s2.o $b.s2.e
done
echo "$D unstable: $(wc -l < $D/unstable.txt)"
