#!/bin/bash
# final.sh EMAXX: the round's comparisons against GNU on one build.
E=$1
W=/home/user/emaxx-work
G=/home/user/emacs/src/emacs
echo "== probes"
for d in final extra; do echo -n "$d: "; sh /home/user/emaxx-wt/wip/r364/scripts/rv.sh $E $W/r364rv/$d | tr '\n' ' '; echo; done
echo -n "mine: "; sh /home/user/emaxx-wt/wip/r364/scripts/rv.sh $E $W/r364mine | tr '\n' ' '; echo
for d in review2 p1r; do echo "$d:"; sh /home/user/emaxx-wt/wip/r364/scripts/rv.sh $E $W/r364rv/$d; done
echo "== forms"
same=0; diff=0
for f in $W/r364val/forms/*.el; do
  b=${f%.el}
  timeout 60 $G -Q --batch -l $f > $b.g.o 2> $b.g.e < /dev/null; gs=$?
  timeout 60 $E -Q --batch -l $f > $b.e.o 2> $b.e.e < /dev/null; es=$?
  if [ $gs = $es ] && cmp -s $b.g.o $b.e.o && cmp -s $b.g.e $b.e.e; then same=$((same+1)); else diff=$((diff+1)); echo "DIFF $f"; fi
done
echo "forms same $same different $diff"
echo "== fuzz (plain)"
for c in c1 c2 c3 c4; do sh $W/r364fz/fz.sh $E $W/r364fz/$c | tail -1; done
echo "== fuzz (stack grown first)"
for c in c1 c2 c3 c4; do sh $W/r364fz/fzp.sh $E $W/r364fz/$c; done
for c in c5 c6; do sh $W/r364fz/fz.sh $E $W/r364fz/$c | tail -1; sh $W/r364fz/fzp.sh $E $W/r364fz/$c; done
echo "== fuzz (the review's corpus)"
bash $W/r364val/fzrun-review.sh $E | tail -4
echo "== strace"
for w in w1 w2 w3 w4; do for m in pipe file full1 tty; do bash $W/r364st/stw.sh $E $w $m -Q --batch -l $W/r364st/$w.el; done; done
echo "== tty"
cd $W/r364val/tty && rm -rf gd ed && mkdir gd ed && (cd gd && timeout 120 python3 ../run.py $G ../echo364.el g > g.log 2>&1); (cd ed && timeout 120 python3 ../run.py $E ../echo364.el e > e.log 2>&1); cmp gd/echo364.out ed/echo364.out && echo "tty same ($(wc -l < gd/echo364.out) lines)"
echo FINAL-DONE
