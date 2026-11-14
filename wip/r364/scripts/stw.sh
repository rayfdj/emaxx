#!/bin/bash
# stw.sh EMAXX NAME MODE args...: the writes of descriptors 1 and 2 under
# strace, GNU against EMAXX.  MODE: pipe | file | full1 | tty (tty needs a
# runner that gives the program a pty for descriptors 1 and 2: TTY below).
E=$1; name=$2; mode=$3; shift 3
G=/home/user/emacs-mirror/emacs/src/emacs
R=$(dirname "$0")/out; mkdir -p $R
TTY=$(dirname "$0")/ttyrun.py
for t in gnu emx; do
  B=$G; [ $t = emx ] && B=$E
  tr=$R/$name.$mode.$t.strace
  S="timeout 60 strace -f -o $tr -e trace=write -e signal=none -s 24"
  case $mode in
    pipe) ( $S $B "$@" 2>&1 >&3 </dev/null | cat >$R/$name.$mode.$t.err ) 3>&1 | cat >$R/$name.$mode.$t.out;;
    file) $S $B "$@" 2>$R/$name.$mode.$t.err >$R/$name.$mode.$t.out </dev/null;;
    full1) $S $B "$@" 2>$R/$name.$mode.$t.err >/dev/full </dev/null; echo "rc=$?" >> $R/$name.$mode.$t.err; : > $R/$name.$mode.$t.out;;
    tty) python3 $TTY both $tr $R/$name.$mode.$t.out $B "$@"; : > $R/$name.$mode.$t.err;;
  esac
  sed -i "s|$B|EMACS|g" $R/$name.$mode.$t.err $R/$name.$mode.$t.out
  grep -E 'write\((1|2),' $tr | sed -E 's/^[0-9]+ +//' | sed -E 's/write\(([12]), (".*"(\.\.\.)?), ([0-9]+)\) += +(-?[0-9]+)(.*)/fd\1 len=\4 ret=\5\6 \2/' | sed "s|$B|EMACS|g" > $R/$name.$mode.$t.w
done
a=same; cmp -s $R/$name.$mode.gnu.w $R/$name.$mode.emx.w || a=DIFF
b=same; cmp -s $R/$name.$mode.gnu.out $R/$name.$mode.emx.out || b=DIFF
c=same; cmp -s $R/$name.$mode.gnu.err $R/$name.$mode.emx.err || c=DIFF
echo "$name[$mode]: writes=$a stdout=$b stderr=$c ($(wc -l < $R/$name.$mode.gnu.w) writes)"
