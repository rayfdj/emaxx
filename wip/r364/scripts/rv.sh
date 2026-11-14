#!/bin/sh
# rv.sh EMAXX DIR: each DIR/*.el on GNU and EMAXX; stdout, stderr, status.
G=/home/user/emacs-mirror/emacs/src/emacs
E=$1; D=$2; O=$D/out2; mkdir -p "$O"
same=0; diff=0
for f in "$D"/*.el; do
  n=$(basename "$f" .el)
  timeout 30 "$G" -Q --batch -l "$f" > "$O/$n.g.o" 2> "$O/$n.g.e" < /dev/null; gs=$?
  timeout 30 "$E" -Q --batch -l "$f" > "$O/$n.e.o" 2> "$O/$n.e.e" < /dev/null; es=$?
  r=""
  [ "$gs" = "$es" ] || r="status $gs/$es"
  cmp -s "$O/$n.g.o" "$O/$n.e.o" || r="$r stdout"
  cmp -s "$O/$n.g.e" "$O/$n.e.e" || r="$r stderr"
  if [ -n "$r" ]; then diff=$((diff+1)); echo "$n: $r"; else same=$((same+1)); fi
done
echo "same $same, different $diff"
