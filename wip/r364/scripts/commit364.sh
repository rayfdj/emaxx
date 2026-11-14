#!/bin/sh
# commit364.sh GATE_COUNT: the round's commit, after the audit.
set -e
S=/home/user/emaxx-work
cd /home/user/emaxx-wt
cargo fmt --check
# The round audit (docs/round-audit.md): its negative controls, then the
# round's diff; a hard finding stops the commit.
python3 -m unittest discover -s tools -p test_round_audit.py
python3 tools/round_audit.py --repo . --base HEAD > $S/audit364.txt || { cat $S/audit364.txt; exit 1; }
python3 -I $S/ho170.py 364 "$1"
python3 -I $S/find364.py
git diff --check
git add -A src tests build.rs HANDOVER.md Cargo.toml Cargo.lock compat/generate_builtin_arities.el tools/round_audit.py tools/test_round_audit.py docs/round-audit.md
git status --short | head
git -c user.name=Ray -c user.email=26018378+rayfdj@users.noreply.github.com -c commit.gpgsign=false commit -q -F $S/msg364.txt
git log -1 --format=%B | grep -ciE "co-authored|claude|anthropic|generated with" || true
git log -1 --oneline
