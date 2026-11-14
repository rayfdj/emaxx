import sys
S = '/home/user/emaxx-work'
which, gate = sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else ''
p = '/home/user/emaxx-wt/HANDOVER.md'
s = open(p).read()
entry = open(S + '/ho%s.txt' % which).read().replace('GATE_COUNT', gate)
anchor = "- **The frozen run's timings at round 97, and where the per-file cost\n"
assert s.count(anchor) == 1
s = s.replace(anchor, entry + anchor)
open(p, 'w').write(s)
print('ok')
