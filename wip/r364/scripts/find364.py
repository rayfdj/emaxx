S = '/home/user/emaxx-work'
p = '/home/user/emaxx-wt/HANDOVER.md'
s = open(p).read()
ci = open(S + '/ci363.txt').read().rstrip('\n')
heading = "**Findings recorded, not fixed:**\n"
assert s.count(heading) == 1
new = open(S + '/findings364.txt').read()
assert new.endswith('\n') and 'PROBES364' not in new and 'REVIEW364' not in new
s = s.replace(heading, ci + "\n\n" + heading + new)
open(p, 'w').write(s)
print('find364 ok')
