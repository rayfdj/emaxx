import sys, textwrap
S = '/home/user/emaxx-work'
for name in sys.argv[1:]:
    lines = [l for l in open(S + '/' + name + '.src').read().split('\n') if l.strip()]
    out = ''.join(textwrap.fill(l, width=72, subsequent_indent='  ', break_long_words=False, break_on_hyphens=False) + '\n' for l in lines)
    open(S + '/' + name + '.txt', 'w').write(out)
    print(name, len(lines))
