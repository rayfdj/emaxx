import sys, textwrap
src, dst = sys.argv[1], sys.argv[2]
paras = [p.strip() for p in open(src).read().split('\n\n') if p.strip()]
out = [paras[0]]
for p in paras[1:]:
    text = ' '.join(l.strip() for l in p.split('\n'))
    out.append(textwrap.fill(text, width=72, break_on_hyphens=False, break_long_words=False))
open(dst, 'w').write('\n\n'.join(out) + '\n')
print(len(out), 'paragraphs')
