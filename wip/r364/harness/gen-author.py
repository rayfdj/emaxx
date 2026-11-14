#!/usr/bin/env python3
"""gen.py SEED COUNT DIR: COUNT random print programs zNNNN.el in DIR."""
import random, sys, os
seed, count, outdir = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
os.makedirs(outdir, exist_ok=True)
R = random.Random(seed)

ASCII_SYMS = ['a', 'b', 'foo', 'bar-baz', 'nil', 't', ':key', 'quote', 'function', 'x1', '-', '1+', 'a.b']
ODD_SYMS = ['(intern "")', '(intern "1")', '(intern "-1.5")', '(intern "a b")', '(intern "?x")', '(intern ".x")',
            '(intern "a;b")', '(intern "1e5")', '(intern "+")', '(intern "a\\\\b")', '(intern "\\"q")', '(make-symbol "g")',
            '(make-symbol "")', '(intern "a,b")', '(intern "#a")', '(intern "a#")', '(intern "1.0e+INF")', '(intern "-")',
            '(intern "+1")', '(intern ".")', '(intern "?")', '(intern "a\\nb")', '(intern "a\\tb")']
MB_SYMS = ['(intern "caf\\u00e9")', '(intern "\\u00e9")', '(make-symbol "\\u20ac\\u00e9")', '(intern (string-to-unibyte "u\\351"))']
ASCII_STRS = ['""', '"a"', '"hello world"', '"with \\"quotes\\" and \\\\ backslash"', '"line1\\nline2"', '"tab\\there"',
              '"ctl\\1\\2x"', '"\\0nul"', '"form\\ffeed"', '"7\\177"', '"abc123"', '"x\\n"', '"\\n"', '"ctl\\0019"']
MB_STRS = ['"caf\\u00e9"', '"\\u00e9a"', '"\\u20ac100"', '"\\u00e9\\u00e9"', '(string-to-multibyte "raw\\351x")', '"\\u00e9f"',
           '"\\U0001F600!"', '(string #x3fff80 ?a)', '"\\u00e91"']
UB_STRS = ['(string-to-unibyte "u\\351")', '(string-to-unibyte "\\200\\377")', '(string-to-unibyte "a\\351b")']
NUMS = ['0', '1', '-1', '42', '97', '10', '32', '40', '233', '1114111', '1114112', '4194303', 'most-positive-fixnum',
        'most-negative-fixnum', '(expt 2 70)', '(- (expt 10 25))', '1.5', '-0.0', '1e10', '1.0e+INF', '0.0e+NaN', '100.0', '0.1', '1e-7', '123456789.125']

def atom(mb, ub):
    k = R.random()
    if k < 0.25: return "'" + R.choice(ASCII_SYMS)
    if k < 0.35: return R.choice(ODD_SYMS)
    if k < 0.40 and mb: return R.choice(MB_SYMS)
    if k < 0.60: return R.choice(NUMS)
    if k < 0.75: return R.choice(ASCII_STRS)
    if k < 0.85 and mb: return R.choice(MB_STRS)
    if k < 0.88 and ub: return R.choice(UB_STRS)
    if k < 0.91: return '(make-bool-vector %d %s)' % (R.choice([0, 1, 3, 8, 9, 16, 20, 40]), R.choice(['t', 'nil']))
    if k < 0.93: return "(bool-vector t nil t nil nil nil nil nil nil t nil t nil nil nil nil t t)"
    if k < 0.95: return R.choice(['(make-marker)', 'mk', 'bf', "(symbol-function 'car)", '(obarray-make)', '(current-window-configuration)',
                                    '(selected-window)', 'killed', 'ov'])
    if k < 0.97: return '(propertize %s %s)' % (R.choice(ASCII_STRS), R.choice(["'face 'bold", "'k \"v\"", "'a 1 'b '(2 3)", "'charset 'ascii"]))
    return R.choice(['nil', 't', "'()"])

def obj(depth, mb, ub):
    if depth <= 0 or R.random() < 0.3:
        return atom(mb, ub)
    k = R.random()
    n = R.randint(0, 5)
    kids = [obj(depth - 1, mb, ub) for _ in range(n)]
    if k < 0.40: return '(list %s)' % ' '.join(kids)
    if k < 0.55: return '(vector %s)' % ' '.join(kids)
    if k < 0.62 and kids: return '(cons %s %s)' % (kids[0], obj(depth - 1, mb, ub))
    if k < 0.68: return "(list 'quote %s)" % obj(depth - 1, mb, ub)
    if k < 0.72: return "(list 'function %s)" % obj(depth - 1, mb, ub)
    if k < 0.76: return "(list '\\` (list 'a (list '\\, %s) (list '\\,@ %s)))" % (obj(depth - 1, mb, ub), obj(depth - 1, mb, ub))
    if k < 0.80: return "(list '\\, %s)" % obj(depth - 1, mb, ub)
    if k < 0.86: return '(record %s)' % ' '.join(["'rec"] + kids)
    if k < 0.92:
        test = R.choice(['eql', 'eq', 'equal'])
        weak = R.choice(['', '', '', " :weakness 'key"])
        pairs = ' '.join('(puthash %s %s h)' % (obj(depth - 1, mb, ub), obj(depth - 1, mb, ub)) for _ in range(R.randint(0, 3)))
        return "(let ((h (make-hash-table :test '%s%s))) %s h)" % (test, weak, pairs)
    if k < 0.96: return '(let ((s %s)) (list s s (vector s)))' % obj(depth - 1, mb, ub)
    if k < 0.98: return '(let ((c (list %s 2 3))) (setcdr (cddr c) c) c)' % atom(mb, ub)
    return '(let ((v (vector 1 nil))) (aset v 1 v) v)'

def settings(allow_circle=True):
    s = []
    if R.random() < 0.3: s.append('(print-length %s)' % R.choice(['0', '1', '2', '3', '-1', 'nil', '2.5']))
    if R.random() < 0.3: s.append('(print-level %s)' % R.choice(['0', '1', '2', '3', '-1', 'nil']))
    if allow_circle and R.random() < 0.3: s.append('(print-circle t)')
    if R.random() < 0.2: s.append('(print-quoted nil)')
    if R.random() < 0.2: s.append('(print-escape-newlines t)')
    if R.random() < 0.2: s.append('(print-escape-control-characters t)')
    if R.random() < 0.2: s.append('(print-escape-nonascii t)')
    if R.random() < 0.2: s.append('(print-escape-multibyte t)')
    if R.random() < 0.2: s.append('(print-gensym t)')
    if R.random() < 0.2: s.append('(print-integers-as-characters t)')
    if R.random() < 0.1: s.append('(float-output-format %s)' % R.choice(['"%.3f"', '"%.2e"', '"%.0f"', '"%.5g"', '"bad"', '"%.20f"']))
    if R.random() < 0.1: s.append('(print-charset-text-property %s)' % R.choice(['t', 'nil']))
    if allow_circle and R.random() < 0.08: s.append('(print-continuous-numbering t)')
    if R.random() < 0.1: s.append('(print-symbols-bare t)')
    return s

SETVARS = ["(setq print-length 1)", "(setq print-length nil)", "(setq print-level 1)", "(setq print-quoted (not print-quoted))",
           "(setq print-escape-newlines t)", "(setq print-escape-control-characters t)", "(setq print-gensym t)",
           "(setq print-integers-as-characters t)", "(setq float-output-format \"%.2f\")", "(setq print-escape-multibyte t)",
           "(setq print-escape-nonascii t)"]

def unreadable(mode):
    """print-unreadable-function: MODE says whether prints into buffers may nest."""
    k = R.random()
    if k < 0.4: return None
    if k < 0.5: return '(lambda (_o _e) "U")'
    if k < 0.55: return '(lambda (_o _e) nil)'
    if k < 0.60: return '(lambda (_o _e) t)'
    if k < 0.65: return '(lambda (_o _e) "")'
    if k < 0.70: return '(lambda (o e) (format "<%s>" (if e "E" "P")))'
    if k < 0.75: return '(lambda (_o _e) (princ "{out}") nil)'
    if k < 0.80: return '(lambda (_o _e) (message "msg") nil)'
    if k < 0.85: return '(lambda (_o _e) (set-buffer other) nil)'
    if k < 0.90: return '(lambda (_o _e) %s nil)' % R.choice(SETVARS)
    if k < 0.95 and mode == 'nest': return '(lambda (_o _e) (prin1-to-string %s) %s)' % (R.choice(["'zz", "'(a b)", '""', '12345', "(intern \"\")"]), R.choice(['nil', '"R"']))
    if k < 0.97: return '(lambda (_o _e) (error "unreadable"))'
    return '(lambda (_o _e) (prin1 (quote inner) (function ignore)) nil)'

def sink_fn():
    """A function to print to: collects, and at some character does something."""
    n = R.randint(1, 25)
    k = R.random()
    if k < 0.35: act = ''
    elif k < 0.55: act = '(when (= n %d) %s)' % (n, R.choice(SETVARS))
    elif k < 0.65: act = "(when (= n %d) (throw 'out 'thrown))" % n
    elif k < 0.75: act = '(when (= n %d) (error "sink %%d" n))' % n
    elif k < 0.82: act = "(when (= n %d) (push (prin1-to-string '(nested %s)) acc))" % (n, R.choice(['1', '"s"', '(a (b))', '[v]']))
    elif k < 0.88: act = "(when (= n %d) (catch 'in (prin1 '(p q r s) (lambda (d) (when (= d ?r) (throw 'in nil))))))" % n
    elif k < 0.92: act = '(when (= n %d) (set-buffer other))' % n
    elif k < 0.96: act = '(when (= n %d) (princ "{mid}"))' % n
    else: act = '(when (= n %d) (garbage-collect))' % n
    return '(lambda (c) (setq n (1+ n)) (push c acc) %s)' % act

def program(i):
    mb = R.random() < 0.5
    ub = R.random() < 0.3
    lines = [';;; -*- lexical-binding: t -*-',
             '(defvar other (get-buffer-create "other"))',
             '(defvar mk (with-current-buffer (get-buffer-create "mkbuf") (insert "abc") (copy-marker 2)))',
             '(defvar bf (get-buffer-create "named buffer"))',
             '(defvar killed (let ((b (generate-new-buffer "k"))) (kill-buffer b) b))',
             '(defvar ov (with-current-buffer other (make-overlay 1 1)))',
             '(defvar n 0) (defvar acc nil)',
             ';; The print stack grown once, so that no print run inside another moves it.',
             '(let ((deep nil)) (dotimes (_ 150) (setq deep (list deep))) (prin1-to-string deep))',
             '(defun out (tag v) (princ (format "\\n[%s %S]" tag v)))']
    for step in range(R.randint(1, 4)):
        kind = R.choice(['stdout', 'stdout', 'buffer', 'unibuffer', 'marker', 'function', 'function', 'string', 'format', 'errmsg', 'message', 'hookbuf', 'table', 'chars'])
        if kind == 'hookbuf':
            hook = R.choice(["(format \"%S\" 'h)", "(prin1-to-string '(a b c))", "(prin1 'x other)", "(princ \"str\" other)", "(format \"%S\" \"a much longer text than that\")",
                             "(prin1-to-string 12345)", "(error-message-string '(error \"e\" 1))", "(set-buffer other)", "(prin1 'y (function ignore))", "(terpri other)"])
            o2 = R.choice(["'outer-text", "'(a b c)", '"str"', '12345', "'(1 (2 (3)))", '""', "'ab", "[v w]", "(list 'q \"s\" 1.5)"])
            pre = R.choice(['', '(insert "PRE")', '(insert "0123456789") (goto-char 4)'])
            first = R.choice(['before-change-functions', 'before-change-functions', 'first-change-hook'])
            hookfn = '(lambda (&rest _) %s)' % hook
            lines.append('(condition-case err (let ((l nil)) (with-temp-buffer %s (add-hook \'%s %s nil t) (add-hook \'after-change-functions (lambda (&rest a) (push a l)) nil t) (%s %s (current-buffer)) (%s %s (current-buffer)) (out "hook" (list l (buffer-string) (buffer-name) (with-current-buffer other (buffer-string)))))) (error (out "error" err)))'
                         % (pre, first, hookfn, R.choice(['prin1', 'princ', 'print']), o2, R.choice(['prin1', 'princ']), R.choice(["'second", '""', '7'])))
            lines.append('(out "after" (list (buffer-name) (terpri nil t)))')
            continue
        if kind == 'table':
            ents = ' '.join('(aset standard-display-table %s %s)' % (R.choice(['?a', '?b', '?\\(', '?\\n', '?1', '?\\"', '#xe9', '?\\s']), R.choice(['(vector ?< ?>)', '(vector)', "(vector ?x 'y ?z)", '(vector #xe9)', '"str"', '?Q', '(vector ?\\n)'])) for _ in range(R.randint(0, 3)))
            cod = R.choice(['', '', "(coding-system-for-write 'utf-8)", "(coding-system-for-write 'latin-1)"])
            o2 = obj(R.randint(0, 2), True, True)
            lines.append('(condition-case err (let ((standard-display-table (make-display-table)) %s) %s (%s %s) (external-debugging-output ?a) (out "table" (terpri nil t)) (write-char %s) (message "tm")) (error (out "error" err)))'
                         % (cod, ents, R.choice(['prin1', 'princ', 'print']), o2, R.choice(['?a', '?\\n', '#xe9', '#x3fffe9', '#x8000041'])))
            lines.append('(out "after" (list (buffer-name) (terpri nil t)))')
            continue
        if kind == 'chars':
            seq = ' '.join(R.choice(['(write-char ?a)', '(write-char ?\\n)', '(princ "s")', '(princ "s\\n")', '(princ "")', '(terpri)', '(prin1 5)', "(prin1 'x)", '(prin1 "q\\n")', '(print 1)',
                                     '(out "e" (terpri nil t))', '(message "m")', '(write-char #xe9)', '(write-char -1)', "(write-char ?z (function external-debugging-output))",
                                     "(princ 'sym (function external-debugging-output))", '(external-debugging-output ?\\n)', '(flush-standard-output)', '(prin1 (intern ""))',
                                     "(prin1 (make-marker))", "(princ (current-buffer))", '(prin1 1.5)', "(terpri (function external-debugging-output))", '(write-char ?b other)',
                                     '(out "eb" (terpri other t))', '(princ "x" t)', '(prin1 nil)', "(prin1 '(a . b))", '(prin1 [])']) for _ in range(R.randint(3, 12)))
            lines.append('(condition-case err (progn %s) (error (out "error" err)))' % seq)
            lines.append('(out "after" (list (buffer-name) (terpri nil t)))')
            continue
        nest_ok = kind in ('function',) or not mb and not ub
        sets = settings(allow_circle=(kind != 'errmsg'))
        ur = unreadable('nest' if (kind in ('function', 'stdout') or (not mb and not ub)) else 'plain')
        fn = sink_fn() if kind == 'function' else ''
        nests = any(w in (ur or '') + fn for w in ('prin1', 'format'))
        o = obj(R.randint(0, 1) if nests else R.randint(0, 3), mb and kind != 'unibuffer' or kind in ('stdout', 'function', 'string', 'format'), ub and kind in ('stdout', 'function'))
        if nests and any(w in o for w in ('propertize', "'\\`", "'\\,", 'make-hash-table')):
            o = "(list 'a \"b\" (list 1 2.5) [v] mk bf)"
        if ur: sets.append('(print-unreadable-function %s)' % ur)
        printer = R.choice(['prin1', 'prin1', 'princ', 'print'])
        lines.append('(setq n 0 acc nil)')
        if kind == 'stdout':
            body = '(%s %s) (out "terpri" (terpri nil t))' % (printer, o)
        elif kind in ('buffer', 'unibuffer'):
            uni = '(set-buffer-multibyte nil)' if kind == 'unibuffer' else ''
            pre = R.choice(['', '(insert "pre")', '(insert "pre\\n")', '(insert "ab") (goto-char 2)'])
            body = '(with-temp-buffer %s %s (%s %s (current-buffer)) (out "ensure" (terpri (current-buffer) t)) (out "buf" (list (point) (append (buffer-string) nil) (buffer-name))))' % (uni, pre, printer, o)
        elif kind == 'marker':
            body = '(with-temp-buffer (insert "0123456789") (let ((m (copy-marker %d %s))) (goto-char %d) (%s %s m) (out "marker" (list m (point) (buffer-string)))))' % (R.randint(1, 11), R.choice(['nil', 't']), R.randint(1, 11), printer, o)
        elif kind == 'function':
            body = "(out \"caught\" (catch 'out (%s %s %s))) (out \"chars\" (list n (reverse acc) (buffer-name)))" % (printer, o, fn)
        elif kind == 'string':
            body = '(out "string" (prin1-to-string %s %s))' % (o, R.choice(['nil', 't']))
        elif kind == 'format':
            body = '(out "format" (format "%s" %s))' % (R.choice(['%S', '%s', '<%S|%s>', '%10S', '%-8s|']), ' '.join([o, o][:1]) if True else o)
            if '<%S|%s>' in body: body = '(let ((o %s)) (out "format" (format "<%%S|%%s>" o o)))' % o
        elif kind == 'errmsg':
            body = '(out "errmsg" (error-message-string (list %s %s %s)))' % (R.choice(["'error", "'wrong-type-argument", "'file-error", "'user-error", "'end-of-file", "'args-out-of-range", "'no-such-error-zz"]), o, atom(mb, False))
        else:
            body = '(message "%s" %s)' % (R.choice(['%S', '%s']), o)
        form = '(let (%s) %s)' % (' '.join(sets), body) if sets else '(progn %s)' % body
        lines.append('(condition-case err %s (error (out "error" err)))' % form)
        lines.append('(out "after" (list (buffer-name) (terpri nil t)))')
    lines.append('(terpri)')
    return '\n'.join(lines) + '\n'

for i in range(count):
    open(os.path.join(outdir, 'z%04d.el' % i), 'w').write(program(i))
