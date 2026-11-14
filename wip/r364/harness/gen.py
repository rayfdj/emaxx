#!/usr/bin/env python3
"""gen.py SEED COUNT DIR: COUNT fuzz programs of printing (z0000.el ...).
Each is a run of cases, every case under condition-case, its text ending in
a marker line; the sinks are functions (which change the printed object,
the print variables, run prints of their own, throw or signal at a chosen
character), buffers, markers and stdout."""
import random, sys, os
seed, count, out = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
R = random.Random(seed)
os.makedirs(out, exist_ok=True)

OBJS = [
    "1", "-17", "0.5", "-0.0", "1.0e+INF", "0.0e+NaN", "(expt 2 70)", "?a", "nil", "t",
    "'sym", "'Sym-Bol", "(intern \"\")", "(intern \"a b\")", "(intern \"1e\")", "(intern \"-\")",
    "(intern \"..\")", "(intern \"?x\")", "(intern \"#x10\")", "(intern \"é\")", "(make-symbol \"g\")",
    "':key", "\"str\"", "\"a\\nb\"", "\"q\\\"q\"", "\"é\"", "(string-to-unibyte \"a\\377\")",
    "(string-to-multibyte \"a\\377\")", "\"\"", "(propertize \"abc\" 'k 'v)",
    "(let ((s (propertize \"abc\" 'k 'v))) (put-text-property 1 2 'self s s) s)",
    "(list 1 2 3)", "'(a . b)", "'(quote x)", "''x", "'(function f)", "'(\\` (a (\\, b) (\\,@ c)))",
    "(list 1 (list 2 (list 3 (list 4))))", "[1 2 3]", "[]", "(vector 1 \"s\" 'a [2])",
    "(let ((x (list 1 2 3))) (setcdr (cddr x) x) x)", "(let ((x (list 1 2 3))) (setcar (cdr x) x) x)",
    "(let ((x (list 1 2 3 4 5 6 7))) (setcdr (nthcdr 6 x) (nthcdr 2 x)) x)",
    "(let ((x (list 1))) (list x x x))", "(let ((v (make-vector 2 nil))) (aset v 0 v) v)",
    "(let ((h (make-hash-table))) (puthash 'a 1 h) (puthash 'b 2 h) h)",
    "(let ((h (make-hash-table :test 'equal))) (puthash \"k\" '(1 2) h) h)",
    "(let ((h (make-hash-table))) (puthash h h h) h)", "(make-hash-table :weakness 'key)",
    "(bool-vector t nil t)", "(make-bool-vector 16 nil)", "(bool-vector)", "(make-char-table 'x)",
    "(record 'r 1 \"two\")", "(make-marker)", "(current-buffer)", "(obarray-make)",
    "(symbol-function 'car)", "(lambda (x) x)", "(make-string 5 ?x)", "(list ?a ?\\n 160 #x3fffff)",
    "(let ((m (make-marker))) (set-marker-insertion-type m t) m)", "(list 0.0e+NaN 160)",
    "(let ((g (make-symbol \"g\")) (h (make-symbol \"g\"))) (list g h g (intern \"g\")))",
    "(list (current-buffer) '(1 (2 (3 (4)))))", "(string-to-unibyte (propertize \"u\" 'p 1))",
]
VARS = [
    ("print-length", ["nil", "1", "2", "0", "'x"]), ("print-level", ["nil", "1", "2", "0", "-1", "1.5", "(1+ most-positive-fixnum)"]),
    ("print-circle", ["nil", "t"]), ("print-gensym", ["nil", "t", "'x"]), ("print-quoted", ["nil", "t"]),
    ("print-escape-newlines", ["nil", "t"]), ("print-escape-nonascii", ["nil", "t"]),
    ("print-escape-multibyte", ["nil", "t"]), ("print-escape-control-characters", ["nil", "t"]),
    ("print-integers-as-characters", ["nil", "t"]), ("print-symbols-bare", ["nil", "t"]),
    ("print-continuous-numbering", ["nil", "t"]), ("print-charset-text-property", ["nil", "t", "'default"]),
    ("float-output-format", ["nil", "\"%5.2f\"", "\"%.3e\""]),
    ("print-number-table", ["nil", "'x", "(let ((h (make-hash-table :test 'eq))) (puthash 'sym \"STR\" h) h)"]),
]
UNREADABLE = [
    "nil", "(lambda (o e) nil)", "(lambda (o e) \"R\")", "(lambda (o e) t)", "(lambda (o e) \"\")",
    "(lambda (o e) (format \"<%s %S>\" (type-of o) e))", "(lambda (o e) (prin1-to-string 'zz))",
    "(lambda (o e) (prin1-to-string 'zz) nil)",
    "(lambda (o e) (with-temp-buffer (prin1 (quote inner-print) (current-buffer)) \"R\"))",
    "(lambda (o e) (setq print-escape-newlines t print-gensym t print-quoted nil) nil)", "\"notfn\"",
]
PRINTERS = ["prin1", "princ", "print"]
REBIND = ("(let ((print-length print-length) (print-level print-level) (print-escape-newlines print-escape-newlines) "
          "(print-escape-nonascii print-escape-nonascii) (print-escape-multibyte print-escape-multibyte) "
          "(print-quoted print-quoted) (print-gensym print-gensym) (print-integers-as-characters print-integers-as-characters) "
          "(float-output-format float-output-format) (print-escape-control-characters print-escape-control-characters) "
          "(print-symbols-bare print-symbols-bare) (print-circle print-circle) "
          "(print-charset-text-property print-charset-text-property)) %s)")
ACTIONS = [
    "(setq print-length 2)", "(setq print-length nil)", "(setq print-level 1)", "(setq print-escape-newlines t)",
    "(setq print-quoted nil)", "(setq print-gensym t)", "(setq print-circle t)", "(setq print-charset-text-property t)",
    "(setq print-escape-multibyte t)", "(prin1-to-string (list 1 (list 2)))", "(format \"%S\" 'q)",
    "(prin1 'in (function ignore))", "(princ \"plain\" (function ignore))", "(throw (quote stop) nil)", "(error \"boom\")",
    "(with-temp-buffer (prin1 '(x y) (current-buffer)))", "(garbage-collect)", "(set-buffer (get-buffer-create \"fzother\"))",
]

def obj():
    if R.random() < 0.25:
        return "(list %s %s)" % (R.choice(OBJS), R.choice(OBJS))
    if R.random() < 0.1:
        return "(vector %s %s %s)" % (R.choice(OBJS), R.choice(OBJS), R.choice(OBJS))
    return R.choice(OBJS)

def bindings():
    picks = R.sample(VARS, R.randint(0, 5))
    b = ["(%s %s)" % (n, R.choice(vs)) for n, vs in picks]
    if R.random() < 0.4:
        b.append("(print-unreadable-function %s)" % R.choice(UNREADABLE))
    return b

def wrap(b, body):
    return "(let (%s) %s)" % (" ".join(b), body) if b else body

def case_function():
    p = R.choice(PRINTERS)
    n = R.randint(1, 14)
    act = R.choice(ACTIONS)
    body = ("(let ((acc nil) (n 0)) (unwind-protect (catch (quote stop) (%s %s (lambda (c) (push c acc) (setq n (1+ n)) "
            "(when (= n %d) %s)))) (princ (format \"<%%S>\" (reverse acc)))))" % (p, obj(), n, act))
    return wrap(bindings(), REBIND % body)

def case_symbol_function():
    body = ("(let ((acc nil)) (defalias (quote my-stream-fn) (lambda (c) (push c acc))) "
            "(unwind-protect (%s %s 'my-stream-fn) (princ (format \"<%%S>\" (reverse acc)))))" % (R.choice(PRINTERS), obj()))
    return wrap(bindings(), body)

def case_buffer():
    body = ("(with-temp-buffer (insert \"pre|post\") (goto-char 5) (unwind-protect (%s %s (current-buffer)) "
            "(princ (format \"<%%S %%d>\" (buffer-string) (point)))))" % (R.choice(PRINTERS), obj()))
    return wrap(bindings(), body)

def case_unibyte_buffer():
    body = ("(with-temp-buffer (set-buffer-multibyte nil) (unwind-protect (%s %s (current-buffer)) "
            "(princ (format \"<%%S>\" (append (buffer-string) nil)))))" % (R.choice(PRINTERS), obj()))
    return wrap(bindings(), body)

def case_marker():
    ins = R.choice(["nil", "t"])
    pt = R.choice(["1", "3", "5"])
    body = ("(with-temp-buffer (insert \"abcdef\") (let ((m (copy-marker 3 %s))) (goto-char %s) (unwind-protect "
            "(%s %s m) (princ (format \"<%%S %%d %%S>\" (buffer-string) (point) (marker-position m))))))"
            % (ins, pt, R.choice(PRINTERS), obj()))
    return wrap(bindings(), body)

def case_stdout():
    p = R.choice(PRINTERS)
    tail = R.choice(["(princ (if (terpri nil t) \"|T\" \"|N\"))", "(terpri)", "(write-char ?w)", "(princ \"\")", ""])
    return wrap(bindings(), "(progn (%s %s) %s)" % (p, obj(), tail))

def case_standard_output():
    body = ("(with-temp-buffer (insert \"xy\") (let ((standard-output (point-min-marker))) (unwind-protect (progn (%s %s) "
            "(terpri) (write-char ?w) (princ (list (terpri nil t)) (function ignore))) (let ((standard-output t)) "
            "(princ (format \"<%%S>\" (buffer-string)))))))" % (R.choice(PRINTERS), obj()))
    return wrap(bindings(), body)

def case_to_string():
    f = R.choice(["(prin1-to-string %s)", "(prin1-to-string %s t)", "(format \"%%S\" %s)", "(format \"%%s\" %s)",
                  "(format \"<%%S|%%5S|%%-8S>\" %s 1 (quote a))", "(error-message-string (list 'error \"m\" %s))"])
    return wrap(bindings(), "(princ %s)" % (f % obj()))

def case_hooks():
    hook = R.choice(["(format \"%S\" 'hk)", "(prin1-to-string '(a longer piece))", "(prin1 'x (function ignore))", "nil"])
    body = ("(let ((l nil)) (with-temp-buffer (insert \"0123\") (goto-char 3) (add-hook 'before-change-functions (lambda (&rest _) %s) nil t) "
            "(add-hook 'after-change-functions (lambda (&rest a) (push a l)) nil t) (%s %s (current-buffer)) "
            "(princ (format \"<%%S %%S>\" l (buffer-string)))))" % (hook, R.choice(PRINTERS), R.choice(["'sym", "\"text\"", "'(1 2)", "\"\"", "12"])))
    return wrap(bindings(), body)

CASES = [(case_function, 5), (case_symbol_function, 1), (case_buffer, 2), (case_unibyte_buffer, 1), (case_marker, 2),
         (case_stdout, 3), (case_standard_output, 1), (case_to_string, 2), (case_hooks, 1)]
POOL = [c for c, w in CASES for _ in range(w)]
for i in range(count):
    lines = [";; -*- lexical-binding: t -*-"]
    for k in range(14):
        lines.append("(condition-case err (progn %s) (error (princ (format \"{ERR %%S}\" err))))" % R.choice(POOL)())
        lines.append("(princ \"\\n%d:\")" % k)
    open(os.path.join(out, "z%04d.el" % i), "w").write("\n".join(lines) + "\n")
print("wrote", count, "programs to", out)
