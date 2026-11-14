;; eval.c:2001 signal_error: a proper list ARG is the error's data itself.
(princ (condition-case err (terpri '(lambda (c)) t) (error err))) (terpri)
