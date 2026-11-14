;; print.c:2274: make_fixnum (- n) of the most negative fixnum is that fixnum again.
(let ((x (list 'x)) (print-circle t) (print-continuous-numbering t) (print-number-table (make-hash-table :test 'eq)))
  (puthash x most-negative-fixnum print-number-table)
  (prin1 (list x x)) (terpri))
