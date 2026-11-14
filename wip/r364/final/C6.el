(let ((acc nil) (h (make-hash-table)))
  (puthash 'a 1 h) (puthash 'b 2 h)
  (prin1 h (lambda (c) (push c acc) (when (= c ?#) (puthash 'c 3 h))))
  (princ (concat (nreverse acc))))
