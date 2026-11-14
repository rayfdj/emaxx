(let ((print-circle t) (x (list 1)) (acc nil))
  (prin1 (list x x x) (lambda (c) (push c acc) (when (= c ?=) (clrhash print-number-table))))
  (princ (concat (nreverse acc))))
