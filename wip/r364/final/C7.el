(let ((acc nil) (s (make-symbol "abcdef")))
  (prin1 s (lambda (c) (push c acc) (when (= c ?b) (aset (symbol-name s) 3 ?Z))))
  (princ (concat (nreverse acc))))
