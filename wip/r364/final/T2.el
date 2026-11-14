(let ((s (copy-sequence "aébcdef")) (acc nil))
  (princ s (lambda (c) (push c acc) (when (= c ?b) (aset s 1 ?x))))
  (prin1 (nreverse acc)))
