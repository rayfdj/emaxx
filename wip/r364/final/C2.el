(let ((l (list 'quote 'b)) (acc nil))
  (prin1 l (lambda (c) (push c acc) (when (= c ?\') (setcar (cdr l) 'y))))
  (princ (concat (nreverse acc))))
