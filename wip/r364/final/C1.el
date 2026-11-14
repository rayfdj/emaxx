(let ((l (list 'a 'b 'c)) (acc nil))
  (prin1 l (lambda (c) (push c acc) (when (= c ?\() (setcar l 'z))))
  (princ (concat (nreverse acc))))
