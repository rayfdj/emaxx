(let ((s (propertize "abcd" 'k 'v)) (acc nil) (print-charset-text-property t))
  (prin1 s (lambda (c) (push c acc) (when (= c ?b) (set-text-properties 0 4 nil s))))
  (princ (concat (nreverse acc))))
