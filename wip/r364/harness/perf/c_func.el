(let ((obj (list 'alpha "beta" 12345 2.5 [x y z] '(nested (list here)) :key nil t)) (n 0))
  (dotimes (_ 20000) (prin1 obj (lambda (c) (setq n (1+ n)))))
  (princ n))
