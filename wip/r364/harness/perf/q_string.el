(let ((obj (list 'alpha "beta" 12345 2.5 [x y z] '(nested (list here)) :key nil t)) (n 0))
  (dotimes (_ 400000) (setq n (+ n (length (prin1-to-string obj)))))
  (princ n))
