(let ((obj (list 'alpha "beta" 12345 2.5 [x y z] '(nested (list here)) :key nil t)))
  (dotimes (_ 400000) (prin1 obj) (terpri)))
