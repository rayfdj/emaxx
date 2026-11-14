(let ((n 0))
  (dotimes (i 1000000)
    (setq n (+ n (length (prin1-to-string 'sym)) (length (prin1-to-string i)))))
  (princ n))
