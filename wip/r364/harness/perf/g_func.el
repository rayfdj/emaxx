(let ((obj (list 'alpha "beta" 12345 2.5 [x y z] '(nested (list here)) :key nil t)) (n 0) (g0 gcs-done))
  (dotimes (_ 200000) (prin1 obj (lambda (c) (setq n (1+ n)))))
  (princ (list n (- gcs-done g0) gc-elapsed)))
