(with-temp-buffer
  (insert "hello world")
  (let ((m (copy-marker 8)))
    (narrow-to-region 1 4)
    (condition-case e (prin1 'x m) (error (prin1 e)))))
