(with-temp-buffer
  (let ((b (get-buffer-create "other")))
    (prin1 'x (lambda (c) (set-buffer b)))
    (princ (buffer-name))))
