(with-temp-buffer
  (let* ((b (get-buffer-create "other"))
         (print-unreadable-function (lambda (o e) (set-buffer b) nil)))
    (prin1 (make-marker))
    (princ (buffer-name))))
