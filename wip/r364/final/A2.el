(with-temp-buffer
  (let* ((b (get-buffer-create "other"))
         (print-unreadable-function (lambda (o e) (set-buffer b) nil)))
    (prin1 (list 'a (make-marker)) (current-buffer))
    (princ (format "%S %S" (buffer-string) (with-current-buffer b (buffer-string))))))
