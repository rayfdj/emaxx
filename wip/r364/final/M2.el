(with-temp-buffer
  (let* ((cur (current-buffer)) (dest (generate-new-buffer "dst"))
         (m (with-current-buffer dest (insert "abc") (copy-marker 2)))
         (print-unreadable-function (lambda (o e) (kill-buffer dest) nil)))
    (prin1 (list 'aa (make-marker)) m)
    (prin1 (list m (buffer-string)))))
