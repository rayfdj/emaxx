(with-temp-buffer
  (insert "abcdef") (goto-char 4)
  (let ((log nil))
    (add-hook 'after-change-functions (lambda (b e l) (push (list b e l) log)) nil t)
    (princ "" (current-buffer))
    (terpri (point-min-marker) t)
    (prin1 log)))
