;; first-change-hook and a print to a buffer from it.
(let ((l nil) (tb (generate-new-buffer "tb")))
  (with-temp-buffer
    (add-hook 'first-change-hook (lambda () (princ "first" tb)) nil t)
    (add-hook 'after-change-functions (lambda (&rest a) (push a l)) nil t)
    (princ "some text" (current-buffer))
    (princ "more" (current-buffer))
    (prin1 (list l (buffer-string) (with-current-buffer tb (buffer-string))))))
