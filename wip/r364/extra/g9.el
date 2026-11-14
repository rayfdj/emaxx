;; A hook that kills or switches the buffer while the text waits.
(let ((ob (generate-new-buffer "ob")) (l nil))
  (with-temp-buffer
    (add-hook 'before-change-functions (lambda (&rest _) (set-buffer ob)) nil t)
    (prin1 'moved (current-buffer))
    (prin1 (list (buffer-name) (buffer-string) (with-current-buffer ob (buffer-string))))))
