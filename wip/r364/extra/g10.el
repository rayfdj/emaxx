;; Multibyte text waiting while a hook prints.
(let ((l nil))
  (with-temp-buffer
    (add-hook 'before-change-functions (lambda (&rest _) (format "%S" 'h)) nil t)
    (add-hook 'after-change-functions (lambda (&rest a) (push a l)) nil t)
    (princ "aébc" (current-buffer))
    (prin1 (list l (append (buffer-string) nil)))))
