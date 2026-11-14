;; The after-change function prints: the insertion is done by then.
(let ((l nil))
  (with-temp-buffer
    (add-hook 'after-change-functions (lambda (&rest a) (push (list a (format "%S" a)) l)) nil t)
    (prin1 '(outer text) (current-buffer))
    (terpri (current-buffer))
    (prin1 (list l (buffer-string)))))
