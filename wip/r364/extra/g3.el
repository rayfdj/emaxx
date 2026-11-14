;; The hook's print is longer than the text being inserted.
(let ((l nil))
  (with-temp-buffer
    (add-hook 'before-change-functions (lambda (&rest _) (prin1-to-string '(a much longer piece of text))) nil t)
    (add-hook 'after-change-functions (lambda (&rest a) (push a l)) nil t)
    (prin1 'ab (current-buffer))
    (prin1 (list l (buffer-string)))))
