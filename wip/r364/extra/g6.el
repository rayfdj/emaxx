;; A marker stream, hooks that print, point before and after the marker.
(let ((l nil))
  (with-temp-buffer
    (insert "0123456789")
    (let ((m (copy-marker 4)))
      (goto-char 8)
      (add-hook 'before-change-functions (lambda (&rest _) (format "%S" 'q)) nil t)
      (add-hook 'after-change-functions (lambda (&rest a) (push a l)) nil t)
      (prin1 'abc m)
      (prin1 (list l (buffer-string) (point) m))
      (goto-char 2)
      (princ "XY" m)
      (prin1 (list l (buffer-string) (point) m)))))
