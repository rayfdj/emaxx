;; Compositions at the border of what a print inserts are left alone.
(with-temp-buffer
  (insert "abcdef")
  (compose-region 2 5)
  (goto-char 3)
  (prin1 'x (current-buffer))
  (prin1 (list (buffer-string) (find-composition 2 nil nil t) (get-text-property 2 'composition)))
  (goto-char 3)
  (insert "y")
  (prin1 (list (buffer-string) (find-composition 2 nil nil t))))
