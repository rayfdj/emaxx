;; Text properties' modification hooks and overlays' insertion hooks run a print.
(let ((l nil))
  (with-temp-buffer
    (insert "0123456789")
    (let ((o (make-overlay 3 7)))
      (overlay-put o 'insert-in-front-hooks (list (lambda (&rest a) (push (cons 'front (cdr a)) l) (format "%S" a))))
      (overlay-put o 'modification-hooks (list (lambda (&rest a) (push (cons 'mod (cdr a)) l) (format "%S" 'zz))))
      (goto-char 5)
      (prin1 'inside (current-buffer))
      (goto-char 3)
      (prin1 'front (current-buffer))
      (prin1 (list (reverse l) (buffer-string))))))
