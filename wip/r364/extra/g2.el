;; Two before-change functions each run a print into print_buffer.
(let ((l nil))
  (with-temp-buffer
    (add-hook 'before-change-functions (lambda (&rest _) (format "%S" 'hk)) nil t)
    (add-hook 'before-change-functions (lambda (&rest _) (format "%S" 'xyz1)) t t)
    (add-hook 'after-change-functions (lambda (&rest a) (push a l)) nil t)
    (prin1 'outer-text (current-buffer))
    (prin1 (list l (buffer-string)))))
