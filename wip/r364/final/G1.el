(let ((tb (generate-new-buffer "tb")) (l nil))
  (with-temp-buffer
    (add-hook 'before-change-functions (lambda (&rest _) (prin1 'hk tb)) nil t)
    (add-hook 'after-change-functions (lambda (&rest a) (push a l)) nil t)
    (prin1 'outer-text (current-buffer))
    (prin1 l)))
