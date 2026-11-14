(let ((ob (get-buffer-create "other")))
  (with-temp-buffer
    (rename-buffer "home")
    (let ((m (point-marker)))
      (add-hook 'after-change-functions (lambda (&rest _) (set-buffer ob)) nil t)
      (prin1 'q m)
      (prin1 (list m (with-current-buffer "home" (buffer-string)))))))
