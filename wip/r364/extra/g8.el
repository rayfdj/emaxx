;; A read-only destination: the error comes from print_finish, after the walk.
(let ((n 0))
  (with-temp-buffer
    (setq buffer-read-only t)
    (let ((print-unreadable-function (lambda (_o _e) (setq n (1+ n)) nil)))
      (prin1 (list (condition-case e (prin1 (list 1 (make-marker)) (current-buffer)) (error e)) n (buffer-string)
                   (condition-case e (princ "" (current-buffer)) (error e))
                   (prin1-to-string '(still works)))))))
