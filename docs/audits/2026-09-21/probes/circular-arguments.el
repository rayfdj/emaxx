(setq audit-args (list 1))
(setcdr audit-args audit-args)
(condition-case err
    (prin1 (eval (cons 'list audit-args)))
  (error (prin1 (car err))))
(terpri)
