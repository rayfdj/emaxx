(setq audit-side-effect nil)
(condition-case err
    (setcar (progn (setq audit-side-effect t) (cons 1 2)))
  (error (prin1 (list (car err) audit-side-effect))))
(terpri)
