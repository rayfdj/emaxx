(condition-case err
    (prin1 (eval '(list 1 . 2)))
  (error (prin1 err)))
(terpri)
