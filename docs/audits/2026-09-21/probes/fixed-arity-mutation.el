(setq audit-form
      '(cons (progn (setcdr (cdr audit-form) (list 3)) 1) 2))
(prin1 (eval audit-form))
(terpri)
