(setq audit-form
      '(list (progn (setcdr (cddr audit-form) (list 3)) 1) 2))
(prin1 (eval audit-form))
(terpri)
