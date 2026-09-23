(setq audit-form
      '(list (progn (setcdr (cddr audit-form) (number-sequence 3 12)) 1) 2))
(prin1 (eval audit-form))
(terpri)
