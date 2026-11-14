(setq standard-display-table (make-display-table))
(condition-case nil (let ((coding-system-for-write 'nosuch-coding-zz)) (princ "aé")) (error nil))
(message "m")
