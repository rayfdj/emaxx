(progn
  (defvar prog1-identity-form nil)
  (defvar prog1-identity-events nil)
  (list
   (let* ((current (vector 1))
          (original current)
          (result (prog1 current
                    (aset current 0 7)
                    (setq current (vector 2)))))
     (list (eq result original) (aref result 0) (aref current 0)))
   (let* ((current (make-bool-vector 1 nil))
          (original current)
          (result (prog1 current
                    (aset current 0 t)
                    (setq current (make-bool-vector 1 nil)))))
     (list (eq result original) (aref result 0) (aref current 0)))
   (let* ((current (make-char-table nil 'old))
          (original current)
          (result (prog1 current
                    (set-char-table-range current t 'changed)
                    (setq current (make-char-table nil 'replacement)))))
     (list (eq result original) (aref result 65) (aref current 65)))
   (let ((prog1-identity-events nil)
         (prog1-identity-form
          '(prog1
               (progn
                 (setcdr (cdr prog1-identity-form)
                         '((setq prog1-identity-events 'replacement)))
                 23)
             (setq prog1-identity-events 'original))))
     (list (eval prog1-identity-form) prog1-identity-events))
   (condition-case error-data
       (prog1 (vector 1) (error "prog1-body-error"))
     (error error-data))))
