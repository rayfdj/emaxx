(mapcar
 (lambda (reader)
   (let ((characters (string-to-list "delta omega"))
         (events nil)
         (calls 0)
         (name (make-symbol "changing-stream")))
     (unwind-protect
         (progn
           (fset name
                 (lambda (&optional character)
                   (push 'first events)
                   (setq calls (1+ calls))
                   (when (= calls 2)
                     (fset name
                           (lambda (&optional returned)
                             (push 'replacement events)
                             (if returned (push returned characters)
                               (pop characters)))))
                   (if character (push character characters)
                     (pop characters))))
           (let ((form (funcall reader name)))
             (list (symbol-name (bare-symbol form))
                   (concat characters) (nreverse events))))
       (fmakunbound name))))
 '(read read-positioning-symbols))
