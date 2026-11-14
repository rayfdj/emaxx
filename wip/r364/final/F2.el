(let ((print-unreadable-function
       (lambda (o e)
         (condition-case nil
             (let ((print-unreadable-function (lambda (o e) (error "x"))))
               (prin1-to-string (list 1 (make-marker) 2)))
           (error nil))
         nil)))
  (princ (prin1-to-string (list 'a (make-marker) 'c))))
