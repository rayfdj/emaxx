(let ((bad (make-symbol "unregistered-coding-object")))
  (mapcar
   (lambda (operation)
     (condition-case failure
         (progn (funcall operation "payload" bad) 'unexpected-success)
       (error (list operation (car failure)
                    (eq (cadr failure) bad)
                    (cadr failure)
                    (length (cdr failure))))))
   '(encode-coding-string decode-coding-string)))
