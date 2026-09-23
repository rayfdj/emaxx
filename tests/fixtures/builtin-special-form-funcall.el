(let ((special (symbol-function 'if)))
  (prin1
   (list
    (condition-case err (funcall special t 1 2)
      (error (list (car err) (eq (cadr err) special))))
    (condition-case err (funcall special)
      (error (list (car err) (eq (cadr err) special)))))))
