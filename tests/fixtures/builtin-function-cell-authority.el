(let ((saved-identity (symbol-function 'identity))
      (saved-if (symbol-function 'if)))
  (unwind-protect
      (progn
        (fmakunbound 'identity)
        (fmakunbound 'if)
        (prin1
         (list (fboundp 'identity) (symbol-function 'identity)
               (condition-case err (funcall 'identity 5)
                 (void-function (car err)))
               (funcall saved-identity 7)
               (fboundp 'if) (symbol-function 'if)
               (condition-case err (eval '(if t 1 2))
                 (void-function (car err))))))
    (fset 'if saved-if)
    (fset 'identity saved-identity)))
