(let ((original (symbol-function 'identity)))
  (unwind-protect
      (progn
        (fset 'identity nil)
        (defalias 'function-cell-delta 'identity)
        (prin1
         (list (fboundp 'identity) (symbol-function 'identity)
               (fboundp 'function-cell-delta)
               (symbol-function 'function-cell-delta)
               (condition-case err (funcall 'identity 1) (error err))
               (condition-case err (funcall 'function-cell-delta 2) (error err))
               (funcall original 3))))
    (fset 'identity original)
    (fmakunbound 'function-cell-delta)))
