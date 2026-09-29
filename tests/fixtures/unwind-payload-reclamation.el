(progn
  (require 'comp)
  (defvar unwind-reclamation-table nil)
  (put 'unwind-reclamation-error 'error-conditions '(unwind-reclamation-error error))
  (put 'unwind-reclamation-error 'error-message "Root reclamation")
  (let* ((comp-no-spawn nil)
         (comp-running-batch-compilation t)
         (native-comp-jit-compilation nil)
         (body '(lambda ()
                  (condition-case data
                      (unwind-protect
                          (let ((payload (vector (make-symbol "ephemeral-payload")
                                                 (cons 29 83))))
                            (puthash 'payload payload unwind-reclamation-table)
                            (signal 'unwind-reclamation-error (list payload)))
                        (garbage-collect))
                    (unwind-reclamation-error
                     (and (eq (cadr data) (gethash 'payload unwind-reclamation-table))
                          (equal (symbol-name (aref (cadr data) 0)) "ephemeral-payload")
                          (equal (aref (cadr data) 1) '(29 . 83)))))))
         (bytecode (byte-compile body))
         (native (native-compile body))
         answers)
    (unless (and (byte-code-function-p bytecode) (native-comp-function-p native))
      (error "Reclamation modes were not compiled"))
    ;; GNU's conservative native stack scan can retain the most recent
    ;; payload until the enclosing caller returns on Linux. Collect only
    ;; after all creating/calling frames have returned, while keeping each
    ;; weak table and its live-payload verdict. No payload becomes strong.
    (setq answers
          (funcall
           (lambda (functions)
             (let (results)
               (dolist (function functions (nreverse results))
                 (let ((unwind-reclamation-table
                        (make-hash-table :test 'eq :weakness 'value)))
                   (push (list (funcall function) unwind-reclamation-table)
                         results)))))
           (list body bytecode native)))
    (garbage-collect)
    (list (byte-code-function-p bytecode) (native-comp-function-p native)
          (mapcar (lambda (result)
                    (list (car result) (hash-table-count (cadr result))))
                  answers))))
