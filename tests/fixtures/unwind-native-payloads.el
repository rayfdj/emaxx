(progn
  (require 'comp)
  (put 'unwind-native-condition 'error-conditions '(unwind-native-condition error))
  (put 'unwind-native-condition 'error-message "Native protected payload")
  (let* ((source (make-temp-file "unwind-native-payload-" nil ".el"))
         (output (make-temp-file "unwind-native-output-" t))
         (comp-no-spawn nil)
         (comp-running-batch-compilation t)
         (native-comp-jit-compilation nil))
    (unwind-protect
        (progn
          (with-temp-file source
            (insert ";;; -*- lexical-binding: t -*-\n")
            (prin1
             '(defun unwind-native-payload (exit-kind replace)
                (unwind-protect
                    (let ((payload (list (make-string 257 121)
                                         (vector (make-symbol "native-payload")
                                                 (cons 43 (list 97))))))
                      (cond ((eq exit-kind 'signal)
                             (signal 'unwind-native-condition payload))
                            ((eq exit-kind 'throw)
                             (throw 'native-payload-exit payload))
                            (t payload)))
                  (garbage-collect)
                  (when replace
                    (signal 'unwind-native-condition (list "replacement")))))
             (current-buffer)))
          (native-elisp-load
           (native-compile source (expand-file-name "unwind.eln" output)))
          (unless (native-comp-function-p (symbol-function 'unwind-native-payload))
            (error "Payload control did not compile to native code"))
          (let (answers)
            (dolist (exit-kind '(return signal throw))
              (let ((result
                     (condition-case data
                         (catch 'native-payload-exit
                           (unwind-native-payload exit-kind nil))
                       (unwind-native-condition (cdr data)))))
                (push (list (length (car result))
                            (symbol-name (aref (cadr result) 0))
                            (aref (cadr result) 1))
                      answers)))
            (list (native-comp-function-p (symbol-function 'unwind-native-payload))
                  (nreverse answers)
                  (condition-case data (unwind-native-payload 'signal t)
                    (unwind-native-condition data)))))
      (fmakunbound 'unwind-native-payload)
      (delete-file source)
      (delete-directory output t))))
