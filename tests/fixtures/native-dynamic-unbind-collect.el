;;; -*- lexical-binding: t; -*-
(require 'comp)
;; eval.c:funcall_lambda for a native function compiled with dynamic
;; binding whose body signals: the signal unwinds through the function's
;; unwind-protect, then its parameter is unbound, and a watcher of the
;; parameter conses enough to collect while the signal is on its way to
;; the handler outside.  The handler gets the signal's data whole.
(defvar unbind-param nil)
(defvar unbind-log nil)
(defvar unbind-signals nil)
(defun unbind-cons (n)
  (let ((acc nil))
    (dotimes (i n) (push (cons i (make-string 2 ?a)) acc))
    (length acc)))
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (let ((lexical-binding nil))
    (setq unbind-signals
          (native-compile
           '(lambda (unbind-param x)
              (unwind-protect (car x) (setq unbind-param 'changed)))))))
(setq gc-cons-threshold 40000 gc-cons-percentage 0.0)
(add-variable-watcher
 'unbind-param
 (lambda (_symbol new operation _where)
   (when (memq operation '(let unlet))
     (unbind-cons 500)
     (push (list operation new) unbind-log))))
(let ((results
       (list (condition-case err
                 (funcall unbind-signals 2 3)
               (error (list 'err err)))
             (condition-case err
                 (funcall unbind-signals 4 (list 5 6))
               (error (list 'err err))))))
  (setq gc-cons-threshold 800000)
  (prin1 (list (native-comp-function-p unbind-signals)
               results
               unbind-param
               (reverse unbind-log)))
  (terpri))
