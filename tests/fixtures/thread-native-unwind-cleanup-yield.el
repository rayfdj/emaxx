;;; -*- lexical-binding: t; -*-
(require 'comp)
;; eval.c:unwind_to_catch and comp.c:helper_unbind_n from native code:
;; a native function's unwind-protect whose cleanup, native too, calls a
;; native function that switches threads.  The body ends normally (the
;; cleanup run by helper_unbind_n) or signals from native code's own
;; type check (wrong-type-argument, unwound to the handler outside past
;; the cleanup).  Another thread runs while each cleanup is parked; then
;; the cleanup ends and the call returns or the signal is caught.
(defvar unwind-cleanups nil)
(defvar unwind-other 0)
(defvar unwind-yielder nil)
(defvar unwind-body nil)
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq unwind-yielder
        (native-compile
         '(lambda (tag)
            (thread-yield)
            (setq unwind-cleanups (cons tag unwind-cleanups))
            tag)))
  (setq unwind-body
        (native-compile
         '(lambda (tag x)
            (unwind-protect
                (car x)
              (funcall unwind-yielder tag))))))
(defun unwind-run (tag)
  (list (funcall unwind-body tag (list tag))
        (condition-case err
            (funcall unwind-body (+ tag 10) tag)
          (wrong-type-argument (list 'caught (cadr err) (caddr err))))))
(let* ((threads
        (list (make-thread (lambda () (unwind-run 1)))
              (make-thread (lambda () (unwind-run 2)))
              (make-thread (lambda ()
                             (dotimes (_ 10)
                               (setq unwind-other (1+ unwind-other))
                               (thread-yield))
                             unwind-other))))
       (results (mapcar #'thread-join threads)))
  (prin1 (list (native-comp-function-p unwind-body)
               (native-comp-function-p unwind-yielder)
               results
               (sort unwind-cleanups #'<)))
  (terpri))
