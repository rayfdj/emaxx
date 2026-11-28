;;; -*- lexical-binding: t; -*-
(require 'comp)
;; eval.c:funcall_lambda for a native function compiled with dynamic
;; binding: its parameter is specbound before the call and unbound after
;; it, and a variable watcher on the parameter runs each time.  The
;; watcher calls a native function that switches threads; another
;; thread runs, and the binding, the call and the unbinding go on.  Each
;; thread's function binds a parameter of its own.
(defvar watch-a nil)
(defvar watch-b nil)
(defvar watch-seen nil)
(defvar watch-other 0)
(defvar watch-yielder nil)
(defvar watch-first nil)
(defvar watch-second nil)
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq watch-yielder
        (native-compile '(lambda (x) (thread-yield) x)))
  (let ((lexical-binding nil))
    (setq watch-first
          (native-compile '(lambda (watch-a) (list watch-a 'body))))
    (setq watch-second
          (native-compile '(lambda (watch-b) (list watch-b 'body))))))
(defun watch-note (symbol new operation _where)
  (when (memq operation '(let unlet))
    (funcall watch-yielder new)
    (setq watch-seen (cons (list symbol operation new) watch-seen))))
(add-variable-watcher 'watch-a #'watch-note)
(add-variable-watcher 'watch-b #'watch-note)
(let* ((threads
        (list (make-thread (lambda () (funcall watch-first 1)))
              (make-thread (lambda () (funcall watch-second 2)))
              (make-thread (lambda ()
                             (dotimes (_ 10)
                               (setq watch-other (1+ watch-other))
                               (thread-yield))
                             watch-other))))
       (results (mapcar #'thread-join threads)))
  (prin1 (list (native-comp-function-p watch-first)
               (native-comp-function-p watch-yielder)
               (func-arity watch-first)
               results
               (sort watch-seen
                     (lambda (x y)
                       (string< (format "%S" x) (format "%S" y))))))
  (terpri))
