;;; -*- lexical-binding: t; -*-
(require 'comp)
;; Ffuncall's exit debugger (eval.c:Ffuncall, `backtrace_debug_on_exit'
;; then call_debugger with (exit VAL)) for a frame of native code's
;; funcall, three ways: a subr that marks its own frame (the plain
;; path's subr arm), a native function that marks its own (the plain
;; path's native arm) and an interpreted one (the full path).  The
;; debugger switches threads before it answers; another thread runs,
;; and the funcall then pops its frame and returns the debugger's
;; value.  Each thread counts its backtrace's frames before the call
;; and after it.
(defvar exit-seen nil)
(defvar exit-native nil)
(defvar exit-caller nil)
(defvar exit-other 0)
(defun exit-interpreted (x _) (backtrace-debug 1 t) (list 'interpreted x))
(defun exit-frames ()
  (let ((count 0))
    (mapbacktrace (lambda (&rest _) (setq count (1+ count))))
    count))
(defun exit-run (call)
  (let* ((before (exit-frames))
         (value (funcall call))
         (after (exit-frames)))
    (list value (= before after))))
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq exit-native
        (native-compile
         '(lambda (x _) (backtrace-debug 0 t) (list 'native x))))
  (setq exit-caller
        (native-compile '(lambda (f a b) (list (funcall f a b) 'after)))))
(setq debugger
      (lambda (&rest args)
        (thread-yield)
        (push (car args) exit-seen)
        (list 'replaced (cadr args))))
(let ((threads
       (list (make-thread
              (lambda ()
                (exit-run
                 (lambda () (funcall exit-caller #'backtrace-debug 0 t)))))
             (make-thread
              (lambda ()
                (exit-run
                 (lambda () (funcall exit-caller exit-native 1 nil)))))
             (make-thread
              (lambda ()
                (exit-run
                 (lambda () (funcall exit-caller #'exit-interpreted 2 nil)))))
             (make-thread (lambda ()
                            (dotimes (_ 6)
                              (setq exit-other (1+ exit-other))
                              (thread-yield))
                            exit-other)))))
  (prin1 (list (native-comp-function-p exit-native)
               (native-comp-function-p exit-caller)
               (interpreted-function-p (symbol-function 'exit-interpreted))
               (mapcar #'thread-join threads)
               exit-seen))
  (terpri))
