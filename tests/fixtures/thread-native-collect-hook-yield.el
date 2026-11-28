;;; -*- lexical-binding: t; -*-
(require 'comp)
;; lisp.h:maybe_gc in the evaluator's own frames under native code:
;; native code funcalls an interpreted allocator (eval.c:eval_sub's
;; maybe_gc) and a byte-compiled one (bytecode.c's, at the backward
;; branch and the call), with a small gc-cons-threshold, so collections
;; begin there while the native call is active.  post-gc-hook switches
;; threads; another thread runs, and the allocator goes on.  The hook
;; inhibits collections, so each allocator has a phase of its own.  Each
;; allocating thread counts its backtrace's frames before the call and
;; after it.
(defvar collect-hooks 0)
(defvar collect-other 0)
(defvar collect-caller nil)
(defun collect-interpreted (n)
  (let ((kept nil))
    (dotimes (i n)
      (push (make-list 8 i) kept))
    (length kept)))
(defvar collect-byte-code
  (byte-compile
   '(lambda (n)
      (let ((kept nil))
        (dotimes (i n)
          (push (make-list 8 i) kept))
        (length kept)))))
(defun collect-frames ()
  (let ((count 0))
    (mapbacktrace (lambda (&rest _) (setq count (1+ count))))
    count))
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq collect-caller
        (native-compile '(lambda (f n) (list (funcall f n) 'after)))))
(defun collect-run (function)
  (let* ((before (collect-frames))
         (value (funcall collect-caller function 5000))
         (after (collect-frames)))
    (list value (= before after))))
(setq gc-cons-threshold 100000 gc-cons-percentage 0.0)
(setq post-gc-hook
      (list (lambda ()
              (setq collect-hooks (1+ collect-hooks))
              (thread-yield))))
(defun collect-phase (function)
  (setq collect-other 0)
  (let ((threads
         (list (make-thread (lambda () (collect-run function)))
               (make-thread (lambda ()
                              (dotimes (_ 20)
                                (setq collect-other (1+ collect-other))
                                (thread-yield))
                              collect-other)))))
    (mapcar #'thread-join threads)))
(let ((results (list (collect-phase #'collect-interpreted)
                     (collect-phase collect-byte-code))))
  (setq post-gc-hook nil)
  (prin1 (list (native-comp-function-p collect-caller)
               (interpreted-function-p (symbol-function 'collect-interpreted))
               (byte-code-function-p collect-byte-code)
               results
               (> collect-hooks 4)))
  (terpri))
