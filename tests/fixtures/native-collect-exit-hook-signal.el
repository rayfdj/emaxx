;;; -*- lexical-binding: t; -*-
(require 'comp)
;; alloc.c:garbage_collect begun by lisp.h:maybe_gc in the evaluator's
;; own frames, under native code's funcall, its post-gc-hook
;; signalling, which safe_run_hooks catches (keyboard.c), under a
;; condition-case in that native code: no exit, the allocator's value.
(defvar hook-count 0)
(defvar catcher nil)
(defun hook-alloc (n)
  (eval `(let ((kept nil)) (dotimes (i ,n) (push (make-list 8 i) kept)) (length kept)) t))
(defun after-fn (a b) (let ((c (+ a b))) (list a b c)))
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq catcher (native-compile '(lambda (f n)
                                   (let ((r (condition-case e (funcall f n) (error (list 'caught (car e))))))
                                     (list r (funcall 'after-fn 1 2) (funcall 'after-fn 3 4)))))))
(setq gc-cons-threshold 100000 gc-cons-percentage 0.0)
(setq post-gc-hook (list (lambda () (setq hook-count (1+ hook-count)) (when (= hook-count 2) (error "hook-error")))))
(let ((r (condition-case e (funcall catcher #'hook-alloc 5000) (error (list 'err e)))))
  (setq post-gc-hook nil)
  (prin1 (list r (>= hook-count 2))))
(terpri)
