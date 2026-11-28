;;; -*- lexical-binding: t; -*-
(require 'comp)
;; alloc.c:garbage_collect begun by lisp.h:maybe_gc in the evaluator's
;; own frames, under native code's funcall, its post-gc-hook
;; throwing to a catch in that native code (eval.c:unwind_to_catch).
;; The evaluator runs in the native call's own environment.  The native
;; function goes on and funcalls interpreted functions; then it is
;; called again with no collection.
(defvar hook-count 0)
(defvar catcher nil)
(defun hook-alloc (n)
  (let ((kept nil))
    (dotimes (i n) (push (make-list 8 i) kept))
    (length kept)))
(defun after-fn (a b) (let ((c (+ a b))) (list a b c)))
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq catcher (native-compile '(lambda (f n)
                                   (let ((r (catch 'out (funcall f n))))
                                     (list r (funcall 'after-fn 1 2) (funcall 'after-fn 3 4)))))))
(setq gc-cons-threshold 100000 gc-cons-percentage 0.0)
(setq post-gc-hook (list (lambda () (setq hook-count (1+ hook-count)) (when (= hook-count 2) (throw 'out 'thrown)))))
(let ((r (funcall catcher #'hook-alloc 5000)))
  (setq post-gc-hook nil)
  (prin1 (list r (>= hook-count 2) (funcall catcher #'hook-alloc 10))))
(terpri)
