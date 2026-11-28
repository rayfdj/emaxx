;;; -*- lexical-binding: t; -*-
(require 'comp)
;; alloc.c:garbage_collect begun by lisp.h:maybe_gc in the evaluator's
;; own frames, under native code's funcall, a finalizer
;; throwing to a catch in that native code, the allocating forms run
;; under (eval FORM t).  The native function goes on and funcalls
;; interpreted functions.
(defvar fin-count 0)
(defvar catcher nil)
(defun make-garbage-finalizers () (dotimes (_ 3) (make-finalizer (lambda () (setq fin-count (1+ fin-count)) (throw 'out 'fin-thrown)))))
(defun hook-alloc (n)
  (eval `(progn (make-garbage-finalizers) (let ((kept nil)) (dotimes (i ,n) (push (make-list 8 i) kept)) (length kept))) t))
(defun after-fn (a b) (let ((c (+ a b))) (list a b c)))
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq catcher (native-compile '(lambda (f n)
                                   (let ((r (catch 'out (funcall f n))))
                                     (list r (funcall 'after-fn 1 2) (funcall 'after-fn 3 4)))))))
(setq gc-cons-threshold 100000 gc-cons-percentage 0.0)
(let ((r (condition-case e (funcall catcher #'hook-alloc 5000) (error (list 'err e)))))
  (prin1 (list r (> fin-count 0))))
(terpri)
