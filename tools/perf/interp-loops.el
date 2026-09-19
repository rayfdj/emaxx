;;; -*- lexical-binding: t -*-
;; The audit's interpreted loops (checkpoints 20a on): the lexical and
;; dynamic source-interpreter loop over 2M iterations and 1M calls of an
;; interpreted defun.  emacs -Q --batch -l tools/perf/interp-loops.el
;; the pure source interpreter: no macro in the loop body
(defvar interp-form '(let ((n 0)) (let ((i 0)) (while (< i 2000000) (let* ((x (and (> i -1) i))) (if x (setq n (+ n 1)))) (setq i (1+ i)))) n))
(let ((t0 (float-time))) (eval interp-form t) (princ (format "interp lexical: %.3f s\n" (- (float-time) t0))))
(let ((t0 (float-time))) (eval interp-form nil) (princ (format "interp dynamic: %.3f s\n" (- (float-time) t0))))
(defun interp-f (a b) (+ a b))
(defvar interp-call '(let ((i 0) (n 0)) (while (< i 1000000) (setq n (interp-f n 1)) (setq i (1+ i))) n))
(let ((t0 (float-time))) (eval interp-call t) (princ (format "interp call of an interpreted defun: %.3f s\n" (- (float-time) t0))))
