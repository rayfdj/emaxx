;;; -*- lexical-binding: t -*-
;; The audit's byte-code call loop, 10M calls (call-loop.el is the
;; 200k-call ledger probe).  emacs -Q --batch -l tools/perf/call-loop-10m.el
(defun micro-id (x) x)
(defun micro-call-loop () (dotimes (i 10000000) (micro-id i)))
(mapc #'byte-compile '(micro-id micro-call-loop))
(let ((t0 (float-time))) (micro-call-loop) (princ (format "byte-code call loop (10M): %.3f s\n" (- (float-time) t0))))
