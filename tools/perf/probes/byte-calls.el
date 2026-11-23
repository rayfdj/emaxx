;;; -*- lexical-binding: t -*-
;; The locked suite's bytecode-calls loop (compat/core_runtime_perf.el).
(defun probe-byte-add (a b) (+ a b))
(defun probe-byte-calls (n)
  (let ((i 0) (sum 0))
    (while (< i n)
      (setq sum (probe-byte-add sum i) i (1+ i)))
    sum))
(mapc #'byte-compile '(probe-byte-add probe-byte-calls))
(unless (= (probe-byte-calls @N@) (/ (* @N@ (1- @N@)) 2)) (error "byte-calls"))
(princ "DIV 1\n")
