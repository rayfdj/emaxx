;;; -*- lexical-binding: t -*-
;; A one-argument recursion, lexical binding, byte code calling byte code.
(defun probe-lex-rec (n) (if (= n 0) 0 (probe-lex-rec (1- n))))
(defun probe-lex-loop (k) (let ((i 0) (s 0)) (while (< i k) (setq s (+ s (probe-lex-rec 100))) (setq i (1+ i))) s))
(mapc #'byte-compile '(probe-lex-rec probe-lex-loop))
(unless (= (probe-lex-loop @N@) 0) (error "lex-rec"))
(princ "DIV 101\n")
