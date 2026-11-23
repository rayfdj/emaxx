;;; -*- lexical-binding: t -*-
;; A miss in the initial obarray, then Funintern: intern_driver and its
;; undoing (D14: 24,914 against GNU's 2,555 in round 374).
(defun probe-in (n) (let ((i 0) (r nil)) (while (< i n) (setq r (intern "zz-probe-fresh-name")) (unintern "zz-probe-fresh-name" obarray) (setq i (1+ i))) r))
(byte-compile 'probe-in)
(unless (equal (symbol-name (probe-in @N@)) "zz-probe-fresh-name") (error "intern-new"))
(princ "DIV 1\n")
