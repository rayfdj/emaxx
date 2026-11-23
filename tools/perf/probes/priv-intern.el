;;; -*- lexical-binding: t -*-
;; A miss in a private obarray: intern_driver's Fmake_symbol and
;; intern_sym, then Funintern (D14: 5,132 against GNU's 2,424 in round 374).
(defun probe-pi (n) (let ((i 0) (ob (obarray-make)) (r nil)) (while (< i n) (setq r (intern "x" ob)) (unintern "x" ob) (setq i (1+ i))) r))
(byte-compile 'probe-pi)
(unless (equal (symbol-name (probe-pi @N@)) "x") (error "priv-intern"))
(princ "DIV 1\n")
