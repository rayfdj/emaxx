;;; -*- lexical-binding: t -*-
;; (make-symbol "x") (HANDOVER: about 690 instructions over GNU's).
(defun probe-ms (n) (let ((i 0) (r nil)) (while (< i n) (setq r (make-symbol "x")) (setq i (1+ i))) r))
(byte-compile 'probe-ms)
(unless (equal (symbol-name (probe-ms @N@)) "x") (error "make-symbol"))
(princ "DIV 1\n")
