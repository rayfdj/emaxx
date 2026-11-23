;;; -*- lexical-binding: t -*-
(defun probe-sm (n) (let ((i 0) (r 0)) (while (< i n) (if (string-match "x" "abc") (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-sm)
(unless (= (probe-sm @N@) 0) (error "string-match"))
(princ "DIV 1\n")
