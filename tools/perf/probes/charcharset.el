;;; -*- lexical-binding: t -*-
;; (char-charset ?é) (the mirrors plan: 634.6 against GNU's 535.0).
(defun probe-cc (n) (let ((i 0) (r 0)) (while (< i n) (when (char-charset ?é) (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-cc)
(unless (= (probe-cc @N@) @N@) (error "charcharset"))
(princ "DIV 1\n")
