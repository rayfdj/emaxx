;;; -*- lexical-binding: t -*-
;; (encode-char ?é 'latin-iso8859-1) (the mirrors plan: 733.6 against
;; GNU's 493.0).
(defun probe-ec (n) (let ((i 0) (r 0)) (while (< i n) (when (eq (encode-char ?é 'latin-iso8859-1) 105) (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-ec)
(unless (= (probe-ec @N@) @N@) (error "encodechar"))
(princ "DIV 1\n")
