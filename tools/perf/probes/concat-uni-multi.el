;;; -*- lexical-binding: t -*-
;; (concat UNIBYTE MULTIBYTE): fns.c:concat_to_string copies the unibyte
;; argument (two of its bytes raw) into the multibyte result through
;; str_to_multibyte.
(defun probe-cum (n a b) (let ((i 0) (r nil)) (while (< i n) (setq r (concat a b)) (setq i (1+ i))) r))
(byte-compile 'probe-cum)
(unless (equal (probe-cum @N@ "GREEK \310SMALL\377 LETTER " (string #x3b1)) (concat "GREEK \310SMALL\377 LETTER " (string #x3b1)))
  (error "concat"))
(princ "DIV 1\n")
