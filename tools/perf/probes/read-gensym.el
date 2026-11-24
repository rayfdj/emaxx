;;; -*- lexical-binding: t -*-
;; `read' of "#:x": the reader's Fmake_symbol of the name it read (D14).
(defun probe-rg (n) (let ((i 0) (r nil)) (while (< i n) (setq r (read "#:x")) (setq i (1+ i))) r))
(byte-compile 'probe-rg)
(unless (equal (symbol-name (probe-rg @N@)) "x") (error "read-gensym"))
(princ "DIV 1\n")
