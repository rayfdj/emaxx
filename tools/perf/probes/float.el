;;; -*- lexical-binding: t -*-
;; (float I): alloc.c:make_float, from the float free list or block.
(defun probe-fl (n) (let ((i 0) (r nil)) (while (< i n) (setq r (float i)) (setq i (1+ i))) r))
(byte-compile 'probe-fl)
(unless (= (probe-fl @N@) (float (1- @N@))) (error "float"))
(princ "DIV 1\n")
