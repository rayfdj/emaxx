;;; -*- lexical-binding: t -*-
;; (make-vector 4 nil): alloc.c:allocate_vector of a small vector, from a
;; vector block's free lists.
(defun probe-mv (n) (let ((i 0) (r nil)) (while (< i n) (setq r (make-vector 4 nil)) (setq i (1+ i))) r))
(byte-compile 'probe-mv)
(unless (equal (probe-mv @N@) (make-vector 4 nil)) (error "make-vector"))
(princ "DIV 1\n")
