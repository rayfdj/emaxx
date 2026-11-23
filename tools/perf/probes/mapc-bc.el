;;; -*- lexical-binding: t -*-
;; mapc calling a byte-compiled function: the call from C into byte code.
(defun probe-cb (s) s)
(byte-compile 'probe-cb)
(defvar probe-list (make-list 1000 1))
(unless (equal (mapcar #'probe-cb '(1 2)) '(1 2)) (error "mapc-bc"))
(dotimes (_ @N@) (mapc #'probe-cb probe-list))
(princ "DIV 1000\n")
