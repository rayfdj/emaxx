;;; -*- lexical-binding: t -*-
;; mapc calling a subr: the call from C into C beside the one into byte
;; code (D17), through the same Ffuncall.
(defvar probe-list (make-list 1000 1))
(unless (equal (mapcar #'identity '(1 2)) '(1 2)) (error "mapc-subr"))
(dotimes (_ @N@) (mapc #'identity probe-list))
(princ "DIV 1000\n")
