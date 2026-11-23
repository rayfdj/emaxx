;;; -*- lexical-binding: t -*-
;; mapc calling a byte-compiled function, itself called from byte code:
;; the call from C into byte code with no interpreter environment above.
(defun probe-cb (s) s)
(defun probe-run (n list) (dotimes (_ n) (mapc #'probe-cb list)))
(byte-compile 'probe-cb)
(byte-compile 'probe-run)
(defvar probe-list (make-list 1000 1))
(unless (equal (mapcar #'probe-cb '(1 2)) '(1 2)) (error "mapc-bc-bc"))
(probe-run @N@ probe-list)
(princ "DIV 1000\n")
