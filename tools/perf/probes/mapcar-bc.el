;;; -*- lexical-binding: t -*-
;; mapcar calling a byte-compiled function: mapcar1 and the cons of each result.
(defun probe-cb (s) s)
(byte-compile 'probe-cb)
(defvar probe-list (make-list 1000 1))
(unless (equal (mapcar #'probe-cb '(1 2)) '(1 2)) (error "mapcar-bc"))
(dotimes (_ @N@) (mapcar #'probe-cb probe-list))
(princ "DIV 1000\n")
