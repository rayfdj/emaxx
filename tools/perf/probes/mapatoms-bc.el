;;; -*- lexical-binding: t -*-
;; mapatoms with a byte-compiled callback (HANDOVER: 502 a symbol against 382).
(defun probe-cb (s) s)
(byte-compile 'probe-cb)
(let ((n 0)) (mapatoms (lambda (_) (setq n (1+ n)))) (princ (format "DIV %d\n" n)))
(unless (equal (mapcar #'probe-cb '(1 2)) '(1 2)) (error "mapatoms-bc"))
(dotimes (_ @N@) (mapatoms #'probe-cb))
