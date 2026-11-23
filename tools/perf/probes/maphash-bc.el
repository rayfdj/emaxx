;;; -*- lexical-binding: t -*-
;; maphash with a byte-compiled callback (HANDOVER: 522 an entry against 397).
(defun probe-cb2 (k v) (ignore k) v)
(byte-compile 'probe-cb2)
(defvar probe-table (make-hash-table :test 'eq))
(dotimes (i 1000) (puthash i i probe-table))
(unless (equal (probe-cb2 1 2) 2) (error "maphash-bc"))
(dotimes (_ @N@) (maphash #'probe-cb2 probe-table))
(princ "DIV 1000\n")
