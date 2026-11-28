;;; -*- lexical-binding: t -*-
;; mapc calling a byte-compiled lexical function whose argument is a
;; &rest list: exec_byte_code gathers it before the code runs.
(defvar probe-cb (byte-compile (lambda (&rest s) s)))
(defvar probe-list (make-list 1000 1))
(unless (equal (mapcar probe-cb '(1 2)) '((1) (2))) (error "mapc-bc-rest"))
(dotimes (_ @N@) (mapc probe-cb probe-list))
(princ "DIV 1000\n")
