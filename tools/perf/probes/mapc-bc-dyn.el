;;; -*- lexical-binding: t -*-
;; mapc calling a byte-compiled dynamic-binding function: funcall_lambda
;; binds its formal, then exec_byte_code runs the code with no arguments.
(defvar probe-cb (byte-compile (eval '(lambda (s) s) nil)))
(defvar probe-list (make-list 1000 1))
(unless (and (consp (aref probe-cb 0)) (equal (mapcar probe-cb '(1 2)) '(1 2)))
  (error "mapc-bc-dyn"))
(dotimes (_ @N@) (mapc probe-cb probe-list))
(princ "DIV 1000\n")
