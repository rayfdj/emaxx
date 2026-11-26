;;; -*- lexical-binding: t -*-
;; mapc calling a native-compiled function: Ffuncall from C into native code.
(defun probe-ncb (s) s)
(native-compile 'probe-ncb)
(unless (native-comp-function-p (symbol-function 'probe-ncb)) (error "mapc-native: not native"))
(defvar probe-list (make-list 1000 1))
(unless (equal (mapcar #'probe-ncb '(1 2)) '(1 2)) (error "mapc-native"))
(dotimes (_ @N@) (mapc #'probe-ncb probe-list))
(princ "DIV 1000\n")
