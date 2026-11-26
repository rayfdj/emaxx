;;; -*- lexical-binding: t -*-
;; `funcall' from native code of a byte-compiled function (D4).
(defun probe-funcall-callee (x) x)
(byte-compile 'probe-funcall-callee)
(defun probe-native-funcall (n f x)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (funcall f x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-funcall)
(unless (native-comp-function-p (symbol-function 'probe-native-funcall)) (error "native-funcall: not native"))
(unless (eq (probe-native-funcall @N@ #'probe-funcall-callee 'foo) 'foo) (error "native-funcall"))
(princ "DIV 1\n")
