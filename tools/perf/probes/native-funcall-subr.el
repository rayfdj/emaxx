;;; -*- lexical-binding: t -*-
;; `funcall' from native code of a C subr: the plain path's subr arm
;; (invoke_plain_builtin_funcall).
(defun probe-native-funcall-subr (n f x)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (funcall f x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-funcall-subr)
(unless (native-comp-function-p (symbol-function 'probe-native-funcall-subr)) (error "native-funcall-subr: not native"))
(unless (eq (probe-native-funcall-subr @N@ 'symbol-plist 'probe-no-plist) nil) (error "native-funcall-subr"))
(princ "DIV 1\n")
