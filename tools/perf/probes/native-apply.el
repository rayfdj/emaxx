;;; -*- lexical-binding: t -*-
;; `apply' from native code: one fixed argument and a two-element list
;; spread into a byte-compiled function's three (D4).
(defun probe-apply-callee (a _b _c) a)
(byte-compile 'probe-apply-callee)
(defun probe-native-apply (n f x l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (apply f x l))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-apply)
(unless (native-comp-function-p (symbol-function 'probe-native-apply)) (error "native-apply: not native"))
(unless (eq (probe-native-apply @N@ #'probe-apply-callee 'foo (list 1 2)) 'foo) (error "native-apply"))
(princ "DIV 1\n")
