;;; -*- lexical-binding: t -*-
;; An interpreted loop calling a native-compiled function: each call
;; enters native code from eval, an outermost native activation.
(defun probe-native-callee (x) x)
(native-compile 'probe-native-callee)
(unless (native-comp-function-p (symbol-function 'probe-native-callee)) (error "interp-to-native: not native"))
(defun probe-interp-to-native (n x)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (probe-native-callee x))
      (setq i (1+ i)))
    r))
(unless (interpreted-function-p (symbol-function 'probe-interp-to-native)) (error "interp-to-native: not interpreted"))
(unless (eq (probe-interp-to-native @N@ 'foo) 'foo) (error "interp-to-native"))
(princ "DIV 1\n")
