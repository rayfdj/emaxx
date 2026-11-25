;;; -*- lexical-binding: t -*-
;; A byte-compiled loop calling a native-compiled function: each call
;; enters native code from byte code, an outermost native activation.
(defun probe-native-callee (x) x)
(native-compile 'probe-native-callee)
(unless (native-comp-function-p (symbol-function 'probe-native-callee)) (error "bc-to-native: not native"))
(defun probe-bc-to-native (n x)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (probe-native-callee x))
      (setq i (1+ i)))
    r))
(byte-compile 'probe-bc-to-native)
(unless (byte-code-function-p (symbol-function 'probe-bc-to-native)) (error "bc-to-native: not byte code"))
(unless (eq (probe-bc-to-native @N@ 'foo) 'foo) (error "bc-to-native"))
(princ "DIV 1\n")
