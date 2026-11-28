;;; -*- lexical-binding: t -*-
;; `funcall' from native code of a native-compiled function: the plain
;; path's native arm (invoke_plain_native_funcall).
(defun probe-funcall-native-callee (x) x)
(native-compile 'probe-funcall-native-callee)
(unless (native-comp-function-p (symbol-function 'probe-funcall-native-callee)) (error "native-funcall-native: callee not native"))
(defun probe-native-funcall-native (n f x)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (funcall f x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-funcall-native)
(unless (native-comp-function-p (symbol-function 'probe-native-funcall-native)) (error "native-funcall-native: not native"))
(unless (eq (probe-native-funcall-native @N@ 'probe-funcall-native-callee 'foo) 'foo) (error "native-funcall-native"))
(princ "DIV 1\n")
