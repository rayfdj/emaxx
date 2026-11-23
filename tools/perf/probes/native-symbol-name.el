;;; -*- lexical-binding: t -*-
;; `symbol-name' from native code (D4): a small subr on the general
;; fixed-arity entry.
(defun probe-native-symbol-name (n x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (symbol-name x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-symbol-name)
(unless (native-comp-function-p (symbol-function 'probe-native-symbol-name)) (error "native-symbol-name: not native"))
(unless (equal (probe-native-symbol-name @N@ 'foo (list 1 2 3)) "foo") (error "native-symbol-name"))
(princ "DIV 1\n")
