;;; -*- lexical-binding: t -*-
;; `>=' of two fixnums from native code (D4): a guard, as `='.
(defun probe-native-geq (n _x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (>= i 7))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-geq)
(unless (native-comp-function-p (symbol-function 'probe-native-geq)) (error "native-geq: not native"))
(unless (null (probe-native-geq 1 'foo nil)) (error "native-geq"))
(unless (eq (probe-native-geq @N@ 'foo (list 1 2 3)) t) (error "native-geq"))
(princ "DIV 1\n")
