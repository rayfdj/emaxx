;;; -*- lexical-binding: t -*-
;; `+' of two fixnums from native code (D4).
(defun probe-native-plus (n _x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (+ i 3))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-plus)
(unless (native-comp-function-p (symbol-function 'probe-native-plus)) (error "native-plus: not native"))
(unless (eql (probe-native-plus @N@ 'foo (list 1 2 3)) (+ @N@ 2)) (error "native-plus"))
(princ "DIV 1\n")
