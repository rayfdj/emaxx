;;; -*- lexical-binding: t -*-
;; `max' of two fixnums from native code (D4).
(defun probe-native-max (n _x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (max i 3))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-max)
(unless (native-comp-function-p (symbol-function 'probe-native-max)) (error "native-max: not native"))
(unless (eql (probe-native-max 2 'foo nil) 3) (error "native-max"))
(unless (eql (probe-native-max @N@ 'foo (list 1 2 3)) (1- @N@)) (error "native-max"))
(princ "DIV 1\n")
