;;; -*- lexical-binding: t -*-
;; `point' from native code (D4): a subr that reads the current buffer
;; and cannot signal.
(defun probe-native-point (n _x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (point))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-point)
(unless (native-comp-function-p (symbol-function 'probe-native-point)) (error "native-point: not native"))
(unless (eql (probe-native-point @N@ 'foo (list 1 2 3)) (point)) (error "native-point"))
(princ "DIV 1\n")
