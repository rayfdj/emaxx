;;; -*- lexical-binding: t -*-
;; `recordp' from native code (D4).
(defun probe-native-recordp (n x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (recordp x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-recordp)
(unless (native-comp-function-p (symbol-function 'probe-native-recordp)) (error "native-recordp: not native"))
(unless (eq (probe-native-recordp 1 (record 'foo 1) nil) t) (error "native-recordp"))
(unless (null (probe-native-recordp @N@ 'foo (list 1 2 3))) (error "native-recordp"))
(princ "DIV 1\n")
