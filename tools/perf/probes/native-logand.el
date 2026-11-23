;;; -*- lexical-binding: t -*-
;; `logand' from native code (D4): a subr of any number of arguments
;; on the general entry.
(defun probe-native-logand (n _x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (logand i 7))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-logand)
(unless (native-comp-function-p (symbol-function 'probe-native-logand)) (error "native-logand: not native"))
(unless (eql (probe-native-logand @N@ 'foo (list 1 2 3)) (logand (1- @N@) 7)) (error "native-logand"))
(princ "DIV 1\n")
