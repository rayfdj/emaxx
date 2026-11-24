;;; -*- lexical-binding: t -*-
;; `purecopy' from native code (D4): a subr that returns its argument
;; while `purify-flag' is nil.
(defun probe-native-purecopy (n x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (purecopy x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-purecopy)
(unless (native-comp-function-p (symbol-function 'probe-native-purecopy)) (error "native-purecopy: not native"))
(unless (eq (probe-native-purecopy @N@ 'foo (list 1 2 3)) 'foo) (error "native-purecopy"))
(princ "DIV 1\n")
