;;; -*- lexical-binding: t -*-
;; `cons' from native code (D4): a guard, the entry is the allocation.
(defun probe-native-cons (n x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (cons x i))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-cons)
(unless (native-comp-function-p (symbol-function 'probe-native-cons)) (error "native-cons: not native"))
(unless (equal (probe-native-cons @N@ 'foo (list 1 2 3)) (cons 'foo (1- @N@))) (error "native-cons"))
(princ "DIV 1\n")
