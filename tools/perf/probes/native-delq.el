;;; -*- lexical-binding: t -*-
;; `delq' from native code (D4): the general entry and a walk of three
;; conses.
(defun probe-native-delq (n x l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (delq x l))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-delq)
(unless (native-comp-function-p (symbol-function 'probe-native-delq)) (error "native-delq: not native"))
(unless (equal (probe-native-delq 1 2 (list 1 2 3)) '(1 3)) (error "native-delq"))
(unless (equal (probe-native-delq @N@ 'foo (list 1 2 3)) '(1 2 3)) (error "native-delq"))
(princ "DIV 1\n")
