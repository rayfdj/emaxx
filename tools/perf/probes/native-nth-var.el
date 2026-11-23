;;; -*- lexical-binding: t -*-
;; `nth' with a variable index from native code (D4): a call of the
;; subr itself.
(defun probe-native-nth-var (n x l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (nth x l))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-nth-var)
(unless (native-comp-function-p (symbol-function 'probe-native-nth-var)) (error "native-nth-var: not native"))
(unless (null (probe-native-nth-var 1 5 (list 1 2 3))) (error "native-nth-var"))
(unless (eql (probe-native-nth-var @N@ 1 (list 1 2 3)) 2) (error "native-nth-var"))
(princ "DIV 1\n")
