;;; -*- lexical-binding: t -*-
;; `nth' with a constant index from native code (D4): the compiler
;; opens it into `car' and `cdr', so the row guards those entries.
(defun probe-native-nth (n _x l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (nth 1 l))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-nth)
(unless (native-comp-function-p (symbol-function 'probe-native-nth)) (error "native-nth: not native"))
(unless (eql (probe-native-nth @N@ 'foo (list 1 2 3)) 2) (error "native-nth"))
(princ "DIV 1\n")
