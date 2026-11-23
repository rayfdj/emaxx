;;; -*- lexical-binding: t -*-
;; The loop the D4 probes share, with no call in it: what a row of
;; theirs costs beyond its subr.
(defun probe-native-base (n x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r x)
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-base)
(unless (native-comp-function-p (symbol-function 'probe-native-base)) (error "native-base: not native"))
(unless (eq (probe-native-base @N@ 'foo (list 1 2 3)) 'foo) (error "native-base"))
(princ "DIV 1\n")
