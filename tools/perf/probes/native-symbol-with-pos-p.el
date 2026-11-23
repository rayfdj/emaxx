;;; -*- lexical-binding: t -*-
;; `symbol-with-pos-p' from native code (D4), which the byte compiler
;; calls on every form it walks.
(defun probe-native-symbol-with-pos-p (n x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (symbol-with-pos-p x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-symbol-with-pos-p)
(unless (native-comp-function-p (symbol-function 'probe-native-symbol-with-pos-p)) (error "native-symbol-with-pos-p: not native"))
(unless (eq (probe-native-symbol-with-pos-p 1 (position-symbol 'foo 1) nil) t) (error "native-symbol-with-pos-p"))
(unless (null (probe-native-symbol-with-pos-p @N@ 'foo (list 1 2 3))) (error "native-symbol-with-pos-p"))
(princ "DIV 1\n")
