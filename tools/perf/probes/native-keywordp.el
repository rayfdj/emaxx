;;; -*- lexical-binding: t -*-
;; `keywordp' from native code (D4): the entry and the test of the
;; symbol's name.
(defun probe-native-keywordp (n x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (keywordp x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-keywordp)
(unless (native-comp-function-p (symbol-function 'probe-native-keywordp)) (error "native-keywordp: not native"))
(unless (eq (probe-native-keywordp 1 :foo nil) t) (error "native-keywordp"))
(unless (null (probe-native-keywordp @N@ 'foo (list 1 2 3))) (error "native-keywordp"))
(princ "DIV 1\n")
