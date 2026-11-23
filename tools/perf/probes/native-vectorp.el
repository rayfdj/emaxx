;;; -*- lexical-binding: t -*-
;; `vectorp' from native code (D4): a type predicate that reads the tag
;; and, of a vectorlike, the header.
(defun probe-native-vectorp (n x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (vectorp x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-vectorp)
(unless (native-comp-function-p (symbol-function 'probe-native-vectorp)) (error "native-vectorp: not native"))
(unless (eq (probe-native-vectorp 1 [1] nil) t) (error "native-vectorp"))
(unless (null (probe-native-vectorp @N@ 'foo (list 1 2 3))) (error "native-vectorp"))
(princ "DIV 1\n")
