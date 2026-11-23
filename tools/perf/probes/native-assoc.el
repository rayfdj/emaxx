;;; -*- lexical-binding: t -*-
;; `assoc' from native code (D4): the general entry with an optional
;; argument and a walk of three elements.
(defun probe-native-assoc (n x l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (assoc x l))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-assoc)
(unless (native-comp-function-p (symbol-function 'probe-native-assoc)) (error "native-assoc: not native"))
(unless (equal (probe-native-assoc 1 'foo (list (cons 'foo 1))) '(foo . 1)) (error "native-assoc"))
(unless (null (probe-native-assoc @N@ 'foo (list 1 2 3))) (error "native-assoc"))
(princ "DIV 1\n")
