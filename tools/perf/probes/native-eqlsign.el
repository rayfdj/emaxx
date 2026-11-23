;;; -*- lexical-binding: t -*-
;; `=' of two fixnums from native code (D4): a guard, the comparison's
;; own entry.
(defun probe-native-eqlsign (n _x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (= i 7))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-eqlsign)
(unless (native-comp-function-p (symbol-function 'probe-native-eqlsign)) (error "native-eqlsign: not native"))
(unless (eq (probe-native-eqlsign 8 'foo nil) t) (error "native-eqlsign"))
(unless (null (probe-native-eqlsign @N@ 'foo (list 1 2 3))) (error "native-eqlsign"))
(princ "DIV 1\n")
