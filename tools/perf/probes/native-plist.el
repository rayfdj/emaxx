;;; -*- lexical-binding: t -*-
;; `symbol-plist' from native code: a subr with no entry of its own,
;; through the general entry (D4).
(defun probe-native-plist (n x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (symbol-plist x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-plist)
(unless (native-comp-function-p (symbol-function 'probe-native-plist)) (error "native-plist: not native"))
(put 'probe-plist-sym 'k 1)
(unless (equal (probe-native-plist @N@ 'probe-plist-sym nil) '(k 1)) (error "native-plist"))
(princ "DIV 1\n")
