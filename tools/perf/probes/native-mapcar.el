;;; -*- lexical-binding: t -*-
;; `mapcar' from native code over ten elements with a byte-compiled
;; function (D4): a unit is one element's call.
(defun probe-mapcar-callee (x) x)
(byte-compile 'probe-mapcar-callee)
(defvar probe-mapcar-list (make-list 10 'foo))
(defun probe-native-mapcar (n f l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (mapcar f l))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-mapcar)
(unless (native-comp-function-p (symbol-function 'probe-native-mapcar)) (error "native-mapcar: not native"))
(unless (equal (probe-native-mapcar @N@ #'probe-mapcar-callee probe-mapcar-list) probe-mapcar-list) (error "native-mapcar"))
(princ "DIV 10\n")
