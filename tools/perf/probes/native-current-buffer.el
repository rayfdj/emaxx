;;; -*- lexical-binding: t -*-
;; `current-buffer' from native code (D4): a subr that reads the
;; interpreter's current buffer and cannot signal.
(defun probe-native-current-buffer (n _x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (current-buffer))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-current-buffer)
(unless (native-comp-function-p (symbol-function 'probe-native-current-buffer)) (error "native-current-buffer: not native"))
(unless (eq (probe-native-current-buffer @N@ 'foo (list 1 2 3)) (current-buffer)) (error "native-current-buffer"))
(princ "DIV 1\n")
