;;; -*- lexical-binding: t -*-
;; A subr called from native code (HANDOVER: `bufferp' 65 against 14).
(defun probe-native-subr (n x)
  (let ((i 0) (r 0))
    (while (< i n)
      (if (bufferp x) (setq r (1+ r)))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-subr)
(unless (native-comp-function-p (symbol-function 'probe-native-subr)) (error "native-subr: not native"))
(unless (= (probe-native-subr @N@ (current-buffer)) @N@) (error "native-subr"))
(princ "DIV 1\n")
