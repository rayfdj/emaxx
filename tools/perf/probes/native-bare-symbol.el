;;; -*- lexical-binding: t -*-
;; `bare-symbol' from native code (D4): a word-level subr with a
;; signalling path.
(defun probe-native-bare-symbol (n x _l)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (bare-symbol x))
      (setq i (1+ i)))
    r))
(native-compile 'probe-native-bare-symbol)
(unless (native-comp-function-p (symbol-function 'probe-native-bare-symbol)) (error "native-bare-symbol: not native"))
(unless (eq (probe-native-bare-symbol 1 (position-symbol 'foo 1) nil) 'foo) (error "native-bare-symbol"))
(unless (eq (probe-native-bare-symbol @N@ 'foo (list 1 2 3)) 'foo) (error "native-bare-symbol"))
(princ "DIV 1\n")
