;;; -*- lexical-binding: t -*-
;; Four (ignore X) forms in an interpreted loop (D125).
(let ((i 0)) (while (< i @N@) (ignore i) (ignore i) (ignore i) (ignore i) (setq i (1+ i)))
  (unless (= i @N@) (error "interp-ignore")))
(princ "DIV 1\n")
