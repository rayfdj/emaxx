;;; -*- lexical-binding: t -*-
;; (setq inhibit-quit nil): a store into an evaluator C variable
;; (the mirrors plan: 932.6 against GNU's 248.0; setq print-length 120.6).
(defun probe-sq (n) (let ((i 0)) (while (< i n) (setq inhibit-quit nil) (setq i (1+ i))) i))
(byte-compile 'probe-sq)
(unless (and (= (probe-sq @N@) @N@) (null inhibit-quit)) (error "setq-iq"))
(princ "DIV 1\n")
