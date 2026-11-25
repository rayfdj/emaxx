;;; -*- lexical-binding: t -*-
;; (setq last-command nil): a store into a KBOARD variable
;; (the mirrors plan: 1,003.6 against GNU's 257.0).
(defun probe-sl (n) (let ((i 0)) (while (< i n) (setq last-command nil) (setq i (1+ i))) i))
(byte-compile 'probe-sl)
(unless (and (= (probe-sl @N@) @N@) (null last-command)) (error "setq-lastcmd"))
(princ "DIV 1\n")
