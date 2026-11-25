;;; -*- lexical-binding: t -*-
;; A read of last-command, a KBOARD variable, by Bvarref.
(defun probe-rl (n) (let ((i 0) (r 0)) (while (< i n) (unless last-command (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-rl)
(setq last-command nil)
(unless (= (probe-rl @N@) @N@) (error "ref-lastcmd"))
(princ "DIV 1\n")
