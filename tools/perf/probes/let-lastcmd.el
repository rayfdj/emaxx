;;; -*- lexical-binding: t -*-
;; (let ((last-command nil)) ...): specbind and unbind of a KBOARD
;; variable.
(defun probe-ll (n) (let ((i 0) (r 0)) (while (< i n) (let ((last-command nil)) (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-ll)
(unless (= (probe-ll @N@) @N@) (error "let-lastcmd"))
(princ "DIV 1\n")
