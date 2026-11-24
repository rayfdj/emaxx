;;; -*- lexical-binding: t -*-
;; (let ((inhibit-quit nil)) ...): specbind and unbind of an evaluator
;; C variable (the mirrors plan: 1,312.6 against GNU's 630.0).
(defun probe-li (n) (let ((i 0) (r 0)) (while (< i n) (let ((inhibit-quit nil)) (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-li)
(unless (= (probe-li @N@) @N@) (error "let-iq"))
(princ "DIV 1\n")
