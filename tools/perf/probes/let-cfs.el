;;; -*- lexical-binding: t -*-
;; (let ((case-fold-search nil)) ...): specbind of a buffer-local
;; evaluator C variable (the mirrors plan: 974.6 against GNU's 1,291.0).
(defun probe-lc (n) (let ((i 0) (r 0)) (while (< i n) (let ((case-fold-search nil)) (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-lc)
(unless (= (probe-lc @N@) @N@) (error "let-cfs"))
(princ "DIV 1\n")
