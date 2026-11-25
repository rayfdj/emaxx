;;; -*- lexical-binding: t -*-
;; (coding-system-get 'utf-8 :mime-charset) (the mirrors plan: 806.6
;; against GNU's 708.0).
(defun probe-cg (n) (let ((i 0) (r 0)) (while (< i n) (when (eq (coding-system-get 'utf-8 :mime-charset) 'utf-8) (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-cg)
(unless (= (probe-cg @N@) @N@) (error "codingget"))
(princ "DIV 1\n")
