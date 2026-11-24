;;; -*- lexical-binding: t -*-
;; (put SYMBOL 'foo i) (the mirrors plan: 564.6 against GNU's 399.0).
(defun probe-put (n) (let ((i 0)) (while (< i n) (put 'probe-sym 'foo i) (setq i (1+ i))) i))
(byte-compile 'probe-put)
(unless (and (= (probe-put @N@) @N@) (= (get 'probe-sym 'foo) (1- @N@))) (error "put"))
(princ "DIV 1\n")
