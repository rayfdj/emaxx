;;; -*- lexical-binding: t -*-
;; A signal caught by a condition-case 20 byte-compiled frames up (the
;; mirrors plan, at 20000:60000: 11,322.4 against GNU's 7,248.3).
(defun probe-deep (k) (if (= k 0) (signal 'error nil) (1+ (probe-deep (1- k)))))
(byte-compile 'probe-deep)
(defun probe-hd (n) (let ((i 0) (r 0)) (while (< i n) (condition-case nil (probe-deep 20) (error (setq r (1+ r)))) (setq i (1+ i))) r))
(byte-compile 'probe-hd)
(unless (= (probe-hd @N@) @N@) (error "handler-deep"))
(princ "DIV 1\n")
