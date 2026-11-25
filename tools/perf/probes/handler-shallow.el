;;; -*- lexical-binding: t -*-
;; A signal caught by the condition-case of its caller (the mirrors
;; plan, at 20000:60000: 2,542.4 against GNU's 1,328.3).
(defun probe-deep (k) (if (= k 0) (signal 'error nil) (1+ (probe-deep (1- k)))))
(byte-compile 'probe-deep)
(defun probe-hs (n) (let ((i 0) (r 0)) (while (< i n) (condition-case nil (probe-deep 0) (error (setq r (1+ r)))) (setq i (1+ i))) r))
(byte-compile 'probe-hs)
(unless (= (probe-hs @N@) @N@) (error "handler-shallow"))
(princ "DIV 1\n")
