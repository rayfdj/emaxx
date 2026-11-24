;;; -*- lexical-binding: t -*-
;; mutex-lock and mutex-unlock of one mutex (the mirrors plan: 726.6
;; against GNU's 709.0).
(defvar probe-m (make-mutex))
(defun probe-mx (n) (let ((i 0)) (while (< i n) (mutex-lock probe-m) (mutex-unlock probe-m) (setq i (1+ i))) i))
(byte-compile 'probe-mx)
(unless (= (probe-mx @N@) @N@) (error "mutex"))
(princ "DIV 1\n")
