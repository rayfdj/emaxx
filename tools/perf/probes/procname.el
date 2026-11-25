;;; -*- lexical-binding: t -*-
;; process-name of a pipe process (the mirrors plan: 392.6 against
;; GNU's 371.0).
(defvar probe-p (make-pipe-process :name "probe" :buffer nil :noquery t))
(defun probe-pn (n) (let ((i 0) (r 0)) (while (< i n) (when (process-name probe-p) (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-pn)
(unless (= (probe-pn @N@) @N@) (error "procname"))
(princ "DIV 1\n")
