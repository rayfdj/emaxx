;;; -*- lexical-binding: t -*-
;; process-name of the first of 21 pipe processes made (the mirrors
;; plan: 525.6 against GNU's 371.0 for the last made).
(defvar probe-p (make-pipe-process :name "probe" :buffer nil :noquery t))
(defvar probe-ps (let (l) (dotimes (k 20) (push (make-pipe-process :name (format "probe%d" k) :buffer nil :noquery t) l)) l))
(defun probe-pn (n) (let ((i 0) (r 0)) (while (< i n) (when (process-name probe-p) (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-pn)
(unless (and (= (probe-pn @N@) @N@) (= (length probe-ps) 20)) (error "procname20"))
(princ "DIV 1\n")
