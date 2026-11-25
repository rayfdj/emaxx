;;; -*- lexical-binding: t -*-
;; A read of inhibit-quit, an evaluator C variable, by Bvarref.
(defun probe-ri (n) (let ((i 0) (r 0)) (while (< i n) (unless inhibit-quit (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-ri)
(unless (= (probe-ri @N@) @N@) (error "ref-iq"))
(princ "DIV 1\n")
