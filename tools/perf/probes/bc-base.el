;;; -*- lexical-binding: t -*-
;; The loop bc-plist shares, with no call in it.
(defun probe-bc-base (n x)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r x)
      (setq i (1+ i)))
    r))
(byte-compile 'probe-bc-base)
(unless (eq (probe-bc-base @N@ 'foo) 'foo) (error "bc-base"))
(princ "DIV 1\n")
