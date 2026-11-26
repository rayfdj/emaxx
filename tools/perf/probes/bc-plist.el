;;; -*- lexical-binding: t -*-
;; `symbol-plist' from byte code: Bcall of a subr with no opcode.
(defun probe-bc-plist (n x)
  (let ((i 0) (r nil))
    (while (< i n)
      (setq r (symbol-plist x))
      (setq i (1+ i)))
    r))
(byte-compile 'probe-bc-plist)
(put 'probe-plist-sym 'k 1)
(unless (equal (probe-bc-plist @N@ 'probe-plist-sym) '(k 1)) (error "bc-plist"))
(princ "DIV 1\n")
