;;; -*- lexical-binding: t -*-
;; A print to an interpreted one-argument function (D125): nine calls a print.
(defvar probe-chars 0)
(let ((standard-output (lambda (c) (setq probe-chars (1+ probe-chars)))) (i 0))
  (while (< i @N@) (princ "abcdefghi") (setq i (1+ i))))
(unless (= probe-chars (* 9 @N@)) (error "princ-interp"))
(princ "DIV 9\n")
