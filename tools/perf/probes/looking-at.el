;;; -*- lexical-binding: t -*-
;; (looking-at "x") at a failing position (HANDOVER: 1,385 against 1,129).
(defun probe-la (n) (let ((i 0) (r 0)) (while (< i n) (if (looking-at "x") (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-la)
(with-temp-buffer (insert "abc\n") (goto-char (point-min))
  (unless (= (probe-la @N@) 0) (error "looking-at")))
(princ "DIV 1\n")
