;;; -*- lexical-binding: t -*-
;; (char-after 2) in an ASCII buffer (buffer plan: 443.6 against GNU's 260.0
;; in round 377).
(defun probe-ca (n) (let ((i 0) (r 0)) (while (< i n) (setq r (char-after 2)) (setq i (1+ i))) r))
(byte-compile 'probe-ca)
(with-temp-buffer (insert "abc\n")
  (unless (eq (probe-ca @N@) ?b) (error "char-after")))
(princ "DIV 1\n")
