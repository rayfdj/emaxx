;;; -*- lexical-binding: t -*-
;; (goto-char 2) (buffer plan: 133.6 against GNU's 232.0 in round 377).
(defun probe-gc (n) (let ((i 0)) (while (< i n) (goto-char 2) (setq i (1+ i)))))
(byte-compile 'probe-gc)
(with-temp-buffer (setq buffer-undo-list t) (insert "abc\n") (probe-gc @N@)
  (unless (= (point) 2) (error "goto-char")))
(princ "DIV 1\n")
