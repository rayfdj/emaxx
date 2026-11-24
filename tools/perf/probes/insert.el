;;; -*- lexical-binding: t -*-
;; (insert "x") with no undo (buffer plan: 2,083.5 against GNU's 3,412.4 in
;; round 377).
(defun probe-in (n) (let ((i 0)) (while (< i n) (insert "x") (setq i (1+ i)))))
(byte-compile 'probe-in)
(with-temp-buffer (setq buffer-undo-list t) (probe-in @N@)
  (unless (= (buffer-size) @N@) (error "insert")))
(princ "DIV 1\n")
