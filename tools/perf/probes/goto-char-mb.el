;;; -*- lexical-binding: t -*-
;; (goto-char 2) in a buffer whose characters are not all one byte (Z !=
;; Z_BYTE): SET_PT's conversion, beside goto-char's ASCII buffer.
(defun probe-gcm (n) (let ((i 0)) (while (< i n) (goto-char 2) (setq i (1+ i)))))
(byte-compile 'probe-gcm)
(with-temp-buffer (setq buffer-undo-list t) (insert "αβγ\n") (probe-gcm @N@)
  (unless (and (= (point) 2) (= (position-bytes (point)) 3)) (error "goto-char")))
(princ "DIV 1\n")
