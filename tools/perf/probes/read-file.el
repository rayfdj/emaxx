;;; -*- lexical-binding: t -*-
;; insert-file-contents of NormalizationTest.txt (HANDOVER: 85M against 67M).
(defvar probe-size 0)
(dotimes (_ @N@) (with-temp-buffer (insert-file-contents (expand-file-name "admin/unidata/NormalizationTest.txt" source-directory)) (setq probe-size (buffer-size))))
(princ (format "RESULT %d\n" probe-size))
(princ "DIV 1\n")
