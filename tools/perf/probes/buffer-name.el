;;; -*- lexical-binding: t -*-
;; (buffer-name B) of a buffer not current (buffer plan: 405.6 against GNU's
;; 355.0 in round 377).
(defvar probe-other (get-buffer-create "probe-other"))
(defun probe-bn (n) (let ((i 0) (r nil)) (while (< i n) (setq r (buffer-name probe-other)) (setq i (1+ i))) r))
(byte-compile 'probe-bn)
(with-temp-buffer
  (unless (equal (probe-bn @N@) "probe-other") (error "buffer-name")))
(princ "DIV 1\n")
