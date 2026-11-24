;;; -*- lexical-binding: t -*-
;; with-current-buffer of another buffer and back: set_buffer_internal_2
;; twice (buffer plan: 697.7 against GNU's 515.0 in round 377).
(defvar probe-other (get-buffer-create "probe-other"))
(defun probe-sb (n) (let ((i 0) (r 0)) (while (< i n) (with-current-buffer probe-other (setq r i)) (setq i (1+ i))) r))
(byte-compile 'probe-sb)
(with-temp-buffer
  (unless (= (probe-sb @N@) (1- @N@)) (error "set-buffer")))
(princ "DIV 1\n")
