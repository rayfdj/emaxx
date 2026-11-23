;;; -*- lexical-binding: nil -*-
;; The same recursion under dynamic binding (HANDOVER: 746 against 665).
(defun probe-dyn-rec (n) (if (= n 0) 0 (probe-dyn-rec (1- n))))
(defun probe-dyn-loop (k) (let ((i 0) (s 0)) (while (< i k) (setq s (+ s (probe-dyn-rec 100))) (setq i (1+ i))) s))
(mapc #'byte-compile '(probe-dyn-rec probe-dyn-loop))
(unless (= (probe-dyn-loop @N@) 0) (error "dyn-rec"))
(princ "DIV 101\n")
