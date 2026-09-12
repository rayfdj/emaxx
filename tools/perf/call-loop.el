;;; -*- lexical-binding: t -*-
(defun micro-id (x) x)
(defun micro-call-loop () (dotimes (i 200000) (micro-id i)))
(mapc #'byte-compile '(micro-id micro-call-loop))
(micro-call-loop)
