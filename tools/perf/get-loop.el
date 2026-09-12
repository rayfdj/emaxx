;;; -*- lexical-binding: t -*-
(defun micro-get-loop () (dotimes (_ 200000) (get 'foo 'bar)))
(byte-compile 'micro-get-loop)
(micro-get-loop)
