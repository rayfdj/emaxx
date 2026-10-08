;; -*- coding: utf-8; lexical-binding: t; -*-
(defun runtime-unit309-file-worker (input)
  "Retain and read this compilation unit across a collection."
  (garbage-collect)
  (list (+ input 17) '(payload (5 . 13) [23 47])))
