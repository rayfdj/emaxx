;;; -*- lexical-binding: t -*-
;; The locked suite's cons-allocation loop, interpreted as it is there,
;; under the suite's collector settings.
(setq gc-cons-threshold 800000 gc-cons-percentage 1.0)
(defun probe-conses (n)
  (let ((i 0) (items nil))
    (while (< i n)
      (setq items (cons i items) i (1+ i)))
    items))
(when (byte-code-function-p (symbol-function 'probe-conses)) (error "conses: compiled"))
(unless (= (length (probe-conses @N@)) @N@) (error "conses"))
(princ "DIV 1\n")
