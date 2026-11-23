;;; -*- lexical-binding: t -*-
;; The locked suite's cons-allocation loop, interpreted as it is there,
;; with no collection: the mutator's part alone.
(setq gc-cons-threshold most-positive-fixnum)
(defun probe-conses (n)
  (let ((i 0) (items nil))
    (while (< i n)
      (setq items (cons i items) i (1+ i)))
    items))
(when (byte-code-function-p (symbol-function 'probe-conses)) (error "conses-nogc: compiled"))
(unless (= (length (probe-conses @N@)) @N@) (error "conses-nogc"))
(princ "DIV 1\n")
