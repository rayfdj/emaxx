;;; -*- lexical-binding: t -*-
;; (get nil 'foo) (the mirrors plan: 521.6 against GNU's 388.0; get of
;; another symbol 220.6 against 338.0).
(defun probe-gn (n) (let ((i 0) (r 0)) (while (< i n) (unless (get nil 'foo) (setq r (1+ r))) (setq i (1+ i))) r))
(byte-compile 'probe-gn)
(unless (= (probe-gn @N@) @N@) (error "getnil"))
(princ "DIV 1\n")
