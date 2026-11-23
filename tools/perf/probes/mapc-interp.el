;;; -*- lexical-binding: t -*-
;; mapc calling an interpreted one-argument closure: the call from C into
;; the interpreter (D125).
(defvar probe-list (make-list 1000 1))
(defvar probe-fn (lambda (x) x))
(when (or (byte-code-function-p probe-fn) (subrp probe-fn)) (error "mapc-interp: compiled"))
(unless (equal (mapcar probe-fn '(1 2)) '(1 2)) (error "mapc-interp"))
(dotimes (_ @N@) (mapc probe-fn probe-list))
(princ "DIV 1000\n")
