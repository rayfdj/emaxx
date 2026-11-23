;;; -*- lexical-binding: t -*-
;; mapatoms with a byte-compiled closure, as comp--all-classes' (HANDOVER: 807 against 639).
(defun probe-run () (let ((res 0)) (mapatoms (lambda (s) (when (get s 'probe-no-such-property) (setq res (1+ res))))) res))
(byte-compile 'probe-run)
(let ((n 0)) (mapatoms (lambda (_) (setq n (1+ n)))) (princ (format "DIV %d\n" n)))
(dotimes (_ @N@) (unless (= (probe-run) 0) (error "mapatoms-closure")))
