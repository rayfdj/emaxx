;;; -*- lexical-binding: t -*-
;; mutex-lock and mutex-unlock with fifty other mutexes alive (the
;; mirrors plan: 1,726.6 against GNU's 709.0).
(defvar probe-ms (let (l) (dotimes (_ 50) (push (make-mutex) l)) l))
(defvar probe-m (make-mutex))
(defun probe-mx (n) (let ((i 0)) (while (< i n) (mutex-lock probe-m) (mutex-unlock probe-m) (setq i (1+ i))) i))
(byte-compile 'probe-mx)
(unless (and (= (probe-mx @N@) @N@) (= (length probe-ms) 50)) (error "mutex50"))
(princ "DIV 1\n")
