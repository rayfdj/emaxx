;;; -*- lexical-binding: t -*-
;; Lexical byte code at a lexical-binding file's top level, a non-nil
;; environment, calling dynamic-binding byte code (D17): funcall_lambda
;; binds the formal, then the environment to the callee's nil lexenv.
(defalias 'probe-dyn-callee (let ((lexical-binding nil)) (byte-compile '(lambda (a) (1+ a)))))
(defun probe-lex-dyn (n) (let ((s 0) (i 0)) (while (< i n) (setq s (probe-dyn-callee s)) (setq i (1+ i))) s))
(byte-compile 'probe-lex-dyn)
(unless (= (probe-lex-dyn @N@) @N@) (error "lex-dyn-calls"))
(princ "DIV 1\n")
