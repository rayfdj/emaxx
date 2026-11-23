;;; -*- lexical-binding: t -*-
;; The first char-from-name, which builds the ucs-names table (HANDOVER:
;; 1,024M against 818M).  Counts 0 and 1.
(defvar probe-char nil)
(dotimes (_ @N@) (setq probe-char (char-from-name "LATIN SMALL LETTER A")))
(princ (format "RESULT %S\n" probe-char))
(princ "DIV 1\n")
