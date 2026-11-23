;;; -*- lexical-binding: t -*-
;; Building the ucs-names table (HANDOVER: 1,024M against 818M).
(dotimes (_ @N@) (setq ucs-names nil) (when (= (hash-table-count (ucs-names)) 0) (error "ucs-names")))
(princ (format "RESULT %d\n" (hash-table-count (ucs-names))))
(princ "DIV 1\n")
