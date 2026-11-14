(dotimes (i 5000) (princ i) (terpri))
(princ (mapconcat #'number-to-string (number-sequence 1 400) "\n"))
