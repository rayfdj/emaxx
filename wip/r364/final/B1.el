(let ((noninteractive nil)) (princ "to stdout\n") (prin1 '(a b)) (terpri))
(princ (format "Messages=%S\n" (with-current-buffer "*Messages*" (buffer-string))))
