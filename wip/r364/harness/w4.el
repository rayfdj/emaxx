(princ "before")
(let ((print-unreadable-function (lambda (o e) (message "from the function") nil)))
  (prin1 (list 1 2 (make-marker) 3 4)))
(terpri)
(condition-case e
    (prin1 (list 1 2 3) (lambda (c) (when (= c ?2) (error "at two")) (write-char c)))
  (error (princ (format " caught %S\n" e))))
(prin1 '(some list that is cut))
(error "an uncaught error with data %S" '(1 "two"))
