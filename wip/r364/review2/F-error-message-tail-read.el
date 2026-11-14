;; print_error_message (print.c:1108-1119): FOR_EACH_TAIL reads XCDR (tail) after the datum
;; is printed, and XCAR (tail) after the separator is written.
(let* ((data (list 'foo 1 (make-marker) 2 3))
       (print-unreadable-function (lambda (_o _e) (setcdr (cddr data) (list 'replaced)) nil)))
  (princ (error-message-string data)) (terpri))
