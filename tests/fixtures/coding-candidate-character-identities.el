(progn
  (require 'cl-lib)
  (let ((codings '(utf-8 utf-8-emacs iso-latin-1 raw-text no-conversion)))
    (cl-labels
        ((membership (code exclude)
           (let ((found (find-coding-systems-region-internal (string code) nil exclude)))
             (if (eq found t) 'all
               (mapcar (lambda (coding) (not (null (memq coding found)))) codings)))))
      (list
       (mapcar (lambda (code) (list code (membership code nil)))
               '(57343 57344 57471 57472 57599 57600 1114111 1114112 4194175 4194176 4194303))
       (membership 233 (list (make-symbol "utf-8")))
       (membership 233 '(utf-8))
       (let ((coding-system-list '(utf-8-emacs utf-8 utf-8-emacs)))
         (find-coding-systems-region-internal (string 233) nil))
       (let ((coding-system-list '(utf-8-emacs raw-text no-conversion)))
         (find-coding-systems-region-internal (string 233) nil '(raw-text no-conversion)))))))
