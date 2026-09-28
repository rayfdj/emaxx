(let ((results nil))
  (dolist (multibyte '(nil t))
    (dolist (fail '(nil t))
      (with-temp-buffer
        (set-buffer-multibyte multibyte)
        (let ((print-escape-nonascii nil)
              (print-escape-multibyte nil)
              (observed nil))
          (add-hook 'before-change-functions
                    (lambda (_start _end)
                      (push (list print-escape-nonascii print-escape-multibyte)
                            observed)
                      (when fail (error "printer hook")))
                    nil t)
          (let ((outcome (condition-case err
                             (progn (prin1 (unibyte-string 177) (current-buffer)) 'ok)
                           (error (car err)))))
            (push (list multibyte fail outcome (nreverse observed)
                        print-escape-nonascii print-escape-multibyte
                        (string-to-list (buffer-string)))
                  results))))))
  (nreverse results))
