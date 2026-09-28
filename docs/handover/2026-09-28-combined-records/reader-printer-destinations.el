(let ((results nil))
  (dolist (destination '(buffer marker))
    (dolist (fail '(nil t))
      (let ((source (current-buffer))
            (target (generate-new-buffer " *printer-destination*")))
        (unwind-protect
            (let ((marker nil) (seen nil)
                  (print-escape-nonascii nil) (print-escape-multibyte nil))
              (with-current-buffer target
                (insert "abc")
                (goto-char 3)
                (setq marker (copy-marker 2))
                (add-hook 'before-change-functions
                          (lambda (start end)
                            (push (list (eq (current-buffer) target) start end
                                        print-escape-nonascii print-escape-multibyte)
                                  seen)
                            (when fail (error "printer-hook"))) nil t))
              (let ((outcome (condition-case err
                                 (progn (prin1 (unibyte-string 177)
                                               (if (eq destination 'buffer) target marker))
                                        'ok)
                               (error (car err)))))
                (push (list destination fail outcome (eq source (current-buffer))
                            (nreverse seen) (marker-position marker)
                            (with-current-buffer target
                              (list (point) (buffer-string)))
                            print-escape-nonascii print-escape-multibyte)
                      results)))
          (kill-buffer target)))))
  (nreverse results))
