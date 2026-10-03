(progn
  (require 'cl-lib)
  (define-coding-system 'runtime-coding-ascii "ASCII candidate control."
    :coding-type 'utf-8 :mnemonic ?R :charset-list '(ascii))
  (define-coding-system 'runtime-coding-unicode "Unicode candidate control."
    :coding-type 'utf-8 :mnemonic ?R :charset-list '(unicode))
  (define-coding-system 'runtime-coding-emacs "Emacs candidate control."
    :coding-type 'utf-8 :mnemonic ?R :charset-list '(emacs))
  (define-coding-system-alias 'runtime-coding-alias 'runtime-coding-unicode)
  (let ((candidates '(utf-8 utf-8-emacs iso-latin-1 raw-text no-conversion
                     runtime-coding-ascii runtime-coding-unicode
                     runtime-coding-emacs runtime-coding-alias)))
    (cl-labels
        ((membership (source exclude)
           (let ((found (find-coding-systems-region-internal source nil exclude)))
             (if (eq found t) 'all
               (mapcar (lambda (coding) (not (null (memq coding found)))) candidates)))))
      (list
       (membership "ascii" 1)
       (membership (unibyte-string 233) '(utf-8 raw-text no-conversion))
       (mapcar
        (lambda (code)
          (let ((source (concat "x" (string code) "y")))
            (list code (membership source nil))))
        '(233 9731 55296 1114111 1114112 4194175 4194176 4194303))
       (membership (string 233) '(utf-8 raw-text no-conversion runtime-coding-unicode))
       (membership (string 233) '(runtime-coding-alias))
       (progn
         (set-coding-system-priority 'runtime-coding-unicode)
         (membership (string 233) nil))
       (with-temp-buffer
         (insert "a" (string 233) "z")
         (let ((start (copy-marker 1)) (end (copy-marker (point-max))))
           (narrow-to-region 2 3)
           (condition-case error
               (list (eq (find-coding-systems-region-internal start start) t)
                     (not (null (memq 'utf-8-emacs
                                      (find-coding-systems-region-internal start end)))))
             (error (list 'range-error (car error))))))
       (with-temp-buffer
         (insert ";; -*- coding: utf-8-emacs-unix; -*-\n" (string 233) "\n")
         (condition-case error
             (select-safe-coding-system (point-min) (point-max)
                                        'prefer-utf-8 nil "ordinary-coding-cookie.el")
           (error (list 'selection-error (car error)))))))))
