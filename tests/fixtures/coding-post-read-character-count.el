(let ((runtime-post-read-observed nil))
  (fset 'runtime-post-read-count
        (lambda (length)
          (setq runtime-post-read-observed
                (list length (point) (point-min) (point-max)
                      (string-to-list (buffer-string))))
          length))
  (define-coding-system 'runtime-post-read-utf8 "Post-read character count"
    :coding-type 'utf-8 :mnemonic ?U :eol-type 'unix
    :ascii-compatible-p t :post-read-conversion 'runtime-post-read-count)
  (define-coding-system 'runtime-post-read-dos "Post-read character count"
    :coding-type 'utf-8 :mnemonic ?U :eol-type 'dos
    :ascii-compatible-p t :post-read-conversion 'runtime-post-read-count)
  (define-coding-system 'runtime-post-read-raw "Post-read character count"
    :coding-type 'raw-text :mnemonic ?R :eol-type 'unix
    :ascii-compatible-p t :post-read-conversion 'runtime-post-read-count)
  (unwind-protect
      (mapcar
       (lambda (case)
         (setq runtime-post-read-observed nil)
         (let ((result (decode-coding-string (car case) (cadr case))))
           (list (string-to-list result) runtime-post-read-observed)))
       (list
        (list "plain" 'runtime-post-read-utf8)
        (list (unibyte-string 65 226 152 131 194 233) 'runtime-post-read-utf8)
        (list (unibyte-string 65 226 152 131 13 10 66) 'runtime-post-read-dos)
        (list (unibyte-string 200 201) 'runtime-post-read-raw)))
    (fmakunbound 'runtime-post-read-count)))
