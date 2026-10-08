(let ((runtime-post-read-observed nil)
      (source (propertize (copy-sequence "plain") 'tag 'shared)))
  (fset 'runtime-post-read-fast
        (lambda (length)
          (setq runtime-post-read-observed
                (list length (point) (buffer-size)))
          length))
  (define-coding-system 'runtime-post-read-fast "Post-read ASCII path"
    :coding-type 'utf-8 :mnemonic ?U :eol-type 'unix
    :ascii-compatible-p t :post-read-conversion 'runtime-post-read-fast)
  (unwind-protect
      (list
       (let ((result (decode-coding-string source 'runtime-post-read-fast)))
         (list (eq result source) (multibyte-string-p result)
               (get-text-property 0 'tag result) runtime-post-read-observed))
       (let ((result (decode-coding-string source 'runtime-post-read-fast t)))
         (list (eq result source) (get-text-property 0 'tag result)
               runtime-post-read-observed))
       (with-temp-buffer
         (insert "plain")
         (setq runtime-post-read-observed nil)
         (let ((result (decode-coding-region (point-min) (point-max)
                                            'runtime-post-read-fast t)))
           (list (string-to-list result) runtime-post-read-observed)))
       (with-temp-buffer
         (setq runtime-post-read-observed nil)
         (decode-coding-string source 'runtime-post-read-fast nil (current-buffer))
         (list (string-to-list (buffer-string)) runtime-post-read-observed)))
    (fmakunbound 'runtime-post-read-fast)))
