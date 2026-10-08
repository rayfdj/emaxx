(let ((runtime-coding-observed nil)
      (source (propertize (string 65 63743 10 55296 1114112 4194175 4194176)
                          'tag '(shared)))
      (path (make-temp-file "emaxx-conversion-storage-")))
  (fset 'runtime-coding-prewrite
        (lambda (from to)
          (setq runtime-coding-observed
                (list (string-to-list (buffer-substring from to))
                      (get-text-property from 'tag)))
          (goto-char from)
          (insert (string 1114112))
          (garbage-collect)))
  (define-coding-system 'runtime-character-utf8 "Character storage probe"
    :coding-type 'utf-8 :mnemonic ?U :eol-type 'unix
    :ascii-compatible-p t :pre-write-conversion 'runtime-coding-prewrite)
  (unwind-protect
      (list
       (condition-case problem
           (let ((result (encode-coding-string "plain" 'runtime-character-utf8 t)))
             (list (string-to-list result) runtime-coding-observed))
         (error (list 'error problem)))
       (condition-case problem
           (let ((result (encode-coding-string source 'runtime-character-utf8)))
             (list (string-to-list result) runtime-coding-observed))
         (error (list 'error problem)))
       (mapcar
        (lambda (coding)
          (let ((coding-system-for-write coding))
            (condition-case problem
                (with-temp-buffer
                  (insert "prefix" source "tail")
                  (narrow-to-region 8 (+ 6 (length source)))
                  (garbage-collect)
                  (write-region (point-min) (point-max) path nil 'silent)
                  (with-temp-buffer
                    (set-buffer-multibyte nil)
                    (insert-file-contents-literally path)
                    (string-to-list (buffer-string))))
              (error (list 'error problem)))))
        '(utf-8 utf-8-dos utf-8-mac utf-8-with-signature raw-text no-conversion))
       (let ((inhibit-eol-conversion t))
         (condition-case problem
             (string-to-list (encode-coding-string source 'utf-8-dos))
           (error (list 'error problem)))))
    (delete-file path)
    (fmakunbound 'runtime-coding-prewrite)))
