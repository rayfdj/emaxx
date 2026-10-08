(let ((codes '(65 128 55296 63743 1114111 1114112 4194175 4194176)))
  (mapcar
   (lambda (code)
     (list
      code
      (mapcar
       (lambda (coding)
         (list coding
               (condition-case problem
                   (let ((encoded (encode-coding-string (string code) coding t)))
                     (list 'ok (multibyte-string-p encoded)
                           (string-to-list encoded)))
                 (error (list 'error problem)))))
       '(utf-8 utf-8-emacs no-conversion raw-text))
      (let ((path (make-temp-file "emaxx-character-storage-"))
            (coding-system-for-write 'utf-8))
        (unwind-protect
            (condition-case problem
                (progn
                  (write-region (string code) nil path nil 'silent)
                  (with-temp-buffer
                    (set-buffer-multibyte nil)
                    (insert-file-contents-literally path)
                    (list 'ok (string-to-list (buffer-string)))))
              (error (list 'error problem)))
          (delete-file path)))))
   codes))
