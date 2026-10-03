(let ((print-length nil) (print-level nil) (print-circle nil)
      (print-escape-control-characters t)
      (print-escape-newlines t))
  (mapcar
   (lambda (source)
     (let* ((copy (copy-sequence source))
            (escaped
             (let ((print-escape-multibyte t) (print-escape-nonascii t))
               (prin1-to-string (list source
                                     (propertize copy 'printer-probe 729)))))
            (raw (prin1-to-string source t))
            (string-result (prin1-to-string source))
            (buffer-result
             (with-temp-buffer
               (prin1 source (current-buffer))
               (buffer-string)))
            (callback-result
             (let (characters)
               (prin1 source (lambda (character) (push character characters)))
               (nreverse characters)))
            (princ-result
             (let (characters)
               (princ source (lambda (character) (push character characters)))
               (nreverse characters))))
       (garbage-collect)
       (list (string-to-list source) (multibyte-string-p source)
             escaped (string-to-list raw) (multibyte-string-p raw)
             (string-to-list string-result) (multibyte-string-p string-result)
             (string-to-list buffer-result) (multibyte-string-p buffer-result)
             callback-result princ-result
             (let ((print-escape-multibyte t) (print-escape-nonascii t))
               (equal source (car (read-from-string (prin1-to-string source))))))))
   (append
    (mapcar (lambda (character) (string character 65 55))
            '(0 34 65 92 127 128 255 55296 57343 63743 65536 1114111
              1114112 2097151 2097152 4194175 4194176 4194303 983168))
    (list (unibyte-string 128 65 55) (unibyte-string 255 65 55)
          (string) (string-to-multibyte "")))))
