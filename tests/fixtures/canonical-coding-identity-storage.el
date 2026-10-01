(mapcar
 (lambda (form)
   (let ((source (eval form)))
     (list form
           (mapcar
            (lambda (coding)
              (list coding
                    (mapcar
                     (lambda (nocopy)
                       (condition-case problem
                           (let ((result (encode-coding-string source coding nocopy)))
                             (list (eq source result) (multibyte-string-p result)
                                   (string-to-list result)
                                   (and (> (length result) 0)
                                        (text-properties-at 0 result))))
                         (error (list 'error problem))))
                     '(nil t))))
            '(nil utf-8 utf-8-dos utf-8-with-signature no-conversion raw-text)))))
 '((unibyte-string 128 233 255)
   (propertize "plain" 'tag '(identity))
   (propertize (string 233 63743) 'tag '(identity))
   (propertize (string 1114112) 'tag '(identity))
   (string 983168)
   (make-string 0 0 t)))
