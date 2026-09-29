(mapcar
 (lambda (text)
   (mapcar
    (lambda (flags)
      (let ((print-escape-nonascii (car flags))
            (print-escape-multibyte (cadr flags)))
        (list
         (mapcar (lambda (noescape)
                   (let ((printed (prin1-to-string text noescape)))
                     (list (multibyte-string-p printed) (string-to-list printed))))
                 '(nil t))
         (mapcar
          (lambda (printer)
            (let ((codes nil))
              (funcall printer text (lambda (code) (push code codes)))
              (nreverse codes)))
          '(prin1 princ))
         (mapcar
          (lambda (multibyte)
            (with-temp-buffer
              (set-buffer-multibyte multibyte)
              (prin1 text (current-buffer))
              (string-to-list (buffer-string))))
          '(nil t)))))
    '((nil nil) (nil t) (t nil) (t t))))
 (list (unibyte-string 177 233 255)
       (string-as-multibyte (unibyte-string 177 233 255))
       (string 177 233)))
