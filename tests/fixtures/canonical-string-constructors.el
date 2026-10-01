(prin1
 (list
  (mapcar
   (lambda (code)
     (list
      code
      (mapcar
       (lambda (length)
         (let ((text (make-string length code)))
           (list (length text) (string-bytes text)
                 (multibyte-string-p text)
                 (append text nil)
                 (append (string-as-unibyte text) nil))))
       '(0 1 3 17))
      (let ((text (string code)))
        (list (equal text (make-string 1 code))
              (equal text (char-to-string code))
              (multibyte-string-p text)
              (aref text 0)))))
   '(0 127 128 2047 2048 55295 55296 57343 57344 65535
     65536 1114111 1114112 2097151 2097152 4194175 4194176 4194303))
  (mapcar
   (lambda (form)
     (condition-case err (eval form t)
       (error (list (car err) (cadr err) (caddr err)))))
   '((string -1) (string 4194304) (string nil) (string 1.0)
     (string 97 'bad 'later) (char-to-string -1) (char-to-string 'bad)
     (make-string -1 'bad) (make-string nil 'bad) (make-string 1.0 'bad)
     (make-string 0 'bad) (make-string 0 -1) (make-string 0 4194304)
     (make-string 2305843009213693952 'bad)))
  (mapcar
   (lambda (form)
     (condition-case err (eval form t)
       (error (list (car err) (nth 2 err)))))
   '((make-string) (make-string 1) (make-string 1 97 t nil)))
  (let* ((text (string 65 55296 1114112 4194303)) (alias text))
    (aset alias 1 2097152)
    (garbage-collect)
    (list (eq alias text) (append text nil)))
  (mapcar (lambda (flag)
            (mapcar (lambda (length)
                      (let ((text (make-string length 65 flag)))
                        (list (length text) (string-bytes text)
                              (multibyte-string-p text))))
                    '(0 1 17)))
          '(nil t 0))
  (let ((text (string)))
    (list (length text) (string-bytes text) (multibyte-string-p text)))))
