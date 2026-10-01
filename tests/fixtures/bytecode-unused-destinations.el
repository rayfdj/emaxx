(prin1
 (list
  (mapcar
   (lambda (case)
     (let ((code (unibyte-string 192 (car case) 255 255 193 135))
           (constants (vector (cadr case) 73)))
       (list (funcall (make-byte-code 0 code constants 2))
             (byte-code code constants 2))))
   '((131 t) (132 nil) (133 t) (134 nil)))
  (mapcar
   (lambda (case)
     (let ((code (unibyte-string 192 (car case) 255 255 193 48 135))
           (constants (vector (cadr case) 89)))
       (list (funcall (make-byte-code 0 code constants 2))
             (byte-code code constants 2))))
   '((49 (error)) (50 unused-tag)))))
