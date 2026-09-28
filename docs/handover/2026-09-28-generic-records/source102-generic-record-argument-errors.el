(mapcar
 (lambda (value)
   (condition-case err
       (let ((record (make-record 'slot-count-107 value nil)))
         (list (length record) (aref record 0)))
     (error err)))
 (list 0 1 -1 1.0 nil 'slot-count-109 most-positive-fixnum
       (1+ most-positive-fixnum) 9223372036854775808))
