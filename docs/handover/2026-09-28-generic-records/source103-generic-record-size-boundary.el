(mapcar
 (lambda (slots)
   (condition-case err
       (let ((record (make-record 'record-boundary-kind slots 'record-boundary-fill)))
         (list slots (recordp record) (length record) (aref record 0)
               (and (> slots 0) (aref record slots))))
     (error (list slots err))))
 '(0 1 9 257 4094 4095 8192))
