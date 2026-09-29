(let ((symbols-with-pos-enabled t))
  (define-hash-table-test 'hash-descriptor-position-test
    (position-symbol 'equal 11) (position-symbol 'identity 12))
  (let ((first (make-hash-table :test 'hash-descriptor-position-test)))
    (puthash 1 'kept first)
    (define-hash-table-test 'hash-descriptor-position-test
      (position-symbol 'equal 21) (position-symbol 'identity 22))
    (let ((second (make-hash-table :test 'hash-descriptor-position-test)))
      (puthash 1 'kept second)
      (let ((symbols-with-pos-enabled nil))
        (mapcar
         (lambda (table)
           (condition-case error
               (gethash 1 table)
             (error (list (car error)
                          (if (symbol-with-pos-p (cadr error))
                              (symbol-with-pos-pos (cadr error))
                            (cadr error))))))
         (list first second))))))
