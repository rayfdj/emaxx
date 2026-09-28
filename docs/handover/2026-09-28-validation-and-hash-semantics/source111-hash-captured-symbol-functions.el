(progn
  (fset 'capture-comparison (lambda (left right) (equal left right)))
  (fset 'capture-hash-code (lambda (_key) 0))
  ;; GNU accepts a property with two cons cells and an arbitrary tail.
  (put 'capture-symbol-test 'hash-table-test
       (cons 'capture-comparison (cons 'capture-hash-code 'unused-tail)))
  (let ((table (make-hash-table :test 'capture-symbol-test))
        (key (copy-sequence "function-key")))
    (puthash key 'kept table)
    (let ((before (gethash (copy-sequence key) table 'absent)))
      (put 'capture-symbol-test 'hash-table-test nil)
      (fset 'capture-comparison (lambda (_left _right) nil))
      (let ((after-compare (gethash (copy-sequence key) table 'absent)))
        (fset 'capture-comparison (lambda (left right) (equal left right)))
        (fset 'capture-hash-code (lambda (_key) 51))
        (let ((after-hash (gethash (copy-sequence key) table 'absent)))
          (fset 'capture-hash-code (lambda (_key) 0))
          (list before after-compare after-hash
                (gethash (copy-sequence key) table 'absent)
                (hash-table-count table)))))))
