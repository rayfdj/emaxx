(progn
  (define-hash-table-test 'renamed-string-properties
    #'equal-including-properties #'sxhash-equal-including-properties)
  (let* ((inner-left (propertize "payload" 'nested 'left))
         (inner-right (propertize "payload" 'nested 'right))
         (left (propertize "outer" 'value inner-left))
         (right (propertize "outer" 'value inner-right))
         (table (make-hash-table :test 'renamed-string-properties))
         (result nil))
    (puthash left 17 table)
    (push (list 'initial
                (equal-including-properties left right)
                (= (sxhash-equal-including-properties left)
                   (sxhash-equal-including-properties right))
                (gethash right table 'missing)) result)
    (put-text-property 0 7 'nested 'mutated inner-right)
    (garbage-collect)
    (push (list 'nested-property-mutation
                (equal-including-properties left right)
                (= (sxhash-equal-including-properties left)
                   (sxhash-equal-including-properties right))
                (gethash right table 'missing)) result)
    (puthash right 29 table)
    (push (list 'replace-equivalent-key
                (hash-table-count table)
                (gethash left table 'missing)
                (gethash right table 'missing)) result)
    (remhash right table)
    (push (list 'remove-equivalent-key
                (hash-table-count table)
                (gethash left table 'missing)) result)
    (let ((vector-left (propertize "array" 'value (vector inner-left)))
          (vector-right (propertize "array" 'value (vector inner-right)))
          (list-left (propertize "list" 'value (list inner-left 37)))
          (list-right (propertize "list" 'value (list inner-right 37))))
      (push (list 'nested-vector
                  (equal-including-properties vector-left vector-right)
                  (= (sxhash-equal-including-properties vector-left)
                     (sxhash-equal-including-properties vector-right))) result)
      (push (list 'nested-list
                  (equal-including-properties list-left list-right)
                  (= (sxhash-equal-including-properties list-left)
                     (sxhash-equal-including-properties list-right))) result))
    (nreverse result)))
