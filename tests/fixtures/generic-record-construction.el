(let ((print-circle t))
  (list
   (mapcar
    (lambda (count)
      (let* ((fields (make-list count 79))
             (object (apply #'record 'arity-73 fields)))
        (list count (length object) (aref object 0)
              (if (> count 0) (aref object count) 'empty))))
    '(0 1 17 259 4094))
   (mapcar
    (lambda (count)
      (condition-case err
          (length (apply #'record 'too-many-83 (make-list count nil)))
        (error err)))
    '(4095 4096))
   (mapcar
    (lambda (count)
      (condition-case err
          (length (read (concat "#s(read-type-89" (apply #'concat (make-list count " nil")) ")")))
        (error err)))
    '(0 1 257 4094 4095))
   (let* ((original (record 'pure-97 (propertize "text" 'face 'bold)))
          (copy (let ((purify-flag t)) (purecopy original))))
     (list (eq original copy) (equal original copy)
           (equal-including-properties original copy)
           (text-properties-at 0 (aref copy 1))))))
