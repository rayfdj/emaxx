(let* ((left (make-symbol "runtime-property-subject"))
       (right (make-symbol "runtime-property-subject"))
       (property (make-symbol "runtime-property-key"))
       (other-property (make-symbol "runtime-property-key")))
  (put left property 'left-own)
  (put right property 'right-own)
  (list
   (list (get left property) (get right property))
   (let ((overriding-plist-environment
          (list (list right property 'right-override))))
     (list (get left property) (get right property)))
   (let ((overriding-plist-environment
          (list (list left other-property 'different-key))))
     (get left property))
   (let ((overriding-plist-environment
          (list (list left property nil property 'later-duplicate))))
     (get left property))
   (let ((overriding-plist-environment
          (list 'ignored-atom (list left property 'after-atom))))
     (get left property))))
