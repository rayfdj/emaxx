(progn
  (setq word-census-kept nil)
  (let ((gc-cons-threshold most-positive-fixnum)
        (gc-cons-percentage 0)
        (native-comp-jit-compilation nil)
        (symbols-with-pos-enabled nil))
    ;; This is a whole-heap census. Finish startup reclamation before
    ;; measuring a new object: the first two GNU collections can otherwise
    ;; differ by an unrelated closure and its constants vector.
    (garbage-collect)
    (garbage-collect)
    (list
     (mapcar
      (lambda (size)
        (let ((before (nth 2 (assq 'vector-slots (garbage-collect)))))
          (push (make-record 'word-census-record size nil) word-census-kept)
          (list size
                (- (nth 2 (assq 'vector-slots (garbage-collect))) before))))
      '(0 1 2 3 4 5 6 7 8 9 249 250 251 252 257 511 512 4094))
     (mapcar
      (lambda (extras)
        (put 'word-census-table 'char-table-extra-slots extras)
        (let ((before (nth 2 (assq 'vector-slots (garbage-collect)))))
          (push (make-char-table 'word-census-table nil) word-census-kept)
          (list extras
                (- (nth 2 (assq 'vector-slots (garbage-collect))) before))))
      '(0 1 2 3 4 5 6 7 8 9 10))
     (mapcar
      (lambda (arguments)
        (let ((before (nth 2 (assq 'vector-slots (garbage-collect)))))
          (push (apply (car arguments) (cdr arguments)) word-census-kept)
          (list (car arguments)
                (- (nth 2 (assq 'vector-slots (garbage-collect))) before))))
      '((read-positioning-symbols "word-census-symbol")
        (make-hash-table :size 0)
        (make-hash-table :size 31))))))
