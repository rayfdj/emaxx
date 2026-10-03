(progn
  (setq rounded-census-kept nil)
  (let ((gc-cons-threshold most-positive-fixnum)
        (gc-cons-percentage 0)
        (native-comp-jit-compilation nil))
    (list
     (mapcar
      (lambda (size)
        (let ((before (nth 2 (assq 'vector-slots (garbage-collect)))))
          (push (make-vector size 'rounded-marker) rounded-census-kept)
          (list size
                (- (nth 2 (assq 'vector-slots (garbage-collect))) before))))
      '(0 1 2 3 4 5 6 7 8 31 32 511 512 1023 1024))
     (mapcar
      (lambda (arguments)
        (let ((before (nth 2 (assq 'vector-slots (garbage-collect)))))
          (push (apply #'make-interpreted-closure arguments) rounded-census-kept)
          (list (length (car rounded-census-kept))
                (- (nth 2 (assq 'vector-slots (garbage-collect))) before))))
      '((nil (nil) (t))
        (nil (nil) (t) "documented")
        (nil (nil) (t) nil (interactive)))))))
