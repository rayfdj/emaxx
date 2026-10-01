(progn
  (set 'runtime-metadata-global 17)
  (defvar runtime-metadata-dynamic 23)
  (let ((reports nil)
        (captured
         (eval '(let ((counter 40))
                  (function
                   (lambda (value)
                     (interactive
                      (progn
                        (garbage-collect)
                        (setq counter (1+ counter))
                        (list counter)))
                     value)))
               t)))
    (dolist (size '(1 17 257))
      (let* ((constants (make-vector size 'metadata-sentinel))
             (command
              (make-byte-code
               257 (unibyte-string 135) constants 1 nil
               '(progn (garbage-collect) (list runtime-metadata-global))))
             (from-caller
              (eval '(let ((runtime-metadata-global 99))
                       (list (call-interactively command)
                             runtime-metadata-global))
                    (list (cons 'command command)))))
        (aset constants 0 'metadata-mutated)
        (push (list (byte-code-function-p command)
                    (call-interactively command)
                    from-caller
                    (eq (aref command 2) constants)
                    (length (aref command 2)))
              reports)))
    (list
     (nreverse reports)
     (list (call-interactively captured)
           (call-interactively captured))
     (let ((runtime-metadata-dynamic 47))
       (call-interactively
        (make-byte-code 257 (unibyte-string 135) [] 1 nil
                        '(list runtime-metadata-dynamic))))
     (eval '(let ((runtime-metadata-global 99))
              (list
               (call-interactively
                '(lambda (value)
                   (interactive (list runtime-metadata-global))
                   value))
               runtime-metadata-global))
           t)
     (eval '(let ((runtime-metadata-global 99))
              (list (condition-case failure
                        (call-interactively command)
                      (error (car failure)))
                    runtime-metadata-global))
           (list (cons 'command
                       (make-byte-code
                        257 (unibyte-string 135) [unrelated] 1 nil
                        '(progn (garbage-collect)
                                (error "interactive control error")))))))))
