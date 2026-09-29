(progn
  (require 'cl-lib)
  (let ((map (make-sparse-keymap)))
    (define-key map [97] 'letter-a)
    (mapcar
     (lambda (parsed)
       (cl-letf (((symbol-function 'key-valid-p) (lambda (_description) t))
                 ((symbol-function 'key-parse)
                  (lambda (_description) (garbage-collect) parsed)))
         (condition-case error
             (lookup-key map ["changed-description"])
           (error error))))
     (list nil 17 'unexpected '(97) [] "" [97] "a"))))
