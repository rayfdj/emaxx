(let ((map (make-sparse-keymap)))
  (define-key map [97] 'letter-a)
  (mapcar
   (lambda (keys)
     (condition-case data
         (let ((result (lookup-key map keys)))
           (if (eq result map) 'same-map result))
       (error data)))
   (list nil 17 'unexpected '(97) [97] "a" [] "")))
