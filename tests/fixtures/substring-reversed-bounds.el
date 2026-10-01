(mapcar
 (lambda (arguments)
   (condition-case error
       (list 'returned (apply (car arguments) (cdr arguments)))
     (error error)))
 (list (list 'substring "abc" 2 1)
       (list 'substring-no-properties "abc" 2 1)
       (list 'substring [1 2 3] 2 1)))
