(progn
  (defvar let-order-trace nil)
  (defvar let-order-form nil)
  (list
   ;; All initializers run before let validates any variable name.
   (let ((let-order-trace nil))
     (list
      (condition-case error-data
          (eval '(let ((7 (setq let-order-trace (cons 'first let-order-trace)))
                       (value (progn
                                (setq let-order-trace (cons 'second let-order-trace))
                                (error "initializer-error"))))))
        (error error-data))
      (reverse let-order-trace)))
   ;; The binding pass rereads both individual names and the varlist itself.
   (let ((let-order-form
          '(let ((original-name
                  (progn
                    (setcar (car (cadr let-order-form)) 'changed-name)
                    41)))
             (list (boundp 'original-name) changed-name))))
     (eval let-order-form))
   (let ((let-order-trace nil))
     (list
      (condition-case error-data
          (eval '(let (nil
                       (value (progn
                                (setq let-order-trace (cons 'initialized let-order-trace))
                                73)))))
        (error error-data))
      let-order-trace))
   (let ((let-order-form
          '(let ((original-name
                  (progn
                    (setcar (cdr let-order-form) '((replacement-name 999)))
                    41)))
             (list (boundp 'original-name) replacement-name))))
     (eval let-order-form))
   ;; The body is read after setup, so replacing it takes effect.
   (let ((let-order-form
          '(let ((value
                  (progn
                    (setcdr (cdr let-order-form) '((1+ value)))
                    40)))
             -7)))
     (eval let-order-form))
   ;; let* validates each saved name after evaluating that initializer.
   (let ((let-order-trace nil))
     (list
      (condition-case error-data
          (eval '(let* ((7 (progn
                             (setq let-order-trace (cons 'initialized let-order-trace))
                             (error "initializer-error"))))))
        (error error-data))
      let-order-trace))
   (let ((let-order-form
          '(let* ((original-name
                   (progn
                     (setcar (car (cadr let-order-form)) 'changed-name)
                     42)))
             (list original-name (boundp 'changed-name)))))
     (eval let-order-form))
   ;; let* reads the next cdr after the initializer, not before it.
   (let ((let-order-form
          '(let* ((value (progn (setcdr (cadr let-order-form) nil) 1))
                  (later-name (error "removed initializer")))
             (list value (boundp 'later-name)))))
     (eval let-order-form))
   ;; Circular binding lists signal, with let and let*'s distinct ordering.
   (mapcar
    (lambda (operator)
      (let ((let-order-trace nil)
            (bindings (list '(value
                              (setq let-order-trace
                                    (cons 'initialized let-order-trace))))))
        (setcdr bindings bindings)
        (condition-case error-data
            (eval (list operator bindings))
          (circular-list
           (list (car error-data) (eq (cadr error-data) bindings)
                 (length let-order-trace))))))
    '(let let*))))
