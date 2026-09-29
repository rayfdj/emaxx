(let ((print-symbols-bare t))
  (mapcar
   (lambda (reader)
     (mapcar
      (lambda (text)
        (let* ((characters (string-to-list text))
               (events nil)
               (stream (lambda (&optional character)
                         (if character
                             (progn (push (list 'unread character) events)
                                    (push character characters))
                           (let ((next (pop characters)))
                             (push (list 'read next) events)
                             next))))
               (form (condition-case error (funcall reader stream)
                       (error (car error)))))
          (list (prin1-to-string form) characters (nreverse events))))
      '(".foo tail" "[.foo .25] tail" "(aa .foo) tail"
        "(.) tail" "(aa .) tail" "[.] tail" ". tail"
        "(. bb) tail" "[. bb] tail" "(aa .(bb)) tail"
        "(aa .'bb) tail" "(aa .#'bb) tail" "(aa .?x) tail"
        "(aa .;comment\n bb) tail")))
   '(read read-positioning-symbols)))
