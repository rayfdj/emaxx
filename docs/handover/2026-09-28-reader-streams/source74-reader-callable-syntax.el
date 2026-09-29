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
      '("(aa . bb) tail" "(.foo 2) tail" ",symbol tail"
        "#x+2f tail" "#16r2f tail" "#xgg tail"
        "#_ss-x tail" "#: tail" "## tail"
        "\"\\é\" tail" "?\\u03B1 tail" "?\\C-\\M-a tail")))
   '(read read-positioning-symbols)))
