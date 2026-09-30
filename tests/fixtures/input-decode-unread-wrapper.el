(mapcar
 (lambda (seconds)
   (mapcar
    (lambda (event)
      (let ((unread-command-events (list event))
            (executing-kbd-macro nil))
        (list (read-event nil nil seconds) unread-command-events)))
    (list 97 'reader-direct-key (cons t 98)
          (cons t 'reader-wrapped-key) (cons t (logior (lsh 1 27) 99)))))
 '(nil 0))
