(mapcar
 (lambda (array)
   (let ((executing-kbd-macro array)
         (executing-kbd-macro-index 0)
         (unread-command-events nil)
         (last-event-frame nil))
     (let ((first (read-event nil nil 0)))
       (list first executing-kbd-macro-index
             (read-event nil nil 0) executing-kbd-macro-index
             (read-event nil nil 0) executing-kbd-macro-index
             (eq last-event-frame 'macro)))))
 (list (vector 'reader-first 'reader-second)
       (vector 225 233)
       (string 225 233)
       (unibyte-string 225 233)))
