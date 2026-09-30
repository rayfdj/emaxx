(progn
  (put 'down-mouse-1 'event-kind 'mouse-click)
  (put 'mouse-1 'event-kind 'mouse-click)
  (mapcar
   (lambda (shared)
     (let* ((position (list nil 'menu-bar '(5 . 0) 0))
            (unread-command-events
             (list (list 'down-mouse-1 position)
                   (list 'mouse-1 (if shared position
                                   (list nil 'menu-bar '(5 . 0) 0))))))
       (let ((keys (read-key-sequence-vector nil)))
         (list keys (key-binding keys) (this-single-command-raw-keys)))))
   '(nil t)))
