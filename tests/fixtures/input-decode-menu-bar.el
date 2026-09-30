(let* ((position (list nil 'menu-bar '(5 . 0) 0))
       (unread-command-events
        (list (list 'down-mouse-1 position)
              (list 'mouse-1 position))))
  (let ((keys (read-key-sequence-vector nil)))
    (list keys (key-binding keys) (this-single-command-raw-keys))))
