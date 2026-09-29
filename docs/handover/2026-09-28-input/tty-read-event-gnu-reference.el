(let ((unread-command-events '((down-mouse-1 (nil menu-bar (5 . 0) 0)) (mouse-1 (nil menu-bar (5 . 0) 0)) 97 98 99))) (prin1 (list (read-event) (read-event) (read-event) unread-command-events)))
