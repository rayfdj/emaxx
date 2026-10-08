(let ((saved-map (current-global-map))
      (map (make-sparse-keymap)))
  (unwind-protect
      (progn
        (use-global-map map)
        (define-key map [f49] 'ignore)
        (list
         (let ((unread-command-events '((down-mouse-1 nil 1) f49)))
           (read-key-sequence-vector nil))
         (progn
           (define-key map [down-mouse-1] 'ignore)
           (let ((unread-command-events '((down-mouse-1 nil 1) f49)))
             (list (read-key-sequence-vector nil) unread-command-events)))
         (progn
           (define-key map [down-mouse-1] nil)
           (let ((unread-command-events '((down-mouse-1 nil 1) f49)))
             (read-key-sequence-vector nil)))))
    (use-global-map saved-map)))
