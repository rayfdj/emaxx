(list
 (mapcar
  (lambda (depth)
    (let* ((special-event-map (make-sparse-keymap))
           (parent special-event-map)
           (seen nil)
           (filters 0))
      (dotimes (_ depth)
        (let ((next (make-sparse-keymap)))
          (set-keymap-parent parent next)
          (setq parent next)))
      (define-key parent [sigusr1]
        (list 'menu-item "Signal callback"
              (lambda () (interactive) (push last-input-event seen))
              :filter (lambda (command)
                        (setq filters (1+ filters))
                        (garbage-collect)
                        command)))
      (condition-case error
          (progn
            (call-process "kill" nil nil nil "-USR1"
                          (number-to-string (emacs-pid)))
            (list (read-event nil nil 0.05) seen filters))
        (error (list 'failed (car error))))))
  '(0 40))
 (let ((special-event-map (make-sparse-keymap))
       (filters 0))
   (define-key special-event-map [sigusr1]
     (list 'menu-item "Rejected signal" 'ignore
           :filter (lambda (_command)
                     (setq filters (1+ filters))
                     (garbage-collect)
                     (error "No event binding"))))
   (condition-case error
       (progn
         (call-process "kill" nil nil nil "-USR1"
                       (number-to-string (emacs-pid)))
         (list (read-event nil nil 0.05) filters))
     (error (list 'failed (car error)))))
 (let ((special-event-map (make-sparse-keymap))
       (seen nil)
       (filters 0))
   (define-key special-event-map [thread-event]
     (list 'menu-item "Thread payload"
           (lambda ()
             (interactive)
             (let ((data (nth 3 last-input-event)))
               (setq seen (list (length (car data))
                                (aref (car data) 256)
                                (symbol-name (cadr data))))))
           :filter (lambda (command)
                     (setq filters (1+ filters))
                     (garbage-collect)
                     command)))
   (condition-case error
       (progn
         (thread-join
          (make-thread
           (lambda ()
             (thread-signal main-thread 'error
                            (list (make-string 257 ?q)
                                  (make-symbol "event-marker"))))))
         (list (read-event nil nil 0.05) seen filters))
     (error (list 'failed (car error))))))
