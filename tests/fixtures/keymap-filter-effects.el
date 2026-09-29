(progn
  (defvar keymap-filter-trace nil)
  (defvar keymap-filter-outer-errors nil)
  (let* ((top (make-sparse-keymap))
         (lower (make-sparse-keymap))
         (top-prefix (make-sparse-keymap))
         (lower-prefix (make-sparse-keymap))
         (keymap-filter-trace nil)
         (keymap-filter-outer-errors nil)
         (top-filter (lambda (command)
                       (push (list 'top inhibit-redisplay) keymap-filter-trace)
                       (garbage-collect)
                       command))
         (lower-filter (lambda (command)
                         (push (list 'lower inhibit-redisplay) keymap-filter-trace)
                         (garbage-collect)
                         command))
         answers)
    (define-key top [entry]
      (list 'menu-item "Top prefix" top-prefix :filter top-filter))
    (define-key lower [entry]
      (list 'menu-item "Lower prefix" lower-prefix :filter lower-filter))
    (define-key top-prefix [own] 'top-command)
    (define-key lower-prefix [leaf] 'lower-command)
    (dolist (keys '([entry absent tail] [entry own] [entry leaf]))
      (setq keymap-filter-trace nil)
      (push (list (lookup-key (list top lower) keys)
                  (reverse keymap-filter-trace)) answers))
    (set-keymap-parent top lower)
    (setq keymap-filter-trace nil)
    (push (list (lookup-key top [entry leaf])
                (reverse keymap-filter-trace)) answers)
    (define-key top [error-entry]
      '(menu-item "Error" ignore :filter
                  (lambda (_command) (garbage-collect) (error "filter failure"))))
    (push (handler-bind
              ((error (lambda (data) (push (car data) keymap-filter-outer-errors))))
            (list (lookup-key top [error-entry])
                  (lookup-key top [error-entry trailing])))
          answers)
    (push keymap-filter-outer-errors answers)
    (define-key top [exit-entry]
      '(menu-item "Exit" ignore :filter (lambda (_command) (throw 'filter-exit 'escaped))))
    (push (catch 'filter-exit (lookup-key top [exit-entry])) answers)
    (define-key top [quit-entry]
      '(menu-item "Quit" ignore :filter (lambda (_command) (signal 'quit nil))))
    (push (condition-case data (lookup-key top [quit-entry])
            (quit (car data))) answers)
    ;; Returning an anonymous prefix keeps that result live across the next filter.
    (define-key top [fresh]
      '(menu-item "Fresh" nil :filter
                  (lambda (_command)
                    (let ((prefix (make-sparse-keymap)))
                      (define-key prefix [leaf]
                        '(menu-item "Leaf" done :filter
                                    (lambda (command) (garbage-collect) command)))
                      prefix))))
    (push (lookup-key top [fresh leaf]) answers)
    (list (nreverse answers) inhibit-redisplay)))
