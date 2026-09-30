(progn
  (defun runtime-reader-command-setup ()
    (setq runtime-reader-calls nil
          runtime-reader-child (make-sparse-keymap)
          runtime-reader-replacement (make-sparse-keymap)
          overriding-terminal-local-map (make-sparse-keymap)
          input-decode-map (make-sparse-keymap)
          local-function-key-map (make-sparse-keymap)
          key-translation-map (make-sparse-keymap))
    (define-key runtime-reader-replacement [remap ignore] 'forward-char)
    (define-key runtime-reader-child [b]
      '(menu-item "leaf" ignore :filter
                  (lambda (value)
                    (setq runtime-reader-calls (cons 'leaf runtime-reader-calls))
                    (garbage-collect)
                    value)))
    (define-key overriding-terminal-local-map [a]
      (list 'menu-item "prefix" runtime-reader-child :filter
            (lambda (value)
              (setq runtime-reader-calls (cons 'prefix runtime-reader-calls)
                    overriding-terminal-local-map runtime-reader-replacement
                    runtime-reader-child nil)
              (garbage-collect)
              value))))
  (runtime-reader-command-setup)
  (let ((unread-command-events '(a b)))
    (list (read-key-sequence-vector nil)
          (command-remapping 'ignore)
          (nreverse runtime-reader-calls))))
