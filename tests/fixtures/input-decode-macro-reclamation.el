(progn
  (setq reader-macro-weak (make-hash-table :test 'eq :weakness 'key)
        reader-macro-root-trace nil)
  (fset 'reader-drop-macro
        (lambda ()
          (interactive)
          (setq executing-kbd-macro t)
          (garbage-collect)
          (push (list 'inside (hash-table-count reader-macro-weak))
                reader-macro-root-trace)
          nil))
  (fset 'reader-drop-macro-error
        (lambda ()
          (interactive)
          (reader-drop-macro)
          (error "macro reclamation control")))
  (mapcar
   (lambda (command)
     (clrhash reader-macro-weak)
     (setq reader-macro-root-trace nil)
     (let ((overriding-terminal-local-map (make-sparse-keymap))
           (kbd-macro-termination-hook
            (list (lambda ()
                    (garbage-collect)
                    (push (list 'termination (hash-table-count reader-macro-weak))
                          reader-macro-root-trace)))))
       (define-key overriding-terminal-local-map [f35] command)
       (funcall
        (lambda ()
          (let ((macro (vector 'f35)))
            (puthash macro t reader-macro-weak)
            (condition-case failure
                (execute-kbd-macro macro)
              (error (push (cons 'caught failure) reader-macro-root-trace)))))))
     (garbage-collect)
     (garbage-collect)
     (list (nreverse reader-macro-root-trace)
           (hash-table-count reader-macro-weak)))
   '(reader-drop-macro reader-drop-macro-error)))
