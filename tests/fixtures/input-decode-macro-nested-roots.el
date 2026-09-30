(let* ((overriding-local-map (make-sparse-keymap))
       (executing-kbd-macro nil)
       (executing-kbd-macro-index 7)
       (trace nil)
       (outer (vector 'reader-outer 'reader-old))
       (kbd-macro-termination-hook
        (list (lambda ()
                (garbage-collect)
                (push (list 'terminated (eq executing-kbd-macro outer)
                            executing-kbd-macro-index) trace)))))
  (define-key overriding-local-map [reader-outer]
    (lambda ()
      (interactive)
      (push (list 'outer executing-kbd-macro-index) trace)
      (execute-kbd-macro (vector 'reader-inner))
      (push (list 'restored (eq executing-kbd-macro outer)
                  executing-kbd-macro-index) trace)))
  (define-key overriding-local-map [reader-inner]
    (lambda ()
      (interactive)
      (aset outer 1 'reader-changed)
      (garbage-collect)
      (push 'inner trace)))
  (define-key overriding-local-map [reader-old]
    (lambda () (interactive) (push 'old trace)))
  (define-key overriding-local-map [reader-changed]
    (lambda () (interactive) (push 'changed trace)))
  (define-key overriding-local-map [reader-error]
    (lambda () (interactive) (error "reader nested failure")))
  (execute-kbd-macro outer)
  (condition-case failure
      (execute-kbd-macro (vector 'reader-error))
    (error (push (list 'error (cadr failure) executing-kbd-macro
                       executing-kbd-macro-index) trace)))
  (nreverse trace))
