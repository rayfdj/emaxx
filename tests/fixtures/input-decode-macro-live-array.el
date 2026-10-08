(let ((overriding-local-map (make-sparse-keymap))
      (trace nil))
  (define-key overriding-local-map [reader-edit]
    (lambda ()
      (interactive)
      (push (list 'edit executing-kbd-macro-index) trace)
      (aset executing-kbd-macro 1 'reader-changed)
      (garbage-collect)))
  (define-key overriding-local-map [reader-changed]
    (lambda () (interactive) (push 'changed trace)))
  (define-key overriding-local-map [reader-old]
    (lambda () (interactive) (push 'old trace)))
  (define-key overriding-local-map [reader-stop]
    (lambda ()
      (interactive)
      (push (list 'stop executing-kbd-macro-index) trace)
      (setq executing-kbd-macro t)))
  (define-key overriding-local-map [reader-replace]
    (lambda ()
      (interactive)
      (push (list 'replace executing-kbd-macro-index) trace)
      (setq executing-kbd-macro (vector 'reader-changed)
            executing-kbd-macro-index 0)
      (garbage-collect)))
  (execute-kbd-macro (vector 'reader-edit 'reader-old))
  (execute-kbd-macro (vector 'reader-stop 'reader-old))
  (execute-kbd-macro (vector 'reader-replace 'reader-old))
  (nreverse trace))
