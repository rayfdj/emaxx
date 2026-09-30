(let ((file (make-temp-file "runtime-reader-prefix-" nil ".el"))
      (overriding-terminal-local-map (make-sparse-keymap))
      (input-decode-map (make-sparse-keymap))
      (local-function-key-map (make-sparse-keymap))
      (key-translation-map (make-sparse-keymap))
      (inhibit-message t)
      (trace nil))
  (unwind-protect
      (progn
        (write-region
         "(garbage-collect)\n(fset 'runtime-reader-lazy-prefix (let ((map (make-sparse-keymap))) (define-key map [b] 'runtime-reader-lazy-target) map))\n"
         nil file nil 'silent)
        (fmakunbound 'runtime-reader-lazy-prefix)
        (autoload 'runtime-reader-lazy-prefix file nil nil 'keymap)
        (fset 'runtime-reader-lazy-target
              (lambda () (interactive) (push (this-command-keys-vector) trace)))
        (define-key overriding-terminal-local-map [a] 'runtime-reader-lazy-prefix)
        (let* ((unread-command-events '(a b))
               (keys (read-key-sequence-vector nil)))
          (execute-kbd-macro [a b])
          (list keys (keymapp (symbol-function 'runtime-reader-lazy-prefix))
                (nreverse trace))))
    (delete-file file)
    (fmakunbound 'runtime-reader-lazy-prefix)
    (fmakunbound 'runtime-reader-lazy-target)))
