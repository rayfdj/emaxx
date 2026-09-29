(let* ((map (make-sparse-keymap))
       (name (intern (string-make-multibyte "Qux-Casetable-Γ")))
       (key (vector 'menu-bar name))
       (saved-registry char-code-property-alist)
       result)
  (define-key map (vector 'menu-bar (intern "qux-casetable-γ")) 'unicode-first)
  (define-key map (vector 'menu-bar (intern "zux-casetable-γ")) 'local-second)
  (with-case-table (copy-case-table (current-case-table))
    (aset (current-case-table) ?Q ?z)
    (setq result (list (multibyte-string-p (symbol-name name))
                       (lookup-key map key)))
    (define-key map (vector 'menu-bar (intern "qux-casetable-γ")) nil)
    (setq result (append result (list (lookup-key map key))))
    (define-key map (vector 'menu-bar (intern "qux-casetable-γ")) 'unicode-restored)
    (unwind-protect
        (progn
          (setq char-code-property-alist
                (assq-delete-all 'lowercase
                                 (copy-sequence char-code-property-alist)))
          (garbage-collect)
          (setq result (append result (list (lookup-key map key)))))
      (setq char-code-property-alist saved-registry)))
  result)
