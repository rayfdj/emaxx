(progn
 (defvar char-table-lucid-mode nil)
 (let ((map (make-keymap)))
  (define-key map [(97)] 'plain)
  (define-key map [(control 97)] 'control)
  (with-temp-buffer
    (use-local-map map)
    (let ((minor-mode-map-alist (list (cons 'char-table-lucid-mode map)))
          (char-table-lucid-mode t))
      (list (lookup-key map [97])
            (lookup-key map [(97)])
            (lookup-key map [(control 97)])
            (key-binding [(control 97)])
            (minor-mode-key-binding [(control 97)]))))))
