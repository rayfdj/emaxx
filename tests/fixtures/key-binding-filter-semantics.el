(let (answers)
  (dolist (exit '(normal error quit throw))
    (let ((map (make-sparse-keymap))
          (prefix (make-sparse-keymap))
          (keys (vector 'prefix 'first))
          (calls 0))
      (define-key prefix [first] 'first-command)
      (define-key prefix [second] 'second-command)
      (define-key map [prefix]
        (list 'menu-item "Prefix" prefix
              :filter (lambda (definition)
                        (setq calls (1+ calls))
                        (aset keys 1 'second)
                        (garbage-collect)
                        (cond ((eq exit 'error) (error "filter failure"))
                              ((eq exit 'quit) (signal 'quit nil))
                              ((eq exit 'throw) (throw 'filter-exit 'thrown))
                              (t definition)))))
      (let ((overriding-local-map map))
        (push (list exit
                    (catch 'filter-exit
                      (condition-case data (key-binding keys)
                        (quit (car data)) (error data)))
                    (aref keys 1) calls)
              answers))))
  (let ((map (make-sparse-keymap))
        (replacement (make-sparse-keymap))
        (calls 0))
    (define-key replacement [remap original-command] 'remapped-command)
    (define-key map [leaf]
      (list 'menu-item "Leaf" 'original-command
            :filter (lambda (definition)
                      (setq calls (1+ calls))
                      (setq overriding-local-map replacement)
                      (garbage-collect)
                      definition)))
    (let ((overriding-local-map map))
      (push (list (key-binding [leaf]) calls) answers))
    (let ((overriding-local-map map))
      (push (list (key-binding [leaf] nil t) calls) answers)))
  (let ((map (make-sparse-keymap))
        (prefix (make-sparse-keymap))
        (calls 0))
    (define-key prefix [original-command]
      (list 'menu-item "Command" 'remapped-command
            :filter (lambda (definition)
                      (setq calls (1+ calls))
                      (garbage-collect)
                      definition)))
    (define-key map [remap]
      (list 'menu-item "Remap" prefix
            :filter (lambda (definition)
                      (setq calls (1+ calls))
                      (garbage-collect)
                      definition)))
    (push (list (command-remapping 'original-command nil map) calls
                (command-remapping '(function original-command) nil map)
                (key-binding [])) answers))
  (nreverse answers))
