(let ((map (make-sparse-keymap))
      (prefix (make-sparse-keymap))
      (calls 0))
  (define-key map [entry]
    `(menu-item "Filtered prefix" ,prefix
                :filter ,(lambda (command)
                          (setq calls (1+ calls))
                          (garbage-collect)
                          command)))
  (let ((missing (lookup-key map [entry absent tail])))
    (list missing calls
          (progn (setq calls 0)
                 (define-key prefix [leaf] 'finish)
                 (list (lookup-key map [entry leaf]) calls)))))
