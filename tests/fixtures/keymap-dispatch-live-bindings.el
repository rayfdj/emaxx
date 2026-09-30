(let ((saved (current-global-map))
      (map (make-sparse-keymap))
      (prefix (make-sparse-keymap))
      (filter-count 0)
      calls)
  (unwind-protect
      (progn
        (use-global-map map)
        (define-key map [24] (lambda () (interactive) (push 'rebound-x calls)))
        (define-key prefix [97] (lambda () (interactive) (push 'custom-prefix calls)))
        (define-key map [17] prefix)
        (define-key map [66]
          (list 'menu-item "Filtered"
                (lambda () (interactive) (push 'filtered calls))
                :filter (lambda (definition)
                          (setq filter-count (1+ filter-count))
                          (garbage-collect)
                          definition)))
        (execute-kbd-macro [24])
        (execute-kbd-macro [17 97])
        (execute-kbd-macro [66])
        (list (nreverse calls) filter-count))
    (use-global-map saved)))
