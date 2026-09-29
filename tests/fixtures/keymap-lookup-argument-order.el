(let ((map (make-sparse-keymap)) answers)
  (fset 'lookup-argument-map map)
  (unwind-protect
      (progn
        (define-key map [leaf] 'command)
        (dolist (object '(17 "invalid" [keymap] missing-lookup-map))
          (dolist (keys '([] [leaf] 73))
            (push (condition-case data (lookup-key object keys)
                    (wrong-type-argument
                     (list (car data) (cadr data) (eq object (caddr data)))))
                  answers)))
        (list (nreverse answers)
              (eq (lookup-key 'lookup-argument-map []) map)
              (lookup-key 'lookup-argument-map [leaf])))
    (fmakunbound 'lookup-argument-map)))
