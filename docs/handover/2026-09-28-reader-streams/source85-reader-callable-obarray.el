(mapcar
 (lambda (reader)
   (let* ((old (obarray-make))
          (new (obarray-make))
          (obarray old)
          (read-symbol-shorthands nil)
          (characters (string-to-list "aa bb"))
          (changed nil)
          (stream (lambda (&optional character)
                    (if character
                        (progn
                          (unless changed
                            (setq changed t obarray new
                                  read-symbol-shorthands '(("aa" . "renamed-"))))
                          (push character characters))
                      (pop characters))))
          (one (funcall reader stream))
          (two (funcall reader stream)))
     (list (symbol-name (bare-symbol one))
           (symbol-name (bare-symbol two))
           (eq (bare-symbol one) (intern "renamed-" new))
           (null (intern-soft "aa" old))
           (null (intern-soft "renamed-" old))
           (and (symbol-with-pos-p one) (symbol-with-pos-pos one))
           (and (symbol-with-pos-p two) (symbol-with-pos-pos two)))))
 '(read read-positioning-symbols))
