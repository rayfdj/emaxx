(mapcar (lambda (range)
          (condition-case err
              (let ((map (make-keymap)))
                (define-key map (vector range) 'binding))
            (error (list (car err) (cadr err) (nth 2 err)))))
        '((97 . -1) (97 . 4194304) (97 . wrong) (4194304 . 97) (-1 . 97)))
