(mapcar
 (lambda (reader)
   (let* ((chars (string-to-list "(one) (two)"))
          (source (lambda (&optional char)
                    (if char (push char chars) (pop chars))))
          (first (funcall reader source))
          (remaining (length chars))
          (second (condition-case error (funcall reader source)
                    (error (car error)))))
     (list (symbol-name (bare-symbol (car first))) remaining
           (if (consp second)
               (symbol-name (bare-symbol (car second))) second))))
 '(read read-positioning-symbols))
