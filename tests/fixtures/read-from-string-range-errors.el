(let ((bounds (list nil -10 -4 -3 -2 -1 0 1 2 3 4 10
                    1.5 (expt 2 100) 'range-input-59273))
      (rows nil))
  (dolist (source (list "abc" (propertize (string 955 946 947) 'range-property 59273) ""))
    (dolist (start bounds)
      (dolist (end bounds)
        (push
         (list start end
               (condition-case condition
                   (list 'ok (read-from-string source start end))
                 (error (list 'error condition
                              (eq (nth 1 condition) source)))))
         rows))))
  (dolist (source (list nil 19 'range-source-59273 [1 2]))
    (push (condition-case condition
              (read-from-string source 'bad-start 'bad-end)
            (error condition))
          rows))
  (nreverse rows))
