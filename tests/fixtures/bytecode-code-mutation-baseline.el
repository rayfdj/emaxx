(let ((code (unibyte-string 192 135))
      (constants [41 67])
      function results)
  (setq function (make-byte-code 0 code constants 1))
  (push (list 'before (funcall function) (eq code (aref function 1))) results)
  (push (condition-case err
            (progn
              (aset (aref function 1) 0 193)
              (garbage-collect)
              (list 'after (aref (aref function 1) 0) (funcall function)))
          (error (list 'mutation-error err))) results)
  (nreverse results))
