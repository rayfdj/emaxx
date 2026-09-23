(prin1
 (length
  (catch 'audit-result
    (unwind-protect
        (throw 'audit-result (make-string 1024 ?x))
      (dotimes (_ 5) (garbage-collect))))))
(terpri)
