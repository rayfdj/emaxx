(let ((s (make-string 100000 ?x)) (l (make-list 20000 'sym)))
  (dotimes (_ 200) (prin1 s) (princ s) (prin1 l) (prin1-to-string l) (prin1-to-string s)))
