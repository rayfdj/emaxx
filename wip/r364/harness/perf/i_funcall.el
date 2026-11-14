(let ((n 0) (f (lambda (c) (setq n (1+ n))))) (dotimes (_ 200000) (dotimes (_ 62) (funcall f 97))) (princ n))
