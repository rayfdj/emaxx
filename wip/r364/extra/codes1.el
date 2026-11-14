(dolist (n (list -1 -2 -129 -1000 -65536 #x400000 #x3fffff #x3fff80 #x8000041 #x4000041
                 #x10000041 #x20000061 #x2000040 #x100000041 (1+ (ash 1 32)) (- (ash 1 31))
                 #x7fffffff #xffffffff most-negative-fixnum))
  (condition-case e (write-char n) (error (princ (format "E%S" e))))
  (princ "|")
  (condition-case e (write-char n 'external-debugging-output) (error (princ (format "E%S" e))))
  (external-debugging-output ?|)
  (condition-case e (write-char n (lambda (c) (princ (format "%S" c)))) (error (princ (format "E%S" e))))
  (princ "|")
  (condition-case e (external-debugging-output n) (error (princ (format "E%S" e))))
  (external-debugging-output ?\n)
  ;; In a buffer a negative code whose low byte is no ASCII character is
  ;; a byte that is no character, and what is read back is past it.
  (unless (and (>= (logand n #xffffffff) #x80000000) (>= (logand n #xff) #x80))
    (with-temp-buffer
      (condition-case e (write-char n (current-buffer)) (error (princ (format "E%S" e))))
      (princ (format "%S" (append (buffer-string) nil)))))
  (terpri))
