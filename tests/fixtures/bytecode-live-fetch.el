(prin1
 (list
  (mapcar
   (lambda (index)
     (let* ((code (unibyte-string 192 32 136 193 135 0 135))
            (constants (make-vector (1+ index) nil))
            (function (make-byte-code 0 code constants 1))
            (value (vector index 'same-object)))
       (aset constants index value)
       (aset constants 0
             (lambda ()
               ;; Replace a one-byte instruction and its following bytes
               ;; with a three-byte constant instruction during execution.
               (aset code 3 129)
               (aset code 4 (logand index 255))
               (aset code 5 (lsh index -8))
               (garbage-collect)))
       (let ((result (funcall function)))
         (list (eq result value) (aref result 0) (append code nil)))))
   '(2 17 257))
  (mapcar
   (lambda (size)
     (let* ((code (make-string size 0))
            (destination (- size 2))
            (constants (vector nil size))
            (function (make-byte-code 0 code constants 1)))
       (aset code 0 192)
       (aset code 1 32)
       (aset code 2 136)
       (aset code 3 130)
       (aset code 4 6)
       (aset code destination 193)
       (aset code (1+ destination) 135)
       (aset constants 0
             (lambda ()
               (aset code 4 (logand destination 255))
               (aset code 5 (lsh destination -8))
               (garbage-collect)))
       (list (funcall function) (aref code 4) (aref code 5))))
   '(17 257 513))
  (let* ((code (unibyte-string 192 32 136 193 135))
         (inner-constants
          (vector (lambda () (aset code 3 194) (garbage-collect))))
         (inner (make-byte-code 0 (unibyte-string 192 32 135) inner-constants 1))
         (function (make-byte-code 0 code (vector inner 41 67) 1)))
    (list (funcall function) (aref code 3)))
  ;; Dead bytes after the return are not executed or prevalidated by GNU.
  (funcall (make-byte-code 0 (unibyte-string 192 135 0) [73] 1))
  (let* ((code (unibyte-string 192 32 136 193 135))
         (constants (vector nil 41 89)))
    (aset constants 0 (lambda () (aset code 3 194) (garbage-collect)))
    (list (byte-code code constants 1) (aref code 3)))
  (byte-code (string-to-multibyte (unibyte-string 192 135)) [91] 1)))
