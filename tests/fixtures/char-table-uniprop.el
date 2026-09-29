;;; -*- lexical-binding: t; -*-
(let ((build
       (lambda (compressed)
         (let* ((depth2 (concat "#^^[2 4096 " (prin1-to-string compressed)
                                (apply #'concat (make-list 31 " nil")) "]"))
                (depth1 (concat "#^^[1 0 nil " depth2
                                (apply #'concat (make-list 14 " nil")) "]")))
           (read (concat "#^[default nil char-code-property-table nil " depth1
                         (apply #'concat (make-list 63 " nil"))
                         " nil nil nil nil nil]"))))))
  (let* ((simple (funcall build (string 1 3 0 7 8)))
         (rle (funcall build (string 2 4 131 0 130 9)))
         (copy (copy-sequence simple))
         (before (equal simple copy)))
    (list before
          (mapcar (lambda (char) (aref simple char)) '(4096 4099 4100 4101 4102 4223))
          (mapcar (lambda (char) (aref rle char)) '(4096 4097 4098 4099 4100 4101 4102 4223))
          (equal simple copy)
          (progn (aref copy 4100) (equal simple copy))
          (progn (aset simple 4100 'changed) (list (aref simple 4100) (aref copy 4100))))))
