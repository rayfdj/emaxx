;;; -*- lexical-binding: t; -*-
(let ((build
       (lambda (values)
         (let* ((chunks (mapcar (lambda (value)
                                 (prin1-to-string (string 2 value 256))) values))
                (depth2 (concat "#^^[2 4096 " (mapconcat #'identity chunks " ")
                                (apply #'concat (make-list (- 32 (length values)) " nil")) "]"))
                (depth1 (concat "#^^[1 0 nil " depth2
                                (apply #'concat (make-list 14 " nil")) "]")))
           (read (concat "#^[nil nil char-code-property-table nil " depth1
                         (apply #'concat (make-list 63 " nil"))
                         " nil nil nil nil nil]"))))))
  (mapcar (lambda (spec)
            (let* ((table (funcall build (car spec)))
                   (control (copy-sequence table))
                   (value (char-table-range table (cadr spec))))
              (list value
                    (progn (aref control (nth 2 spec)) (equal table control))
                    (progn (aref control (nth 3 spec)) (equal table control))
                    (progn (aref control (nth 4 spec)) (equal table control)))))
          '(((7 7 7) (4096 . 4479) 4096 4224 4352)
            ((7 8 7) (4096 . 4479) 4096 4224 4352)
            ((7 7 7) (4096 . 4096) 4096 4224 4352)
            ((7 8 7) (4224 . 4096) 4224 4096 4352))))
