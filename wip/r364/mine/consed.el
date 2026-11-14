;;; -*- lexical-binding: t -*-
;; print_prune_string_charset copies each string `prin1' prints unless
;; `print-charset-text-property' is t: a string and its characters
;; consed, whatever the print goes to.
(defun consed (thunk)
  (let ((before (memory-use-counts)) after)
    (dotimes (_ 100) (funcall thunk))
    (setq after (memory-use-counts))
    (list (- (nth 4 after) (nth 4 before)) (- (nth 6 after) (nth 6 before)))))
(prin1-to-string "warm")
(princ (list
        'function (consed (lambda () (prin1 "beta" #'ignore)))
        'princ (consed (lambda () (princ "beta" #'ignore)))
        'buffer (with-temp-buffer (consed (lambda () (prin1 "beta" (current-buffer)))))
        'string (consed (lambda () (prin1-to-string "beta")))
        'format (consed (lambda () (format "%S" "beta")))
        'list (consed (lambda () (prin1-to-string '("ab" sym "cde" 1.5))))
        'kept (let ((print-charset-text-property t)) (consed (lambda () (prin1 "beta" #'ignore))))
        'properties (let ((s (propertize "beta" 'face 'bold))) (consed (lambda () (prin1 s #'ignore))))
        'multibyte (consed (lambda () (prin1 "é€" #'ignore)))
        'empty (consed (lambda () (prin1 "" #'ignore)))
        'symbol (consed (lambda () (prin1 'beta #'ignore)))))
(terpri)
(princ (list 'stdout (consed (lambda () (prin1 "b")))))
(terpri)
