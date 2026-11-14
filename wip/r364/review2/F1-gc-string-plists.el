;; the function printed to drops the string's properties and collects garbage
;; while the string is printed: GNU prints the copy print_prune_string_charset made
(defvar junk nil)
(defun churn () (garbage-collect) (setq junk nil) (dotimes (i 300000) (setq junk (cons (list i 'junk "junk") junk))) (garbage-collect))
(defun mk () (mapconcat (lambda (i) (propertize (string (+ ?a i)) 'face (list 'x i (list 'y 1.5 i) (format "str%d" i)))) (number-sequence 0 7) ""))
(let* ((s (mk)) (acc nil) (done nil))
  (prin1 s (lambda (c) (push c acc)
             (when (and (= c ?\") (not done)) (setq done t) (set-text-properties 0 8 nil s) (churn))))
  (princ (concat (nreverse acc))) (terpri))
(let* ((s (mk)) (acc nil) (done nil))
  (prin1 s (lambda (c) (push c acc)
             (when (and (= c ?x) (not done)) (setq done t) (set-text-properties 0 8 nil s) (churn))))
  (princ (concat (nreverse acc))) (terpri))
