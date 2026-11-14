;; print_preprocess walks a string's intervals with traverse_intervals_noorder
;; (print.c:1434, intervals.c:237): the root first, then its left subtree, then its right.
(dolist (n '(2 4 7))
  (let* ((objs (mapcar (lambda (i) (list i)) (number-sequence 1 n)))
         (s (mapconcat (lambda (o) (propertize "x" 'k o 'j o)) objs ""))
         (print-circle t))
    (prin1 s) (terpri)))
