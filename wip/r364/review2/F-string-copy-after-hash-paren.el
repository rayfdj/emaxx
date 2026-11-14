;; print.c:2349 copies the string (print_prune_string_charset) before it writes "#(" (2352):
;; what the function printed to does to the string from then on is not printed.
(let* ((s (propertize (copy-sequence "abcd") 'face 'bold)) (acc nil))
  (prin1 s (lambda (c) (push c acc) (when (= c ?#) (aset s 0 ?Z))))
  (princ (concat (nreverse acc))) (terpri))
(let* ((s (propertize (copy-sequence "abcd") 'face 'bold)) (acc nil))
  (prin1 s (lambda (c) (push c acc) (when (= c ?\() (aset s 1 ?Y))))
  (princ (concat (nreverse acc))) (terpri))
