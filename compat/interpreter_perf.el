;;; interpreter_perf.el --- Shared source-evaluator benchmarks  -*- lexical-binding: t; -*-

;; This file is deliberately loaded as source by both GNU Emacs and Emaxx.
;; The shared runner times these bodies and validates their returned checksums
;; after timing.  Neither editor substitutes host-language loops for them.

(defalias 'emaxx-perf-interpreted-list-walk
  #'(lambda (n)
     (let ((entries '((alpha . 3)
                      (beta . 5)
                      (gamma . 7)
                      (delta . 11)
                      (epsilon . 13)
                      (zeta . 17)
                      (eta . 19)
                      (theta . 23)))
           (iteration 0)
           (total 0))
       (while (< iteration n)
         (let ((cursor entries))
           (while cursor
             (let* ((entry (car cursor))
                    (key (car entry))
                    (value (cdr entry)))
               (setq total
                     (+ total
                        (if (or (eq key 'beta)
                                (eq key 'epsilon)
                                (eq key 'theta))
                            (* value 2)
                          value))))
             (setq cursor (cdr cursor))))
         (setq iteration (1+ iteration)))
       total)))

(defalias 'emaxx-perf-interpreted-cons-allocation
  #'(lambda (n)
     (let ((index 0)
           (rows nil)
           (total 0))
       (while (< index n)
         (setq rows
               (cons (cons (if (= (mod index 2) 0) 'even 'odd) index)
                     rows))
         (setq index (1+ index)))
       (while rows
         (let ((entry (car rows)))
           (setq total
                 (+ total
                    (cdr entry)
                    (if (eq (car entry) 'even) 3 7))))
         (setq rows (cdr rows)))
       total)))

(defalias 'emaxx-perf-interpreted--invoke
  #'(lambda (function value side)
     (funcall function value side)))

(defalias 'emaxx-perf-interpreted-function-calls
  #'(lambda (n)
     (let ((scale 3)
           (left-bias 5)
           (right-bias 9)
           (index 0)
           (total 0))
       (let ((step #'(lambda (value side)
                     (+ (* scale value)
                        (if (eq side 'left) left-bias right-bias)))))
         (while (< index n)
           (setq total
                 (+ total
                    (emaxx-perf-interpreted--invoke
                     step index (if (= (mod index 2) 0) 'left 'right))))
           (setq index (1+ index))))
       total)))

(provide 'interpreter_perf)

;;; interpreter_perf.el ends here
