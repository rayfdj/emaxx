;;; interpreter_perf.el --- Shared source-evaluator benchmarks  -*- lexical-binding: t; -*-

;; This file is deliberately loaded as source by both GNU Emacs and Emaxx.
;; Keep setup outside the timed calls in the runners.  Each case validates its
;; own checksum so a faster, semantically wrong implementation cannot produce
;; an accepted performance sample.

(defun emaxx-perf-interpreted--check (case actual expected)
  (if (= actual expected)
      t
    (error "%s produced %S; expected %S" case actual expected)))

(defun emaxx-perf-interpreted-list-walk (n)
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
    (emaxx-perf-interpreted--check
     'list-walk total (* n 139))))

(defun emaxx-perf-interpreted-cons-allocation (n)
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
    (let ((even-count (/ (+ n 1) 2))
          (odd-count (/ n 2)))
      (emaxx-perf-interpreted--check
       'cons-allocation
       total
       (+ (/ (* n (1- n)) 2)
          (* even-count 3)
          (* odd-count 7))))))

(defun emaxx-perf-interpreted--invoke (function value side)
  (funcall function value side))

(defun emaxx-perf-interpreted-function-calls (n)
  (let ((scale 3)
        (left-bias 5)
        (right-bias 9)
        (index 0)
        (total 0))
    (let ((step (lambda (value side)
                  (+ (* scale value)
                     (if (eq side 'left) left-bias right-bias)))))
      (while (< index n)
        (setq total
              (+ total
                 (emaxx-perf-interpreted--invoke
                  step index (if (= (mod index 2) 0) 'left 'right))))
        (setq index (1+ index))))
    (let ((left-count (/ (+ n 1) 2))
          (right-count (/ n 2)))
      (emaxx-perf-interpreted--check
       'function-calls
       total
       (+ (* 3 (/ (* n (1- n)) 2))
          (* left-count left-bias)
          (* right-count right-bias))))))

(provide 'interpreter_perf)

;;; interpreter_perf.el ends here
