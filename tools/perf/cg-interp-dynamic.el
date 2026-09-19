;; callgrind-diff.sh probe: the dynamic interpreted loop (the audit's instructions an
;; iteration; the second run length is the first times three).
(defvar interp-form '(let ((n 0)) (let ((i 0)) (while (< i 100000) (let* ((x (and (> i -1) i))) (if x (setq n (+ n 1)))) (setq i (1+ i)))) n))
(eval interp-form nil)
