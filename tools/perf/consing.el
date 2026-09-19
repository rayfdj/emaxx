;; The audit's consing rows: 300k conses pushed and the collection over
;; them, ten collections of the idle booted heap, mapcar over 100k
;; twenty times.  emacs -Q --batch -l tools/perf/consing.el
;; consing and collection: 300k conses pushed, then a collection over them
(let ((t0 (float-time)) (l nil))
  (dotimes (i 300000) (push (cons i i) l))
  (let ((t1 (float-time)))
    (garbage-collect)
    (let ((t2 (float-time)))
      (princ (format "cons300k %.3f gc %.3f (%d conses live)\n" (- t1 t0) (- t2 t1)
                     (car (cdr (assq 'conses (garbage-collect))))))
      (setq l nil))))
(let ((t0 (float-time)))
  (dotimes (_ 10) (garbage-collect))
  (princ (format "gc-idle x10 %.3f\n" (- (float-time) t0))))
(let ((t0 (float-time)))
  (dotimes (_ 20) (mapcar (function 1+) (number-sequence 1 100000)))
  (princ (format "mapcar-2M %.3f\n" (- (float-time) t0))))
