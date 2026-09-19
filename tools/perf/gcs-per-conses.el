;; Collections per six million conses under the booted heap's threshold
;; (`gcs-done' before and after), and the census after an explicit
;; collection.  emacs -Q --batch -l tools/perf/gcs-per-conses.el
(let ((before gcs-done) (t0 (float-time)))
  (dotimes (i 6000000) (cons i i))
  (princ (format "collections %d in %.2f s; threshold %s percentage %s\n" (- gcs-done before) (- (float-time) t0) gc-cons-threshold gc-cons-percentage)))
(princ (format "gc: %S\n" (garbage-collect)))
(let ((before gcs-done) (t0 (float-time)))
  (dotimes (i 6000000) (cons i i))
  (princ (format "collections %d in %.2f s after an explicit gc\n" (- gcs-done before) (- (float-time) t0))))
