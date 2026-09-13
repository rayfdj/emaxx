;;; Collections and collector time across five in-process compiles.
(require 'comp)
(setq comp-running-batch-compilation t)
(native-compile '(lambda () 1))
(let ((g0 gcs-done) (e0 gc-elapsed))
  (dotimes (i 5)
    (native-compile `(lambda (x y) (if (> x ,i) (+ x y) (list x y ,i)))))
  (princ (format "%s: %d GCs, %.3f s in GC\n" invocation-name (- gcs-done g0) (- gc-elapsed e0))))
