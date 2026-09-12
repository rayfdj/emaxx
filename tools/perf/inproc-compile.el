;;; One warm-up, then N in-process native compiles of a small lambda
;;; (N from the environment variable PERF_N, default 1).
(require 'comp)
(setq comp-running-batch-compilation t)
(native-compile '(lambda () 1))
(dotimes (i (string-to-number (or (getenv "PERF_N") "1")))
  (native-compile `(lambda (x y) (if (> x ,i) (+ x y) (list x y ,i)))))
