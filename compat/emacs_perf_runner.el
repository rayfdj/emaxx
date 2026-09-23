;;; emacs_perf_runner.el --- Structured perf runner for emaxx  -*- lexical-binding: t; -*-

(require 'cl-lib)
(require 'json)
(require 'seq)
(require 'benchmark)

(defun emaxx-perf--write-report (report)
  (let ((path (getenv "EMAXX_PERF_RESULT_FILE")))
    (unless path
      (error "EMAXX_PERF_RESULT_FILE is not set"))
    (with-temp-file path
      (insert (json-encode report)))))

(defun emaxx-perf--sample-summary (samples)
  (let* ((sorted (sort (copy-sequence samples) #'<))
         (len (length sorted))
         (min (nth 0 sorted))
         (max (nth (1- len) sorted))
         (mean (/ (apply #'+ sorted) (float len)))
         (median (emaxx-perf--percentile sorted 0.5))
         (p95 (emaxx-perf--percentile sorted 0.95)))
    (list min median mean p95 max)))

(defun emaxx-perf--percentile (sorted percentile)
  (let* ((len (length sorted))
         (idx (round (* (1- len) percentile))))
    (nth (min idx (1- len)) sorted)))

(defun emaxx-perf--completed-case (case-id samples gc-count gc-seconds &optional notes)
  (pcase-let ((`(,min ,median ,mean ,p95 ,max)
               (emaxx-perf--sample-summary samples)))
    `(("case_id" . ,case-id)
      ("status" . "completed")
      ("metric_unit" . "seconds")
      ("samples" . ,(vconcat samples))
      ("min" . ,min)
      ("median" . ,median)
      ("mean" . ,mean)
      ("p95" . ,p95)
      ("max" . ,max)
      ("gc_count" . ,gc-count)
      ("gc_seconds" . ,gc-seconds)
      ("notes" . ,notes))))

(defun emaxx-perf--unsupported-case (case-id note)
  `(("case_id" . ,case-id)
    ("status" . "unsupported")
    ("metric_unit" . "seconds")
    ("samples" . [])
    ("min" . nil)
    ("median" . nil)
    ("mean" . nil)
    ("p95" . nil)
    ("max" . nil)
    ("gc_count" . 0)
    ("gc_seconds" . 0.0)
    ("notes" . ,note)))

(defun emaxx-perf--suite-cases (suite n warmup samples)
  (let ((occurrences nil))
    (mapcar
     (lambda (test)
       ;; The pinned perf-noc-suite repeats one benchmark.  Run every
       ;; occurrence and identify it separately so no map drops a sample.
       (let* ((count (1+ (or (alist-get test occurrences) 0)))
              (case-id (if (= count 1) (symbol-name test)
                         (format "%s#%d" test count))))
         (setf (alist-get test occurrences) count)
         (emaxx-perf--benchmark-case
          case-id warmup samples
          (lambda ()
            (cond ((perf-variable-test-p test) (funcall test n))
                  ((perf-constant-test-p test) (funcall test))
                  (t (error "Invalid upstream benchmark: %S" test)))))))
     (perf-expand-suites (list suite)))))

(defun emaxx-perf--coding-decoder-cases (warmup samples)
  (generate-benchmark-test-file)
  (let (cases)
    (dolist (files test-file-list)
      (dolist (file (cdr files))
        (let ((path (car file)))
          (push
           (emaxx-perf--benchmark-case
            (format "without-optimization/%s" (file-name-nondirectory path))
            warmup
            samples
            (lambda ()
              (let ((disable-ascii-optimization t))
                (benchmark-run 10
                  (with-temp-buffer
                    (insert-file-contents path))))))
           cases)
          (push
           (emaxx-perf--benchmark-case
            (format "with-optimization/%s" (file-name-nondirectory path))
            warmup
            samples
            (lambda ()
              (let ((disable-ascii-optimization nil))
                (benchmark-run 10
                  (with-temp-buffer
                    (insert-file-contents path))))))
           cases))))
    (nreverse cases)))

(defun emaxx-perf--interpreter-cases (n warmup samples)
  (let* ((even-count (/ (+ n 1) 2))
         (odd-count (/ n 2))
         (triangular (/ (* n (1- n)) 2)))
    (mapcar
     (lambda (case)
       (emaxx-perf--benchmark-case
        (symbol-name (car case)) warmup samples
        (lambda ()
          (let* ((result nil)
                 (measurement (benchmark-run 1
                                (setq result (funcall (car case) n)))))
            (unless (equal result (cdr case))
              (error "%s produced %S; expected %S; timing %S"
                     (car case) result (cdr case) measurement))
            measurement))))
     `((emaxx-perf-interpreted-list-walk . ,(* n 139))
       (emaxx-perf-interpreted-cons-allocation
        . ,(+ triangular (* even-count 3) (* odd-count 7)))
       (emaxx-perf-interpreted-function-calls
        . ,(+ (* 3 triangular) (* even-count 5) (* odd-count 9)))))))

(defun emaxx-perf--benchmark-case (case-id warmup samples thunk)
  (let ((sample-values nil)
        (gc-count 0)
        (gc-seconds 0.0))
    (dotimes (sample (+ warmup samples))
      (garbage-collect)
      ;; Errors must fail the process.  A partial set of successful samples
      ;; cannot turn an unsuccessful workload into a completed case.
      (let ((result (funcall thunk)))
        (princ (format "PERF-SAMPLE %s %d %S\n" case-id sample result))
        (unless (and (listp result) (= (length result) 3)
                     (numberp (nth 0 result)) (> (nth 0 result) 0)
                     (integerp (nth 1 result)) (>= (nth 1 result) 0)
                     (numberp (nth 2 result)) (>= (nth 2 result) 0))
          (error "%s returned an invalid timing sample: %S" case-id result))
        (when (>= sample warmup)
          (push (float (nth 0 result)) sample-values)
          (cl-incf gc-count (nth 1 result))
          (cl-incf gc-seconds (float (nth 2 result))))))
    (emaxx-perf--completed-case
     case-id (nreverse sample-values) gc-count gc-seconds)))

(defun emaxx-perf--scenario-tier (scenario-id)
  (pcase scenario-id
    ((or "interpreter/source-eval-suite"
         "noverlay/perf-marker-suite"
         "noverlay/perf-insert-delete-suite")
     "comparable")
    ("noverlay/perf-realworld-suite" "provisional")
    (_ "oracle_only")))

(defun emaxx-perf--scenario-group (scenario-id)
  (car (split-string scenario-id "/")))

(defun emaxx-perf-run-scenario (scenario-id n warmup samples)
  (unless (and (integerp n) (> n 0)
               (integerp warmup) (>= warmup 0)
               (integerp samples) (> samples 0))
    (error "Invalid performance parameters: %S %S %S" n warmup samples))
  (let* ((cases
          (pcase scenario-id
            ("interpreter/source-eval-suite"
             (emaxx-perf--interpreter-cases n warmup samples))
            ("noverlay/perf-marker-suite"
             (emaxx-perf--suite-cases 'perf-marker-suite n warmup samples))
            ("noverlay/perf-insert-delete-suite"
             (emaxx-perf--suite-cases 'perf-insert-delete-suite n warmup samples))
            ("noverlay/perf-realworld-suite"
             (emaxx-perf--suite-cases 'perf-realworld-suite n warmup samples))
            ("noverlay/perf-display-suite"
             (emaxx-perf--suite-cases 'perf-display-suite n warmup samples))
            ("noverlay/perf-noc-suite"
             (emaxx-perf--suite-cases 'perf-noc-suite n warmup samples))
            ("coding/decoder"
             (emaxx-perf--coding-decoder-cases warmup samples))
            (_ (error "Unknown perf scenario: %s" scenario-id))))
         (status "completed")
         (report
          `(("runner" . ,(or (getenv "EMAXX_PERF_RUNNER") "oracle"))
            ("scenario_id" . ,scenario-id)
            ("tier" . ,(emaxx-perf--scenario-tier scenario-id))
            ("status" . ,status)
            ("cases" . ,(vconcat cases))
            ("metadata" . (("group" . ,(emaxx-perf--scenario-group scenario-id))
                           ("n" . ,(number-to-string n))
                           ("warmup" . ,(number-to-string warmup))
                           ("samples" . ,(number-to-string samples)))))))
    (emaxx-perf--write-report report)
    report))

(provide 'emacs_perf_runner)
