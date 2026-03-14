;;; emacs_perf_runner.el --- Structured perf runner for emaxx  -*- lexical-binding: t; -*-

(require 'cl-lib)
(require 'json)
(require 'seq)

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
  (mapcar
   (lambda (test)
     (let ((sample-values nil)
           (gc-count 0)
           (gc-seconds 0.0)
           (unsupported-note nil))
       (dotimes (sample (+ warmup samples))
         (garbage-collect)
         (let ((result (condition-case err
                           (cond
                            ((perf-variable-test-p test) (funcall test n))
                            ((perf-constant-test-p test) (funcall test))
                            (t nil))
                         (error
                          (setq unsupported-note (error-message-string err))
                          nil))))
           (cond
            ((and (consp result) (numberp (nth 0 result)))
             (when (>= sample warmup)
               (push (float (nth 0 result)) sample-values)
               (cl-incf gc-count (or (nth 1 result) 0))
               (cl-incf gc-seconds (float (or (nth 2 result) 0.0)))))
            (result)
            (t
             (unless unsupported-note
               (setq unsupported-note "benchmark returned nil"))))))
       (if sample-values
           (emaxx-perf--completed-case
            (symbol-name test)
            (nreverse sample-values)
            gc-count
            gc-seconds
            unsupported-note)
         (emaxx-perf--unsupported-case
          (symbol-name test)
          (or unsupported-note "benchmark produced no samples")))))
   (perf-expand-suites (list suite))))

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

(defun emaxx-perf--benchmark-case (case-id warmup samples thunk)
  (let ((sample-values nil)
        (gc-count 0)
        (gc-seconds 0.0)
        (unsupported-note nil))
    (dotimes (sample (+ warmup samples))
      (garbage-collect)
      (let ((result (condition-case err
                        (funcall thunk)
                      (error
                       (setq unsupported-note (error-message-string err))
                       nil))))
        (cond
         ((and (consp result) (numberp (nth 0 result)))
          (when (>= sample warmup)
            (push (float (nth 0 result)) sample-values)
            (cl-incf gc-count (or (nth 1 result) 0))
            (cl-incf gc-seconds (float (or (nth 2 result) 0.0)))))
         (result)
         (t
          (unless unsupported-note
            (setq unsupported-note "benchmark produced no samples"))))))
    (if sample-values
        (emaxx-perf--completed-case
         case-id
         (nreverse sample-values)
         gc-count
         gc-seconds
         unsupported-note)
      (emaxx-perf--unsupported-case case-id (or unsupported-note "benchmark produced no samples")))))

(defun emaxx-perf--scenario-tier (scenario-id)
  (pcase scenario-id
    ((or "noverlay/perf-marker-suite"
         "noverlay/perf-insert-delete-suite")
     "comparable")
    ("noverlay/perf-realworld-suite" "provisional")
    (_ "oracle_only")))

(defun emaxx-perf--scenario-group (scenario-id)
  (car (split-string scenario-id "/")))

(defun emaxx-perf-run-scenario (scenario-id n warmup samples)
  (let* ((cases
          (pcase scenario-id
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
         (status (if (seq-some (lambda (case)
                                 (equal (alist-get "status" case nil nil #'equal)
                                        "completed"))
                               cases)
                     "completed"
                   "unsupported"))
         (report
          `(("runner" . "oracle")
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
