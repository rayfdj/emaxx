;;; emacs_compat_runner.el --- Oracle compat batch helper -*- lexical-binding: t; -*-

(require 'ert)
(require 'json)

(defvar emaxx-compat--tests-before-load nil
  "Names of ERT tests bound before the target file was loaded.")

(defvar emaxx-compat--selector
  (or (getenv "EMAXX_COMPAT_SELECTOR") "(quote t)")
  "Stringified selector used when synthesizing load-error reports.")

(defvar emaxx-compat--command-error-default-function command-error-function
  "Original command error function installed before the compat wrapper.")

(setq emaxx-compat--tests-before-load
      (sort
       (mapcar #'symbol-name
               (mapcar #'ert-test-name (ert-select-tests t t)))
       #'string<))

(defun emaxx-compat--result-status (result)
  (cond
   ((ert-test-passed-p result) "passed")
   ((ert-test-skipped-p result) "skipped")
   (t "failed")))

(defun emaxx-compat--result-condition-type (result)
  (when (ert-test-result-with-condition-p result)
    (symbol-name (car (ert-test-result-with-condition-condition result)))))

(defun emaxx-compat--result-message (result)
  (when (ert-test-result-with-condition-p result)
    (format "%S" (ert-test-result-with-condition-condition result))))

(defun emaxx-compat--test-metadata (test)
  (list
   (cons 'name (symbol-name (ert-test-name test)))
   (cons 'tags (vconcat (mapcar #'symbol-name (ert-test-tags test))))
   (cons 'expected_result (format "%S" (ert-test-expected-result-type test)))))

(defun emaxx-compat--sorted-tests (tests)
  (sort tests
        (lambda (left right)
          (string<
           (symbol-name (ert-test-name left))
           (symbol-name (ert-test-name right))))))

(defun emaxx-compat--run-tests (selector)
  (let* ((discovered (cl-remove-if
                      (lambda (test)
                        (member (symbol-name (ert-test-name test))
                                emaxx-compat--tests-before-load))
                      (ert-select-tests t t)))
         (selected (ert-select-tests selector discovered))
         (stats (ert--make-stats selected selector))
         (listener (lambda (&rest _ignored))))
    (setf (ert--stats-start-time stats) (current-time))
    (dolist (test selected)
      (ert-run-or-rerun-test stats test listener))
    (setf (ert--stats-end-time stats) (current-time))
    (list discovered selected stats)))

(defun emaxx-compat--relative-file ()
  (or (getenv "EMAXX_COMPAT_RELATIVE_FILE")
      (and load-file-name
           (file-relative-name load-file-name
                               (file-name-directory
                                (directory-file-name
                                 (getenv "EMACS_TEST_DIRECTORY")))))
      "unknown"))

(defun emaxx-compat--load-error-report (data)
  (list
   (cons 'runner "oracle")
   (cons 'file (emaxx-compat--relative-file))
   (cons 'selector emaxx-compat--selector)
   (cons 'file_status "load_error")
   (cons 'file_error (error-message-string data))
   (cons 'discovered_tests [])
   (cons 'selected_tests [])
   (cons 'results [])
   (cons 'summary
         '((total . 0)
           (passed . 0)
           (failed . 0)
           (skipped . 0)
           (unexpected . 0)))))

(defun emaxx-compat--maybe-write-load-error-report (data)
  (let ((result-file (getenv "EMAXX_BATCH_RESULT_FILE")))
    (when (and result-file
               (not (file-exists-p result-file)))
      (with-temp-file result-file
        (insert (json-encode (emaxx-compat--load-error-report data)))))))

(defun emaxx-compat--command-error-function (data context signal)
  (ignore-errors
    (emaxx-compat--maybe-write-load-error-report data))
  (funcall emaxx-compat--command-error-default-function data context signal))

(setq command-error-function #'emaxx-compat--command-error-function)

(defun emaxx-compat--report (selector)
  (cl-destructuring-bind (discovered selected stats)
      (emaxx-compat--run-tests selector)
    (setq discovered (emaxx-compat--sorted-tests discovered))
    (setq selected (emaxx-compat--sorted-tests selected))
    (list
     (cons 'runner "oracle")
     (cons 'file (emaxx-compat--relative-file))
     (cons 'selector (format "%S" selector))
     (cons 'file_status "loaded")
     (cons 'file_error nil)
     (cons 'discovered_tests
           (vconcat (mapcar #'emaxx-compat--test-metadata discovered)))
     (cons 'selected_tests
           (vconcat (mapcar (lambda (test)
                              (symbol-name (ert-test-name test)))
                            selected)))
     (cons 'results
           (vconcat
            (mapcar
             (lambda (test)
               (let ((result (ert-test-most-recent-result test)))
                 (list
                  (cons 'name (symbol-name (ert-test-name test)))
                  (cons 'status (emaxx-compat--result-status result))
                  (cons 'condition_type
                        (emaxx-compat--result-condition-type result))
                  (cons 'message (emaxx-compat--result-message result)))))
             selected)))
     (cons 'summary
           (list
            (cons 'total (ert-stats-total stats))
            (cons 'passed
                  (cl-count-if
                   (lambda (test)
                     (ert-test-passed-p
                      (ert-test-most-recent-result test)))
                   selected))
            (cons 'failed
                  (cl-count-if
                   (lambda (test)
                     (let ((result (ert-test-most-recent-result test)))
                       (and (not (ert-test-passed-p result))
                            (not (ert-test-skipped-p result)))))
                   selected))
            (cons 'skipped (ert-stats-skipped stats))
            (cons 'unexpected (ert-stats-completed-unexpected stats)))))))

(defun emaxx-compat-run (selector)
  (let ((result-file (getenv "EMAXX_BATCH_RESULT_FILE")))
    (unless result-file
      (error "EMAXX_BATCH_RESULT_FILE must be set"))
    (with-temp-file result-file
      (insert (json-encode (emaxx-compat--report selector))))))
