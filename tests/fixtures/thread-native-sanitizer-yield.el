;;; -*- lexical-binding: t; -*-
(require 'comp)
(require 'cl-lib)
;; Native code compiled with the sanitizer's assertions
;; (helper_sanitizer_assert), run with the sanitizer active and a
;; `cl-typep' that parks the thread until the other thread has had its
;; turn and then answers nil: the helper goes on to its message, its
;; backtrace and its signal after the thread resumes.
(defvar helper-yield-turn 0)
(defun helper-yield-wait (index)
  (while (/= helper-yield-turn index)
    (thread-yield))
  (setq helper-yield-turn (- 1 index))
  index)
(defvar helper-yield-index nil)
(defvar helper-yield-typep-calls 0)
(defvar helper-yield-sanitized nil)
(let ((comp-no-spawn nil) (comp-running-batch-compilation t)
      (comp-sanitizer-emit t))
  (setq helper-yield-sanitized
        (native-compile
         '(lambda (value) (if (integerp value) 'integer 'other)))))
(advice-add 'cl-typep :override
            (lambda (&rest _)
              (setq helper-yield-typep-calls (1+ helper-yield-typep-calls))
              (helper-yield-wait helper-yield-index)
              nil))
(defun helper-yield-run (index rounds)
  (let ((helper-yield-index index)
        (standard-output #'ignore)
        (inhibit-message t)
        (caught 0))
    (dotimes (_ rounds)
      (condition-case err
          (funcall helper-yield-sanitized index)
        (comp-sanitizer-error
         (setq caught (+ caught (length err))))))
    caught))
(setq comp-sanitizer-active t)
(let ((first (make-thread (lambda () (helper-yield-run 0 40))))
      (second (make-thread (lambda () (helper-yield-run 1 40)))))
  (let ((results (list (thread-join first) (thread-join second))))
    (setq comp-sanitizer-active nil)
    (advice-remove 'cl-typep (lambda (&rest _) nil))
    (prin1 (list (native-comp-function-p helper-yield-sanitized)
                 results
                 helper-yield-typep-calls))))
(terpri)
