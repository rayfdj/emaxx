;;; -*- lexical-binding: t; -*-
(defvar signal-wait-mutex (make-mutex))
(defvar signal-wait-condition (make-condition-variable signal-wait-mutex))
(defvar signal-wait-count 0)
(defvar signal-woken-count 0)
(defun signal-wait-body ()
  (with-mutex signal-wait-mutex
    (setq signal-wait-count (1+ signal-wait-count))
    (condition-wait signal-wait-condition)
    (setq signal-woken-count (1+ signal-woken-count))))
(let ((first (make-thread #'signal-wait-body))
      (second (make-thread #'signal-wait-body)))
  (with-timeout (5 (error "Workers did not reach condition-wait"))
    (while (< signal-wait-count 2) (sleep-for 0.01)))
  ;; Fthread_signal broadcasts wait_condvar even when ERROR-SYMBOL is nil.
  ;; Both waiters on that same condition variable are awakened.
  (thread-signal first nil nil)
  (with-timeout (5 (error "Nil signal did not wake condition waiters"))
    (while (< signal-woken-count 2) (sleep-for 0.01)))
  (thread-join first)
  (thread-join second)
  (prin1 signal-woken-count))
(terpri)
