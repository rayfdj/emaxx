;;; -*- lexical-binding: t; -*-
(defvar signal-boundary-mutex (make-mutex))
(defvar signal-boundary-result nil)
(mutex-lock signal-boundary-mutex)
(let ((target (make-thread (lambda ()
                            (with-mutex signal-boundary-mutex nil)))))
  (with-timeout (5 (error "Target did not block on mutex"))
    (while (not (eq (thread--blocker target) signal-boundary-mutex))
      (sleep-for 0.01)))
  (let ((caller (make-thread
                 (lambda ()
                   (condition-case nil
                       (progn
                         (thread-signal target nil nil)
                         (setq signal-boundary-result 'returned))
                     (error (setq signal-boundary-result 'caught)))))))
    ;; GNU defers this signal at the new thread's first lock acquisition;
    ;; signaling a blocked target then executes its caller-side callback.
    (thread-signal caller 'error '("caller pending"))
    (condition-case nil (thread-join caller) (error nil)))
  (mutex-unlock signal-boundary-mutex)
  (thread-join target))
(prin1 signal-boundary-result)
(terpri)
