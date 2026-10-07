(progn
  (require 'timer)
  (defvar snapshot-timer-events nil)
  (let ((timer-list nil) (timer-idle-list nil)
        (snapshot-timer-events nil))
    (dotimes (index 80)
      (run-at-time '(0 1 0 0) nil
                   (lambda (payload) (push payload snapshot-timer-events))
                   (make-vector (+ index 3) index)))
    (run-at-time '(0 0 0 0) nil
                 (lambda ()
                   (setq timer-list nil)
                   (garbage-collect)
                   (push 'first snapshot-timer-events)))
    (input-pending-p t)
    (list snapshot-timer-events timer-list)))
