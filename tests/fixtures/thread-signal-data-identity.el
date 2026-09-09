;;; -*- lexical-binding: t; -*-
(defvar signal-data-ready nil)
(defvar signal-data-result nil)
(defvar signal-data-payload (list 'before))
(defvar signal-data-worker
  (make-thread
   (lambda ()
     (condition-case caught
         (progn
           (setq signal-data-ready t)
           (while t (thread-yield)))
       (error
        (setq signal-data-result
              (list (eq (cdr caught) signal-data-payload)
                    (cadr caught))))))))
(while (not signal-data-ready) (thread-yield))
(thread-signal signal-data-worker 'error signal-data-payload)
(setcar signal-data-payload 'after)
(condition-case nil (thread-join signal-data-worker) (error nil))
(prin1 signal-data-result)
(terpri)
