;;; -*- lexical-binding: t; -*-
(require 'comp)
(defvar thread-port-stages [nil nil])
(defvar thread-port-cleanups [0 0])
(defvar thread-port-table (make-hash-table :test 'eq :weakness 'key))
(defvar thread-port-worker nil)
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq thread-port-worker
        (native-compile
         '(lambda (index)
            (let ((key (list index)))
              (puthash key index thread-port-table)
              (condition-case nil
                  (unwind-protect
                      (progn
                        (aset thread-port-stages index 'parked)
                        (while (eq (aref thread-port-stages index) 'parked)
                          (thread-yield))
                        (if (= index 1) (error "worker failure")
                          (list index (gethash key thread-port-table 'lost))))
                    (aset thread-port-cleanups index
                          (1+ (aref thread-port-cleanups index))))
                (error (list index 'caught (gethash key thread-port-table 'lost)))))))))
(let ((first (make-thread (lambda () (funcall thread-port-worker 0))))
      (second (make-thread (lambda () (funcall thread-port-worker 1)))))
  (while (not (and (eq (aref thread-port-stages 0) 'parked)
                  (eq (aref thread-port-stages 1) 'parked)))
    (thread-yield))
  (garbage-collect)
  (let ((parked-count (hash-table-count thread-port-table)))
    ;; The first native activation returns while the second remains parked.
    (aset thread-port-stages 0 'release)
    (let ((first-result (thread-join first)))
      (garbage-collect)
      (aset thread-port-stages 1 'release)
      (prin1 (list (native-comp-function-p thread-port-worker)
                   parked-count first-result (thread-join second)
                   thread-port-cleanups)))))
(terpri)
