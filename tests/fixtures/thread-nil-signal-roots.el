;;; -*- lexical-binding: t; -*-
(defvar nil-signal-table (make-hash-table :test 'eq :weakness 'key))
(defvar nil-signal-thread (make-thread (lambda () nil)))
(thread-join nil-signal-thread)
(let ((key (list 'payload)))
  (puthash key t nil-signal-table)
  (thread-signal nil-signal-thread nil (list key)))
(garbage-collect)
(defvar nil-signal-retained (hash-table-count nil-signal-table))
(setq nil-signal-thread nil)
;; Observe release after this file returns: GNU's conservative collector can
;; retain the dead thread through values still present in active loader frames.
