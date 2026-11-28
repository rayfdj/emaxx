;;; -*- lexical-binding: t; -*-
(require 'comp)
;; A native function's unbinding of two specpdl entries at once
;; (helper_unbind_n), the inner one's undo running Lisp that parks the
;; thread until the other thread has had its turn: an unwind-protect's
;; cleanup, then a watched variable's unbinding.  The outer entry, a
;; plain binding, is undone after the thread resumes.  A variable's
;; watchers are off while one of them runs, so the watched variable is
;; bound in one thread only, and the other just takes its turns.
(defvar helper-yield-turn 0)
(defun helper-yield-wait (index)
  (while (/= helper-yield-turn index)
    (thread-yield))
  (setq helper-yield-turn (- 1 index))
  index)
(defvar helper-yield-outer 'unbound)
(defvar helper-yield-watched 'unbound)
(defvar helper-yield-index nil)
(defvar helper-yield-watcher-calls 0)
(add-variable-watcher
 'helper-yield-watched
 (lambda (_symbol _value operation _where)
   (when (eq operation 'unlet)
     (setq helper-yield-watcher-calls (1+ helper-yield-watcher-calls))
     (helper-yield-wait helper-yield-index))))
(defvar helper-yield-cleanup nil)
(defvar helper-yield-unlet nil)
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq helper-yield-cleanup
        (native-compile
         '(lambda (index rounds)
            (let ((sum 0))
              (dotimes (_ rounds)
                (let ((helper-yield-outer (1+ index)))
                  (unwind-protect
                      (setq sum (+ sum helper-yield-outer))
                    (helper-yield-wait index))))
              (list sum helper-yield-outer)))))
  (setq helper-yield-unlet
        (native-compile
         '(lambda (index rounds)
            (let ((sum 0) (helper-yield-index index))
              (dotimes (_ rounds)
                (let ((helper-yield-outer (1+ index))
                      (helper-yield-watched index))
                  (setq sum (+ sum helper-yield-outer helper-yield-watched))))
              (list sum helper-yield-outer helper-yield-watched))))))
(defun helper-yield-run (function other)
  (setq helper-yield-turn 0)
  (let ((first (make-thread (lambda () (funcall function 0 40))))
        (second (make-thread (lambda () (funcall other 1 40)))))
    (list (thread-join first) (thread-join second))))
(defun helper-yield-turns (index rounds)
  (dotimes (_ rounds)
    (helper-yield-wait index))
  rounds)
(prin1 (list (native-comp-function-p helper-yield-cleanup)
             (native-comp-function-p helper-yield-unlet)
             (helper-yield-run helper-yield-cleanup helper-yield-cleanup)
             (helper-yield-run helper-yield-unlet #'helper-yield-turns)
             helper-yield-watcher-calls
             helper-yield-outer
             helper-yield-watched))
(terpri)
