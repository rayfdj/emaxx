;;; -*- lexical-binding: t; -*-
(require 'comp)
;; A native function's Ffuncall of a callee that parks its thread until
;; the other thread has had its turn, by each way the callee can be
;; called: interpreted, byte code, native, and one that signals after it
;; resumes.  The caller then uses its own frame again.
(defvar helper-yield-turn 0)
(defun helper-yield-wait (index)
  (while (/= helper-yield-turn index)
    (thread-yield))
  (setq helper-yield-turn (- 1 index))
  index)
(defvar helper-yield-interpreted
  (lambda (index) (1+ (helper-yield-wait index))))
(defvar helper-yield-byte-code
  (byte-compile '(lambda (index) (1+ (helper-yield-wait index)))))
(defvar helper-yield-native nil)
(defvar helper-yield-signals
  (lambda (index) (signal 'wrong-type-argument (list (1+ (helper-yield-wait index))))))
(defvar helper-yield-caller nil)
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq helper-yield-native
        (native-compile '(lambda (index) (1+ (helper-yield-wait index)))))
  (setq helper-yield-caller
        (native-compile
         '(lambda (function index rounds)
            (let ((sum 0) (own (list index rounds)))
              (dotimes (_ rounds)
                (setq sum (+ sum (condition-case err
                                     (funcall function index)
                                   (wrong-type-argument (car (cdr err)))))))
              (list sum own))))))
(defun helper-yield-run (function)
  (setq helper-yield-turn 0)
  (let ((first (make-thread (lambda () (funcall helper-yield-caller function 0 40))))
        (second (make-thread (lambda () (funcall helper-yield-caller function 1 40)))))
    (list (thread-join first) (thread-join second))))
(prin1 (list (native-comp-function-p helper-yield-caller)
             (native-comp-function-p helper-yield-native)
             (helper-yield-run helper-yield-interpreted)
             (helper-yield-run helper-yield-byte-code)
             (helper-yield-run helper-yield-signals)
             ;; A quit that inhibit-quit holds back sends every call the
             ;; full way of Ffuncall, the native callee's too.
             (progn
               (setq inhibit-quit t quit-flag t)
               (prog1 (helper-yield-run helper-yield-native)
                 (setq quit-flag nil inhibit-quit nil)))
             (helper-yield-run helper-yield-native)))
(terpri)
