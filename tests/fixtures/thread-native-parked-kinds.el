;;; -*- lexical-binding: t; -*-
(require 'comp)
;; A collection while a Lisp thread is parked in each way a thread
;; parks: thread-yield, mutex-lock, condition-wait, thread-join and
;; sleep-for.  Each thread's worker holds a fresh list and a fresh
;; string in its frame alone (the weak table does not keep the list)
;; across the wait and reads them after it: they are the evaluated
;; arguments of a call of `list' whose last argument is the wait.
;;
;; The worker is compiled three ways, and each way parks in each of the
;; five.  What the collector has to read to find the two objects
;; differs:
;; - native code keeps them in its frame on the machine stack;
;; - the evaluator keeps a subr's evaluated arguments in an array on the
;;   machine stack too (eval_sub's `vals', SAFE_ALLOCA_LISP), and no
;;   native frame is beside them: only the scan of the parked thread's
;;   own stack (thread.c:mark_one_thread's mark_c_stack) reaches them;
;; - byte code keeps them on the byte-code stack, which mark_bytecode
;;   marks.
(defvar parked-stages (make-vector 15 nil))
(defvar parked-table (make-hash-table :test 'eq :weakness 'key))
(defvar parked-mutex (make-mutex))
(defvar parked-condition-mutex (make-mutex))
(defvar parked-condition (make-condition-variable parked-condition-mutex))
(defvar parked-release nil)
(defvar parked-helper-release nil)
(defvar parked-helper nil)
(defun parked-key (index)
  (let ((key (list index (1+ index))))
    (puthash key index parked-table)
    key))
(defun parked-wait (index)
  (aset parked-stages index 'parked)
  (pcase (% index 5)
    (0 (while (not parked-release)
         (thread-yield)))
    (1 (mutex-lock parked-mutex)
       (mutex-unlock parked-mutex))
    (2 (with-mutex parked-condition-mutex
         (while (not parked-release)
           (condition-wait parked-condition))))
    (3 (thread-join parked-helper))
    (4 (while (not parked-release)
         (sleep-for 0.02))))
  'waited)
(defvar parked-workers nil)
(let ((comp-no-spawn nil) (comp-running-batch-compilation t)
      (worker '(lambda (index)
                 (let ((held (list (parked-key index)
                                   (make-string (+ 20 index) (+ ?a index))
                                   (parked-wait index))))
                   (list (car (car held)) (car (cdr (car held)))
                         (car (cdr held))
                         (gethash (car held) parked-table 'lost)
                         (car (cdr (cdr held))))))))
  (setq parked-workers
        (list (native-compile worker) (eval worker t) (byte-compile worker))))
(defun parked-churn ()
  ;; Cells and string data a freed object's would be given to.
  (let ((junk nil))
    (dotimes (i 2000)
      (push (list i i i) junk)
      (push (make-string 24 ?z) junk))
    (length junk)))
(mutex-lock parked-mutex)
(setq parked-helper
      (make-thread (lambda ()
                     (while (not parked-helper-release)
                       (thread-yield)))))
;; Worker INDEX is of kind INDEX / 5 and waits in way INDEX % 5.
(let ((threads (mapcar (lambda (index)
                         (make-thread
                          (lambda ()
                            (funcall (nth (/ index 5) parked-workers) index))))
                       (number-sequence 0 14))))
  (while (not (equal parked-stages (make-vector 15 'parked)))
    (thread-yield))
  ;; Every worker is in its wait.
  (garbage-collect)
  (parked-churn)
  (garbage-collect)
  (let ((parked-count (hash-table-count parked-table)))
    (setq parked-release t)
    (mutex-unlock parked-mutex)
    (with-mutex parked-condition-mutex
      (condition-notify parked-condition t))
    (setq parked-helper-release t)
    (prin1 (list (mapcar (lambda (worker)
                           (cond ((native-comp-function-p worker) 'native)
                                 ((byte-code-function-p worker) 'byte-code)
                                 ((interpreted-function-p worker) 'interpreted)))
                         parked-workers)
                 parked-count))
    (dolist (thread threads)
      (terpri)
      (prin1 (thread-join thread)))))
(terpri)
