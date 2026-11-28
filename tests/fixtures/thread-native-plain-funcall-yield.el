;;; -*- lexical-binding: t; -*-
(require 'comp)
;; Ffuncall from native code three ways on stacks that then switch
;; threads, a native call nested in the thread's own between them.
;; - Native `plain-outer' funcalls the subr `mapcar', which calls the
;;   interpreted `plain-between' for each element; that calls native
;;   `plain-inner' (nested, from eval), which funcalls native
;;   `plain-leaf'; that yields.
;; - Native `plain-outer-bc' funcalls the byte-compiled
;;   `plain-between-bc'; that calls `plain-inner' (nested, from byte
;;   code), and so on to the yield.
;; Another thread runs, and each frame then goes on: the leaf adds one,
;; the funcalls pop their backtrace records, the nested calls unlink
;; their frames.  Each thread counts its backtrace's frames before the
;; call and after it.
(defvar plain-leaf nil)
(defvar plain-inner nil)
(defvar plain-outer nil)
(defvar plain-outer-bc nil)
(defvar plain-other 0)
(defun plain-between (x) (funcall plain-inner x))
(defalias 'plain-between-bc
  (byte-compile '(lambda (x) (funcall plain-inner x))))
(defun plain-frames ()
  (let ((count 0))
    (mapbacktrace (lambda (&rest _) (setq count (1+ count))))
    count))
(defun plain-run (call)
  (let* ((before (plain-frames))
         (value (funcall call))
         (after (plain-frames)))
    (list value (= before after))))
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq plain-leaf (native-compile '(lambda (x) (thread-yield) (1+ x))))
  (setq plain-inner (native-compile '(lambda (x) (funcall plain-leaf x))))
  (setq plain-outer
        (native-compile
         '(lambda (f x) (funcall f #'plain-between (list x (1+ x))))))
  (setq plain-outer-bc
        (native-compile
         '(lambda (f x) (list (funcall f x) (funcall f (1+ x)))))))
(let ((threads
       (list (make-thread
              (lambda ()
                (plain-run (lambda () (funcall plain-outer #'mapcar 1)))))
             (make-thread
              (lambda ()
                (plain-run
                 (lambda () (funcall plain-outer-bc #'plain-between-bc 10)))))
             (make-thread (lambda ()
                            (dotimes (_ 4)
                              (setq plain-other (1+ plain-other))
                              (thread-yield))
                            plain-other)))))
  (prin1 (list (native-comp-function-p plain-outer)
               (native-comp-function-p plain-outer-bc)
               (native-comp-function-p plain-inner)
               (native-comp-function-p plain-leaf)
               (interpreted-function-p (symbol-function 'plain-between))
               (byte-code-function-p (symbol-function 'plain-between-bc))
               (mapcar #'thread-join threads)))
  (terpri))
