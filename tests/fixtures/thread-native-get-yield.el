;;; -*- lexical-binding: t; -*-
(require 'comp)
;; A native function's Fget while `overriding-plist-environment' is an
;; alist long enough for Fassq's maybe_quit, with a quit pending and the
;; debugger entered for it: the debugger parks the thread until the
;; other thread has had its turn and returns, the quit goes on where it
;; was noticed, and Fget reads the symbol's own plist after that.
(defvar helper-yield-turn 0)
(defun helper-yield-wait (index)
  (while (/= helper-yield-turn index)
    (thread-yield))
  (setq helper-yield-turn (- 1 index))
  index)
(defvar helper-yield-index nil)
(defvar helper-yield-debugger-entries 0)
(put 'helper-yield-symbol 'helper-yield-property 7)
(defvar helper-yield-get nil)
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq helper-yield-get
        (native-compile
         '(lambda (index rounds)
            (let ((sum 0) (helper-yield-index index))
              (dotimes (_ rounds)
                ;; The debugger is entered once for each input event;
                ;; a batch run reads none.
                (setq internal-when-entered-debugger -1)
                (setq quit-flag t)
                (setq sum (+ sum (get 'helper-yield-symbol
                                      'helper-yield-property))))
              (list sum quit-flag))))))
(setq overriding-plist-environment (list (list 'a) (list 'b) (list 'c)))
;; A thread's own handler takes every signal, and the debugger is then
;; entered only with `debug-on-signal'.
(setq debug-on-quit t debug-on-signal t)
(setq debugger (lambda (&rest _)
                 (setq helper-yield-debugger-entries
                       (1+ helper-yield-debugger-entries))
                 (helper-yield-wait helper-yield-index)
                 nil))
(let ((first (make-thread (lambda () (funcall helper-yield-get 0 40))))
      (second (make-thread (lambda () (funcall helper-yield-get 1 40)))))
  (let ((results (list (thread-join first) (thread-join second))))
    (setq debug-on-quit nil debug-on-signal nil
          overriding-plist-environment nil)
    (prin1 (list (native-comp-function-p helper-yield-get)
                 results
                 helper-yield-debugger-entries))))
(terpri)
