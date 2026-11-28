;;; -*- lexical-binding: t; -*-
(require 'comp)
;; A regexp match of a string from native code, with a quit pending when
;; the matcher first asks: the debugger is entered for it, frees the
;; strings allocated just before the subject, collects (which moves the
;; subject's data down over theirs) and allocates strings again (which
;; land where the subject's data was), then returns; the quit goes on
;; where it was noticed and the match reads the rest of the subject.
(defvar repro-garbage nil)
(defvar repro-subject nil)
(defvar repro-debugger-entries 0)
(defun repro-debugger (&rest _)
  (setq repro-debugger-entries (1+ repro-debugger-entries))
  (setq repro-garbage nil)
  (garbage-collect)
  (dotimes (_ 40)
    (push (make-string 61 ?x) repro-garbage))
  nil)
(defvar repro-match nil)
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq repro-match
        (native-compile '(lambda (regexp string)
                           (setq quit-flag t)
                           (string-match regexp string)))))
(setq debugger #'repro-debugger debug-on-quit t)
(let ((results nil))
  (dotimes (_ 8)
    (setq repro-garbage nil)
    (garbage-collect)
    (dotimes (_ 3)
      (push (make-string 61 ?g) repro-garbage))
    (setq repro-subject (make-string 61 ?a))
    (aset repro-subject 60 ?b)
    (setq internal-when-entered-debugger -1)
    (push (list (funcall repro-match "a*b" repro-subject) (match-end 0))
          results))
  (setq debug-on-quit nil)
  (prin1 (list (reverse results) repro-debugger-entries)))
(terpri)
