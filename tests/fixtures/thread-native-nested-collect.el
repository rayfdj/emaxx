;;; -*- lexical-binding: t; -*-
(require 'comp)
;; Three native calls, one inside the other, with an interpreted
;; function and a byte-compiled one between them.  Each holds a fresh
;; list and a fresh string in its frame alone (the weak table does not
;; keep the list) and binds a special variable; the innermost allocates
;; and collects; each then reads its list, its string, its binding and
;; what its closure captured.
;;
;; The table's count is printed as a bound, not as a number.  The three
;; keys of the round in progress are in the table whatever else is: the
;; three frames hold them.  The keys of the rounds before are in it or
;; not as the machine stack has it: alloc.c:mark_c_stack marks every
;; object a word of the stack names, and the frames of a round that
;; returned leave their words behind until later frames write over
;; them.  GNU's own count therefore depends on what its compiler's
;; frames left on the C stack (3 in each round as it is built here), and
;; so does Emaxx's (3 as the repository builds it; a build with other
;; compiler flags counted 4 in the second and third rounds).
(defvar nested-special 'top)
(defvar nested-round 0)
(defvar nested-table (make-hash-table :test 'eq :weakness 'key))
(defvar nested-inner nil)
(defvar nested-middle nil)
(defvar nested-outer nil)
(defun nested-churn ()
  ;; Cells and string data a freed object's would be given to.
  (let ((junk nil))
    (dotimes (i 2000)
      (push (list i i i) junk)
      (push (make-string 24 ?z) junk))
    (length junk)))
(defun nested-from-eval (depth)
  (funcall nested-middle depth))
(defalias 'nested-from-byte-code
  (byte-compile '(lambda (depth) (funcall nested-inner depth))))
(let ((comp-no-spawn nil) (comp-running-batch-compilation t))
  (setq nested-inner
        (native-compile
         '(lambda (depth)
            (let ((key (list depth 'inner))
                  (text (make-string (+ 20 depth) ?i))
                  (nested-special 'inner))
              (puthash key depth nested-table)
              (garbage-collect)
              (nested-churn)
              (garbage-collect)
              (list (car key) text nested-special
                    (gethash key nested-table 'lost)
                    (<= 3 (hash-table-count nested-table)
                        (* 3 nested-round)))))))
  (setq nested-middle
        (native-compile
         '(lambda (depth)
            (let ((key (list depth 'middle))
                  (text (make-string (+ 20 depth) ?m))
                  (nested-special 'middle))
              (puthash key depth nested-table)
              (let ((below (nested-from-byte-code (1+ depth))))
                (list (car key) text nested-special
                      (gethash key nested-table 'lost) below))))))
  (setq nested-outer
        (native-compile
         '(lambda (depth)
            (let* ((key (list depth 'outer))
                   (text (make-string (+ 20 depth) ?o))
                   (nested-special 'outer)
                   (reader (lambda () (list (car key) text))))
              (puthash key depth nested-table)
              (let ((below (nested-from-eval (1+ depth))))
                (list (funcall reader) nested-special
                      (gethash key nested-table 'lost) below)))))))
(let ((results nil))
  (dotimes (_ 3)
    (setq nested-round (1+ nested-round))
    (push (funcall nested-outer 1) results))
  (prin1 (list (native-comp-function-p nested-outer)
               (native-comp-function-p nested-middle)
               (native-comp-function-p nested-inner)
               nested-special
               results)))
(terpri)
