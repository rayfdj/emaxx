;;; -*- lexical-binding: t; -*-
(progn
  (require 'bytecomp)
  (let* ((terminal (frame-terminal))
         (key (make-symbol "terminal-sharing-key"))
         (first-key (list 'equal-terminal-key))
         (second-key (list 'equal-terminal-key))
         (payload (vector nil terminal))
         (getter (byte-compile '(lambda () (frame-terminal))))
         (setter (byte-compile
                  '(lambda (terminal key value)
                     (set-terminal-parameter terminal key value)))))
    (aset payload 0 payload)
    (unwind-protect
        (let* ((previous (funcall setter terminal key payload))
               (first-copy (terminal-parameters terminal))
               (second-copy (terminal-parameters terminal))
               (pair (assq key first-copy))
               (separate-lists (not (eq first-copy second-copy)))
               (separate-pairs (not (eq pair (assq key second-copy))))
               (shared-value (eq (cdr pair) payload)))
          (setcdr pair 'changed-copy)
          (set-terminal-parameter terminal first-key 17)
          (set-terminal-parameter terminal second-key 29)
          (garbage-collect)
          (list
           (eq terminal (funcall getter))
           (not (null (memq terminal (terminal-list))))
           (null previous)
           separate-lists separate-pairs shared-value
           (eq (terminal-parameter terminal key) payload)
           (eq (aref (terminal-parameter terminal key) 0) payload)
           (eq (aref payload 1) terminal)
           (eq (funcall setter terminal key payload) payload)
           (= (cdr (assq first-key (terminal-parameters terminal))) 17)
           (= (cdr (assq second-key (terminal-parameters terminal))) 29)
           (condition-case error
               (progn (terminal-parameter terminal first-key) nil)
             (wrong-type-argument
              (and (eq (cadr error) 'symbolp)
                   (eq (caddr error) first-key))))))
      (set-terminal-parameter terminal key nil)
      (set-terminal-parameter terminal first-key nil)
      (set-terminal-parameter terminal second-key nil))))
