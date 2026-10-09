;;; Actual symbol fields must keep their children, not keep their owner alive.
;;; alloc.c:process_mark_stack follows Lisp_Symbol fields only after reaching
;;; the symbol; sweep_symbols releases unreachable symbol/field cycles.

(progn
(defun symbol-field-lifetime-populate (tables)
  (let ((live (make-vector 9 nil)) (index 0))
    (dolist (size '(1 17 129))
      (dotimes (kind 3)
        (let ((symbol (make-symbol (format "field-owner-%d-%d" size kind))))
          (cond
           ((= kind 0) (set symbol (cons size (make-string size ?v))))
           ((= kind 1) (fset symbol (list 'lambda nil (list 'quote size))))
           (t (setplist symbol (list 'payload (cons symbol (make-string size ?p))))))
          (puthash symbol size (aref tables kind))
          (aset live index symbol)
          (setq index (1+ index)))))
    live))

(let* ((tables (vector (make-hash-table :test 'eq :weakness 'key)
                       (make-hash-table :test 'eq :weakness 'key)
                       (make-hash-table :test 'eq :weakness 'key)))
       (live (symbol-field-lifetime-populate tables))
       before values)
  (garbage-collect)
  (setq before (mapcar #'hash-table-count (append tables nil)))
  (dotimes (index 9)
    (let ((symbol (aref live index)))
      (push (cond
             ((= (% index 3) 0) (car (symbol-value symbol)))
             ((= (% index 3) 1) (funcall symbol))
             (t (and (eq (car (get symbol 'payload)) symbol)
                     (length (cdr (get symbol 'payload))))))
            values)))
  (setq live nil)
  (garbage-collect)
  (garbage-collect)
  (list before (nreverse values)
        (mapcar #'hash-table-count (append tables nil)))))
