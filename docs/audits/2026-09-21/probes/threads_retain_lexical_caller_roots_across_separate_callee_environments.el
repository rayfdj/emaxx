(prin1
        (progn
          (defvar suspension-roots-table nil)
          (defvar suspension-roots-stage nil)
          (defun suspension-roots-wait ()
            (setq suspension-roots-stage 'parked)
            (while (eq suspension-roots-stage 'parked) (thread-yield)))
          (defun suspension-roots-run (helper)
            (setq suspension-roots-table (make-hash-table :test 'eq :weakness 'key)
                  suspension-roots-stage nil)
            (fset 'suspension-roots-helper helper)
            (let ((worker
                   (make-thread
                    (eval '(lambda ()
                             (let ((key (list 'owned-only-by-caller)))
                               (puthash key t suspension-roots-table)
                               (suspension-roots-helper)
                               (gethash key suspension-roots-table))) t))))
              (while (not (eq suspension-roots-stage 'parked)) (thread-yield))
              (garbage-collect)
              (let ((during (hash-table-count suspension-roots-table)))
                (setq suspension-roots-stage 'resume)
                (let ((result (thread-join worker)))
                  (garbage-collect)
                  (list during result (hash-table-count suspension-roots-table))))))
          (list
           (suspension-roots-run (eval '(lambda () (suspension-roots-wait)) nil))
           (suspension-roots-run (eval '(lambda () (suspension-roots-wait)) t)))))
(terpri)
