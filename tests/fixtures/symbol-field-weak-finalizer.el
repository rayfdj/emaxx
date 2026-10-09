(progn
  ;; A retained weak-table value can reach a symbol, whose fields then reach
  ;; another symbol and its payload. Revisit these edges during weak marking.
  (defun symbol-edge-fill (key bridge symbols payloads)
    (let ((owner (make-symbol "edge-owner"))
          (child (make-symbol "edge-child"))
          (data (vector 91 97)))
      (set owner child)
      (setplist child (list 'payload data))
      (puthash key owner bridge)
      (puthash child t symbols)
      (puthash data t payloads)))
  (defvar symbol-edge-finalized 0)
  (defun symbol-edge-finalizer (owner)
    (setplist owner (list 'cleanup
                         (make-finalizer
                          (lambda () (setq symbol-edge-finalized
                                           (1+ symbol-edge-finalized)))))))
  (let* ((key (make-symbol "edge-key"))
         (bridge (make-hash-table :test 'eq :weakness 'key))
         (symbols (make-hash-table :test 'eq :weakness 'key))
         (payloads (make-hash-table :test 'eq :weakness 'key))
         (owner (make-symbol "finalizer-owner"))
         (alias (make-symbol "edge-alias"))
         (target (make-symbol "edge-target"))
         (aliases (make-hash-table :test 'eq :weakness 'key))
         live result)
    (setq symbol-edge-finalized 0)
    (symbol-edge-fill key bridge symbols payloads)
    (symbol-edge-finalizer owner)
    (set target (vector 101))
    (defvaralias alias target)
    (puthash alias t aliases)
    (puthash target t aliases)
    (setq target nil)
    (garbage-collect)
    (setq live (list (hash-table-count bridge)
                     (hash-table-count symbols)
                     (hash-table-count payloads)
                     (get (symbol-value (gethash key bridge)) 'payload)
                     symbol-edge-finalized
                     (symbol-value alias)
                     (hash-table-count aliases)))
    (setq key nil owner nil alias nil)
    (garbage-collect)
    (garbage-collect)
    (setq result (list live (hash-table-count bridge)
                       (hash-table-count symbols)
                       (hash-table-count payloads)
                       symbol-edge-finalized
                       (hash-table-count aliases)))
    ;; LIVE deliberately retains the payload vector. eval.c:Fdefvaralias's
    ;; LOADHIST_ATTACH retains the alias and, through its field, the target.
    ;; The finalizer must stay dormant while OWNER lives and run after release.
    result))
