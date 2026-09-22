;;; -*- lexical-binding: t; -*-
        (require 'bytecomp)
        (defvar vm-suspension-table nil)
        (defvar vm-suspension-stage nil)
        (defvar vm-suspension-observed nil)
        (defvar vm-suspension-result nil)
        (defun vm-suspension-pause (&rest _)
          (setq vm-suspension-stage 'parked)
          (while (eq vm-suspension-stage 'parked) (thread-yield)))
        (defun vm-suspension-key ()
          (let ((key (list 'runtime-key)))
            (puthash key t vm-suspension-table)
            key))
        (defun vm-suspension-cleanup ()
          (let ((key (vm-suspension-key)))
            (lambda ()
              (setq vm-suspension-observed (gethash key vm-suspension-table)))))
        (defun vm-suspension-lookup (key)
          (gethash key vm-suspension-table))
        (defun vm-suspension-run (function)
          (setq vm-suspension-table (make-hash-table :test 'eq :weakness 'key)
                vm-suspension-stage nil vm-suspension-observed nil)
          (let ((worker (make-thread function)))
            (while (not (eq vm-suspension-stage 'parked)) (thread-yield))
            (garbage-collect)
            (let ((during (hash-table-count vm-suspension-table)))
              (setq vm-suspension-stage 'resume)
              (let ((result (thread-join worker)))
                (garbage-collect)
                (list during (or result vm-suspension-observed)
                      (hash-table-count vm-suspension-table))))))
        (setq vm-suspension-result
              (list
               ;; Runtime cleanup closure held in the VM's unwind stack.
               (vm-suspension-run
                (make-byte-code
                 0
                 (unibyte-string byte-constant byte-call byte-unwind-protect
                                 (+ byte-constant 1) byte-call
                                 (+ byte-unbind 1) byte-return)
                 [vm-suspension-cleanup vm-suspension-pause] 1))
               ;; A live key below Binsert's argument while its ordinary
               ;; modification hook suspends in the dedicated opcode.
               (with-temp-buffer
                 (add-hook 'before-change-functions #'vm-suspension-pause nil t)
                 (vm-suspension-run
                  (make-byte-code
                   0
                   (unibyte-string byte-constant (+ byte-constant 1) byte-call
                                   (+ byte-constant 2) byte-insert byte-discard
                                   (+ byte-call 1) byte-return)
                   [vm-suspension-lookup vm-suspension-key "payload"] 3)))))
        (prin1 vm-suspension-result)
