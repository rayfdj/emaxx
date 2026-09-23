;;; file-name-handler-lookup.el --- Lookup order controls -*- lexical-binding: t; -*-
(defun handler-control (function)
  (condition-case condition (funcall function) (error condition)))
(list
  (handler-control
   (lambda ()
     (let ((file-name-handler-alist '(("needle" . audit-handler-a) . tail)))
       (find-file-name-handler "/dir/needle" 'copy-file))))
  (handler-control
   (lambda ()
     (let ((file-name-handler-alist '(("absent" . audit-handler-a)))
           (inhibit-file-name-operation 'copy-file)
           (inhibit-file-name-handlers 'tail))
       (find-file-name-handler "/dir/needle" 'copy-file))))
  (handler-control
   (lambda ()
     (put 'audit-handler-b 'operations 'tail)
     (let ((file-name-handler-alist '(("absent" . audit-handler-b))))
       (find-file-name-handler "/dir/needle" 'copy-file))))
  (handler-control
   (lambda ()
     (put 'audit-handler-c 'operations '(other-operation))
     (let ((file-name-handler-alist '(("[" . audit-handler-c))))
       (find-file-name-handler "/dir/needle" 'copy-file))))
  (handler-control
   (lambda ()
     (let ((file-name-handler-alist '(("needle" . audit-handler-a)
                                     ("needle" . audit-handler-b))))
       (find-file-name-handler "/dir/needle" 'copy-file))))
  (handler-control
   (lambda ()
     (let* ((handler (list 'lambda nil nil))
            (file-name-handler-alist (list (cons "needle" handler)))
            (inhibit-file-name-operation 'copy-file)
            (inhibit-file-name-handlers (list (copy-tree handler))))
       (eq (find-file-name-handler "/dir/needle" 'copy-file) handler))))
  (handler-control
   (lambda ()
     (let ((operations (list 'copy-file)))
       (setcdr operations operations)
       (put 'audit-handler-d 'operations operations))
     (let ((file-name-handler-alist '(("needle" . audit-handler-d))))
       (find-file-name-handler "/dir/needle" 'copy-file))))
  (handler-control
   (lambda ()
     (let* ((file-name-handler-alist '(("needle" . audit-handler-a)))
            (inhibit-file-name-operation 'copy-file)
            (inhibit-file-name-handlers (list 'audit-handler-a)))
       (setcdr inhibit-file-name-handlers inhibit-file-name-handlers)
       (find-file-name-handler "/dir/needle" 'copy-file))))
  (handler-control
   (lambda ()
     (let ((file-name-handler-alist '(("needle" . audit-handler-a))))
       (find-file-name-handler "/dir/needle" 42))))
  (handler-control
   (lambda ()
     (let ((file-name-handler-alist '(("needle" . nil)
                                     ("needle" . audit-handler-a))))
       (list (find-file-name-handler "/dir/needle" 'file-exists-p)
             (file-exists-p "/dir/needle"))))))
