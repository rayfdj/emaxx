(progn
  (require 'bytecomp)
  (let* ((buffer (generate-new-buffer " *overlay-plist-object*"))
         (key-a (make-symbol "overlay-first-property"))
         (key-b (make-symbol "overlay-second-property"))
         (key-float-a (string-to-number "1.25"))
         (key-float-b (string-to-number "1.25"))
         (shared (vector 'initial))
         (read-property (byte-compile '(lambda (object key) (overlay-get object key))))
         overlay answer)
    (unwind-protect
        (with-current-buffer buffer
          (insert "alpha λ omega")
          (setq overlay (make-overlay 2 8))
          (overlay-put overlay key-a shared)
          (overlay-put overlay key-b 'second)
          (let ((properties (overlay-properties overlay)))
            (setq answer (list (eq (car properties) key-b)
                               (eq (cadr (cddr properties)) shared)))
            (setcar properties 'changed-copy-key)
            (setcar (cdr properties) 'changed-copy-value)
            (setq answer (append answer
                                 (list (eq (overlay-get overlay key-b) 'second)))))
          (overlay-put overlay key-a 'replacement)
          (setq answer (append answer
                               (list (eq (car (overlay-properties overlay)) key-b)
                                     (eq (funcall read-property overlay key-a) 'replacement))))
          (overlay-put overlay key-float-a 'left)
          (overlay-put overlay key-float-b 'right)
          (setq answer (append answer
                               (list (not (eq key-float-a key-float-b))
                                     (eq (funcall read-property overlay key-float-a) 'left)
                                     (eq (overlay-get overlay key-float-b) 'right))))
          (overlay-put overlay key-a shared)
          (aset shared 0 overlay)
          (delete-overlay overlay)
          (garbage-collect)
          (setq answer (append answer
                               (list (null (overlay-buffer overlay))
                                     (eq (aref (funcall read-property overlay key-a) 0) overlay))))
          (move-overlay overlay 1 4 buffer)
          (setq answer (append answer
                               (list (eq (funcall read-property overlay key-a) shared))))
          (let* ((copy-buffer (make-indirect-buffer buffer " *overlay-plist-copy*" t))
                 (copy-overlay (car (with-current-buffer copy-buffer (overlays-at 2)))))
            (unwind-protect
                (progn
                  (overlay-put copy-overlay key-a 'copy-value)
                  (setq answer (append answer
                                       (list (eq (overlay-get overlay key-a) shared)
                                             (eq (overlay-get copy-overlay key-a) 'copy-value)))))
              (kill-buffer copy-buffer)))
          answer)
      (when (buffer-live-p buffer) (kill-buffer buffer)))))
