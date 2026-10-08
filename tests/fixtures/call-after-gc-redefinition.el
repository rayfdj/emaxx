;; alloc.c completion and bytecode.c:Bcall resolve the live function cell only
;; after maybe_gc. Vary the replacement's callable kind and allocation size.
(progn
  (require 'bytecomp)
  (defvar gc-call-ready nil)
  (defvar gc-call-hook-count 0)
  (defvar gc-call-nested 'not-run)
  (defvar gc-call-replacement nil)
  (defalias 'gc-call-alias-target (lambda (value) (list 'alias (length value))))
  (let ((answers nil))
    (dolist (size '(100000 180001))
      (dolist (kind '(bytecode interpreted primitive alias void))
        (setq gc-call-ready nil gc-call-hook-count 0 gc-call-nested 'not-run)
        (fset 'gc-call-target
              (byte-compile '(lambda (value) (list 'old (length value)))))
        (setq gc-call-replacement
              (cond ((eq kind 'bytecode)
                     (byte-compile '(lambda (value) (list 'new (length value)))))
                    ((eq kind 'interpreted) (lambda (value) (list 'interpreted (length value))))
                    ((eq kind 'primitive) (symbol-function 'length))
                    ((eq kind 'alias) 'gc-call-alias-target)
                    (t nil)))
        (let ((caller (byte-compile
                       `(lambda ()
                          (let ((argument (make-vector ,size nil)))
                            (setq gc-call-ready t)
                            (gc-call-target argument))))))
          (garbage-collect)
          (let ((gc-cons-threshold 10000) (gc-cons-percentage 0)
                (gc-elapsed nil)
                (post-gc-hook
                 (list (lambda ()
                         (when gc-call-ready
                           (setq gc-call-hook-count (1+ gc-call-hook-count)
                                 gc-call-nested (garbage-collect))
                           (fset 'gc-call-target gc-call-replacement))))))
            (garbage-collect)
            (let* ((before gcs-done)
                   (answer (condition-case condition (funcall caller)
                             (error (car condition)))))
              (push (list size kind answer gc-call-hook-count
                          gc-call-nested (null gc-elapsed) (> gcs-done before)
                          (byte-code-function-p caller))
                    answers))))))
    (nreverse answers)))
