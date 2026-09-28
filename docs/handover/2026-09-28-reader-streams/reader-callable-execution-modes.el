(progn
  (require 'comp)
  (let* ((comp-no-spawn nil)
         (comp-running-batch-compilation t)
         (native-comp-jit-compilation nil)
         (root (make-temp-file "reader-stream-native-" t))
         (native-comp-eln-load-path (list (file-name-as-directory root)))
         (form '(lambda (&optional character)
                  (if character
                      (push character reader-stream-characters)
                    (setq reader-stream-count (1+ reader-stream-count))
                    (when (= (% reader-stream-count 11) 0) (garbage-collect))
                    (pop reader-stream-characters)))))
    (defvar reader-stream-characters)
    (defvar reader-stream-count)
    (unwind-protect
        (let ((functions (list (eval form t) (byte-compile form) (native-compile form))))
          (mapcar
           (lambda (function)
             (list (list (interpreted-function-p function)
                         (byte-code-function-p function)
                         (native-comp-function-p function))
                   (mapcar
                    (lambda (reader)
                      (let ((reader-stream-characters
                             (string-to-list "(#:first [#:second \"payload\"]) (#:next)"))
                            (reader-stream-count 0))
                        (let* ((first (funcall reader function))
                               (remaining (concat reader-stream-characters))
                               (second (funcall reader function)))
                          (list (symbol-name (bare-symbol (car first)))
                                (symbol-name (bare-symbol (aref (nth 1 first) 0)))
                                (aref (nth 1 first) 1)
                                remaining
                                (symbol-name (bare-symbol (car second)))
                                (null reader-stream-characters)))))
                    '(read read-positioning-symbols))))
           functions))
      (delete-directory root t))))
