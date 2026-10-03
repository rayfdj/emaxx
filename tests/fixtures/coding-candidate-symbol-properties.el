(progn
  (require 'cl-lib)
  (let ((good (make-char-table 'translation-table nil))
        (empty (make-char-table 'translation-table nil))
        (saved-true (symbol-plist t))
        (saved-nil (symbol-plist nil)))
    (aset good 9731 65)
    (unwind-protect
        (progn
          (put t 'translation-table good)
          (put nil 'translation-table good)
          (put 'runtime-symbol-translation 'translation-table good)
          (dolist (definition
                   '((runtime-safe-true . t)
                     (runtime-safe-true-list t)
                     (runtime-safe-nil-list nil)
                     (runtime-safe-property . runtime-symbol-translation)))
            (define-coding-system (car definition) "Translation property control."
              :coding-type 'utf-8 :mnemonic ?R :charset-list '(ascii)
              :encode-translation-table (cdr definition)))
          (let ((coding-system-list '(runtime-safe-true runtime-safe-true-list
                                      runtime-safe-nil-list runtime-safe-property))
                (standard-translation-table-for-encode nil))
            (cl-labels
                ((membership ()
                   (let ((found (find-coding-systems-region-internal (string 9731) nil)))
                     (mapcar (lambda (coding) (not (null (memq coding found))))
                             coding-system-list))))
              (list
               (membership)
               (let ((overriding-plist-environment
                      (list (list 'runtime-symbol-translation 'translation-table empty))))
                 (membership))
               (let ((overriding-plist-environment
                      (list (list nil 'translation-table empty))))
                 (membership))
               (let ((overriding-plist-environment
                      (list (list t 'translation-table empty))))
                 (membership))
               (let ((overriding-plist-environment
                      '((t translation-table nil)
                        (nil translation-table nil)
                        (runtime-symbol-translation translation-table nil))))
                 (membership))))))
      (setplist t saved-true)
      (setplist nil saved-nil))))
