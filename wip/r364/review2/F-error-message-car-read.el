;; the datum is read after its separator has been written (to a function here:
;; Fcommand_error_default_function prints onto `external-debugging-output')
(defvar acc nil)
(defvar data (list 'foo 1 2 3))
(defun external-debugging-output (c) (push c acc) (when (and (= c ?,) (eq (nth 2 data) 2)) (setcar (cddr data) 'changed)))
(add-hook 'kill-emacs-hook (lambda () (princ (concat (reverse acc)))))
(command-error-default-function data "" nil)
