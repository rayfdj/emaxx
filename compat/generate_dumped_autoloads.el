;;; generate_dumped_autoloads.el --- Generate GNU dumped autoload manifest -*- lexical-binding: t; -*-

;; Usage:
;;   emacs -Q --batch -l compat/generate_dumped_autoloads.el \
;;     --eval '(emaxx-generate-dumped-autoloads "../emacs/lisp/loaddefs.el" "src/lisp/eval/generated_autoloads.rs")'

;;; Code:

(require 'cl-lib)

(defun emaxx--dumped-literal-p (value &optional seen)
  "Return non-nil when VALUE can be reproduced by Emaxx's source reader."
  (let ((seen (or seen (make-hash-table :test #'eq))))
    (cond
     ((or (null value) (stringp value) (numberp value) (symbolp value)) t)
     ((or (byte-code-function-p value)
          (char-table-p value)
          (bool-vector-p value)
          (hash-table-p value)
          (recordp value))
      nil)
     ((consp value)
      (if (gethash value seen)
          nil
        (puthash value t seen)
        (and (emaxx--dumped-literal-p (car value) seen)
             (emaxx--dumped-literal-p (cdr value) seen))))
     ((vectorp value)
      (and (not (gethash value seen))
           (progn
             (puthash value t seen)
             (cl-every (lambda (item)
                         (emaxx--dumped-literal-p item seen))
                       value))))
     (t nil))))

(defun emaxx--rust-string-literal (value)
  "Return VALUE encoded as a single-line Rust string literal."
  (concat
   "\""
   (mapconcat
    (lambda (character)
      (pcase character
        (?\\ "\\\\")
        (?\" "\\\"")
        (?\n "\\n")
        (?\r "\\r")
        (?\t "\\t")
        (?\0 "\\0")
        ((pred (lambda (value) (or (< value 32) (= value 127))))
         (format "\\u{%x}" character))
        (_ (char-to-string character))))
    value "")
   "\""))

(defun emaxx--form-contains-call-p (form function)
  "Return non-nil when FORM contains a call to FUNCTION.
Quoted data and function bodies are not executable initializer calls."
  (and (consp form)
       (not (memq (car form) '(quote function lambda)))
       (or (eq (car form) function)
           (cl-some (lambda (item)
                      (emaxx--form-contains-call-p item function))
                    form))))

(defun emaxx-generate-dumped-autoloads (loaddefs-file output-file)
  "Generate Rust autoload metadata from GNU LOADDEFS-FILE."
  (let ((entries (make-hash-table :test #'equal))
        (functions (make-hash-table :test #'equal))
        (variables (make-hash-table :test #'equal))
        initializers)
    (with-temp-buffer
      (insert-file-contents loaddefs-file)
      (goto-char (point-min))
      (condition-case nil
          (while t
            (let ((form (read (current-buffer))))
              (cond
               ((and (consp form)
                     (eq (car form) 'autoload)
                     (eq (car-safe (cadr form)) 'quote)
                     (symbolp (cadr (cadr form)))
                     (stringp (nth 2 form)))
                (let ((name (symbol-name (cadr (cadr form))))
                      (file (nth 2 form))
                      (interactive (and (nth 4 form) t))
                      (kind (let ((raw (nth 5 form)))
                              (if (eq (car-safe raw) 'quote)
                                  (cadr raw)
                                raw))))
                  (puthash name (list file interactive kind) entries)))
               ((and (consp form)
                     (eq (car form) 'defun)
                     (symbolp (nth 1 form))
                     (listp (nth 2 form)))
                ;; Some autoload cookies deliberately emit a complete small
                ;; function into loaddefs.el instead of an `autoload' form.
                ;; These function cells are part of the dumped startup
                ;; contract just as much as ordinary autoload entries.
                (let* ((name (symbol-name (nth 1 form)))
                       (lambda-form (cons 'lambda (cddr form)))
                       (rendered (prin1-to-string lambda-form))
                       (round-trip (car (read-from-string rendered))))
                  (when (and (emaxx--dumped-literal-p lambda-form)
                             (equal lambda-form round-trip)
                             (not (string-match-p
                                   "[\0-\x08\x0b\x0c\x0e-\x1f\r]"
                                   rendered)))
                    (puthash name rendered functions))))
               ((and (consp form)
                     (memq (car form) '(defvar defconst))
                     (symbolp (nth 1 form))
                     (>= (length form) 3))
                (condition-case nil
                    (let* ((name (symbol-name (nth 1 form)))
                           (value (eval (nth 2 form) t))
                           (rendered (prin1-to-string value))
                           (round-trip (car (read-from-string rendered))))
                      (when (and (emaxx--dumped-literal-p value)
                                 (equal value round-trip)
                                 (not (string-match-p
                                       "[\0-\x08\x0b\x0c\x0e-\x1f\r]"
                                       rendered)))
                        (puthash name rendered variables)))
                  (error nil)))
               ((and (consp form)
                     (symbolp (car form))
                     (or
                      ;; Autoload cookies also emit declarative mode
                      ;; registrations.  They run after the owning dumped
                      ;; libraries have established the base alists, so keep
                      ;; the source forms in order rather than copying
                      ;; individual Python/AWK/etc. entries into Rust.
                      (eq (car form) 'add-to-list)
                      ;; Key-binding autoload cookies install the startup
                      ;; bindings as well as the function stubs.  Keeping the
                      ;; generated forms preserves the dumped global map
                      ;; without duplicating package-specific keys in Rust.
                      (eq (car form) 'global-set-key)
                      (and (eq (car form) 'dolist)
                           (emaxx--form-contains-call-p form 'add-to-list))
                      ;; A generated helper can be followed by a top-level
                      ;; call that installs additional dumped startup state.
                      (gethash (symbol-name (car form)) functions))
                     (emaxx--dumped-literal-p form))
                (let* ((rendered (prin1-to-string form))
                       (round-trip (car (read-from-string rendered))))
                  ;; Initializer source is passed through
                  ;; `emaxx--rust-string-literal', which safely escapes
                  ;; control characters used by legacy key strings.
                  (when (equal form round-trip)
                    (push rendered initializers)))))))
        (end-of-file nil)))
    (let (names)
      (maphash (lambda (name _metadata) (push name names)) entries)
      (setq names (sort names #'string<))
      (with-temp-file output-file
        (insert "// @generated by compat/generate_dumped_autoloads.el using GNU Emacs loaddefs.el.\n")
        (insert "// Regenerate when the compatibility oracle version changes.\n\n")
        (insert "const GENERATED_DUMPED_AUTOLOADS: &[(&str, &str, bool, Option<&str>)] = &[\n")
        (dolist (name names)
          (pcase-let ((`(,file ,interactive ,kind) (gethash name entries)))
            (insert (format "    (%S, %S, %s, %s),\n"
                            name file
                            (if interactive "true" "false")
                            (if kind
                                (format "Some(%S)" (symbol-name kind))
                              "None")))))
        (insert "];\n\n")
        (insert "pub(super) fn generated_dumped_autoload(\n")
        (insert "    name: &str,\n")
        (insert ") -> Option<(&'static str, bool, Option<&'static str>)> {\n")
        (insert "    let index = GENERATED_DUMPED_AUTOLOADS\n")
        (insert "        .binary_search_by_key(&name, |(candidate, _, _, _)| *candidate)\n")
        (insert "        .ok()?;\n")
        (insert "    let (_, file, interactive, kind) = GENERATED_DUMPED_AUTOLOADS[index];\n")
        (insert "    Some((file, interactive, kind))\n")
        (insert "}\n\n")
        (let (function-names)
          (maphash (lambda (name _value) (push name function-names)) functions)
          (setq function-names (sort function-names #'string<))
          (insert "const GENERATED_DUMPED_FUNCTIONS: &[(&str, &str)] = &[\n")
          (dolist (name function-names)
            (insert (format "    (%S, %s),\n"
                            name
                            (emaxx--rust-string-literal
                             (gethash name functions)))))
          (insert "];\n\n")
          (insert "pub(super) fn generated_dumped_function(name: &str) -> Option<&'static str> {\n")
          (insert "    let index = GENERATED_DUMPED_FUNCTIONS\n")
          (insert "        .binary_search_by_key(&name, |(candidate, _)| *candidate)\n")
          (insert "        .ok()?;\n")
          (insert "    Some(GENERATED_DUMPED_FUNCTIONS[index].1)\n")
          (insert "}\n\n"))
        (insert "const GENERATED_DUMPED_INITIALIZERS: &[&str] = &[\n")
        (dolist (initializer (nreverse initializers))
          (insert (format "    %s,\n"
                          (emaxx--rust-string-literal initializer))))
        (insert "];\n\n")
        (insert "pub(super) fn generated_dumped_initializers() -> &'static [&'static str] {\n")
        (insert "    GENERATED_DUMPED_INITIALIZERS\n")
        (insert "}\n\n")
        (let (variable-names)
          (maphash (lambda (name _value) (push name variable-names)) variables)
          (setq variable-names (sort variable-names #'string<))
          (insert "const GENERATED_DUMPED_VARIABLES: &[(&str, &str)] = &[\n")
          (dolist (name variable-names)
            (insert (format "    (%S, %s),\n"
                            name
                            (emaxx--rust-string-literal
                             (gethash name variables)))))
          (insert "];\n\n")
          (insert "pub(super) fn generated_dumped_variable(name: &str) -> Option<&'static str> {\n")
          (insert "    let index = GENERATED_DUMPED_VARIABLES\n")
          (insert "        .binary_search_by_key(&name, |(candidate, _)| *candidate)\n")
          (insert "        .ok()?;\n")
          (insert "    Some(GENERATED_DUMPED_VARIABLES[index].1)\n")
          (insert "}\n"))))))

(provide 'generate-dumped-autoloads)

;;; generate_dumped_autoloads.el ends here
