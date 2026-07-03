;;; simple_compat.el --- Batch eval/display compatibility helpers -*- lexical-binding: t; -*-

;;; Commentary:

;; This file provides a narrow slice of the preloaded simple.el,
;; subr.el, and elisp-mode.el helpers that upstream batch Emacs has
;; available before the target test file loads.

;;; Code:

(defvar values nil
  "List of values returned by expressions evaluated with `eval-expression'.")

(defun values--store-value (value)
  "Store VALUE at the front of the `values' list, then return VALUE."
  (setq values (cons value values))
  value)

(defvar eval-expression-print-level 4
  "Value for `print-level' while `eval-expression' prints its value.")
(defvar eval-expression-print-length 12
  "Value for `print-length' while `eval-expression' prints its value.")
(defvar eval-expression-print-maximum-character 127
  "Largest integer rendered as a character by `eval-expression-print-format'.")

(defun event-modifiers (event)
  "Return the list of modifier symbols carried by EVENT."
  (unless (stringp event)
    (let ((type event))
      (when (listp type)
        (setq type (car type)))
      (cond
       ((symbolp type) nil)
       ((integerp type)
        (let ((base (logand type (lognot (logior (ash 1 27) (ash 1 26)
                                                 (ash 1 25) (ash 1 24)
                                                 (ash 1 23) (ash 1 22)))))
              (mods nil))
          (unless (zerop (logand type (ash 1 27)))
            (push 'meta mods))
          (when (or (not (zerop (logand type (ash 1 26))))
                    (< base 32))
            (push 'control mods))
          (when (or (not (zerop (logand type (ash 1 25))))
                    (/= base (downcase base)))
            (push 'shift mods))
          (unless (zerop (logand type (ash 1 24)))
            (push 'hyper mods))
          (unless (zerop (logand type (ash 1 23)))
            (push 'super mods))
          (unless (zerop (logand type (ash 1 22)))
            (push 'alt mods))
          (nreverse mods)))))))

(defun event-basic-type (event)
  "Return EVENT's basic type with all modifier bits removed."
  (unless (stringp event)
    (when (consp event)
      (setq event (car event)))
    (cond
     ((symbolp event) (car (get event 'event-symbol-elements)))
     ((integerp event)
      (let* ((base (logand event (1- (ash 1 22))))
             (uncontrolled (if (< base 32) (logior base 64) base)))
        (condition-case nil
            (downcase uncontrolled)
          (error uncontrolled)))))))

(defun prin1-char (char)
  "Return a string printing CHAR as a character literal, or nil."
  (and (integerp char) (eventp char)
       (let ((base (event-basic-type char))
             (mods (event-modifiers char))
             (string nil))
         (when (and (memq 'shift mods)
                    (zerop (logand char (ash 1 25)))
                    (let ((case-fold-search nil))
                      (not (char-equal base (upcase base)))))
           (setq base (upcase base))
           (setq mods nil))
         (condition-case nil
             (setq string
                   (concat
                    "?"
                    (mapconcat (lambda (modifier)
                                 (if (eq modifier 'super)
                                     "\\s-"
                                   (string ?\\
                                           (upcase (aref (symbol-name modifier) 0))
                                           ?-)))
                               mods
                               "")
                    (cond
                     ((memq base '(?\; ?\( ?\) ?\{ ?\} ?\[ ?\] ?\" ?\' ?\\))
                      (string ?\\ base))
                     ((eq base 127) "\\C-?")
                     (t (string base)))))
           (error nil))
         (and string
              (equal (car (read-from-string string)) char)
              string))))

(defun eval-expression-print-format (value)
  "Return an octal/hex/character rendering for integer VALUE, else nil."
  (when (integerp value)
    (let ((as-char (and (characterp value)
                        (<= value eval-expression-print-maximum-character)
                        (char-displayable-p value)
                        (prin1-char value))))
      (if as-char
          (format " (#o%o, #x%x, %s)" value value as-char)
        (format " (#o%o, #x%x)" value value)))))

(defun eval-expression (exp &optional insert-value no-truncate char-print-limit)
  "Evaluate EXP; print its value like the interactive command does.
INSERT-VALUE prints into the current buffer instead of the echo
area.  NO-TRUNCATE disables `eval-expression-print-length' and
`eval-expression-print-level' truncation.  CHAR-PRINT-LIMIT adds
the extra integer formats for values it covers."
  (let ((result (values--store-value (eval exp t))))
    (let ((print-length (unless no-truncate eval-expression-print-length))
          (print-level (unless no-truncate eval-expression-print-level))
          (eval-expression-print-maximum-character char-print-limit)
          (deactivate-mark))
      (ignore deactivate-mark)
      (let ((out (if insert-value (current-buffer) t)))
        (prog1
            (prin1 result out)
          (let ((extra (and char-print-limit
                            (eval-expression-print-format result))))
            (when extra
              (princ extra out))))))))

(defvar read-expression-map (make-sparse-keymap)
  "Minibuffer keymap used for reading Lisp expressions.")

(defvar read-expression-history nil
  "History list for expressions read by `read--expression'.")

(defun read--expression (prompt &optional initial-contents)
  "Read an Emacs Lisp expression from the minibuffer.
PROMPT and INITIAL-CONTENTS are as in `read-from-minibuffer'."
  (read-from-minibuffer prompt initial-contents read-expression-map
                        t 'read-expression-history))

(defun subr-primitive-p (object)
  "Return non-nil if OBJECT is a built-in primitive function."
  (subrp object))

(defun cl-generic-p (f)
  "Return non-nil if F names a generic function."
  (and (symbolp f)
       (fboundp f)
       (or (get f 'emaxx-cl-defgeneric-lambda-list)
           (get f 'emaxx-cl-defmethod-specializers))
       t))

(defun one-window-p (&optional _nomini _all-frames)
  "Return non-nil when the selected frame shows exactly one window.
The batch frame always shows a single window."
  t)

(defvar with-timeout-timers nil
  "List of timers armed by `with-timeout' forms currently in flight.")

(defun with-timeout-suspend ()
  "Cancel the pending `with-timeout' timers, returning a resume token."
  (let ((timers with-timeout-timers))
    (dolist (timer timers)
      (when (fboundp 'cancel-timer)
        (cancel-timer timer)))
    timers))

(defun with-timeout-unsuspend (timer-spec-list)
  "Re-arm the `with-timeout' timers in TIMER-SPEC-LIST."
  (dolist (timer timer-spec-list)
    (when (fboundp 'timer-activate)
      (timer-activate timer t))))

(defun syntax-ppss-toplevel-pos (ppss)
  "Return the start of the outermost construct recorded in PPSS, or nil."
  (or (car (nth 9 ppss))
      (nth 8 ppss)))

(defun eval-sexp-add-defvars (exp &optional pos)
  "Wrap EXP so preceding non-special defvar names become special.
POS bounds the scan and defaults to point."
  (if (not lexical-binding)
      exp
    (save-excursion
      (unless pos
        (setq pos (point)))
      (goto-char (point-min))
      (let ((vars nil))
        (while (re-search-forward
                "(def\\(?:var\\|const\\|custom\\)[ \t\n]+\\([^; '()\n\t]+\\)"
                pos t)
          (let ((var (intern (match-string 1))))
            (unless (or (special-variable-p var)
                        (syntax-ppss-toplevel-pos
                         (save-excursion
                           (syntax-ppss (match-beginning 0)))))
              (push var vars))))
        `(progn ,@(mapcar (lambda (v) `(defvar ,v)) vars) ,exp)))))


(defun cl--generic-search-method (met-name)
  "For `find-function-regexp-alist'.  Search for a `cl-defmethod'.
MET-NAME is as recorded in `load-history' for the method."
  (let ((base-re (concat "(\\(?:cl-\\)?defmethod[ \t]+"
                         (regexp-quote (format "%s" (car met-name)))
                         "\\_>")))
    (or
     (re-search-forward
      (concat base-re "[^&\"\n]*"
              (mapconcat (lambda (qualifier)
                           (regexp-quote (format "%S" qualifier)))
                         (cadr met-name)
                         "[ \t\n]*")
              (mapconcat (lambda (specializer)
                           (regexp-quote
                            (format "%S" (if (consp specializer)
                                             (nth 1 specializer) specializer))))
                         (remq t (cddr met-name))
                         "[ \t\n]*)[^&\"\n]*"))
      nil t)
     (re-search-forward base-re nil t))))

(with-eval-after-load 'find-func
  (defvar find-function-regexp-alist)
  (add-to-list 'find-function-regexp-alist
               (cons 'cl-defmethod #'cl--generic-search-method)))

(defun cl-generic--method-qualifier-p (x)
  "Return non-nil if X is a method qualifier rather than an arglist."
  (not (listp x)))

(defvar cl--generic-edebug-name nil)

(defun cl--generic-edebug-remember-name (name pf &rest specs)
  ;; Remember the name in `cl-defgeneric' so we can use it when building
  ;; the names of its `:methods'.
  (let ((cl--generic-edebug-name (car name)))
    (funcall pf specs)))

(defun cl--generic-edebug-make-name (in:method _oldname &rest quals-and-args)
  ;; The name to use in Edebug for a method: use the generic
  ;; function's name plus all its qualifiers and finish with
  ;; its specializers.
  (pcase-let*
      ((basename (if in:method cl--generic-edebug-name (pop quals-and-args)))
       (args (car (last quals-and-args)))
       (`(,spec-args . ,_) (cl--generic-split-args args))
       (specializers (mapcar (lambda (spec-arg)
                               (if (eq '&context (car-safe (car spec-arg)))
                                   spec-arg (cdr spec-arg)))
                             spec-args)))
    (format "%s %s"
            (mapconcat (lambda (sexp) (format "%s" sexp))
                       (cons basename (butlast quals-and-args))
                       " ")
            specializers)))

(defun cl--generic-split-args (args)
  "Return (SPEC-ARGS . PLAIN-ARGS)."
  (let ((plain-args ())
        (specializers nil)
        (mandatory t))
    (dolist (arg args)
      (push (pcase arg
              ((or '&optional '&rest '&key) (setq mandatory nil) arg)
              ('&context
               (unless mandatory
                 (error "&context not immediately after mandatory args"))
               (setq mandatory 'context) nil)
              ((let 'nil mandatory) arg)
              ((let 'context mandatory)
               (unless (consp arg)
                 (error "Invalid &context arg: %S" arg))
               (let* ((name (car arg))
                      (rewriter
                       (and (symbolp name)
                            (get name 'cl-generic--context-rewriter))))
                 (if rewriter (setq arg (apply rewriter (cdr arg)))))
               (push `((&context . ,(car arg)) . ,(cadr arg)) specializers)
               nil)
              (`(,name . ,type)
               (push (cons name (car type)) specializers)
               name)
              (_
               (push (cons arg t) specializers)
               arg))
            plain-args))
    (cons (nreverse specializers)
          (nreverse (delq nil plain-args)))))

;; Edebug element specs and the macrolet interposer that GNU registers in
;; cl-macs.el, needed to instrument `cl-macrolet' forms (Bug#29919).
(def-edebug-elem-spec 'cl-declarations
  '(&rest ("cl-declare" &rest sexp)))

(def-edebug-elem-spec 'cl-declarations-or-string
  '(lambda-doc &or ("declare" def-declarations) cl-declarations))

(def-edebug-elem-spec 'cl-macro-list
  '(([&optional "&whole" arg] ; Only for compiler-macros or at lower levels.
     [&optional "&environment" arg]     ; Only at top-level.
     [&rest cl-macro-arg]
     [&optional ["&optional" &rest
		 &or (cl-macro-arg &optional def-form cl-macro-arg) arg]]
     [&optional [[&or "&rest" "&body"] cl-macro-arg]]
     [&optional ["&key" [&rest
			 [&or ([&or (symbolp cl-macro-arg) arg]
			       &optional def-form cl-macro-arg)
			      arg]]
		 &optional "&allow-other-keys"]]
     [&optional ["&aux" &rest
		 &or (cl-macro-arg &optional def-form) arg]]
     [&optional "&environment" arg]     ; Only at top-level.
     . [&or arg nil]                    ; Only allowed at lower levels.
     )))

(def-edebug-elem-spec 'cl-macro-arg
  '(&or arg cl-macro-list))

(defun cl--edebug-macrolet-interposer (bindings pf &rest specs)
  ;; (cl-assert (null (cdr bindings)))
  (setq bindings (car bindings))
  (let ((edebug-lexical-macro-ctx
         (nconc (mapcar (lambda (binding)
                          (cons (car binding)
                                (when (eq 'declare (car-safe (nth 2 binding)))
                                  (nth 1 (assq 'debug (cdr (nth 2 binding)))))))
                        bindings)
                edebug-lexical-macro-ctx)))
    (funcall pf specs)))

;; Obsolete EIEIO `defmethod'/`defgeneric' support: the macros come from
;; lisp/obsolete/eieio-compat.el, but its runtime helpers are written
;; against GNU cl-generic's internal method table.  Redefine them here in
;; terms of `cl-defmethod' so the old API lowers onto the native dispatch
;; machinery.
(with-eval-after-load 'eieio-compat
  ;; Old EIEIO's `constructor' generic is make-instance under a
  ;; different name; methods defined on it must land on make-instance.
  (unless (fboundp 'constructor)
    (defalias 'constructor 'make-instance))
  (defun eieio-compat--rename-cnm (form)
    "Rename old-style `call-next-method'/`next-method-p' in FORM."
    (cond
     ((eq form 'call-next-method) 'cl-call-next-method)
     ((eq form 'next-method-p) 'cl-next-method-p)
     ((consp form)
      (cons (eieio-compat--rename-cnm (car form))
            (eieio-compat--rename-cnm (cdr form))))
     (t form)))

  (defun eieio--defgeneric-init-form (method doc-string)
    (if doc-string (put method 'function-documentation doc-string))
    (if (fboundp method)
        (indirect-function method)
      (symbol-function 'ignore)))

  (defun eieio--defmethod (method kind argclass code)
    (setq kind (intern (downcase (symbol-name kind))))
    (when (eq kind :primary) (setq kind nil))
    (let* ((static (eq kind :static))
           (kind (if static nil kind))
           (args (aref code 0))
           (body (aref code 1))
           (arg1 (or (car args) '_eieio-arg))
           (rest (cdr args))
           (spec (or argclass t))
           (body (if (memq kind '(:before :after))
                     body
                   (eieio-compat--rename-cnm body))))
      ;; Old EIEIO did not require a primary method for :before/:after
      ;; methods to run; give the dispatch chain a pass-through primary.
      (when (and (memq kind '(:before :after))
                 (or (not (fboundp method))
                     (eq (symbol-function method) #'ignore)))
        (eval `(cl-defmethod ,method ((,arg1 ,spec) ,@rest)
                 (when (cl-next-method-p) (cl-call-next-method)))
              t))
      (if static
          (progn
            (eval `(cl-defmethod ,method ((,arg1 (subclass ,spec)) ,@rest) ,@body) t)
            (eval `(cl-defmethod ,method ((,arg1 ,spec) ,@rest) ,@body) t))
        (eval `(cl-defmethod ,method ,@(if kind (list kind))
                 ((,arg1 ,spec) ,@rest) ,@body)
              t))
      method)))

;;; simple_compat.el ends here
