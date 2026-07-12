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

;; GNU emacs-lisp/timer.el (verbatim): run BODY with a timeout.
(defmacro with-timeout (list &rest body)
  "Run BODY, but if it doesn't finish in SECONDS seconds, give up.
If we give up, we run the TIMEOUT-FORMS and return the value of the last one.
The timeout is checked whenever Emacs waits for some kind of external
event (such as keyboard input, input from subprocesses, or a certain time);
if the program loops without waiting in any way, the timeout will not
be detected.
\n(fn (SECONDS TIMEOUT-FORMS...) BODY)"
  (declare (indent 1) (debug ((form body) body)))
  (let ((seconds (car list))
	(timeout-forms (cdr list))
        (timeout (make-symbol "timeout")))
    `(let ((-with-timeout-value-
            (catch ',timeout
              (let* ((-with-timeout-timer-
                      (run-with-timer ,seconds nil
                                      (lambda () (throw ',timeout ',timeout))))
                     (with-timeout-timers
                         (cons -with-timeout-timer- with-timeout-timers)))
                (unwind-protect
                    (progn ,@body)
                  (cancel-timer -with-timeout-timer-))))))
       (if (eq -with-timeout-value- ',timeout)
           (progn ,@timeout-forms)
         -with-timeout-value-))))

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

  (defun generic-p (fname)
    "Return non-nil if FNAME is a cl-generic function."
    (and (symbolp fname)
         (or (get fname 'eieio--generic)
             (get fname 'emaxx-cl-defmethod-specializers)
             (get fname 'emaxx-cl-defgeneric-lambda-list))
         t))

  (defun eieio--defgeneric-init-form (method doc-string)
    (if doc-string (put method 'function-documentation doc-string))
    (if (memq method '(no-next-method no-applicable-method))
        ;; GNU eieio-compat leaves these two alone: their cl-generic
        ;; counterparts have different calling conventions.
        (and (fboundp method) (indirect-function method))
      ;; GNU's `cl-generic-ensure-function' refuses to convert an existing
      ;; non-generic function into a generic; it follows defalias chains
      ;; first (old EIEIO's `constructor' aliases `make-instance').
      (let ((target method) (guard 0))
        (while (and (fboundp target)
                    (symbolp (symbol-function target))
                    (not (generic-p target))
                    (< guard 100))
          (setq target (symbol-function target)
                guard (1+ guard)))
        (when (and (fboundp target)
                   (not (generic-p target))
                   (not (eq (indirect-function target) (symbol-function 'ignore))))
          (error "Function %S is already defined as something else than a generic function"
                 method)))
      (put method 'eieio--generic t)
      (if (fboundp method)
          (indirect-function method)
        (symbol-function 'ignore))))

  ;; GNU cl-generic dispatches exhausted calls through these generics;
  ;; their default methods signal like cl-generic.el's cl-defgenerics.
  (cl-defgeneric cl-no-next-method (generic method &rest args))
  (cl-defmethod cl-no-next-method (generic _method &rest args)
    (error "cl-no-next-method: %S%S" generic args))
  (cl-defgeneric cl-no-applicable-method (generic &rest args))
  (cl-defmethod cl-no-applicable-method (generic &rest args)
    (error "No applicable method: %S, %S" generic args))

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
      ;; Old EIEIO's `no-next-method' and `no-applicable-method' have
      ;; different calling conventions than their cl-generic namesakes;
      ;; register them as methods on the cl-generic hooks with the same
      ;; argument shuffling GNU eieio-compat.el performs.
      (cond
       ((eq method 'no-next-method)
        (eval `(cl-defmethod cl-no-next-method (_generic _method (,arg1 ,spec)
                                                         &rest eieio-compat--rest)
                 (apply (lambda ,args ,@body) ,arg1 eieio-compat--rest))
              t))
       ((eq method 'no-applicable-method)
        (eval `(cl-defmethod cl-no-applicable-method (generic (,arg1 ,spec)
                                                              &rest eieio-compat--rest)
                 (apply (lambda ,args ,@body)
                        ,arg1 generic (cons ,arg1 eieio-compat--rest)))
              t))
       (t
        ;; Old EIEIO did not require a primary method for :before/:after
        ;; methods to run; give the dispatch chain a pass-through primary.
        (when (and (memq kind '(:before :after))
                   (or (not (fboundp method))
                       (eq (indirect-function method)
                           (symbol-function 'ignore))))
          (eval `(cl-defmethod ,method ((,arg1 ,spec) ,@rest)
                   (when (cl-next-method-p) (cl-call-next-method)))
                t))
        (if static
            (progn
              (eval `(cl-defmethod ,method ((,arg1 (subclass ,spec)) ,@rest) ,@body) t)
              (eval `(cl-defmethod ,method ((,arg1 ,spec) ,@rest) ,@body) t))
          (eval `(cl-defmethod ,method ,@(if kind (list kind))
                   ((,arg1 ,spec) ,@rest) ,@body)
                t))))
      method)))

;; GNU cl-loop, ported verbatim from lisp/emacs-lisp/cl-macs.el (with its
;; helpers from macroexp.el and cl-extra.el) so `cl-loop' has upstream
;; clause semantics; the native special form defers to this macro.

(defvar cl--gensym-counter 0)
(defun cl-gensym (&optional prefix)
  "Generate a new uninterned symbol.
The name is made by appending a number to PREFIX, default \"G\"."
  (let ((pfix (if (stringp prefix) prefix "G"))
	(num (if (integerp prefix) prefix
	       (prog1 cl--gensym-counter
		 (setq cl--gensym-counter (1+ cl--gensym-counter))))))
    (make-symbol (format "%s%d" pfix num))))

(defmacro cl--pop2 (place)
  (declare (debug edebug-sexps))
  `(prog1 (car (cdr ,place))
     (setq ,place (cdr (cdr ,place)))))

(defun cl--expr-contains (x y)
  "Count number of times X refers to Y.  Return nil for 0 times."
  ;; FIXME: This is naive, and it will cl-count Y as referred twice in
  ;; (let ((Y 1)) Y) even though it should be 0.  Also it is often called on
  ;; non-macroexpanded code, so it may also miss some occurrences that would
  ;; only appear in the expanded code.
  (cond ((equal y x) 1)
	((and (consp x) (not (memq (car x) '(quote function cl-function))))
	 (let ((sum 0))
	   (while (consp x)
	     (setq sum (+ sum (or (cl--expr-contains (pop x) y) 0))))
	   (setq sum (+ sum (or (cl--expr-contains x y) 0)))
	   (and (> sum 0) sum)))
	(t nil)))

(defun cl--expr-contains-any (x y)
  (while (and y (not (cl--expr-contains x (car y)))) (pop y))
  y)

(defun cl--expr-depends-p (x y)
  "Check whether X may depend on any of the symbols in Y."
  (and (not (macroexp-const-p x))
       (or (not (cl--safe-expr-p x)) (cl--expr-contains-any x y))))

(defmacro macroexp-let2 (test sym exp &rest body)
  "Evaluate BODY with SYM bound to an expression for EXP's value.
The intended usage is that BODY generates an expression that
will refer to EXP's value multiple times, but will evaluate
EXP only once.  As BODY generates that expression, it should
use SYM to stand for the value of EXP.

If EXP is a simple, safe expression, then SYM's value is EXP itself.
Otherwise, SYM's value is a symbol which holds the value produced by
evaluating EXP.  The return value incorporates the value of BODY, plus
additional code to evaluate EXP once and save the result so SYM can
refer to it.

If BODY consists of multiple forms, they are all evaluated
but only the last one's value matters.

TEST is a predicate to determine whether EXP qualifies as simple and
safe; if TEST is nil, only constant expressions qualify.

Example:
 (macroexp-let2 nil foo EXP
   \\=`(* ,foo ,foo))
generates an expression that evaluates EXP once,
then returns the square of that value.
You could do this with
  (let ((foovar EXP))
    (* foovar foovar))
but using `macroexp-let2' produces more efficient code in
cases where EXP is a constant."
  (declare (indent 3) (debug (sexp sexp form body)))
  (let ((bodysym (make-symbol "body"))
        (expsym (make-symbol "exp")))
    `(let* ((,expsym ,exp)
            (,sym (if (funcall #',(or test #'macroexp-const-p) ,expsym)
                      ,expsym (make-symbol ,(symbol-name sym))))
            (,bodysym ,(macroexp-progn body)))
       (if (eq ,sym ,expsym) ,bodysym
         (macroexp-let* (list (list ,sym ,expsym))
                        ,bodysym)))))

(defun macroexp-const-p (exp)
  "Return non-nil if EXP will always evaluate to the same value."
  (cond ((consp exp) (or (eq (car exp) 'quote)
                         (and (eq (car exp) 'function)
                              (symbolp (cadr exp)))))
        ;; It would sometimes make sense to pass `any-value', but it's not
        ;; always safe since a "constant" variable may not actually always have
        ;; the same value.
        ((symbolp exp) (macroexp--const-symbol-p exp))
        (t t)))

(defun macroexp-copyable-p (exp)
  "Return non-nil if EXP can be copied without extra cost."
  (or (symbolp exp) (macroexp-const-p exp)))

(defun macroexp-quote (v)
  "Return an expression E such that `(eval E)' is V.

E is either V or (quote V) depending on whether V evaluates to
itself or not."
  (if (and (not (consp v))
	   (or (keywordp v)
	       (not (symbolp v))
	       (memq v '(nil t))))
      v
    (list 'quote v)))

(defun macroexp-copyable-p (exp)
  "Return non-nil if EXP can be copied without extra cost."
  (or (symbolp exp) (macroexp-const-p exp)))

(defun cl--map-keymap-recursively (cl-func-rec cl-map &optional cl-base)
  (or cl-base
      (setq cl-base (copy-sequence [0])))
  (map-keymap
   (lambda (cl-key cl-bind)
     (aset cl-base (1- (length cl-base)) cl-key)
     (if (keymapp cl-bind)
         (cl--map-keymap-recursively
          cl-func-rec cl-bind
          (vconcat cl-base (list 0)))
       (funcall cl-func-rec cl-base cl-bind)))
   cl-map))

;;;###autoload
(defun cl--map-intervals (cl-func &optional cl-what cl-prop cl-start cl-end)
  (or cl-what (setq cl-what (current-buffer)))
  (if (bufferp cl-what)
      (let (cl-mark cl-mark2 (cl-next t) cl-next2)
	(with-current-buffer cl-what
	  (setq cl-mark (copy-marker (or cl-start (point-min))))
	  (setq cl-mark2 (and cl-end (copy-marker cl-end))))
	(while (and cl-next (or (not cl-mark2) (< cl-mark cl-mark2)))
	  (setq cl-next (if cl-prop (next-single-property-change
				     cl-mark cl-prop cl-what)
			  (next-property-change cl-mark cl-what))
		cl-next2 (or cl-next (with-current-buffer cl-what
				       (point-max))))
	  (funcall cl-func (prog1 (marker-position cl-mark)
			     (set-marker cl-mark cl-next2))
		   (if cl-mark2 (min cl-next2 cl-mark2) cl-next2)))
	(set-marker cl-mark nil) (if cl-mark2 (set-marker cl-mark2 nil)))
    (or cl-start (setq cl-start 0))
    (or cl-end (setq cl-end (length cl-what)))
    (while (< cl-start cl-end)
      (let ((cl-next (or (if cl-prop (next-single-property-change
				      cl-start cl-prop cl-what)
			   (next-property-change cl-start cl-what))
			 cl-end)))
	(funcall cl-func cl-start (min cl-next cl-end))
	(setq cl-start cl-next)))))

;;;###autoload
(defun cl--map-overlays (cl-func &optional cl-buffer cl-start cl-end cl-arg)
  (or cl-buffer (setq cl-buffer (current-buffer)))
  (let (cl-ovl)
    (with-current-buffer cl-buffer
      (setq cl-ovl (overlay-lists))
      (if cl-start (setq cl-start (copy-marker cl-start)))
      (if cl-end (setq cl-end (copy-marker cl-end))))
    (setq cl-ovl (nconc (car cl-ovl) (cdr cl-ovl)))
    (while (and cl-ovl
		(or (not (overlay-start (car cl-ovl)))
		    (and cl-end (>= (overlay-start (car cl-ovl)) cl-end))
		    (and cl-start (<= (overlay-end (car cl-ovl)) cl-start))
		    (not (funcall cl-func (car cl-ovl) cl-arg))))
      (setq cl-ovl (cdr cl-ovl)))
    (if cl-start (set-marker cl-start nil))
    (if cl-end (set-marker cl-end nil))))

(defvar cl--loop-args) (defvar cl--loop-accum-var) (defvar cl--loop-accum-vars)
(defvar cl--loop-bindings) (defvar cl--loop-body) (defvar cl--loop-conditions)
(defvar cl--loop-finally)
(defvar cl--loop-finish-flag)           ;Symbol set to nil to exit the loop?
(defvar cl--loop-first-flag)
(defvar cl--loop-initially) (defvar cl--loop-iterator-function)
(defvar cl--loop-name)
(defvar cl--loop-result) (defvar cl--loop-result-explicit)
(defvar cl--loop-result-var) (defvar cl--loop-steps)
(defvar cl--loop-symbol-macs)

(defun cl--loop-set-iterator-function (kind iterator)
  (if cl--loop-iterator-function
      ;; FIXME: Of course, we could make it work, but why bother.
      (error "Iteration on %S does not support this combination" kind)
    (setq cl--loop-iterator-function iterator)))

;;;###autoload
(defmacro cl-loop (&rest loop-args)
  "The Common Lisp `loop' macro.
Valid clauses include:
  For clauses:
    for VAR from/upfrom/downfrom EXPR1 to/upto/downto/above/below EXPR2
        [by EXPR3]
    for VAR = EXPR1 then EXPR2
    for VAR in/on/in-ref LIST [by FUNC]
    for VAR across/across-ref ARRAY
    for VAR being:
      the elements of/of-ref SEQUENCE [using (index VAR2)]
      the symbols [of OBARRAY]
      the hash-keys/hash-values of HASH-TABLE [using (hash-values/hash-keys V2)]
      the key-codes/key-bindings/key-seqs of KEYMAP [using (key-bindings VAR2)]
      the overlays/intervals [of BUFFER] [from POS1] [to POS2]
      the frames/buffers
      the windows [of FRAME]
  Iteration clauses:
    repeat INTEGER
    while/until/always/never/thereis CONDITION
  Accumulation clauses:
    collect/append/nconc/concat/vconcat/count/sum/maximize/minimize FORM
      [into VAR]
  Miscellaneous clauses:
    with VAR = INIT
    if/when/unless COND CLAUSE [and CLAUSE]... else CLAUSE [and CLAUSE...]
    named NAME
    initially/finally [do] EXPRS...
    do EXPRS...
    [finally] return EXPR

For more details, see Info node `(cl)Loop Facility'.

\(fn CLAUSE...)"
  (declare (debug (&rest &or
                         ;; These are usually followed by a symbol, but it can
                         ;; actually be any destructuring-bind pattern, which
                         ;; would erroneously match `form'.
                         [[&or "for" "as" "with" "and"] sexp]
                         ;; These are followed by expressions which could
                         ;; erroneously match `symbolp'.
                         [[&or "from" "upfrom" "downfrom" "to" "upto" "downto"
                               "above" "below" "by" "in" "on" "=" "across"
                               "repeat" "while" "until" "always" "never"
                               "thereis" "collect" "append" "nconc" "sum"
                               "count" "maximize" "minimize"
                               "if" "when" "unless"
                               "return"]
                          form]
                         ["using" (symbolp symbolp)]
                         ;; Simple default, which covers 99% of the cases.
                         symbolp form)))
  (if (not (memq t (mapcar #'symbolp
                           (delq nil (delq t (cl-copy-list loop-args))))))
      `(cl-block nil (while t ,@loop-args))
    (let ((cl--loop-args loop-args) (cl--loop-name nil) (cl--loop-bindings nil)
	  (cl--loop-body nil)		(cl--loop-steps nil)
	  (cl--loop-result nil)		(cl--loop-result-explicit nil)
	  (cl--loop-result-var nil)	(cl--loop-finish-flag nil)
	  (cl--loop-accum-var nil)	(cl--loop-accum-vars nil)
	  (cl--loop-initially nil)	(cl--loop-finally nil)
	  (cl--loop-iterator-function nil) (cl--loop-first-flag nil)
          (cl--loop-symbol-macs nil)
          (cl--loop-conditions nil))
      ;; Here is more or less how those dynbind vars are used after looping
      ;; over cl--parse-loop-clause:
      ;;
      ;; (cl-block ,cl--loop-name
      ;;   (cl-symbol-macrolet ,cl--loop-symbol-macs
      ;;     (foldl #'cl--loop-let
      ;;            `((,cl--loop-result-var)
      ;;              ((,cl--loop-first-flag t))
      ;;              ((,cl--loop-finish-flag t))
      ;;              ,@cl--loop-bindings)
      ;;           ,@(nreverse cl--loop-initially)
      ;;           (while                   ;(well: cl--loop-iterator-function)
      ;;               ,(car (cl--loop-build-ands (nreverse cl--loop-body)))
      ;;             ,@(cadr (cl--loop-build-ands (nreverse cl--loop-body)))
      ;;             ,@(nreverse cl--loop-steps)
      ;;             (setq ,cl--loop-first-flag nil))
      ;;           (if (not ,cl--loop-finish-flag) ;FIXME: Why `if' vs `progn'?
      ;;               ,cl--loop-result-var
      ;;             ,@(nreverse cl--loop-finally)
      ;;             ,(or cl--loop-result-explicit
      ;;                  cl--loop-result)))))
      ;;
      (setq cl--loop-args (append cl--loop-args '(cl-end-loop)))
      (while (not (eq (car cl--loop-args) 'cl-end-loop))
        (cl--parse-loop-clause))
      (if cl--loop-finish-flag
	  (push `((,cl--loop-finish-flag t)) cl--loop-bindings))
      (if cl--loop-first-flag
	  (progn (push `((,cl--loop-first-flag t)) cl--loop-bindings)
		 (push `(setq ,cl--loop-first-flag nil) cl--loop-steps)))
      (let* ((epilogue (nconc (nreverse cl--loop-finally)
			      (list (or cl--loop-result-explicit
                                        cl--loop-result))))
	     (ands (cl--loop-build-ands (nreverse cl--loop-body)))
	     (while-body (nconc (cadr ands) (nreverse cl--loop-steps)))
	     (body (append
		    (nreverse cl--loop-initially)
		    (list (if cl--loop-iterator-function
			      `(cl-block --cl-finish--
                                 ,(funcall cl--loop-iterator-function
                                           (if (eq (car ands) t) while-body
                                             (cons `(or ,(car ands)
                                                        (cl-return-from
                                                            --cl-finish--
                                                          nil))
                                                   while-body))))
			    `(while ,(car ands) ,@while-body)))
		    (if cl--loop-finish-flag
			(if (equal epilogue '(nil)) (list cl--loop-result-var)
			  `((if ,cl--loop-finish-flag
				(progn ,@epilogue) ,cl--loop-result-var)))
		      epilogue))))
	(if cl--loop-result-var
            (push (list cl--loop-result-var) cl--loop-bindings))
	(while cl--loop-bindings
	  (if (cdar cl--loop-bindings)
	      (setq body (list (cl--loop-let (pop cl--loop-bindings) body t)))
	    (let ((lets nil))
	      (while (and cl--loop-bindings
			  (not (cdar cl--loop-bindings)))
		(push (car (pop cl--loop-bindings)) lets))
	      (setq body (list (cl--loop-let lets body nil))))))
	(if cl--loop-symbol-macs
	    (setq body
                  (list `(cl-symbol-macrolet ,cl--loop-symbol-macs ,@body))))
	`(cl-block ,cl--loop-name ,@body)))))

(defmacro cl--push-clause-loop-body (clause)
  "Apply CLAUSE to both `cl--loop-conditions' and `cl--loop-body'."
  (macroexp-let2 nil sym clause
    `(progn
       (push ,sym cl--loop-conditions)
       (push ,sym cl--loop-body))))

;; Below is a complete spec for cl-loop, in several parts that correspond
;; to the syntax given in CLtL2.  The specs do more than specify where
;; the forms are; it also specifies, as much as Edebug allows, all the
;; syntactically valid cl-loop clauses.  The disadvantage of this
;; completeness is rigidity, but the "for ... being" clause allows
;; arbitrary extensions of the form: [symbolp &rest &or symbolp form].

;; (def-edebug-spec cl-loop
;;   ([&optional ["named" symbolp]]
;;    [&rest
;;     &or
;;     ["repeat" form]
;;     loop-for-as
;;     loop-with
;;     loop-initial-final]
;;    [&rest loop-clause]
;;    ))

;; (def-edebug-elem-spec 'loop-with
;;  '("with" loop-var
;;    loop-type-spec
;;    [&optional ["=" form]]
;;    &rest ["and" loop-var
;; 	  loop-type-spec
;; 	  [&optional ["=" form]]]))

;; (def-edebug-elem-spec 'loop-for-as
;;  '([&or "for" "as"] loop-for-as-subclause
;;    &rest ["and" loop-for-as-subclause]))

;; (def-edebug-elem-spec 'loop-for-as-subclause
;;  '(loop-var
;;    loop-type-spec
;;    &or
;;    [[&or "in" "on" "in-ref" "across-ref"]
;;     form &optional ["by" function-form]]

;;    ["=" form &optional ["then" form]]
;;    ["across" form]
;;    ["being"
;;     [&or "the" "each"]
;;     &or
;;     [[&or "element" "elements"]
;;      [&or "of" "in" "of-ref"] form
;;      &optional "using" ["index" symbolp]];; is this right?
;;     [[&or "hash-key" "hash-keys"
;; 	  "hash-value" "hash-values"]
;;      [&or "of" "in"]
;;      hash-table-p &optional ["using" ([&or "hash-value" "hash-values"
;; 					   "hash-key" "hash-keys"] sexp)]]

;;     [[&or "symbol" "present-symbol" "external-symbol"
;; 	  "symbols" "present-symbols" "external-symbols"]
;;      [&or "in" "of"] package-p]

;;     ;; Extensions for Emacs Lisp, including Lucid Emacs.
;;     [[&or "frame" "frames"
;; 	  "screen" "screens"
;; 	  "buffer" "buffers"]]

;;     [[&or "window" "windows"]
;;      [&or "of" "in"] form]

;;     [[&or "overlay" "overlays"
;; 	  "extent" "extents"]
;;      [&or "of" "in"] form
;;      &optional [[&or "from" "to"] form]]

;;     [[&or "interval" "intervals"]
;;      [&or "in" "of"] form
;;      &optional [[&or "from" "to"] form]
;;      ["property" form]]

;;     [[&or "key-code" "key-codes"
;; 	  "key-seq" "key-seqs"
;; 	  "key-binding" "key-bindings"]
;;      [&or "in" "of"] form
;;      &optional ["using" ([&or "key-code" "key-codes"
;; 			      "key-seq" "key-seqs"
;; 			      "key-binding" "key-bindings"]
;; 			 sexp)]]
;;     ;; For arbitrary extensions, recognize anything else.
;;     [symbolp &rest &or symbolp form]
;;     ]

;;    ;; arithmetic - must be last since all parts are optional.
;;    [[&optional [[&or "from" "downfrom" "upfrom"] form]]
;;     [&optional [[&or "to" "downto" "upto" "below" "above"] form]]
;;     [&optional ["by" form]]
;;     ]))

;; (def-edebug-elem-spec 'loop-initial-final
;;  '(&or ["initially"
;; 	;; [&optional &or "do" "doing"]  ;; CLtL2 doesn't allow this.
;; 	&rest loop-non-atomic-expr]
;;        ["finally" &or
;; 	[[&optional &or "do" "doing"] &rest loop-non-atomic-expr]
;; 	["return" form]]))

;; (def-edebug-elem-spec 'loop-and-clause
;;   '(loop-clause &rest ["and" loop-clause]))

;; (def-edebug-elem-spec 'loop-clause
;;  '(&or
;;    [[&or "while" "until" "always" "never" "thereis"] form]

;;    [[&or "collect" "collecting"
;; 	 "append" "appending"
;; 	 "nconc" "nconcing"
;; 	 "concat" "vconcat"] form
;; 	 [&optional ["into" loop-var]]]

;;    [[&or "count" "counting"
;; 	 "sum" "summing"
;; 	 "maximize" "maximizing"
;; 	 "minimize" "minimizing"] form
;; 	 [&optional ["into" loop-var]]
;; 	 loop-type-spec]

;;    [[&or "if" "when" "unless"]
;;     form loop-and-clause
;;     [&optional ["else" loop-and-clause]]
;;     [&optional "end"]]

;;    [[&or "do" "doing"] &rest loop-non-atomic-expr]

;;    ["return" form]
;;    loop-initial-final
;;    ))

;; (def-edebug-elem-spec 'loop-non-atomic-expr
;;   '([&not atom] form))

;; (def-edebug-elem-spec 'loop-var
;;   ;; The symbolp must be last alternative to recognize e.g. (a b . c)
;;   ;; loop-var =>
;;   ;; (loop-var . [&or nil loop-var])
;;   ;; (symbolp . [&or nil loop-var])
;;   ;; (symbolp . loop-var)
;;   ;; (symbolp . (symbolp . [&or nil loop-var]))
;;   ;; (symbolp . (symbolp . loop-var))
;;   ;; (symbolp . (symbolp . symbolp)) == (symbolp symbolp . symbolp)
;;   '(&or (loop-var . [&or nil loop-var]) [gate symbolp]))

;; (def-edebug-elem-spec 'loop-type-spec
;;   '(&optional ["of-type" loop-d-type-spec]))

;; (def-edebug-elem-spec 'loop-d-type-spec
;;   '(&or (loop-d-type-spec . [&or nil loop-d-type-spec]) cl-type-spec))

(defun cl--parse-loop-clause ()		; uses loop-*
  (let ((word (pop cl--loop-args))
	(hash-types '(hash-key hash-keys hash-value hash-values))
	(key-types '(key-code key-codes key-seq key-seqs
		     key-binding key-bindings)))
    (cond

     ((null cl--loop-args)
      (error "Malformed `cl-loop' macro"))

     ((eq word 'named)
      (setq cl--loop-name (pop cl--loop-args)))

     ((eq word 'initially)
      (if (memq (car cl--loop-args) '(do doing)) (pop cl--loop-args))
      (or (consp (car cl--loop-args))
          (error "Syntax error on `initially' clause"))
      (while (consp (car cl--loop-args))
	(push (pop cl--loop-args) cl--loop-initially)))

     ((eq word 'finally)
      (if (eq (car cl--loop-args) 'return)
	  (setq cl--loop-result-explicit
                (or (cl--pop2 cl--loop-args) '(quote nil)))
	(if (memq (car cl--loop-args) '(do doing)) (pop cl--loop-args))
	(or (consp (car cl--loop-args))
            (error "Syntax error on `finally' clause"))
	(if (and (eq (caar cl--loop-args) 'return) (null cl--loop-name))
	    (setq cl--loop-result-explicit
                  (or (nth 1 (pop cl--loop-args)) '(quote nil)))
	  (while (consp (car cl--loop-args))
	    (push (pop cl--loop-args) cl--loop-finally)))))

     ((memq word '(for as))
      (let ((loop-for-bindings nil) (loop-for-sets nil) (loop-for-steps nil)
	    (ands nil))
	(while
	    ;; Use `cl-gensym' rather than `make-symbol'.  It's important that
	    ;; (not (eq (symbol-name var1) (symbol-name var2))) because
	    ;; these vars get added to the macro-environment.
	    (let ((var (or (pop cl--loop-args) (cl-gensym "--cl-var--"))))
	      (setq word (pop cl--loop-args))
	      (if (eq word 'being) (setq word (pop cl--loop-args)))
	      (if (memq word '(the each)) (setq word (pop cl--loop-args)))
	      (if (memq word '(buffer buffers))
		  (setq word 'in
                        cl--loop-args (cons '(buffer-list) cl--loop-args)))
	      (cond

	       ((memq word '(from downfrom upfrom to downto upto
			     above below by))
		(push word cl--loop-args)
		(if (memq (car cl--loop-args) '(downto above))
		    (error "Must specify `from' value for downward cl-loop"))
		(let* ((down (or (eq (car cl--loop-args) 'downfrom)
				 (memq (nth 2 cl--loop-args)
                                       '(downto above))))
		       (excl (or (memq (car cl--loop-args) '(above below))
				 (memq (nth 2 cl--loop-args)
                                       '(above below))))
		       (start (and (memq (car cl--loop-args)
                                         '(from upfrom downfrom))
				   (cl--pop2 cl--loop-args)))
		       (end (and (memq (car cl--loop-args)
				       '(to upto downto above below))
				 (cl--pop2 cl--loop-args)))
		       (step (and (eq (car cl--loop-args) 'by)
                                  (cl--pop2 cl--loop-args)))
		       (end-var (and (not (macroexp-const-p end))
				     (make-symbol "--cl-var--")))
		       (step-var (and (not (macroexp-const-p step))
				      (make-symbol "--cl-var--"))))
		  (and step (numberp step) (<= step 0)
		       (error "Loop `by' value is not positive: %s" step))
		  (push (list var (or start 0)) loop-for-bindings)
		  (if end-var (push (list end-var end) loop-for-bindings))
		  (if step-var (push (list step-var step)
				     loop-for-bindings))
		  (when end
                    (cl--push-clause-loop-body
                     (list
                      (if down (if excl '> '>=) (if excl '< '<=))
                      var (or end-var end))))
		  (push (list var (list (if down '- '+) var
					(or step-var step 1)))
			loop-for-steps)))

	       ((memq word '(in in-ref on))
		(let* ((on (eq word 'on))
		       (temp (if (and on (symbolp var))
				 var (make-symbol "--cl-var--"))))
		  (push (list temp (pop cl--loop-args)) loop-for-bindings)
                  (cl--push-clause-loop-body `(consp ,temp))
		  (if (eq word 'in-ref)
		      (push (list var `(car ,temp)) cl--loop-symbol-macs)
		    (or (eq temp var)
			(progn
			  (push (list var nil) loop-for-bindings)
			  (push (list var (if on temp `(car ,temp)))
				loop-for-sets))))
		  (push (list temp
			      (if (eq (car cl--loop-args) 'by)
				  (let ((step (cl--pop2 cl--loop-args)))
				    (if (and (memq (car-safe step)
						   '(quote function
							   cl-function))
					     (symbolp (nth 1 step)))
					(list (nth 1 step) temp)
				      `(funcall ,step ,temp)))
				`(cdr ,temp)))
			loop-for-steps)))

	       ((eq word '=)
		(let* ((start (pop cl--loop-args))
		       (then (if (eq (car cl--loop-args) 'then)
                                 (cl--pop2 cl--loop-args) start))
                       (first-assign (or cl--loop-first-flag
					 (setq cl--loop-first-flag
					       (make-symbol "--cl-var--")))))
		  (push (list var nil) loop-for-bindings)
		  (if (or ands (eq (car cl--loop-args) 'and))
		      (progn
			(push `(,var (if ,first-assign ,start ,var)) loop-for-sets)
			(push `(,var (if ,(car (cl--loop-build-ands
                                                (nreverse cl--loop-conditions)))
                                         ,then ,var))
                              loop-for-steps))
                    (push (if (eq start then)
		              `(,var ,then)
                            `(,var (if ,first-assign ,start ,then)))
                          loop-for-sets))))

	       ((memq word '(across across-ref))
		(let ((temp-vec (make-symbol "--cl-vec--"))
		      (temp-idx (make-symbol "--cl-idx--")))
		  (push (list temp-vec (pop cl--loop-args)) loop-for-bindings)
		  (push (list temp-idx -1) loop-for-bindings)
                  (push `(setq ,temp-idx (1+ ,temp-idx)) cl--loop-body)
		  (cl--push-clause-loop-body
                   `(< ,temp-idx (length ,temp-vec)))
		  (if (eq word 'across-ref)
		      (push (list var `(aref ,temp-vec ,temp-idx))
			    cl--loop-symbol-macs)
		    (push (list var nil) loop-for-bindings)
		    (push (list var `(aref ,temp-vec ,temp-idx))
			  loop-for-sets))))

	       ((memq word '(element elements))
		(let ((ref (or (memq (car cl--loop-args) '(in-ref of-ref))
			       (and (not (memq (car cl--loop-args) '(in of)))
				    (error "Expected `of'"))))
		      (seq (cl--pop2 cl--loop-args))
		      (temp-seq (make-symbol "--cl-seq--"))
		      (temp-idx
                       (if (eq (car cl--loop-args) 'using)
                           (if (and (= (length (cadr cl--loop-args)) 2)
                                    (eq (caadr cl--loop-args) 'index))
                               (cadr (cl--pop2 cl--loop-args))
                             (error "Bad `using' clause"))
                         (make-symbol "--cl-idx--"))))
		  (push (list temp-seq seq) loop-for-bindings)
		  (push (list temp-idx 0) loop-for-bindings)
		  (if ref
                      (let ((temp-len (make-symbol "--cl-len--")))
			(push (list temp-len `(length ,temp-seq))
			      loop-for-bindings)
			(push (list var `(elt ,temp-seq ,temp-idx))
			      cl--loop-symbol-macs)
                        (cl--push-clause-loop-body `(< ,temp-idx ,temp-len)))
		    (push (list var nil) loop-for-bindings)
		    (cl--push-clause-loop-body `(and ,temp-seq
                                                     (or (consp ,temp-seq)
                                                         (< ,temp-idx (length ,temp-seq)))))
		    (push (list var `(if (consp ,temp-seq)
                                         (pop ,temp-seq)
                                       (aref ,temp-seq ,temp-idx)))
			  loop-for-sets))
		  (push (list temp-idx `(1+ ,temp-idx))
			loop-for-steps)))

	       ((memq word hash-types)
		(or (memq (car cl--loop-args) '(in of))
                    (error "Expected `of'"))
		(let* ((table (cl--pop2 cl--loop-args))
		       (other
                        (if (eq (car cl--loop-args) 'using)
                            (if (and (= (length (cadr cl--loop-args)) 2)
                                     (memq (caadr cl--loop-args) hash-types)
                                     (not (eq (caadr cl--loop-args) word)))
                                (cadr (cl--pop2 cl--loop-args))
                              (error "Bad `using' clause"))
                          (make-symbol "--cl-var--"))))
		  (if (memq word '(hash-value hash-values))
		      (setq var (prog1 other (setq other var))))
		  (cl--loop-set-iterator-function
                   'hash-tables (lambda (body)
                                  `(maphash (lambda (,var ,other) . ,body)
                                            ,table)))))

	       ((memq word '(symbol present-symbol external-symbol
			     symbols present-symbols external-symbols))
		(let ((ob (and (memq (car cl--loop-args) '(in of))
                               (cl--pop2 cl--loop-args))))
		  (cl--loop-set-iterator-function
                   'symbols (lambda (body)
                              `(mapatoms (lambda (,var) . ,body) ,ob)))))

	       ((memq word '(overlay overlays extent extents))
		(let ((buf nil) (from nil) (to nil))
		  (while (memq (car cl--loop-args) '(in of from to))
		    (cond ((eq (car cl--loop-args) 'from)
                           (setq from (cl--pop2 cl--loop-args)))
			  ((eq (car cl--loop-args) 'to)
                           (setq to (cl--pop2 cl--loop-args)))
			  (t (setq buf (cl--pop2 cl--loop-args)))))
		  (cl--loop-set-iterator-function
                   'overlays (lambda (body)
                               `(cl--map-overlays
                                 (lambda (,var ,(make-symbol "--cl-var--"))
                                   (progn . ,body) nil)
                                 ,buf ,from ,to)))))

	       ((memq word '(interval intervals))
		(let ((buf nil) (prop nil) (from nil) (to nil)
		      (var1 (make-symbol "--cl-var1--"))
		      (var2 (make-symbol "--cl-var2--")))
		  (while (memq (car cl--loop-args) '(in of property from to))
		    (cond ((eq (car cl--loop-args) 'from)
                           (setq from (cl--pop2 cl--loop-args)))
			  ((eq (car cl--loop-args) 'to)
                           (setq to (cl--pop2 cl--loop-args)))
			  ((eq (car cl--loop-args) 'property)
			   (setq prop (cl--pop2 cl--loop-args)))
			  (t (setq buf (cl--pop2 cl--loop-args)))))
		  (if (and (consp var) (symbolp (car var)) (symbolp (cdr var)))
		      (setq var1 (car var) var2 (cdr var))
		    (push (list var nil) loop-for-bindings)
		    (push (list var `(cons ,var1 ,var2)) loop-for-sets))
		  (cl--loop-set-iterator-function
                   'intervals (lambda (body)
                                `(cl--map-intervals
                                  (lambda (,var1 ,var2) . ,body)
                                  ,buf ,prop ,from ,to)))))

	       ((memq word key-types)
		(or (memq (car cl--loop-args) '(in of))
                    (error "Expected `of'"))
		(let ((cl-map (cl--pop2 cl--loop-args))
		      (other
                       (if (eq (car cl--loop-args) 'using)
                           (if (and (= (length (cadr cl--loop-args)) 2)
                                    (memq (caadr cl--loop-args) key-types)
                                    (not (eq (caadr cl--loop-args) word)))
                               (cadr (cl--pop2 cl--loop-args))
                             (error "Bad `using' clause"))
                         (make-symbol "--cl-var--"))))
		  (if (memq word '(key-binding key-bindings))
		      (setq var (prog1 other (setq other var))))
		  (cl--loop-set-iterator-function
                   'keys (lambda (body)
                           `(,(if (memq word '(key-seq key-seqs))
                                  'cl--map-keymap-recursively 'map-keymap)
                             (lambda (,var ,other) . ,body) ,cl-map)))))

	       ((memq word '(frame frames screen screens))
		(let ((temp (make-symbol "--cl-var--")))
		  (push (list var  '(selected-frame))
			loop-for-bindings)
		  (push (list temp nil) loop-for-bindings)
		  (cl--push-clause-loop-body `(prog1 (not (eq ,var ,temp))
                                                (or ,temp (setq ,temp ,var))))
		  (push (list var `(next-frame ,var))
			loop-for-steps)))

	       ((memq word '(window windows))
		(let ((scr (and (memq (car cl--loop-args) '(in of))
                                (cl--pop2 cl--loop-args)))
		      (temp (make-symbol "--cl-var--"))
		      (minip (make-symbol "--cl-minip--")))
		  (push (list var (if scr
				      `(frame-selected-window ,scr)
				    '(selected-window)))
			loop-for-bindings)
		  ;; If we started in the minibuffer, we need to
		  ;; ensure that next-window will bring us back there
		  ;; at some point.  (Bug#7492).
		  ;; (Consider using walk-windows instead of cl-loop if
		  ;; you care about such things.)
		  (push (list minip `(minibufferp (window-buffer ,var)))
			loop-for-bindings)
		  (push (list temp nil) loop-for-bindings)
		  (cl--push-clause-loop-body `(prog1 (not (eq ,var ,temp))
                                                (or ,temp (setq ,temp ,var))))
		  (push (list var `(next-window ,var ,minip))
			loop-for-steps)))

	       (t
		;; This is an advertised interface: (info "(cl)Other Clauses").
		(let ((handler (and (symbolp word)
				    (get word 'cl-loop-for-handler))))
		  (if handler
		      (funcall handler var)
		    (error "Expected a `for' preposition, found %s" word)))))
	      (eq (car cl--loop-args) 'and))
	  (setq ands t)
	  (pop cl--loop-args))
	(if (and ands loop-for-bindings)
	    (push (nreverse loop-for-bindings) cl--loop-bindings)
	  (setq cl--loop-bindings (nconc (mapcar #'list loop-for-bindings)
				         cl--loop-bindings)))
	(if loop-for-sets
	    (push `(progn
                     ,(cl--loop-let (nreverse loop-for-sets) 'setq ands)
                     t)
                  cl--loop-body))
	(when loop-for-steps
	  (push (cons (if ands 'cl-psetq 'setq)
		      (apply #'append (nreverse loop-for-steps)))
		cl--loop-steps))))

     ((eq word 'repeat)
      (let ((temp (make-symbol "--cl-var--")))
	(push (list (list temp (pop cl--loop-args))) cl--loop-bindings)
	(push `(>= (setq ,temp (1- ,temp)) 0) cl--loop-body)))

     ((memq word '(collect collecting))
      (let ((what (pop cl--loop-args))
	    (var (cl--loop-handle-accum nil 'nreverse)))
	(if (eq var cl--loop-accum-var)
	    (push `(progn (push ,what ,var) t) cl--loop-body)
	  (push `(progn
                   (setq ,var (nconc ,var (list ,what)))
                   t)
                cl--loop-body))))

     ((memq word '(nconc nconcing append appending))
      (let ((what (pop cl--loop-args))
	    (var (cl--loop-handle-accum nil 'nreverse)))
	(push `(progn
                 (setq ,var
                       ,(if (eq var cl--loop-accum-var)
                            `(nconc
                              (,(if (memq word '(nconc nconcing))
                                    #'nreverse #'reverse)
                               ,what)
                              ,var)
                          `(,(if (memq word '(nconc nconcing))
                                 #'nconc #'append)
                            ,var ,what)))
                 t)
              cl--loop-body)))

     ((memq word '(concat concating))
      (let ((what (pop cl--loop-args))
	    (var (cl--loop-handle-accum "")))
	(push `(progn (cl-callf concat ,var ,what) t) cl--loop-body)))

     ((memq word '(vconcat vconcating))
      (let ((what (pop cl--loop-args))
	    (var (cl--loop-handle-accum [])))
	(push `(progn (cl-callf vconcat ,var ,what) t) cl--loop-body)))

     ((memq word '(sum summing))
      (let ((what (pop cl--loop-args))
	    (var (cl--loop-handle-accum 0)))
	(push `(progn (cl-incf ,var ,what) t) cl--loop-body)))

     ((memq word '(count counting))
      (let ((what (pop cl--loop-args))
	    (var (cl--loop-handle-accum 0)))
	(push `(progn (if ,what (cl-incf ,var)) t) cl--loop-body)))

     ((memq word '(minimize minimizing maximize maximizing))
      (push `(progn ,(macroexp-let2 macroexp-copyable-p temp
                                    (pop cl--loop-args)
                       (let* ((var (cl--loop-handle-accum nil))
                              (func (intern (substring (symbol-name word)
                                                       0 3))))
                         `(setq ,var (if ,var (,func ,var ,temp) ,temp))))
                    t)
            cl--loop-body))

     ((eq word 'with)
      (let ((bindings nil))
	(while (progn (push (list (pop cl--loop-args)
				  (and (eq (car cl--loop-args) '=)
                                       (cl--pop2 cl--loop-args)))
			    bindings)
		      (eq (car cl--loop-args) 'and))
	  (pop cl--loop-args))
	(push (nreverse bindings) cl--loop-bindings)))

     ((eq word 'while)
      (push (pop cl--loop-args) cl--loop-body))

     ((eq word 'until)
      (push `(not ,(pop cl--loop-args)) cl--loop-body))

     ((eq word 'always)
      (or cl--loop-finish-flag
          (setq cl--loop-finish-flag (make-symbol "--cl-flag--")))
      (push `(setq ,cl--loop-finish-flag ,(pop cl--loop-args)) cl--loop-body)
      (setq cl--loop-result t))

     ((eq word 'never)
      (or cl--loop-finish-flag
          (setq cl--loop-finish-flag (make-symbol "--cl-flag--")))
      (push `(setq ,cl--loop-finish-flag (not ,(pop cl--loop-args)))
	    cl--loop-body)
      (setq cl--loop-result t))

     ((eq word 'thereis)
      (or cl--loop-finish-flag
          (setq cl--loop-finish-flag (make-symbol "--cl-flag--")))
      (or cl--loop-result-var
          (setq cl--loop-result-var (make-symbol "--cl-var--")))
      (push `(setq ,cl--loop-finish-flag
                   (not (setq ,cl--loop-result-var ,(pop cl--loop-args))))
	    cl--loop-body))

     ((memq word '(if when unless))
      (let* ((cond (pop cl--loop-args))
	     (then (let ((cl--loop-body nil))
		     (cl--parse-loop-clause)
		     (cl--loop-build-ands (nreverse cl--loop-body))))
	     (else (let ((cl--loop-body nil))
		     (if (eq (car cl--loop-args) 'else)
			 (progn (pop cl--loop-args) (cl--parse-loop-clause)))
		     (cl--loop-build-ands (nreverse cl--loop-body))))
	     (simple (and (eq (car then) t) (eq (car else) t))))
	(if (eq (car cl--loop-args) 'end) (pop cl--loop-args))
	(if (eq word 'unless) (setq then (prog1 else (setq else then))))
	(let ((form (cons (if simple (cons 'progn (nth 1 then)) (nth 2 then))
			  (if simple (nth 1 else) (list (nth 2 else))))))
	  (setq form (if (cl--expr-contains form 'it)
                         `(let ((it ,cond)) (if it ,@form))
                       `(if ,cond ,@form)))
	  (push (if simple `(progn ,form t) form) cl--loop-body))))

     ((memq word '(do doing))
      (let ((body nil))
	(or (consp (car cl--loop-args)) (error "Syntax error on `do' clause"))
	(while (consp (car cl--loop-args)) (push (pop cl--loop-args) body))
	(push (cons 'progn (nreverse (cons t body))) cl--loop-body)))

     ((eq word 'return)
      (or cl--loop-finish-flag
          (setq cl--loop-finish-flag (make-symbol "--cl-var--")))
      (or cl--loop-result-var
          (setq cl--loop-result-var (make-symbol "--cl-var--")))
      (push `(setq ,cl--loop-result-var ,(pop cl--loop-args)
                   ,cl--loop-finish-flag nil)
            cl--loop-body))

     (t
      ;; This is an advertised interface: (info "(cl)Other Clauses").
      (let ((handler (and (symbolp word) (get word 'cl-loop-handler))))
	(or handler (error "Expected a cl-loop keyword, found %s" word))
	(funcall handler))))
    (if (eq (car cl--loop-args) 'and)
	(progn (pop cl--loop-args) (cl--parse-loop-clause)))))

(defun cl--unused-var-p (sym)
  (or (null sym) (eq ?_ (aref (symbol-name sym) 0))))

(defun cl--loop-let (specs body par)    ; modifies cl--loop-bindings
  "Build an expression equivalent to (let SPECS BODY).
SPECS can include bindings using `cl-loop's destructuring (not to be
confused with the patterns of `cl-destructuring-bind').
If PAR is nil, do the bindings step by step, like `let*'.
If BODY is `setq', then use SPECS for assignments rather than for bindings."
  (let ((temps nil) (new nil))
    (when par
      (let ((p specs))
        (while (and p (or (symbolp (car-safe (car p))) (null (cadar p))))
          (setq p (cdr p)))
        (when p
          (setq par nil)
          (dolist (spec specs)
            (or (macroexp-const-p (cadr spec))
                (let ((temp (make-symbol "--cl-var--")))
                  (push (list temp (cadr spec)) temps)
                  (setcar (cdr spec) temp)))))))
    (while specs
      (let* ((binding (pop specs))
             (spec (car-safe binding)))
        (if (and (consp binding) (or (consp spec) (cl--unused-var-p spec)))
            (let* ((nspecs nil)
                   (expr (car (cdr-safe binding)))
                   (temp (last spec 0)))
              (if (and (cl--unused-var-p temp) (null expr))
                  nil ;; Don't bother declaring/setting `temp' since it won't
		      ;; be used when `expr' is nil, anyway.
		(when (or (null temp)
                          (and (eq body 'setq) (cl--unused-var-p temp)))
                  ;; Prefer a fresh uninterned symbol over "_to", to avoid
                  ;; warnings that we set an unused variable.
                  (setq temp (make-symbol "--cl-var--"))
                  ;; Make sure this temp variable is locally declared.
                  (when (eq body 'setq)
                    (push (list (list temp)) cl--loop-bindings)))
                (push (list temp expr) new))
              (while (consp spec)
                (push (list (pop spec)
                            (and expr (list (if spec 'pop 'car) temp)))
                      nspecs))
              (setq specs (nconc (nreverse nspecs) specs)))
          (push binding new))))
    (if (eq body 'setq)
	(let ((set (cons (if par 'cl-psetq 'setq)
                         (apply #'nconc (nreverse new)))))
	  (if temps `(let* ,(nreverse temps) ,set) set))
      `(,(if par 'let 'let*)
        ,(nconc (nreverse temps) (nreverse new)) ,@body))))

(defun cl--loop-handle-accum (def &optional func) ; uses loop-*
  (if (eq (car cl--loop-args) 'into)
      (let ((var (cl--pop2 cl--loop-args)))
	(or (memq var cl--loop-accum-vars)
	    (progn (push (list (list var def)) cl--loop-bindings)
		   (push var cl--loop-accum-vars)))
	var)
    (or cl--loop-accum-var
	(progn
	  (push (list (list
                       (setq cl--loop-accum-var (make-symbol "--cl-var--"))
                       def))
                cl--loop-bindings)
	  (setq cl--loop-result (if func (list func cl--loop-accum-var)
                                  cl--loop-accum-var))
	  cl--loop-accum-var))))

(defun cl--loop-build-ands (clauses)
  "Return various representations of (and . CLAUSES).
CLAUSES is a list of Elisp expressions, where clauses of the form
\(progn E1 E2 E3 .. t) are the focus of particular optimizations.
The return value has shape (COND BODY COMBO)
such that COMBO is equivalent to (and . CLAUSES)."
  (let ((ands nil)
	(body nil))
    ;; Look through `clauses', trying to optimize (progn ,@A t) (progn ,@B) ,@C
    ;; into (progn ,@A ,@B) ,@C.
    (while clauses
      (if (and (eq (car-safe (car clauses)) 'progn)
	       (eq (car (last (car clauses))) t))
	  (if (cdr clauses)
	      (setq clauses (cons (nconc (butlast (car clauses))
					 (if (eq (car-safe (cadr clauses))
						 'progn)
                                             (cdadr clauses)
					   (list (cadr clauses))))
				  (cddr clauses)))
            ;; A final (progn ,@A t) is moved outside of the `and'.
	    (setq body (cdr (butlast (pop clauses)))))
	(push (pop clauses) ands)))
    (setq ands (or (nreverse ands) (list t)))
    (list (if (cdr ands) (cons 'and ands) (car ands))
	  body
	  (let ((full (if body
			  (append ands (list (cons 'progn (append body '(t)))))
			ands)))
	    (if (cdr full) (cons 'and full) (car full))))))


;;; Other iteration control structures.

;;;###autoload
;; GNU `cl-case'/`cl-ecase' (from cl-macs.el) with the macroexp warning
;; helpers they rely on, so misplaced t clauses signal and suspicious
;; cases warn like upstream.

(defun macroexp-compiling-p ()
  "Return non-nil if we're macroexpanding for the compiler."
  ;; FIXME: ¡¡Major Ugly Hack!! To determine whether the output of this
  ;; macro-expansion will be processed by the byte-compiler, we check
  ;; circumstantial evidence.
  (member '(declare-function . byte-compile-macroexpand-declare-function)
          macroexpand-all-environment))

(defun macroexp-file-name ()
  "Return the name of the file from which the code comes.
Returns nil when we do not know.
A non-nil result is expected to be reliable when called from a macro in order
to find the file in which the macro's call was found, and it should be
reliable as well when used at the top-level of a file.
Other uses risk returning non-nil value that point to the wrong file."
  ;; `eval-buffer' binds `current-load-list' but not `load-file-name',
  ;; so prefer using it over using `load-file-name'.
  (let ((file (car (last current-load-list))))
    (or (if (stringp file) file)
        (bound-and-true-p byte-compile-current-file))))

(defvar macroexp--warned (make-hash-table :test #'equal :weakness 'key))

(defun macroexp--warn-wrap (arg msg form category)
  (let ((when-compiled
	 (lambda ()
           (when (if (consp category)
                     (apply #'byte-compile-warning-enabled-p category)
                   (byte-compile-warning-enabled-p category))
             (byte-compile-warn-x arg "%s" msg)))))
    `(progn
       (macroexp--funcall-if-compiled ',when-compiled)
       ,form)))

(define-obsolete-function-alias 'macroexp--warn-and-return
  #'macroexp-warn-and-return "28.1")
(defun macroexp-warn-and-return (msg form &optional category compile-only arg)
  "Return code equivalent to FORM labeled with warning MSG.
CATEGORY is the category of the warning, like the categories that
can appear in `byte-compile-warnings'.
COMPILE-ONLY non-nil means no warning should be emitted if the code
is executed without being compiled first.
ARG is a symbol (or a form) giving the source code position for the message.
It should normally be a symbol with position and it defaults to FORM."
  (cond
   ((null msg) form)
   ((macroexp-compiling-p)
    (if (and (consp form) (gethash form macroexp--warned))
        ;; Already wrapped this exp with a warning: avoid inf-looping
        ;; where we keep adding the same warning onto `form' because
        ;; macroexpand-all gets right back to macroexpanding `form'.
        form
      (puthash form form macroexp--warned)
      (macroexp--warn-wrap (or arg form) msg form category)))
   (t
    (unless compile-only
      (message "%sWarning: %s"
               (if (stringp load-file-name)
                   (concat (file-relative-name load-file-name) ": ")
                 "")
               msg))
    form)))

(defun macroexp--obsolete-warning (fun obsolescence-data type &optional key)
  (let ((instead (car obsolescence-data))
        (asof (nth 2 obsolescence-data)))
    (format-message
     "`%s' is an obsolete %s%s%s" fun type
     (if asof (concat " (as of " asof ")") "")
     (cond ((stringp instead) (concat "; " (substitute-command-keys instead)))
           ((and instead key)
            (format-message "; use `%s' (%s) instead." instead key))
           (instead (format-message "; use `%s' instead." instead))
           (t ".")))))

(defun macroexpand-1 (form &optional environment)
  "Perform (at most) one step of macroexpansion."
  (cond
   ((consp form)
    (let* ((head (car form))
           (env-expander (assq head environment)))
      (if env-expander
          (if (cdr env-expander)
              (apply (cdr env-expander) (cdr form))
            form)
        (if (not (and (symbolp head) (fboundp head)))
            form
          (let ((def (autoload-do-load (symbol-function head) head 'macro)))
            (cond
             ;; Follow alias, but only for macros, otherwise we may end up
             ;; skipping an important compiler-macro (e.g. cl--block-wrapper).
             ((and (symbolp def) (macrop def)) (cons def (cdr form)))
             ((not (consp def)) form)
             (t
              (if (eq 'macro (car def))
                  (apply (cdr def) (cdr form))
                form))))))))
   (t form)))

(defun macroexp-macroexpand (form env)
  "Like `macroexpand' but checking obsolescence."
  (let* ((macroexpand-all-environment env)
         new-form)
    (while (not (eq form (setq new-form (macroexpand-1 form env))))
      (let ((fun (car-safe form)))
        (setq form
              (if (and fun (symbolp fun)
                       (get fun 'byte-obsolete-info))
                  (macroexp-warn-and-return
                   (macroexp--obsolete-warning
                    fun (get fun 'byte-obsolete-info)
                    (if (symbolp (symbol-function fun)) "alias" "macro"))
                   new-form (list 'obsolete fun) nil fun)
                new-form))))
    form))

(defvar macroexp--warned (make-hash-table :test #'equal))

(defun macroexp--warn-wrap (_arg msg form _category)
  (message "Warning: %s" msg)
  form)

(defun macroexp-warn-and-return (msg form &optional category compile-only arg)
  "Return code equivalent to FORM labeled with warning MSG.
CATEGORY is the category of the warning, like the categories that
can appear in `byte-compile-warnings'.
COMPILE-ONLY non-nil means no warning should be emitted if the code
is executed without being compiled first.
ARG is a symbol (or a form) giving the source code position for the message.
It should normally be a symbol with position and it defaults to FORM."
  (cond
   ((null msg) form)
   ((macroexp-compiling-p)
    (if (and (consp form) (gethash form macroexp--warned))
        ;; Already wrapped this exp with a warning: avoid inf-looping
        ;; where we keep adding the same warning onto `form' because
        ;; macroexpand-all gets right back to macroexpanding `form'.
        form
      (puthash form form macroexp--warned)
      (macroexp--warn-wrap (or arg form) msg form category)))
   (t
    (unless compile-only
      (message "%sWarning: %s"
               (if (stringp load-file-name)
                   (concat (file-relative-name load-file-name) ": ")
                 "")
               msg))
    form)))

(defmacro cl-case (expr &rest clauses)
  "Eval EXPR and choose among clauses on that value.
Each clause looks like (KEYLIST BODY...).  EXPR is evaluated and
compared against each key in each KEYLIST; the corresponding BODY
is evaluated.  If no clause succeeds, this macro returns nil.  A
single non-nil atom may be used in place of a KEYLIST of one
atom.  A KEYLIST of t or `otherwise' is allowed only in the final
clause, and matches if no other keys match.  Key values are
compared by `eql'.

\(fn EXPR (KEYLIST BODY...)...)"
  (declare (indent 1) (debug (form &rest (sexp body))))
  (macroexp-let2 macroexp-copyable-p temp expr
    (let* ((head-list nil)
           (has-otherwise nil))
      `(cond
        ,@(mapcar
           (lambda (c)
             (cons (cond (has-otherwise
                          (error "Misplaced t or `otherwise' clause"))
                         ((memq (car c) '(t otherwise))
                          (setq has-otherwise t)
                          t)
                         ((eq (car c) 'cl--ecase-error-flag)
                          `(error "cl-ecase failed: %s, %s"
                                  ,temp ',(reverse head-list)))
                         ((null (car c))
                          (macroexp-warn-and-return
                           "Case nil will never match"
                           nil 'suspicious))
                         ((and (consp (car c)) (cdar c) (not (cddar c))
                               (memq (caar c) '(quote function)))
                          (macroexp-warn-and-return
                           (format-message
                            (concat "Case %s will match `%s'.  If "
                                    "that's intended, write %s "
                                    "instead.  Otherwise, don't "
                                    "quote `%s'.")
                            (car c) (caar c) (list (cadar c) (caar c))
                            (cadar c))
                           `(cl-member ,temp ',(car c)) 'suspicious))
                         ((listp (car c))
                          (setq head-list (append (car c) head-list))
                          `(cl-member ,temp ',(car c)))
                         (t
                          (if (memq (car c) head-list)
                              (error "Duplicate key in case: %s"
                                     (car c)))
                          (push (car c) head-list)
                          `(eql ,temp ',(car c))))
                   (or (cdr c) '(nil))))
           clauses)))))

;;;###autoload

(defmacro cl-ecase (expr &rest clauses)
  "Like `cl-case', but error if no case fits.
`otherwise'-clauses are not allowed.
\n(fn EXPR (KEYLIST BODY...)...)"
  (declare (indent 1) (debug cl-case))
  `(cl-case ,expr ,@clauses (cl--ecase-error-flag)))

;;;###autoload

;; Helpers for the ported cl-flet/cl-labels (macroexp.el, cl-macs.el).

(defun macroexp-let* (bindings exp)
  "Return an expression equivalent to \\=`(let* ,BINDINGS ,EXP)."
  (cond
   ((null bindings) exp)
   ((eq 'let* (car-safe exp)) `(let* (,@bindings ,@(cadr exp)) ,@(cddr exp)))
   (t `(let* ,bindings ,exp))))

(defun macroexp-if (test then else)
  "Return an expression equivalent to \\=`(if ,TEST ,THEN ,ELSE)."
  (cond
   ((eq (car-safe else) 'if)
    (cond
     ;; Drop this optimization: It's unsafe (it assumes that `test' is
     ;; pure, or at least idempotent), and it's not used even a single
     ;; time while compiling Emacs's sources.
     ;;((equal test (nth 1 else))
     ;; ;; Doing a test a second time: get rid of the redundancy.
     ;; (message "macroexp-if: sharing 'test' %S" test)
     ;; `(if ,test ,then ,@(nthcdr 3 else)))
     ((equal then (nth 2 else))
      ;; (message "macroexp-if: sharing 'then' %S" then)
      `(if (or ,test ,(nth 1 else)) ,then ,@(nthcdr 3 else)))
     ((equal (macroexp-unprogn then) (nthcdr 3 else))
      ;; (message "macroexp-if: sharing 'then' with not %S" then)
      `(if (or ,test (not ,(nth 1 else)))
           ,then ,@(macroexp-unprogn (nth 2 else))))
     (t
      `(cond (,test ,@(macroexp-unprogn then))
             (,(nth 1 else) ,@(macroexp-unprogn (nth 2 else)))
             ,@(let ((def (nthcdr 3 else))) (if def `((t ,@def))))))))
   ((eq (car-safe else) 'cond)
    `(cond (,test ,@(macroexp-unprogn then)) ,@(cdr else)))
   ;; Invert the test if that lets us reduce the depth of the tree.
   ((memq (car-safe then) '(if cond)) (macroexp-if `(not ,test) else then))
   (t `(if ,test ,then ,@(if else (macroexp-unprogn else))))))

(defconst cl--labels-magic (make-symbol "cl--labels-magic"))

(defvar cl--labels-convert-cache nil)

(defun cl--labels-convert (f)
  "Special macro-expander to rename (function F) references in `cl-labels'."
  (cond
   ;; ¡¡Big Ugly Hack!! We can't use a compiler-macro because those are checked
   ;; *after* handling `function', but we want to stop macroexpansion from
   ;; being applied infinitely, so we use a cache to return the exact `form'
   ;; being expanded even though we don't receive it.
   ;; In Common Lisp, we'd use the `&whole' arg instead (see
   ;; "Macro Lambda Lists" in the CLHS).
   ((let ((symbols-with-pos-enabled nil)) ;Don't rewrite #'<X@5> => #'<X@3>
      (eq f (car cl--labels-convert-cache)))
    ;; This value should be `eq' to the `&whole' form.
    ;; If this is not the case, we have a bug.
    (prog1 (cdr cl--labels-convert-cache)
      ;; Drop it, so it can't accidentally interfere with some
      ;; unrelated subsequent use of `function' with the same symbol.
      (setq cl--labels-convert-cache nil)))
   (t
    (let* ((found (assq f macroexpand-all-environment))
           (replacement (and found
                             (ignore-errors
                               (funcall (cdr found) cl--labels-magic)))))
      (if (and replacement (eq cl--labels-magic (car replacement)))
          (nth 1 replacement)
        ;; FIXME: Here, we'd like to return the `&whole' form, but since ELisp
        ;; doesn't have that, we approximate it via `cl--labels-convert-cache'.
        (let ((res `(function ,f)))
          (setq cl--labels-convert-cache (cons f res))
          res))))))

;;;###autoload

;; GNU `cl-flet'/`cl-flet*'/`cl-labels' (from cl-macs.el) so local
;; function expansion shapes and tail-call handling match upstream.

(defmacro cl-flet (bindings &rest body)
  "Make local function definitions.

Each definition can take the form (FUNC EXP) where FUNC is the function
name, and EXP is an expression that returns the function value to which
it should be bound, or it can take the more common form (FUNC ARGLIST
BODY...) which is a shorthand for (FUNC (lambda ARGLIST BODY)).

FUNC is defined only within FORM, not BODY, so you can't write recursive
function definitions.  Use `cl-labels' for that.  See Info node
`(cl) Function Bindings' for details.

\(fn ((FUNC ARGLIST BODY...) ...) FORM...)"
  (declare (indent 1)
           ;; The first (symbolp form) case doesn't use `&name' because
           ;; it's hard to associate this name with the body of the function
           ;; that `form' will return (bug#65344).
           ;; We could try and use a `&name' for those cases where the
           ;; body of the function can be found, (e.g. the form wraps
           ;; some `prog1/progn/let' around the final `lambda'), but it's
           ;; not clear it's worth the trouble.
           (debug ((&rest [&or (symbolp form)
                               (&define [&name symbolp "@cl-flet@"]
                                        [&name [] gensym] ;Make it unique!
                                        cl-lambda-list
                                        cl-declarations-or-string
                                        [&optional ("interactive" interactive)]
                                        def-body)])
                   cl-declarations body)))
  (let ((binds ()) (newenv macroexpand-all-environment))
    (dolist (binding bindings)
      (let ((var (make-symbol (format "--cl-%s--" (car binding))))
            (args-and-body (cdr binding)))
        (if (and (= (length args-and-body) 1)
                 (macroexp-copyable-p (car args-and-body)))
            ;; Optimize (cl-flet ((fun var)) body).
            (setq var (car args-and-body))
          (push (list var (if (= (length args-and-body) 1)
                              (car args-and-body)
                            `(cl-function (lambda . ,args-and-body))))
                binds))
	(push (cons (car binding)
                    (lambda (&rest args)
                      (if (eq (car args) cl--labels-magic)
                          (list cl--labels-magic var)
                        `(funcall ,var ,@args))))
              newenv)))
    ;; FIXME: Eliminate those functions which aren't referenced.
    (macroexp-let* (nreverse binds)
                   (macroexpand-all
                    `(progn ,@body)
                    ;; Don't override lexical-let's macro-expander.
                    (if (assq 'function newenv) newenv
                      (cons (cons 'function #'cl--labels-convert) newenv))))))

;;;###autoload
(defmacro cl-flet* (bindings &rest body)
  "Make local function definitions.
Like `cl-flet' but the definitions can refer to previous ones.

\(fn ((FUNC ARGLIST BODY...) ...) FORM...)"
  (declare (indent 1) (debug cl-flet))
  (cond
   ((null bindings) (macroexp-progn body))
   ((null (cdr bindings)) `(cl-flet ,bindings ,@body))
   (t `(cl-flet (,(pop bindings)) (cl-flet* ,bindings ,@body)))))

(defun cl--self-tco (var fargs body)
  ;; This tries to "optimize" tail calls for the specific case
  ;; of recursive self-calls by replacing them with a `while' loop.
  ;; It is quite far from a general tail-call optimization, since it doesn't
  ;; even handle mutually recursive functions.
  (letrec
      ((done nil) ;; Non-nil if some TCO happened.
       ;; This var always holds the value nil until (just before) we
       ;; exit the loop.
       (retvar (make-symbol "retval"))
       (ofargs (mapcar (lambda (s) (if (memq s cl--lambda-list-keywords) s
                                (make-symbol (symbol-name s))))
                       fargs))
       (opt-exps (lambda (exps) ;; `exps' is in tail position!
                   (append (butlast exps)
                           (list (funcall opt (car (last exps)))))))
       (opt
        (lambda (exp) ;; `exp' is in tail position!
          (pcase exp
            ;; FIXME: Optimize `apply'?
            (`(funcall ,(pred (eq var)) . ,aargs)
             ;; This is a self-recursive call in tail position.
             (let ((sets nil)
                   (fargs ofargs))
               (while fargs
                 (pcase (pop fargs)
                   ('&rest
                    (push (pop fargs) sets)
                    (push `(list . ,aargs) sets)
                    ;; (cl-assert (null fargs))
                    )
                   ('&optional nil)
                   (farg
                    (push farg sets)
                    (push (pop aargs) sets))))
               (setq done t)
               `(progn (setq . ,(nreverse sets))
                       :recurse)))
            (`(progn . ,exps) `(progn . ,(funcall opt-exps exps)))
            (`(if ,cond ,then . ,else)
             `(if ,cond ,(funcall opt then) . ,(funcall opt-exps else)))
            (`(and  . ,exps) `(and . ,(funcall opt-exps exps)))
            (`(or ,arg) (funcall opt arg))
            (`(or ,arg . ,args)
             (let ((val (make-symbol "val")))
               `(let ((,val ,arg))
                  (if ,val ,(funcall opt val) ,(funcall opt `(or . ,args))))))
            (`(cond . ,conds)
             (let ((cs '()))
               (while conds
                 (pcase (pop conds)
                   (`(,exp)
                    (push (if conds
                              ;; This returns the value of `exp' but it's
                              ;; only in tail position if it's the
                              ;; last condition.
                              ;; Note: This may set the var before we
                              ;; actually exit the loop, but luckily it's
                              ;; only the case if we set the var to nil,
                              ;; so it does preserve the invariant that
                              ;; the var is nil until we exit the loop.
                              `((setq ,retvar ,exp) nil)
                            `(,(funcall opt exp)))
                          cs))
                   (exps
                    (push (funcall opt-exps exps) cs))))
               ;; No need to set `retvar' to return nil.
               `(cond . ,(nreverse cs))))
            ((and `(,(or 'let 'let*) ,bindings . ,exps)
                  (guard
                   ;; Note: it's OK for this `let' to shadow any
                   ;; of the formal arguments since we will only
                   ;; setq the fresh new `ofargs' vars instead ;-)
                   (let ((shadowings
                          (mapcar (lambda (b) (if (consp b) (car b) b)) bindings)))
                     (and
                      ;; If `var' is shadowed, then it clearly can't be
                      ;; tail-called any more.
                      (not (memq var shadowings))
                      ;; If any of the new bindings is a dynamic
                      ;; variable, the body is not in tail position.
                      (not (delq nil (mapcar #'macroexp--dynamic-variable-p
                                             shadowings)))))))
             `(,(car exp) ,bindings . ,(funcall opt-exps exps)))
            ((and `(condition-case ,err-var ,bodyform . ,handlers)
                  (guard (not (eq err-var var))))
             `(condition-case ,err-var
                  ,(if (assq :success handlers)
                       bodyform
                     `(progn (setq ,retvar ,bodyform) nil))
                . ,(mapcar (lambda (h)
                             (cons (car h) (funcall opt-exps (cdr h))))
                           handlers)))
            ('nil nil)  ;No need to set `retvar' to return nil.
            (_ `(progn (setq ,retvar ,exp) nil))))))

    (let ((optimized-body (funcall opt-exps body)))
      (if (not done)
          (cons fargs body)
        ;; We use two sets of vars: `ofargs' and `fargs' because we need
        ;; to be careful that if a closure captures a formal argument
        ;; in one iteration, it needs to capture a different binding
        ;; then that of other iterations, e.g.
        (cons
         ofargs
         `((let (,retvar)
             (while (let ,(delq nil
                                (cl-mapcar
                                 (lambda (a oa)
                                   (unless (memq a cl--lambda-list-keywords)
                                     (list a oa)))
                                 fargs ofargs))
                      . ,optimized-body))
             ,retvar)))))))

;;;###autoload
(defmacro cl-labels (bindings &rest body)
  "Make local (recursive) function definitions.

Each definition can take the form (FUNC EXP) where FUNC is the function
name, and EXP is an expression that returns the function value to which
it should be bound, or it can take the more common form (FUNC ARGLIST
BODY...) which is a shorthand for (FUNC (lambda ARGLIST BODY)).

FUNC is defined in any BODY, as well as FORM, so you can write recursive
and mutually recursive function definitions.  See Info node
`(cl) Function Bindings' for details.

\(fn ((FUNC ARGLIST BODY...) ...) FORM...)"
  (declare (indent 1) (debug cl-flet))
  (let ((binds ()) (newenv macroexpand-all-environment))
    (dolist (binding bindings)
      (let ((var (make-symbol (format "--cl-%s--" (car binding)))))
	(push (cons var (cdr binding)) binds)
	(push (cons (car binding)
                    (lambda (&rest args)
                      (if (eq (car args) cl--labels-magic)
                          (list cl--labels-magic var)
                        (cl-list* 'funcall var args))))
              newenv)))
    ;; Don't override lexical-let's macro-expander.
    (unless (assq 'function newenv)
      (push (cons 'function #'cl--labels-convert) newenv))
    ;; Perform self-tail call elimination.
    (setq binds (mapcar
                 (lambda (bind)
                   (pcase-let*
                       ((`(,var ,sargs . ,sbody) bind)
                        (`(function (lambda ,fargs . ,ebody))
                         (macroexpand-all `(cl-function (lambda ,sargs . ,sbody))
                                          newenv))
                        (`(,ofargs . ,obody)
                         (cl--self-tco var fargs ebody)))
                     `(,var (function (lambda ,ofargs . ,obody)))))
                 (nreverse binds)))
    `(letrec ,binds
       . ,(macroexp-unprogn
           (macroexpand-all
            (macroexp-progn body)
            newenv)))))


;; From GNU Emacs 30.2 macroexp.el
(defun macroexp--fgrep (bindings sexp)
  "Return those of the BINDINGS which might be used in SEXP.
It is used as a poor-man's \"free variables\" test.  It differs from a true
test of free variables in the following ways:
- It does not distinguish variables from functions, so it can be used
  both to detect whether a given variable is used by SEXP and to
  detect whether a given function is used by SEXP.
- It does not actually know ELisp syntax, so it only looks for the presence
  of symbols in SEXP and can't distinguish if those symbols are truly
  references to the given variable (or function).  That can make the result
  include bindings which actually aren't used.
- For the same reason it may cause the result to fail to include bindings
  which will be used if SEXP is not yet fully macro-expanded and the
  use of the binding will only be revealed by macro expansion."
  (let ((res '())
        ;; Cyclic code should not happen, but code can contain cyclic data :-(
        (seen (make-hash-table :test #'eq))
        (sexpss (list (list sexp))))
    ;; Use a nested while loop to reduce the amount of heap allocations for
    ;; pushes to `sexpss' and the `gethash' overhead.
    (while (and sexpss bindings)
      (let ((sexps (pop sexpss)))
        (unless (gethash sexps seen)
          (puthash sexps t seen) ;; Using `setf' here causes bootstrap problems.
          (if (vectorp sexps) (setq sexps (mapcar #'identity sexps)))
          (let ((tortoise sexps) (skip t))
            (while sexps
              (let ((sexp (if (consp sexps) (pop sexps)
                            (prog1 sexps (setq sexps nil)))))
                (if skip
                    (setq skip nil)
                  (setq tortoise (cdr tortoise))
                  (if (eq tortoise sexps)
                      (setq sexps nil) ;; Found a cycle: we're done!
                    (setq skip t)))
                (cond
                 ((or (consp sexp) (vectorp sexp)) (push sexp sexpss))
                 (t
                  (let ((tmp (assq sexp bindings)))
                    (when tmp
                      (push tmp res)
                      (setq bindings (remove tmp bindings))))))))))))
    res))

;; From GNU Emacs 30.2 subr.el
(defmacro letrec (binders &rest body)
  "Bind variables according to BINDERS then eval BODY.
The value of the last form in BODY is returned.
Each element of BINDERS is a list (SYMBOL VALUEFORM) that binds
SYMBOL to the value of VALUEFORM.

The main difference between this macro and `let'/`let*' is that
all symbols are bound before any of the VALUEFORMs are evalled."
  ;; Useful only in lexical-binding mode.
  ;; As a special-form, we could implement it more efficiently (and cleanly,
  ;; making the vars actually unbound during evaluation of the binders).
  (declare (debug let) (indent 1))
  ;; Use plain `let*' for the non-recursive definitions.
  ;; This only handles the case where the first few definitions are not
  ;; recursive.  Nothing as fancy as an SCC analysis.
  (let ((seqbinds nil))
    ;; Our args haven't yet been macro-expanded, so `macroexp--fgrep'
    ;; may fail to see references that will be introduced later by
    ;; macroexpansion.  We could call `macroexpand-all' to avoid that,
    ;; but in order to avoid that, we instead check to see if the binders
    ;; appear in the macroexp environment, since that's how references can be
    ;; introduced later on.
    (unless (macroexp--fgrep binders macroexpand-all-environment)
      (while (and binders
                  (null (macroexp--fgrep binders (nth 1 (car binders)))))
        (push (pop binders) seqbinds)))
    (let ((nbody (if (null binders)
                     (macroexp-progn body)
                   `(let ,(mapcar #'car binders)
                      ,@(mapcan (lambda (binder)
                                  (and (cdr binder) (list `(setq ,@binder))))
                                binders)
                      ,@body))))
      (cond
       ;; All bindings are recursive.
       ((null seqbinds) nbody)
       ;; Special case for trivial uses.
       ((and (symbolp nbody) (null (cdr seqbinds)) (eq nbody (caar seqbinds)))
        (nth 1 (car seqbinds)))
       ;; General case.
       (t `(let* ,(nreverse seqbinds) ,nbody))))))

;; From GNU Emacs 30.2 cl-lib.el
(defun cl-list* (arg &rest rest)
  "Return a new list with specified ARGs as elements, consed to last ARG.
Thus, `(cl-list* A B C D)' is equivalent to `(nconc (list A B C) D)', or to
`(cons A (cons B (cons C D)))'.
\n(fn ARG...)"
  (declare (compiler-macro cl--compiler-macro-list*))
  (cond ((not rest) arg)
	((not (cdr rest)) (cons arg (car rest)))
	(t (let* ((n (length rest))
		  (copy (copy-sequence rest))
		  (last (nthcdr (- n 2) copy)))
	     (setcdr last (car (cdr last)))
	     (cons arg copy)))))

(defun cl-ldiff (list sublist)
  "Return a copy of LIST with the tail SUBLIST removed."
  (let ((res nil))
    (while (and (consp list) (not (eq list sublist)))
      (push (pop list) res))
    (nreverse res)))

(defun cl-copy-list (list)
  "Return a copy of LIST, which may be a dotted list.
The elements of LIST are not copied, just the list structure itself."
  (declare (side-effect-free error-free))
  (if (consp list)
      (let ((res nil))
	(while (consp list) (push (pop list) res))
	(prog1 (nreverse res) (setcdr res list)))
    (car list)))

;; From GNU Emacs 30.2 cl-macs.el / macroexp.el
(defvar byte-compile-const-variables nil)
(defconst cl--simple-funcs '(car cdr nth aref elt if and or + - 1+ 1- min max
			    car-safe cdr-safe progn prog1 prog2))

(defconst cl--safe-funcs '(* / % length memq list vector vectorp
			  < > <= >= = error))

(defun cl--safe-expr-p (x)
  "Check if no side effects."
  (or (not (and (consp x) (not (memq (car x) '(quote function cl-function)))))
      (and (symbolp (car x))
	   (or (memq (car x) cl--simple-funcs)
	       (memq (car x) cl--safe-funcs)
	       (get (car x) 'side-effect-free))
	   (progn
	     (while (and (setq x (cdr x)) (cl--safe-expr-p (car x))))
	     (null x)))))

(defsubst macroexp--const-symbol-p (symbol &optional any-value)
  "Non-nil if SYMBOL is constant.
If ANY-VALUE is nil, only return non-nil if the value of the symbol is the
symbol itself."
  (or (memq symbol '(nil t))
      (keywordp symbol)
      (if any-value
	  (or (memq symbol byte-compile-const-variables)
	      ;; FIXME: We should provide a less intrusive way to find out
	      ;; if a variable is "constant".
	      (and (boundp symbol)
		   (condition-case nil
		       (progn (set symbol (symbol-value symbol)) nil)
		     (setting-constant t)))))))

(defun macroexp--funcall-if-compiled (_form)
  "Pseudo function used internally by macroexp to delay warnings.
The purpose is to delay warnings to bytecomp.el, so they can use things
like `byte-compile-warn' to get better file-and-line-number data
and also to avoid outputting the warning during normal execution."
  nil)

;; Blocks and exits, from GNU Emacs 30.2 cl-lib.el / cl-macs.el
(defalias 'cl--block-wrapper 'identity)
(defalias 'cl--block-throw 'throw)
(defmacro cl-block (name &rest body)
  "Define a lexically-scoped block named NAME.
NAME may be any symbol.  Code inside the BODY forms can call `cl-return-from'
to jump prematurely out of the block.  This differs from `catch' and `throw'
in two respects:  First, the NAME is an unevaluated symbol rather than a
quoted symbol or other form; and second, NAME is lexically rather than
dynamically scoped:  Only references to it within BODY will work.  These
references may appear inside macro expansions, but not inside functions
called from BODY."
  (declare (indent 1) (debug (symbolp body)))
  (if (cl--safe-expr-p `(progn ,@body)) `(progn ,@body)
    `(cl--block-wrapper
      (catch ',(intern (format "--cl-block-%s--" name))
        ,@body))))

;; Support helpers from GNU Emacs 30.2 cl-macs/macroexp/cl-seq/cl-extra
(defun cl--const-expr-p (x)
  "Check if X is constant (i.e., no side effects or dependencies).

See `macroexp-const-p' for similar functionality without cl-lib dependency."
  (cond ((consp x)
	 (or (eq (car x) 'quote)
	     (and (memq (car x) '(function cl-function))
		  (or (symbolp (nth 1 x))
		      (and (eq (car-safe (nth 1 x)) 'lambda) 'func)))))
	((symbolp x) (and (memq x '(nil t)) t))
	(t t)))

(defun cl--const-expr-val (x)
  "Return the value of X known at compile-time.
If X is not known at compile time, return nil.  Before testing
whether X is known at compile time, macroexpand it completely in
`macroexpand-all-environment'."
  (let ((x (macroexpand-all x macroexpand-all-environment)))
    (if (macroexp-const-p x)
        (if (consp x) (nth 1 x) x))))

(defun macroexp-parse-body (body)
  "Parse a function BODY into (DECLARATIONS . EXPS)."
  (let ((decls ()))
    (while
        (and body
             (let ((e (car body)))
               (or (and (stringp e)
                        ;; If there is only a string literal with
                        ;; nothing following, we consider this to be
                        ;; part of the body (the return value) rather
                        ;; than a declaration at this point.
                        (cdr body))
                   (memq (car-safe e)
                         '(:documentation declare interactive cl-declare)))))
      (push (pop body) decls))
    (cons (nreverse decls) body)))

(defmacro cl--parsing-keywords (kwords other-keys &rest body)
  (declare (indent 2) (debug (sexp sexp &rest form)))
  `(let* ,(mapcar
           (lambda (x)
             (let* ((var (if (consp x) (car x) x))
                    (mem `(car (cdr (memq ',var cl-keys)))))
               (if (eq var :test-not)
                   (setq mem `(and ,mem (setq cl-test ,mem) t)))
               (if (eq var :if-not)
                   (setq mem `(and ,mem (setq cl-if ,mem) t)))
               (list (intern
                      (format "cl-%s" (substring (symbol-name var) 1)))
                     (if (consp x) `(or ,mem ,(car (cdr x))) mem))))
           kwords)
     ,@(append
        (and (not (eq other-keys t))
             (list
              (list 'let '((cl-keys-temp cl-keys))
                    (list 'while 'cl-keys-temp
                          (list 'or (list 'memq '(car cl-keys-temp)
                                          (list 'quote
                                                (mapcar
                                                 (lambda (x)
                                                   (if (consp x)
                                                       (car x) x))
                                                 (append kwords
                                                         other-keys))))
                                '(car (cdr (memq (quote :allow-other-keys)
                                                 cl-keys)))
                                '(error "Bad keyword argument %s"
                                        (car cl-keys-temp)))
                          '(setq cl-keys-temp (cdr (cdr cl-keys-temp)))))))
        body)))

(defmacro cl--check-key (x)     ;Expects `cl-key' in context of generated code.
  (declare (debug edebug-forms))
  `(if cl-key (funcall cl-key ,x) ,x))

(defmacro cl--check-test-nokey (item x) ;cl-test cl-if cl-test-not cl-if-not.
  (declare (debug edebug-forms))
  `(cond
    (cl-test (eq (not (funcall cl-test ,item ,x))
                 cl-test-not))
    (cl-if (eq (not (funcall cl-if ,x)) cl-if-not))
    (t (eql ,item ,x))))

(defmacro cl--check-test (item x)       ;all of the above.
  (declare (debug edebug-forms))
  `(cl--check-test-nokey ,item (cl--check-key ,x)))

(defun cl-position (cl-item cl-seq &rest cl-keys)
  "Find the first occurrence of ITEM in SEQ.
Return the index of the matching item, or nil if not found.
\nKeywords supported:  :test :test-not :key :start :end :from-end
\n(fn ITEM SEQ [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :if :if-not
			(:start 0) :end :from-end) ()
    (cl--position cl-item cl-seq cl-start cl-end cl-from-end)))

(defun cl--position (cl-item cl-seq cl-start &optional cl-end cl-from-end)
  (if (listp cl-seq)
      (let ((cl-p (nthcdr cl-start cl-seq))
	    cl-res)
	(while (and cl-p (or (null cl-end) (< cl-start cl-end)) (or (null cl-res) cl-from-end))
	    (if (cl--check-test cl-item (car cl-p))
		(setq cl-res cl-start))
	    (setq cl-p (cdr cl-p) cl-start (1+ cl-start)))
	cl-res)
    (or cl-end (setq cl-end (length cl-seq)))
    (if cl-from-end
	(progn
	  (while (and (>= (setq cl-end (1- cl-end)) cl-start)
		      (not (cl--check-test cl-item (aref cl-seq cl-end)))))
	  (and (>= cl-end cl-start) cl-end))
      (while (and (< cl-start cl-end)
		  (not (cl--check-test cl-item (aref cl-seq cl-start))))
	(setq cl-start (1+ cl-start)))
      (and (< cl-start cl-end) cl-start))))

(defun cl-subseq (seq start &optional end)
  "Return the subsequence of SEQ from START to END.
If END is omitted, it defaults to the length of the sequence.
If START or END is negative, it counts from the end.
Signal an error if START or END are outside of the sequence (i.e
too large if positive or too small if negative)."
  (declare (gv-setter
            (lambda (new)
              (macroexp-let2 nil new new
		`(progn (cl-replace ,seq ,new :start1 ,start :end1 ,end)
			,new)))))
  (seq-subseq seq start end))

(defconst cl--lambda-list-keywords
  '(&optional &rest &key &allow-other-keys &aux &whole &body &environment))

(defun macroexp-unprogn (exp)
  "Turn EXP into a list of expressions to execute in sequence.
Never returns an empty list."
  (if (eq (car-safe exp) 'progn) (or (cdr exp) '(nil)) (list exp)))

;; From GNU Emacs 30.2 cl-seq.el
(defun cl--defalias (cl-f el-f &optional doc)
  "Define function CL-F as definition EL-F.
Like `defalias' but marks the alias itself as inlinable."
  (defalias cl-f el-f doc)
  (put cl-f 'byte-optimizer 'byte-compile-inline-expand))

(cl--defalias 'cl-values #'list
  "Return multiple values, Common Lisp style.
The arguments of `cl-values' are the values
that the containing function should return.

\(fn &rest VALUES)")

(defun cl-values-list (list)
  "Return multiple values, Common Lisp style, taken from a list.
LIST specifies the list of values that the containing function
should return.

Note that Emacs Lisp doesn't really support multiple values, so
all this function does is return LIST."
  (unless (listp list)
    (signal 'wrong-type-argument (list list)))
  list)

(defsubst cl-multiple-value-list (expression)
  "Return a list of the multiple values produced by EXPRESSION.
This handles multiple values in Common Lisp style, but it does not
work right when EXPRESSION calls an ordinary Emacs Lisp function
that returns just one value."
  expression)

(defsubst cl-multiple-value-apply (function expression)
  "Evaluate EXPRESSION to get multiple values and apply FUNCTION to them.
This handles multiple values in Common Lisp style, but it does not work
right when EXPRESSION calls an ordinary Emacs Lisp function that returns just
one value."
  (apply function expression))

(defalias 'cl-multiple-value-call 'apply
  "Apply FUNCTION to ARGUMENTS, taking multiple values into account.
This implementation only handles the case where there is only one argument.")

(cl--defalias 'cl-nth-value #'nth
  "Evaluate EXPRESSION to get multiple values and return the Nth one.
This handles multiple values in Common Lisp style, but it does not work
right when EXPRESSION calls an ordinary Emacs Lisp function that returns just
one value.

\(fn N EXPRESSION)")

(defun cl-adjoin (cl-item cl-list &rest cl-keys)
  "Return ITEM consed onto the front of LIST only if it's not already there.
Otherwise, return LIST unmodified.
\nKeywords supported:  :test :test-not :key
\n(fn ITEM LIST [KEYWORD VALUE]...)"
  (declare (compiler-macro cl--compiler-macro-adjoin))
  (cond ((or (equal cl-keys '(:test eq))
	     (and (null cl-keys) (not (numberp cl-item))))
	 (if (memq cl-item cl-list) cl-list (cons cl-item cl-list)))
	((or (equal cl-keys '(:test equal)) (null cl-keys))
	 (if (member cl-item cl-list) cl-list (cons cl-item cl-list)))
	(t (apply 'cl--adjoin cl-item cl-list cl-keys))))

(defun cl-constantly (value)
  "Return a function that takes any number of arguments, but returns VALUE."
  (lambda (&rest _)
    value))

(defun cl--adjoin (cl-item cl-list &rest cl-keys)
  (if (cl--parsing-keywords (:key) t
	(apply 'cl-member (cl--check-key cl-item) cl-list cl-keys))
      cl-list
    (cons cl-item cl-list)))

(defmacro cl--check-match (x y)         ;cl-key cl-test cl-test-not
  (declare (debug edebug-forms))
  (setq x `(cl--check-key ,x) y `(cl--check-key ,y))
  `(if cl-test
       (eq (not (funcall cl-test ,x ,y)) cl-test-not)
     (eql ,x ,y)))

;; Yuck!  These vars are set/bound by cl--parsing-keywords to match :if :test
;; and :key keyword args, and they are also accessed (sometimes) via dynamic
;; scoping (and some of those accesses are from macro-expanded code).
(defvar cl-test) (defvar cl-test-not)
(defvar cl-if) (defvar cl-if-not)
(defvar cl-key)

;;;###autoload
(defun cl-endp (x)
  "Return true if X is the empty list; false if it is a cons.
Signal an error if X is not a list."
  (cl-check-type x list)
  (null x))

;;;###autoload
(defun cl-reduce (cl-func cl-seq &rest cl-keys)
  "Reduce two-argument FUNCTION across SEQ.
\nKeywords supported:  :start :end :from-end :initial-value :key

Return the result of calling FUNCTION with the first and the
second element of SEQ, then calling FUNCTION with that result and
the third element of SEQ, then with that result and the fourth
element of SEQ, etc.

If :INITIAL-VALUE is specified, it is logically added to the
front of SEQ (or the back if :FROM-END is non-nil).  If SEQ is
empty, return :INITIAL-VALUE and FUNCTION is not called.

If SEQ is empty and no :INITIAL-VALUE is specified, then return
the result of calling FUNCTION with zero arguments.  This is the
only case where FUNCTION is called with fewer than two arguments.

If SEQ contains exactly one element and no :INITIAL-VALUE is
specified, then return that element and FUNCTION is not called.

If :FROM-END is non-nil, the reduction occurs from the back of
the SEQ moving forward, and the order of arguments to the
FUNCTION is also reversed.

\n(fn FUNCTION SEQ [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:from-end (:start 0) :end :initial-value :key) ()
    (or (listp cl-seq) (setq cl-seq (append cl-seq nil)))
    (setq cl-seq (cl-subseq cl-seq cl-start cl-end))
    (if cl-from-end (setq cl-seq (nreverse cl-seq)))
    (let ((cl-accum (cond ((memq :initial-value cl-keys) cl-initial-value)
			  (cl-seq (cl--check-key (pop cl-seq)))
			  (t (funcall cl-func)))))
      (if cl-from-end
	  (while cl-seq
	    (setq cl-accum (funcall cl-func (cl--check-key (pop cl-seq))
				    cl-accum)))
	(while cl-seq
	  (setq cl-accum (funcall cl-func cl-accum
				  (cl--check-key (pop cl-seq))))))
      cl-accum)))

;;;###autoload
(defun cl-fill (cl-seq cl-item &rest cl-keys)
  "Fill the elements of SEQ with ITEM.
\nKeywords supported:  :start :end
\n(fn SEQ ITEM [KEYWORD VALUE]...)"
  (cl--parsing-keywords ((:start 0) :end) ()
    (if (listp cl-seq)
	(let ((p (nthcdr cl-start cl-seq))
	      (n (and cl-end (- cl-end cl-start))))
	  (while (and p (or (null n) (>= (cl-decf n) 0)))
	    (setcar p cl-item)
	    (setq p (cdr p))))
      (or cl-end (setq cl-end (length cl-seq)))
      (if (and (= cl-start 0) (= cl-end (length cl-seq)))
	  (fillarray cl-seq cl-item)
	(while (< cl-start cl-end)
	  (aset cl-seq cl-start cl-item)
	  (setq cl-start (1+ cl-start)))))
    cl-seq))

;;;###autoload
(defun cl-replace (cl-seq1 cl-seq2 &rest cl-keys)
  "Replace the elements of SEQ1 with the elements of SEQ2.
SEQ1 is destructively modified, then returned.
\nKeywords supported:  :start1 :end1 :start2 :end2
\n(fn SEQ1 SEQ2 [KEYWORD VALUE]...)"
  (cl--parsing-keywords ((:start1 0) :end1 (:start2 0) :end2) ()
    (if (and (eq cl-seq1 cl-seq2) (<= cl-start2 cl-start1))
	(or (= cl-start1 cl-start2)
	    (let* ((cl-len (length cl-seq1))
		   (cl-n (min (- (or cl-end1 cl-len) cl-start1)
			      (- (or cl-end2 cl-len) cl-start2))))
	      (while (>= (setq cl-n (1- cl-n)) 0)
		(setf (elt cl-seq1 (+ cl-start1 cl-n))
			    (elt cl-seq2 (+ cl-start2 cl-n))))))
      (if (listp cl-seq1)
	  (let ((cl-p1 (nthcdr cl-start1 cl-seq1))
		(cl-n1 (and cl-end1 (- cl-end1 cl-start1))))
	    (if (listp cl-seq2)
		(let ((cl-p2 (nthcdr cl-start2 cl-seq2))
		      (cl-n (cond ((and cl-n1 cl-end2)
				   (min cl-n1 (- cl-end2 cl-start2)))
				  ((and cl-n1 (null cl-end2)) cl-n1)
				  ((and (null cl-n1) cl-end2) (- cl-end2 cl-start2)))))
		  (while (and cl-p1 cl-p2 (or (null cl-n) (>= (cl-decf cl-n) 0)))
		    (setcar cl-p1 (car cl-p2))
		    (setq cl-p1 (cdr cl-p1) cl-p2 (cdr cl-p2))))
	      (setq cl-end2 (if (null cl-n1)
				(or cl-end2 (length cl-seq2))
			      (min (or cl-end2 (length cl-seq2))
				   (+ cl-start2 cl-n1))))
	      (while (and cl-p1 (< cl-start2 cl-end2))
		(setcar cl-p1 (aref cl-seq2 cl-start2))
		(setq cl-p1 (cdr cl-p1) cl-start2 (1+ cl-start2)))))
	(setq cl-end1 (min (or cl-end1 (length cl-seq1))
			   (+ cl-start1 (- (or cl-end2 (length cl-seq2))
					   cl-start2))))
	(if (listp cl-seq2)
	    (let ((cl-p2 (nthcdr cl-start2 cl-seq2)))
	      (while (< cl-start1 cl-end1)
		(aset cl-seq1 cl-start1 (car cl-p2))
		(setq cl-p2 (cdr cl-p2) cl-start1 (1+ cl-start1))))
	  (while (< cl-start1 cl-end1)
	    (aset cl-seq1 cl-start1 (aref cl-seq2 cl-start2))
	    (setq cl-start2 (1+ cl-start2) cl-start1 (1+ cl-start1))))))
    cl-seq1))

;;;###autoload
(defun cl-remove (cl-item cl-seq &rest cl-keys)
  "Remove all occurrences of ITEM in SEQ.
This is a non-destructive function; it makes a copy of SEQ if necessary
to avoid corrupting the original SEQ.
\nKeywords supported:  :test :test-not :key :count :start :end :from-end
\n(fn ITEM SEQ [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :if :if-not :count :from-end
			(:start 0) :end) ()
    (let ((len (length cl-seq)))
      (if (<= (or cl-count (setq cl-count len)) 0)
	cl-seq
        (if (or (nlistp cl-seq) (and cl-from-end (< cl-count (/ len 2))))
	  (let ((cl-i (cl--position cl-item cl-seq cl-start cl-end
                                    cl-from-end)))
	    (if cl-i
		(let ((cl-res (apply 'cl-delete cl-item (append cl-seq nil)
				     (append (if cl-from-end
						 (list :end (1+ cl-i))
					       (list :start cl-i))
					     cl-keys))))
		  (if (listp cl-seq) cl-res
		    (if (stringp cl-seq) (concat cl-res) (vconcat cl-res))))
	      cl-seq))
	  (setq cl-end (- (or cl-end len) cl-start))
	(if (= cl-start 0)
	    (while (and cl-seq (> cl-end 0)
			(cl--check-test cl-item (car cl-seq))
			(setq cl-end (1- cl-end) cl-seq (cdr cl-seq))
			(> (setq cl-count (1- cl-count)) 0))))
	(if (and (> cl-count 0) (> cl-end 0))
	    (let ((cl-p (if (> cl-start 0) (nthcdr cl-start cl-seq)
			  (setq cl-end (1- cl-end)) (cdr cl-seq))))
	      (while (and cl-p (> cl-end 0)
			  (not (cl--check-test cl-item (car cl-p))))
		(setq cl-p (cdr cl-p) cl-end (1- cl-end)))
	      (if (and cl-p (> cl-end 0))
		  (nconc (cl-ldiff cl-seq cl-p)
			 (if (= cl-count 1) (cdr cl-p)
			   (and (cdr cl-p)
				(apply 'cl-delete cl-item
				       (copy-sequence (cdr cl-p))
				       :start 0 :end (1- cl-end)
				       :count (1- cl-count) cl-keys))))
		cl-seq))
	  cl-seq))))))

;;;###autoload
(defun cl-remove-if (cl-pred cl-list &rest cl-keys)
  "Remove all items satisfying PREDICATE in SEQ.
This is a non-destructive function; it makes a copy of SEQ if necessary
to avoid corrupting the original SEQ.
\nKeywords supported:  :key :count :start :end :from-end
\n(fn PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-remove nil cl-list :if cl-pred cl-keys))

;;;###autoload
(defun cl-remove-if-not (cl-pred cl-list &rest cl-keys)
  "Remove all items not satisfying PREDICATE in SEQ.
This is a non-destructive function; it makes a copy of SEQ if necessary
to avoid corrupting the original SEQ.
\nKeywords supported:  :key :count :start :end :from-end
\n(fn PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-remove nil cl-list :if-not cl-pred cl-keys))

;;;###autoload
(defun cl-delete (cl-item cl-seq &rest cl-keys)
  "Remove all occurrences of ITEM in SEQ.
This is a destructive function; it reuses the storage of SEQ whenever possible.
\nKeywords supported:  :test :test-not :key :count :start :end :from-end
\n(fn ITEM SEQ [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :if :if-not :count :from-end
			(:start 0) :end) ()
    (let ((len (length cl-seq)))
      (if (<= (or cl-count (setq cl-count len)) 0)
	cl-seq
      (if (listp cl-seq)
	  (if (and cl-from-end (< cl-count (/ len 2)))
	      (let (cl-i)
		(while (and (>= (setq cl-count (1- cl-count)) 0)
			    (setq cl-i (cl--position cl-item cl-seq cl-start
						     cl-end cl-from-end)))
		  (if (= cl-i 0) (setq cl-seq (cdr cl-seq))
		    (let ((cl-tail (nthcdr (1- cl-i) cl-seq)))
		      (setcdr cl-tail (cdr (cdr cl-tail)))))
		  (setq cl-end cl-i))
		cl-seq)
	    (setq cl-end (- (or cl-end len) cl-start))
	    (if (= cl-start 0)
		(progn
		  (while (and cl-seq
			      (> cl-end 0)
			      (cl--check-test cl-item (car cl-seq))
			      (setq cl-end (1- cl-end) cl-seq (cdr cl-seq))
			      (> (setq cl-count (1- cl-count)) 0)))
		  (setq cl-end (1- cl-end)))
	      (setq cl-start (1- cl-start)))
	    (if (and (> cl-count 0) (> cl-end 0))
		(let ((cl-p (nthcdr cl-start cl-seq)))
		  (while (and (cdr cl-p) (> cl-end 0))
		    (if (cl--check-test cl-item (car (cdr cl-p)))
			(progn
			  (setcdr cl-p (cdr (cdr cl-p)))
			  (if (= (setq cl-count (1- cl-count)) 0)
			      (setq cl-end 1)))
		      (setq cl-p (cdr cl-p)))
		    (setq cl-end (1- cl-end)))))
	    cl-seq)
	(apply 'cl-remove cl-item cl-seq cl-keys))))))

;;;###autoload
(defun cl-delete-if (cl-pred cl-list &rest cl-keys)
  "Remove all items satisfying PREDICATE in SEQ.
This is a destructive function; it reuses the storage of SEQ whenever possible.
\nKeywords supported:  :key :count :start :end :from-end
\n(fn PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-delete nil cl-list :if cl-pred cl-keys))

;;;###autoload
(defun cl-delete-if-not (cl-pred cl-list &rest cl-keys)
  "Remove all items not satisfying PREDICATE in SEQ.
This is a destructive function; it reuses the storage of SEQ whenever possible.
\nKeywords supported:  :key :count :start :end :from-end
\n(fn PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-delete nil cl-list :if-not cl-pred cl-keys))

;;;###autoload
(defun cl-remove-duplicates (cl-seq &rest cl-keys)
  "Return a copy of SEQ with all duplicate elements removed.
\nKeywords supported:  :test :test-not :key :start :end :from-end
\n(fn SEQ [KEYWORD VALUE]...)"
  (cl--delete-duplicates cl-seq cl-keys t))

;;;###autoload
(defun cl-delete-duplicates (cl-seq &rest cl-keys)
  "Remove all duplicate elements from SEQ (destructively).
\nKeywords supported:  :test :test-not :key :start :end :from-end
\n(fn SEQ [KEYWORD VALUE]...)"
  (cl--delete-duplicates cl-seq cl-keys nil))

(defun cl--delete-duplicates (cl-seq cl-keys cl-copy)
  (if (listp cl-seq)
      (cl--parsing-keywords
          ;; We need to parse :if, otherwise `cl-if' is unbound.
          (:test :test-not :key (:start 0) :end :from-end :if)
	  ()
	(if cl-from-end
	    (let ((cl-p (nthcdr cl-start cl-seq)) cl-i)
	      (setq cl-end (- (or cl-end (length cl-seq)) cl-start))
	      (while (> cl-end 1)
		(setq cl-i 0)
		(while (setq cl-i (cl--position (cl--check-key (car cl-p))
                                                (cdr cl-p) cl-i (1- cl-end)))
		  (if cl-copy (setq cl-seq (copy-sequence cl-seq)
				    cl-p (nthcdr cl-start cl-seq) cl-copy nil))
		  (let ((cl-tail (nthcdr cl-i cl-p)))
		    (setcdr cl-tail (cdr (cdr cl-tail))))
		  (setq cl-end (1- cl-end)))
		(setq cl-p (cdr cl-p) cl-end (1- cl-end)
		      cl-start (1+ cl-start)))
	      cl-seq)
	  (setq cl-end (- (or cl-end (length cl-seq)) cl-start))
	  (while (and (cdr cl-seq) (= cl-start 0) (> cl-end 1)
		      (cl--position (cl--check-key (car cl-seq))
                                    (cdr cl-seq) 0 (1- cl-end)))
	    (setq cl-seq (cdr cl-seq) cl-end (1- cl-end)))
	  (let ((cl-p (if (> cl-start 0) (nthcdr (1- cl-start) cl-seq)
			(setq cl-end (1- cl-end) cl-start 1) cl-seq)))
	    (while (and (cdr (cdr cl-p)) (> cl-end 1))
	      (if (cl--position (cl--check-key (car (cdr cl-p)))
                                (cdr (cdr cl-p)) 0 (1- cl-end))
		  (progn
		    (if cl-copy (setq cl-seq (copy-sequence cl-seq)
				      cl-p (nthcdr (1- cl-start) cl-seq)
				      cl-copy nil))
		    (setcdr cl-p (cdr (cdr cl-p))))
		(setq cl-p (cdr cl-p)))
	      (setq cl-end (1- cl-end) cl-start (1+ cl-start)))
	    cl-seq)))
    (let ((cl-res (cl--delete-duplicates (append cl-seq nil) cl-keys nil)))
      (if (stringp cl-seq) (concat cl-res) (vconcat cl-res)))))

;;;###autoload
(defun cl-substitute (cl-new cl-old cl-seq &rest cl-keys)
  "Substitute NEW for OLD in SEQ.
This is a non-destructive function; it makes a copy of SEQ if necessary
to avoid corrupting the original SEQ.
\nKeywords supported:  :test :test-not :key :count :start :end :from-end
\n(fn NEW OLD SEQ [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :if :if-not :count
			(:start 0) :end :from-end) ()
    (if (or (eq cl-old cl-new)
	    (<= (or cl-count (setq cl-from-end nil
				   cl-count (length cl-seq))) 0))
	cl-seq
      (let ((cl-i (cl--position cl-old cl-seq cl-start cl-end)))
	(if (not cl-i)
	    cl-seq
	  (setq cl-seq (copy-sequence cl-seq))
	  (unless cl-from-end
	    (setf (elt cl-seq cl-i) cl-new)
	    (cl-incf cl-i)
	    (cl-decf cl-count))
	  (apply 'cl-nsubstitute cl-new cl-old cl-seq :count cl-count
		 :start cl-i cl-keys))))))

;;;###autoload
(defun cl-substitute-if (cl-new cl-pred cl-list &rest cl-keys)
  "Substitute NEW for all items satisfying PREDICATE in SEQ.
This is a non-destructive function; it makes a copy of SEQ if necessary
to avoid corrupting the original SEQ.
\nKeywords supported:  :key :count :start :end :from-end
\n(fn NEW PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-substitute cl-new nil cl-list :if cl-pred cl-keys))

;;;###autoload
(defun cl-substitute-if-not (cl-new cl-pred cl-list &rest cl-keys)
  "Substitute NEW for all items not satisfying PREDICATE in SEQ.
This is a non-destructive function; it makes a copy of SEQ if necessary
to avoid corrupting the original SEQ.
\nKeywords supported:  :key :count :start :end :from-end
\n(fn NEW PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-substitute cl-new nil cl-list :if-not cl-pred cl-keys))

;;;###autoload
(defun cl-nsubstitute (cl-new cl-old seq &rest cl-keys)
  "Substitute NEW for OLD in SEQ.
This is a destructive function; it reuses the storage of SEQ whenever possible.
\nKeywords supported:  :test :test-not :key :count :start :end :from-end
\n(fn NEW OLD SEQ [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :if :if-not :count
			(:start 0) :end :from-end) ()
    (let* ((cl-seq (if (stringp seq) (string-to-vector seq) seq))
           (len (length cl-seq)))
      (or (eq cl-old cl-new) (<= (or cl-count (setq cl-count len)) 0)
	  (if (and (listp cl-seq) (or (not cl-from-end) (> cl-count (/ len 2))))
	    (let ((cl-p (nthcdr cl-start cl-seq)))
	      (setq cl-end (- (or cl-end len) cl-start))
	      (while (and cl-p (> cl-end 0) (> cl-count 0))
		(if (cl--check-test cl-old (car cl-p))
		    (progn
		      (setcar cl-p cl-new)
		      (setq cl-count (1- cl-count))))
		(setq cl-p (cdr cl-p) cl-end (1- cl-end))))
	    (or cl-end (setq cl-end len))
	  (if cl-from-end
	      (while (and (< cl-start cl-end) (> cl-count 0))
		(setq cl-end (1- cl-end))
		(if (cl--check-test cl-old (elt cl-seq cl-end))
		    (progn
		      (setf (elt cl-seq cl-end) cl-new)
		      (setq cl-count (1- cl-count)))))
	    (while (and (< cl-start cl-end) (> cl-count 0))
	      (if (cl--check-test cl-old (aref cl-seq cl-start))
		  (progn
		    (aset cl-seq cl-start cl-new)
		    (setq cl-count (1- cl-count))))
	      (setq cl-start (1+ cl-start))))))
      (if (stringp seq) (concat cl-seq) cl-seq))))

;;;###autoload
(defun cl-nsubstitute-if (cl-new cl-pred cl-list &rest cl-keys)
  "Substitute NEW for all items satisfying PREDICATE in SEQ.
This is a destructive function; it reuses the storage of SEQ whenever possible.
\nKeywords supported:  :key :count :start :end :from-end
\n(fn NEW PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-nsubstitute cl-new nil cl-list :if cl-pred cl-keys))

;;;###autoload
(defun cl-nsubstitute-if-not (cl-new cl-pred cl-list &rest cl-keys)
  "Substitute NEW for all items not satisfying PREDICATE in SEQ.
This is a destructive function; it reuses the storage of SEQ whenever possible.
\nKeywords supported:  :key :count :start :end :from-end
\n(fn NEW PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-nsubstitute cl-new nil cl-list :if-not cl-pred cl-keys))

;;;###autoload
(defun cl-find (cl-item cl-seq &rest cl-keys)
  "Find the first occurrence of ITEM in SEQ.
Return the matching ITEM, or nil if not found.
\nKeywords supported:  :test :test-not :key :start :end :from-end
\n(fn ITEM SEQ [KEYWORD VALUE]...)"
  (let ((cl-pos (apply 'cl-position cl-item cl-seq cl-keys)))
    (and cl-pos (elt cl-seq cl-pos))))

;;;###autoload
(defun cl-find-if (cl-pred cl-list &rest cl-keys)
  "Find the first item satisfying PREDICATE in SEQ.
Return the matching item, or nil if not found.
\nKeywords supported:  :key :start :end :from-end
\n(fn PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-find nil cl-list :if cl-pred cl-keys))

;;;###autoload
(defun cl-find-if-not (cl-pred cl-list &rest cl-keys)
  "Find the first item not satisfying PREDICATE in SEQ.
Return the matching item, or nil if not found.
\nKeywords supported:  :key :start :end :from-end
\n(fn PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-find nil cl-list :if-not cl-pred cl-keys))

;;;###autoload
(defun cl-position (cl-item cl-seq &rest cl-keys)
  "Find the first occurrence of ITEM in SEQ.
Return the index of the matching item, or nil if not found.
\nKeywords supported:  :test :test-not :key :start :end :from-end
\n(fn ITEM SEQ [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :if :if-not
			(:start 0) :end :from-end) ()
    (cl--position cl-item cl-seq cl-start cl-end cl-from-end)))

(defun cl--position (cl-item cl-seq cl-start &optional cl-end cl-from-end)
  (if (listp cl-seq)
      (let ((cl-p (nthcdr cl-start cl-seq))
	    cl-res)
	(while (and cl-p (or (null cl-end) (< cl-start cl-end)) (or (null cl-res) cl-from-end))
	    (if (cl--check-test cl-item (car cl-p))
		(setq cl-res cl-start))
	    (setq cl-p (cdr cl-p) cl-start (1+ cl-start)))
	cl-res)
    (or cl-end (setq cl-end (length cl-seq)))
    (if cl-from-end
	(progn
	  (while (and (>= (setq cl-end (1- cl-end)) cl-start)
		      (not (cl--check-test cl-item (aref cl-seq cl-end)))))
	  (and (>= cl-end cl-start) cl-end))
      (while (and (< cl-start cl-end)
		  (not (cl--check-test cl-item (aref cl-seq cl-start))))
	(setq cl-start (1+ cl-start)))
      (and (< cl-start cl-end) cl-start))))

;;;###autoload
(defun cl-position-if (cl-pred cl-list &rest cl-keys)
  "Find the first item satisfying PREDICATE in SEQ.
Return the index of the matching item, or nil if not found.
\nKeywords supported:  :key :start :end :from-end
\n(fn PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-position nil cl-list :if cl-pred cl-keys))

;;;###autoload
(defun cl-position-if-not (cl-pred cl-list &rest cl-keys)
  "Find the first item not satisfying PREDICATE in SEQ.
Return the index of the matching item, or nil if not found.
\nKeywords supported:  :key :start :end :from-end
\n(fn PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-position nil cl-list :if-not cl-pred cl-keys))

;;;###autoload
(defun cl-count (cl-item cl-seq &rest cl-keys)
  "Count the number of occurrences of ITEM in SEQ.
\nKeywords supported:  :test :test-not :key :start :end
\n(fn ITEM SEQ [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :if :if-not (:start 0) :end) ()
    (let ((cl-count 0) cl-x)
      (or cl-end (setq cl-end (length cl-seq)))
      (if (consp cl-seq) (setq cl-seq (nthcdr cl-start cl-seq)))
      (while (< cl-start cl-end)
	(setq cl-x (if (consp cl-seq) (pop cl-seq) (aref cl-seq cl-start)))
	(if (cl--check-test cl-item cl-x) (setq cl-count (1+ cl-count)))
	(setq cl-start (1+ cl-start)))
      cl-count)))

;;;###autoload
(defun cl-count-if (cl-pred cl-list &rest cl-keys)
  "Count the number of items satisfying PREDICATE in SEQ.
\nKeywords supported:  :key :start :end
\n(fn PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-count nil cl-list :if cl-pred cl-keys))

;;;###autoload
(defun cl-count-if-not (cl-pred cl-list &rest cl-keys)
  "Count the number of items not satisfying PREDICATE in SEQ.
\nKeywords supported:  :key :start :end
\n(fn PREDICATE SEQ [KEYWORD VALUE]...)"
  (apply 'cl-count nil cl-list :if-not cl-pred cl-keys))

;;;###autoload
(defun cl-mismatch (cl-seq1 cl-seq2 &rest cl-keys)
  "Compare SEQ1 with SEQ2, return index of first mismatching element.
Return nil if the sequences match.  If one sequence is a prefix of the
other, the return value indicates the end of the shorter sequence.
\nKeywords supported:  :test :test-not :key :start1 :end1 :start2 :end2 :from-end
\n(fn SEQ1 SEQ2 [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :from-end
			(:start1 0) :end1 (:start2 0) :end2) ()
    (or cl-end1 (setq cl-end1 (length cl-seq1)))
    (or cl-end2 (setq cl-end2 (length cl-seq2)))
    (if cl-from-end
	(progn
	  (while (and (< cl-start1 cl-end1) (< cl-start2 cl-end2)
		      (cl--check-match (elt cl-seq1 (1- cl-end1))
				      (elt cl-seq2 (1- cl-end2))))
	    (setq cl-end1 (1- cl-end1) cl-end2 (1- cl-end2)))
	  (and (or (< cl-start1 cl-end1) (< cl-start2 cl-end2))
	       (1- cl-end1)))
      (let ((cl-p1 (and (listp cl-seq1) (nthcdr cl-start1 cl-seq1)))
	    (cl-p2 (and (listp cl-seq2) (nthcdr cl-start2 cl-seq2))))
	(while (and (< cl-start1 cl-end1) (< cl-start2 cl-end2)
		    (cl--check-match (if cl-p1 (car cl-p1)
				      (aref cl-seq1 cl-start1))
				    (if cl-p2 (car cl-p2)
				      (aref cl-seq2 cl-start2))))
	  (setq cl-p1 (cdr cl-p1) cl-p2 (cdr cl-p2)
		cl-start1 (1+ cl-start1) cl-start2 (1+ cl-start2)))
	(and (or (< cl-start1 cl-end1) (< cl-start2 cl-end2))
	     cl-start1)))))

;;;###autoload
(defun cl-search (cl-seq1 cl-seq2 &rest cl-keys)
  "Search for SEQ1 as a subsequence of SEQ2.
Return the index of the leftmost element of the first match found;
return nil if there are no matches.
\nKeywords supported:  :test :test-not :key :start1 :end1 :start2 :end2 :from-end
\n(fn SEQ1 SEQ2 [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :from-end
			(:start1 0) :end1 (:start2 0) :end2) ()
    (or cl-end1 (setq cl-end1 (length cl-seq1)))
    (or cl-end2 (setq cl-end2 (length cl-seq2)))
    (if (>= cl-start1 cl-end1)
	(if cl-from-end cl-end2 cl-start2)
      (let* ((cl-len (- cl-end1 cl-start1))
	     (cl-first (cl--check-key (elt cl-seq1 cl-start1)))
	     (cl-if nil) cl-pos)
	(setq cl-end2 (- cl-end2 (1- cl-len)))
	(while (and (< cl-start2 cl-end2)
		    (setq cl-pos (cl--position cl-first cl-seq2
                                               cl-start2 cl-end2 cl-from-end))
		    (apply 'cl-mismatch cl-seq1 cl-seq2
			   :start1 (1+ cl-start1) :end1 cl-end1
			   :start2 (1+ cl-pos) :end2 (+ cl-pos cl-len)
			   :from-end nil cl-keys))
	  (if cl-from-end (setq cl-end2 cl-pos) (setq cl-start2 (1+ cl-pos))))
	(and (< cl-start2 cl-end2) cl-pos)))))

;;;###autoload
(defun cl-sort (cl-seq cl-pred &rest cl-keys)
  "Sort the argument SEQ according to PREDICATE.
This is a destructive function; it reuses the storage of SEQ if possible.
\nKeywords supported:  :key
\n(fn SEQ PREDICATE [KEYWORD VALUE]...)"
  (if (nlistp cl-seq)
      (if (stringp cl-seq)
          (concat (apply #'cl-sort (vconcat cl-seq) cl-pred cl-keys))
        (cl-replace cl-seq
                    (apply #'cl-sort (append cl-seq nil) cl-pred cl-keys)))
    (cl--parsing-keywords (:key) ()
      (if (memq cl-key '(nil identity))
	  (sort cl-seq cl-pred)
        (sort cl-seq (lambda (cl-x cl-y)
                       (funcall cl-pred (funcall cl-key cl-x)
                                (funcall cl-key cl-y))))))))

;;;###autoload
(defun cl-stable-sort (cl-seq cl-pred &rest cl-keys)
  "Sort the argument SEQ stably according to PREDICATE.
This is a destructive function; it reuses the storage of SEQ if possible.
\nKeywords supported:  :key
\n(fn SEQ PREDICATE [KEYWORD VALUE]...)"
  (apply 'cl-sort cl-seq cl-pred cl-keys))

;;;###autoload
(defun cl-merge (cl-type cl-seq1 cl-seq2 cl-pred &rest cl-keys)
  "Destructively merge the two sequences to produce a new sequence.
TYPE is the sequence type to return, SEQ1 and SEQ2 are the two argument
sequences, and PREDICATE is a `less-than' predicate on the elements.
\nKeywords supported:  :key
\n(fn TYPE SEQ1 SEQ2 PREDICATE [KEYWORD VALUE]...)"
  (or (listp cl-seq1) (setq cl-seq1 (append cl-seq1 nil)))
  (or (listp cl-seq2) (setq cl-seq2 (append cl-seq2 nil)))
  (cl--parsing-keywords (:key) ()
    (let ((cl-res nil))
      (while (and cl-seq1 cl-seq2)
	(if (funcall cl-pred (cl--check-key (car cl-seq2))
		     (cl--check-key (car cl-seq1)))
	    (push (pop cl-seq2) cl-res)
	  (push (pop cl-seq1) cl-res)))
      (cl-coerce (nconc (nreverse cl-res) cl-seq1 cl-seq2) cl-type))))

;;;###autoload
(defun cl-member (cl-item cl-list &rest cl-keys)
  "Find the first occurrence of ITEM in LIST.
Return the sublist of LIST whose car is ITEM.
\nKeywords supported:  :test :test-not :key
\n(fn ITEM LIST [KEYWORD VALUE]...)"
  (declare (compiler-macro cl--compiler-macro-member))
  (if cl-keys
      (cl--parsing-keywords (:test :test-not :key :if :if-not) ()
	(while (and cl-list (not (cl--check-test cl-item (car cl-list))))
	  (setq cl-list (cdr cl-list)))
	cl-list)
    (memql cl-item cl-list)))
(autoload 'cl--compiler-macro-member "cl-macs")

;;;###autoload
(defun cl-member-if (cl-pred cl-list &rest cl-keys)
  "Find the first item satisfying PREDICATE in LIST.
Return the sublist of LIST whose car matches.
\nKeywords supported:  :key
\n(fn PREDICATE LIST [KEYWORD VALUE]...)"
  (apply 'cl-member nil cl-list :if cl-pred cl-keys))

;;;###autoload
(defun cl-member-if-not (cl-pred cl-list &rest cl-keys)
  "Find the first item not satisfying PREDICATE in LIST.
Return the sublist of LIST whose car matches.
\nKeywords supported:  :key
\n(fn PREDICATE LIST [KEYWORD VALUE]...)"
  (apply 'cl-member nil cl-list :if-not cl-pred cl-keys))

;;;###autoload
(defun cl--adjoin (cl-item cl-list &rest cl-keys)
  (if (cl--parsing-keywords (:key) t
	(apply 'cl-member (cl--check-key cl-item) cl-list cl-keys))
      cl-list
    (cons cl-item cl-list)))

;;;###autoload
(defun cl-assoc (cl-item cl-alist &rest cl-keys)
  "Find the first item whose car matches ITEM in LIST.
\nKeywords supported:  :test :test-not :key
\n(fn ITEM LIST [KEYWORD VALUE]...)"
  (declare (compiler-macro cl--compiler-macro-assoc))
  (if cl-keys
      (cl--parsing-keywords (:test :test-not :key :if :if-not) ()
	(while (and cl-alist
		    (or (not (consp (car cl-alist)))
			(not (cl--check-test cl-item (car (car cl-alist))))))
	  (setq cl-alist (cdr cl-alist)))
	(and cl-alist (car cl-alist)))
    (if (and (numberp cl-item) (not (fixnump cl-item)))
	(assoc cl-item cl-alist)
      (assq cl-item cl-alist))))
(autoload 'cl--compiler-macro-assoc "cl-macs")

;;;###autoload
(defun cl-assoc-if (cl-pred cl-list &rest cl-keys)
  "Find the first item whose car satisfies PREDICATE in LIST.
\nKeywords supported:  :key
\n(fn PREDICATE LIST [KEYWORD VALUE]...)"
  (apply 'cl-assoc nil cl-list :if cl-pred cl-keys))

;;;###autoload
(defun cl-assoc-if-not (cl-pred cl-list &rest cl-keys)
  "Find the first item whose car does not satisfy PREDICATE in LIST.
\nKeywords supported:  :key
\n(fn PREDICATE LIST [KEYWORD VALUE]...)"
  (apply 'cl-assoc nil cl-list :if-not cl-pred cl-keys))

;;;###autoload
(defun cl-rassoc (cl-item cl-alist &rest cl-keys)
  "Find the first item whose cdr matches ITEM in LIST.
\nKeywords supported:  :test :test-not :key
\n(fn ITEM LIST [KEYWORD VALUE]...)"
  (if (or cl-keys (numberp cl-item))
      (cl--parsing-keywords (:test :test-not :key :if :if-not) ()
	(while (and cl-alist
		    (or (not (consp (car cl-alist)))
			(not (cl--check-test cl-item (cdr (car cl-alist))))))
	  (setq cl-alist (cdr cl-alist)))
	(and cl-alist (car cl-alist)))
    (rassq cl-item cl-alist)))

;;;###autoload
(defun cl-rassoc-if (cl-pred cl-list &rest cl-keys)
  "Find the first item whose cdr satisfies PREDICATE in LIST.
\nKeywords supported:  :key
\n(fn PREDICATE LIST [KEYWORD VALUE]...)"
  (apply 'cl-rassoc nil cl-list :if cl-pred cl-keys))

;;;###autoload
(defun cl-rassoc-if-not (cl-pred cl-list &rest cl-keys)
  "Find the first item whose cdr does not satisfy PREDICATE in LIST.
\nKeywords supported:  :key
\n(fn PREDICATE LIST [KEYWORD VALUE]...)"
  (apply 'cl-rassoc nil cl-list :if-not cl-pred cl-keys))

;;;###autoload
(defun cl-union (cl-list1 cl-list2 &rest cl-keys)
  "Combine LIST1 and LIST2 using a set-union operation.
The resulting list contains all items that appear in either LIST1 or LIST2.
This is a non-destructive function; it makes a copy of the data if necessary
to avoid corrupting the original LIST1 and LIST2.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (cond ((null cl-list1) cl-list2) ((null cl-list2) cl-list1)
	((and (not cl-keys) (equal cl-list1 cl-list2)) cl-list1)
	(t
	 (or (>= (length cl-list1) (length cl-list2))
	     (setq cl-list1 (prog1 cl-list2 (setq cl-list2 cl-list1))))
	 (while cl-list2
	   (if (or cl-keys (numberp (car cl-list2)))
	       (setq cl-list1
                     (apply 'cl-adjoin (car cl-list2) cl-list1 cl-keys))
	     (or (memq (car cl-list2) cl-list1)
		 (push (car cl-list2) cl-list1)))
	   (pop cl-list2))
	 cl-list1)))

;;;###autoload
(defun cl-nunion (cl-list1 cl-list2 &rest cl-keys)
  "Combine LIST1 and LIST2 using a set-union operation.
The resulting list contains all items that appear in either LIST1 or LIST2.
This is a destructive function; it reuses the storage of LIST1 and LIST2
whenever possible.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (cond ((null cl-list1) cl-list2) ((null cl-list2) cl-list1)
	(t (apply 'cl-union cl-list1 cl-list2 cl-keys))))

;;;###autoload
(defun cl-intersection (cl-list1 cl-list2 &rest cl-keys)
  "Combine LIST1 and LIST2 using a set-intersection operation.
The resulting list contains all items that appear in both LIST1 and LIST2.
This is a non-destructive function; it makes a copy of the data if necessary
to avoid corrupting the original LIST1 and LIST2.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (and cl-list1 cl-list2
       (if (equal cl-list1 cl-list2) cl-list1
	 (cl--parsing-keywords (:key) (:test :test-not)
	   (let ((cl-res nil))
	     (or (>= (length cl-list1) (length cl-list2))
		 (setq cl-list1 (prog1 cl-list2 (setq cl-list2 cl-list1))))
	     (while cl-list2
	       (if (if (or cl-keys (numberp (car cl-list2)))
		       (apply 'cl-member (cl--check-key (car cl-list2))
			      cl-list1 cl-keys)
		     (memq (car cl-list2) cl-list1))
		   (push (car cl-list2) cl-res))
	       (pop cl-list2))
	     cl-res)))))

;;;###autoload
(defun cl-nintersection (cl-list1 cl-list2 &rest cl-keys)
  "Combine LIST1 and LIST2 using a set-intersection operation.
The resulting list contains all items that appear in both LIST1 and LIST2.
This is a destructive function; it reuses the storage of LIST1 and LIST2
whenever possible.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (and cl-list1 cl-list2 (apply 'cl-intersection cl-list1 cl-list2 cl-keys)))

;;;###autoload
(defun cl-set-difference (cl-list1 cl-list2 &rest cl-keys)
  "Combine LIST1 and LIST2 using a set-difference operation.
The resulting list contains all items that appear in LIST1 but not LIST2.
This is a non-destructive function; it makes a copy of the data if necessary
to avoid corrupting the original LIST1 and LIST2.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (if (or (null cl-list1) (null cl-list2)) cl-list1
    (cl--parsing-keywords (:key) (:test :test-not)
      (let ((cl-res nil))
	(while cl-list1
	  (or (if (or cl-keys (numberp (car cl-list1)))
		  (apply 'cl-member (cl--check-key (car cl-list1))
			 cl-list2 cl-keys)
		(memq (car cl-list1) cl-list2))
	      (push (car cl-list1) cl-res))
	  (pop cl-list1))
        (nreverse cl-res)))))

;;;###autoload
(defun cl-nset-difference (cl-list1 cl-list2 &rest cl-keys)
  "Combine LIST1 and LIST2 using a set-difference operation.
The resulting list contains all items that appear in LIST1 but not LIST2.
This is a destructive function; it reuses the storage of LIST1 and LIST2
whenever possible.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (if (or (null cl-list1) (null cl-list2)) cl-list1
    (apply 'cl-set-difference cl-list1 cl-list2 cl-keys)))

;;;###autoload
(defun cl-set-exclusive-or (cl-list1 cl-list2 &rest cl-keys)
  "Combine LIST1 and LIST2 using a set-exclusive-or operation.
The resulting list contains all items appearing in exactly one of LIST1, LIST2.
This is a non-destructive function; it makes a copy of the data if necessary
to avoid corrupting the original LIST1 and LIST2.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (cond ((null cl-list1) cl-list2) ((null cl-list2) cl-list1)
	((equal cl-list1 cl-list2) nil)
	(t (append (apply 'cl-set-difference cl-list1 cl-list2 cl-keys)
		   (apply 'cl-set-difference cl-list2 cl-list1 cl-keys)))))

;;;###autoload
(defun cl-nset-exclusive-or (cl-list1 cl-list2 &rest cl-keys)
  "Combine LIST1 and LIST2 using a set-exclusive-or operation.
The resulting list contains all items appearing in exactly one of LIST1, LIST2.
This is a destructive function; it reuses the storage of LIST1 and LIST2
whenever possible.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (cond ((null cl-list1) cl-list2) ((null cl-list2) cl-list1)
	((equal cl-list1 cl-list2) nil)
	(t (nconc (apply 'cl-nset-difference cl-list1 cl-list2 cl-keys)
		  (apply 'cl-nset-difference cl-list2 cl-list1 cl-keys)))))

;;;###autoload
(defun cl-subsetp (cl-list1 cl-list2 &rest cl-keys)
  "Return true if LIST1 is a subset of LIST2.
I.e., if every element of LIST1 also appears in LIST2.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (cond ((null cl-list1) t) ((null cl-list2) nil)
	((equal cl-list1 cl-list2) t)
	(t (cl--parsing-keywords (:key) (:test :test-not)
	     (while (and cl-list1
			 (apply 'cl-member (cl--check-key (car cl-list1))
				cl-list2 cl-keys))
	       (pop cl-list1))
	     (null cl-list1)))))

;;;###autoload
(defun cl-subst-if (cl-new cl-pred cl-tree &rest cl-keys)
  "Substitute NEW for elements matching PREDICATE in TREE (non-destructively).
Return a copy of TREE with all matching elements replaced by NEW.
\nKeywords supported:  :key
\n(fn NEW PREDICATE TREE [KEYWORD VALUE]...)"
  (apply 'cl-sublis (list (cons nil cl-new)) cl-tree :if cl-pred cl-keys))

;;;###autoload
(defun cl-subst-if-not (cl-new cl-pred cl-tree &rest cl-keys)
  "Substitute NEW for elts not matching PREDICATE in TREE (non-destructively).
Return a copy of TREE with all non-matching elements replaced by NEW.
\nKeywords supported:  :key
\n(fn NEW PREDICATE TREE [KEYWORD VALUE]...)"
  (apply 'cl-sublis (list (cons nil cl-new)) cl-tree :if-not cl-pred cl-keys))

;;;###autoload
(defun cl-nsubst (cl-new cl-old cl-tree &rest cl-keys)
  "Substitute NEW for OLD everywhere in TREE (destructively).
Any element of TREE which is `eql' to OLD is changed to NEW (via a call
to `setcar').
\nKeywords supported:  :test :test-not :key
\n(fn NEW OLD TREE [KEYWORD VALUE]...)"
  (apply 'cl-nsublis (list (cons cl-old cl-new)) cl-tree cl-keys))

;;;###autoload
(defun cl-nsubst-if (cl-new cl-pred cl-tree &rest cl-keys)
  "Substitute NEW for elements matching PREDICATE in TREE (destructively).
Any element of TREE which matches is changed to NEW (via a call to `setcar').
\nKeywords supported:  :key
\n(fn NEW PREDICATE TREE [KEYWORD VALUE]...)"
  (apply 'cl-nsublis (list (cons nil cl-new)) cl-tree :if cl-pred cl-keys))

;;;###autoload
(defun cl-nsubst-if-not (cl-new cl-pred cl-tree &rest cl-keys)
  "Substitute NEW for elements not matching PREDICATE in TREE (destructively).
Any element of TREE which matches is changed to NEW (via a call to `setcar').
\nKeywords supported:  :key
\n(fn NEW PREDICATE TREE [KEYWORD VALUE]...)"
  (apply 'cl-nsublis (list (cons nil cl-new)) cl-tree :if-not cl-pred cl-keys))

(defun cl-delete (cl-item cl-seq &rest cl-keys)
  "Remove all occurrences of ITEM in SEQ.
This is a destructive function; it reuses the storage of SEQ whenever possible.
\nKeywords supported:  :test :test-not :key :count :start :end :from-end
\n(fn ITEM SEQ [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :if :if-not :count :from-end
			(:start 0) :end) ()
    (let ((len (length cl-seq)))
      (if (<= (or cl-count (setq cl-count len)) 0)
	cl-seq
      (if (listp cl-seq)
	  (if (and cl-from-end (< cl-count (/ len 2)))
	      (let (cl-i)
		(while (and (>= (setq cl-count (1- cl-count)) 0)
			    (setq cl-i (cl--position cl-item cl-seq cl-start
						     cl-end cl-from-end)))
		  (if (= cl-i 0) (setq cl-seq (cdr cl-seq))
		    (let ((cl-tail (nthcdr (1- cl-i) cl-seq)))
		      (setcdr cl-tail (cdr (cdr cl-tail)))))
		  (setq cl-end cl-i))
		cl-seq)
	    (setq cl-end (- (or cl-end len) cl-start))
	    (if (= cl-start 0)
		(progn
		  (while (and cl-seq
			      (> cl-end 0)
			      (cl--check-test cl-item (car cl-seq))
			      (setq cl-end (1- cl-end) cl-seq (cdr cl-seq))
			      (> (setq cl-count (1- cl-count)) 0)))
		  (setq cl-end (1- cl-end)))
	      (setq cl-start (1- cl-start)))
	    (if (and (> cl-count 0) (> cl-end 0))
		(let ((cl-p (nthcdr cl-start cl-seq)))
		  (while (and (cdr cl-p) (> cl-end 0))
		    (if (cl--check-test cl-item (car (cdr cl-p)))
			(progn
			  (setcdr cl-p (cdr (cdr cl-p)))
			  (if (= (setq cl-count (1- cl-count)) 0)
			      (setq cl-end 1)))
		      (setq cl-p (cdr cl-p)))
		    (setq cl-end (1- cl-end)))))
	    cl-seq)
	(apply 'cl-remove cl-item cl-seq cl-keys))))))

(defun cl-remove (cl-item cl-seq &rest cl-keys)
  "Remove all occurrences of ITEM in SEQ.
This is a non-destructive function; it makes a copy of SEQ if necessary
to avoid corrupting the original SEQ.
\nKeywords supported:  :test :test-not :key :count :start :end :from-end
\n(fn ITEM SEQ [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :if :if-not :count :from-end
			(:start 0) :end) ()
    (let ((len (length cl-seq)))
      (if (<= (or cl-count (setq cl-count len)) 0)
	cl-seq
        (if (or (nlistp cl-seq) (and cl-from-end (< cl-count (/ len 2))))
	  (let ((cl-i (cl--position cl-item cl-seq cl-start cl-end
                                    cl-from-end)))
	    (if cl-i
		(let ((cl-res (apply 'cl-delete cl-item (append cl-seq nil)
				     (append (if cl-from-end
						 (list :end (1+ cl-i))
					       (list :start cl-i))
					     cl-keys))))
		  (if (listp cl-seq) cl-res
		    (if (stringp cl-seq) (concat cl-res) (vconcat cl-res))))
	      cl-seq))
	  (setq cl-end (- (or cl-end len) cl-start))
	(if (= cl-start 0)
	    (while (and cl-seq (> cl-end 0)
			(cl--check-test cl-item (car cl-seq))
			(setq cl-end (1- cl-end) cl-seq (cdr cl-seq))
			(> (setq cl-count (1- cl-count)) 0))))
	(if (and (> cl-count 0) (> cl-end 0))
	    (let ((cl-p (if (> cl-start 0) (nthcdr cl-start cl-seq)
			  (setq cl-end (1- cl-end)) (cdr cl-seq))))
	      (while (and cl-p (> cl-end 0)
			  (not (cl--check-test cl-item (car cl-p))))
		(setq cl-p (cdr cl-p) cl-end (1- cl-end)))
	      (if (and cl-p (> cl-end 0))
		  (nconc (cl-ldiff cl-seq cl-p)
			 (if (= cl-count 1) (cdr cl-p)
			   (and (cdr cl-p)
				(apply 'cl-delete cl-item
				       (copy-sequence (cdr cl-p))
				       :start 0 :end (1- cl-end)
				       :count (1- cl-count) cl-keys))))
		cl-seq))
	  cl-seq))))))

(defun cl-mismatch (cl-seq1 cl-seq2 &rest cl-keys)
  "Compare SEQ1 with SEQ2, return index of first mismatching element.
Return nil if the sequences match.  If one sequence is a prefix of the
other, the return value indicates the end of the shorter sequence.
\nKeywords supported:  :test :test-not :key :start1 :end1 :start2 :end2 :from-end
\n(fn SEQ1 SEQ2 [KEYWORD VALUE]...)"
  (cl--parsing-keywords (:test :test-not :key :from-end
			(:start1 0) :end1 (:start2 0) :end2) ()
    (or cl-end1 (setq cl-end1 (length cl-seq1)))
    (or cl-end2 (setq cl-end2 (length cl-seq2)))
    (if cl-from-end
	(progn
	  (while (and (< cl-start1 cl-end1) (< cl-start2 cl-end2)
		      (cl--check-match (elt cl-seq1 (1- cl-end1))
				      (elt cl-seq2 (1- cl-end2))))
	    (setq cl-end1 (1- cl-end1) cl-end2 (1- cl-end2)))
	  (and (or (< cl-start1 cl-end1) (< cl-start2 cl-end2))
	       (1- cl-end1)))
      (let ((cl-p1 (and (listp cl-seq1) (nthcdr cl-start1 cl-seq1)))
	    (cl-p2 (and (listp cl-seq2) (nthcdr cl-start2 cl-seq2))))
	(while (and (< cl-start1 cl-end1) (< cl-start2 cl-end2)
		    (cl--check-match (if cl-p1 (car cl-p1)
				      (aref cl-seq1 cl-start1))
				    (if cl-p2 (car cl-p2)
				      (aref cl-seq2 cl-start2))))
	  (setq cl-p1 (cdr cl-p1) cl-p2 (cdr cl-p2)
		cl-start1 (1+ cl-start1) cl-start2 (1+ cl-start2)))
	(and (or (< cl-start1 cl-end1) (< cl-start2 cl-end2))
	     cl-start1)))))

(defun cl-coerce (x type)
  "Coerce OBJECT to type TYPE.
TYPE is a Common Lisp type specifier.
\n(fn OBJECT TYPE)"
  (cond ((eq type 'list) (if (listp x) x (append x nil)))
	((eq type 'vector) (if (vectorp x) x (vconcat x)))
	((eq type 'bool-vector)
         (if (bool-vector-p x) x (apply #'bool-vector (cl-coerce x 'list))))
	((eq type 'string) (if (stringp x) x (concat x)))
	((eq type 'array) (if (arrayp x) x (vconcat x)))
	((and (eq type 'character) (stringp x) (= (length x) 1)) (aref x 0))
	((and (eq type 'character) (symbolp x))
         (cl-coerce (symbol-name x) type))
	((eq type 'float) (float x))
	((cl-typep x type) x)
	(t (error "Can't coerce %s to type %s" x type))))

(defsubst cl-plusp (number)
  "Return t if NUMBER is positive."
  (> number 0))

(defsubst cl-minusp (number)
  "Return t if NUMBER is negative."
  (< number 0))

(defconst cl-digit-char-table
  (let* ((digits (make-vector 256 nil))
         (populate (lambda (start end base)
                     (mapc (lambda (i)
                             (aset digits i (+ base (- i start))))
                           (number-sequence start end)))))
    (funcall populate ?0 ?9 0)
    (funcall populate ?A ?Z 10)
    (funcall populate ?a ?z 10)
    digits))

(defun cl-digit-char-p (char &optional radix)
  "Test if CHAR is a digit in the specified RADIX (default 10).
If true return the decimal value of digit CHAR in RADIX."
  (or (<= 2 (or radix 10) 36)
      (signal 'args-out-of-range (list 'radix radix '(2 36))))
  (let ((n (aref cl-digit-char-table char)))
    (and n (< n (or radix 10)) n)))

(defun cl--do-remf (plist tag)
  (let ((p (cdr plist)))
    ;; Can't use `plist-member' here because it goes to the cons-cell
    ;; of TAG and we need the one before.
    (while (and (cdr p) (not (eq (car (cdr p)) tag))) (setq p (cdr (cdr p))))
    (and (cdr p) (progn (setcdr p (cdr (cdr (cdr p)))) t))))

(defun cl-remprop (symbol propname)
  "Remove from SYMBOL's plist the property PROPNAME and its value."
  ;; emaxx materializes `symbol-plist' on each call, so the destructive
  ;; `cl--do-remf' must be followed by a `setplist' write-back (a no-op
  ;; in GNU Emacs where the returned plist is the live object).
  (let ((plist (symbol-plist symbol)))
    (if (and plist (eq propname (car plist)))
	(progn (setplist symbol (cdr (cdr plist))) t)
      (and (cl--do-remf plist propname)
           (progn (setplist symbol plist) t)))))

(defun cl-set-difference (cl-list1 cl-list2 &rest cl-keys)
  "Combine LIST1 and LIST2 using a set-difference operation.
The resulting list contains all items that appear in LIST1 but not LIST2.
This is a non-destructive function; it makes a copy of the data if necessary
to avoid corrupting the original LIST1 and LIST2.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (if (or (null cl-list1) (null cl-list2)) cl-list1
    (cl--parsing-keywords (:key) (:test :test-not)
      (let ((cl-res nil))
	(while cl-list1
	  (or (if (or cl-keys (numberp (car cl-list1)))
		  (apply 'cl-member (cl--check-key (car cl-list1))
			 cl-list2 cl-keys)
		(memq (car cl-list1) cl-list2))
	      (push (car cl-list1) cl-res))
	  (pop cl-list1))
        (nreverse cl-res)))))

(defun cl-intersection (cl-list1 cl-list2 &rest cl-keys)
  "Combine LIST1 and LIST2 using a set-intersection operation.
The resulting list contains all items that appear in both LIST1 and LIST2.
This is a non-destructive function; it makes a copy of the data if necessary
to avoid corrupting the original LIST1 and LIST2.
\nKeywords supported:  :test :test-not :key
\n(fn LIST1 LIST2 [KEYWORD VALUE]...)"
  (and cl-list1 cl-list2
       (if (equal cl-list1 cl-list2) cl-list1
	 (cl--parsing-keywords (:key) (:test :test-not)
	   (let ((cl-res nil))
	     (or (>= (length cl-list1) (length cl-list2))
		 (setq cl-list1 (prog1 cl-list2 (setq cl-list2 cl-list1))))
	     (while cl-list2
	       (if (if (or cl-keys (numberp (car cl-list2)))
		       (apply 'cl-member (cl--check-key (car cl-list2))
			      cl-list1 cl-keys)
		     (memq (car cl-list2) cl-list1))
		   (push (car cl-list2) cl-res))
	       (pop cl-list2))
	     cl-res)))))

;; cl--transform-lambda machinery, from GNU Emacs 30.2 cl-macs.el
(defvar cl--bind-block) ;Name of surrounding block, only use for `signal' data.
(defvar cl--bind-defs) ;(DEF . DEFS) giving the "default default" for optargs.
(defvar cl--bind-enquote)      ;Non-nil if &cl-quote was in the formal arglist!
(defvar cl--bind-lets) (defvar cl--bind-forms)

(defun cl--slet (bindings body &optional nowarn)
  "Like `cl--slet*' but for \"parallel let\"."
  (let ((dyns nil)) ;Vars declared as dynbound among the bindings?
    (when lexical-binding
      (dolist (binding bindings) ;; `seq-some' lead to bootstrap problems.
        (when (macroexp--dynamic-variable-p (car binding))
          (push (car binding) dyns))))
    (cond
     (dyns
      (let ((form `(funcall (lambda (,@(mapcar #'car bindings))
                              ,@(macroexp-unprogn body))
                            ,@(mapcar #'cadr bindings))))
        (if (not nowarn) form
          `(with-suppressed-warnings ((lexical ,@dyns)) ,form))))
     ((null (cdr bindings))
      (macroexp-let* bindings body))
     (t `(let ,bindings ,@(macroexp-unprogn body))))))

(defun cl--slet* (bindings body)
  "Like `macroexp-let*' but uses static scoping for all the BINDINGS."
  (if (null bindings) body
    (cl--slet `(,(car bindings)) (cl--slet* (cdr bindings) body))))

(defun cl--transform-lambda (form bind-block)
  "Transform a function form FORM of name BIND-BLOCK.
BIND-BLOCK is the name of the symbol to which the function will be bound,
and which will be used for the name of the `cl-block' surrounding the
function's body.
FORM is of the form (ARGS . BODY)."
  (let* ((args (car form)) (body (cdr form)) (orig-args args)
	 (cl--bind-block bind-block) (cl--bind-defs nil) (cl--bind-enquote nil)
         (parsed-body (macroexp-parse-body body))
	 (header (car parsed-body)) (simple-args nil))
    (setq body (cdr parsed-body))
    ;; "(. X) to (&rest X)" conversion already done in cl--do-arglist, but we
    ;; do it here as well, so as to be able to see if we can avoid
    ;; cl--do-arglist.
    (setq args (if (listp args) (cl-copy-list args) (list '&rest args)))
    (let ((p (last args))) (if (cdr p) (setcdr p (list '&rest (cdr p)))))
    (let ((cl-defs (memq '&cl-defs args)))
      (when cl-defs
        (setq cl--bind-defs (cadr cl-defs))
	;; Remove "&cl-defs DEFS" from args.
        (setcdr cl-defs (cddr cl-defs))
	(setq args (delq '&cl-defs args))))
    (if (setq cl--bind-enquote (memq '&cl-quote args))
	(setq args (delq '&cl-quote args)))
    (if (memq '&whole args) (error "&whole not currently implemented"))
    (let* ((p (memq '&environment args))
           (v (cadr p)))
      (if p (setq args (nconc (delq (car p) (delq v args))
                              `(&aux (,v macroexpand-all-environment))))))
    ;; Take away all the simple args whose parsing can be handled more
    ;; efficiently by a plain old `lambda' than the manual parsing generated
    ;; by `cl--do-arglist'.
    (let ((optional nil))
      (while (and args (symbolp (car args))
                  (not (memq (car args) '(nil &rest &body &key &aux)))
                  (or (not optional)
                      ;; Optional args whose default is nil are simple.
                      (null (nth 1 (assq (car args) (cdr cl--bind-defs)))))
                  (not (and (eq (car args) '&optional) (setq optional t)
                            (car cl--bind-defs))))
        (push (pop args) simple-args))
      (when optional
        (if args (push '&optional args))
        ;; Don't keep a dummy trailing &optional without actual optional args.
        (if (eq '&optional (car simple-args)) (pop simple-args))))
    (or (eq cl--bind-block 'cl-none)
	(setq body (list `(cl-block ,cl--bind-block ,@body))))
    (let* ((cl--bind-lets nil) (cl--bind-forms nil)
           (rest-args
            (cond
             ((null args) nil)
             ((eq (car args) '&aux)
              (cl--do-&aux args)
              (setq cl--bind-lets (nreverse cl--bind-lets))
              nil)
             (t ;; `simple-args' doesn't handle all the parsing that we need,
              ;; so we pass the rest to cl--do-arglist which will do
              ;; "manual" parsing.
              (let ((slen (length simple-args))
                    (usage-str
                      ;; Macro expansion can take place in the middle of
                      ;; apparently harmless computation, so it should not
                      ;; touch the match-data.
                      (save-match-data
                        (help--docstring-quote
                         (let ((print-gensym nil) (print-quoted t)
                               (print-escape-newlines t))
                           (format "%S" (cons 'fn (cl--make-usage-args
                                                   orig-args))))))))
                (when (memq '&optional simple-args)
                  (cl-decf slen))
                (setq header
                      (cons
                       (if (eq :documentation (car-safe (car header)))
                           `(:documentation (help-add-fundoc-usage
                                             ,(cadr (pop header))
                                             ,usage-str))
                         (help-add-fundoc-usage
                          (if (stringp (car header)) (pop header))
                          ;; Be careful with make-symbol and (back)quote,
                          ;; see bug#12884.
                          usage-str))
                       header))
                ;; FIXME: we'd want to choose an arg name for the &rest param
                ;; and pass that as `expr' to cl--do-arglist, but that ends up
                ;; generating code with a redundant let-binding, so we instead
                ;; pass a dummy and then look in cl--bind-lets to find what var
                ;; this was bound to.
                (cl--do-arglist args :dummy slen)
                (setq cl--bind-lets (nreverse cl--bind-lets))
                ;; (cl-assert (eq :dummy (nth 1 (car cl--bind-lets))))
                (list '&rest (car (pop cl--bind-lets))))))))
      `((,@(nreverse simple-args) ,@rest-args)
        ,@header
        ;; Function arguments are unconditionally statically scoped (bug#47552).
        ,(cl--slet* cl--bind-lets
                    (macroexp-progn
                     `(,@(nreverse cl--bind-forms)
                       ,@body)))))))

(defmacro cl-function (func)
  "Introduce a function.
Like normal `function', except that if argument is a lambda form,
its argument list allows full Common Lisp conventions."
  (declare (debug (&or symbolp cl-lambda-expr)))
  (if (eq (car-safe func) 'lambda)
      `(function (lambda . ,(cl--transform-lambda (cdr func) 'cl-none)))
    `(function ,func)))

(defun cl--make-usage-var (x)
  "X can be a var or a (destructuring) lambda-list."
  (cond
   ((symbolp x) (make-symbol (upcase (symbol-name x))))
   ((consp x) (cl--make-usage-args x))
   (t x)))

(defun cl--make-usage-args (arglist)
  (let ((aux (ignore-errors (cl-position '&aux arglist))))
    (when aux
      ;; `&aux' args aren't arguments, so let's just drop them from the
      ;; usage info.
      (setq arglist (cl-subseq arglist 0 aux))))
  (if (not (proper-list-p arglist))
      (let* ((last (last arglist))
             (tail (cdr last)))
        (unwind-protect
            (progn
              (setcdr last nil)
              (nconc (cl--make-usage-args arglist) (cl--make-usage-var tail)))
          (setcdr last tail)))
    ;; `orig-args' can contain &cl-defs.
    (let ((x (memq '&cl-defs arglist)))
      (when x (setq arglist (delq (car x) (remq (cadr x) arglist)))))
    (let ((state nil))
      (mapcar (lambda (x)
                (cond
                 ((symbolp x)
                  (let ((first (aref (symbol-name x) 0)))
                    (if (eq ?\& first)
                        (setq state x)
                      ;; Strip a leading underscore, since it only
                      ;; means that this argument is unused.
                      (make-symbol (upcase (if (eq ?_ first)
                                               (substring (symbol-name x) 1)
                                             (symbol-name x)))))))
                 ((not (consp x)) x)
                 ((memq state '(nil &rest)) (cl--make-usage-args x))
                 (t      ;(VAR INITFORM SVAR) or ((KEYWORD VAR) INITFORM SVAR).
                  (cl-list*
                   (if (and (consp (car x)) (eq state '&key))
                       (list (caar x) (cl--make-usage-var (nth 1 (car x))))
                     (cl--make-usage-var (car x)))
                   (nth 1 x)                        ;INITFORM.
                   (cl--make-usage-args (nthcdr 2 x)) ;SVAR.
                   ))))
              arglist))))

(defun cl--do-&aux (args)
  (while (and (eq (car args) '&aux) (pop args))
    (while (and args (not (memq (car args) cl--lambda-list-keywords)))
      (if (consp (car args))
          (if (and cl--bind-enquote (cadar args))
              (cl--do-arglist (caar args)
                              `',(cadr (pop args)))
            (cl--do-arglist (caar args) (cadr (pop args))))
        (cl--do-arglist (pop args) nil))))
  (if args (error "Malformed argument list ends with: %S" args)))

(defun cl--do-arglist (args expr &optional num)   ; uses cl--bind-*
  (if (nlistp args)
      (if (or (memq args cl--lambda-list-keywords) (not (symbolp args)))
	  (error "Invalid argument name: %s" args)
	(push (list args expr) cl--bind-lets))
    (setq args (cl-copy-list args))
    (let ((p (last args))) (if (cdr p) (setcdr p (list '&rest (cdr p)))))
    (let ((p (memq '&body args))) (if p (setcar p '&rest)))
    (if (memq '&environment args) (error "&environment used incorrectly"))
    (let ((restarg (memq '&rest args))
	  (safety (if (macroexp-compiling-p) cl--optimize-safety 3))
	  (keys t)
	  (laterarg nil) (exactarg nil) minarg)
      (or num (setq num 0))
      (setq restarg (if (listp (cadr restarg))
                        (make-symbol "--cl-rest--")
                      (cadr restarg)))
      (push (list restarg expr) cl--bind-lets)
      (if (eq (car args) '&whole)
	  (push (list (cl--pop2 args) restarg) cl--bind-lets))
      (let ((p args))
	(setq minarg restarg)
	(while (and p (not (memq (car p) cl--lambda-list-keywords)))
	  (or (eq p args) (setq minarg (list 'cdr minarg)))
	  (setq p (cdr p)))
	(if (memq (car p) '(nil &aux))
	    (setq minarg `(= (length ,restarg)
                             ,(length (cl-ldiff args p)))
		  exactarg (not (eq args p)))))
      (while (and args (not (memq (car args) cl--lambda-list-keywords)))
	(let ((poparg (list (if (or (cdr args) (not exactarg)) 'pop 'car-safe)
			    restarg)))
	  (cl--do-arglist
	   (pop args)
	   (if (or laterarg (= safety 0)) poparg
	     `(if ,minarg ,poparg
                (signal 'wrong-number-of-arguments
                        (list ,(and (not (eq cl--bind-block 'cl-none))
                                    `',cl--bind-block)
                              (length ,restarg)))))))
	(setq num (1+ num) laterarg t))
      (while (and (eq (car args) '&optional) (pop args))
	(while (and args (not (memq (car args) cl--lambda-list-keywords)))
	  (let ((arg (pop args)))
	    (or (consp arg) (setq arg (list arg)))
	    (if (cddr arg) (cl--do-arglist (nth 2 arg) `(and ,restarg t)))
	    (let ((def (if (cdr arg) (nth 1 arg)
			 (or (car cl--bind-defs)
			     (nth 1 (assq (car arg) cl--bind-defs)))))
		  (poparg `(pop ,restarg)))
	      (and def cl--bind-enquote (setq def `',def))
	      (cl--do-arglist (car arg)
			     (if def `(if ,restarg ,poparg ,def) poparg))
	      (setq num (1+ num))))))
      (if (eq (car args) '&rest)
	  (let ((arg (cl--pop2 args)))
	    (if (consp arg) (cl--do-arglist arg restarg)))
	(or (eq (car args) '&key) (= safety 0) exactarg
	    (push `(if ,restarg
                       (signal 'wrong-number-of-arguments
                               (list
                                ,(and (not (eq cl--bind-block 'cl-none))
                                      `',cl--bind-block)
                                (+ ,num (length ,restarg)))))
                  cl--bind-forms)))
      (while (and (eq (car args) '&key) (pop args))
        (unless (listp keys) (setq keys nil))
	(while (and args (not (memq (car args) cl--lambda-list-keywords)))
	  (let ((arg (pop args)))
	    (or (consp arg) (setq arg (list arg)))
	    (let* ((karg (if (consp (car arg)) (caar arg)
                           (let ((name (symbol-name (car arg))))
                             ;; Strip a leading underscore, since it only
                             ;; means that this argument is unused, but
                             ;; shouldn't affect the key's name (bug#12367).
                             (if (eq ?_ (aref name 0))
                                 (setq name (substring name 1)))
                             (intern (format ":%s" name)))))
                   (varg (if (consp (car arg)) (cadar arg) (car arg)))
		   (def (if (cdr arg) (cadr arg)
                          ;; The ordering between those two or clauses is
                          ;; irrelevant, since in practice only one of the two
                          ;; is ever non-nil (the car is only used for
                          ;; cl-deftype which doesn't use the cdr).
			  (or (car cl--bind-defs)
                              (cadr (assq varg cl--bind-defs)))))
                   (look `(plist-member ,restarg ',karg)))
	      (and def cl--bind-enquote (setq def `',def))
	      (if (cddr arg)
		  (let* ((temp (or (nth 2 arg) (make-symbol "--cl-var--")))
			 (val `(car (cdr ,temp))))
		    (cl--do-arglist temp look)
		    (cl--do-arglist varg
				   `(if ,temp
                                        (prog1 ,val (setq ,temp t))
                                      ,def)))
		(cl--do-arglist
		 varg
		 `(car (cdr ,(if (null def)
				 look
			       `(or ,look
                                    ,(if (eq (cl--const-expr-p def) t)
					 `'(nil ,(cl--const-expr-val def))
				       `(list nil ,def))))))))
	      (push karg keys)))))
      (when (consp keys) (setq keys (nreverse keys)))
      (or (and (eq (car args) '&allow-other-keys) (pop args))
	  (= safety 0)
          (cond
           ((eq keys t) nil)            ;No &keys at all
           ((null keys)                 ;A &key but no actual keys specified.
            (push `(when ,restarg
                     (error ,(format "Keyword argument %%s not one of %s"
                                     keys)
                            (car ,restarg)))
                  cl--bind-forms))
           (t
	    (let* ((var (make-symbol "--cl-keys--"))
		   (allow '(:allow-other-keys))
		   (check `(while ,var
                             (cond
                              ((memq (car ,var) ',(append keys allow))
                               (unless (cdr ,var)
                                 (error "Missing argument for %s" (car ,var)))
                               (setq ,var (cdr (cdr ,var))))
                              ((car (cdr (memq (quote ,@allow) ,restarg)))
                               (setq ,var nil))
                              (t
                               (error
                                ,(format "Keyword argument %%s not one of %s"
                                         keys)
                                (car ,var)))))))
	      (push `(let ((,var ,restarg)) ,check) cl--bind-forms)))))
      (cl--do-&aux args)
      nil)))

(defun cl--arglist-args (args)
  (if (nlistp args) (list args)
    (let ((res nil) (kind nil) arg)
      (while (consp args)
	(setq arg (pop args))
	(if (memq arg cl--lambda-list-keywords) (setq kind arg)
	  (if (eq arg '&cl-defs) (pop args)
	    (and (consp arg) kind (setq arg (car arg)))
	    (and (consp arg) (cdr arg) (eq kind '&key) (setq arg (cadr arg)))
	    (setq res (nconc res (cl--arglist-args arg))))))
      (nconc res (and args (list args))))))

;; Standard error hierarchy, mirroring GNU Emacs 30.2 C bootstrap
;; (src/data.c, src/fileio.c, src/search.c syms_of_* sections).
(put 'error 'error-conditions '(error))
(put 'error 'error-message "error")
(put 'quit 'error-conditions '(quit))
(put 'quit 'error-message "Quit")
(put 'minibuffer-quit 'error-conditions '(minibuffer-quit quit))
(put 'minibuffer-quit 'error-message "Quit")
(put 'user-error 'error-conditions '(user-error error))
(put 'user-error 'error-message "")
(put 'wrong-length-argument 'error-conditions '(wrong-length-argument error))
(put 'wrong-length-argument 'error-message "Wrong length argument")
(put 'wrong-type-argument 'error-conditions '(wrong-type-argument error))
(put 'wrong-type-argument 'error-message "Wrong type argument")
(put 'type-mismatch 'error-conditions '(type-mismatch error))
(put 'type-mismatch 'error-message "Types do not match")
(put 'args-out-of-range 'error-conditions '(args-out-of-range error))
(put 'args-out-of-range 'error-message "Args out of range")
(put 'void-function 'error-conditions '(void-function error))
(put 'void-function 'error-message "Symbol's function definition is void")
(put 'cyclic-function-indirection 'error-conditions
     '(cyclic-function-indirection error))
(put 'cyclic-function-indirection 'error-message
     "Symbol's chain of function indirections contains a loop")
(put 'cyclic-variable-indirection 'error-conditions
     '(cyclic-variable-indirection error))
(put 'cyclic-variable-indirection 'error-message
     "Symbol's chain of variable indirections contains a loop")
(put 'circular-list 'error-conditions '(circular-list error))
(put 'circular-list 'error-message "List contains a loop")
(put 'void-variable 'error-conditions '(void-variable error))
(put 'void-variable 'error-message "Symbol's value as variable is void")
(put 'setting-constant 'error-conditions '(setting-constant error))
(put 'setting-constant 'error-message "Attempt to set a constant symbol")
(put 'trapping-constant 'error-conditions '(trapping-constant error))
(put 'trapping-constant 'error-message
     "Attempt to trap writes to a constant symbol")
(put 'invalid-read-syntax 'error-conditions '(invalid-read-syntax error))
(put 'invalid-read-syntax 'error-message "Invalid read syntax")
(put 'invalid-function 'error-conditions '(invalid-function error))
(put 'invalid-function 'error-message "Invalid function")
(put 'wrong-number-of-arguments 'error-conditions
     '(wrong-number-of-arguments error))
(put 'wrong-number-of-arguments 'error-message "Wrong number of arguments")
(put 'no-catch 'error-conditions '(no-catch error))
(put 'no-catch 'error-message "No catch for tag")
(put 'end-of-file 'error-conditions '(end-of-file error))
(put 'end-of-file 'error-message "End of file during parsing")
(put 'arith-error 'error-conditions '(arith-error error))
(put 'arith-error 'error-message "Arithmetic error")
(put 'beginning-of-buffer 'error-conditions '(beginning-of-buffer error))
(put 'beginning-of-buffer 'error-message "Beginning of buffer")
(put 'end-of-buffer 'error-conditions '(end-of-buffer error))
(put 'end-of-buffer 'error-message "End of buffer")
(put 'buffer-read-only 'error-conditions '(buffer-read-only error))
(put 'buffer-read-only 'error-message "Buffer is read-only")
(put 'text-read-only 'error-conditions '(text-read-only buffer-read-only error))
(put 'text-read-only 'error-message "Text is read-only")
(put 'inhibited-interaction 'error-conditions '(inhibited-interaction error))
(put 'inhibited-interaction 'error-message "User interaction while inhibited")
(put 'domain-error 'error-conditions '(domain-error arith-error error))
(put 'domain-error 'error-message "Arithmetic domain error")
(put 'range-error 'error-conditions '(range-error arith-error error))
(put 'range-error 'error-message "Arithmetic range error")
(put 'singularity-error 'error-conditions
     '(singularity-error domain-error arith-error error))
(put 'singularity-error 'error-message "Arithmetic singularity error")
(put 'overflow-error 'error-conditions
     '(overflow-error range-error arith-error error))
(put 'overflow-error 'error-message "Arithmetic overflow error")
(put 'underflow-error 'error-conditions
     '(underflow-error range-error arith-error error))
(put 'underflow-error 'error-message "Arithmetic underflow error")
(put 'recursion-error 'error-conditions '(recursion-error error))
(put 'recursion-error 'error-message "Excessive recursive calling error")
(put 'excessive-lisp-nesting 'error-conditions
     '(excessive-lisp-nesting recursion-error error))
(put 'excessive-lisp-nesting 'error-message
     "Lisp nesting exceeds `max-lisp-eval-depth'")
(put 'excessive-variable-binding 'error-conditions
     '(excessive-variable-binding recursion-error error))
(put 'excessive-variable-binding 'error-message
     "Variable binding depth exceeds max-specpdl-size")
(put 'file-error 'error-conditions '(file-error error))
(put 'file-error 'error-message "File error")
(put 'file-already-exists 'error-conditions
     '(file-already-exists file-error error))
(put 'file-already-exists 'error-message "File already exists")
(put 'file-date-error 'error-conditions '(file-date-error file-error error))
(put 'file-date-error 'error-message "Cannot set file date")
(put 'file-missing 'error-conditions '(file-missing file-error error))
(put 'file-missing 'error-message "File is missing")
(put 'permission-denied 'error-conditions '(permission-denied file-error error))
(put 'permission-denied 'error-message "Cannot access file or directory")
(put 'file-notify-error 'error-conditions '(file-notify-error file-error error))
(put 'file-notify-error 'error-message "File notification error")
(put 'remote-file-error 'error-conditions '(remote-file-error file-error error))
(put 'remote-file-error 'error-message "Remote file error")
(put 'search-failed 'error-conditions '(search-failed error))
(put 'search-failed 'error-message "Search failed")
(put 'user-search-failed 'error-conditions
     '(user-search-failed user-error search-failed error))
(put 'user-search-failed 'error-message "Search failed")
(put 'invalid-regexp 'error-conditions '(invalid-regexp error))
(put 'invalid-regexp 'error-message "Invalid regexp")
(put 'scan-error 'error-conditions '(scan-error error))
(put 'scan-error 'error-message "Scan error")

;; From GNU Emacs 30.2 subr.el
(defun define-error (name message &optional parent)
  "Define NAME as a new error signal.
MESSAGE is a string that will be output to the echo area if such an error
is signaled without being caught by a `condition-case'.
PARENT is either a signal or a list of signals from which it inherits.
Defaults to `error'."
  (unless parent (setq parent 'error))
  (let ((conditions
         (if (consp parent)
             (apply #'append
                    (mapcar (lambda (parent)
                              (cons parent
                                    (or (get parent 'error-conditions)
                                        (error "Unknown signal `%s'" parent))))
                            parent))
           (cons parent (get parent 'error-conditions)))))
    (put name 'error-conditions
         (delete-dups (copy-sequence (cons name conditions))))
    (when message (put name 'error-message message))))

;; From GNU Emacs 30.2 cl-macs.el
(defvar cl--optimize-safety 1)
(defvar cl--optimize-speed 1)
(defmacro cl-the (type form)
  "Return FORM.  If type-checking is enabled, assert that it is of TYPE."
  (declare (indent 1) (debug (cl-type-spec form)))
  ;; When native compiling possibly add the appropriate type hint.
  (when (and (boundp 'byte-native-compiling)
             byte-native-compiling)
    (setf form
          (cl-case type
            (fixnum `(comp-hint-fixnum ,form))
            (cons `(comp-hint-cons ,form))
            (otherwise form))))
  (if (not (or (not (macroexp-compiling-p))
               (< cl--optimize-speed 3)
               (= cl--optimize-safety 3)))
      form
    (macroexp-let2 macroexp-copyable-p temp form
      `(progn (unless (cl-typep ,temp ',type)
                (signal 'wrong-type-argument
                        (list ',type ,temp ',form)))
              ,temp))))

;; cl-struct-slot-* helpers over emaxx's struct metadata (GNU semantics:
;; offsets are 1-based past the cl-tag-slot; unknown slots signal
;; `cl-struct-unknown-slot' like GNU Emacs 30.2 cl-macs.el).
(define-error 'cl-struct-unknown-slot "struct has no slot")

(defun cl-struct-slot-info (struct-type)
  "Return a list of slot names of struct STRUCT-TYPE."
  (or (get struct-type 'emaxx-struct-slot-descs)
      (error "%s is not a struct type" struct-type)))

(defun cl-struct-slot-offset (struct-type slot-name)
  "Return the offset of slot SLOT-NAME in STRUCT-TYPE."
  (let ((slots (get struct-type 'emaxx-struct-slots))
        (idx 1))
    (while (and slots (not (eq (car slots) slot-name)))
      (setq slots (cdr slots) idx (1+ idx)))
    (if slots idx
      (signal 'cl-struct-unknown-slot (list struct-type slot-name)))))

(defun cl--struct-slot-accessor (struct-type slot-name)
  (or (cdr (assq slot-name (get struct-type 'emaxx-struct-accessors)))
      (signal 'cl-struct-unknown-slot (list struct-type slot-name))))

(defun cl-struct-slot-value (struct-type slot-name inst)
  "Return the value of slot SLOT-NAME in INST of STRUCT-TYPE."
  (funcall (cl--struct-slot-accessor struct-type slot-name) inst))

(fset (intern "(setf cl-struct-slot-value)")
      (lambda (value struct-type slot-name inst)
        (eval (list 'setf
                    (list (cl--struct-slot-accessor struct-type slot-name)
                          (list 'quote inst))
                    (list 'quote value))
              t)))

;; GNU preloads lisp-mode.el; pp.el calls `lisp-mode-variables' assuming it.
(autoload 'lisp-mode-variables "lisp-mode")

;; files.el: interpreter-line matcher used by derived shell modes.
(defvar auto-mode-interpreter-regexp
  (concat
   "#![ \t]*"
   "\\("
   "[^ \t\n]*/bin/env[ \t]*"
   "\\(?:"
   "\\(?:-[0a-z]*S[ \t]*\\|--split-string=\\)"
   "\\(?:-[^ \t\n]+[ \t]+\\)*"
   "\\(?:[^ \t\n]+=[^ \t\n]*[ \t]+\\)*"
   "\\)?"
   "\\)?"
   "\\([^ \t\n]+\\)"))

;; GNU loaddefs autoloads these major modes.
(autoload 'sh-mode "sh-script" nil t)
(autoload 'shell-script-mode "sh-script" nil t)

;; GNU preloads isearch.el; char-fold's symmetric mode reads these.
(defvar isearch-regexp nil)
(defvar isearch-lax-whitespace t)
(defvar isearch-regexp-lax-whitespace nil)

;; simple.el: the *Messages* buffer accessor.
(defun messages-buffer ()
  "Return the \"*Messages*\" buffer.
If it does not exist, create it and switch it to `messages-buffer-mode'."
  (or (get-buffer "*Messages*")
      (with-current-buffer (get-buffer-create "*Messages*")
        (when (fboundp 'messages-buffer-mode)
          (messages-buffer-mode))
        (current-buffer))))


(defun beginning-of-defun-comments (&optional arg)
  "Move to the beginning of ARGth defun, including comments."
  (interactive "^p")
  (unless arg (setq arg 1))
  (beginning-of-defun arg)
  (let (first-line-p)
    (while (let ((ppss (progn (setq first-line-p (= (forward-line -1) -1))
                              (syntax-ppss (line-end-position)))))
             (while (and (nth 4 ppss) ; If eol is in a line-spanning comment,
                         (< (nth 8 ppss) (line-beginning-position)))
               (goto-char (nth 8 ppss)) ; skip to comment start.
               (setq ppss (syntax-ppss (line-end-position))))
             (and (not first-line-p)
                  (progn (skip-syntax-backward
                          "-" (line-beginning-position))
                         (not (bolp))) ; Check for blank line.
                  (beginning-of-defun--in-emptyish-line-p)))) ; Check for non-comment text.
    (forward-line (if first-line-p 0 1))))

(defvar forward-sexp-function nil
  ;; FIXME:
  ;; - for some uses, we may want a "sexp-only" version, which only
  ;;   jumps over a well-formed sexp, rather than some dwimish thing
  ;;   like jumping from an "else" back up to its "if".
  ;; - for up-list, we could use the "sexp-only" behavior as well
  ;;   to treat the dwimish halfsexp as a form of "up-list" step.
  "If non-nil, `forward-sexp' delegates to this function.
Should take the same arguments and behave similarly to `forward-sexp'.")

(defvar insert-pair-alist
  '((40 41) (91 93) (123 125) (60 62) (34 34) (39 39) (96 39))
  "Alist of paired characters inserted by `insert-pair'.")

(defvar delete-pair-blink-delay 1
  "Time in seconds to delay after showing a paired character to delete.
It's used by the command `delete-pair'.  The value 0 disables blinking.")

(defun activate-mark (&optional no-tmm)
  "Activate the mark.
If NO-TMM is non-nil, leave `transient-mark-mode' alone."
  (when (mark t)
    (unless (region-active-p)
      (force-mode-line-update) ;Refresh toolbar (bug#16382).
      (setq mark-active t)
      (unless (or transient-mark-mode no-tmm)
        (setq-local transient-mark-mode 'lambda))
      (run-hooks 'activate-mark-hook))))

(defmacro with-buffer-unmodified-if-unchanged (&rest body)
  "Like `progn', but change buffer-modified status only if buffer text changes.
If the buffer was unmodified before execution of BODY, and
buffer text after execution of BODY is identical to what it was
before, ensure that buffer is still marked unmodified afterwards.
For example, the following won't change the buffer's modification
status:

  (with-buffer-unmodified-if-unchanged
    (insert \"a\")
    (delete-char -1))

Note that only changes in the raw byte sequence of the buffer text,
as stored in the internal representation, are monitored for the
purpose of detecting the lack of changes in buffer text.  Any other
changes that are normally perceived as \"buffer modifications\", such
as changes in text properties, `buffer-file-coding-system', buffer
multibyteness, etc. -- will not be noticed, and the buffer will still
be marked unmodified, effectively ignoring those changes."
  (declare (debug t) (indent 0))
  (let ((hash (gensym))
        (buffer (gensym)))
    `(let ((,hash (and (not (buffer-modified-p))
                       (buffer-hash)))
           (,buffer (current-buffer)))
       (prog1
           (progn
             ,@body)
         ;; If we didn't change anything in the buffer (and the buffer
         ;; was previously unmodified), then flip the modification status
         ;; back to "unchanged".
         (when (and ,hash (buffer-live-p ,buffer))
           (with-current-buffer ,buffer
             (when (and (buffer-modified-p)
                        (equal ,hash (buffer-hash)))
               (restore-buffer-modified-p nil))))))))



;; python.el syntax-propertize (simplified GNU port): mark the opening
;; and closing quote of a triple-quote run as generic string fences so
;; sexp motion treats the whole docstring as one string.
(defun emaxx--python-syntax-stringify ()
  (let* ((ppss (save-excursion (backward-char 3) (syntax-ppss)))
         (string-start (and (eq t (nth 3 ppss)) (nth 8 ppss)))
         (quote-starting-pos (- (point) 3))
         (quote-ending-pos (point)))
    (cond ((or (nth 4 ppss)
               (and string-start
                    (not (eql (char-after string-start)
                              (char-after quote-starting-pos)))))
           nil)
          ((not string-start)
           (put-text-property quote-starting-pos (1+ quote-starting-pos)
                              'syntax-table (string-to-syntax "|")))
          (t
           (put-text-property (1- quote-ending-pos) quote-ending-pos
                              'syntax-table (string-to-syntax "|"))))))

(defun emaxx--python-syntax-propertize (start end)
  (goto-char start)
  (while (re-search-forward "\\(?:\"\"\"\\|'''\\)" end t)
    (emaxx--python-syntax-stringify)))

;; paragraphs.el forward/backward-paragraph (verbatim): the native
;; forward-paragraph only finds blank lines and ignores the let-bound
;; `paragraph-start'/`paragraph-separate' that lisp-fill-paragraph
;; installs (Bug#7751).
(defvar use-hard-newlines nil
  "Non-nil means to distinguish hard and soft newlines.")
(defvar paragraph-ignore-fill-prefix nil
  "Non-nil means the paragraph commands are not affected by `fill-prefix'.")

(defun forward-paragraph (&optional arg)
  "Move forward to end of paragraph.
With argument ARG, do it ARG times;
a negative argument ARG = -N means move backward N paragraphs.

A line which `paragraph-start' matches either separates paragraphs
\(if `paragraph-separate' matches it also) or is the first line of a paragraph.
A paragraph end is the beginning of a line which is not part of the paragraph
to which the end of the previous line belongs, or the end of the buffer.
Returns the count of paragraphs left to move."
  (interactive "^p")
  (or arg (setq arg 1))
  (let* ((opoint (point))
	 (fill-prefix-regexp
	  (and fill-prefix (not (equal fill-prefix ""))
	       (not paragraph-ignore-fill-prefix)
	       (regexp-quote fill-prefix)))
	 ;; Remove ^ from paragraph-start and paragraph-sep if they are there.
	 ;; These regexps shouldn't be anchored, because we look for them
	 ;; starting at the left-margin.  This allows paragraph commands to
	 ;; work normally with indented text.
	 ;; This hack will not find problem cases like "whatever\\|^something".
	 (parstart (if (and (not (equal "" paragraph-start))
			    (equal ?^ (aref paragraph-start 0)))
		       (substring paragraph-start 1)
		     paragraph-start))
	 (parsep (if (and (not (equal "" paragraph-separate))
			  (equal ?^ (aref paragraph-separate 0)))
		     (substring paragraph-separate 1)
		   paragraph-separate))
	 (parsep
	  (if fill-prefix-regexp
	      (concat parsep "\\|"
		      fill-prefix-regexp "[ \t]*$")
	    parsep))
	 ;; This is used for searching.
	 (sp-parstart (concat "^[ \t]*\\(?:" parstart "\\|" parsep "\\)"))
	 start found-start)
    (while (and (< arg 0) (not (bobp)))
      (if (and (not (looking-at parsep))
	       (re-search-backward "^\n" (max (1- (point)) (point-min)) t)
	       (looking-at parsep))
	  (setq arg (1+ arg))
	(setq start (point))
	;; Move back over paragraph-separating lines.
	(forward-char -1) (beginning-of-line)
	(while (and (not (bobp))
		    (progn (move-to-left-margin)
			   (looking-at parsep)))
	  (forward-line -1))
	(if (bobp)
	    nil
	  (setq arg (1+ arg))
	  ;; Go to end of the previous (non-separating) line.
	  (end-of-line)
	  ;; Search back for line that starts or separates paragraphs.
	  (if (if fill-prefix-regexp
		  ;; There is a fill prefix; it overrides parstart.
		  (let () ;; multiple-lines
		    (while (and (progn (beginning-of-line) (not (bobp)))
				(progn (move-to-left-margin)
				       (not (looking-at parsep)))
				(looking-at fill-prefix-regexp))
		      ;; (unless (= (point) start)
		      ;;   (setq multiple-lines t))
		      (forward-line -1))
		    (move-to-left-margin)
		    ;; This deleted code caused a long hanging-indent line
		    ;; not to be filled together with the following lines.
		    ;; ;; Don't move back over a line before the paragraph
		    ;; ;; which doesn't start with fill-prefix
		    ;; ;; unless that is the only line we've moved over.
		    ;; (and (not (looking-at fill-prefix-regexp))
		    ;;      multiple-lines
		    ;;      (forward-line 1))
		    (not (bobp)))
		(while (and (re-search-backward sp-parstart nil 1)
			    (setq found-start t)
			    ;; Found a candidate, but need to check if it is a
			    ;; REAL parstart.
			    (progn (setq start (point))
				   (move-to-left-margin)
				   (not (looking-at parsep)))
			    (not (and (looking-at parstart)
				      (or (not use-hard-newlines)
					  (bobp)
					  (get-text-property
					   (1- start) 'hard)))))
		  (setq found-start nil)
		  (goto-char start))
		found-start)
	      ;; Found one.
	      (progn
		;; Move forward over paragraph separators.
		;; We know this cannot reach the place we started
		;; because we know we moved back over a non-separator.
		(while (and (not (eobp))
			    (progn (move-to-left-margin)
				   (looking-at parsep)))
		  (forward-line 1))
		;; If line before paragraph is just margin, back up to there.
		(end-of-line 0)
		(if (> (current-column) (current-left-margin))
		    (forward-char 1)
		  (skip-chars-backward " \t")
		  (if (not (bolp))
		      (forward-line 1))))
	    ;; No starter or separator line => use buffer beg.
	    (goto-char (point-min))))))

    (while (and (> arg 0) (not (eobp)))
      ;; Move forward over separator lines...
      (while (and (not (eobp))
		  (progn (move-to-left-margin) (not (eobp)))
		  (looking-at parsep))
	(forward-line 1))
      (unless (eobp) (setq arg (1- arg)))
      ;; ... and one more line.
      (forward-line 1)
      (if fill-prefix-regexp
	  ;; There is a fill prefix; it overrides parstart.
	  (while (and (not (eobp))
		      (progn (move-to-left-margin) (not (eobp)))
		      (not (looking-at parsep))
		      (looking-at fill-prefix-regexp))
	    (forward-line 1))
	(while (and (re-search-forward sp-parstart nil 1)
		    (progn (setq start (match-beginning 0))
			   (goto-char start)
			   (not (eobp)))
		    (progn (move-to-left-margin)
			   (not (looking-at parsep)))
		    (or (not (looking-at parstart))
			(and use-hard-newlines
			     (not (get-text-property (1- start) 'hard)))))
	  (forward-char 1))
	(if (< (point) (point-max))
	    (goto-char start))))
    (constrain-to-field nil opoint t)
    ;; Return the number of steps that could not be done.
    arg))

(defun backward-paragraph (&optional arg)
  "Move backward to start of paragraph.
With argument ARG, do it ARG times;
a negative argument ARG = -N means move forward N paragraphs.

A paragraph start is the beginning of a line which is a
`paragraph-start' or which is ordinary text and follows a
`paragraph-separate'ing line; except: if the first real line of a
paragraph is preceded by a blank line, the paragraph starts at that
blank line.

See `forward-paragraph' for more information."
  (interactive "^p")
  (or arg (setq arg 1))
  (forward-paragraph (- arg)))

;; C defvars (buffer.c/indent.c) that fill.el and paragraphs.el read.
(defvar fill-prefix nil
  "String for filling to insert at front of new line, or nil for none.")
(defvar left-margin 0
  "Column for the default `indent-line-function' to indent to.")
(defvar sentence-end-double-space t
  "Non-nil means a single space does not end a sentence.")
(defvar colon-double-space nil
  "Non-nil means put two spaces after a colon when filling.")
(defvar sentence-end nil
  "Regexp describing the end of a sentence, or nil to use the default.")
(defvar sentence-end-without-period nil
  "Non-nil means a sentence will end without a period.")

(defvar sentence-end-without-space
  "\u3002\uff61\uff0e\uff1f\uff01"
  "String of characters that end sentence without following spaces.")

(defvar sentence-end-base "[.?!…‽][]\"'”’)}»›]*")

(defun sentence-end ()
  "Return the regexp describing the end of a sentence.

This function returns either the value of the variable `sentence-end'
if it is non-nil, or the default value constructed from the
variables `sentence-end-base', `sentence-end-double-space',
`sentence-end-without-period' and `sentence-end-without-space'.

The default value specifies that in order to be recognized as the
end of a sentence, the ending period, question mark, or exclamation point
must be followed by two spaces, with perhaps some closing delimiters
in between.  See Info node `(elisp)Standard Regexps'."
  (or sentence-end
      ;; We accept non-break space along with space.
      (concat (if sentence-end-without-period "\\w[ \u00a0][ \u00a0]\\|")
	      "\\("
	      sentence-end-base
              (if sentence-end-double-space
                  "\\($\\|[ \u00a0]$\\|\t\\|[ \u00a0][ \u00a0]\\)" "\\($\\|[\t \u00a0]\\)")
              "\\|[" sentence-end-without-space "]+"
	      "\\)"
              "[ \u00a0\t\n]*")))

;; indent.el (preloaded in GNU): fill.el moves to the left margin.
(defun current-left-margin ()
  "Return the left margin to use for this line.
This is the value of the buffer-local variable `left-margin' plus the value
of the `left-margin' text-property at the start of the line."
  (save-excursion
    (back-to-indentation)
    (max 0
	 (+ left-margin (or (get-text-property
			     (if (and (eobp) (not (bobp)))
				 (1- (point)) (point))
			     'left-margin) 0)))))

(defun move-to-left-margin (&optional n force)
  "Move to the left margin of the current line.
With optional argument, move forward N-1 lines first.
The column moved to is the one given by the `current-left-margin' function.
If the line's indentation appears to be wrong, and this command is called
interactively or with optional argument FORCE, it will be fixed."
  (interactive (list (prefix-numeric-value current-prefix-arg) t))
  (beginning-of-line n)
  (skip-chars-forward " \t")
  (if (minibufferp (current-buffer))
      (if (save-excursion (beginning-of-line) (bobp))
	  (goto-char (minibuffer-prompt-end))
	(beginning-of-line))
    (let ((lm (current-left-margin))
	  (cc (current-column)))
      (cond ((> cc lm)
	     (if (> (move-to-column lm force) lm)
		 ;; If lm is in a tab and we are not forcing, move before tab
		 (backward-char 1)))
	    ((and force (< cc lm))
	     (indent-to-left-margin))))))

;; This used to be the default indent-line-function,
;; used in Fundamental Mode, Text Mode, etc.
(defun indent-to-left-margin ()
  "Indent current line to the column given by `current-left-margin'."
  (save-excursion (indent-line-to (current-left-margin)))
  ;; If we are within the indentation, move past it.
  (when (save-excursion
	  (skip-chars-backward " \t")
	  (bolp))
    (skip-chars-forward " \t")))

;; paragraphs.el (preloaded in GNU): the paragraph boundary regexps
;; fill.el consults.
(defvar paragraph-start "\f\\|[ \t]*$"
  "Regexp for beginning of a line that starts OR separates paragraphs.")
(defvar paragraph-separate "[ \t\f]*$"
  "Regexp for beginning of a line that separates paragraphs.")

;; lisp.el (preloaded in GNU): list motion with escape-strings, pair
;; deletion and defun marking.  These are verbatim GNU ports; the
;; native up-list arm was removed in favor of this definition.

(defun up-list (&optional arg escape-strings no-syntax-crossing)
  "Move forward out of one level of parentheses.
This command will also work on other parentheses-like expressions
defined by the current language mode.  With ARG, do this that
many times.  A negative argument means move backward but still to
a less deep spot.

If ESCAPE-STRINGS is non-nil (as it is interactively), move out
of enclosing strings as well.

If NO-SYNTAX-CROSSING is non-nil (as it is interactively), prefer
to break out of any enclosing string instead of moving to the
end of a list broken across multiple strings.

On error, location of point is unspecified."
  (interactive "^p\nd\nd")
  (or arg (setq arg 1))
  (let ((inc (if (> arg 0) 1 -1))
        (pos nil))
    (while (/= arg 0)
      (condition-case err
          (save-restriction
            ;; If we've been asked not to cross string boundaries
            ;; and we're inside a string, narrow to that string so
            ;; that scan-lists doesn't find a match in a different
            ;; string.
            (when no-syntax-crossing
              (let* ((syntax (syntax-ppss))
                     (string-comment-start (nth 8 syntax)))
                (when string-comment-start
                  (save-excursion
                    (goto-char string-comment-start)
                    (narrow-to-region
                     (point)
                     (if (nth 3 syntax) ; in string
                         (condition-case nil
                             (progn (forward-sexp) (point))
                           (scan-error (point-max)))
                       (forward-comment 1)
                       (point)))))))
            (if (null forward-sexp-function)
                (goto-char (or (scan-lists (point) inc 1)
                               (buffer-end arg)))
              (condition-case err
                  (while (progn (setq pos (point))
                                (forward-sexp inc)
                                (/= (point) pos)))
                (scan-error (goto-char (nth (if (> arg 0) 3 2) err))))
              (if (= (point) pos)
                  (signal 'scan-error
                          (list "Unbalanced parentheses" (point) (point))))))
        (scan-error
         (let ((syntax nil))
           (or
            ;; If we bumped up against the end of a list, see whether
            ;; we're inside a string: if so, just go to the beginning
            ;; or end of that string.
            (and escape-strings
                 (or syntax (setf syntax (syntax-ppss)))
                 (nth 3 syntax)
                 (goto-char (nth 8 syntax))
                 (progn (when (> inc 0)
                          (forward-sexp))
                        t))
            ;; If we narrowed to a comment above and failed to escape
            ;; it, the error might be our fault, not an indication
            ;; that we're out of syntax.  Try again from beginning or
            ;; end of the comment.
            (and no-syntax-crossing
                 (or syntax (setf syntax (syntax-ppss)))
                 (nth 4 syntax)
                 (goto-char (nth 8 syntax))
                 (or (< inc 0)
                     (forward-comment 1))
                 (setf arg (+ arg inc)))
            (if no-syntax-crossing
                ;; Assume called interactively; don't signal an error.
                (user-error "At top level")
              (signal (car err) (cdr err)))))))
      (setq arg (- arg inc)))))

(defun backward-up-list (&optional arg escape-strings no-syntax-crossing)
  "Move backward out of one level of parentheses.
This command will also work on other parentheses-like expressions
defined by the current language mode.  With ARG, do this that
many times.  A negative argument means move forward but still to
a less deep spot.

If ESCAPE-STRINGS is non-nil (as it is interactively), move out
of enclosing strings as well.

If NO-SYNTAX-CROSSING is non-nil (as it is interactively), prefer
to break out of any enclosing string instead of moving to the
start of a list broken across multiple strings.

On error, location of point is unspecified."
  (interactive "^p\nd\nd")
  (up-list (- (or arg 1)) escape-strings no-syntax-crossing))

(defun delete-pair (&optional arg)
  "Delete a pair of characters enclosing ARG sexps that follow point.
A negative ARG deletes a pair around the preceding ARG sexps instead.
The option `delete-pair-blink-delay' can disable blinking."
  (interactive "P")
  (if arg
      (setq arg (prefix-numeric-value arg))
    (setq arg 1))
  (if (< arg 0)
      (save-excursion
	(skip-chars-backward " \t")
	(save-excursion
	  (let ((close-char (char-before)))
	    (forward-sexp arg)
	    (unless (member (list (char-after) close-char)
			    (mapcar (lambda (p)
				      (if (= (length p) 3) (cdr p) p))
				    insert-pair-alist))
	      (error "Not after matching pair"))
	    (when (and (numberp delete-pair-blink-delay)
		       (> delete-pair-blink-delay 0))
	      (sit-for delete-pair-blink-delay))
	    (delete-char 1)))
	(delete-char -1))
    (save-excursion
      (skip-chars-forward " \t")
      (save-excursion
	(let ((open-char (char-after)))
	  (forward-sexp arg)
	  (unless (member (list open-char (char-before))
			  (mapcar (lambda (p)
				    (if (= (length p) 3) (cdr p) p))
				  insert-pair-alist))
	    (error "Not before matching pair"))
	  (when (and (numberp delete-pair-blink-delay)
		     (> delete-pair-blink-delay 0))
	    (sit-for delete-pair-blink-delay))
	  (delete-char -1)))
      (delete-char 1))))

(defun mark-defun (&optional arg interactive)
  "Put mark at end of this defun, point at beginning.
The defun marked is the one that contains point or follows point.
With positive ARG, mark this and that many next defuns; with negative
ARG, change the direction of marking.

If the mark is active, it marks the next or previous defun(s) after
the one(s) already marked.

If INTERACTIVE is non-nil, as it is interactively,
report errors as appropriate for this kind of usage."
  (interactive "p\nd")
  (if interactive
      (condition-case e
          (mark-defun arg nil)
        (scan-error (user-error (cadr e))))
    (setq arg (or arg 1))
    ;; There is no `mark-defun-back' function - see
    ;; https://lists.gnu.org/r/bug-gnu-emacs/2016-11/msg00079.html
    ;; for explanation
    (when (eq last-command 'mark-defun-back)
      (setq arg (- arg)))
    (when (< arg 0)
      (setq this-command 'mark-defun-back))
    (cond ((use-region-p)
           (if (>= arg 0)
               (set-mark
                (save-excursion
                  (goto-char (mark))
                  ;; change the dotimes below to (end-of-defun arg)
                  ;; once bug #24427 is fixed
                  (dotimes (_ignore arg)
                    (end-of-defun))
                  (point)))
             (beginning-of-defun-comments (- arg))))
          (t
           (let ((opoint (point))
                 beg end)
             (push-mark opoint)
             ;; Try first in this order for the sake of languages with nested
             ;; functions where several can end at the same place as with the
             ;; offside rule, e.g. Python.
             (beginning-of-defun-comments)
             (setq beg (point))
             (end-of-defun)
             (setq end (point))
             (when (or (and (<= (point) opoint)
                            (> arg 0))
                       (= beg (point-min))) ; we were before the first defun!
               ;; beginning-of-defun moved back one defun so we got the wrong
               ;; one.  If ARG < 0, however, we actually want to go back.
               (goto-char opoint)
               (end-of-defun)
               (setq end (point))
               (beginning-of-defun-comments)
               (setq beg (point)))
             (goto-char beg)
             (cond ((> arg 0)
                    ;; change the dotimes below to (end-of-defun arg)
                    ;; once bug #24427 is fixed
                    (dotimes (_ignore arg)
                      (end-of-defun))
                    (setq end (point))
                    (push-mark end nil t)
                    (goto-char beg))
                   (t
                    (goto-char beg)
                    (unless (= arg -1)
                      ;; beginning-of-defun behaves strange with zero arg - see
                      ;; lists.gnu.org/r/bug-gnu-emacs/2017-02/msg00196.html
                      (beginning-of-defun (1- (- arg))))
                    (push-mark end nil t))))))
    (skip-chars-backward "[:space:]\n")
    (unless (bobp)
      (forward-line 1))))

(defun beginning-of-defun--in-emptyish-line-p ()
  "Return non-nil if the point is in an \"emptyish\" line.
This means a line that consists entirely of comments and/or
whitespace."
;; See https://lists.gnu.org/r/help-gnu-emacs/2016-08/msg00141.html
  (save-excursion
    (forward-line 0)
    (let ((ppss (syntax-ppss)))
      (and (null (nth 3 ppss))
           (< (line-end-position)
              (progn (when (nth 4 ppss)
                       (goto-char (nth 8 ppss)))
                     (forward-comment (point-max))
                     (point)))))))

;; help.el (preloaded in GNU): confusable-character hints.  lisp-mode's
;; `lisp--match-confusable-symbol-character' fontifier consults the regexp.
(defconst help-uni-confusables
  '((#x2018 . "'") ;; LEFT SINGLE QUOTATION MARK
    (#x2019 . "'") ;; RIGHT SINGLE QUOTATION MARK
    (#x201B . "'") ;; SINGLE HIGH-REVERSED-9 QUOTATION MARK
    (#x201C . "\"") ;; LEFT DOUBLE QUOTATION MARK
    (#x201D . "\"") ;; RIGHT DOUBLE QUOTATION MARK
    (#x201F . "\"") ;; DOUBLE HIGH-REVERSED-9 QUOTATION MARK
    (#x301E . "\"") ;; DOUBLE PRIME QUOTATION MARK
    (#xFF02 . "'") ;; FULLWIDTH QUOTATION MARK
    (#xFF07 . "'") ;; FULLWIDTH APOSTROPHE
    )
  "An alist of confusable characters to give hints about.")

(defconst help-uni-confusables-regexp
  (concat "[" (mapcar #'car help-uni-confusables) "]")
  "Regexp matching any character listed in `help-uni-confusables'.")

;; indent.el: the default region indenter drives the buffer's
;; `indent-line-function' over each nonblank line.
(defun indent-region (start end &optional column)
  "Indent each nonblank line in the region.
With no argument, indent each line using the mode's
`indent-line-function', or the mode's `indent-region-function'
when one is set."
  (interactive "r")
  (if (and (null column)
           (boundp 'indent-region-function)
           indent-region-function)
      (funcall indent-region-function start end)
    (save-excursion
      (goto-char end)
      (setq end (point-marker))
      (goto-char start)
      (beginning-of-line)
      (while (< (point) end)
        (unless (and (bolp) (eolp))
          (if column
              (indent-line-to column)
            (funcall indent-line-function)))
        (forward-line 1))
      (set-marker end nil)))
  nil)

;; ert-x.el helpers: ert-x is a preloaded feature here, so GNU's file
;; never loads; these are its portable definitions.
(defmacro ert-with-buffer-selected (buffer-or-name &rest body)
  "Display a buffer in a temporary selected window and run BODY.

If BUFFER-OR-NAME is nil, the current buffer is used.

The buffer is made the current buffer, and the temporary window
becomes the `selected-window', before BODY is evaluated.  The
window configuration is restored before returning, even if BODY
exits nonlocally.  The return value is the last form in BODY."
  (declare (indent 1))
  `(save-window-excursion
     (with-current-buffer (or ,buffer-or-name (current-buffer))
       (with-selected-window (display-buffer (current-buffer))
         ,@body))))

(defmacro ert-with-test-buffer-selected (spec &rest body)
  "Create a test buffer, switch to it, and run BODY.

This combines `ert-with-test-buffer' and
`ert-with-buffer-selected'.  The return value is the last form in
BODY."
  (declare (indent 1))
  `(ert-with-test-buffer (:name ,(plist-get spec :name))
     (ert-with-buffer-selected (current-buffer)
       ,@body)))

(defun ert-call-with-buffer-renamed (buffer-name thunk)
  "Protect the buffer named BUFFER-NAME from side-effects and run THUNK.

Renames the buffer BUFFER-NAME to a new temporary name, creates a
new buffer named BUFFER-NAME, executes THUNK, kills the new
buffer, and renames the original buffer back to BUFFER-NAME."
  (let ((new-buffer-name (generate-new-buffer-name
                          (format "%s orig buffer" buffer-name))))
    (with-current-buffer (get-buffer-create buffer-name)
      (rename-buffer new-buffer-name))
    (unwind-protect
        (progn
          (get-buffer-create buffer-name)
          (funcall thunk))
      (when (get-buffer buffer-name)
        (kill-buffer buffer-name))
      (with-current-buffer new-buffer-name
        (rename-buffer buffer-name)))))

(defmacro ert-with-buffer-renamed (spec &rest body)
  "Protect the buffer named by SPEC's form from side-effects and run BODY.

See `ert-call-with-buffer-renamed' for details."
  (declare (indent 1))
  `(ert-call-with-buffer-renamed ,(car spec) (lambda () ,@body)))

(defun ert-buffer-string-reindented (&optional buffer)
  "Return the contents of BUFFER after reindentation.

BUFFER defaults to current buffer.  Does not modify BUFFER."
  (with-current-buffer (or buffer (current-buffer))
    (let ((mode major-mode)
          (contents (buffer-string)))
      (with-temp-buffer
        (insert contents)
        (funcall mode)
        (let ((inhibit-read-only t))
          (indent-region (point-min) (point-max)))
        (buffer-string)))))

(defun ert-filter-string (s &rest regexps)
  "Return a copy of S with all matches of REGEXPS removed.

Elements of REGEXPS may also be two-element lists (REGEXP
SUBEXP), where SUBEXP is the number of a subexpression in
REGEXP.  In that case, only that subexpression will be removed
rather than the entire match."
  (with-temp-buffer
    (insert s)
    (dolist (x regexps)
      (let ((regexp (if (listp x) (nth 0 x) x))
            (subexp (if (listp x) (nth 1 x) nil)))
        (goto-char (point-min))
        (while (re-search-forward regexp nil t)
          (replace-match "" t t nil subexp))))
    (buffer-string)))

(defun ert-propertized-string (&rest args)
  "Return a string with properties as specified by ARGS.

ARGS is a list of strings and plists.  The strings in ARGS are
concatenated to produce an output string.  In the output string,
each string from ARGS will have the preceding plist as its
property list, or no properties if there is no plist before it."
  (with-temp-buffer
    (let ((current-plist nil))
      (dolist (x args)
        (cond
         ((stringp x)
          (let ((begin (point)))
            (insert x)
            (set-text-properties begin (point) current-plist)))
         ((listp x)
          (unless (zerop (mod (length x) 2))
            (error "Odd number of args in plist: %S" x))
          (setq current-plist x))
         (t (signal 'wrong-type-argument (list '(or string list) x))))))
    (buffer-string)))

(defun ert--with-temp-file-generate-suffix (filename)
  "Generate temp file suffix from FILENAME."
  (concat "-"
          (replace-regexp-in-string "\\`\\(.+?\\)-?tests?\\'" "\\1"
                                    (file-name-base filename))))

;; xdisp.c: the message log line limit is a special variable so tests
;; can rebind it dynamically around `message' calls.
(defvar message-log-max 1000)

(defvar ert--test-buffers (make-hash-table :weakness t)
  "Table of all test buffers.  Keys are the buffer objects, values are t.")

;; syntax.el: apply `syntax-propertize-function' up to POS once per
;; region; fontification and `syntax-ppss' rely on the resulting
;; `syntax-table' text properties.
(defun syntax-propertize (pos)
  "Ensure that syntax-table properties are set until POS in current buffer."
  (when (and (boundp 'syntax-propertize-function)
             syntax-propertize-function
             (< syntax-propertize--done pos))
    (save-excursion
      (let ((start (max (min syntax-propertize--done (point-max)) (point-min)))
            (end (max pos (point-min))))
        ;; Advance the high-water mark first: the propertize function
        ;; may itself trigger machinery that calls back into us.
        (setq syntax-propertize--done (max (point-max) end))
        (remove-text-properties start end
                                '(syntax-table nil syntax-multiline nil))
        (funcall syntax-propertize-function start end)))))

;; font-core.el: the default `font-lock-function'; the native
;; `font-lock-mode' has already recorded the mode state when a custom
;; function delegates here.
(defun font-lock-default-function (_mode) nil)

;; help.el: run BODY with the help buffer erased, then display it.
(defmacro with-help-window (buffer-or-name &rest body)
  "Evaluate BODY, then display the help buffer BUFFER-OR-NAME.
Like GNU's `help--window-setup': BODY runs in the help buffer with
`standard-output' bound to it and read-only checks inhibited."
  (declare (indent 1))
  `(with-current-buffer (get-buffer-create ,buffer-or-name)
     (when (and (fboundp 'help-mode)
                (not (derived-mode-p 'help-mode)))
       (help-mode))
     (setq buffer-read-only t
           buffer-file-name nil)
     (buffer-disable-undo)
     (let ((inhibit-read-only t)
           (inhibit-modification-hooks t))
       (erase-buffer)
       (prog1
           (let ((standard-output (current-buffer)))
             ,@body)
         ;; GNU help--window-setup runs `help-make-xrefs' on the result
         ;; ([back]/[forward] buttons, cross-references) and leaves point
         ;; at the top of the rendered help buffer.
         (when (fboundp 'help-make-xrefs)
           (help-make-xrefs (current-buffer)))
         (goto-char (point-min))
         (display-buffer (current-buffer))))))

;; fill.el: paragraph filler; a no-op suffices while emaxx has no
;; window-width-driven line breaking.
;; syntax.el: the parse-partial-sexp state accessors; syntax.el never
;; loads (native syntax machinery), so define its list-backed struct.
(cl-defstruct (ppss
               (:constructor make-ppss)
               (:copier nil)
               (:type list))
  depth
  innermost-start
  last-complete-sexp-start
  string-terminator
  comment-depth
  quoted-p
  min-depth
  comment-style
  comment-or-string-start
  open-parens
  two-character-syntax)

;; simple.el: `goto-line', the batch-relevant core of GNU's command.
(defun goto-line (line &optional buffer relative)
  "Go to LINE, counting from line 1 at beginning of buffer."
  (declare (interactive-only forward-line))
  (interactive "NGoto line: ")
  (when buffer
    (switch-to-buffer buffer))
  (or (region-active-p) (push-mark))
  (let ((pos (save-restriction
               (unless relative (widen))
               (goto-char (point-min))
               (forward-line (1- line))
               (point))))
    (goto-char pos)))

;; byte-run.el: `inline' marks a form for inline expansion; at run
;; time it is progn.
(defmacro inline (&rest body)
  "Like `progn', but when compiled inline top-level function calls in body."
  (cons 'progn body))

;; simple.el: join the current line to the previous one.
(defun delete-indentation (&optional arg)
  "Join this line to previous and fix up whitespace at join."
  (interactive "*P")
  (beginning-of-line)
  (when arg (forward-line 1))
  (when (eq (preceding-char) ?\n)
    (delete-region (point) (1- (point)))
    (delete-horizontal-space)
    (unless (or (bolp) (eolp)
                (eq (char-after) ?\))
                (eq (char-before) ?\())
      (insert " "))))

;; simple.el: `join-line' is the historical alias.
(defalias 'join-line #'delete-indentation)

;; loaddefs: thingatpt autoloads.
(autoload 'thing-at-point "thingatpt")

;; loaddefs: edebug autoloads.
(autoload 'edebug-defun "edebug" nil t)
(autoload 'edebug-eval-top-level-form "edebug" nil t)

;; lread.c: completion table for library names under DIRS with SUFFIXES.
;; Candidates carry any directory part of STRING so plain prefix
;; completion over the returned list matches GNU's behavior.
(defun locate-file-completion-table (dirs suffixes string pred action)
  "Do completion for file names passed to `locate-file'."
  (let* ((dirpart (or (file-name-directory string) ""))
         (suffix-re (concat (regexp-opt (delete "" (copy-sequence suffixes)))
                            "\\'"))
         (names nil))
    (dolist (dir dirs)
      (let ((full (expand-file-name dirpart (or dir default-directory))))
        (when (file-directory-p full)
          (dolist (file (directory-files full))
            (unless (member file '("." ".."))
              (if (file-directory-p (expand-file-name file full))
                  (push (concat dirpart file "/") names)
                (when (string-match suffix-re file)
                  (push (concat dirpart
                                (substring file 0 (match-beginning 0)))
                        names))))))))
    (setq names (delete-dups (nreverse names)))
    (cond
     ((eq action t) (all-completions string names pred))
     ((null action) (try-completion string names pred))
     ((eq action 'lambda) (test-completion string names pred))
     ((eq (car-safe action) 'boundaries)
      `(boundaries ,(length dirpart) . ,(length (cdr action))))
     (t nil))))

;; help.el: the function around point, or the one called by the list
;; containing point.
(defun function-called-at-point ()
  "Return a function around point or else called by the list containing point.
If that doesn't give a function, return nil."
  (with-syntax-table emacs-lisp-mode-syntax-table
    (or (condition-case ()
            (save-excursion
              (or (not (zerop (skip-syntax-backward "_w")))
                  (eq (char-syntax (following-char)) ?w)
                  (eq (char-syntax (following-char)) ?_)
                  (forward-sexp -1))
              (skip-chars-forward "'")
              (let ((obj (read (current-buffer))))
                (and (symbolp obj) (fboundp obj) obj)))
          (error nil))
        (condition-case ()
            (save-excursion
              (save-restriction
                (narrow-to-region (max (point-min) (- (point) 1000))
                                  (point-max))
                (backward-up-list 1)
                (forward-char 1)
                (let ((obj (read (current-buffer))))
                  (and (symbolp obj) (fboundp obj) obj))))
          (error nil)))))

;; help-fns.el: resolve a primitive's C source file from the DOC file.
(defun help-C-file-name (subr-or-var kind)
  "Return the name of the C file where SUBR-OR-VAR is defined.
KIND should be `var' for a variable or `subr' for a subroutine."
  (let ((docbuf (get-buffer-create " *DOC*"))
        (name (if (eq 'var kind)
                  (concat "V" (symbol-name subr-or-var))
                (concat "F" (subr-name (advice--cd*r subr-or-var))))))
    (with-current-buffer docbuf
      (unless (eq (char-after (point-min)) ?)
        (erase-buffer)
        (insert-file-contents-literally
         (expand-file-name internal-doc-file-name doc-directory)))
      (goto-char (point-min))
      (when (search-forward (concat "" name "
") nil t)
        (search-backward "S")
        (let ((file (buffer-substring (+ (point) 2) (line-end-position))))
          (setq file (replace-regexp-in-string "\\.o\\'" ".c" file))
          (if (string-match "\\.\\(c\\|m\\)\\'" file)
              (concat "src/" file)
            file))))))

(defun fill-region-as-paragraph (_from _to &optional _justify
                                        _nosqueeze _squeeze-after)
  nil)

;; ert.el: lisp reimplementation of message_dolog()'s truncation.
(defun ert--force-message-log-buffer-truncation ()
  "Immediately truncate *Messages* buffer according to `message-log-max'."
  (with-current-buffer (messages-buffer)
    (when (natnump message-log-max)
      (let ((begin (point-min))
            (end (save-excursion
                   (goto-char (point-max))
                   (forward-line (- message-log-max))
                   (point)))
            (inhibit-read-only t))
        (delete-region begin end)))))

;;; simple_compat.el ends here

;; GNU cl-print.el's generic and its default method (cl-print.el itself is
;; not loaded; the native cl-prin1 renderer dispatches oclosures here, and
;; nadvice.el adds its own `advice' method for "#f(advice ...)").
(cl-defgeneric cl-print-object (object stream)
  "Dispatcher to print OBJECT on STREAM according to its type.")
(cl-defmethod cl-print-object (object stream)
  ;; The base method (cl-print.el prints strings/conses itself; everything
  ;; else falls back to `prin1').
  (prin1 object stream))

;; GNU files.el (verbatim): memory-report--format needs it.
(defun file-size-human-readable (file-size &optional flavor space unit)
  "Produce a string showing FILE-SIZE in human-readable form.

Optional second argument FLAVOR controls the units and the display format:

 If FLAVOR is nil or omitted, each kilobyte is 1024 bytes and the produced
    suffixes are \"k\", \"M\", \"G\", \"T\", etc.
 If FLAVOR is `si', each kilobyte is 1000 bytes and the produced suffixes
    are \"k\", \"M\", \"G\", \"T\", etc.
 If FLAVOR is `iec', each kilobyte is 1024 bytes and the produced suffixes
    are \"KiB\", \"MiB\", \"GiB\", \"TiB\", etc.

Optional third argument SPACE is a string put between the number and unit.
It defaults to the empty string.  We recommend a single space or
non-breaking space, unless other constraints prohibit a space in that
position.

Optional fourth argument UNIT is the unit to use.  It defaults to \"B\"
when FLAVOR is `iec' and the empty string otherwise.  We recommend \"B\"
in all cases, since that is the standard symbol for byte."
  (let ((power (if (or (null flavor) (eq flavor 'iec))
		   1024.0
		 1000.0))
	(prefixes '("" "k" "M" "G" "T" "P" "E" "Z" "Y" "R" "Q")))
    (while (and (>= file-size power) (cdr prefixes))
      (setq file-size (/ file-size power)
	    prefixes (cdr prefixes)))
    (let* ((prefix (car prefixes))
           (prefixed-unit (if (eq flavor 'iec)
                              (concat
                               (if (string= prefix "k") "K" prefix)
                               (if (string= prefix "") "" "i")
                               (or unit "B"))
                            (concat prefix unit))))
      ;; Mimic what GNU "ls -lh" does:
      ;; If the formatted size will have just one digit before the decimal...
      (format (if (and (< file-size 10)
                       ;; ...and its fractional part is not too small...
                       (>= (mod file-size 1.0) 0.05)
                       (< (mod file-size 1.0) 0.95))
                  ;; ...then emit one digit after the decimal.
		  "%.1f%s%s"
	        "%.0f%s%s")
	      file-size
              (if (string= prefixed-unit "") "" (or space ""))
              prefixed-unit))))

;; GNU files.el (verbatim): `byte-count-to-string-function's default.
(defun file-size-human-readable-iec (size)
  "Human-readable string for SIZE bytes, using IEC prefixes."
  (file-size-human-readable size 'iec " "))

(defvar byte-count-to-string-function #'file-size-human-readable-iec
  "Function that turns a number of bytes into a human-readable string.
It is for use when displaying file sizes and disk space where other
constraints do not force a specific format.")

;; GNU subr.el (verbatim).
(defun readablep (object)
  "Say whether OBJECT has a readable syntax.
This means that OBJECT can be printed out and then read back
again by the Lisp reader.  This function returns nil if OBJECT is
unreadable, and the printed representation (from `prin1') of
OBJECT if it is readable."
  (declare (side-effect-free error-free))
  (catch 'unreadable
    (let ((print-unreadable-function
           (lambda (_object _escape)
             (throw 'unreadable nil))))
      (prin1-to-string object))))

;; GNU cl.el aliases (advice.el preactivation uses them).
(defalias 'rplaca #'setcar)
(defalias 'rplacd #'setcdr)

;; GNU files.el: package-tests et al. `cd' into scratch dirs.  Bodies
;; verbatim; `cd's interactive spec is simplified to plain "D" (the GNU
;; one builds a cd-path completion table; batch tests never use it).
(defvar cd-path nil
  "Value of the CDPATH environment variable, as a list.
Not actually set up until the first time you use it.")

(defun cd-absolute (dir)
  "Change current directory to given absolute file name DIR."
  ;; Put the name into directory syntax now,
  ;; because otherwise expand-file-name may give some bad results.
  (setq dir (file-name-as-directory dir))
  ;; We used to additionally call abbreviate-file-name here, for an
  ;; unknown reason.  Problem is that most buffers are setup
  ;; without going through cd-absolute and don't call
  ;; abbreviate-file-name on their default-directory, so the few that
  ;; do end up using a superficially different directory.
  (setq dir (expand-file-name dir))
  (if (not (file-directory-p dir))
      (error (if (file-exists-p dir)
	         "%s is not a directory"
               "%s: no such directory")
             dir)
    (unless (file-accessible-directory-p dir)
      (error "Cannot cd to %s:  Permission denied" dir))
    (setq default-directory dir)
    (setq list-buffers-directory dir)))

(defun cd (dir)
  "Make DIR become the current buffer's default directory.
If your environment includes a `CDPATH' variable, try each one of
that list of directories (separated by occurrences of
`path-separator') when resolving a relative directory name.
The path separator is colon in GNU and GNU-like systems."
  (interactive "DChange default directory: ")
  (unless cd-path
    (setq cd-path (or (parse-colon-path (getenv "CDPATH"))
                      (list "./"))))
  (cd-absolute
   (or
    ;; locate-file doesn't support remote file names, so detect them
    ;; and support them here by hand.
    (and (file-remote-p (expand-file-name dir))
         (file-accessible-directory-p (expand-file-name dir))
         (expand-file-name dir))
    (locate-file dir cd-path nil
                 (lambda (f) (and (file-directory-p f) 'dir-ok)))
    (if (getenv "CDPATH")
        (error "No such directory found via CDPATH environment variable: %s" dir)
      (error "No such directory: %s" dir)))))

;; GNU subr.el (verbatim).
(defmacro with-file-modes (modes &rest body)
  "Execute BODY with default file permissions temporarily set to MODES.
MODES is as for `set-default-file-modes'."
  (declare (indent 1) (debug t))
  (let ((umask (make-symbol "umask")))
    `(let ((,umask (default-file-modes)))
       (unwind-protect
           (progn
             (set-default-file-modes ,modes)
             ,@body)
         (set-default-file-modes ,umask)))))

;; GNU subr.el (verbatim; loaddefs.el pushes more entries in GNU).
(defvar package--builtin-versions
  ;; Mostly populated by loaddefs.el.
  (purecopy `((emacs . ,(version-to-list emacs-version))))
  "Alist giving the version of each versioned builtin package.
I.e. each element of the list is of the form (NAME . VERSION) where
NAME is the package name as a symbol, and VERSION is its version
as a list.")

;; GNU files.el (verbatim).
(defun parse-colon-path (search-path)
  "Explode a search path into a list of directory names.
Directories are separated by `path-separator' (which is colon in
GNU and Unix systems).  Substitute environment variables into the
resulting list of directory names.  For an empty path element (i.e.,
a leading or trailing separator, or two adjacent separators), return
nil (meaning `default-directory') as the associated list element."
  (declare (ftype (function (string) list)))
  (when (stringp search-path)
    (let ((spath (substitute-env-vars search-path))
          (double-slash-special-p
           (memq system-type '(windows-nt cygwin ms-dos))))
      (mapcar (lambda (f)
                (if (equal "" f) nil
                  (let ((dir (file-name-as-directory f)))
                    ;; Previous implementation used `substitute-in-file-name'
                    ;; which collapses multiple "/" in front, while
                    ;; preserving double slash where it matters.  Do
                    ;; the same for backward compatibility.
                    (if (string-match "\\`//+" dir)
                        (substring dir (- (match-end 0)
                                          (if double-slash-special-p 2 1)))
                      dir))))
              (split-string spath path-separator)))))

;; GNU subr.el (verbatim).
(defun version-list-< (l1 l2)
  "Return t if L1, a list specification of a version, is lower than L2.

Note that a version specified by the list (1) is equal to (1 0),
\(1 0 0), (1 0 0 0), etc.  That is, the trailing zeros are insignificant.
Also, a version given by the list (1) is higher than (1 -1), which in
turn is higher than (1 -2), which is higher than (1 -3)."
  (declare (pure t) (side-effect-free t))
  (while (and l1 l2 (= (car l1) (car l2)))
    (setq l1 (cdr l1)
	  l2 (cdr l2)))
  (cond
   ;; l1 not null and l2 not null
   ((and l1 l2) (< (car l1) (car l2)))
   ;; l1 null and l2 null         ==> l1 length = l2 length
   ((and (null l1) (null l2)) nil)
   ;; l1 not null and l2 null     ==> l1 length > l2 length
   (l1 (< (version-list-not-zero l1) 0))
   ;; l1 null and l2 not null     ==> l2 length > l1 length
   (t  (< 0 (version-list-not-zero l2)))))

(defun version-list-= (l1 l2)
  "Return t if L1, a list specification of a version, is equal to L2.

Note that a version specified by the list (1) is equal to (1 0),
\(1 0 0), (1 0 0 0), etc.  That is, the trailing zeros are insignificant.
Also, a version given by the list (1) is higher than (1 -1), which in
turn is higher than (1 -2), which is higher than (1 -3)."
  (declare (pure t) (side-effect-free t))
  (while (and l1 l2 (= (car l1) (car l2)))
    (setq l1 (cdr l1)
	  l2 (cdr l2)))
  (cond
   ;; l1 not null and l2 not null
   ((and l1 l2) nil)
   ;; l1 null and l2 null     ==> l1 length = l2 length
   ((and (null l1) (null l2)))
   ;; l1 not null and l2 null ==> l1 length > l2 length
   (l1 (zerop (version-list-not-zero l1)))
   ;; l1 null and l2 not null ==> l2 length > l1 length
   (t  (zerop (version-list-not-zero l2)))))

(defun version-list-<= (l1 l2)
  "Return t if L1, a list specification of a version, is lower or equal to L2.

Note that integer list (1) is equal to (1 0), (1 0 0), (1 0 0 0),
etc.  That is, the trailing zeroes are insignificant.  Also, integer
list (1) is greater than (1 -1) which is greater than (1 -2)
which is greater than (1 -3)."
  (declare (pure t) (side-effect-free t))
  (while (and l1 l2 (= (car l1) (car l2)))
    (setq l1 (cdr l1)
	  l2 (cdr l2)))
  (cond
   ;; l1 not null and l2 not null
   ((and l1 l2) (< (car l1) (car l2)))
   ;; l1 null and l2 null     ==> l1 length = l2 length
   ((and (null l1) (null l2)))
   ;; l1 not null and l2 null ==> l1 length > l2 length
   (l1 (<= (version-list-not-zero l1) 0))
   ;; l1 null and l2 not null ==> l2 length > l1 length
   (t  (<= 0 (version-list-not-zero l2)))))

(defun version-list-not-zero (lst)
  "Return the first non-zero element of LST, which is a list of integers.

If all LST elements are zeros or LST is nil, return zero."
  (declare (pure t) (side-effect-free t))
  (while (and lst (zerop (car lst)))
    (setq lst (cdr lst)))
  (if lst
      (car lst)
    ;; there is no element different of zero
    0))

;; GNU cus-edit.el customize-save-variable, with the theme/custom-file
;; machinery guarded (cus-edit.el itself is not loaded; batch tests have
;; no custom-file so GNU takes the message branch anyway).
(defun customize-save-variable (variable value &optional comment)
  "Set the default for VARIABLE to VALUE, and save it for future sessions.
Return VALUE."
  (funcall (or (get variable 'custom-set) 'set-default) variable value)
  (put variable 'saved-value (list (custom-quote value)))
  (when (fboundp 'custom-push-theme)
    (custom-push-theme 'theme-value variable 'user 'set (custom-quote value)))
  (cond ((string= comment "")
	 (put variable 'variable-comment nil)
	 (put variable 'saved-variable-comment nil))
	(comment
	 (put variable 'variable-comment comment)
	 (put variable 'saved-variable-comment comment)))
  (put variable 'customized-value nil)
  (put variable 'customized-variable-comment nil)
  (when (and (fboundp 'custom-file) (custom-file t) (fboundp 'custom-save-all))
    (custom-save-all))
  value)

;; GNU isearch.el (verbatim).
(defvar isearch-fold-quotes-mode--state)
(define-minor-mode isearch-fold-quotes-mode
  "Minor mode to aid searching for \\=` characters in help modes."
  :lighter ""
  (if isearch-fold-quotes-mode
      (setq-local isearch-fold-quotes-mode--state
                  (buffer-local-set-state
                   search-default-mode
                   (lambda (string &optional _lax)
                     (thread-last
                       (regexp-quote string)
                       (replace-regexp-in-string "`" "[`‘]")
                       (replace-regexp-in-string "'" "['’]")
                       (replace-regexp-in-string "\"" "[\"“”]")))))
    (buffer-local-restore-state isearch-fold-quotes-mode--state)))

;; GNU custom.el (verbatim).
(defun custom-quote (sexp)
  "Quote SEXP if it is not self quoting."
  ;; Can't use `macroexp-quote' because it is loaded after `custom.el'
  ;; during bootstrap.  See `loadup.el'.
  (if (and (not (consp sexp))
           (or (keywordp sexp)
               (not (symbolp sexp))
               (booleanp sexp)))
      sexp
    (list 'quote sexp)))

;; GNU subr.el (verbatim).
(defmacro buffer-local-set-state (&rest pairs)
  "Like `setq-local', but allow restoring the previous state of locals later.
This macro returns an object that can be passed to `buffer-local-restore-state'
in order to restore the state of the local variables set via this macro.

\(fn [VARIABLE VALUE]...)"
  (declare (debug setq))
  (unless (zerop (mod (length pairs) 2))
    (error "PAIRS must have an even number of variable/value members"))
  `(prog1
       (buffer-local-set-state--get ',pairs)
     (setq-local ,@pairs)))

(defun buffer-local-set-state--get (pairs)
  (let ((states nil))
    (while pairs
      (push (list (car pairs)
                  (and (boundp (car pairs))
                       (local-variable-p (car pairs)))
                  (and (boundp (car pairs))
                       (symbol-value (car pairs))))
            states)
      (setq pairs (cddr pairs)))
    (nreverse states)))

(defun buffer-local-restore-state (states)
  "Restore values of buffer-local variables recorded in STATES.
STATES should be an object returned by `buffer-local-set-state'."
  (pcase-dolist (`(,variable ,local ,value) states)
    (if local
        (set variable value)
      (kill-local-variable variable))))

;; GNU subr.el (verbatim).
(defun package--description-file (dir)
  "Return package description file name for package DIR."
  (concat (let ((subdir (file-name-nondirectory
                         (directory-file-name dir))))
            (if (string-match "\\([^.].*?\\)-\\([0-9]+\\(?:[.][0-9]+\\|\\(?:pre\\|beta\\|alpha\\)[0-9]+\\)*\\)" subdir)
                (match-string 1 subdir) subdir))
          "-pkg.el"))

;; GNU subr.el (verbatim).
(defmacro with-existing-directory (&rest body)
  "Execute BODY with `default-directory' bound to an existing directory.
If `default-directory' is already an existing directory, it's not changed."
  (declare (indent 0) (debug t))
  `(let ((default-directory (seq-find (lambda (dir)
                                        (and dir
                                             (file-exists-p dir)))
                                      (list default-directory
                                            (expand-file-name "~/")
                                            temporary-file-directory
                                            (getenv "TMPDIR")
                                            "/tmp/")
                                      "/")))
     ,@body))

;; GNU custom.el (verbatim).
(defun custom--standard-value (variable)
  "Return the standard value of VARIABLE."
  (eval (car (get variable 'standard-value)) t))

;; GNU byte-run.el (verbatim): loaddefs-gen.el logs through it.
(defun byte-compile-info (string &optional message type)
  "Format STRING in a way that looks pleasing in the compilation output.
If MESSAGE, output the message, too.

If TYPE, it should be a string that says what the information
type is.  This defaults to \"INFO\"."
  (let ((string (format "  %-9s%s" (or type "INFO") string)))
    (when message
      (message "%s" string))
    string))

;; GNU subr.el (verbatim).
(defun ensure-empty-lines (&optional lines)
  "Ensure that there are LINES number of empty lines before point.
If LINES is nil or omitted, ensure that there is a single empty
line before point.

If called interactively, LINES is given by the prefix argument.

If there are more than LINES empty lines before point, the number
of empty lines is reduced to LINES.

If point is not at the beginning of a line, a newline character
is inserted before adjusting the number of empty lines."
  (interactive "p")
  (unless (bolp)
    (insert "\n"))
  (let ((lines (or lines 1))
        (start (save-excursion
                 (if (re-search-backward "[^\n]" nil t)
                     (+ (point) 2)
                   (point-min)))))
    (cond
     ((> (- (point) start) lines)
      (delete-region (point) (- (point) (- (point) start lines))))
     ((< (- (point) start) lines)
      (insert (make-string (- lines (- (point) start)) ?\n))))))

;; Shim over the native byte-compile machinery: GNU's
;; byte-recompile-directory lives in bytecomp.el, which must not be loaded
;; (it would shadow the native compiler).  Compiles every .el file below
;; DIRECTORY that has no up-to-date .elc, like GNU with ARG 0.
(defun byte-recompile-directory (directory &optional arg force _follow-symlinks)
  "Recompile every `.el' file in DIRECTORY that needs recompilation.
Files in subdirectories of DIRECTORY are processed also."
  (interactive "DByte recompile directory: \nP")
  (dolist (file (directory-files-recursively directory "\\.el\\'"))
    (let ((dest (concat file "c")))
      (when (or force
                (not (file-exists-p dest))
                (file-newer-than-file-p file dest))
        (when (or (file-exists-p dest) (numberp arg) force)
          (ignore-errors (byte-compile-file file))))))
  nil)

;; GNU files.el (verbatim).
(defun directory-files-recursively (dir regexp
                                        &optional include-directories predicate
                                        follow-symlinks)
  "Return list of all files under directory DIR whose names match REGEXP.
This function works recursively.  Files are returned in \"depth
first\" order, and files from each directory are sorted in
alphabetical order.  Each file name appears in the returned list
in its absolute form.

By default, the returned list excludes directories, but if
optional argument INCLUDE-DIRECTORIES is non-nil, they are
included.

PREDICATE can be either nil (which means that all subdirectories
of DIR are descended into), t (which means that subdirectories that
can't be read are ignored), or a function (which is called with
the name of each subdirectory, and should return non-nil if the
subdirectory is to be descended into).

If FOLLOW-SYMLINKS is non-nil, symbolic links that point to
directories are followed.  Note that this can lead to infinite
recursion."
  (let* ((result nil)
	 (files nil)
         (dir (directory-file-name dir))
	 ;; When DIR is "/", remote file names like "/method:" could
	 ;; also be offered.  We shall suppress them.
	 (tramp-mode (and tramp-mode (file-remote-p (expand-file-name dir)))))
    (dolist (file (sort (file-name-all-completions "" dir)
			'string<))
      (unless (member file '("./" "../"))
	(if (directory-name-p file)
	    (let* ((leaf (substring file 0 (1- (length file))))
		   (full-file (concat dir "/" leaf)))
	      ;; Don't follow symlinks to other directories.
	      (when (and (or (not (file-symlink-p full-file))
                             (and (file-symlink-p full-file)
                                  follow-symlinks))
                         ;; Allow filtering subdirectories.
                         (or (eq predicate nil)
                             (eq predicate t)
                             (funcall predicate full-file)))
                (let ((sub-files
                       (if (eq predicate t)
                           (ignore-error file-error
                             (directory-files-recursively
			      full-file regexp include-directories
                              predicate follow-symlinks))
                         (directory-files-recursively
			  full-file regexp include-directories
                          predicate follow-symlinks))))
		  (setq result (nconc result sub-files))))
	      (when (and include-directories
			 (string-match regexp leaf))
		(setq result (nconc result (list full-file)))))
	  (when (string-match regexp file)
	    (push (concat dir "/" file) files)))))
    (nconc result (nreverse files))))

;; GNU comp.el helper: emaxx produces no .eln artifacts, so there is
;; nothing to clean; package.el calls this when deleting packages.
(defun comp-clean-up-stale-eln (_file)
  "Remove stale .eln files (no-op: emaxx has no native compilation cache)."
  nil)

;; GNU comp.el helper: emaxx never writes .eln files, so map to a path
;; that cannot exist (package-delete probes it before removing).
(defun comp-el-to-eln-filename (filename &optional base-dir)
  "Return the .eln path FILENAME would compile to (never exists here)."
  (expand-file-name (concat (file-name-base filename) ".eln")
                    (or base-dir (expand-file-name "eln-cache" temporary-file-directory))))

;; GNU files.el (verbatim).
(defun prune-directory-list (dirs &optional keep reject)
  "Return a copy of DIRS with all non-existent directories removed.
The optional argument KEEP is a list of directories to retain even if
they don't exist, and REJECT is a list of directories to remove from
DIRS, even if they exist; REJECT takes precedence over KEEP.

Note that membership in REJECT and KEEP is checked using simple string
comparison."
  (apply #'nconc
	 (mapcar (lambda (dir)
		   (and (not (member dir reject))
			(or (member dir keep) (file-directory-p dir))
			(list dir)))
		 dirs)))

;; lisp-mode.el (verbatim): `lm-section-end' (lisp-mnt.el) calls
;; `lisp-outline-level' directly when computing Commentary bounds.
(defconst lisp-mode-autoload-regexp
  "^;;;###\\(\\([-[:alnum:]]+?\\)-\\)?\\(autoload\\)"
  "Regexp to match autoload cookies.
The second group matches package names used to redirect autoloads
to a package-local loaddefs file.")

(defun lisp-outline-level ()
  "Lisp mode `outline-level' function."
  ;; Expects outline-regexp is ";;;\\(;* [^ \t\n]\\|###autoload\\)\\|("
  ;; and point is at the beginning of a matching line.
  (let ((len (- (match-end 0) (match-beginning 0))))
    (cond ((or (looking-at-p "(")
               (looking-at-p lisp-mode-autoload-regexp))
           1000)
          ((looking-at ";;\\(;+\\) ")
           (- (match-end 1) (match-beginning 1)))
          ;; Above should match everything but just in case.
          (t
           len))))

;; outline.el: GNU's file builds its mode menus by walking keymaps as
;; raw lists (`mapcar'/`nconc' over `outline-mode-menu-bar-map'), which
;; the record-backed emaxx keymaps cannot satisfy, so the real file
;; cannot load.  Provide the feature with the pieces its library
;; consumers (lisp-mnt.el's `lm-section-end') actually read.  All
;; definitions are verbatim from outline.el.
(defvar outline-regexp "[*\^L]+"
  "Regular expression to match the beginning of a heading.
Any line whose beginning matches this regexp is considered to start a heading.
Note that Outline mode only checks this regexp at the start of a line,
so the regexp need not (and usually does not) start with `^'.
The recommended way to set this is with a `Local Variables:' list
in the file it applies to.")

(defvar outline-heading-end-regexp "\n"
  "Regular expression to match the end of a heading line.
You can assume that point is at the beginning of a heading when this
regexp is searched for.  The heading ends at the end of the match.
The recommended way to set this is with a `Local Variables:' list
in the file it applies to.")

(defvar outline-search-function nil
  "Function to search for the next outline heading.")

(defvar outline-heading-alist ()
  "Alist associating a heading for every possible level.")

(defvar outline-level #'outline-level
  "Function of no args to compute a header's nesting level in an outline.
It can assume point is at the beginning of a header line and that the match
data reflects the `outline-regexp'.")

(defun outline-level ()
  "Return the depth to which a statement is nested in the outline.
Point must be at the beginning of a header line.
This is actually either the level specified in `outline-heading-alist'
or else the number of characters matched by `outline-regexp'."
  (or (cdr (assoc (match-string 0) outline-heading-alist))
      (- (match-end 0) (match-beginning 0))))

(provide 'outline)

;; help.el (verbatim): describe-package quotes the install directory
;; with this.
(defun substitute-quotes (string)
  "Substitute quote characters in STRING for display.
Each grave accent \\=` is replaced by left quote, and each
apostrophe \\=' is replaced by right quote.  Which left and right
quote characters to use is determined by the variable
`text-quoting-style'."
  (cond ((eq (text-quoting-style) 'curve)
         (string-replace "`" "‘"
                         (string-replace "'" "’" string)))
        ((eq (text-quoting-style) 'straight)
         (string-replace "`" "'" string))
        (t string)))

;; warnings.el (verbatim): tar-mode's link handler calls this; the
;; native display-warning does the rest.
(defun lwarn (type level message &rest args)
  "Display a warning message made from (format-message MESSAGE ARGS...).
\\<special-mode-map>
Aside from generating the message with `format-message',
this is equivalent to `display-warning'.

TYPE is the warning type: either a custom group name (a symbol),
or a list of symbols whose first element is a custom group name.
\(The rest of the symbols represent subcategories and
can be whatever you like.)

LEVEL should be either :debug, :warning, :error, or :emergency
\(but see `warning-minimum-level' and `warning-minimum-log-level').

:emergency -- a problem that will seriously impair Emacs operation soon
	      if you do not attend to it promptly.
:error     -- invalid data or circumstances.
:warning   -- suspicious data or circumstances.
:debug     -- info for debugging only."
  (display-warning type (apply #'format-message message args) level))

;; mule-conf.el/jka-compr defaults: GNU builds this alist in C/mule
;; setup; package-install-from-buffer consults it (via
;; find-operation-coding-system) to decode literally-read buffers.
(setq file-coding-system-alist
  '(("\\.tzst\\'" no-conversion . no-conversion)
    ("\\.zst\\'" no-conversion . no-conversion)
    ("\\.dz\\'" no-conversion . no-conversion)
    ("\\.txz\\'" no-conversion . no-conversion)
    ("\\.xz\\'" no-conversion . no-conversion)
    ("\\.lzma\\'" no-conversion . no-conversion)
    ("\\.lz\\'" no-conversion . no-conversion)
    ("\\.g?z\\'" no-conversion . no-conversion)
    ("\\.\\(?:tgz\\|svgz\\|sifz\\)\\'" no-conversion . no-conversion)
    ("\\.tbz2?\\'" no-conversion . no-conversion)
    ("\\.bz2\\'" no-conversion . no-conversion)
    ("\\.Z\\'" no-conversion . no-conversion)
    ("\\.elc\\'" . utf-8-emacs)
    ("\\.el\\'" . prefer-utf-8)
    ("\\.utf\\(-8\\)?\\'" . utf-8)
    ("\\.xml\\'" . xml-find-file-coding-system)
    ("\\(\\`\\|/\\)loaddefs.el\\'" raw-text . raw-text-unix)
    ("\\.tar\\'" no-conversion . no-conversion)
    ("\\.po[tx]?\\'\\|\\.po\\." . po-find-file-coding-system)
    ("\\.\\(tex\\|ltx\\|dtx\\|drv\\)\\'" . latexenc-find-file-coding-system)
    ("" undecided)))

;; url-http.el surface: the real file drives make-network-process, which
;; emaxx cannot run (url-retrieve is native).  url-methods.el's scheme
;; registry only needs the `url-http' loader to be fbound plus the
;; method symbols it interns (url-http-expand-file-name and friends).
(defun url-http (url callback &optional cbargs &rest _ignored)
  "Retrieve URL via the native HTTP client and call CALLBACK."
  (require 'url-parse)
  (url-retrieve (if (url-p url) (url-recreate-url url) url) callback cbargs))

(defun url-http-expand-file-name (urlobj defobj)
  "Expand URLOBJ relative to DEFOBJ (GNU aliases `url-default-expander')."
  (require 'url-expand)
  (url-default-expander urlobj defobj))

(defalias 'url-https 'url-http)
(defalias 'url-https-expand-file-name 'url-http-expand-file-name)

;; byte-opt.el (verbatim): the side-effect-free / pure function
;; properties.  GNU installs these whenever byte-opt loads (which real
;; sessions do early via bytecomp); pcase--split-pred consults
;; `side-effect-free' to fold predicate calls over quoted values and
;; prune shadowed branches.  byte-opt.el itself must not load (it would
;; drag bytecomp over the native compiler).
(let ((side-effect-free-fns
       '(
         ;; alloc.c
         make-bool-vector make-byte-code make-list make-record make-string
         make-symbol make-vector
         ;; buffer.c
         buffer-base-buffer buffer-chars-modified-tick buffer-file-name
         buffer-local-value buffer-local-variables buffer-modified-p
         buffer-modified-tick buffer-name get-buffer next-overlay-change
         overlay-buffer overlay-end overlay-get overlay-properties
         overlay-start overlays-at overlays-in previous-overlay-change
         ;; callint.c
         prefix-numeric-value
         ;; casefiddle.c
         capitalize downcase upcase upcase-initials
         ;; category.c
         category-docstring category-set-mnemonics char-category-set
         copy-category-table get-unused-category make-category-set
         ;; character.c
         char-width get-byte multibyte-char-to-unibyte string string-width
         unibyte-char-to-multibyte unibyte-string
         ;; charset.c
         decode-char encode-char
         ;; chartab.c
         make-char-table
         ;; data.c
         % * + - / /= 1+ 1- < <= = > >=
         aref ash bare-symbol
         bool-vector-count-consecutive bool-vector-count-population
         bool-vector-subsetp
         boundp car cdr default-boundp default-value fboundp
         get-variable-watchers indirect-variable
         local-variable-if-set-p local-variable-p
         logand logcount logior lognot logxor max min mod
         number-to-string position-symbol string-to-number
         subr-arity subr-name subr-native-lambda-list subr-type
         symbol-function symbol-name symbol-plist symbol-value
         symbol-with-pos-pos variable-binding-locus
         ;; doc.c
         documentation
         ;; editfns.c
         buffer-substring buffer-substring-no-properties
         byte-to-position byte-to-string
         char-after char-before char-equal char-to-string
         compare-buffer-substrings
         format format-message
         group-name
         line-beginning-position line-end-position ngettext pos-bol pos-eol
         propertize region-beginning region-end string-to-char
         user-full-name user-login-name
         ;; eval.c
         special-variable-p
         ;; fileio.c
         car-less-than-car directory-name-p file-directory-p file-exists-p
         file-name-absolute-p file-name-concat file-newer-than-file-p
         file-readable-p file-symlink-p file-writable-p
         ;; filelock.c
         file-locked-p
         ;; floatfns.c
         abs acos asin atan ceiling copysign cos exp expt fceiling ffloor
         float floor frexp fround ftruncate isnan ldexp log logb round
         sin sqrt tan
         truncate
         ;; fns.c
         append assq
         base64-decode-string base64-encode-string base64url-encode-string
         buffer-hash buffer-line-statistics
         compare-strings concat copy-alist copy-hash-table copy-sequence elt
         equal equal-including-properties
         featurep get
         gethash hash-table-count hash-table-rehash-size
         hash-table-rehash-threshold hash-table-size hash-table-test
         hash-table-weakness
         length length< length= length>
         line-number-at-pos load-average locale-info make-hash-table md5
         member memq memql nth nthcdr
         object-intervals rassoc rassq reverse secure-hash
         string-as-multibyte string-as-unibyte string-bytes
         string-collate-equalp string-collate-lessp string-distance
         string-equal string-lessp string-make-multibyte string-make-unibyte
         string-search string-to-multibyte string-to-unibyte
         string-version-lessp
         substring substring-no-properties
         sxhash-eq sxhash-eql sxhash-equal sxhash-equal-including-properties
         take value< vconcat
         ;; frame.c
         frame-ancestor-p frame-bottom-divider-width frame-char-height
         frame-char-width frame-child-frame-border-width frame-focus
         frame-fringe-width frame-internal-border-width frame-native-height
         frame-native-width frame-parameter frame-parameters frame-parent
         frame-pointer-visible-p frame-position frame-right-divider-width
         frame-scale-factor frame-scroll-bar-height frame-scroll-bar-width
         frame-text-cols frame-text-height frame-text-lines frame-text-width
         frame-total-cols frame-total-lines frame-visible-p
         frame-window-state-change next-frame previous-frame
         tool-bar-pixel-width window-system
         ;; fringe.c
         fringe-bitmaps-at-pos
         ;; keyboard.c
         posn-at-point posn-at-x-y
         ;; keymap.c
         copy-keymap keymap-parent keymap-prompt make-keymap make-sparse-keymap
         ;; lread.c
         intern-soft read-from-string
         ;; marker.c
         copy-marker marker-buffer marker-insertion-type marker-position
         ;; minibuf.c
         active-minibuffer-window assoc-string innermost-minibuffer-p
         minibuffer-innermost-command-loop-p minibufferp
         ;; print.c
         error-message-string prin1-to-string
         ;; process.c
         format-network-address get-buffer-process get-process
         process-buffer process-coding-system process-command process-filter
         process-id process-inherit-coding-system-flag process-mark
         process-name process-plist process-query-on-exit-flag
         process-running-child-p process-sentinel process-thread
         process-tty-name process-type
         ;; search.c
         match-beginning match-end regexp-quote
         ;; sqlite.c
         sqlite-columns sqlite-more-p sqlite-version
         ;; syntax.c
         char-syntax copy-syntax-table matching-paren string-to-syntax
         syntax-class-to-char
         ;; term.c
         controlling-tty-p tty-display-color-cells tty-display-color-p
         tty-top-frame tty-type
         ;; terminal.c
         frame-terminal terminal-list terminal-live-p terminal-name
         terminal-parameter terminal-parameters
         ;; textprop.c
         get-char-property get-char-property-and-overlay get-text-property
         next-char-property-change next-property-change
         next-single-char-property-change next-single-property-change
         previous-char-property-change previous-property-change
         previous-single-char-property-change previous-single-property-change
         text-properties-at text-property-any text-property-not-all
         ;; thread.c
         all-threads condition-mutex condition-name mutex-name thread-live-p
         thread-name
         ;; timefns.c
         current-cpu-time
         current-time-string current-time-zone decode-time encode-time
         float-time format-time-string time-add time-convert time-equal-p
         time-less-p time-subtract
         ;; window.c
         coordinates-in-window-p frame-first-window frame-root-window
         frame-selected-window get-buffer-window minibuffer-selected-window
         minibuffer-window next-window previous-window window-at
         window-body-height window-body-width window-buffer
         window-combination-limit window-configuration-equal-p
         window-dedicated-p window-display-table window-frame window-fringes
         window-hscroll window-left-child window-left-column window-margins
         window-minibuffer-p window-new-normal window-new-total
         window-next-buffers window-next-sibling window-normal-size
         window-parameter window-parameters window-parent window-point
         window-prev-buffers window-prev-sibling window-scroll-bars
         window-start window-text-height window-top-child window-top-line
         window-total-height window-total-width window-use-time window-vscroll
         ;; xdisp.c
         buffer-text-pixel-size current-bidi-paragraph-direction
         get-display-property invisible-p line-pixel-height lookup-image-map
         tab-bar-height tool-bar-height window-text-pixel-size
         ))
      (side-effect-and-error-free-fns
       '(
         ;; alloc.c
         bool-vector cons list make-marker purecopy record vector
         ;; buffer.c
         buffer-list buffer-live-p current-buffer overlay-lists overlayp
         ;; casetab.c
         case-table-p current-case-table standard-case-table
         ;; category.c
         category-table category-table-p make-category-table
         standard-category-table
         ;; character.c
         characterp max-char
         ;; charset.c
         charsetp
         ;; data.c
         arrayp atom bare-symbol-p bool-vector-p bufferp byte-code-function-p
         interpreted-function-p closurep
         byteorder car-safe cdr-safe char-or-string-p char-table-p
         condition-variable-p consp eq floatp indirect-function
         integer-or-marker-p integerp keywordp listp markerp
         module-function-p multibyte-string-p mutexp native-comp-function-p
         natnump nlistp null
         number-or-marker-p numberp recordp remove-pos-from-symbol
         sequencep stringp subrp symbol-with-pos-p symbolp
         threadp type-of user-ptrp vector-or-char-table-p vectorp wholenump
         ;; editfns.c
         bobp bolp buffer-size buffer-string current-message emacs-pid
         eobp eolp following-char gap-position gap-size group-gid
         group-real-gid mark-marker point point-marker point-max point-min
         position-bytes preceding-char system-name
         user-real-login-name user-real-uid user-uid
         ;; emacs.c
         invocation-directory invocation-name
         ;; eval.c
         commandp functionp
         ;; fileio.c
         default-file-modes
         ;; fns.c
         eql
         hash-table-p identity proper-list-p safe-length
         secure-hash-algorithms
         ;; frame.c
         frame-list frame-live-p framep last-nonminibuffer-frame
         old-selected-frame selected-frame visible-frame-list
         ;; image.c
         imagep
         ;; indent.c
         current-column current-indentation
         ;; keyboard.c
         current-idle-time current-input-mode recent-keys recursion-depth
         this-command-keys this-command-keys-vector this-single-command-keys
         this-single-command-raw-keys
         ;; keymap.c
         current-global-map current-local-map current-minor-mode-maps keymapp
         ;; minibuf.c
         minibuffer-contents minibuffer-contents-no-properties minibuffer-depth
         minibuffer-prompt minibuffer-prompt-end
         ;; process.c
         process-list processp signal-names waiting-for-user-input-p
         ;; sqlite.c
         sqlite-available-p sqlitep
         ;; syntax.c
         standard-syntax-table syntax-table syntax-table-p
         ;; thread.c
         current-thread
         ;; timefns.c
         current-time
         ;; window.c
         selected-window window-configuration-p window-live-p window-valid-p
         windowp
         ;; xdisp.c
         long-line-optimizations-p
         )))
  (while side-effect-free-fns
    (put (car side-effect-free-fns) 'side-effect-free t)
    (setq side-effect-free-fns (cdr side-effect-free-fns)))
  (while side-effect-and-error-free-fns
    (put (car side-effect-and-error-free-fns) 'side-effect-free 'error-free)
    (setq side-effect-and-error-free-fns (cdr side-effect-and-error-free-fns)))
  nil)


;; Pure functions are side-effect free functions whose values depend
;; only on their arguments, not on the platform.  For these functions,
;; calls with constant arguments can be evaluated at compile time.
;; For example, ash is pure since its results are machine-independent,
;; whereas lsh is not pure because (lsh -1 -1)'s value depends on the
;; fixnum range.
;;
;; When deciding whether a function is pure, do not worry about
;; mutable strings or markers, as they are so unlikely in real code
;; that they are not worth worrying about.  Thus string-to-char is
;; pure even though it might return different values if a string is
;; changed, and logand is pure even though it might return different
;; values if a marker is moved.

(let ((pure-fns
       '(
         ;; character.c
         characterp max-char
         ;; data.c
         % * + - / /= 1+ 1- < <= = > >= aref arrayp ash atom bare-symbol
         bool-vector-count-consecutive bool-vector-count-population
         bool-vector-p bool-vector-subsetp
         bufferp car car-safe cdr cdr-safe char-or-string-p char-table-p
         condition-variable-p consp eq floatp integer-or-marker-p integerp
         keywordp listp logand logcount logior lognot logxor markerp max min
         mod multibyte-string-p mutexp natnump nlistp null number-or-marker-p
         numberp recordp remove-pos-from-symbol sequencep stringp symbol-name
         symbolp threadp type-of vector-or-char-table-p vectorp
         ;; editfns.c
         string-to-char
         ;; floatfns.c
         abs ceiling copysign fceiling ffloor float floor fround ftruncate
         isnan ldexp logb round sqrt truncate
         ;; fns.c
         assq base64-decode-string base64-encode-string base64url-encode-string
         concat elt eql equal equal-including-properties
         hash-table-p identity length length< length=
         length> member memq memql nth nthcdr proper-list-p rassoc rassq
         safe-length string-bytes string-distance string-equal string-lessp
         string-search string-version-lessp take value<
         ;; search.c
         regexp-quote
         ;; syntax.c
         string-to-syntax
         )))
  (while pure-fns
    (put (car pure-fns) 'pure t)
    (setq pure-fns (cdr pure-fns)))
  nil)

;; isearch.el defcustom: how-many consults it for case folding.
(defvar search-upper-case 'not-yanks
  "If non-nil, upper case chars disable case fold searching.")

;; replace.el (verbatim): count-matches over a regexp.
(defun how-many (regexp &optional rstart rend interactive)
  "Print and return number of matches for REGEXP following point.
When called from Lisp and INTERACTIVE is omitted or nil, just return
the number, do not print it; if INTERACTIVE is t, the function behaves
in all respects as if it had been called interactively.

If REGEXP contains upper case characters (excluding those preceded by `\\')
and `search-upper-case' is non-nil, the matching is case-sensitive.

Second and third arg RSTART and REND specify the region to operate on.

Interactively, in Transient Mark mode when the mark is active, operate
on the contents of the region.  Otherwise, operate from point to the
end of (the accessible portion of) the buffer.

This function starts looking for the next match from the end of
the previous match.  Hence, it ignores matches that overlap
a previously found match."
  (interactive
   (keep-lines-read-args "How many matches for regexp"))
  (save-excursion
    (if rstart
        (if rend
            (progn
              (goto-char (min rstart rend))
              (setq rend (max rstart rend)))
          (goto-char rstart)
          (setq rend (point-max)))
      (if (and interactive (use-region-p))
	  (setq rstart (region-beginning)
		rend (region-end))
	(setq rstart (point)
	      rend (point-max)))
      (goto-char rstart))
    (let ((count 0)
	  (case-fold-search
	   (if (and case-fold-search search-upper-case)
	       (isearch-no-upper-case-p regexp t)
	     case-fold-search)))
      (while (and (< (point) rend)
		  (re-search-forward regexp rend t))
        ;; Ensure forward progress on zero-length matches like "^$".
        (when (and (= (match-beginning 0) (match-end 0))
                   (not (eobp)))
          (forward-char 1))
	(setq count (1+ count)))
      (when interactive (message (ngettext "%d occurrence"
					   "%d occurrences"
					   count)
				 count))
      count)))
(defalias 'count-matches 'how-many)

;; subr.el (verbatim): pp-emacs-lisp-code copies its temp buffer out.
(defun insert-into-buffer (buffer &optional start end)
  "Insert the contents of the current buffer into BUFFER.
If START/END, only insert that region from the current buffer.
Point in BUFFER will be placed after the inserted text."
  (let ((current (current-buffer)))
    (with-current-buffer buffer
      (insert-buffer-substring current start end))))

;; tabify.el (verbatim): pp's code-format tests untabify their output.
(defun untabify (start end &optional _arg)
  "Convert all tabs in region to multiple spaces, preserving columns.
If called interactively with prefix ARG, convert for the entire
buffer.

Called non-interactively, the region is specified by arguments
START and END, rather than by the position of point and mark.
The variable `tab-width' controls the spacing of tab stops."
  (interactive (if current-prefix-arg
		   (list (point-min) (point-max) current-prefix-arg)
		 (list (region-beginning) (region-end) nil)))
  (let ((c (current-column)))
    (save-excursion
      (save-restriction
        (narrow-to-region (point-min) end)
        (goto-char start)
        (while (search-forward "\t" nil t)      ; faster than re-search
          (forward-char -1)
          (let ((tab-beg (point))
                (indent-tabs-mode nil)
                column)
            (skip-chars-forward "\t")
            (setq column (current-column))
            (delete-region tab-beg (point))
            (indent-to column)))))
    (move-to-column c)))

;; subr.el: whether the current command should use a dialog box.  The
;; emaxx batch harness is always a TTY, so the mouse/menu branches never
;; fire; the defvars keep the guards valid.
(defvar from--tty-menu-p nil
  "Non-nil means the current command was invoked via a TTY menu.")
(defvar use-dialog-box-override nil
  "If non-nil, `use-dialog-box-p' always returns non-nil.")
(defun use-dialog-box-p ()
  "Return non-nil if the current command should prompt the user via a dialog box."
  (or use-dialog-box-override
      (and last-input-event                 ; not during startup
           (or (consp last-nonmenu-event)   ; invoked by a mouse event
               (and (null last-nonmenu-event)
                    (consp last-input-event))
               from--tty-menu-p)            ; invoked via TTY menu
           use-dialog-box)))

;; subr.el (verbatim): if-let*/when-let*/and-let* MACROS.  emaxx
;; evaluates these via native special forms (checked first in the eval
;; dispatch, so they stay fast), but `macroexpand' consults the macro
;; table, and subr-x-tests checks the exact GNU `let*' expansion.
(defun internal--build-binding (binding prev-var)
  "Check and build a single BINDING with PREV-VAR."
  (setq binding
        (cond
         ((symbolp binding)
          (list binding binding))
         ((null (cdr binding))
          (list (make-symbol "s") (car binding)))
         ((eq '_ (car binding))
          (list (make-symbol "s") (cadr binding)))
         (t binding)))
  (when (> (length binding) 2)
    (signal 'error
            (cons "`let' bindings can have only one value-form" binding)))
  (let ((var (car binding)))
    `(,var (and ,prev-var ,(cadr binding)))))

(defun internal--build-bindings (bindings)
  "Check and build conditional value forms for BINDINGS."
  (let ((prev-var t))
    (mapcar (lambda (binding)
              (let ((binding (internal--build-binding binding prev-var)))
                (setq prev-var (car binding))
                binding))
            bindings)))

(defmacro if-let* (varlist then &rest else)
  "Bind variables according to VARLIST and evaluate THEN or ELSE."
  (declare (indent 2)
           (debug ((&rest [&or symbolp (symbolp form) (form)])
                   body)))
  (if varlist
      `(let* ,(setq varlist (internal--build-bindings varlist))
         (if ,(caar (last varlist))
             ,then
           ,@else))
    `(let* () ,then)))

(defmacro when-let* (varlist &rest body)
  "Bind variables according to VARLIST and conditionally evaluate BODY."
  (declare (indent 1) (debug if-let*))
  (list 'if-let* varlist (macroexp-progn body)))

(defmacro and-let* (varlist &rest body)
  "Bind variables according to VARLIST and conditionally evaluate BODY."
  (declare (indent 1) (debug if-let*))
  (let (res)
    (if varlist
        `(let* ,(setq varlist (internal--build-bindings varlist))
           (when ,(setq res (caar (last varlist)))
             ,@(or body `(,res))))
      `(let* () ,@(or body '(t))))))

;; subr.el (verbatim): split a string into lines (subr-x-tests, and used
;; by string-limit's tests).
(defun string-lines (string &optional omit-nulls keep-newlines)
  "Split STRING into a list of lines.
If OMIT-NULLS, empty lines will be removed from the results.
If KEEP-NEWLINES, don't strip trailing newlines from the result
lines."
  (declare (side-effect-free t))
  (if (equal string "")
      (if omit-nulls
          nil
        (list ""))
    (let ((lines nil)
          (start 0))
      (while (< start (length string))
        (let ((newline (string-search "\n" string start)))
          (if newline
              (progn
                (when (or (not omit-nulls)
                          (not (= start newline)))
                  (let ((line (substring string start
                                         (if keep-newlines
                                             (1+ newline)
                                           newline))))
                    (when (not (and keep-newlines omit-nulls
                                    (equal line "\n")))
                      (push line lines))))
                (setq start (1+ newline)))
            ;; No newline in the remaining part.
            (if (zerop start)
                ;; Avoid a string copy if there are no newlines at all.
                (push string lines)
              (push (substring string start) lines))
            (setq start (length string)))))
      (nreverse lines))))

;;; shortdoc.el support: functions listed in the built-in documentation
;;; groups so `shortdoc-all-functions-fboundp' passes and the groups
;;; display without error.  Ports are verbatim from the GNU sources
;;; (subr.el, files.el, simple.el) except where a self-contained
;;; equivalent avoids dragging preloaded internals.

;; `rx-let-eval' is provided by GNU rx.el, which emaxx loads lazily on the
;; first rx form.  Mirror GNU's loaddefs autoload so it is `fboundp' before
;; that first use (e.g. for `shortdoc-all-functions-fboundp'); an actual call
;; loads rx.el via the native trigger.
(autoload 'rx-let-eval "rx" "Evaluate BODY with local rx-definitions." nil 'macro)

(defmacro noreturn (form)
  "Evaluate FORM, expecting it not to return.
If FORM does return, signal an error."
  (declare (debug t))
  `(prog1 ,form
     (error "Form marked with `noreturn' did return")))

(defmacro 1value (form)
  "Evaluate FORM, expecting a constant return value.
If FORM returns differing values when running under Testcover,
Testcover will raise an error."
  (declare (debug t))
  form)

(defun timer-next-integral-multiple-of-time (time secs)
  "Yield the next value after TIME that is an integral multiple of SECS.
More precisely, the next value, after TIME, that is an integral multiple
of SECS seconds since the epoch.  SECS may be a fraction."
  (let* ((ticks-hz (time-convert time t))
	 (ticks (car ticks-hz))
	 (hz (cdr ticks-hz))
	 trunc-s-ticks)
    (while (let ((s-ticks (* secs hz)))
	     (setq trunc-s-ticks (truncate s-ticks))
	     (/= s-ticks trunc-s-ticks))
      (setq ticks (ash ticks 1))
      (setq hz (ash hz 1)))
    (let ((more-ticks (+ ticks trunc-s-ticks)))
      (time-convert (cons (- more-ticks (% more-ticks trunc-s-ticks)) hz) t))))

;; GNU subr.el: `not' is an alias of `null', so `function-get' reads
;; null's `side-effect-free' property through the alias (unsafep).
(defalias 'not #'null)

;; GNU simple.el (preloaded): viper reads this at load time.
(defvar next-line-add-newlines nil
  "If non-nil, `next-line' inserts newline to avoid `end of buffer' error.")

;; GNU C display variables (window.c/xdisp.c defaults): viper reads and
;; let-binds these at load time.
(defvar scroll-step 0)
(defvar scroll-conservatively 0)
(defvar scroll-margin 0)
(defvar global-mode-string nil)
(defvar mark-even-if-inactive t)
(defvar emulation-mode-map-alists nil)
(defvar initial-major-mode 'lisp-interaction-mode)
(defvar-local abbrev-mode nil)
(defvar-local auto-fill-function nil)
(defalias 'beep #'ding)

;; GNU mule-cmds.el (preloaded): the input-method state surface, enough
;; for viper's conditional deactivation in batch (no input method active).
(defvar current-input-method nil)
(defvar current-transient-input-method nil)
(defvar current-input-method-title nil)
(defvar input-method-history nil)
(defvar input-method-deactivate-hook nil)
(defvar deactivate-current-input-method-function nil)

(defvar history-length 100)
(defvar history-delete-duplicates nil)

(defun add-to-history (history-var newelt &optional maxelt keep-all)
  "Add NEWELT to the history list stored in the variable HISTORY-VAR.
Return the new history list.
If MAXELT is non-nil, it specifies the maximum length of the history.
Otherwise, the maximum history length is the value of the `history-length'
property on symbol HISTORY-VAR, if set, or the value of the `history-length'
variable.
Remove duplicates of NEWELT if `history-delete-duplicates' is non-nil.
If optional fourth arg KEEP-ALL is non-nil, add NEWELT to history even
if it is empty or duplicates the most recent entry in the history.
HISTORY-VAR cannot refer to a lexical variable."
  (unless maxelt
    (setq maxelt (or (get history-var 'history-length)
		     history-length)))
  (let ((history (symbol-value history-var))
	tail)
    (when (and (listp history)
	       (or keep-all
		   (not (stringp newelt))
		   (> (length newelt) 0))
	       (or keep-all
		   (not (equal (car history) newelt))))
      (if history-delete-duplicates
	  (setq history (delete newelt history)))
      (setq history (cons newelt history))
      (when (integerp maxelt)
        (if (>= 0 maxelt)
	    (setq history nil)
	  (setq tail (nthcdr (1- maxelt) history))
	  (when (consp tail)
            (setcdr tail nil))))
      (set history-var history))))

(defun deactivate-input-method ()
  "Turn off the current input method."
  (when current-input-method
    (unless current-transient-input-method
      (add-to-history 'input-method-history current-input-method))
    (unwind-protect
	(progn
	  (setq input-method-function nil
		current-input-method-title nil)
	  (funcall deactivate-current-input-method-function))
      (unwind-protect
	  (run-hooks 'input-method-deactivate-hook)
	(setq current-input-method nil)
	(force-mode-line-update)))))

(defun shell-command-to-string (command)
  "Execute shell command COMMAND and return its output as a string.
Use `shell-quote-argument' to quote dangerous characters in
COMMAND before passing it as an argument to this function."
  (with-output-to-string
    (with-current-buffer standard-output
      (shell-command command t))))

(defun load-library (library &optional _interactive-call)
  "Load the Emacs Lisp library named LIBRARY.
LIBRARY should be a string.  This is an interface to the function `load'."
  (interactive "sLoad library: ")
  (load library))

(defun add-to-ordered-list (list-var element &optional order)
  "Add ELEMENT to the value of LIST-VAR if it isn't there yet.
The test for presence of ELEMENT is done with `eq'.

The value of LIST-VAR is kept ordered based on the ORDER
parameter.  The list order for each element is stored in
LIST-VAR's `list-order' property.

The return value is the new value of LIST-VAR."
  (let ((ordering (get list-var 'list-order)))
    (unless ordering
      (put list-var 'list-order
           (setq ordering (make-hash-table :weakness 'key :test 'eq))))
    (when order
      (puthash element (and (numberp order) order) ordering))
    (unless (memq element (symbol-value list-var))
      (set list-var (cons element (symbol-value list-var))))
    (set list-var (sort (symbol-value list-var)
			(lambda (a b)
			  (let ((oa (gethash a ordering))
				(ob (gethash b ordering)))
			    (if (and oa ob)
				(< oa ob)
			      oa)))))))

;; GNU format.el (preloaded) marks `format-alist' risky.
(put 'format-alist 'risky-local-variable t)

;; GNU mule.el (preloaded): the ASCII case table, saved in case a locale
;; modifies ASCII case behavior.  erc builds its IRC casemapping from it.
(defvar ascii-case-table
  ;; Code copied from copy-case-table to avoid requiring case-table.el
  (let ((tbl (copy-sequence (standard-case-table)))
	(up  (char-table-extra-slot (standard-case-table) 0)))
    (if up (set-char-table-extra-slot tbl 0 (copy-sequence up)))
    (set-char-table-extra-slot tbl 1 nil)
    (set-char-table-extra-slot tbl 2 nil)
    tbl)
  "Case table for the ASCII character set.")

(defmacro while-let (spec &rest body)
  "Bind variables according to SPEC and conditionally evaluate BODY.
Evaluate each binding in turn, stopping if a binding value is nil.
If all bindings are non-nil, eval BODY and repeat.

The variable list SPEC is the same as in `if-let*'."
  (declare (indent 1) (debug if-let))
  (let ((done (gensym "done")))
    `(catch ',done
       (while t
         ;; This is `if-let*', not `if-let', deliberately, despite the
         ;; name of this macro.  See bug#60758.
         (if-let* ,spec
             (progn
               ,@body)
           (throw ',done nil))))))

(defun backtrace-frames (&optional base)
  "Collect all frames of current backtrace into a list.
If non-nil, BASE should be a function, and frames before its
nearest activation frame are discarded."
  (let ((frames nil))
    (mapbacktrace (lambda (&rest frame) (push frame frames))
                  (or base #'backtrace-frames))
    (nreverse frames)))

(defun risky-local-variable-p (sym &optional _ignored)
  "Non-nil if SYM could be dangerous as a file-local variable.
It is dangerous if either of these conditions are met:

 * Its `risky-local-variable' property is non-nil.

 * Its name ends with \"hook(s)\", \"function(s)\", \"form(s)\", \"map\",
   \"program\", \"command(s)\", \"predicate(s)\", \"frame-alist\",
   \"mode-alist\", \"font-lock-(syntactic-)keyword*\",
   \"map-alist\", or \"bindat-spec\"."
  ;; If this is an alias, check the base name.
  (condition-case nil
      (setq sym (indirect-variable sym))
    (error nil))
  (or (get sym 'risky-local-variable)
      (string-match "-hooks?$\\|-functions?$\\|-forms?$\\|-program$\\|\
-commands?$\\|-predicates?$\\|font-lock-keywords$\\|font-lock-keywords\
-[0-9]+$\\|font-lock-syntactic-keywords$\\|-frame-alist$\\|-mode-alist$\\|\
-map$\\|-map-alist$\\|-bindat-spec$" (symbol-name sym))))

(defun make-separator-line (&optional length)
  "Make a string appropriate for usage as a visual separator line.
This uses the `separator-line' face.

If LENGTH is nil, use the window width."
  (if (or (display-graphic-p)
          (display-supports-face-attributes-p '(:underline t)))
      (if length
          (concat (propertize (make-string length ?\s) 'face 'separator-line)
                  "\n")
        (propertize "\n" 'face '(:inherit separator-line :extend t)))
    (concat (propertize (make-string (or length (1- (window-width))) ?-)
                        'face 'separator-line)
            "\n")))

(defun string-or-null-p (object)
  "Return t if OBJECT is a string or nil.
Otherwise, return nil."
  (declare (pure t) (side-effect-free error-free))
  (or (stringp object) (null object)))

(defun string-greaterp (string1 string2)
  "Return non-nil if STRING1 is greater than STRING2 in lexicographic order.
Case is significant.
Symbols are also allowed; their print names are used instead."
  (declare (pure t) (side-effect-free t))
  (string-lessp string2 string1))

(defun assoc-default (key alist &optional test default)
  "Find object KEY in a pseudo-alist ALIST.
ALIST is a list of conses or objects.  Each element
 (or the element's car, if it is a cons) is compared with KEY by
 calling TEST, with two arguments: (i) the element or its car,
 and (ii) KEY.
If that is non-nil, the element matches; then `assoc-default'
 returns the element's cdr, if it is a cons, or DEFAULT if the
 element is not a cons.

If no element matches, the value is nil.
If TEST is omitted or nil, `equal' is used."
  (declare (important-return-value t))
  (let (found (tail alist) value)
    (while (and tail (not found))
      (let ((elt (car tail)))
	(when (funcall (or test #'equal) (if (consp elt) (car elt) elt) key)
	  (setq found t value (if (consp elt) (cdr elt) default))))
      (setq tail (cdr tail)))
    value))

(defun char-uppercase-p (char)
  "Return non-nil if CHAR is an upper-case character.
If the Unicode tables are not yet available, e.g. during bootstrap,
then gives correct answers only for ASCII characters."
  (cond ((unicode-property-table-internal 'lowercase)
         (characterp (get-char-code-property char 'lowercase)))
        ((<= ?A char ?Z))))

(defun split-string-and-unquote (string &optional separator)
  "Split the STRING into a list of strings.
It understands Emacs Lisp quoting within STRING, such that
  (split-string-and-unquote (combine-and-quote-strings strs)) == strs
The SEPARATOR regexp defaults to \"\\s-+\"."
  (declare (important-return-value t))
  (let ((sep (or separator "\\s-+"))
	(i (string-search "\"" string)))
    (if (null i)
	(split-string string sep t)	; no quoting:  easy
      (append (unless (eq i 0) (split-string (substring string 0 i) sep t))
	      (let ((rfs (read-from-string string i)))
		(cons (car rfs)
		      (split-string-and-unquote (substring string (cdr rfs))
						sep)))))))

(defun split-string-shell-command (string)
  "Split STRING (a shell command) into a list of strings.
General shell syntax, like single and double quoting, as well as
backslash quoting, is respected."
  (let ((pos 0) (len (length string)) (args nil) (cur nil) (in-word nil))
    (while (< pos len)
      (let ((ch (aref string pos)))
        (cond
         ((and (not in-word) (memq ch '(?\s ?\t ?\n)))
          (setq pos (1+ pos)))
         ((memq ch '(?\s ?\t ?\n))
          (push cur args) (setq cur nil in-word nil pos (1+ pos)))
         ((eq ch ?\')
          (setq in-word t)
          (let ((end (or (string-search "'" string (1+ pos)) len)))
            (setq cur (concat cur (substring string (1+ pos) end))
                  pos (min len (1+ end)))))
         ((eq ch ?\")
          (setq in-word t pos (1+ pos))
          (while (and (< pos len) (not (eq (aref string pos) ?\")))
            (if (and (eq (aref string pos) ?\\) (< (1+ pos) len))
                (setq cur (concat cur (char-to-string (aref string (1+ pos))))
                      pos (+ pos 2))
              (setq cur (concat cur (char-to-string (aref string pos)))
                    pos (1+ pos))))
          (setq pos (1+ pos)))
         ((eq ch ?\\)
          (setq in-word t)
          (when (< (1+ pos) len)
            (setq cur (concat cur (char-to-string (aref string (1+ pos))))))
          (setq pos (+ pos 2)))
         (t (setq in-word t cur (concat cur (char-to-string ch)) pos (1+ pos))))))
    (when (or in-word cur) (push (or cur "") args))
    (nreverse args)))

(defun get-char-property-and-overlay (position prop &optional object)
  "Like `get-char-property', but with extra overlay information.
The value is a cons cell.  Its car is the return value of
`get-char-property' with the same arguments.  Its cdr is the overlay in
which the property was found, or nil if it was found as a text property
or not found at all."
  (let ((overlay nil) (val nil))
    (unless (stringp object)
      (let ((buffer (cond ((bufferp object) object)
                          ((windowp object) (window-buffer object))
                          (t (current-buffer)))))
        (with-current-buffer buffer
          (catch 'done
            (dolist (ov (overlays-at position))
              (let ((v (overlay-get ov prop)))
                (when v (setq val v overlay ov) (throw 'done nil))))))))
    (if overlay
        (cons val overlay)
      (cons (get-char-property position prop object) nil))))

(defun string-glyph-compose (string)
  "Compose STRING according to the Unicode NFC."
  (ucs-normalize-NFC-string string))

(defun string-glyph-decompose (string)
  "Decompose STRING according to the Unicode NFD."
  (ucs-normalize-NFD-string string))

(defun next-property-change (position &optional object limit)
  "Return the position of next property change from POSITION.
Scan forward in OBJECT (a buffer or string, defaulting to the current
buffer) until the text properties differ from those at POSITION, and
return that position.  Return nil (or LIMIT if given) if none is found."
  (let* ((end (cond (limit limit)
                    ((stringp object) (length object))
                    (t (point-max))))
         (initial (text-properties-at position object))
         (pos position))
    (setq pos (1+ pos))
    (while (and (< pos end)
                (equal (text-properties-at pos object) initial))
      (setq pos (1+ pos)))
    (cond ((< pos end) pos)
          (limit limit)
          (t nil))))

(defun previous-property-change (position &optional object limit)
  "Return the position of previous property change from POSITION.
Scan backward in OBJECT (a buffer or string, defaulting to the current
buffer) until the text properties differ from those just before
POSITION, and return that position.  Return nil (or LIMIT if given) if
none is found."
  (let* ((start (cond (limit limit)
                      ((stringp object) 0)
                      (t (point-min))))
         (initial (text-properties-at (1- position) object))
         (pos position))
    (setq pos (1- pos))
    (while (and (> pos start)
                (equal (text-properties-at (1- pos) object) initial))
      (setq pos (1- pos)))
    (cond ((> pos start) pos)
          (limit limit)
          (t nil))))

(defun file-name-with-extension (filename extension)
  "Return FILENAME modified to have the specified EXTENSION.
The extension (in a file name) is the part that begins with the last \".\".
This function removes any existing extension from FILENAME, and then
appends EXTENSION to it.

EXTENSION may include the leading dot; if it doesn't, this function
will provide it.

It is an error if FILENAME or EXTENSION is empty, or if FILENAME
is in the form of a directory name according to `directory-name-p'."
  (let ((extn (string-trim-left extension "[.]")))
    (cond ((string-empty-p filename)
           (error "Empty filename"))
          ((string-empty-p extn)
           (error "Malformed extension: %s" extension))
          ((directory-name-p filename)
           (error "Filename is a directory: %s" filename))
          (t
           (concat (file-name-sans-extension filename) "." extn)))))

(defun file-name-parent-directory (filename)
  "Return the directory name of the parent directory of FILENAME.
If FILENAME is at the root of the filesystem, return nil.
If FILENAME is relative, it is interpreted to be relative
to `default-directory', and the result will also be relative."
  (let* ((expanded-filename (expand-file-name filename))
         (parent (file-name-directory (directory-file-name expanded-filename))))
    (cond
     ((or (null parent)
          (equal parent expanded-filename))
      nil)
     ((not (file-name-absolute-p filename))
      (file-relative-name parent))
     (t
      parent))))

(defsubst file-name-quoted-p (name &optional top)
  "Whether NAME is quoted with prefix \"/:\".
If NAME is a remote file name and TOP is nil, check the local part of NAME."
  (let ((file-name-handler-alist (unless top file-name-handler-alist)))
    (string-prefix-p "/:" (file-local-name name))))

(defsubst file-name-quote (name &optional top)
  "Add the quotation prefix \"/:\" to file NAME.
If NAME is a remote file name and TOP is nil, the local part of
NAME is quoted.  If NAME is already a quoted file name, NAME is
returned unchanged."
  (let ((file-name-handler-alist (unless top file-name-handler-alist)))
    (if (file-name-quoted-p name top)
        name
      (concat (file-remote-p name) "/:" (file-local-name name)))))

(defsubst file-name-unquote (name &optional top)
  "Remove quotation prefix \"/:\" from file NAME, if any.
If NAME is a remote file name and TOP is nil, the local part of
NAME is unquoted."
  (let* ((file-name-handler-alist (unless top file-name-handler-alist))
         (localname (file-local-name name)))
    (when (file-name-quoted-p localname top)
      (setq
       localname (if (= (length localname) 2) "/" (substring localname 2))))
    (concat (file-remote-p name) localname)))

(defun file-modes-char-to-who (char)
  "Convert CHAR to a numeric bit-mask for extracting mode bits.
CHAR is in [ugoa] and represents the category of users (Owner, Group,
Others, or All) for whom to produce the mask."
  (cond ((eq char ?u) #o4700)
	((eq char ?g) #o2070)
	((eq char ?o) #o1007)
	((eq char ?a) #o7777)
        (t (error "%c: Bad `who' character" char))))

(defun file-modes-char-to-right (char &optional from)
  "Convert CHAR to a numeric value of mode bits.
CHAR is in [rwxXstugo] and represents symbolic access permissions.
If CHAR is in [Xugo], the value is taken from FROM (or 0 if omitted)."
  (or from (setq from 0))
  (cond ((eq char ?r) #o0444)
	((eq char ?w) #o0222)
	((eq char ?x) #o0111)
	((eq char ?s) #o6000)
	((eq char ?t) #o1000)
	((eq char ?X) (if (= (logand from #o111) 0) 0 #o0111))
	((eq char ?u) (let ((uright (logand #o4700 from)))
		        (+ uright (/ uright #o10) (/ uright #o100))))
	((eq char ?g) (let ((gright (logand #o2070 from)))
		        (+ gright (/ gright #o10) (* gright #o10))))
	((eq char ?o) (let ((oright (logand #o1007 from)))
		        (+ oright (* oright #o10) (* oright #o100))))
        (t (error "%c: Bad right character" char))))

(defun file-modes-rights-to-number (rights who-mask &optional from)
  "Convert a symbolic mode string specification to an equivalent number.
RIGHTS is the symbolic mode spec, it should match \"([+=-][rwxXstugo]*)+\".
WHO-MASK is the bit-mask specifying the category of users to which to
apply the access permissions.  See `file-modes-char-to-who'.
FROM (or 0 if nil) gives the mode bits on which to base permissions if
RIGHTS request to add, remove, or set permissions based on existing ones."
  (let* ((num-rights (or from 0))
	 (list-rights (string-to-list rights))
	 (op (pop list-rights)))
    (while (memq op '(?+ ?- ?=))
      (let ((num-right 0)
	    char-right)
	(while (memq (setq char-right (pop list-rights))
		     '(?r ?w ?x ?X ?s ?t ?u ?g ?o))
	  (setq num-right
		(logior num-right
			(file-modes-char-to-right char-right num-rights))))
	(setq num-right (logand who-mask num-right)
	      num-rights
	      (cond ((= op ?+) (logior num-rights num-right))
		    ((= op ?-) (logand num-rights (lognot num-right)))
		    (t (logior (logand num-rights (lognot who-mask)) num-right)))
	      op char-right)))
    num-rights))

(defun file-modes-symbolic-to-number (modes &optional from)
  "Convert symbolic file modes to numeric file modes.
MODES is the string to convert, it should match
\"[ugoa]*([+=-][rwxXstugo]*)+,...\".
FROM (or 0 if nil) gives the mode bits on which to base permissions."
  (save-match-data
    (let ((case-fold-search nil)
	  (num-modes (or from 0)))
      (while (/= (string-to-char modes) 0)
	(if (string-match "^\\([ugoa]*\\)\\([+=-][rwxXstugo]*\\)+\\(,\\|\\)" modes)
	    (let ((num-who (apply 'logior 0
				  (mapcar 'file-modes-char-to-who
					  (match-string 1 modes)))))
	      (when (= num-who 0)
		(setq num-who (logior #o7000 (default-file-modes))))
	      (setq num-modes
		    (file-modes-rights-to-number (substring modes (match-end 1))
						 num-who num-modes)
		    modes (substring modes (match-end 3))))
	  (error "Parse error in modes near `%s'" (substring modes 0))))
      num-modes)))

(defun match-substitute-replacement (replacement
				     &optional fixedcase literal string subexp)
  "Return REPLACEMENT as it will be inserted by `replace-match'.
In other words, all back-references in the form `\\&' and `\\N'
are substituted with actual strings matched by the last search.
Optional FIXEDCASE, LITERAL, STRING and SUBEXP have the same
meaning as for `replace-match'."
  (declare (side-effect-free t))
  (let ((match (match-string 0 string)))
    (save-match-data
      (set-match-data (mapcar (lambda (x)
                                (if (numberp x) (- x (match-beginning 0)) x))
                              (match-data t)))
      (replace-match replacement fixedcase literal match subexp))))

(defun replace-regexp-in-region (regexp replacement &optional start end)
  "Replace REGEXP with REPLACEMENT in the region from START to END.
The number of replaced occurrences are returned, or nil if REGEXP
doesn't exist in the region.

If START is nil, use the current point.  If END is nil, use `point-max'.

Comparisons and replacements are done with fixed case."
  (if start
      (when (< start (point-min))
        (error "Start before start of buffer"))
    (setq start (point)))
  (if end
      (when (> end (point-max))
        (error "End after end of buffer"))
    (setq end (point-max)))
  (save-excursion
    (goto-char start)
    (save-restriction
      (narrow-to-region start end)
      (let ((matches 0)
            (case-fold-search nil))
        (while (re-search-forward regexp nil t)
          (replace-match replacement t)
          (setq matches (1+ matches)))
        (and (not (zerop matches))
             matches)))))

(defun replace-string-in-region (string replacement &optional start end)
  "Replace STRING with REPLACEMENT in the region from START to END.
The number of replaced occurrences are returned, or nil if STRING
doesn't exist in the region.

If START is nil, use the current point.  If END is nil, use `point-max'.

Comparisons and replacements are done with fixed case."
  (if start
      (when (< start (point-min))
        (error "Start before start of buffer"))
    (setq start (point)))
  (if end
      (when (> end (point-max))
        (error "End after end of buffer"))
    (setq end (point-max)))
  (save-excursion
    (goto-char start)
    (save-restriction
      (narrow-to-region start end)
      (let ((matches 0)
            (case-fold-search nil))
        (while (search-forward string nil t)
          (delete-region (match-beginning 0) (match-end 0))
          (insert replacement)
          (setq matches (1+ matches)))
        (and (not (zerop matches))
             matches)))))

(defvar locate-dominating-stop-dir-regexp
  "\\`\\(?:[\\/][\\/][^\\/]+[\\/]\\|/\\(?:net\\|afs\\|\\.\\.\\.\\)/\\)\\'"
  "Regexp of directory names that stop the search in `locate-dominating-file'.")

(defun locate-dominating-file (file name)
  "Starting at FILE, look up directory hierarchy for directory containing NAME.
FILE can be a file or a directory.  If it's a file, its directory will
serve as the starting point for searching the hierarchy of directories.
Stop at the first parent directory containing a file NAME,
and return the directory.  Return nil if not found.
Instead of a string, NAME can also be a predicate taking one argument
\(a directory) and returning a non-nil value if that directory is the one for
which we're looking."
  (setq file (abbreviate-file-name (expand-file-name file)))
  (let ((root nil)
        try)
    (while (not (or root
                    (null file)
                    (string-match locate-dominating-stop-dir-regexp file)))
      (setq file (if (file-directory-p file)
                     file
                   (file-name-directory file))
            try (if (stringp name)
                    (file-exists-p (expand-file-name name file))
                  (funcall name file)))
      (cond (try (setq root file))
            ((equal file (setq file (file-name-directory
                                     (directory-file-name file))))
             (setq file nil))))
    (if root (file-name-as-directory root))))

(defun file-equal-p (file1 file2)
  "Return non-nil if files FILE1 and FILE2 name the same file.
If FILE1 or FILE2 does not exist, the return value is unspecified."
  (let ((handler (or (find-file-name-handler file1 'file-equal-p)
                     (find-file-name-handler file2 'file-equal-p))))
    (if handler
        (funcall handler 'file-equal-p file1 file2)
      (let (f1-attr f2-attr)
        (and (setq f1-attr (file-attributes (file-truename file1)))
	     (setq f2-attr (file-attributes (file-truename file2)))
             (equal f1-attr f2-attr))))))

(defun file-newer-than-file-p (file1 file2)
  "Return non-nil if file FILE1 is newer than file FILE2.
If FILE1 does not exist, the return value is nil;
otherwise, if FILE2 does not exist, the return value is t."
  (let ((handler (or (find-file-name-handler file1 'file-newer-than-file-p)
                     (find-file-name-handler file2 'file-newer-than-file-p))))
    (if handler
        (funcall handler 'file-newer-than-file-p file1 file2)
      (let ((mt1 (file-attribute-modification-time (file-attributes file1)))
            (mt2 (file-attribute-modification-time (file-attributes file2))))
        (cond ((not mt1) nil)
              ((not mt2) t)
              (t (time-less-p mt2 mt1)))))))

(defun file-chase-links (filename &optional limit)
  "Chase links in FILENAME until a name that is not a link.
Unlike `file-truename', this does not check whether a parent
directory name is a symbolic link.
If the optional argument LIMIT is a number,
it means chase no more than that many links and then stop."
  (let (tem (newname filename)
	    (count 0))
    (while (and (or (null limit) (< count limit))
		(setq tem (file-symlink-p newname)))
      (save-match-data
	(if (and (null limit) (= count 100))
	    (error "Apparent cycle of symbolic links for %s" filename))
	(while (string-match "//+" tem)
	  (setq tem (replace-match "/" nil nil tem)))
	(while (string-match "\\`\\.\\./" tem)
	  (setq tem (substring tem 3))
	  (setq newname (expand-file-name newname))
	  (setq newname
		(file-chase-links
		 (directory-file-name (file-name-directory newname))))
	  (setq newname (file-name-directory newname)))
	(setq newname (if (file-name-absolute-p tem)
                          tem
                        (concat (file-name-directory newname) tem)))
	(setq count (1+ count))))
    newname))

(defun copy-directory (directory newname &optional keep-time parents copy-contents)
  "Copy DIRECTORY to NEWNAME.  Both args must be strings.
Copy the contents of DIRECTORY into NEWNAME, creating it if necessary."
  (setq directory (directory-file-name (expand-file-name directory))
        newname (expand-file-name newname))
  (cond ((not (file-directory-p newname))
         (make-directory newname parents))
        ((not copy-contents)
         (setq newname (expand-file-name
                        (file-name-nondirectory directory) newname))))
  (unless (file-directory-p newname)
    (make-directory newname t))
  (dolist (file (directory-files directory nil directory-files-no-dot-files-regexp))
    (let ((source (expand-file-name file directory))
          (target (expand-file-name file newname)))
      (if (file-directory-p source)
          (copy-directory source target keep-time parents t)
        (copy-file source target t keep-time))))
  (when keep-time
    (set-file-times newname (file-attribute-modification-time
                             (file-attributes directory)))))

(defun make-nearby-temp-file (prefix &optional dir-flag suffix)
  "Create a temporary file as close as possible to `default-directory'.
Regardless of PREFIX, this creates the file in the local temporary
directory in this environment (there is no remote support)."
  (make-temp-file prefix dir-flag suffix))

;; Filesystem features that this environment does not implement.  GNU
;; Emacs returns these degraded values when the underlying OS lacks the
;; corresponding support; they are listed only so the built-in shortdoc
;; groups can reference them.
(defun file-acl (_file) "Return the ACL entries of FILE, or nil if unsupported." nil)
(defun set-file-acl (_file _acl) "Set the ACL entries of FILE (unsupported)." nil)
(defun file-selinux-context (_file)
  "Return the SELinux context of FILE (unsupported)."
  '(nil nil nil nil))
(defun set-file-selinux-context (_file _context)
  "Set the SELinux context of FILE (unsupported)."
  nil)
(defun file-extended-attributes (_file)
  "Return an alist of extended attributes of FILE (unsupported)."
  nil)
(defun set-file-extended-attributes (_file _attributes)
  "Set the extended attributes of FILE (unsupported)."
  nil)

(defun add-name-to-file (file newname &optional ok-if-already-exists)
  "Give FILE additional name NEWNAME (a hard link).
Hard links are not supported in this environment."
  (ignore ok-if-already-exists)
  (signal 'file-error (list "Adding new name" "Operation not supported"
                            file newname)))

(defun kill-process (&optional process _current-group)
  "Kill process PROCESS.  PROCESS may be a process object or a buffer."
  (let ((proc (or (get-process process) process)))
    (when (processp proc)
      (delete-process proc))))

(defun set-process-sentinel (_process sentinel)
  "Give PROCESS the sentinel function SENTINEL.
Process sentinels are not dispatched in this batch environment."
  sentinel)

(defun vc-responsible-backend (_file &optional _no-error)
  "Return the version-control backend responsible for FILE.
Version control is not integrated in this environment."
  nil)

;;; Change-group / undo machinery (verbatim from GNU simple.el and
;;; subr.el): viper's undo grouping builds on these.

(defun undo-start (&optional beg end)
  "Set `pending-undo-list' to the front of the undo list.
The next call to `undo-more' will undo the most recently made change.
If BEG and END are specified, then undo only elements
that apply to text between BEG and END are used; other undo elements
are ignored.  If BEG and END are nil, all undo elements are used."
  (if (eq buffer-undo-list t)
      (user-error "No undo information in this buffer"))
  (setq pending-undo-list
	(if (and beg end (not (= beg end)))
	    (undo-make-selective-list (min beg end) (max beg end))
	  buffer-undo-list)))

(defun prepare-change-group (&optional buffer)
  "Return a handle for the current buffer's state, for a change group.
If you specify BUFFER, make a handle for BUFFER's state instead.

Pass the handle to `activate-change-group' afterward to initiate
the actual changes of the change group.

To finish the change group, call either `accept-change-group' or
`cancel-change-group' passing the same handle as argument."
  (if buffer
      (list (cons buffer (with-current-buffer buffer buffer-undo-list)))
    (list (cons (current-buffer) buffer-undo-list))))

(defun activate-change-group (handle)
  "Activate a change group made with `prepare-change-group' (which see)."
  (dolist (elt handle)
    (with-current-buffer (car elt)
      (if (eq buffer-undo-list t)
	  (setq buffer-undo-list nil)
        ;; Add a boundary to make sure the upcoming changes won't be
        ;; merged/combined with any previous changes (bug#33341).
        (when (numberp (car-safe (car buffer-undo-list)))
          (push (cons (caar buffer-undo-list) (caar buffer-undo-list))
                buffer-undo-list))))))

(defun accept-change-group (handle)
  "Finish a change group made with `prepare-change-group' (which see).
This finishes the change group by accepting its changes as final."
  (dolist (elt handle)
    (with-current-buffer (car elt)
      (if (eq (cdr elt) t)
	  (setq buffer-undo-list t)))))

(defun cancel-change-group (handle)
  "Finish a change group made with `prepare-change-group' (which see).
This finishes the change group by reverting all of its changes."
  (dolist (elt handle)
    (with-current-buffer (car elt)
      (setq elt (cdr elt))
      (save-restriction
	(widen)
	(let ((old-car (car-safe elt))
	      (old-cdr (cdr-safe elt))
	      (pending-undo-list buffer-undo-list))
          (unwind-protect
              (progn
                (when (consp elt)
                  (setcar elt nil) (setcdr elt nil))
                (when (and (consp elt) (not (eq elt (last pending-undo-list))))
                  (error "Undoing to some unrelated state"))
                (save-excursion
                  (while (listp pending-undo-list) (undo-more 1)))
                (setq buffer-undo-list elt))
            (when (consp elt)
              (setcar elt old-car)
              (setcdr elt old-cdr))))))))

(defun undo-amalgamate-change-group (handle)
  "Amalgamate changes in change-group since HANDLE.
Remove all undo boundaries between the state of HANDLE and now.
HANDLE is as returned by `prepare-change-group'.

GNU's implementation truncates the shared list structure in place; the
undo list emaxx exposes is rebuilt on each read, so the entries recorded
since HANDLE are identified by length instead."
  (dolist (elt handle)
    (with-current-buffer (car elt)
      (let ((old (cdr elt))
            (cur buffer-undo-list))
        (when (consp cur)
          (let* ((old-len (if (listp old) (length old) 0))
                 (new-count (max 0 (- (length cur) old-len)))
                 (head (delq nil (seq-take cur new-count)))
                 (tail (nthcdr new-count cur)))
            (setq buffer-undo-list (append head tail))))))))

(defvar pending-undo-list nil
  "Within a run of consecutive undo commands, list remaining to be undone.
If t, we undid all the way to the end of it.")
(defvar undo-in-progress nil
  "Non-nil while performing an undo.
Some change-hooks test this variable to do something different.")
(defvar undo-in-region nil
  "Non-nil if `pending-undo-list' is not just a tail of `buffer-undo-list'.")

(defun primitive-undo (n list)
  "Undo N records from the front of the list LIST.
Return what remains of the list."
  (let ((arg n)
        ;; In a writable buffer, enable undoing read-only text that is
        ;; so because of text properties.
        (inhibit-read-only t)
        ;; We use oldlist only to check for EQ.  ++kfs
        (oldlist buffer-undo-list)
        (did-apply nil)
        (next nil))
    (while (> arg 0)
      (while (setq next (pop list))     ;Exit inner loop at undo boundary.
        ;; Handle an integer by setting point to that value.
        (pcase next
          ((pred integerp) (goto-char next))
          ;; Element (t . TIME) records previous modtime.
          (`(t . ,time)
           (let ((visited-file-time (visited-file-modtime)))
             (when (time-equal-p time visited-file-time)
               (unlock-buffer)
               (set-buffer-modified-p nil))))
          ;; Element (nil PROP VAL BEG . END) is property change.
          (`(nil . ,(or `(,prop ,val ,beg . ,end) pcase--dontcare))
           (when (or (> (point-min) beg) (< (point-max) end))
             (error "Changes to be undone are outside visible portion of buffer"))
           (put-text-property beg end prop val))
          ;; Element (BEG . END) means range was inserted.
          (`(,(and beg (pred integerp)) . ,(and end (pred integerp)))
           (when (or (> (point-min) beg) (< (point-max) end))
             (error "Changes to be undone are outside visible portion of buffer"))
           ;; Set point first thing, so that undoing this undo
           ;; does not send point back to where it is now.
           (goto-char beg)
           (delete-region beg end))
          ;; Element (apply FUN . ARGS) means call FUN to undo.
          (`(apply . ,fun-args)
           (let ((currbuff (current-buffer)))
             (if (integerp (car fun-args))
                 ;; Long format: (apply DELTA START END FUN . ARGS).
                 (pcase-let* ((`(,delta ,start ,end ,fun . ,args) fun-args)
                              (start-mark (copy-marker start nil))
                              (end-mark (copy-marker end t)))
                   (when (or (> (point-min) start) (< (point-max) end))
                     (error "Changes to be undone are outside visible portion of buffer"))
                   (apply fun args)
                   (unless (and (= start start-mark)
                                (= (+ delta end) end-mark))
                     (error "Changes undone by function are different from the announced ones"))
                   (set-marker start-mark nil)
                   (set-marker end-mark nil))
               (apply fun-args))
             (unless (eq currbuff (current-buffer))
               (error "Undo function switched buffer"))
             (setq did-apply t)))
          ;; Element (STRING . POS) means STRING was deleted.
          (`(,(and string (pred stringp)) . ,(and pos (pred integerp)))
           (let ((valid-marker-adjustments nil)
                 (apos (abs pos)))
             (when (or (< apos (point-min)) (> apos (point-max)))
               (error "Changes to be undone are outside visible portion of buffer"))
             (while (and (markerp (car-safe (car list)))
                         (integerp (cdr-safe (car list))))
               (let* ((marker-adj (pop list))
                      (m (car marker-adj)))
                 (and (eq (marker-buffer m) (current-buffer))
                      (= apos m)
                      (push marker-adj valid-marker-adjustments))))
             ;; Insert string and adjust point
             (if (< pos 0)
                 (progn
                   (goto-char (- pos))
                   (insert string))
               (goto-char pos)
               (insert string)
               (goto-char pos))
             (dolist (adj valid-marker-adjustments)
               (if (marker-buffer (car adj))
                   (set-marker (car adj)
                               (- (car adj) (cdr adj)))))))
          ;; (MARKER . OFFSET) means a marker MARKER was adjusted by OFFSET.
          (`(,(and marker (pred markerp)) . ,(and offset (pred integerp)))
           (warn "Encountered %S entry in undo list with no matching (TEXT . POS) entry"
                 next)
           (when (marker-buffer marker)
             (set-marker marker
                         (- marker offset)
                         (marker-buffer marker))))
          (_ (error "Unrecognized entry in undo list %S" next))))
      (setq arg (1- arg)))
    ;; Make sure an apply entry produces at least one undo entry,
    ;; so the test in `undo' for continuing an undo series
    ;; will work right.
    (if (and did-apply
             (eq oldlist buffer-undo-list))
        (setq buffer-undo-list
              (cons (list 'apply 'cdr nil) buffer-undo-list))))
  list)

(defun undo-more (n)
  "Undo back N undo-boundaries beyond what was already undone recently.
Call `undo-start' to get ready to undo recent changes,
then call `undo-more' one or more times to undo them."
  (or (listp pending-undo-list)
      (user-error (concat "No further undo information"
                          (and undo-in-region " for region"))))
  (let ((undo-in-progress t))
    ;; Note: The following, while pulling elements off
    ;; `pending-undo-list' will call primitive change functions which
    ;; will push more elements onto `buffer-undo-list'.
    (setq pending-undo-list (primitive-undo n pending-undo-list))
    (if (null pending-undo-list)
	(setq pending-undo-list t))))

(defmacro with-case-table (table &rest body)
  "Execute the forms in BODY with TABLE as the current case table.
The value returned is the value of the last form in BODY."
  (declare (indent 1) (debug t))
  (let ((old-case-table (make-symbol "table"))
	(old-buffer (make-symbol "buffer")))
    `(let ((,old-case-table (current-case-table))
	   (,old-buffer (current-buffer)))
       (unwind-protect
	   (progn (set-case-table ,table)
		  ,@body)
	 (with-current-buffer ,old-buffer
	   (set-case-table ,old-case-table))))))

(defvar custom-load-recursion nil
  "Hack to avoid recursive dependencies.")

(defun custom-load-symbol (symbol)
  "Load all dependencies for SYMBOL."
  (unless custom-load-recursion
    (let ((custom-load-recursion t))
      (ignore-errors
        (require 'cus-load))
      (ignore-errors
        (require 'cus-start))
      (dolist (load (get symbol 'custom-loads))
        (cond ((symbolp load) (ignore-errors (require load)))
	      ((assoc load load-history))
	      ((let ((regexp (concat "\\(\\`\\|/\\)" (regexp-quote load)
				     "\\(\\'\\|\\.\\)"))
		     (found nil))
		 (dolist (loaded load-history)
		   (and (stringp (car loaded))
			(string-match-p regexp (car loaded))
			(setq found t)))
		 found))
	      ((equal load "cus-edit"))
              (t (ignore-errors (load load))))))))

(defun custom-variable-p (variable)
  "Return non-nil if VARIABLE is a customizable variable.
A customizable variable is either (i) a variable whose property
list contains a non-nil `standard-value' or `custom-autoload'
property, or (ii) an alias for another customizable variable."
  (declare (side-effect-free t))
  (when (symbolp variable)
    (setq variable (indirect-variable variable))
    (or (get variable 'standard-value)
	(get variable 'custom-autoload))))

(defun add-to-invisibility-spec (element)
  "Add ELEMENT to `buffer-invisibility-spec'.
See documentation for `buffer-invisibility-spec' for the kind of elements
that can be added."
  (if (eq buffer-invisibility-spec t)
      (setq buffer-invisibility-spec (list t)))
  (setq buffer-invisibility-spec
	(cons element buffer-invisibility-spec)))

(defun remove-from-invisibility-spec (element)
  "Remove ELEMENT from `buffer-invisibility-spec'.
If `buffer-invisibility-spec' isn't a list before calling this
function, it will be made into a list containing just t as the
only list member."
  (setq buffer-invisibility-spec
        (if (consp buffer-invisibility-spec)
	    (delete element buffer-invisibility-spec)
          (list t))))

;; GNU window.el (preloaded) display-buffer action variables.
(defvar display-buffer-overriding-action '(nil)
  "Overriding action to perform to display a buffer.")

;; GNU custom.el (preloaded): the standard defcustom :initialize
;; functions, called by the defcustom machinery.  The custom-check-value
;; widget validation is omitted (no widget library in batch).
(defun custom-initialize-default (symbol exp)
  "Initialize SYMBOL with EXP unless it already has a default binding."
  (condition-case nil
      (default-toplevel-value symbol)
    (void-variable
     (set-default-toplevel-value
      symbol (eval (let ((sv (get symbol 'saved-value)))
                     (if sv (car sv) exp))
                   t)))))

(defun custom-initialize-set (symbol exp)
  "Initialize SYMBOL based on EXP via its `:set' function when unbound."
  (condition-case nil
      (default-toplevel-value symbol)
    (error
     (funcall (or (get symbol 'custom-set) #'set-default-toplevel-value)
              symbol
              (eval (let ((sv (get symbol 'saved-value)))
                      (if sv (car sv) exp)))))))

(defun custom-initialize-reset (symbol exp)
  "Initialize SYMBOL based on EXP, using its `:set' function."
  (funcall (or (get symbol 'custom-set) #'set-default-toplevel-value)
           symbol
           (condition-case nil
               (let ((def (default-toplevel-value symbol))
                     (getter (get symbol 'custom-get)))
                 (if getter (funcall getter symbol) def))
             (error
              (eval (let ((sv (get symbol 'saved-value)))
                      (if sv (car sv) exp)))))))

(defun custom-initialize-changed (symbol exp)
  "Initialize SYMBOL with EXP, using `:set' only for non-standard settings."
  (condition-case nil
      (let ((def (default-toplevel-value symbol)))
        (funcall (or (get symbol 'custom-set) #'set-default-toplevel-value)
                 symbol
                 (let ((getter (get symbol 'custom-get)))
                   (if getter (funcall getter symbol) def))))
    (error
     (cond
      ((get symbol 'saved-value)
       (funcall (or (get symbol 'custom-set) #'set-default-toplevel-value)
                symbol
                (eval (car (get symbol 'saved-value)))))
      (t
       (set-default-toplevel-value symbol (eval exp)))))))

(defvar custom-delayed-init-variables nil
  "List of variables whose initialization is pending until startup.")

(defun custom-initialize-delay (symbol exp)
  "Delay initialization of SYMBOL to the next startup.
In this environment startup has already happened, so initialize
immediately like `custom-initialize-set'."
  (set-default-toplevel-value symbol nil)
  (push symbol custom-delayed-init-variables)
  (custom-initialize-set symbol exp))

;; GNU newcomment.el autoloaded variables (preloaded surface): fill.el's
;; adaptive fill consults these in any buffer.
(defvar comment-start nil "String to insert to start a new comment, or nil if no comment syntax.")
(defvar comment-start-skip nil "Regexp to match the start of a comment plus everything up to its body.")
(defvar comment-end-skip nil "Regexp to match the end of a comment plus everything back to its body.")
(defvar comment-end (purecopy "") "String to insert to end a new comment.")
(defvar comment-indent-function 'comment-indent-default "Function to compute desired indentation for a comment.")
(defvar comment-insert-comment-function nil "Function to insert a comment when a line doesn't contain one.")
(defvar comment-column 32 "Column to indent right-margin comments to.")
(make-variable-buffer-local 'comment-start)
(make-variable-buffer-local 'comment-start-skip)
(make-variable-buffer-local 'comment-end-skip)
(make-variable-buffer-local 'comment-end)
(make-variable-buffer-local 'comment-column)

;; GNU custom.el (preloaded): theme recording used by customize-mark-as-set
;; and setopt.
(defvar custom--inhibit-theme-enable 'apply-only-user
  "Whether the custom-theme-set-* functions act immediately.")

(defun custom--should-apply-setting (theme)
  (or (null custom--inhibit-theme-enable)
      (and (eq custom--inhibit-theme-enable 'apply-only-user)
           (eq theme 'user))))
(defun custom-push-theme (prop symbol theme mode &optional value)
  "Record VALUE for face or variable SYMBOL in custom theme THEME.
PROP is `theme-face' for a face, `theme-value' for a variable.

MODE can be either the symbol `set' or the symbol `reset'.  If it is the
symbol `set', then VALUE is the value to use.  If it is the symbol
`reset', then SYMBOL will be removed from THEME (VALUE is ignored).

See `custom-known-themes' for a list of known themes."
  (unless (memq prop '(theme-value theme-face theme-icon))
    (error "Unknown theme property"))
  (let* ((old (get symbol prop))
	 (setting (assq theme old))  ; '(theme value)
	 (theme-settings             ; '(prop symbol theme value)
	  (get theme 'theme-settings)))
    (cond
     ;; Remove a setting:
     ((eq mode 'reset)
      (when setting
	(let (res)
	  (dolist (theme-setting theme-settings)
	    (if (and (eq (car  theme-setting) prop)
		     (eq (cadr theme-setting) symbol))
		(setq res theme-setting)))
	  (put theme 'theme-settings (delq res theme-settings)))
	(put symbol prop (delq setting old))))
     ;; Alter an existing setting:
     (setting
      (let (res)
	(dolist (theme-setting theme-settings)
	  (if (and (eq (car  theme-setting) prop)
		   (eq (cadr theme-setting) symbol))
	      (setq res theme-setting)))
	(put theme 'theme-settings
	     (cons (list prop symbol theme value)
		   (delq res theme-settings)))
        ;; It's tempting to use setcar here, but that could
        ;; inadvertently modify other properties in SYMBOL's proplist,
        ;; if those just happen to share elements with the value of PROP.
        (put symbol prop (cons (list theme value) (delq setting old)))))
     ;; Add a new setting:
     (t
      (when (custom--should-apply-setting theme)
	(unless old
	  ;; If the user changed a variable outside of Customize, save
	  ;; the value to a fake theme, `changed'.  If the theme is
	  ;; later disabled, we use this to bring back the old value.
	  ;;
	  ;; For faces, we just use `face--new-frame-defaults' to
	  ;; recompute when the theme is disabled.
	  (when (and (eq prop 'theme-value)
		     (boundp symbol))
	    (let ((sv  (get symbol 'standard-value))
		  (val (symbol-value symbol)))
	      (unless (or
                       ;; We only do this trick if the current value
                       ;; is different from the standard value.
                       (and sv (equal (eval (car sv)) val))
                       ;; And we don't do it if we would end up recording
                       ;; the same value for the user theme.  This way we avoid
                       ;; having ((user VALUE) (changed VALUE)).  That would be
                       ;; useless, because we don't disable the user theme.
                       (and (eq theme 'user) (equal (custom-quote val) value)))
		(setq old `((changed ,(custom-quote val))))))))
	(put symbol prop (cons (list theme value) old)))
      (put theme 'theme-settings
	   (cons (list prop symbol theme value) theme-settings))))))

(defun customize-mark-as-set (symbol)
  "Mark current value of SYMBOL as being set from customize.
Return non-nil if the `customized-value' property actually changed."
  (custom-load-symbol symbol)
  (let* ((get (or (get symbol 'custom-get) #'default-value))
	 (value (funcall get symbol))
	 (customized (get symbol 'customized-value))
	 (old (or (get symbol 'saved-value) (get symbol 'standard-value))))
    ;; Mark default value as set if different from old value.
    (if (not (and old
                  (equal value (ignore-errors
                                 (eval (car old))))))
	(progn (put symbol 'customized-value (list (custom-quote value)))
	       (custom-push-theme 'theme-value symbol 'user 'set
				  (custom-quote value)))
      (custom-push-theme 'theme-value symbol 'user
                         (if (get symbol 'saved-value) 'set 'reset)
                         (custom-quote value))
      (put symbol 'customized-value nil))
    ;; Changed?
    (not (equal customized (get symbol 'customized-value)))))

;; GNU minibuffer.el (verbatim): quoted completion tables, needed by
;; `pcomplete-completions-at-point' (erc-dcc /dcc completion).
(defun completion-boundaries (string collection pred suffix)
  "Return the boundaries of text on which COLLECTION will operate.
STRING is the string on which completion will be performed.
SUFFIX is the string after point.
If COLLECTION is a function, it is called with 3 arguments: STRING,
PRED, and a cons cell of the form (boundaries . SUFFIX).

The result is of the form (START . END) where START is the position
in STRING of the beginning of the completion field and END is the position
in SUFFIX of the end of the completion field.
E.g. for simple completion tables, the result is always (0 . (length SUFFIX))
and for file names the result is the positions delimited by
the closest directory separators."
  (let ((boundaries (if (functionp collection)
                        (funcall collection string pred
                                 (cons 'boundaries suffix)))))
    (if (not (eq (car-safe boundaries) 'boundaries))
        (setq boundaries nil))
    (cons (or (cadr boundaries) 0)
          (or (cddr boundaries) (length suffix)))))


(defun complete-with-action (action collection string predicate)
  "Perform completion according to ACTION.
STRING, COLLECTION and PREDICATE are used as in `try-completion'.

If COLLECTION is a function, it will be called directly to
perform completion, no matter what ACTION is.

If ACTION is `metadata' or a list where the first element is
`boundaries', return nil.  If ACTION is nil, this function works
like `try-completion'; if it is t, this function works like
`all-completion'; and any other value makes it work like
`test-completion'."
  (cond
   ((functionp collection) (funcall collection string predicate action))
   ((eq (car-safe action) 'boundaries) nil)
   ((eq action 'metadata) nil)
   (t
    (funcall
     (cond
      ((null action) 'try-completion)
      ((eq action t) 'all-completions)
      (t 'test-completion))
     string collection predicate))))

(defun completion-table-subvert (table s1 s2)
  "Return a completion table from TABLE with S1 replaced by S2.
The result is a completion table which completes strings of the
form (concat S1 S) in the same way as TABLE completes strings of
the form (concat S2 S)."
  (lambda (string pred action)
    (let* ((str (if (string-prefix-p s1 string completion-ignore-case)
                    (concat s2 (substring string (length s1)))))
           (res (if str (complete-with-action action table str pred))))
      (when (or res (eq (car-safe action) 'boundaries))
        (cond
         ((eq (car-safe action) 'boundaries)
          (let ((beg (or (and (eq (car-safe res) 'boundaries) (cadr res)) 0)))
            `(boundaries
              ,(min (length string)
                    (max (length s1)
                         (+ beg (- (length s1) (length s2)))))
              . ,(and (eq (car-safe res) 'boundaries) (cddr res)))))
         ((stringp res)
          (if (string-prefix-p s2 res completion-ignore-case)
              (concat s1 (substring res (length s2)))))
         ((eq action t)
          (let ((bounds (completion-boundaries str table pred "")))
            (if (>= (car bounds) (length s2))
                res
              (let ((re (concat "\\`"
                                (regexp-quote (substring s2 (car bounds))))))
                (delq nil
                      (mapcar (lambda (c)
                                (if (string-match re c)
                                    (substring c (match-end 0))))
                              res))))))
         ;; E.g. action=nil and it's the only completion.
         (res))))))

(defun completion-table-with-quoting (table unquote requote)
  ;; A difficult part of completion-with-quoting is to map positions in the
  ;; quoted string to equivalent positions in the unquoted string and
  ;; vice-versa.  There is no efficient and reliable algorithm that works for
  ;; arbitrary quote and unquote functions.
  ;; So to map from quoted positions to unquoted positions, we simply assume
  ;; that `concat' and `unquote' commute (which tends to be the case).
  ;; And we ask `requote' to do the work of mapping from unquoted positions
  ;; back to quoted positions.
  ;; FIXME: For some forms of "quoting" such as the truncation behavior of
  ;; substitute-in-file-name, it would be desirable not to requote completely.
  "Return a new completion table operating on quoted text.
TABLE operates on the unquoted text.
UNQUOTE is a function that takes a string and returns a new unquoted string.
REQUOTE is a function of 2 args (UPOS QSTR) where
  QSTR is a string entered by the user (and hence indicating
  the user's preferred form of quoting); and
  UPOS is a position within the unquoted form of QSTR.
REQUOTE should return a pair (QPOS . QFUN) such that QPOS is the
position corresponding to UPOS but in QSTR, and QFUN is a function
of one argument (a string) which returns that argument appropriately quoted
for use at QPOS."
  ;; FIXME: One problem with the current setup is that `qfun' doesn't know if
  ;; its argument is "the end of the completion", so if the quoting used double
  ;; quotes (for example), we end up completing "fo" to "foobar and throwing
  ;; away the closing double quote.
  (lambda (string pred action)
    (cond
     ((eq action 'metadata)
      (append (completion-metadata string table pred)
              '((completion--unquote-requote . t))))

     ((eq action 'lambda) ;;test-completion
      (let ((ustring (funcall unquote string)))
        (test-completion ustring table pred)))

     ((eq (car-safe action) 'boundaries)
      (let* ((ustring (funcall unquote string))
             (qsuffix (cdr action))
             (ufull (if (zerop (length qsuffix)) ustring
                      (funcall unquote (concat string qsuffix))))
             ;; If (not (string-prefix-p ustring ufull)) we have a problem:
             ;; unquoting the qfull gives something "unrelated" to ustring.
             ;; E.g. "~/" and "/" where "~//" gets unquoted to just "/" (see
             ;; bug#47678).
             ;; In that case we can't even tell if we're right before the
             ;; "/" or right after it (aka if this "/" is from qstring or
             ;; from qsuffix), thus which usuffix to use is very unclear.
             (usuffix (if (string-prefix-p ustring ufull)
                          (substring ufull (length ustring))
                        ;; FIXME: Maybe "" is preferable/safer?
                        qsuffix))
             (boundaries (completion-boundaries ustring table pred usuffix))
             (qlboundary (car (funcall requote (car boundaries) string)))
             (qrboundary (if (zerop (cdr boundaries)) 0 ;Common case.
                           (let* ((urfullboundary
                                   (+ (cdr boundaries) (length ustring))))
                             (- (car (funcall requote urfullboundary
                                              (concat string qsuffix)))
                                (length string))))))
        `(boundaries ,qlboundary . ,qrboundary)))

     ;; In "normal" use a c-t-with-quoting completion table should never be
     ;; called with action in (t nil) because `completion--unquote' should have
     ;; been called before and would have returned a different completion table
     ;; to apply to the unquoted text.  But there's still a lot of code around
     ;; that likes to use all/try-completions directly, so we do our best to
     ;; handle those calls as well as we can.

     ((eq action nil) ;;try-completion
      (let* ((ustring (funcall unquote string))
             (completion (try-completion ustring table pred)))
        ;; Most forms of quoting allow several ways to quote the same string.
        ;; So here we could simply requote `completion' in a kind of
        ;; "canonical" quoted form without paying attention to the way
        ;; `string' was quoted.  But since we have to solve the more complex
        ;; problems of "pay attention to the original quoting" for
        ;; all-completions, we may as well use it here, since it provides
        ;; a nicer behavior.
        (if (not (stringp completion)) completion
          (car (completion--twq-try
                string ustring completion 0 unquote requote)))))

     ((eq action t) ;;all-completions
      ;; When all-completions is used for completion-try/all-completions
      ;; (e.g. for `pcm' style), we can't do the job properly here because
      ;; the caller will match our output against some pattern derived from
      ;; the user's (quoted) input, and we don't have access to that
      ;; pattern, so we can't know how to requote our output so that it
      ;; matches the quoting used in the pattern.  It is to fix this
      ;; fundamental problem that we have to introduce the new
      ;; unquote-requote method so that completion-try/all-completions can
      ;; pass the unquoted string to the style functions.
      (pcase-let*
          ((ustring (funcall unquote string))
           (completions (all-completions ustring table pred))
           (boundary (car (completion-boundaries ustring table pred "")))
           (completions
            (completion--twq-all
             string ustring completions boundary unquote requote))
           (last (last completions)))
        (when (consp last) (setcdr last nil))
        completions))

     ((eq action 'completion--unquote)
      ;; PRED is really a POINT in STRING.
      ;; We should return a new set (STRING TABLE POINT REQUOTE)
      ;; where STRING is a new (unquoted) STRING to match against the new TABLE
      ;; using a new POINT inside it, and REQUOTE is a requoting function which
      ;; should reverse the unquoting, (i.e. it receives the completion result
      ;; of using the new TABLE and should turn it into the corresponding
      ;; quoted result).
      (let* ((qpos pred)
	     (ustring (funcall unquote string))
	     (uprefix (funcall unquote (substring string 0 qpos)))
	     ;; FIXME: we really should pass `qpos' to `unquote' and have that
	     ;; function give us the corresponding `uqpos'.  But for now we
	     ;; presume (more or less) that `concat' and `unquote' commute.
	     (uqpos (if (string-prefix-p uprefix ustring)
			;; Yay!!  They do seem to commute!
			(length uprefix)
		      ;; They don't commute this time!  :-(
		      ;; Maybe qpos is in some text that disappears in the
		      ;; ustring (bug#17239).  Let's try a second chance guess.
		      (let ((usuffix (funcall unquote (substring string qpos))))
			(if (string-suffix-p usuffix ustring)
			    ;; Yay!!  They still "commute" in a sense!
			    (- (length ustring) (length usuffix))
			  ;; Still no luck!  Let's just choose *some* position
			  ;; within ustring.
			  (/ (+ (min (length uprefix) (length ustring))
				(max (- (length ustring) (length usuffix)) 0))
			     2))))))
        (list ustring table uqpos
              (lambda (unquoted-result op)
                (pcase op
                  (1 ;;try
                   (if (not (stringp (car-safe unquoted-result)))
                       unquoted-result
                     (completion--twq-try
                      string ustring
                      (car unquoted-result) (cdr unquoted-result)
                      unquote requote)))
                  (2 ;;all
                   (let* ((last (last unquoted-result))
                          (base (or (cdr last) 0)))
                     (when last
                       (setcdr last nil)
                       (completion--twq-all string ustring
                                            unquoted-result base
                                            unquote requote))))))))))))

(defun completion--twq-try (string ustring completion point
                                   unquote requote)
  ;; Basically two cases: either the new result is
  ;; - commonprefix1 <point> morecommonprefix <qpos> suffix
  ;; - commonprefix <qpos> newprefix <point> suffix
  (pcase-let*
      ((prefix (fill-common-string-prefix ustring completion))
       (suffix (substring completion (max point (length prefix))))
       (`(,qpos . ,qfun) (funcall requote (length prefix) string))
       (qstr1 (if (> point (length prefix))
                  (funcall qfun (substring completion (length prefix) point))))
       (qsuffix (funcall qfun suffix))
       (qstring (concat (substring string 0 qpos) qstr1 qsuffix))
       (qpoint
        (cond
         ((zerop point) 0)
         ((> point (length prefix)) (+ qpos (length qstr1)))
         (t (car (funcall requote point string))))))
    ;; Make sure `requote' worked.
    (if (equal (funcall unquote qstring) completion)
	(cons qstring qpoint)
      ;; If requote failed (e.g. because sifn-requote did not handle
      ;; Tramp's "/foo:/bar//baz -> /foo:/baz" truncation), then at least
      ;; try requote properly.
      (let ((qstr (funcall qfun completion)))
	(cons qstr (length qstr))))))

(defun completion--twq-all (string ustring completions boundary
                                   _unquote requote)
  (when completions
    (pcase-let*
        ((prefix
          (let ((completion-regexp-list nil))
            (try-completion "" (cons (substring ustring boundary)
                                     completions))))
         (`(,qfullpos . ,qfun)
          (funcall requote (+ boundary (length prefix)) string))
         (qfullprefix (substring string 0 qfullpos))
	 ;; FIXME: This assertion can be wrong, e.g. in Cygwin, where
	 ;; (unquote "c:\bin") => "/usr/bin" but (unquote "c:\") => "/".
         ;;(cl-assert (string-equal-ignore-case
         ;;            (funcall unquote qfullprefix)
         ;;            (concat (substring ustring 0 boundary) prefix))
         ;;           t))
         (qboundary (car (funcall requote boundary string)))
         (_ (cl-assert (<= qboundary qfullpos)))
         ;; FIXME: this split/quote/concat business messes up the carefully
         ;; placed completions-common-part and completions-first-difference
         ;; faces.  We could try within the mapcar loop to search for the
         ;; boundaries of those faces, pass them to `requote' to find their
         ;; equivalent positions in the quoted output and re-add the faces:
         ;; this might actually lead to correct results but would be
         ;; pretty expensive.
         ;; The better solution is to not quote the *Completions* display,
         ;; which nicely circumvents the problem.  The solution I used here
         ;; instead is to hope that `qfun' preserves the text-properties and
         ;; presume that the `first-difference' is not within the `prefix';
         ;; this presumption is not always true, but at least in practice it is
         ;; true in most cases.
         (qprefix (propertize (substring qfullprefix qboundary)
                              'face 'completions-common-part)))

      ;; Here we choose to quote all elements returned, but a better option
      ;; would be to return unquoted elements together with a function to
      ;; requote them, so that *Completions* can show nicer unquoted values
      ;; which only get quoted when needed by choose-completion.
      (nconc
       (mapcar (lambda (completion)
                 (cl-assert (string-prefix-p prefix completion 'ignore-case) t)
                 (let* ((new (substring completion (length prefix)))
                        (qnew (funcall qfun new))
			(qprefix
                         (if (not completion-ignore-case)
                             qprefix
                           ;; Make qprefix inherit the case from `completion'.
                           (let* ((rest (substring completion
                                                   0 (length prefix)))
                                  (qrest (funcall qfun rest)))
                             (if (string-equal-ignore-case qprefix qrest)
                                 (propertize qrest 'face
                                             'completions-common-part)
                               qprefix))))
                        (qcompletion (concat qprefix qnew)))
                   ;; Some completion tables (including this one) pass
                   ;; along necessary information as text properties
                   ;; on the first character of the completion.  Make
                   ;; sure the quoted completion has these properties
                   ;; too.
                   (add-text-properties 0 1 (text-properties-at 0 completion)
                                        qcompletion)
                   ;; Attach unquoted completion string, which is needed
                   ;; to score the completion in `completion--flex-score'.
                   (put-text-property 0 1 'completion--unquoted
                                      completion qcompletion)
		   ;; FIXME: Similarly here, Cygwin's mapping trips this
		   ;; assertion.
                   ;;(cl-assert
                   ;; (string-equal-ignore-case
		   ;;  (funcall unquote
		   ;;           (concat (substring string 0 qboundary)
		   ;;                   qcompletion))
		   ;;  (concat (substring ustring 0 boundary)
		   ;;          completion))
		   ;; t)
                   qcompletion))
               completions)
       qboundary))))

;; GNU fill.el (verbatim): used by `completion--twq-try'.
(defun fill-common-string-prefix (s1 s2)
  "Return the longest common prefix of strings S1 and S2, or nil if none."
  (let ((cmp (compare-strings s1 nil nil s2 nil nil)))
    (if (eq cmp t)
	s1
      (setq cmp (1- (abs cmp)))
      (unless (zerop cmp)
	(substring s1 0 cmp)))))

;; GNU minibuffer.el (verbatim).
(defun completion-table-case-fold (table &optional dont-fold)
  "Return new completion TABLE that is case insensitive.
If DONT-FOLD is non-nil, return a completion table that is
case sensitive instead."
  (lambda (string pred action)
    (let ((completion-ignore-case (not dont-fold)))
      (complete-with-action action table string pred))))

;; GNU subr.el (verbatim): pcomplete quoting helpers.
(defun combine-and-quote-strings (strings &optional separator)
  "Concatenate the STRINGS, adding the SEPARATOR (default \" \").
This tries to quote the strings to avoid ambiguity such that
  (split-string-and-unquote (combine-and-quote-strings strs)) == strs
Only some SEPARATORs will work properly.

Note that this is not intended to protect STRINGS from
interpretation by shells, use `shell-quote-argument' for that."
  (declare (important-return-value t))
  (let* ((sep (or separator " "))
         (re (concat "[\\\"]" "\\|" (regexp-quote sep))))
    (mapconcat
     (lambda (str)
       (if (string-match re str)
	   (concat "\"" (replace-regexp-in-string "[\\\"]" "\\\\\\&" str) "\"")
	 str))
     strings sep)))

(defun split-string-and-unquote (string &optional separator)
  "Split the STRING into a list of strings.
It understands Emacs Lisp quoting within STRING, such that
  (split-string-and-unquote (combine-and-quote-strings strs)) == strs
The SEPARATOR regexp defaults to \"\\s-+\"."
  (declare (important-return-value t))
  (let ((sep (or separator "\\s-+"))
	(i (string-search "\"" string)))
    (if (null i)
	(split-string string sep t)	; no quoting:  easy
      (append (unless (eq i 0) (split-string (substring string 0 i) sep t))
	      (let ((rfs (read-from-string string i)))
		(cons (car rfs)
		      (split-string-and-unquote (substring string (cdr rfs))
						sep)))))))


;; GNU C-defined hook variables (keyboard.c, window.c): always bound.
;; The native command dispatch runs the command hooks; these defvars only
;; provide the variable bindings Lisp expects to read and modify.
(defvar pre-command-hook nil
  "Normal hook run before each command is executed.")
(defvar post-command-hook nil
  "Normal hook run after each command is executed.")
(defvar window-buffer-change-functions nil
  "Functions called during redisplay when window buffers have changed.")
(defvar window-selection-change-functions nil
  "Functions called during redisplay when the selected window changed.")
(defvar window-state-change-functions nil
  "Functions called during redisplay when the window state changed.")
(defvar window-state-change-hook nil
  "Normal hook run when the window state changed during redisplay.")
(defvar window-size-change-functions nil
  "Functions called during redisplay when window sizes have changed.")
(defvar window-configuration-change-hook nil
  "Normal hook run when the window configuration changed.")

;; GNU buffer.c permanent buffer-local display variables.
(defvar left-margin-width 0
  "Width in columns of left marginal area for display of a buffer.")
(defvar right-margin-width 0
  "Width in columns of right marginal area for display of a buffer.")
(defvar fringes-outside-margins nil
  "Non-nil means to display fringes outside display margins.")
(make-variable-buffer-local 'left-margin-width)
(make-variable-buffer-local 'right-margin-width)
(make-variable-buffer-local 'fringes-outside-margins)

;; emaxx models batch sessions as a single full-frame window, so the
;; window metric accessors coincide with `window-width'/`window-height'.
(defun window-normalize-window (window &optional _live-only)
  "Return WINDOW or the selected window when WINDOW is nil."
  (or window (selected-window)))
(defun window-total-width (&optional window &rest _round)
  "Return the total width, in columns, of WINDOW."
  (window-width window))
(defun window-total-height (&optional window &rest _round)
  "Return the total height, in lines, of WINDOW."
  (window-height window))
(defun window-body-width (&optional window _pixelwise)
  "Return the width of WINDOW's text area."
  (window-width window))
(defun window-body-height (&optional window _pixelwise)
  "Return the height of WINDOW's text area."
  (window-height window))
(defun window-full-width-p (&optional _window)
  "Return t if WINDOW is as wide as its containing frame."
  t)

;; GNU C-defined/simple.el variables read by the kill commands below.
(defvar kill-whole-line nil
  "If non-nil, `kill-line' with no arg at start of line kills the whole line.")
(defvar show-trailing-whitespace nil
  "Non-nil means highlight trailing whitespace.")
(defvar kill-read-only-ok nil
  "Non-nil means don't signal an error for killing read-only text.")
(defvar truncate-partial-width-windows 50
  "Non-nil means truncate lines in windows narrower than the frame.")

;; GNU window.el (verbatim).
(defun count-screen-lines (&optional beg end count-final-newline window)
  "Return the number of screen lines in the region.
The number of screen lines may be different from the number of actual lines,
due to line breaking, display table, etc.

Optional arguments BEG and END default to `point-min' and `point-max'
respectively.

If region ends with a newline, ignore it unless optional third argument
COUNT-FINAL-NEWLINE is non-nil.

The optional fourth argument WINDOW specifies the window used for obtaining
parameters such as width, horizontal scrolling, and so on.  The default is
to use the selected window's parameters.

Like `vertical-motion', `count-screen-lines' always uses the current buffer,
regardless of which buffer is displayed in WINDOW.  This makes possible to use
`count-screen-lines' in any buffer, whether or not it is currently displayed
in some window."
  (unless beg
    (setq beg (point-min)))
  (unless end
    (setq end (point-max)))
  (if (= beg end)
      0
    (let ((start (min beg end))
          (finish (max beg end))
          count end-invisible-p)
      ;; When END is invisible because lines are truncated in WINDOW,
      ;; vertical-motion returns a number that is 1 larger than it
      ;; should.  We need to fix that.
      (setq end-invisible-p
            (and (or truncate-lines (truncated-partial-width-window-p window))
                 (save-excursion
                   (goto-char finish)
                   (> (- (current-column) (window-hscroll window))
                      (window-body-width window)))))
      (save-excursion
        (save-restriction
          (widen)
          (narrow-to-region start
                            (if (and (not count-final-newline)
                                     (= ?\n (char-before finish)))
                                (1- finish)
                              finish))
          (goto-char start)
          (setq count (vertical-motion (buffer-size) window))
          (if end-invisible-p count (1+ count)))))))

(defun truncated-partial-width-window-p (&optional window)
  "Return non-nil if lines in WINDOW are specifically truncated due to its width.
WINDOW must be a live window and defaults to the selected one.
Return nil if WINDOW is not a partial-width window
 (regardless of the value of `truncate-lines').
Otherwise, consult the value of `truncate-partial-width-windows'
 for the buffer shown in WINDOW."
  (setq window (window-normalize-window window t))
  (unless (window-full-width-p window)
    (let ((t-p-w-w (buffer-local-value 'truncate-partial-width-windows
				       (window-buffer window))))
      (if (integerp t-p-w-w)
	  (< (window-total-width window) t-p-w-w)
        t-p-w-w))))


;; GNU simple.el (verbatim).
(defun kill-line (&optional arg)
  "Kill the rest of the current line; if no nonblanks there, kill thru newline.
With prefix argument ARG, kill that many lines from point.
Negative arguments kill lines backward.
With zero argument, kills the text before point on the current line.

When calling from a program, nil means \"no arg\",
a number counts as a prefix arg.

To kill a whole line, when point is not at the beginning, type \
\\[move-beginning-of-line] \\[kill-line] \\[kill-line].

If `show-trailing-whitespace' is non-nil, this command will just
kill the rest of the current line, even if there are no nonblanks
there.

If option `kill-whole-line' is non-nil, then this command kills the whole line
including its terminating newline, when used at the beginning of a line
with no argument.  As a consequence, you can always kill a whole line
by typing \\[move-beginning-of-line] \\[kill-line].

If you want to append the killed line to the last killed text,
use \\[append-next-kill] before \\[kill-line].

If the buffer is read-only, Emacs will beep and refrain from deleting
the line, but put the line in the kill ring anyway.  This means that
you can use this command to copy text from a read-only buffer.
\(If the variable `kill-read-only-ok' is non-nil, then this won't
even beep.)"
  (interactive "P")
  (kill-region (point)
	       ;; It is better to move point to the other end of the kill
	       ;; before killing.  That way, in a read-only buffer, point
	       ;; moves across the text that is copied to the kill ring.
	       ;; The choice has no effect on undo now that undo records
	       ;; the value of point from before the command was run.
	       (progn
		 (if arg
		     (forward-visible-line (prefix-numeric-value arg))
		   (if (eobp)
		       (signal 'end-of-buffer nil))
		   (let ((end
			  (save-excursion
			    (end-of-visible-line) (point))))
		     (if (or (save-excursion
			       ;; If trailing whitespace is visible,
			       ;; don't treat it as nothing.
			       (unless show-trailing-whitespace
				 (skip-chars-forward " \t" end))
			       (= (point) end))
			     (and kill-whole-line (bolp)))
			 (forward-visible-line 1)
		       (goto-char end))))
		 (point))))

(defun forward-visible-line (arg)
  "Move forward by ARG lines, ignoring currently invisible newlines only.
If ARG is negative, move backward -ARG lines.
If ARG is zero, move to the beginning of the current line."
  (condition-case nil
      (if (> arg 0)
	  (progn
	    (while (> arg 0)
	      (or (zerop (forward-line 1))
		  (signal 'end-of-buffer nil))
	      ;; If the newline we just skipped is invisible,
	      ;; don't count it.
	      (if (invisible-p (1- (point)))
		  (setq arg (1+ arg)))
	      (setq arg (1- arg)))
	    ;; If invisible text follows, and it is a number of complete lines,
	    ;; skip it.
	    (let ((opoint (point)))
	      (while (and (not (eobp))
			  (invisible-p (point)))
		(goto-char
		 (if (get-text-property (point) 'invisible)
		     (or (next-single-property-change (point) 'invisible)
			 (point-max))
		   (next-overlay-change (point)))))
	      (unless (bolp)
		(goto-char opoint))))
	(let ((first t))
	  (while (or first (<= arg 0))
	    (if first
		(beginning-of-line)
	      (or (zerop (forward-line -1))
		  (signal 'beginning-of-buffer nil)))
	    ;; If the newline we just moved to is invisible,
	    ;; don't count it.
	    (unless (bobp)
	      (unless (invisible-p (1- (point)))
		(setq arg (1+ arg))))
	    (setq first nil))
	  ;; If invisible text follows, and it is a number of complete lines,
	  ;; skip it.
	  (let ((opoint (point)))
	    (while (and (not (bobp))
			(invisible-p (1- (point))))
	      (goto-char
	       (if (get-text-property (1- (point)) 'invisible)
		   (or (previous-single-property-change (point) 'invisible)
		       (point-min))
		 (previous-overlay-change (point)))))
	    (unless (bolp)
	      (goto-char opoint)))))
    ((beginning-of-buffer end-of-buffer)
     nil)))

(defun end-of-visible-line ()
  "Move to end of current visible line."
  (end-of-line)
  ;; If the following character is currently invisible,
  ;; skip all characters with that same `invisible' property value,
  ;; then find the next newline.
  (while (and (not (eobp))
	      (save-excursion
		(skip-chars-forward "^\n")
		(invisible-p (point))))
    (skip-chars-forward "^\n")
    (if (get-text-property (point) 'invisible)
	(goto-char (or (next-single-property-change (point) 'invisible)
		       (point-max)))
      (goto-char (next-overlay-change (point))))
    (end-of-line)))


;; GNU buffer.c per-buffer display variables.
(defvar truncate-lines nil
  "Non-nil means do not display continuation lines.")
(defvar word-wrap nil
  "Non-nil means to use word-wrapping for continuation lines.")
(make-variable-buffer-local 'truncate-lines)
(make-variable-buffer-local 'word-wrap)

;; GNU subr.el (verbatim): transient keymaps.
(defun internal-push-keymap (keymap symbol)
  (let ((map (symbol-value symbol)))
    (unless (memq keymap map)
      (unless (memq 'add-keymap-witness (symbol-value symbol))
        (setq map (make-composed-keymap nil (symbol-value symbol)))
        (push 'add-keymap-witness (cdr map))
        (set symbol map))
      (push keymap (cdr map)))))

(defun internal-pop-keymap (keymap symbol)
  (let ((map (symbol-value symbol)))
    (when (memq keymap map)
      (setf (cdr map) (delq keymap (cdr map))))
    (let ((tail (cddr map)))
      (and (or (null tail) (keymapp tail))
           (eq 'add-keymap-witness (nth 1 map))
           (set symbol tail)))))

(defvar set-transient-map-timeout nil
  "Timeout in seconds for deactivation of a transient keymap.
If this is a number, it specifies the amount of idle time
after which to deactivate the keymap set by `set-transient-map',
thus overriding the value of the TIMEOUT argument to that function.")

(defvar set-transient-map-timer nil
  "Timer for `set-transient-map-timeout'.")

(defun set-transient-map (map &optional keep-pred on-exit message timeout)
  "Set MAP as a temporary keymap taking precedence over other keymaps.
Normally, MAP is used only once, to look up the very next key.
However, if the optional argument KEEP-PRED is t, MAP stays
active if a key from MAP is used.  KEEP-PRED can also be a
function of no arguments: it is called from `pre-command-hook' and
if it returns non-nil, then MAP stays active.

Optional arg ON-EXIT, if non-nil, specifies a function that is
called, with no arguments, after MAP is deactivated.

Optional arg MESSAGE, if non-nil, requests display of an informative
message after activating the transient map.  If MESSAGE is a string,
it specifies the format string for the message to display, and the %k
specifier in the string is replaced with the list of keys from the
transient map.  Any other non-nil value of MESSAGE means to use the
message format string \"Repeat with %k\".  Upon deactivating the map,
the displayed message will be cleared out.

Optional arg TIMEOUT, if non-nil, should be a number specifying the
number of seconds of idle time after which the map is deactivated.
The variable `set-transient-map-timeout', if non-nil, overrides the
value of TIMEOUT.

This function uses `overriding-terminal-local-map', which takes precedence
over all other keymaps.  As usual, if no match for a key is found in MAP,
the normal key lookup sequence then continues.

This returns an \"exit function\", which can be called with no argument
to deactivate this transient map, regardless of KEEP-PRED."
  (let* ((timeout (or set-transient-map-timeout timeout))
         (message
          (when message
            (let (keys)
              (map-keymap (lambda (key cmd) (and cmd (push key keys))) map)
              (format-spec (if (stringp message) message "Repeat with %k")
                           `((?k . ,(mapconcat
                                     (lambda (key)
                                       (substitute-command-keys
                                        (format "\\`%s'"
                                                (key-description (vector key)))))
                                     keys ", ")))))))
         (clearfun (make-symbol "clear-transient-map"))
         (exitfun
          (lambda ()
            (internal-pop-keymap map 'overriding-terminal-local-map)
            (remove-hook 'pre-command-hook clearfun)
            ;; Clear the prompt after exiting.
            (when message (message ""))
            (when set-transient-map-timer (cancel-timer set-transient-map-timer))
            (when on-exit (funcall on-exit)))))
    ;; Don't use letrec, because equal (in add/remove-hook) could get trapped
    ;; in a cycle. (bug#46326)
    (fset clearfun
          (lambda ()
            (with-demoted-errors "set-transient-map PCH: %S"
              (if (cond
                       ((null keep-pred) nil)
                       ((and (not (eq map (cadr overriding-terminal-local-map)))
                             (memq map (cddr overriding-terminal-local-map)))
                        ;; There's presumably some other transient-map in
                        ;; effect.  Wait for that one to terminate before we
                        ;; remove ourselves.
                        ;; For example, if isearch and C-u both use transient
                        ;; maps, then the lifetime of the C-u should be nested
                        ;; within isearch's, so the pre-command-hook of
                        ;; isearch should be suspended during the C-u one so
                        ;; we don't exit isearch just because we hit 1 after
                        ;; C-u and that 1 exits isearch whereas it doesn't
                        ;; exit C-u.
                        t)
                       ((eq t keep-pred)
                        (let ((mc (lookup-key map (this-command-keys-vector))))
                          ;; We may have a remapped command, so chase
                          ;; down that.
                          (when (and mc (symbolp mc))
                            (setq mc (or (command-remapping mc) mc)))
                          ;; If the key is unbound `this-command` is
                          ;; nil and so is `mc`.
                          (and mc (eq this-command mc))))
                       (t (funcall keep-pred)))
                  ;; Repeat the message for the next command.
                  (when message (message "%s" message))
                (funcall exitfun)))))
    (add-hook 'pre-command-hook clearfun)
    (internal-push-keymap map 'overriding-terminal-local-map)
    (when timeout
      (when set-transient-map-timer (cancel-timer set-transient-map-timer))
      (setq set-transient-map-timer (run-with-idle-timer timeout nil exitfun)))
    (when message (message "%s" message))
    exitfun))

;; GNU keyboard.c: no idle time accumulates during batch execution.
(defun current-idle-time ()
  "Return the time elapsed since last user input, or nil if not idle."
  nil)

;; GNU subr.el (verbatim).
(defun make-composed-keymap (maps &optional parent)
  "Construct a new keymap composed of MAPS and inheriting from PARENT.
When looking up a key in the returned map, the key is looked in each
keymap of MAPS in turn until a binding is found.
If no binding is found in MAPS, the lookup continues in PARENT, if non-nil.
As always with keymap inheritance, a nil binding in MAPS overrides
any corresponding binding in PARENT, but it does not override corresponding
bindings in other keymaps of MAPS.
MAPS can be a list of keymaps or a single keymap.
PARENT if non-nil should be a keymap."
  (declare (side-effect-free t))
  `(keymap
    ,@(if (keymapp maps) (list maps) maps)
    ,@parent))

;; GNU subr.el (verbatim).
(defun field-at-pos (pos)
  "Return the field at position POS, taking stickiness etc into account."
  (declare (important-return-value t))
  (let ((raw-field (get-char-property (field-beginning pos) 'field)))
    (if (eq raw-field 'boundary)
	(get-char-property (1- (field-end pos)) 'field)
      raw-field)))


;; GNU keyboard.c: position info for a buffer shown in WINDOW; nil when
;; the buffer isn't displayed there (like an off-screen position).
;; `kill-visual-line' reads the (COL . ROW) slot to detect line wraps.
(defun posn-at-point (&optional pos window)
  "Return position information for POS in WINDOW, or nil."
  (setq window (or window (selected-window)))
  (when (eq (window-buffer window) (current-buffer))
    (let* ((pos (or pos (point)))
           (col (save-excursion (goto-char pos)
                                (- pos (progn (vertical-motion 0) (point)))))
           (row (max 0 (1- (count-screen-lines (window-start window) pos t)))))
      (list window pos (cons col row) 0 nil pos (cons col row)))))
(defvar word-wrap-by-category nil
  "Non-nil means also wrap after characters of a certain category.")

;; GNU simple.el (verbatim): visual-line motion commands.
(defun end-of-visual-line (&optional n)
  "Move point to end of current visual line.
With argument N not nil or 1, move forward N - 1 visual lines first.
If point reaches the beginning or end of buffer, it stops there.
To ignore intangibility, bind `inhibit-point-motion-hooks' to t."
  (interactive "^p")
  (or n (setq n 1))
  (if (/= n 1)
      (let ((line-move-visual t))
	(line-move (1- n) t)))
  ;; Unlike `move-beginning-of-line', `move-end-of-line' doesn't
  ;; constrain to field boundaries, so we don't either.
  (vertical-motion (cons (window-width) 0)))

(defun beginning-of-visual-line (&optional n)
  "Move point to beginning of current visual line.
With argument N not nil or 1, move forward N - 1 visual lines first.
If point reaches the beginning or end of buffer, it stops there.
\(But if the buffer doesn't end in a newline, it stops at the
beginning of the last visual line.)
To ignore intangibility, bind `inhibit-point-motion-hooks' to t."
  (interactive "^p")
  (or n (setq n 1))
  (let ((opoint (point)))
    (if (/= n 1)
	(let ((line-move-visual t))
	  (line-move (1- n) t)))
    (vertical-motion 0)
    ;; Constrain to field boundaries, like `move-beginning-of-line'.
    (goto-char (constrain-to-field (point) opoint (/= n 1)))))

(defun kill-visual-line (&optional arg)
  "Kill the rest of the visual line.
With prefix argument ARG, kill that many visual lines from point.
If ARG is negative, kill visual lines backward.
If ARG is zero, kill the text before point on the current visual
line.

If the variable `kill-whole-line' is non-nil, and this command is
invoked at start of a line that ends in a newline, kill the newline
as well.

If you want to append the killed line to the last killed text,
use \\[append-next-kill] before \\[kill-line].

If the buffer is read-only, Emacs will beep and refrain from deleting
the line, but put the line in the kill ring anyway.  This means that
you can use this command to copy text from a read-only buffer.
\(If the variable `kill-read-only-ok' is non-nil, then this won't
even beep.)"
  (interactive "P")
  ;; Like in `kill-line', it's better to move point to the other end
  ;; of the kill before killing.
  (let ((opoint (point))
        (kill-whole-line (and kill-whole-line (bolp)))
        (orig-vlnum (cdr (nth 6 (posn-at-point)))))
    (if arg
	(vertical-motion (prefix-numeric-value arg))
      (end-of-visual-line 1)
      (if (= (point) opoint)
	  (vertical-motion 1)
        ;; The first condition below verifies we are still on the same
        ;; screen line, i.e. that the line isn't continued, and that
        ;; end-of-visual-line didn't overshoot due to complications
        ;; like display or overlay strings, intangible text, etc.:
        ;; otherwise, we don't want to kill a character that's
        ;; unrelated to the place where the visual line wraps.
        (and (= (cdr (nth 6 (posn-at-point))) orig-vlnum)
             ;; Make sure we delete the character where the line wraps
             ;; under visual-line-mode, be it whitespace or a
             ;; character whose category set permits wrapping at it.
             (or (looking-at-p "[ \t]")
                 (and word-wrap-by-category
                      (aref (char-category-set (following-char)) ?\|)))
             (forward-char))))
    (kill-region opoint (if (and kill-whole-line (= (following-char) ?\n))
			    (1+ (point))
			  (point)))))


;; GNU batch sessions have no window-system selections.
(defvar interprogram-cut-function nil
  "Function to call to make a killed region available to other programs.")
(defvar interprogram-paste-function nil
  "Function to call to get text cut from other programs.")

;; GNU simple.el (verbatim): the kill ring.
(defvar kill-ring nil
  "List of killed text sequences.
Since the kill ring is supposed to interact nicely with cut-and-paste
facilities offered by window systems, use of this variable should
interact nicely with `interprogram-cut-function' and
`interprogram-paste-function'.  The functions `kill-new',
`kill-append', and `current-kill' are supposed to implement this
interaction; you may want to use them instead of manipulating the kill
ring directly.")

(defcustom kill-ring-max 120
  "Maximum length of kill ring before oldest elements are thrown away."
  :type 'natnum
  :group 'killing
  :version "29.1")

(defvar kill-ring-yank-pointer nil
  "The tail of the kill ring whose car is the last thing yanked.")

(defcustom save-interprogram-paste-before-kill nil
  "Whether to save existing clipboard text into kill ring before replacing it.
A non-nil value means the clipboard text is saved to the `kill-ring'
prior to any kill command.  Such text can subsequently be retrieved
via \\[yank] \\[yank-pop].  This ensures that Emacs kill operations
do not irrevocably overwrite existing clipboard text.

The value of this variable can also be a number, in which case the
clipboard data is only saved to the `kill-ring' if it's shorter
(in characters) than that number.  Any other non-nil value will save
the clipboard data unconditionally."
  :type '(choice (const nil)
                 number
                 (other :tag "Always" t))
  :group 'killing
  :version "23.2")

(defcustom kill-do-not-save-duplicates nil
  "If non-nil, don't add a string to `kill-ring' if it duplicates the last one.
The comparison is done using `equal-including-properties'."
  :type 'boolean
  :group 'killing
  :version "23.2")

(defcustom kill-transform-function nil
  "Function to call to transform a string before it's put on the kill ring.
The function is called with one parameter (the string that's to
be put on the kill ring).  It should return a string or nil.  If
the latter, the string is not put on the kill ring."
  :type '(choice (const :tag "No transform" nil)
                 function)
  :group 'killing
  :version "28.1")

(defun kill-new (string &optional replace)
  "Make STRING the latest kill in the kill ring.
Set `kill-ring-yank-pointer' to point to it.
If `interprogram-cut-function' is non-nil, apply it to STRING.
Optional second argument REPLACE non-nil means that STRING will replace
the front of the kill ring, rather than being added to the list.

When `save-interprogram-paste-before-kill' and `interprogram-paste-function'
are non-nil, save the interprogram paste string(s) into `kill-ring' before
STRING.

When the yank handler has a non-nil PARAM element, the original STRING
argument is not used by `insert-for-yank'.  However, since Lisp code
may access and use elements from the kill ring directly, the STRING
argument should still be a \"useful\" string for such uses."
  ;; Allow the user to transform or ignore the string.
  (when (or (not kill-transform-function)
            (setq string (funcall kill-transform-function string)))
    (unless (and kill-do-not-save-duplicates
	         ;; Due to text properties such as 'yank-handler that
	         ;; can alter the contents to yank, comparison using
	         ;; `equal' is unsafe.
	         (equal-including-properties string (car kill-ring)))
      (if (fboundp 'menu-bar-update-yank-menu)
	  (menu-bar-update-yank-menu string (and replace (car kill-ring)))))
    (when save-interprogram-paste-before-kill
      (let ((interprogram-paste
             (and interprogram-paste-function
                  ;; On X, the selection owner might be slow, so the user might
                  ;; interrupt this. If they interrupt it, we want to continue
                  ;; so we become selection owner, so this doesn't stay slow.
                  (if (eq (window-system) 'x)
                      (ignore-error quit (funcall interprogram-paste-function))
                    (funcall interprogram-paste-function)))))
        (when interprogram-paste
          (setq interprogram-paste
                (if (listp interprogram-paste)
                    ;; Use `reverse' to avoid modifying external data.
                    (reverse interprogram-paste)
		  (list interprogram-paste)))
          (when (or (not (numberp save-interprogram-paste-before-kill))
                    (< (seq-reduce #'+ (mapcar #'length interprogram-paste) 0)
                       save-interprogram-paste-before-kill))
            (dolist (s interprogram-paste)
	      (unless (and kill-do-not-save-duplicates
                           (equal-including-properties s (car kill-ring)))
	        (push s kill-ring)))))))
    (unless (and kill-do-not-save-duplicates
	         (equal-including-properties string (car kill-ring)))
      (if (and replace kill-ring)
	  (setcar kill-ring string)
        (let ((history-delete-duplicates nil))
          (add-to-history 'kill-ring string kill-ring-max t))))
    (setq kill-ring-yank-pointer kill-ring)
    (if interprogram-cut-function
        (funcall interprogram-cut-function string))))

;; It has been argued that this should work like `self-insert-command'
;; which merges insertions in `buffer-undo-list' in groups of 20
;; (hard-coded in `undo-auto-amalgamate').
(defcustom kill-append-merge-undo nil
  "Amalgamate appending kills with the last kill for undo.
When non-nil, appending or prepending text to the last kill makes
\\[undo] restore both pieces of text simultaneously."
  :type 'boolean
  :group 'killing
  :version "25.1")

(defun kill-append (string before-p)
  "Append STRING to the end of the latest kill in the kill ring.
If BEFORE-P is non-nil, prepend STRING to the kill instead.
If `interprogram-cut-function' is non-nil, call it with the
resulting kill.
If `kill-append-merge-undo' is non-nil, remove the last undo
boundary in the current buffer."
  (let ((cur (car kill-ring)))
    (kill-new (if before-p (concat string cur) (concat cur string))
              (or (= (length cur) 0)
                  (null (get-text-property 0 'yank-handler cur)))))
  (when (and kill-append-merge-undo (not buffer-read-only))
    (let ((prev buffer-undo-list)
          (next (cdr buffer-undo-list)))
      ;; Find the next undo boundary.
      (while (car next)
        (pop next)
        (pop prev))
      ;; Remove this undo boundary.
      (when prev
        (setcdr prev (cdr next))))))

(defcustom yank-pop-change-selection nil
  "Whether rotating the kill ring changes the window system selection.
If non-nil, whenever the kill ring is rotated (usually via the
`yank-pop' command), Emacs also calls `interprogram-cut-function'
to copy the new kill to the window system selection."
  :type 'boolean
  :group 'killing
  :version "23.1")

(defun current-kill (n &optional do-not-move)
  "Rotate the yanking point by N places, and then return that kill.
If N is zero and `interprogram-paste-function' is set to a
function that returns a string or a list of strings, and if that
function doesn't return nil, then that string (or list) is added
to the front of the kill ring and the string (or first string in
the list) is returned as the latest kill.

If N is not zero, and if `yank-pop-change-selection' is
non-nil, use `interprogram-cut-function' to transfer the
kill at the new yank point into the window system selection.

If optional arg DO-NOT-MOVE is non-nil, then don't actually
move the yanking point; just return the Nth kill forward."

  (let ((interprogram-paste (and (= n 0)
				 interprogram-paste-function
				 (funcall interprogram-paste-function))))
    (if interprogram-paste
	(progn
	  ;; Disable the interprogram cut function when we add the new
	  ;; text to the kill ring, so Emacs doesn't try to own the
	  ;; selection, with identical text.
          ;; Also disable the interprogram paste function, so that
          ;; `kill-new' doesn't call it repeatedly.
          (let ((interprogram-cut-function nil)
                (interprogram-paste-function nil))
	    (if (listp interprogram-paste)
                ;; Use `reverse' to avoid modifying external data.
                (mapc #'kill-new (reverse interprogram-paste))
	      (kill-new interprogram-paste)))
	  (car kill-ring))
      (or kill-ring (error "Kill ring is empty"))
      (let ((ARGth-kill-element
	     (nthcdr (mod (- n (length kill-ring-yank-pointer))
			  (length kill-ring))
		     kill-ring)))
	(unless do-not-move
	  (setq kill-ring-yank-pointer ARGth-kill-element)
	  (when (and yank-pop-change-selection
		     (> n 0)
		     interprogram-cut-function)
	    (funcall interprogram-cut-function (car ARGth-kill-element))))
	(car ARGth-kill-element)))))



;;;; Commands for manipulating the kill ring.

(defcustom kill-read-only-ok nil
  "Non-nil means don't signal an error for killing read-only text."
  :type 'boolean
  :group 'killing)

(defun kill-region (beg end &optional region)
  "Kill (\"cut\") text between point and mark.
This deletes the text from the buffer and saves it in the kill ring.
The command \\[yank] can retrieve it from there.
\(If you want to save the region without killing it, use \\[kill-ring-save].)

If you want to append the killed region to the last killed text,
use \\[append-next-kill] before \\[kill-region].

Any command that calls this function is a \"kill command\".
If the previous command was also a kill command,
the text killed this time appends to the text killed last time
to make one entry in the kill ring.

The killed text is filtered by `filter-buffer-substring' before it is
saved in the kill ring, so the actual saved text might be different
from what was killed.

If the buffer is read-only, Emacs will beep and refrain from deleting
the text, but put the text in the kill ring anyway.  This means that
you can use the killing commands to copy text from a read-only buffer.

Lisp programs should use this function for killing text.
 (To delete text, use `delete-region'.)
Supply two arguments, character positions BEG and END indicating the
 stretch of text to be killed.  If the optional argument REGION is
 non-nil, the function ignores BEG and END, and kills the current
 region instead.  Interactively, REGION is always non-nil, and so
 this command always kills the current region."
  ;; Pass mark first, then point, because the order matters when
  ;; calling `kill-append'.
  (interactive (progn
                 (let ((beg (mark))
                       (end (point)))
                   (unless (and beg end)
                     (user-error "The mark is not set now, so there is no region"))
                   (list beg end 'region))))
  (condition-case nil
      (let ((string (if region
                        (funcall region-extract-function 'delete)
                      (filter-buffer-substring beg end 'delete))))
	(when string			;STRING is nil if BEG = END
	  ;; Add that string to the kill ring, one way or another.
	  (if (eq last-command 'kill-region)
	      (kill-append string (< end beg))
	    (kill-new string)))
	(when (or string (eq last-command 'kill-region))
	  (setq this-command 'kill-region))
	(setq deactivate-mark t)
	nil)
    ((buffer-read-only text-read-only)
     ;; The code above failed because the buffer, or some of the characters
     ;; in the region, are read-only.
     ;; We should beep, in case the user just isn't aware of this.
     ;; However, there's no harm in putting
     ;; the region's text in the kill ring, anyway.
     (copy-region-as-kill beg end region)
     ;; Set this-command now, so it will be set even if we get an error.
     (setq this-command 'kill-region)
     ;; This should barf, if appropriate, and give us the correct error.
     (if kill-read-only-ok
	 (progn (message "Read only text copied to kill ring") nil)
       ;; Signal an error if the buffer is read-only.
       (barf-if-buffer-read-only)
       ;; If the buffer isn't read-only, the text is.
       (signal 'text-read-only (list (current-buffer)))))))

;; copy-region-as-kill no longer sets this-command, because it's confusing
;; to get two copies of the text when the user accidentally types M-w and
;; then corrects it with the intended C-w.
(defun copy-region-as-kill (beg end &optional region)
  "Save the region as if killed, but don't kill it.
In Transient Mark mode, deactivate the mark.
If `interprogram-cut-function' is non-nil, also save the text for a window
system cut and paste.

The copied text is filtered by `filter-buffer-substring' before it is
saved in the kill ring, so the actual saved text might be different
from what was in the buffer.

When called from Lisp, save in the kill ring the stretch of text
between BEG and END, unless the optional argument REGION is
non-nil, in which case ignore BEG and END, and save the current
region instead.

This command's old key binding has been given to `kill-ring-save'."
  ;; Pass mark first, then point, because the order matters when
  ;; calling `kill-append'.
  (interactive (list (mark) (point) 'region))
  (let ((str (if region
                 (funcall region-extract-function nil)
               (filter-buffer-substring beg end))))
    (if (eq last-command 'kill-region)
        (kill-append str (< end beg))
      (kill-new str)))
  (setq deactivate-mark t)
  nil)

(defun kill-ring-save (beg end &optional region)
  "Save the region as if killed, but don't kill it.
In Transient Mark mode, deactivate the mark.
If `interprogram-cut-function' is non-nil, also save the text for a window
system cut and paste.

If you want to append the killed region to the last killed text,
use \\[append-next-kill] before \\[kill-ring-save].

The copied text is filtered by `filter-buffer-substring' before it is
saved in the kill ring, so the actual saved text might be different
from what was in the buffer.

When called from Lisp, save in the kill ring the stretch of text
between BEG and END, unless the optional argument REGION is
non-nil, in which case ignore BEG and END, and save the current
region instead.

This command is similar to `copy-region-as-kill', except that it gives
visual feedback indicating the extent of the region being copied."
  ;; Pass mark first, then point, because the order matters when
  ;; calling `kill-append'.
  (interactive (list (mark) (point) 'region))
  (copy-region-as-kill beg end region)
  ;; This use of called-interactively-p is correct because the code it
  ;; controls just gives the user visual feedback.
  (if (called-interactively-p 'interactive)
      (indicate-copied-region)))


;; GNU simple.el (verbatim): yanking.
(defcustom yank-handled-properties
  '((font-lock-face . yank-handle-font-lock-face-property)
    (category . yank-handle-category-property))
  "List of special text property handling conditions for yanking.
Each element should have the form (PROP . FUN), where PROP is a
property symbol and FUN is a function.  When the `yank' command
inserts text into the buffer, it scans the inserted text for
stretches of text that have `eq' values of the text property
PROP; for each such stretch of text, FUN is called with three
arguments: the property's value in that text, and the start and
end positions of the text.

This is done prior to removing the properties specified by
`yank-excluded-properties'."
  :group 'killing
  :type '(repeat (cons (symbol :tag "property symbol")
                       function))
  :version "24.3")

;; This is actually used in subr.el but defcustom does not work there.
(defcustom yank-excluded-properties
  '(category field follow-link fontified font-lock-face help-echo
    intangible invisible keymap local-map mouse-face read-only
    yank-handler)
  "Text properties to discard when yanking.
The value should be a list of text properties to discard or t,
which means to discard all text properties.

See also `yank-handled-properties'."
  :type '(choice (const :tag "All" t) (repeat symbol))
  :group 'killing
  :version "24.3")

(defvar yank-transform-functions nil
  "Hook run on strings to be yanked.
Each function in this list will be called (in order) with the
string to be yanked as the sole argument, and should return the (possibly)
transformed string.

The functions will be called with the destination buffer as the current
buffer, and with point at the place where the string is to be inserted.")

(defvar yank-window-start nil)
(defvar yank-undo-function nil
  "If non-nil, function used by `yank-pop' to delete last stretch of yanked text.
Function is called with two parameters, START and END corresponding to
the value of the mark and point; it is guaranteed that START <= END.
Normally set from the UNDO element of a yank-handler; see `insert-for-yank'.")

(defun yank-pop (&optional arg)
  "Replace just-yanked stretch of killed text with a different stretch.
The main use of this command is immediately after a `yank' or a
`yank-pop'.  At such a time, the region contains a stretch of
reinserted (\"pasted\") previously-killed text.  `yank-pop' deletes
that text and inserts in its place a different stretch of killed text
by traversing the value of the `kill-ring' variable and selecting
another kill from there.

With no argument, the previous kill is inserted.
With argument N, insert the Nth previous kill.
If N is negative, it means to use a more recent kill.

The sequence of kills wraps around, so if you keep invoking this command
time after time, and pass the oldest kill, you get the newest one.

You can also invoke this command after a command other than `yank'
or `yank-pop'.  This is the same as invoking `yank-from-kill-ring',
including the effect of the prefix argument; see there for the details.

This command honors the `yank-handled-properties' and
`yank-excluded-properties' variables, and the `yank-handler' text
property, in the way that `yank' does."
  (interactive "p")
  (if (not (eq last-command 'yank))
      (yank-from-kill-ring (read-from-kill-ring "Yank from kill-ring: ")
                           current-prefix-arg)
    (setq this-command 'yank)
    (unless arg (setq arg 1))
    (let ((inhibit-read-only t)
          (before (< (point) (mark t))))
      (if before
          (funcall (or yank-undo-function 'delete-region) (point) (mark t))
        (funcall (or yank-undo-function 'delete-region) (mark t) (point)))
      (setq yank-undo-function nil)
      (set-marker (mark-marker) (point) (current-buffer))
      (insert-for-yank (current-kill arg))
      ;; Set the window start back where it was in the yank command,
      ;; if possible.
      (set-window-start (selected-window) yank-window-start t)
      (if before
          ;; This is like exchange-point-and-mark, but doesn't activate the mark.
          ;; It is cleaner to avoid activation, even though the command
          ;; loop would deactivate the mark because we inserted text.
          (goto-char (prog1 (mark t)
                       (set-marker (mark-marker) (point) (current-buffer))))))
    nil))

(defun yank (&optional arg)
  "Reinsert (\"paste\") the last stretch of killed text.
More precisely, reinsert the most recent kill, which is the stretch of
text most recently killed OR yanked, as returned by `current-kill' (which
see).  Put point at the end, and set mark at the beginning without
activating it. With just \\[universal-argument] as argument, put point
at beginning, and mark at end.
With argument N, reinsert the Nth most recent kill.

This command honors the `yank-handled-properties' and
`yank-excluded-properties' variables, and the `yank-handler' text
property, as described below.

Properties listed in `yank-handled-properties' are processed,
then those listed in `yank-excluded-properties' are discarded.

STRING will be run through `yank-transform-functions'.
`yank-in-context' is a command that uses this mechanism to
provide a `yank' alternative that conveniently preserves
string/comment syntax.

If STRING has a non-nil `yank-handler' property anywhere, the
normal insert behavior is altered, and instead, for each contiguous
segment of STRING that has a given value of the `yank-handler'
property, that value is used as follows:

The value of a `yank-handler' property must be a list of one to four
elements, of the form (FUNCTION PARAM NOEXCLUDE UNDO).
FUNCTION, if non-nil, should be a function of one argument (the
 object to insert); FUNCTION is called instead of `insert'.
PARAM, if present and non-nil, is passed to FUNCTION (to be handled
 in whatever way is appropriate; e.g. if FUNCTION is `yank-rectangle',
 PARAM may be a list of strings to insert as a rectangle).  If PARAM
 is nil, then the current segment of STRING is used.
If NOEXCLUDE is present and non-nil, the normal removal of
 `yank-excluded-properties' is not performed; instead FUNCTION is
 responsible for the removal.  This may be necessary if FUNCTION
 adjusts point before or after inserting the object.
UNDO, if present and non-nil, should be a function to be called
 by `yank-pop' to undo the insertion of the current PARAM.  It is
 given two arguments, the start and end of the region.  FUNCTION
 may set `yank-undo-function' to override UNDO.

See also the command `yank-pop' (\\[yank-pop])."
  (interactive "*P")
  (setq yank-window-start (window-start))
  ;; If we don't get all the way thru, make last-command indicate that
  ;; for the following command.
  (setq this-command t)
  (push-mark)
  (insert-for-yank (current-kill (cond
				  ((listp arg) 0)
				  ((eq arg '-) -2)
				  (t (1- arg)))))
  (if (consp arg)
      ;; This is like exchange-point-and-mark, but doesn't activate the mark.
      ;; It is cleaner to avoid activation, even though the command
      ;; loop would deactivate the mark because we inserted text.
      (goto-char (prog1 (mark t)
		   (set-marker (mark-marker) (point) (current-buffer)))))
  ;; If we do get all the way thru, make this-command indicate that.
  (if (eq this-command t)
      (setq this-command 'yank))
  nil)

(defun rotate-yank-pointer (arg)
  "Rotate the yanking point in the kill ring.
With ARG, rotate that many kills forward (or backward, if negative)."
  (interactive "p")
  (current-kill arg))


;; GNU subr.el (verbatim): yank insertion helpers.
(defun remove-yank-excluded-properties (start end)
  "Process text properties between START and END, inserted for a `yank'.
Perform the handling specified by `yank-handled-properties', then
remove properties specified by `yank-excluded-properties'."
  (let ((inhibit-read-only t))
    (dolist (handler yank-handled-properties)
      (let ((prop (car handler))
            (fun  (cdr handler))
            (run-start start))
        (while (< run-start end)
          (let ((value (get-text-property run-start prop))
                (run-end (next-single-property-change
                          run-start prop nil end)))
            (funcall fun value run-start run-end)
            (setq run-start run-end)))))
    (if (eq yank-excluded-properties t)
        (set-text-properties start end nil)
      (remove-list-of-text-properties start end yank-excluded-properties))))

(defvar yank-undo-function)

(defun insert-for-yank (string)
  "Insert STRING at point for the `yank' command.

This function is like `insert', except it honors the variables
`yank-handled-properties' and `yank-excluded-properties', and the
`yank-handler' text property, in the way that `yank' does.

It also runs the string through `yank-transform-functions'."
  ;; Allow altering the yank string.
  (run-hook-wrapped 'yank-transform-functions
                    (lambda (f) (setq string (funcall f string)) nil))
  (let (to)
    (while (setq to (next-single-property-change 0 'yank-handler string))
      (insert-for-yank-1 (substring string 0 to))
      (setq string (substring string to))))
  (insert-for-yank-1 string))

(defun insert-for-yank-1 (string)
  "Helper for `insert-for-yank', which see."
  (let* ((handler (and (stringp string)
		       (get-text-property 0 'yank-handler string)))
	 (param (or (nth 1 handler) string))
	 (opoint (point))
	 (inhibit-read-only inhibit-read-only)
	 end)

    ;; FIXME: This throws away any yank-undo-function set by previous calls
    ;; to insert-for-yank-1 within the loop of insert-for-yank!
    (setq yank-undo-function t)
    (if (nth 0 handler) ; FUNCTION
	(funcall (car handler) param)
      (insert param))
    (setq end (point))

    ;; Prevent read-only properties from interfering with the
    ;; following text property changes.
    (setq inhibit-read-only t)

    (unless (nth 2 handler) ; NOEXCLUDE
      (remove-yank-excluded-properties opoint end))

    ;; If last inserted char has properties, mark them as rear-nonsticky.
    (if (and (> end opoint)
	     (text-properties-at (1- end)))
	(put-text-property (1- end) end 'rear-nonsticky t))

    (if (eq yank-undo-function t)		   ; not set by FUNCTION
	(setq yank-undo-function (nth 3 handler))) ; UNDO
    (if (nth 4 handler)				   ; COMMAND
	(setq this-command (nth 4 handler)))))


;; GNU subr.el (verbatim): yank-handled-properties handlers.
(defun yank-handle-font-lock-face-property (face start end)
  "If `font-lock-defaults' is nil, apply FACE as a `face' property.
START and END denote the start and end of the text to act on.
Do nothing if FACE is nil."
  (and face
       (null font-lock-defaults)
       (put-text-property start end 'face face)))

;; This removes `mouse-face' properties in *Help* buffer buttons:
;; https://lists.gnu.org/r/emacs-devel/2002-04/msg00648.html
(defun yank-handle-category-property (category start end)
  "Apply property category CATEGORY's properties between START and END."
  (when category
    (let ((start2 start))
      (while (< start2 end)
	(let ((end2     (next-property-change start2 nil end))
	      (original (text-properties-at start2)))
	  (set-text-properties start2 end2 (symbol-plist category))
	  (add-text-properties start2 end2 original)
	  (setq start2 end2))))))



;; GNU window.c: the batch frame has no fringes.
(defun window-fringes (&optional _window)
  "Return fringe settings of specified WINDOW."
  (list 0 0 nil nil))
(defun set-window-fringes (_window _left &optional _right _outside-margins _persistent)
  "Set fringes of specified WINDOW."
  nil)

;; GNU subr.el (verbatim).
(defun buffer-narrowed-p ()
  "Return non-nil if the current buffer is narrowed."
  (/= (- (point-max) (point-min)) (buffer-size)))


;; GNU term/tty-colors.el (verbatim): batch color-name resolution.
;; `color-values' on a text terminal approximates any standard X color
;; name (from `color-name-rgb-alist') to the nearest of the 8 standard
;; tty colors, exactly as GNU does in --batch.

(defconst color-name-rgb-alist
  '(("snow"		65535 64250 64250)
    ("ghostwhite"	63736 63736 65535)
    ("whitesmoke"	62965 62965 62965)
    ("gainsboro"	56540 56540 56540)
    ("floralwhite"	65535 64250 61680)
    ("oldlace"		65021 62965 59110)
    ("linen"		64250 61680 59110)
    ("antiquewhite"	64250 60395 55255)
    ("papayawhip"	65535 61423 54741)
    ("blanchedalmond"	65535 60395 52685)
    ("bisque"		65535 58596 50372)
    ("peachpuff"	65535 56026 47545)
    ("navajowhite"	65535 57054 44461)
    ("moccasin"		65535 58596 46517)
    ("cornsilk"		65535 63736 56540)
    ("ivory"		65535 65535 61680)
    ("lemonchiffon"	65535 64250 52685)
    ("seashell"		65535 62965 61166)
    ("honeydew"		61680 65535 61680)
    ("mintcream"	62965 65535 64250)
    ("azure"		61680 65535 65535)
    ("aliceblue"	61680 63736 65535)
    ("lavender"		59110 59110 64250)
    ("lavenderblush"	65535 61680 62965)
    ("mistyrose"	65535 58596 57825)
    ("white"		65535 65535 65535)
    ("black"		    0     0     0)
    ("darkslategray"	12079 20303 20303)
    ("darkslategrey"	12079 20303 20303)
    ("dimgray"		26985 26985 26985)
    ("dimgrey"		26985 26985 26985)
    ("slategray"	28784 32896 37008)
    ("slategrey"	28784 32896 37008)
    ("lightslategray"	30583 34952 39321)
    ("lightslategrey"	30583 34952 39321)
    ("gray"		48830 48830 48830)
    ("grey"		48830 48830 48830)
    ("lightgrey"	54227 54227 54227)
    ("lightgray"	54227 54227 54227)
    ("midnightblue"	 6425  6425 28784)
    ("navy"		    0     0 32896)
    ("navyblue"		    0     0 32896)
    ("cornflowerblue"	25700 38293 60909)
    ("darkslateblue"	18504 15677 35723)
    ("slateblue"	27242 23130 52685)
    ("mediumslateblue"	31611 26728 61166)
    ("lightslateblue"	33924 28784 65535)
    ("mediumblue"	    0     0 52685)
    ("royalblue"	16705 26985 57825)
    ("blue"		    0     0 65535)
    ("dodgerblue"	 7710 37008 65535)
    ("deepskyblue"	    0 49087 65535)
    ("skyblue"		34695 52942 60395)
    ("lightskyblue"	34695 52942 64250)
    ("steelblue"	17990 33410 46260)
    ("lightsteelblue"	45232 50372 57054)
    ("lightblue"	44461 55512 59110)
    ("powderblue"	45232 57568 59110)
    ("paleturquoise"	44975 61166 61166)
    ("darkturquoise"	    0 52942 53713)
    ("mediumturquoise"	18504 53713 52428)
    ("turquoise"	16448 57568 53456)
    ("cyan"		    0 65535 65535)
    ("lightcyan"	57568 65535 65535)
    ("cadetblue"	24415 40606 41120)
    ("mediumaquamarine"	26214 52685 43690)
    ("aquamarine"	32639 65535 54484)
    ("darkgreen"	    0 25700     0)
    ("darkolivegreen"	21845 27499 12079)
    ("darkseagreen"	36751 48316 36751)
    ("seagreen"		11822 35723 22359)
    ("mediumseagreen"	15420 46003 29041)
    ("lightseagreen"	 8224 45746 43690)
    ("palegreen"	39064 64507 39064)
    ("springgreen"	    0 65535 32639)
    ("lawngreen"	31868 64764     0)
    ("green"		    0 65535     0)
    ("chartreuse"	32639 65535     0)
    ("mediumspringgreen"    0 64250 39578)
    ("greenyellow"	44461 65535 12079)
    ("limegreen"	12850 52685 12850)
    ("yellowgreen"	39578 52685 12850)
    ("forestgreen"	 8738 35723  8738)
    ("olivedrab"	27499 36494  8995)
    ("darkkhaki"	48573 47031 27499)
    ("khaki"		61680 59110 35980)
    ("palegoldenrod"	61166 59624 43690)
    ("lightgoldenrodyellow" 64250 64250 53970)
    ("lightyellow"	65535 65535 57568)
    ("yellow"		65535 65535     0)
    ("gold"		65535 55255     0)
    ("lightgoldenrod"	61166 56797 33410)
    ("goldenrod"	56026 42405  8224)
    ("darkgoldenrod"	47288 34438  2827)
    ("rosybrown"	48316 36751 36751)
    ("indianred"	52685 23644 23644)
    ("saddlebrown"	35723 17733  4883)
    ("sienna"		41120 21074 11565)
    ("peru"		52685 34181 16191)
    ("burlywood"	57054 47288 34695)
    ("beige"		62965 62965 56540)
    ("wheat"		62965 57054 46003)
    ("sandybrown"	62708 42148 24672)
    ("tan"		53970 46260 35980)
    ("chocolate"	53970 26985  7710)
    ("firebrick"	45746  8738  8738)
    ("brown"		42405 10794 10794)
    ("darksalmon"	59881 38550 31354)
    ("salmon"		64250 32896 29298)
    ("lightsalmon"	65535 41120 31354)
    ("orange"		65535 42405     0)
    ("darkorange"	65535 35980     0)
    ("coral"		65535 32639 20560)
    ("lightcoral"	61680 32896 32896)
    ("tomato"		65535 25443 18247)
    ("orangered"	65535 17733     0)
    ("red"		65535     0     0)
    ("hotpink"		65535 26985 46260)
    ("deeppink"		65535  5140 37779)
    ("pink"		65535 49344 52171)
    ("lightpink"	65535 46774 49601)
    ("palevioletred"	56283 28784 37779)
    ("maroon"		45232 12336 24672)
    ("mediumvioletred"	51143  5397 34181)
    ("violetred"	53456  8224 37008)
    ("magenta"		65535     0 65535)
    ("violet"		61166 33410 61166)
    ("plum"		56797 41120 56797)
    ("orchid"		56026 28784 54998)
    ("mediumorchid"	47802 21845 54227)
    ("darkorchid"	39321 12850 52428)
    ("darkviolet"	38036     0 54227)
    ("blueviolet"	35466 11051 58082)
    ("purple"		41120  8224 61680)
    ("mediumpurple"	37779 28784 56283)
    ("thistle"		55512 49087 55512)
    ("snow1"		65535 64250 64250)
    ("snow2"		61166 59881 59881)
    ("snow3"		52685 51657 51657)
    ("snow4"		35723 35209 35209)
    ("seashell1"	65535 62965 61166)
    ("seashell2"	61166 58853 57054)
    ("seashell3"	52685 50629 49087)
    ("seashell4"	35723 34438 33410)
    ("antiquewhite1"	65535 61423 56283)
    ("antiquewhite2"	61166 57311 52428)
    ("antiquewhite3"	52685 49344 45232)
    ("antiquewhite4"	35723 33667 30840)
    ("bisque1"		65535 58596 50372)
    ("bisque2"		61166 54741 47031)
    ("bisque3"		52685 47031 40606)
    ("bisque4"		35723 32125 27499)
    ("peachpuff1"	65535 56026 47545)
    ("peachpuff2"	61166 52171 44461)
    ("peachpuff3"	52685 44975 38293)
    ("peachpuff4"	35723 30583 25957)
    ("navajowhite1"	65535 57054 44461)
    ("navajowhite2"	61166 53199 41377)
    ("navajowhite3"	52685 46003 35723)
    ("navajowhite4"	35723 31097 24158)
    ("lemonchiffon1"	65535 64250 52685)
    ("lemonchiffon2"	61166 59881 49087)
    ("lemonchiffon3"	52685 51657 42405)
    ("lemonchiffon4"	35723 35209 28784)
    ("cornsilk1"	65535 63736 56540)
    ("cornsilk2"	61166 59624 52685)
    ("cornsilk3"	52685 51400 45489)
    ("cornsilk4"	35723 34952 30840)
    ("ivory1"		65535 65535 61680)
    ("ivory2"		61166 61166 57568)
    ("ivory3"		52685 52685 49601)
    ("ivory4"		35723 35723 33667)
    ("honeydew1"	61680 65535 61680)
    ("honeydew2"	57568 61166 57568)
    ("honeydew3"	49601 52685 49601)
    ("honeydew4"	33667 35723 33667)
    ("lavenderblush1"	65535 61680 62965)
    ("lavenderblush2"	61166 57568 58853)
    ("lavenderblush3"	52685 49601 50629)
    ("lavenderblush4"	35723 33667 34438)
    ("mistyrose1"	65535 58596 57825)
    ("mistyrose2"	61166 54741 53970)
    ("mistyrose3"	52685 47031 46517)
    ("mistyrose4"	35723 32125 31611)
    ("azure1"		61680 65535 65535)
    ("azure2"		57568 61166 61166)
    ("azure3"		49601 52685 52685)
    ("azure4"		33667 35723 35723)
    ("slateblue1"	33667 28527 65535)
    ("slateblue2"	31354 26471 61166)
    ("slateblue3"	26985 22873 52685)
    ("slateblue4"	18247 15420 35723)
    ("royalblue1"	18504 30326 65535)
    ("royalblue2"	17219 28270 61166)
    ("royalblue3"	14906 24415 52685)
    ("royalblue4"	10023 16448 35723)
    ("blue1"		    0     0 65535)
    ("blue2"		    0     0 61166)
    ("blue3"		    0     0 52685)
    ("blue4"		    0     0 35723)
    ("dodgerblue1"	 7710 37008 65535)
    ("dodgerblue2"	 7196 34438 61166)
    ("dodgerblue3"	 6168 29812 52685)
    ("dodgerblue4"	 4112 20046 35723)
    ("steelblue1"	25443 47288 65535)
    ("steelblue2"	23644 44204 61166)
    ("steelblue3"	20303 38036 52685)
    ("steelblue4"	13878 25700 35723)
    ("deepskyblue1"	    0 49087 65535)
    ("deepskyblue2"	    0 45746 61166)
    ("deepskyblue3"	    0 39578 52685)
    ("deepskyblue4"	    0 26728 35723)
    ("skyblue1"		34695 52942 65535)
    ("skyblue2"		32382 49344 61166)
    ("skyblue3"		27756 42662 52685)
    ("skyblue4"		19018 28784 35723)
    ("lightskyblue1"	45232 58082 65535)
    ("lightskyblue2"	42148 54227 61166)
    ("lightskyblue3"	36237 46774 52685)
    ("lightskyblue4"	24672 31611 35723)
    ("slategray1"	50886 58082 65535)
    ("slategray2"	47545 54227 61166)
    ("slategray3"	40863 46774 52685)
    ("slategray4"	27756 31611 35723)
    ("lightsteelblue1"	51914 57825 65535)
    ("lightsteelblue2"	48316 53970 61166)
    ("lightsteelblue3"	41634 46517 52685)
    ("lightsteelblue4"	28270 31611 35723)
    ("lightblue1"	49087 61423 65535)
    ("lightblue2"	45746 57311 61166)
    ("lightblue3"	39578 49344 52685)
    ("lightblue4"	26728 33667 35723)
    ("lightcyan1"	57568 65535 65535)
    ("lightcyan2"	53713 61166 61166)
    ("lightcyan3"	46260 52685 52685)
    ("lightcyan4"	31354 35723 35723)
    ("paleturquoise1"	48059 65535 65535)
    ("paleturquoise2"	44718 61166 61166)
    ("paleturquoise3"	38550 52685 52685)
    ("paleturquoise4"	26214 35723 35723)
    ("cadetblue1"	39064 62965 65535)
    ("cadetblue2"	36494 58853 61166)
    ("cadetblue3"	31354 50629 52685)
    ("cadetblue4"	21331 34438 35723)
    ("turquoise1"	    0 62965 65535)
    ("turquoise2"	    0 58853 61166)
    ("turquoise3"	    0 50629 52685)
    ("turquoise4"	    0 34438 35723)
    ("cyan1"		    0 65535 65535)
    ("cyan2"		    0 61166 61166)
    ("cyan3"		    0 52685 52685)
    ("cyan4"		    0 35723 35723)
    ("darkslategray1"	38807 65535 65535)
    ("darkslategray2"	36237 61166 61166)
    ("darkslategray3"	31097 52685 52685)
    ("darkslategray4"	21074 35723 35723)
    ("aquamarine1"	32639 65535 54484)
    ("aquamarine2"	30326 61166 50886)
    ("aquamarine3"	26214 52685 43690)
    ("aquamarine4"	17733 35723 29812)
    ("darkseagreen1"	49601 65535 49601)
    ("darkseagreen2"	46260 61166 46260)
    ("darkseagreen3"	39835 52685 39835)
    ("darkseagreen4"	26985 35723 26985)
    ("seagreen1"	21588 65535 40863)
    ("seagreen2"	20046 61166 38036)
    ("seagreen3"	17219 52685 32896)
    ("seagreen4"	11822 35723 22359)
    ("palegreen1"	39578 65535 39578)
    ("palegreen2"	37008 61166 37008)
    ("palegreen3"	31868 52685 31868)
    ("palegreen4"	21588 35723 21588)
    ("springgreen1"	    0 65535 32639)
    ("springgreen2"	    0 61166 30326)
    ("springgreen3"	    0 52685 26214)
    ("springgreen4"	    0 35723 17733)
    ("green1"		    0 65535     0)
    ("green2"		    0 61166     0)
    ("green3"		    0 52685     0)
    ("green4"		    0 35723     0)
    ("chartreuse1"	32639 65535     0)
    ("chartreuse2"	30326 61166     0)
    ("chartreuse3"	26214 52685     0)
    ("chartreuse4"	17733 35723     0)
    ("olivedrab1"	49344 65535 15934)
    ("olivedrab2"	46003 61166 14906)
    ("olivedrab3"	39578 52685 12850)
    ("olivedrab4"	26985 35723  8738)
    ("darkolivegreen1"	51914 65535 28784)
    ("darkolivegreen2"	48316 61166 26728)
    ("darkolivegreen3"	41634 52685 23130)
    ("darkolivegreen4"	28270 35723 15677)
    ("khaki1"		65535 63222 36751)
    ("khaki2"		61166 59110 34181)
    ("khaki3"		52685 50886 29555)
    ("khaki4"		35723 34438 20046)
    ("lightgoldenrod1"	65535 60652 35723)
    ("lightgoldenrod2"	61166 56540 33410)
    ("lightgoldenrod3"	52685 48830 28784)
    ("lightgoldenrod4"	35723 33153 19532)
    ("lightyellow1"	65535 65535 57568)
    ("lightyellow2"	61166 61166 53713)
    ("lightyellow3"	52685 52685 46260)
    ("lightyellow4"	35723 35723 31354)
    ("yellow1"		65535 65535     0)
    ("yellow2"		61166 61166     0)
    ("yellow3"		52685 52685     0)
    ("yellow4"		35723 35723     0)
    ("gold1"		65535 55255     0)
    ("gold2"		61166 51657     0)
    ("gold3"		52685 44461     0)
    ("gold4"		35723 30069     0)
    ("goldenrod1"	65535 49601  9509)
    ("goldenrod2"	61166 46260  8738)
    ("goldenrod3"	52685 39835  7453)
    ("goldenrod4"	35723 26985  5140)
    ("darkgoldenrod1"	65535 47545  3855)
    ("darkgoldenrod2"	61166 44461  3598)
    ("darkgoldenrod3"	52685 38293  3084)
    ("darkgoldenrod4"	35723 25957  2056)
    ("rosybrown1"	65535 49601 49601)
    ("rosybrown2"	61166 46260 46260)
    ("rosybrown3"	52685 39835 39835)
    ("rosybrown4"	35723 26985 26985)
    ("indianred1"	65535 27242 27242)
    ("indianred2"	61166 25443 25443)
    ("indianred3"	52685 21845 21845)
    ("indianred4"	35723 14906 14906)
    ("sienna1"		65535 33410 18247)
    ("sienna2"		61166 31097 16962)
    ("sienna3"		52685 26728 14649)
    ("sienna4"		35723 18247  9766)
    ("burlywood1"	65535 54227 39835)
    ("burlywood2"	61166 50629 37265)
    ("burlywood3"	52685 43690 32125)
    ("burlywood4"	35723 29555 21845)
    ("wheat1"		65535 59367 47802)
    ("wheat2"		61166 55512 44718)
    ("wheat3"		52685 47802 38550)
    ("wheat4"		35723 32382 26214)
    ("tan1"		65535 42405 20303)
    ("tan2"		61166 39578 18761)
    ("tan3"		52685 34181 16191)
    ("tan4"		35723 23130 11051)
    ("chocolate1"	65535 32639  9252)
    ("chocolate2"	61166 30326  8481)
    ("chocolate3"	52685 26214  7453)
    ("chocolate4"	35723 17733  4883)
    ("firebrick1"	65535 12336 12336)
    ("firebrick2"	61166 11308 11308)
    ("firebrick3"	52685  9766  9766)
    ("firebrick4"	35723  6682  6682)
    ("brown1"		65535 16448 16448)
    ("brown2"		61166 15163 15163)
    ("brown3"		52685 13107 13107)
    ("brown4"		35723  8995  8995)
    ("salmon1"		65535 35980 26985)
    ("salmon2"		61166 33410 25186)
    ("salmon3"		52685 28784 21588)
    ("salmon4"		35723 19532 14649)
    ("lightsalmon1"	65535 41120 31354)
    ("lightsalmon2"	61166 38293 29298)
    ("lightsalmon3"	52685 33153 25186)
    ("lightsalmon4"	35723 22359 16962)
    ("orange1"		65535 42405     0)
    ("orange2"		61166 39578     0)
    ("orange3"		52685 34181     0)
    ("orange4"		35723 23130     0)
    ("darkorange1"	65535 32639     0)
    ("darkorange2"	61166 30326     0)
    ("darkorange3"	52685 26214     0)
    ("darkorange4"	35723 17733     0)
    ("coral1"		65535 29298 22102)
    ("coral2"		61166 27242 20560)
    ("coral3"		52685 23387 17733)
    ("coral4"		35723 15934 12079)
    ("tomato1"		65535 25443 18247)
    ("tomato2"		61166 23644 16962)
    ("tomato3"		52685 20303 14649)
    ("tomato4"		35723 13878  9766)
    ("orangered1"	65535 17733     0)
    ("orangered2"	61166 16448     0)
    ("orangered3"	52685 14135     0)
    ("orangered4"	35723  9509     0)
    ("red1"		65535     0     0)
    ("red2"		61166     0     0)
    ("red3"		52685     0     0)
    ("red4"		35723     0     0)
    ("deeppink1"	65535  5140 37779)
    ("deeppink2"	61166  4626 35209)
    ("deeppink3"	52685  4112 30326)
    ("deeppink4"	35723  2570 20560)
    ("hotpink1"		65535 28270 46260)
    ("hotpink2"		61166 27242 42919)
    ("hotpink3"		52685 24672 37008)
    ("hotpink4"		35723 14906 25186)
    ("pink1"		65535 46517 50629)
    ("pink2"		61166 43433 47288)
    ("pink3"		52685 37265 40606)
    ("pink4"		35723 25443 27756)
    ("lightpink1"	65535 44718 47545)
    ("lightpink2"	61166 41634 44461)
    ("lightpink3"	52685 35980 38293)
    ("lightpink4"	35723 24415 25957)
    ("palevioletred1"	65535 33410 43947)
    ("palevioletred2"	61166 31097 40863)
    ("palevioletred3"	52685 26728 35209)
    ("palevioletred4"	35723 18247 23901)
    ("maroon1"		65535 13364 46003)
    ("maroon2"		61166 12336 42919)
    ("maroon3"		52685 10537 37008)
    ("maroon4"		35723  7196 25186)
    ("violetred1"	65535 15934 38550)
    ("violetred2"	61166 14906 35980)
    ("violetred3"	52685 12850 30840)
    ("violetred4"	35723  8738 21074)
    ("magenta1"		65535     0 65535)
    ("magenta2"		61166     0 61166)
    ("magenta3"		52685     0 52685)
    ("magenta4"		35723     0 35723)
    ("orchid1"		65535 33667 64250)
    ("orchid2"		61166 31354 59881)
    ("orchid3"		52685 26985 51657)
    ("orchid4"		35723 18247 35209)
    ("plum1"		65535 48059 65535)
    ("plum2"		61166 44718 61166)
    ("plum3"		52685 38550 52685)
    ("plum4"		35723 26214 35723)
    ("mediumorchid1"	57568 26214 65535)
    ("mediumorchid2"	53713 24415 61166)
    ("mediumorchid3"	46260 21074 52685)
    ("mediumorchid4"	31354 14135 35723)
    ("darkorchid1"	49087 15934 65535)
    ("darkorchid2"	45746 14906 61166)
    ("darkorchid3"	39578 12850 52685)
    ("darkorchid4"	26728  8738 35723)
    ("purple1"		39835 12336 65535)
    ("purple2"		37265 11308 61166)
    ("purple3"		32125  9766 52685)
    ("purple4"		21845  6682 35723)
    ("mediumpurple1"	43947 33410 65535)
    ("mediumpurple2"	40863 31097 61166)
    ("mediumpurple3"	35209 26728 52685)
    ("mediumpurple4"	23901 18247 35723)
    ("thistle1"		65535 57825 65535)
    ("thistle2"		61166 53970 61166)
    ("thistle3"		52685 46517 52685)
    ("thistle4"		35723 31611 35723)
    ("gray0"		    0     0     0)
    ("grey0"		    0     0     0)
    ("gray1"		  771   771   771)
    ("grey1"		  771   771   771)
    ("gray2"		 1285  1285  1285)
    ("grey2"		 1285  1285  1285)
    ("gray3"		 2056  2056  2056)
    ("grey3"		 2056  2056  2056)
    ("gray4"		 2570  2570  2570)
    ("grey4"		 2570  2570  2570)
    ("gray5"		 3341  3341  3341)
    ("grey5"		 3341  3341  3341)
    ("gray6"		 3855  3855  3855)
    ("grey6"		 3855  3855  3855)
    ("gray7"		 4626  4626  4626)
    ("grey7"		 4626  4626  4626)
    ("gray8"		 5140  5140  5140)
    ("grey8"		 5140  5140  5140)
    ("gray9"		 5911  5911  5911)
    ("grey9"		 5911  5911  5911)
    ("gray10"		 6682  6682  6682)
    ("grey10"		 6682  6682  6682)
    ("gray11"		 7196  7196  7196)
    ("grey11"		 7196  7196  7196)
    ("gray12"		 7967  7967  7967)
    ("grey12"		 7967  7967  7967)
    ("gray13"		 8481  8481  8481)
    ("grey13"		 8481  8481  8481)
    ("gray14"		 9252  9252  9252)
    ("grey14"		 9252  9252  9252)
    ("gray15"		 9766  9766  9766)
    ("grey15"		 9766  9766  9766)
    ("gray16"		10537 10537 10537)
    ("grey16"		10537 10537 10537)
    ("gray17"		11051 11051 11051)
    ("grey17"		11051 11051 11051)
    ("gray18"		11822 11822 11822)
    ("grey18"		11822 11822 11822)
    ("gray19"		12336 12336 12336)
    ("grey19"		12336 12336 12336)
    ("gray20"		13107 13107 13107)
    ("grey20"		13107 13107 13107)
    ("gray21"		13878 13878 13878)
    ("grey21"		13878 13878 13878)
    ("gray22"		14392 14392 14392)
    ("grey22"		14392 14392 14392)
    ("gray23"		15163 15163 15163)
    ("grey23"		15163 15163 15163)
    ("gray24"		15677 15677 15677)
    ("grey24"		15677 15677 15677)
    ("gray25"		16448 16448 16448)
    ("grey25"		16448 16448 16448)
    ("gray26"		16962 16962 16962)
    ("grey26"		16962 16962 16962)
    ("gray27"		17733 17733 17733)
    ("grey27"		17733 17733 17733)
    ("gray28"		18247 18247 18247)
    ("grey28"		18247 18247 18247)
    ("gray29"		19018 19018 19018)
    ("grey29"		19018 19018 19018)
    ("gray30"		19789 19789 19789)
    ("grey30"		19789 19789 19789)
    ("gray31"		20303 20303 20303)
    ("grey31"		20303 20303 20303)
    ("gray32"		21074 21074 21074)
    ("grey32"		21074 21074 21074)
    ("gray33"		21588 21588 21588)
    ("grey33"		21588 21588 21588)
    ("gray34"		22359 22359 22359)
    ("grey34"		22359 22359 22359)
    ("gray35"		22873 22873 22873)
    ("grey35"		22873 22873 22873)
    ("gray36"		23644 23644 23644)
    ("grey36"		23644 23644 23644)
    ("gray37"		24158 24158 24158)
    ("grey37"		24158 24158 24158)
    ("gray38"		24929 24929 24929)
    ("grey38"		24929 24929 24929)
    ("gray39"		25443 25443 25443)
    ("grey39"		25443 25443 25443)
    ("gray40"		26214 26214 26214)
    ("grey40"		26214 26214 26214)
    ("gray41"		26985 26985 26985)
    ("grey41"		26985 26985 26985)
    ("gray42"		27499 27499 27499)
    ("grey42"		27499 27499 27499)
    ("gray43"		28270 28270 28270)
    ("grey43"		28270 28270 28270)
    ("gray44"		28784 28784 28784)
    ("grey44"		28784 28784 28784)
    ("gray45"		29555 29555 29555)
    ("grey45"		29555 29555 29555)
    ("gray46"		30069 30069 30069)
    ("grey46"		30069 30069 30069)
    ("gray47"		30840 30840 30840)
    ("grey47"		30840 30840 30840)
    ("gray48"		31354 31354 31354)
    ("grey48"		31354 31354 31354)
    ("gray49"		32125 32125 32125)
    ("grey49"		32125 32125 32125)
    ("gray50"		32639 32639 32639)
    ("grey50"		32639 32639 32639)
    ("gray51"		33410 33410 33410)
    ("grey51"		33410 33410 33410)
    ("gray52"		34181 34181 34181)
    ("grey52"		34181 34181 34181)
    ("gray53"		34695 34695 34695)
    ("grey53"		34695 34695 34695)
    ("gray54"		35466 35466 35466)
    ("grey54"		35466 35466 35466)
    ("gray55"		35980 35980 35980)
    ("grey55"		35980 35980 35980)
    ("gray56"		36751 36751 36751)
    ("grey56"		36751 36751 36751)
    ("gray57"		37265 37265 37265)
    ("grey57"		37265 37265 37265)
    ("gray58"		38036 38036 38036)
    ("grey58"		38036 38036 38036)
    ("gray59"		38550 38550 38550)
    ("grey59"		38550 38550 38550)
    ("gray60"		39321 39321 39321)
    ("grey60"		39321 39321 39321)
    ("gray61"		40092 40092 40092)
    ("grey61"		40092 40092 40092)
    ("gray62"		40606 40606 40606)
    ("grey62"		40606 40606 40606)
    ("gray63"		41377 41377 41377)
    ("grey63"		41377 41377 41377)
    ("gray64"		41891 41891 41891)
    ("grey64"		41891 41891 41891)
    ("gray65"		42662 42662 42662)
    ("grey65"		42662 42662 42662)
    ("gray66"		43176 43176 43176)
    ("grey66"		43176 43176 43176)
    ("gray67"		43947 43947 43947)
    ("grey67"		43947 43947 43947)
    ("gray68"		44461 44461 44461)
    ("grey68"		44461 44461 44461)
    ("gray69"		45232 45232 45232)
    ("grey69"		45232 45232 45232)
    ("gray70"		46003 46003 46003)
    ("grey70"		46003 46003 46003)
    ("gray71"		46517 46517 46517)
    ("grey71"		46517 46517 46517)
    ("gray72"		47288 47288 47288)
    ("grey72"		47288 47288 47288)
    ("gray73"		47802 47802 47802)
    ("grey73"		47802 47802 47802)
    ("gray74"		48573 48573 48573)
    ("grey74"		48573 48573 48573)
    ("gray75"		49087 49087 49087)
    ("grey75"		49087 49087 49087)
    ("gray76"		49858 49858 49858)
    ("grey76"		49858 49858 49858)
    ("gray77"		50372 50372 50372)
    ("grey77"		50372 50372 50372)
    ("gray78"		51143 51143 51143)
    ("grey78"		51143 51143 51143)
    ("gray79"		51657 51657 51657)
    ("grey79"		51657 51657 51657)
    ("gray80"		52428 52428 52428)
    ("grey80"		52428 52428 52428)
    ("gray81"		53199 53199 53199)
    ("grey81"		53199 53199 53199)
    ("gray82"		53713 53713 53713)
    ("grey82"		53713 53713 53713)
    ("gray83"		54484 54484 54484)
    ("grey83"		54484 54484 54484)
    ("gray84"		54998 54998 54998)
    ("grey84"		54998 54998 54998)
    ("gray85"		55769 55769 55769)
    ("grey85"		55769 55769 55769)
    ("gray86"		56283 56283 56283)
    ("grey86"		56283 56283 56283)
    ("gray87"		57054 57054 57054)
    ("grey87"		57054 57054 57054)
    ("gray88"		57568 57568 57568)
    ("grey88"		57568 57568 57568)
    ("gray89"		58339 58339 58339)
    ("grey89"		58339 58339 58339)
    ("gray90"		58853 58853 58853)
    ("grey90"		58853 58853 58853)
    ("gray91"		59624 59624 59624)
    ("grey91"		59624 59624 59624)
    ("gray92"		60395 60395 60395)
    ("grey92"		60395 60395 60395)
    ("gray93"		60909 60909 60909)
    ("grey93"		60909 60909 60909)
    ("gray94"		61680 61680 61680)
    ("grey94"		61680 61680 61680)
    ("gray95"		62194 62194 62194)
    ("grey95"		62194 62194 62194)
    ("gray96"		62965 62965 62965)
    ("grey96"		62965 62965 62965)
    ("gray97"		63479 63479 63479)
    ("grey97"		63479 63479 63479)
    ("gray98"		64250 64250 64250)
    ("grey98"		64250 64250 64250)
    ("gray99"		64764 64764 64764)
    ("grey99"		64764 64764 64764)
    ("gray100"		65535 65535 65535)
    ("grey100"		65535 65535 65535)
    ("darkgrey"		43433 43433 43433)
    ("darkgray"		43433 43433 43433)
    ("darkblue"		    0     0 35723)
    ("darkcyan"		    0 35723 35723) ; no "lightmagenta", see comment above
    ("darkmagenta"	35723     0 35723)
    ("darkred"		35723     0     0)  ; but no "lightred", see comment above
    ("lightgreen"	37008 61166 37008))
  "An alist of X color names and associated 16-bit RGB values.")

(defconst tty-standard-colors
  '(("black"	0     0     0     0)
    ("red"	1 65535     0     0)
    ("green"	2     0 65535     0)
    ("yellow"	3 65535 65535     0)
    ("blue"	4     0     0 65535)
    ("magenta"	5 65535     0 65535)
    ("cyan"	6     0 65535 65535)
    ("white"	7 65535 65535 65535))
  "An alist of 8 standard tty colors, their indices and RGB values.")

(defun tty-color-alist (&optional _frame)
  "Return an alist of colors supported by FRAME's terminal.
FRAME defaults to the selected frame.
Each element of the returned alist is of the form:
 (NAME INDEX R G B)
where NAME is the name of the color, a string;
INDEX is the index of this color to be sent to the terminal driver
when the color should be displayed; it is typically a small integer;
R, G, and B are the intensities of, accordingly, red, green, and blue
components of the color, represented as numbers between 0 and 65535.
The file `etc/rgb.txt' in the Emacs distribution lists the standard
RGB values of the X colors.  If RGB is nil, this color will not be
considered by `tty-color-translate' as an approximation to another
color."
  tty-defined-color-alist)

(defun tty-color-canonicalize (color)
  "Return COLOR in canonical form.
A canonicalized color name is all-lower case, with any blanks removed."
  (let ((case-fold-search nil))
    (if (string-match-p "[A-Z ]" color)
	(replace-regexp-in-string " +" "" (downcase color))
      color)))

(defun tty-color-24bit (rgb &optional display)
  "Return 24-bit color pixel value for RGB value on DISPLAY.
DISPLAY can be a display name or a frame, and defaults to the
selected frame's display.
If DISPLAY is not on a 24-but TTY terminal, return nil."
  (when (and rgb (= (display-color-cells display) 16777216))
    (let ((r (ash (car rgb) -8))
	  (g (ash (cadr rgb) -8))
	  (b (ash (nth 2 rgb) -8)))
      (logior (ash r 16) (ash g 8) b))))

(defun tty-color-off-gray-diag (r g b)
  "Compute the angle between the color given by R,G,B and the gray diagonal.
The gray diagonal is the diagonal of the 3D cube in RGB space which
connects the points corresponding to the black and white colors.  All the
colors whose RGB coordinates belong to this diagonal are various shades
of gray, thus the name."
  (let ((mag (sqrt (* 3 (+ (* r r) (* g g) (* b b))))))
    (if (< mag 1) 0 (acos (/ (+ r g b) mag)))))

(defun tty-color-approximate (rgb &optional frame)
  "Find the color in `tty-color-alist' that best approximates RGB.
Value is a list of the form (NAME INDEX R G B).
The argument RGB should be an rgb value, that is, a list of three
integers in the 0..65535 range.
FRAME defaults to the selected frame."
  (let* ((color-list (tty-color-alist frame))
	 (candidate (car color-list))
	 (best-distance 195076)	;; 3 * 255^2 + 15
	 (r (ash (car rgb) -8))
	 (g (ash (cadr rgb) -8))
	 (b (ash (nth 2 rgb) -8))
	 best-color)
    (while candidate
      (let ((try-rgb (cddr candidate))
	    ;; If the approximated color is not close enough to the
	    ;; gray diagonal of the RGB cube, favor non-gray colors.
	    ;; (The number 0.065 is an empirical ad-hoc'ery.)
	    (favor-non-gray (>= (tty-color-off-gray-diag r g b) 0.065))
	    try-r try-g try-b
	    dif-r dif-g dif-b dist)
	;; If the RGB values of the candidate color are unknown, we
	;; never consider it for approximating another color.
	(if try-rgb
	    (progn
	      (setq try-r (ash (car try-rgb) -8)
		    try-g (ash (cadr try-rgb) -8)
		    try-b (ash (nth 2 try-rgb) -8))
	      (setq dif-r (- r try-r)
		    dif-g (- g try-g)
		    dif-b (- b try-b))
	      (setq dist (+ (* dif-r dif-r) (* dif-g dif-g) (* dif-b dif-b)))
	      (if (and (< dist best-distance)
		       ;; The candidate color is on the gray diagonal
		       ;; if its RGB components are all equal.
		       (or (/= try-r try-g) (/= try-g try-b)
			   (not favor-non-gray)))
		  (setq best-distance dist
			best-color candidate)))))
      (setq color-list (cdr color-list))
      (setq candidate (car color-list)))
    best-color))

(defun tty-color-standard-values (color)
"Return standard RGB values of the color COLOR.

The result is a list of integer RGB values--(RED GREEN BLUE).
These values range from 0 to 65535; white is (65535 65535 65535).

The returned value reflects the standard Emacs definition of
COLOR (see the info node `(emacs) Colors'), regardless of whether
the terminal can display it, so the return value should be the
same regardless of what display is being used."
  (or (color-values-from-color-spec color)
      (cdr (assoc color color-name-rgb-alist))))

(defun tty-color-values (color &optional frame)
  "Return RGB values of the color COLOR on a termcap frame FRAME.

If COLOR is not directly supported by the display, return the RGB
values for a supported color that is its best approximation.
The value is a list of integer RGB values--(RED GREEN BLUE).
These values range from 0 to 65535; white is (65535 65535 65535).
If FRAME is omitted or nil, use the selected frame."
  (cddr (tty-color-desc color frame)))

(defun tty-color-desc (color &optional frame)
  "Return the description of the color COLOR for a character terminal.
Value is a list of the form (NAME INDEX R G B).  The returned NAME or
RGB value may not be the same as the argument COLOR, because the latter
might need to be approximated if it is not supported directly."
  (and (stringp color)
       (let ((color (tty-color-canonicalize color)))
	  (or (assoc color (tty-color-alist frame))
	      (let ((rgb (tty-color-standard-values color)))
		(and rgb
		     (let ((pixel (tty-color-24bit rgb frame)))
		       (or (and pixel (cons color (cons pixel rgb)))
			   (tty-color-approximate rgb frame)))))))))


;; GNU startup registers the 8 standard colors for the initial tty
;; terminal (tty-register-default-colors); this is that end state.
(defvar tty-defined-color-alist (mapcar #'copy-sequence tty-standard-colors)
  "An alist of defined terminal colors and their RGB values.")

;; GNU faces.el (verbatim).
(defun color-values (color &optional frame)
  "Return a description of the color named COLOR on frame FRAME.
COLOR should be a string naming a color (e.g. \"white\"), or a
string specifying a color's RGB components (e.g. \"#ff12ec\").

Return a list of three integers, (RED GREEN BLUE), each between 0
and 65535 inclusive.
Use `color-name-to-rgb' if you want RGB floating-point values
normalized to 1.0.

If FRAME is omitted or nil, use the selected frame.
If FRAME cannot display COLOR, the value is nil.

COLOR can also be the symbol `unspecified' or one of the strings
\"unspecified-fg\" or \"unspecified-bg\", in which case the
return value is nil."
  (cond
   ((member color '(unspecified "unspecified-fg" "unspecified-bg"))
    nil)
   ((display-graphic-p frame)
    (xw-color-values color frame))
   (t
    (tty-color-values color frame))))

;; GNU faces.el (verbatim).
(defun readable-foreground-color (color)
  "Return a readable foreground color for background COLOR.
The returned value is a string representing black or white, depending
on which one provides better contrast with COLOR."
  ;; We use #ffffff instead of "white", because the latter is sometimes
  ;; less than white.  That way, we get the best contrast possible.
  (if (color-dark-p (mapcar (lambda (c) (/ c 65535.0))
                            (color-values color)))
      "#ffffff" "black"))

(defconst color-luminance-dark-limit 0.325
  "The relative luminance below which a color is considered \"dark\".
A \"dark\" color in this sense provides better contrast with white
than with black; see `color-dark-p'.
This value was determined experimentally.")

(defun color-dark-p (rgb)
  "Whether RGB is more readable against white than black.
RGB is a 3-element list (R G B), each component in the range [0,1].
This predicate can be used both for determining a suitable (black or white)
contrast color with RGB as background and as foreground."
  (unless (<= 0 (apply #'min rgb) (apply #'max rgb) 1)
    (error "RGB components %S not in [0,1]" rgb))
  ;; Compute the relative luminance after gamma-correcting (assuming sRGB),
  ;; and compare to a cut-off value determined experimentally.
  ;; See https://en.wikipedia.org/wiki/Relative_luminance for details.
  (let* ((sr (nth 0 rgb))
         (sg (nth 1 rgb))
         (sb (nth 2 rgb))
         ;; Gamma-correct the RGB components to linear values.
         ;; Use the power 2.2 as an approximation to sRGB gamma;
         ;; it should be good enough for the purpose of this function.
         (r (expt sr 2.2))
         (g (expt sg 2.2))
         (b (expt sb 2.2))
         (y (+ (* r 0.2126) (* g 0.7152) (* b 0.0722))))
    (< y color-luminance-dark-limit)))

;; GNU faces.el (verbatim).
(defconst list-faces-sample-text
  "abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  "Text string to display as the sample text for `list-faces-display'.")


(defvar help-xref-stack)
(defun list-faces-display (&optional regexp)
  "List all faces, using the same sample text in each.
The sample text is a string that comes from the variable
`list-faces-sample-text'.

If REGEXP is non-nil, list only those faces with names matching
this regular expression.  When called interactively with a prefix
argument, prompt for a regular expression using `read-regexp'."
  (interactive (list (and current-prefix-arg
                          (read-regexp "List faces matching regexp"))))
  (let ((all-faces (zerop (length regexp)))
	(frame (selected-frame))
	(max-length 0)
	faces line-format
	disp-frame window face-name)
    ;; We filter and take the max length in one pass
    (setq faces
	  (delq nil
		(mapcar (lambda (f)
			  (let ((s (symbol-name f)))
			    (when (or all-faces (string-match-p regexp s))
			      (setq max-length (max (length s) max-length))
			      f)))
			(sort (face-list) #'string-lessp))))
    (unless faces
      (error "No faces matching \"%s\"" regexp))
    (setq max-length (1+ max-length)
	  line-format (format "%%-%ds" max-length))
    (with-help-window "*Faces*"
      (with-current-buffer standard-output
	(setq truncate-lines t)
	(insert
	 (substitute-command-keys
	  (concat
	   "\\<help-mode-map>Use "
	   (if (display-mouse-p) "\\[help-follow-mouse] or ")
	   "\\[help-follow] on a face name to customize it\n"
	   "or on its sample text for a description of the face.\n\n")))
	(setq help-xref-stack nil)
	(dolist (face faces)
	  (setq face-name (symbol-name face))
	  (insert (format line-format face-name))
	  ;; Hyperlink to a customization buffer for the face.  Using
	  ;; the help xref mechanism may not be the best way.
	  (save-excursion
	    (save-match-data
	      (search-backward face-name)
	      (setq help-xref-stack-item `(list-faces-display ,regexp))
	      (help-xref-button 0 'help-customize-face face)))
	  (let ((beg (point))
		(line-beg (line-beginning-position)))
	    (insert list-faces-sample-text)
	    ;; Hyperlink to a help buffer for the face.
	    (save-excursion
	      (save-match-data
		(search-backward list-faces-sample-text)
		(help-xref-button 0 'help-face face)))
	    (insert "\n")
	    (put-text-property beg (1- (point)) 'face face)
	    ;; Make all face commands default to the proper face
	    ;; anywhere in the line.
	    (put-text-property line-beg (1- (point)) 'read-face-name face)
	    ;; If the sample text has multiple lines, line up all of them.
	    (goto-char beg)
	    (forward-line 1)
	    (while (not (eobp))
	      (insert-char ?\s max-length)
	      (forward-line 1))))
	(goto-char (point-min))))
    ;; If the *Faces* buffer appears in a different frame,
    ;; copy all the face definitions from FRAME,
    ;; so that the display will reflect the frame that was selected.
    (setq window (get-buffer-window (get-buffer "*Faces*") t))
    (setq disp-frame (if window (window-frame window)
		       (car (frame-list))))
    (or (eq frame disp-frame)
	(dolist (face (face-list))
	  (copy-face face face frame disp-frame)))))

;; GNU faces.el (verbatim).
(defun face-documentation (face)
  "Get the documentation string for FACE.
If FACE is a face-alias, get the documentation for the target face."
  (let ((alias (get face 'face-alias)))
    (if alias
        (let ((doc (documentation-property alias 'face-documentation)))
	  (format "%s is an alias for the face `%s'.%s" face alias
                  (if doc (format "\n%s" doc)
                    "")))
      (documentation-property face 'face-documentation))))


(defun set-face-documentation (face string)
  "Set the documentation string for FACE to STRING."
  ;; Perhaps the text should go in DOC.
  (put face 'face-documentation (purecopy string)))

;; GNU faces.el (verbatim).
(defsubst face-default-spec (face)
  "Return the default face-spec for FACE, ignoring any user customization.
If there is no default for FACE, return nil."
  (get face 'face-defface-spec))

(defsubst face-user-default-spec (face)
  "Return the user's customized face-spec for FACE, or the default if none.
If there is neither a user setting nor a default for FACE, return nil."
  (or (get face 'customized-face)
      (get face 'saved-face)
      (face-default-spec face)))


(defun face-spec-choose (spec &optional frame no-match-retval)
  "Return the proper attributes for FRAME, out of SPEC.

Value is a plist of face attributes in the form of attribute-value pairs.
If no match is found or SPEC is nil, return nil, unless NO-MATCH-RETVAL
is given, in which case return its value instead."
  (unless frame
    (setq frame (selected-frame)))
  (let ((tail spec)
	result defaults match-found)
    (while tail
      (let* ((entry (pop tail))
	     (display (car entry))
	     (attrs (cdr entry))
	     thisval)
	;; Get the attributes as actually specified by this alternative.
	(setq thisval
	      (if (null (cdr attrs)) ;; was (listp (car attrs))
		  ;; Old-style entry, the attribute list is the
		  ;; first element.
		  (car attrs)
		attrs))

	;; If the condition is `default', that sets the default
	;; for following conditions.
	(if (eq display 'default)
	    (setq defaults thisval)
	  ;; Otherwise, if it matches, use it.
	  (when (face-spec-set-match-display display frame)
	    (setq result thisval
	          tail nil
		  match-found t)))))
    ;; If defaults have been found, it's safe to just append those to the result
    ;; list (which at this point will be either nil or contain actual specs) and
    ;; return it to the caller. Since there will most definitely be something to
    ;; return in this case, there's no need to know/check if a match was found.
    (if defaults
	(append defaults result)
      (if match-found
	  result
	no-match-retval))))

(defun face-spec-set-match-display (display frame)
  "Non-nil if DISPLAY matches FRAME.
DISPLAY is part of a spec such as can be used in `defface'.
If FRAME is nil, the current FRAME is used."
  (let* ((conjuncts display)
	 conjunct req options
	 ;; t means we have succeeded against all the conjuncts in
	 ;; DISPLAY that have been tested so far.
	 (match t))
    (if (eq conjuncts t)
	(setq conjuncts nil))
    (while (and conjuncts match)
      (setq conjunct (car conjuncts)
	    conjuncts (cdr conjuncts)
	    req (car conjunct)
	    options (cdr conjunct)
	    match (cond ((eq req 'type)
			 (or (memq (window-system frame) options)
			     (and (memq 'graphic options)
				  (memq (window-system frame) '(x w32 ns pgtk)))
			     ;; FIXME: This should be revisited to use
			     ;; display-graphic-p, provided that the
			     ;; color selection depends on the number
			     ;; of supported colors, and all defface's
			     ;; are changed to look at number of colors
			     ;; instead of (type graphic) etc.
			     (if (null (window-system frame))
				 (memq 'tty options)
			       (or (and (memq 'motif options)
					(featurep 'motif))
				   (and (memq 'gtk options)
					(featurep 'gtk))
				   (and (memq 'lucid options)
					(featurep 'x-toolkit)
					(not (featurep 'motif))
					(not (featurep 'gtk)))
				   (and (memq 'x-toolkit options)
					(featurep 'x-toolkit))))))
			((eq req 'min-colors)
			 (>= (display-color-cells frame) (car options)))
			((eq req 'class)
			 (memq (frame-parameter frame 'display-type) options))
			((eq req 'background)
			 (memq (frame-parameter frame 'background-mode)
			       options))
			((eq req 'supports)
			 (display-supports-face-attributes-p options frame))
			(t (error "Unknown req `%S' with options `%S'"
				  req options)))))
    match))


(defun face-attr-match-p (face attrs &optional frame)
  "Return t if attributes of FACE match values in plist ATTRS.
Optional parameter FRAME is the frame whose definition of FACE
is used.  If nil or omitted, use the selected frame."
  (unless frame
    (setq frame (selected-frame)))
  (let* ((list face-attribute-name-alist)
	 (match t)
	 (bold (and (plist-member attrs :bold)
		    (not (plist-member attrs :weight))))
	 (italic (and (plist-member attrs :italic)
		      (not (plist-member attrs :slant))))
	 (plist (if (or bold italic)
		    (copy-sequence attrs)
		  attrs)))
    ;; Handle the Emacs 20 :bold and :italic properties.
    (if bold
	(plist-put plist :weight (if bold 'bold 'normal)))
    (if italic
	(plist-put plist :slant (if italic 'italic 'normal)))
    (while (and match list)
      (let* ((attr (caar list))
	     (specified-value
	      (if (plist-member plist attr)
		  (plist-get plist attr)
		'unspecified))
	     (value-now (face-attribute face attr frame)))
	(setq match (equal specified-value value-now))
	(setq list (cdr list))))
    match))

(defsubst face-spec-match-p (face spec &optional frame)
  "Return t if FACE, on FRAME, matches what SPEC says it should look like."
  (face-attr-match-p face (face-spec-choose spec frame) frame))

;; GNU doc.c documentation-stringp.
(defun documentation-stringp (object)
  "Return non-nil if OBJECT is a well-formed docstring object.
OBJECT can be either a string or a reference if it's kept externally."
  (or (stringp object)
      (integerp object)                 ; Reference to DOC.
      (and (consp object)               ; Reference to .elc.
           (stringp (car object))
           (integerp (cdr object)))))

;; GNU custom.el (verbatim).
(defun custom-handle-all-keywords (symbol args type)
  "For customization option SYMBOL, handle keyword arguments ARGS.
Third argument TYPE is the custom option type."
  (unless (memq :group args)
    (let ((cg (custom-current-group)))
      (when cg
        (custom-add-to-group cg symbol type))))
  (while args
    (let ((arg (car args)))
      (setq args (cdr args))
      (unless (symbolp arg)
	(error "Junk in args %S" args))
      (let ((keyword arg)
	    (value (car args)))
	(unless args
	  (error "Keyword %s is missing an argument" keyword))
	(setq args (cdr args))
	(custom-handle-keyword symbol keyword value type)))))

(defun custom-handle-keyword (symbol keyword value type)
  "For customization option SYMBOL, handle KEYWORD with VALUE.
Fourth argument TYPE is the custom option type."
  (if purify-flag
      (setq value (purecopy value)))
  (cond ((eq keyword :group)
	 (custom-add-to-group value symbol type))
	((eq keyword :version)
	 (custom-add-version symbol value))
	((eq keyword :package-version)
	 (custom-add-package-version symbol value))
	((eq keyword :link)
	 (custom-add-link symbol value))
	((eq keyword :load)
	 (custom-add-load symbol value))
	((eq keyword :tag)
	 (put symbol 'custom-tag value))
	((eq keyword :set-after)
	 (custom-add-dependencies symbol value))
	(t
	 (error "Unknown keyword %s" keyword))))

(defun custom-add-dependencies (symbol value)
  "To the custom option SYMBOL, add dependencies specified by VALUE.
VALUE should be a list of symbols.  For each symbol in that list,
this specifies that SYMBOL should be set after the specified symbol,
if both appear in constructs like `custom-set-variables'."
  (unless (listp value)
    (error "Invalid custom dependency `%s'" value))
  (let* ((deps (get symbol 'custom-dependencies))
	 (new-deps deps))
    (while value
      (let ((dep (car value)))
	(unless (symbolp dep)
	  (error "Invalid custom dependency `%s'" dep))
	(unless (memq dep new-deps)
	  (setq new-deps (cons dep new-deps)))
	(setq value (cdr value))))
    (unless (eq deps new-deps)
      (put symbol 'custom-dependencies new-deps))))

(defun custom-add-link (symbol widget)
  "To the custom option SYMBOL add the link WIDGET."
  (let ((links (get symbol 'custom-links)))
    (unless (member widget links)
      (put symbol 'custom-links (cons (purecopy widget) links)))))

(defun custom-add-version (symbol version)
  "To the custom option SYMBOL add the version VERSION."
  (put symbol 'custom-version (purecopy version)))

(defun custom-add-package-version (symbol version)
  "To the custom option SYMBOL add the package version VERSION."
  (put symbol 'custom-package-version (purecopy version)))

;; GNU cus-face.el (verbatim).
(defun custom-declare-face (face spec doc &rest args)
  "Like `defface', but with FACE evaluated as a normal argument."
  (when (and doc
             (not (documentation-stringp doc)))
    (error "Invalid (or missing) doc string %S" doc))
  (unless (get face 'face-defface-spec)
    (face-spec-set face (purecopy spec) 'face-defface-spec)
    (push (cons 'defface face) current-load-list)
    (when doc
      (set-face-documentation face (purecopy doc)))
    (custom-handle-all-keywords face args 'custom-face)
    (run-hooks 'custom-define-hook))
  face)

;; GNU loaddefs: cus-edit autoloads used by preloaded code.
(autoload 'customize-face "cus-edit"
  "Customize FACE, which should be a face name or nil." t)
(autoload 'customize-face-other-window "cus-edit"
  "Show customization buffer for face FACE in other window." t)
(autoload 'describe-face "help-fns"
  "Display the properties of face FACE on FRAME." t)

;; GNU faces.el (verbatim).
(defconst face-attribute-name-alist
  '((:family . "font family")
    (:foundry . "font foundry")
    (:width . "character set width")
    (:height . "height in 1/10 pt")
    (:weight . "weight")
    (:slant . "slant")
    (:underline . "underline")
    (:overline . "overline")
    (:extend . "extend")
    (:strike-through . "strike-through")
    (:box . "box")
    (:inverse-video . "inverse-video display")
    (:foreground . "foreground color")
    (:background . "background color")
    (:stipple . "background stipple")
    (:inherit . "inheritance"))
  "An alist of descriptive names for face attributes.
Each element has the form (ATTRIBUTE-NAME . DESCRIPTION) where
ATTRIBUTE-NAME is a face attribute name (a keyword symbol), and
DESCRIPTION is a descriptive name for ATTRIBUTE-NAME.")


(defun face-descriptive-attribute-name (attribute)
  "Return a descriptive name for ATTRIBUTE."
  (cdr (assq attribute face-attribute-name-alist)))

;; GNU custom.el (verbatim).
(defun custom-fix-face-spec (spec)
  "Convert face SPEC, replacing obsolete :bold and :italic attributes.
Also change :reverse-video to :inverse-video."
  (when (listp spec)
    (if (or (memq :bold spec)
	    (memq :italic spec)
	    (memq :inverse-video spec))
	(let (result)
	  (while spec
	    (let ((key (car spec))
		  (val (car (cdr spec))))
	      (cond ((eq key :italic)
		     (push :slant result)
		     (push (if val 'italic 'normal) result))
		    ((eq key :bold)
		     (push :weight result)
		     (push (if val 'bold 'normal) result))
		    ((eq key :reverse-video)
		     (push :inverse-video result)
		     (push val result))
		    (t
		     (push key result)
		     (push val result))))
	    (setq spec (cddr spec)))
	  (nreverse result))
      spec)))
;; GNU progmodes/elisp-mode.el (verbatim).
(define-derived-mode lisp-interaction-mode emacs-lisp-mode "Lisp Interaction"
  "Major mode for typing and evaluating Lisp forms."
  :abbrev-table nil
  (setq-local lexical-binding t))

;; GNU buffer.c: the C-managed list of buffer-local minor modes
;; currently enabled in the buffer.
(defvar-local local-minor-modes nil
  "Minor modes currently active in the current buffer.
This is a list of mode commands.")

;; GNU minibuf.c DEFVAR: whether to mask characters in the minibuffer.
(defvar read-hide-char nil
  "Whether to hide input characters in noninteractive mode.
If non-nil, it must be a character, which will be used to mask the
input characters.")

;; GNU emacs-lisp/cursor-sensor.el subset: erc-stamp toggles these modes
;; and reads `cursor-sensor-inhibit'.  Batch has no redisplay, so the
;; point-motion machinery is inert, but the variable and modes must exist.
(defvar cursor-sensor-inhibit nil
  "When non-nil, suspend `cursor-sensor-mode' and `cursor-intangible-mode'.
By convention, this is a list of symbols where each symbol stands for the
\"cause\" of the suspension.")

(defun cursor-sensor--move-to-tangible (_window) nil)

(define-minor-mode cursor-intangible-mode
  "Keep cursor outside of any `cursor-intangible' text property."
  :global nil
  (if cursor-intangible-mode
      (add-hook 'pre-redisplay-functions #'cursor-sensor--move-to-tangible
                nil t)
    (remove-hook 'pre-redisplay-functions #'cursor-sensor--move-to-tangible t)))

(defun cursor-sensor--detect (&optional _window) nil)

(define-minor-mode cursor-sensor-mode
  "Handle the `cursor-sensor-functions' text property."
  :global nil
  (if cursor-sensor-mode
      (add-hook 'pre-redisplay-functions #'cursor-sensor--detect nil t)
    (remove-hook 'pre-redisplay-functions #'cursor-sensor--detect t)))
