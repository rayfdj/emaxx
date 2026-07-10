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
  "Evaluate BODY, then display the help buffer BUFFER-OR-NAME."
  (declare (indent 1))
  `(let ((emaxx--help-window-buffer (get-buffer-create ,buffer-or-name)))
     (with-current-buffer emaxx--help-window-buffer
       (let ((inhibit-read-only t))
         (erase-buffer)))
     (prog1 (progn ,@body)
       (display-buffer emaxx--help-window-buffer))))

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
