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

;;; simple_compat.el ends here
