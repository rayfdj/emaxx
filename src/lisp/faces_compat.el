;;; faces_compat.el --- Batch face compatibility helpers -*- lexical-binding: t; -*-

;;; Commentary:

;; This file provides a narrow slice of face/theme helpers that upstream
;; batch Emacs normally has available before the target test file loads.

;;; Code:

(defvar emaxx--next-face-id 1)
(defvar custom-theme-load-path nil)
(defvar custom-enabled-themes nil)
(defvar custom-known-themes nil)

(defun emaxx--register-theme (theme)
  (unless (memq theme custom-known-themes)
    (setq custom-known-themes
          (append custom-known-themes (list theme)))))

(defmacro deftheme (theme &optional doc &rest properties)
  `(progn
     (emaxx--register-theme ',theme)
     (put ',theme 'theme-documentation ,doc)
     ',theme))

(defun provide-theme (theme)
  (emaxx--register-theme theme)
  (put theme 'emaxx-theme-loaded t)
  theme)

(defun emaxx--normalize-face (face)
  (if (stringp face)
      (intern face)
    face))

(defun face-id (face &optional _frame)
  (setq face (emaxx--normalize-face face))
  (unless (facep face)
    (error "Not a face: %s" face))
  (or (get face 'face)
      (let ((alias (get face 'face-alias)))
        (if alias
            (face-id alias)
          (setq emaxx--next-face-id (1+ emaxx--next-face-id))
          (put face 'face emaxx--next-face-id)
          emaxx--next-face-id))))

(defun set-face-foreground (face color &optional frame)
  (set-face-attribute face frame :foreground (or color 'unspecified)))

(defun set-face-background (face color &optional frame)
  (set-face-attribute face frame :background (or color 'unspecified)))

(defun emaxx--unquote-face-spec (value)
  (if (and (consp value) (eq (car value) 'quote))
      (car (cdr value))
    value))

(defun emaxx--face-spec-default-attrs (spec)
  (setq spec (emaxx--unquote-face-spec spec))
  (catch 'attrs
    (dolist (clause spec)
      (when (and (consp clause)
                 (let ((display (car clause)))
                   (or (eq display t) (eq display 'default))))
        (let ((tail (cdr clause)))
          (when tail
            (if (and (null (cdr tail))
                     (consp (car tail))
                     (keywordp (car (car tail))))
                (throw 'attrs (car tail))
              (throw 'attrs tail))))))
    nil))

(defun emaxx--plist-keys (plist)
  (let (keys)
    (while plist
      (when (keywordp (car plist))
        (setq keys (cons (car plist) keys)))
      (setq plist (cdr (cdr plist))))
    keys))

(defun emaxx--apply-face-attributes (face attrs)
  (while attrs
    (set-face-attribute face nil (car attrs) (car (cdr attrs)))
    (setq attrs (cdr (cdr attrs)))))

(defun custom-theme-set-faces (theme &rest specs)
  (emaxx--register-theme theme)
  (put theme 'emaxx-theme-face-specs
       (append (or (get theme 'emaxx-theme-face-specs) nil) specs))
  (when (memq theme custom-enabled-themes)
    (dolist (spec specs)
      (emaxx--recompute-theme-face (car spec))))
  theme)

(defun emaxx--theme-face-spec (theme face)
  (let ((specs (or (get theme 'emaxx-theme-face-specs) nil))
        found)
    (dolist (spec specs)
      (when (eq (car spec) face)
        (setq found spec)))
    found))

(defun emaxx--theme-face-attrs (theme face)
  (let ((spec (emaxx--theme-face-spec theme face)))
    (if spec
        (emaxx--face-spec-default-attrs (car (cdr spec)))
      nil)))

(defun emaxx--face-known-attributes (face)
  (let ((keys (emaxx--plist-keys
               (emaxx--face-spec-default-attrs (get face 'face-defface-spec)))))
    (dolist (theme custom-known-themes)
      (let ((theme-keys (emaxx--plist-keys (emaxx--theme-face-attrs theme face))))
        (dolist (key theme-keys)
          (unless (memq key keys)
            (setq keys (cons key keys))))))
    keys))

(defun emaxx--clear-face-attributes (face)
  (when (facep face)
    (dolist (attribute (emaxx--face-known-attributes face))
      (set-face-attribute face nil attribute
                          (if (eq attribute :inherit) nil 'unspecified)))))

(defun emaxx--recompute-theme-face (face)
  (when (facep face)
    (emaxx--clear-face-attributes face)
    (let ((base (emaxx--face-spec-default-attrs (get face 'face-defface-spec))))
      (when base
        (emaxx--apply-face-attributes face base)))
    (dolist (theme custom-enabled-themes)
      (let ((attrs (emaxx--theme-face-attrs theme face)))
        (when attrs
          (emaxx--apply-face-attributes face attrs))))))

(defun enable-theme (theme)
  (unless (get theme 'emaxx-theme-loaded)
    (load-theme theme nil t))
  (unless (memq theme custom-enabled-themes)
    (setq custom-enabled-themes
          (append custom-enabled-themes (list theme))))
  (dolist (spec (or (get theme 'emaxx-theme-face-specs) nil))
    (emaxx--recompute-theme-face (car spec)))
  theme)

(defun disable-theme (theme)
  (let (remaining)
    (dolist (entry custom-enabled-themes)
      (unless (eq entry theme)
        (setq remaining (cons entry remaining))))
    (setq custom-enabled-themes (nreverse remaining)))
  (dolist (spec (or (get theme 'emaxx-theme-face-specs) nil))
    (emaxx--recompute-theme-face (car spec)))
  theme)

(defun emaxx--find-theme-file (theme)
  (let ((base (concat (symbol-name theme) "-theme")))
    (catch 'found
      (dolist (entry custom-theme-load-path)
        (when (stringp entry)
          (let ((source (expand-file-name (concat base ".el") entry))
                (plain (expand-file-name base entry)))
            (cond
             ((file-readable-p source)
              (throw 'found source))
             ((file-readable-p plain)
              (throw 'found plain))))))
      nil)))

(defun load-theme (theme &optional _no-confirm no-enable)
  (unless (get theme 'emaxx-theme-loaded)
    (let ((file (emaxx--find-theme-file theme)))
      (unless file
        (error "Unable to find theme %s" theme))
      (load file)))
  (unless no-enable
    (enable-theme theme))
  theme)

(defun frame-set-background-mode (&optional _frame _keep)
  nil)

(defun emaxx--face-ref-list-p (value)
  (and (listp value)
       value
       (not (keywordp (car value)))
       (not (memq (car value) '(foreground-color background-color)))))

(defun emaxx--attribute-from-face-ref (face attribute)
  (cond
   ((and face (symbolp face))
    (let ((value (face-attribute face attribute nil t)))
      (if (eq value 'unspecified)
          nil
        value)))
   ((and (consp face) (keywordp (car face)))
    (plist-get face attribute))
   (t nil)))

(defun emaxx--attribute-at-point (attribute)
  (let ((faces (or (get-char-property (point) 'read-face-name)
                   (and font-lock-mode
                        (get-char-property (point) 'font-lock-face))
                   (get-char-property (point) 'face)))
        found)
    (if (emaxx--face-ref-list-p faces)
        (dolist (face faces)
          (unless found
            (setq found (emaxx--attribute-from-face-ref face attribute))))
      (setq found (emaxx--attribute-from-face-ref faces attribute)))
    (or found
        (face-attribute 'default attribute))))

(defun foreground-color-at-point ()
  (emaxx--attribute-at-point :foreground))

(defun background-color-at-point ()
  (emaxx--attribute-at-point :background))

(defun font-lock-fontify-region (&optional beg end)
  (setq beg (or beg (point-min)))
  (setq end (or end (point-max)))
  (font-lock-ensure beg end)
  (when (eq major-mode 'emacs-lisp-mode)
    (save-excursion
      (goto-char beg)
      (while (re-search-forward "^;.*$" end t)
        (put-text-property (match-beginning 0) (match-end 0)
                           'face 'font-lock-comment-face))
      (goto-char beg)
      (while (re-search-forward "`[^']+'" end t)
        (put-text-property (match-beginning 0) (match-end 0)
                           'face 'font-lock-constant-face))))
  nil)

(defun tty-find-type (pred type)
  (let (hyphend)
    (while (and type
                (not (funcall pred type)))
      (setq type
            (if (setq hyphend (string-match-p "[-_.][^-_.]+$" type))
                (substring type 0 hyphend)
              nil))))
  type)

(defface tooltip '((t)) "Compatibility tooltip face.")
(put 'tooltip 'face 1)
(defface diff-changed '((t)) "Compatibility diff face.")
(defface font-lock-comment-face '((t)) "Compatibility font-lock face.")
(defface font-lock-constant-face '((t)) "Compatibility font-lock face.")

(provide 'emaxx-faces-compat)

;;; faces_compat.el ends here
