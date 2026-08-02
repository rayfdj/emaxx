use super::*;

// Prefix a macro-table entry is renamed to when a function definition
// shadows it (see `shadow_macro_binding').
pub(crate) const MACRO_SHADOW_PREFIX: &str = "--emaxx-shadowed-macro--";

// Marks an env frame whose bindings are FUNCTION bindings (cl-flet /
// cl-labels).  Only such frames may shadow a builtin in the function
// position: a plain `let' of a variable named `car' to a lambda must not
// hijack `(car x)' (GNU separates value and function cells).
pub(crate) const FUNCTION_FRAME_MARKER: &str = "--emaxx-function-frame--";

fn dynamic_library_suffix_values() -> Vec<Value> {
    #[cfg(target_os = "macos")]
    let suffixes = [".dylib", ".so"].as_slice();
    #[cfg(all(unix, not(target_os = "macos")))]
    let suffixes = [".so"].as_slice();
    #[cfg(windows)]
    let suffixes = [".dll"].as_slice();
    suffixes
        .iter()
        .map(|suffix| Value::String((*suffix).into()))
        .collect()
}

fn module_file_suffix_value() -> Value {
    #[cfg(target_os = "macos")]
    let suffix = ".dylib";
    #[cfg(all(unix, not(target_os = "macos")))]
    let suffix = ".so";
    #[cfg(windows)]
    let suffix = ".dll";
    Value::String(suffix.into())
}

impl Interpreter {
    pub fn lookup_var(&self, name: &str, env: &Env) -> Option<Value> {
        // Cow avoids a per-lookup String allocation for the overwhelmingly
        // common non-aliased name.
        let resolved: std::borrow::Cow<str> = if self.direct_variable_alias(name).is_none() {
            name.into()
        } else {
            self.resolve_variable_name(name)
                .unwrap_or_else(|_| name.to_string())
                .into()
        };
        self.lookup_var_with_resolved_name(name, resolved.as_ref(), env)
    }

    fn lookup_var_with_resolved_name(
        &self,
        name: &str,
        resolved: &str,
        env: &Env,
    ) -> Option<Value> {
        let mut special: Option<bool> = None;
        for (index, frame) in env.iter().enumerate().rev() {
            // Below the caller boundary, references to SPECIAL variables
            // resolve dynamically like GNU rather than through a caller's
            // same-named lexical binding (bug#47552 semantics).  A name made
            // locally special by a `defvar' in THIS scope (marker above the
            // floor) is dynamic here too, so it must likewise not resolve to
            // a caller's same-named lexical binding — e.g. `erc--run-send-hooks'
            // reads its own dynamic `str', never `erc-send-current-line's
            // lexical one.
            if index < self.special_scan_floor
                && *special.get_or_insert_with(|| {
                    self.is_dynamic_binding_name(name) || self.local_special_active(name, env)
                })
            {
                break;
            }
            let shared_updates = Self::frame_identity(frame)
                .and_then(|frame_id| self.lexical_cell_updates.get(&frame_id));
            for (k, v) in frame.iter().rev() {
                if k == name {
                    return Some(
                        shared_updates
                            .and_then(|updates| updates.get(name))
                            .cloned()
                            .unwrap_or_else(|| v.clone()),
                    );
                }
            }
        }
        // A buffer-local cell always wins over a dynamically bound default.
        // This also covers a plain special that becomes buffer-local while
        // its global `let' is active: GNU then reads the newly created local
        // cell, while the specbind layer continues to own/restores only the
        // default value.
        if let Some(value) = self.buffer_local_value(self.current_buffer_id(), resolved) {
            return Some(value);
        }
        if let Some(value) = self.active_global_special_value(resolved) {
            return value.or_else(|| self.builtin_var_value(resolved));
        }
        if let Some(value) = self.global_value(resolved) {
            return Some(value);
        }
        self.builtin_var_value(resolved)
    }

    pub fn symbol_value_cell(&self, name: &str) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(name)?;
        self.lookup_var_with_resolved_name(&resolved, &resolved, &Env::new())
            .ok_or(LispError::Void(resolved))
    }

    pub(crate) fn builtin_var_value(&self, name: &str) -> Option<Value> {
        match name {
            "nil" => Some(Value::Nil),
            "t" => Some(Value::T),
            "region-extract-function" => Some(Value::Symbol(
                "emaxx-default-region-extract-function".into(),
            )),
            "region-insert-function" => Some(Value::Symbol(
                "emaxx-default-region-insert-function".into(),
            )),
            "redisplay-highlight-region-function" => {
                Some(Value::Symbol("redisplay--highlight-overlay-function".into()))
            }
            "redisplay-unhighlight-region-function" => Some(Value::Symbol(
                "redisplay--unhighlight-overlay-function".into(),
            )),
            "case-fold-search" => Some(Value::T),
            // buffer.c's reset_buffer_local_variables defaults.  These are
            // native DEFVAR_PER_BUFFER slots, so they exist before any Lisp
            // library is loaded even when their value is nil.
            "abbrev-mode"
            | "auto-fill-function"
            | "bidi-paragraph-direction"
            | "bidi-paragraph-separate-re"
            | "bidi-paragraph-start-re"
            | "buffer-backed-up"
            | "buffer-display-time"
            | "buffer-file-format"
            | "fringe-cursor-alist"
            | "fringe-indicator-alist"
            | "fringes-outside-margins"
            | "indicate-buffer-boundaries"
            | "indicate-empty-lines"
            | "left-fringe-width"
            | "local-minor-modes"
            | "point-before-scroll"
            | "right-fringe-width"
            | "scroll-bar-height"
            | "scroll-bar-width"
            | "scroll-down-aggressively"
            | "scroll-up-aggressively"
            | "text-conversion-style"
            | "truncate-lines"
            | "word-wrap" => Some(Value::Nil),
            "bidi-display-reordering"
            | "buffer-auto-save-file-format"
            | "cache-long-scans"
            | "ctl-arrow"
            | "cursor-type"
            | "cursor-in-non-selected-windows"
            | "horizontal-scroll-bar"
            | "selective-display-ellipses"
            | "vertical-scroll-bar" => Some(Value::T),
            "buffer-display-count"
            | "buffer-saved-size"
            | "left-margin-width"
            | "right-margin-width" => Some(Value::Integer(0)),
            "case-replace" => Some(Value::T),
            "case-symbols-as-words" => Some(Value::Nil),
            "use-hard-newlines" => Some(Value::Nil),
            "fill-column" => Some(Value::Integer(70)),
            "indent-according-to-mode" => Some(Value::Symbol("indent-according-to-mode".into())),
            "filter-buffer-substring-function" => {
                Some(Value::Symbol("buffer-substring--filter".into()))
            }
            "meta-prefix-char" => Some(Value::Integer(27)),
            // character.c reserves sixteen IDs initially.  mule.el doubles
            // this vector when it fills; an empty vector can never grow
            // because doubling zero still yields zero.
            "translation-table-vector" => Some(Value::list(
                std::iter::once(Value::symbol("vector-literal"))
                    .chain(std::iter::repeat_n(Value::Nil, 16)),
            )),
            "float-e" => Some(Value::Float(std::f64::consts::E)),
            "float-pi" => Some(Value::Float(std::f64::consts::PI)),
            "gc-elapsed" => Some(Value::Float(0.0)),
            "gcs-done" => Some(Value::Integer(0)),
            "most-positive-fixnum" => Some(Value::Integer(2_305_843_009_213_693_951)),
            "most-negative-fixnum" => Some(Value::Integer(-2_305_843_009_213_693_952)),
            "enable-multibyte-characters" => Some(if self.buffer.is_multibyte() {
                Value::T
            } else {
                Value::Nil
            }),
            "buffer-undo-list" => Some(crate::lisp::primitives::buffer_undo_list_value(
                &self.buffer,
            )),
            "buffer-file-name" => Some(
                self.buffer
                    .file
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "buffer-file-truename" => Some(
                self.buffer
                    .file_truename
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "buffer-file-coding-system" => Some(
                self.buffer_local_value(self.current_buffer_id(), "buffer-file-coding-system")
                    .unwrap_or(Value::Nil),
            ),
            "mark-active" => Some(if self.buffer.mark_active() {
                Value::T
            } else {
                Value::Nil
            }),
            "buffer-invisibility-spec" => Some(
                self.buffer_local_value(self.current_buffer_id(), "buffer-invisibility-spec")
                    .unwrap_or(Value::T),
            ),
            "buffer-display-table" => Some(Value::Nil),
            "last-coding-system-used" => Some(Value::Nil),
            "locale-coding-system" => Some(Value::Nil),
            "coding-system-for-read" => Some(Value::Nil),
            "coding-system-for-write" => Some(Value::Nil),
            "delay-mode-hooks" => Some(Value::Nil),
            "delayed-mode-hooks" => Some(Value::Nil),
            "delayed-after-hook-functions" => Some(Value::Nil),
            "coding-system-list" => Some(Value::list(
                self.coding_system_list(false)
                    .into_iter()
                    .map(Value::Symbol)
                    .collect::<Vec<_>>(),
            )),
            "coding-system-alist" => Some(Value::list(
                self.coding_system_list(false)
                    .into_iter()
                    .map(|name| Value::list([Value::String(name)]))
                    .collect::<Vec<_>>(),
            )),
            "char-code-property-alist" => Some(Value::Nil),
            "set-auto-coding-function" => Some(Value::Nil),
            "file-coding-system-alist" => Some(Value::Nil),
            "file-name-coding-system" => Some(Value::Nil),
            "default-file-name-coding-system" => Some(Value::Nil),
            "completion-styles" => Some(Value::list([
                Value::symbol("basic"),
                Value::symbol("partial-completion"),
                Value::symbol("emacs22"),
            ])),
            "completion-styles-alist" => Some(Value::list([
                Value::list([
                    Value::symbol("basic"),
                    Value::symbol("completion-basic-try-completion"),
                    Value::symbol("completion-basic-all-completions"),
                    Value::String("Basic prefix and suffix completion.".into()),
                ]),
                Value::list([
                    Value::symbol("partial-completion"),
                    Value::symbol("completion-pcm-try-completion"),
                    Value::symbol("completion-pcm-all-completions"),
                    Value::String("Partial completion across word components.".into()),
                ]),
                Value::list([
                    Value::symbol("emacs22"),
                    Value::symbol("completion-emacs22-try-completion"),
                    Value::symbol("completion-emacs22-all-completions"),
                    Value::String("Prefix completion before point.".into()),
                ]),
            ])),
            "version-control" => Some(Value::Nil),
            "dired-kept-versions" => Some(Value::Integer(2)),
            "delete-old-versions" => Some(Value::Nil),
            "kept-old-versions" => Some(Value::Integer(2)),
            "kept-new-versions" => Some(Value::Integer(2)),
            "inhibit-eol-conversion" => Some(Value::Nil),
            "inhibit-null-byte-detection" => Some(Value::Nil),
            "inhibit-iso-escape-detection" => Some(Value::Nil),
            "create-lockfiles" => Some(Value::T),
            "display-hourglass" => Some(Value::Nil),
            "page-delimiter" => Some(Value::String("^\u{000c}".into())),
            "adaptive-fill-mode" => Some(Value::T),
            "adaptive-fill-regexp" => Some(Value::String(
                "[-–!|#%;>*·•‣⁃◦ \t]*".into(),
            )),
            "adaptive-fill-first-line-regexp" => Some(Value::String("\\`[ \t]*\\'".into())),
            "gc-cons-threshold" => Some(Value::Integer(800_000)),
            "auto-save-timeout" => Some(Value::Integer(30)),
            "auto-save-interval" => Some(Value::Integer(300)),
            "load-read-function" => Some(Value::Symbol("read".into())),
            // GNU xdisp.c defvar; simple.el reads it at load time
            // ((when (eq pre-redisplay-function #'ignore) ...)).
            "pre-redisplay-function" => Some(Value::Symbol("ignore".into())),
            // GNU startup.el defvar; bytecomp reads it.
            "startup-redirect-eln-cache" => Some(Value::Nil),
            // GNU tramp defcustom (preloaded via tramp-loaddefs);
            // directory-files-recursively let-binds it.
            "tramp-mode" => Some(Value::T),
            // GNU emacs.c defvar (":" on POSIX).
            "path-separator" => Some(Value::String(":".into())),
            // GNU callproc.c defvar (paths.h).
            "configure-info-directory" => Some(Value::String("/usr/share/info".into())),
            // GNU files.el defconst; shadow.el's shadow-tests read it.
            "dir-locals-file" => Some(Value::String(".dir-locals.el".into())),
            // GNU xdisp.c defvar; tests let-bind it around noisy calls.
            "inhibit-message" => Some(Value::Nil),
            // GNU isearch.el defcustom; package.el's quick-help reads it.
            "search-default-mode" => Some(Value::Nil),
            // GNU keyboard.c keymaps; simple.el define-keys them at load
            // time (event-apply-*-modifier bindings).
            "function-key-map" | "key-translation-map" | "input-decode-map"
            | "local-function-key-map" => Some(Value::list([Value::Symbol("keymap".into())])),
            "values" => Some(Value::Nil),
            "read-circle" => Some(Value::T),
            "gensym-counter" => Some(Value::Integer(0)),
            "load-suffixes" => Some(Value::list(
                dynamic_library_suffix_values()
                    .into_iter()
                    .chain([Value::String(".elc".into()), Value::String(".el".into())]),
            )),
            "module-file-suffix" => Some(module_file_suffix_value()),
            "dynamic-library-suffixes" => Some(Value::list(dynamic_library_suffix_values())),
            // GNU's dumped image has jka-compr's representation suffix
            // installed; the native loader likewise understands gzip.
            "load-file-rep-suffixes" => Some(Value::list([
                Value::String(String::new()),
                Value::String(".gz".into()),
            ])),
            "after-load-alist" => Some(Value::Nil),
            "load-true-file-name" => Some(
                self.current_load_file
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "load-source-file-function" => {
                Some(Value::Symbol("load-with-code-conversion".into()))
            }
            "load-force-doc-strings" | "load-convert-to-unibyte" => Some(Value::Nil),
            "preloaded-file-list" | "byte-boolean-vars" => Some(Value::Nil),
            "load-dangerous-libraries" | "force-load-messages" => Some(Value::Nil),
            "bytecomp-version-regexp" => Some(Value::String(
                "^;;;.\\(?:in Emacs version\\|bytecomp version FSF\\)".into(),
            )),
            "lread--unescaped-character-literals" => Some(Value::Nil),
            "load-prefer-newer" | "load-no-native" => Some(Value::Nil),
            "read-symbol-shorthands" => Some(Value::Nil),
            "debug-on-quit" => Some(Value::Nil),
            "inhibit-redisplay" => Some(Value::Nil),
            "inhibit-quit" => Some(Value::Nil),
            "quit-flag" => Some(Value::Nil),
            "overlay-arrow-position" => Some(Value::Nil),
            "overlay-arrow-string" => Some(Value::Nil),
            "track-mouse" => Some(Value::Nil),
            "last-input-event" => Some(Value::Nil),
            "last-event-frame" => Some(Value::Nil),
            "last-nonmenu-event" => Some(Value::Nil),
            "signal-hook-function" => Some(Value::Nil),
            "minor-mode-overriding-map-alist" => Some(Value::Nil),
            "standard-input" => Some(Value::T),
            "temporary-file-directory" => Some(Value::String(temp_directory_name())),
            "auto-mode-alist" => Some(builtin_auto_mode_alist()),
            "auto-compression-mode" => Some(Value::T),
            "command-switch-alist" => Some(Value::Nil),
            "command-line-args-left" => Some(Value::Nil),
            "purify-flag" => Some(Value::Nil),
            "require-final-newline" => Some(Value::Nil),
            "sentence-end" => Some(Value::Nil),
            "sentence-end-double-space" => Some(Value::T),
            "null-device" => Some(Value::String("/dev/null".into())),
            "exec-suffixes" => Some(Value::list([Value::String(String::new())])),
            "debug-on-error" => Some(Value::Nil),
            "eval-expression-debug-on-error" => Some(Value::T),
            "debugger-stack-frame-as-list" => Some(Value::Nil),
            "print-quoted" => Some(Value::T),
            "eval-buffer-list" => Some(Value::Nil),
            "load-in-progress" => Some(if self.current_load_file.is_some() {
                Value::T
            } else {
                Value::Nil
            }),
            "features" => Some(self.features_value()),
            "selection-converter-alist" => Some(Value::Nil),
            "early-init-file" => Some(Value::Nil),
            "before-init-time" => Some(Value::Nil),
            "after-init-time" => Some(Value::T),
            "init-file-user" => Some(Value::Nil),
            "site-run-file" => Some(Value::Nil),
            "user-init-file" => Some(Value::Nil),
            "custom-file" => Some(Value::Nil),
            "custom-versions-load-alist" => Some(Value::Nil),
            "completion-ignored-extensions" => Some(Value::Nil),
            "regexp-unmatchable" => Some(Value::String("\\`a\\`".into())),
            "ignored-local-variables" => Some(Value::list([
                Value::Symbol("ignored-local-variables".into()),
                Value::Symbol("safe-local-variable-values".into()),
                Value::Symbol("file-local-variables-alist".into()),
                Value::Symbol("dir-local-variables-alist".into()),
            ])),
            "ignored-local-variable-values" => Some(Value::Nil),
            "safe-local-variable-values" => Some(Value::Nil),
            "file-local-variables-alist" | "dir-local-variables-alist" => Some(Value::Nil),
            "text-quoting-style" => Some(Value::Nil),
            "hack-local-variables-hook" => Some(Value::Nil),
            "custom-current-group-alist" => Some(Value::Nil),
            "defun-declarations-alist" => Some(Value::Nil),
            "macro-declarations-alist" => Some(Value::Nil),
            "post-self-insert-hook" => Some(Value::Nil),
            "macroexp--dynvars" => Some(Value::Nil),
            "macroexpand-all-environment" => Some(Value::Nil),
            "image-types" => Some(Value::list([
                Value::Symbol("pbm".into()),
                Value::Symbol("png".into()),
                Value::Symbol("jpeg".into()),
                Value::Symbol("gif".into()),
                Value::Symbol("svg".into()),
                Value::Symbol("xbm".into()),
                Value::Symbol("xpm".into()),
                Value::Symbol("webp".into()),
                Value::Symbol("tiff".into()),
            ])),
            "ls-lisp-use-insert-directory-program" => Some(Value::T),
            "transient-mark-mode" => Some(Value::T),
            "desktop-buffer-mode-handlers" => Some(Value::Nil),
            "find-file-visit-truename" => Some(Value::Nil),
            "insert-directory-wildcard-in-dir-p" => Some(Value::Nil),
            "insert-directory-program" => Some(Value::String("ls".into())),
            "line-move-ignore-invisible" => Some(Value::T),
            "line-move-visual" => Some(Value::T),
            "file-name-invalid-regexp" => Some(Value::String("\0".into())),
            "directory-listing-before-filename-regexp" => Some(Value::String(
                concat!(
                    ".*[0-9][BkKMGTPEZYRQ]? ",
                    "\\(",
                    "[0-9][0-9][0-9][0-9]-[01][0-9]-[0-3][0-9]\\([ T][ 0-2][0-9][:.][0-5][0-9]\\)?",
                    "\\|",
                    "[A-Za-z][A-Za-z][A-Za-z] +[ 0-3][0-9] +\\([ 0-2][0-9][:.][0-5][0-9]\\|[0-9][0-9][0-9][0-9]\\)",
                    "\\)",
                    " +"
                )
                .into(),
            )),
            "minor-mode-alist" => Some(Value::Nil),
            "timer-list" | "timer-idle-list" => Some(Value::Nil),
            "revert-buffer-function" => {
                Some(Value::Symbol("revert-buffer--default".into()))
            }
            "buffer-stale-function" => Some(Value::Symbol(
                "buffer-stale--default-function".into(),
            )),
            "buffer-auto-revert-by-notification" => Some(Value::Nil),
            "non-essential" => Some(Value::Nil),
            "remote-file-name-inhibit-cache" => Some(Value::Nil),
            // files.el: derived modes consult this when setting
            // `require-final-newline' buffer-locally.
            "mode-require-final-newline" => Some(Value::T),
            // syntax.el: propertize progress marker (defvar-local -1).
            "syntax-propertize--done" => Some(Value::Integer(-1)),
            // font-lock.el: defvar-local, nil until a mode installs defaults.
            "font-lock-defaults" => Some(Value::Nil),
            // insdel.c: change hooks run unless a primitive binds this.
            "inhibit-modification-hooks" => Some(Value::Nil),
            // doc.c: name of the DOC file inside `doc-directory'.
            "internal-doc-file-name" => Some(Value::String("DOC".into())),
            // font-lock.el: the standard face variables are self-quoting
            // defvars that keyword FACENAME expressions evaluate.
            "font-lock-comment-face" => Some(Value::Symbol("font-lock-comment-face".into())),
            "font-lock-comment-delimiter-face" => Some(Value::Symbol("font-lock-comment-delimiter-face".into())),
            "font-lock-string-face" => Some(Value::Symbol("font-lock-string-face".into())),
            "font-lock-doc-face" => Some(Value::Symbol("font-lock-doc-face".into())),
            "font-lock-doc-markup-face" => Some(Value::Symbol("font-lock-doc-markup-face".into())),
            "font-lock-keyword-face" => Some(Value::Symbol("font-lock-keyword-face".into())),
            "font-lock-builtin-face" => Some(Value::Symbol("font-lock-builtin-face".into())),
            "font-lock-function-name-face" => Some(Value::Symbol("font-lock-function-name-face".into())),
            "font-lock-variable-name-face" => Some(Value::Symbol("font-lock-variable-name-face".into())),
            "font-lock-type-face" => Some(Value::Symbol("font-lock-type-face".into())),
            "font-lock-constant-face" => Some(Value::Symbol("font-lock-constant-face".into())),
            "font-lock-warning-face" => Some(Value::Symbol("font-lock-warning-face".into())),
            "font-lock-negation-char-face" => Some(Value::Symbol("font-lock-negation-char-face".into())),
            "font-lock-preprocessor-face" => Some(Value::Symbol("font-lock-preprocessor-face".into())),
            "overriding-local-map" => Some(Value::Nil),
            "overriding-terminal-local-map" => Some(Value::Nil),
            "menu-bar-final-items" => Some(Value::Nil),
            "menu-bar-separator" => Some(Value::Symbol("menu-bar-separator".into())),
            "mode-line-modes" => Some(Value::Nil),
            "window-display-table" => Some(Value::Nil),
            "standard-display-table" => Some(Value::Nil),
            "text-mode-syntax-table" => Some(Value::CharTable(2)),
            "emacs-lisp-mode-syntax-table" | "lisp-mode-syntax-table"
            | "lisp-data-mode-syntax-table" => {
                Some(Value::CharTable(self.lisp_data_syntax_table_id()))
            }
            "prog-mode-syntax-table" => Some(Value::CharTable(self.standard_syntax_table_id())),
            "compilation-error-regexp-alist-alist" => Some(Value::Nil),
            "compilation-error-regexp-alist" => Some(Value::Nil),
            "tex-mode" => Some(Value::Symbol("tex-mode".into())),
            "frame-internal-parameters" => Some(Value::Nil),
            "password-word-equivalents" => Some(Value::list([
                Value::String("password".into()),
                Value::String("passcode".into()),
                Value::String("passphrase".into()),
                Value::String("pass phrase".into()),
                Value::String("pin".into()),
                Value::String("decryption key".into()),
                Value::String("encryption key".into()),
                Value::String("암호".into()),
                Value::String("パスワード".into()),
                Value::String("ପ୍ରବେଶ ସଙ୍କେତ".into()),
                Value::String("ពាក្យសម្ងាត់".into()),
                Value::String("adgangskode".into()),
                Value::String("contraseña".into()),
                Value::String("contrasenya".into()),
                Value::String("geslo".into()),
                Value::String("hasło".into()),
                Value::String("heslo".into()),
                Value::String("iphasiwedi".into()),
                Value::String("jelszó".into()),
                Value::String("lösenord".into()),
                Value::String("lozinka".into()),
                Value::String("mật khẩu".into()),
                Value::String("mot de passe".into()),
                Value::String("parola".into()),
                Value::String("pasahitza".into()),
                Value::String("passord".into()),
                Value::String("passwort".into()),
                Value::String("pasvorto".into()),
                Value::String("salasana".into()),
                Value::String("senha".into()),
                Value::String("slaptažodis".into()),
                Value::String("wachtwoord".into()),
                Value::String("كلمة السر".into()),
                Value::String("ססמה".into()),
                Value::String("лозинка".into()),
                Value::String("пароль".into()),
                Value::String("गुप्तशब्द".into()),
                Value::String("शब्दकूट".into()),
                Value::String("પાસવર્ડ".into()),
                Value::String("సంకేతపదము".into()),
                Value::String("ਪਾਸਵਰਡ".into()),
                Value::String("ಗುಪ್ತಪದ".into()),
                Value::String("கடவுச்சொல்".into()),
                Value::String("അടയാളവാക്ക്".into()),
                Value::String("গুপ্তশব্দ".into()),
                Value::String("পাসওয়ার্ড".into()),
                Value::String("රහස්පදය".into()),
                Value::String("密码".into()),
                Value::String("密碼".into()),
            ])),
            "password-colon-equivalents" => Some(Value::list([
                Value::Integer(':' as i64),
                Value::Integer(0xFF1A),
                Value::Integer(0xFE55),
                Value::Integer(0xFE13),
                Value::Integer(0x17D6),
            ])),
            "source-directory" => Some(Value::String(
                std::env::var("EMACS_TEST_DIRECTORY")
                    .ok()
                    .and_then(|path| {
                        std::path::PathBuf::from(path)
                            .parent()
                            .map(|path| path.display().to_string())
                    })
                    .unwrap_or_else(primitives::default_directory),
            )),
            "data-directory" | "doc-directory" => Some(Value::String(
                primitives::compat_data_directory().unwrap_or_else(primitives::default_directory),
            )),
            "user-login-name" => Some(Value::String(
                primitives::current_user_login_name().unwrap_or_else(|| "user".into()),
            )),
            // sysdep.c initializes this dumped variable from the same host
            // identity returned by the `system-name' primitive.
            "system-name" => Some(Value::String(primitives::system_name_value())),
            "user-full-name" => Some(Value::String(
                primitives::current_user_full_name()
                    .or_else(primitives::current_user_login_name)
                    .unwrap_or_else(|| "user".into()),
            )),
            "user-mail-address" => Some(Value::String(format!(
                "{}@{}",
                primitives::current_user_login_name().unwrap_or_else(|| "user".into()),
                primitives::system_name_value()
            ))),
            "default-directory" => Some(Value::String(primitives::default_directory())),
            "current-language-environment" => Some(Value::String("English".into())),
            "window-system" => Some(Value::Nil),
            "initial-window-system" => Some(Value::Nil),
            "left-margin" => Some(Value::Integer(0)),
            "last-command" => Some(Value::Nil),
            "real-last-command" => Some(Value::Nil),
            "this-command" => Some(Value::Nil),
            "this-original-command" => Some(Value::Nil),
            "this-single-command-keys" => Some(Value::list([Value::Symbol("vector-literal".into())])),
            "unread-command-events" => Some(Value::Nil),
            "deactivate-mark" => Some(Value::Nil),
            "line-spacing" => Some(Value::Nil),
            "scroll-margin" => Some(Value::Integer(0)),
            "scroll-preserve-screen-position" => Some(Value::Nil),
            "overwrite-mode" => Some(Value::Nil),
            "load-path" => Some(Value::list(
                self.load_path
                    .iter()
                    .map(|path| Value::String(primitives::path_to_directory_string(path)))
                    .collect::<Vec<_>>(),
            )),
            "image-load-path" => Some(Value::list([
                Value::String(
                    primitives::compat_data_directory()
                        .map(|path| {
                            let mut path = std::path::PathBuf::from(path);
                            path.push("images");
                            primitives::path_to_directory_string(&path)
                        })
                        .unwrap_or_else(primitives::default_directory),
                ),
                Value::Symbol("data-directory".into()),
                Value::Symbol("load-path".into()),
            ])),
            "installation-directory" => Some(
                primitives::compat_installation_directory()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "tab-width" => Some(Value::Integer(8)),
            "indent-tabs-mode" => Some(Value::T),
            "indent-line-function" => Some(Value::Symbol("indent-relative".into())),
            "tab-stop-list" => Some(Value::Nil),
            "use-dialog-box" => Some(Value::T),
            "use-file-dialog" => Some(Value::T),
            "help-char" => Some(Value::Integer(8)),
            "help-event-list" => Some(Value::Nil),
            "help-form" => Some(Value::Nil),
            "prefix-help-command" => Some(Value::Nil),
            "command-error-function" => {
                Some(Value::Symbol("command-error-default-function".into()))
            }
            "read-file-name-completion-ignore-case" => Some(Value::Nil),
            "mounted-file-systems" => Some(Value::String(
                r"^\(?:/\(?:afs/\|m\(?:edia/\|nt\)\|\(?:ne\|tmp_mn\)t/\)\)".into(),
            )),
            "system-type" => Some(Value::Symbol(
                std::env::consts::OS.replace("macos", "darwin"),
            )),
            "system-configuration" => Some(Value::String(primitives::system_configuration())),
            "system-configuration-features" => Some(Value::String(
                std::env::var("EMAXX_SYSTEM_CONFIGURATION_FEATURES").unwrap_or_default(),
            )),
            "system-configuration-options" => Some(Value::String(
                std::env::var("EMAXX_SYSTEM_CONFIGURATION_OPTIONS").unwrap_or_default(),
            )),
            "charset-list" => Some(Value::list(
                self.charset_priority_list()
                    .into_iter()
                    .map(Value::Symbol)
                    .collect::<Vec<_>>(),
            )),
            "ert-resource-directory-format" => Some(Value::String("%s-resources/".into())),
            "ert-resource-directory-trim-left-regexp" => Some(Value::String(String::new())),
            "ert-resource-directory-trim-right-regexp" => {
                Some(Value::String("\\(-tests?\\)?\\.el".into()))
            }
            "load-file-name" => Some(
                self.current_load_file
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            // Macro expansion can happen when an interpreted ERT body is
            // invoked, after its defining file has finished loading.  GNU's
            // macroexp-file-name still identifies that call-site file; the
            // native ERT runner retains it explicitly for this purpose.
            "macroexp-file-name" => Some(
                self.current_load_file
                    .clone()
                    .or_else(|| self.ert_test_source_file.clone())
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "read-buffer-function" | "read-file-name-function" => Some(Value::Nil),
            "delete-by-moving-to-trash" => Some(Value::Nil),
            "directory-files-no-dot-files-regexp" => Some(Value::String("[^.]\\|\\.\\.\\.".into())),
            "user-emacs-directory" => Some(Value::String("/nonexistent/.emacs.d/".into())),
            // Defaults read by the Rust URL transport before url.el has
            // loaded.  The `url' feature remains Lisp-owned, and loading the
            // real package replaces/extends this bootstrap state.
            "url-configuration-directory" => {
                Some(Value::String("/nonexistent/.emacs.d/url/".into()))
            }
            "url-redirect-buffer" | "url-dead-buffer-list" => Some(Value::Nil),
            "url-retrieve-number-of-calls" => Some(Value::Integer(0)),
            "url-asynchronous" => Some(Value::T),
            "invocation-name" => Some(Value::String(
                primitives::current_invocation_name().unwrap_or_else(|| "emaxx".into()),
            )),
            "invocation-directory" => Some(Value::String(
                primitives::current_invocation_directory()
                    .unwrap_or_else(primitives::default_directory),
            )),
            "shell-file-name" => Some(Value::String(
                primitives::find_executable("sh").unwrap_or_else(|| "/bin/sh".into()),
            )),
            "shell-command-switch" => Some(Value::String("-c".into())),
            "emacs-version" => Some(Value::String(primitives::emacs_version_value())),
            "emacs-major-version" => Some(Value::Integer(primitives::emacs_major_version_value())),
            "emacs-minor-version" => Some(Value::Integer(primitives::emacs_minor_version_value())),
            // GNU records the dump time; erc's version stamp reads it.  A
            // fixed nil means "not recorded", which string-replace paths
            // handle (erc--make-message-variable-name checks it).
            "emacs-build-time" => Some(Value::Nil),
            "ctags-program-name" => Some(Value::String("ctags".into())),
            "etags-program-name" => Some(Value::String("etags".into())),
            "hexl-program-name" => Some(Value::String("hexl".into())),
            "emacsclient-program-name" => Some(Value::String(
                primitives::compat_emacsclient_program_name()
                    .unwrap_or_else(|| "emacsclient".into()),
            )),
            "movemail-program-name" => Some(Value::String("movemail".into())),
            "ebrowse-program-name" => Some(Value::String("ebrowse".into())),
            "rcs2log-program-name" => Some(Value::String("rcs2log".into())),
            "process-environment" | "initial-environment" => Some(Value::list(
                std::env::vars()
                    .map(|(name, value)| Value::String(format!("{name}={value}")))
                    .collect::<Vec<_>>(),
            )),
            "find-program" => Some(Value::String("find".into())),
            "grep-program" => Some(Value::String("grep".into())),
            _ if name.starts_with('.') => Some(Value::Nil),
            _ if name.starts_with(':') => Some(Value::Symbol(name.to_string())),
            _ => generated_autoloads::generated_dumped_variable(name).and_then(|source| {
                crate::lisp::reader::Reader::new(source)
                    .read()
                    .ok()
                    .flatten()
            }),
        }
    }

    /// Look up a variable in the given local env, then globals.
    pub(crate) fn lookup(&self, name: &str, env: &Env) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(name)?;
        self.lookup_var_with_resolved_name(name, &resolved, env)
            .ok_or(LispError::Void(resolved))
    }

    pub fn raw_function_binding(&self, name: &str, env: &Env) -> Option<Value> {
        let facts = primitives::name_facts(name);
        if facts.prefer_override {
            return Some(Value::BuiltinFunc(name.to_string()));
        }
        let name_is_builtin = facts.builtin || facts.special_form;
        for frame in env.iter().rev() {
            // Marker frames (oclosure slots, cl-flet/cl-labels functions)
            // always carry their marker as the FIRST entry, so one
            // comparison classifies the frame.
            let frame_marker = frame.first().map(|(key, _)| key.as_str());
            // Oclosure slot frames bind names like `car'/`cdr' as VALUES;
            // GNU never resolves the function position through them.
            if frame_marker == Some(crate::lisp::eval::OCLOSURE_TYPE_MARKER) {
                continue;
            }
            // A builtin's function position can only be shadowed by a real
            // function frame (cl-flet/cl-labels); a plain `let' binding a
            // VARIABLE named `car' to a lambda must not hijack `(car x)'.
            if name_is_builtin && frame_marker != Some(FUNCTION_FRAME_MARKER) {
                continue;
            }
            for (k, v) in frame.iter().rev() {
                if k == name && matches!(v, Value::BuiltinFunc(_) | Value::Lambda(_, _, _)) {
                    return Some(v.clone());
                }
            }
        }
        if let Some(v) = self.functions_index.get(name) {
            return Some(v.clone());
        }
        if let Some(value) = builtin_autoload_function(name) {
            return Some(value);
        }
        if matches!(name, "incf" | "decf") {
            return Some(Value::BuiltinFunc(name.to_string()));
        }
        // Special forms live in function cells in GNU Emacs, so symbol
        // indirection (indirect-function, fboundp, macrop) must resolve them
        // instead of signaling a void-function error.
        if name_is_builtin {
            return Some(Value::BuiltinFunc(name.to_string()));
        }
        None
    }

    /// Return the Lisp-visible function cell even when execution of NAME is
    /// pinned to a native implementation.  GNU metadata consumers such as
    /// gv-get follow symbol aliases through `symbol-function`; hiding an
    /// alias here loses declarations attached to its target.
    pub fn logical_function_binding(&self, name: &str, env: &Env) -> Option<Value> {
        if primitives::name_facts(name).prefer_override
            && let Some(binding) = self.functions_index.get(name)
        {
            return Some(binding.clone());
        }
        self.raw_function_binding(name, env)
    }

    /// Resolve NAME the way GNU macro dispatch sees the function cell:
    /// macros live in function cells, so only genuine function-binding
    /// frames (cl-flet/cl-labels push FUNCTION_FRAME_MARKER as their
    /// FIRST entry) can shadow them — a plain `let'-bound value never
    /// does in GNU.  Skipping the per-entry scan of ordinary frames
    /// keeps the per-form macro probe cheap on deep call stacks.  The
    /// bool is true when the binding came from an env frame (such a
    /// verdict must not be cached as a global fact).
    fn macro_position_binding(&self, name: &str, env: &Env) -> Option<(Value, bool)> {
        let facts = primitives::name_facts(name);
        if facts.prefer_override {
            return Some((Value::BuiltinFunc(name.to_string()), false));
        }
        for frame in env.iter().rev() {
            if frame
                .first()
                .is_none_or(|(key, _)| key != FUNCTION_FRAME_MARKER)
            {
                continue;
            }
            for (key, value) in frame.iter().rev() {
                if key == name && matches!(value, Value::BuiltinFunc(_) | Value::Lambda(_, _, _)) {
                    return Some((value.clone(), true));
                }
            }
        }
        if let Some(value) = self.functions_index.get(name) {
            return Some((value.clone(), false));
        }
        if let Some(value) = builtin_autoload_function(name) {
            return Some((value, false));
        }
        if matches!(name, "incf" | "decf") || facts.builtin || facts.special_form {
            return Some((Value::BuiltinFunc(name.to_string()), false));
        }
        None
    }

    /// `macro_position_binding' with symbol-alias indirection, for the
    /// is-this-an-autoloaded-macro probe in macro expansion.  The bool
    /// is true when any step resolved through an env frame.
    pub(crate) fn macro_position_function(&self, name: &str, env: &Env) -> Option<(Value, bool)> {
        let mut current = name.to_string();
        let mut seen = HashSet::new();
        let mut from_frame = false;
        loop {
            if !seen.insert(current.clone()) {
                return None;
            }
            let (binding, frame_hit) = self.macro_position_binding(&current, env)?;
            from_frame |= frame_hit;
            match binding {
                Value::Symbol(next) => current = next,
                other => return Some((other, from_frame)),
            }
        }
    }

    pub fn lookup_function(&self, name: &str, env: &Env) -> Result<Value, LispError> {
        let mut current = name.to_string();
        let mut seen = HashSet::new();

        loop {
            if !seen.insert(current.clone()) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-function-indirection".into()),
                    Value::Symbol(name.to_string()),
                ])));
            }

            let Some(binding) = self.raw_function_binding(&current, env) else {
                return Err(LispError::VoidFunction(current));
            };
            match binding {
                Value::Symbol(next) => current = next,
                other => return Ok(other),
            }
        }
    }

    pub fn has_macro_binding(&self, name: &str) -> bool {
        self.resolve_macro_binding(name).is_some()
    }

    pub(crate) fn macro_function_value(&self, name: &str) -> Option<Value> {
        let expander = self.resolve_macro_binding(name)?;
        Some(Value::cons(Value::Symbol("macro".into()), expander))
    }

    pub fn known_symbol_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        let mut seen = HashSet::new();
        let mut push_name = |name: &str| {
            if crate::lisp::types::visible_symbol_name(name) != name {
                return;
            }
            if seen.insert(name.to_string()) {
                names.push(name.to_string());
            }
        };
        push_name("nil");
        push_name("t");
        for (name, _) in &self.globals {
            push_name(name);
        }
        for (name, _) in &self.variable_aliases {
            push_name(name);
        }
        for (name, _) in &self.functions {
            push_name(name);
        }
        for binding in &self.macros {
            if !binding.name.starts_with(MACRO_SHADOW_PREFIX) {
                push_name(&binding.name);
            }
        }
        for (name, _) in &self.symbol_properties {
            push_name(name);
        }
        for name in &self.interned_symbols {
            push_name(name);
        }
        names
    }

    /// O(1) membership probe for GNU's standard obarray.  Enumeration keeps
    /// a deterministic vector view for `mapatoms' and completion, but
    /// `intern-soft' is a hash-table operation upstream and must not rebuild
    /// that complete view for every lookup.
    pub(crate) fn standard_obarray_contains_symbol(&self, name: &str) -> bool {
        matches!(name, "nil" | "t")
            || self.interned_symbol_names.contains(name)
            || self.globals_index.contains_key(name)
            || self.variable_aliases_index.contains_key(name)
            || self.functions_index.contains_key(name)
            || (self.macros_name_counts.contains_key(name)
                && !name.starts_with(MACRO_SHADOW_PREFIX))
            || self.symbol_property_index(name).is_some()
    }

    /// Track a macro-table insertion so name-count lookups stay in sync;
    /// every push/extend into `macros` must call this.
    pub(crate) fn note_macro_added(&mut self, name: &str) {
        *self.macros_name_counts.entry(name.to_string()).or_insert(0) += 1;
        self.note_definition_changed();
    }

    /// Track a macro-table removal (drain/rename); the counterpart of
    /// `note_macro_added`.
    pub(crate) fn note_macro_removed(&mut self, name: &str) {
        if let Some(count) = self.macros_name_counts.get_mut(name) {
            if *count <= 1 {
                self.macros_name_counts.remove(name);
            } else {
                *count -= 1;
            }
        }
        self.note_definition_changed();
    }

    /// Invalidate all cached not-a-macro verdicts; called on every
    /// function or macro (re)definition.
    pub(crate) fn note_definition_changed(&mut self) {
        self.definition_generation = self.definition_generation.wrapping_add(1);
    }

    /// Whether the macroexpansion probe already concluded (at the current
    /// definition generation) that NAME is not a macro.
    pub(crate) fn known_not_macro(&self, name: &str) -> bool {
        self.not_macro_names.get(name).copied() == Some(self.definition_generation)
    }

    /// Record a global (frame-independent) not-a-macro verdict for NAME.
    pub(crate) fn note_not_macro(&mut self, name: &str) {
        let generation = self.definition_generation;
        self.not_macro_names.insert(name.to_string(), generation);
    }

    /// A still-current cached expansion for FORM (keyed by its car cell
    /// identity — the entry pins the form so the address can't be reused).
    pub(crate) fn cached_macro_expansion(&self, form: &Value, lexical: bool) -> Option<Value> {
        let Value::Cons(car, _) = form else {
            return None;
        };
        let key = (std::rc::Rc::as_ptr(car) as usize, lexical);
        let (generation, expanded, _) = self.macro_expansion_cache.get(&key)?;
        (*generation == self.definition_generation).then(|| expanded.clone())
    }

    /// Cache FORM's macro expansion at the current definition generation.
    pub(crate) fn cache_macro_expansion(&mut self, form: &Value, lexical: bool, expanded: Value) {
        let Value::Cons(car, _) = form else {
            return;
        };
        let key = (std::rc::Rc::as_ptr(car) as usize, lexical);
        // A runaway cache would pin unbounded transient forms; the cap is
        // far above any real load (function bodies are finite).
        if self.macro_expansion_cache.len() >= (1 << 20) {
            self.macro_expansion_cache.clear();
        }
        self.macro_expansion_cache
            .insert(key, (self.definition_generation, expanded, form.clone()));
    }

    /// Append cl-macrolet-style local macros to the positional table,
    /// returning the (start, count) range for `drain_local_macros`.
    pub(crate) fn push_local_macros(
        &mut self,
        local_macros: &[crate::lisp::eval::MacroBinding],
    ) -> (usize, usize) {
        let local_start = self.macros.len();
        for binding in local_macros {
            self.note_macro_added(&binding.name);
            self.macros.push(binding.clone());
        }
        (local_start, local_macros.len())
    }

    /// Remove the local-macro range installed by `push_local_macros`.
    pub(crate) fn drain_local_macros(&mut self, local_start: usize, local_count: usize) {
        let names: Vec<String> = self.macros[local_start..local_start + local_count]
            .iter()
            .map(|binding| binding.name.clone())
            .collect();
        self.macros.drain(local_start..local_start + local_count);
        for name in names {
            self.note_macro_removed(&name);
        }
    }

    pub(super) fn resolve_macro_binding(&self, name: &str) -> Option<Value> {
        let mut current = name.to_string();
        let mut seen = Vec::new();
        loop {
            if seen.iter().any(|existing| existing == &current) {
                return None;
            }
            seen.push(current.clone());
            if self.macros_name_counts.contains_key(&current)
                && let Some(binding) = self
                    .macros
                    .iter()
                    .rev()
                    .find(|binding| binding.name == current)
            {
                return Some(binding.expander.clone());
            }
            let value = self.functions_index.get(&current)?;
            let Value::Symbol(next) = value else {
                return None;
            };
            current = next.clone();
        }
    }

    /// Set a variable in the innermost local frame, or in globals.
    pub fn set_variable(&mut self, name: &str, value: Value, env: &mut Env) {
        let floor = self.special_scan_floor;
        let mut special: Option<bool> = None;
        for (index, frame) in env.iter_mut().enumerate().rev() {
            // Mirror `lookup': below the caller boundary a SPECIAL variable
            // is set dynamically, never through a caller's lexical frame.
            if index < floor && *special.get_or_insert_with(|| self.is_dynamic_binding_name(name)) {
                break;
            }
            let frame_id = Self::frame_identity(frame);
            if let Some(binding_index) = frame.iter().rposition(|(key, _)| key == name) {
                let stored = Self::stored_value(value);
                frame[binding_index].1 = stored.clone();
                if let Some(frame_id) = frame_id {
                    self.record_lexical_cell_update_if_captured(frame_id, name, &stored);
                }
                return;
            }
        }
        self.set_symbol_value_cell(name, value);
    }

    pub fn set_symbol_value_cell(&mut self, name: &str, value: Value) {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        let value = Self::stored_value(value);
        if resolved == "buffer-file-name" {
            self.buffer.file = match value {
                Value::Nil => None,
                Value::String(path) => Some(path),
                Value::StringObject(state) => Some(state.borrow().text.clone()),
                other => Some(other.to_string()),
            };
            return;
        }
        if resolved == "buffer-file-truename" {
            self.buffer.file_truename = match value {
                Value::Nil => None,
                Value::String(path) => Some(path),
                Value::StringObject(state) => Some(state.borrow().text.clone()),
                other => Some(other.to_string()),
            };
            return;
        }
        if resolved == "buffer-undo-list" {
            if value.is_nil() {
                self.undo_sequence = None;
                self.buffer.clear_undo_history();
            } else if let Some((head, tail)) = value.cons_values()
                && tail == crate::lisp::primitives::buffer_undo_list_value(&self.buffer)
            {
                let entry = buffer_undo_head_to_entry(&head);
                self.buffer.push_undo_entry(entry);
            } else {
                // A structural replacement (undo truncation after
                // cancel-change-group, boundary removal by
                // undo-amalgamate-change-group, ...): rebuild the native undo
                // state from the assigned list so the Lisp view round-trips.
                self.undo_sequence = None;
                if let Ok(items) = value.to_vec() {
                    self.buffer.clear_undo_history();
                    // The Lisp view is newest-first; the native list is
                    // oldest-first.
                    for item in items.into_iter().rev() {
                        // The trailing (t . TIME) modtime marker is
                        // synthesized on read for file-visiting buffers;
                        // storing it back would duplicate it.
                        if let Value::Cons(car, _) = &item
                            && matches!(&*car.borrow(), Value::T)
                        {
                            continue;
                        }
                        self.buffer
                            .push_undo_entry(buffer_undo_head_to_entry(&item));
                    }
                }
            }
            return;
        }
        if let Some(scope) = self.assignment_scope(&resolved) {
            match scope {
                SpecialBindingScope::Global => {
                    self.set_global_binding(&resolved, value);
                }
                SpecialBindingScope::BufferLocal(buffer_id) => {
                    self.set_buffer_local_value(buffer_id, &resolved, value);
                }
            }
            return;
        }
        // Set in globals
        self.globals_index.insert(resolved.clone(), value.clone());
        for (k, v) in self.globals.iter_mut().rev() {
            if k == &resolved {
                *v = value;
                return;
            }
        }
        self.globals.push((resolved, value));
    }

    // Two advice functions match when `equal' (GNU advice--member-p) or,
    // for separately evaluated lambdas, when their code is identical
    // (captured environments are not compared).
    pub(crate) fn advice_functions_match(&mut self, a: &Value, b: &Value) -> bool {
        if let (Value::Lambda(params_a, body_a, _), Value::Lambda(params_b, body_b, _)) = (a, b) {
            return params_a == params_b && body_a == body_b;
        }
        crate::lisp::primitives::values_equal(self, a, b)
    }

    // Compose BASE with the symbol's advice entries (newest entry ends up
    // outermost, like add-function at depth 0).
    pub(crate) fn compose_advice_chain(&self, name: &str, base: Value) -> Value {
        let Some(state) = self.advice_registry.get(name) else {
            return base;
        };
        let mut composed = base;
        for entry in state.entries.iter().rev() {
            if let Some(wrapped) = crate::lisp::primitives::wrap_advice(
                &entry.where_kind,
                composed.clone(),
                entry.function.clone(),
            ) {
                composed = wrapped;
            }
        }
        composed
    }

    // Rebuild the advised function binding from the registry.  Called after
    // advice-add/-remove and after defun/defalias redefine an advised name.
    pub(crate) fn advice_reapply(&mut self, name: &str) {
        // Refresh the base from the live binding first: cl-defmethod
        // re-registration mutates the definition captured under our
        // wrapper, so the stored snapshot can go stale.
        if let Ok(current) = self.lookup_function(name, &Env::new()) {
            let stripped = crate::lisp::primitives::strip_advice_wrappers(&current);
            if !matches!(stripped, Value::Lambda(_, _, _)) || stripped != current {
                if let Some(state) = self.advice_registry.get_mut(name)
                    && state.base.is_some()
                {
                    state.base = Some(stripped);
                }
            } else if let Some(state) = self.advice_registry.get_mut(name)
                && state.base.is_some()
                && state.entries.is_empty()
            {
                // Nothing wrapped (all advice removed elsewhere): the live
                // binding IS the base.
                state.base = Some(current);
            }
        }
        let Some(state) = self.advice_registry.get(name) else {
            return;
        };
        if state.entries.is_empty() {
            let base = state.base.clone();
            self.advice_registry.remove(name);
            if let Some(base) = base {
                self.set_function_binding(name, Some(base));
            }
            self.put_symbol_property(name, "defalias-fset-function", Value::Nil);
            return;
        }
        let Some(base) = state.base.clone() else {
            // Pending: no definition yet; defun/defalias will call back.
            return;
        };
        let composed = self.compose_advice_chain(name, base);
        self.set_function_binding(name, Some(composed));
        // GNU marks advised symbols so defalias routes through the advice
        // machinery; the tests observe the property's presence.
        self.put_symbol_property(
            name,
            "defalias-fset-function",
            Value::Symbol("advice--defalias-fset".into()),
        );
    }

    // A (re)definition of NAME becomes the new advice base; the composed
    // wrapper is reinstalled on top of it (GNU advice--defalias-fset).
    pub(crate) fn advice_note_new_definition(&mut self, name: &str) {
        let has_entries = self
            .advice_registry
            .get(name)
            .is_some_and(|state| !state.entries.is_empty());
        if !has_entries {
            return;
        }
        if let Ok(definition) = self.lookup_function(name, &Env::new())
            && let Some(state) = self.advice_registry.get_mut(name)
        {
            state.base = Some(definition);
        }
        self.advice_reapply(name);
    }

    // GNU stores a macro in the function cell as (macro . EXPANDER); emaxx
    // keeps a native macro table, so synthesize the GNU shape on demand
    // (nadvice reads and rewrites it when advising macros).
    pub(crate) fn macro_binding_as_function(&self, name: &str) -> Option<Value> {
        if !self.macros_name_counts.contains_key(name) {
            return None;
        }
        self.macros
            .iter()
            .rev()
            .find(|binding| binding.name == name)
            .map(|binding| Value::cons(Value::Symbol("macro".into()), binding.expander.clone()))
    }

    // Follow the function cell (through symbol aliases) to a
    // (macro . EXPANDER) cons; nadvice installs advised macros that way.
    pub(crate) fn function_cell_macro_expander(&self, name: &str, env: &Env) -> Option<Value> {
        let mut current = name.to_string();
        for _ in 0..10 {
            let (binding, _) = self.macro_position_binding(&current, env)?;
            match binding {
                Value::Symbol(next) => current = next,
                Value::Cons(car, cdr) => {
                    return match &*car.borrow() {
                        Value::Symbol(head) if head == "macro" => Some(cdr.borrow().clone()),
                        _ => None,
                    };
                }
                _ => return None,
            }
        }
        None
    }

    // GNU defalias consults the symbol's `defalias-fset-function' (nadvice
    // sets advice--defalias-fset there) instead of writing the cell
    // directly.  Returns true when the property handled the definition.
    pub(crate) fn defalias_fset_function_handles(
        &mut self,
        name: &str,
        definition: &Value,
        env: &mut Env,
    ) -> bool {
        let Some(fsetfun) = self.get_symbol_property(name, "defalias-fset-function") else {
            return false;
        };
        if !fsetfun.is_truthy() {
            return false;
        }
        let handled = self.call_function_value(
            fsetfun,
            None,
            &[Value::Symbol(name.to_string()), definition.clone()],
            env,
        );
        handled.is_ok()
    }

    // GNU keeps macro-ness in the function cell: fsetting a plain function
    // over a macro name (or voiding the cell) erases the macro definition.
    // The macro table is positional (cl-macrolet drains index ranges), so
    // entries are renamed out of resolution instead of removed.
    pub(crate) fn shadow_macro_binding(&mut self, name: &str) {
        let mut renamed = 0u32;
        for entry in self.macros.iter_mut() {
            if entry.name == name {
                entry.name = format!("{MACRO_SHADOW_PREFIX}{name}");
                renamed += 1;
            }
        }
        for _ in 0..renamed {
            self.note_macro_removed(name);
            self.note_macro_added(&format!("{MACRO_SHADOW_PREFIX}{name}"));
        }
    }

    pub fn push_function_binding(&mut self, name: &str, function: Value) {
        self.functions_index
            .insert(name.to_string(), function.clone());
        self.functions.push((name.to_string(), function));
        self.note_definition_changed();
    }

    /// Rebuild the last-wins index entry for NAME after an ad-hoc removal
    /// or in-place mutation of `functions`.
    pub(crate) fn reindex_function_binding(&mut self, name: &str) {
        match self.functions.iter().rev().find(|(fname, _)| fname == name) {
            Some((_, value)) => {
                let value = value.clone();
                self.functions_index.insert(name.to_string(), value);
            }
            None => {
                self.functions_index.remove(name);
            }
        }
        self.note_definition_changed();
    }

    pub fn function_binding_name(&self, function: &Value) -> Option<String> {
        match function {
            Value::Symbol(name) | Value::BuiltinFunc(name) => Some(name.clone()),
            other => self
                .functions
                .iter()
                .rev()
                .find(|(_, value)| value == other)
                .map(|(name, _)| name.clone()),
        }
    }

    pub fn pop_function_binding(&mut self, name: &str) {
        if let Some(index) = self.functions.iter().rposition(|(fname, _)| fname == name) {
            self.functions.remove(index);
            self.reindex_function_binding(name);
        }
    }

    pub fn replace_next_function_binding(&mut self, name: &str, function: Value) {
        let mut skip_top = true;
        for (fname, value) in self.functions.iter_mut().rev() {
            if fname != name {
                continue;
            }
            if skip_top {
                skip_top = false;
                continue;
            }
            *value = function;
            return;
        }
        self.push_function_binding(name, function);
    }

    pub fn remove_all_function_bindings(&mut self, name: &str) {
        self.functions.retain(|(fname, _)| fname != name);
        self.functions_index.remove(name);
        self.note_definition_changed();
    }

    pub fn set_function_binding(&mut self, name: &str, function: Option<Value>) {
        if let Some(index) = self.functions.iter().rposition(|(fname, _)| fname == name) {
            self.functions.remove(index);
        }
        match function {
            Some(function) => self.push_function_binding(name, function),
            None => self.reindex_function_binding(name),
        }
    }

    pub fn validate_function_binding(&self, name: &str, function: &Value) -> Result<(), LispError> {
        let Value::Symbol(current) = function else {
            return Ok(());
        };
        let mut current = current.clone();
        let mut seen = vec![name.to_string()];
        loop {
            if seen.iter().any(|existing| existing == &current) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-function-indirection".into()),
                    Value::Symbol(name.to_string()),
                ])));
            }
            seen.push(current.clone());
            let Some((_, value)) = self
                .functions
                .iter()
                .rev()
                .find(|(function_name, _)| function_name == &current)
            else {
                return Ok(());
            };
            let Value::Symbol(next) = value else {
                return Ok(());
            };
            current = next.clone();
        }
    }
}

fn temp_directory_name() -> String {
    let mut directory = std::env::temp_dir().display().to_string();
    if !directory.ends_with('/') {
        directory.push('/');
    }
    directory
}
