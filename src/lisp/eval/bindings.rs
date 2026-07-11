use super::*;

// Prefix a macro-table entry is renamed to when a function definition
// shadows it (see `shadow_macro_binding').
pub(crate) const MACRO_SHADOW_PREFIX: &str = "--emaxx-shadowed-macro--";

// Marks an env frame whose bindings are FUNCTION bindings (cl-flet /
// cl-labels).  Only such frames may shadow a builtin in the function
// position: a plain `let' of a variable named `car' to a lambda must not
// hijack `(car x)' (GNU separates value and function cells).
pub(crate) const FUNCTION_FRAME_MARKER: &str = "--emaxx-function-frame--";

impl Interpreter {
    pub fn lookup_var(&self, name: &str, env: &Env) -> Option<Value> {
        for frame in env.iter().rev() {
            for (k, v) in frame.iter().rev() {
                if k == name {
                    return Some(v.clone());
                }
            }
        }
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        let active_global_special = self.active_special_restores.iter().rev().any(|restore| {
            restore.name == resolved && matches!(restore.scope, SpecialBindingScope::Global)
        });
        // DEFVAR_PER_BUFFER semantics: the current buffer's own local wins
        // over a global `let' made in another buffer.
        if self.is_auto_buffer_local(&resolved)
            && let Some(value) = self.buffer_local_value(self.current_buffer_id(), &resolved)
        {
            return Some(value);
        }
        if active_global_special && let Some(value) = self.global_value(&resolved) {
            return Some(value);
        }
        if let Some(value) = self.buffer_local_value(self.current_buffer_id(), &resolved) {
            return Some(value);
        }
        if let Some(value) = self.global_value(&resolved) {
            return Some(value);
        }
        self.builtin_var_value(&resolved)
    }

    pub fn symbol_value_cell(&self, name: &str) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(name)?;
        let active_global_special = self.active_special_restores.iter().rev().any(|restore| {
            restore.name == resolved && matches!(restore.scope, SpecialBindingScope::Global)
        });
        // DEFVAR_PER_BUFFER semantics, as in lookup_var above.
        if self.is_auto_buffer_local(&resolved)
            && let Some(value) = self.buffer_local_value(self.current_buffer_id(), &resolved)
        {
            return Ok(value);
        }
        if active_global_special && let Some(value) = self.global_value(&resolved) {
            return Ok(value);
        }
        if let Some(value) = self.buffer_local_value(self.current_buffer_id(), &resolved) {
            return Ok(value);
        }
        if matches!(
            resolved.as_str(),
            "buffer-file-name" | "buffer-file-truename"
        ) && let Some(value) = self.builtin_var_value(&resolved)
        {
            return Ok(value);
        }
        if let Some(value) = self.global_value(&resolved) {
            return Ok(value);
        }
        if let Some(value) = self.builtin_var_value(&resolved) {
            return Ok(value);
        }
        if resolved == "buffer-undo-list" {
            return Ok(crate::lisp::primitives::buffer_undo_list_value(
                &self.buffer,
            ));
        }
        Err(LispError::Void(resolved))
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
            "case-replace" => Some(Value::T),
            "case-symbols-as-words" => Some(Value::Nil),
            "use-hard-newlines" => Some(Value::Nil),
            "fill-column" => Some(Value::Integer(70)),
            "indent-according-to-mode" => Some(Value::Symbol("indent-according-to-mode".into())),
            "filter-buffer-substring-function" => {
                Some(Value::Symbol("buffer-substring--filter".into()))
            }
            "meta-prefix-char" => Some(Value::Integer(27)),
            "translation-table-vector" => Some(Value::list([Value::symbol("vector-literal")])),
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
            // GNU xdisp.c defvar; tests let-bind it around noisy calls.
            "inhibit-message" => Some(Value::Nil),
            // GNU isearch.el defcustom; package.el's quick-help reads it.
            "search-default-mode" => Some(Value::Nil),
            // GNU keyboard.c keymaps; simple.el define-keys them at load
            // time (event-apply-*-modifier bindings).
            "function-key-map" | "key-translation-map" | "input-decode-map"
            | "local-function-key-map" => Some(Value::list([Value::Symbol("keymap".into())])),
            "read-circle" => Some(Value::T),
            "gensym-counter" => Some(Value::Integer(0)),
            "load-file-rep-suffixes" => Some(Value::list([Value::String(String::new())])),
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
            "ert-remote-temporary-file-directory" => {
                let temporary = temp_directory_name();
                Some(if self.has_feature("tramp") {
                    Value::String(format!("/mock::{temporary}"))
                } else {
                    Value::Nil
                })
            }
            "auto-mode-alist" => Some(builtin_auto_mode_alist()),
            "auto-compression-mode" => Some(Value::T),
            "command-switch-alist" => Some(Value::Nil),
            "command-line-args-left" => Some(Value::Nil),
            "purify-flag" => Some(Value::Nil),
            "require-final-newline" => Some(Value::T),
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
            "obarray" => Some(Value::Nil),
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
                Some(Value::Symbol("emaxx-default-revert-buffer-function".into()))
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
            "text-mode-map" => Some(primitives::keymap_placeholder(Some("text-mode-map"))),
            "lisp-mode-shared-map" => {
                Some(primitives::keymap_placeholder(Some("lisp-mode-shared-map")))
            }
            "lisp-mode-map" => Some(primitives::keymap_placeholder(Some("lisp-mode-map"))),
            "emacs-lisp-mode-map" => {
                Some(primitives::keymap_placeholder(Some("emacs-lisp-mode-map")))
            }
            "tex-mode" => Some(Value::Symbol("tex-mode".into())),
            "tex-mode-map" => Some(primitives::keymap_placeholder(Some("tex-mode-map"))),
            "texinfo-mode-map" => Some(primitives::keymap_placeholder(Some("texinfo-mode-map"))),
            "special-mode-map" => Some(primitives::keymap_placeholder(Some("special-mode-map"))),
            "global-map" => Some(primitives::keymap_placeholder(Some("global-map"))),
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
            "scroll-up-aggressively" => Some(Value::Nil),
            "vertical-scroll-bar" => Some(Value::Symbol("right".into())),
            "overwrite-mode" => Some(Value::Nil),
            "cursor-in-non-selected-windows" => Some(Value::Nil),
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
            "mounted-file-systems" => Some(Value::String(String::new())),
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
            "load-file-name" | "macroexp-file-name" => Some(
                self.current_load_file
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "read-buffer-function" | "read-file-name-function" => Some(Value::Nil),
            "delete-by-moving-to-trash" => Some(Value::Nil),
            "directory-files-no-dot-files-regexp" => Some(Value::String("[^.]\\|\\.\\.\\.".into())),
            "user-emacs-directory" => Some(Value::String("/nonexistent/.emacs.d/".into())),
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
            "etags-program-name" => Some(Value::String(
                primitives::find_executable("etags").unwrap_or_else(|| "etags".into()),
            )),
            "emacsclient-program-name" => Some(Value::String(
                primitives::compat_emacsclient_program_name()
                    .unwrap_or_else(|| "emacsclient".into()),
            )),
            "process-environment" | "initial-environment" => Some(Value::list(
                std::env::vars()
                    .map(|(name, value)| Value::String(format!("{name}={value}")))
                    .collect::<Vec<_>>(),
            )),
            "find-program" => Some(Value::String("find".into())),
            "grep-program" => Some(Value::String("grep".into())),
            _ if name.starts_with('.') => Some(Value::Nil),
            _ if name.starts_with(':') => Some(Value::Symbol(name.to_string())),
            _ => None,
        }
    }

    /// Look up a variable in the given local env, then globals.
    pub(crate) fn lookup(&self, name: &str, env: &Env) -> Result<Value, LispError> {
        // Search local frames from innermost to outermost
        for frame in env.iter().rev() {
            for (k, v) in frame.iter().rev() {
                if k == name {
                    return Ok(v.clone());
                }
            }
        }
        let resolved = self.resolve_variable_name(name)?;
        // Search globals
        if let Some(value) = self.buffer_local_value(self.current_buffer_id(), &resolved) {
            return Ok(value);
        }
        if matches!(
            resolved.as_str(),
            "buffer-file-name" | "buffer-file-truename"
        ) && let Some(value) = self.builtin_var_value(&resolved)
        {
            return Ok(value);
        }
        if let Some(value) = self.global_value(&resolved) {
            return Ok(value);
        }
        if resolved == "buffer-undo-list" {
            return Ok(crate::lisp::primitives::buffer_undo_list_value(
                &self.buffer,
            ));
        }
        self.builtin_var_value(&resolved)
            .ok_or(LispError::Void(resolved))
    }

    pub fn raw_function_binding(&self, name: &str, env: &Env) -> Option<Value> {
        if primitives::prefer_builtin_override(name) {
            return Some(Value::BuiltinFunc(name.to_string()));
        }
        let name_is_builtin =
            primitives::is_builtin(name) || primitives::is_special_form_name(name);
        for frame in env.iter().rev() {
            // Oclosure slot frames bind names like `car'/`cdr' as VALUES;
            // GNU never resolves the function position through them.
            if frame
                .iter()
                .any(|(k, _)| k == crate::lisp::eval::OCLOSURE_TYPE_MARKER)
            {
                continue;
            }
            // A builtin's function position can only be shadowed by a real
            // function frame (cl-flet/cl-labels); a plain `let' binding a
            // VARIABLE named `car' to a lambda must not hijack `(car x)'.
            if name_is_builtin && !frame.iter().any(|(k, _)| k == FUNCTION_FRAME_MARKER) {
                continue;
            }
            for (k, v) in frame.iter().rev() {
                if k == name && matches!(v, Value::BuiltinFunc(_) | Value::Lambda(_, _, _)) {
                    return Some(v.clone());
                }
            }
        }
        for (k, v) in self.functions.iter().rev() {
            if k == name {
                return Some(v.clone());
            }
        }
        if let Some(value) = builtin_autoload_function(name) {
            return Some(value);
        }
        if matches!(name, "incf" | "decf") {
            return Some(Value::BuiltinFunc(name.to_string()));
        }
        if primitives::is_builtin(name) {
            return Some(Value::BuiltinFunc(name.to_string()));
        }
        // Special forms live in function cells in GNU Emacs, so symbol
        // indirection (indirect-function, fboundp, macrop) must resolve them
        // instead of signaling a void-function error.
        if primitives::is_special_form_name(name) {
            return Some(Value::BuiltinFunc(name.to_string()));
        }
        None
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
                return Err(LispError::Void(current));
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
        let (params, body) = self.resolve_macro_binding(name)?;
        Some(Value::cons(
            Value::Symbol("macro".into()),
            Value::Lambda(params, body, shared_env(Vec::new())),
        ))
    }

    pub fn known_symbol_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        let mut push_name = |name: &str| {
            if !names.iter().any(|existing| existing == name) {
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
        for (name, _, _) in &self.macros {
            if !name.starts_with(MACRO_SHADOW_PREFIX) {
                push_name(name);
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

    pub(super) fn resolve_macro_binding(&self, name: &str) -> Option<(Vec<String>, Vec<Value>)> {
        let mut current = name.to_string();
        let mut seen = Vec::new();
        loop {
            if seen.iter().any(|existing| existing == &current) {
                return None;
            }
            seen.push(current.clone());
            if let Some((_, params, body)) = self
                .macros
                .iter()
                .rev()
                .find(|(macro_name, _, _)| macro_name == &current)
            {
                return Some((params.clone(), body.clone()));
            }
            let (_, value) = self
                .functions
                .iter()
                .rev()
                .find(|(function_name, _)| function_name == &current)?;
            let Value::Symbol(next) = value else {
                return None;
            };
            current = next.clone();
        }
    }

    /// Set a variable in the innermost local frame, or in globals.
    pub fn set_variable(&mut self, name: &str, value: Value, env: &mut Env) {
        for frame in env.iter_mut().rev() {
            for (k, v) in frame.iter_mut().rev() {
                if k == name {
                    *v = Self::stored_value(value);
                    return;
                }
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
                self.undo_sequence = None;
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
        self.macros
            .iter()
            .rev()
            .find(|(mname, _, _)| mname == name)
            .map(|(_, params, body)| {
                Value::cons(
                    Value::Symbol("macro".into()),
                    Value::Lambda(params.clone(), body.clone(), shared_env(Vec::new())),
                )
            })
    }

    // Follow the function cell (through symbol aliases) to a
    // (macro . EXPANDER) cons; nadvice installs advised macros that way.
    pub(crate) fn function_cell_macro_expander(&self, name: &str, env: &Env) -> Option<Value> {
        let mut current = name.to_string();
        for _ in 0..10 {
            let binding = self.raw_function_binding(&current, env)?;
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
        for entry in self.macros.iter_mut() {
            if entry.0 == name {
                entry.0 = format!("{MACRO_SHADOW_PREFIX}{name}");
            }
        }
    }

    pub fn push_function_binding(&mut self, name: &str, function: Value) {
        self.functions.push((name.to_string(), function));
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
        self.functions.push((name.to_string(), function));
    }

    pub fn remove_all_function_bindings(&mut self, name: &str) {
        self.functions.retain(|(fname, _)| fname != name);
    }

    pub fn set_function_binding(&mut self, name: &str, function: Option<Value>) {
        if let Some(index) = self.functions.iter().rposition(|(fname, _)| fname == name) {
            self.functions.remove(index);
        }
        if let Some(function) = function {
            self.functions.push((name.to_string(), function));
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
