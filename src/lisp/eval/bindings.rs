use super::*;

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
            "fill-column" => Some(Value::Integer(70)),
            "indent-according-to-mode" => Some(Value::Symbol("indent-according-to-mode".into())),
            "meta-prefix-char" => Some(Value::Integer(27)),
            "translation-table-vector" => Some(Value::list([Value::symbol("vector")])),
            "float-e" => Some(Value::Float(std::f64::consts::E)),
            "float-pi" => Some(Value::Float(std::f64::consts::PI)),
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
            "set-auto-coding-function" => Some(Value::Nil),
            "file-coding-system-alist" => Some(Value::Nil),
            "file-name-coding-system" => Some(Value::Nil),
            "default-file-name-coding-system" => Some(Value::Nil),
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
            "temporary-file-directory" => {
                Some(Value::String(std::env::temp_dir().display().to_string()))
            }
            "auto-mode-alist" => Some(builtin_auto_mode_alist()),
            "auto-compression-mode" => Some(Value::T),
            "command-switch-alist" => Some(Value::Nil),
            "command-line-args-left" => Some(Value::Nil),
            "purify-flag" => Some(Value::Nil),
            "require-final-newline" => Some(Value::T),
            "sentence-end" => Some(Value::Nil),
            "null-device" => Some(Value::String("/dev/null".into())),
            "exec-suffixes" => Some(Value::list([Value::String(String::new())])),
            "debug-on-error" => Some(Value::Nil),
            "load-in-progress" => Some(if self.current_load_file.is_some() {
                Value::T
            } else {
                Value::Nil
            }),
            "selection-converter-alist" => Some(Value::Nil),
            "early-init-file" => Some(Value::Nil),
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
            "overriding-local-map" => Some(Value::Nil),
            "overriding-terminal-local-map" => Some(Value::Nil),
            "menu-bar-final-items" => Some(Value::Nil),
            "menu-bar-separator" => Some(Value::Symbol("menu-bar-separator".into())),
            "mode-line-modes" => Some(Value::Nil),
            "window-display-table" => Some(Value::Nil),
            "standard-display-table" => Some(Value::Nil),
            "text-mode-syntax-table" | "emacs-lisp-mode-syntax-table" => {
                Some(Value::CharTable(self.standard_syntax_table_id()))
            }
            "compilation-error-regexp-alist-alist" => Some(Value::Nil),
            "compilation-error-regexp-alist" => Some(Value::Nil),
            "text-mode-map" => Some(primitives::keymap_placeholder(Some("text-mode-map"))),
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
            "unread-command-events" => Some(Value::Nil),
            "deactivate-mark" => Some(Value::Nil),
            "line-spacing" => Some(Value::Nil),
            "scroll-margin" => Some(Value::Integer(0)),
            "scroll-preserve-screen-position" => Some(Value::Nil),
            "scroll-up-aggressively" => Some(Value::Nil),
            "vertical-scroll-bar" => Some(Value::Symbol("right".into())),
            "overwrite-mode" => Some(Value::Symbol("overwrite-mode-binary".into())),
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
        for frame in env.iter().rev() {
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
            push_name(name);
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
