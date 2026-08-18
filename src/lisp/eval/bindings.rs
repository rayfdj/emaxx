use super::*;

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

fn malformed_lisp_environment_error(environment: &Value, circular: bool) -> LispError {
    let condition = if circular {
        "circular-list"
    } else {
        "wrong-type-argument"
    };
    let mut data = vec![Value::Symbol(condition.into())];
    if !circular {
        data.push(Value::Symbol("listp".into()));
    }
    data.push(environment.clone());
    LispError::SignalValue(Value::list(data))
}

/// Return the first ENV alist entry accepted by MATCHES.
///
/// GNU's evaluator uses `assq' directly on the live interpreted environment.
/// Consequently, finding an entry before a malformed tail succeeds, while
/// exhausting an improper or circular environment signals with the complete
/// original ENV object.  Keep that decision table in one walker so reads,
/// writes, and locally-special declarations cannot diverge.
fn find_lisp_environment_entry(
    environment: &Value,
    mut matches: impl FnMut(&Value) -> bool,
) -> Result<Option<Value>, LispError> {
    let mut cursor = environment.clone();
    let mut seen = HashSet::new();
    loop {
        match cursor {
            Value::Nil => return Ok(None),
            Value::Cons(list_cell) => {
                if !seen.insert(ConsCell::identity(&list_cell)) {
                    return Err(malformed_lisp_environment_error(environment, true));
                }
                let entry = list_cell.car.borrow().clone();
                cursor = list_cell.cdr.borrow().clone();
                if matches(&entry) {
                    return Ok(Some(entry));
                }
            }
            _ => return Err(malformed_lisp_environment_error(environment, false)),
        }
    }
}

pub(super) fn lisp_environment_binding_checked(
    environment: &Value,
    name: &str,
) -> Result<Option<Value>, LispError> {
    find_lisp_environment_entry(environment, |entry| {
        let Value::Cons(binding) = entry else {
            return false;
        };
        binding
            .car
            .borrow()
            .as_symbol()
            .is_ok_and(|symbol| symbol == name)
    })
    .map(|entry| entry.and_then(|entry| entry.cdr().ok()))
}

fn set_lisp_environment_binding_checked(
    environment: &Value,
    name: &str,
    value: Value,
) -> Result<bool, LispError> {
    let entry = find_lisp_environment_entry(environment, |entry| {
        let Value::Cons(binding) = entry else {
            return false;
        };
        binding
            .car
            .borrow()
            .as_symbol()
            .is_ok_and(|symbol| symbol == name)
    })?;
    let Some(binding) = entry else {
        return Ok(false);
    };
    binding.set_cdr(value)?;
    Ok(true)
}

pub(super) fn set_lisp_environment_binding(environment: &Value, name: &str, value: Value) -> bool {
    set_lisp_environment_binding_checked(environment, name, value).unwrap_or(false)
}

pub(super) fn lisp_environment_declares_special(environment: &Value, name: &str) -> bool {
    find_lisp_environment_entry(environment, |entry| {
        entry.as_symbol().is_ok_and(|symbol| symbol == name)
    })
    .ok()
    .flatten()
    .is_some()
}

fn directory_listing_before_filename_regexp() -> &'static str {
    static REGEXP: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    REGEXP
        .get_or_init(|| {
            let letter = "\\([A-Za-z]\\|[^\0-\u{7f}]\\)";
            let letter_or_quote = "\\([A-Za-z']\\|[^\0-\u{7f}]\\)";
            let month = format!("{letter_or_quote}{letter_or_quote}+\\.?");
            let year = "[0-9][0-9][0-9][0-9]";
            let day = "[ 0-3][0-9]";
            let hour_minute = "[ 0-2][0-9][:.][0-5][0-9]";
            let seconds = "[0-6][0-9]\\([.,][0-9]+\\)?";
            let zone = "[-+][0-2][0-9][0-5][0-9]";
            let iso_month_day = "[01][0-9]-[0-3][0-9]";
            let iso_time = format!("{hour_minute}\\(:{seconds}\\( ?{zone}\\)?\\)?");
            let iso = format!(
                "\\(\\({year}-\\)?{iso_month_day}[ T]{iso_time}\\|{year}-{iso_month_day}\\)"
            );
            let western = format!(
                "\\({month} +{day}\\|{day}\\.? {month}\\) +\\({hour_minute}\\|{year}\\)"
            );
            let western_comma = format!("{month} +{day}, +{year}");
            let day_month_year = format!("{day}-{month}-{year} {hour_minute}");
            let east_asian = format!(
                "\\([ 0-1]?[0-9]{letter}? {day}{letter}? +\\|{day} [ 0-1]?[0-9] +\\)\\({hour_minute}\\|{year}{letter}?\\)"
            );

            format!(
                concat!(
                    "\\([0-9][BkKMGTPEZYRQ]? {iso}",
                    "\\|.*[0-9][BkKMGTPEZYRQ]? ",
                    "\\({western}\\|{western_comma}\\|{day_month_year}\\|{east_asian}\\)\\) +"
                ),
                iso = iso,
                western = western,
                western_comma = western_comma,
                day_month_year = day_month_year,
                east_asian = east_asian,
            )
        })
        .as_str()
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
            .ok()
            .flatten()
    }

    fn lookup_var_with_resolved_name(
        &self,
        name: &str,
        resolved: &str,
        env: &Env,
    ) -> Result<Option<Value>, LispError> {
        if resolved == "buffer-undo-list" {
            return Ok(Some(crate::lisp::primitives::buffer_undo_list_value(
                &self.buffer,
            )));
        }
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
            if let Some(environment) = frame.lisp_environment() {
                if let Some(value) = lisp_environment_binding_checked(environment, name)? {
                    return Ok(Some(value));
                }
                // The live Lisp alist is authoritative.  Its names and
                // binding cells may have changed since construction, so the
                // frame's typed snapshot must never become a stale fallback.
                continue;
            }
            let shared_updates = Self::frame_identity(frame)
                .and_then(|frame_id| self.lexical_cell_updates.get(&frame_id));
            for (k, v) in frame.iter().rev() {
                if k == name {
                    return Ok(Some(
                        shared_updates
                            .and_then(|updates| updates.get(name))
                            .cloned()
                            .unwrap_or_else(|| v.clone()),
                    ));
                }
            }
        }
        // A buffer-local cell always wins over a dynamically bound default.
        // This also covers a plain special that becomes buffer-local while
        // its global `let' is active: GNU then reads the newly created local
        // cell, while the specbind layer continues to own/restores only the
        // default value.
        if let Some(value) = self.buffer_local_value(self.current_buffer_id(), resolved) {
            return Ok(Some(value));
        }
        if let Some(value) = self.active_global_special_value(resolved) {
            return Ok(value.or_else(|| self.builtin_var_value(resolved)));
        }
        if let Some(value) = self.global_value(resolved) {
            return Ok(Some(value));
        }
        Ok(self.builtin_var_value(resolved))
    }

    pub fn symbol_value_cell(&self, name: &str) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(name)?;
        self.lookup_var_with_resolved_name(&resolved, &resolved, &Env::new())?
            .ok_or(LispError::Void(resolved))
    }

    pub(crate) fn builtin_var_value(&self, name: &str) -> Option<Value> {
        if let Some(variable) = DUMPED_AUTO_BUFFER_LOCALS
            .iter()
            .find(|variable| variable.name == name)
        {
            return Some(variable.default.value());
        }
        match name {
            "nil" => Some(Value::Nil),
            "t" => Some(Value::T),
            // casefiddle.c creates this value cell as nil; simple.el installs
            // the real extraction policy during loadup.
            "region-extract-function" => Some(Value::Nil),
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
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil),
            ),
            "buffer-file-truename" => Some(
                self.buffer
                    .file_truename
                    .clone()
                    .map(|value| Value::String(value.into()))
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
                    .map(|value| Value::Symbol(value.into()))
                    .collect::<Vec<_>>(),
            )),
            "coding-system-alist" => Some(Value::list(
                self.coding_system_list(false)
                    .into_iter()
                    .map(|name| Value::list([Value::String(name.into())]))
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
            "adaptive-fill-regexp" => Some(Value::String("[-–!|#%;>*·•‣⁃◦ \t]*".into())),
            "adaptive-fill-first-line-regexp" => Some(Value::String("\\`[ \t]*\\'".into())),
            "gc-cons-threshold" => Some(Value::Integer(800_000)),
            // GNU fileio.c defvar; simple.el reads it before files.el policy
            // is necessarily loaded.
            "auto-save-visited-file-name" => Some(Value::Nil),
            "load-read-function" => Some(Value::Symbol("read".into())),
            // GNU xdisp.c defvar; simple.el reads it at load time
            // ((when (eq pre-redisplay-function #'ignore) ...)).
            "pre-redisplay-function" => Some(Value::Symbol("ignore".into())),
            // GNU xfaces.c DEFVAR_LISP initializes the terminal color
            // registry before tty-colors.el layers its portable policy over
            // the host value cell.
            "tty-defined-color-alist" => Some(Value::Nil),
            // GNU startup.el defvar; bytecomp reads it.
            "startup-redirect-eln-cache" => Some(Value::Nil),
            // GNU startup.el defcustom.  Its declared special binding is
            // observable by separately loaded lexical libraries such as
            // time-stamp.el, even when the user leaves the value nil.
            "mail-host-address" => Some(Value::Nil),
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
            // GNU frame.c defines this value cell before tab-bar.el is
            // preloaded.  The Lisp owner deliberately uses `:variable' in
            // define-minor-mode because the host already owns the default.
            "tab-bar-mode" => Some(Value::Nil),
            // GNU xdisp.c publishes the tab/tool-bar resize and hover policy
            // controls as one native variable cluster before their Elisp
            // command layers are preloaded.
            "auto-resize-tab-bars"
            | "auto-raise-tab-bar-buttons"
            | "auto-resize-tool-bars"
            | "auto-raise-tool-bar-buttons" => Some(Value::T),
            "tab-bar-border" | "tool-bar-border" => {
                Some(Value::Symbol("internal-border-width".into()))
            }
            "tab-bar-button-margin" | "tab-bar-button-relief" | "tool-bar-button-relief" => {
                Some(Value::Integer(1))
            }
            "tool-bar-button-margin" => Some(Value::Integer(4)),
            // GNU minibuf.c publishes this before minibuffer.el; tab restore
            // policy consults it without requiring another Lisp library.
            "read-minibuffer-restore-windows" => Some(Value::T),
            // GNU's dumped image leaves automatic mini-window resizing at
            // its user-facing default and exposes textprop.c's point-motion
            // guard before any Lisp library is loaded.
            "resize-mini-windows" => Some(Value::Symbol("grow-only".into())),
            "max-mini-window-height" => Some(Value::Float(0.25)),
            "inhibit-point-motion-hooks" => Some(Value::T),
            "inhibit-x-resources" => Some(Value::T),
            // GNU isearch.el defcustom; package.el's quick-help reads it.
            "search-default-mode" => Some(Value::Nil),
            // GNU keyboard.c keymaps; simple.el define-keys them at load
            // time (event-apply-*-modifier bindings).
            "function-key-map"
            | "key-translation-map"
            | "input-decode-map"
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
                Value::String(String::new().into()),
                Value::String(".gz".into()),
            ])),
            "after-load-alist" => Some(Value::Nil),
            "load-true-file-name" => Some(
                self.current_load_file
                    .clone()
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil),
            ),
            "load-source-file-function" => Some(Value::Symbol("load-with-code-conversion".into())),
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
            "track-mouse" => Some(Value::Nil),
            "last-input-event" => Some(Value::Nil),
            "last-nonmenu-event" => Some(Value::Nil),
            "signal-hook-function" => Some(Value::Nil),
            "minor-mode-overriding-map-alist" => Some(Value::Nil),
            "standard-input" => Some(Value::T),
            "temporary-file-directory" => Some(Value::String(temp_directory_name().into())),
            "auto-compression-mode" => Some(Value::T),
            "command-switch-alist" => Some(Value::Nil),
            "command-line-args-left" => Some(Value::Nil),
            "purify-flag" => Some(Value::Nil),
            "require-final-newline" => Some(Value::Nil),
            "sentence-end" => Some(Value::Nil),
            "sentence-end-double-space" => Some(Value::T),
            "null-device" => Some(Value::String("/dev/null".into())),
            "exec-suffixes" => Some(Value::list([Value::String(String::new().into())])),
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
            // emacs.c defines both cells as nil.  Batch startup records the
            // real before/after values around reconstructed initialization,
            // matching startup.el's lifecycle rather than using sentinels.
            "before-init-time" | "after-init-time" => Some(Value::Nil),
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
            // File-less embeddings may load a downstream library without
            // reconstructing GNU loadup first.  Keep the early nil contract
            // only until byte-run.el begins installing its real declaration
            // helpers; from that point its defvars must be allowed to bind
            // the authoritative registries.
            "defun-declarations-alist" | "macro-declarations-alist"
                if !self.has_lisp_function("byte-run--set-speed") =>
            {
                Some(Value::Nil)
            }
            "post-self-insert-hook" => Some(Value::Nil),
            "macroexp--dynvars" => Some(Value::Nil),
            "macroexpand-all-environment" => Some(Value::Nil),
            "ls-lisp-use-insert-directory-program" => Some(Value::T),
            "transient-mark-mode" => Some(Value::Nil),
            "select-active-regions" => Some(Value::T),
            "saved-region-selection" => Some(Value::Nil),
            "desktop-buffer-mode-handlers" => Some(Value::Nil),
            "find-file-visit-truename" => Some(Value::Nil),
            "insert-directory-wildcard-in-dir-p" => Some(Value::Nil),
            "insert-directory-program" => Some(Value::String("ls".into())),
            "line-move-ignore-invisible" => Some(Value::T),
            "line-move-visual" => Some(Value::T),
            "file-name-invalid-regexp" => Some(Value::String("\0".into())),
            "directory-listing-before-filename-regexp" => Some(Value::String(
                directory_listing_before_filename_regexp().into(),
            )),
            "minor-mode-alist" => Some(Value::Nil),
            "timer-list" | "timer-idle-list" => Some(Value::Nil),
            "revert-buffer-function" => Some(Value::Symbol("revert-buffer--default".into())),
            "buffer-stale-function" => Some(Value::Symbol("buffer-stale--default-function".into())),
            "buffer-auto-revert-by-notification" => Some(Value::Nil),
            "non-essential" => Some(Value::Nil),
            "remote-file-name-inhibit-cache" => Some(Value::Nil),
            // files.el: derived modes consult this when setting
            // `require-final-newline' buffer-locally.
            "mode-require-final-newline" => Some(Value::T),
            // insdel.c: change hooks run unless a primitive binds this.
            "inhibit-modification-hooks" => Some(Value::Nil),
            // doc.c: name of the DOC file inside `doc-directory'.
            "internal-doc-file-name" => Some(Value::String("DOC".into())),
            // font-lock.el: the standard face variables are self-quoting
            // defvars that keyword FACENAME expressions evaluate.
            "font-lock-comment-face" => Some(Value::Symbol("font-lock-comment-face".into())),
            "font-lock-comment-delimiter-face" => {
                Some(Value::Symbol("font-lock-comment-delimiter-face".into()))
            }
            "font-lock-string-face" => Some(Value::Symbol("font-lock-string-face".into())),
            "font-lock-doc-face" => Some(Value::Symbol("font-lock-doc-face".into())),
            "font-lock-doc-markup-face" => Some(Value::Symbol("font-lock-doc-markup-face".into())),
            "font-lock-keyword-face" => Some(Value::Symbol("font-lock-keyword-face".into())),
            "font-lock-builtin-face" => Some(Value::Symbol("font-lock-builtin-face".into())),
            "font-lock-function-name-face" => {
                Some(Value::Symbol("font-lock-function-name-face".into()))
            }
            "font-lock-variable-name-face" => {
                Some(Value::Symbol("font-lock-variable-name-face".into()))
            }
            "font-lock-type-face" => Some(Value::Symbol("font-lock-type-face".into())),
            "font-lock-constant-face" => Some(Value::Symbol("font-lock-constant-face".into())),
            "font-lock-warning-face" => Some(Value::Symbol("font-lock-warning-face".into())),
            "font-lock-negation-char-face" => {
                Some(Value::Symbol("font-lock-negation-char-face".into()))
            }
            "font-lock-preprocessor-face" => {
                Some(Value::Symbol("font-lock-preprocessor-face".into()))
            }
            "overriding-local-map" => Some(Value::Nil),
            "overriding-terminal-local-map" => Some(Value::Nil),
            "menu-bar-final-items" => Some(Value::Nil),
            "menu-bar-separator" => Some(Value::Symbol("menu-bar-separator".into())),
            "window-display-table" => Some(Value::Nil),
            // GNU C DEFVARs cus-start.el customizes: each is a genuine
            // native value cell (window.c, xdisp.c, keyboard.c, undo.c,
            // minibuf.c, callint.c, frame.c, dispnew.c, alloc.c, emacs.c)
            // with its C initialization value.
            "blink-cursor-alist" => Some(Value::Nil),
            "composition-break-at-point" => Some(Value::Nil),
            "debug-on-event" => Some(Value::Symbol("sigusr2".into())),
            "display-fill-column-indicator" => Some(Value::Nil),
            "display-fill-column-indicator-character" => Some(Value::Nil),
            "display-fill-column-indicator-column" => Some(Value::T),
            "display-line-numbers" => Some(Value::Nil),
            "display-line-numbers-current-absolute" => Some(Value::T),
            "display-line-numbers-major-tick" => Some(Value::Integer(0)),
            "display-line-numbers-minor-tick" => Some(Value::Integer(0)),
            "display-line-numbers-widen" => Some(Value::Nil),
            "display-line-numbers-width" => Some(Value::Nil),
            "display-raw-bytes-as-hex" => Some(Value::Nil),
            "focus-follows-mouse" => Some(Value::Nil),
            "frame-inhibit-implied-resize" => {
                Some(Value::list([Value::Symbol("tab-bar-lines".into())]))
            }
            "frame-resize-pixelwise" => Some(Value::Nil),
            "garbage-collection-messages" => Some(Value::Nil),
            "gc-cons-percentage" => Some(Value::Float(0.1)),
            "highlight-nonselected-windows" => Some(Value::Nil),
            "history-delete-duplicates" => Some(Value::Nil),
            "history-length" => Some(Value::Integer(100)),
            "hourglass-delay" => Some(Value::Integer(1)),
            "iconify-child-frame" => Some(Value::Symbol("iconify-top-level".into())),
            "inverse-video" => Some(Value::Nil),
            "line-number-display-limit" => Some(Value::Nil),
            "line-number-display-limit-width" => Some(Value::Integer(200)),
            "make-cursor-line-fully-visible" => Some(Value::T),
            "make-pointer-invisible" => Some(Value::T),
            "mark-even-if-inactive" => Some(Value::T),
            "maximum-scroll-margin" => Some(Value::Float(0.25)),
            "menu-bar-mode" => Some(Value::T),
            "menu-prompting" => Some(Value::T),
            "minibuffer-follows-selected-frame" => Some(Value::T),
            "mode-line-compact" => Some(Value::Nil),
            "mouse-autoselect-window" => Some(Value::Nil),
            "mouse-highlight" => Some(Value::T),
            "mouse-prefer-closest-glyph" => Some(Value::Nil),
            "no-redraw-on-reenter" => Some(Value::Nil),
            "overflow-newline-into-fringe" => Some(Value::T),
            "overline-margin" => Some(Value::Integer(2)),
            "read-buffer-completion-ignore-case" => Some(Value::Nil),
            "record-all-keys" => Some(Value::Nil),
            "report-emacs-bug-address" => Some(Value::String("bug-gnu-emacs@gnu.org".into())),
            "resize-mini-frames" => Some(Value::Nil),
            "ring-bell-function" => Some(Value::Nil),
            "scroll-conservatively" => Some(Value::Integer(0)),
            "scroll-step" => Some(Value::Integer(0)),
            "show-trailing-whitespace" => Some(Value::Nil),
            "tab-bar-position" => Some(Value::Nil),
            "tool-bar-max-label-size" => Some(Value::Integer(14)),
            "tool-bar-mode" => Some(Value::T),
            "tool-bar-style" => Some(Value::Nil),
            "tooltip-reuse-hidden-frame" => Some(Value::Nil),
            "translate-upper-case-key-bindings" => Some(Value::T),
            "underline-minimum-offset" => Some(Value::Integer(1)),
            "undo-limit" => Some(Value::Integer(160000)),
            "undo-outer-limit" => Some(Value::Integer(24000000)),
            "undo-strong-limit" => Some(Value::Integer(240000)),
            "unibyte-display-via-language-environment" => Some(Value::Nil),
            "use-system-tooltips" => Some(Value::T),
            "visible-bell" => Some(Value::Nil),
            "visible-cursor" => Some(Value::T),
            "void-text-area-pointer" => Some(Value::Symbol("arrow".into())),
            "word-wrap-by-category" => Some(Value::Nil),
            "words-include-escapes" => Some(Value::Nil),
            "x-underline-at-descent-line" => Some(Value::Nil),
            "x-use-underline-position-properties" => Some(Value::T),
            "yes-or-no-prompt" => Some(Value::String("(yes or no) ".into())),
            "standard-display-table" => Some(Value::Nil),
            "text-mode-syntax-table" => Some(Value::CharTable(2)),
            "emacs-lisp-mode-syntax-table" => {
                Some(Value::CharTable(self.emacs_lisp_mode_syntax_table_id()))
            }
            "lisp-mode-syntax-table" | "lisp-data-mode-syntax-table" => {
                Some(Value::CharTable(self.lisp_data_syntax_table_id()))
            }
            "prog-mode-syntax-table" => Some(Value::CharTable(self.standard_syntax_table_id())),
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
                    .unwrap_or_else(primitives::default_directory)
                    .into(),
            )),
            "data-directory" | "doc-directory" => Some(Value::String(
                primitives::compat_data_directory()
                    .unwrap_or_else(primitives::default_directory)
                    .into(),
            )),
            "user-login-name" => Some(Value::String(
                primitives::current_user_login_name()
                    .unwrap_or_else(|| "user".into())
                    .into(),
            )),
            "user-real-login-name" => Some(Value::String(
                primitives::current_real_user_login_name()
                    .unwrap_or_else(|| "user".into())
                    .into(),
            )),
            // sysdep.c initializes this dumped variable from the same host
            // identity returned by the `system-name' primitive.
            "system-name" => Some(Value::String(primitives::system_name_value().into())),
            "user-full-name" => Some(Value::String(
                primitives::current_user_full_name()
                    .or_else(primitives::current_user_login_name)
                    .unwrap_or_else(|| "user".into())
                    .into(),
            )),
            "user-mail-address" => Some(Value::String(
                format!(
                    "{}@{}",
                    primitives::current_user_login_name().unwrap_or_else(|| "user".into()),
                    primitives::system_name_value()
                )
                .into(),
            )),
            "default-directory" => Some(Value::String(primitives::default_directory().into())),
            "current-language-environment" => Some(Value::String("English".into())),
            "window-system" => Some(Value::Nil),
            "initial-window-system" => Some(Value::Nil),
            "left-margin" => Some(Value::Integer(0)),
            "this-single-command-keys" => {
                Some(Value::list([Value::Symbol("vector-literal".into())]))
            }
            "unread-command-events" => Some(Value::Nil),
            "deactivate-mark" => Some(Value::Nil),
            // xdisp.c: the frame whose menu bar is being updated, nil
            // outside menu updates (menu-bar.el's :enable forms read it).
            "menu-updating-frame" => Some(Value::Nil),
            // xdisp.c's DEFVAR_BOOL, default off; xt-mouse consults it.
            "x-stretch-cursor" => Some(Value::Nil),
            // startup.el's dumped defcustom default; the File menu's
            // session-recovery :enable reads it.
            "auto-save-list-file-prefix" => {
                Some(Value::String("~/.emacs.d/auto-save-list/.saves-".into()))
            }
            "line-spacing" => Some(Value::Nil),
            "scroll-margin" => Some(Value::Integer(0)),
            "scroll-preserve-screen-position" => Some(Value::Nil),
            "overwrite-mode" => Some(Value::Nil),
            "load-path" => Some(Value::list(
                self.load_path
                    .iter()
                    .map(|path| Value::String(primitives::path_to_directory_string(path).into()))
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
                        .unwrap_or_else(primitives::default_directory)
                        .into(),
                ),
                Value::Symbol("data-directory".into()),
                Value::Symbol("load-path".into()),
            ])),
            "installation-directory" => Some(
                primitives::compat_installation_directory()
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil),
            ),
            "tab-width" => Some(Value::Integer(8)),
            "indent-tabs-mode" => Some(Value::T),
            "indent-line-function" => Some(Value::Symbol("indent-relative".into())),
            "tab-stop-list" => Some(Value::Nil),
            "use-dialog-box" => Some(Value::T),
            "use-file-dialog" => Some(Value::T),
            "command-error-function" => {
                Some(Value::Symbol("command-error-default-function".into()))
            }
            "read-file-name-completion-ignore-case" => Some(Value::Nil),
            "mounted-file-systems" => Some(Value::String(
                r"^\(?:/\(?:afs/\|m\(?:edia/\|nt\)\|\(?:ne\|tmp_mn\)t/\)\)".into(),
            )),
            "system-type" => Some(Value::Symbol(primitives::gnu_system_type().into())),
            "system-configuration" => {
                Some(Value::String(primitives::system_configuration().into()))
            }
            "system-configuration-features" => Some(Value::String(
                std::env::var("EMAXX_SYSTEM_CONFIGURATION_FEATURES")
                    .unwrap_or_default()
                    .into(),
            )),
            "system-configuration-options" => Some(Value::String(
                std::env::var("EMAXX_SYSTEM_CONFIGURATION_OPTIONS")
                    .unwrap_or_default()
                    .into(),
            )),
            "charset-list" => Some(Value::list(
                self.charset_priority_list()
                    .into_iter()
                    .map(|value| Value::Symbol(value.into()))
                    .collect::<Vec<_>>(),
            )),
            "ert-resource-directory-format" => Some(Value::String("%s-resources/".into())),
            "ert-resource-directory-trim-left-regexp" => Some(Value::String(String::new().into())),
            "ert-resource-directory-trim-right-regexp" => {
                Some(Value::String("\\(-tests?\\)?\\.el".into()))
            }
            "load-file-name" => Some(
                self.current_load_file
                    .clone()
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil),
            ),
            "read-buffer-function" | "read-file-name-function" => Some(Value::Nil),
            "delete-by-moving-to-trash" => Some(Value::Nil),
            "directory-files-no-dot-files-regexp" => Some(Value::String("[^.]\\|\\.\\.\\.".into())),
            "user-emacs-directory" => Some(Value::String("/nonexistent/.emacs.d/".into())),
            "invocation-name" => Some(Value::String(
                primitives::current_invocation_name()
                    .unwrap_or_else(|| "emaxx".into())
                    .into(),
            )),
            "invocation-directory" => Some(Value::String(
                primitives::current_invocation_directory()
                    .unwrap_or_else(primitives::default_directory)
                    .into(),
            )),
            "shell-file-name" => Some(Value::String(
                primitives::find_executable("sh")
                    .unwrap_or_else(|| "/bin/sh".into())
                    .into(),
            )),
            "shell-command-switch" => Some(Value::String("-c".into())),
            "emacs-version" => Some(Value::String(primitives::emacs_version_value().into())),
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
                    .unwrap_or_else(|| "emacsclient".into())
                    .into(),
            )),
            "movemail-program-name" => Some(Value::String("movemail".into())),
            "ebrowse-program-name" => Some(Value::String("ebrowse".into())),
            "rcs2log-program-name" => Some(Value::String("rcs2log".into())),
            "process-environment" | "initial-environment" => Some(Value::list(
                std::env::vars()
                    .map(|(name, value)| Value::String(format!("{name}={value}").into()))
                    .collect::<Vec<_>>(),
            )),
            "find-program" => Some(Value::String("find".into())),
            "grep-program" => Some(Value::String("grep".into())),
            _ if name.starts_with('.') => Some(Value::Nil),
            _ if name.starts_with(':') => Some(Value::Symbol(name.to_string().into())),
            _ => None,
        }
    }

    /// Look up a variable in the given local env, then globals.
    pub(crate) fn lookup(&self, name: &str, env: &Env) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(name)?;
        self.lookup_var_with_resolved_name(name, &resolved, env)?
            .ok_or(LispError::Void(resolved))
    }

    /// Whether NAME has a user-level function definition (defun/fset).
    pub(crate) fn function_index_has(&self, name: &str) -> bool {
        self.functions_index.contains_key(name)
    }

    /// Whether local state can change symbol-function resolution.
    ///
    /// cl-flet/cl-labels frames are explicit, but this interpreter also
    /// represents some generated local functions as callable values in an
    /// ordinary lexical frame.  Symbol aliases can reach either kind, so a
    /// global resolution cache is safe only when neither is present.
    pub(crate) fn env_may_affect_function_resolution(env: &Env) -> bool {
        env.iter().any(|frame| {
            if frame.lisp_environment().is_some() {
                return false;
            }
            frame.has_function_bindings()
                || frame
                    .iter()
                    .any(|(_, value)| matches!(value, Value::BuiltinFunc(_) | Value::Lambda(_)))
        })
    }

    pub fn raw_function_binding(&self, name: &str, env: &Env) -> Option<Value> {
        let facts = primitives::name_facts(name);
        if facts.prefer_override {
            return Some(Value::BuiltinFunc(name.to_string().into()));
        }
        let name_is_builtin = facts.builtin || facts.special_form;
        for frame in env.iter().rev() {
            // GNU's interpreted lexical environment is a value namespace;
            // callable values stored there never shadow a function cell.
            if frame.lisp_environment().is_some() {
                continue;
            }
            // A builtin's function position can only be shadowed by a real
            // function frame (cl-flet/cl-labels); a plain `let' binding a
            // VARIABLE named `car' to a lambda must not hijack `(car x)'.
            if name_is_builtin && !frame.has_function_bindings() {
                continue;
            }
            for (k, v) in frame.iter().rev() {
                if k == name && matches!(v, Value::BuiltinFunc(_) | Value::Lambda(_)) {
                    return Some(v.clone());
                }
            }
        }
        if let Some(v) = self.functions_index.get(name) {
            return Some(v.clone());
        }
        // Special forms live in function cells in GNU Emacs, so symbol
        // indirection (indirect-function, fboundp, macrop) must resolve them
        // instead of signaling a void-function error.
        if name_is_builtin {
            return Some(Value::BuiltinFunc(name.to_string().into()));
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
    /// frames (typed cl-flet/cl-labels frames) can shadow them — a plain
    /// `let'-bound value never
    /// does in GNU.  Skipping the per-entry scan of ordinary frames
    /// keeps the per-form macro probe cheap on deep call stacks.  The
    /// bool is true when the binding came from an env frame (such a
    /// verdict must not be cached as a global fact).
    fn macro_position_binding(&self, name: &str, env: &Env) -> Option<(Value, bool)> {
        let facts = primitives::name_facts(name);
        if facts.prefer_override {
            return Some((Value::BuiltinFunc(name.to_string().into()), false));
        }
        for frame in env.iter().rev() {
            if !frame.has_function_bindings() {
                continue;
            }
            for (key, value) in frame.iter().rev() {
                if key == name && matches!(value, Value::BuiltinFunc(_) | Value::Lambda(_)) {
                    return Some((value.clone(), true));
                }
            }
        }
        if let Some(value) = self.functions_index.get(name) {
            return Some((value.clone(), false));
        }
        if matches!(name, "incf" | "decf") || facts.builtin || facts.special_form {
            return Some((Value::BuiltinFunc(name.to_string().into()), false));
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
                Value::Symbol(next) => current = next.to_string(),
                other => return Some((other, from_frame)),
            }
        }
    }

    pub fn lookup_function(&self, name: &str, env: &Env) -> Result<Value, LispError> {
        // Fast path: nearly every function cell holds the callable directly,
        // so don't pay for indirection-cycle bookkeeping until a symbol
        // chain actually appears.
        let Some(binding) = self.raw_function_binding(name, env) else {
            return Err(LispError::VoidFunction(name.to_string()));
        };
        let Value::Symbol(next) = binding else {
            return Ok(binding);
        };

        let mut current = next.to_string();
        let mut seen = HashSet::new();
        seen.insert(name.to_string());
        loop {
            if !seen.insert(current.clone()) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-function-indirection".into()),
                    Value::Symbol(name.to_string().into()),
                ])));
            }

            let Some(binding) = self.raw_function_binding(&current, env) else {
                return Err(LispError::VoidFunction(current));
            };
            match binding {
                Value::Symbol(next) => current = next.to_string(),
                other => return Ok(other),
            }
        }
    }

    pub fn has_macro_binding(&self, name: &str) -> bool {
        self.function_cell_macro_expander(name, &Env::new())
            .is_some()
    }

    pub fn known_symbol_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        let mut seen = HashSet::new();
        let mut push_name = |name: &str| {
            if crate::lisp::types::visible_symbol_name(name) != name
                || self.uninterned_standard_symbol_names.contains(name)
            {
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
        if self.uninterned_standard_symbol_names.contains(name) {
            return false;
        }
        matches!(name, "nil" | "t")
            || self.interned_symbol_names.contains(name)
            || self.globals.contains_key(name)
            || self.variable_aliases_index.contains_key(name)
            || self.functions_index.contains_key(name)
            || self.symbol_property_index(name).is_some()
    }

    pub(crate) fn standard_obarray_symbol_is_uninterned(&self, name: &str) -> bool {
        self.uninterned_standard_symbol_names.contains(name)
    }

    /// Invalidate all cached not-a-macro verdicts; called on every
    /// function or macro (re)definition.
    pub(crate) fn note_definition_changed(&mut self) {
        self.definition_generation = self.definition_generation.wrapping_add(1);
    }

    pub(crate) fn current_definition_generation(&self) -> u64 {
        self.definition_generation
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

    pub(super) fn source_call_known_not_macro(
        &self,
        cache: &Rc<RefCell<SourceMacroCallCache>>,
    ) -> bool {
        cache.borrow().not_macro_generation == Some(self.definition_generation)
    }

    pub(super) fn cache_source_not_macro(&self, cache: &Rc<RefCell<SourceMacroCallCache>>) {
        cache.borrow_mut().not_macro_generation = Some(self.definition_generation);
    }

    /// Set a variable in the innermost local frame, or in globals.
    ///
    /// Source `setq' uses the checked form so GNU's live-alist errors remain
    /// observable.  The infallible wrapper remains for host-side state
    /// restoration paths, whose environments are constructed internally and
    /// therefore cannot contain malformed Lisp tails.
    pub fn set_variable(&mut self, name: &str, value: Value, env: &mut Env) {
        let result = self.set_variable_checked(name, value, env);
        debug_assert!(result.is_ok(), "host environment must be well formed");
    }

    pub(super) fn set_variable_checked(
        &mut self,
        name: &str,
        value: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        if !self.set_lexical_variable_checked(name, value.clone(), env)? {
            self.set_symbol_value_cell(name, value);
        }
        Ok(())
    }

    fn set_lexical_variable_checked(
        &mut self,
        name: &str,
        value: Value,
        env: &mut Env,
    ) -> Result<bool, LispError> {
        let floor = self.special_scan_floor;
        let mut special: Option<bool> = None;
        for (index, frame) in env.iter_mut().enumerate().rev() {
            // Mirror `lookup': below the caller boundary a SPECIAL variable
            // is set dynamically, never through a caller's lexical frame.
            if index < floor && *special.get_or_insert_with(|| self.is_dynamic_binding_name(name)) {
                break;
            }
            if let Some(environment) = frame.lisp_environment().cloned() {
                let frame_id = Self::frame_identity(frame);
                let stored = Self::stored_value(value.clone());
                if set_lisp_environment_binding_checked(&environment, name, stored.clone())? {
                    if let Some(frame_id) = frame_id {
                        self.record_lexical_cell_update_if_captured(frame_id, name, &stored);
                    }
                    return Ok(true);
                }
                continue;
            }
            let frame_id = Self::frame_identity(frame);
            if let Some(binding_index) = frame.iter().rposition(|(key, _)| key == name) {
                let stored = Self::stored_value(value);
                frame[binding_index].1 = stored.clone();
                if let Some(frame_id) = frame_id {
                    self.record_lexical_cell_update_if_captured(frame_id, name, &stored);
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// GNU's `setq' probes the lexical alist before entering `Fset'.  A
    /// lexical assignment therefore neither invokes variable watchers nor
    /// reaches a buffer/global value cell, and a malformed alist errors before
    /// either side effect.  Keep that ordering at this boundary.
    pub(super) fn setq_variable(
        &mut self,
        name: &str,
        value: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        if self.set_lexical_variable_checked(name, value.clone(), env)? {
            return Ok(());
        }
        let buffer_id = self.assignment_buffer_id(name);
        self.notify_variable_watchers(name, value.clone(), "set", buffer_id, env)?;
        self.set_symbol_value_cell(name, value);
        Ok(())
    }

    pub fn set_symbol_value_cell(&mut self, name: &str, value: Value) {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        let value = Self::stored_value(value);
        if resolved == "buffer-file-name" {
            let file = match value {
                Value::Nil => None,
                Value::String(path) => Some(path.to_string()),
                Value::StringObject(state) => Some(state.borrow().text.clone()),
                other => Some(other.to_string()),
            };
            self.set_current_buffer_file_name(file);
            return;
        }
        if resolved == "buffer-file-truename" {
            self.buffer.file_truename = match value {
                Value::Nil => None,
                Value::String(path) => Some(path.to_string()),
                Value::StringObject(state) => Some(state.borrow().text.clone()),
                other => Some(other.to_string()),
            };
            return;
        }
        if resolved == "mark-active" {
            self.buffer.set_mark_active(value.is_truthy());
            return;
        }
        if resolved == "buffer-undo-list" {
            if value.is_nil() {
                self.buffer.enable_undo();
                self.buffer.clear_undo_history();
            } else if matches!(value, Value::T) {
                self.buffer.disable_undo();
            } else if let Some((head, tail)) = value.cons_values()
                && crate::lisp::primitives::values_eql(
                    &tail,
                    &crate::lisp::primitives::buffer_undo_list_value(&self.buffer),
                )
            {
                self.buffer.enable_undo();
                let entry = buffer_undo_head_to_entry(&head);
                self.buffer.push_undo_entry(entry);
                self.buffer.set_undo_list_view(value);
            } else {
                // A structural replacement (undo truncation after
                // cancel-change-group, boundary removal by
                // undo-amalgamate-change-group, ...): rebuild the native undo
                // state from the assigned list so the Lisp view round-trips.
                if let Ok(items) = value.to_vec() {
                    self.buffer.enable_undo();
                    self.buffer.clear_undo_history();
                    // The Lisp view is newest-first; the native list is
                    // oldest-first.
                    for item in items.into_iter().rev() {
                        self.buffer
                            .push_undo_entry(buffer_undo_head_to_entry(&item));
                    }
                    self.buffer.set_undo_list_view(value);
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
        self.set_global_binding(&resolved, value);
    }

    // GNU stores a macro in the function cell as (macro . EXPANDER); emaxx
    // keeps a native macro table, so synthesize the GNU shape on demand
    // (nadvice reads and rewrites it when advising macros).
    // Follow the function cell (through symbol aliases) to a
    // (macro . EXPANDER) cons; nadvice installs advised macros that way.
    pub(crate) fn function_cell_macro_expander(&self, name: &str, env: &Env) -> Option<Value> {
        let mut current = name.to_string();
        for _ in 0..10 {
            let (binding, _) = self.macro_position_binding(&current, env)?;
            match binding {
                Value::Symbol(next) => current = next.to_string(),
                Value::Cons(cons_cell) => {
                    let car = &cons_cell.car;
                    let cdr = &cons_cell.cdr;
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
            &[Value::Symbol(name.to_string().into()), definition.clone()],
            env,
        );
        handled.is_ok()
    }

    // GNU keeps macro-ness in the function cell: fsetting a plain function
    // over a macro name (or voiding the cell) erases the macro definition.
    // The macro table is positional (cl-macrolet drains index ranges), so
    // entries are renamed out of resolution instead of removed.
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
            Value::Symbol(name) | Value::BuiltinFunc(name) => Some(name.to_string()),
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

    pub(crate) fn begin_timer_callback(&mut self) {
        self.timer_callback_depth += 1;
    }

    pub(crate) fn defer_unloaded_defsubst(&mut self, name: &str, env: &Env) -> bool {
        if self.timer_callback_depth == 0
            || !self
                .lookup_var("loadhist-unload-filename", env)
                .is_some_and(|value| value.is_truthy())
            || self.get_symbol_property(name, "byte-optimizer")
                != Some(Value::Symbol("byte-compile-inline-expand".into()))
        {
            return false;
        }
        let Some(definition) = self.functions_index.get(name).cloned() else {
            return false;
        };
        if !self
            .deferred_defsubst_unbindings
            .iter()
            .any(|(queued, _)| queued == name)
        {
            self.deferred_defsubst_unbindings
                .push((name.to_string(), definition));
        }
        true
    }

    pub(crate) fn end_timer_callback(&mut self) {
        debug_assert!(self.timer_callback_depth > 0);
        self.timer_callback_depth -= 1;
        if self.timer_callback_depth != 0 {
            return;
        }
        for (name, definition) in std::mem::take(&mut self.deferred_defsubst_unbindings) {
            if self.functions_index.get(&name) == Some(&definition) {
                self.set_function_binding(&name, None);
            }
        }
    }

    pub fn validate_function_binding(&self, name: &str, function: &Value) -> Result<(), LispError> {
        let Value::Symbol(current) = function else {
            return Ok(());
        };
        let mut current = current.to_string();
        let mut seen = vec![name.to_string()];
        loop {
            if seen.iter().any(|existing| existing == &current) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-function-indirection".into()),
                    Value::Symbol(name.to_string().into()),
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
            current = next.to_string();
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
