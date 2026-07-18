use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "set-buffer"
            | "switch-to-buffer"
            | "switch-to-buffer-other-window"
            | "pop-to-buffer"
            | "pop-to-buffer-same-window"
            | "create-file-buffer"
            | "buffer-file-name"
            | "backup-file-name-p"
            | "auto-save-file-name-p"
            | "visited-file-modtime"
            | "verify-visited-file-modtime"
            | "set-visited-file-modtime"
            | "rename-visited-file"
            | "read-only-mode"
            | "set-buffer-file-coding-system"
            | "after-insert-file-set-coding"
            | "local"
            | "add-function"
            | "fundamental-mode"
            | "prog-mode"
            | "emacs-lisp-mode"
            | "special-mode"
            | "normal-mode"
            | "find-file-noselect"
            | "find-file-literally"
            | "find-buffer-visiting"
            | "find-file"
            | "get-file-buffer"
            | "file-has-changed-p"
            | "expand-file-name"
            | "locate-file"
            | "file-relative-name"
            | "jka-compr-get-compression-info"
            | "substitute-in-file-name"
            | "file-name-directory"
            | "file-name-nondirectory"
            | "file-name-split"
            | "file-name-sans-versions"
            | "file-name-sans-extension"
            | "file-name-base"
            | "file-name-extension"
            | "file-name-as-directory"
            | "directory-file-name"
            | "directory-name-p"
            | "file-name-absolute-p"
            | "file-name-case-insensitive-p"
            | "file-name-concat"
            | "file-name-unquote"
            | "file-local-name"
            | "file-local-copy"
            | "file-remote-p"
            | "file-expand-wildcards"
            | "find-file-name-handler"
            | "emaxx-mock-file-name-handler"
            | "dired-noselect"
            | "dired-revert"
            | "emaxx-dired-revert"
            | "dired-buffer-stale-p"
            | "shell-quote-argument"
            | "locate-user-emacs-file"
            | "ert-resource-directory"
            | "ert-resource-file"
            | "ert-gcc-is-clang-p"
            | "ert-fail"
            | "ert-pass"
            | "locate-library"
            | "get-load-suffixes"
            | "load"
            | "load-file"
            | "locate-file-internal"
            | "directory-files"
            | "directory-files-and-attributes"
            | "directory-empty-p"
            | "file-directory-p"
            | "file-in-directory-p"
            | "file-accessible-directory-p"
            | "file-readable-p"
            | "file-regular-p"
            | "file-writable-p"
            | "file-exists-p"
            | "file-executable-p"
            | "file-attributes"
            | "file-attribute-type"
            | "file-attribute-link-number"
            | "file-attribute-user-id"
            | "file-attribute-group-id"
            | "file-attribute-access-time"
            | "file-attribute-modification-time"
            | "file-attribute-status-change-time"
            | "file-attribute-size"
            | "file-attribute-modes"
            | "file-attribute-inode-number"
            | "file-attribute-device-number"
            | "file-attribute-file-identifier"
            | "delete-file"
            | "copy-file"
            | "rename-file"
            | "delete-file-internal"
            | "delete-directory"
            | "delete-directory-internal"
            | "make-directory"
            | "make-empty-file"
            | "mkdir"
            | "make-directory-internal"
            | "make-temp-file"
            | "make-temp-file-internal"
            | "file-locked-p"
            | "write-region"
            | "write-file"
            | "kqueue-add-watch"
            | "kqueue-rm-watch"
            | "kqueue-valid-p"
            | "default-file-modes"
            | "file-modes"
            | "file-modes-number-to-symbolic"
            | "set-default-file-modes"
            | "set-file-modes"
            | "set-file-times"
            | "insert-directory"
            | "insert-file-contents"
            | "insert-file-contents-literally"
            | "get-free-disk-space"
            | "set-visited-file-name"
            | "file-name-all-completions"
            | "file-symlink-p"
            | "make-symbolic-link"
            | "call-process"
            | "make-process"
            | "make-pipe-process"
            | "start-process"
            | "start-file-process"
            | "get-buffer-process"
            | "process-buffer"
            | "process-mark"
            | "process-status"
            | "process-exit-status"
            | "process-plist"
            | "set-process-plist"
            | "process-get"
            | "process-put"
            | "process-live-p"
            | "process-attributes"
            | "process-coding-system"
            | "set-process-coding-system"
            | "set-process-filter"
            | "set-process-sentinel"
            | "set-process-buffer"
            | "process-sentinel"
            | "process-name"
            | "process-command"
            | "process-tty-name"
            | "get-process"
            | "process-contact"
            | "make-network-process"
            | "open-network-stream"
            | "set-network-process-option"
            | "network-interface-list"
            | "network-interface-info"
            | "delete-process"
            | "set-process-query-on-exit-flag"
            | "process-send-string"
            | "process-send-eof"
            | "url-retrieve"
            | "url-retrieve-synchronously"
            | "url-http-file-exists-p"
            | "url-insert"
            | "process-lines"
            | "get-locale-names"
            | "zlib-decompress-region"
            | "libxml-parse-xml-region"
            | "call-process-region"
            | "shell-command"
            | "kill-buffer"
            | "bury-buffer"
            | "set-mark"
            | "set-mark-command"
            | "push-mark"
            | "mark"
            | "make-marker"
            | "copy-marker"
            | "point-marker"
            | "mark-marker"
            | "point-min-marker"
            | "point-max-marker"
            | "marker-buffer"
            | "marker-position"
            | "marker-last-position"
            | "marker-insertion-type"
            | "set-marker-insertion-type"
            | "set-marker"
            | "move-marker"
            | "region-beginning"
            | "region-end"
            | "deactivate-mark"
            | "region-active-p"
            | "use-region-p"
    )
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match name {
        "set-buffer" => {
            need_args(name, args, 1)?;
            let id = interp.resolve_buffer_id(&args[0])?;
            interp.set_current_buffer_id(id)?;
            Ok(Value::Buffer(id, interp.buffer.name.clone()))
        }
        "switch-to-buffer" => {
            need_args(name, args, 1)?;
            let id = if let Some(name) = string_like(&args[0]).map(|string| string.text) {
                interp
                    .find_buffer(&name)
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer(&name).0)
            } else {
                interp.resolve_buffer_id(&args[0])?
            };
            interp.switch_to_buffer_id(id)?;
            let norecord = args.get(1).is_some_and(|value| value.is_truthy());
            if !norecord {
                interp.record_buffer_front(id);
            }
            Ok(Value::Buffer(id, interp.buffer.name.clone()))
        }
        "pop-to-buffer" | "pop-to-buffer-same-window" | "switch-to-buffer-other-window" => {
            need_args(name, args, 1)?;
            let id = if let Some(name) = string_like(&args[0]).map(|string| string.text) {
                interp
                    .find_buffer(&name)
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer(&name).0)
            } else {
                interp.resolve_buffer_id(&args[0])?
            };
            interp.switch_to_buffer_id(id)?;
            // NORECORD is the third arg for pop-to-buffer (after ACTION)
            // and the second for the same-window/other-window variants.
            let norecord_index = if name == "pop-to-buffer" { 2 } else { 1 };
            let norecord = args
                .get(norecord_index)
                .is_some_and(|value| value.is_truthy());
            if !norecord {
                interp.record_buffer_front(id);
            }
            Ok(Value::Buffer(id, interp.buffer.name.clone()))
        }
        "create-file-buffer" => {
            need_args(name, args, 1)?;
            let filename = string_text(&args[0])?;
            let path = std::path::Path::new(&filename);
            let basename = path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.is_empty())
                .unwrap_or(filename.as_str());
            let basename = if basename.starts_with(' ') {
                format!("|{basename}")
            } else {
                basename.to_string()
            };
            let buf_name = if interp.has_buffer(&basename) {
                let mut n = 2;
                loop {
                    let candidate = format!("{}<{}>", basename, n);
                    if !interp.has_buffer(&candidate) {
                        break candidate;
                    }
                    n += 1;
                }
            } else {
                basename
            };
            let (id, _) = interp.create_buffer(&buf_name);
            Ok(Value::Buffer(id, buf_name))
        }
        "buffer-file-name" => {
            need_arg_range(name, args, 0, 1)?;
            let buffer_id = if let Some(buffer) = args.first().filter(|value| !value.is_nil()) {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            Ok(interp
                .get_buffer_by_id(buffer_id)
                .and_then(|buffer| buffer.file.clone())
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "backup-file-name-p" => {
            need_args(name, args, 1)?;
            Ok(if string_text(&args[0])?.ends_with('~') {
                Value::T
            } else {
                Value::Nil
            })
        }
        "auto-save-file-name-p" => {
            need_args(name, args, 1)?;
            let filename = string_text(&args[0])?;
            Ok(
                if filename.starts_with('#') && filename.ends_with('#') && filename.len() >= 2 {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "visited-file-modtime" => Ok(interp
            .buffer
            .visited_file_modtime()
            .and_then(|modtime| system_time_seconds_value(modtime.modified).ok())
            .unwrap_or(Value::Integer(0))),
        "verify-visited-file-modtime" => {
            need_args(name, args, 1)?;
            let buffer_id = interp.resolve_buffer_id(&args[0])?;
            let remote_visit = interp
                .buffer_local_value(buffer_id, "emaxx--visited-remote-prefix")
                .is_some_and(|value| value.is_truthy());
            let Some(buffer) = interp.get_buffer_by_id(buffer_id) else {
                return Ok(Value::Nil);
            };
            let Some(path) = buffer.file.as_deref() else {
                return Ok(Value::T);
            };
            let current = file_modtime(path)?;
            let visited = buffer.visited_file_modtime();
            // Tramp reports remote modification times with one-second
            // resolution; a same-second rewrite looks unchanged.
            let unchanged = if remote_visit {
                modtimes_equal_whole_seconds(&visited, &current)
            } else {
                visited == current
            };
            Ok(if unchanged { Value::T } else { Value::Nil })
        }
        "set-visited-file-modtime" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let modtime = match args.first() {
                None | Some(Value::Nil) => {
                    if let Some(path) = interp.buffer.file.clone() {
                        file_modtime(&path)?
                    } else {
                        None
                    }
                }
                Some(Value::Integer(0)) => None,
                Some(value) => Some(file_modtime_from_value(interp, value)?),
            };
            interp.buffer.set_visited_file_modtime(modtime);
            Ok(Value::T)
        }
        "rename-visited-file" => {
            need_args(name, args, 1)?;
            let mut new_location = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            if Path::new(&new_location).is_dir() {
                let Some(old_location) = interp.buffer.file.clone() else {
                    return Err(LispError::Signal(
                        "Can't rename buffer to a directory file name".into(),
                    ));
                };
                let basename = Path::new(&old_location)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| LispError::Signal("Empty file name".into()))?;
                new_location = Path::new(&new_location)
                    .join(basename)
                    .to_string_lossy()
                    .into_owned();
            }

            if let Some(old_location) = interp.buffer.file.clone()
                && Path::new(&old_location).exists()
            {
                fs::rename(&old_location, &new_location)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
            }

            interp.buffer.file = Some(new_location.clone());
            interp.buffer.file_truename = Some(new_location.clone());
            interp
                .buffer
                .set_visited_file_modtime(file_modtime(&new_location)?);
            Ok(Value::Nil)
        }
        "read-only-mode" => {
            need_arg_range(name, args, 0, 1)?;
            let enabled = match args.first() {
                Some(Value::Integer(n)) => *n > 0,
                Some(value) => value.is_truthy(),
                None => !interp
                    .lookup_var("buffer-read-only", env)
                    .is_some_and(|value| value.is_truthy()),
            };
            let value = if enabled { Value::T } else { Value::Nil };
            interp.set_variable("buffer-read-only", value.clone(), env);
            interp.set_variable("read-only-mode", value, env);
            Ok(Value::Nil)
        }
        "set-buffer-file-coding-system" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let coding = checked_coding_name(interp, &args[0])?;
            let value = coding
                .as_ref()
                .map(|coding| Value::Symbol(coding.clone()))
                .unwrap_or(Value::Nil);
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "buffer-file-coding-system",
                value,
            );
            if !args.get(2).is_some_and(Value::is_truthy) {
                interp.buffer.set_modified();
            }
            Ok(coding.map(Value::Symbol).unwrap_or(Value::Nil))
        }
        "after-insert-file-set-coding" => {
            need_arg_range(name, args, 1, 1)?;
            let coding = interp
                .lookup_var("last-coding-system-used", env)
                .filter(|value| !value.is_nil())
                .and_then(|value| checked_coding_name(interp, &value).ok().flatten())
                .unwrap_or_else(|| {
                    detect_coding_names_for_text(interp, &interp.buffer.buffer_string(), env)
                        .into_iter()
                        .next()
                        .unwrap_or_else(|| "undecided".into())
                });
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "buffer-file-coding-system",
                Value::Symbol(coding.clone()),
            );
            set_last_coding_system_used(interp, &coding, env);
            Ok(Value::Symbol(coding))
        }
        "local" => {
            need_args(name, args, 1)?;
            Ok(Value::list([
                Value::Symbol("emaxx-local-function-place".into()),
                args[0].clone(),
            ]))
        }
        // File-less fallback; GNU nadvice.el's macro takes over once loaded.
        "add-function" if !interp.has_lisp_macro("add-function") => {
            need_arg_range(name, args, 3, 4)?;
            let where_sym = args[0].as_symbol()?;
            let advice = match &args[2] {
                Value::Symbol(symbol) => interp
                    .lookup_function(symbol, env)
                    .unwrap_or(args[2].clone()),
                other => other.clone(),
            };
            match &args[1] {
                Value::Symbol(variable) => {
                    let original = interp.lookup_var(variable, env).unwrap_or(Value::Nil);
                    if let Some(wrapped) = wrap_advice(where_sym, original, advice) {
                        interp.set_global_binding(variable, wrapped);
                    }
                    Ok(Value::Nil)
                }
                place_value => {
                    let place = place_value.to_vec()?;
                    if place.len() == 2
                        && place[0] == Value::Symbol("emaxx-local-function-place".into())
                        && let Value::Symbol(variable) = &place[1]
                    {
                        interp.set_buffer_local_value(interp.current_buffer_id(), variable, advice);
                        return Ok(Value::Nil);
                    }
                    Err(LispError::Signal("Unsupported add-function place".into()))
                }
            }
        }
        "fundamental-mode" => {
            need_args(name, args, 0)?;
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "major-mode",
                Value::Symbol("fundamental-mode".into()),
            );
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "mode-name",
                Value::String("Fundamental".into()),
            );
            Ok(Value::Nil)
        }
        "prog-mode" => {
            need_args(name, args, 0)?;
            derived_mode_set_parent(interp, "prog-mode", Some("fundamental-mode"));
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "major-mode",
                Value::Symbol("prog-mode".into()),
            );
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "mode-name",
                Value::String("Prog".into()),
            );
            Ok(Value::Nil)
        }
        "emacs-lisp-mode" => {
            need_args(name, args, 0)?;
            let buffer_id = interp.current_buffer_id();
            interp.set_buffer_local_value(
                buffer_id,
                "major-mode",
                Value::Symbol("emacs-lisp-mode".into()),
            );
            interp.set_buffer_local_value(
                buffer_id,
                "mode-name",
                Value::String("Emacs-Lisp".into()),
            );
            interp.set_buffer_local_value(buffer_id, "comment-start", Value::String(";".into()));
            interp.set_buffer_local_value(buffer_id, "comment-end", Value::String(String::new()));
            interp.set_buffer_local_value(buffer_id, "comment-use-syntax", Value::T);
            interp.set_buffer_local_value(buffer_id, "comment-add", Value::Integer(1));
            // GNU lisp-mode-variables comment settings.
            interp.set_buffer_local_value(
                buffer_id,
                "comment-start-skip",
                Value::String(";+ *".into()),
            );
            interp.set_buffer_local_value(
                buffer_id,
                "comment-indent-function",
                Value::Symbol("lisp-comment-indent".into()),
            );
            interp.set_buffer_local_value(buffer_id, "comment-column", Value::Integer(40));
            interp.set_buffer_local_value(
                buffer_id,
                "fill-paragraph-function",
                Value::Symbol("lisp-fill-paragraph".into()),
            );
            // GNU lisp-mode-variables installs the lisp line indenter
            // and makes sexp scanning skip comments.
            interp.set_buffer_local_value(
                buffer_id,
                "indent-line-function",
                Value::Symbol("lisp-indent-line".into()),
            );
            interp.set_buffer_local_value(
                buffer_id,
                "indent-region-function",
                Value::Symbol("lisp-indent-region".into()),
            );
            interp.set_buffer_local_value(buffer_id, "parse-sexp-ignore-comments", Value::T);
            interp.set_buffer_local_value(buffer_id, "font-lock-defaults", Value::T);
            // GNU lisp-mode-variables outline settings (lisp-mnt.el's
            // lm-section-end relies on these in emacs-lisp-mode buffers).
            interp.set_buffer_local_value(
                buffer_id,
                "outline-regexp",
                Value::String(
                    ";;;;* [^ \t\n]\\|(\\|\\(^;;;###\\(\\([-[:alnum:]]+?\\)-\\)?\\(autoload\\)\\)"
                        .into(),
                ),
            );
            interp.set_buffer_local_value(
                buffer_id,
                "outline-level",
                Value::Symbol("lisp-outline-level".into()),
            );
            let Value::CharTable(syntax_table_id) =
                interp.make_char_table(Some("syntax-table".into()), Value::Nil)
            else {
                unreachable!("make_char_table returns a char-table");
            };
            // A fresh per-buffer table inheriting the static GNU
            // lisp-data-mode-syntax-table (built in Interpreter::new).
            interp
                .set_char_table_parent(syntax_table_id, Some(interp.lisp_data_syntax_table_id()))?;
            interp.set_current_syntax_table(syntax_table_id);
            Ok(Value::Nil)
        }
        "special-mode" => {
            need_args(name, args, 0)?;
            // GNU special-mode derives from nil, so its body starts with
            // kill-all-local-variables (running change-major-mode-hook;
            // tar-mode re-entry relies on this to unswap its data buffer).
            let _ = call_named_function(interp, "kill-all-local-variables", &[], env)?;
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "major-mode",
                Value::Symbol("special-mode".into()),
            );
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "mode-name",
                Value::String("Special".into()),
            );
            interp.set_variable("buffer-read-only", Value::T, env);
            Ok(Value::Nil)
        }
        "normal-mode" => {
            need_arg_range(name, args, 0, 1)?;
            let _ = call_named_function(interp, "kill-all-local-variables", &[], env)?;
            let source = interp.buffer.full_buffer_string();
            if file_local_variable_is_truthy(&source, "no-byte-compile") {
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "no-byte-compile",
                    Value::T,
                );
            }
            if let Some(path) = current_buffer_file(interp)
                && let Some(mode) = modes::auto_mode_function_for_file_name(interp, env, path)?
            {
                let _ = call_major_or_named_mode(interp, &mode, env)?;
            }
            Ok(Value::Nil)
        }
        "find-file-noselect" => {
            need_arg_range(name, args, 1, 4)?;
            let requested = string_text(&args[0])?;
            let remote_prefix = parse_remote_file_name(&requested).map(|remote| remote.prefix);
            let path = resolve_file_name_in_env(interp, env, &requested);
            let literal = args.get(2).is_some_and(Value::is_truthy);
            if !literal && fs::metadata(&path).is_ok_and(|metadata| metadata.is_dir()) {
                return super::call(interp, "dired-noselect", &[Value::String(path)], env);
            }
            if let Some((id, name)) = interp.find_buffer(&path) {
                if let Some(prefix) = remote_prefix {
                    interp.set_buffer_local_value(
                        id,
                        "emaxx--visited-remote-prefix",
                        Value::String(prefix),
                    );
                }
                return Ok(Value::Buffer(id, name));
            }
            let (id, _) = interp.create_buffer(&path);
            let saved_buffer_id = interp.current_buffer_id();
            interp.switch_to_buffer_id(id)?;
            let result: Result<(), LispError> = (|| {
                let mut mode = modes::auto_mode_function_for_file_name(interp, env, &path)?;
                let file_exists = Path::new(&path).exists();
                let mut bytes = if file_exists {
                    read_insert_file_bytes(&path, None, None)?
                } else {
                    Vec::new()
                };
                if !literal && should_auto_decompress(interp, env, &path) {
                    bytes = maybe_decompress_file_bytes(&path, bytes)?;
                }
                if !literal && mode.is_none() {
                    mode = modes::auto_mode_function_for_contents(&bytes).map(str::to_string);
                }
                let raw_archive =
                    !literal && matches!(mode.as_deref(), Some("tar-mode" | "archive-mode"));
                let (contents, coding, multibyte) = if literal || raw_archive {
                    (
                        decode_raw_text_bytes(&bytes),
                        "no-conversion".to_string(),
                        false,
                    )
                } else {
                    let (text, coding) = decode_file_contents(interp, env, &bytes, false)?;
                    (text, coding, true)
                };
                interp.buffer = crate::buffer::Buffer::from_text(&path, &contents);
                interp.buffer.set_multibyte(multibyte);
                interp.buffer.file = Some(path.clone());
                interp.buffer.file_truename = Some(path.clone());
                interp.buffer.set_unmodified();
                interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
                if let Some(parent) = Path::new(&path).parent() {
                    interp.set_buffer_local_value(
                        interp.current_buffer_id(),
                        "default-directory",
                        Value::String(file_name_as_directory(&parent.to_string_lossy())),
                    );
                }
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "buffer-file-coding-system",
                    Value::Symbol(coding.clone()),
                );
                set_last_coding_system_used(interp, &coding, env);
                if literal {
                    let _ = call_named_function(interp, "fundamental-mode", &[], env)?;
                    interp.set_buffer_local_value(
                        interp.current_buffer_id(),
                        "find-file-literally",
                        Value::T,
                    );
                } else if let Some(mode) = mode.as_deref() {
                    let _ = call_major_or_named_mode(interp, mode, env)?;
                } else {
                    let _ = call_named_function(interp, "normal-mode", &[Value::T], env)?;
                }
                if file_exists && !file_writable_p(&path) {
                    let buffer_id = interp.current_buffer_id();
                    interp.set_buffer_local_value(buffer_id, "buffer-read-only", Value::T);
                    interp.set_buffer_local_value(buffer_id, "read-only-mode", Value::T);
                }
                if !interp
                    .lookup_var("semantic-init-hook", env)
                    .is_some_and(|value| value.is_nil())
                {
                    run_named_hooks(
                        interp,
                        "find-file-hook",
                        env,
                        Some(interp.current_buffer_id()),
                    )?;
                }
                Ok(())
            })();
            // After mode setup: `kill-all-local-variables' would have wiped
            // an earlier assignment.
            if let Some(prefix) = remote_prefix {
                interp.set_buffer_local_value(
                    id,
                    "emaxx--visited-remote-prefix",
                    Value::String(prefix),
                );
            }
            let _ = interp.switch_to_buffer_id(saved_buffer_id);
            result?;
            Ok(Value::Buffer(id, path))
        }
        "find-file-literally" => {
            need_args(name, args, 1)?;
            super::call(
                interp,
                "find-file-noselect",
                &[args[0].clone(), Value::Nil, Value::T],
                env,
            )
        }
        "find-buffer-visiting" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            Ok(interp
                .find_buffer(&path)
                .map(|(id, name)| Value::Buffer(id, name))
                .unwrap_or(Value::Nil))
        }
        "find-file" => {
            need_arg_range(name, args, 1, 2)?;
            let requested = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            if args.get(1).is_some_and(Value::is_truthy)
                && file_name_nondirectory(&requested) == "*"
                && let Some(directory) = Path::new(&requested).parent()
            {
                let mut entries = fs::read_dir(directory)
                    .map_err(|error| file_output_error(&requested, &error))?
                    .filter_map(Result::ok)
                    .map(|entry| entry.path().to_string_lossy().into_owned())
                    .collect::<Vec<_>>();
                entries.sort();
                let mut buffers = Vec::new();
                for entry in entries {
                    let buffer =
                        super::call(interp, "find-file-noselect", &[Value::String(entry)], env)?;
                    buffers.push(buffer);
                }
                return Ok(Value::list(buffers));
            }
            let buffer = super::call(interp, "find-file-noselect", args, env)?;
            let id = interp.resolve_buffer_id(&buffer)?;
            interp.switch_to_buffer_id(id)?;
            Ok(buffer)
        }
        "get-file-buffer" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            Ok(interp
                .find_buffer(&path)
                .map(|(id, name)| Value::Buffer(id, name))
                .unwrap_or(Value::Nil))
        }
        "file-has-changed-p" => {
            need_arg_range(name, args, 1, 2)?;
            let path = directory_file_name(&resolve_file_name_in_env(
                interp,
                env,
                &string_text(&args[0])?,
            ));
            let key = file_change_cache_key(&path, args.get(1))?;
            let current = file_change_cache_value(&path)?;
            let cache = FILE_CHANGE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            let mut cache = cache
                .lock()
                .map_err(|_| LispError::Signal("file change cache poisoned".into()))?;
            let cached = cache.get(&key).cloned().flatten();
            if current != cached {
                cache.insert(key, current);
                Ok(Value::T)
            } else {
                Ok(Value::Nil)
            }
        }
        "expand-file-name" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let path = string_text(&args[0])?;
            let base = match args.get(1) {
                Some(value) if !value.is_nil() => Some(string_text(value)?),
                _ => interp
                    .lookup_var("default-directory", env)
                    .and_then(|value| string_like(&value).map(|string| string.text)),
            };
            Ok(Value::String(expand_file_name_runtime(
                interp,
                env,
                &path,
                base.as_deref(),
            )?))
        }
        "locate-file" => {
            need_arg_range(name, args, 2, 4)?;
            let filename = string_text(&args[0])?;
            let paths = args[1].to_vec()?;
            let predicate = args.get(3).filter(|value| !value.is_nil()).cloned();
            let suffixes = match args.get(2) {
                Some(Value::Nil) | None => vec![String::new()],
                Some(Value::String(_) | Value::StringObject(_)) => vec![string_text(&args[2])?],
                Some(value) => value
                    .to_vec()?
                    .into_iter()
                    .map(|item| string_text(&item))
                    .collect::<Result<Vec<_>, _>>()?,
            };
            for directory in paths {
                let directory = string_text(&directory)?;
                for suffix in &suffixes {
                    let candidate = expand_file_name_runtime(
                        interp,
                        env,
                        &format!("{filename}{suffix}"),
                        Some(&directory),
                    )?;
                    if locate_file_candidate_matches(interp, predicate.as_ref(), &candidate, env)? {
                        return Ok(Value::String(candidate));
                    }
                }
            }
            Ok(Value::Nil)
        }
        "file-relative-name" => {
            need_arg_range(name, args, 1, 2)?;
            let file = string_text(&args[0])?;
            let directory = match args.get(1) {
                Some(value) if !value.is_nil() => string_text(value)?,
                _ => interp
                    .lookup_var("default-directory", env)
                    .and_then(|value| string_like(&value).map(|string| string.text))
                    .unwrap_or_else(default_directory),
            };
            let file = expand_file_name_runtime(interp, env, &file, None)?;
            let directory = expand_file_name_runtime(interp, env, &directory, None)?;
            Ok(Value::String(file_relative_name(&file, &directory)))
        }
        "jka-compr-get-compression-info" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            Ok(if path_is_gzip_encoded(&path) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "substitute-in-file-name" => {
            need_args(name, args, 1)?;
            Ok(Value::String(substitute_in_file_name(&string_text(
                &args[0],
            )?)))
        }
        "file-name-directory" => {
            need_args(name, args, 1)?;
            Ok(file_name_directory(&string_text(&args[0])?)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "file-name-nondirectory" => {
            need_args(name, args, 1)?;
            Ok(Value::String(file_name_nondirectory(&string_text(
                &args[0],
            )?)))
        }
        "file-name-split" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            let parts = Path::new(&path)
                .components()
                .map(|component| match component {
                    Component::Prefix(prefix) => prefix.as_os_str().to_string_lossy().into(),
                    Component::RootDir => std::path::MAIN_SEPARATOR.to_string(),
                    Component::CurDir => ".".into(),
                    Component::ParentDir => "..".into(),
                    Component::Normal(part) => part.to_string_lossy().into(),
                })
                .map(Value::String);
            Ok(Value::list(parts))
        }
        "file-name-sans-extension" => {
            need_args(name, args, 1)?;
            Ok(Value::String(file_name_sans_extension(&string_text(
                &args[0],
            )?)))
        }
        "file-name-sans-versions" => {
            need_arg_range(name, args, 1, 2)?;
            let mut file = string_text(&args[0])?;
            if !args.get(1).is_some_and(Value::is_truthy)
                && let Some(index) = file.rfind(".~")
                && file.ends_with('~')
                && file[index + 2..file.len() - 1]
                    .chars()
                    .all(|ch| ch.is_ascii_digit())
            {
                file.truncate(index);
            }
            Ok(Value::String(file))
        }
        "file-name-base" => {
            need_args(name, args, 1)?;
            let nondirectory = file_name_nondirectory(&string_text(&args[0])?);
            Ok(Value::String(file_name_sans_extension(&nondirectory)))
        }
        "file-name-extension" => {
            need_arg_range(name, args, 1, 2)?;
            let extension = file_name_extension(
                &string_text(&args[0])?,
                args.get(1).is_some_and(Value::is_truthy),
            );
            Ok(extension.map(Value::String).unwrap_or(Value::Nil))
        }
        "file-name-as-directory" => {
            need_args(name, args, 1)?;
            Ok(Value::String(file_name_as_directory(&string_text(
                &args[0],
            )?)))
        }
        "directory-file-name" => {
            need_args(name, args, 1)?;
            Ok(Value::String(directory_file_name(&string_text(&args[0])?)))
        }
        "directory-name-p" => {
            need_args(name, args, 1)?;
            Ok(if directory_name_p(&string_text(&args[0])?) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-name-absolute-p" => {
            need_args(name, args, 1)?;
            Ok(if file_name_absolute_p(&string_text(&args[0])?) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-name-case-insensitive-p" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "file-name-concat" => Ok(Value::String(file_name_concat(
            &args
                .iter()
                .filter(|value| !value.is_nil())
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        "file-name-unquote" => {
            need_args(name, args, 1)?;
            Ok(Value::String(string_text(&args[0])?))
        }
        "file-local-name" => {
            need_args(name, args, 1)?;
            let file = string_text(&args[0])?;
            Ok(parse_remote_file_name(&file)
                .map(|remote| Value::String(resolved_remote_localname(&remote)))
                .unwrap_or(Value::String(file)))
        }
        "file-local-copy" => {
            need_args(name, args, 1)?;
            let file = string_text(&args[0])?;
            if parse_remote_file_name(&file).is_some_and(|remote| remote.method == "mock") {
                mock_file_local_copy(&file).map(Value::String)
            } else {
                Ok(Value::Nil)
            }
        }
        "file-remote-p" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let file = string_text(&args[0])?;
            let Some(remote) = parse_remote_file_name(&file) else {
                return Ok(Value::Nil);
            };
            let identification = args.get(1).cloned().unwrap_or(Value::Nil);
            let result = match identification.as_symbol().ok() {
                None | Some("nil") | Some("t") => Value::String(remote.prefix),
                Some("method") => Value::String(remote.method),
                Some("user") => remote.user.map(Value::String).unwrap_or(Value::Nil),
                Some("host") => Value::String(remote.host),
                Some("localname") => Value::String(resolved_remote_localname(&remote)),
                _ => Value::String(remote.prefix),
            };
            Ok(result)
        }
        "file-expand-wildcards" => {
            need_arg_range(name, args, 1, 3)?;
            let pattern = string_text(&args[0])?;
            let full = args.get(1).is_some_and(Value::is_truthy);
            let expanded = match expand_simple_wildcard_paths(&pattern) {
                Ok(paths) => paths,
                Err(_) => return Ok(Value::Nil),
            };
            let matches = expanded
                .into_iter()
                .filter(|path| Path::new(path).exists())
                .map(|path| {
                    if full {
                        Value::String(resolve_file_name_in_env(interp, env, &path))
                    } else {
                        Value::String(path)
                    }
                })
                .collect::<Vec<_>>();
            Ok(Value::list(matches))
        }
        "find-file-name-handler" => {
            need_args(name, args, 2)?;
            let file = string_text(&args[0])?;
            let operation = args[1].as_symbol().ok();
            if operation == Some("file-local-copy")
                && parse_remote_file_name(&file).is_some_and(|remote| remote.method == "mock")
            {
                Ok(Value::Symbol("emaxx-mock-file-name-handler".into()))
            } else {
                Ok(Value::Nil)
            }
        }
        "emaxx-mock-file-name-handler" => {
            need_args(name, args, 2)?;
            let operation = args[0].as_symbol()?;
            let file = string_text(&args[1])?;
            match operation {
                "file-local-copy" => mock_file_local_copy(&file).map(Value::String),
                _ => Ok(Value::Nil),
            }
        }
        "dired-noselect" => {
            need_arg_range(name, args, 1, 2)?;
            let requested = string_text(&args[0])?;
            let remote_prefix = parse_remote_file_name(&requested).map(|remote| remote.prefix);
            let directory = resolve_file_name_in_env(interp, env, &requested);
            let buffer_name = dired_buffer_name(&directory);
            let (buffer_id, buffer_name) = interp
                .find_buffer(&buffer_name)
                .unwrap_or_else(|| interp.create_buffer(&buffer_name));
            let saved_buffer_id = interp.current_buffer_id();
            interp.switch_to_buffer_id(buffer_id)?;
            initialize_dired_buffer(interp, &buffer_name, &directory)?;
            if let Some(prefix) = remote_prefix {
                interp.set_buffer_local_value(
                    buffer_id,
                    "emaxx--visited-remote-prefix",
                    Value::String(prefix),
                );
            }
            interp.switch_to_buffer_id(saved_buffer_id)?;
            Ok(Value::Buffer(buffer_id, buffer_name))
        }
        "dired-revert" | "emaxx-dired-revert" => {
            need_arg_range(name, args, 0, 4)?;
            let directory = interp
                .buffer_local_value(interp.current_buffer_id(), "dired-directory")
                .and_then(|value| string_like(&value).map(|string| string.text))
                .ok_or_else(|| LispError::Signal("Current buffer is not a Dired buffer".into()))?;
            let buffer_name = interp.buffer.name.clone();
            initialize_dired_buffer(interp, &buffer_name, &directory)?;
            if let Some(first_entry) = fs::read_dir(&directory).ok().and_then(|entries| {
                let mut names = entries
                    .filter_map(Result::ok)
                    .map(|entry| entry.file_name().to_string_lossy().into_owned())
                    .collect::<Vec<_>>();
                names.sort();
                names.into_iter().next()
            }) {
                crate::lisp::primitives::goto_dired_listing_entry(interp, &first_entry);
            }
            Ok(Value::Nil)
        }
        "dired-buffer-stale-p" => {
            need_arg_range(name, args, 0, 1)?;
            let Some(directory) = interp
                .buffer_local_value(interp.current_buffer_id(), "dired-directory")
                .and_then(|value| string_like(&value).map(|string| string.text))
            else {
                return Ok(Value::Nil);
            };
            if !interp
                .lookup_var("buffer-read-only", env)
                .unwrap_or(Value::Nil)
                .is_truthy()
            {
                return Ok(Value::Nil);
            }
            // A remote dired buffer reads its listing through Tramp's
            // file-name cache: changes on disk stay invisible to the stale
            // check until the cache is flushed.
            if interp
                .buffer_local_value(interp.current_buffer_id(), "emaxx--visited-remote-prefix")
                .is_some_and(|value| value.is_truthy())
            {
                return Ok(Value::Nil);
            }
            let current = file_modtime(&directory)?;
            let listing_changed = dired_listing_for_directory(&directory)
                .map(|listing| listing != interp.buffer.buffer_string())
                .unwrap_or(false);
            Ok(
                if interp.buffer.visited_file_modtime() != current || listing_changed {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "shell-quote-argument" => {
            need_args(name, args, 1)?;
            let argument = string_text(&args[0])?;
            Ok(Value::String(shell_quote_argument(&argument)))
        }
        "locate-user-emacs-file" => {
            need_arg_range(name, args, 1, 2)?;
            let user_emacs_directory = interp
                .lookup_var("user-emacs-directory", env)
                .and_then(|value| string_like(&value).map(|string| string.text))
                .unwrap_or_else(default_directory);
            let resolved = match &args[0] {
                Value::Nil => {
                    return Err(LispError::TypeError("stringp".into(), args[0].type_name()));
                }
                Value::Cons(_, _) => {
                    let names = args[0]
                        .to_vec()?
                        .into_iter()
                        .map(|value| string_text(&value))
                        .collect::<Result<Vec<_>, _>>()?;
                    let Some(default_name) = names.first() else {
                        return Err(LispError::TypeError("consp".into(), args[0].type_name()));
                    };
                    names
                        .iter()
                        .rev()
                        .map(|name| expand_file_name(name, Some(&user_emacs_directory)))
                        .find(|path| Path::new(path).exists())
                        .unwrap_or_else(|| {
                            expand_file_name(default_name, Some(&user_emacs_directory))
                        })
                }
                _ => expand_file_name(&string_text(&args[0])?, Some(&user_emacs_directory)),
            };
            if let Some(old_name) = args.get(1).filter(|value| !value.is_nil()) {
                let home = expand_home_prefix("~");
                let legacy = expand_file_name(&string_text(old_name)?, Some(&home));
                if !file_readable_p(&resolved) && file_readable_p(&legacy) {
                    return Ok(Value::String(legacy));
                }
            }
            Ok(Value::String(resolved))
        }
        "ert-resource-directory" => {
            need_args(name, args, 0)?;
            Ok(ert_resource_directory(interp)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "ert-resource-file" => {
            need_args(name, args, 1)?;
            let file = string_text(&args[0])?;
            let Some(directory) = ert_resource_directory(interp) else {
                return Err(LispError::Signal(
                    "Cannot determine the current ERT resource directory".into(),
                ));
            };
            Ok(Value::String(expand_file_name(&file, Some(&directory))))
        }
        "ert-gcc-is-clang-p" => {
            if !args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let output = match Command::new("gcc").arg("--version").output() {
                Ok(output) => output,
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Value::Nil),
                Err(error) => return Err(LispError::Signal(error.to_string())),
            };
            let text = String::from_utf8_lossy(&output.stdout);
            Ok(apple_gcc_version_match(&text)
                .map(|index| Value::Integer(index as i64))
                .unwrap_or(Value::Nil))
        }
        "ert-fail" => {
            need_args(name, args, 1)?;
            let message = match &args[0] {
                Value::String(message) => message.clone(),
                Value::StringObject(state) => state.borrow().text.clone(),
                value => value.to_string(),
            };
            Err(LispError::ErtTestFailed(message))
        }
        "ert-pass" => {
            need_args(name, args, 0)?;
            Err(LispError::Throw(
                Value::Symbol("ert--pass".into()),
                Value::Nil,
            ))
        }
        "locate-library" => {
            if args.is_empty() || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let library = string_text(&args[0])?;
            // Third argument: an explicit search PATH overriding `load-path'
            // (erc-d's subprocess launcher locates its own library this way).
            if let Some(path_arg) = args.get(2).filter(|value| value.is_truthy()) {
                let nosuffix = args.get(1).is_some_and(Value::is_truthy);
                let has_ext = library.ends_with(".el") || library.ends_with(".elc");
                // GNU searches load-suffixes (.elc before .el) unless the
                // NOSUFFIX arg is set or the name already carries one.  The
                // package tests compile to .elc and gzip the .el, so the
                // .elc must win — matching real locate-library.
                let suffixes: &[&str] = if nosuffix || has_ext {
                    &[""]
                } else {
                    &[".elc", ".el"]
                };
                for dir in path_arg.to_vec()? {
                    let Ok(dir_text) = string_text(&dir) else {
                        continue;
                    };
                    for suffix in suffixes {
                        let candidate =
                            std::path::Path::new(&dir_text).join(format!("{library}{suffix}"));
                        if candidate.is_file() {
                            return Ok(Value::String(candidate.display().to_string()));
                        }
                    }
                }
                return Ok(Value::Nil);
            }
            Ok(resolve_load_target_in_env(interp, &library, env)
                .map(|path| Value::String(path.display().to_string()))
                .unwrap_or(Value::Nil))
        }
        "get-load-suffixes" => {
            need_args(name, args, 0)?;
            get_load_suffixes_value(interp, env)
        }
        "load" => {
            if args.is_empty() || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let target = string_text(&args[0])?;
            let noerror = args.get(1).is_some_and(Value::is_truthy);
            let Some(path) = resolve_load_target_in_env(interp, &target, env) else {
                if noerror {
                    return Ok(Value::Nil);
                }
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("file-missing".into()),
                    Value::String("Cannot open load file".into()),
                    Value::String("No such file or directory".into()),
                    Value::String(target),
                ])));
            };
            crate::lisp::load_file_strict(interp, &path)?;
            Ok(Value::T)
        }
        "load-file" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            let path_buf = std::path::PathBuf::from(&path);
            if !path_buf.is_file() {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("file-missing".into()),
                    Value::String("Cannot open load file".into()),
                    Value::String("No such file or directory".into()),
                    Value::String(path),
                ])));
            }
            crate::lisp::load_file_strict(interp, &path_buf)?;
            Ok(Value::T)
        }
        "locate-file-internal" => {
            need_args(name, args, 4)?;
            locate_file_internal(interp, &args[0], &args[1], &args[2], &args[3], env)
        }
        "directory-files" => {
            need_arg_range(name, args, 1, 5)?;
            let directory = string_text(&args[0])?;
            let full = args.get(1).is_some_and(Value::is_truthy);
            let matcher = args.get(2).filter(|value| !value.is_nil());
            let nosort = args.get(3).is_some_and(Value::is_truthy);
            let count = args
                .get(4)
                .filter(|value| !value.is_nil())
                .map(Value::as_integer)
                .transpose()?
                .map(|value| value.max(0) as usize);
            directory_files(interp, &directory, full, matcher, nosort, count, env)
        }
        "directory-files-and-attributes" => {
            need_arg_range(name, args, 1, 6)?;
            let directory = string_text(&args[0])?;
            let full = args.get(1).is_some_and(Value::is_truthy);
            let id_format = args.get(4).cloned().unwrap_or(Value::Nil);
            let mut directory_files_args = vec![
                args[0].clone(),
                args.get(1).cloned().unwrap_or(Value::Nil),
                args.get(2).cloned().unwrap_or(Value::Nil),
                args.get(3).cloned().unwrap_or(Value::Nil),
            ];
            if let Some(count) = args.get(5) {
                directory_files_args.push(count.clone());
            }
            let file_names = super::call(interp, "directory-files", &directory_files_args, env)?;
            let entries = file_names
                .to_vec()?
                .into_iter()
                .map(|name_value| {
                    let name_text = string_text(&name_value)?;
                    let attribute_path = if full {
                        name_text.clone()
                    } else {
                        Path::new(&resolve_file_name_in_env(interp, env, &directory))
                            .join(&name_text)
                            .display()
                            .to_string()
                    };
                    let attributes = super::call(
                        interp,
                        "file-attributes",
                        &[Value::String(attribute_path), id_format.clone()],
                        env,
                    )?;
                    Ok(Value::cons(name_value, attributes))
                })
                .collect::<Result<Vec<_>, LispError>>()?;
            Ok(Value::list(entries))
        }
        "directory-empty-p" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            let Ok(mut entries) = fs::read_dir(path) else {
                return Ok(Value::Nil);
            };
            Ok(if entries.next().is_none() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-directory-p" | "file-accessible-directory-p" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            Ok(
                if fs::metadata(&path)
                    .map(|metadata| metadata.is_dir())
                    .unwrap_or(false)
                    && (name == "file-directory-p" || file_readable_p(&path))
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "file-in-directory-p" => {
            need_args(name, args, 2)?;
            let file = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&file)?;
            let dir = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
            validate_file_name(&dir)?;
            let in_dir = fs::canonicalize(&file)
                .and_then(|file| fs::canonicalize(&dir).map(|dir| file.starts_with(dir)))
                .unwrap_or(false);
            Ok(if in_dir { Value::T } else { Value::Nil })
        }
        "file-readable-p" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            Ok(if file_readable_p(&path) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-regular-p" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            Ok(
                if fs::metadata(&path)
                    .map(|metadata| metadata.is_file())
                    .unwrap_or(false)
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "file-writable-p" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            Ok(if file_writable_p(&path) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-exists-p" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            Ok(if fs::metadata(path).is_ok() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-executable-p" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            Ok(if file_executable_p(&path) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "file-attributes" => {
            need_arg_range(name, args, 1, 3)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Value::Nil),
                Err(error) => return Err(LispError::Signal(error.to_string())),
            };
            let file_type = metadata.file_type();
            let type_value = if file_type.is_dir() {
                Value::T
            } else if file_type.is_symlink() {
                fs::read_link(&path)
                    .ok()
                    .map(|target| Value::String(target.to_string_lossy().into_owned()))
                    .unwrap_or(Value::String(path.clone()))
            } else {
                Value::Nil
            };
            let accessed = metadata
                .accessed()
                .ok()
                .map(system_time_list_value)
                .transpose()?
                .unwrap_or(Value::Integer(0));
            let modified = metadata
                .modified()
                .ok()
                .map(system_time_list_value)
                .transpose()?
                .unwrap_or(Value::Integer(0));
            let changed = metadata
                .created()
                .ok()
                .map(system_time_list_value)
                .transpose()?
                .unwrap_or_else(|| modified.clone());
            Ok(Value::list([
                type_value,
                Value::Integer(1),
                Value::Integer(0),
                Value::Integer(0),
                accessed,
                modified,
                changed,
                Value::Integer(metadata.len() as i64),
                Value::String(if file_type.is_dir() {
                    "drwxr-xr-x".into()
                } else {
                    "-rw-r--r--".into()
                }),
                Value::Nil,
                Value::Integer(0),
                Value::Integer(0),
            ]))
        }
        "file-attribute-type" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 0)
        }
        "file-attribute-link-number" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 1)
        }
        "file-attribute-user-id" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 2)
        }
        "file-attribute-group-id" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 3)
        }
        "file-attribute-access-time" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 4)
        }
        "file-attribute-modification-time" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 5)
        }
        "file-attribute-status-change-time" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 6)
        }
        "file-attribute-size" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 7)
        }
        "file-attribute-modes" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 8)
        }
        "file-attribute-inode-number" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 10)
        }
        "file-attribute-device-number" => {
            need_args(name, args, 1)?;
            file_attribute_field(&args[0], 11)
        }
        "file-attribute-file-identifier" => {
            need_args(name, args, 1)?;
            Ok(Value::cons(
                file_attribute_field(&args[0], 10)?,
                file_attribute_field(&args[0], 11)?,
            ))
        }
        "delete-file" => {
            need_arg_range(name, args, 1, 2)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(LispError::Signal(error.to_string())),
            }
            invalidate_file_notify_watches_for_path(&path);
            dispatch_file_notification(interp, env, &path, "deleted")?;
            Ok(Value::Nil)
        }
        "copy-file" => {
            need_arg_range(name, args, 2, 6)?;
            let source = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&source)?;
            let mut target = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
            validate_file_name(&target)?;
            if directory_name_p(&target) {
                target = file_name_concat(&[target, file_name_nondirectory(&source)]);
            }
            if fs::metadata(&target).is_ok() && args.get(2).is_none_or(Value::is_nil) {
                return Err(LispError::Signal(format!("File already exists: {target}")));
            }
            fs::copy(&source, &target).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        "rename-file" => {
            need_arg_range(name, args, 2, 3)?;
            let source = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&source)?;
            let mut target = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
            validate_file_name(&target)?;
            if directory_name_p(&target)
                || fs::metadata(&target).is_ok_and(|metadata| metadata.is_dir())
            {
                target = file_name_concat(&[target, file_name_nondirectory(&source)]);
            }
            if fs::metadata(&target).is_ok() && args.get(2).is_none_or(Value::is_nil) {
                return Err(LispError::Signal(format!("File already exists: {target}")));
            }
            fs::rename(&source, &target).map_err(|error| LispError::Signal(error.to_string()))?;
            invalidate_file_notify_watches_for_path(&source);
            dispatch_file_notification(interp, env, &source, "deleted")?;
            dispatch_file_notification(interp, env, &target, "created")?;
            Ok(Value::Nil)
        }
        "delete-file-internal" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(LispError::Signal(error.to_string())),
            }
            invalidate_file_notify_watches_for_path(&path);
            dispatch_file_notification(interp, env, &path, "deleted")?;
            Ok(Value::Nil)
        }
        "delete-directory" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            if args.get(1).is_some_and(Value::is_truthy) {
                fs::remove_dir_all(path).map_err(|error| LispError::Signal(error.to_string()))?;
            } else {
                fs::remove_dir(path).map_err(|error| LispError::Signal(error.to_string()))?;
            }
            Ok(Value::Nil)
        }
        "delete-directory-internal" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            fs::remove_dir(path).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        "make-directory" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            if args.get(1).is_some_and(Value::is_truthy) {
                fs::create_dir_all(&path).map_err(|error| LispError::Signal(error.to_string()))?;
            } else {
                fs::create_dir(&path).map_err(|error| LispError::Signal(error.to_string()))?;
            }
            if interp
                .lookup_var("dired-auto-revert-buffer", env)
                .is_some_and(|value| value.is_truthy())
            {
                refresh_current_dired_buffer_for_path(interp, &path, env)?;
                interp.buffer.goto_char(interp.buffer.point_max());
            }
            Ok(Value::Nil)
        }
        "make-empty-file" => {
            need_arg_range(name, args, 1, 2)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            let create_parents = args.get(1).is_some_and(Value::is_truthy);
            if !create_parents && fs::metadata(&path).is_ok() {
                return Err(LispError::SignalValue(file_error_with_detail_value(
                    "File exists",
                    "File exists",
                    &path,
                )));
            }
            if create_parents
                && let Some(parent) = Path::new(&path).parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent).map_err(|error| file_output_error(&path, &error))?;
            }
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .map_err(|error| file_output_error(&path, &error))?;
            Ok(Value::Nil)
        }
        "mkdir" => super::call(interp, "make-directory", args, env),
        "make-directory-internal" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            fs::create_dir(path).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        "make-temp-file" => {
            need_arg_range(name, args, 1, 4)?;
            let prefix = string_text(&args[0])?;
            let dir_flag = args.get(1).cloned().unwrap_or(Value::Nil);
            let suffix = args.get(2).cloned().unwrap_or(Value::String(String::new()));
            let suffix = if suffix.is_nil() {
                Value::String(String::new())
            } else {
                suffix
            };
            let suffix_text = string_text(&suffix)?;
            // A relative prefix expands against `temporary-file-directory'.
            let prefix = if std::path::Path::new(&prefix).is_absolute()
                || parse_remote_file_name(&prefix).is_some()
            {
                prefix
            } else {
                let temp_dir = interp
                    .lookup_var("temporary-file-directory", env)
                    .and_then(|value| string_text(&value).ok())
                    .unwrap_or_else(|| std::env::temp_dir().display().to_string());
                format!("{}{prefix}", file_name_as_directory(&temp_dir))
            };
            // A remote prefix creates the file at the resolved location but
            // the returned name keeps the remote prefix, like Tramp.
            let remote_prefix = parse_remote_file_name(&prefix).map(|remote| remote.prefix);
            let prefix_path = if let Some(remote) = parse_remote_file_name(&prefix) {
                remote.localname
            } else {
                prefix
            };
            let created =
                make_temp_file_internal(&prefix_path, &dir_flag, &suffix_text, args.get(3))?;
            Ok(Value::String(match remote_prefix {
                Some(remote) => format!("{remote}{created}"),
                None => created,
            }))
        }
        "make-temp-file-internal" => {
            need_args(name, args, 4)?;
            let prefix = string_text(&args[0])?;
            let suffix = string_text(&args[2])?;
            validate_file_name(&prefix)?;
            validate_file_name(&suffix)?;
            Ok(Value::String(make_temp_file_internal(
                &prefix,
                &args[1],
                &suffix,
                args.get(3),
            )?))
        }
        "file-locked-p" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            file_locked_p(&path)
        }
        "write-region" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            write_region_value(interp, args, env)
        }
        "write-file" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            write_file_value(interp, args, env)
        }
        "kqueue-add-watch" => {
            need_args(name, args, 3)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            let descriptor =
                FILE_NOTIFY_DESCRIPTOR_COUNTER.fetch_add(1, AtomicOrdering::Relaxed) as i64;
            active_file_notify_descriptors()
                .lock()
                .map_err(|_| LispError::Signal("file notify descriptor set poisoned".into()))?
                .insert(descriptor);
            // Watches taken from a remotely visited buffer model Tramp's
            // gio monitors: they outlive deletions of the watched file, so
            // they get no local path registration to invalidate.
            let remote_watch = parse_remote_file_name(&string_text(&args[0])?).is_some()
                || interp
                    .buffer_local_value(interp.current_buffer_id(), "emaxx--visited-remote-prefix")
                    .is_some_and(|value| value.is_truthy());
            if !remote_watch {
                file_notify_watched_paths()
                    .lock()
                    .map_err(|_| LispError::Signal("file notify watch paths poisoned".into()))?
                    .insert(descriptor, path);
            }
            Ok(Value::Integer(descriptor))
        }
        "kqueue-rm-watch" => {
            need_args(name, args, 1)?;
            let descriptor = args[0].as_integer()?;
            active_file_notify_descriptors()
                .lock()
                .map_err(|_| LispError::Signal("file notify descriptor set poisoned".into()))?
                .remove(&descriptor);
            file_notify_watched_paths()
                .lock()
                .map_err(|_| LispError::Signal("file notify watch paths poisoned".into()))?
                .remove(&descriptor);
            Ok(Value::Nil)
        }
        "kqueue-valid-p" => {
            need_args(name, args, 1)?;
            let descriptor = args[0].as_integer()?;
            let active = active_file_notify_descriptors()
                .lock()
                .map_err(|_| LispError::Signal("file notify descriptor set poisoned".into()))?
                .contains(&descriptor);
            Ok(if active { Value::T } else { Value::Nil })
        }
        "default-file-modes" => {
            need_args(name, args, 0)?;
            Ok(interp
                .lookup_var("emaxx-default-file-modes", env)
                .unwrap_or(Value::Integer(0o666)))
        }
        "file-modes" => {
            need_arg_range(name, args, 1, 2)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            #[cfg(unix)]
            {
                let metadata = if args.get(1).is_some_and(Value::is_truthy) {
                    fs::symlink_metadata(&path)
                } else {
                    fs::metadata(&path)
                };
                Ok(metadata
                    .map(|metadata| Value::Integer((metadata.permissions().mode() & 0o7777) as i64))
                    .unwrap_or(Value::Nil))
            }
            #[cfg(not(unix))]
            {
                Ok(if fs::metadata(&path).is_ok() {
                    Value::Integer(0)
                } else {
                    Value::Nil
                })
            }
        }
        "file-modes-number-to-symbolic" => {
            need_arg_range(name, args, 1, 2)?;
            let mode = args[0].as_integer()?;
            let filetype = match args.get(1) {
                None | Some(Value::Nil) => None,
                Some(value) => {
                    let code = value.as_integer()?;
                    Some(
                        u32::try_from(code)
                            .ok()
                            .and_then(char::from_u32)
                            .ok_or_else(|| {
                                LispError::Signal(format!("Invalid character: {code}"))
                            })?,
                    )
                }
            };
            Ok(Value::String(file_modes_number_to_symbolic(mode, filetype)))
        }
        "set-default-file-modes" => {
            need_args(name, args, 1)?;
            let mode = args[0].as_integer()?;
            interp.set_global_binding("emaxx-default-file-modes", Value::Integer(mode));
            Ok(Value::Nil)
        }
        "set-file-modes" => {
            need_args(name, args, 2)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            let mode = args[1].as_integer()?;
            #[cfg(unix)]
            {
                let mut permissions = fs::metadata(&path)
                    .map_err(|error| LispError::Signal(error.to_string()))?
                    .permissions();
                permissions.set_mode(mode as u32);
                fs::set_permissions(&path, permissions)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
            }
            Ok(Value::Nil)
        }
        "file-name-all-completions" => {
            need_args(name, args, 2)?;
            let prefix = string_text(&args[0])?;
            let directory = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
            let mut names: Vec<String> = Vec::new();
            for special in ["./", "../"] {
                if special.starts_with(&prefix) {
                    names.push(special.to_string());
                }
            }
            if let Ok(entries) = std::fs::read_dir(&directory) {
                for entry in entries.flatten() {
                    let file_name = entry.file_name().to_string_lossy().into_owned();
                    if !file_name.starts_with(&prefix) {
                        continue;
                    }
                    // Directories (following symlinks) get a trailing slash.
                    let is_directory = std::fs::metadata(entry.path())
                        .map(|metadata| metadata.is_dir())
                        .unwrap_or(false);
                    names.push(if is_directory {
                        format!("{file_name}/")
                    } else {
                        file_name
                    });
                }
            }
            names.sort();
            Ok(Value::list(names.into_iter().map(Value::String)))
        }
        "set-visited-file-name" => {
            need_arg_range(name, args, 1, 3)?;
            if args[0].is_nil() {
                interp.buffer.file = None;
                interp.buffer.file_truename = None;
                return Ok(Value::Nil);
            }
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            let truename = std::fs::canonicalize(&path)
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|_| path.clone());
            interp.buffer.file = Some(path.clone());
            interp.buffer.file_truename = Some(truename);
            // GNU renames the buffer to the file's base name (uniquely) and
            // marks it modified unless ALONG-WITH-FILE.
            let base = std::path::Path::new(&path)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.clone());
            let _ = super::call(
                interp,
                "rename-buffer",
                &[Value::String(base), Value::T],
                env,
            );
            if !args.get(2).is_some_and(Value::is_truthy) {
                interp.buffer.set_modified();
            }
            Ok(Value::Nil)
        }
        "set-file-times" => {
            need_arg_range(name, args, 1, 3)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            let modified = match args.get(1) {
                None | Some(Value::Nil) => SystemTime::now(),
                Some(value) => file_modtime_from_value(interp, value)?.modified,
            };
            set_file_times_path(&path, modified, args.get(2).is_some_and(Value::is_truthy))?;
            dispatch_file_notification(interp, env, &path, "attribute-changed")?;
            Ok(Value::T)
        }
        "insert-directory" => {
            need_arg_range(name, args, 2, 4)?;
            let file = string_text(&args[0])?;
            let switches = string_text(&args[1])?;
            let program = interp
                .lookup_var("insert-directory-program", env)
                .filter(|value| value.is_truthy())
                .map(|value| string_text(&value))
                .transpose()?
                .unwrap_or_else(|| "ls".into());
            let files = expand_simple_wildcard_paths(&file)?;
            let argv = switches
                .split_whitespace()
                .map(str::to_string)
                .chain(files)
                .collect::<Vec<_>>();
            let process_output = run_external_process(interp, &program, &argv, None, env)?;
            if !process_output.status.success() {
                let stderr = String::from_utf8_lossy(&process_output.stderr)
                    .trim()
                    .to_string();
                return Err(LispError::Signal(if stderr.is_empty() {
                    format!(
                        "{program} exited with status {}",
                        exit_status_code(&process_output.status)
                    )
                } else {
                    stderr
                }));
            }
            let mut output = String::from_utf8_lossy(&process_output.stdout).into_owned();
            // GNU insert-directory-clean: with "--dired" in the switches, ls
            // appends //DIRED//, //DIRED-OPTIONS// (and possibly //SUBDIRED//)
            // marker lines.  The //DIRED// offsets mark filename extents; the
            // marker lines themselves must never remain in the buffer.
            let mut filename_ranges: Vec<(usize, usize)> = Vec::new();
            if switches
                .split_whitespace()
                .any(|switch| switch == "--dired")
            {
                let mut kept_lines = Vec::new();
                for line in output.split_inclusive('\n') {
                    if let Some(rest) = line.strip_prefix("//DIRED//") {
                        let mut numbers = rest
                            .split_whitespace()
                            .filter_map(|token| token.parse::<usize>().ok());
                        while let (Some(start), Some(end)) = (numbers.next(), numbers.next()) {
                            filename_ranges.push((start, end));
                        }
                    } else if !line.starts_with("//DIRED-OPTIONS//")
                        && !line.starts_with("//SUBDIRED//")
                    {
                        kept_lines.push(line);
                    }
                }
                output = kept_lines.concat();
            }
            let mut free_space_offset = 0;
            if let Some(free_space_line) = insert_directory_free_space_line(interp, env, &file)? {
                free_space_offset = free_space_line.chars().count();
                output.insert_str(0, &free_space_line);
            }
            let beg = super::call(interp, "point", &[], env)?.as_integer()? as usize;
            interp.insert_current_buffer(&output);
            for (start, end) in filename_ranges {
                let from = beg + free_space_offset + start;
                let to = beg + free_space_offset + end;
                super::call(
                    interp,
                    "put-text-property",
                    &[
                        Value::Integer(from as i64),
                        Value::Integer(to as i64),
                        Value::Symbol("dired-filename".into()),
                        Value::T,
                    ],
                    env,
                )?;
            }
            Ok(Value::Nil)
        }
        "insert-file-contents" => insert_file_contents(interp, env, args, false),
        "insert-file-contents-literally" => insert_file_contents(interp, env, args, true),
        "get-free-disk-space" => {
            need_args(name, args, 1)?;
            // GNU files.el: format (nth 2 (file-system-info dir)) through
            // `byte-count-to-string-function', nil when unavailable.
            let Ok(info_function) = interp.lookup_function("file-system-info", env) else {
                return Ok(Value::Nil);
            };
            let target = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            let info = interp.call_function_value(
                info_function,
                Some("file-system-info"),
                &[Value::String(target)],
                env,
            )?;
            let Some(available_bytes) = nth_list_item(&info, 2) else {
                return Ok(Value::Nil);
            };
            if available_bytes.is_nil() {
                return Ok(Value::Nil);
            }
            Ok(Value::String(file_size_human_readable(
                interp,
                env,
                &available_bytes,
            )?))
        }
        "file-symlink-p" => {
            need_args(name, args, 1)?;
            let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
            validate_file_name(&path)?;
            let target = fs::symlink_metadata(&path)
                .ok()
                .filter(|metadata| metadata.file_type().is_symlink())
                .and_then(|_| fs::read_link(&path).ok());
            Ok(target
                .map(|path| Value::String(path.to_string_lossy().into_owned()))
                .unwrap_or(Value::Nil))
        }
        "make-symbolic-link" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let target = string_text(&args[0])?;
            let link = string_text(&args[1])?;
            validate_file_name(&target)?;
            validate_file_name(&link)?;
            if args.get(2).is_some_and(Value::is_truthy) && fs::symlink_metadata(&link).is_ok() {
                fs::remove_file(&link).map_err(|error| LispError::Signal(error.to_string()))?;
            }
            #[cfg(unix)]
            {
                symlink(&target, &link).map_err(|error| LispError::Signal(error.to_string()))?;
                Ok(Value::Nil)
            }
            #[cfg(not(unix))]
            {
                Err(LispError::Signal("make-symbolic-link not supported".into()))
            }
        }
        "call-process" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let program = string_text(&args[0])?;
            let input = match args.get(1) {
                Some(value) if !value.is_nil() => match value {
                    Value::Integer(0) => None,
                    _ => {
                        let infile = string_text(value)?;
                        // GNU report_file_error: an unreadable INFILE is a
                        // `file-error' (epg's tty probe catches those).
                        Some(fs::read(&infile).map_err(|error| {
                            LispError::SignalValue(
                                crate::lisp::primitives::file_io::file_error_value(
                                    &error.to_string(),
                                    &infile,
                                ),
                            )
                        })?)
                    }
                },
                _ => None,
            };
            let destination = args.get(2).unwrap_or(&Value::Nil);
            let argv = args
                .get(4..)
                .unwrap_or(&[])
                .iter()
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?;
            let process_output =
                run_external_process(interp, &program, &argv, input.as_deref(), env)?;
            write_process_output(
                interp,
                destination,
                &process_output.stdout,
                &process_output.stderr,
            )?;
            Ok(Value::Integer(exit_status_code(&process_output.status)))
        }
        "make-process" | "make-pipe-process" => {
            let parsed = parse_make_process_args(interp, args)?;
            let runtime = if let Some(command) = parsed.program.as_ref() {
                Some(spawn_persistent_process(
                    interp,
                    command,
                    &parsed.argv,
                    env,
                )?)
            } else {
                None
            };
            let process = interp.create_process(
                parsed.buffer_id,
                parsed.program,
                parsed.argv,
                runtime,
                parsed.name,
            )?;
            let process_id = interp.resolve_process_id(&process)?;
            interp.set_process_filter(process_id, parsed.filter)?;
            interp.set_process_sentinel(process_id, parsed.sentinel);
            interp.set_process_stderr(process_id, parsed.stderr_process_id);
            if let Some((decoding, encoding)) = parsed.coding {
                interp.set_process_coding_system(process_id, decoding, encoding)?;
            }
            Ok(process)
        }
        "start-process" | "start-file-process" => {
            need_arg_range(name, args, 3, usize::MAX)?;
            let buffer_id = process_buffer_target(interp, &args[1])?;
            let program = string_text(&args[2])?;
            let argv = args[3..]
                .iter()
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?;
            let runtime = spawn_persistent_process(interp, &program, &argv, env)?;
            let process_name = string_text(&args[0])?;
            interp.create_process(
                buffer_id,
                Some(program),
                argv,
                Some(runtime),
                Some(process_name),
            )
        }
        "get-buffer-process" => {
            need_arg_range(name, args, 0, 1)?;
            let buffer_id = if let Some(buffer) = args.first() {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            Ok(interp
                .process_value_for_buffer(buffer_id)
                .unwrap_or(Value::Nil))
        }
        "process-buffer" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            Ok(interp
                .process_buffer_id(process_id)
                .and_then(|buffer_id| interp.buffer_identity_value(buffer_id))
                .unwrap_or(Value::Nil))
        }
        "process-mark" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            let marker_id = interp
                .process_mark_id(process_id)
                .ok_or_else(|| LispError::Signal("Invalid process mark".into()))?;
            Ok(Value::Marker(marker_id))
        }
        "process-status" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            interp
                .process_status_value(process_id)
                .ok_or_else(|| LispError::Signal("Invalid process state".into()))
        }
        "process-exit-status" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            interp
                .process_exit_status_value(process_id)
                .ok_or_else(|| LispError::Signal("Invalid process state".into()))
        }
        "process-plist" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            Ok(interp.process_plist_value(process_id).unwrap_or(Value::Nil))
        }
        "set-process-plist" => {
            need_args(name, args, 2)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            interp.set_process_plist_value(process_id, args[1].clone());
            Ok(args[1].clone())
        }
        "process-get" => {
            need_args(name, args, 2)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            let plist = interp.process_plist_value(process_id).unwrap_or(Value::Nil);
            super::call(interp, "plist-get", &[plist, args[1].clone()], env)
        }
        "process-put" => {
            need_args(name, args, 3)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            let plist = interp.process_plist_value(process_id).unwrap_or(Value::Nil);
            let updated = super::call(
                interp,
                "plist-put",
                &[plist, args[1].clone(), args[2].clone()],
                env,
            )?;
            interp.set_process_plist_value(process_id, updated);
            Ok(args[2].clone())
        }
        "process-live-p" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            Ok(if interp.process_is_live(process_id) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "process-attributes" => {
            need_args(name, args, 1)?;
            Ok(process_attributes_value(args[0].as_integer()?))
        }
        "process-coding-system" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            interp.process_coding_system(process_id)
        }
        "set-process-coding-system" => {
            need_arg_range(name, args, 1, 3)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            let decoding = args.get(1).cloned().unwrap_or(Value::Nil);
            let encoding = args.get(2).cloned().unwrap_or(Value::Nil);
            interp.set_process_coding_system(process_id, decoding, encoding)?;
            Ok(Value::T)
        }
        "set-process-filter" => {
            need_args(name, args, 2)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            let filter = if args[1].is_nil() {
                None
            } else {
                Some(args[1].clone())
            };
            interp.set_process_filter(process_id, filter)?;
            Ok(args[1].clone())
        }
        "set-process-sentinel" => {
            need_args(name, args, 2)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            let sentinel = (!args[1].is_nil()).then(|| args[1].clone());
            interp.set_process_sentinel(process_id, sentinel);
            Ok(args[1].clone())
        }
        "set-process-buffer" => {
            need_args(name, args, 2)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            let buffer_id = if args[1].is_nil() {
                None
            } else {
                Some(interp.resolve_buffer_id(&args[1])?)
            };
            interp.set_process_buffer_id(process_id, buffer_id);
            Ok(args[1].clone())
        }
        "process-sentinel" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            Ok(interp.process_sentinel(process_id).unwrap_or(Value::Nil))
        }
        "process-name" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            Ok(interp
                .process_name(process_id)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "process-command" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            Ok(interp
                .process_command_value(process_id)
                .unwrap_or(Value::Nil))
        }
        "process-tty-name" => {
            need_arg_range(name, args, 1, 2)?;
            interp.resolve_process_id(&args[0])?;
            // Emaxx currently implements child processes with pipes, not
            // pseudo-terminals, so neither stdin nor stdout has a tty name.
            Ok(Value::Nil)
        }
        "get-process" => {
            need_args(name, args, 1)?;
            if matches!(&args[0], Value::Record(_)) {
                return Ok(args[0].clone());
            }
            let requested = string_text(&args[0])?;
            Ok(interp
                .find_process_id_by_name(&requested)
                .map(Value::Record)
                .unwrap_or(Value::Nil))
        }
        "process-contact" => {
            // GNU Fprocess_contact: the stored contact plist (p->childp)
            // is t for a real child and is returned as-is no matter the
            // KEY; for a network process KEY t returns the whole plist,
            // KEY nil the (HOST SERVICE) pair, any other KEY a plist_get.
            need_arg_range(name, args, 1, 3)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            let contact = interp
                .process_contact_plist(process_id)
                .unwrap_or(Value::Nil);
            if !matches!(contact, Value::Cons(_, _)) {
                return Ok(contact);
            }
            match args.get(1) {
                Some(Value::T) => Ok(contact),
                None | Some(Value::Nil) => Ok(Value::list([
                    contact_plist_get(&contact, ":host"),
                    contact_plist_get(&contact, ":service"),
                ])),
                Some(key) => Ok(key
                    .as_symbol()
                    .map(|key| contact_plist_get(&contact, key))
                    .unwrap_or(Value::Nil)),
            }
        }
        "make-network-process" => make_network_process(interp, args, env),
        "open-network-stream" => {
            // network-stream.el open-network-stream, plain-connection
            // subset: (NAME BUFFER HOST SERVICE &rest PARAMETERS).
            need_arg_range(name, args, 4, usize::MAX)?;
            let mut network_args = vec![
                Value::Symbol(":name".into()),
                args[0].clone(),
                Value::Symbol(":buffer".into()),
                args[1].clone(),
                Value::Symbol(":host".into()),
                args[2].clone(),
                Value::Symbol(":service".into()),
                args[3].clone(),
            ];
            // Forward :coding / :filter / :sentinel / :nowait if present.
            let mut index = 4;
            while index + 1 < args.len() {
                if let Ok(keyword) = args[index].as_symbol()
                    && matches!(keyword, ":coding" | ":filter" | ":sentinel" | ":nowait")
                {
                    network_args.push(args[index].clone());
                    network_args.push(args[index + 1].clone());
                }
                index += 2;
            }
            make_network_process(interp, &network_args, env)
        }
        "set-network-process-option" => {
            need_arg_range(name, args, 2, 4)?;
            Ok(Value::T)
        }
        "network-interface-list" | "network-interface-info" => Ok(Value::Nil),
        "delete-process" => {
            // PROCESS may be a process, a buffer, the name of a buffer, or
            // nil, defaulting to the current buffer's process.
            let process_value = match args.first() {
                None | Some(Value::Nil) => {
                    let buffer_id = interp.current_buffer_id();
                    interp.process_value_for_buffer(buffer_id)
                }
                Some(process @ Value::Record(_)) => Some(process.clone()),
                Some(other) => {
                    let buffer_id = interp.resolve_buffer_id(other)?;
                    interp.process_value_for_buffer(buffer_id)
                }
            };
            let Some(process_value) = process_value else {
                return Err(wrong_type_argument(
                    "processp",
                    args.first().cloned().unwrap_or(Value::Nil),
                ));
            };
            let process_id = interp.resolve_process_id(&process_value)?;
            delete_process_notifying(interp, process_id, env)?;
            Ok(Value::Nil)
        }
        "set-process-query-on-exit-flag" => {
            need_args(name, args, 2)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            interp.set_process_query_on_exit_flag(process_id, args[1].is_truthy())?;
            Ok(args[1].clone())
        }
        "url-retrieve" => {
            need_arg_range(name, args, 2, 5)?;
            let url = string_text(&args[0])?;
            let callback = args[1].clone();
            let cbargs = args
                .get(2)
                .map(|value| value.to_vec())
                .transpose()?
                .unwrap_or_default();
            crate::lisp::primitives::processes::start_url_retrieval(
                interp, env, &url, callback, cbargs,
            )
        }
        "url-retrieve-synchronously" => {
            need_arg_range(name, args, 1, 4)?;
            let url = string_text(&args[0])?;
            let buffer = super::call(
                interp,
                "generate-new-buffer",
                &[Value::String(format!(" *http {url}*"))],
                env,
            )?;
            let buffer_id = interp.resolve_buffer_id(&buffer)?;
            match crate::lisp::primitives::processes::http_fetch_raw(&url) {
                Ok(bytes) => {
                    let saved = interp.current_buffer_id();
                    interp.switch_to_buffer_id(buffer_id)?;
                    let text: String = bytes.iter().map(|byte| char::from(*byte)).collect();
                    interp.insert_current_buffer(&text);
                    let _ = interp.switch_to_buffer_id(saved);
                    Ok(buffer)
                }
                Err(_) => Ok(Value::Nil),
            }
        }
        "url-insert" => {
            // GNU url-handlers.el extracts the body via mm-dissect-buffer;
            // the native url-retrieve buffers hold the raw response, so
            // split at the header/body boundary directly.
            need_arg_range(name, args, 1, 4)?;
            let buffer_id = interp.resolve_buffer_id(&args[0])?;
            let saved = interp.current_buffer_id();
            interp.switch_to_buffer_id(buffer_id)?;
            let text = interp.buffer.full_buffer_string();
            interp.switch_to_buffer_id(saved)?;
            let body_start = text
                .find("\r\n\r\n")
                .map(|index| index + 4)
                .or_else(|| text.find("\n\n").map(|index| index + 2))
                .unwrap_or(0);
            let body = &text[body_start..];
            let begin = args
                .get(1)
                .filter(|value| !value.is_nil())
                .map(|value| value.as_integer())
                .transpose()?
                .unwrap_or(0)
                .max(0) as usize;
            let end = args
                .get(2)
                .filter(|value| !value.is_nil())
                .map(|value| value.as_integer())
                .transpose()?
                .map(|value| (value.max(0) as usize).min(body.chars().count()));
            let body: String = match end {
                Some(end) => body.chars().take(end).skip(begin).collect(),
                None => body.chars().skip(begin).collect(),
            };
            let size = body.chars().count() as i64;
            interp.insert_current_buffer(&body);
            Ok(Value::list([Value::Integer(size), Value::Nil]))
        }
        "url-http-file-exists-p" => {
            need_arg_range(name, args, 1, 1)?;
            let url = string_text(&args[0])?;
            Ok(
                match crate::lisp::primitives::processes::http_fetch_raw(&url) {
                    Ok(bytes) => {
                        let head: String = bytes
                            .iter()
                            .take(64)
                            .map(|byte| char::from(*byte))
                            .collect();
                        let ok = head
                            .split_whitespace()
                            .nth(1)
                            .is_some_and(|code| code.starts_with('2'));
                        if ok { Value::T } else { Value::Nil }
                    }
                    Err(_) => Value::Nil,
                },
            )
        }
        "process-send-eof" => {
            need_arg_range(name, args, 0, 1)?;
            let process = match args.first() {
                Some(value) if !value.is_nil() => value.clone(),
                _ => call(interp, "get-buffer-process", &[Value::Nil], env)?,
            };
            let process_id = interp.resolve_process_id(&process)?;
            let (stdout, stderr) = interp.process_send_eof(process_id)?;
            deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
            Ok(process)
        }
        "process-send-string" => {
            need_args(name, args, 2)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            let input = string_text(&args[1])?;
            // GNU encodes eight-bit (raw byte) characters as their single
            // byte value; epg pipes binary signatures to gpg this way.
            let encoded = crate::lisp::primitives::encode_utf8_bytes(&input, false)?;
            let (stdout, stderr) = interp.process_send_string(process_id, &encoded)?;
            deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
            Ok(Value::Nil)
        }
        "process-lines" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let program = string_text(&args[0])?;
            let argv = args[1..]
                .iter()
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?;
            let process_output = run_external_process(interp, &program, &argv, None, env)?;
            if !process_output.status.success() {
                let stderr = String::from_utf8_lossy(&process_output.stderr)
                    .trim()
                    .to_string();
                return Err(LispError::Signal(if stderr.is_empty() {
                    format!(
                        "process-lines exited with status {}",
                        exit_status_code(&process_output.status)
                    )
                } else {
                    stderr
                }));
            }
            let lines = String::from_utf8_lossy(&process_output.stdout)
                .lines()
                .map(|line| Value::String(line.to_string()))
                .collect::<Vec<_>>();
            Ok(Value::list(lines))
        }
        "get-locale-names" => {
            if !args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let output = Command::new("locale")
                .arg("-a")
                .output()
                .map_err(|error| LispError::Signal(error.to_string()))?;
            if !output.status.success() {
                return Ok(Value::Nil);
            }
            let locales = String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(|line| Value::String(line.to_string()))
                .collect::<Vec<_>>();
            Ok(Value::list(locales))
        }
        "zlib-decompress-region" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let compressed = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let input = compressed
                .chars()
                .map(|ch| {
                    u8::try_from(ch as u32)
                        .map_err(|_| LispError::Signal("Invalid byte in compressed data".into()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut decoder = GzDecoder::new(&input[..]);
            let mut output = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut output)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            ensure_region_modifiable(interp, start, end, env)?;
            delete_region_with_hooks(interp, start, end, env)?;
            let text = output
                .iter()
                .map(|byte| char::from(*byte))
                .collect::<String>();
            insert_text_with_hooks(interp, &text, &[], false, false, env)?;
            Ok(Value::Nil)
        }
        "libxml-parse-xml-region" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let xml = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            parse_xml_region(&xml).map_err(|error| LispError::Signal(error.to_string()))
        }
        "call-process-region" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let (start, end) = if args[0].is_nil() && args[1].is_nil() {
                (interp.buffer.point_min(), interp.buffer.point_max())
            } else {
                (
                    position_from_value(interp, &args[0])?,
                    position_from_value(interp, &args[1])?,
                )
            };
            let input = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let program = string_text(&args[2])?;
            let delete_region = args.get(3).is_some_and(Value::is_truthy);
            let destination = args.get(4).unwrap_or(&Value::Nil);
            let argv = args
                .get(6..)
                .unwrap_or(&[])
                .iter()
                .map(string_text)
                .collect::<Result<Vec<_>, _>>()?;
            let process_output =
                run_external_process(interp, &program, &argv, Some(input.as_bytes()), env)?;
            if delete_region {
                interp
                    .buffer
                    .delete_region(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
            }
            write_process_output(
                interp,
                destination,
                &process_output.stdout,
                &process_output.stderr,
            )?;
            Ok(Value::Integer(exit_status_code(&process_output.status)))
        }
        "shell-command" => {
            need_arg_range(name, args, 1, 3)?;
            let command = string_text(&args[0])?;
            // GNU inserts the output into OUTPUT-BUFFER when it is t (or a
            // buffer): shell-command-to-string relies on the current buffer
            // receiving stdout.
            let capture = args.get(1).is_some_and(|value| !value.is_nil());
            if capture {
                let output = Command::new("sh")
                    .arg("-c")
                    .arg(&command)
                    .output()
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let target_buffer_id = match args.get(1) {
                    Some(Value::T) => Some(interp.current_buffer_id()),
                    Some(value @ (Value::Buffer(..) | Value::String(_))) => {
                        interp.resolve_buffer_id(value).ok()
                    }
                    _ => Some(interp.current_buffer_id()),
                };
                let text = String::from_utf8_lossy(&output.stdout).into_owned();
                if let Some(buffer_id) = target_buffer_id {
                    let saved = interp.current_buffer_id();
                    if buffer_id != saved {
                        interp.switch_to_buffer_id(buffer_id)?;
                    }
                    interp.insert_current_buffer(&text);
                    if buffer_id != saved {
                        interp.switch_to_buffer_id(saved)?;
                    }
                }
                return Ok(Value::Integer(output.status.code().unwrap_or(1) as i64));
            }
            let status = Command::new("sh")
                .arg("-c")
                .arg(&command)
                .status()
                .map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Integer(status.code().unwrap_or(1) as i64))
        }
        "kill-buffer" => {
            need_arg_range(name, args, 0, 1)?;
            let id = if let Some(buffer) = args.first().filter(|buffer| !buffer.is_nil()) {
                match interp.resolve_buffer_id(buffer) {
                    Ok(id) => id,
                    Err(_) if matches!(buffer, Value::Buffer(_, _)) => return Ok(Value::Nil),
                    Err(error) => return Err(error),
                }
            } else {
                interp.current_buffer_id()
            };
            let inhibit_hooks = interp.buffer_hooks_inhibited(id);
            let auto_save = interp.buffer_local_value(id, "buffer-auto-save-file-name");
            let auto_save_path = auto_save.as_ref().and_then(|value| string_text(value).ok());
            // GNU only asks about a modified buffer when it VISITS a file
            // (Fkill_buffer checks BVAR (b, filename)); scratch buffers
            // filled via insert-file-contents die silently.
            let modified = interp
                .get_buffer_by_id(id)
                .map(|buffer| buffer.is_modified() && buffer.file.is_some())
                .unwrap_or(false);
            if modified {
                let answer = call_named_function(
                    interp,
                    "yes-or-no-p",
                    &[Value::String("Buffer modified; kill anyway?".into())],
                    env,
                )?;
                if answer.is_nil() {
                    return Ok(Value::Nil);
                }
                if let Some(path) = auto_save_path.as_ref()
                    && fs::metadata(path).is_ok()
                    && interp
                        .lookup_var("kill-buffer-delete-auto-save-files", env)
                        .is_some_and(|value| value.is_truthy())
                {
                    let delete = call_named_function(
                        interp,
                        "yes-or-no-p",
                        &[Value::String("Delete auto-save file?".into())],
                        env,
                    )?;
                    if delete.is_truthy() {
                        let _ = fs::remove_file(path);
                    }
                }
                if id == interp.current_buffer_id() {
                    unlock_current_buffer(interp, env)?;
                }
            }
            if !inhibit_hooks {
                // The kill hooks run with the dying buffer current, as in
                // GNU (auto-revert's rm-watch reads its buffer-locals there).
                let saved = interp.current_buffer_id();
                let switched = saved != id && interp.switch_to_buffer_id(id).is_ok();
                let hooks_result: Result<bool, LispError> = (|| {
                    for hook in hook_values(interp, "kill-buffer-query-functions", env, Some(id)) {
                        let result = call_function_value(interp, &hook, &[], env)?;
                        if result.is_nil() {
                            return Ok(false);
                        }
                    }
                    run_named_hooks(interp, "kill-buffer-hook", env, Some(id))?;
                    Ok(true)
                })();
                if switched {
                    let _ = interp.switch_to_buffer_id(saved);
                }
                if !hooks_result? {
                    return Ok(Value::Nil);
                }
            }
            if !interp.allow_kill_buffer_for_threads(id) {
                return Ok(Value::Nil);
            }
            interp.kill_buffer_id(id);
            if !inhibit_hooks {
                run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
            }
            Ok(Value::T)
        }
        "bury-buffer" => {
            need_arg_range(name, args, 0, 1)?;
            let id = if let Some(buffer) = args.first().filter(|value| !value.is_nil()) {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            if let Some(index) = interp
                .buffer_list
                .iter()
                .position(|(buffer_id, _)| *buffer_id == id)
            {
                let entry = interp.buffer_list.remove(index);
                interp.buffer_list.push(entry);
            }
            if id == interp.current_buffer_id()
                && let Some((next_id, _)) = interp
                    .buffer_list
                    .iter()
                    .find(|(buffer_id, _)| *buffer_id != id)
                    .cloned()
            {
                interp.switch_to_buffer_id(next_id)?;
            }
            Ok(Value::Nil)
        }
        "set-mark" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            interp.buffer.set_mark(pos);
            Ok(Value::Nil)
        }
        "set-mark-command" => {
            need_arg_range(name, args, 0, 1)?;
            interp.buffer.set_mark(interp.buffer.point());
            Ok(Value::Nil)
        }
        "push-mark" => {
            let pos = if args.is_empty() || args[0].is_nil() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            interp.buffer.set_mark(pos);
            if !args.get(2).is_some_and(Value::is_truthy) {
                interp.buffer.deactivate_mark();
            }
            Ok(Value::Nil)
        }
        "mark" => Ok(match interp.buffer.mark() {
            Some(m) => Value::Integer(m as i64),
            None => Value::Nil,
        }),
        "make-marker" => Ok(interp.make_marker()),
        "copy-marker" => {
            need_args(name, args, 1)?;
            let insertion_type = args.get(1).is_some_and(Value::is_truthy);
            interp.copy_marker_value(&args[0], insertion_type)
        }
        "point-marker" => {
            interp.copy_marker_value(&Value::Integer(interp.buffer.point() as i64), false)
        }
        "mark-marker" => match interp.buffer.mark() {
            Some(pos) => interp.copy_marker_value(&Value::Integer(pos as i64), false),
            None => interp.copy_marker_value(&Value::Nil, false),
        },
        "point-min-marker" => {
            interp.copy_marker_value(&Value::Integer(interp.buffer.point_min() as i64), false)
        }
        "point-max-marker" => {
            interp.copy_marker_value(&Value::Integer(interp.buffer.point_max() as i64), false)
        }
        "marker-buffer" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            match interp.marker_buffer_id(marker_id) {
                Some(buffer_id) => {
                    let buffer_name = interp
                        .buffer_list
                        .iter()
                        .find(|(id, _)| *id == buffer_id)
                        .map(|(_, name)| name.clone())
                        .unwrap_or_else(|| "*unknown*".to_string());
                    Ok(Value::Buffer(buffer_id, buffer_name))
                }
                None => Ok(Value::Nil),
            }
        }
        "marker-position" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            Ok(interp
                .marker_position(marker_id)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "marker-last-position" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            Ok(interp
                .marker_last_position(marker_id)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "marker-insertion-type" => {
            need_args(name, args, 1)?;
            let marker_id = marker_id_from_value(&args[0])?;
            Ok(
                if interp.marker_insertion_type(marker_id).unwrap_or(false) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "set-marker-insertion-type" => {
            need_args(name, args, 2)?;
            let marker_id = marker_id_from_value(&args[0])?;
            let insertion_type = args[1].is_truthy();
            interp.set_marker_insertion_type(marker_id, insertion_type);
            Ok(if insertion_type { Value::T } else { Value::Nil })
        }
        "set-marker" | "move-marker" => {
            need_args(name, args, 2)?;
            let marker_id = marker_id_from_value(&args[0])?;
            let (position, buffer_id) = marker_target(interp, &args[1], args.get(2))?;
            interp.set_marker(marker_id, position, buffer_id)?;
            Ok(args[0].clone())
        }
        "region-beginning" => match interp.buffer.region() {
            Some((beg, _)) => Ok(Value::Integer(beg as i64)),
            None => Err(LispError::Signal("The mark is not set now".into())),
        },
        "region-end" => match interp.buffer.region() {
            Some((_, end)) => Ok(Value::Integer(end as i64)),
            None => Err(LispError::Signal("The mark is not set now".into())),
        },
        "deactivate-mark" => {
            interp.buffer.deactivate_mark();
            interp.set_variable("deactivate-mark", Value::Nil, env);
            Ok(Value::Nil)
        }
        "region-active-p" | "use-region-p" => Ok(
            if interp.buffer.mark_active()
                && interp
                    .lookup_var("transient-mark-mode", env)
                    .unwrap_or(Value::T)
                    .is_truthy()
            {
                Value::T
            } else {
                Value::Nil
            },
        ),

        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}

fn insert_directory_free_space_line(
    interp: &mut Interpreter,
    env: &mut Env,
    file: &str,
) -> Result<Option<String>, LispError> {
    if interp
        .lookup_var("dired-free-space", env)
        .and_then(|value| value.as_symbol().ok().map(str::to_owned))
        .as_deref()
        != Some("separate")
    {
        return Ok(None);
    }

    let Ok(info_function) = interp.lookup_function("file-system-info", env) else {
        return Ok(None);
    };
    let target = resolve_file_name_in_env(interp, env, file);
    let info = interp.call_function_value(
        info_function,
        Some("file-system-info"),
        &[Value::String(target)],
        env,
    )?;
    let Some(available_bytes) = nth_list_item(&info, 2)
        .or_else(|| nth_list_item(&info, 1))
        .or_else(|| nth_list_item(&info, 0))
    else {
        return Ok(None);
    };
    if available_bytes.is_nil() {
        return Ok(None);
    }
    let human_readable = file_size_human_readable(interp, env, &available_bytes)?;
    Ok(Some(format!("available {human_readable}\n")))
}

fn mock_file_local_copy(file: &str) -> Result<String, LispError> {
    let Some(remote) = parse_remote_file_name(file) else {
        return Ok(file.to_string());
    };
    let mut prefix = std::env::temp_dir();
    let mut name = file_name_nondirectory(&remote.localname);
    if name.is_empty() {
        name = "copy".into();
    }
    prefix.push(format!("emaxx-mock-copy-{name}-"));
    let target = make_temp_file_internal(&prefix.display().to_string(), &Value::Nil, "", None)?;
    fs::copy(&remote.localname, &target).map_err(|error| LispError::Signal(error.to_string()))?;
    Ok(target)
}

fn file_local_variable_is_truthy(source: &str, variable: &str) -> bool {
    source.lines().take(2).any(|line| {
        line.split("-*-")
            .nth(1)
            .is_some_and(|settings| file_local_settings_contain_truthy(settings, variable))
    }) || file_local_variables_block_value(source, variable).is_some_and(|value| value == "t")
}

fn file_local_settings_contain_truthy(settings: &str, variable: &str) -> bool {
    settings
        .split(';')
        .filter_map(|part| part.split_once(':'))
        .any(|(name, value)| name.trim() == variable && value.trim() == "t")
}

fn file_local_variables_block_value(source: &str, variable: &str) -> Option<String> {
    let mut inside_block = false;
    for line in source.lines().rev() {
        let trimmed = line.trim_start();
        let comment_text = trimmed.trim_start_matches(';').trim_start();
        if comment_text == "End:" {
            inside_block = true;
            continue;
        }
        if !inside_block {
            continue;
        }
        if comment_text == "Local Variables:" {
            break;
        }
        let (name, value) = comment_text.split_once(':')?;
        if name.trim() == variable {
            return Some(value.trim().to_string());
        }
    }
    None
}

fn nth_list_item(list: &Value, index: usize) -> Option<Value> {
    let mut current = list.clone();
    for _ in 0..index {
        current = current.cdr().ok()?;
    }
    current.car().ok()
}

fn file_size_human_readable(
    interp: &mut Interpreter,
    env: &mut Env,
    size: &Value,
) -> Result<String, LispError> {
    // GNU get-free-disk-space funcalls `byte-count-to-string-function',
    // whose default is file-size-human-readable-iec (IEC prefixes with a
    // space separator, e.g. "10 B").
    if let Some(function) = interp
        .lookup_var("byte-count-to-string-function", env)
        .filter(|value| value.is_truthy())
    {
        let rendered =
            interp.call_function_value(function, None, std::slice::from_ref(size), env)?;
        return string_text(&rendered);
    }
    if let Ok(function) = interp.lookup_function("file-size-human-readable", env) {
        let rendered = interp.call_function_value(
            function,
            Some("file-size-human-readable"),
            &[
                size.clone(),
                Value::Symbol("iec".into()),
                Value::String(" ".into()),
            ],
            env,
        )?;
        return string_text(&rendered);
    }

    let bytes = size.as_integer()?;
    Ok(if bytes == 1 {
        "1 B".to_string()
    } else {
        format!("{bytes} B")
    })
}

fn call_major_or_named_mode(
    interp: &mut Interpreter,
    mode: &str,
    env: &mut Env,
) -> Result<Value, LispError> {
    if modes::is_major_mode_builtin(mode) {
        modes::call_major_mode(interp, mode)
    } else {
        call_named_function(interp, mode, &[], env)
    }
}
