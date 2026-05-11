use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "define-key"
            | "define-key-after"
            | "bindings--define-key"
            | "keymap-set"
            | "keymap-unset"
            | "lookup-key"
            | "keymap-lookup"
            | "accessible-keymaps"
            | "keymap--read-only-filter"
            | "keymap-read-only-bind"
            | "keymap--get-keyelt"
            | "describe-buffer-bindings"
            | "help--describe-vector"
            | "key-binding"
            | "command-remapping"
            | "keymap-parent"
            | "set-keymap-parent"
            | "map-keymap"
            | "suppress-keymap"
            | "use-local-map"
            | "current-local-map"
            | "current-global-map"
            | "global-set-key"
            | "local-set-key"
            | "global-unset-key"
            | "local-unset-key"
            | "substitute-key-definition"
            | "easy-menu-binding"
            | "easy-menu-add-item"
            | "tool-bar-local-item"
            | "tool-bar-local-item-from-menu"
            | "custom-add-choice"
            | "custom-add-option"
            | "define-widget"
            | "widget-create"
            | "define-button-type"
            | "display-mouse-p"
            | "make-button"
            | "button-at"
            | "button-type"
            | "defined-colors"
            | "color-defined-p"
            | "symbol-function"
            | "symbol-file"
            | "symbol-name"
            | "user-login-name"
            | "system-name"
            | "user-full-name"
            | "macroexp-file-name"
            | "char-from-name"
            | "always"
            | "evenp"
            | "seq-subseq"
            | "text-quoting-style"
            | "file-truename"
            | "group-gid"
            | "group-name"
            | "save-buffer"
            | "emaxx-default-revert-buffer-function"
            | "buffer-stale--default-function"
            | "revert-buffer"
            | "lock-buffer"
            | "unlock-buffer"
            | "ask-user-about-supersession-threat"
            | "advice-add"
            | "advice-member-p"
            | "advice-remove"
            | "emaxx-apply-around-advice"
            | "emaxx-apply-after-advice"
            | "remove-function"
            | "userlock--handle-unlock-error"
            | "recent-auto-save-p"
            | "set-buffer-auto-saved"
            | "clear-buffer-auto-save-failure"
            | "next-read-file-uses-dialog-p"
            | "auto-save-mode"
            | "do-auto-save"
            | "unix-sync"
            | "set-binary-mode"
            | "obarray-make"
            | "obarrayp"
            | "obarray-clear"
            | "internal--obarray-buckets"
            | "define-hash-table-test"
            | "make-hash-table"
            | "hash-table-p"
            | "hash-table-contains-p"
            | "copy-hash-table"
            | "gethash"
            | "puthash"
            | "maphash"
            | "remhash"
            | "clrhash"
            | "completion-table-case-fold"
            | "hash-table-count"
            | "hash-table-rehash-size"
            | "hash-table-rehash-threshold"
            | "hash-table-size"
            | "hash-table-test"
            | "hash-table-weakness"
            | "hash-table-keys"
            | "try-completion"
            | "all-completions"
            | "test-completion"
            | "map-pairs"
            | "internal--hash-table-index-size"
            | "internal--hash-table-histogram"
            | "internal--hash-table-buckets"
            | "profiler-memory-running-p"
            | "profiler-memory-start"
            | "profiler-memory-stop"
            | "profiler-memory-log"
            | "profiler-cpu-running-p"
            | "profiler-cpu-start"
            | "profiler-cpu-stop"
            | "profiler-cpu-log"
            | "byte-compile-check-lambda-list"
            | "byte-compile"
            | "funcall-with-delayed-message"
            | "handler-bind-1"
            | "debugger-trap"
            | "backtrace-frame--internal"
            | "backtrace-debug"
            | "backtrace-eval"
            | "backtrace--locals"
            | "current-thread"
            | "all-threads"
            | "make-thread"
            | "thread-live-p"
            | "thread-join"
            | "thread-name"
            | "thread-signal"
            | "thread-last-error"
            | "thread-yield"
            | "make-mutex"
            | "mutex-lock"
            | "mutex-unlock"
            | "make-condition-variable"
            | "condition-mutex"
            | "condition-name"
            | "condition-notify"
            | "thread--blocker"
            | "thread-buffer-disposition"
            | "thread-set-buffer-disposition"
            | "backtrace--frames-from-thread"
            | "list-threads"
            | "thread-list-send-error-signal"
            | "thread-list-pop-to-backtrace"
            | "regexp-quote"
            | "regexp-opt"
            | "rx-to-string"
            | "convert-standard-filename"
            | "abbreviate-file-name"
            | "files--name-absolute-system-p"
            | "files--use-insert-directory-program-p"
            | "insert-directory-wildcard-in-dir-p"
            | "connection-local-value"
            | "propertized-buffer-identification"
            | "called-interactively-p"
            | "kill-all-local-variables"
            | "hack-local-variables-filter"
            | "hack-local-variables-apply"
            | "hack-dir-local-variables-non-file-buffer"
            | "force-mode-line-update"
            | "garbage-collect"
            | "num-processors"
            | "current-cpu-time"
            | "emacs-pid"
            | "type-of"
            | "cl-type-of"
            | "cl-find-class"
            | "cl--class-parents"
            | "cl--class-allparents"
            | "cl--class-children"
            | "eieio--object-class"
            | "eieio--class-name"
            | "eieio-object-p"
            | "slot-boundp"
            | "make-instance"
            | "emaxx-class-make"
            | "eieio-oref"
            | "slot-value"
            | "eieio-oset"
            | "built-in-class-p"
            | "cl-typep"
            | "cl-functionp"
            | "cl-proclaim"
            | "url-scheme-get-property"
    )
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match name {
        "define-key" => {
            need_arg_range(name, args, 3, 4)?;
            let key = key_sequence_binding_text(&args[1])?;
            let key_parts = key_sequence_binding_parts(&args[1])?;
            if args[2].is_nil() && args.get(3).is_some_and(Value::is_truthy) {
                keymap_remove_binding(interp, &args[0], &key)?;
            } else {
                keymap_define_binding_with_placement(
                    interp,
                    &args[0],
                    &key,
                    Some(key_parts),
                    args[2].clone(),
                    false,
                )?;
            }
            Ok(args[2].clone())
        }
        "define-key-after" => {
            need_arg_range(name, args, 3, 4)?;
            let key = key_sequence_binding_text(&args[1])?;
            let key_parts = key_sequence_binding_parts(&args[1])?;
            let after = args
                .get(3)
                .filter(|value| !value.is_nil())
                .map(key_sequence_binding_parts)
                .transpose()?;
            keymap_define_binding_after(
                interp,
                &args[0],
                &key,
                Some(key_parts),
                args[2].clone(),
                after.as_deref(),
            )?;
            Ok(Value::Nil)
        }
        "bindings--define-key" => {
            need_args(name, args, 3)?;
            let key = key_sequence_binding_text(&args[1])?;
            let key_parts = key_sequence_binding_parts(&args[1])?;
            keymap_define_binding_with_placement(
                interp,
                &args[0],
                &key,
                Some(key_parts),
                args[2].clone(),
                false,
            )?;
            Ok(Value::Nil)
        }
        "keymap-set" => {
            need_args(name, args, 3)?;
            let key = key_sequence_binding_text(&args[1])?;
            let key_parts = key_sequence_binding_parts(&args[1])?;
            keymap_define_binding_with_placement(
                interp,
                &args[0],
                &key,
                Some(key_parts),
                args[2].clone(),
                true,
            )?;
            Ok(args[2].clone())
        }
        "keymap-unset" => {
            need_arg_range(name, args, 2, 3)?;
            let key = key_sequence_binding_text(&args[1])?;
            if args.get(2).is_some_and(Value::is_truthy) {
                keymap_remove_binding(interp, &args[0], &key)?;
            } else {
                let key_parts = key_sequence_binding_parts(&args[1])?;
                keymap_define_binding_with_placement(
                    interp,
                    &args[0],
                    &key,
                    Some(key_parts),
                    Value::Nil,
                    true,
                )?;
            }
            Ok(Value::Nil)
        }
        "lookup-key" | "keymap-lookup" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let key_parts = key_sequence_binding_parts(&args[1])?;
            keymap_lookup_sequence_value(interp, &args[0], &key_parts, env)
        }
        "accessible-keymaps" => accessible_keymaps(interp, args, env),
        "keymap--read-only-filter" => {
            need_args(name, args, 1)?;
            if interp
                .lookup_var("buffer-read-only", env)
                .is_some_and(|value| value.is_truthy())
            {
                Ok(args[0].clone())
            } else {
                Ok(Value::Nil)
            }
        }
        "keymap-read-only-bind" => {
            need_args(name, args, 1)?;
            Ok(Value::list([
                Value::Symbol("menu-item".into()),
                Value::String(String::new()),
                args[0].clone(),
                Value::Symbol(":filter".into()),
                Value::list([
                    Value::Symbol("function".into()),
                    Value::Symbol("keymap--read-only-filter".into()),
                ]),
            ]))
        }
        "keymap--get-keyelt" => {
            need_args(name, args, 2)?;
            keymap_get_keyelt(interp, &args[0], args[1].is_truthy(), env)
        }
        "describe-buffer-bindings" => describe_buffer_bindings(interp, args, env),
        "help--describe-vector" => help_describe_vector(interp, args, env),
        "key-binding" => {
            need_arg_range(name, args, 1, 3)?;
            let key = key_sequence_binding_text(&args[0])?;
            key_binding(interp, &key, env)
        }
        "command-remapping" => {
            need_arg_range(name, args, 1, 3)?;
            command_remapping(interp, &args[0], args.get(2), env)
        }
        "keymap-parent" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Record(id)
                    if interp
                        .find_record(*id)
                        .is_some_and(|record| record.type_name == KEYMAP_RECORD_TYPE) =>
                {
                    Ok(interp
                        .find_record(*id)
                        .and_then(|record| record.slots.get(KEYMAP_PARENT_SLOT).cloned())
                        .unwrap_or(Value::Nil))
                }
                _ => Ok(Value::Nil),
            }
        }
        "set-keymap-parent" => {
            need_args(name, args, 2)?;
            if let Value::Record(id) = &args[0]
                && let Some(record) = interp.find_record_mut(*id)
                && record.type_name == KEYMAP_RECORD_TYPE
            {
                if record.slots.len() <= KEYMAP_PARENT_SLOT {
                    record.slots.resize(KEYMAP_PARENT_SLOT + 1, Value::Nil);
                }
                record.slots[KEYMAP_PARENT_SLOT] = args[1].clone();
            }
            Ok(Value::Nil)
        }
        "map-keymap" => {
            need_args(name, args, 2)?;
            let Some(id) = keymap_record_id(interp, &args[1]) else {
                return Ok(Value::Nil);
            };
            let Some(record) = interp.find_record(id) else {
                return Ok(Value::Nil);
            };
            let bindings = keymap_bindings(record)?;
            for binding in bindings {
                interp.call_function_value(
                    args[0].clone(),
                    None,
                    &[
                        keymap_entry_key_value(&binding_key_parts(&binding), &binding.key),
                        binding.value,
                    ],
                    env,
                )?;
            }
            Ok(Value::Nil)
        }
        "suppress-keymap" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[0].clone())
        }
        "use-local-map" => {
            need_args(name, args, 1)?;
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "current-local-map",
                args[0].clone(),
            );
            Ok(args[0].clone())
        }
        "current-local-map" => {
            need_args(name, args, 0)?;
            Ok(interp
                .lookup_var("current-local-map", env)
                .unwrap_or(Value::Nil))
        }
        "current-global-map" => {
            need_args(name, args, 0)?;
            Ok(interp
                .lookup_var("global-map", env)
                .unwrap_or_else(|| keymap_placeholder(Some("global-map"))))
        }
        "global-set-key" => {
            need_args(name, args, 2)?;
            let key = key_sequence_binding_text(&args[0])?;
            let key_parts = key_sequence_binding_parts(&args[0])?;
            let global_map = interp
                .lookup_var("global-map", env)
                .unwrap_or_else(|| keymap_placeholder(Some("global-map")));
            keymap_define_binding_with_placement(
                interp,
                &global_map,
                &key,
                Some(key_parts),
                args[1].clone(),
                true,
            )?;
            Ok(args[1].clone())
        }
        "local-set-key" => {
            need_args(name, args, 2)?;
            Ok(args[1].clone())
        }
        "global-unset-key" => {
            need_args(name, args, 1)?;
            let key = key_sequence_binding_text(&args[0])?;
            let global_map = interp
                .lookup_var("global-map", env)
                .unwrap_or_else(|| keymap_placeholder(Some("global-map")));
            keymap_remove_binding(interp, &global_map, &key)?;
            Ok(Value::Nil)
        }
        "local-unset-key" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "substitute-key-definition" => {
            if args.len() < 3 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[2].clone())
        }
        "easy-menu-binding" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let item_name = args.get(1).cloned().unwrap_or(Value::Nil);
            Ok(Value::list([
                Value::Symbol("menu-item".into()),
                item_name,
                args[0].clone(),
            ]))
        }
        "easy-menu-add-item" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[2].clone())
        }
        "tool-bar-local-item" => {
            if args.len() < 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[3].clone())
        }
        "tool-bar-local-item-from-menu" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[2].clone())
        }
        "custom-add-choice" => {
            need_args(name, args, 2)?;
            let variable = args[0].as_symbol()?;
            let choice = args[1].clone();
            let choices = interp
                .get_symbol_property(variable, "custom-type")
                .unwrap_or(Value::Nil);
            let mut entries = choices.to_vec()?;
            if !matches!(entries.first(), Some(Value::Symbol(kind)) if kind == "choice") {
                return Err(LispError::Signal(format!("Not a choice type: {choices}")));
            }
            let new_tag = custom_choice_tag(&choice);
            let already_present = new_tag.as_ref().is_some_and(|tag| {
                entries[1..]
                    .iter()
                    .filter_map(custom_choice_tag)
                    .any(|existing| values_equal(interp, &existing, tag))
            });
            if !already_present {
                entries.push(choice);
                interp.put_symbol_property(variable, "custom-type", Value::list(entries));
            }
            Ok(Value::Nil)
        }
        "custom-add-option" => {
            need_args(name, args, 2)?;
            let variable = args[0].as_symbol()?;
            let option = args[1].clone();
            let existing = interp
                .get_symbol_property(variable, "custom-options")
                .unwrap_or(Value::Nil);
            let mut options = existing.to_vec()?;
            if !options
                .iter()
                .any(|existing| values_equal(interp, existing, &option))
            {
                options.push(option);
            }
            let updated = Value::list(options);
            interp.put_symbol_property(variable, "custom-options", updated.clone());
            Ok(updated)
        }
        "define-widget" => {
            if args.len() < 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let name = args[0].as_symbol()?;
            let class = args[1].clone();
            let doc = args[2].clone();
            let widget_type = if args.len() > 3 {
                Value::cons(class, Value::list(args[3..].to_vec()))
            } else {
                Value::list([class])
            };
            interp.put_symbol_property(name, "widget-type", widget_type);
            interp.put_symbol_property(name, "widget-documentation", doc);
            Ok(Value::Symbol(name.to_string()))
        }
        "widget-create" => {
            need_args(name, args, 1)?;
            if let Some(label) = args.iter().skip(1).find_map(string_like) {
                interp.buffer.insert(&label.text);
            }
            Ok(Value::cons(
                args[0].clone(),
                Value::list(args[1..].to_vec()),
            ))
        }
        "define-button-type" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "display-mouse-p" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "make-button" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if args[0].is_nil() || args[1].is_nil() {
                return Ok(Value::Nil);
            }
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            let mut cursor = 2usize;
            while cursor + 1 < args.len() {
                if let Ok(property) = args[cursor].as_symbol() {
                    let property = if property == "type" {
                        "button-type"
                    } else {
                        property
                    };
                    interp
                        .buffer
                        .put_text_property(start, end, property, args[cursor + 1].clone());
                }
                cursor += 2;
            }
            Ok(Value::Integer(start as i64))
        }
        "button-at" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            Ok(interp
                .buffer
                .text_property_at(pos, "button-type")
                .map(|_| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "button-type" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            Ok(interp
                .buffer
                .text_property_at(pos, "button-type")
                .unwrap_or(Value::Nil))
        }
        "defined-colors" => {
            need_args(name, args, 0)?;
            Ok(Value::list([
                Value::String("black".into()),
                Value::String("white".into()),
                Value::String("red".into()),
                Value::String("green".into()),
                Value::String("blue".into()),
            ]))
        }
        "color-defined-p" => {
            need_args(name, args, 1)?;
            let color = string_text(&args[0])?;
            Ok(
                if ["black", "white", "red", "green", "blue"]
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(&color))
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "symbol-function" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(match interp.raw_function_binding(symbol, env) {
                Some(value) => value,
                None if is_special_form_name(symbol) => Value::BuiltinFunc(symbol.to_string()),
                None if symbol == "benchmark-run" => Value::list([
                    Value::Symbol("autoload".into()),
                    Value::String("benchmark.el".into()),
                    Value::String("Autoloaded benchmark-run.".into()),
                    Value::Nil,
                    Value::Nil,
                ]),
                None if symbol == "tetris" => Value::list([
                    Value::Symbol("autoload".into()),
                    Value::String("tetris.el".into()),
                    Value::String("Autoloaded tetris.".into()),
                    Value::T,
                    Value::Nil,
                ]),
                None => Value::String(format!("#<function {}>", symbol)),
            })
        }
        "symbol-file" => {
            need_arg_range(name, args, 1, 2)?;
            Ok(Value::Nil)
        }
        "symbol-name" => {
            need_args(name, args, 1)?;
            let s = args[0].as_symbol()?;
            Ok(Value::String(
                crate::lisp::types::visible_symbol_name(s).to_string(),
            ))
        }
        "user-login-name" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::String(
                current_user_login_name().unwrap_or_else(|| "user".into()),
            ))
        }
        "system-name" => {
            if !args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(Value::String(system_name_value()))
        }
        "user-full-name" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let requested = args.first().and_then(|value| {
                if value.is_nil() {
                    None
                } else {
                    string_text(value).ok()
                }
            });
            Ok(user_full_name(requested.as_deref())
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "macroexp-file-name" => {
            need_args(name, args, 0)?;
            Ok(interp
                .lookup_var("macroexp-file-name", env)
                .unwrap_or(Value::Nil))
        }
        "char-from-name" => {
            need_args(name, args, 1)?;
            let name = string_text(&args[0])?;
            let ch = match name.as_str() {
                "SMILE" => 0x263A,
                _ => return Ok(Value::Nil),
            };
            Ok(Value::Integer(ch))
        }
        "always" => Ok(Value::T),
        "evenp" => {
            need_args(name, args, 1)?;
            Ok(
                if (&integer_like_bigint(interp, &args[0])? & BigInt::from(1u8)).is_zero() {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "seq-subseq" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            seq_subseq(
                &args[0],
                args[1].as_integer()?,
                args.get(2).map(Value::as_integer).transpose()?,
            )
        }
        "text-quoting-style" => Ok(Value::Symbol("grave".into())),
        "file-truename" => {
            need_args(name, args, 1)?;
            Ok(Value::String(string_text(&args[0])?))
        }
        "group-gid" => Ok(Value::Integer(current_group_id()? as i64)),
        "group-name" => {
            need_args(name, args, 1)?;
            let gid = match &args[0] {
                Value::Integer(value) => *value,
                Value::Float(value) => *value as i64,
                _ => return Err(LispError::Signal("Invalid GID specification".into())),
            };
            Ok(group_name_from_gid(gid)?
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "save-buffer" => {
            let Some(path) = interp.buffer.file.clone() else {
                return Ok(Value::Nil);
            };
            if !interp.buffer.is_modified() {
                return Ok(Value::Nil);
            }
            let buffer_text = interp.buffer.full_buffer_string();
            if std::fs::read_to_string(&path).is_ok_and(|contents| contents == buffer_text) {
                interp.buffer.set_unmodified();
                interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
                unlock_current_buffer(interp, env)?;
                return Ok(Value::Nil);
            }
            ensure_no_supersession_threat(interp, env)?;
            if run_write_buffer_hooks_until_success(interp, env)? {
                return Ok(Value::Nil);
            }
            std::fs::write(&path, &buffer_text).map_err(|e| LispError::Signal(e.to_string()))?;
            interp.buffer.set_unmodified();
            interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
            unlock_current_buffer(interp, env)?;
            Ok(Value::Nil)
        }
        "emaxx-default-revert-buffer-function" => {
            revert_current_buffer(interp, env)?;
            Ok(Value::Nil)
        }
        "buffer-stale--default-function" => {
            need_arg_range(name, args, 0, 1)?;
            let Some(path) = interp.buffer.file.clone() else {
                return Ok(Value::Nil);
            };
            if interp.buffer.is_modified() || !Path::new(&path).is_file() {
                return Ok(Value::Nil);
            }
            let current = file_modtime(&path)?;
            Ok(if interp.buffer.visited_file_modtime() != current {
                Value::T
            } else {
                Value::Nil
            })
        }
        "revert-buffer" => {
            if let Some(revert_function) = interp.lookup_var("revert-buffer-function", env)
                && revert_function.is_truthy()
            {
                let mut revert_args = Vec::with_capacity(args.len() + 1);
                revert_args.push(Value::Symbol("emaxx-default-revert-buffer-function".into()));
                revert_args.extend(args.iter().cloned());
                return interp.call_function_value(revert_function, None, &revert_args, env);
            }
            revert_current_buffer(interp, env)?;
            Ok(Value::Nil)
        }
        "lock-buffer" => {
            maybe_lock_current_buffer(interp, env)?;
            Ok(Value::Nil)
        }
        "unlock-buffer" => unlock_current_buffer(interp, env),
        "ask-user-about-supersession-threat" => {
            need_args(name, args, 1)?;
            Ok(Value::T)
        }
        "advice-add" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let function_name = args[0].as_symbol()?.to_string();
            let where_sym = args[1].as_symbol()?;
            let original = interp.lookup_function(&function_name, env)?;
            let advice = match &args[2] {
                Value::Symbol(symbol) => interp.lookup_function(symbol, env)?,
                other => other.clone(),
            };
            let wrapped = match where_sym {
                ":override" => advice,
                ":after" => make_advice_wrapper_after(original, advice),
                ":around" => make_advice_wrapper_around(original, advice),
                _ => return Ok(Value::Nil),
            };
            interp.push_function_binding(&function_name, wrapped);
            Ok(Value::Nil)
        }
        "advice-member-p" => {
            need_args(name, args, 2)?;
            Ok(Value::Nil)
        }
        "advice-remove" => {
            need_args(name, args, 2)?;
            let function_name = args[0].as_symbol()?.to_string();
            interp.pop_function_binding(&function_name);
            Ok(Value::Nil)
        }
        "emaxx-apply-around-advice" => {
            need_args(name, args, 3)?;
            let original = args[0].clone();
            let advice = args[1].clone();
            let mut advice_args = Vec::with_capacity(1 + args[2].to_vec()?.len());
            advice_args.push(original);
            advice_args.extend(args[2].to_vec()?);
            interp.call_function_value(advice, None, &advice_args, env)
        }
        "emaxx-apply-after-advice" => {
            need_args(name, args, 3)?;
            let original = args[0].clone();
            let advice = args[1].clone();
            let original_args = args[2].to_vec()?;
            let result = interp.call_function_value(original, None, &original_args, env)?;
            interp.call_function_value(advice, None, &original_args, env)?;
            Ok(result)
        }
        "remove-function" => {
            need_args(name, args, 2)?;
            Ok(Value::Nil)
        }
        "userlock--handle-unlock-error" => Ok(Value::Nil),
        "recent-auto-save-p" => {
            need_args(name, args, 0)?;
            Ok(if interp.buffer.is_autosaved() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "set-buffer-auto-saved" => {
            need_args(name, args, 0)?;
            interp.buffer.set_autosaved();
            Ok(Value::Nil)
        }
        "clear-buffer-auto-save-failure" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
        "next-read-file-uses-dialog-p" => {
            need_args(name, args, 0)?;
            let use_dialog = interp
                .lookup_var("use-dialog-box", env)
                .is_some_and(|value| value.is_truthy());
            let use_file_dialog = interp
                .lookup_var("use-file-dialog", env)
                .is_some_and(|value| value.is_truthy());
            Ok(if use_dialog && use_file_dialog {
                Value::T
            } else {
                Value::Nil
            })
        }
        "auto-save-mode" => {
            let enabled = args.first().is_none_or(Value::is_truthy);
            if enabled {
                let path = auto_save_path_for_buffer(&interp.buffer);
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "buffer-auto-save-file-name",
                    Value::String(path),
                );
                Ok(Value::T)
            } else {
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "buffer-auto-save-file-name",
                    Value::Nil,
                );
                Ok(Value::Nil)
            }
        }
        "do-auto-save" => {
            let path = interp
                .buffer_local_value(interp.current_buffer_id(), "buffer-auto-save-file-name")
                .and_then(|value| string_text(&value).ok())
                .unwrap_or_else(|| auto_save_path_for_buffer(&interp.buffer));
            std::fs::write(&path, interp.buffer.buffer_string())
                .map_err(|e| LispError::Signal(e.to_string()))?;
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "buffer-auto-save-file-name",
                Value::String(path),
            );
            interp.buffer.set_autosaved();
            Ok(Value::Nil)
        }
        "unix-sync" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
        "set-binary-mode" => {
            need_args(name, args, 2)?;
            match &args[0] {
                Value::Symbol(stream)
                    if matches!(stream.as_str(), "stdin" | "stdout" | "stderr") =>
                {
                    Ok(Value::Nil)
                }
                _ => Err(LispError::Signal("Invalid stream".into())),
            }
        }
        "obarray-make" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if let Some(size) = args.first()
                && size.as_integer()? < 0
            {
                return Err(LispError::TypeError("natnump".into(), size.type_name()));
            }
            Ok(make_obarray(interp))
        }
        "obarrayp" => {
            need_args(name, args, 1)?;
            Ok(if is_obarray_like_value(interp, &args[0]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "obarray-clear" => {
            need_args(name, args, 1)?;
            clear_obarray(interp, &args[0])
        }
        "internal--obarray-buckets" => {
            need_args(name, args, 1)?;
            Ok(Value::list(
                obarray_symbols(interp, &args[0])?
                    .into_iter()
                    .map(|symbol| Value::list([symbol])),
            ))
        }
        "define-hash-table-test" => {
            need_args(name, args, 3)?;
            let symbol = args[0].as_symbol()?;
            let spec = Value::list([args[1].clone(), args[2].clone()]);
            interp.put_symbol_property(symbol, "hash-table-test", spec.clone());
            Ok(spec)
        }
        "make-hash-table" => {
            let mut test = "eql".to_string();
            let mut size = Value::Integer(65);
            let mut rehash_size = Value::Float(1.5);
            let mut rehash_threshold = Value::Float(0.8125);
            let mut weakness = Value::Nil;
            let mut index = 0usize;
            while index + 1 < args.len() {
                let key = args[index].as_symbol()?;
                match key {
                    ":test" => {
                        test = match &args[index + 1] {
                            Value::Symbol(name) => name.clone(),
                            Value::BuiltinFunc(name) => name.clone(),
                            other => {
                                return Err(LispError::TypeError(
                                    "symbol".into(),
                                    other.type_name(),
                                ));
                            }
                        };
                    }
                    ":size" => size = args[index + 1].clone(),
                    ":rehash-size" => rehash_size = args[index + 1].clone(),
                    ":rehash-threshold" => rehash_threshold = args[index + 1].clone(),
                    ":weakness" => {
                        weakness = match &args[index + 1] {
                            Value::T => Value::Symbol("key-and-value".into()),
                            other => other.clone(),
                        };
                    }
                    ":purecopy" => {}
                    _ => {
                        return Err(LispError::Signal(format!(
                            "Invalid hash table parameter: {key}"
                        )));
                    }
                }
                index += 2;
            }
            if !matches!(test.as_str(), "eq" | "eql" | "equal")
                && hash_table_user_test_functions(interp, &test).is_none()
            {
                return Err(LispError::Signal("Invalid hash table test".into()));
            }
            let table = json::make_hash_table(interp, &test, Vec::new());
            let Value::Record(id) = table.clone() else {
                unreachable!("hash tables are represented as records")
            };
            let record = interp
                .find_record_mut(id)
                .expect("make_hash_table should create a record");
            if record.slots.len() < 6 {
                record.slots.resize(6, Value::Nil);
            }
            record.slots[2] = size;
            record.slots[3] = rehash_size;
            record.slots[4] = rehash_threshold;
            record.slots[5] = weakness;
            Ok(table)
        }
        "hash-table-p" => {
            need_args(name, args, 1)?;
            Ok(if json::is_hash_table(interp, &args[0]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "hash-table-contains-p" => {
            need_args(name, args, 2)?;
            let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[1].type_name(),
                ));
            };
            for (existing_key, _) in entries {
                if hash_table_key_matches(interp, &args[1], &test, &existing_key, &args[0], env)? {
                    return Ok(Value::T);
                }
            }
            Ok(Value::Nil)
        }
        "copy-hash-table" => {
            need_args(name, args, 1)?;
            let Value::Record(id) = args[0] else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            };
            let Some(record) = interp.find_record(id) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            };
            if record.type_name != "hash-table" {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            }
            interp.copy_record(id)
        }
        "gethash" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[1].type_name(),
                ));
            };
            let default = args.get(2).cloned().unwrap_or(Value::Nil);
            for (existing_key, value) in entries {
                if hash_table_key_matches(interp, &args[1], &test, &existing_key, &args[0], env)? {
                    return Ok(value);
                }
            }
            Ok(default)
        }
        "puthash" => {
            need_args(name, args, 3)?;
            let Some((test, mut entries)) = json::hash_table_entries(interp, &args[2]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[2].type_name(),
                ));
            };
            touch_hash_table_key(interp, &args[2], &test, &args[0], env)?;
            let mut replaced = false;
            for (existing_key, existing_value) in &mut entries {
                if hash_table_key_matches(interp, &args[2], &test, existing_key, &args[0], env)? {
                    *existing_value = args[1].clone();
                    replaced = true;
                    break;
                }
            }
            if !replaced {
                entries.push((args[0].clone(), args[1].clone()));
            }
            set_hash_table_entries(interp, &args[2], entries)?;
            Ok(args[1].clone())
        }
        "maphash" => {
            need_args(name, args, 2)?;
            let Some((_, entries)) = json::hash_table_entries(interp, &args[1]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[1].type_name(),
                ));
            };
            for (key, value) in entries {
                call_function_value(interp, &args[0], &[key, value], env)?;
            }
            Ok(Value::Nil)
        }
        "remhash" => {
            need_args(name, args, 2)?;
            let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[1].type_name(),
                ));
            };
            let mut retained = Vec::new();
            for (existing_key, value) in entries {
                if !hash_table_key_matches(interp, &args[1], &test, &existing_key, &args[0], env)? {
                    retained.push((existing_key, value));
                }
            }
            set_hash_table_entries(interp, &args[1], retained)?;
            Ok(Value::Nil)
        }
        "clrhash" => {
            need_args(name, args, 1)?;
            if json::hash_table_entries(interp, &args[0]).is_none() {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            }
            set_hash_table_entries(interp, &args[0], Vec::new())?;
            Ok(args[0].clone())
        }
        "completion-table-case-fold" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            Ok(args[0].clone())
        }
        "hash-table-count" => {
            need_args(name, args, 1)?;
            let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(Value::Integer(entries.len() as i64))
        }
        "hash-table-rehash-size" => {
            need_args(name, args, 1)?;
            Ok(hash_table_metadata_slot(
                interp,
                &args[0],
                3,
                Value::Float(1.5),
            )?)
        }
        "hash-table-rehash-threshold" => {
            need_args(name, args, 1)?;
            Ok(hash_table_metadata_slot(
                interp,
                &args[0],
                4,
                Value::Float(0.8125),
            )?)
        }
        "hash-table-size" => {
            need_args(name, args, 1)?;
            let default_size = json::hash_table_entries(interp, &args[0])
                .map(|(_, entries)| Value::Integer(entries.len().max(65) as i64))
                .unwrap_or(Value::Integer(65));
            Ok(hash_table_metadata_slot(interp, &args[0], 2, default_size)?)
        }
        "hash-table-test" => {
            need_args(name, args, 1)?;
            Ok(hash_table_metadata_slot(
                interp,
                &args[0],
                0,
                Value::Symbol("eql".into()),
            )?)
        }
        "hash-table-weakness" => {
            need_args(name, args, 1)?;
            Ok(hash_table_metadata_slot(interp, &args[0], 5, Value::Nil)?)
        }
        "hash-table-keys" => {
            need_args(name, args, 1)?;
            let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(Value::list(entries.into_iter().map(|(key, _)| key)))
        }
        "try-completion" => try_completion(interp, args, env),
        "all-completions" => all_completions(interp, args, env),
        "test-completion" => test_completion(interp, args, env),
        "map-pairs" => {
            need_args(name, args, 1)?;
            let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(Value::list(
                entries
                    .into_iter()
                    .map(|(key, value)| Value::cons(key, value)),
            ))
        }
        "internal--hash-table-index-size" => {
            need_args(name, args, 1)?;
            let default_size = json::hash_table_entries(interp, &args[0])
                .map(|(_, entries)| Value::Integer(entries.len().max(65) as i64))
                .unwrap_or(Value::Integer(65));
            Ok(hash_table_metadata_slot(interp, &args[0], 2, default_size)?)
        }
        "internal--hash-table-histogram" => {
            need_args(name, args, 1)?;
            if json::hash_table_entries(interp, &args[0]).is_none() {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            }
            Ok(Value::Nil)
        }
        "internal--hash-table-buckets" => {
            need_args(name, args, 1)?;
            let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                return Err(LispError::TypeError(
                    "hash-table".into(),
                    args[0].type_name(),
                ));
            };
            Ok(Value::list(entries.into_iter().map(|(key, value)| {
                Value::list([Value::cons(key, value)])
            })))
        }
        "profiler-memory-running-p" => Ok(if interp.profiler_memory_running {
            Value::T
        } else {
            Value::Nil
        }),
        "profiler-memory-start" => {
            if interp.profiler_memory_running {
                return Err(LispError::Signal("Memory profiler already running".into()));
            }
            interp.profiler_memory_running = true;
            interp.profiler_memory_log_pending = true;
            Ok(Value::Nil)
        }
        "profiler-memory-stop" => {
            let was_running = interp.profiler_memory_running;
            interp.profiler_memory_running = false;
            Ok(if was_running { Value::T } else { Value::Nil })
        }
        "profiler-memory-log" => {
            if interp.profiler_memory_running || interp.profiler_memory_log_pending {
                if !interp.profiler_memory_running {
                    interp.profiler_memory_log_pending = false;
                }
                Ok(Value::String("#<hash-table>".into()))
            } else {
                Ok(Value::Nil)
            }
        }
        "profiler-cpu-running-p" => Ok(if interp.profiler_cpu_running {
            Value::T
        } else {
            Value::Nil
        }),
        "profiler-cpu-start" => {
            if interp.profiler_cpu_running {
                return Err(LispError::Signal("CPU profiler already running".into()));
            }
            interp.profiler_cpu_running = true;
            interp.profiler_cpu_log_pending = true;
            if let Some(interval) = args.first() {
                interp.set_variable(
                    "profiler-sampling-interval",
                    interval.clone(),
                    &mut Vec::new(),
                );
            }
            Ok(Value::Nil)
        }
        "profiler-cpu-stop" => {
            let was_running = interp.profiler_cpu_running;
            interp.profiler_cpu_running = false;
            Ok(if was_running { Value::T } else { Value::Nil })
        }
        "profiler-cpu-log" => {
            if interp.profiler_cpu_running || interp.profiler_cpu_log_pending {
                if !interp.profiler_cpu_running {
                    interp.profiler_cpu_log_pending = false;
                }
                Ok(Value::String("#<hash-table>".into()))
            } else {
                Ok(Value::Nil)
            }
        }
        "byte-compile-check-lambda-list" => {
            need_args(name, args, 1)?;
            validate_lambda_params(&args[0])?;
            Ok(Value::Nil)
        }
        "byte-compile" => {
            need_args(name, args, 1)?;
            if is_lambda_value(&args[0]) {
                validate_lambda_form(&args[0])?;
                return Ok(interp.create_record("byte-code-function", vec![args[0].clone()]));
            }
            if matches!(args[0], Value::Lambda(_, _, _)) {
                return Ok(interp.create_record("byte-code-function", vec![args[0].clone()]));
            }
            Ok(args[0].clone())
        }
        "funcall-with-delayed-message" => {
            need_args(name, args, 3)?;
            let timeout = numeric_to_f64(interp, &args[0])?;
            let delayed = string_text(&args[1])?;
            let callback = resolve_callable(interp, &args[2], env)?;
            let buffer_id = interp
                .find_buffer("*Messages*")
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer("*Messages*").0);
            let before = interp
                .get_buffer_by_id(buffer_id)
                .map(|buffer| buffer.buffer_string())
                .unwrap_or_default();
            let start = Instant::now();
            let result = interp.call_function_value(callback, None, &[], env)?;
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed >= timeout
                && let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id)
            {
                let current = buffer.buffer_string();
                let suffix = current
                    .strip_prefix(&before)
                    .map(str::to_string)
                    .unwrap_or(current);
                let rewritten = if suffix.is_empty() {
                    format!("{delayed}\n")
                } else {
                    format!("{delayed}\n{suffix}")
                };
                let end = buffer.point_max();
                let _ = buffer.delete_region(1, end);
                buffer.goto_char(1);
                buffer.insert(&(before + &rewritten));
            }
            Ok(result)
        }
        "handler-bind-1" => {
            if args.len() != 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let thunk = resolve_callable(interp, &args[0], env)?;
            let condition = args[1].as_symbol()?.to_string();
            let handler = resolve_callable(interp, &args[2], env)?;
            match interp.call_function_value(thunk, None, &[], env) {
                Ok(value) => Ok(value),
                Err(error) => {
                    let error_type = error.condition_type();
                    if condition != "error" && condition != error_type {
                        return Err(error);
                    }
                    let error_value = error_condition_value(&error);
                    let _ = interp.call_function_value(
                        handler,
                        None,
                        std::slice::from_ref(&error_value),
                        env,
                    )?;
                    Err(LispError::SignalValue(error_value))
                }
            }
        }
        "debugger-trap" => Ok(Value::Nil),
        "backtrace-frame--internal" => {
            need_args(name, args, 3)?;
            let callback = resolve_callable(interp, &args[0], env)?;
            let Some((function, frame_args, debug_on_exit)) = interp.current_backtrace_frame()
            else {
                return Ok(Value::Nil);
            };
            let flags = if debug_on_exit {
                Value::list([Value::Symbol(":debug-on-exit".into()), Value::T])
            } else {
                Value::Nil
            };
            interp.call_function_value(
                callback,
                None,
                &[Value::T, function, Value::list(frame_args), flags],
                env,
            )
        }
        "backtrace-debug" => {
            need_arg_range(name, args, 2, 3)?;
            interp.set_current_backtrace_debug(args[1].is_truthy());
            Ok(Value::Nil)
        }
        "backtrace-eval" => {
            need_args(name, args, 3)?;
            interp.lookup(args[0].as_symbol()?, env)
        }
        "backtrace--locals" => {
            need_args(name, args, 2)?;
            let mut locals = Vec::new();
            for frame in env.iter().rev() {
                for (name, value) in frame.iter().rev() {
                    if !locals.iter().any(|entry: &Value| {
                        entry.to_vec().ok().and_then(|items| items.first().cloned())
                            == Some(Value::Symbol(name.clone()))
                    }) {
                        locals.push(Value::cons(Value::Symbol(name.clone()), value.clone()));
                    }
                }
            }
            for name in interp.special_variable_names() {
                if locals.iter().any(|entry: &Value| {
                    entry.to_vec().ok().and_then(|items| items.first().cloned())
                        == Some(Value::Symbol(name.clone()))
                }) {
                    continue;
                }
                if let Some(value) = interp.lookup_var(&name, env) {
                    locals.push(Value::cons(Value::Symbol(name), value));
                }
            }
            Ok(Value::list(locals))
        }
        "current-thread" => Ok(interp.current_thread_value()),
        "all-threads" => {
            need_args(name, args, 0)?;
            Ok(Value::list(interp.live_threads()))
        }
        "make-thread" => {
            need_arg_range(name, args, 1, 3)?;
            let thread_name = args.get(1).and_then(|value| {
                if value.is_nil() {
                    None
                } else {
                    string_like(value).map(|string| string.text)
                }
            });
            let disposition = match args.get(2) {
                None | Some(Value::Nil) => BufferDisposition::Default,
                Some(Value::T) => BufferDisposition::Preserve,
                Some(Value::Symbol(symbol)) if symbol == "silently" => BufferDisposition::Silently,
                Some(other) => {
                    return Err(LispError::TypeError(
                        "thread-buffer-disposition".into(),
                        other.type_name(),
                    ));
                }
            };
            interp.make_thread(args[0].clone(), thread_name, disposition)
        }
        "thread-live-p" => {
            need_args(name, args, 1)?;
            Ok(if interp.thread_live(interp.resolve_thread_id(&args[0])?) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "thread-join" => {
            need_args(name, args, 1)?;
            interp.thread_join(interp.resolve_thread_id(&args[0])?, env)
        }
        "thread-name" => {
            need_args(name, args, 1)?;
            Ok(interp
                .thread_name(interp.resolve_thread_id(&args[0])?)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "thread-signal" => {
            need_args(name, args, 3)?;
            interp.signal_thread(
                interp.resolve_thread_id(&args[0])?,
                args[1].clone(),
                args[2].clone(),
                env,
            )
        }
        "thread-last-error" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(interp.thread_last_error(args.first().is_some_and(Value::is_truthy)))
        }
        "thread-yield" => {
            need_args(name, args, 0)?;
            interp.drive_threads(env, false)?;
            Ok(Value::Nil)
        }
        "make-mutex" => {
            need_arg_range(name, args, 0, 1)?;
            let mutex_name = args.first().and_then(|value| {
                if value.is_nil() {
                    None
                } else {
                    string_like(value).map(|string| string.text)
                }
            });
            Ok(interp.make_mutex(mutex_name))
        }
        "mutex-lock" => {
            need_args(name, args, 1)?;
            interp.lock_mutex_for_current_thread(interp.resolve_mutex_id(&args[0])?, env)
        }
        "mutex-unlock" => {
            need_args(name, args, 1)?;
            interp.unlock_mutex_for_current_thread(interp.resolve_mutex_id(&args[0])?)
        }
        "make-condition-variable" => {
            need_arg_range(name, args, 1, 2)?;
            let mutex_id = interp.resolve_mutex_id(&args[0])?;
            let condvar_name = args.get(1).and_then(|value| {
                if value.is_nil() {
                    None
                } else {
                    string_like(value).map(|string| string.text)
                }
            });
            Ok(interp.make_condition_variable(mutex_id, condvar_name))
        }
        "condition-mutex" => {
            need_args(name, args, 1)?;
            let condvar_id = interp.resolve_condition_variable_id(&args[0])?;
            let mutex_id = interp
                .condition_variable_mutex_id(condvar_id)
                .ok_or_else(|| {
                    LispError::TypeError("condition-variable-p".into(), args[0].type_name())
                })?;
            Ok(Value::Record(mutex_id))
        }
        "condition-name" => {
            need_args(name, args, 1)?;
            Ok(interp
                .condition_variable_name(interp.resolve_condition_variable_id(&args[0])?)
                .map(Value::String)
                .unwrap_or(Value::Nil))
        }
        "condition-notify" => {
            need_arg_range(name, args, 1, 2)?;
            interp.notify_condition_variable(
                interp.resolve_condition_variable_id(&args[0])?,
                args.get(1).is_some_and(Value::is_truthy),
            );
            Ok(Value::Nil)
        }
        "thread--blocker" => {
            need_args(name, args, 1)?;
            Ok(interp.thread_blocker_value(interp.resolve_thread_id(&args[0])?))
        }
        "thread-buffer-disposition" => {
            need_args(name, args, 1)?;
            interp.thread_buffer_disposition(interp.resolve_thread_id(&args[0])?)
        }
        "thread-set-buffer-disposition" => {
            need_args(name, args, 2)?;
            interp.set_thread_buffer_disposition(interp.resolve_thread_id(&args[0])?, &args[1])
        }
        "backtrace--frames-from-thread" => {
            need_args(name, args, 1)?;
            let frames = interp
                .thread_backtrace_frames_snapshot(interp.resolve_thread_id(&args[0])?)
                .into_iter()
                .map(|(function, frame_args, _debug_on_exit)| {
                    let mut items = vec![Value::T, function];
                    items.extend(frame_args);
                    Value::list(items)
                })
                .collect::<Vec<_>>();
            Ok(Value::list(frames))
        }
        "list-threads" => {
            need_args(name, args, 0)?;
            let buffer_id = interp
                .find_buffer("*Threads*")
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer("*Threads*").0);
            let mut text = String::from("Thread Name\tStatus\tBlocked On\n");
            for thread in interp.live_threads() {
                let thread_id = interp.resolve_thread_id(&thread)?;
                text.push_str(&thread_list_row(interp, thread_id, env)?);
            }
            replace_buffer_contents(interp, buffer_id, &text)?;
            interp.switch_to_buffer_id(buffer_id)?;
            Ok(Value::Buffer(buffer_id, interp.buffer.name.clone()))
        }
        "thread-list-send-error-signal" => {
            need_args(name, args, 0)?;
            let thread_id = thread_list_thread_at_point(interp)?;
            interp.signal_thread(thread_id, Value::Symbol("error".into()), Value::Nil, env)
        }
        "thread-list-pop-to-backtrace" => {
            need_args(name, args, 0)?;
            let thread_id = thread_list_thread_at_point(interp)?;
            let thread_name = interp
                .thread_name(thread_id)
                .unwrap_or_else(|| format!("#<thread id:{thread_id}>"));
            let buffer_id = interp
                .find_buffer("*Thread Backtrace*")
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer("*Thread Backtrace*").0);
            let mut text = format!("Backtrace for thread `{thread_name}':\n");
            for (function, frame_args, _) in interp.thread_backtrace_frames_snapshot(thread_id) {
                text.push_str(&render_prin1_ephemeral(interp, &function, env)?);
                for arg in frame_args {
                    text.push(' ');
                    text.push_str(&render_prin1_ephemeral(interp, &arg, env)?);
                }
                text.push('\n');
            }
            replace_buffer_contents(interp, buffer_id, &text)?;
            interp.switch_to_buffer_id(buffer_id)?;
            Ok(Value::Buffer(buffer_id, interp.buffer.name.clone()))
        }
        "regexp-quote" => {
            need_args(name, args, 1)?;
            Ok(Value::String(regexp::regexp_quote_elisp(&string_text(
                &args[0],
            )?)))
        }
        "regexp-opt" => {
            need_arg_range(name, args, 1, 2)?;
            let strings = args[0].to_vec()?;
            let mut patterns = strings
                .iter()
                .map(|value| string_text(value).map(|text| regexp::regexp_quote_elisp(&text)))
                .collect::<Result<Vec<_>, _>>()?;
            if patterns.is_empty() {
                return Ok(Value::String(String::new()));
            }
            patterns.sort();
            patterns.dedup();
            Ok(Value::String(if patterns.len() == 1 {
                patterns[0].clone()
            } else {
                format!("\\(?:{}\\)", patterns.join("\\|"))
            }))
        }
        "rx-to-string" => {
            need_arg_range(name, args, 1, 2)?;
            let no_group = args.get(1).is_some_and(Value::is_truthy);
            Ok(Value::String(crate::lisp::eval::compile_rx_to_string(
                interp, &args[0], env, no_group,
            )?))
        }
        "convert-standard-filename" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "abbreviate-file-name" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "files--name-absolute-system-p" => {
            need_args(name, args, 1)?;
            let path = string_text(&args[0])?;
            Ok(if file_name_absolute_p(&path) && !path.starts_with('~') {
                Value::T
            } else {
                Value::Nil
            })
        }
        "files--use-insert-directory-program-p" => {
            need_args(name, args, 0)?;
            Ok(
                if interp
                    .lookup_var("ls-lisp-use-insert-directory-program", env)
                    .is_some_and(|value| value.is_truthy())
                    && interp
                        .lookup_var("insert-directory-program", env)
                        .is_some_and(|value| value.is_truthy())
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "insert-directory-wildcard-in-dir-p" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "connection-local-value" => {
            need_arg_range(name, args, 1, 2)?;
            Ok(args[0].clone())
        }
        "propertized-buffer-identification" => {
            need_args(name, args, 1)?;
            Ok(Value::list([args[0].clone()]))
        }
        "called-interactively-p" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "kill-all-local-variables" => {
            need_args(name, args, 0)?;
            for (name, _) in interp.buffer_local_variables(interp.current_buffer_id()) {
                interp.notify_variable_watchers(
                    &name,
                    Value::Nil,
                    "makunbound",
                    Some(interp.current_buffer_id()),
                    env,
                )?;
            }
            interp.clear_buffer_local_state(interp.current_buffer_id());
            Ok(Value::Nil)
        }
        "hack-local-variables-filter" => {
            need_args(name, args, 2)?;
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "file-local-variables-alist",
                args[0].clone(),
            );
            Ok(args[0].clone())
        }
        "hack-local-variables-apply" => {
            need_args(name, args, 0)?;
            let pending = interp
                .buffer_local_value(interp.current_buffer_id(), "file-local-variables-alist")
                .or_else(|| interp.lookup_var("file-local-variables-alist", env))
                .unwrap_or(Value::Nil);
            for entry in pending.to_vec()? {
                let Some((variable, value)) = entry.cons_values() else {
                    continue;
                };
                let symbol = variable.as_symbol()?.to_string();
                let prepared = interp.prepare_variable_assignment(&symbol, value)?;
                interp.set_buffer_local_value(interp.current_buffer_id(), &symbol, prepared);
            }
            Ok(Value::Nil)
        }
        "hack-dir-local-variables-non-file-buffer" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
        "force-mode-line-update" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "garbage-collect" => {
            need_args(name, args, 0)?;
            collect_weak_hash_tables(interp)?;
            Ok(Value::Nil)
        }
        "num-processors" => {
            need_args(name, args, 0)?;
            let count = std::thread::available_parallelism()
                .map(|count| count.get() as i64)
                .unwrap_or(1);
            Ok(Value::Integer(count.max(1)))
        }
        "current-cpu-time" => {
            need_args(name, args, 0)?;
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            Ok(Value::list([normalize_bigint_value(BigInt::from(nanos))]))
        }
        "emacs-pid" => {
            need_args(name, args, 0)?;
            Ok(Value::Integer(emacs_pid_value()))
        }
        "type-of" => {
            need_args(name, args, 1)?;
            let name = match &args[0] {
                Value::Nil => "symbol",
                Value::T => "symbol",
                Value::Integer(_) => "integer",
                Value::BigInteger(_) => "integer",
                Value::Float(_) => "float",
                Value::String(_) => "string",
                Value::StringObject(_) => "string",
                Value::Symbol(_) => "symbol",
                Value::Cons(_, _) if is_vector_value(&args[0]) => "vector",
                Value::Cons(_, _) => "cons",
                Value::BuiltinFunc(_) => "subr",
                Value::Lambda(_, _, _) => "cons", // Emacs closures are cons cells
                Value::Buffer(_, _) => "buffer",
                Value::Marker(_) => "marker",
                Value::Overlay(_) => "overlay",
                Value::CharTable(_) => "char-table",
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    return Ok(Value::Symbol(record.type_name.clone()));
                }
                Value::Finalizer(_) => "finalizer",
            };
            Ok(Value::Symbol(name.into()))
        }
        "cl-type-of" => {
            need_args(name, args, 1)?;
            Ok(Value::Symbol(cl_type_name(interp, &args[0])?.into()))
        }
        "cl-find-class" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(if let Some(class_value) = interp.class_value(symbol) {
                class_value
            } else if is_builtin_class_name(symbol) {
                Value::Symbol(symbol.into())
            } else {
                Value::Nil
            })
        }
        "cl--class-parents" => {
            need_args(name, args, 1)?;
            interp.class_parents_value(&args[0])
        }
        "cl--class-allparents" => {
            need_args(name, args, 1)?;
            let Some(symbol) = interp.class_name_from_value(&args[0]) else {
                return Err(LispError::TypeError("class".into(), args[0].type_name()));
            };
            Ok(if interp.class_value(&symbol).is_some() {
                Value::list(interp.class_allparents(&symbol))
            } else if symbol == "t" {
                Value::list([Value::Symbol("t".into())])
            } else {
                Value::list([Value::Symbol(symbol), Value::Symbol("t".into())])
            })
        }
        "cl--class-children" => {
            need_args(name, args, 1)?;
            let Some(symbol) = interp.class_name_from_value(&args[0]) else {
                return Err(LispError::TypeError("class".into(), args[0].type_name()));
            };
            Ok(Value::list(interp.class_children(&symbol)))
        }
        "eieio--object-class" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Record(id) => interp
                    .find_record(*id)
                    .map(|record| Value::Symbol(record.type_name.clone()))
                    .ok_or_else(|| {
                        LispError::TypeError("eieio-object".into(), args[0].type_name())
                    }),
                _ => Err(LispError::TypeError(
                    "eieio-object".into(),
                    args[0].type_name(),
                )),
            }
        }
        "eieio--class-name" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Symbol(symbol) => Ok(Value::Symbol(symbol.clone())),
                Value::Record(id) => interp
                    .find_record(*id)
                    .map(|record| Value::Symbol(record.type_name.clone()))
                    .ok_or_else(|| LispError::TypeError("class".into(), args[0].type_name())),
                _ => Err(LispError::TypeError("class".into(), args[0].type_name())),
            }
        }
        "eieio-object-p" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::Record(id) if interp.find_record(*id).is_some()) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "slot-boundp" => {
            need_args(name, args, 2)?;
            let slot_name = args[1].as_symbol()?;
            match &args[0] {
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("eieio-object-p".into(), args[0].type_name())
                    })?;
                    let slots = eieio_slot_specs(interp, &record.type_name)?;
                    Ok(if eieio_slot_index(&slots, slot_name).is_some() {
                        Value::T
                    } else {
                        Value::Nil
                    })
                }
                _ => Err(LispError::TypeError(
                    "eieio-object-p".into(),
                    args[0].type_name(),
                )),
            }
        }
        "make-instance" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                return Err(LispError::TypeError("class".into(), args[0].type_name()));
            };
            make_eieio_instance(interp, &class_name, &args[1..], false, env)
        }
        "emaxx-class-make" => {
            need_args(name, args, 2)?;
            let class_name = args[0].as_symbol()?;
            let initargs = args[1].to_vec()?;
            make_eieio_instance(interp, class_name, &initargs, true, env)
        }
        "eieio-oref" | "slot-value" => {
            need_args(name, args, 2)?;
            let slot_name = args[1].as_symbol()?;
            eieio_slot_value(interp, &args[0], slot_name)
        }
        "eieio-oset" => {
            need_args(name, args, 3)?;
            let slot_name = args[1].as_symbol()?;
            set_eieio_slot_value(interp, &args[0], slot_name, args[2].clone())
        }
        "built-in-class-p" => {
            need_args(name, args, 1)?;
            Ok(
                if args[0].as_symbol().ok().is_some_and(is_builtin_class_name) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "cl-typep" => {
            need_args(name, args, 2)?;
            let target = args[1].as_symbol()?;
            let actual = cl_type_name(interp, &args[0])?;
            let matches = target == "t"
                || (target == "list" && args[0].is_list())
                || (target == "eieio-object" && matches!(args[0], Value::Record(_)))
                || target == actual
                || (!is_builtin_class_name(target)
                    && interp.value_is_instance_of_class(&args[0], target))
                || (target == "function"
                    && matches!(
                        actual,
                        "primitive-function"
                            | "special-form"
                            | "interpreted-function"
                            | "byte-code-function"
                    ));
            Ok(if matches { Value::T } else { Value::Nil })
        }
        "cl-functionp" => {
            need_args(name, args, 1)?;
            Ok(
                if is_lambda_expression(&args[0])
                    || matches!(
                        cl_type_name(interp, &args[0])?,
                        "primitive-function"
                            | "special-form"
                            | "interpreted-function"
                            | "byte-code-function"
                    )
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "cl-proclaim" => {
            need_args(name, args, 1)?;
            let existing = interp
                .lookup_var("cl--proclaims-deferred", env)
                .unwrap_or(Value::Nil);
            interp.set_global_binding(
                "cl--proclaims-deferred",
                Value::cons(args[0].clone(), existing),
            );
            Ok(Value::Nil)
        }
        "url-scheme-get-property" => {
            need_args(name, args, 2)?;
            let scheme = match &args[0] {
                Value::Symbol(symbol) => symbol.clone(),
                _ => string_text(&args[0])?,
            }
            .to_ascii_lowercase();
            let property = args[1].as_symbol()?;
            let value = match property {
                "default-port" => match scheme.as_str() {
                    "ftp" => Value::Integer(21),
                    "http" => Value::Integer(80),
                    "https" => Value::Integer(443),
                    "imap" => Value::Integer(143),
                    "ldap" => Value::Integer(389),
                    "nntp" => Value::Integer(119),
                    "pop" | "pop3" => Value::Integer(110),
                    "smtp" => Value::Integer(25),
                    "telnet" => Value::Integer(23),
                    _ => Value::Integer(0),
                },
                "name" => {
                    if scheme.is_empty() {
                        Value::String("unknown".into())
                    } else {
                        Value::String(scheme)
                    }
                }
                "loader" => {
                    if scheme.is_empty() {
                        Value::Symbol("url-scheme-default-loader".into())
                    } else {
                        Value::Symbol(format!("url-{scheme}"))
                    }
                }
                "parse-url" => Value::Symbol("url-generic-parse-url".into()),
                "asynchronous-p" => Value::Nil,
                "file-directory-p" => Value::Symbol("ignore".into()),
                _ => Value::Nil,
            };
            Ok(value)
        }

        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}
