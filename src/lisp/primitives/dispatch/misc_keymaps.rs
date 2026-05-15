use super::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

thread_local! {
    static SEMANTIC_CPP_INCLUDE_TAG_CACHE: RefCell<HashMap<PathBuf, Vec<Value>>> =
        RefCell::new(HashMap::new());
}

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
            | "clone"
            | "semanticdb-find-tags-by-class"
            | "semanticdb-find-tags-by-name"
            | "semanticdb-find-tags-for-completion"
            | "semantic-ctxt-current-symbol"
            | "semantic-ctxt-current-symbol-and-bounds"
            | "semantic-analyze-possible-completions"
            | "semantic-analyze-tag-references"
            | "semantic-analyze-refs-impl"
            | "semantic-analyze-refs-proto"
            | "semantic-equivalent-tag-p"
            | "semantic-go-to-tag"
            | "semantic-clear-toplevel-cache"
            | "semanticdb-typecache-find"
            | "semanticdb-typecache-add-dependant"
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
                Value::Unbound => "unbound",
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
                    let bound = eieio_slot_index(&slots, slot_name)
                        .and_then(|slot_index| record.slots.get(slot_index))
                        .is_some_and(|value| !matches!(value, Value::Unbound));
                    Ok(if bound { Value::T } else { Value::Nil })
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
        "clone" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            clone_eieio_instance(interp, &args[0], &args[1..])
        }
        "semanticdb-find-tags-by-class" => {
            need_arg_range(name, args, 1, 3)?;
            semanticdb_find_tags_by_class(interp, args, env)
        }
        "semanticdb-find-tags-by-name" => {
            need_arg_range(name, args, 1, 3)?;
            semanticdb_find_tags_by_name(interp, args, env)
        }
        "semanticdb-find-tags-for-completion" => {
            need_arg_range(name, args, 1, 3)?;
            semanticdb_find_tags_for_completion(interp, args, env)
        }
        "semantic-ctxt-current-symbol" => {
            need_args(name, args, 0)?;
            Ok(semantic_ctxt_current_symbol(interp)
                .map(|symbol| symbol.parts_value)
                .unwrap_or(Value::Nil))
        }
        "semantic-ctxt-current-symbol-and-bounds" => {
            need_args(name, args, 0)?;
            Ok(if let Some(symbol) = semantic_ctxt_current_symbol(interp) {
                Value::list([
                    symbol.parts_value,
                    Value::String(symbol.text),
                    Value::cons(
                        Value::Integer(symbol.start as i64),
                        Value::Integer(symbol.end as i64),
                    ),
                ])
            } else {
                Value::list([Value::Nil, Value::Nil, Value::Nil])
            })
        }
        "semantic-analyze-possible-completions" => {
            need_args(name, args, 1)?;
            semantic_analyze_possible_completions(interp, env)
        }
        "semantic-analyze-tag-references" => {
            need_args(name, args, 1)?;
            semantic_analyze_tag_references(interp, &args[0], env)
        }
        "semantic-analyze-refs-impl" => {
            need_arg_range(name, args, 1, 2)?;
            semantic_analyze_refs_part(&args[0], 1)
        }
        "semantic-analyze-refs-proto" => {
            need_arg_range(name, args, 1, 2)?;
            semantic_analyze_refs_part(&args[0], 2)
        }
        "semantic-equivalent-tag-p" => {
            need_args(name, args, 2)?;
            Ok(if semantic_tags_equivalent(&args[0], &args[1]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "semantic-go-to-tag" => {
            need_arg_range(name, args, 1, 2)?;
            semantic_go_to_tag(interp, &args[0])
        }
        "semantic-clear-toplevel-cache" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "semanticdb-typecache-find" => {
            need_arg_range(name, args, 1, 3)?;
            semanticdb_typecache_find(interp, args, env)
        }
        "semanticdb-typecache-add-dependant" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
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

struct SemanticCurrentSymbol {
    parts_value: Value,
    text: String,
    start: usize,
    end: usize,
}

fn semantic_ctxt_current_symbol(interp: &Interpreter) -> Option<SemanticCurrentSymbol> {
    let point = interp.buffer.point();
    if point <= interp.buffer.point_min() {
        return None;
    }
    let mut start = point;
    while start > interp.buffer.point_min() {
        let Some(ch) = interp.buffer.char_at(start - 1) else {
            break;
        };
        if !is_semantic_member_expr_char(ch) {
            break;
        }
        start -= 1;
    }
    if start == point {
        return None;
    }
    let text = interp.buffer.buffer_substring(start, point).ok()?;
    let parts = semantic_member_expression_parts(&text);
    if parts.is_empty() {
        return None;
    }
    let parts_value = Value::list(parts.into_iter().map(Value::String));
    Some(SemanticCurrentSymbol {
        parts_value,
        text,
        start,
        end: point,
    })
}

fn is_semantic_member_expr_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | ':' | '-' | '>')
}

fn semantic_member_expression_parts(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                parts.push(std::mem::take(&mut current));
            }
            ':' if chars.peek() == Some(&':') => {
                chars.next();
                parts.push(std::mem::take(&mut current));
            }
            '-' if chars.peek() == Some(&'>') => {
                chars.next();
                parts.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    parts.push(current);
    while parts.first().is_some_and(|part| part.is_empty()) {
        parts.remove(0);
    }
    parts
}

fn semantic_analyze_possible_completions(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some(symbol) = semantic_ctxt_current_symbol(interp) else {
        return Ok(Value::Nil);
    };
    let parts = symbol.parts_value.to_vec()?;
    let parts = parts
        .iter()
        .filter_map(|part| string_text(part).ok())
        .collect::<Vec<_>>();

    let table = interp
        .lookup_var("semanticdb-current-table", env)
        .unwrap_or(Value::Nil);
    if table.is_nil() {
        return Ok(Value::Nil);
    }
    let mut tags = semantic_tags_for_search(interp, &table)?;
    extend_semantic_c_like_table_tags(interp, &table, &mut tags);
    if parts.len() == 1 {
        let prefix = &parts[0];
        let mut matches = Vec::new();
        if let Some(current_type) = semantic_cpp_current_enclosing_type(interp, &tags)
            .or_else(|| semantic_c_like_current_enclosing_type(interp, &tags))
        {
            collect_semantic_member_completion_tags(
                &current_type,
                &tags,
                prefix,
                true,
                &mut matches,
            );
            if let Some(expected_type) = semantic_c_like_assignment_expected_type(interp) {
                matches.retain(|tag| {
                    semantic_tag_class(tag).as_deref() != Some("function")
                        || semantic_tag_attr(tag, ":type")
                            .and_then(|value| semantic_type_name_parts(&value).ok())
                            .is_some_and(|parts| parts.iter().any(|part| part == &expected_type))
                });
            }
        }
        if matches.is_empty() {
            collect_semantic_named_completion_tags(&tags, prefix, &mut matches);
        }
        return Ok(Value::list(unique_semantic_completion_tags(matches)));
    }

    let Some(mut current_type) = semantic_cpp_root_type(interp, &tags, &parts[0])
        .or_else(|| semantic_type_from_name(&tags, &parts[0]))
    else {
        return Ok(Value::Nil);
    };
    for member_name in &parts[1..parts.len() - 1] {
        let Some(member) = semantic_type_member_named(&current_type, member_name).or_else(|| {
            semantic_tag_name(&current_type).and_then(|type_name| {
                semantic_type_member_named_in_named_types(&tags, &type_name, member_name)
            })
        }) else {
            return Ok(Value::Nil);
        };
        let Some(member_type) = semantic_type_candidate(&tags, &member) else {
            return Ok(Value::Nil);
        };
        current_type = member_type;
    }
    let prefix = parts.last().map(String::as_str).unwrap_or("");
    let mut matches = Vec::new();
    collect_semantic_public_member_completion_tags(&current_type, prefix, &mut matches);
    Ok(Value::list(unique_semantic_completion_tags(matches)))
}

fn semantic_analyze_tag_references(
    interp: &mut Interpreter,
    tag: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if semantic_tag_class(tag).as_deref() != Some("function") {
        return Ok(Value::Nil);
    }
    let table = interp
        .lookup_var("semanticdb-current-table", env)
        .unwrap_or(Value::Nil);
    if table.is_nil() {
        return Ok(Value::Nil);
    }
    let tags = semantic_tags_for_search(interp, &table)?;
    let key = semantic_function_signature_key(tag);
    let mut impls = Vec::new();
    let mut protos = Vec::new();
    collect_semantic_function_references(&tags, &key, &mut impls, &mut protos);
    Ok(Value::list([
        Value::Symbol("emaxx-semantic-refs".into()),
        Value::list(impls),
        Value::list(protos),
    ]))
}

fn semantic_analyze_refs_part(refs: &Value, index: usize) -> Result<Value, LispError> {
    let refs = refs.to_vec()?;
    Ok(refs.get(index).cloned().unwrap_or(Value::Nil))
}

fn semantic_tags_equivalent(left: &Value, right: &Value) -> bool {
    let left_class = semantic_tag_class(left);
    if left_class != semantic_tag_class(right)
        || semantic_tag_name(left) != semantic_tag_name(right)
    {
        return false;
    }
    if left_class.as_deref() == Some("function") {
        return semantic_function_signature_matches(
            &semantic_function_signature_key(left),
            &semantic_function_signature_key(right),
        ) || semantic_function_signature_matches(
            &semantic_function_signature_key(right),
            &semantic_function_signature_key(left),
        );
    }
    true
}

fn semantic_go_to_tag(interp: &mut Interpreter, tag: &Value) -> Result<Value, LispError> {
    let items = tag.to_vec()?;
    if let Some(Value::Overlay(overlay_id)) = items.get(4)
        && let Some(overlay) = interp.find_overlay(*overlay_id)
    {
        interp.buffer.goto_char(overlay.beg);
    }
    Ok(tag.clone())
}

#[derive(Eq, PartialEq)]
struct SemanticFunctionSignatureKey {
    name: Option<String>,
    parent: Option<String>,
    arg_types: Vec<String>,
}

fn semantic_function_signature_key(tag: &Value) -> SemanticFunctionSignatureKey {
    SemanticFunctionSignatureKey {
        name: semantic_tag_name(tag),
        parent: semantic_tag_attr(tag, ":parent").and_then(|value| string_text(&value).ok()),
        arg_types: semantic_function_arg_types(tag),
    }
}

fn semantic_function_arg_types(tag: &Value) -> Vec<String> {
    semantic_tag_attr(tag, ":arguments")
        .and_then(|args| args.to_vec().ok())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|arg| semantic_tag_attr(&arg, ":type"))
        .filter_map(|type_value| {
            semantic_type_name_parts(&type_value)
                .ok()
                .map(|parts| parts.join("::"))
        })
        .collect()
}

fn collect_semantic_function_references(
    tags: &[Value],
    key: &SemanticFunctionSignatureKey,
    impls: &mut Vec<Value>,
    protos: &mut Vec<Value>,
) {
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("function")
            && semantic_function_signature_matches(&semantic_function_signature_key(tag), key)
        {
            if semantic_tag_attr(tag, ":prototype-flag").is_some_and(|value| value.is_truthy()) {
                protos.push(tag.clone());
            } else {
                impls.push(tag.clone());
            }
        }
        collect_semantic_function_references(&semantic_tag_members(tag), key, impls, protos);
    }
}

fn semantic_function_signature_matches(
    candidate: &SemanticFunctionSignatureKey,
    target: &SemanticFunctionSignatureKey,
) -> bool {
    candidate.name == target.name
        && candidate.arg_types == target.arg_types
        && (candidate.parent == target.parent || candidate.parent.is_none())
}

fn semantic_cpp_root_type(interp: &Interpreter, tags: &[Value], name: &str) -> Option<Value> {
    semantic_cpp_declared_type_before_point(interp, name)
        .and_then(|type_name| semantic_type_from_name(tags, &type_name))
        .or_else(|| {
            find_semantic_variable_deep(tags, name)
                .and_then(|tag| semantic_type_candidate(tags, &tag))
        })
}

fn semantic_cpp_current_enclosing_type(interp: &Interpreter, tags: &[Value]) -> Option<Value> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()?;
    for line in text.lines().rev() {
        let line = line.split("//").next().unwrap_or(line);
        let Some(scope_index) = line.rfind("::") else {
            continue;
        };
        let before_scope = line[..scope_index].trim_end();
        let class_name = before_scope
            .split_whitespace()
            .last()
            .map(|name| name.trim_matches(|ch| matches!(ch, '*' | '&')))?;
        if let Some(type_tag) = semantic_type_from_name(tags, class_name) {
            return Some(type_tag);
        }
    }
    None
}

fn semantic_c_like_current_enclosing_type(interp: &Interpreter, tags: &[Value]) -> Option<Value> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()?;
    let mut stack = Vec::new();
    let mut pending_class: Option<String> = None;
    let mut index = 0usize;
    let bytes = text.as_bytes();
    while index < bytes.len() {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'/') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
            continue;
        }
        if word_at(&text, index, "class") || word_at(&text, index, "interface") {
            index += if word_at(&text, index, "interface") {
                "interface".len()
            } else {
                "class".len()
            };
            while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
                index += 1;
            }
            let start = index;
            while bytes.get(index).is_some_and(|byte| is_ident_byte(*byte)) {
                index += 1;
            }
            if index > start {
                pending_class = Some(text[start..index].to_string());
            }
            continue;
        }
        match bytes[index] {
            b'{' => {
                if let Some(class_name) = pending_class.take() {
                    stack.push(class_name);
                } else {
                    stack.push(String::new());
                }
            }
            b'}' => {
                stack.pop();
            }
            _ => {}
        }
        index += 1;
    }
    stack
        .into_iter()
        .rev()
        .find(|name| !name.is_empty())
        .and_then(|name| semantic_type_from_name(tags, &name))
}

fn word_at(text: &str, index: usize, word: &str) -> bool {
    text[index..].starts_with(word)
        && (index == 0 || !is_ident_byte(text.as_bytes()[index - 1]))
        && text
            .as_bytes()
            .get(index + word.len())
            .is_none_or(|byte| !is_ident_byte(*byte))
}

fn semantic_cpp_declared_type_before_point(interp: &Interpreter, name: &str) -> Option<String> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()?;
    text.split([';', '(', ')', ',', '\n'])
        .rev()
        .filter_map(|segment| semantic_cpp_declared_type_from_segment(segment, name))
        .next()
}

fn semantic_c_like_assignment_expected_type(interp: &Interpreter) -> Option<String> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()?;
    let line = text.lines().last()?.split("//").next()?.trim_end();
    let eq_index = line.rfind('=')?;
    let lhs = line[..eq_index]
        .trim_end()
        .rsplit(|ch: char| !is_ident_byte(ch as u8))
        .find(|part| !part.is_empty())?;
    semantic_cpp_declared_type_before_point(interp, lhs)
}

fn semantic_cpp_declared_type_from_segment(segment: &str, name: &str) -> Option<String> {
    let segment = segment.split("//").next().unwrap_or(segment).trim();
    let index = segment.rfind(name)?;
    let before = &segment[..index];
    let after = &segment[index + name.len()..];
    if before
        .chars()
        .next_back()
        .is_some_and(|ch| is_ident_byte(ch as u8))
        || after
            .chars()
            .next()
            .is_some_and(|ch| is_ident_byte(ch as u8))
    {
        return None;
    }
    let after = after.trim_start();
    if !after.is_empty() && !matches!(after.chars().next(), Some(';' | ',' | ')' | '=')) {
        return None;
    }
    let before = before
        .trim_end_matches(|ch: char| ch.is_whitespace() || matches!(ch, '*' | '&'))
        .trim();
    let type_name = before
        .split_whitespace()
        .rev()
        .find(|token| {
            !matches!(
                *token,
                "const" | "struct" | "class" | "mutable" | "static" | "volatile"
            )
        })?
        .trim_matches(|ch| matches!(ch, '*' | '&'));
    (!type_name.is_empty()).then(|| type_name.to_string())
}

fn semantic_type_from_name(tags: &[Value], type_name: &str) -> Option<Value> {
    let parts = type_name
        .split("::")
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    find_semantic_type_chain(tags, &parts).or_else(|| {
        parts
            .last()
            .and_then(|name| find_semantic_type_deep(tags, name))
    })
}

fn find_semantic_variable_deep(tags: &[Value], name: &str) -> Option<Value> {
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("variable")
            && semantic_tag_name(tag).as_deref() == Some(name)
        {
            return Some(tag.clone());
        }
        if let Some(found) = find_semantic_variable_deep(&semantic_tag_members(tag), name) {
            return Some(found);
        }
    }
    None
}

fn semantic_type_member_named(type_tag: &Value, name: &str) -> Option<Value> {
    semantic_tag_members(type_tag)
        .into_iter()
        .find(|member| semantic_tag_name(member).as_deref() == Some(name))
}

fn semantic_type_member_named_in_named_types(
    tags: &[Value],
    type_name: &str,
    member_name: &str,
) -> Option<Value> {
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_name(tag).as_deref() == Some(type_name)
            && let Some(member) = semantic_type_member_named(tag, member_name)
        {
            return Some(member);
        }
    }
    None
}

fn collect_semantic_named_completion_tags(tags: &[Value], prefix: &str, matches: &mut Vec<Value>) {
    for tag in tags {
        if matches!(
            semantic_tag_class(tag).as_deref(),
            Some("function" | "variable" | "type")
        ) && semantic_tag_name(tag)
            .as_deref()
            .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(tag.clone());
        }
        collect_semantic_named_completion_tags(&semantic_tag_members(tag), prefix, matches);
    }
}

fn unique_semantic_completion_tags(tags: Vec<Value>) -> Vec<Value> {
    let mut names = Vec::new();
    let mut unique = Vec::new();
    for tag in tags {
        let Some(name) = semantic_tag_name(&tag) else {
            continue;
        };
        if names.iter().any(|existing| existing == &name) {
            continue;
        }
        names.push(name);
        unique.push(tag);
    }
    unique
}

fn collect_semantic_public_member_completion_tags(
    type_tag: &Value,
    prefix: &str,
    matches: &mut Vec<Value>,
) {
    collect_semantic_member_completion_tags(type_tag, &[], prefix, false, matches);
}

fn collect_semantic_member_completion_tags(
    type_tag: &Value,
    root_tags: &[Value],
    prefix: &str,
    include_private: bool,
    matches: &mut Vec<Value>,
) {
    let type_name = semantic_tag_name(type_tag);
    for member in semantic_tag_members(type_tag) {
        let class = semantic_tag_class(&member);
        if !include_private
            && class.as_deref() == Some("label")
            && semantic_tag_name(&member).as_deref() == Some("private")
        {
            break;
        }
        if !include_private && semantic_tag_has_typemodifier(&member, "private") {
            continue;
        }
        if !matches!(class.as_deref(), Some("function" | "variable" | "type")) {
            continue;
        }
        let Some(name) = semantic_tag_name(&member) else {
            continue;
        };
        if !name.starts_with(prefix)
            || type_name.as_deref() == Some(name.as_str())
            || name.starts_with('~')
        {
            continue;
        }
        matches.push(member);
    }
    if root_tags.is_empty() {
        return;
    }
    if let Some(superclasses) = semantic_tag_attr(type_tag, ":superclasses")
        .and_then(|superclasses| superclasses.to_vec().ok())
    {
        for superclass in superclasses {
            let Some(super_type) = semantic_type_candidate(root_tags, &superclass) else {
                continue;
            };
            collect_semantic_member_completion_tags(
                &super_type,
                root_tags,
                prefix,
                include_private,
                matches,
            );
        }
    }
}

fn semanticdb_find_tags_by_class(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let class = args[0].as_symbol()?;
    let path = args.get(1).cloned().unwrap_or(Value::Nil);
    if args.get(2).is_some_and(Value::is_truthy) {
        return Ok(Value::Nil);
    }

    let tables = semanticdb_search_tables(interp, Some(&path), env);

    let mut results = Vec::new();
    for table in tables {
        let tags = semantic_tags_for_search(interp, &table)?;
        let matches = tags
            .into_iter()
            .filter(|tag| semantic_tag_class(tag).as_deref() == Some(class))
            .collect::<Vec<_>>();
        if !matches.is_empty() {
            results.push(Value::cons(table, Value::list(matches)));
        }
    }

    Ok(Value::list(results))
}

fn semanticdb_find_tags_by_name(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let name = string_text(&args[0])?;
    if args.get(2).is_some_and(Value::is_truthy) {
        return Ok(Value::Nil);
    }
    let tables = semanticdb_search_tables(interp, args.get(1), env);
    let mut results = Vec::new();
    for table in tables {
        let tags = semantic_tags_for_search(interp, &table)?;
        let matches = tags
            .into_iter()
            .filter(|tag| semantic_tag_name(tag).as_deref() == Some(name.as_str()))
            .collect::<Vec<_>>();
        if !matches.is_empty() {
            results.push(Value::cons(table, Value::list(matches)));
        }
    }
    Ok(Value::list(results))
}

fn semanticdb_find_tags_for_completion(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let prefix = string_text(&args[0])?;
    if args.get(2).is_some_and(Value::is_truthy) {
        return Ok(Value::Nil);
    }
    let tables = semanticdb_search_tables(interp, args.get(1), env);
    let mut results = Vec::new();
    for table in tables {
        let tags = semantic_tags_for_search(interp, &table)?;
        let matches = semantic_completion_matches(&tags, &prefix);
        if !matches.is_empty() {
            results.push(Value::cons(table, Value::list(matches)));
        }
    }
    Ok(Value::list(results))
}

fn semanticdb_typecache_find(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.get(2).is_some_and(Value::is_truthy) {
        return Ok(Value::Nil);
    }
    let names = semantic_type_name_parts(&args[0])?;
    if names.is_empty() {
        return Ok(Value::Nil);
    }

    for table in semanticdb_search_tables(interp, args.get(1), env) {
        let tags = semantic_tags_for_search(interp, &table)?;
        let found = if names.len() == 1 {
            find_semantic_type_deep(&tags, &names[0])
                .map(|tag| resolve_semantic_typedef(&tags, &tag))
        } else {
            find_semantic_type_chain(&tags, &names)
        };
        if let Some(found) = found {
            return Ok(found);
        }
    }
    Ok(Value::Nil)
}

fn semanticdb_search_tables(
    interp: &mut Interpreter,
    path: Option<&Value>,
    env: &mut Env,
) -> Vec<Value> {
    match path {
        Some(record @ Value::Record(_)) => vec![record.clone()],
        Some(Value::Nil) | None => {
            let mut tables = interp
                .lookup_var("semanticdb-current-table", env)
                .into_iter()
                .collect::<Vec<_>>();
            if let Some(database) = interp.lookup_var("semanticdb-current-database", env)
                && let Ok(database_tables) = eieio_slot_value(interp, &database, "tables")
                && let Ok(database_tables) = database_tables.to_vec()
            {
                for table in database_tables {
                    if !tables.contains(&table) {
                        tables.push(table);
                    }
                }
            }
            tables
        }
        _ => Vec::new(),
    }
}

fn semantic_type_name_parts(value: &Value) -> Result<Vec<String>, LispError> {
    if let Ok(symbol) = value.as_symbol() {
        return Ok(vec![symbol.to_string()]);
    }
    if let Ok(name) = string_text(value) {
        return Ok(name
            .split("::")
            .filter(|part| !part.is_empty())
            .map(str::to_string)
            .collect());
    }
    let Ok(items) = value.to_vec() else {
        return Ok(Vec::new());
    };
    if matches!(items.get(1), Some(Value::Symbol(class)) if class == "type")
        && let Some(name) = items.first()
    {
        return semantic_type_name_parts(name);
    }
    Ok(items
        .into_iter()
        .filter_map(|part| {
            part.as_symbol()
                .map(str::to_string)
                .or_else(|_| string_text(&part))
                .ok()
        })
        .collect())
}

fn semantic_tags_for_search(
    interp: &mut Interpreter,
    table: &Value,
) -> Result<Vec<Value>, LispError> {
    let tags = match eieio_slot_value(interp, table, "tags") {
        Ok(tags) => tags,
        Err(_) => return Ok(Vec::new()),
    };
    let mut tags = match tags.to_vec() {
        Ok(tags) => tags,
        Err(LispError::TypeError(expected, _)) if expected == "list" => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let include_tags = tags
        .iter()
        .filter(|tag| semantic_tag_class(tag).as_deref() == Some("include"))
        .cloned()
        .collect::<Vec<_>>();
    for include_tag in include_tags {
        let Some(path) = semantic_include_path(interp, table, &include_tag) else {
            continue;
        };
        tags.extend(cached_semantic_cpp_tags(&path));
    }
    Ok(tags)
}

fn extend_semantic_c_like_table_tags(
    interp: &mut Interpreter,
    table: &Value,
    tags: &mut Vec<Value>,
) {
    if let Some(path) = semantic_table_file_path(interp, table)
        && matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("c" | "cc" | "cpp" | "cxx" | "h" | "hh" | "hpp" | "java")
        )
    {
        append_semantic_search_tags(tags, cached_semantic_cpp_tags(&path));
    }
}

fn append_semantic_search_tags(tags: &mut Vec<Value>, candidates: Vec<Value>) {
    for candidate in candidates {
        if semantic_tag_class(&candidate).is_some() {
            tags.push(candidate);
        } else if let Ok(items) = candidate.to_vec() {
            append_semantic_search_tags(tags, items);
        }
    }
}

fn cached_semantic_cpp_tags(path: &Path) -> Vec<Value> {
    SEMANTIC_CPP_INCLUDE_TAG_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let cached = cache.entry(path.to_path_buf()).or_insert_with(|| {
            std::fs::read_to_string(path)
                .map(|source| parse_semantic_cpp_tags(&source))
                .unwrap_or_default()
        });
        cached.iter().map(deep_copy_semantic_value).collect()
    })
}

fn deep_copy_semantic_value(value: &Value) -> Value {
    match value {
        Value::Cons(car, cdr) => Value::cons(
            deep_copy_semantic_value(&car.borrow()),
            deep_copy_semantic_value(&cdr.borrow()),
        ),
        _ => value.clone(),
    }
}

fn semantic_include_path(
    interp: &mut Interpreter,
    table: &Value,
    include_tag: &Value,
) -> Option<PathBuf> {
    let include = semantic_tag_name(include_tag)?;
    let include_path = Path::new(&include);
    if include_path.is_absolute() && include_path.exists() {
        return Some(include_path.to_path_buf());
    }
    let table_file = semantic_table_file_path(interp, table)?;
    let table_path = table_file.as_path();
    let base = if table_path.is_absolute() {
        table_path.parent()?.to_path_buf()
    } else {
        let database = interp.lookup_var("semanticdb-current-database", &Vec::new())?;
        let directory = eieio_slot_value(interp, &database, "reference-directory")
            .ok()
            .and_then(|value| string_text(&value).ok())?;
        Path::new(&directory).to_path_buf()
    };
    let candidate = base.join(include_path);
    candidate.exists().then_some(candidate)
}

fn semantic_table_file_path(interp: &mut Interpreter, table: &Value) -> Option<PathBuf> {
    let path = eieio_slot_value(interp, table, "file")
        .ok()
        .and_then(|value| string_text(&value).ok())
        .map(PathBuf::from)?;
    if path.is_absolute() {
        return Some(path);
    }
    interp
        .lookup_var("semanticdb-current-database", &Vec::new())
        .and_then(|database| {
            eieio_slot_value(interp, &database, "reference-directory")
                .ok()
                .and_then(|value| string_text(&value).ok())
        })
        .map(|directory| Path::new(&directory).join(&path))
        .or(Some(path))
}

fn parse_semantic_cpp_tags(source: &str) -> Vec<Value> {
    let cleaned = strip_cpp_comments(source);
    let mut parser = CppTagParser::new(&cleaned);
    parser.parse_until(None)
}

fn strip_cpp_comments(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '/' && chars.peek() == Some(&'/') {
            for next in chars.by_ref() {
                if next == '\n' {
                    out.push('\n');
                    break;
                }
            }
        } else if ch == '/' && chars.peek() == Some(&'*') {
            chars.next();
            let mut previous = '\0';
            for next in chars.by_ref() {
                if next == '\n' {
                    out.push('\n');
                }
                if previous == '*' && next == '/' {
                    break;
                }
                previous = next;
            }
        } else {
            out.push(ch);
        }
    }
    out
}

struct CppTagParser<'a> {
    source: &'a str,
    pos: usize,
}

impl<'a> CppTagParser<'a> {
    fn new(source: &'a str) -> Self {
        Self { source, pos: 0 }
    }

    fn parse_until(&mut self, terminator: Option<u8>) -> Vec<Value> {
        let mut tags = Vec::new();
        while self.pos < self.source.len() {
            self.skip_ws();
            if terminator.is_some_and(|term| self.peek_byte() == Some(term)) {
                self.pos += 1;
                break;
            }
            if let Some(tag) = self.parse_namespace() {
                tags.push(tag);
            } else if let Some(tag) = self.parse_type_block() {
                tags.push(tag);
            } else if let Some(tag) = self.parse_statement() {
                tags.push(tag);
            } else {
                self.pos += 1;
            }
        }
        tags
    }

    fn parse_namespace(&mut self) -> Option<Value> {
        let start = self.pos;
        self.consume_word("namespace")?;
        self.skip_ws();
        let name = self.read_ident()?;
        self.skip_until_byte(b'{')?;
        self.pos += 1;
        let members = self.parse_until(Some(b'}'));
        semantic_type_tag(
            &name,
            vec![
                (":members", Value::list(members)),
                (":type", Value::String("namespace".into())),
            ],
        )
        .or_else(|| {
            self.pos = start;
            None
        })
    }

    fn parse_type_block(&mut self) -> Option<Value> {
        let start = self.pos;
        while self
            .consume_one_of_words(&[
                "public",
                "private",
                "protected",
                "static",
                "final",
                "abstract",
                "strictfp",
            ])
            .is_some()
        {
            self.skip_ws();
        }
        let kind = if self.consume_word("class").is_some() {
            "class"
        } else if self.consume_word("struct").is_some() {
            "struct"
        } else if self.consume_word("interface").is_some() {
            "interface"
        } else {
            self.pos = start;
            return None;
        };
        self.skip_ws();
        let name = self.read_ident()?;
        self.skip_until_byte(b'{')?;
        self.pos += 1;
        let members = self.parse_until(Some(b'}'));
        let variable_name = self.read_trailing_decl_name();
        if variable_name.is_some() {
            self.consume_optional_statement_tail();
        }
        let mut tags = vec![semantic_type_tag(
            &name,
            vec![
                (":members", Value::list(members)),
                (":type", Value::String(kind.into())),
            ],
        )?];
        if let Some(variable_name) = variable_name {
            tags.insert(
                0,
                semantic_variable_tag(&variable_name, semantic_type_ref(&name), false),
            );
        }
        if tags.len() == 1 {
            tags.pop()
        } else {
            Some(Value::list(tags))
        }
        .or_else(|| {
            self.pos = start;
            None
        })
    }

    fn parse_statement(&mut self) -> Option<Value> {
        let statement = self.read_statement()?;
        let statement = statement.trim();
        if statement.is_empty() {
            return None;
        }
        if matches!(statement, "public:" | "private:" | "protected:") {
            return Some(semantic_label_tag(statement.trim_end_matches(':')));
        }
        if statement
            .split_whitespace()
            .next()
            .is_some_and(|word| word == "typedef")
        {
            let rest = statement
                .split_once(char::is_whitespace)
                .map(|(_, rest)| rest)
                .unwrap_or("");
            return parse_cpp_typedef(rest);
        }
        if statement.contains('(') && statement.contains(')') {
            return parse_cpp_function(statement);
        }
        parse_cpp_variable(statement)
    }

    fn read_statement(&mut self) -> Option<String> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.source.len() {
            match self.peek_byte()? {
                b';' => {
                    let statement = self.source[start..self.pos].to_string();
                    self.pos += 1;
                    return Some(statement);
                }
                b':' => {
                    let statement = self.source[start..=self.pos].to_string();
                    self.pos += 1;
                    return Some(statement);
                }
                b'{' => {
                    let statement = self.source[start..self.pos].to_string();
                    self.skip_balanced_block();
                    return (!statement.trim().is_empty()).then_some(statement);
                }
                b'}' => return None,
                _ => self.pos += 1,
            }
        }
        None
    }

    fn skip_balanced_block(&mut self) {
        if self.peek_byte() != Some(b'{') {
            return;
        }
        let mut depth = 0usize;
        while self.pos < self.source.len() {
            match self.peek_byte() {
                Some(b'{') => {
                    depth += 1;
                    self.pos += 1;
                }
                Some(b'}') => {
                    depth = depth.saturating_sub(1);
                    self.pos += 1;
                    if depth == 0 {
                        break;
                    }
                }
                Some(_) => self.pos += 1,
                None => break,
            }
        }
    }

    fn read_trailing_decl_name(&mut self) -> Option<String> {
        self.skip_ws();
        let checkpoint = self.pos;
        while self.pos < self.source.len() {
            match self.peek_byte()? {
                b';' => break,
                b'{' | b'}' => {
                    self.pos = checkpoint;
                    return None;
                }
                _ => self.pos += 1,
            }
        }
        if self.peek_byte() != Some(b';') {
            self.pos = checkpoint;
            return None;
        }
        self.pos = checkpoint;
        let name = self.read_ident();
        self.pos = checkpoint;
        name
    }

    fn consume_optional_statement_tail(&mut self) {
        while self.pos < self.source.len() && self.peek_byte() != Some(b';') {
            if self.peek_byte() == Some(b'{') || self.peek_byte() == Some(b'}') {
                return;
            }
            self.pos += 1;
        }
        if self.peek_byte() == Some(b';') {
            self.pos += 1;
        }
    }

    fn consume_word(&mut self, word: &str) -> Option<()> {
        if self.source[self.pos..].starts_with(word)
            && self
                .source
                .as_bytes()
                .get(self.pos + word.len())
                .is_none_or(|byte| !is_ident_byte(*byte))
        {
            self.pos += word.len();
            Some(())
        } else {
            None
        }
    }

    fn consume_one_of_words(&mut self, words: &[&str]) -> Option<()> {
        words.iter().find_map(|word| self.consume_word(word))
    }

    fn read_ident(&mut self) -> Option<String> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.source.len()
            && self
                .source
                .as_bytes()
                .get(self.pos)
                .is_some_and(|byte| is_ident_byte(*byte))
        {
            self.pos += 1;
        }
        (self.pos > start).then(|| self.source[start..self.pos].to_string())
    }

    fn skip_until_byte(&mut self, byte: u8) -> Option<()> {
        while self.pos < self.source.len() {
            if self.peek_byte() == Some(byte) {
                return Some(());
            }
            self.pos += 1;
        }
        None
    }

    fn skip_ws(&mut self) {
        while self
            .source
            .as_bytes()
            .get(self.pos)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.pos += 1;
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        self.source.as_bytes().get(self.pos).copied()
    }
}

fn parse_cpp_typedef(rest: &str) -> Option<Value> {
    let rest = rest.trim();
    let (type_text, name) = rest.rsplit_once(char::is_whitespace)?;
    semantic_type_tag(
        name.trim(),
        vec![
            (":typedef", semantic_cpp_type_value(type_text.trim())),
            (":type", Value::String("typedef".into())),
        ],
    )
}

fn parse_cpp_function(statement: &str) -> Option<Value> {
    let open = statement.find('(')?;
    let close = statement.rfind(')')?;
    let head = statement[..open].trim();
    let args = statement[open + 1..close].trim();
    let mut parts = head.split_whitespace().collect::<Vec<_>>();
    let raw_name = parts.pop()?.trim_start_matches('~');
    let name = raw_name.rsplit("::").next().unwrap_or(raw_name);
    let return_type = parts.join(" ");
    let mut attrs = Vec::new();
    attrs.push((":prototype-flag", Value::T));
    if let Some(modifiers) = semantic_c_like_typemodifiers(statement) {
        attrs.push((":typemodifiers", modifiers));
    }
    if raw_name != name || return_type.is_empty() || statement.contains(&format!("~{name}")) {
        if return_type.is_empty() || raw_name == name {
            attrs.push((":constructor-flag", Value::T));
            attrs.push((":type", semantic_type_ref(name)));
        } else {
            attrs.push((":destructor-flag", Value::T));
            attrs.push((":type", Value::String("void".into())));
        }
    } else {
        let arguments = parse_cpp_arguments(args);
        if !arguments.is_empty() {
            attrs.push((":arguments", Value::list(arguments)));
        }
        attrs.push((":type", semantic_cpp_type_value(&return_type)));
    }
    semantic_function_tag(name, attrs)
}

fn parse_cpp_arguments(args: &str) -> Vec<Value> {
    args.split(',')
        .filter_map(|arg| {
            let arg = arg.trim();
            if arg.is_empty() || arg == "void" {
                return None;
            }
            let mut parts = arg.split_whitespace().collect::<Vec<_>>();
            let name = parts.pop().unwrap_or("");
            let type_text;
            let (name, type_text_ref) =
                if parts.is_empty() || name.chars().all(|ch| ch == '*' || ch == '&') {
                    ("", arg)
                } else {
                    type_text = parts.join(" ");
                    (name.trim_matches(['*', '&']), type_text.as_str())
                };
            Some(semantic_variable_tag(
                name,
                semantic_cpp_type_value(type_text_ref.trim()),
                arg.contains('*'),
            ))
        })
        .collect()
}

fn parse_cpp_variable(statement: &str) -> Option<Value> {
    let statement = statement.trim();
    let mut parts = statement.split_whitespace().collect::<Vec<_>>();
    let raw_name = parts.pop()?.trim();
    let name = raw_name.trim_matches(['*', '&']);
    let type_text = parts.join(" ");
    let mut attrs = Vec::new();
    if statement.contains('*') {
        attrs.push((":pointer", Value::Integer(1)));
    }
    attrs.push((":type", semantic_cpp_type_value(type_text.trim())));
    if let Some(modifiers) = semantic_c_like_typemodifiers(statement) {
        attrs.push((":typemodifiers", modifiers));
    }
    Some(semantic_tag(name, "variable", semantic_plist(attrs)))
}

fn semantic_c_like_typemodifiers(statement: &str) -> Option<Value> {
    let modifiers = statement
        .split_whitespace()
        .take_while(|word| {
            matches!(
                *word,
                "public" | "private" | "protected" | "static" | "final" | "abstract" | "strictfp"
            )
        })
        .map(|word| Value::String(word.into()))
        .collect::<Vec<_>>();
    (!modifiers.is_empty()).then(|| Value::list(modifiers))
}

fn semantic_type_tag(name: &str, attrs: Vec<(&str, Value)>) -> Option<Value> {
    Some(semantic_tag(name, "type", semantic_plist(attrs)))
}

fn semantic_function_tag(name: &str, attrs: Vec<(&str, Value)>) -> Option<Value> {
    Some(semantic_tag(name, "function", semantic_plist(attrs)))
}

fn semantic_variable_tag(name: &str, type_value: Value, pointer: bool) -> Value {
    let mut attrs = Vec::new();
    if pointer {
        attrs.push((":pointer", Value::Integer(1)));
    }
    attrs.push((":type", type_value));
    semantic_tag(name, "variable", semantic_plist(attrs))
}

fn semantic_label_tag(name: &str) -> Value {
    semantic_tag(name, "label", Value::Nil)
}

fn semantic_tag(name: &str, class: &str, attrs: Value) -> Value {
    Value::list([
        Value::String(name.into()),
        Value::Symbol(class.into()),
        attrs,
        Value::Nil,
        Value::Nil,
    ])
}

fn semantic_plist(attrs: Vec<(&str, Value)>) -> Value {
    Value::list(
        attrs
            .into_iter()
            .flat_map(|(key, value)| [Value::Symbol(key.into()), value])
            .collect::<Vec<_>>(),
    )
}

fn semantic_cpp_type_value(type_text: &str) -> Value {
    let type_text = type_text
        .replace("const ", "")
        .replace("mutable ", "")
        .replace("struct ", "")
        .replace("public ", "")
        .replace("private ", "")
        .replace("protected ", "")
        .replace("static ", "")
        .replace("final ", "")
        .replace("abstract ", "")
        .replace("strictfp ", "")
        .replace(['*', '&'], "")
        .trim()
        .to_string();
    if matches!(
        type_text.as_str(),
        "void" | "int" | "char" | "unsigned int" | "long" | "short" | "float" | "double"
    ) {
        Value::String(type_text)
    } else {
        semantic_type_ref(&type_text)
    }
}

fn semantic_type_ref(name: &str) -> Value {
    Value::list([
        Value::String(name.into()),
        Value::Symbol("type".into()),
        semantic_plist(vec![(":type", Value::String("class".into()))]),
        Value::Nil,
        Value::Nil,
    ])
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn semantic_completion_matches(tags: &[Value], prefix: &str) -> Vec<Value> {
    let parts = prefix
        .split("::")
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if parts.len() > 1 {
        let (parents, final_prefix) = parts.split_at(parts.len() - 1);
        if let Some(parent) = find_semantic_type_chain(tags, parents) {
            let mut matches = Vec::new();
            collect_semantic_completion_tags(
                &semantic_tag_members(&parent),
                &final_prefix[0],
                &mut matches,
            );
            return matches;
        }
        return Vec::new();
    }
    let mut matches = Vec::new();
    collect_semantic_completion_tags(tags, prefix, &mut matches);
    matches
}

fn find_semantic_type_chain(tags: &[Value], names: &[String]) -> Option<Value> {
    find_semantic_type_chain_in(tags, tags, names).or_else(|| {
        names
            .last()
            .and_then(|name| find_semantic_type_deep(tags, name))
            .map(|tag| resolve_semantic_typedef(tags, &tag))
    })
}

fn find_semantic_type_chain_in(
    root_tags: &[Value],
    tags: &[Value],
    names: &[String],
) -> Option<Value> {
    let (first, rest) = names.split_first()?;
    for tag in tags {
        if semantic_tag_name(tag).as_deref() == Some(first)
            && let Some(resolved) = semantic_type_candidate(root_tags, tag)
        {
            if rest.is_empty() {
                return Some(resolved);
            }
            if let Some(found) =
                find_semantic_type_chain_in(root_tags, &semantic_tag_members(&resolved), rest)
            {
                return Some(found);
            }
        }
    }
    None
}

fn resolve_semantic_typedef(root_tags: &[Value], tag: &Value) -> Value {
    let mut current = tag.clone();
    let mut seen = Vec::new();
    loop {
        let Some(target) = semantic_tag_attr(&current, ":typedef") else {
            return current;
        };
        let Ok(parts) = semantic_type_name_parts(&target) else {
            return current;
        };
        if parts.is_empty() || seen.contains(&parts) {
            return current;
        }
        seen.push(parts.clone());
        let Some(next) = find_semantic_type_chain(root_tags, &parts)
            .or_else(|| find_semantic_type_deep(root_tags, parts.last()?))
        else {
            return current;
        };
        current = next;
    }
}

fn find_semantic_type_deep(tags: &[Value], name: &str) -> Option<Value> {
    for tag in tags {
        if semantic_tag_name(tag).as_deref() == Some(name)
            && let Some(found) = semantic_type_candidate(tags, tag)
        {
            return Some(found);
        }
        if let Some(found) = find_semantic_type_deep(&semantic_tag_members(tag), name) {
            return Some(found);
        }
    }
    None
}

fn semantic_type_candidate(root_tags: &[Value], tag: &Value) -> Option<Value> {
    match semantic_tag_class(tag).as_deref() {
        Some("type") => Some(resolve_semantic_typedef(root_tags, tag)),
        Some("variable") => semantic_tag_attr(tag, ":type")
            .and_then(|type_value| semantic_type_name_parts(&type_value).ok())
            .and_then(|parts| {
                find_semantic_type_chain(root_tags, &parts).or_else(|| {
                    parts
                        .last()
                        .and_then(|name| find_semantic_type_deep(root_tags, name))
                })
            }),
        _ => None,
    }
}

fn collect_semantic_completion_tags(tags: &[Value], prefix: &str, matches: &mut Vec<Value>) {
    for tag in tags {
        if semantic_tag_name(tag)
            .as_deref()
            .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(tag.clone());
        }
        collect_semantic_completion_tags(&semantic_tag_members(tag), prefix, matches);
    }
}

fn semantic_tag_name(tag: &Value) -> Option<String> {
    tag.to_vec()
        .ok()
        .and_then(|items| items.first().cloned())
        .and_then(|name| string_text(&name).ok())
}

fn semantic_tag_class(tag: &Value) -> Option<String> {
    tag.to_vec()
        .ok()
        .and_then(|items| items.get(1).cloned())
        .and_then(|class| class.as_symbol().ok().map(str::to_string))
}

fn semantic_tag_members(tag: &Value) -> Vec<Value> {
    semantic_tag_attr(tag, ":members")
        .and_then(|members| members.to_vec().ok())
        .unwrap_or_default()
}

fn semantic_tag_attr(tag: &Value, attr: &str) -> Option<Value> {
    let Ok(items) = tag.to_vec() else {
        return None;
    };
    let attrs = items.get(2).and_then(|attrs| attrs.to_vec().ok())?;
    let mut index = 0usize;
    while index + 1 < attrs.len() {
        if matches!(&attrs[index], Value::Symbol(symbol) if symbol == attr) {
            return Some(attrs[index + 1].clone());
        }
        index += 2;
    }
    None
}

fn semantic_tag_has_typemodifier(tag: &Value, modifier: &str) -> bool {
    semantic_tag_attr(tag, ":typemodifiers")
        .and_then(|value| value.to_vec().ok())
        .is_some_and(|modifiers| {
            modifiers.iter().any(|value| {
                matches!(value, Value::String(text) if text == modifier)
                    || matches!(value, Value::Symbol(symbol) if symbol == modifier)
            })
        })
}
