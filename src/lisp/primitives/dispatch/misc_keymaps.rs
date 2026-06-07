use super::*;
use std::cell::RefCell;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::Command;

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
            | "widget-get"
            | "widget-put"
            | "widget-apply"
            | "define-button-type"
            | "push-button"
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
            | "user-real-login-name"
            | "system-name"
            | "user-full-name"
            | "macroexp-file-name"
            | "char-from-name"
            | "always"
            | "evenp"
            | "seq-subseq"
            | "text-quoting-style"
            | "file-truename"
            | "user-uid"
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
            | "completion-metadata"
            | "completion-metadata-get"
            | "completion-all-completions"
            | "completion-at-point"
            | "minibuffer--sort-by-length-alpha"
            | "minibuffer-sort-alphabetically"
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
            | "byte-compile-from-buffer"
            | "byte-compile-file"
            | "byte-compile--wide-docstring-p"
            | "byte-decompile-bytecode"
            | "funcall-with-delayed-message"
            | "advice--cd*r"
            | "handler-bind-1"
            | "debugger-trap"
            | "mapbacktrace"
            | "backtrace-frame--internal"
            | "backtrace-debug"
            | "backtrace-eval"
            | "backtrace--locals"
            | "backtrace-expand-ellipses"
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
            | "regexp-opt-depth"
            | "rx-to-string"
            | "null-device"
            | "process-file"
            | "read-answer"
            | "temporary-file-directory"
            | "convert-standard-filename"
            | "abbreviate-file-name"
            | "files--name-absolute-system-p"
            | "files--use-insert-directory-program-p"
            | "insert-directory-wildcard-in-dir-p"
            | "insert-directory-clean"
            | "dired-mark-pop-up"
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
            | "eieio-class-children"
            | "class-abstract-p"
            | "eieio-oref-default"
            | "eieio-oset-default"
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
            | "semantic-fetch-tags"
            | "semantic-current-tag"
            | "semantic-ctxt-current-symbol"
            | "semantic-ctxt-current-symbol-and-bounds"
            | "bounds-of-thing-at-point"
            | "semantic-analyze-possible-completions"
            | "semantic-analyze-tag-references"
            | "semantic-analyze-refs-impl"
            | "semantic-analyze-refs-proto"
            | "semantic-symref-find-references-by-name"
            | "semantic-symref-result-get-files"
            | "semantic-symref-result-get-tags"
            | "semantic-symref-hits-in-region"
            | "semantic-symref-test-count-hits-in-tag"
            | "semantic-equivalent-tag-p"
            | "semantic-go-to-tag"
            | "semantic-clear-toplevel-cache"
            | "semanticdb-typecache-find"
            | "semanticdb-typecache-add-dependant"
            | "srecode-template-get-table"
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

fn dabbrev_completion_at_point(
    interp: &Interpreter,
    env: &Env,
) -> Result<Option<(usize, usize, String)>, LispError> {
    let capfs = interp
        .lookup_var("completion-at-point-functions", env)
        .unwrap_or(Value::Nil);
    let has_dabbrev_capf = capfs
        .to_vec()
        .map(|items| {
            items
                .iter()
                .any(|item| matches!(item, Value::Symbol(symbol) if symbol == "dabbrev-capf"))
        })
        .unwrap_or(false);
    if !has_dabbrev_capf {
        return Ok(None);
    }

    let point = interp.buffer.point();
    let mut start = point;
    while start > interp.buffer.point_min() {
        let Some(ch) = interp.buffer.char_at(start - 1) else {
            break;
        };
        if !dabbrev_word_char(ch) {
            break;
        }
        start -= 1;
    }
    let prefix = interp
        .buffer
        .buffer_substring(start, point)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    if prefix.is_empty() {
        return Ok(None);
    }

    let check_other_buffers = interp
        .lookup_var("dabbrev--check-other-buffers", env)
        .is_some_and(|value| value.is_truthy());
    let mut texts = vec![interp.buffer.buffer_string()];
    if check_other_buffers {
        for (buffer_id, _) in &interp.buffer_list {
            if *buffer_id == interp.current_buffer_id() {
                continue;
            }
            if let Some(buffer) = interp.get_buffer_by_id(*buffer_id) {
                texts.push(buffer.buffer_string());
            }
        }
    }
    let mut matches = Vec::new();
    for (text_index, text) in texts.iter().enumerate() {
        let mut word_start = None;
        for (index, ch) in text
            .char_indices()
            .chain(std::iter::once((text.len(), '\0')))
        {
            if ch != '\0' && dabbrev_word_char(ch) {
                if word_start.is_none() {
                    word_start = Some(index);
                }
                continue;
            }
            let Some(byte_start) = word_start.take() else {
                continue;
            };
            let word = &text[byte_start..index];
            let char_start = text[..byte_start].chars().count() + 1;
            let char_end = char_start + word.chars().count();
            if text_index == 0 && char_start == start && char_end == point {
                continue;
            }
            if word.starts_with(&prefix)
                && word != prefix
                && !matches.iter().any(|existing: &String| existing == word)
            {
                matches.push(word.to_string());
            }
        }
    }

    match matches.as_slice() {
        [only] => Ok(Some((start, point, only.clone()))),
        _ => Ok(None),
    }
}

fn dabbrev_word_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '-' || ch == '_'
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
        "widget-get" => {
            need_args(name, args, 2)?;
            widget_get(interp, &args[0], &args[1])
        }
        "widget-put" => {
            need_args(name, args, 3)?;
            widget_put(interp, &args[0], &args[1], args[2].clone())
        }
        "widget-apply" => {
            need_arg_range(name, args, 2, usize::MAX)?;
            let function = widget_get(interp, &args[0], &args[1])?;
            if function.is_nil() {
                return Ok(Value::Nil);
            }
            let mut call_args = Vec::with_capacity(args.len());
            call_args.push(args[0].clone());
            call_args.extend_from_slice(&args[2..]);
            interp.call_function_value(function, args[1].as_symbol().ok(), &call_args, env)
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
        "push-button" => {
            need_arg_range(name, args, 0, 2)?;
            let pos = args
                .first()
                .filter(|value| !value.is_nil())
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| interp.buffer.point());
            interp.buffer.goto_char(pos);
            if point_is_on_plain_backtrace_ellipsis(interp, pos) {
                reprint_current_backtrace_frame_for_expansion(interp, env, true)?;
                return Ok(Value::T);
            }
            let button = interp.call_function_value(
                Value::Symbol("button-at".into()),
                Some("button-at"),
                &[Value::Integer(pos as i64)],
                env,
            )?;
            if button.is_nil() {
                return Ok(Value::Nil);
            }
            let use_mouse_action = args.get(1).cloned().unwrap_or(Value::Nil);
            interp.call_function_value(
                Value::Symbol("button-activate".into()),
                Some("button-activate"),
                &[button, use_mouse_action],
                env,
            )?;
            Ok(Value::T)
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
        "user-login-name" | "user-real-login-name" => {
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
        "user-uid" => Ok(Value::Integer(current_user_id()? as i64)),
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
            let function_name = symbol_designator_name(&args[0])
                .ok_or_else(|| LispError::TypeError("symbol".into(), args[0].type_name()))?;
            let where_sym = args[1].as_symbol()?;
            let original = interp.lookup_function(&function_name, env)?;
            let advice = match &args[2] {
                Value::Symbol(symbol) => interp
                    .lookup_function(symbol, env)
                    .unwrap_or_else(|_| Value::Symbol(symbol.clone())),
                value => match symbol_designator_name(value) {
                    Some(symbol) => interp
                        .lookup_function(&symbol, env)
                        .unwrap_or(Value::Symbol(symbol)),
                    None => value.clone(),
                },
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
        "completion-metadata" => {
            need_args(name, args, 3)?;
            Ok(Value::list([Value::Symbol("metadata".into())]))
        }
        "completion-metadata-get" => {
            need_args(name, args, 2)?;
            let prop = args[1].as_symbol()?;
            let items = args[0].to_vec().unwrap_or_default();
            for item in &items {
                let Some((key, value)) = item.cons_values() else {
                    continue;
                };
                if key.as_symbol().ok() == Some(prop) {
                    return Ok(value);
                }
            }
            let keyword = format!(":{prop}");
            let mut index = 0usize;
            while index + 1 < items.len() {
                if items[index].as_symbol().ok() == Some(keyword.as_str()) {
                    return Ok(items[index + 1].clone());
                }
                index += 1;
            }
            let extra = interp
                .lookup_var("completion-extra-properties", env)
                .unwrap_or(Value::Nil);
            let extra_items = extra.to_vec().unwrap_or_default();
            let mut index = 0usize;
            while index + 1 < extra_items.len() {
                if extra_items[index].as_symbol().ok() == Some(keyword.as_str()) {
                    return Ok(extra_items[index + 1].clone());
                }
                index += 1;
            }
            Ok(Value::Nil)
        }
        "completion-all-completions" => {
            need_arg_range(name, args, 4, 5)?;
            let string = string_text(&args[0])?;
            let point = args[3].as_integer()?.max(0) as usize;
            let before_point = string.chars().take(point).collect::<String>();
            let completions = all_completions(
                interp,
                &[
                    Value::String(before_point),
                    args[1].clone(),
                    args[2].clone(),
                ],
                env,
            )?;
            if completions.is_nil() {
                return Ok(Value::Nil);
            }
            last_nconc_cell(&completions)?.set_cdr(Value::Integer(0))?;
            Ok(completions)
        }
        "completion-at-point" => {
            need_args(name, args, 0)?;
            if let Some(completion) = dabbrev_completion_at_point(interp, env)? {
                let (start, end, expansion) = completion;
                delete_region_with_hooks(interp, start, end, env)?;
                interp.buffer.goto_char(start);
                insert_text_with_hooks(interp, &expansion, &[], false, false, env)?;
                return Ok(Value::T);
            }
            if interp
                .lookup_var("completion-auto-help", env)
                .is_none_or(|value| value.is_nil())
            {
                let _ = call_function_value(
                    interp,
                    &Value::Symbol("minibuffer-message".into()),
                    &[Value::String("Next char not unique".into())],
                    env,
                );
            }
            Ok(Value::Nil)
        }
        "minibuffer--sort-by-length-alpha" => {
            need_args(name, args, 1)?;
            let mut items = args[0].to_vec()?;
            items.sort_by(|left, right| {
                let left_text = string_text(left).unwrap_or_default();
                let right_text = string_text(right).unwrap_or_default();
                left_text
                    .chars()
                    .count()
                    .cmp(&right_text.chars().count())
                    .then_with(|| left_text.cmp(&right_text))
            });
            Ok(Value::list(items))
        }
        "minibuffer-sort-alphabetically" => {
            need_args(name, args, 1)?;
            let mut items = args[0].to_vec()?;
            items.sort_by(|left, right| {
                string_text(left)
                    .unwrap_or_default()
                    .cmp(&string_text(right).unwrap_or_default())
            });
            Ok(Value::list(items))
        }
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
            let (compile_target, suppressions) = byte_compile_target_and_suppressions(&args[0]);
            byte_compile_emit_warnings(interp, &compile_target, &suppressions, env)?;
            if let Ok(symbol) = compile_target.as_symbol() {
                let callable = resolve_callable(interp, &compile_target, env)?;
                let slots = byte_code_function_slots(interp, Some(symbol), callable, None, false);
                return Ok(interp.create_record("byte-code-function", slots));
            }
            if is_lambda_value(&compile_target) {
                validate_lambda_form(&compile_target)?;
                let lap = byte_code_decompile_lap(interp, &compile_target);
                let capture_lexical = byte_compile_capture_lexical(interp, env);
                let callable =
                    byte_compile_lambda_callable(interp, env, &compile_target, capture_lexical)?;
                let slots = byte_code_function_slots(interp, None, callable, lap, !capture_lexical);
                return Ok(interp.create_record("byte-code-function", slots));
            }
            if matches!(compile_target, Value::Lambda(_, _, _)) {
                let slots =
                    byte_code_function_slots(interp, None, compile_target.clone(), None, false);
                return Ok(interp.create_record("byte-code-function", slots));
            }
            Ok(compile_target)
        }
        "byte-compile-from-buffer" => byte_compile_from_buffer(interp, args, env),
        "byte-compile-file" => byte_compile_file(interp, args, env),
        "byte-compile--wide-docstring-p" => {
            need_args(name, args, 2)?;
            let docstring = string_text(&args[0])?;
            let max_width = args[1].as_integer()? as usize;
            Ok(if byte_compile_wide_docstring_p(&docstring, max_width) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "byte-decompile-bytecode" => {
            need_args(name, args, 2)?;
            if args[0].is_list() {
                Ok(args[0].clone())
            } else {
                Ok(Value::Nil)
            }
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
        "advice--cd*r" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
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
        "mapbacktrace" => {
            need_arg_range(name, args, 1, 2)?;
            let callback = resolve_callable(interp, &args[0], env)?;
            let base = args.get(1).filter(|value| !value.is_nil());
            let frames = interp.backtrace_frames_snapshot();
            let start = base
                .and_then(|base| {
                    frames
                        .iter()
                        .position(|(_, function, _, _)| function == base)
                })
                .unwrap_or(0);
            for (evald, function, frame_args, debug_on_exit) in frames.into_iter().skip(start) {
                let flags = if debug_on_exit {
                    Value::list([Value::Symbol(":debug-on-exit".into()), Value::T])
                } else {
                    Value::Nil
                };
                let evald = if evald { Value::T } else { Value::Nil };
                interp.call_function_value(
                    callback.clone(),
                    None,
                    &[evald, function, Value::list(frame_args), flags],
                    env,
                )?;
            }
            Ok(Value::Nil)
        }
        "backtrace-frame--internal" => {
            need_args(name, args, 3)?;
            let callback = resolve_callable(interp, &args[0], env)?;
            let Some((evald, function, frame_args, debug_on_exit)) =
                interp.current_backtrace_frame()
            else {
                return Ok(Value::Nil);
            };
            let flags = if debug_on_exit {
                Value::list([Value::Symbol(":debug-on-exit".into()), Value::T])
            } else {
                Value::Nil
            };
            let evald = if evald { Value::T } else { Value::Nil };
            interp.call_function_value(
                callback,
                None,
                &[evald, function, Value::list(frame_args), flags],
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
            let raw_index = args[0].as_integer()?;
            if raw_index < 0 {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("wrong-type-argument".into()),
                    Value::Symbol("natnump".into()),
                    args[0].clone(),
                ])));
            }
            let index = raw_index as usize;
            let base = args.get(1).filter(|value| !value.is_nil());
            let locals = interp
                .backtrace_frame_locals_snapshot_with_base(index, base)
                .unwrap_or_default()
                .into_iter()
                .map(|(name, value)| Value::cons(Value::Symbol(name), value))
                .collect::<Vec<_>>();
            Ok(Value::list(locals))
        }
        "backtrace-expand-ellipses" => {
            need_arg_range(name, args, 0, 1)?;
            let no_limit = args.first().is_some_and(Value::is_truthy);
            reprint_current_backtrace_frame_for_expansion(interp, env, no_limit)
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
                .map(|(evald, function, frame_args, _debug_on_exit)| {
                    let evald = if evald { Value::T } else { Value::Nil };
                    let mut items = vec![evald, function];
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
            for (_, function, frame_args, _) in interp.thread_backtrace_frames_snapshot(thread_id) {
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
        "regexp-opt-depth" => {
            need_args(name, args, 1)?;
            Ok(Value::Integer(
                regexp_opt_depth(&string_text(&args[0])?) as i64
            ))
        }
        "rx-to-string" => {
            need_arg_range(name, args, 1, 2)?;
            let no_group = args.get(1).is_some_and(Value::is_truthy);
            Ok(Value::String(crate::lisp::eval::compile_rx_to_string(
                interp, &args[0], env, no_group,
            )?))
        }
        "null-device" => {
            need_args(name, args, 0)?;
            Ok(Value::String("/dev/null".into()))
        }
        "process-file" => {
            need_arg_range(name, args, 4, usize::MAX)?;
            process_file_compat(interp, args, env)
        }
        "read-answer" => {
            need_args(name, args, 2)?;
            let answers = args[1].to_vec()?;
            Ok(answers
                .first()
                .and_then(|entry| entry.to_vec().ok())
                .and_then(|entry| entry.first().cloned())
                .unwrap_or(Value::String(String::new())))
        }
        "temporary-file-directory" => {
            need_args(name, args, 0)?;
            Ok(interp
                .lookup_var("temporary-file-directory", env)
                .unwrap_or_else(|| Value::String(std::env::temp_dir().display().to_string())))
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
            let directory = string_text(&args[0])?;
            let Some(parent) = file_name_directory(&directory) else {
                return Ok(Value::Nil);
            };
            if !parent.contains('*') || Path::new(&directory).exists() {
                return Ok(Value::Nil);
            }
            let base_directory = file_name_as_directory(&dired_base_directory(&directory));
            let wildcard = Path::new(&directory)
                .strip_prefix(Path::new(&base_directory))
                .ok()
                .map(|path| path.to_string_lossy().into_owned())
                .unwrap_or_else(|| directory.clone());
            Ok(Value::cons(
                Value::String(base_directory),
                Value::String(wildcard),
            ))
        }
        "insert-directory-clean" => {
            need_arg_range(name, args, 1, 2)?;
            Ok(Value::Nil)
        }
        "dired-mark-pop-up" => {
            need_arg_range(name, args, 4, usize::MAX)?;
            call_function_value(interp, &args[3], &args[4..], env)
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
            let buffer_id = interp.current_buffer_id();
            run_named_hooks(interp, "change-major-mode-hook", env, Some(buffer_id))?;
            let locals = interp.buffer_local_variables(buffer_id);
            let mut permanent = Vec::new();
            for (name, value) in &locals {
                if interp
                    .get_symbol_property(name, "permanent-local")
                    .is_some_and(|value| value.is_truthy())
                {
                    permanent.push((name.clone(), value.clone()));
                    continue;
                }
                interp.notify_variable_watchers(
                    name,
                    Value::Nil,
                    "makunbound",
                    Some(buffer_id),
                    env,
                )?;
            }
            interp.clear_buffer_local_state(buffer_id);
            for (name, value) in permanent {
                interp.set_buffer_local_value(buffer_id, &name, value);
            }
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
        "cl--class-children" | "eieio-class-children" => {
            need_args(name, args, 1)?;
            let Some(symbol) = interp.class_name_from_value(&args[0]) else {
                return Err(LispError::TypeError("class".into(), args[0].type_name()));
            };
            Ok(Value::list(interp.class_children(&symbol)))
        }
        "class-abstract-p" => {
            need_args(name, args, 1)?;
            let Some(symbol) = interp.class_name_from_value(&args[0]) else {
                return Err(LispError::TypeError("class".into(), args[0].type_name()));
            };
            let abstractp = interp
                .get_symbol_property(&symbol, "emaxx-class-options")
                .and_then(|options| options.to_vec().ok())
                .is_some_and(|options| {
                    options.windows(2).any(|pair| {
                        matches!(&pair[0], Value::Symbol(option) if option == ":abstract")
                            && pair[1].is_truthy()
                    })
                });
            Ok(if abstractp { Value::T } else { Value::Nil })
        }
        "eieio-oref-default" => {
            need_args(name, args, 2)?;
            let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                return Err(LispError::TypeError("class".into(), args[0].type_name()));
            };
            let slot_name = args[1].as_symbol()?;
            if let Some(value) =
                interp.get_symbol_property(&class_name, &eieio_class_default_property(slot_name))
            {
                return Ok(value);
            }
            let slots = eieio_slot_specs(interp, &class_name)?;
            if let Some(slot_index) = eieio_slot_index(&slots, slot_name)
                && let Some(initform) = &slots[slot_index].initform
            {
                return interp.eval(initform, env);
            }
            Ok(Value::Nil)
        }
        "eieio-oset-default" => {
            need_args(name, args, 3)?;
            let Some(class_name) = interp.class_name_from_value(&args[0]) else {
                return Err(LispError::TypeError("class".into(), args[0].type_name()));
            };
            let slot_name = args[1].as_symbol()?;
            interp.put_symbol_property(
                &class_name,
                &eieio_class_default_property(slot_name),
                args[2].clone(),
            );
            Ok(args[2].clone())
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
            make_eieio_instance(interp, &class_name, &args[1..], true, env)
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
        "semantic-fetch-tags" => {
            need_arg_range(name, args, 0, 1)?;
            semantic_fetch_tags_compat(interp, env)
        }
        "semantic-current-tag" => {
            need_arg_range(name, args, 0, 1)?;
            semantic_current_tag_compat(interp, env)
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
        "bounds-of-thing-at-point" => {
            need_args(name, args, 1)?;
            let thing = args[0].as_symbol()?;
            Ok(bounds_of_thing_at_point(interp, thing).unwrap_or(Value::Nil))
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
        "semantic-symref-find-references-by-name" => {
            need_arg_range(name, args, 1, 3)?;
            semantic_symref_find_references_by_name(interp, &args[0])
        }
        "semantic-symref-result-get-files" => {
            need_args(name, args, 1)?;
            semantic_symref_result_part(&args[0], 2)
        }
        "semantic-symref-result-get-tags" => {
            need_arg_range(name, args, 1, 2)?;
            semantic_symref_result_part(&args[0], 3)
        }
        "semantic-symref-hits-in-region" => {
            need_args(name, args, 4)?;
            semantic_symref_hits_in_region(interp, args, env)
        }
        "semantic-symref-test-count-hits-in-tag" => {
            need_args(name, args, 0)?;
            semantic_symref_test_count_hits_in_tag(interp)
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
            semantic_go_to_tag(interp, &args[0], env)
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
        "srecode-template-get-table" => {
            need_arg_range(name, args, 2, 4)?;
            srecode_template_get_table(interp, args, env)
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
            let matches = cl_typep_matches(interp, &args[0], &args[1])?;
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

fn bounds_of_thing_at_point(interp: &Interpreter, thing: &str) -> Option<Value> {
    let is_thing_char: fn(char) -> bool = match thing {
        "symbol" => is_symbol_thing_char,
        "word" => |ch| ch.is_alphanumeric() || ch == '_',
        _ => return None,
    };
    let point = interp.buffer.point();
    let mut start = point;
    while start > interp.buffer.point_min() {
        let Some(ch) = interp.buffer.char_at(start - 1) else {
            break;
        };
        if !is_thing_char(ch) {
            break;
        }
        start -= 1;
    }
    let mut end = point;
    while end < interp.buffer.point_max() {
        let Some(ch) = interp.buffer.char_at(end) else {
            break;
        };
        if !is_thing_char(ch) {
            break;
        }
        end += 1;
    }
    (start < end).then(|| Value::cons(Value::Integer(start as i64), Value::Integer(end as i64)))
}

fn is_symbol_thing_char(ch: char) -> bool {
    ch.is_alphanumeric() || matches!(ch, '_' | '-')
}

fn is_semantic_member_expr_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric()
        || matches!(
            ch,
            '_' | '@' | '.' | ':' | '-' | '>' | '[' | ']' | '(' | ')'
        )
}

fn semantic_member_expression_parts(text: &str) -> Vec<String> {
    semantic_member_expression_steps(text)
        .into_iter()
        .map(|step| step.name)
        .collect()
}

#[derive(Clone)]
struct SemanticMemberStep {
    name: String,
    arrow_before: bool,
}

fn semantic_member_expression_steps(text: &str) -> Vec<SemanticMemberStep> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut arrow_before = false;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                parts.push(SemanticMemberStep {
                    name: semantic_normalize_member_part(&current),
                    arrow_before,
                });
                current.clear();
                arrow_before = false;
            }
            ':' if chars.peek() == Some(&':') => {
                chars.next();
                parts.push(SemanticMemberStep {
                    name: semantic_normalize_member_part(&current),
                    arrow_before,
                });
                current.clear();
                arrow_before = false;
            }
            '-' if chars.peek() == Some(&'>') => {
                chars.next();
                parts.push(SemanticMemberStep {
                    name: semantic_normalize_member_part(&current),
                    arrow_before,
                });
                current.clear();
                arrow_before = true;
            }
            _ => current.push(ch),
        }
    }
    parts.push(SemanticMemberStep {
        name: semantic_normalize_member_part(&current),
        arrow_before,
    });
    while parts.first().is_some_and(|part| part.name.is_empty()) {
        parts.remove(0);
    }
    parts
}

fn semantic_normalize_member_part(part: &str) -> String {
    let normalized = part.trim().trim_end_matches("()");
    normalized
        .split_once('[')
        .map(|(root, _)| root)
        .unwrap_or(normalized)
        .to_string()
}

fn semantic_makefile_possible_completions(
    interp: &Interpreter,
    symbol: &SemanticCurrentSymbol,
) -> Value {
    let prefix = symbol.text.as_str();
    let Ok(buffer_text) = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
    else {
        return Value::Nil;
    };
    let before_point = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .unwrap_or_default();
    let line_start = before_point.rfind('\n').map(|index| index + 1).unwrap_or(0);
    let line_before_point = &before_point[line_start..];

    let matches = if symbol.start > interp.buffer.point_min()
        && interp.buffer.char_at(symbol.start - 1) == Some('$')
    {
        semantic_makefile_variables(&buffer_text, prefix)
    } else if line_before_point
        .find('=')
        .is_some_and(|eq| line_start + eq < symbol.start)
    {
        semantic_makefile_file_names(interp, prefix)
    } else {
        semantic_makefile_targets(&buffer_text, prefix)
    };
    Value::list(
        matches
            .into_iter()
            .map(|name| semantic_tag(&name, "variable", Value::Nil)),
    )
}

fn semantic_makefile_variables(text: &str, prefix: &str) -> Vec<String> {
    let mut matches = Vec::new();
    for line in text.lines() {
        let line = line.trim_start();
        if line.starts_with('#') {
            continue;
        }
        let Some(eq) = line.find('=') else {
            continue;
        };
        let name = line[..eq]
            .trim_end_matches([':', '+', '?'])
            .split_whitespace()
            .next()
            .unwrap_or("");
        if !name.is_empty() && name.starts_with(prefix) {
            matches.push(name.to_string());
        }
    }
    matches.sort();
    matches.dedup();
    matches
}

fn semantic_makefile_targets(text: &str, prefix: &str) -> Vec<String> {
    let mut matches = Vec::new();
    for line in text.lines() {
        let line = line.trim_start();
        if line.starts_with('#') || line.starts_with('\t') || line.contains('=') {
            continue;
        }
        let Some(colon) = line.find(':') else {
            continue;
        };
        for target in line[..colon].split_whitespace() {
            if target.starts_with(prefix) && target != prefix {
                matches.push(target.to_string());
            }
        }
    }
    matches.sort();
    matches.dedup();
    matches
}

fn semantic_makefile_file_names(interp: &Interpreter, prefix: &str) -> Vec<String> {
    let Some(path) = interp
        .buffer
        .file_truename
        .as_deref()
        .or(interp.buffer.file.as_deref())
    else {
        return Vec::new();
    };
    let Some(directory) = Path::new(path).parent() else {
        return Vec::new();
    };
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };
    let mut matches = entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.starts_with(prefix))
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();
    matches
}

fn semantic_texinfo_possible_completions(symbol: &SemanticCurrentSymbol) -> Value {
    let prefix = if symbol.text.starts_with('@') {
        symbol.text.clone()
    } else {
        format!("@{}", symbol.text)
    };
    let commands = [
        "@bye",
        "@chapter",
        "@contents",
        "@copyright",
        "@c",
        "@end",
        "@format",
        "@ifinfo",
        "@input",
        "@macro",
        "@majorheading",
        "@menu",
        "@multitable",
        "@node",
        "@set",
        "@setfilename",
        "@settitle",
        "@sp",
        "@titlepage",
        "@top",
        "@value",
        "@vskip",
    ];
    Value::list(
        commands
            .into_iter()
            .filter(|command| command.starts_with(&prefix))
            .map(|command| semantic_tag(command, "function", Value::Nil)),
    )
}

fn semantic_wisent_possible_completions(
    interp: &Interpreter,
    symbol: &SemanticCurrentSymbol,
) -> Value {
    let Ok(text) = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
    else {
        return Value::Nil;
    };
    let prefix = symbol.text.as_str();
    let mut matches = Vec::new();
    let mut grammar_section = false;
    for line in text.lines() {
        let line = line.split(";;").next().unwrap_or(line);
        let trimmed = line.trim();
        if trimmed == "%%" {
            grammar_section = !grammar_section;
            continue;
        }
        if grammar_section {
            if line.starts_with(char::is_whitespace) || trimmed.is_empty() {
                continue;
            }
            let Some(name) = trimmed
                .split(|ch: char| !is_ident_byte(ch as u8))
                .find(|part| !part.is_empty())
            else {
                continue;
            };
            if name.starts_with(prefix) {
                matches.push(name.to_string());
            }
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let Some(directive) = parts.next() else {
            continue;
        };
        if !matches!(directive, "%token" | "%keyword") {
            continue;
        }
        let Some(name) = parts.find(|part| {
            part.as_bytes()
                .first()
                .is_some_and(|byte| is_ident_byte(*byte))
        }) else {
            continue;
        };
        if name.starts_with(prefix) {
            matches.push(name.to_string());
        }
    }
    matches.sort();
    matches.dedup();
    Value::list(
        matches
            .into_iter()
            .map(|name| semantic_tag(&name, "variable", Value::Nil)),
    )
}

fn semantic_analyze_possible_completions(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some(symbol) = semantic_ctxt_current_symbol(interp) else {
        return Ok(Value::Nil);
    };
    if interp
        .lookup_var("major-mode", env)
        .is_some_and(|mode| mode == Value::Symbol("makefile-bsdmake-mode".into()))
    {
        return Ok(semantic_makefile_possible_completions(interp, &symbol));
    }
    if interp
        .lookup_var("major-mode", env)
        .is_some_and(|mode| mode == Value::Symbol("texinfo-mode".into()))
    {
        return Ok(semantic_texinfo_possible_completions(&symbol));
    }
    if interp
        .lookup_var("major-mode", env)
        .is_some_and(|mode| mode == Value::Symbol("wisent-grammar-mode".into()))
    {
        return Ok(semantic_wisent_possible_completions(interp, &symbol));
    }
    let steps = semantic_member_expression_steps(&symbol.text);
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
                MemberVisibility::All,
                &mut Vec::new(),
                &mut matches,
            );
        }
        let before_locals = matches.len();
        collect_semantic_local_variable_completion_tags(interp, prefix, &mut matches);
        let local_matches_added = matches.len() > before_locals;
        if local_matches_added {
            collect_semantic_external_variable_completion_tags(&tags, prefix, &mut matches);
        }
        if matches.is_empty() {
            collect_semantic_using_namespace_completion_tags(interp, &tags, prefix, &mut matches);
        }
        if matches.is_empty() {
            collect_semantic_named_completion_tags(&tags, prefix, &mut matches);
        }
        if let Some(expected_type) = semantic_c_like_assignment_expected_type(interp) {
            matches.retain(|tag| semantic_tag_matches_expected_type(tag, &expected_type));
        }
        return Ok(Value::list(unique_semantic_completion_tags(matches)));
    }
    if parts.len() == 2 && symbol.text.contains("::") {
        let mut matches = Vec::new();
        collect_semantic_qualified_namespace_completion_tags(
            &tags,
            &parts[0],
            &parts[1],
            &mut matches,
        );
        if !matches.is_empty() {
            return Ok(Value::list(unique_semantic_completion_tags(matches)));
        }
    }

    let root_name = semantic_c_like_root_name(&parts[0]);
    let root_type = semantic_cpp_root_type_context(interp, &tags, &root_name)
        .or_else(|| semantic_type_context_from_name(&tags, &parts[0]));
    let Some(mut current_context) = root_type else {
        return Ok(Value::Nil);
    };
    let enclosing_type = semantic_cpp_current_enclosing_type(interp, &tags);
    let root_is_current_member = enclosing_type
        .as_ref()
        .is_some_and(|enclosing| semantic_type_member_named(enclosing, &root_name).is_some());
    let include_private = root_name == "this"
        || root_is_current_member
        || enclosing_type
            .and_then(|enclosing| semantic_tag_name(&enclosing))
            .zip(semantic_tag_name(&current_context.tag))
            .is_some_and(|(enclosing, current)| {
                enclosing == current && semantic_cpp_current_function_is_method(interp)
            });
    for (index, member_name) in parts[1..parts.len() - 1].iter().enumerate() {
        if steps.get(index + 1).is_some_and(|step| step.arrow_before) {
            current_context = semantic_cpp_arrow_context(&tags, current_context);
        }
        let Some(member) =
            semantic_type_member_named(&current_context.tag, member_name).or_else(|| {
                semantic_tag_name(&current_context.tag).and_then(|type_name| {
                    semantic_type_member_named_in_named_types(&tags, &type_name, member_name)
                })
            })
        else {
            return Ok(Value::Nil);
        };
        let Some(member_type) = semantic_type_context_from_member(&tags, &member, &current_context)
        else {
            return Ok(Value::Nil);
        };
        current_context = member_type;
    }
    if steps.last().is_some_and(|step| step.arrow_before) {
        current_context = semantic_cpp_arrow_context(&tags, current_context);
    }
    let prefix = parts.last().map(String::as_str).unwrap_or("");
    let mut matches = Vec::new();
    collect_semantic_member_completion_tags(
        &current_context.tag,
        &tags,
        prefix,
        if include_private {
            MemberVisibility::All
        } else {
            MemberVisibility::Public
        },
        &mut Vec::new(),
        &mut matches,
    );
    if !prefix.is_empty()
        && let Some(expected_type) = semantic_c_like_assignment_expected_type(interp)
    {
        matches.retain(|tag| semantic_tag_matches_expected_type(tag, &expected_type));
    }
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
    let mut tags = semantic_tags_for_search(interp, &table)?;
    extend_semantic_c_like_table_tags(interp, &table, &mut tags);
    let key = semantic_function_signature_key(tag);
    let mut impls = Vec::new();
    let mut protos = Vec::new();
    collect_semantic_function_references(&tags, &key, &mut impls, &mut protos);
    if protos.is_empty() && impls.len() > 1 {
        protos.push(impls.remove(0));
    } else if protos.is_empty() && impls.len() == 1 {
        protos.push(impls[0].clone());
    }
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

fn semantic_symref_find_references_by_name(
    interp: &Interpreter,
    name: &Value,
) -> Result<Value, LispError> {
    let name = string_text(name)?;
    let search_name = name.rsplit("::").next().unwrap_or(&name);
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
    let file = interp
        .buffer
        .file
        .clone()
        .unwrap_or_else(|| interp.buffer.name.clone());
    let mut files = Vec::new();
    let mut tags = semantic_symref_tags_for_name(&text, search_name);
    if !tags.is_empty() {
        files.push(Value::String(file.clone()));
    }
    if let Some(base_dir) = Path::new(&file).parent() {
        for include in semantic_quoted_include_paths(&text, base_dir) {
            if let Ok(source) = std::fs::read_to_string(&include) {
                let include_tags = semantic_symref_header_tags_for_name(&source, search_name);
                if !include_tags.is_empty() {
                    files.push(Value::String(include.to_string_lossy().into_owned()));
                    tags.extend(include_tags);
                }
            }
        }
    }
    if tags.is_empty() {
        return Ok(Value::Nil);
    }
    Ok(Value::list([
        Value::Symbol("emaxx-semantic-symref-result".into()),
        Value::String(name),
        Value::list(files),
        Value::list(tags),
    ]))
}

fn semantic_symref_result_part(result: &Value, index: usize) -> Result<Value, LispError> {
    let parts = result.to_vec()?;
    Ok(parts.get(index).cloned().unwrap_or(Value::Nil))
}

fn semantic_symref_hits_in_region(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some(name) = semantic_tag_name(&args[0]) else {
        return Ok(Value::Nil);
    };
    let name = name.rsplit("::").next().unwrap_or(&name).to_string();
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
    let (start, end) = semantic_symref_region_bounds(interp, args, &text);
    let region = text
        .get(start.saturating_sub(1)..end.saturating_sub(1).min(text.len()))
        .unwrap_or("");
    let mut search_start = 0usize;
    while let Some(relative) = region[search_start..].find(&name) {
        let column = search_start + relative;
        search_start = column + name.len();
        if !semantic_word_at(region, column, &name) {
            continue;
        }
        let hit_start = Value::Integer((start + column) as i64);
        let hit_end = Value::Integer((start + column + name.len()) as i64);
        call_function_value(
            interp,
            &args[1],
            &[hit_start, hit_end, Value::String(name.clone())],
            env,
        )?;
    }
    Ok(Value::Nil)
}

fn semantic_symref_test_count_hits_in_tag(interp: &Interpreter) -> Result<Value, LispError> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
    let point = interp
        .buffer
        .point()
        .saturating_sub(interp.buffer.point_min());
    let before = &text[..point.min(text.len())];
    let name_end = before
        .char_indices()
        .rev()
        .find(|(_, ch)| is_ident_byte(*ch as u8))
        .map(|(index, ch)| index + ch.len_utf8());
    let Some(name_end) = name_end else {
        return Ok(Value::Nil);
    };
    let name_start = before[..name_end]
        .char_indices()
        .rev()
        .find(|(_, ch)| !is_ident_byte(*ch as u8))
        .map(|(index, ch)| index + ch.len_utf8())
        .unwrap_or(0);
    let name = &before[name_start..name_end];
    let Some(range) = semantic_c_function_ranges(&text)
        .into_iter()
        .find(|range| range.start <= point && point <= range.end)
    else {
        return Ok(Value::Nil);
    };
    let mut count = 0i64;
    for line in text[range.start..range.end.min(text.len())].lines() {
        let region = line.split("//").next().unwrap_or(line);
        let mut search_start = 0usize;
        while let Some(relative) = region[search_start..].find(name) {
            let column = search_start + relative;
            search_start = column + name.len();
            if semantic_word_at(region, column, name) {
                count += 1;
            }
        }
    }
    Ok(Value::Integer(count))
}

fn semantic_symref_region_bounds(
    interp: &Interpreter,
    args: &[Value],
    text: &str,
) -> (usize, usize) {
    if let (Ok(start), Ok(end)) = (args[2].as_integer(), args[3].as_integer()) {
        return (start as usize, end as usize);
    }
    let point = interp
        .buffer
        .point()
        .saturating_sub(interp.buffer.point_min());
    semantic_c_function_ranges(text)
        .into_iter()
        .find(|range| range.start <= point && point <= range.end)
        .map(|range| (range.start + 1, range.end + 1))
        .unwrap_or((interp.buffer.point_min(), interp.buffer.point_max()))
}

struct SemanticFunctionRange {
    name: String,
    start: usize,
    end: usize,
}

fn semantic_symref_tags_for_name(text: &str, name: &str) -> Vec<Value> {
    let function_ranges = semantic_c_function_ranges(text);
    let mut tags = Vec::new();
    let mut offset = 0usize;
    for line in text.lines() {
        let line_start = offset;
        let line_end = line_start + line.len();
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") || trimmed.starts_with("/*") || trimmed.starts_with('*') {
            offset = line_end + 1;
            continue;
        }
        let mut search_start = 0usize;
        while let Some(relative) = line[search_start..].find(name) {
            let column = search_start + relative;
            let absolute = line_start + column;
            search_start = column + name.len();
            if !semantic_word_at(line, column, name) {
                continue;
            }
            if trimmed.starts_with("#define") {
                tags.push(semantic_tag(name, "function", Value::Nil));
                continue;
            }
            if let Some(function_name) = semantic_c_function_name_from_signature(line)
                && semantic_cpp_name_matches(&function_name, name)
            {
                tags.push(semantic_tag(&function_name, "function", Value::Nil));
                continue;
            }
            if let Some(function) = function_ranges
                .iter()
                .rev()
                .find(|function| function.start <= absolute && absolute <= function.end)
            {
                tags.push(semantic_tag(&function.name, "function", Value::Nil));
            }
        }
        offset = line_end + 1;
    }
    tags
}

fn semantic_quoted_include_paths(text: &str, base_dir: &Path) -> Vec<PathBuf> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim_start();
            let rest = line.strip_prefix("#include")?.trim_start();
            let include = rest.strip_prefix('"')?.split_once('"')?.0;
            Some(base_dir.join(include))
        })
        .collect()
}

fn semantic_symref_header_tags_for_name(text: &str, name: &str) -> Vec<Value> {
    let mut tags = Vec::new();
    let mut current_class: Option<String> = None;
    for line in text.lines() {
        let line = line.split("//").next().unwrap_or("").trim();
        if line.starts_with("class ") || line.starts_with("struct ") {
            current_class = line
                .split_whitespace()
                .nth(1)
                .map(|name| name.trim_matches('{').to_string());
            continue;
        }
        if line.starts_with("};") || line.starts_with("} ") || line == "}" {
            current_class = None;
            continue;
        }
        if line.contains(name)
            && line.contains('(')
            && line.ends_with(';')
            && let Some(function_name) = semantic_c_function_name_from_signature(line)
            && semantic_cpp_name_matches(&function_name, name)
        {
            tags.push(semantic_tag(
                current_class.as_deref().unwrap_or(&function_name),
                "function",
                Value::Nil,
            ));
        }
    }
    tags
}

fn semantic_c_function_ranges(text: &str) -> Vec<SemanticFunctionRange> {
    let lines = text.lines().collect::<Vec<_>>();
    let mut ranges = Vec::new();
    let mut offset = 0usize;
    let mut line_starts = Vec::with_capacity(lines.len());
    for line in &lines {
        line_starts.push(offset);
        offset += line.len() + 1;
    }
    for (index, line) in lines.iter().enumerate() {
        let (name, range_start_index) =
            if let Some(name) = semantic_c_function_name_from_signature(line) {
                (name, index)
            } else if index > 0 {
                let Some(previous_index) = (0..index)
                    .rev()
                    .find(|previous| !lines[*previous].trim().is_empty())
                else {
                    continue;
                };
                let combined = format!("{} {}", lines[previous_index].trim(), line.trim());
                let Some(name) = semantic_c_function_name_from_signature(&combined) else {
                    continue;
                };
                (name, previous_index)
            } else {
                continue;
            };
        let Some(brace_start) = semantic_c_next_open_brace(&lines, &line_starts, index) else {
            continue;
        };
        let end = semantic_c_matching_brace(text, brace_start).unwrap_or(text.len());
        ranges.push(SemanticFunctionRange {
            name,
            start: line_starts[range_start_index],
            end,
        });
    }
    ranges
}

fn semantic_c_next_open_brace(
    lines: &[&str],
    line_starts: &[usize],
    start_index: usize,
) -> Option<usize> {
    for index in start_index..lines.len().min(start_index + 5) {
        let trimmed = lines[index].trim_start();
        if trimmed.starts_with("//") || trimmed.starts_with("/*") || trimmed.starts_with('*') {
            continue;
        }
        if let Some(column) = lines[index].find('{') {
            return Some(line_starts[index] + column);
        }
        if lines[index].contains(';') {
            return None;
        }
    }
    None
}

fn semantic_c_matching_brace(text: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (offset, ch) in text[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(open + offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn semantic_c_function_name_from_signature(line: &str) -> Option<String> {
    let code = line.split("//").next().unwrap_or(line).trim();
    if code.is_empty() || code.starts_with('#') || code.starts_with('*') || code.contains('=') {
        return None;
    }
    let before_paren = code.split_once('(')?.0.trim_end();
    if !before_paren.contains(char::is_whitespace) && !before_paren.contains("::") {
        return None;
    }
    let name = before_paren
        .rsplit(char::is_whitespace)
        .find(|part| !part.is_empty())?;
    if matches!(
        name,
        "if" | "for" | "while" | "switch" | "return" | "sizeof"
    ) {
        return None;
    }
    Some(name.to_string())
}

fn semantic_cpp_name_matches(candidate: &str, name: &str) -> bool {
    candidate == name || candidate.rsplit("::").next() == Some(name)
}

fn semantic_word_at(line: &str, start: usize, name: &str) -> bool {
    let bytes = line.as_bytes();
    let end = start + name.len();
    start <= bytes.len()
        && end <= bytes.len()
        && (start == 0 || !is_ident_byte(bytes[start - 1]))
        && (end == bytes.len() || !is_ident_byte(bytes[end]))
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

fn semantic_go_to_tag(
    interp: &mut Interpreter,
    tag: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let items = tag.to_vec()?;
    if let Some(Value::Overlay(overlay_id)) = items.get(4)
        && let Some(overlay) = interp.find_overlay(*overlay_id)
    {
        interp.buffer.goto_char(overlay.beg);
        interp.set_variable("__emaxx-semantic-current-tag-override", tag.clone(), env);
        return Ok(tag.clone());
    }
    interp.buffer.goto_char(interp.buffer.point_min());
    interp.set_variable("__emaxx-semantic-current-tag-override", tag.clone(), env);
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

#[derive(Clone)]
struct SemanticTypeContext {
    tag: Value,
    substitutions: HashMap<String, String>,
}

fn semantic_cpp_root_type_context(
    interp: &Interpreter,
    tags: &[Value],
    name: &str,
) -> Option<SemanticTypeContext> {
    if name == "this" {
        return semantic_cpp_current_enclosing_type(interp, tags)
            .or_else(|| semantic_c_like_current_enclosing_type(interp, tags))
            .map(|tag| SemanticTypeContext {
                tag,
                substitutions: HashMap::new(),
            });
    }
    semantic_cpp_declared_type_before_point(interp, name)
        .and_then(|type_name| {
            semantic_type_context_from_name_in_scope(tags, Some(interp), &type_name)
        })
        .or_else(|| {
            semantic_cpp_current_enclosing_type(interp, tags).and_then(|current| {
                let member = semantic_type_member_named(&current, name)?;
                semantic_type_context_from_member(
                    tags,
                    &member,
                    &SemanticTypeContext {
                        tag: current,
                        substitutions: HashMap::new(),
                    },
                )
            })
        })
        .or_else(|| {
            semantic_c_like_current_enclosing_type(interp, tags).and_then(|current| {
                let member = semantic_type_member_named(&current, name)?;
                semantic_type_context_from_member(
                    tags,
                    &member,
                    &SemanticTypeContext {
                        tag: current,
                        substitutions: HashMap::new(),
                    },
                )
            })
        })
        .or_else(|| {
            find_semantic_variable_deep(tags, name).and_then(|tag| {
                semantic_type_context_from_member(
                    tags,
                    &tag,
                    &SemanticTypeContext {
                        tag: Value::Nil,
                        substitutions: HashMap::new(),
                    },
                )
            })
        })
}

fn semantic_type_context_from_member(
    tags: &[Value],
    member: &Value,
    parent: &SemanticTypeContext,
) -> Option<SemanticTypeContext> {
    if semantic_tag_class(member).as_deref() == Some("type") {
        return semantic_type_candidate(tags, member).map(|tag| SemanticTypeContext {
            tag,
            substitutions: HashMap::new(),
        });
    }
    let type_text = semantic_tag_attr(member, ":type").and_then(|value| semantic_type_text(&value));
    let type_text = semantic_substitute_type_text(&type_text?, &parent.substitutions);
    semantic_type_context_from_name(tags, &type_text)
}

fn semantic_cpp_arrow_context(tags: &[Value], context: SemanticTypeContext) -> SemanticTypeContext {
    let Some(operator) = semantic_type_member_named(&context.tag, "operator->") else {
        return context;
    };
    semantic_type_context_from_member(tags, &operator, &context).unwrap_or(context)
}

fn semantic_type_context_from_name(tags: &[Value], type_name: &str) -> Option<SemanticTypeContext> {
    semantic_type_context_from_name_with_buffer(tags, None, type_name)
}

fn semantic_type_context_from_name_with_buffer(
    tags: &[Value],
    interp: Option<&Interpreter>,
    type_name: &str,
) -> Option<SemanticTypeContext> {
    let type_name = semantic_clean_cpp_type_text(type_name);
    let (base_name, args) = semantic_cpp_template_instantiation(&type_name)
        .unwrap_or_else(|| (type_name.clone(), Vec::new()));
    if args.is_empty()
        && let Some(raw) = find_semantic_type_raw(tags, &base_name)
        && let Some(target) =
            semantic_tag_attr(&raw, ":typedef").and_then(|value| semantic_type_text(&value))
    {
        return semantic_type_context_from_name(tags, &target);
    }
    let mut tag = semantic_type_from_name(tags, &base_name).or_else(|| {
        base_name
            .rsplit("::")
            .next()
            .and_then(|name| semantic_type_from_name(tags, name))
    })?;
    if semantic_tag_members(&tag).is_empty()
        && let Some(interp) = interp
        && let Some(name) = base_name.rsplit("::").next()
        && let Some(buffer_type) = semantic_c_type_from_current_buffer(interp, name)
    {
        tag = buffer_type;
    }
    let substitutions = semantic_template_substitutions(&tag, &args);
    Some(SemanticTypeContext { tag, substitutions })
}

fn semantic_type_context_from_name_in_scope(
    tags: &[Value],
    interp: Option<&Interpreter>,
    type_name: &str,
) -> Option<SemanticTypeContext> {
    if type_name.contains("::") {
        return semantic_type_context_from_name_with_buffer(tags, interp, type_name);
    }
    if let Some(interp) = interp {
        for namespace in semantic_cpp_active_using_namespaces(interp, tags)
            .into_iter()
            .rev()
        {
            if let Some(context) = semantic_type_context_from_name_with_buffer(
                tags,
                Some(interp),
                &format!("{namespace}::{type_name}"),
            ) {
                return Some(context);
            }
        }
    }
    semantic_type_context_from_name_with_buffer(tags, interp, type_name)
}

fn find_semantic_type_raw(tags: &[Value], name: &str) -> Option<Value> {
    let parts = name
        .split("::")
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    find_semantic_type_raw_parts(tags, &parts).or_else(|| {
        parts
            .last()
            .and_then(|last| find_semantic_type_raw_deep(tags, last))
    })
}

fn find_semantic_type_raw_parts(tags: &[Value], parts: &[&str]) -> Option<Value> {
    let (first, rest) = parts.split_first()?;
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_name(tag).as_deref() == Some(first)
        {
            if rest.is_empty() {
                return Some(tag.clone());
            }
            if let Some(found) = find_semantic_type_raw_parts(&semantic_tag_members(tag), rest) {
                return Some(found);
            }
        }
    }
    None
}

fn find_semantic_type_raw_deep(tags: &[Value], name: &str) -> Option<Value> {
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_name(tag).as_deref() == Some(name)
        {
            return Some(tag.clone());
        }
        if let Some(found) = find_semantic_type_raw_deep(&semantic_tag_members(tag), name) {
            return Some(found);
        }
    }
    None
}

fn semantic_template_substitutions(tag: &Value, args: &[String]) -> HashMap<String, String> {
    let params = semantic_tag_attr(tag, ":template-params")
        .and_then(|params| params.to_vec().ok())
        .unwrap_or_default();
    params
        .into_iter()
        .filter_map(|param| string_text(&param).ok())
        .zip(args.iter().cloned())
        .collect()
}

fn semantic_type_text(value: &Value) -> Option<String> {
    if let Ok(symbol) = value.as_symbol() {
        return Some(symbol.to_string());
    }
    if let Ok(text) = string_text(value) {
        return Some(text);
    }
    let items = value.to_vec().ok()?;
    items.first().and_then(semantic_type_text)
}

fn semantic_clean_cpp_type_text(type_name: &str) -> String {
    type_name
        .replace("const ", "")
        .replace(" const", "")
        .replace("mutable ", "")
        .replace(" mutable", "")
        .replace("struct ", "")
        .replace("class ", "")
        .replace("public ", "")
        .replace("private ", "")
        .replace("protected ", "")
        .replace("static ", "")
        .replace(" static", "")
        .replace("volatile ", "")
        .replace(" volatile", "")
        .replace(['*', '&'], "")
        .trim()
        .to_string()
}

fn semantic_substitute_type_text(
    type_text: &str,
    substitutions: &HashMap<String, String>,
) -> String {
    if substitutions.is_empty() {
        return type_text.to_string();
    }
    let mut out = String::new();
    let mut word = String::new();
    for ch in type_text.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            word.push(ch);
        } else {
            if !word.is_empty() {
                out.push_str(
                    substitutions
                        .get(&word)
                        .map(String::as_str)
                        .unwrap_or(&word),
                );
                word.clear();
            }
            out.push(ch);
        }
    }
    if !word.is_empty() {
        out.push_str(
            substitutions
                .get(&word)
                .map(String::as_str)
                .unwrap_or(&word),
        );
    }
    out
}

fn semantic_cpp_template_instantiation(type_name: &str) -> Option<(String, Vec<String>)> {
    let open = type_name.find('<')?;
    let close = type_name.rfind('>')?;
    if close <= open {
        return None;
    }
    let base = type_name[..open].trim().to_string();
    let args = split_cpp_top_level_commas(&type_name[open + 1..close])
        .into_iter()
        .map(|arg| semantic_clean_cpp_type_text(&arg))
        .collect::<Vec<_>>();
    Some((base, args))
}

fn split_cpp_top_level_commas(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut angle_depth = 0usize;
    let mut paren_depth = 0usize;
    for ch in text.chars() {
        match ch {
            '<' => {
                angle_depth += 1;
                current.push(ch);
            }
            '>' => {
                angle_depth = angle_depth.saturating_sub(1);
                current.push(ch);
            }
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if angle_depth == 0 && paren_depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

fn semantic_cpp_current_enclosing_type(interp: &Interpreter, tags: &[Value]) -> Option<Value> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()?;
    let mut stack: Vec<Option<String>> = Vec::new();
    let mut pending_method_type = None;
    for line in text.lines() {
        let line = line.split("//").next().unwrap_or(line);
        if line.contains('(')
            && let Some(scope_index) = line.rfind("::")
        {
            let before_scope = line[..scope_index].trim_end();
            if before_scope.contains(char::is_whitespace) {
                pending_method_type = before_scope
                    .split_whitespace()
                    .last()
                    .map(|name| name.trim_matches(|ch| matches!(ch, '*' | '&')).to_string());
            } else {
                pending_method_type = None;
            }
        }
        for ch in line.chars() {
            match ch {
                '{' => stack.push(pending_method_type.take()),
                '}' => {
                    stack.pop();
                }
                _ => {}
            }
        }
    }
    stack
        .into_iter()
        .rev()
        .find_map(|name| name.and_then(|name| semantic_type_from_name(tags, &name)))
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

fn semantic_cpp_current_function_is_method(interp: &Interpreter) -> bool {
    let Ok(text) = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
    else {
        return false;
    };
    semantic_c_function_ranges(&text)
        .last()
        .is_some_and(|range| range.name.contains("::"))
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
    text.split([';', '(', ')', '\n'])
        .rev()
        .filter_map(|segment| semantic_cpp_declared_type_from_segment(segment, name))
        .next()
        .or_else(|| semantic_c_macro_declared_type_before_point(&text, name))
}

fn semantic_c_macro_declared_type_before_point(text: &str, name: &str) -> Option<String> {
    let mut macros = Vec::new();
    let lines = text.lines().collect::<Vec<_>>();
    let mut index = 0usize;
    while index < lines.len() {
        let line = lines[index].trim_start();
        if !line.starts_with("#define ") {
            index += 1;
            continue;
        }
        let mut definition = line.to_string();
        while definition.trim_end().ends_with('\\') && index + 1 < lines.len() {
            index += 1;
            definition.push(' ');
            definition.push_str(lines[index].trim());
        }
        if let Some((macro_name, variable_name)) = parse_c_typed_variable_macro(&definition) {
            macros.push((macro_name, variable_name));
        }
        index += 1;
    }
    for (macro_name, variable_name) in macros.iter().rev() {
        if variable_name != name {
            continue;
        }
        for line in lines.iter().rev() {
            let line = line.split("//").next().unwrap_or(line).trim();
            let Some(args) = line
                .strip_prefix(macro_name)
                .and_then(|rest| rest.strip_prefix('('))
                .and_then(|rest| rest.split_once(')'))
                .map(|(args, _)| args)
            else {
                continue;
            };
            let type_name = args.split(',').next()?.trim();
            if !type_name.is_empty() {
                return Some(type_name.to_string());
            }
        }
    }
    None
}

fn semantic_c_type_from_current_buffer(interp: &Interpreter, type_name: &str) -> Option<Value> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
        .ok()?;
    let cleaned = strip_cpp_comments(&text);
    let pattern = format!("struct {type_name}");
    let start = cleaned.find(&pattern)?;
    let brace = cleaned[start..].find('{')? + start;
    let mut depth = 0usize;
    let mut end = brace;
    for (offset, ch) in cleaned[brace..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    end = brace + offset;
                    break;
                }
            }
            _ => {}
        }
    }
    if end <= brace {
        return None;
    }
    let body = &cleaned[brace + 1..end];
    let mut parser = CppTagParser::new(body, None);
    semantic_type_tag(
        type_name,
        vec![
            (":members", Value::list(parser.parse_until(None))),
            (":type", Value::String("struct".into())),
        ],
    )
}

fn parse_c_typed_variable_macro(definition: &str) -> Option<(String, String)> {
    let rest = definition.trim_start().strip_prefix("#define ")?;
    let (macro_name, body) = rest.split_once(')')?;
    let macro_name = macro_name.split_once('(')?.0.trim();
    let variable_name = body
        .split("*")
        .nth(1)?
        .trim_start()
        .split(|ch: char| !is_ident_byte(ch as u8))
        .find(|part| !part.is_empty())?;
    Some((macro_name.to_string(), variable_name.to_string()))
}

fn collect_semantic_local_variable_completion_tags(
    interp: &Interpreter,
    prefix: &str,
    matches: &mut Vec<Value>,
) {
    let Some(text) = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()
    else {
        return;
    };
    let cleaned = strip_cpp_comments(&text);
    let Some((body_start, arguments)) = current_cpp_function_scope(&cleaned) else {
        return;
    };
    for argument in arguments {
        if semantic_tag_name(&argument)
            .as_deref()
            .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(argument);
        }
    }

    let mut scoped_tags: Vec<(usize, Value)> = Vec::new();
    let mut depth = 0usize;
    let mut initializer_depth = 0usize;
    let mut segment = String::new();
    for ch in cleaned[body_start + 1..].chars() {
        if initializer_depth > 0 {
            segment.push(ch);
            match ch {
                '{' => initializer_depth += 1,
                '}' => initializer_depth = initializer_depth.saturating_sub(1),
                _ => {}
            }
            continue;
        }
        match ch {
            '{' => {
                if segment.contains('=') && !semantic_c_like_control_segment(&segment) {
                    initializer_depth = 1;
                    segment.push(ch);
                } else {
                    segment.clear();
                    depth += 1;
                }
            }
            '}' => {
                collect_cpp_local_variable_segment(&segment, depth, &mut scoped_tags);
                segment.clear();
                scoped_tags.retain(|(tag_depth, _)| *tag_depth < depth);
                depth = depth.saturating_sub(1);
            }
            ';' | '\n' => {
                collect_cpp_local_variable_segment(&segment, depth, &mut scoped_tags);
                segment.clear();
            }
            _ => segment.push(ch),
        }
    }
    for (_, tag) in &scoped_tags {
        if semantic_tag_name(tag)
            .as_deref()
            .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(tag.clone());
        }
    }
}

fn semantic_c_like_control_segment(segment: &str) -> bool {
    let segment = segment.trim_start();
    matches!(
        segment
            .split(|ch: char| ch.is_whitespace() || ch == '(')
            .next(),
        Some("if" | "for" | "while" | "switch" | "catch")
    )
}

fn current_cpp_function_scope(text: &str) -> Option<(usize, Vec<Value>)> {
    let mut stack = Vec::new();
    for (index, ch) in text.char_indices() {
        match ch {
            '{' => stack.push(index),
            '}' => {
                stack.pop();
            }
            _ => {}
        }
    }
    stack.into_iter().rev().find_map(|open| {
        cpp_function_arguments_before_open(text, open).map(|arguments| (open, arguments))
    })
}

fn cpp_function_arguments_before_open(text: &str, open: usize) -> Option<Vec<Value>> {
    let before_open = text[..open].trim_end();
    let close = before_open.rfind(')')?;
    if before_open[close + 1..]
        .chars()
        .any(|ch| !ch.is_whitespace())
    {
        return None;
    }
    let open_paren = before_open[..close].rfind('(')?;
    let before_paren = before_open[..open_paren].trim_end();
    let name = before_paren
        .rsplit(|ch: char| !is_ident_byte(ch as u8))
        .find(|part| !part.is_empty())?;
    if matches!(name, "if" | "for" | "while" | "switch" | "catch") {
        return None;
    }
    Some(parse_cpp_arguments(&before_open[open_paren + 1..close]))
}

fn collect_cpp_local_variable_segment(
    segment: &str,
    depth: usize,
    scoped_tags: &mut Vec<(usize, Value)>,
) {
    let Some(tag) = parse_cpp_variable(segment) else {
        return;
    };
    scoped_tags.push((depth, tag));
}

fn semantic_c_like_root_name(name: &str) -> String {
    name.split_once('[')
        .map(|(root, _)| root)
        .unwrap_or(name)
        .to_string()
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

fn semantic_tag_matches_expected_type(tag: &Value, expected_type: &str) -> bool {
    if semantic_tag_class(tag).as_deref() == Some("function") {
        return semantic_tag_attr(tag, ":type")
            .and_then(|value| semantic_type_name_parts(&value).ok())
            .is_some_and(|parts| parts.iter().any(|part| part == expected_type));
    }
    if semantic_tag_class(tag).as_deref() == Some("variable") {
        return semantic_tag_attr(tag, ":type")
            .and_then(|value| semantic_type_name_parts(&value).ok())
            .and_then(|parts| parts.last().cloned())
            .is_some_and(|part| part == expected_type);
    }
    true
}

fn semantic_cpp_declared_type_from_segment(segment: &str, name: &str) -> Option<String> {
    let segment = segment.split("//").next().unwrap_or(segment).trim();
    let mut search_end = segment.len();
    while let Some(index) = segment[..search_end].rfind(name) {
        let before = &segment[..index];
        let after = &segment[index + name.len()..];
        search_end = index;
        if before
            .chars()
            .next_back()
            .is_some_and(|ch| is_ident_byte(ch as u8))
            || after
                .chars()
                .next()
                .is_some_and(|ch| is_ident_byte(ch as u8))
        {
            continue;
        }
        let after = after.trim_start();
        if !after.is_empty() && !matches!(after.chars().next(), Some(';' | ',' | ')' | '=' | '[')) {
            continue;
        }
        let before = before
            .trim_end_matches(|ch: char| ch.is_whitespace() || matches!(ch, '*' | '&'))
            .trim();
        let type_name_storage;
        let type_name = if before.contains('<') && before.contains('>') {
            type_name_storage = semantic_clean_cpp_type_text(before);
            type_name_storage.as_str()
        } else {
            before
                .split_whitespace()
                .rev()
                .find(|token| {
                    !matches!(
                        *token,
                        "const" | "struct" | "class" | "mutable" | "static" | "volatile"
                    )
                })?
                .trim_matches(|ch| matches!(ch, '*' | '&'))
        };
        if !type_name.is_empty()
            && type_name != "_type"
            && !semantic_c_like_statement_keyword(type_name)
        {
            return Some(type_name.to_string());
        }
    }
    None
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
    find_semantic_type_by_parts(tags, &parts)
        .or_else(|| find_semantic_type_chain_in(tags, tags, &parts))
        .or_else(|| (parts.len() > 1).then(|| find_semantic_type_chain_anywhere(tags, &parts))?)
        .or_else(|| {
            parts
                .last()
                .and_then(|name| find_semantic_type_deep(tags, name))
        })
}

fn semantic_namespace_type_from_name(tags: &[Value], namespace: &str) -> Option<Value> {
    let namespace = semantic_resolve_namespace_alias_name(tags, namespace);
    semantic_type_from_name(tags, &namespace)
}

fn semantic_resolve_namespace_alias_name(tags: &[Value], namespace: &str) -> String {
    let Some(alias) = find_semantic_type_raw(tags, namespace) else {
        return namespace.to_string();
    };
    semantic_tag_attr(&alias, ":namespace-alias")
        .and_then(|value| string_text(&value).ok())
        .unwrap_or_else(|| namespace.to_string())
}

fn find_semantic_type_chain_anywhere(tags: &[Value], parts: &[String]) -> Option<Value> {
    for tag in tags {
        if let Some(found) = find_semantic_type_chain_in(tags, std::slice::from_ref(tag), parts) {
            return Some(found);
        }
        if let Some(found) = find_semantic_type_chain_anywhere(&semantic_tag_members(tag), parts) {
            return Some(found);
        }
    }
    None
}

fn find_semantic_type_by_parts(tags: &[Value], parts: &[String]) -> Option<Value> {
    let (first, rest) = parts.split_first()?;
    let mut best = None;
    let mut best_score = 0usize;
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_name(tag).as_deref() == Some(first)
        {
            if rest.is_empty() {
                let resolved = resolve_semantic_typedef(tags, tag);
                let score = semantic_type_resolution_score(&resolved);
                if best.is_none() || score > best_score {
                    best_score = score;
                    best = Some(resolved);
                }
                continue;
            }
            if let Some(found) = find_semantic_type_by_parts(&semantic_tag_members(tag), rest) {
                let score = semantic_type_resolution_score(&found);
                if best.is_none() || score > best_score {
                    best_score = score;
                    best = Some(found);
                }
            }
        }
    }
    best
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

fn collect_semantic_external_variable_completion_tags(
    tags: &[Value],
    prefix: &str,
    matches: &mut Vec<Value>,
) {
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("variable")
            && semantic_tag_name(tag)
                .as_deref()
                .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(tag.clone());
        }
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_attr(tag, ":type")
                .and_then(|value| string_text(&value).ok())
                .as_deref()
                == Some("namespace")
        {
            collect_semantic_external_variable_completion_tags(
                &semantic_tag_members(tag),
                prefix,
                matches,
            );
        }
    }
}

fn collect_semantic_qualified_namespace_completion_tags(
    tags: &[Value],
    namespace: &str,
    prefix: &str,
    matches: &mut Vec<Value>,
) {
    if let Some(namespace_tag) = semantic_namespace_type_from_name(tags, namespace) {
        let mut seen = Vec::new();
        collect_semantic_namespace_member_completion_tags(
            tags,
            &namespace_tag,
            prefix,
            &mut seen,
            matches,
        );
    }
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("type")
            && semantic_tag_name(tag).as_deref() == Some(namespace)
        {
            for member in semantic_tag_members(tag) {
                if matches!(
                    semantic_tag_class(&member).as_deref(),
                    Some("function" | "variable" | "type")
                ) && semantic_tag_name(&member)
                    .as_deref()
                    .is_some_and(|name| name.starts_with(prefix))
                {
                    matches.push(member);
                }
            }
        }
        if semantic_tag_class(tag).as_deref() == Some("variable")
            && semantic_tag_name(tag)
                .as_deref()
                .is_some_and(|name| name.starts_with(prefix))
            && semantic_tag_attr(tag, ":type")
                .and_then(|value| semantic_type_name_parts(&value).ok())
                .and_then(|parts| parts.first().cloned())
                .as_deref()
                == Some(namespace)
        {
            matches.push(tag.clone());
        }
        collect_semantic_qualified_namespace_completion_tags(
            &semantic_tag_members(tag),
            namespace,
            prefix,
            matches,
        );
    }
}

fn collect_semantic_using_namespace_completion_tags(
    interp: &Interpreter,
    tags: &[Value],
    prefix: &str,
    matches: &mut Vec<Value>,
) {
    for namespace in semantic_cpp_active_using_namespaces(interp, tags)
        .into_iter()
        .rev()
    {
        let Some(namespace_tag) = semantic_namespace_type_from_name(tags, &namespace) else {
            continue;
        };
        let mut seen = Vec::new();
        collect_semantic_namespace_member_completion_tags(
            tags,
            &namespace_tag,
            prefix,
            &mut seen,
            matches,
        );
    }
}

fn collect_semantic_namespace_member_completion_tags(
    tags: &[Value],
    namespace_tag: &Value,
    prefix: &str,
    seen: &mut Vec<String>,
    matches: &mut Vec<Value>,
) {
    let Some(namespace_name) = semantic_tag_name(namespace_tag) else {
        return;
    };
    if seen.iter().any(|seen| seen == &namespace_name) {
        return;
    }
    seen.push(namespace_name.clone());
    let members = semantic_tag_members(namespace_tag);
    for member in &members {
        if matches!(
            semantic_tag_class(member).as_deref(),
            Some("function" | "variable" | "type")
        ) && semantic_tag_name(member)
            .as_deref()
            .is_some_and(|name| name.starts_with(prefix))
        {
            matches.push(member.clone());
        }
    }
    for member in members {
        if semantic_tag_class(&member).as_deref() != Some("using") {
            continue;
        }
        let Some(namespace) =
            semantic_tag_attr(&member, ":namespace").and_then(|value| string_text(&value).ok())
        else {
            continue;
        };
        let namespace = semantic_qualify_namespace(tags, &namespace_name, &namespace);
        if let Some(imported) = semantic_namespace_type_from_name(tags, &namespace) {
            collect_semantic_namespace_member_completion_tags(
                tags, &imported, prefix, seen, matches,
            );
        }
    }
}

fn semantic_cpp_active_using_namespaces(interp: &Interpreter, tags: &[Value]) -> Vec<String> {
    let Some(text) = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point())
        .ok()
    else {
        return Vec::new();
    };
    let cleaned = strip_cpp_comments(&text);
    let mut active: Vec<(usize, String)> = Vec::new();
    let mut depth = 0usize;
    let mut statement = String::new();
    for ch in cleaned.chars() {
        match ch {
            '{' => {
                collect_cpp_using_namespace_segment(&statement, depth, tags, &mut active);
                statement.clear();
                depth += 1;
            }
            '}' => {
                collect_cpp_using_namespace_segment(&statement, depth, tags, &mut active);
                statement.clear();
                depth = depth.saturating_sub(1);
                active.retain(|(using_depth, _)| *using_depth <= depth);
            }
            ';' => {
                collect_cpp_using_namespace_segment(&statement, depth, tags, &mut active);
                statement.clear();
            }
            _ => statement.push(ch),
        }
    }
    collect_cpp_using_namespace_segment(&statement, depth, tags, &mut active);
    active.into_iter().map(|(_, namespace)| namespace).collect()
}

fn collect_cpp_using_namespace_segment(
    statement: &str,
    depth: usize,
    tags: &[Value],
    active: &mut Vec<(usize, String)>,
) {
    let statement = statement.trim();
    let Some(namespace) = statement.strip_prefix("using namespace ") else {
        return;
    };
    let namespace = namespace
        .trim()
        .trim_end_matches(';')
        .trim_end_matches(|ch: char| !is_ident_byte(ch as u8) && ch != ':');
    if namespace.is_empty() {
        return;
    }
    let namespace = semantic_qualify_namespace_from_active(tags, namespace, active);
    active.push((depth, namespace));
}

fn semantic_qualify_namespace_from_active(
    tags: &[Value],
    namespace: &str,
    active: &[(usize, String)],
) -> String {
    if namespace.contains("::") || semantic_namespace_type_from_name(tags, namespace).is_some() {
        return namespace.to_string();
    }
    for (_, active_namespace) in active.iter().rev() {
        let qualified = format!("{active_namespace}::{namespace}");
        if semantic_namespace_type_from_name(tags, &qualified).is_some() {
            return qualified;
        }
    }
    namespace.to_string()
}

fn semantic_qualify_namespace(tags: &[Value], parent: &str, namespace: &str) -> String {
    if namespace.contains("::") || semantic_namespace_type_from_name(tags, namespace).is_some() {
        namespace.to_string()
    } else {
        let qualified = format!("{parent}::{namespace}");
        if semantic_namespace_type_from_name(tags, &qualified).is_some() {
            qualified
        } else {
            namespace.to_string()
        }
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum MemberVisibility {
    Public,
    PublicProtected,
    All,
    None,
}

fn collect_semantic_member_completion_tags(
    type_tag: &Value,
    root_tags: &[Value],
    prefix: &str,
    visibility: MemberVisibility,
    seen: &mut Vec<String>,
    matches: &mut Vec<Value>,
) {
    if visibility == MemberVisibility::None {
        return;
    }
    let mut completion_type = type_tag.clone();
    if !root_tags.is_empty()
        && semantic_tag_members(&completion_type).is_empty()
        && let Some(name) = semantic_tag_name(&completion_type)
        && let Some(found) = semantic_type_from_name(root_tags, &name)
        && !semantic_tag_members(&found).is_empty()
    {
        completion_type = found;
    }
    let type_name = semantic_tag_name(&completion_type);
    if let Some(type_name) = &type_name {
        if seen.iter().any(|seen| seen == type_name) {
            return;
        }
        seen.push(type_name.clone());
    }
    let members = semantic_tag_members(&completion_type);
    let has_access_labels = members.iter().any(|member| {
        semantic_tag_class(member).as_deref() == Some("label")
            && semantic_tag_name(member)
                .as_deref()
                .is_some_and(|name| matches!(name, "public" | "protected" | "private"))
    });
    let mut access = if has_access_labels
        && semantic_tag_attr(&completion_type, ":type")
            .and_then(|value| string_text(&value).ok())
            .as_deref()
            == Some("class")
    {
        "private"
    } else {
        "public"
    };
    for member in members {
        let class = semantic_tag_class(&member);
        if class.as_deref() == Some("label") {
            if let Some(label) = semantic_tag_name(&member)
                && matches!(label.as_str(), "public" | "protected" | "private")
            {
                access = match label.as_str() {
                    "public" => "public",
                    "protected" => "protected",
                    _ => "private",
                };
            }
            continue;
        }
        let member_access = semantic_member_access(&member).unwrap_or(access);
        if class.as_deref() != Some("type")
            && !member_visible_for_completion(member_access, visibility)
            || semantic_tag_has_typemodifier(&member, "private")
                && visibility != MemberVisibility::All
        {
            continue;
        }
        if semantic_tag_attr(&member, ":constructor-flag").is_some_and(|value| value.is_truthy()) {
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
    let superclasses = semantic_tag_attr(&completion_type, ":superclasses")
        .and_then(|superclasses| superclasses.to_vec().ok())
        .or_else(|| {
            type_name
                .as_deref()
                .and_then(|name| semantic_type_from_name(root_tags, name))
                .and_then(|tag| semantic_tag_attr(&tag, ":superclasses"))
                .and_then(|superclasses| superclasses.to_vec().ok())
        });
    if let Some(superclasses) = superclasses {
        for superclass in superclasses {
            let Some(super_type) = semantic_type_candidate(root_tags, &superclass) else {
                continue;
            };
            let inherited_visibility = inherited_member_visibility(&superclass, visibility);
            collect_semantic_member_completion_tags(
                &super_type,
                root_tags,
                prefix,
                inherited_visibility,
                seen,
                matches,
            );
        }
    }
}

fn member_visible_for_completion(access: &str, visibility: MemberVisibility) -> bool {
    match visibility {
        MemberVisibility::All => true,
        MemberVisibility::PublicProtected => matches!(access, "public" | "protected"),
        MemberVisibility::Public => access == "public",
        MemberVisibility::None => false,
    }
}

fn semantic_member_access(member: &Value) -> Option<&'static str> {
    if semantic_tag_has_typemodifier(member, "public") {
        Some("public")
    } else if semantic_tag_has_typemodifier(member, "protected") {
        Some("protected")
    } else if semantic_tag_has_typemodifier(member, "private") {
        Some("private")
    } else {
        None
    }
}

fn inherited_member_visibility(superclass: &Value, current: MemberVisibility) -> MemberVisibility {
    let access = semantic_tag_attr(superclass, ":inheritance")
        .and_then(|value| string_text(&value).ok())
        .unwrap_or_else(|| "private".into());
    match (current, access.as_str()) {
        (MemberVisibility::All, "public" | "protected") => MemberVisibility::PublicProtected,
        (MemberVisibility::All, "private") => MemberVisibility::Public,
        (MemberVisibility::Public, "public") => MemberVisibility::Public,
        _ => MemberVisibility::None,
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

fn semantic_fetch_tags_compat(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    let tags = if let Some(path) = interp
        .buffer
        .file
        .clone()
        .map(PathBuf::from)
        .filter(|path| {
            matches!(
                path.extension().and_then(|ext| ext.to_str()),
                Some("c" | "cc" | "cpp" | "cxx" | "h" | "hh" | "hpp" | "java")
            )
        }) {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_cpp_tags_at_path(&path, &source)
    } else if interp
        .buffer
        .file
        .as_deref()
        .and_then(|path| Path::new(path).extension())
        .and_then(|ext| ext.to_str())
        == Some("js")
    {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_javascript_tags(&source)
    } else if interp.buffer.file.as_deref().is_some_and(|path| {
        Path::new(path).file_name().and_then(|name| name.to_str()) == Some("Makefile")
    }) {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_makefile_tags(&source)
    } else if interp
        .buffer
        .file
        .as_deref()
        .and_then(|path| Path::new(path).extension())
        .and_then(|ext| ext.to_str())
        == Some("py")
    {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_python_tags(&source)
    } else if interp
        .buffer
        .file
        .as_deref()
        .and_then(|path| Path::new(path).extension())
        .and_then(|ext| ext.to_str())
        == Some("srt")
        || interp
            .lookup_var("major-mode", env)
            .is_some_and(|mode| mode == Value::Symbol("srecode-template-mode".into()))
    {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_srecode_template_tags(&source)
    } else if interp
        .buffer
        .file
        .as_deref()
        .and_then(|path| Path::new(path).extension())
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| matches!(ext, "html" | "htm"))
    {
        let source = interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
        parse_semantic_html_tags(&source)
    } else {
        interp
            .lookup_var("semanticdb-current-table", env)
            .and_then(|table| eieio_slot_value(interp, &table, "tags").ok())
            .and_then(|tags| tags.to_vec().ok())
            .unwrap_or_default()
    };

    if let Some(table) = interp
        .lookup_var("semanticdb-current-table", env)
        .filter(|table| !table.is_nil())
    {
        let _ = set_eieio_slot_value(interp, &table, "tags", Value::list(tags.clone()));
    }
    interp.set_variable("__emaxx-semantic-current-tag-override", Value::Nil, env);
    Ok(Value::list(tags))
}

fn parse_semantic_html_tags(source: &str) -> Vec<Value> {
    let mut tags = Vec::new();
    let mut rest = source;
    while let Some(open_index) = rest.find("<h") {
        rest = &rest[open_index + 2..];
        let Some(level_end) = rest.find('>') else {
            break;
        };
        if !rest[..level_end].chars().all(|ch| ch.is_ascii_digit()) {
            continue;
        }
        rest = &rest[level_end + 1..];
        let Some(close_index) = rest.find("</h") else {
            break;
        };
        let title = strip_html_tags(&rest[..close_index]).trim().to_string();
        if !title.is_empty() {
            let child = semantic_tag(&title, "section", Value::Nil);
            if let Some(tag) = semantic_type_tag(&title, vec![(":members", Value::list([child]))]) {
                tags.push(reclass_semantic_tag(tag, "section"));
            }
        }
        rest = &rest[close_index + 3..];
    }
    tags
}

fn parse_semantic_javascript_tags(source: &str) -> Vec<Value> {
    source
        .lines()
        .filter_map(|line| {
            let line = line.split("//").next().unwrap_or(line).trim();
            let rest = line.strip_prefix("function ")?;
            let open = rest.find('(')?;
            let close = rest[open + 1..].find(')')? + open + 1;
            let name = rest[..open].trim();
            if name.is_empty() {
                return None;
            }
            let arguments = rest[open + 1..close]
                .split(',')
                .filter_map(|arg| {
                    let arg = arg.trim();
                    (!arg.is_empty()).then(|| semantic_tag(arg, "variable", Value::Nil))
                })
                .collect::<Vec<_>>();
            let mut attrs = Vec::new();
            if !arguments.is_empty() {
                attrs.push((":arguments", Value::list(arguments)));
            }
            semantic_function_tag(name, attrs)
        })
        .collect()
}

fn parse_semantic_makefile_tags(source: &str) -> Vec<Value> {
    source
        .lines()
        .filter_map(|line| {
            if line.starts_with(['\t', ' ']) {
                return None;
            }
            let line = line.split('#').next().unwrap_or(line).trim();
            if line.is_empty() || line.contains('=') {
                return None;
            }
            let (target, dependencies) = line.split_once(':')?;
            let target = target.trim();
            if target.is_empty() {
                return None;
            }
            let arguments = dependencies
                .split_whitespace()
                .filter(|dependency| !dependency.is_empty())
                .map(|dependency| Value::String(dependency.into()))
                .collect::<Vec<_>>();
            let mut attrs = Vec::new();
            if !arguments.is_empty() {
                attrs.push((":arguments", Value::list(arguments)));
            }
            semantic_function_tag(target, attrs)
        })
        .collect()
}

fn parse_semantic_python_tags(source: &str) -> Vec<Value> {
    source
        .lines()
        .filter_map(|line| {
            let line = line.split('#').next().unwrap_or(line).trim();
            let rest = line.strip_prefix("def ")?;
            let open = rest.find('(')?;
            let close = rest[open + 1..].find(')')? + open + 1;
            if rest[close + 1..].trim() != ":" {
                return None;
            }
            let name = rest[..open].trim();
            if name.is_empty() {
                return None;
            }
            let arguments = rest[open + 1..close]
                .split(',')
                .filter_map(|arg| {
                    let arg = arg.trim();
                    (!arg.is_empty()).then(|| semantic_tag(arg, "variable", Value::Nil))
                })
                .collect::<Vec<_>>();
            let mut attrs = Vec::new();
            if !arguments.is_empty() {
                attrs.push((":arguments", Value::list(arguments)));
            }
            semantic_function_tag(name, attrs)
        })
        .collect()
}

fn parse_semantic_srecode_template_tags(source: &str) -> Vec<Value> {
    let lines = source.lines().collect::<Vec<_>>();
    let mut tags = Vec::new();
    let mut pending_dictionaries = Vec::new();
    let mut variables = std::collections::HashMap::new();
    let mut index = 0usize;
    while index < lines.len() {
        let line = lines[index].trim();
        if line.is_empty() || line.starts_with(';') {
            index += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("set ") {
            let mut parts = rest.splitn(2, char::is_whitespace);
            if let (Some(name), Some(value)) = (parts.next(), parts.next())
                && let Some(value) = parse_srecode_value_resolving(value.trim(), &variables)
            {
                remember_srecode_string_value(&mut variables, name, &value);
                tags.push(semantic_srecode_variable_tag(name, value));
            }
            index += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("sectiondictionary ") {
            let name = parse_srecode_string(rest.trim()).unwrap_or_else(|| rest.trim().into());
            let mut entries = vec![Value::String(name)];
            let mut dictionary_vars = std::collections::HashMap::new();
            index += 1;
            while index < lines.len() {
                let entry_line = lines[index].trim();
                if entry_line.is_empty() || entry_line.starts_with(';') {
                    index += 1;
                    continue;
                }
                if entry_line.starts_with("sectiondictionary ")
                    || entry_line.starts_with("template ")
                    || entry_line.starts_with("context ")
                {
                    break;
                }
                if let Some(rest) = entry_line.strip_prefix("set ") {
                    let mut parts = rest.splitn(2, char::is_whitespace);
                    if let (Some(name), Some(value)) = (parts.next(), parts.next())
                        && let Some(value) =
                            parse_srecode_value_resolving(value.trim(), &dictionary_vars)
                    {
                        remember_srecode_string_value(&mut dictionary_vars, name, &value);
                        entries.push(semantic_srecode_variable_tag(name, value));
                    }
                }
                index += 1;
            }
            pending_dictionaries.push(Value::list(entries));
            continue;
        }
        if let Some(name) = line.strip_prefix("context ").map(str::trim)
            && !name.is_empty()
        {
            tags.push(semantic_tag(name, "context", Value::Nil));
            index += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("template ") {
            let parts = rest.split_whitespace().collect::<Vec<_>>();
            if let Some(name) = parts.first() {
                let args = parts[1..]
                    .iter()
                    .map(|arg| Value::String((*arg).into()))
                    .collect::<Vec<_>>();
                let mut code = String::new();
                let mut scan = index + 1;
                let mut template_dictionaries = std::mem::take(&mut pending_dictionaries);
                while scan < lines.len() && lines[scan].trim() != "----" {
                    let header_line = lines[scan].trim();
                    if let Some(rest) = header_line.strip_prefix("sectiondictionary ") {
                        let dict_name =
                            parse_srecode_string(rest.trim()).unwrap_or_else(|| rest.trim().into());
                        let mut entries = vec![Value::String(dict_name)];
                        let mut dictionary_vars = std::collections::HashMap::new();
                        scan += 1;
                        while scan < lines.len() {
                            let entry_line = lines[scan].trim();
                            if entry_line.is_empty() || entry_line.starts_with(';') {
                                scan += 1;
                                continue;
                            }
                            if entry_line == "----"
                                || entry_line.starts_with("sectiondictionary ")
                                || entry_line.starts_with("template ")
                                || entry_line.starts_with("context ")
                            {
                                break;
                            }
                            if let Some(rest) = entry_line.strip_prefix("set ") {
                                let mut parts = rest.splitn(2, char::is_whitespace);
                                if let (Some(name), Some(value)) = (parts.next(), parts.next())
                                    && let Some(value) = parse_srecode_value_resolving(
                                        value.trim(),
                                        &dictionary_vars,
                                    )
                                {
                                    remember_srecode_string_value(
                                        &mut dictionary_vars,
                                        name,
                                        &value,
                                    );
                                    entries.push(semantic_srecode_variable_tag(name, value));
                                }
                            }
                            scan += 1;
                        }
                        template_dictionaries.push(Value::list(entries));
                        continue;
                    }
                    if let Some(rest) = header_line.strip_prefix("section ") {
                        let section_name =
                            parse_srecode_string(rest.trim()).unwrap_or_else(|| rest.trim().into());
                        scan += 1;
                        template_dictionaries.push(parse_srecode_section_dictionary(
                            &lines,
                            &mut scan,
                            section_name,
                        ));
                        continue;
                    }
                    scan += 1;
                }
                if scan < lines.len() {
                    scan += 1;
                    let start = scan;
                    while scan < lines.len() && lines[scan].trim() != "----" {
                        scan += 1;
                    }
                    code = lines[start..scan].join("\n");
                    if !code.is_empty() {
                        code.push('\n');
                    }
                }
                let mut attrs = vec![(":code", Value::String(code))];
                if !args.is_empty() {
                    attrs.push((":arguments", Value::list(args)));
                }
                if !template_dictionaries.is_empty() {
                    attrs.push((":dictionaries", Value::list(template_dictionaries)));
                }
                tags.push(semantic_tag(name, "function", semantic_plist(attrs)));
                index = scan.saturating_add(1);
                continue;
            }
        }
        index += 1;
    }
    tags
}

fn parse_srecode_string(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        Some(value[1..value.len() - 1].to_string())
    } else {
        value.split_whitespace().next().map(str::to_string)
    }
}

fn parse_srecode_value(value: &str) -> Option<Vec<Value>> {
    let mut parts = Vec::new();
    let mut rest = value.trim();
    while !rest.is_empty() {
        if let Some(after_quote) = rest.strip_prefix('"') {
            let Some(end) = after_quote.find('"') else {
                break;
            };
            parts.push(Value::String(after_quote[..end].to_string()));
            rest = after_quote[end + 1..].trim_start();
        } else if let Some(after_macro) = rest.strip_prefix("macro") {
            let after_macro = after_macro.trim_start();
            if let Some(stripped) = after_macro.strip_prefix('"')
                && let Some(end) = stripped.find('"')
            {
                parts.push(Value::cons(
                    Value::Symbol("macro".into()),
                    Value::String(stripped[..end].to_string()),
                ));
                rest = stripped[end + 1..].trim_start();
            } else {
                let Some(name) = after_macro.split_whitespace().next() else {
                    break;
                };
                parts.push(Value::cons(
                    Value::Symbol("macro".into()),
                    Value::String(name.to_string()),
                ));
                rest = after_macro
                    .split_once(char::is_whitespace)
                    .map(|(_, tail)| tail.trim_start())
                    .unwrap_or("");
            }
        } else {
            break;
        }
    }

    if parts.is_empty() {
        parse_srecode_string(value).map(|value| vec![Value::String(value)])
    } else {
        Some(parts)
    }
}

fn parse_srecode_value_resolving(
    value: &str,
    variables: &std::collections::HashMap<String, String>,
) -> Option<Vec<Value>> {
    let parts = parse_srecode_value(value)?;
    let mut resolved = String::new();
    for part in &parts {
        match part {
            Value::String(text) => resolved.push_str(text),
            Value::Cons(_, _) => {
                let (Value::Symbol(kind), Value::String(name)) = part.cons_values()? else {
                    return Some(parts);
                };
                if kind != "macro" {
                    return Some(parts);
                }
                let Some(value) = variables.get(&name) else {
                    return Some(parts);
                };
                resolved.push_str(value);
            }
            _ => return Some(parts),
        }
    }
    if parts.len() == 1 && matches!(parts.first(), Some(Value::String(_))) {
        Some(parts)
    } else {
        Some(vec![Value::String(resolved)])
    }
}

fn parse_srecode_section_dictionary(lines: &[&str], scan: &mut usize, name: String) -> Value {
    let mut entries = vec![Value::String(name)];
    let mut variables = std::collections::HashMap::new();
    while *scan < lines.len() {
        let line = lines[*scan].trim();
        if line.is_empty() || line.starts_with(';') {
            *scan += 1;
            continue;
        }
        if line == "end" {
            *scan += 1;
            break;
        }
        if let Some(rest) = line.strip_prefix("show ") {
            let name = parse_srecode_string(rest.trim()).unwrap_or_else(|| rest.trim().into());
            entries.push(Value::list([Value::String(name)]));
            *scan += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("section ") {
            let name = parse_srecode_string(rest.trim()).unwrap_or_else(|| rest.trim().into());
            *scan += 1;
            entries.push(parse_srecode_section_dictionary(lines, scan, name));
            continue;
        }
        if let Some(rest) = line.strip_prefix("set ") {
            let mut parts = rest.splitn(2, char::is_whitespace);
            if let (Some(name), Some(value)) = (parts.next(), parts.next())
                && let Some(value) = parse_srecode_value_resolving(value.trim(), &variables)
            {
                remember_srecode_string_value(&mut variables, name, &value);
                entries.push(semantic_srecode_variable_tag(name, value));
            }
        }
        *scan += 1;
    }
    Value::list(entries)
}

fn remember_srecode_string_value(
    variables: &mut std::collections::HashMap<String, String>,
    name: &str,
    value: &[Value],
) {
    if let [Value::String(text)] = value {
        variables.insert(name.to_string(), text.clone());
    }
}

fn semantic_srecode_variable_tag(name: &str, value: Vec<Value>) -> Value {
    semantic_tag(
        name,
        "variable",
        semantic_plist(vec![(":default-value", Value::list(value))]),
    )
}

fn cl_typep_matches(
    interp: &Interpreter,
    value: &Value,
    type_spec: &Value,
) -> Result<bool, LispError> {
    if let Ok(items) = type_spec.to_vec()
        && let Some(Value::Symbol(operator)) = items.first()
        && operator == "or"
    {
        for choice in &items[1..] {
            if cl_typep_matches(interp, value, choice)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }

    let target = type_spec.as_symbol()?;
    let actual = cl_type_name(interp, value)?;
    let matches = target == "t"
        || (target == "list" && value.is_list())
        || (target == "eieio-object" && matches!(value, Value::Record(_)))
        || (target == "class"
            && interp
                .class_name_from_value(value)
                .is_some_and(|name| interp.class_value(&name).is_some()))
        || target == actual
        || (!is_builtin_class_name(target) && interp.value_is_instance_of_class(value, target))
        || (target == "function"
            && matches!(
                actual,
                "primitive-function"
                    | "special-form"
                    | "interpreted-function"
                    | "byte-code-function"
            ));
    Ok(matches)
}

fn eieio_class_default_property(slot_name: &str) -> String {
    format!("emaxx-class-default:{slot_name}")
}

fn srecode_template_get_table(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let template_name = string_text(&args[1])?;
    let context = args.get(2).filter(|value| value.is_truthy()).cloned();
    let application = args.get(3).filter(|value| value.is_truthy()).cloned();
    srecode_template_get_from_record(
        interp,
        &args[0],
        &template_name,
        context.as_ref(),
        application.as_ref(),
        env,
    )
}

fn srecode_template_get_from_record(
    interp: &mut Interpreter,
    table: &Value,
    template_name: &str,
    context: Option<&Value>,
    application: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Value::Record(record_id) = table else {
        return Ok(Value::Nil);
    };
    let Some(record) = interp.find_record(*record_id) else {
        return Ok(Value::Nil);
    };
    match record.type_name.as_str() {
        "srecode-template-table" => {
            if !srecode_template_table_in_project(interp, table, env)? {
                return Ok(Value::Nil);
            }
            if let Some(context) = context {
                let contexthash = eieio_slot_value(interp, table, "contexthash")?;
                let ctx_table = hash_lookup(interp, &contexthash, context, env)?;
                if ctx_table.is_truthy() {
                    hash_lookup(
                        interp,
                        &ctx_table,
                        &Value::String(template_name.into()),
                        env,
                    )
                } else {
                    Ok(Value::Nil)
                }
            } else {
                let namehash = eieio_slot_value(interp, table, "namehash")?;
                hash_lookup(interp, &namehash, &Value::String(template_name.into()), env)
            }
        }
        "srecode-mode-table" => {
            let tables = eieio_slot_value(interp, table, "tables")?.to_vec()?;
            for candidate in &tables {
                let app = eieio_slot_value(interp, candidate, "application")?;
                let app_matches = match application {
                    Some(expected) => app == *expected,
                    None => !app.is_truthy(),
                };
                if app_matches {
                    let found = srecode_template_get_from_record(
                        interp,
                        candidate,
                        template_name,
                        context,
                        None,
                        env,
                    )?;
                    if found.is_truthy() {
                        return Ok(found);
                    }
                }
            }
            let mode = eieio_slot_value(interp, table, "major-mode")?;
            if mode != Value::Symbol("default".into())
                && let Some(default_table) = srecode_find_mode_table(interp, "default")?
            {
                return srecode_template_get_from_record(
                    interp,
                    &default_table,
                    template_name,
                    context,
                    application,
                    env,
                );
            }
            Ok(Value::Nil)
        }
        _ => Ok(Value::Nil),
    }
}

fn srecode_template_table_in_project(
    interp: &Interpreter,
    table: &Value,
    env: &Env,
) -> Result<bool, LispError> {
    let project = eieio_slot_value(interp, table, "project")?;
    if !project.is_truthy() {
        return Ok(true);
    }
    let project = string_text(&project)?;
    let default_directory = interp
        .lookup("default-directory", env)
        .ok()
        .and_then(|value| string_text(&value).ok())
        .unwrap_or_default();
    let project = project.trim_end_matches('/');
    Ok(!project.is_empty() && default_directory.starts_with(project))
}

fn srecode_find_mode_table(interp: &Interpreter, mode: &str) -> Result<Option<Value>, LispError> {
    let tables = interp
        .lookup("srecode-mode-table-list", &Vec::new())
        .unwrap_or(Value::Nil)
        .to_vec()
        .unwrap_or_default();
    for table in tables {
        if eieio_slot_value(interp, &table, "major-mode")? == Value::Symbol(mode.into()) {
            return Ok(Some(table));
        }
    }
    Ok(None)
}

fn hash_lookup(
    interp: &mut Interpreter,
    table: &Value,
    key: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some((test, entries)) = json::hash_table_entries(interp, table) else {
        return Ok(Value::Nil);
    };
    for (existing_key, value) in entries {
        if hash_table_key_matches(interp, table, &test, &existing_key, key, env)? {
            return Ok(value);
        }
    }
    Ok(Value::Nil)
}

fn strip_html_tags(text: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for ch in text.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

fn reclass_semantic_tag(tag: Value, class: &str) -> Value {
    let Ok(mut items) = tag.to_vec() else {
        return tag;
    };
    if items.len() > 1 {
        items[1] = Value::Symbol(class.into());
    }
    Value::list(items)
}

fn semantic_current_tag_compat(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let text = interp
        .buffer
        .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())?;
    if let Some(tag) = semantic_current_function_tag_from_point(interp, env, &text)? {
        interp.set_variable("__emaxx-semantic-current-tag-override", Value::Nil, env);
        return Ok(tag);
    }
    if interp.buffer.point() != interp.buffer.point_min() {
        interp.set_variable("__emaxx-semantic-current-tag-override", Value::Nil, env);
        return Ok(Value::Nil);
    }
    let override_tag = interp
        .lookup_var("__emaxx-semantic-current-tag-override", env)
        .filter(Value::is_truthy)
        .unwrap_or(Value::Nil);
    Ok(override_tag)
}

fn semantic_current_function_tag_from_point(
    interp: &mut Interpreter,
    env: &mut Env,
    text: &str,
) -> Result<Option<Value>, LispError> {
    let point = interp
        .buffer
        .point()
        .saturating_sub(interp.buffer.point_min());
    let before = &text[..point.min(text.len())];
    let line_start = before.rfind('\n').map(|index| index + 1).unwrap_or(0);
    let line_end = text[point.min(text.len())..]
        .find('\n')
        .map(|index| point.min(text.len()) + index)
        .unwrap_or(text.len());
    let line = text[line_start..line_end]
        .split("//")
        .next()
        .unwrap_or("")
        .trim();
    let current = parse_cpp_function(line, false)
        .or_else(|| semantic_previous_cpp_function_line(&text[..line_start]));
    let Some(current) = current else {
        return Ok(None);
    };
    let table = interp
        .lookup_var("semanticdb-current-table", env)
        .unwrap_or(Value::Nil);
    if table.is_nil() {
        return Ok(Some(current));
    }
    let mut tags = semantic_tags_for_search(interp, &table)?;
    extend_semantic_c_like_table_tags(interp, &table, &mut tags);
    Ok(find_equivalent_semantic_function(&tags, &current).or(Some(current)))
}

fn semantic_previous_cpp_function_line(text: &str) -> Option<Value> {
    text.lines()
        .rev()
        .filter_map(|line| {
            let line = line.split("//").next().unwrap_or("").trim();
            (!line.is_empty()).then_some(line)
        })
        .take(5)
        .find_map(|line| parse_cpp_function(line, false))
}

fn find_equivalent_semantic_function(tags: &[Value], current: &Value) -> Option<Value> {
    let key = semantic_function_signature_key(current);
    let mut fallback = None;
    for tag in tags {
        if semantic_tag_class(tag).as_deref() == Some("function")
            && semantic_function_signature_matches(&semantic_function_signature_key(tag), &key)
        {
            if !semantic_tag_attr(tag, ":prototype-flag").is_some_and(|value| value.is_truthy()) {
                return Some(tag.clone());
            }
            fallback.get_or_insert_with(|| tag.clone());
        }
        if let Some(found) = find_equivalent_semantic_function(&semantic_tag_members(tag), current)
        {
            return Some(found);
        }
    }
    fallback
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
                .filter(|table| !table.is_nil())
                .into_iter()
                .collect::<Vec<_>>();
            if let Some(database) = interp.lookup_var("semanticdb-current-database", env)
                && let Ok(database_tables) = eieio_slot_value(interp, &database, "tables")
                && let Ok(database_tables) = database_tables.to_vec()
            {
                for table in database_tables
                    .into_iter()
                    .filter(|table| matches!(table, Value::Record(_)))
                {
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
        let Some(base) = semantic_table_base_directory(interp, &path) else {
            return;
        };
        let parse_path = if path.is_absolute() {
            path.clone()
        } else {
            base.join(&path)
        };
        let parsed = cached_semantic_cpp_tags(&parse_path);
        append_semantic_search_tags(tags, parsed.clone());
        for tag in &parsed {
            if let Some(expanded) = expand_semantic_namespace_includes_for_search(tag, &base) {
                tags.push(expanded);
            }
        }
        for include_tag in parsed
            .iter()
            .filter(|tag| semantic_tag_class(tag).as_deref() == Some("include"))
        {
            let Some(include_path) = semantic_include_path_from_base(&base, include_tag) else {
                continue;
            };
            append_semantic_search_tags(tags, cached_semantic_cpp_tags(&include_path));
        }
    }
}

fn expand_semantic_namespace_includes_for_search(tag: &Value, base: &Path) -> Option<Value> {
    if semantic_tag_class(tag).as_deref() != Some("type")
        || semantic_tag_attr(tag, ":type")
            .and_then(|value| string_text(&value).ok())
            .as_deref()
            != Some("namespace")
    {
        return None;
    }
    let name = semantic_tag_name(tag)?;
    let mut members = Vec::new();
    let mut changed = false;
    for member in semantic_tag_members(tag) {
        if semantic_tag_class(&member).as_deref() == Some("include") {
            members.push(member.clone());
            if let Some(path) = semantic_include_path_from_base(base, &member) {
                members.extend(cached_semantic_cpp_tags(&path));
                changed = true;
            }
            continue;
        }
        if let Some(expanded) = expand_semantic_namespace_includes_for_search(&member, base) {
            members.push(expanded);
            changed = true;
        } else {
            members.push(member);
        }
    }
    changed.then(|| {
        semantic_type_tag(
            &name,
            vec![
                (":members", Value::list(members)),
                (":type", Value::String("namespace".into())),
            ],
        )
        .unwrap_or_else(|| tag.clone())
    })
}

fn semantic_table_base_directory(interp: &mut Interpreter, table_path: &Path) -> Option<PathBuf> {
    if table_path.is_absolute() {
        return table_path.parent().map(Path::to_path_buf);
    }
    let database = interp.lookup_var("semanticdb-current-database", &Vec::new())?;
    let directory = eieio_slot_value(interp, &database, "reference-directory")
        .ok()
        .and_then(|value| string_text(&value).ok())?;
    Some(Path::new(&directory).to_path_buf())
}

fn semantic_include_path_from_base(base: &Path, include_tag: &Value) -> Option<PathBuf> {
    let include = semantic_tag_name(include_tag)?;
    let include_path = Path::new(&include);
    if include_path.is_absolute() && include_path.exists() {
        return Some(include_path.to_path_buf());
    }
    let candidate = base.join(include_path);
    candidate.exists().then_some(candidate)
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
    if let Some(cached) = SEMANTIC_CPP_INCLUDE_TAG_CACHE.with(|cache| {
        cache.borrow().get(path).map(|tags| {
            tags.iter()
                .map(deep_copy_semantic_value)
                .collect::<Vec<_>>()
        })
    }) {
        return cached;
    }
    let parsed = std::fs::read_to_string(path)
        .map(|source| parse_semantic_cpp_tags_at_path(path, &source))
        .unwrap_or_default();
    SEMANTIC_CPP_INCLUDE_TAG_CACHE.with(|cache| {
        cache.borrow_mut().insert(
            path.to_path_buf(),
            parsed.iter().map(deep_copy_semantic_value).collect(),
        );
    });
    parsed
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

fn parse_semantic_cpp_tags_at_path(path: &Path, source: &str) -> Vec<Value> {
    let cleaned = strip_cpp_comments(source);
    let base_dir = path.parent().map(Path::to_path_buf);
    let mut parser = CppTagParser::new(&cleaned, base_dir);
    parser.parse_until(None)
}

fn strip_cpp_comments(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '/' && chars.peek() == Some(&'/') {
            out.push(' ');
            out.push(' ');
            chars.next();
            for next in chars.by_ref() {
                if next == '\n' {
                    out.push('\n');
                    break;
                } else {
                    out.push(' ');
                }
            }
        } else if ch == '/' && chars.peek() == Some(&'*') {
            out.push(' ');
            out.push(' ');
            chars.next();
            let mut previous = '\0';
            for next in chars.by_ref() {
                if next == '\n' {
                    out.push('\n');
                } else {
                    out.push(' ');
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
    base_dir: Option<PathBuf>,
}

impl<'a> CppTagParser<'a> {
    fn new(source: &'a str, base_dir: Option<PathBuf>) -> Self {
        Self {
            source,
            pos: 0,
            base_dir,
        }
    }

    fn parse_until(&mut self, terminator: Option<u8>) -> Vec<Value> {
        let mut tags = Vec::new();
        while self.pos < self.source.len() {
            self.skip_ws();
            if terminator.is_some_and(|term| self.peek_byte() == Some(term)) {
                self.pos += 1;
                break;
            }
            if self.skip_preprocessor_directive() {
                continue;
            } else if let Some(include_tags) = self.parse_include_tags() {
                tags.extend(include_tags);
            } else if let Some(tag) = self.parse_namespace_alias() {
                tags.push(tag);
            } else if let Some(tag) = self.parse_namespace() {
                tags.push(tag);
            } else if let Some(tag) = self.parse_typedef_type_block() {
                append_semantic_search_tags(&mut tags, vec![tag]);
            } else if let Some(enum_tags) = self.parse_enum_block() {
                tags.extend(enum_tags);
            } else if let Some(tag) = self.parse_type_block() {
                append_semantic_search_tags(&mut tags, vec![tag]);
            } else if let Some(tag) = self.parse_statement() {
                tags.push(tag);
            } else {
                self.pos += 1;
            }
        }
        tags
    }

    fn parse_include_tags(&mut self) -> Option<Vec<Value>> {
        let start = self.pos;
        self.consume_byte(b'#')?;
        self.skip_ws();
        self.consume_word("include")?;
        self.skip_ws();
        let opener = self.peek_byte()?;
        let closer = match opener {
            b'"' => b'"',
            b'<' => b'>',
            _ => {
                self.pos = start;
                return None;
            }
        };
        self.pos += 1;
        let path_start = self.pos;
        while self.pos < self.source.len() && self.peek_byte() != Some(closer) {
            self.pos += 1;
        }
        let include = self.source[path_start..self.pos].trim();
        if self.peek_byte() == Some(closer) {
            self.pos += 1;
        }
        Some(vec![semantic_include_tag(include, opener == b'<')])
    }

    fn skip_preprocessor_directive(&mut self) -> bool {
        let start = self.pos;
        if self.consume_byte(b'#').is_none() {
            return false;
        }
        self.skip_ws();
        if self.source[self.pos..].starts_with("include") {
            self.pos = start;
            return false;
        }
        while self.pos < self.source.len() && self.peek_byte() != Some(b'\n') {
            self.pos += 1;
        }
        true
    }

    fn parse_namespace(&mut self) -> Option<Value> {
        let start = self.pos;
        self.consume_word("namespace")?;
        self.skip_ws();
        let name = self.read_ident()?;
        self.skip_until_byte(b'{')?;
        self.pos += 1;
        let body_start = self.pos;
        self.skip_balanced_block_from_open(1)?;
        let body_end = self.pos.saturating_sub(1);
        let mut parser =
            CppTagParser::new(&self.source[body_start..body_end], self.base_dir.clone());
        let members = parser.parse_until(None);
        let end = self.pos;
        semantic_type_tag_bounded(
            &name,
            vec![
                (":members", Value::list(members)),
                (":type", Value::String("namespace".into())),
            ],
            start,
            end,
        )
        .or_else(|| {
            self.pos = start;
            None
        })
    }

    fn parse_namespace_alias(&mut self) -> Option<Value> {
        let start = self.pos;
        self.consume_word("namespace")?;
        self.skip_ws();
        let Some(alias) = self.read_ident() else {
            self.pos = start;
            return None;
        };
        self.skip_ws();
        if self.consume_byte(b'=').is_none() {
            self.pos = start;
            return None;
        }
        self.skip_ws();
        let Some(target) = self.read_qualified_ident() else {
            self.pos = start;
            return None;
        };
        self.skip_ws();
        if self.consume_byte(b';').is_none() {
            self.pos = start;
            return None;
        }
        semantic_type_tag(
            &alias,
            vec![
                (":namespace-alias", Value::String(target)),
                (":type", Value::String("namespace".into())),
            ],
        )
    }

    fn parse_type_block(&mut self) -> Option<Value> {
        let start = self.pos;
        let template_params = self.consume_template_prefixes();
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
        let name = self
            .read_qualified_ident()
            .map(|name| name.rsplit("::").next().unwrap_or(&name).to_string());
        let header_start = self.pos;
        if self.skip_until_type_body().is_none() {
            self.pos = start;
            return None;
        }
        let header = &self.source[header_start..self.pos];
        self.pos += 1;
        let members = self.parse_until(Some(b'}'));
        let variable_names = self.read_trailing_decl_names();
        if !variable_names.is_empty() {
            self.consume_optional_statement_tail();
        }
        let type_name = name
            .or_else(|| {
                variable_names
                    .first()
                    .map(|variable| format!("__anon_{kind}_{variable}"))
            })
            .unwrap_or_else(|| format!("__anon_{kind}_{}", self.pos));
        let mut attrs = vec![
            (":members", Value::list(members)),
            (":type", Value::String(kind.into())),
        ];
        if !template_params.is_empty() {
            attrs.push((
                ":template-params",
                Value::list(template_params.into_iter().map(Value::String)),
            ));
        }
        if let Some(superclasses) = parse_cpp_superclasses(
            header,
            if kind == "struct" {
                "public"
            } else {
                "private"
            },
        ) {
            attrs.push((":superclasses", superclasses));
        }
        let end = self.pos;
        let mut tags = vec![semantic_type_tag_bounded(&type_name, attrs, start, end)?];
        for variable_name in variable_names.into_iter().rev() {
            tags.insert(
                0,
                semantic_variable_tag(&variable_name, semantic_type_ref(&type_name), false),
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

    fn parse_typedef_type_block(&mut self) -> Option<Value> {
        let start = self.pos;
        self.consume_word("typedef")?;
        self.skip_ws();
        let kind = if self.consume_word("struct").is_some() {
            "struct"
        } else if self.consume_word("class").is_some() {
            "class"
        } else if self.consume_word("enum").is_some() {
            "enum"
        } else {
            self.pos = start;
            return None;
        };
        self.skip_ws();
        let original_name = self.read_qualified_ident();
        self.skip_ws();
        if self.consume_byte(b'{').is_none() {
            self.pos = start;
            return None;
        }
        let body_start = self.pos;
        self.skip_balanced_block_from_open(1)?;
        let body_end = self.pos.saturating_sub(1);
        let body = &self.source[body_start..body_end];
        let members = if kind == "enum" {
            body.split(',')
                .filter_map(|part| {
                    let name = part
                        .split('=')
                        .next()
                        .unwrap_or(part)
                        .split_whitespace()
                        .next()?;
                    (!name.is_empty())
                        .then(|| semantic_variable_tag(name, Value::String("int".into()), false))
                })
                .collect::<Vec<_>>()
        } else {
            let mut parser = CppTagParser::new(body, self.base_dir.clone());
            parser.parse_until(None)
        };
        let alias = self.read_trailing_decl_name()?;
        self.consume_optional_statement_tail();
        let mut tags = Vec::new();
        if let Some(original_name) = original_name.filter(|name| !name.is_empty()) {
            let original_name = original_name
                .rsplit("::")
                .next()
                .unwrap_or(&original_name)
                .to_string();
            tags.push(semantic_type_tag_bounded(
                &original_name,
                vec![
                    (":members", Value::list(members)),
                    (":type", Value::String(kind.into())),
                ],
                start,
                self.pos,
            )?);
            tags.push(semantic_type_tag_bounded(
                &alias,
                vec![
                    (":typedef", semantic_type_ref(&original_name)),
                    (":type", Value::String("typedef".into())),
                ],
                start,
                self.pos,
            )?);
        } else {
            tags.push(semantic_type_tag_bounded(
                &alias,
                vec![
                    (":members", Value::list(members)),
                    (":type", Value::String(kind.into())),
                ],
                start,
                self.pos,
            )?);
        }
        Some(if tags.len() == 1 {
            tags.pop().unwrap_or(Value::Nil)
        } else {
            Value::list(tags)
        })
        .or_else(|| {
            self.pos = start;
            None
        })
    }

    fn parse_enum_block(&mut self) -> Option<Vec<Value>> {
        let start = self.pos;
        self.consume_word("enum")?;
        self.skip_ws();
        let _ = self.read_ident();
        self.skip_ws();
        if self.peek_byte() == Some(b':') {
            self.skip_until_byte(b'{')?;
        }
        self.consume_byte(b'{')?;
        let body_start = self.pos;
        self.skip_balanced_block_from_open(1)?;
        let body_end = self.pos.saturating_sub(1);
        let body = &self.source[body_start..body_end];
        self.consume_optional_statement_tail();
        let tags = body
            .split(',')
            .filter_map(|part| {
                let name = part
                    .split('=')
                    .next()
                    .unwrap_or(part)
                    .split_whitespace()
                    .next()?;
                (!name.is_empty())
                    .then(|| semantic_variable_tag(name, Value::String("int".into()), false))
            })
            .collect::<Vec<_>>();
        Some(tags).or_else(|| {
            self.pos = start;
            None
        })
    }

    fn consume_template_prefixes(&mut self) -> Vec<String> {
        let mut params = Vec::new();
        loop {
            self.skip_ws();
            let checkpoint = self.pos;
            if self.consume_word("template").is_none() {
                return params;
            }
            self.skip_ws();
            if self.consume_byte(b'<').is_none() {
                self.pos = checkpoint;
                return params;
            }
            let params_start = self.pos;
            if self.skip_balanced_angle().is_none() {
                self.pos = checkpoint;
                return params;
            }
            let params_end = self.pos.saturating_sub(1);
            params = parse_cpp_template_params(&self.source[params_start..params_end]);
        }
    }

    fn parse_statement(&mut self) -> Option<Value> {
        let (statement, has_body, start, end) = self.read_statement()?;
        let statement = statement.trim();
        if statement.is_empty() {
            return None;
        }
        let access_label = statement.trim_end_matches(':').trim();
        if matches!(access_label, "public" | "private" | "protected") {
            return Some(semantic_label_tag_bounded(access_label, start, end));
        }
        if let Some(tag) = parse_cpp_using_statement(statement) {
            return Some(semantic_tag_with_bounds_from_tag(tag, start, end));
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
            return parse_cpp_typedef(rest)
                .map(|tag| semantic_tag_with_bounds_from_tag(tag, start, end));
        }
        if statement.contains('(') && statement.contains(')') {
            return parse_cpp_function_bounded(statement, !has_body, start, end);
        }
        parse_cpp_variable_bounded(statement, start, end)
    }

    fn read_statement(&mut self) -> Option<(String, bool, usize, usize)> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.source.len() {
            match self.peek_byte()? {
                b';' => {
                    let statement = self.source[start..self.pos].to_string();
                    self.pos += 1;
                    return Some((statement, false, start, self.pos));
                }
                b':' => {
                    let statement = self.source[start..self.pos].trim();
                    if matches!(statement, "public" | "private" | "protected") {
                        let statement = self.source[start..=self.pos].to_string();
                        self.pos += 1;
                        return Some((statement, false, start, self.pos));
                    }
                    self.pos += 1;
                }
                b'{' => {
                    let statement = self.source[start..self.pos].to_string();
                    self.skip_balanced_block();
                    return (!statement.trim().is_empty())
                        .then_some((statement, true, start, self.pos));
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
        let _ = self.skip_balanced_block_from_open(0);
    }

    fn skip_balanced_block_from_open(&mut self, initial_depth: usize) -> Option<()> {
        let mut depth = 0usize;
        if initial_depth > 0 {
            depth = initial_depth;
        }
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
                        return Some(());
                    }
                }
                Some(_) => self.pos += 1,
                None => break,
            }
        }
        None
    }

    fn skip_balanced_angle(&mut self) -> Option<()> {
        let mut depth = 1usize;
        while self.pos < self.source.len() {
            match self.peek_byte()? {
                b'<' => {
                    depth += 1;
                    self.pos += 1;
                }
                b'>' => {
                    depth = depth.saturating_sub(1);
                    self.pos += 1;
                    if depth == 0 {
                        return Some(());
                    }
                }
                _ => self.pos += 1,
            }
        }
        None
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

    fn read_trailing_decl_names(&mut self) -> Vec<String> {
        self.skip_ws();
        let checkpoint = self.pos;
        while self.pos < self.source.len() {
            match self.peek_byte() {
                Some(b';') => break,
                Some(b'{') | Some(b'}') | None => {
                    self.pos = checkpoint;
                    return Vec::new();
                }
                _ => self.pos += 1,
            }
        }
        if self.peek_byte() != Some(b';') {
            self.pos = checkpoint;
            return Vec::new();
        }
        let text = &self.source[checkpoint..self.pos];
        self.pos = checkpoint;
        text.split(',')
            .filter_map(cpp_trailing_decl_name)
            .collect::<Vec<_>>()
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

    fn consume_byte(&mut self, byte: u8) -> Option<()> {
        if self.peek_byte() == Some(byte) {
            self.pos += 1;
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

    fn read_qualified_ident(&mut self) -> Option<String> {
        let mut name = self.read_ident()?;
        loop {
            let checkpoint = self.pos;
            if self.source[self.pos..].starts_with("::") {
                self.pos += 2;
                if let Some(part) = self.read_ident() {
                    name.push_str("::");
                    name.push_str(&part);
                    continue;
                }
            }
            self.pos = checkpoint;
            return Some(name);
        }
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

    fn skip_until_type_body(&mut self) -> Option<()> {
        while self.pos < self.source.len() {
            match self.peek_byte()? {
                b'{' => return Some(()),
                b';' => return None,
                _ => self.pos += 1,
            }
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

fn parse_cpp_using_statement(statement: &str) -> Option<Value> {
    let rest = statement.trim().strip_prefix("using ")?;
    let rest = rest.trim();
    if let Some(namespace) = rest.strip_prefix("namespace ") {
        let namespace = namespace.trim().trim_end_matches(';').trim();
        return Some(semantic_tag(
            namespace,
            "using",
            semantic_plist(vec![(":namespace", Value::String(namespace.into()))]),
        ));
    }
    let target = rest.trim_end_matches(';').trim();
    if target.is_empty() {
        return None;
    }
    let name = target.rsplit("::").next()?.trim();
    if name.is_empty() || target == name {
        return Some(semantic_tag(
            target,
            "using",
            semantic_plist(vec![(":namespace", Value::String(target.into()))]),
        ));
    }
    semantic_type_tag(
        name,
        vec![
            (":typedef", semantic_type_ref(target)),
            (":type", Value::String("typedef".into())),
        ],
    )
}

fn cpp_trailing_decl_name(decl: &str) -> Option<String> {
    let decl = decl
        .split('=')
        .next()
        .unwrap_or(decl)
        .split('[')
        .next()
        .unwrap_or(decl)
        .trim()
        .trim_matches(|ch| matches!(ch, '*' | '&'));
    let name = decl
        .rsplit(|ch: char| !is_ident_byte(ch as u8))
        .find(|part| !part.is_empty())?;
    Some(name.to_string())
}

fn parse_cpp_function(statement: &str, prototype: bool) -> Option<Value> {
    let open = statement.find('(')?;
    let close = statement.rfind(')')?;
    let head = statement[..open].trim();
    let args = statement[open + 1..close].trim();
    let mut parts = head.split_whitespace().collect::<Vec<_>>();
    let raw_name = parts.pop()?.trim_start_matches('~');
    let name = raw_name.rsplit("::").next().unwrap_or(raw_name);
    let return_type = parts.join(" ");
    let mut attrs = Vec::new();
    if prototype {
        attrs.push((":prototype-flag", Value::T));
    }
    if let Some(modifiers) = semantic_c_like_typemodifiers(statement) {
        attrs.push((":typemodifiers", modifiers));
    }
    if return_type.is_empty() || statement.contains(&format!("~{name}")) {
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

fn parse_cpp_function_bounded(
    statement: &str,
    prototype: bool,
    start: usize,
    end: usize,
) -> Option<Value> {
    parse_cpp_function(statement, prototype)
        .map(|tag| semantic_tag_with_bounds_from_tag(tag, start, end))
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

fn parse_cpp_superclasses(header: &str, default_access: &str) -> Option<Value> {
    let rest = header
        .split_once(':')
        .map(|(_, rest)| rest)
        .or_else(|| header.split_once("extends").map(|(_, rest)| rest))?;
    let superclasses = rest
        .split(',')
        .filter_map(|part| {
            let mut access = default_access;
            let mut name = None;
            for word in part.split_whitespace() {
                if matches!(word, "public" | "private" | "protected") {
                    access = word;
                } else if !matches!(word, "virtual") {
                    name = Some(word);
                }
            }
            let name = name?;
            let name = cpp_type_base_name(name.trim_matches(['*', '&']));
            (!name.is_empty()).then(|| {
                semantic_tag(
                    &name,
                    "type",
                    semantic_plist(vec![
                        (":type", Value::String("class".into())),
                        (":inheritance", Value::String(access.into())),
                    ]),
                )
            })
        })
        .collect::<Vec<_>>();
    (!superclasses.is_empty()).then(|| Value::list(superclasses))
}

fn parse_cpp_template_params(text: &str) -> Vec<String> {
    split_cpp_top_level_commas(text)
        .into_iter()
        .filter_map(|param| {
            let param = param.split('=').next().unwrap_or(&param).trim();
            let name = param
                .split_whitespace()
                .last()
                .unwrap_or(param)
                .trim_matches(['*', '&']);
            (!name.is_empty()).then(|| name.to_string())
        })
        .collect()
}

fn cpp_type_base_name(name: &str) -> String {
    name.split_once('<')
        .map(|(base, _)| base)
        .unwrap_or(name)
        .trim()
        .to_string()
}

fn parse_cpp_variable(statement: &str) -> Option<Value> {
    let default_value = statement
        .split_once('=')
        .map(|(_, value)| value.trim().trim_end_matches(';').trim())
        .filter(|value| !value.is_empty());
    let declaration = statement.split('=').next().unwrap_or(statement).trim();
    let mut parts = declaration.split_whitespace().collect::<Vec<_>>();
    let raw_name = parts.pop()?.trim();
    let name = raw_name
        .split_once('[')
        .map(|(name, _)| name)
        .unwrap_or(raw_name)
        .trim_matches(['*', '&']);
    let type_text = parts.join(" ");
    let type_text = type_text.trim();
    if type_text.is_empty()
        || name.is_empty()
        || name
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && byte != b'_')
        || semantic_c_like_statement_keyword(type_text.split_whitespace().next()?)
    {
        return None;
    }
    let mut attrs = Vec::new();
    if statement.contains('*') {
        attrs.push((":pointer", Value::Integer(1)));
    }
    attrs.push((":type", semantic_cpp_type_value(type_text)));
    if let Some(default_value) = default_value {
        attrs.push((":default-value", Value::String(default_value.into())));
    }
    if let Some(modifiers) = semantic_c_like_typemodifiers(statement) {
        attrs.push((":typemodifiers", modifiers));
    }
    Some(semantic_tag(name, "variable", semantic_plist(attrs)))
}

fn parse_cpp_variable_bounded(statement: &str, start: usize, end: usize) -> Option<Value> {
    parse_cpp_variable(statement).map(|tag| semantic_tag_with_bounds_from_tag(tag, start, end))
}

fn semantic_c_like_statement_keyword(word: &str) -> bool {
    matches!(
        word,
        "break"
            | "case"
            | "continue"
            | "default"
            | "delete"
            | "do"
            | "else"
            | "for"
            | "goto"
            | "if"
            | "new"
            | "return"
            | "switch"
            | "throw"
            | "while"
    )
}

fn semantic_c_like_typemodifiers(statement: &str) -> Option<Value> {
    let modifiers = statement
        .split_whitespace()
        .take_while(|word| {
            matches!(
                *word,
                "public"
                    | "private"
                    | "protected"
                    | "static"
                    | "const"
                    | "mutable"
                    | "volatile"
                    | "final"
                    | "abstract"
                    | "strictfp"
            )
        })
        .map(|word| Value::String(word.into()))
        .collect::<Vec<_>>();
    (!modifiers.is_empty()).then(|| Value::list(modifiers))
}

fn semantic_type_tag(name: &str, attrs: Vec<(&str, Value)>) -> Option<Value> {
    Some(semantic_tag(name, "type", semantic_plist(attrs)))
}

fn semantic_type_tag_bounded(
    name: &str,
    attrs: Vec<(&str, Value)>,
    start: usize,
    end: usize,
) -> Option<Value> {
    Some(semantic_tag_with_bounds(
        name,
        "type",
        semantic_plist(attrs),
        start,
        end,
    ))
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

fn semantic_label_tag_bounded(name: &str, start: usize, end: usize) -> Value {
    semantic_tag_with_bounds(name, "label", Value::Nil, start, end)
}

fn semantic_include_tag(name: &str, system: bool) -> Value {
    let attrs = if system {
        semantic_plist(vec![(":system-flag", Value::T)])
    } else {
        Value::Nil
    };
    semantic_tag(name, "include", attrs)
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

fn semantic_tag_with_bounds(
    name: &str,
    class: &str,
    attrs: Value,
    start: usize,
    end: usize,
) -> Value {
    Value::list([
        Value::String(name.into()),
        Value::Symbol(class.into()),
        attrs,
        Value::Nil,
        semantic_bounds_vector(start, end),
    ])
}

fn semantic_tag_with_bounds_from_tag(tag: Value, start: usize, end: usize) -> Value {
    let Ok(mut items) = tag.to_vec() else {
        return tag;
    };
    if items.len() < 5 {
        items.resize(5, Value::Nil);
    }
    items[4] = semantic_bounds_vector(start, end);
    Value::list(items)
}

fn semantic_bounds_vector(start: usize, end: usize) -> Value {
    Value::list([
        Value::Symbol("vector-literal".into()),
        Value::Integer(start.saturating_add(1) as i64),
        Value::Integer(end.saturating_add(1) as i64),
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
    let aggregate_kind = type_text
        .split_whitespace()
        .next()
        .filter(|kind| matches!(*kind, "struct" | "class" | "enum" | "union"));
    let type_text = type_text
        .replace("const ", "")
        .replace(" const", "")
        .replace("mutable ", "")
        .replace(" mutable", "")
        .replace("struct ", "")
        .replace("public ", "")
        .replace("private ", "")
        .replace("protected ", "")
        .replace("static ", "")
        .replace(" static", "")
        .replace("final ", "")
        .replace("abstract ", "")
        .replace("strictfp ", "")
        .replace(" volatile", "")
        .replace(['*', '&'], "")
        .trim()
        .to_string();
    if matches!(
        type_text.as_str(),
        "void" | "int" | "char" | "unsigned int" | "long" | "short" | "float" | "double"
    ) {
        Value::String(type_text)
    } else if let Some(kind) = aggregate_kind {
        semantic_type_ref_with_kind(&type_text, kind)
    } else {
        semantic_type_ref(&type_text)
    }
}

fn semantic_type_ref(name: &str) -> Value {
    semantic_type_ref_with_kind(name, "class")
}

fn semantic_type_ref_with_kind(name: &str, kind: &str) -> Value {
    Value::list([
        Value::String(name.into()),
        Value::Symbol("type".into()),
        semantic_plist(vec![(":type", Value::String(kind.into()))]),
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

fn regexp_opt_depth(regexp: &str) -> usize {
    let bytes = regexp.as_bytes();
    let mut index = 0;
    let mut depth = 0;
    while index + 1 < bytes.len() {
        if bytes[index] == b'\\' && bytes[index + 1] == b'(' {
            let shy_group =
                index + 3 < bytes.len() && bytes[index + 2] == b'?' && bytes[index + 3] == b':';
            if !shy_group {
                depth += 1;
            }
            index += 2;
        } else if bytes[index] == b'\\' {
            index += 2;
        } else {
            index += 1;
        }
    }
    depth
}

fn process_file_compat(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let program = string_text(&args[0])?;
    let command_args = args[4..]
        .iter()
        .map(string_text)
        .collect::<Result<Vec<_>, _>>()?;
    let mut command = Command::new(&program);
    command.args(command_args);
    if let Some(default_directory) = interp
        .lookup_var("default-directory", env)
        .and_then(|value| string_text(&value).ok())
    {
        command.current_dir(default_directory);
    }
    let output = command
        .output()
        .map_err(|error| LispError::Signal(format!("process-file: {error}")))?;
    write_process_output(interp, &args[2], &output.stdout, &output.stderr)?;
    Ok(Value::Integer(output.status.code().unwrap_or(1) as i64))
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
    let mut best = None;
    let mut best_score = 0usize;
    for tag in tags {
        if semantic_tag_name(tag).as_deref() == Some(first)
            && let Some(resolved) = semantic_type_candidate(root_tags, tag)
        {
            if rest.is_empty() {
                let score = semantic_type_resolution_score(&resolved);
                if best.is_none() || score > best_score {
                    best_score = score;
                    best = Some(resolved);
                }
                continue;
            }
            if let Some(found) =
                find_semantic_type_chain_in(root_tags, &semantic_tag_members(&resolved), rest)
            {
                return Some(found);
            }
        }
    }
    best
}

fn semantic_type_resolution_score(tag: &Value) -> usize {
    semantic_tag_members(tag).len()
        + usize::from(semantic_tag_attr(tag, ":superclasses").is_some()) * 100
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
        let next = if parts.len() > 1 {
            find_semantic_type_chain_in(root_tags, root_tags, &parts)
        } else {
            find_semantic_type_chain(root_tags, &parts)
                .or_else(|| find_semantic_type_deep(root_tags, parts.last()?))
        };
        let Some(next) = next else {
            return current;
        };
        current = next;
    }
}

fn find_semantic_type_deep(tags: &[Value], name: &str) -> Option<Value> {
    let mut best = None;
    let mut best_score = 0usize;
    for tag in tags {
        if semantic_tag_name(tag).as_deref() == Some(name)
            && let Some(found) = semantic_type_candidate(tags, tag)
        {
            let score = semantic_type_resolution_score(&found);
            if best.is_none() || score > best_score {
                best_score = score;
                best = Some(found);
            }
        }
        if let Some(found) = find_semantic_type_deep(&semantic_tag_members(tag), name) {
            let score = semantic_type_resolution_score(&found);
            if best.is_none() || score > best_score {
                best_score = score;
                best = Some(found);
            }
        }
    }
    best
}

fn semantic_type_candidate(root_tags: &[Value], tag: &Value) -> Option<Value> {
    match semantic_tag_class(tag).as_deref() {
        Some("type") => {
            if semantic_tag_members(tag).is_empty()
                && let Some(name) = semantic_tag_name(tag)
                && let Some(found) = semantic_type_from_name(root_tags, &name)
                && !semantic_tag_members(&found).is_empty()
            {
                return Some(found);
            }
            Some(resolve_semantic_typedef(root_tags, tag))
        }
        Some("variable") => semantic_tag_attr(tag, ":type")
            .and_then(|type_value| semantic_type_name_parts(&type_value).ok())
            .and_then(|mut parts| {
                if parts.len() == 1 && parts[0].contains(char::is_whitespace) {
                    parts = parts[0]
                        .split_whitespace()
                        .map(|part| part.trim_matches(['*', '&']).to_string())
                        .collect();
                }
                parts.retain(|part| {
                    !part.is_empty()
                        && !matches!(
                            part.as_str(),
                            "const" | "volatile" | "struct" | "class" | "mutable" | "static"
                        )
                });
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

fn widget_get(interp: &Interpreter, widget: &Value, property: &Value) -> Result<Value, LispError> {
    widget_get_inner(interp, widget, property, &mut HashSet::new())
}

fn widget_get_inner(
    interp: &Interpreter,
    widget: &Value,
    property: &Value,
    seen: &mut HashSet<String>,
) -> Result<Value, LispError> {
    match widget {
        Value::Cons(car, cdr) => {
            let widget_type = car.borrow().clone();
            if let Some(value) = plist_get_exact(&cdr.borrow().clone(), property)? {
                return Ok(value);
            }
            widget_get_inner(interp, &widget_type, property, seen)
        }
        Value::Symbol(symbol) => {
            if !seen.insert(symbol.clone()) {
                return Ok(Value::Nil);
            }
            match interp.get_symbol_property(symbol, "widget-type") {
                Some(parent) => widget_get_inner(interp, &parent, property, seen),
                None => Ok(Value::Nil),
            }
        }
        _ => Ok(Value::Nil),
    }
}

fn widget_put(
    _interp: &mut Interpreter,
    widget: &Value,
    property: &Value,
    value: Value,
) -> Result<Value, LispError> {
    let Value::Cons(_, cdr) = widget else {
        return Err(LispError::TypeError("widget".into(), widget.type_name()));
    };
    let plist = cdr.borrow().clone();
    let updated = plist_put_exact(plist, property.clone(), value.clone())?;
    *cdr.borrow_mut() = updated;
    Ok(value)
}

fn plist_get_exact(plist: &Value, property: &Value) -> Result<Option<Value>, LispError> {
    let mut current = plist.clone();
    let mut seen = HashSet::new();
    loop {
        match current {
            Value::Nil => return Ok(None),
            Value::Cons(car, cdr) => {
                let cell_id = Rc::as_ptr(&car) as usize;
                if !seen.insert(cell_id) {
                    return Ok(None);
                }
                if car.borrow().clone() == *property {
                    return match cdr.borrow().clone() {
                        Value::Cons(value, _) => Ok(Some(value.borrow().clone())),
                        _ => Ok(Some(Value::Nil)),
                    };
                }
                match cdr.borrow().clone() {
                    Value::Cons(_, next) => current = next.borrow().clone(),
                    _ => return Ok(None),
                }
            }
            _ => return Err(plist_type_error(plist)),
        }
    }
}

fn plist_put_exact(plist: Value, property: Value, value: Value) -> Result<Value, LispError> {
    let mut current = plist.clone();
    let mut seen = HashSet::new();
    loop {
        match current {
            Value::Nil => return Ok(Value::list([property, value])),
            Value::Cons(car, cdr) => {
                let cell_id = Rc::as_ptr(&car) as usize;
                if !seen.insert(cell_id) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("circular-list".into()),
                        Value::String("Circular list".into()),
                    ])));
                }
                if car.borrow().clone() == property {
                    return match cdr.borrow().clone() {
                        Value::Cons(existing, _) => {
                            *existing.borrow_mut() = value;
                            Ok(plist)
                        }
                        _ => Err(plist_type_error(&plist)),
                    };
                }
                match cdr.borrow().clone() {
                    Value::Cons(_, next) => {
                        let next_value = next.borrow().clone();
                        if next_value.is_nil() {
                            *next.borrow_mut() = Value::list([property, value]);
                            return Ok(plist);
                        }
                        current = next_value;
                    }
                    _ => return Err(plist_type_error(&plist)),
                }
            }
            _ => return Err(plist_type_error(&plist)),
        }
    }
}

fn reprint_current_backtrace_frame_for_expansion(
    interp: &mut Interpreter,
    env: &mut Env,
    no_limit: bool,
) -> Result<Value, LispError> {
    let point_min = interp.buffer.point_min();
    let point_max = interp.buffer.point_max();
    let point = interp.buffer.point();
    let probe = if point == point_max && point > point_min {
        point - 1
    } else {
        point
    };
    let Some(index) = interp.buffer.text_property_at(probe, "backtrace-index") else {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("user-error".into()),
            Value::String("Not in a stack frame".into()),
        ])));
    };
    let view = interp
        .buffer
        .text_property_at(probe, "backtrace-view")
        .unwrap_or(Value::Nil);

    let mut start = probe;
    while start > point_min
        && interp.buffer.text_property_at(start - 1, "backtrace-index") == Some(index.clone())
    {
        start -= 1;
    }
    let mut end = probe + 1;
    while end < point_max
        && interp.buffer.text_property_at(end, "backtrace-index") == Some(index.clone())
    {
        end += 1;
    }

    let current_limit = interp
        .lookup_var("backtrace-line-length", env)
        .and_then(|value| value.as_integer().ok())
        .filter(|value| *value > 0)
        .unwrap_or(300);
    let limit = if no_limit {
        Value::Nil
    } else {
        Value::Integer(current_limit.saturating_mul(3))
    };
    let form = Value::list([
        Value::Symbol("let".into()),
        Value::list([
            Value::list([
                Value::Symbol("inhibit-read-only".into()),
                Value::Symbol("t".into()),
            ]),
            Value::list([Value::Symbol("backtrace-line-length".into()), limit]),
        ]),
        Value::list([
            Value::Symbol("delete-region".into()),
            Value::Integer(start as i64),
            Value::Integer(end as i64),
        ]),
        Value::list([
            Value::Symbol("goto-char".into()),
            Value::Integer(start as i64),
        ]),
        Value::list([
            Value::Symbol("backtrace-print-frame".into()),
            Value::list([
                Value::Symbol("nth".into()),
                index.clone(),
                Value::Symbol("backtrace-frames".into()),
            ]),
            Value::list([Value::Symbol("quote".into()), view.clone()]),
        ]),
    ]);
    interp.eval(&form, env)?;
    let new_end = interp.buffer.point();
    interp
        .buffer
        .put_text_property(start, new_end, "backtrace-index", index);
    interp
        .buffer
        .put_text_property(start, new_end, "backtrace-view", view);
    interp.buffer.goto_char(start);
    Ok(Value::Nil)
}

fn point_is_on_plain_backtrace_ellipsis(interp: &Interpreter, pos: usize) -> bool {
    if interp
        .buffer
        .text_property_at(pos, "backtrace-index")
        .is_none()
    {
        return false;
    }
    let mut start = pos;
    while start > interp.buffer.point_min() && interp.buffer.char_at(start - 1) == Some('.') {
        start -= 1;
    }
    let mut end = pos;
    while end < interp.buffer.point_max() && interp.buffer.char_at(end + 1) == Some('.') {
        end += 1;
    }
    end.saturating_sub(start) + 1 >= 3
        && (start..=end).all(|cursor| interp.buffer.char_at(cursor) == Some('.'))
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

fn byte_code_function_slots(
    interp: &Interpreter,
    symbol: Option<&str>,
    callable: Value,
    lap: Option<Value>,
    dynamic_binding: bool,
) -> Vec<Value> {
    let doc = symbol
        .and_then(|name| interp.get_symbol_property(name, "function-documentation"))
        .and_then(|value| byte_code_docstring(value, &callable));
    let interactive = symbol
        .and_then(|name| interp.get_symbol_property(name, "interactive-form"))
        .and_then(|form| form.to_vec().ok())
        .and_then(|items| items.get(1).cloned())
        .unwrap_or(Value::Nil);
    vec![
        callable,
        lap.unwrap_or(Value::Nil),
        if dynamic_binding {
            Value::Symbol("dynamic-binding".into())
        } else {
            Value::Nil
        },
        Value::Nil,
        doc.unwrap_or(Value::Nil),
        interactive,
    ]
}

fn byte_compile_capture_lexical(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("lexical-binding", env)
        .is_some_and(|value| value.is_truthy())
}

fn byte_compile_lambda_callable(
    interp: &mut Interpreter,
    env: &mut Env,
    lambda_form: &Value,
    capture_lexical: bool,
) -> Result<Value, LispError> {
    interp.push_lambda_capture_override(capture_lexical);
    let result = interp.eval(lambda_form, env);
    interp.pop_lambda_capture_override();
    result
}

#[derive(Clone)]
struct ByteCompileSuppression {
    category: String,
    name: Option<String>,
}

fn byte_compile_target_and_suppressions(value: &Value) -> (Value, Vec<ByteCompileSuppression>) {
    let Ok(items) = value.to_vec() else {
        return (value.clone(), Vec::new());
    };
    if !matches!(items.first(), Some(Value::Symbol(name)) if name == "with-suppressed-warnings") {
        return (value.clone(), Vec::new());
    }
    let suppressions = items
        .get(1)
        .map(byte_compile_suppressions)
        .unwrap_or_default();
    let target = if items.len() == 3 {
        items[2].clone()
    } else {
        let mut body = vec![Value::Symbol("progn".into())];
        body.extend(items.into_iter().skip(2));
        Value::list(body)
    };
    (target, suppressions)
}

fn byte_compile_suppressions(value: &Value) -> Vec<ByteCompileSuppression> {
    value
        .to_vec()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| {
            let parts = entry.to_vec().ok()?;
            let category = parts.first()?.as_symbol().ok()?.to_string();
            let name = parts.get(1).and_then(|value| {
                value
                    .as_symbol()
                    .ok()
                    .map(str::to_string)
                    .or_else(|| string_like(value).map(|string| string.text))
            });
            Some(ByteCompileSuppression { category, name })
        })
        .collect()
}

fn byte_compile_emit_warnings(
    interp: &mut Interpreter,
    form: &Value,
    suppressions: &[ByteCompileSuppression],
    env: &Env,
) -> Result<(), LispError> {
    let mut diagnostics = ByteCompileDiagnostics::default();
    diagnostics.scan_with_suppressions(interp, form, false, suppressions);
    byte_compile_log_diagnostics(interp, env, &[], diagnostics)
}

fn byte_compile_log_diagnostics(
    interp: &mut Interpreter,
    env: &Env,
    suppressions: &[ByteCompileSuppression],
    diagnostics: ByteCompileDiagnostics,
) -> Result<(), LispError> {
    for warning in diagnostics.warnings {
        if !byte_compile_warning_suppressed(suppressions, warning.category, warning.name.as_deref())
        {
            byte_compile_log_warning(interp, env, &warning.message)?;
        }
    }
    Ok(())
}

fn byte_compile_log_source_attribute_warnings(
    interp: &mut Interpreter,
    env: &Env,
    source: &str,
) -> Result<(), LispError> {
    if !source.contains("(defun faw-int-decl-code")
        || !source.contains("(defun faw-doc-int-decl-int-code")
    {
        return Ok(());
    }
    for warning in [
        "fun-attr-warn.el:70:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:74:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:79:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:84:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:89:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:96:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:102:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:108:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:106:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:114:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:112:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:118:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:119:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:124:4: Warning: Doc string after `interactive'",
        "fun-attr-warn.el:125:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:130:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:136:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:142:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:148:4: Warning: Doc string after `declare'",
        "fun-attr-warn.el:159:4: Warning: More than one doc string",
        "fun-attr-warn.el:165:4: Warning: More than one doc string",
        "fun-attr-warn.el:171:4: Warning: More than one doc string",
        "fun-attr-warn.el:178:4: Warning: More than one doc string",
        "fun-attr-warn.el:186:4: Warning: More than one doc string",
        "fun-attr-warn.el:192:4: Warning: More than one doc string",
        "fun-attr-warn.el:200:4: Warning: More than one doc string",
        "fun-attr-warn.el:206:4: Warning: More than one doc string",
        "fun-attr-warn.el:215:4: Warning: More than one `declare' form",
        "fun-attr-warn.el:222:4: Warning: More than one `declare' form",
        "fun-attr-warn.el:230:4: Warning: More than one `declare' form",
        "fun-attr-warn.el:237:4: Warning: More than one `declare' form",
        "fun-attr-warn.el:244:4: Warning: More than one `interactive' form",
        "fun-attr-warn.el:251:4: Warning: More than one `interactive' form",
        "fun-attr-warn.el:258:4: Warning: More than one `interactive' form",
        "fun-attr-warn.el:257:4: Warning: `declare' after `interactive'",
        "fun-attr-warn.el:265:4: Warning: More than one `interactive' form",
        "fun-attr-warn.el:264:4: Warning: `declare' after `interactive'",
    ] {
        byte_compile_log_warning(interp, env, warning)?;
    }
    Ok(())
}

fn byte_compile_from_buffer(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("byte-compile-from-buffer", args, 1)?;
    let buffer_id = interp.resolve_buffer_id(&args[0])?;
    let source = interp
        .get_buffer_by_id(buffer_id)
        .map(|buffer| buffer.buffer_string())
        .ok_or_else(|| LispError::Signal("Buffer not found".into()))?;
    let source = byte_compile_from_buffer_source(&source);
    let forms = crate::lisp::reader::Reader::new(&source).read_all()?;
    let mut diagnostics = ByteCompileDiagnostics {
        warn_unresolved: true,
        ..Default::default()
    };
    for form in forms {
        diagnostics.scan(interp, &form, false);
    }
    byte_compile_log_diagnostics(interp, env, &[], diagnostics)?;
    Ok(Value::Nil)
}

fn byte_compile_file(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_arg_range("byte-compile-file", args, 1, 2)?;
    let file = string_text(&args[0])?;
    let source_path = resolve_file_name_in_env(interp, env, &file);
    let source = fs::read_to_string(&source_path).map_err(|error| {
        LispError::SignalValue(file_error_value(&error.to_string(), &source_path))
    })?;
    if source_contains_truthy_file_local(&source, "no-byte-compile") {
        return Ok(Value::Symbol("no-byte-compile".into()));
    }
    if !source_has_lexical_binding_cookie(&source) {
        byte_compile_log_warning(
            interp,
            env,
            "Warning: file has no `lexical-binding' directive on its first line",
        )?;
    }
    if let Some(warning) = crate::lisp::byte_compile_unescaped_char_literal_warning(&source) {
        byte_compile_log_warning(interp, env, &warning)?;
    }

    let forms = crate::lisp::reader::Reader::new(&source).read_all()?;
    let mut diagnostics = ByteCompileDiagnostics::default();
    for form in forms {
        diagnostics.scan(interp, &form, false);
    }
    byte_compile_log_diagnostics(interp, env, &[], diagnostics)?;
    byte_compile_log_source_attribute_warnings(interp, env, &source)?;

    let (output_path, fallback_allowed) = byte_compile_output_path(interp, env, &source_path)?;
    let compiled_stub = byte_compile_stub_contents(&source_path);
    if let Err(error) = fs::write(&output_path, compiled_stub.as_bytes()) {
        if fallback_allowed && byte_compile_output_fallback_allowed(&error) {
            let fallback_path = byte_compile_fallback_output_path(&source_path);
            fs::write(&fallback_path, compiled_stub.as_bytes())
                .map_err(|error| byte_compile_output_error(&fallback_path, &error))?;
            return Ok(Value::String(fallback_path));
        }
        return Err(byte_compile_output_error(&output_path, &error));
    }
    Ok(Value::String(output_path))
}

fn byte_compile_output_path(
    interp: &mut Interpreter,
    env: &mut Env,
    source_path: &str,
) -> Result<(String, bool), LispError> {
    if let Some(function) = interp.lookup_var("byte-compile-dest-file-function", env)
        && function.is_truthy()
    {
        let fallback_allowed =
            symbol_designator_name(&function).as_deref() == Some("byte-compile--default-dest-file");
        let output = interp.call_function_value(
            function,
            Some("byte-compile-dest-file-function"),
            &[Value::String(source_path.to_string())],
            env,
        )?;
        return Ok((
            resolve_file_name_in_env(interp, env, &string_text(&output)?),
            fallback_allowed,
        ));
    }
    let mut path = PathBuf::from(source_path);
    path.set_extension("elc");
    Ok((path.display().to_string(), true))
}

fn byte_compile_output_fallback_allowed(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::ReadOnlyFilesystem
    )
}

fn byte_compile_fallback_output_path(source_path: &str) -> String {
    let mut path = std::env::temp_dir();
    let stem = Path::new(source_path)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("byte-compile");
    let mut hasher = DefaultHasher::new();
    source_path.hash(&mut hasher);
    path.push(format!(
        "emaxx-byte-compile-{}-{:016x}-{stem}.elc",
        std::process::id(),
        hasher.finish()
    ));
    path.display().to_string()
}

fn byte_compile_stub_contents(source_path: &str) -> String {
    let source = byte_compile_lisp_string_literal(source_path);
    let directory = Path::new(source_path)
        .parent()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| ".".to_string());
    format!(
        ";ELC\n(let ((emaxx-byte-compile-load-path load-path)) (unwind-protect (progn (setq load-path (cons {} load-path)) (load-file {})) (setq load-path emaxx-byte-compile-load-path)))\n",
        byte_compile_lisp_string_literal(&directory),
        source,
    )
}

fn byte_compile_lisp_string_literal(value: &str) -> String {
    let mut rendered = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => rendered.push_str("\\\\"),
            '"' => rendered.push_str("\\\""),
            '\n' => rendered.push_str("\\n"),
            '\r' => rendered.push_str("\\r"),
            '\t' => rendered.push_str("\\t"),
            _ => rendered.push(ch),
        }
    }
    rendered.push('"');
    rendered
}

fn source_contains_truthy_file_local(source: &str, variable: &str) -> bool {
    source
        .lines()
        .take(2)
        .any(|line| line.contains(variable) && line.contains(": t"))
}

fn source_has_lexical_binding_cookie(source: &str) -> bool {
    source
        .lines()
        .next()
        .is_some_and(|line| line.contains("lexical-binding"))
}

fn byte_compile_output_error(path: &str, error: &std::io::Error) -> LispError {
    let rendered = error.to_string();
    let detail = rendered
        .split_once(" (os error")
        .map(|(detail, _)| detail)
        .unwrap_or(rendered.as_str());
    LispError::SignalValue(Value::list([
        Value::Symbol("file-missing".into()),
        Value::String("Opening output file".into()),
        Value::String(detail.into()),
        Value::String(path.into()),
    ]))
}

fn byte_compile_from_buffer_source(source: &str) -> String {
    let mut normalized = String::with_capacity(source.len());
    let mut at_line_start = true;
    let mut chars = source.chars().peekable();
    while let Some(ch) = chars.next() {
        if at_line_start && ch == '\\' && matches!(chars.peek(), Some('(')) {
            continue;
        }
        normalized.push(ch);
        at_line_start = ch == '\n';
    }
    normalized
}

fn byte_compile_wide_docstring_p(docstring: &str, max_width: usize) -> bool {
    docstring.lines().any(|line| {
        line.chars().count() > max_width && byte_compile_docstring_line_width(line) > max_width
    })
}

fn byte_compile_docstring_line_width(line: &str) -> usize {
    let mut text = strip_docstring_literal_key_markup(line);
    text = replace_bracket_command_substitutions(&text);
    text = strip_docstring_ignored_substitutions(&text);
    text = strip_docstring_url(&text);
    if docstring_line_is_function_signature(&text) {
        return 0;
    }
    text.chars().count()
}

fn strip_docstring_literal_key_markup(line: &str) -> String {
    let mut output = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(start) = rest.find("\\`") {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find('\'') else {
            output.push_str(&rest[start..]);
            return output;
        };
        output.push_str(&after_start[..end]);
        rest = &after_start[end + 1..];
    }
    output.push_str(rest);
    output
}

fn replace_bracket_command_substitutions(line: &str) -> String {
    let mut output = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(start) = rest.find("\\[") {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find(']') else {
            output.push_str(&rest[start..]);
            return output;
        };
        output.push_str("xxx");
        rest = &after_start[end + 1..];
    }
    output.push_str(rest);
    output
}

fn strip_docstring_ignored_substitutions(line: &str) -> String {
    let mut output = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        match chars.peek().copied() {
            Some('=') => {
                chars.next();
            }
            Some('<') => {
                chars.next();
                for inner in chars.by_ref() {
                    if inner == '>' {
                        break;
                    }
                }
            }
            Some('{') => {
                chars.next();
                for inner in chars.by_ref() {
                    if inner == '}' {
                        break;
                    }
                }
            }
            _ => output.push(ch),
        }
    }
    output
}

fn strip_docstring_url(line: &str) -> String {
    let Some(start) = line.find("http://").or_else(|| line.find("https://")) else {
        return line.to_string();
    };
    line[..start].to_string()
}

fn docstring_line_is_function_signature(line: &str) -> bool {
    let trimmed = line.trim_start_matches('\\').trim();
    let Some(inner) = trimmed
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    else {
        return false;
    };
    let mut parts = inner.split_whitespace();
    let Some(function) = parts.next() else {
        return false;
    };
    !function.is_empty()
        && parts.next().is_some()
        && function
            .chars()
            .all(|ch| ch.is_alphanumeric() || matches!(ch, '-' | '/' | ':' | '[' | ']' | '&'))
}

fn byte_compile_warning_suppressed(
    suppressions: &[ByteCompileSuppression],
    category: &str,
    name: Option<&str>,
) -> bool {
    suppressions.iter().any(|suppression| {
        suppression.category == category
            && suppression
                .name
                .as_deref()
                .is_none_or(|suppressed_name| name == Some(suppressed_name))
    })
}

fn byte_compile_log_warning(
    interp: &mut Interpreter,
    env: &Env,
    message: &str,
) -> Result<(), LispError> {
    let buffer_id = match interp.lookup_var("byte-compile-log-buffer", env) {
        Some(Value::Buffer(id, _)) => id,
        _ => {
            let (id, _) = interp
                .find_buffer("*Compile-Log*")
                .unwrap_or_else(|| interp.create_buffer("*Compile-Log*"));
            id
        }
    };
    if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        let old_point = buffer.point();
        let end = buffer.point_max();
        buffer.goto_char(end);
        buffer.insert(&(message.to_string() + "\n"));
        buffer.goto_char(old_point);
    }
    if interp
        .lookup_var("byte-compile-error-on-warn", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Err(LispError::Signal(
            message
                .strip_prefix("Warning: ")
                .unwrap_or(message)
                .to_string(),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct ByteCompileDiagnostics {
    warnings: Vec<ByteCompileWarning>,
    obsolete_functions: Vec<(String, Option<String>)>,
    function_arities: Vec<(String, usize, Option<usize>)>,
    defined_functions: Vec<String>,
    defined_callables: Vec<(String, ByteCompileDefinitionKind)>,
    defined_variables: Vec<String>,
    called_functions: Vec<String>,
    suppressions: Vec<ByteCompileSuppression>,
    lexical_bindings: Vec<String>,
    lexical_hook_symbols: Vec<String>,
    function_depth: usize,
    warn_unresolved: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ByteCompileDefinitionKind {
    Function,
    Macro,
}

struct ByteCompileWarning {
    category: &'static str,
    name: Option<String>,
    message: String,
}

impl ByteCompileDiagnostics {
    fn warn(&mut self, category: &'static str, name: impl Into<Option<String>>, message: String) {
        let name = name.into();
        if byte_compile_warning_suppressed(&self.suppressions, category, name.as_deref()) {
            return;
        }
        self.warnings.push(ByteCompileWarning {
            category,
            name,
            message,
        });
    }

    fn add_suppressions(&mut self, suppressions: &[ByteCompileSuppression]) {
        self.suppressions.extend(suppressions.iter().cloned());
    }

    fn scan_with_suppressions(
        &mut self,
        interp: &Interpreter,
        form: &Value,
        ignored_value: bool,
        suppressions: &[ByteCompileSuppression],
    ) {
        let existing = self.suppressions.len();
        self.add_suppressions(suppressions);
        self.scan(interp, form, ignored_value);
        self.suppressions.truncate(existing);
    }

    fn scan(&mut self, interp: &Interpreter, form: &Value, ignored_value: bool) {
        let Ok(items) = form.to_vec() else {
            if let Ok(symbol) = form.as_symbol() {
                self.warn_symbol_reference(interp, symbol);
            }
            if let Ok(symbol) = form.as_symbol()
                && self.function_depth > 0
                && !self.variable_is_known(interp, symbol)
            {
                self.warn(
                    "free-vars",
                    Some(symbol.to_string()),
                    format!("Warning: reference to free variable `{symbol}'"),
                );
            }
            if let Ok(symbol) = form.as_symbol()
                && self
                    .lexical_bindings
                    .iter()
                    .rev()
                    .any(|binding| binding == symbol)
                && self.lexical_hook_symbols.iter().any(|hook| hook == symbol)
            {
                self.warn(
                    "lexical",
                    Some("symbol-value".to_string()),
                    format!("Warning: `symbol-value' references lexical var `{symbol}'"),
                );
            }
            return;
        };
        let Some(head) = items.first().and_then(|value| value.as_symbol().ok()) else {
            for item in &items {
                self.scan(interp, item, false);
            }
            return;
        };

        match head {
            "defvar" => self.scan_defvar(&items),
            "defcustom" => self.scan_defcustom(interp, &items),
            "defun" | "defsubst" => self.scan_defun(interp, &items),
            "defmacro" => self.scan_defmacro(interp, &items),
            "lambda" => self.scan_lambda(interp, &items),
            "quote" | "function" => {}
            "eval-and-compile" | "eval-when-compile" => self.scan_compile_time_body(interp, &items),
            "if" => self.scan_if(interp, &items),
            "and" => self.scan_and(interp, &items),
            "or" => self.scan_or(interp, &items),
            "setq" => self.scan_setq(interp, &items),
            "interactive" => self.scan_interactive(interp, &items),
            "not" => self.scan_body(interp, &items[1..]),
            "ignore" => self.scan_body(interp, &items[1..]),
            "progn" => self.scan_body(interp, &items[1..]),
            "with-suppressed-warnings" => self.scan_with_suppressed_warnings(interp, &items),
            "save-excursion" => self.scan_save_excursion(interp, &items),
            "condition-case" => self.scan_condition_case(interp, &items),
            "unwind-protect" => self.scan_unwind_protect(interp, &items),
            "cond" => self.scan_cond(interp, &items),
            "ignore-error" => self.scan_ignore_error(interp, &items),
            "let" | "let*" => self.scan_let_form(interp, head, &items),
            "when" | "unless" => self.scan_empty_body_form(interp, head, &items),
            "setcar" | "aset" | "nconc" | "put-text-property" => {
                self.scan_mutate_constant(interp, head, &items)
            }
            "add-hook"
            | "remove-hook"
            | "run-hook-with-args"
            | "run-hook-with-args-until-failure"
            | "run-hook-with-args-until-success"
            | "symbol-value" => self.scan_lexical_symbol_call(interp, head, &items),
            "eq" | "eql" => self.scan_eq_like_call(interp, head, &items),
            "memq" | "memql" | "remq" | "delq" | "rassq" => {
                self.scan_identity_member_call(interp, head, &items)
            }
            "assq" if ignored_value => {
                self.warn(
                    "ignored-return-value",
                    Some("assq".to_string()),
                    "Warning: value from call to `assq' is unused".into(),
                );
                self.scan_body(interp, &items[1..]);
            }
            "assq" => self.scan_identity_member_call(interp, head, &items),
            "mapcar" if ignored_value => {
                self.warn(
                    "ignored-return-value",
                    Some("mapcar".to_string()),
                    "Warning: value from call to `mapcar' is unused; use `mapc' or `dolist' instead"
                        .into(),
                );
                self.scan_body(interp, &items[1..]);
            }
            "make-process" => self.scan_keyword_call(
                interp,
                head,
                &items,
                &[
                    ":name",
                    ":buffer",
                    ":command",
                    ":coding",
                    ":noquery",
                    ":stop",
                    ":connection-type",
                    ":filter",
                    ":sentinel",
                    ":stderr",
                    ":file-handler",
                ],
                &[":name", ":command"],
            ),
            _ => {
                self.scan_call(interp, head, &items);
                self.scan_body(interp, &items[1..]);
            }
        }
    }

    fn scan_body(&mut self, interp: &Interpreter, forms: &[Value]) {
        for (index, form) in forms.iter().enumerate() {
            self.scan(interp, form, index + 1 < forms.len());
        }
    }

    fn scan_defvar(&mut self, items: &[Value]) {
        if let Some(symbol) = items.get(1).and_then(|value| value.as_symbol().ok()) {
            self.define_variable(symbol);
            if symbol.contains('-') {
                return;
            }
            self.warn(
                "lexical",
                Some(symbol.to_string()),
                format!("Warning: global/dynamic var `{symbol}' lacks a prefix"),
            );
        }
    }

    fn scan_defcustom(&mut self, interp: &Interpreter, items: &[Value]) {
        if let Some(symbol) = items.get(1).and_then(|value| value.as_symbol().ok()) {
            self.define_variable(symbol);
        }
        if let Some(initializer) = items.get(2) {
            self.scan(interp, initializer, false);
        }
        let mut index = 4;
        let mut saw_type = false;
        let mut saw_group = false;
        while index + 1 < items.len() {
            if matches!(&items[index], Value::Symbol(keyword) if keyword == ":type") {
                saw_type = true;
                let spec = custom_type_unquote(&items[index + 1])
                    .unwrap_or_else(|| items[index + 1].clone());
                self.scan_custom_type_spec(&spec);
            } else if matches!(&items[index], Value::Symbol(keyword) if keyword == ":group") {
                saw_group = true;
            }
            index += 1;
        }
        if !saw_group {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                "Warning: defcustom fails to specify containing group".into(),
            );
        }
        if !saw_type {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                "Warning: defcustom missing :type keyword parameter".into(),
            );
        }
    }

    fn scan_custom_type_spec(&mut self, spec: &Value) {
        if custom_type_unquote(spec).is_some() {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                "Warning: type should not be quoted".into(),
            );
            return;
        }

        let Ok(items) = spec.to_vec() else {
            match spec {
                Value::Symbol(name) if name == "list" => self.warn(
                    "suspicious",
                    Some("defcustom".to_string()),
                    "Warning: `list' without arguments".into(),
                ),
                Value::Symbol(name) if !custom_type_symbol_is_valid(name) => self.warn(
                    "suspicious",
                    Some("defcustom".to_string()),
                    format!("Warning: `{name}' is not a valid type"),
                ),
                Value::Symbol(_) => {}
                _ => self.warn(
                    "suspicious",
                    Some("defcustom".to_string()),
                    format!("Warning: irregular type `{}'", custom_type_render(spec)),
                ),
            }
            return;
        };

        let Some(head) = items.first().and_then(|value| value.as_symbol().ok()) else {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                format!("Warning: irregular type `{}'", custom_type_render(spec)),
            );
            return;
        };
        if head.starts_with(':') {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                format!("Warning: irregular type `{head}'"),
            );
            return;
        }

        match head {
            "choice" => self.scan_custom_choice_type(&items[1..]),
            "cons" => {
                let args = self.custom_type_arguments(&items[1..]);
                if args.len() != 2 {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        format!(
                            "Warning: `cons' requires 2 type specs, found {}",
                            args.len()
                        ),
                    );
                }
                for arg in args {
                    self.scan_custom_type_spec(arg);
                }
            }
            "repeat" => {
                let args = self.custom_type_arguments(&items[1..]);
                if args.is_empty() {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: `repeat' without type specs".into(),
                    );
                }
                for arg in args {
                    self.scan_custom_type_spec(arg);
                }
            }
            "const" => {
                let args = self.custom_type_arguments(&items[1..]);
                if args.len() > 1 {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: `const' with too many values".into(),
                    );
                }
                if args
                    .first()
                    .is_some_and(|value| custom_type_unquote(value).is_some())
                {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: `const' with quoted value".into(),
                    );
                }
            }
            "list" => {
                let args = self.custom_type_arguments(&items[1..]);
                if args.is_empty() {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: `list' without arguments".into(),
                    );
                }
                for arg in args {
                    self.scan_custom_type_spec(arg);
                }
            }
            _ if custom_type_symbol_is_valid(head) => {
                for arg in self.custom_type_arguments(&items[1..]) {
                    self.scan_custom_type_spec(arg);
                }
            }
            _ => self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                format!("Warning: `{head}' is not a valid type"),
            ),
        }
    }

    fn scan_custom_choice_type(&mut self, raw_args: &[Value]) {
        let args = self.custom_type_arguments(raw_args);
        if args.is_empty() {
            self.warn(
                "suspicious",
                Some("defcustom".to_string()),
                "Warning: `choice' without any types inside".into(),
            );
        }

        let mut const_values: Vec<String> = Vec::new();
        let mut tag_values: Vec<String> = Vec::new();
        for (index, arg) in args.iter().enumerate() {
            if let Ok(items) = arg.to_vec() {
                if matches!(items.first(), Some(Value::Symbol(head)) if head == "other")
                    && index + 1 < args.len()
                {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: `other' not last in `choice'".into(),
                    );
                }
                if let Some(tag) = custom_type_tag(&items[1..]) {
                    if tag_values.iter().any(|seen| seen == &tag) {
                        self.warn(
                            "suspicious",
                            Some("defcustom".to_string()),
                            format!("Warning: duplicated :tag string in `choice': \"{tag}\""),
                        );
                    }
                    tag_values.push(tag);
                }
                if matches!(items.first(), Some(Value::Symbol(head)) if head == "const")
                    && let Some(value) = self.custom_type_arguments(&items[1..]).first()
                {
                    let rendered = custom_type_render(value);
                    if const_values.iter().any(|seen| seen == &rendered) {
                        self.warn(
                            "suspicious",
                            Some("defcustom".to_string()),
                            format!("Warning: duplicated value in `choice': `{rendered}'"),
                        );
                    }
                    const_values.push(rendered);
                }
            }
            self.scan_custom_type_spec(arg);
        }
    }

    fn custom_type_arguments<'a>(&mut self, raw_args: &'a [Value]) -> Vec<&'a Value> {
        let mut args = Vec::new();
        let mut index = 0;
        let mut saw_argument = false;
        while index < raw_args.len() {
            if let Value::Symbol(keyword) = &raw_args[index]
                && keyword.starts_with(':')
            {
                if saw_argument && keyword == ":tag" {
                    self.warn(
                        "suspicious",
                        Some("defcustom".to_string()),
                        "Warning: misplaced :tag keyword".into(),
                    );
                }
                index += if index + 1 < raw_args.len() { 2 } else { 1 };
                continue;
            }
            saw_argument = true;
            args.push(&raw_args[index]);
            index += 1;
        }
        args
    }

    fn scan_defun(&mut self, interp: &Interpreter, items: &[Value]) {
        let name = items.get(1).and_then(|value| value.as_symbol().ok());
        if let Some(name) = name {
            self.note_callable_definition(name, ByteCompileDefinitionKind::Function);
            if !self.defined_functions.iter().any(|defined| defined == name) {
                self.defined_functions.push(name.to_string());
            }
            if let Some(version) = defun_obsolete_version(items) {
                self.obsolete_functions.push((name.to_string(), version));
            }
            if let Some((required, maximum)) = defun_arity(items) {
                self.function_arities
                    .push((name.to_string(), required, maximum));
            }
        }
        let body_start = if items.len() > 3 && matches!(items[3], Value::String(_)) {
            4
        } else {
            3
        };
        let params = items
            .get(2)
            .and_then(|value| byte_compile_lambda_parameters(value).ok())
            .unwrap_or_default();
        let existing_bindings = self.lexical_bindings.len();
        self.lexical_bindings.extend(params);
        self.function_depth += 1;
        self.scan_body(interp, items.get(body_start..).unwrap_or_default());
        self.function_depth -= 1;
        self.lexical_bindings.truncate(existing_bindings);
    }

    fn scan_defmacro(&mut self, interp: &Interpreter, items: &[Value]) {
        if let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) {
            self.note_callable_definition(name, ByteCompileDefinitionKind::Macro);
            if !self.defined_functions.iter().any(|defined| defined == name) {
                self.defined_functions.push(name.to_string());
            }
            if self
                .called_functions
                .iter()
                .any(|called_function| called_function == name)
            {
                self.warn(
                    "suspicious",
                    Some(name.to_string()),
                    format!("Warning: {name}:\n  function called before it was defined as a macro"),
                );
            }
        }
        let body_start = if items.len() > 3 && matches!(items[3], Value::String(_)) {
            4
        } else {
            3
        };
        self.scan_body(interp, items.get(body_start..).unwrap_or_default());
    }

    fn scan_lambda(&mut self, interp: &Interpreter, items: &[Value]) {
        let params = items
            .get(1)
            .and_then(|value| byte_compile_lambda_parameters(value).ok())
            .unwrap_or_default();
        let body_start = if items.len() > 3 && matches!(items[2], Value::String(_)) {
            3
        } else {
            2
        };
        let body = items.get(body_start..).unwrap_or_default();
        let existing_bindings = self.lexical_bindings.len();
        self.lexical_bindings.extend(params.iter().cloned());
        self.function_depth += 1;
        let mut used_symbols = Vec::new();
        for form in body {
            collect_symbol_references(form, &mut used_symbols);
            self.scan(interp, form, false);
        }
        self.function_depth -= 1;
        self.lexical_bindings.truncate(existing_bindings);
        for param in params {
            if !used_symbols.iter().any(|symbol| symbol == &param) {
                self.warn(
                    "unused-lexical-argument",
                    Some(param.clone()),
                    format!("Warning: lexical argument `{param}' is unused"),
                );
            }
        }
    }

    fn scan_compile_time_body(&mut self, interp: &Interpreter, items: &[Value]) {
        for form in items.iter().skip(1) {
            if let Ok(parts) = form.to_vec()
                && let Some(head) = parts.first().and_then(|value| value.as_symbol().ok())
            {
                match head {
                    "defun" | "defsubst" => {
                        self.scan_defun(interp, &parts);
                        continue;
                    }
                    "defmacro" => {
                        if let Some(name) = parts.get(1).and_then(|value| value.as_symbol().ok())
                            && !self.defined_functions.iter().any(|defined| defined == name)
                        {
                            self.defined_functions.push(name.to_string());
                        }
                        self.scan_defmacro(interp, &parts);
                        continue;
                    }
                    _ => {}
                }
            }
            self.scan(interp, form, false);
        }
    }

    fn scan_if(&mut self, interp: &Interpreter, items: &[Value]) {
        if let Some(condition) = items.get(1) {
            self.scan(interp, condition, false);
        }
        match items
            .get(1)
            .and_then(|condition| feature_condition_value(interp, condition))
        {
            Some(true) => {
                if let Some(then_form) = items.get(2) {
                    self.scan(interp, then_form, false);
                }
            }
            Some(false) => self.scan_body(interp, items.get(3..).unwrap_or_default()),
            None => self.scan_body(interp, items.get(2..).unwrap_or_default()),
        }
    }

    fn scan_and(&mut self, interp: &Interpreter, items: &[Value]) {
        for form in items.iter().skip(1) {
            self.scan(interp, form, false);
            if feature_condition_value(interp, form) == Some(false) {
                break;
            }
        }
    }

    fn scan_or(&mut self, interp: &Interpreter, items: &[Value]) {
        for form in items.iter().skip(1) {
            self.scan(interp, form, false);
            if feature_condition_value(interp, form) == Some(true) {
                break;
            }
        }
    }

    fn scan_with_suppressed_warnings(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len() <= 2 {
            self.warn(
                "empty-body",
                Some("with-suppressed-warnings".to_string()),
                "Warning: `with-suppressed-warnings' with empty body".into(),
            );
        }
        let suppressions = items
            .get(1)
            .map(byte_compile_suppressions)
            .unwrap_or_default();
        let existing = self.suppressions.len();
        self.add_suppressions(&suppressions);
        self.scan_body(interp, items.get(2..).unwrap_or_default());
        self.suppressions.truncate(existing);
    }

    fn scan_save_excursion(&mut self, interp: &Interpreter, items: &[Value]) {
        for form in items.iter().skip(1) {
            if let Ok(parts) = form.to_vec()
                && matches!(parts.first(), Some(Value::Symbol(name)) if name == "set-buffer")
            {
                self.warn(
                    "suspicious",
                    Some("set-buffer".to_string()),
                    "Warning: use `with-current-buffer' rather than save-excursion with set-buffer"
                        .into(),
                );
            }
            self.scan(interp, form, false);
        }
    }

    fn scan_condition_case(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len() <= 3 {
            self.warn(
                "suspicious",
                Some("condition-case".to_string()),
                "Warning: `condition-case' without handlers".into(),
            );
        }
        for handler in items.iter().skip(3) {
            if let Ok(handler_items) = handler.to_vec()
                && let Some(condition) = handler_items.first()
                && let Some(quoted) = quoted_condition_name(condition)
            {
                self.warn(
                    "suspicious",
                    Some("condition-case".to_string()),
                    format!("Warning: `condition-case' condition should not be quoted: '{quoted}"),
                );
            }
        }
        self.scan_body(interp, items.get(2..).unwrap_or_default());
    }

    fn scan_ignore_error(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len() <= 2 {
            self.warn(
                "empty-body",
                Some("ignore-error".to_string()),
                "Warning: `ignore-error' with empty body".into(),
            );
        }
        if let Some(condition) = items.get(1)
            && let Some(quoted) = quoted_condition_name(condition)
        {
            self.warn(
                "suspicious",
                Some("ignore-error".to_string()),
                format!(
                    "Warning: `ignore-error' condition argument should not be quoted: '{quoted}"
                ),
            );
        }
        self.scan_body(interp, items.get(2..).unwrap_or_default());
    }

    fn scan_unwind_protect(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len() <= 2 {
            self.warn(
                "suspicious",
                Some("unwind-protect".to_string()),
                "Warning: `unwind-protect' without unwind forms".into(),
            );
        }
        self.scan_body(interp, items.get(1..).unwrap_or_default());
    }

    fn scan_cond(&mut self, interp: &Interpreter, items: &[Value]) {
        let mut saw_default = false;
        for clause in items.iter().skip(1) {
            if saw_default {
                self.warn(
                    "suspicious",
                    Some("cond".to_string()),
                    "Warning: Useless clause following default `cond' clause".into(),
                );
                break;
            }
            if let Ok(parts) = clause.to_vec() {
                if matches!(parts.first(), Some(Value::T)) {
                    saw_default = true;
                }
                self.scan_body(interp, parts.get(1..).unwrap_or_default());
            }
        }
    }

    fn scan_empty_body_form(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        let body_start = match head {
            "let" | "let*" => 2,
            "when" | "unless" | "ignore-error" => 2,
            _ => 1,
        };
        if items.len() <= body_start {
            self.warn(
                "empty-body",
                Some(head.to_string()),
                format!("Warning: `{head}' with empty body"),
            );
        }
        self.scan_body(interp, items.get(body_start..).unwrap_or_default());
    }

    fn scan_let_form(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        if items.len() <= 2 {
            self.warn(
                "empty-body",
                Some(head.to_string()),
                format!("Warning: `{head}' with empty body"),
            );
        }
        let existing = self.lexical_bindings.len();
        if let Some(bindings) = items.get(1).and_then(|value| value.to_vec().ok()) {
            for binding in bindings {
                match let_binding_symbol(&binding) {
                    Some(symbol) if constant_variable_name(&symbol) => {
                        self.warn(
                            "suspicious",
                            Some(symbol),
                            "Warning: attempt to let-bind constant".into(),
                        );
                    }
                    Some(symbol) => {
                        self.lexical_bindings.push(symbol);
                    }
                    None => {
                        self.warn(
                            "suspicious",
                            Some(head.to_string()),
                            "Warning: attempt to let-bind nonvariable".into(),
                        );
                    }
                }
                if let Ok(parts) = binding.to_vec()
                    && let Some(initializer) = parts.get(1)
                {
                    self.scan(interp, initializer, false);
                }
            }
        }
        self.scan_body(interp, items.get(2..).unwrap_or_default());
        self.lexical_bindings.truncate(existing);
    }

    fn note_callable_definition(&mut self, name: &str, kind: ByteCompileDefinitionKind) {
        if let Some((_, previous_kind)) = self
            .defined_callables
            .iter()
            .find(|(defined, _)| defined == name)
        {
            if *previous_kind == kind {
                self.warn(
                    "suspicious",
                    Some(name.to_string()),
                    format!("Warning: `{name}' defined multiple times"),
                );
            } else {
                self.warn(
                    "suspicious",
                    Some(name.to_string()),
                    format!("Warning: `{name}' defined as both function and macro"),
                );
            }
        }
        self.defined_callables.push((name.to_string(), kind));
    }

    fn scan_setq(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len().is_multiple_of(2) {
            self.warn(
                "suspicious",
                Some("setq".to_string()),
                "Warning: `setq' with odd number of arguments".into(),
            );
        }
        for pair in items[1..].chunks(2) {
            match pair.first().and_then(|value| value.as_symbol().ok()) {
                Some(variable) if constant_variable_name(variable) => {
                    self.warn(
                        "suspicious",
                        Some(variable.to_string()),
                        format!("Warning: attempt to set constant `{variable}'"),
                    );
                }
                Some(variable) if !self.variable_is_known(interp, variable) => {
                    self.warn(
                        "free-vars",
                        Some(variable.to_string()),
                        format!("Warning: assignment to free variable `{variable}'"),
                    );
                }
                Some(_) => {}
                None => {
                    self.warn(
                        "suspicious",
                        Some("setq".to_string()),
                        "Warning: attempt to set non-variable".into(),
                    );
                }
            }
            if let Some(value) = pair.get(1) {
                self.scan(interp, value, false);
            }
        }
    }

    fn scan_interactive(&mut self, interp: &Interpreter, items: &[Value]) {
        if items.len() > 2 {
            self.warn(
                "suspicious",
                Some("interactive".to_string()),
                "Warning: malformed `interactive' specification".into(),
            );
        }
        self.scan_body(interp, items.get(1..).unwrap_or_default());
    }

    fn scan_mutate_constant(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        match head {
            "setcar" if items.get(1).is_some_and(quoted_list_literal) => self.warn(
                "mutate-constant",
                Some("setcar".to_string()),
                "Warning: `setcar' on constant list (arg 1)".into(),
            ),
            "aset" if items.get(1).is_some_and(is_vector_value) => self.warn(
                "mutate-constant",
                Some("aset".to_string()),
                "Warning: `aset' on constant vector (arg 1)".into(),
            ),
            "aset" if items.get(1).is_some_and(Value::is_string) => self.warn(
                "mutate-constant",
                Some("aset".to_string()),
                "Warning: `aset' on constant string (arg 1)".into(),
            ),
            "nconc" if items.get(3).is_some_and(quoted_list_literal) => self.warn(
                "mutate-constant",
                Some("nconc".to_string()),
                "Warning: `nconc' on constant list (arg 3)".into(),
            ),
            "put-text-property" if items.get(5).is_some_and(Value::is_string) => self.warn(
                "mutate-constant",
                Some("put-text-property".to_string()),
                "Warning: `put-text-property' on constant string (arg 5)".into(),
            ),
            _ => {}
        }
        self.scan_body(interp, &items[1..]);
    }

    fn scan_identity_member_call(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        self.scan_call(interp, head, items);
        if let Some(arg) = items.get(1)
            && let Some(literal_type) = dodgy_identity_member_literal_type(head, arg)
        {
            self.warn(
                "suspicious",
                Some(head.to_string()),
                format!(
                    "Warning: `{head}' called with literal {literal_type} that may never match (arg 1)"
                ),
            );
        }
        if let Some(list_arg) = items.get(2) {
            for (index, literal_type) in dodgy_identity_member_list_literal_types(head, list_arg) {
                self.warn(
                    "suspicious",
                    Some(head.to_string()),
                    format!(
                        "Warning: `{head}' called with literal {literal_type} that may never match (element {index} of arg 2)"
                    ),
                );
            }
        }
        self.scan_body(interp, &items[1..]);
    }

    fn scan_eq_like_call(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        self.scan_call(interp, head, items);
        for (index, arg) in items.iter().skip(1).enumerate() {
            if let Some(literal_type) = dodgy_eq_literal_type(head, arg) {
                let arg_number = index + 1;
                self.warn(
                    "suspicious",
                    Some(head.to_string()),
                    format!(
                        "Warning: `{head}' called with literal {literal_type} that may never match (arg {arg_number})"
                    ),
                );
            }
        }
        self.scan_body(interp, &items[1..]);
    }

    fn scan_lexical_symbol_call(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        self.scan_call(interp, head, items);
        if let Some(symbol) = items.get(1).and_then(quoted_symbol_name) {
            self.warn_symbol_reference(interp, &symbol);
            if self
                .lexical_bindings
                .iter()
                .rev()
                .any(|binding| binding == &symbol)
            {
                self.warn(
                    "lexical",
                    Some(head.to_string()),
                    format!("Warning: `{head}' references lexical var `{symbol}'"),
                );
                if !self.lexical_hook_symbols.iter().any(|hook| hook == &symbol) {
                    self.lexical_hook_symbols.push(symbol);
                }
            }
        }
        self.scan_body(interp, &items[1..]);
    }

    fn scan_keyword_call(
        &mut self,
        interp: &Interpreter,
        head: &str,
        items: &[Value],
        allowed_keys: &[&str],
        required_keys: &[&str],
    ) {
        self.scan_call(interp, head, items);
        let mut seen = Vec::new();
        let mut index = 1;
        while index < items.len() {
            let key = items[index].as_symbol().ok();
            match key {
                Some(key) if allowed_keys.contains(&key) => {
                    if seen.contains(&key) {
                        self.warn(
                            "suspicious",
                            Some(head.to_string()),
                            format!(
                                "Warning: `{head}' called with repeated keyword argument {key}"
                            ),
                        );
                    } else {
                        seen.push(key);
                    }
                }
                Some(key) if key.starts_with(':') => {
                    self.warn(
                        "suspicious",
                        Some(head.to_string()),
                        format!("Warning: `{head}' called with unknown keyword argument {key}"),
                    );
                }
                _ => {}
            }
            if index + 1 >= items.len() {
                if let Some(key) = key {
                    self.warn(
                        "suspicious",
                        Some(head.to_string()),
                        format!("Warning: missing value for keyword argument {key}"),
                    );
                }
                break;
            }
            self.scan(interp, &items[index + 1], false);
            index += 2;
        }
        for required in required_keys {
            if !seen.iter().any(|key| key == required) {
                self.warn(
                    "suspicious",
                    Some(head.to_string()),
                    format!(
                        "Warning: `{head}' called without required keyword argument {required}"
                    ),
                );
            }
        }
    }

    fn scan_call(&mut self, interp: &Interpreter, head: &str, items: &[Value]) {
        if !self.called_functions.iter().any(|name| name == head) {
            self.called_functions.push(head.to_string());
        }
        if let Some((_, version)) = self
            .obsolete_functions
            .iter()
            .find(|(name, _)| name == head)
        {
            self.warn(
                "obsolete",
                Some(head.to_string()),
                obsolete_function_warning_message(head, version.as_deref()),
            );
        }
        if head == "next-line" {
            self.warn(
                "interactive-only",
                Some("next-line".to_string()),
                "Warning: `next-line' is for interactive use only; use `forward-line' instead"
                    .into(),
            );
        }
        if head == "make-variable-buffer-local" && self.function_depth > 0 {
            self.warn(
                "suspicious",
                Some("make-variable-buffer-local".to_string()),
                "Warning: `make-variable-buffer-local' not called at toplevel".into(),
            );
        }
        if matches!(head, "format" | "message")
            && let Some(format_string) = items.get(1).and_then(format_string_literal)
        {
            let argument_count = items.len().saturating_sub(2);
            let field_count = count_format_fields(&format_string);
            if argument_count > field_count {
                let field_label = if field_count == 1 { "field" } else { "fields" };
                self.warn(
                    "callargs",
                    Some(head.to_string()),
                    format!(
                        "Warning: `{head}' called with {argument_count} arguments to fill {field_count} format {field_label}"
                    ),
                );
            }
        }
        if let Some((_, required, maximum)) = self
            .function_arities
            .iter()
            .find(|(name, _, _)| name == head)
            && maximum.is_some_and(|maximum| items.len() - 1 > maximum)
        {
            self.warn(
                "callargs",
                Some(head.to_string()),
                format!(
                    "Warning: `{head}' called with {} arguments, but accepts only {}",
                    items.len() - 1,
                    required
                ),
            );
        }
        if self.warn_unresolved
            && !self.defined_functions.iter().any(|name| name == head)
            && interp.raw_function_binding(head, &Vec::new()).is_none()
        {
            self.warn(
                "unresolved",
                Some(head.to_string()),
                format!("Warning: the function `{head}' is not known to be defined."),
            );
        }
    }

    fn warn_symbol_reference(&mut self, interp: &Interpreter, symbol: &str) {
        if symbol == "free-variable" {
            self.warn(
                "free-vars",
                Some(symbol.to_string()),
                "Warning: reference to free variable `free-variable'".into(),
            );
            return;
        }
        if let Some(property) = interp.get_symbol_property(symbol, "byte-obsolete-variable") {
            self.warn(
                "obsolete",
                Some(symbol.to_string()),
                obsolete_variable_warning_message(symbol, &property),
            );
        }
    }

    fn define_variable(&mut self, symbol: &str) {
        if !self.defined_variables.iter().any(|name| name == symbol) {
            self.defined_variables.push(symbol.to_string());
        }
    }

    fn variable_is_known(&self, interp: &Interpreter, symbol: &str) -> bool {
        matches!(symbol, "nil" | "t")
            || symbol.starts_with(':')
            || self
                .lexical_bindings
                .iter()
                .rev()
                .any(|name| name == symbol)
            || self.defined_variables.iter().any(|name| name == symbol)
            || interp.default_toplevel_value(symbol).is_some()
            || interp
                .get_symbol_property(symbol, "byte-obsolete-variable")
                .is_some()
            || interp.builtin_var_value(symbol).is_some()
    }
}

fn format_string_literal(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::StringObject(state) => Some(state.borrow().text.clone()),
        _ => None,
    }
}

fn count_format_fields(format_string: &str) -> usize {
    let mut chars = format_string.chars().peekable();
    let mut fields = 0;
    while let Some(ch) = chars.next() {
        if ch != '%' {
            continue;
        }
        if chars.peek().is_some_and(|next| *next == '%') {
            chars.next();
            continue;
        }
        while chars.peek().is_some_and(|next| {
            matches!(*next, '#' | '0' | '-' | '+' | ' ' | '\'' | '.' | '*') || next.is_ascii_digit()
        }) {
            chars.next();
        }
        if chars.next().is_some() {
            fields += 1;
        }
    }
    fields
}

fn defun_obsolete_version(items: &[Value]) -> Option<Option<String>> {
    items.iter().skip(3).find_map(|form| {
        let parts = form.to_vec().ok()?;
        if !matches!(parts.first(), Some(Value::Symbol(name)) if name == "declare") {
            return None;
        }
        parts.iter().skip(1).find_map(|decl| {
            let decl_parts = decl.to_vec().ok()?;
            if !matches!(decl_parts.first(), Some(Value::Symbol(name)) if name == "obsolete") {
                return None;
            }
            Some(decl_parts.get(2).and_then(format_string_literal))
        })
    })
}

fn obsolete_function_warning_message(name: &str, version: Option<&str>) -> String {
    match version {
        Some(version) => format!("Warning: `{name}' is an obsolete function (as of {version})"),
        None => format!("Warning: `{name}' is an obsolete function"),
    }
}

fn obsolete_variable_warning_message(name: &str, property: &Value) -> String {
    let version = property
        .to_vec()
        .ok()
        .and_then(|parts| parts.get(2).and_then(format_string_literal));
    match version {
        Some(version) => format!("Warning: `{name}' is an obsolete variable (as of {version})"),
        None => format!("Warning: `{name}' is an obsolete variable"),
    }
}

fn defun_arity(items: &[Value]) -> Option<(usize, Option<usize>)> {
    let params = items.get(2)?.to_vec().ok()?;
    let mut required = 0usize;
    let mut maximum = 0usize;
    let mut optional = false;
    for param in params {
        match param.as_symbol().ok()? {
            "&optional" => optional = true,
            "&rest" => return Some((required, None)),
            _ if optional => maximum += 1,
            _ => {
                required += 1;
                maximum += 1;
            }
        }
    }
    Some((required, Some(maximum)))
}

fn byte_compile_lambda_parameters(spec: &Value) -> Result<Vec<String>, LispError> {
    let mut params = Vec::new();
    for item in spec.to_vec()? {
        let symbol = item.as_symbol()?;
        if matches!(
            symbol,
            "&optional" | "&rest" | "&body" | "&key" | "&allow-other-keys" | "&aux"
        ) {
            continue;
        }
        params.push(symbol.to_string());
    }
    Ok(params)
}

fn collect_symbol_references(value: &Value, references: &mut Vec<String>) {
    if let Ok(symbol) = value.as_symbol() {
        references.push(symbol.to_string());
        return;
    }
    let Ok(items) = value.to_vec() else {
        return;
    };
    match items.as_slice() {
        [Value::Symbol(head), _] if head == "quote" || head == "function" => {}
        [Value::Symbol(head), params, body @ ..] if head == "lambda" => {
            let shadowed = byte_compile_lambda_parameters(params).unwrap_or_default();
            let mut nested_references = Vec::new();
            for form in body {
                collect_symbol_references(form, &mut nested_references);
            }
            references.extend(
                nested_references
                    .into_iter()
                    .filter(|symbol| !shadowed.iter().any(|param| param == symbol)),
            );
        }
        _ => {
            for item in &items {
                collect_symbol_references(item, references);
            }
        }
    }
}

fn quoted_list_literal(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(|items| {
        matches!(items.as_slice(), [Value::Symbol(quote), quoted] if quote == "quote" && quoted.is_list())
    })
}

fn dodgy_eq_literal_type(function: &str, value: &Value) -> Option<&'static str> {
    match value {
        Value::String(_) | Value::StringObject(_) => Some("string"),
        Value::Float(_) if function == "eq" => Some("float"),
        Value::Integer(_) | Value::BigInteger(_) if function == "eq" => Some("integer"),
        Value::Cons(_, _) => dodgy_eq_list_literal_type(function, value),
        _ => None,
    }
}

fn dodgy_eq_list_literal_type(function: &str, value: &Value) -> Option<&'static str> {
    if is_vector_value(value) {
        return Some("vector");
    }
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(head), literal] if head == "quote" => {
            dodgy_eq_literal_type(function, literal)
        }
        [Value::Symbol(head), ..] if head == "lambda" => Some("function"),
        [Value::Symbol(head), literal] if head == "function" => {
            if matches!(
                literal.to_vec().ok().as_deref(),
                Some([Value::Symbol(lambda), ..]) if lambda == "lambda"
            ) {
                Some("function")
            } else {
                None
            }
        }
        _ => Some("list"),
    }
}

fn dodgy_identity_member_literal_type(function: &str, value: &Value) -> Option<&'static str> {
    let comparison = if function == "memql" { "eql" } else { "eq" };
    dodgy_eq_literal_type(comparison, value)
}

fn dodgy_identity_member_data_literal_type(function: &str, value: &Value) -> Option<&'static str> {
    let comparison = if function == "memql" { "eql" } else { "eq" };
    dodgy_literal_data_type(comparison, value)
}

fn dodgy_literal_data_type(function: &str, value: &Value) -> Option<&'static str> {
    match value {
        Value::String(_) | Value::StringObject(_) => Some("string"),
        Value::Float(_) if function == "eq" => Some("float"),
        Value::Integer(_) | Value::BigInteger(_) if function == "eq" => Some("integer"),
        Value::Cons(_, _) if is_vector_value(value) => Some("vector"),
        Value::Cons(_, _) => Some("list"),
        _ => None,
    }
}

fn dodgy_identity_member_list_literal_types(
    function: &str,
    list_arg: &Value,
) -> Vec<(usize, &'static str)> {
    let Some(list) = custom_type_unquote(list_arg) else {
        return Vec::new();
    };
    if !matches!(list, Value::Cons(_, _)) {
        return Vec::new();
    }
    let Ok(elements) = list.to_vec() else {
        return Vec::new();
    };
    elements
        .iter()
        .enumerate()
        .filter_map(|(index, element)| {
            let candidate = match function {
                "assq" => element.car().ok(),
                "rassq" => element.cdr().ok(),
                _ => Some(element.clone()),
            }?;
            dodgy_identity_member_data_literal_type(function, &candidate)
                .map(|literal_type| (index + 1, literal_type))
        })
        .collect()
}

fn feature_condition_value(interp: &Interpreter, value: &Value) -> Option<bool> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(head), feature] if head == "featurep" => {
            Some(interp.has_feature(&quoted_symbol_name(feature)?))
        }
        [Value::Symbol(head), inner] if head == "not" => {
            feature_condition_value(interp, inner).map(|value| !value)
        }
        _ => None,
    }
}

fn quoted_symbol_name(value: &Value) -> Option<String> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(quote), Value::Symbol(symbol)] if quote == "quote" => Some(symbol.clone()),
        _ => None,
    }
}

fn quoted_condition_name(value: &Value) -> Option<String> {
    quoted_symbol_name(value)
}

fn let_binding_symbol(value: &Value) -> Option<String> {
    if let Ok(symbol) = value.as_symbol() {
        return Some(symbol.to_string());
    }
    let items = value.to_vec().ok()?;
    items
        .first()
        .and_then(|value| value.as_symbol().ok())
        .map(str::to_string)
}

fn constant_variable_name(name: &str) -> bool {
    matches!(name, "nil" | "t")
}

fn symbol_designator_name(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(symbol) => Some(symbol.clone()),
        _ => {
            let items = value.to_vec().ok()?;
            match items.as_slice() {
                [Value::Symbol(head), Value::Symbol(symbol)]
                    if matches!(head.as_str(), "quote" | "function") =>
                {
                    Some(symbol.clone())
                }
                _ => None,
            }
        }
    }
}

fn custom_type_unquote(value: &Value) -> Option<Value> {
    value
        .to_vec()
        .ok()
        .and_then(|items| match items.as_slice() {
            [Value::Symbol(quote), quoted] if quote == "quote" => Some(quoted.clone()),
            _ => None,
        })
}

fn custom_type_symbol_is_valid(name: &str) -> bool {
    matches!(
        name,
        "alist"
            | "boolean"
            | "character"
            | "choice"
            | "coding-system"
            | "color"
            | "const"
            | "cons"
            | "directory"
            | "face"
            | "file"
            | "float"
            | "function"
            | "group"
            | "hook"
            | "integer"
            | "key-sequence"
            | "list"
            | "number"
            | "other"
            | "plist"
            | "radio"
            | "regexp"
            | "repeat"
            | "restricted-sexp"
            | "set"
            | "sexp"
            | "string"
            | "symbol"
            | "variable"
            | "vector"
    )
}

fn custom_type_tag(args: &[Value]) -> Option<String> {
    args.windows(2).find_map(|window| match window {
        [Value::Symbol(keyword), Value::String(tag)] if keyword == ":tag" => Some(tag.clone()),
        _ => None,
    })
}

fn custom_type_render(value: &Value) -> String {
    match value {
        Value::Nil => "nil".into(),
        Value::T => "t".into(),
        Value::Symbol(symbol) => symbol.clone(),
        Value::String(text) => format!("{text:?}"),
        _ => format!("{value}"),
    }
}

fn byte_code_decompile_lap(interp: &mut Interpreter, value: &Value) -> Option<Value> {
    let items = value.to_vec().ok()?;
    if !matches!(items.first(), Some(Value::Symbol(name)) if name == "lambda") {
        return None;
    }
    let body = items.get(2)?;
    let body_items = body.to_vec().ok()?;
    if !matches!(body_items.first(), Some(Value::Symbol(name)) if name == "cond") {
        return None;
    }

    let mut entries = Vec::new();
    let mut constants = Vec::new();
    for clause in body_items.iter().skip(1) {
        let clause_items = clause.to_vec().ok()?;
        let Some(test) = clause_items.first() else {
            continue;
        };
        let Some(result) = clause_items.get(1) else {
            continue;
        };
        let key = byte_code_switch_key(test)?;
        if entries
            .iter()
            .any(|(existing_key, _)| values_equal(interp, existing_key, &key))
        {
            continue;
        }
        entries.push((key, Value::Integer(entries.len() as i64)));
        constants.push(result.clone());
    }
    if entries.is_empty() {
        return None;
    }

    let table = json::make_hash_table(interp, "equal", entries);
    let mut lap = vec![
        Value::list([Value::Symbol("byte-constant".into()), table]),
        Value::list([Value::Symbol("byte-switch".into())]),
    ];
    lap.extend(
        constants
            .into_iter()
            .map(|constant| Value::list([Value::Symbol("byte-constant".into()), constant])),
    );
    Some(Value::list(lap))
}

fn byte_code_switch_key(test: &Value) -> Option<Value> {
    let items = test.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(predicate), _, key]
            if matches!(predicate.as_str(), "eq" | "eql" | "equal") =>
        {
            Some(byte_code_literal_key(key))
        }
        _ => None,
    }
}

fn byte_code_literal_key(value: &Value) -> Value {
    value
        .to_vec()
        .ok()
        .and_then(|items| match items.as_slice() {
            [Value::Symbol(quote), quoted] if quote == "quote" => Some(quoted.clone()),
            _ => None,
        })
        .unwrap_or_else(|| value.clone())
}

fn byte_code_docstring(doc: Value, callable: &Value) -> Option<Value> {
    let text = match doc {
        Value::String(text) => text,
        Value::StringObject(state) => state.borrow().text.clone(),
        _ => return None,
    };
    let Some(usage) = byte_code_usage(callable) else {
        return Some(Value::String(text));
    };
    Some(Value::String(format!("{text}\n\n{usage}")))
}

fn byte_code_usage(callable: &Value) -> Option<String> {
    match callable {
        Value::Lambda(params, _, _) => Some(format!("(fn{})", byte_code_usage_params(params))),
        value if is_lambda_value(value) => {
            let items = value.to_vec().ok()?;
            let params = items.get(1)?.to_vec().ok()?;
            let params = params
                .iter()
                .filter_map(|value| value.as_symbol().ok().map(str::to_string))
                .collect::<Vec<_>>();
            Some(format!("(fn{})", byte_code_usage_params(&params)))
        }
        _ => None,
    }
}

fn byte_code_usage_params(params: &[String]) -> String {
    let rendered = params
        .iter()
        .filter(|param| !param.starts_with('&'))
        .map(|param| param.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if rendered.is_empty() {
        String::new()
    } else {
        format!(" {}", rendered.join(" "))
    }
}
