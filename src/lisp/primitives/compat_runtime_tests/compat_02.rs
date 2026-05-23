use super::*;

#[test]
fn coding_system_get_reports_for_unibyte_for_raw_text() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "coding-system-get",
            &[
                Value::Symbol("raw-text".into()),
                Value::Symbol(":for-unibyte".into()),
            ],
            &mut env,
        )
        .expect("raw-text :for-unibyte"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "coding-system-get",
            &[
                Value::Symbol("utf-8".into()),
                Value::Symbol(":for-unibyte".into()),
            ],
            &mut env,
        )
        .expect("utf-8 is multibyte"),
        Value::Nil
    );
}

#[test]
fn decode_coding_region_inserts_into_destination_buffer() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.buffer = crate::buffer::Buffer::from_text("*source*", "abc");
    let (buffer_id, buffer_name) = interp.create_buffer("*dest*");

    assert!(
        call(
            &mut interp,
            "decode-coding-region",
            &[
                Value::Integer(1),
                Value::Integer(4),
                Value::Symbol("utf-8".into()),
                Value::Buffer(buffer_id, buffer_name),
            ],
            &mut env,
        )
        .is_ok()
    );
    let dest = interp.get_buffer_by_id(buffer_id).expect("dest buffer");
    assert_eq!(dest.buffer_string(), "abc");
}

#[test]
fn add_function_supports_local_place_spec() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "add-function",
            &[
                Value::Symbol(":around".into()),
                Value::list([
                    Value::Symbol("emaxx-local-function-place".into()),
                    Value::Symbol("revert-buffer-function".into()),
                ]),
                Value::Symbol("archive--mode-revert".into()),
            ],
            &mut env,
        )
        .expect("install local function"),
        Value::Nil
    );
    assert_eq!(
        interp.buffer_local_value(interp.current_buffer_id(), "revert-buffer-function"),
        Some(Value::Symbol("archive--mode-revert".into()))
    );
}

#[test]
fn add_function_supports_symbol_variable_place() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (defun emaxx-add-function-original () "base")
              (defun emaxx-add-function-around (orig)
                (concat "wrapped-" (funcall orig)))
              (setq emaxx-add-function-target #'emaxx-add-function-original)
              (add-function :around emaxx-add-function-target
                            #'emaxx-add-function-around)
              (funcall emaxx-add-function-target))
            "#,
    )
    .read_all()
    .expect("add-function symbol-place test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("add-function symbol-place should evaluate");
    assert_eq!(result, Value::String("wrapped-base".into()));
}

#[test]
fn advice_add_supports_around_message_builtin() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (setq emaxx-test-message-log nil)
              (defun emaxx-test-message-advice (orig &rest args)
                (setq emaxx-test-message-log (apply #'format-message args))
                (funcall orig "%s" emaxx-test-message-log))
              (advice-add 'message :around #'emaxx-test-message-advice)
              (let ((result (message "value=%s" 42))
                    (current (current-message)))
                (advice-remove 'message #'emaxx-test-message-advice)
                (list emaxx-test-message-log result current)))
            "#,
    )
    .read_all()
    .expect("message advice test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("message advice should evaluate");
    let values = result.to_vec().expect("message result list");
    assert_eq!(values.len(), 3);
    for value in values {
        assert_eq!(string_text(&value).expect("message string"), "value=42");
    }
}

#[test]
fn advice_add_accepts_forward_referenced_advice_symbol() {
    let forms = Reader::new(
        "(progn
           (defun sample-forward-target () 'original)
           (advice-add 'sample-forward-target :around #'sample-forward-advice)
           (defun sample-forward-advice (orig &rest args)
             (list 'wrapped (apply orig args)))
           (sample-forward-target))",
    )
    .read_all()
    .expect("forward advice test should parse");
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let mut result = Value::Nil;
    for form in forms {
        result = interp
            .eval(&form, &mut env)
            .expect("forward advice symbol should resolve when invoked");
    }
    assert_eq!(
        result,
        Value::list([
            Value::Symbol("wrapped".into()),
            Value::Symbol("original".into()),
        ])
    );
}

#[test]
fn funcall_message_builtin_from_lambda() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (defun emaxx-test-call-message (orig &rest args)
                (setq emaxx-test-message-log (apply #'format-message args))
                (funcall orig "%s" emaxx-test-message-log))
              (funcall #'emaxx-test-call-message (symbol-function 'message) "value=%s" 42))
            "#,
    )
    .read_all()
    .expect("direct message-call test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("direct message-call should evaluate");
    assert_eq!(result, Value::String("value=42".into()));
}

#[test]
fn apply_format_message_from_lambda() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (defun emaxx-test-format-message (&rest args)
                (apply #'format-message args))
              (emaxx-test-format-message "value=%s" 42))
            "#,
    )
    .read_all()
    .expect("format-message lambda test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("format-message lambda should evaluate");
    assert_eq!(result, Value::String("value=42".into()));
}

#[test]
fn apply_format_message_top_level() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(r#"(apply #'format-message '("value=%s" 42))"#)
        .read_all()
        .expect("top-level apply format-message should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("top-level apply format-message should evaluate");
    assert_eq!(result, Value::String("value=42".into()));
}

#[test]
fn direct_format_message_top_level() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(r#"(format-message "value=%s" 42)"#)
        .read_all()
        .expect("top-level direct format-message should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("top-level direct format-message should evaluate");
    assert_eq!(result, Value::String("value=42".into()));
}

#[test]
fn user_error_formats_message_arguments() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let error = call(
        &mut interp,
        "user-error",
        &[
            Value::String("No%s dynamic expansion for `%s' found".into()),
            Value::String(" further".into()),
            Value::String("ab".into()),
        ],
        &mut env,
    )
    .expect_err("user-error should signal");
    let LispError::SignalValue(value) = error else {
        panic!("expected signal value");
    };
    assert_eq!(
        value,
        Value::list([
            Value::Symbol("user-error".into()),
            Value::String("No further dynamic expansion for `ab' found".into()),
        ])
    );
}

#[test]
fn advice_add_supports_around_read_event_override_without_side_effects() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (defun emaxx-test-read-event-advice (_orig &rest _args)
                ?x)
              (advice-add 'read-event :around #'emaxx-test-read-event-advice)
              (let ((last-input-event nil)
                    (unread-command-events nil))
                (prog1
                    (list (read-event)
                          last-input-event
                          unread-command-events)
                  (advice-remove 'read-event #'emaxx-test-read-event-advice))))
            "#,
    )
    .read_all()
    .expect("read-event advice test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("read-event advice should evaluate");
    assert_eq!(
        result,
        Value::list([Value::Integer('x' as i64), Value::Nil, Value::Nil,])
    );
}

#[test]
fn advice_add_supports_after_function() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (setq emaxx-test-after-log nil)
              (defun emaxx-test-after-target () 'done)
              (defun emaxx-test-after-advice (&rest _args)
                (setq emaxx-test-after-log 'after))
              (advice-add 'emaxx-test-after-target :after #'emaxx-test-after-advice)
              (prog1
                  (list (emaxx-test-after-target) emaxx-test-after-log)
                (advice-remove 'emaxx-test-after-target #'emaxx-test-after-advice)))
            "#,
    )
    .read_all()
    .expect("after advice test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("after advice should evaluate");
    assert_eq!(
        result,
        Value::list([Value::Symbol("done".into()), Value::Symbol("after".into())])
    );
}

#[test]
fn kmacro_batch_stub_loads_and_runs_end_macro_advice() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    interp.load_target("kmacro").expect("load kmacro");
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (setq emaxx-test-after-log nil)
              (defun emaxx-test-end-kbd-macro-advice (&rest _args)
                (setq emaxx-test-after-log 'after))
              (advice-add 'end-kbd-macro :after #'emaxx-test-end-kbd-macro-advice)
              (kmacro-start-macro nil)
              (prog1
                  (list (end-kbd-macro) emaxx-test-after-log)
                (advice-remove 'end-kbd-macro #'emaxx-test-end-kbd-macro-advice)))
            "#,
    )
    .read_all()
    .expect("kmacro batch advice test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("kmacro batch advice should evaluate");
    assert_eq!(
        result,
        Value::list([Value::Nil, Value::Symbol("after".into())])
    );
}

#[test]
fn make_temp_file_creates_a_file_for_relative_prefix() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let path = call(
        &mut interp,
        "make-temp-file",
        &[Value::String("emaxx-compat-".into())],
        &mut env,
    )
    .expect("create temp file")
    .as_string()
    .expect("path string")
    .to_string();
    assert!(std::path::Path::new(&path).exists());
    std::fs::remove_file(path).expect("cleanup temp file");
}

#[test]
fn file_name_extension_helpers_match_archive_usage() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "file-name-base",
            &[Value::String("/tmp/demo.tar.gz".into())],
            &mut env,
        )
        .expect("base name"),
        Value::String("demo.tar".into())
    );
    assert_eq!(
        call(
            &mut interp,
            "file-name-sans-extension",
            &[Value::String("demo.tar.gz".into())],
            &mut env,
        )
        .expect("strip suffix"),
        Value::String("demo.tar".into())
    );
    assert_eq!(
        call(
            &mut interp,
            "file-name-extension",
            &[Value::String("demo.tar.gz".into()), Value::T],
            &mut env,
        )
        .expect("extension with period"),
        Value::String(".gz".into())
    );
}

#[test]
fn rename_visited_file_moves_disk_file_and_updates_buffer_path() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let old_path = call(
        &mut interp,
        "make-temp-file",
        &[Value::String("emaxx-rename-visited-file-".into())],
        &mut env,
    )
    .expect("create source file")
    .as_string()
    .expect("temp file path")
    .to_string();
    let new_path = format!("{old_path}.zip");

    interp.buffer.file = Some(old_path.clone());
    interp.buffer.file_truename = Some(old_path.clone());
    interp
        .buffer
        .set_visited_file_modtime(file_modtime(&old_path).expect("source modtime"));

    call(
        &mut interp,
        "rename-visited-file",
        &[Value::String(new_path.clone())],
        &mut env,
    )
    .expect("rename visited file");

    assert!(!Path::new(&old_path).exists());
    assert!(Path::new(&new_path).exists());
    assert_eq!(interp.buffer.file.as_deref(), Some(new_path.as_str()));
    assert_eq!(
        interp.buffer.file_truename.as_deref(),
        Some(new_path.as_str())
    );
    assert!(interp.buffer.visited_file_modtime().is_some());

    std::fs::remove_file(new_path).expect("cleanup renamed file");
}

#[test]
fn revert_buffer_reloads_non_utf8_file_as_raw_text() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let path = call(
        &mut interp,
        "make-temp-file",
        &[Value::String("emaxx-revert-raw-buffer-".into())],
        &mut env,
    )
    .expect("create source file")
    .as_string()
    .expect("temp file path")
    .to_string();
    let bytes = [0xFF, b'a'];
    std::fs::write(&path, bytes).expect("write raw bytes");

    interp.buffer = crate::buffer::Buffer::from_text("*raw*", "");
    interp.buffer.file = Some(path.clone());
    interp.buffer.file_truename = Some(path.clone());
    interp.buffer.set_multibyte(false);
    interp
        .buffer
        .set_visited_file_modtime(file_modtime(&path).expect("source modtime"));

    call(&mut interp, "revert-buffer", &[], &mut env).expect("revert raw buffer");

    assert_eq!(interp.buffer.buffer_string(), decode_raw_text_bytes(&bytes));
    assert!(!interp.buffer.is_multibyte());

    std::fs::remove_file(path).expect("cleanup raw file");
}

#[test]
fn save_buffer_skips_unmodified_and_unchanged_files() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let path = call(
        &mut interp,
        "make-temp-file",
        &[Value::String("emaxx-save-unmodified-".into())],
        &mut env,
    )
    .expect("create source file")
    .as_string()
    .expect("temp file path")
    .to_string();
    std::fs::write(&path, "fresh").expect("write source file");

    interp.buffer = crate::buffer::Buffer::from_text("*save*", "fresh");
    interp.buffer.file = Some(path.clone());
    interp.buffer.file_truename = Some(path.clone());
    interp.buffer.set_unmodified();

    let original_permissions = std::fs::metadata(&path).expect("metadata").permissions();
    let mut permissions = original_permissions.clone();
    permissions.set_readonly(true);
    std::fs::set_permissions(&path, permissions).expect("make file read-only");

    call(&mut interp, "save-buffer", &[], &mut env).expect("unmodified save is a no-op");
    interp.buffer.set_modified();
    call(&mut interp, "save-buffer", &[], &mut env).expect("unchanged text does not need a write");

    std::fs::set_permissions(&path, original_permissions).expect("restore writable file");
    std::fs::remove_file(path).expect("cleanup unmodified save file");
}

#[test]
fn buffer_stale_default_detects_clean_file_modtime_changes() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let path = call(
        &mut interp,
        "make-temp-file",
        &[Value::String("emaxx-buffer-stale-".into())],
        &mut env,
    )
    .expect("create source file")
    .as_string()
    .expect("temp file path")
    .to_string();
    std::fs::write(&path, "fresh").expect("write initial file contents");

    interp.buffer = crate::buffer::Buffer::from_text("*stale*", "fresh");
    interp.buffer.file = Some(path.clone());
    interp.buffer.file_truename = Some(path.clone());
    interp
        .buffer
        .set_visited_file_modtime(file_modtime(&path).expect("source modtime"));

    std::fs::write(&path, "changed").expect("update file contents");
    interp.buffer.set_unmodified();

    assert_eq!(
        call(
            &mut interp,
            "buffer-stale--default-function",
            &[Value::T],
            &mut env,
        )
        .expect("check stale file"),
        Value::T
    );

    interp.buffer.set_modified();
    assert_eq!(
        call(
            &mut interp,
            "buffer-stale--default-function",
            &[Value::T],
            &mut env,
        )
        .expect("modified buffers are not stale"),
        Value::Nil
    );

    std::fs::remove_file(path).expect("cleanup stale file");
}

#[test]
fn revert_buffer_honors_buffer_local_revert_function() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let path = call(
            &mut interp,
            "make-temp-file",
            &[Value::String("emaxx-revert-buffer-function-".into())],
            &mut env,
        )
        .expect("create source file")
        .as_string()
        .expect("temp file path")
        .to_string();
        std::fs::write(&path, "fresh").expect("write file contents");

        interp.buffer = crate::buffer::Buffer::from_text("*revert*", "stale");
        interp.buffer.file = Some(path.clone());
        interp.buffer.file_truename = Some(path.clone());

        let forms = Reader::new(
            r#"
                (progn
                  (defun sample-revert-wrapper (orig-fun &rest _args)
                    (setq sample-revert-called t)
                    (funcall orig-fun))
                  (setq-local sample-revert-called nil)
                  (setq-local revert-buffer-function 'sample-revert-wrapper)
                  (revert-buffer)
                  sample-revert-called)
                "#,
        )
        .read_all()
        .expect("parse wrapper forms");
        let mut result = Value::Nil;
        for form in forms {
            result = interp.eval(&form, &mut env).expect("evaluate wrapper form");
        }

        assert_eq!(result, Value::T);
        assert_eq!(interp.buffer.buffer_string(), "fresh");

        std::fs::remove_file(path).expect("cleanup wrapper file");
    });
}

#[test]
fn revert_buffer_dynamic_nil_suppresses_buffer_local_revert_function() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let path = call(
            &mut interp,
            "make-temp-file",
            &[Value::String("emaxx-revert-buffer-dynamic-".into())],
            &mut env,
        )
        .expect("create source file")
        .as_string()
        .expect("temp file path")
        .to_string();
        std::fs::write(&path, "fresh").expect("write file contents");

        interp.buffer = crate::buffer::Buffer::from_text("*revert*", "stale");
        interp.buffer.file = Some(path.clone());
        interp.buffer.file_truename = Some(path.clone());

        let forms = Reader::new(
            r#"
                (progn
                  (defun sample-revert-wrapper (_orig-fun &rest _args)
                    (error "wrapper should be dynamically suppressed"))
                  (setq-local revert-buffer-function 'sample-revert-wrapper)
                  (let ((revert-buffer-function nil))
                    (revert-buffer))
                  (buffer-string))
                "#,
        )
        .read_all()
        .expect("parse dynamic suppression forms");
        let mut result = Value::Nil;
        for form in forms {
            result = interp.eval(&form, &mut env).expect("evaluate revert form");
        }

        assert_eq!(result, Value::String("fresh".into()));
        std::fs::remove_file(path).expect("cleanup dynamic revert file");
    });
}

#[test]
fn get_byte_reads_unibyte_buffer_positions() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.buffer = crate::buffer::Buffer::from_text("*bytes*", "\u{00ff}");
    interp.buffer.set_multibyte(false);

    assert_eq!(
        call(&mut interp, "get-byte", &[Value::Integer(1)], &mut env).expect("read first byte"),
        Value::Integer(0xFF)
    );
}

#[test]
fn write_process_output_supports_stdout_buffer_and_stderr_file() {
    let mut interp = Interpreter::new();
    let stderr_path = std::env::temp_dir()
        .join("emaxx-process-stderr-test")
        .display()
        .to_string();
    let destination = Value::list([Value::T, Value::String(stderr_path.clone())]);

    write_process_output(&mut interp, &destination, &[0xFF], b"warn\n")
        .expect("write process output");
    assert_eq!(
        interp.buffer.buffer_string(),
        decode_raw_text_bytes(&[0xFF])
    );
    assert_eq!(std::fs::read(&stderr_path).expect("stderr file"), b"warn\n");
    std::fs::remove_file(stderr_path).expect("cleanup stderr file");
}

#[test]
fn write_process_output_merges_stderr_for_t_cons_destination() {
    let mut interp = Interpreter::new();
    let destination = Value::cons(Value::T, Value::T);

    write_process_output(&mut interp, &destination, b"out\n", b"err\n")
        .expect("write merged process output");
    assert_eq!(interp.buffer.buffer_string(), "out\nerr\n");
}

#[test]
fn file_regular_p_distinguishes_files_from_directories() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let dir = std::env::temp_dir().join(format!(
        "emaxx-file-regular-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&dir).expect("create temp directory");
    let file = dir.join("sample");
    std::fs::write(&file, "content").expect("write temp file");

    assert_eq!(
        call(
            &mut interp,
            "file-regular-p",
            &[Value::String(file.display().to_string())],
            &mut env,
        )
        .expect("regular file"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "file-regular-p",
            &[Value::String(dir.display().to_string())],
            &mut env,
        )
        .expect("directory"),
        Value::Nil
    );

    std::fs::remove_file(file).expect("cleanup temp file");
    std::fs::remove_dir(dir).expect("cleanup temp directory");
}

#[test]
fn file_relative_name_returns_child_and_parent_relative_paths() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "file-relative-name",
            &[
                Value::String("/tmp/project/src/main.c".into()),
                Value::String("/tmp/project/".into()),
            ],
            &mut env,
        )
        .expect("child relative path"),
        Value::String("src/main.c".into())
    );
    assert_eq!(
        call(
            &mut interp,
            "file-relative-name",
            &[
                Value::String("/tmp/include/sys/cdefs.h".into()),
                Value::String("/tmp/project/src/".into()),
            ],
            &mut env,
        )
        .expect("parent relative path"),
        Value::String("../../include/sys/cdefs.h".into())
    );
}

#[test]
fn write_region_reports_output_errors_as_file_error() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let directory =
        std::env::temp_dir().join(format!("emaxx-write-region-dir-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("create temp directory");
    let path = directory.to_string_lossy().to_string();
    let error = call(
        &mut interp,
        "write-region",
        &[
            Value::String("content".into()),
            Value::Nil,
            Value::String(path.clone()),
        ],
        &mut env,
    )
    .expect_err("writing to a directory should signal file-error");
    let LispError::SignalValue(value) = error else {
        panic!("expected signal value");
    };
    let items = value.to_vec().expect("file error list");
    assert_eq!(items.first(), Some(&Value::Symbol("file-error".into())));
    assert_eq!(
        items.get(1),
        Some(&Value::String("Opening output file".into()))
    );
    assert_eq!(items.get(3), Some(&Value::String(path)));
    std::fs::remove_dir_all(directory).expect("cleanup temp directory");
}

#[test]
fn make_empty_file_creates_empty_file_and_optional_parents() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let directory =
        std::env::temp_dir().join(format!("emaxx-make-empty-file-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    let nested = directory.join("child").join("empty.txt");
    let nested_text = nested.to_string_lossy().to_string();

    assert_eq!(
        call(
            &mut interp,
            "make-empty-file",
            &[Value::String(nested_text.clone()), Value::T],
            &mut env,
        )
        .expect("create empty file with parents"),
        Value::Nil
    );
    assert_eq!(
        std::fs::metadata(&nested)
            .expect("empty file metadata")
            .len(),
        0
    );
    std::fs::write(&nested, "content").expect("write content");
    assert!(
        call(
            &mut interp,
            "make-empty-file",
            &[Value::String(nested_text.clone())],
            &mut env,
        )
        .is_err()
    );
    assert_eq!(
        call(
            &mut interp,
            "make-empty-file",
            &[Value::String(nested_text), Value::T],
            &mut env,
        )
        .expect("truncate existing file with parents"),
        Value::Nil
    );
    assert_eq!(
        std::fs::metadata(&nested)
            .expect("truncated file metadata")
            .len(),
        0
    );
    std::fs::remove_dir_all(directory).expect("cleanup temp directory");
}

#[test]
fn member_ignore_case_matches_strings_case_insensitively() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "member-ignore-case",
            &[
                Value::String("UNZIP".into()),
                Value::list([
                    Value::String("zip".into()),
                    Value::String("unzip".into()),
                    Value::String("7z".into()),
                ]),
            ],
            &mut env,
        )
        .expect("find case-insensitive member"),
        Value::list([Value::String("unzip".into()), Value::String("7z".into())])
    );
}

#[test]
fn value_less_vectors_break_ties_after_equal_prefix_values() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*value-less-buffer*");
    let buffer = Value::Buffer(buffer_id, buffer_name);
    let marker = interp.make_marker();
    let Value::Marker(marker_id) = marker else {
        panic!("make_marker should return a marker");
    };
    interp
        .set_marker(marker_id, Some(12), Some(buffer_id))
        .expect("set marker");
    let process = interp
        .create_process(None, None, vec![], None)
        .expect("create process");
    let cases = vec![
        ("number", Value::Integer(1)),
        ("symbol", Value::Symbol("a".into())),
        ("string", Value::String("a".into())),
        ("list", Value::list([Value::Integer(1), Value::Integer(2)])),
        (
            "vector",
            call(
                &mut interp,
                "vector",
                &[Value::Integer(1), Value::Integer(2)],
                &mut env,
            )
            .expect("create vector"),
        ),
        (
            "bool-vector",
            make_bool_vector_value(&mut interp, [true, false, true]),
        ),
        (
            "record",
            interp.create_record("a", vec![Value::Integer(2), Value::Integer(3)]),
        ),
        ("buffer", buffer),
        ("marker", marker),
        ("process", process),
    ];

    for (label, value) in cases {
        let left = call(
            &mut interp,
            "vector",
            &[value.clone(), Value::Integer(1)],
            &mut env,
        )
        .expect("create left vector");
        let right = call(
            &mut interp,
            "vector",
            &[value.clone(), Value::Integer(2)],
            &mut env,
        )
        .expect("create right vector");
        let result = call(&mut interp, "value<", &[left, right], &mut env)
            .unwrap_or_else(|error| panic!("{label}: value< errored: {error:?}"));
        assert_eq!(
            result,
            Value::T,
            "{label}: equal-prefix vectors should compare by suffix"
        );
    }
}

#[test]
fn value_less_selected_upstream_ordered_cases_match_emacs() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buf1_id, buf1_name) = interp.create_buffer(" *one*");
    let (buf2_id, buf2_name) = interp.create_buffer(" *two*");
    let (buf3_id, buf3_name) = interp.create_buffer(" *three*");
    let buf1 = Value::Buffer(buf1_id, buf1_name);
    let buf2 = Value::Buffer(buf2_id, buf2_name);
    let buf3 = Value::Buffer(buf3_id, buf3_name);
    interp.kill_buffer_id(buf3_id);

    let mark1 = interp.make_marker();
    let Value::Marker(mark1_id) = mark1 else {
        panic!("mark1 should be a marker");
    };
    interp
        .set_marker(mark1_id, Some(12), Some(buf1_id))
        .expect("set mark1");
    let mark2 = interp.make_marker();
    let Value::Marker(mark2_id) = mark2 else {
        panic!("mark2 should be a marker");
    };
    interp
        .set_marker(mark2_id, Some(13), Some(buf1_id))
        .expect("set mark2");
    let mark3 = interp.make_marker();
    let Value::Marker(mark3_id) = mark3 else {
        panic!("mark3 should be a marker");
    };
    interp
        .set_marker(mark3_id, Some(12), Some(buf2_id))
        .expect("set mark3");
    let mark4 = interp.make_marker();
    let Value::Marker(mark4_id) = mark4 else {
        panic!("mark4 should be a marker");
    };
    interp
        .set_marker(mark4_id, Some(13), Some(buf2_id))
        .expect("set mark4");

    let proc1 = interp
        .create_process(None, None, vec![], None)
        .expect("create proc1");
    let proc2 = interp
        .create_process(None, None, vec![], None)
        .expect("create proc2");
    let uninterned_a = call(
        &mut interp,
        "make-symbol",
        &[Value::String("a".into())],
        &mut env,
    )
    .expect("make uninterned a");
    let uninterned_b = call(
        &mut interp,
        "make-symbol",
        &[Value::String("b".into())],
        &mut env,
    )
    .expect("make uninterned b");

    let parse = |text: &str| {
        Reader::new(text)
            .read()
            .expect("parse upstream case")
            .expect("parsed upstream case should contain a value")
    };
    let big = parse("23058430092136939510");
    let big_plus_one = parse("23058430092136939511");
    let neg_big = parse("-23058430092136939510");
    let neg_big_minus_one = parse("-23058430092136939511");
    let double_big = parse("46116860184273879020");
    let float_big =
        call(&mut interp, "float", std::slice::from_ref(&big), &mut env).expect("float big");
    let float_double_big = call(
        &mut interp,
        "float",
        std::slice::from_ref(&double_big),
        &mut env,
    )
    .expect("float double big");

    let cases = vec![
        ("number", parse("1"), parse("2")),
        ("number_neg_neg", parse("-2"), parse("-1")),
        ("number_neg_pos", parse("-2"), parse("1")),
        ("number_neg_pos_2", parse("-1"), parse("2")),
        ("bignum_inc", big.clone(), big_plus_one),
        ("bignum_neg_pos", neg_big_minus_one.clone(), big.clone()),
        ("bignum_neg_chain", neg_big_minus_one, neg_big.clone()),
        ("fixnum_bignum", parse("1"), big.clone()),
        ("fixnum_neg_bignum", parse("-1"), big.clone()),
        ("bignum_fixnum_neg", neg_big.clone(), parse("-1")),
        ("bignum_fixnum_pos", neg_big.clone(), parse("1")),
        ("float", parse("1.5"), parse("1.6")),
        ("float_neg_neg", parse("-1.3"), parse("-1.2")),
        ("float_neg_pos", parse("-13.0"), parse("12.0")),
        ("fixnum_float", parse("1"), parse("1.1")),
        ("float_fixnum", parse("1.9"), parse("2")),
        ("float_fixnum_neg_pos", parse("-2.0"), parse("1")),
        ("fixnum_float_neg_pos", parse("-2"), parse("1.0")),
        (
            "fixnum_float_unrepresentable_1",
            parse("72057594037927935"),
            parse("72057594037927936.0"),
        ),
        (
            "fixnum_float_unrepresentable_2",
            parse("72057594037927936.0"),
            parse("72057594037927937"),
        ),
        (
            "fixnum_float_unrepresentable_3",
            parse("-72057594037927936.0"),
            parse("-72057594037927935"),
        ),
        (
            "fixnum_float_unrepresentable_4",
            parse("-72057594037927937"),
            parse("-72057594037927936.0"),
        ),
        (
            "fixnum_float_unrepresentable_5",
            parse("2305843009213693951"),
            parse("2305843009213693952.0"),
        ),
        ("bignum_float", big.clone(), float_double_big),
        ("float_bignum", float_big, double_big),
        ("symbol", parse("a"), parse("b")),
        ("symbol_nil", parse("nil"), parse("nix")),
        ("symbol_prefix", parse("b"), parse("ba")),
        (
            "symbol_hash",
            Value::Symbol("##".into()),
            Value::Symbol("a".into()),
        ),
        ("symbol_case", parse("A"), parse("a")),
        (
            "symbol_uninterned",
            uninterned_a.clone(),
            uninterned_b.clone(),
        ),
        (
            "symbol_plain_uninterned",
            Value::Symbol("a".into()),
            uninterned_b.clone(),
        ),
        (
            "symbol_uninterned_plain",
            uninterned_a.clone(),
            Value::Symbol("b".into()),
        ),
        ("string", parse("\"a\""), parse("\"b\"")),
        ("string_empty", parse("\"\""), parse("\"a\"")),
        ("string_prefix", parse("\"b\""), parse("\"ba\"")),
        ("string_case", parse("\"A\""), parse("\"a\"")),
        ("string_abc_abd", parse("\"abc\""), parse("\"abd\"")),
        (
            "string_dotted_pair",
            parse("(\"\" . 2)"),
            parse("(\"a\" . 1)"),
        ),
        (
            "string_unicode_prefix",
            parse("(\"å\" . 2)"),
            parse("(\"åü\" . 1)"),
        ),
        (
            "string_unicode_suffix",
            parse("(\"a\" . 2)"),
            parse("(\"aå\" . 1)"),
        ),
        (
            "string_raw_byte_prefix",
            parse("(\"\\x80\" . 2)"),
            parse("(\"\\x80å\" . 1)"),
        ),
        ("list_lt", parse("(1 2 3)"), parse("(2 3 4)")),
        ("list_prefix", parse("(2)"), parse("(2 1)")),
        ("list_nil_prefix", parse("()"), parse("(0)")),
        ("list_same_head", parse("(1 2 3)"), parse("(1 3)")),
        ("list_same_head_longer", parse("(1 2 3)"), parse("(1 3 2)")),
        (
            "list_nested",
            parse("((b a) (c d) e)"),
            parse("((b a) (c d) f)"),
        ),
        (
            "list_nested_case",
            parse("((b a) (c D) e)"),
            parse("((b a) (c d) e)"),
        ),
        (
            "list_nested_empty",
            parse("((b a) (c d () x) e)"),
            parse("((b a) (c d (1) x) e)"),
        ),
        ("dotted_list", parse("(1 . 2)"), parse("(1 . 3)")),
        ("dotted_list_tail", parse("(1 2 . 3)"), parse("(1 2 . 4)")),
        ("vector_lt", parse("[1 2 3]"), parse("[2 3 4]")),
        ("vector_prefix", parse("[2]"), parse("[2 1]")),
        ("vector_empty_prefix", parse("[]"), parse("[0]")),
        ("vector_same_head", parse("[1 2 3]"), parse("[1 3]")),
        (
            "vector_same_head_longer",
            parse("[1 2 3]"),
            parse("[1 3 2]"),
        ),
        (
            "vector_nested",
            parse("[[b a] [c d] e]"),
            parse("[[b a] [c d] f]"),
        ),
        (
            "vector_nested_case",
            parse("[[b a] [c D] e]"),
            parse("[[b a] [c d] e]"),
        ),
        (
            "vector_nested_empty",
            parse("[[b a] [c d [] x] e]"),
            parse("[[b a] [c d [1] x] e]"),
        ),
        (
            "bool_vector_empty",
            make_bool_vector_value(&mut interp, []),
            make_bool_vector_value(&mut interp, [false]),
        ),
        (
            "bool_vector_false_true",
            make_bool_vector_value(&mut interp, [false]),
            make_bool_vector_value(&mut interp, [true]),
        ),
        (
            "bool_vector_bit_flip",
            make_bool_vector_value(&mut interp, [true, false, true, false]),
            make_bool_vector_value(&mut interp, [true, false, true, true]),
        ),
        (
            "bool_vector_prefix",
            make_bool_vector_value(&mut interp, [true, false, true]),
            make_bool_vector_value(&mut interp, [true, false, true, false]),
        ),
        ("record_type", parse("#s(a 2 3)"), parse("#s(b 3 4)")),
        ("record_prefix", parse("#s(b)"), parse("#s(b a)")),
        ("record_same_type", parse("#s(a 2 3)"), parse("#s(a 3)")),
        (
            "record_same_type_longer",
            parse("#s(a 2 3)"),
            parse("#s(a 3 2)"),
        ),
        (
            "record_nested",
            parse("#s(#s(b a) #s(c d) e)"),
            parse("#s(#s(b a) #s(c d) f)"),
        ),
        (
            "record_nested_case",
            parse("#s(#s(b a) #s(c D) e)"),
            parse("#s(#s(b a) #s(c d) e)"),
        ),
        (
            "record_nested_type",
            parse("#s(#s(b a) #s(c d #s(u) x) e)"),
            parse("#s(#s(b a) #s(c d #s(v) x) e)"),
        ),
        ("marker_same_buffer", mark1, mark2),
        ("marker_other_buffer", Value::Marker(mark1_id), mark3),
        (
            "marker_other_buffer_2",
            Value::Marker(mark1_id),
            Value::Marker(mark4_id),
        ),
        (
            "marker_other_buffer_3",
            Value::Marker(mark2_id),
            Value::Marker(mark3_id),
        ),
        (
            "marker_other_buffer_4",
            Value::Marker(mark2_id),
            Value::Marker(mark4_id),
        ),
        (
            "marker_same_buffer_2",
            Value::Marker(mark3_id),
            Value::Marker(mark4_id),
        ),
        ("live_buffers", buf1.clone(), buf2),
        (
            "dead_buffer_before_live",
            Value::Buffer(buf3_id, " *three*".into()),
            buf1.clone(),
        ),
        (
            "dead_buffer_before_live_2",
            Value::Buffer(buf3_id, " *three*".into()),
            Value::Buffer(buf2_id, " *two*".into()),
        ),
        ("dead_buffer_before_live_3", buf3, buf1),
        ("process", proc1, proc2),
    ];

    for (label, left, right) in cases {
        let forward = call(
            &mut interp,
            "value<",
            &[left.clone(), right.clone()],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{label}: forward value< errored: {error:?}"));
        assert_eq!(forward, Value::T, "{label}: expected left < right");

        let backward = call(
            &mut interp,
            "value<",
            &[right.clone(), left.clone()],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{label}: reverse value< errored: {error:?}"));
        assert_eq!(backward, Value::Nil, "{label}: expected right !< left");

        let vector_forward = call(
            &mut interp,
            "value<",
            &[
                Value::list([Value::symbol("vector"), left.clone(), Value::Integer(2)]),
                Value::list([Value::symbol("vector"), right.clone(), Value::Integer(1)]),
            ],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{label}: vector forward value< errored: {error:?}"));
        assert_eq!(
            vector_forward,
            Value::T,
            "{label}: expected (vector left 2) < (vector right 1)"
        );

        let vector_reverse = call(
            &mut interp,
            "value<",
            &[
                Value::list([Value::symbol("vector"), right.clone(), Value::Integer(1)]),
                Value::list([Value::symbol("vector"), left.clone(), Value::Integer(2)]),
            ],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{label}: vector reverse value< errored: {error:?}"));
        assert_eq!(
            vector_reverse,
            Value::Nil,
            "{label}: expected (vector right 1) !< (vector left 2)"
        );

        let same_left = call(
            &mut interp,
            "value<",
            &[
                Value::list([Value::symbol("vector"), left.clone(), Value::Integer(1)]),
                Value::list([Value::symbol("vector"), left.clone(), Value::Integer(2)]),
            ],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{label}: same-left vector value< errored: {error:?}"));
        assert_eq!(
            same_left,
            Value::T,
            "{label}: expected (vector left 1) < (vector left 2)"
        );

        let same_right = call(
            &mut interp,
            "value<",
            &[
                Value::list([Value::symbol("vector"), right.clone(), Value::Integer(1)]),
                Value::list([Value::symbol("vector"), right, Value::Integer(2)]),
            ],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{label}: same-right vector value< errored: {error:?}"));
        assert_eq!(
            same_right,
            Value::T,
            "{label}: expected (vector right 1) < (vector right 2)"
        );
    }
}

#[test]
fn debug_value_less_ordered_upstream_body() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        r#"
            (let* ((big (* 10 most-positive-fixnum))
                   (buf1 (get-buffer-create " *one*"))
                   (buf2 (get-buffer-create " *two*"))
                   (buf3 (get-buffer-create " *three*"))
                   (_ (progn (with-current-buffer buf1 (insert (make-string 20 ?a)))
                             (with-current-buffer buf2 (insert (make-string 20 ?b)))))
                   (mark1 (set-marker (make-marker) 12 buf1))
                   (mark2 (set-marker (make-marker) 13 buf1))
                   (mark3 (set-marker (make-marker) 12 buf2))
                   (mark4 (set-marker (make-marker) 13 buf2))
                   (proc1 (make-pipe-process :name " *proc one*"))
                   (proc2 (make-pipe-process :name " *proc two*")))
              (kill-buffer buf3)
              (unwind-protect
                  (catch 'fail
                    (let ((case-index -1))
                      (dolist (c
                               `(
                               (1 . 2)  (-2 . -1) (-2 . 1) (-1 . 2)
                               (,big . ,(1+ big)) (,(- big) . ,big)
                               (,(- -1 big) . ,(- big))
                               (1 . ,big) (-1 . ,big) (,(- big) . -1) (,(- big) . 1)
                               (1.5 . 1.6) (-1.3 . -1.2) (-13.0 . 12.0)
                               (1 . 1.1) (1.9 . 2) (-2.0 . 1) (-2 . 1.0)
                               (72057594037927935 . 72057594037927936.0)
                               (72057594037927936.0 . 72057594037927937)
                               (-72057594037927936.0 . -72057594037927935)
                               (-72057594037927937 . -72057594037927936.0)
                               (2305843009213693951 . 2305843009213693952.0)
                               (,big . ,(float (* 2 big))) (,(float big) . ,(* 2 big))
                               (a . b) (nil . nix) (b . ba) (## . a) (A . a)
                               (#:a . #:b) (a . #:b) (#:a . b)
                               ("" . "a") ("a" . "b") ("A" . "a") ("abc" . "abd")
                               ("b" . "ba")
                               (("" . 2) . ("a" . 1))
                               (("å" . 2) . ("åü" . 1))
                               (("a" . 2) . ("aå" . 1))
                               (("\x80" . 2) . ("\x80å" . 1))
                               ((1 2 3) . (2 3 4)) ((2) . (2 1)) (() . (0))
                               ((1 2 3) . (1 3)) ((1 2 3) . (1 3 2))
                               (((b a) (c d) e) . ((b a) (c d) f))
                               (((b a) (c D) e) . ((b a) (c d) e))
                               (((b a) (c d () x) e) . ((b a) (c d (1) x) e))
                               ((1 . 2) . (1 . 3)) ((1 2 . 3) . (1 2 . 4))
                               ([1 2 3] . [2 3 4]) ([2] . [2 1]) ([] . [0])
                               ([1 2 3] . [1 3]) ([1 2 3] . [1 3 2])
                               ([[b a] [c d] e] . [[b a] [c d] f])
                               ([[b a] [c D] e] . [[b a] [c d] e])
                               ([[b a] [c d [] x] e] . [[b a] [c d [1] x] e])
                               (,(bool-vector) . ,(bool-vector nil))
                               (,(bool-vector nil) . ,(bool-vector t))
                               (,(bool-vector t nil t nil) . ,(bool-vector t nil t t))
                               (,(bool-vector t nil t) . ,(bool-vector t nil t nil))
                               (#s(a 2 3) . #s(b 3 4)) (#s(b) . #s(b a))
                               (#s(a 2 3) . #s(a 3)) (#s(a 2 3) . #s(a 3 2))
                               (#s(#s(b a) #s(c d) e) . #s(#s(b a) #s(c d) f))
                               (#s(#s(b a) #s(c D) e) . #s(#s(b a) #s(c d) e))
                               (#s(#s(b a) #s(c d #s(u) x) e)
                                . #s(#s(b a) #s(c d #s(v) x) e))
                               (,mark1 . ,mark2) (,mark1 . ,mark3) (,mark1 . ,mark4)
                               (,mark2 . ,mark3) (,mark2 . ,mark4) (,mark3 . ,mark4)
                               (,buf1 . ,buf2) (,buf3 . ,buf1) (,buf3 . ,buf2)
                               (,proc1 . ,proc2)
                               ))
                        (setq case-index (1+ case-index))
                        (let ((x (car c))
                              (y (cdr c)))
                          (condition-case err
                              (progn
                                (unless (value< x y)
                                  (throw 'fail
                                         (list 'xy-false
                                               case-index
                                               (prin1-to-string c)
                                               (prin1-to-string x)
                                               (prin1-to-string y))))
                                (when (value< y x)
                                  (throw 'fail
                                         (list 'yx-true
                                               case-index
                                               (prin1-to-string c)
                                               (prin1-to-string x)
                                               (prin1-to-string y))))
                                (unless (value< (vector x 2) (vector y 1))
                                  (throw 'fail
                                         (list 'vec-forward
                                               case-index
                                               (prin1-to-string c)
                                               (prin1-to-string x)
                                               (prin1-to-string y))))
                                (when (value< (vector y 1) (vector x 2))
                                  (throw 'fail
                                         (list 'vec-reverse
                                               case-index
                                               (prin1-to-string c)
                                               (prin1-to-string x)
                                               (prin1-to-string y))))
                                (unless (value< (vector x 1) (vector x 2))
                                  (throw 'fail
                                         (list 'same-left
                                               case-index
                                               (prin1-to-string c)
                                               (prin1-to-string x)
                                               (prin1-to-string y))))
                                (unless (value< (vector y 1) (vector y 2))
                                  (throw 'fail
                                         (list 'same-right
                                               case-index
                                               (prin1-to-string c)
                                               (prin1-to-string x)
                                               (prin1-to-string y)))))
                            (error (throw 'fail
                                          (list 'error
                                                case-index
                                                (prin1-to-string c)
                                                (prin1-to-string x)
                                                (prin1-to-string y)
                                                (prin1-to-string err)))))))))))
                (ignore-errors (delete-process proc2))
                (ignore-errors (delete-process proc1))
                (ignore-errors (kill-buffer buf2))
                (ignore-errors (kill-buffer buf1)))))
            "#,
    )
    .read()
    .expect("parse ordered diagnostic form")
    .expect("ordered diagnostic form");

    let result = interp
        .eval(&form, &mut env)
        .expect("evaluate ordered diagnostic form");
    assert_eq!(
        result,
        Value::Nil,
        "unexpected ordered diagnostic result: {result:?}"
    );
}

#[test]
fn value_less_selected_upstream_unordered_cases_match_emacs() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let uninterned_a = call(
        &mut interp,
        "make-symbol",
        &[Value::String("a".into())],
        &mut env,
    )
    .expect("make uninterned a");
    let dead_buf1 = {
        let (id, name) = interp.create_buffer(" *dead-one*");
        let buffer = Value::Buffer(id, name);
        interp.kill_buffer_id(id);
        buffer
    };
    let dead_buf2 = {
        let (id, name) = interp.create_buffer(" *dead-two*");
        let buffer = Value::Buffer(id, name);
        interp.kill_buffer_id(id);
        buffer
    };
    let hash1 = call(&mut interp, "make-hash-table", &[], &mut env).expect("hash1");
    let hash2 = call(&mut interp, "make-hash-table", &[], &mut env).expect("hash2");
    let obarray1 = call(&mut interp, "obarray-make", &[], &mut env).expect("obarray1");
    let obarray2 = call(&mut interp, "obarray-make", &[], &mut env).expect("obarray2");

    let cases = vec![
        ("zero_float", Value::Integer(0), Value::Float(0.0)),
        ("zero_neg_zero", Value::Integer(0), Value::Float(-0.0)),
        ("float_neg_zero", Value::Float(0.0), Value::Float(-0.0)),
        (
            "large_int_float_equal",
            Value::BigInteger(BigInt::from(72057594037927936_i128)),
            Value::Float(72057594037927936.0),
        ),
        ("nan", Value::Integer(1), Value::Float(f64::NAN)),
        (
            "symbol_plain_uninterned_same_visible",
            Value::Symbol("a".into()),
            uninterned_a,
        ),
        ("dead_buffers", dead_buf1, dead_buf2),
        ("hash_tables", hash1, hash2),
        ("obarrays", obarray1, obarray2),
    ];

    for (label, left, right) in cases {
        let forward = call(
            &mut interp,
            "value<",
            &[left.clone(), right.clone()],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{label}: forward value< errored: {error:?}"));
        assert_eq!(forward, Value::Nil, "{label}: expected left !< right");

        let backward = call(
            &mut interp,
            "value<",
            &[right.clone(), left.clone()],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{label}: reverse value< errored: {error:?}"));
        assert_eq!(backward, Value::Nil, "{label}: expected right !< left");

        let forward_cons = call(
            &mut interp,
            "value<",
            &[
                Value::cons(left.clone(), Value::Integer(1)),
                Value::cons(right.clone(), Value::Integer(2)),
            ],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{label}: cons forward value< errored: {error:?}"));
        assert_eq!(forward_cons, Value::T, "{label}: expected cons tiebreak");

        let backward_cons = call(
            &mut interp,
            "value<",
            &[
                Value::cons(left, Value::Integer(2)),
                Value::cons(right, Value::Integer(1)),
            ],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{label}: cons reverse value< errored: {error:?}"));
        assert_eq!(
            backward_cons,
            Value::Nil,
            "{label}: expected reverse cons !<"
        );
    }
}

#[test]
fn value_less_selected_upstream_type_mismatch_cases_match_emacs() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let char_table = call(
        &mut interp,
        "make-char-table",
        &[Value::Symbol("test".into())],
        &mut env,
    )
    .expect("make char-table");
    let hash_table = call(&mut interp, "make-hash-table", &[], &mut env).expect("hash-table");
    let obarray = call(&mut interp, "obarray-make", &[], &mut env).expect("obarray");
    let values = vec![
        ("number", Value::Integer(1)),
        ("symbol", Value::Symbol("a".into())),
        ("string", Value::String("a".into())),
        (
            "list",
            Value::list([Value::Symbol("a".into()), Value::Symbol("b".into())]),
        ),
        (
            "vector",
            call(
                &mut interp,
                "vector",
                &[Value::Symbol("a".into()), Value::Symbol("b".into())],
                &mut env,
            )
            .expect("vector"),
        ),
        (
            "bool_vector",
            make_bool_vector_value(&mut interp, [false, true]),
        ),
        (
            "record",
            interp.create_record("a", vec![Value::Symbol("b".into())]),
        ),
        ("char_table", char_table),
        ("hash_table", hash_table),
        ("obarray", obarray),
    ];

    let is_type_mismatch = |result: &Result<Value, LispError>| {
        matches!(
            result,
            Err(LispError::SignalValue(signal))
                if signal
                    .car()
                    .ok()
                    .is_some_and(|head| head == Value::Symbol("type-mismatch".into()))
        )
    };

    for index in 0..values.len() {
        for other in index + 1..values.len() {
            let (left_label, left) = &values[index];
            let (right_label, right) = &values[other];
            let label = format!("{left_label}_vs_{right_label}");
            let forward = call(
                &mut interp,
                "value<",
                &[left.clone(), right.clone()],
                &mut env,
            );
            assert!(
                is_type_mismatch(&forward),
                "{label}: expected forward type-mismatch, got {forward:?}"
            );
            let backward = call(
                &mut interp,
                "value<",
                &[right.clone(), left.clone()],
                &mut env,
            );
            assert!(
                is_type_mismatch(&backward),
                "{label}: expected reverse type-mismatch, got {backward:?}"
            );
        }
    }
}

#[test]
fn debug_value_less_type_mismatch_upstream_body() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        r#"
            (let ((incomparable
                   `( 1 a "a" (a b) [a b] ,(bool-vector nil t) #s(a b)
                      ,(make-char-table 'test)
                      ,(make-hash-table)
                      ,(obarray-make))))
              (catch 'fail
                (let ((tail incomparable))
                  (while tail
                    (let ((x (car tail)))
                      (dolist (y (cdr tail))
                        (condition-case _
                            (when (value< x y)
                              (throw 'fail (list 'xy x y)))
                          (type-mismatch nil)
                          (error (throw 'fail (list 'xy-wrong-error x y))))
                        (condition-case _
                            (when (value< y x)
                              (throw 'fail (list 'yx x y)))
                          (type-mismatch nil)
                          (error (throw 'fail (list 'yx-wrong-error x y)))))
                      (setq tail (cdr tail))))
                  nil)))
            "#,
    )
    .read()
    .expect("parse diagnostic form")
    .expect("diagnostic form");

    let result = interp
        .eval(&form, &mut env)
        .expect("evaluate diagnostic form");
    assert_eq!(
        result,
        Value::Nil,
        "unexpected type-mismatch diagnostic result: {result:?}"
    );
}

#[test]
fn debug_value_less_unordered_upstream_body() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        r#"
            (let ((buf1 (get-buffer-create " *one*"))
                  (buf2 (get-buffer-create " *two*")))
              (kill-buffer buf2)
              (kill-buffer buf1)
              (catch 'fail
                (dolist (c `(
                             (0 . 0.0) (0 . -0.0) (0.0 . -0.0)
                             (72057594037927936 . 72057594037927936.0)
                             (1 . 0.0e+NaN)
                             (a . #:a)
                             (,buf1 . ,buf2)
                             (,(make-hash-table) . ,(make-hash-table))
                             (,(obarray-make) . ,(obarray-make))
                             ))
                  (let ((x (car c))
                        (y (cdr c)))
                    (when (value< x y)
                      (throw 'fail (list 'xy x y)))
                    (when (value< y x)
                      (throw 'fail (list 'yx x y)))
                    (unless (value< (cons x 1) (cons y 2))
                      (throw 'fail (list 'cons-forward x y)))
                    (when (value< (cons x 2) (cons y 1))
                      (throw 'fail (list 'cons-reverse x y)))))))
            "#,
    )
    .read()
    .expect("parse unordered diagnostic form")
    .expect("unordered diagnostic form");

    let result = interp
        .eval(&form, &mut env)
        .expect("evaluate unordered diagnostic form");
    assert_eq!(
        result,
        Value::Nil,
        "unexpected unordered diagnostic result: {result:?}"
    );
}

#[test]
fn eq_uses_identity_for_copied_sequences() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let list = Value::list([Value::Integer(1), Value::Integer(2)]);
    let copied_list = call(
        &mut interp,
        "copy-sequence",
        std::slice::from_ref(&list),
        &mut env,
    )
    .expect("copy list");
    assert_eq!(
        call(&mut interp, "eq", &[list, copied_list], &mut env).expect("eq copied list"),
        Value::Nil
    );

    let vector = call(
        &mut interp,
        "vector",
        &[Value::Integer(1), Value::Integer(2)],
        &mut env,
    )
    .expect("create vector");
    let copied_vector = call(
        &mut interp,
        "copy-sequence",
        std::slice::from_ref(&vector),
        &mut env,
    )
    .expect("copy vector");
    assert_eq!(
        call(&mut interp, "eq", &[vector, copied_vector], &mut env).expect("eq copied vector"),
        Value::Nil
    );
}

#[test]
fn eq_and_equal_match_emacs_for_symbols_with_position() {
    let mut interp = Interpreter::new();
    let foo1 = call(
        &mut interp,
        "position-symbol",
        &[Value::Symbol("foo".into()), Value::Integer(42)],
        &mut Vec::new(),
    )
    .expect("foo1");
    let foo2 = call(
        &mut interp,
        "position-symbol",
        &[Value::Symbol("foo".into()), Value::Integer(666)],
        &mut Vec::new(),
    )
    .expect("foo2");
    let foo3 = call(
        &mut interp,
        "position-symbol",
        &[Value::Symbol("foo".into()), Value::Integer(42)],
        &mut Vec::new(),
    )
    .expect("foo3");
    let plain = Value::Symbol("foo".into());

    let mut disabled_env = vec![vec![("symbols-with-pos-enabled".into(), Value::Nil)]];
    assert_eq!(
        call(
            &mut interp,
            "eq",
            &[foo1.clone(), foo1.clone()],
            &mut disabled_env
        )
        .expect("disabled eq same"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "equal",
            &[foo1.clone(), foo1.clone()],
            &mut disabled_env
        )
        .expect("disabled equal same"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "eq",
            &[foo1.clone(), foo2.clone()],
            &mut disabled_env
        )
        .expect("disabled eq different pos"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "equal",
            &[foo1.clone(), foo2.clone()],
            &mut disabled_env
        )
        .expect("disabled equal different pos"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "eq",
            &[foo1.clone(), foo3.clone()],
            &mut disabled_env
        )
        .expect("disabled eq same pos"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "equal",
            &[foo1.clone(), foo3.clone()],
            &mut disabled_env
        )
        .expect("disabled equal same pos"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "eq",
            &[foo1.clone(), plain.clone()],
            &mut disabled_env
        )
        .expect("disabled eq plain"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "equal",
            &[foo1.clone(), plain.clone()],
            &mut disabled_env
        )
        .expect("disabled equal plain"),
        Value::Nil
    );

    let mut enabled_env = vec![vec![("symbols-with-pos-enabled".into(), Value::T)]];
    assert_eq!(
        call(
            &mut interp,
            "eq",
            &[foo1.clone(), foo2.clone()],
            &mut enabled_env
        )
        .expect("enabled eq different pos"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "equal",
            &[foo1.clone(), foo2.clone()],
            &mut enabled_env
        )
        .expect("enabled equal different pos"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "eq",
            &[foo1.clone(), foo3.clone()],
            &mut enabled_env
        )
        .expect("enabled eq same pos"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "equal",
            &[foo1.clone(), foo3.clone()],
            &mut enabled_env
        )
        .expect("enabled equal same pos"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "eq",
            &[foo1.clone(), plain.clone()],
            &mut enabled_env
        )
        .expect("enabled eq plain"),
        Value::T
    );
    assert_eq!(
        call(&mut interp, "equal", &[foo1, plain], &mut enabled_env).expect("enabled equal plain"),
        Value::T
    );
}
