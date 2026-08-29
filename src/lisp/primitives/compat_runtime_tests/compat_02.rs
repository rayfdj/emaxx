use super::*;

// `coding-system-get' is mule.el Lisp; the bare host reads the C-owned
// coding-system plist directly (mule.el's accessor is plist-get over it).
fn make_record(interp: &mut Interpreter, env: &mut Env, slots: &[Value]) -> Value {
    call(interp, "record", slots, env).expect("record construction")
}

fn coding_plist_property(
    interp: &mut Interpreter,
    env: &mut Env,
    coding_system: &str,
    property: &str,
) -> Value {
    let plist = call(
        interp,
        "coding-system-plist",
        &[Value::Symbol(coding_system.into())],
        env,
    )
    .unwrap_or_else(|error| panic!("coding-system-plist {coding_system}: {error}"));
    call(
        interp,
        "plist-get",
        &[plist, Value::Symbol(property.into())],
        env,
    )
    .unwrap_or_else(|error| panic!("plist-get {property}: {error}"))
}

#[test]
fn coding_system_get_reports_for_unibyte_for_raw_text() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();

    assert_eq!(
        coding_plist_property(&mut interp, &mut env, "raw-text", ":for-unibyte"),
        Value::T
    );
    assert_eq!(
        coding_plist_property(&mut interp, &mut env, "utf-8", ":for-unibyte"),
        Value::Nil
    );
}

#[test]
fn define_coding_system_internal_derives_public_utf8_attributes_like_coding_c() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    call(
        &mut interp,
        "define-coding-system-internal",
        &[
            Value::Symbol("sample-utf-8".into()),
            Value::Integer('U' as i64),
            Value::Symbol("utf-8".into()),
            Value::list([Value::Symbol("unicode".into())]),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::list([
                Value::Symbol(":name".into()),
                Value::Symbol("sample-utf-8".into()),
            ]),
            Value::Nil,
            Value::Nil,
        ],
        &mut env,
    )
    .expect("define a no-signature UTF-8 coding system");

    for (property, expected) in [
        (":ascii-compatible-p", Value::T),
        (":category", Value::Symbol("coding-category-utf-8".into())),
    ] {
        assert_eq!(
            coding_plist_property(&mut interp, &mut env, "sample-utf-8", property),
            expected,
        );
    }
}

#[test]
fn preloaded_latin_charset_coding_preserves_ascii_and_non_ascii_bytes() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    call(
        &mut interp,
        "define-coding-system-internal",
        &[
            Value::Symbol("sample-latin-charset".into()),
            Value::Integer('C' as i64),
            Value::Symbol("charset".into()),
            Value::list([Value::Symbol("iso-8859-1".into())]),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::list([
                Value::Symbol(":charset-list".into()),
                Value::list([Value::Symbol("iso-8859-1".into())]),
            ]),
            Value::Nil,
        ],
        &mut env,
    )
    .expect("define Latin-1 charset coding");

    let encoded = call(
        &mut interp,
        "encode-coding-string",
        &[
            Value::String("foó".into()),
            Value::Symbol("sample-latin-charset".into()),
        ],
        &mut env,
    )
    .expect("encode Latin-1 through the preloaded direct charset mapping");
    assert_eq!(
        encode_raw_text_bytes(
            &string_text(&encoded).expect("encoded coding string should be string-like"),
        )
        .expect("encoded raw text should convert back to bytes"),
        b"fo\xf3"
    );

    let decoded = call(
        &mut interp,
        "decode-coding-string",
        &[encoded, Value::Symbol("sample-latin-charset".into())],
        &mut env,
    )
    .expect("decode Latin-1 through the preloaded direct charset mapping");
    assert_eq!(
        string_text(&decoded).expect("decoded coding string should be string-like"),
        "foó"
    );
}

#[test]
fn decode_coding_region_inserts_into_destination_buffer() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
                Value::buffer(buffer_id, buffer_name),
            ],
            &mut env,
        )
        .is_ok()
    );
    let dest = interp.get_buffer_by_id(buffer_id).expect("dest buffer");
    assert_eq!(dest.buffer_string(), "abc");
}

#[test]
fn decode_coding_region_reports_the_detected_eol_variant() {
    for (line_ending, expected) in [
        (b"\n".as_slice(), "iso-2022-7bit-unix"),
        (b"\r\n".as_slice(), "iso-2022-7bit-dos"),
        (b"\r".as_slice(), "iso-2022-7bit-mac"),
    ] {
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = Vec::new();
        let mut bytes = vec![0x1b, b'$', b'B', b'$', b'"', 0x1b, b'(', b'B'];
        bytes.extend_from_slice(line_ending);
        interp.buffer =
            crate::buffer::Buffer::from_text("*encoded*", &decode_raw_text_bytes(&bytes));
        interp.buffer.set_multibyte(false);
        let end = interp.buffer.point_max();

        call(
            &mut interp,
            "decode-coding-region",
            &[
                Value::Integer(1),
                Value::Integer(end as i64),
                Value::Symbol("iso-2022-7bit".into()),
            ],
            &mut env,
        )
        .expect("decode ISO-2022 text");

        assert_eq!(
            interp.lookup_var("last-coding-system-used", &env),
            Some(Value::Symbol(expected.into()))
        );
    }
}

#[test]
fn funcall_message_builtin_from_lambda() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (defalias 'emaxx-test-call-message
                #'(lambda (orig &rest args)
                    (setq emaxx-test-message-log (apply #'format-message args))
                    (funcall orig "%s" emaxx-test-message-log)))
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
              (defalias 'emaxx-test-format-message
                #'(lambda (&rest args) (apply #'format-message args)))
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
fn advice_add_supports_after_function() {
    run_with_large_stack(advice_add_supports_after_function_inner);
}

fn advice_add_supports_after_function_inner() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (unless (and (not (subrp (symbol-function 'advice-add)))
                           (string-match-p "nadvice\\.el"
                                           (symbol-file 'advice-add 'defun))
                           (macrop 'add-function)
                           (string-match-p "nadvice\\.el"
                                           (symbol-file 'add-function 'defun))
                           (macrop 'define-advice))
                (error "advice owners did not resolve to GNU nadvice.el"))
              (unless (equal
                       '(car cdr how props)
                       (mapcar #'cl--slot-descriptor-name
                               (oclosure--class-slots
                                (get 'advice 'cl--class))))
                (error "advice OClosure slot layout differs from GNU"))
              (setq emaxx-test-after-log nil)
              (defun emaxx-test-after-target () 'done)
              (defun emaxx-test-after-advice (&rest _args)
                (setq emaxx-test-after-log 'after))
              (advice-add 'emaxx-test-after-target :after #'emaxx-test-after-advice)
              (let ((advice-result
                     (prog1
                         (list (emaxx-test-after-target)
                               emaxx-test-after-log)
                       (advice-remove
                        'emaxx-test-after-target
                        #'emaxx-test-after-advice))))
                (defun emaxx-test-function-base (value)
                  (list 'base value))
                (defun emaxx-test-function-around (original value)
                  (list 'wrapped (funcall original value)))
                (setq emaxx-test-function-value
                      #'emaxx-test-function-base)
                (add-function :around emaxx-test-function-value
                              #'emaxx-test-function-around)
                (setq emaxx-test-add-function-result
                      (funcall emaxx-test-function-value 7))
                (remove-function emaxx-test-function-value
                                 #'emaxx-test-function-around)
                (setq emaxx-test-nil-function-value nil)
                (remove-function emaxx-test-nil-function-value #'ignore)
                (setq-default emaxx-test-local-function-value
                              #'emaxx-test-function-base)
                (add-function :around
                              (local emaxx-test-local-function-value)
                              #'emaxx-test-function-around)
                (setq emaxx-test-local-function-result
                      (funcall emaxx-test-local-function-value 10))
                (remove-function (local emaxx-test-local-function-value)
                                 #'emaxx-test-function-around)
                (setq emaxx-test-nested-lexical-add-function-result
                      (eval
                       '(let ((target
                               (lambda (value) (list 'base value))))
                          (mapc
                           (lambda (tag)
                             (let ((captured-tag nil))
                               (let ((layer
                                      (lambda (oldfun value)
                                        (cons captured-tag
                                              (funcall oldfun value)))))
                                 (setq captured-tag tag)
                                 (add-function :around (var target) layer))))
                           '(outer inner))
                          (funcall target 7))
                       t))
                (setq emaxx-test-nested-dynamic-add-function-result
                      (eval
                       '(progn
                          (defvar emaxx-test-dynamic-function nil)
                          (let ((emaxx-test-dynamic-function
                                 (lambda (value) (list 'base value))))
                            (add-function
                             :around (var emaxx-test-dynamic-function)
                             (lambda (oldfun value)
                               (cons 'outer (funcall oldfun value))))
                            (add-function
                             :around (var emaxx-test-dynamic-function)
                             (lambda (oldfun value)
                               (cons 'inner (funcall oldfun value))))
                            (funcall emaxx-test-dynamic-function 7)))
                       t))
                (defun emaxx-test-buffer-size-around (original)
                  (list 'builtin (funcall original)))
                (advice-add 'buffer-size :around
                            #'emaxx-test-buffer-size-around)
                (setq emaxx-test-builtin-advice-result
                      (with-temp-buffer
                        (insert "abc")
                        (buffer-size)))
                (advice-remove 'buffer-size
                               #'emaxx-test-buffer-size-around)
                (defun emaxx-test-read-event-advice
                    (_original &rest _args)
                  ?x)
                (advice-add 'read-event :around
                            #'emaxx-test-read-event-advice)
                (setq emaxx-test-read-event-advice-result
                      (let ((last-input-event nil)
                            (unread-command-events nil))
                        (list (read-event)
                              last-input-event
                              unread-command-events)))
                (advice-remove 'read-event
                               #'emaxx-test-read-event-advice)
                (defun emaxx-test-forward-target () 'original)
                (advice-add 'emaxx-test-forward-target :around
                            #'emaxx-test-forward-advice)
                (defun emaxx-test-forward-advice (original &rest args)
                  (list 'forward (apply original args)))
                (setq emaxx-test-forward-advice-result
                      (emaxx-test-forward-target))
                (advice-remove 'emaxx-test-forward-target
                               #'emaxx-test-forward-advice)
                (defun emaxx-test-define-advice-target () 'base)
                (define-advice emaxx-test-define-advice-target
                    (:around (original &rest args) named)
                  (cons (apply original args) 'defined))
                (setq emaxx-test-define-advice-result
                      (emaxx-test-define-advice-target))
                (advice-remove 'emaxx-test-define-advice-target 'named)
                (cl-defgeneric emaxx-test-advised-generic (x y))
                (cl-defmethod emaxx-test-advised-generic (x y)
                  (list x y))
                (defun emaxx-test-generic-advice (&rest args)
                  (cons 'advice (apply args)))
                (advice-add 'emaxx-test-advised-generic :around
                            #'emaxx-test-generic-advice)
                (setq emaxx-test-generic-before
                      (emaxx-test-advised-generic 4 5))
                (cl-defmethod emaxx-test-advised-generic
                  ((_x integer) _y)
                  (cons 'integer (cl-call-next-method)))
                (setq emaxx-test-generic-during
                      (emaxx-test-advised-generic 4 5))
                (advice-remove 'emaxx-test-advised-generic
                               #'emaxx-test-generic-advice)
                (setq emaxx-test-generic-after
                      (emaxx-test-advised-generic 4 5))
                (let ((seen nil)
                      (capture
                       (lambda (original &rest args)
                         (setq seen (apply #'format args))
                         (apply original args))))
                  (unwind-protect
                      (progn
                        (advice-add 'message :around capture)
                        (execute-kbd-macro (kbd "C-c C-z"))
                        (setq emaxx-test-message-advice-result seen))
                    (advice-remove 'message capture)))
                (setq emaxx-test-kmacro-after-log nil)
                (defun emaxx-test-end-kbd-macro-advice (&rest _args)
                  (setq emaxx-test-kmacro-after-log 'after))
                (advice-add 'end-kbd-macro :after
                            #'emaxx-test-end-kbd-macro-advice)
                (kmacro-start-macro nil)
                (setq emaxx-test-kmacro-advice-result
                      (list (end-kbd-macro)
                            emaxx-test-kmacro-after-log))
                (advice-remove 'end-kbd-macro
                               #'emaxx-test-end-kbd-macro-advice)
                (oclosure-define
                    (emaxx-test-mutable-cell
                     (:predicate emaxx-test-mutable-cell-p))
                  (value :mutable t))
                (cl-defgeneric emaxx-test-oclosure-dispatch (object))
                (cl-defmethod emaxx-test-oclosure-dispatch
                  ((object emaxx-test-mutable-cell)) 'specific)
                (cl-defmethod emaxx-test-oclosure-dispatch
                  ((object interpreted-function)) 'representation)
                (let ((object
                       (oclosure-lambda
                           (emaxx-test-mutable-cell (value 1)) ()
                         value)))
                  (let ((environment (aref object 2)))
                    (list advice-result
                          emaxx-test-add-function-result
                          (funcall emaxx-test-function-value 9)
                          emaxx-test-nil-function-value
                          emaxx-test-local-function-result
                          emaxx-test-nested-lexical-add-function-result
                          emaxx-test-nested-dynamic-add-function-result
                          emaxx-test-builtin-advice-result
                          emaxx-test-read-event-advice-result
                          emaxx-test-forward-advice-result
                          emaxx-test-define-advice-result
                          emaxx-test-generic-before
                          emaxx-test-generic-during
                          emaxx-test-generic-after
                          emaxx-test-message-advice-result
                          emaxx-test-kmacro-advice-result
                          (eq environment (aref object 2))
                          (emaxx-test-mutable-cell--value object)
                          (progn
                            (setf (emaxx-test-mutable-cell--value object) 2)
                            (emaxx-test-mutable-cell--value object))
                          (funcall object)
                          (emaxx-test-oclosure-dispatch object)
                          (cdr (car environment)))))))
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
        Value::list([
            Value::list([Value::Symbol("done".into()), Value::Symbol("after".into())]),
            Value::list([
                Value::Symbol("wrapped".into()),
                Value::list([Value::Symbol("base".into()), Value::Integer(7)]),
            ]),
            Value::list([Value::Symbol("base".into()), Value::Integer(9)]),
            Value::Nil,
            // GNU's `(local VAR)' proxy does not manufacture a local cell
            // when VAR currently has only a default function value.
            Value::list([Value::Symbol("base".into()), Value::Integer(10)]),
            Value::list([
                Value::Symbol("inner".into()),
                Value::Symbol("outer".into()),
                Value::Symbol("base".into()),
                Value::Integer(7),
            ]),
            Value::list([
                Value::Symbol("inner".into()),
                Value::Symbol("outer".into()),
                Value::Symbol("base".into()),
                Value::Integer(7),
            ]),
            Value::list([Value::Symbol("builtin".into()), Value::Integer(3)]),
            Value::list([Value::Integer('x' as i64), Value::Nil, Value::Nil]),
            Value::list([
                Value::Symbol("forward".into()),
                Value::Symbol("original".into()),
            ]),
            Value::cons(
                Value::Symbol("base".into()),
                Value::Symbol("defined".into()),
            ),
            Value::list([
                Value::Symbol("advice".into()),
                Value::Integer(4),
                Value::Integer(5),
            ]),
            Value::list([
                Value::Symbol("advice".into()),
                Value::Symbol("integer".into()),
                Value::Integer(4),
                Value::Integer(5),
            ]),
            Value::list([
                Value::Symbol("integer".into()),
                Value::Integer(4),
                Value::Integer(5),
            ]),
            // GNU's command-loop undefined-key diagnostic bypasses Lisp
            // `message' advice as well.
            Value::Nil,
            Value::list([Value::Nil, Value::Symbol("after".into())]),
            Value::T,
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(2),
            Value::Symbol("specific".into()),
            Value::Integer(2),
        ])
    );
}

#[test]
fn make_temp_file_creates_a_file_for_relative_prefix() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();

    let path = crate::test_support::call_lisp_function(
        &mut interp,
        &mut env,
        "make-temp-file",
        &[Value::String("emaxx-compat-".into())],
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();

    assert_eq!(
        crate::test_support::call_lisp_function(
            &mut interp,
            &mut env,
            "file-name-base",
            &[Value::String("/tmp/demo.tar.gz".into())],
        )
        .expect("base name"),
        Value::String("demo.tar".into())
    );
    assert_eq!(
        crate::test_support::call_lisp_function(
            &mut interp,
            &mut env,
            "file-name-sans-extension",
            &[Value::String("demo.tar.gz".into())],
        )
        .expect("strip suffix"),
        Value::String("demo.tar".into())
    );
    assert_eq!(
        crate::test_support::call_lisp_function(
            &mut interp,
            &mut env,
            "file-name-extension",
            &[Value::String("demo.tar.gz".into()), Value::T],
        )
        .expect("extension with period"),
        Value::String(".gz".into())
    );
}

#[test]
fn rename_visited_file_moves_disk_file_and_updates_buffer_path() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let old_path = make_compat_temp_file(&mut interp, &mut env, "emaxx-rename-visited-file-");
    let new_path = format!("{old_path}.zip");

    interp.buffer.file = Some(old_path.clone());
    interp.buffer.file_truename = Some(old_path.clone());
    interp
        .buffer
        .set_visited_file_modtime(file_modtime(&old_path).expect("source modtime"));

    crate::test_support::call_lisp_function(
        &mut interp,
        &mut env,
        "rename-visited-file",
        &[Value::String(new_path.clone().into())],
    )
    .expect("rename visited file");

    assert!(!Path::new(&old_path).exists());
    assert!(Path::new(&new_path).exists());
    assert_eq!(interp.buffer.file.as_deref(), Some(new_path.as_str()));
    let canonical_new_path = canonical_file_name(&new_path);
    assert_eq!(
        interp.buffer.file_truename.as_deref(),
        Some(canonical_new_path.as_str())
    );
    assert!(interp.buffer.visited_file_modtime().is_some());

    std::fs::remove_file(new_path).expect("cleanup renamed file");
}

#[test]
fn revert_buffer_reloads_non_utf8_file_as_raw_text() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let path = make_compat_temp_file(&mut interp, &mut env, "emaxx-revert-raw-buffer-");
    let bytes = [0xFF, b'a'];
    std::fs::write(&path, bytes).expect("write raw bytes");

    interp.buffer = crate::buffer::Buffer::from_text("*raw*", "");
    interp.buffer.file = Some(path.clone());
    interp.buffer.file_truename = Some(path.clone());
    interp.buffer.set_multibyte(false);
    interp
        .buffer
        .set_visited_file_modtime(file_modtime(&path).expect("source modtime"));

    // A bare revert-buffer call with no NOCONFIRM asks "(yes or no)" and,
    // in batch, signals end-of-file at EOF -- probed identical on both
    // binaries.  NOCONFIRM exercises the same revert machinery silently.
    crate::test_support::eval_lisp(&mut interp, &mut env, "(revert-buffer nil t)")
        .expect("revert raw buffer");

    assert_eq!(interp.buffer.buffer_string(), decode_raw_text_bytes(&bytes));
    assert!(!interp.buffer.is_multibyte());

    std::fs::remove_file(path).expect("cleanup raw file");
}

#[test]
fn save_buffer_skips_unmodified_and_unchanged_files() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let path = make_compat_temp_file(&mut interp, &mut env, "emaxx-save-unmodified-");
    std::fs::write(&path, "fresh").expect("write source file");

    interp.buffer = crate::buffer::Buffer::from_text("*save*", "fresh");
    interp.buffer.file = Some(path.clone());
    interp.buffer.file_truename = Some(path.clone());
    interp.buffer.set_unmodified();

    let original_permissions = std::fs::metadata(&path).expect("metadata").permissions();
    let mut permissions = original_permissions.clone();
    permissions.set_readonly(true);
    std::fs::set_permissions(&path, permissions).expect("make file read-only");

    crate::test_support::eval_lisp(&mut interp, &mut env, "(save-buffer)")
        .expect("unmodified save is a no-op");
    interp.buffer.set_modified();
    // GNU does not skip the write for a modified buffer whose text happens
    // to equal the file: it reaches the write path, finds the file
    // write-protected, asks "try to save anyway? (yes or no)" and -- in
    // batch -- signals end-of-file at EOF.  Probed identical on both
    // binaries; the old expectation of silent success pinned the auto-t
    // prompt fabrication this round removed (finding 11).
    let error = crate::test_support::eval_lisp(&mut interp, &mut env, "(save-buffer)")
        .expect_err("write-protected save prompts and, in batch, signals");
    assert!(
        format!("{error:?}").contains("end-of-file"),
        "expected the batch prompt EOF signal, got {error:?}"
    );

    std::fs::set_permissions(&path, original_permissions).expect("restore writable file");
    std::fs::remove_file(path).expect("cleanup unmodified save file");
}

#[test]
fn write_region_checks_supersession_when_lockfile_creation_is_disabled() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let path = make_compat_temp_file(&mut interp, &mut env, "emaxx-lock-supersession-");
    std::fs::write(&path, "visited bytes\n").expect("write initial visited file");
    std::fs::File::open(&path)
        .expect("open initial visited file")
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(100_000)),
        )
        .expect("set initial visited timestamp");

    interp.buffer = crate::buffer::Buffer::from_text("*lock-supersession*", "local bytes\n");
    interp.buffer.file = Some(path.clone());
    interp.buffer.file_truename = Some(canonical_file_name(&path));
    interp
        .buffer
        .set_visited_file_modtime(file_modtime(&path).expect("initial visited modtime"));

    std::fs::write(&path, "external bytes\n").expect("replace visited file externally");
    std::fs::File::open(&path)
        .expect("open externally replaced file")
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(200_000)),
        )
        .expect("set external replacement timestamp");
    crate::test_support::eval_lisp(
        &mut interp,
        &mut env,
        "(progn
           (setq create-lockfiles nil emaxx-test-supersession-file nil)
           (defun userlock--ask-user-about-supersession-threat (filename)
             (setq emaxx-test-supersession-file filename)))",
    )
    .expect("install supersession observer");

    call(
        &mut interp,
        "write-region",
        &[
            Value::Nil,
            Value::Nil,
            Value::String(path.clone().into()),
            Value::Nil,
            Value::T,
        ],
        &mut env,
    )
    .expect("write-region supersession check");
    let observed = interp
        .lookup_var("emaxx-test-supersession-file", &env)
        .expect("supersession observer records the visited file");
    assert_eq!(
        string_text(&observed).expect("supersession observer receives a file name"),
        canonical_file_name(&path)
    );
    assert_eq!(
        std::fs::read_to_string(&path).expect("read supersession write"),
        "local bytes\n"
    );

    std::fs::remove_file(path).expect("cleanup supersession fixture");
}

#[test]
fn buffer_stale_default_detects_clean_file_modtime_changes() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let path = make_compat_temp_file(&mut interp, &mut env, "emaxx-buffer-stale-");
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
        crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            "(buffer-stale--default-function t)",
        )
            .expect("check stale file"),
        Value::T
    );

    interp.buffer.set_modified();
    assert_eq!(
        crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            "(buffer-stale--default-function t)",
        )
            .expect("modified buffers are not stale"),
        Value::Nil
    );

    std::fs::remove_file(path).expect("cleanup stale file");
}

#[test]
fn revert_buffer_honors_buffer_local_revert_function() {
    run_with_large_stack(|| {
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = Vec::new();
        let path = make_compat_temp_file(&mut interp, &mut env, "emaxx-revert-buffer-function-");
        std::fs::write(&path, "fresh").expect("write file contents");

        interp.buffer = crate::buffer::Buffer::from_text("*revert*", "stale");
        interp.buffer.file = Some(path.clone());
        interp.buffer.file_truename = Some(path.clone());

        let result = crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            r#"
                (progn
                  (defun sample-revert-function (ignore-auto noconfirm)
                    (setq sample-revert-called t)
                    (revert-buffer--default ignore-auto noconfirm))
                  (setq sample-revert-called nil)
                  (setq-local revert-buffer-function 'sample-revert-function)
                  (revert-buffer nil t)
                  sample-revert-called)
                "#,
        )
        .expect("evaluate wrapper form");

        assert_eq!(result, Value::T);
        assert_eq!(interp.buffer.buffer_string(), "fresh");

        std::fs::remove_file(path).expect("cleanup wrapper file");
    });
}

#[test]
fn revert_buffer_dynamic_nil_suppresses_buffer_local_revert_function() {
    run_with_large_stack(|| {
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = Vec::new();
        let path = make_compat_temp_file(&mut interp, &mut env, "emaxx-revert-buffer-dynamic-");
        std::fs::write(&path, "fresh").expect("write file contents");

        interp.buffer = crate::buffer::Buffer::from_text("*revert*", "stale");
        interp.buffer.file = Some(path.clone());
        interp.buffer.file_truename = Some(path.clone());

        let result = crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            r#"
                (progn
                  (defun sample-revert-wrapper (_orig-fun &rest _args)
                    (error "wrapper should be dynamically suppressed"))
                  (setq-local revert-buffer-function 'sample-revert-wrapper)
                  (let ((revert-buffer-function nil))
                    (revert-buffer nil t))
                  (buffer-string))
                "#,
        )
        .expect("evaluate revert form");

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

    assert_eq!(
        call(&mut interp, "get-byte", &[], &mut env).expect("read byte at point"),
        Value::Integer(0xFF)
    );

    assert_eq!(
        call(
            &mut interp,
            "get-byte",
            &[Value::Nil, Value::String("A".into())],
            &mut env,
        )
        .expect("read first string byte"),
        Value::Integer(b'A' as i64)
    );

    assert_eq!(
        call(
            &mut interp,
            "get-byte",
            &[Value::Nil, Value::String(String::new().into())],
            &mut env,
        )
        .expect("read terminating NUL from an empty string"),
        Value::Integer(0)
    );
}

#[test]
fn extracted_strings_preserve_the_buffer_multibyte_mode() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    interp.buffer = crate::buffer::Buffer::from_text("*text*", "ASCII");

    let multibyte =
        call(&mut interp, "buffer-string", &[], &mut env).expect("extract multibyte buffer string");
    assert!(string_like(&multibyte).expect("string result").multibyte);

    interp.buffer = crate::buffer::Buffer::from_text("*bytes*", "caf\u{00c3}\u{00a9}");
    interp.buffer.set_multibyte(false);
    let unibyte =
        call(&mut interp, "buffer-string", &[], &mut env).expect("extract unibyte buffer string");
    assert!(!string_like(&unibyte).expect("string result").multibyte);

    let decoded = call(
        &mut interp,
        "decode-coding-string",
        &[unibyte, Value::Symbol("utf-8".into())],
        &mut env,
    )
    .expect("decode extracted UTF-8 bytes");
    assert_eq!(string_text(&decoded).expect("decoded string"), "café");
}

#[test]
fn set_buffer_multibyte_reinterprets_the_unchanged_utf8_bytes() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.buffer = crate::buffer::Buffer::from_text("*bytes*", "\u{00d0}\u{0097}");
    interp.buffer.set_multibyte(false);

    call(&mut interp, "set-buffer-multibyte", &[Value::T], &mut env)
        .expect("reinterpret valid UTF-8 bytes");
    assert_eq!(interp.buffer.buffer_string(), "З");
    assert!(interp.buffer.is_multibyte());

    call(&mut interp, "set-buffer-multibyte", &[Value::Nil], &mut env)
        .expect("expose the same UTF-8 byte sequence");
    assert_eq!(interp.buffer.buffer_string(), "\u{00d0}\u{0097}");
    assert!(!interp.buffer.is_multibyte());
}

#[test]
fn write_process_output_supports_stdout_buffer_and_stderr_file() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let stderr_path = std::env::temp_dir()
        .join("emaxx-process-stderr-test")
        .display()
        .to_string();
    let destination = Value::list([Value::T, Value::String(stderr_path.clone().into())]);

    write_process_output(
        &mut interp,
        &destination,
        &[0xFF],
        b"warn\n",
        "call-process",
        &[Value::String("sample".into())],
        &mut env,
    )
    .expect("write process output");
    assert_eq!(
        interp.buffer.buffer_string(),
        decode_raw_text_bytes(&[0xFF])
    );
    assert_eq!(std::fs::read(&stderr_path).expect("stderr file"), b"warn\n");
    std::fs::remove_file(stderr_path).expect("cleanup stderr file");
}

#[test]
fn write_process_output_accepts_a_shared_string_buffer_name() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let (buffer_id, _) = interp.create_buffer("*shared-name-output*");
    let destination =
        make_shared_string_value_with_multibyte("*shared-name-output*".into(), Vec::new(), true);

    write_process_output(
        &mut interp,
        &destination,
        b"output\n",
        b"",
        "call-process",
        &[
            Value::String("sample".into()),
            Value::Nil,
            destination.clone(),
            Value::Nil,
        ],
        &mut env,
    )
    .expect("write output to a buffer named by a shared string");

    assert_eq!(
        interp
            .get_buffer_by_id(buffer_id)
            .expect("output buffer")
            .buffer_string(),
        "output\n"
    );
}

#[test]
fn write_process_output_merges_stderr_for_t_cons_destination() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let destination = Value::cons(Value::T, Value::T);

    write_process_output(
        &mut interp,
        &destination,
        b"out\n",
        b"err\n",
        "call-process",
        &[Value::String("sample".into())],
        &mut env,
    )
    .expect("write merged process output");
    assert_eq!(interp.buffer.buffer_string(), "out\nerr\n");
}

#[test]
fn write_process_output_decodes_with_the_default_process_coding_system() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();

    write_process_output(
        &mut interp,
        &Value::T,
        b"Symbol\xE2\x80\x99s\n",
        b"",
        "call-process",
        &[Value::String("sample".into())],
        &mut env,
    )
    .expect("decode process output");

    assert_eq!(interp.buffer.buffer_string(), "Symbol’s\n");
    assert_eq!(
        interp.lookup_var("last-coding-system-used", &env),
        Some(Value::Symbol("utf-8-unix".into()))
    );
}

#[test]
fn process_coding_alist_overrides_the_default_for_synchronous_output() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = vec![
        vec![(
            "process-coding-system-alist".into(),
            Value::list([Value::cons(
                Value::String("sample\\'".into()),
                Value::cons(
                    Value::Symbol("raw-text-unix".into()),
                    Value::Symbol("raw-text-unix".into()),
                ),
            )]),
        )]
        .into(),
    ];

    write_process_output(
        &mut interp,
        &Value::T,
        b"\xE2\x80\x99",
        b"",
        "call-process",
        &[Value::String("sample".into())],
        &mut env,
    )
    .expect("decode process output through process-coding-system-alist");

    assert_eq!(interp.buffer.buffer_string().chars().count(), 3);
    assert_eq!(
        interp.lookup_var("last-coding-system-used", &env),
        Some(Value::Symbol("raw-text-unix".into()))
    );
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
            &[Value::String(file.display().to_string().into())],
            &mut env,
        )
        .expect("regular file"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "file-regular-p",
            &[Value::String(dir.display().to_string().into())],
            &mut env,
        )
        .expect("directory"),
        Value::Nil
    );

    std::fs::remove_file(file).expect("cleanup temp file");
    std::fs::remove_dir(dir).expect("cleanup temp directory");
}

#[test]
fn write_region_reports_output_errors_as_file_error() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
            Value::String(path.clone().into()),
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
    assert_eq!(items.get(3), Some(&Value::String(path.into())));
    std::fs::remove_dir_all(directory).expect("cleanup temp directory");
}

#[test]
fn value_less_vectors_break_ties_after_equal_prefix_values() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*value-less-buffer*");
    let buffer = Value::buffer(buffer_id, buffer_name);
    let marker = interp.make_marker();
    let Value::Marker(marker_id) = marker else {
        panic!("make_marker should return a marker");
    };
    interp
        .set_marker(marker_id, Some(12), Some(buffer_id))
        .expect("set marker");
    let process = interp
        .create_process(None, None, vec![], None, None)
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
    let buf1 = Value::buffer(buf1_id, buf1_name);
    let buf2 = Value::buffer(buf2_id, buf2_name);
    let buf3 = Value::buffer(buf3_id, buf3_name);
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
        .create_process(None, None, vec![], None, None)
        .expect("create proc1");
    let proc2 = interp
        .create_process(None, None, vec![], None, None)
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

    let sym = Value::symbol;
    let record_a23 = make_record(
        &mut interp,
        &mut env,
        &[sym("a"), Value::Integer(2), Value::Integer(3)],
    );
    let record_b34 = make_record(
        &mut interp,
        &mut env,
        &[sym("b"), Value::Integer(3), Value::Integer(4)],
    );
    let record_b = make_record(&mut interp, &mut env, &[sym("b")]);
    let record_ba = make_record(&mut interp, &mut env, &[sym("b"), sym("a")]);
    let record_a3 = make_record(&mut interp, &mut env, &[sym("a"), Value::Integer(3)]);
    let record_a32 = make_record(
        &mut interp,
        &mut env,
        &[sym("a"), Value::Integer(3), Value::Integer(2)],
    );
    let record_head = make_record(&mut interp, &mut env, &[sym("b"), sym("a")]);
    let record_mid = make_record(&mut interp, &mut env, &[sym("c"), sym("d")]);
    let record_nested_e = make_record(
        &mut interp,
        &mut env,
        &[record_head.clone(), record_mid.clone(), sym("e")],
    );
    let record_nested_f = make_record(&mut interp, &mut env, &[record_head, record_mid, sym("f")]);
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
        ("record_type", record_a23.clone(), record_b34),
        ("record_prefix", record_b, record_ba),
        ("record_same_type", record_a23.clone(), record_a3.clone()),
        ("record_same_type_longer", record_a23, record_a32),
        ("record_nested", record_nested_e, record_nested_f),
        (
            "record_nested_case",
            {
                let head = make_record(&mut interp, &mut env, &[sym("b"), sym("a")]);
                let mid = make_record(&mut interp, &mut env, &[sym("c"), sym("D")]);
                make_record(&mut interp, &mut env, &[head, mid, sym("e")])
            },
            {
                let head = make_record(&mut interp, &mut env, &[sym("b"), sym("a")]);
                let mid = make_record(&mut interp, &mut env, &[sym("c"), sym("d")]);
                make_record(&mut interp, &mut env, &[head, mid, sym("e")])
            },
        ),
        (
            "record_nested_type",
            {
                let head = make_record(&mut interp, &mut env, &[sym("b"), sym("a")]);
                let inner = make_record(&mut interp, &mut env, &[sym("u")]);
                let mid = make_record(
                    &mut interp,
                    &mut env,
                    &[sym("c"), sym("d"), inner, sym("x")],
                );
                make_record(&mut interp, &mut env, &[head, mid, sym("e")])
            },
            {
                let head = make_record(&mut interp, &mut env, &[sym("b"), sym("a")]);
                let inner = make_record(&mut interp, &mut env, &[sym("v")]);
                let mid = make_record(
                    &mut interp,
                    &mut env,
                    &[sym("c"), sym("d"), inner, sym("x")],
                );
                make_record(&mut interp, &mut env, &[head, mid, sym("e")])
            },
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
            Value::buffer(buf3_id, " *three*"),
            buf1.clone(),
        ),
        (
            "dead_buffer_before_live_2",
            Value::buffer(buf3_id, " *three*"),
            Value::buffer(buf2_id, " *two*"),
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
                Value::list([
                    Value::symbol("vector-literal"),
                    left.clone(),
                    Value::Integer(2),
                ]),
                Value::list([
                    Value::symbol("vector-literal"),
                    right.clone(),
                    Value::Integer(1),
                ]),
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
                Value::list([
                    Value::symbol("vector-literal"),
                    right.clone(),
                    Value::Integer(1),
                ]),
                Value::list([
                    Value::symbol("vector-literal"),
                    left.clone(),
                    Value::Integer(2),
                ]),
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
                Value::list([
                    Value::symbol("vector-literal"),
                    left.clone(),
                    Value::Integer(1),
                ]),
                Value::list([
                    Value::symbol("vector-literal"),
                    left.clone(),
                    Value::Integer(2),
                ]),
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
                Value::list([
                    Value::symbol("vector-literal"),
                    right.clone(),
                    Value::Integer(1),
                ]),
                Value::list([Value::symbol("vector-literal"), right, Value::Integer(2)]),
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
        let buffer = Value::buffer(id, name);
        interp.kill_buffer_id(id);
        buffer
    };
    let dead_buf2 = {
        let (id, name) = interp.create_buffer(" *dead-two*");
        let buffer = Value::buffer(id, name);
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
            Value::big_integer(BigInt::from(72057594037927936_i128)),
            Value::Float(72057594037927936.0),
        ),
        // fns.c value_cmp promotes a fixnum to double before comparing, so
        // a fixnum the double cannot represent compares unordered against
        // the neighbouring float even though `<' still orders the pair.
        (
            "fixnum_float_unrepresentable_1",
            Value::Integer(72057594037927935),
            Value::Float(72057594037927936.0),
        ),
        (
            "fixnum_float_unrepresentable_2",
            Value::Float(72057594037927936.0),
            Value::Integer(72057594037927937),
        ),
        (
            "fixnum_float_unrepresentable_3",
            Value::Float(-72057594037927936.0),
            Value::Integer(-72057594037927935),
        ),
        (
            "fixnum_float_unrepresentable_4",
            Value::Integer(-72057594037927937),
            Value::Float(-72057594037927936.0),
        ),
        (
            "fixnum_float_unrepresentable_5",
            Value::Integer(2305843009213693951),
            Value::Float(2305843009213693952.0),
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

    let mut disabled_env = vec![vec![("symbols-with-pos-enabled".into(), Value::Nil)].into()];
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

    let mut enabled_env = vec![vec![("symbols-with-pos-enabled".into(), Value::T)].into()];
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

#[test]
fn member_ignore_case_matches_strings_case_insensitively_on_the_image() {
    // Finding 34 re-host; expectations probed against the pinned oracle
    // (note GNU's compare-strings does NOT fold German sharp s to "SS").
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    assert_eq!(
        crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            "(list (member-ignore-case \"FOO\" '(\"bar\" \"foo\" \"baz\"))
                   (member-ignore-case \"qux\" '(\"bar\"))
                   (member-ignore-case \"\u{00df}\" '(\"SS\")))",
        )
        .expect("member-ignore-case on the dumped image"),
        Value::list([
            Value::list([Value::String("foo".into()), Value::String("baz".into())]),
            Value::Nil,
            Value::Nil,
        ])
    );
}
