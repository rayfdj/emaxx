use super::*;

#[test]
fn define_derived_mode_creates_mode_map_variable() {
    assert_eq!(
        eval_str(
            "(progn
                   (define-derived-mode sample-derived-mode fundamental-mode \"Sample\")
                   (keymapp sample-derived-mode-map))"
        ),
        Value::T
    );
}

#[test]
fn cl_defstruct_generates_constructor_accessors_and_setf() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defstruct (sample-struct
                                  (:constructor nil)
                                  (:constructor make-sample-struct (alpha &key beta)))
                     alpha beta)
                   (let ((sample (make-sample-struct 1 :beta 2)))
                     (setf (sample-struct-alpha sample) 3)
                     (list
                      (sample-struct-p sample)
                      (sample-struct-alpha sample)
                      (sample-struct-beta sample))))"
        ),
        Value::list([Value::T, Value::Integer(3), Value::Integer(2)])
    );
}

#[test]
fn cl_defstruct_honors_conc_name_for_accessors_and_setf() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defstruct (sample-conc
                                  (:constructor make-sample-conc)
                                  (:conc-name sample--))
                     alpha beta)
                   (let ((sample (make-sample-conc :alpha 1 :beta 2)))
                     (setf (sample--alpha sample) 7)
                     (list
                      (fboundp 'sample--alpha)
                      (sample--alpha sample)
                      (sample--beta sample))))"
        ),
        Value::list([Value::T, Value::Integer(7), Value::Integer(2)])
    );
}

#[test]
fn cl_defstruct_constructor_respects_optional_marker() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defstruct (optional-struct
                                  (:constructor make-optional-struct
                                                (&optional alpha beta gamma)))
                     alpha beta gamma)
                   (let ((sample (make-optional-struct 1 2 3)))
                     (list
                      (optional-struct-alpha sample)
                      (optional-struct-beta sample)
                      (optional-struct-gamma sample))))"
        ),
        Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3)])
    );
}

#[test]
fn cl_defstruct_applies_slot_defaults_to_omitted_constructor_args() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defstruct defaulted-struct
                     alpha
                     (beta 7))
                   (defaulted-struct-beta (make-defaulted-struct :alpha 1)))"
        ),
        Value::Integer(7)
    );
}

#[test]
fn cl_defstruct_constructor_evaluates_aux_slot_initializers() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defstruct (aux-struct
                                  (:constructor make-aux-struct
                                                (&aux
                                                 (alpha 3)
                                                 (beta (+ alpha 4)))))
                     alpha beta)
                   (let ((sample (make-aux-struct)))
                     (list (aux-struct-alpha sample)
                           (aux-struct-beta sample))))"
        ),
        Value::list([Value::Integer(3), Value::Integer(7)])
    );
}

#[test]
fn abbrev_expansion_respects_table_props_and_parent_tables() {
    assert_eq!(
        eval_str(
            "(progn
                   (defvar parent-abbrev-table nil)
                   (defvar child-abbrev-table nil)
                   (define-abbrev-table 'parent-abbrev-table '((\"foo\" \"parent\")))
                   (define-abbrev-table 'child-abbrev-table
                     '((\"fb\" \"FooBar\" nil :case-fixed t))
                     \"Child table\"
                     :parents (list parent-abbrev-table))
                   (list
                    (abbrev-expansion \"foo\" child-abbrev-table)
                    (abbrev-expansion \"fb\" child-abbrev-table)
                    (abbrev-expansion \"FB\" child-abbrev-table)))"
        ),
        Value::list([
            Value::String("parent".into()),
            Value::String("FooBar".into()),
            Value::Nil,
        ])
    );
}

#[test]
fn eval_and_compile_runs_its_body_when_loading_helpers() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        r#"
            (eval-and-compile
              (defun helper-name () 'loaded))
            (helper-name)
            "#,
    );
    assert_eq!(
        eval_str_with(&mut interp, "(helper-name)"),
        Value::Symbol("loaded".into())
    );
}

#[test]
fn eval_when_compile_runs_its_body_when_loading_helpers() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        r#"
            (eval-when-compile
              (defun compile-only-helper () 'loaded))
            (compile-only-helper)
            "#,
    );
    assert_eq!(
        eval_str_with(&mut interp, "(compile-only-helper)"),
        Value::Symbol("loaded".into())
    );
}

#[test]
fn expand_file_name_joins_invocation_components() {
    let exe = std::env::current_exe().unwrap();
    let expected = exe.display().to_string();
    assert_eq!(
        eval_str("(expand-file-name invocation-name invocation-directory)"),
        Value::String(expected)
    );
}

#[test]
fn expand_file_name_uses_dynamic_default_directory() {
    let base = format!(
        "{}{}",
        std::env::temp_dir().display(),
        std::path::MAIN_SEPARATOR
    );
    let expected = std::env::temp_dir().join("child").display().to_string();
    let expr = format!(
        "(let ((default-directory {:?})) (expand-file-name \"child\"))",
        base
    );
    assert_eq!(eval_str(&expr), Value::String(expected));
}

#[test]
fn custom_current_group_alist_defaults_to_nil() {
    assert_eq!(eval_str("custom-current-group-alist"), Value::Nil);
}

#[test]
fn emacs_lisp_mode_syntax_table_defaults_to_placeholder() {
    assert_eq!(
        eval_str("emacs-lisp-mode-syntax-table"),
        Value::CharTable(1)
    );
}

#[test]
fn cl_loop_supports_across_with_unbounded_from() {
    assert_eq!(
        eval_str(
            r#"
                (let (pairs)
                  (cl-loop for char across "ab"
                           for i from 0
                           do (setq pairs (cons (list char i) pairs)))
                  (nreverse pairs))
                "#
        ),
        Value::list([
            Value::list([Value::Integer('a' as i64), Value::Integer(0)]),
            Value::list([Value::Integer('b' as i64), Value::Integer(1)]),
        ])
    );
}

#[test]
fn byte_compile_wraps_lambdas_in_byte_code_function_records() {
    assert_eq!(
        eval_str(
            r#"
                (type-of (byte-compile (lambda (x) (char-syntax x))))
                "#
        ),
        Value::Symbol("byte-code-function".into())
    );
}

#[test]
fn byte_compile_symbol_preserves_function_attributes() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defun emaxx-bytecomp-attr-test (x)
                    "tata"
                    (declare (pure t) (indent 1))
                    (interactive "P")
                    (list 'toto x))
                  (let ((bc (byte-compile 'emaxx-bytecomp-attr-test)))
                    (list (byte-code-function-p bc)
                          (funcall bc 'titi)
                          (aref bc 5)
                          (get 'emaxx-bytecomp-attr-test 'pure)
                          (get 'emaxx-bytecomp-attr-test 'lisp-indent-function)
                          (aref bc 4))))
                "#
        ),
        Value::list([
            Value::T,
            Value::list([Value::Symbol("toto".into()), Value::Symbol("titi".into())]),
            Value::String("P".into()),
            Value::T,
            Value::Integer(1),
            Value::String("tata\n\n(fn X)".into()),
        ])
    );
}

#[test]
fn byte_compile_decompile_cond_switch_drops_duplicate_keys() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((bc (byte-compile
                            '(lambda (x)
                               (cond ((eq x 'a) 111)
                                     ((eq x 'b) 222)
                                     ((eq x 'a) 333)
                                     ((eq x 'c) 444)))))
                       (_ (autoload 'byte-decompile-bytecode "byte-opt"))
                       (lap (byte-decompile-bytecode (aref bc 1) (aref bc 2)))
                       (table (cadr (assq 'byte-constant lap))))
                  (list (hash-table-p table)
                        (sort (hash-table-keys table) #'string<)
                        (member '(byte-constant 111) lap)
                        (member '(byte-constant 222) lap)
                        (member '(byte-constant 333) lap)
                        (member '(byte-constant 444) lap)
                        (let* ((bc2 (byte-compile
                                     '(lambda (x)
                                        (cond ((eql x #x10000000000000000) 111)
                                              ((eql x #x10000000000000001) 222)
                                              ((eql x #x10000000000000000) 333)
                                              ((eql x #x10000000000000002) 444)))))
                               (lap2 (byte-decompile-bytecode (aref bc2 1) (aref bc2 2)))
                               (table2 (cadr (assq 'byte-constant lap2))))
                          (mapcar #'numberp (hash-table-keys table2)))))
                "#
        ),
        Value::list([
            Value::T,
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ]),
            Value::list([
                Value::list([Value::Symbol("byte-constant".into()), Value::Integer(111)]),
                Value::list([Value::Symbol("byte-constant".into()), Value::Integer(222)]),
                Value::list([Value::Symbol("byte-constant".into()), Value::Integer(444)]),
            ]),
            Value::list([
                Value::list([Value::Symbol("byte-constant".into()), Value::Integer(222)]),
                Value::list([Value::Symbol("byte-constant".into()), Value::Integer(444)]),
            ]),
            Value::Nil,
            Value::list([Value::list([
                Value::Symbol("byte-constant".into()),
                Value::Integer(444),
            ])]),
            Value::list([Value::T, Value::T, Value::T]),
        ])
    );
}

#[test]
fn byte_compile_warns_for_malformed_defcustom_types() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defun emaxx-bytecomp-defcustom-type-matches-p (pattern form)
                    (with-current-buffer (get-buffer-create "*Compile-Log*")
                      (let ((inhibit-read-only t))
                        (erase-buffer)))
                    (byte-compile form)
                    (with-current-buffer "*Compile-Log*"
                      (not (null (re-search-forward pattern nil t)))))
                  (mapcar
                   (lambda (case)
                     (emaxx-bytecomp-defcustom-type-matches-p (car case) (cadr case)))
                   '(("type should not be quoted"
                      (defcustom mytest nil "doc" :type ''integer :group 'test))
                     ("type should not be quoted"
                      (defcustom mytest nil "doc" :type '(choice '(repeat boolean)) :group 'test))
                     ("misplaced :tag keyword"
                      (defcustom mytest nil "doc" :type '(choice (const b :tag "a")) :group 'test))
                     ("`choice' without any types inside"
                      (defcustom mytest nil "doc" :type '(choice :tag "a") :group 'test))
                     ("`other' not last in `choice'"
                      (defcustom mytest nil "doc" :type '(choice (const a) (other b) (const c)) :group 'test))
                     ("duplicated value in `choice': `a'"
                      (defcustom mytest nil "doc" :type '(choice (const a) (const b) (const a)) :group 'test))
                     ("duplicated :tag string in `choice': \"X\""
                      (defcustom mytest nil "doc" :type '(choice (const :tag "X" a) (const :tag "Y" b) (other :tag "X" c)) :group 'test))
                     ("`cons' requires 2 type specs, found 1"
                      (defcustom mytest nil "doc" :type '(cons :tag "a" integer) :group 'test))
                     ("`repeat' without type specs"
                      (defcustom mytest nil "doc" :type '(repeat :tag "a") :group 'test))
                     ("`const' with too many values"
                      (defcustom mytest nil "doc" :type '(const :tag "a" x y) :group 'test))
                     ("`const' with quoted value"
                      (defcustom mytest nil "doc" :type '(const :tag "a" 'x) :group 'test))
                     ("`bool' is not a valid type"
                      (defcustom mytest nil "doc" :type '(bool :tag "a") :group 'test))
                     ("irregular type `:tag'"
                      (defcustom mytest nil "doc" :type '(:tag "a") :group 'test))
                     ("irregular type"
                      (defcustom mytest nil "doc" :type '(list "string") :group 'test))
                     ("`list' without arguments"
                      (defcustom mytest nil "doc" :type 'list :group 'test))
                     ("`integerp' is not a valid type"
                      (defcustom mytest nil "doc" :type 'integerp :group 'test)))))
                "#
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn byte_compile_from_buffer_warns_for_unresolved_calls_outside_feature_guards() {
    assert_eq!(
        eval_str(
            r#"
                (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*")))
                  (with-temp-buffer
                    (insert "\\(defun foo ()\n"
                            "  (an-undefined-function))\n"
                            "\\(defun foo1 ()\n"
                            "  (if (featurep 'xemacs)\n"
                            "      (some-undefined-function-if)))\n"
                            "\\(defun foo2 ()\n"
                            "  (and (featurep 'xemacs)\n"
                            "       (some-undefined-function-and)))\n"
                            "\\(defun foo3 ()\n"
                            "  (if (not (featurep 'emacs))\n"
                            "      (some-undefined-function-not)))\n"
                            "\\(defun foo4 ()\n"
                            "  (or (featurep 'emacs)\n"
                            "      (some-undefined-function-or)))\n")
                    (byte-compile-from-buffer (current-buffer)))
                  (with-current-buffer byte-compile-log-buffer
                    (list (not (null (search-forward "an-undefined-function" nil t)))
                          (not (null (search-forward "some-undefined-function" nil t))))))
                "#
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn byte_compile_file_logs_and_suppresses_structural_warnings() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-warn-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let warn_src = dir.join("warn.el");
    let suppressed_src = dir.join("suppressed.el");
    let warn_dest = dir.join("warn.elc");
    let suppressed_dest = dir.join("suppressed.elc");
    std::fs::write(
        &warn_src,
        ";;; -*-lexical-binding:t-*-\n(defvar prefixless)\n",
    )
    .unwrap();
    std::fs::write(
        &suppressed_src,
        ";;; -*-lexical-binding:t-*-\n(with-suppressed-warnings ((lexical prefixless)) (defvar prefixless))\n",
    )
    .unwrap();

    let source = format!(
        r#"
            (let* ((warn-src {warn_src:?})
                   (warn-dest {warn_dest:?})
                   (suppressed-src {suppressed_src:?})
                   (suppressed-dest {suppressed_dest:?})
                   (byte-compile-log-buffer (generate-new-buffer " *Compile-Log*")))
              (let ((byte-compile-dest-file-function (lambda (_) warn-dest)))
                (byte-compile-file warn-src))
              (let ((warn-log (with-current-buffer byte-compile-log-buffer
                                (buffer-string))))
                (with-current-buffer byte-compile-log-buffer
                  (let ((inhibit-read-only t))
                    (erase-buffer)))
                (let ((byte-compile-dest-file-function (lambda (_) suppressed-dest)))
                  (byte-compile-file suppressed-src))
                (list (string-match "global/dynamic var .prefixless. lacks" warn-log)
                      (with-current-buffer byte-compile-log-buffer
                        (buffer-string))
                      (file-exists-p warn-dest)
                      (file-exists-p suppressed-dest))))
            "#,
        warn_src = warn_src.display().to_string(),
        warn_dest = warn_dest.display().to_string(),
        suppressed_src = suppressed_src.display().to_string(),
        suppressed_dest = suppressed_dest.display().to_string(),
    );

    let result = eval_str(&source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        result,
        Value::list([
            Value::Integer(9),
            Value::String(String::new()),
            Value::T,
            Value::T
        ])
    );
}

#[test]
fn byte_compile_file_warns_when_lexical_binding_cookie_is_missing() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-cookie-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let missing = dir.join("missing.el");
    let lexical_t = dir.join("lexical-t.el");
    let lexical_nil = dir.join("lexical-nil.el");
    let dest = dir.join("out.elc");
    std::fs::write(&missing, "(defun my-fun () 12)\n").unwrap();
    std::fs::write(
        &lexical_t,
        ";;; -*-lexical-binding:t-*-\n(defun my-fun () 12)\n",
    )
    .unwrap();
    std::fs::write(
        &lexical_nil,
        ";;; -*-lexical-binding:nil-*-\n(defun my-fun () 12)\n",
    )
    .unwrap();

    let source = format!(
        r#"
            (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*"))
                  (byte-compile-dest-file-function (lambda (_) {dest:?})))
              (byte-compile-file {missing:?})
              (let ((missing-log (with-current-buffer byte-compile-log-buffer
                                   (buffer-string))))
                (with-current-buffer byte-compile-log-buffer
                  (let ((inhibit-read-only t))
                    (erase-buffer)))
                (byte-compile-file {lexical_t:?})
                (let ((lexical-t-log (with-current-buffer byte-compile-log-buffer
                                       (buffer-string))))
                  (with-current-buffer byte-compile-log-buffer
                    (let ((inhibit-read-only t))
                      (erase-buffer)))
                  (byte-compile-file {lexical_nil:?})
                  (let ((lexical-nil-log (with-current-buffer byte-compile-log-buffer
                                           (buffer-string))))
                    (list (not (null (string-search "no `lexical-binding' directive" missing-log)))
                          (string-search "no `lexical-binding' directive" lexical-t-log)
                          (string-search "no `lexical-binding' directive" lexical-nil-log))))))
            "#,
        dest = dest.display().to_string(),
        missing = missing.display().to_string(),
        lexical_t = lexical_t.display().to_string(),
        lexical_nil = lexical_nil.display().to_string(),
    );
    let result = eval_str(&source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(result, Value::list([Value::T, Value::Nil, Value::Nil]));
}

#[test]
fn byte_compile_file_errors_on_unescaped_character_literal_warnings() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-unescaped-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let source_path = dir.join("source.el");
    let dest_path = dir.join("source.elc");
    std::fs::write(
        &source_path,
        ";;; -*-lexical-binding:t-*-\n(list ?) ?( ?; ?\" ?[ ?])\n",
    )
    .unwrap();

    let source = format!(
        r#"
            (let ((byte-compile-error-on-warn t)
                  (byte-compile-dest-file-function (lambda (_) {dest_path:?})))
              (cdr (should-error (byte-compile-file {source_path:?}))))
            "#,
        source_path = source_path.display().to_string(),
        dest_path = dest_path.display().to_string(),
    );
    let result = eval_str(&source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        result,
        Value::list([Value::String(
            concat!(
                "unescaped character literals `?\"', `?(', `?)', `?;', `?[', `?]' detected, ",
                "`?\\\"', `?\\(', `?\\)', `?\\;', `?\\[', `?\\]' expected!"
            )
            .into()
        )])
    );
}

#[test]
fn byte_compile_file_warns_when_calls_precede_macro_definitions() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-macro-warn-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let source_path = dir.join("source.el");
    let dest_path = dir.join("source.elc");
    std::fs::write(
        &source_path,
        concat!(
            ";;; -*-lexical-binding:t-*-\n",
            "(progn\n",
            "  (defun my-test0 ()\n",
            "    (my--test11 3)\n",
            "    (my--test12 3)\n",
            "    (my--test2 5))\n",
            "  (defmacro my--test11 (arg) (+ arg 1))\n",
            "  (eval-and-compile\n",
            "    (defmacro my--test12 (arg) (+ arg 1))\n",
            "    (defun my--test2 (arg) (+ arg 1))))\n",
        ),
    )
    .unwrap();

    let source = format!(
        r#"
            (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*"))
                  (byte-compile-dest-file-function (lambda (_) {dest_path:?})))
              (byte-compile-file {source_path:?})
              (with-current-buffer byte-compile-log-buffer
                (list (not (null (re-search-forward "my--test11:\n.*macro" nil t)))
                      (not (null (re-search-forward "my--test12:\n.*macro" nil t)))
                      (progn
                        (goto-char (point-min))
                        (re-search-forward "my--test2" nil t)))))
            "#,
        source_path = source_path.display().to_string(),
        dest_path = dest_path.display().to_string(),
    );
    let result = eval_str(&source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(result, Value::list([Value::T, Value::T, Value::Nil]));
}

#[test]
fn byte_compile_wide_docstring_ignores_function_arg_lists() {
    assert_eq!(
        eval_str(
            r#"
                (list
                 (progn
                   (defun byte-compile--wide-docstring-p (_docstring _max-width) t)
                   (byte-compile--wide-docstring-p
                    "\\(dbus-register-property BUS SERVICE PATH INTERFACE PROPERTY ACCESS [TYPE] VALUE &optional EMITS-SIGNAL DONT-REGISTER-SERVICE)"
                    fill-column))
                 (byte-compile--wide-docstring-p
                  "(dbus-register-property BUS SERVICE PATH INTERFACE PROPERTY ACCESS [TYPE] VALUE &optional EMITS-SIGNAL DONT-REGISTER-SERVICE)"
                  fill-column)
                 (byte-compile--wide-docstring-p
                  "(fn CMD FLAGS FIS &key (BUF (cvs-temp-buffer)) DONT-CHANGE-DISC CVSARGS POSTPROC)"
                  fill-column)
                 (byte-compile--wide-docstring-p
                  "(fn (THIS rudel-protocol-backend) TRANSPORT INFO INFO-CALLBACK &optional PROGRESS-CALLBACK)"
                  fill-column)
                 (byte-compile--wide-docstring-p
                  "(fn NAME FIXTURE INPUT &key SKIP-PAIR-STRING EXPECTED-STRING EXPECTED-POINT BINDINGS (MODES \\='\\='(ruby-mode js-mode python-mode)) (TEST-IN-COMMENTS t) (TEST-IN-STRINGS t) (TEST-IN-CODE t) (FIXTURE-FN \\='#\\='electric-pair-mode))"
                  fill-column)
                 (byte-compile--wide-docstring-p
                  "This ordinary documentation sentence is intentionally long enough to exceed the usual fill column and should still count as wide prose."
                  fill-column))
                "#
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::T
        ])
    );
}

#[test]
fn define_advice_installs_named_around_advice() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defun emaxx-define-advice-target () 'base)
                  (define-advice emaxx-define-advice-target
                      (:around (oldfun &rest args) test)
                    (cons (apply oldfun args) 'advised))
                  (emaxx-define-advice-target))
                "#
        ),
        Value::cons(
            Value::Symbol("base".into()),
            Value::Symbol("advised".into())
        )
    );
}

#[test]
fn eval_lexical_lambda_honors_local_defvar_specialness() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defun emaxx-local-defvar-get-vars ()
                    (list (ignore-errors (symbol-value 'emaxx-local-defvar-var1))
                          (ignore-errors (symbol-value 'emaxx-local-defvar-var2))))
                  (let ((lexical-binding t))
                    (let ((fun '(lambda ()
                                  (defvar emaxx-local-defvar-var1)
                                  (let ((emaxx-local-defvar-var1 'a)
                                        (emaxx-local-defvar-var2 'b))
                                    (ignore emaxx-local-defvar-var2)
                                    (emaxx-local-defvar-get-vars)))))
                      (funcall (eval fun t)))))
                "#
        ),
        Value::list([Value::Symbol("a".into()), Value::Nil])
    );
}

#[test]
fn syntax_table_reports_the_current_buffer_table() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (let ((table (make-syntax-table)))
                    (set-syntax-table table)
                    (eq (syntax-table) table)))
                "#
        ),
        Value::T
    );
}

#[test]
fn with_syntax_table_temporarily_installs_and_restores_table() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (let ((original (syntax-table))
                        (temporary (make-syntax-table)))
                    (list (with-syntax-table temporary
                            (eq (syntax-table) temporary))
                          (eq (syntax-table) original))))
                "#
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn regexp_syntax_classes_match_standard_delimiters() {
    assert_eq!(
        eval_str(
            r#"
                (list
                 (string-match-p "\\s(" "(")
                 (string-match-p "\\s)" ")")
                 (string-match-p "\\s." ";")
                 (string-match-p "\\s." "=")
                 (string-match-p "\\s>" "\n")
                 (string-match-p "\\S>" "n")
                 (string-match-p "\\S(" "a"))
                "#
        ),
        Value::list([
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
        ])
    );
}

#[test]
fn invisible_p_tracks_invisible_text_properties() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "ab")
                  (put-text-property 1 2 'invisible t)
                  (list (invisible-p 1)
                        (invisible-p 2)))
                "#
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn invisible_p_accepts_raw_invisibility_property_values() {
    assert_eq!(
        eval_str(
            "(let ((buffer-invisibility-spec '(outline (secret . t) t)))
                   (list
                    (invisible-p 'outline)
                    (invisible-p '(secret extra))
                    (invisible-p t)
                    (invisible-p 'visible)))"
        ),
        Value::list([Value::T, Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn forward_comment_moves_over_c_comments_in_both_directions() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (set-syntax-table (make-syntax-table))
                  (setq comment-end-can-be-escaped t)
                  (modify-syntax-entry ?/ ". 124b")
                  (modify-syntax-entry ?* ". 23")
                  (modify-syntax-entry ?\n "> b")
                  (insert "1/* comment */1")
                  (let ((after-comment 15))
                    (goto-char 2)
                    (list (forward-comment 1)
                          (point)
                          (progn
                            (goto-char after-comment)
                            (forward-comment -1))
                          (point))))
                "#
        ),
        Value::list([Value::T, Value::Integer(15), Value::T, Value::Integer(2),])
    );
}

#[test]
fn scan_lists_backward_skips_line_comments() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (setq parse-sexp-ignore-comments t)
                  (modify-syntax-entry ?\n "> b")
                  (modify-syntax-entry ?\; "< b")
                  (insert "(; comment\n)")
                  (scan-lists (point-max) -1 0))
                "#
        ),
        Value::Integer(1)
    );
}

#[test]
fn forward_list_moves_over_syntax_table_brace_lists() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (set-syntax-table (make-syntax-table))
                  (modify-syntax-entry ?\{ "(}")
                  (modify-syntax-entry ?\} "){")
                  (insert "{ one { two } three } tail")
                  (goto-char (point-min))
                  (forward-list 1)
                  (point))
                "#
        ),
        Value::Integer(22)
    );
}

#[test]
fn forward_comment_moves_backward_over_lisp_line_comments() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (set-syntax-table (make-syntax-table))
                  (modify-syntax-entry ?\n "> b")
                  (modify-syntax-entry ?\; "< b")
                  (insert "; comment\nx")
                  (goto-char (point-min))
                  (search-forward "x")
                  (backward-char)
                  (list (forward-comment -1) (point)))
                "#
        ),
        Value::list([Value::T, Value::Integer(1)])
    );
}

#[test]
fn forward_comment_matches_syntax_tests_lisp_backward_case() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (set-syntax-table (make-syntax-table))
                  (modify-syntax-entry ?\; "<")
                  (modify-syntax-entry ?\n ">")
                  (insert "31; Comment\n31")
                  (goto-char (point-max))
                  (re-search-backward "\\_<31\\_>")
                  (list (forward-comment -1) (point)))
                "#
        ),
        Value::list([Value::T, Value::Integer(3)])
    );
}

#[test]
fn forward_comment_matches_syntax_tests_c_forward_case() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (set-syntax-table (make-syntax-table))
                  (modify-syntax-entry ?\{ "(}")
                  (modify-syntax-entry ?\} "){")
                  (modify-syntax-entry ?/ ". 124b")
                  (modify-syntax-entry ?* ". 23")
                  (modify-syntax-entry ?\n ">")
                  (modify-syntax-entry ?\\ "\\")
                  (insert "1/* comment */1")
                  (goto-char (point-min))
                  (re-search-forward "\\_<1\\_>")
                  (list (point) (forward-comment 1) (point)))
                "#
        ),
        Value::list([Value::Integer(2), Value::T, Value::Integer(15)])
    );
}

#[test]
fn modify_syntax_entry_defaults_to_current_table() {
    assert_eq!(
        eval_str(
            r#"
                (let ((standard (standard-syntax-table))
                      (table (make-syntax-table)))
                  (set-syntax-table table)
                  (modify-syntax-entry ?\; "<")
                  (list (char-syntax ?\;)
                        (progn
                          (set-syntax-table standard)
                          (char-syntax ?\;))))
                "#
        ),
        Value::list([Value::Integer('<' as i64), Value::Integer('.' as i64),])
    );
}

#[test]
fn forward_comment_ignores_non_comment_double_slash_under_block_comment_syntax() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (modify-syntax-entry ?/ ". 124")
                  (modify-syntax-entry ?* ". 23b")
                  (modify-syntax-entry ?\n ">")
                  (modify-syntax-entry ?\; "<")
                  (insert "// not a comment here\n31; Comment\n31")
                  (goto-char (point-max))
                  (re-search-backward "\\_<31\\_>")
                  (list (forward-comment -1) (point)))
                "#
        ),
        Value::list([Value::T, Value::Integer(25)])
    );
}

#[test]
fn re_search_backward_respects_line_end_anchors() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "1x1\n111\n")
                  (goto-char (point-max))
                  (re-search-backward "\\(^\\|[^0-9]\\)\\(1\\)$")
                  (list (point) (match-beginning 2) (match-end 2)))
                "#
        ),
        Value::list([Value::Integer(2), Value::Integer(3), Value::Integer(4),])
    );
}

#[test]
fn re_search_backward_empty_line_with_bound_returns_line_start() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "a\n\nb\n")
                  (goto-char 4)
                  (let ((bound (save-excursion
                                 (goto-char 1)
                                 (line-end-position))))
                    (list bound
                          (re-search-backward "^$" bound t)
                          (point)
                          (match-beginning 0)
                          (match-end 0))))
                "#
        ),
        Value::list([
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(3),
        ])
    );
}

#[test]
fn re_search_backward_empty_line_before_separator_respects_bound() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "item\n\n==--== DONE \n[DONE x]\n")
                  (goto-char (point-max))
                  (search-backward "[DONE")
                  (let ((bound (save-excursion
                                 (goto-char 1)
                                 (line-end-position))))
                    (list bound
                          (point)
                          (re-search-backward "^$" bound t)
                          (point)
                          (match-beginning 0)
                          (match-end 0))))
                "#
        ),
        Value::list([
            Value::Integer(5),
            Value::Integer(20),
            Value::Integer(6),
            Value::Integer(6),
            Value::Integer(6),
            Value::Integer(6),
        ])
    );
}

#[test]
fn explicitly_numbered_capture_overrides_earlier_capture() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (string-match "\\(?1:a\\)\\(?1:b\\)" "ab")
                  (list (equal (match-string 1 "ab") "b")
                        (match-beginning 1)
                        (match-end 1)))
                "#
        ),
        Value::list([Value::T, Value::Integer(1), Value::Integer(2),])
    );
}

#[test]
fn unmatched_explicit_duplicate_capture_keeps_prior_match() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (string-match "\\(?1:a\\)\\(?1:b\\)?" "a")
                  (list (equal (match-string 1 "a") "a")
                        (match-beginning 1)
                        (match-end 1)))
                "#
        ),
        Value::list([Value::T, Value::Integer(0), Value::Integer(1),])
    );
}

#[test]
fn flatten_tree_returns_non_nil_leaves_in_order() {
    assert_eq!(
        eval_str("(flatten-tree '(1 (2 . 3) nil (4 5 (6)) 7))"),
        Value::list([
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(4),
            Value::Integer(5),
            Value::Integer(6),
            Value::Integer(7),
        ])
    );
}

#[test]
fn flatten_list_alias_matches_flatten_tree() {
    assert_eq!(
        eval_str("(flatten-list '(nil (a . b) (c nil)))"),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
        ])
    );
}

#[test]
fn next_single_char_property_change_observes_overlay_properties() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "abcd")
                  (let ((overlay (make-overlay 2 4)))
                    (overlay-put overlay 'face 'hl-line)
                    (list (next-single-char-property-change 2 'face nil 5)
                          (next-single-char-property-change 4 'face nil 5)
                          (next-single-char-property-change 4 'face))))
                "#
        ),
        Value::list([Value::Integer(4), Value::Integer(5), Value::Integer(5)])
    );
}

#[test]
fn forward_comment_finds_local_nested_comment_despite_earlier_unterminated_one() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (modify-syntax-entry ?# ". 14")
                  (modify-syntax-entry ?| ". 23n")
                  (modify-syntax-entry ?\; "< b")
                  (modify-syntax-entry ?\n "> b")
                  (insert "101#|#\n102#||#102")
                  (goto-char (point-max))
                  (re-search-backward "\\_<102\\_>")
                  (list (forward-comment -1) (point)))
                "#
        ),
        Value::list([Value::T, Value::Integer(11)])
    );
}

#[test]
fn forward_comment_uses_leftmost_line_comment_start() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (modify-syntax-entry ?\n ">")
                  (modify-syntax-entry ?\; "<")
                  (insert "32;;;;;;;;;\n32")
                  (goto-char (point-max))
                  (re-search-backward "\\_<32\\_>")
                  (list (forward-comment -1) (point)))
                "#
        ),
        Value::list([Value::T, Value::Integer(3)])
    );
}

#[test]
fn forward_comment_uses_outer_pascal_comment_start() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (modify-syntax-entry ?{ "<")
                  (modify-syntax-entry ?} ">")
                  (insert "24{\n25{25\n}24")
                  (goto-char (point-max))
                  (re-search-backward "\\_<24\\_>")
                  (list (forward-comment -1) (point)))
                "#
        ),
        Value::list([Value::T, Value::Integer(3)])
    );
}

#[test]
fn forward_comment_backward_prefers_outer_nested_comment_start() {
    assert_eq!(
        eval_str(
            r##"
                (with-temp-buffer
                  (modify-syntax-entry ?# ". 14")
                  (modify-syntax-entry ?| ". 23n")
                  (goto-char (point-min))
                  (insert "#|#|#")
                  (goto-char (point-max))
                  (list (forward-comment -1) (point)))
                "##
        ),
        Value::list([Value::T, Value::Integer(1)])
    );
}

#[test]
fn forward_comment_backward_rejects_overlapping_and_escaped_c_end_markers() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (setq comment-end-can-be-escaped t)
                  (modify-syntax-entry ?/ ". 124b")
                  (modify-syntax-entry ?* ". 23")
                  (modify-syntax-entry ?\n "> b")
                  (insert "5/*/5\n7/* \\*/7")
                  (goto-char (point-min))
                  (search-forward "5")
                  (search-forward "5")
                  (backward-char)
                  (let ((overlap (list (forward-comment -1) (point))))
                    (goto-char (point-max))
                    (search-backward "7")
                    (let ((escaped (list (forward-comment -1) (point))))
                      (list overlap escaped))))
                "#
        ),
        Value::list([
            Value::list([Value::Nil, Value::Integer(5)]),
            Value::list([Value::Nil, Value::Integer(14)]),
        ])
    );
}

#[test]
fn emacs_version_variable_defaults_to_non_empty_string() {
    let value = eval_str("emacs-version");
    match value {
        Value::String(version) => assert!(!version.is_empty()),
        other => panic!("expected string, got {other:?}"),
    }
}

#[test]
fn system_configuration_variable_defaults_to_non_empty_string() {
    let value = eval_str("system-configuration");
    match value {
        Value::String(configuration) => assert!(!configuration.is_empty()),
        other => panic!("expected string, got {other:?}"),
    }
}

#[test]
fn symbol_value_respects_dynamic_bindings() {
    assert_eq!(
        eval_str("(let ((indent-tabs-mode nil)) (symbol-value 'indent-tabs-mode))"),
        Value::Nil
    );
}

#[test]
fn emacs_version_function_mentions_version_and_system_configuration() {
    let mut interp = Interpreter::new();
    let version = eval_str_with(&mut interp, "emacs-version");
    let configuration = eval_str_with(&mut interp, "system-configuration");
    let value = eval_str_with(&mut interp, "(emacs-version)");
    match (version, configuration, value) {
        (Value::String(version), Value::String(configuration), Value::String(description)) => {
            assert!(description.contains(&version));
            assert!(description.contains(&configuration));
        }
        other => panic!("expected strings, got {other:?}"),
    }
}

#[test]
fn process_identity_supports_desktop_lock_checks() {
    let mut interp = Interpreter::new();
    let value = eval_str_with(
        &mut interp,
        r#"
            (let* ((pid (emacs-pid))
                   (attr (process-attributes pid))
                   (proc-cmd (alist-get 'comm attr))
                   (my-cmd (file-name-nondirectory (car command-line-args)))
                   (case-fold-search t))
              (list (integerp pid)
                    (stringp proc-cmd)
                    my-cmd
                    (daemonp)
                    (or (equal proc-cmd my-cmd)
                        (and (string-match-p "emacs" proc-cmd)
                             (string-match-p "emacs" my-cmd)))))
            "#,
    );
    let items = value
        .to_vec()
        .unwrap_or_else(|error| panic!("expected proper list, got {error:?}"));
    assert_eq!(items.len(), 5);
    assert_eq!(items[0], Value::T);
    assert_eq!(items[1], Value::T);
    assert!(
        matches!(&items[2], Value::String(name) if !name.is_empty())
            || matches!(&items[2], Value::StringObject(state) if !state.borrow().text.is_empty())
    );
    assert_eq!(items[3], Value::Nil);
    assert!(items[4].is_truthy());
}

#[test]
fn emacs_major_and_minor_version_variables_default_to_integers() {
    let major = eval_str("emacs-major-version");
    let minor = eval_str("emacs-minor-version");
    match (major, minor) {
        (Value::Integer(major), Value::Integer(minor)) => {
            assert!(major >= 0);
            assert!(minor >= 0);
        }
        other => panic!("expected integers, got {other:?}"),
    }
}

#[test]
fn etags_program_name_defaults_to_non_empty_string() {
    let value = eval_str("etags-program-name");
    match value {
        Value::String(path) => assert!(!path.is_empty()),
        other => panic!("expected string, got {other:?}"),
    }
}

#[test]
fn locate_library_searches_configured_load_path() {
    let temp = std::env::temp_dir().join(format!(
        "emaxx-locate-library-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&temp).unwrap();
    let library = temp.join("sample-lib.el");
    std::fs::write(&library, ";;; sample-lib.el\n").unwrap();

    let mut interp = Interpreter::new();
    interp.set_load_path(vec![temp.clone()]);
    assert_eq!(
        eval_str_with(&mut interp, "(locate-library \"sample-lib\")"),
        Value::String(library.display().to_string())
    );

    std::fs::remove_file(&library).unwrap();
    std::fs::remove_dir(&temp).unwrap();
}

#[test]
fn load_noerror_suppresses_missing_file_signal() {
    let mut interp = Interpreter::new();
    let missing = std::env::temp_dir().join(format!(
        "emaxx-missing-load-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let expr = format!(r#"(load "{}" t)"#, missing.display());
    assert_eq!(eval_str_with(&mut interp, &expr), Value::Nil);

    let mut env = Env::new();
    let strict_expr = format!(r#"(load "{}")"#, missing.display());
    let form = Reader::new(&strict_expr)
        .read_all()
        .expect("read load")
        .remove(0);
    let error = interp.eval(&form, &mut env).unwrap_err();
    assert_eq!(error.condition_type(), "file-missing");
}

#[test]
fn autoload_registers_a_lazy_function_stub() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new("(autoload 'sample-autoload \"sample-autoload\")")
        .read_all()
        .unwrap();
    let result = interp.eval(&forms[0], &mut env).unwrap();
    assert_eq!(result, Value::Symbol("sample-autoload".into()));
    assert_eq!(
        interp.lookup_function("sample-autoload", &env).unwrap(),
        Value::list([
            Value::Symbol("autoload".into()),
            Value::String("sample-autoload".into()),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn custom_autoload_records_expected_symbol_properties() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (custom-autoload 'ps-paper-type \"ps-print\" t)
                   (custom-autoload 'ps-paper-type \"ps-print\" t)
                   (list
                    (get 'ps-paper-type 'custom-autoload)
                    (get 'ps-paper-type 'custom-loads)))"
        ),
        Value::list([
            Value::Symbol("noset".into()),
            Value::list([Value::String("ps-print".into())]),
        ])
    );
}

#[test]
fn temporary_file_directory_exposes_standard_value() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(equal (eval (car (get 'temporary-file-directory 'standard-value)) t)
                            temporary-file-directory)"
            ),
            Value::T
        );
    });
}

#[test]
fn format_time_string_accepts_let_bound_string_zone() {
    run_with_large_stack(|| {
        assert_string_value(
            eval_str(
                "(let ((look '(1202 22527 999999 999999))
                           (fmt \"%Y-%m-%d %H:%M:%S.%3N %z (%Z)\")
                           (zone \"UTC0\"))
                       (format-time-string fmt look zone))",
            ),
            "1972-06-30 23:59:59.999 +0000 (UTC)",
        );
    });
}

#[test]
fn format_supports_float_precision_width_and_flags() {
    assert_eq!(
        eval_str(
            r#"(list (format "%.1f" 6.4)
                         (format "%5.1f" 6.4)
                         (format "%05.1f" 6.4)
                         (format "%+.2f" 1)
                         (format "% .1f" 2))"#,
        ),
        Value::list([
            Value::String("6.4".into()),
            Value::String("  6.4".into()),
            Value::String("006.4".into()),
            Value::String("+1.00".into()),
            Value::String(" 2.0".into()),
        ])
    );
}

#[test]
fn format_seconds_matches_fractional_upstream_cases() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"(progn
                         (require 'time-date)
                         (list
                          (format-seconds "%mm %,1ss" 66.4)
                          (format-seconds "%mm %5,1ss" 66.4)
                          (format-seconds "%mm %.5,1ss" 66.4)
                          (format-seconds "%hh %z%x%mm %ss" (* 60 2))
                          (format-seconds "%Y, %D, %H, %M, %z%S" 0)))"#,
            ),
            Value::list([
                Value::String("1m 6.4s".into()),
                Value::String("1m   6.4s".into()),
                Value::String("1m 006.4s".into()),
                Value::String("2m".into()),
                Value::String("0 seconds".into()),
            ])
        );
    });
}

#[test]
fn vconcat_preserves_string_elements_from_runtime_vectors() {
    assert_eq!(
        eval_str(
            r#"(equal (vconcat ["January"] (vector "*"))
                          ["January" "*"])"#
        ),
        Value::T
    );
}

#[test]
fn todo_mode_loads_date_pattern_with_wildcard_months() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"(progn
                         (require 'todo-mode)
                         (and (stringp todo-date-pattern)
                              (string-match-p "\\\\\\*" todo-date-pattern)
                              t))"#,
            ),
            Value::T
        );
    });
}

#[test]
fn parse_time_string_matches_rfc_822_cases() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"(progn
                         (require 'parse-time)
                         (mapcar
                          (lambda (string)
                            (condition-case err
                                (parse-time-string string)
                              (error (list 'error (car err) (cadr err)))))
                          '("Mon, 22 Feb 2016 19:35:42 +0100"
                            "22 Feb 2016 19:35:42 +0100"
                            "22 Feb 2016 +0100"
                            "Mon, 22 Feb 16 19:35:42 +0100"
                            "Mon, 22 February 2016 19:35:42 +0100"
                            "Mon, 22 feb 2016 19:35:42 +0100"
                            "Monday, 22 february 2016 19:35:42 +0100"
                            "Monday, 22 february 2016 19:35:42 PST"
                            "Friday, 21 Sep 2018 13:47:58 PDT")))"#,
            ),
            Value::list([
                Value::list([
                    Value::Integer(42),
                    Value::Integer(35),
                    Value::Integer(19),
                    Value::Integer(22),
                    Value::Integer(2),
                    Value::Integer(2016),
                    Value::Integer(1),
                    Value::Integer(-1),
                    Value::Integer(3600),
                ]),
                Value::list([
                    Value::Integer(42),
                    Value::Integer(35),
                    Value::Integer(19),
                    Value::Integer(22),
                    Value::Integer(2),
                    Value::Integer(2016),
                    Value::Nil,
                    Value::Integer(-1),
                    Value::Integer(3600),
                ]),
                Value::list([
                    Value::Nil,
                    Value::Nil,
                    Value::Nil,
                    Value::Integer(22),
                    Value::Integer(2),
                    Value::Integer(2016),
                    Value::Nil,
                    Value::Integer(-1),
                    Value::Integer(3600),
                ]),
                Value::list([
                    Value::Integer(42),
                    Value::Integer(35),
                    Value::Integer(19),
                    Value::Integer(22),
                    Value::Integer(2),
                    Value::Integer(2016),
                    Value::Integer(1),
                    Value::Integer(-1),
                    Value::Integer(3600),
                ]),
                Value::list([
                    Value::Integer(42),
                    Value::Integer(35),
                    Value::Integer(19),
                    Value::Integer(22),
                    Value::Integer(2),
                    Value::Integer(2016),
                    Value::Integer(1),
                    Value::Integer(-1),
                    Value::Integer(3600),
                ]),
                Value::list([
                    Value::Integer(42),
                    Value::Integer(35),
                    Value::Integer(19),
                    Value::Integer(22),
                    Value::Integer(2),
                    Value::Integer(2016),
                    Value::Integer(1),
                    Value::Integer(-1),
                    Value::Integer(3600),
                ]),
                Value::list([
                    Value::Integer(42),
                    Value::Integer(35),
                    Value::Integer(19),
                    Value::Integer(22),
                    Value::Integer(2),
                    Value::Integer(2016),
                    Value::Integer(1),
                    Value::Integer(-1),
                    Value::Integer(3600),
                ]),
                Value::list([
                    Value::Integer(42),
                    Value::Integer(35),
                    Value::Integer(19),
                    Value::Integer(22),
                    Value::Integer(2),
                    Value::Integer(2016),
                    Value::Integer(1),
                    Value::Nil,
                    Value::Integer(-28800),
                ]),
                Value::list([
                    Value::Integer(58),
                    Value::Integer(47),
                    Value::Integer(13),
                    Value::Integer(21),
                    Value::Integer(9),
                    Value::Integer(2018),
                    Value::Integer(5),
                    Value::T,
                    Value::Integer(-25200),
                ]),
            ])
        );
    });
}

#[test]
fn cl_parse_integer_handles_keyword_bounds_after_cl_lib_loads() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"(progn
                         (require 'cl-lib)
                         (list
                          (cl-parse-integer "22")
                          (cl-parse-integer "xx-2a yy" :start 2 :end 5 :radix 16)
                          (cl-parse-integer "17" :start nil :end nil :radix nil)
                          (cl-parse-integer "  +101  " :radix 2)
                          (cl-parse-integer "junk" :junk-allowed t)
                          (condition-case err
                              (cl-parse-integer "12x")
                            (error (car err)))))"#,
            ),
            Value::list([
                Value::Integer(22),
                Value::Integer(-42),
                Value::Integer(17),
                Value::Integer(5),
                Value::Nil,
                Value::Symbol("error".into()),
            ])
        );
    });
}

#[test]
fn parse_iso8601_time_string_applies_zone_offsets() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"(progn
                         (require 'parse-time)
                         (append
                          (mapcar
                           (lambda (string)
                             (condition-case err
                                 (format-time-string
                                  "%Y-%m-%d %H:%M:%S"
                                  (parse-iso8601-time-string string) t)
                               (error (list 'error (car err) (cadr err)))))
                           '("1998-09-12T12:21:54-0200"
                             "1998-09-12T12:21:54-0230"
                             "1998-09-12T12:21:54-02:00"
                             "1998-09-12T12:21:54-02"
                             "1998-09-12T12:21:54+0230"
                             "1998-09-12T12:21:54+02"
                             "1998-09-12T12:21:54Z"))
                          (list
                           (condition-case err
                               (equal (parse-iso8601-time-string "1998-09-12T12:21:54")
                                      (encode-time 54 21 12 12 9 1998))
                             (error (list 'error (car err) (cadr err)))))))"#,
            ),
            Value::list([
                Value::String("1998-09-12 14:21:54".into()),
                Value::String("1998-09-12 14:51:54".into()),
                Value::String("1998-09-12 14:21:54".into()),
                Value::String("1998-09-12 14:21:54".into()),
                Value::String("1998-09-12 09:51:54".into()),
                Value::String("1998-09-12 10:21:54".into()),
                Value::String("1998-09-12 12:21:54".into()),
                Value::T,
            ])
        );
    });
}

#[test]
fn decode_time_accepts_let_bound_string_zone() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                "(let ((look '(1202 22527 999999 999999))
                           (zone \"UTC0\"))
                       (equal (decode-time look zone t)
                              (decode-time look \"UTC0\" t)))"
            ),
            Value::T
        );
    });
}

#[test]
fn decoded_time_accessors_read_list_fields() {
    assert_eq!(
        eval_str(
            "(let ((time (decode-time 0 \"UTC0\" 'integer))
                       (short '(0 0 0 1 1 1970)))
                   (list (decoded-time-second time)
                         (decoded-time-minute time)
                         (decoded-time-hour time)
                         (decoded-time-day time)
                         (decoded-time-month time)
                         (decoded-time-year time)
                         (decoded-time-weekday time)
                         (decoded-time-dst short)
                         (decoded-time-zone time)))"
        ),
        Value::list([
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1970),
            Value::Integer(4),
            Value::Nil,
            Value::Integer(0),
        ])
    );
}

#[test]
fn setf_decoded_time_accessors_mutate_time_lists() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                "(let ((time (decode-time 0 \"UTC0\" 'integer)))
                       (setf (decoded-time-hour time) 23)
                       (setf (decoded-time-zone time) -3600)
                       (cl-incf (decoded-time-hour time) 2)
                       (setf (decoded-time-minute time) 45
                             (decoded-time-second time) 30)
                       (list (decoded-time-hour time)
                             (decoded-time-minute time)
                             (decoded-time-second time)
                             (decoded-time-zone time)
                             time))"
            ),
            Value::list([
                Value::Integer(25),
                Value::Integer(45),
                Value::Integer(30),
                Value::Integer(-3600),
                Value::list([
                    Value::Integer(30),
                    Value::Integer(45),
                    Value::Integer(25),
                    Value::Integer(1),
                    Value::Integer(1),
                    Value::Integer(1970),
                    Value::Integer(4),
                    Value::Nil,
                    Value::Integer(-3600),
                ]),
            ])
        );
    });
}

#[test]
fn encode_time_normalizes_overflowing_decoded_fields() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r#"(list (decode-time (encode-time 0 0 0 32 9 2003 nil nil 0) 0 'integer)
                             (decode-time (encode-time 0 0 0 19 13 2003 nil nil 0) 0 'integer)
                             (decode-time (encode-time 0 90 25 1 1 2003 nil nil 0) 0 'integer))"#,
            ),
            Value::list([
                Value::list([
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(2),
                    Value::Integer(10),
                    Value::Integer(2003),
                    Value::Integer(4),
                    Value::Nil,
                    Value::Integer(0),
                ]),
                Value::list([
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(19),
                    Value::Integer(1),
                    Value::Integer(2004),
                    Value::Integer(1),
                    Value::Nil,
                    Value::Integer(0),
                ]),
                Value::list([
                    Value::Integer(0),
                    Value::Integer(30),
                    Value::Integer(2),
                    Value::Integer(2),
                    Value::Integer(1),
                    Value::Integer(2003),
                    Value::Integer(4),
                    Value::Nil,
                    Value::Integer(0),
                ]),
            ])
        );
    });
}

#[test]
fn posix_tz_string_zones_drive_encode_and_decode_time() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r#"(let ((zone "STD+02DST-02,M11.1.0/0,M1.1.0/0"))
                         (list (decode-time (encode-time 0 0 12 15 1 2012 nil nil zone) t 'integer)
                               (decode-time (encode-time 0 0 12 15 12 2012 nil nil zone) t 'integer)
                               (decode-time (encode-time 0 0 12 15 12 2012 nil nil zone) zone 'integer)))"#,
            ),
            Value::list([
                Value::list([
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(14),
                    Value::Integer(15),
                    Value::Integer(1),
                    Value::Integer(2012),
                    Value::Integer(0),
                    Value::Nil,
                    Value::Integer(0),
                ]),
                Value::list([
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(10),
                    Value::Integer(15),
                    Value::Integer(12),
                    Value::Integer(2012),
                    Value::Integer(6),
                    Value::Nil,
                    Value::Integer(0),
                ]),
                Value::list([
                    Value::Integer(0),
                    Value::Integer(0),
                    Value::Integer(12),
                    Value::Integer(15),
                    Value::Integer(12),
                    Value::Integer(2012),
                    Value::Integer(6),
                    Value::T,
                    Value::Integer(7200),
                ]),
            ])
        );
    });
}

#[test]
fn format_time_string_supports_date_and_time_aliases() {
    run_with_large_stack(|| {
        assert_string_value(
            eval_str(
                r#"(format-time-string "%FT%T%z" (encode-time 10 9 5 17 9 2004 nil nil 0) 0)"#,
            ),
            "2004-09-17T05:09:10+0000",
        );
    });
}

#[test]
fn format_time_string_supports_colonized_zone_offsets() {
    run_with_large_stack(|| {
        assert_string_value(
            eval_str(
                r#"(mapconcat
                        #'identity
                        (list (format-time-string "%z|%:z|%:::z" 0 19800)
                              (format-time-string "%z|%:z|%:::z" 0 -18000)
                              (format-time-string "%z|%:z|%:::z" 0 0))
                        "\n")"#,
            ),
            "+0530|+05:30|+05:30\n-0500|-05:00|-05\n+0000|+00:00|+00",
        );
    });
}

#[test]
fn posix_tz_environment_drives_local_encode_and_decode_time() {
    let previous_tz = std::env::var("TZ").ok();
    unsafe {
        std::env::set_var("TZ", "EET-2EEST,M3.5.0/3,M10.5.0/4");
    }
    let result = eval_str(
        r#"(list (decode-time (encode-time '(0 0 10 1 1 2013 nil -1 nil)) nil 'integer)
                     (decode-time (encode-time '(0 0 10 1 8 2013 nil -1 nil)) nil 'integer)
                     (decode-time (encode-time '(0 0 10 1 1 2013 nil -1 t)) nil 'integer))"#,
    );
    if let Some(value) = previous_tz {
        unsafe {
            std::env::set_var("TZ", value);
        }
    } else {
        unsafe {
            std::env::remove_var("TZ");
        }
    }
    assert_eq!(
        result,
        Value::list([
            Value::list([
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(10),
                Value::Integer(1),
                Value::Integer(1),
                Value::Integer(2013),
                Value::Integer(2),
                Value::Nil,
                Value::Integer(7200),
            ]),
            Value::list([
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(10),
                Value::Integer(1),
                Value::Integer(8),
                Value::Integer(2013),
                Value::Integer(4),
                Value::T,
                Value::Integer(10800),
            ]),
            Value::list([
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(12),
                Value::Integer(1),
                Value::Integer(1),
                Value::Integer(2013),
                Value::Integer(2),
                Value::Nil,
                Value::Integer(7200),
            ]),
        ])
    );
}

#[test]
fn char_width_matches_string_width_for_single_characters() {
    assert_eq!(
        eval_str(
            "(let ((tab-width 4))
                   (list (char-width ?a)
                         (char-width ?\t)
                         (char-width ?界)
                         (string-width \"界\")))"
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(8),
            Value::Integer(2),
            Value::Integer(2),
        ])
    );
}

#[test]
fn truncate_string_to_width_uses_display_columns() {
    assert_eq!(
        eval_str(
            "(list (truncate-string-to-width \"abcdef\" 3)
                       (truncate-string-to-width \"界a\" 2)
                       (truncate-string-to-width \"a\" 3 0 ?.)
                       (truncate-string-to-width \"abcdef\" 4 2))"
        ),
        Value::list([
            Value::String("abc".into()),
            Value::String("界".into()),
            Value::String("a..".into()),
            Value::String("cd".into()),
        ])
    );
}

#[test]
fn current_time_string_formats_known_time_with_zone() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str("(current-time-string 0 t)"),
            Value::String("Thu Jan  1 00:00:00 1970".into())
        );
    });
}

#[test]
fn variable_watchers_allow_mutating_lexical_callback_state() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                "(let* ((watch-data nil)
                            (collect-watch-data
                             (lambda (&rest args) (push args watch-data))))
                       (defvar data-tests-var 0)
                       (add-variable-watcher 'data-tests-var collect-watch-data)
                       (setq data-tests-var 1)
                       (remove-variable-watcher 'data-tests-var collect-watch-data)
                       watch-data)"
            ),
            Value::list([Value::list([
                Value::Symbol("data-tests-var".into()),
                Value::Integer(1),
                Value::Symbol("set".into()),
                Value::Nil,
            ])])
        );
    });
}

#[test]
fn local_variable_watchers_allow_mutating_lexical_callback_state() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                "(let* ((watch-data nil)
                            (collect-watch-data
                             (lambda (&rest args) (push args watch-data))))
                       (defvar-local data-tests-lvar 0)
                       (with-temp-buffer
                         (add-variable-watcher 'data-tests-lvar collect-watch-data)
                         (setq data-tests-lvar 1)
                         (remove-variable-watcher 'data-tests-lvar collect-watch-data)
                         (let ((event (car watch-data)))
                           (list (car event)
                                 (nth 1 event)
                                 (nth 2 event)
                                 (bufferp (nth 3 event))))))"
            ),
            Value::list([
                Value::Symbol("data-tests-lvar".into()),
                Value::Integer(1),
                Value::Symbol("set".into()),
                Value::T,
            ])
        );
    });
}

#[test]
fn cl_find_class_prefers_builtin_runtime_for_builtin_classes() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                "(progn
                       (require 'cl-extra)
                       (list (cl-find-class 'fixnum)
                             (built-in-class-p (cl-find-class 'fixnum))
                             (cl-typep 10 'fixnum)))"
            ),
            Value::list([Value::Symbol("fixnum".into()), Value::T, Value::T,])
        );
    });
}

#[test]
fn macrop_recognizes_defined_and_autoloaded_macros() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                       (defmacro sample-live-macro () nil)
                       (defalias 'sample-alias-macro 'sample-live-macro)
                       (list
                        (macrop 'sample-live-macro)
                        (macrop 'sample-alias-macro)
                        (progn
                          (autoload 'sample-auto-macro \"sample-auto\" nil nil 'macro)
                          (macrop 'sample-auto-macro))
                        (sample-alias-macro)
                        (macrop 'car)))"
            ),
            Value::list([Value::T, Value::T, Value::T, Value::Nil, Value::Nil])
        );
    });
}

#[test]
fn apropos_internal_filters_symbols_by_regexp_and_predicate() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                       (defun tramp-compat-sample-fn () t)
                       (defvar tramp-compat-sample-var t)
                       (let ((result (apropos-internal (rx bos \"tramp-compat-\") #'functionp)))
                         (list (length result) (car result) (cdr result))))"
            ),
            Value::list([
                Value::Integer(1),
                Value::Symbol("tramp-compat-sample-fn".into()),
                Value::Nil,
            ])
        );
    });
}

#[test]
fn custom_set_variables_applies_now_specs() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                       (defcustom sample-custom-var nil \"doc\")
                       (custom-set-variables '(sample-custom-var 'set-v now))
                       (list sample-custom-var (car (get 'sample-custom-var 'saved-value))))"
            ),
            Value::list([
                Value::Symbol("set-v".into()),
                Value::list([Value::Symbol("quote".into()), Value::Symbol("set-v".into())]),
            ])
        );
    });
}

#[test]
fn advertised_calling_convention_round_trips_for_symbol_function() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                       (defun sample-adv-cc (arg) arg)
                       (set-advertised-calling-convention 'sample-adv-cc '(value) \"31.1\")
                       (get-advertised-calling-convention (symbol-function 'sample-adv-cc)))"
            ),
            Value::list([Value::Symbol("value".into())])
        );
    });
}

#[test]
fn builtin_autoloads_cover_saveplace_dependencies() {
    let interp = Interpreter::new();
    let env = Vec::new();
    assert_eq!(
        interp
            .lookup_function("cl-delete-duplicates", &env)
            .unwrap(),
        builtin_file_autoload("cl-seq", Value::Nil)
    );
    assert_eq!(
        interp.lookup_function("cl-assoc-if", &env).unwrap(),
        builtin_file_autoload("cl-seq", Value::Nil)
    );
    assert_eq!(
        interp.lookup_function("dired", &env).unwrap(),
        builtin_file_autoload("dired", Value::T)
    );
    assert_eq!(
        interp
            .lookup_function("with-connection-local-variables", &env)
            .unwrap(),
        builtin_macro_autoload("files-x")
    );
    assert_eq!(
        interp
            .lookup_function("connection-local-value", &env)
            .unwrap(),
        builtin_macro_autoload("files-x")
    );
    assert_eq!(
        interp.lookup_function("key-valid-p", &env).unwrap(),
        builtin_file_autoload("keymap", Value::Nil)
    );
    assert_eq!(
        interp.lookup_function("keymap-set", &env).unwrap(),
        builtin_file_autoload("keymap", Value::Nil)
    );
    assert_eq!(
        interp.lookup_function("pp", &env).unwrap(),
        builtin_file_autoload("pp", Value::Nil)
    );
}

#[test]
fn autoloaded_functions_load_on_funcall() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-autoload-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("sample-autoload.el");
    std::fs::write(&target, "(defun sample-autoload () 42)\n").unwrap();

    let mut interp = Interpreter::new();
    interp.set_load_path(vec![root.clone()]);
    eval_str_with(
        &mut interp,
        "(autoload 'sample-autoload \"sample-autoload\")",
    );
    assert_eq!(
        eval_str_with(&mut interp, "(funcall 'sample-autoload)"),
        Value::Integer(42)
    );

    std::fs::remove_file(&target).unwrap();
    std::fs::remove_dir(&root).unwrap();
}

#[test]
fn autoload_do_load_loads_function_stubs() {
    run_with_large_stack(|| {
        let root = std::env::temp_dir().join(format!(
            "emaxx-autoload-do-load-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let target = root.join("sample-autoload-do-load.el");
        std::fs::write(&target, "(defun sample-autoload-do-load () 42)\n").unwrap();

        let mut interp = Interpreter::new();
        interp.set_load_path(vec![root.clone()]);
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                       (autoload 'sample-autoload-do-load \"sample-autoload-do-load\")
                       (autoload-do-load
                         (symbol-function 'sample-autoload-do-load)
                         'sample-autoload-do-load)
                       (list
                         (autoloadp (symbol-function 'sample-autoload-do-load))
                         (sample-autoload-do-load)))"
            ),
            Value::list([Value::Nil, Value::Integer(42)])
        );

        std::fs::remove_file(&target).unwrap();
        std::fs::remove_dir(&root).unwrap();
    });
}

#[test]
fn autoload_do_load_respects_macro_only_for_non_macros() {
    run_with_large_stack(|| {
        let root = std::env::temp_dir().join(format!(
            "emaxx-autoload-macro-only-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let target = root.join("sample-autoload-macro-only.el");
        std::fs::write(&target, "(defun sample-autoload-macro-only () 42)\n").unwrap();

        let mut interp = Interpreter::new();
        interp.set_load_path(vec![root.clone()]);
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                       (autoload 'sample-autoload-macro-only \"sample-autoload-macro-only\")
                       (let ((f (symbol-function 'sample-autoload-macro-only)))
                         (list
                           (autoloadp f)
                           (equal f
                                  (autoload-do-load
                                    f
                                    'sample-autoload-macro-only
                                    'macro))
                           (autoloadp (symbol-function 'sample-autoload-macro-only)))))"
            ),
            Value::list([Value::T, Value::T, Value::T])
        );

        std::fs::remove_file(&target).unwrap();
        std::fs::remove_dir(&root).unwrap();
    });
}

#[test]
fn autoload_do_load_loads_macros_in_macro_mode() {
    run_with_large_stack(|| {
        let root = std::env::temp_dir().join(format!(
            "emaxx-autoload-macro-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let target = root.join("sample-auto-macro.el");
        std::fs::write(&target, "(defmacro sample-auto-macro () 42)\n").unwrap();

        let mut interp = Interpreter::new();
        interp.set_load_path(vec![root.clone()]);
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                       (autoload 'sample-auto-macro \"sample-auto-macro\" nil nil 'macro)
                       (autoload-do-load
                         (symbol-function 'sample-auto-macro)
                         'sample-auto-macro
                         'macro)
                       (list
                         (autoloadp (symbol-function 'sample-auto-macro))
                         (macrop 'sample-auto-macro)
                         (macroexpand '(sample-auto-macro))))"
            ),
            Value::list([Value::Nil, Value::T, Value::Integer(42)])
        );

        std::fs::remove_file(&target).unwrap();
        std::fs::remove_dir(&root).unwrap();
    });
}

#[test]
fn autoloaded_macros_expand_when_called() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-autoload-macroexpand-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("sample-auto-expand.el");
    std::fs::write(&target, "(defmacro sample-auto-expand () 42)\n").unwrap();

    let mut interp = Interpreter::new();
    interp.set_load_path(vec![root.clone()]);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (autoload 'sample-auto-expand \"sample-auto-expand\" nil nil 'macro)
                   (sample-auto-expand))"
        ),
        Value::Integer(42)
    );

    std::fs::remove_file(&target).unwrap();
    std::fs::remove_dir(&root).unwrap();
}

#[test]
fn memq_returns_the_original_tail_cell() {
    assert_eq!(
        eval_str(
            "(let ((xs '(a b c)))
                   (let ((tail (memq 'b xs)))
                     (setcar tail 'x)
                     xs))"
        ),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("x".into()),
            Value::Symbol("c".into()),
        ])
    );
}

#[test]
fn memq_uses_identity_for_cons_elements() {
    assert_eq!(
        eval_str(
            "(let ((left (list 'a))
                       (right (list 'a)))
                   (list (memq left (list right))
                         (not (null (memq left (list left))))))"
        ),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn last_returns_the_original_tail_cell() {
    assert_eq!(
        eval_str(
            "(let ((xs '(a b c)))
                   (let ((tail (last xs)))
                     (setcdr tail '(d))
                     xs))"
        ),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
            Value::Symbol("d".into()),
        ])
    );
}

#[test]
fn cl_defmacro_autoloads_and_expands_in_batch_runtime() {
    run_with_large_stack(|| {
        let emacs_repo = upstream_emacs_repo();
        let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
        let mut interp = Interpreter::new();
        interp.set_load_path(load_path);
        interp.set_variable("noninteractive", Value::T, &mut Vec::new());
        interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
        let _ = interp.load_target("backquote");
        load_faces_compat(&mut interp);
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                       (require 'cl-lib)
                       (cl-defmacro sample-cl-macro () 42)
                       (sample-cl-macro))"
            ),
            Value::Integer(42)
        );
    });
}

#[test]
fn macroexpand_all_expands_let_when_compile_constants() {
    assert_string_list(
        eval_str(
            r#"
                (progn
                  (defmacro let-when-compile (bindings &rest body)
                    (declare (indent 1) (debug let))
                    (letrec ((loop
                              (lambda (bindings)
                                (if (null bindings)
                                    (macroexpand-all (macroexp-progn body)
                                                     macroexpand-all-environment)
                                  (let ((binding (pop bindings)))
                                    (cl-progv (list (car binding))
                                        (list (eval (nth 1 binding) t))
                                      (funcall loop bindings)))))))
                      (funcall loop bindings)))
                  (eval
                   (macroexpand-all
                    '(let-when-compile
                       ((lisp-vdefs '("defvar"))
                        (el-vdefs '("defconst")))
                       (let ((vdefs (eval-when-compile
                                      (append lisp-vdefs el-vdefs))))
                         vdefs)))))
                "#,
        ),
        &["defvar", "defconst"],
    );
}

#[test]
fn macroexpand_all_uses_local_macro_environment() {
    assert_eq!(
        eval_str(
            r#"
                (let ((env (list (cons 'sample-env-macro
                                       (lambda (&rest args)
                                         (cons 'list args))))))
                  (equal (macroexpand-all '(sample-env-macro a b) env)
                         '(list a b)))
                "#
        ),
        Value::T
    );
}

#[test]
fn define_obsolete_variable_alias_sets_up_the_alias() {
    assert_eq!(
        eval_str(
            "(progn
                   (define-obsolete-variable-alias 'old-name 'new-name \"31.1\")
                   (setq new-name 42)
                   old-name)"
        ),
        Value::Integer(42)
    );
}

#[test]
fn vectorp_recognizes_vector_literals() {
    assert_eq!(
        eval_str(r#"(list (vectorp [1 2]) (vectorp '(1 2)) (vectorp "ab"))"#),
        Value::list([Value::T, Value::Nil, Value::Nil])
    );
}

#[test]
fn make_display_table_creates_a_display_char_table() {
    assert_eq!(
        eval_str(
            "(let ((table (make-display-table))) \
                   (list (char-table-p table) (char-table-subtype table)))"
        ),
        Value::list([Value::T, Value::Symbol("display-table".into())])
    );
}

#[test]
fn translate_region_uses_char_tables() {
    let value = eval_str(
        r#"
            (with-temp-buffer
              (insert "Super-secret text")
              (let ((table (make-char-table 'translation-table)))
                (dotimes (i 26)
                  (aset table (+ i ?a) (+ (% (+ i 13) 26) ?a))
                  (aset table (+ i ?A) (+ (% (+ i 13) 26) ?A)))
                (list
                 (translate-region (point-min) (point-max) table)
                 (buffer-string))))
            "#,
    );
    let items = value.to_vec().unwrap();
    assert_eq!(items[0], Value::Integer(15));
    assert_string_value(items[1].clone(), "Fhcre-frperg grkg");
}

#[test]
fn preloaded_point_to_register_stub_is_fboundp() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(&mut interp, "(fboundp 'point-to-register)"),
        Value::T
    );
}

#[test]
fn preloaded_point_to_register_quits_on_quit_events() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let ((last-input-event ?\C-g)
                         (register-alist nil))
                     (condition-case err
                         (call-interactively 'point-to-register)
                       (quit (car err))))"#
        ),
        Value::Symbol("quit".into())
    );
}

#[test]
fn preloaded_command_line_1_processes_command_switch_alist() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let* ((foo-args ())
                          (bar-args ())
                          (command-switch-alist
                           (list (cons "--foo"
                                       (lambda (arg)
                                         (push arg foo-args)
                                         (pop command-line-args-left)))
                                 (cons "--bar=value"
                                       (lambda (arg)
                                         (push arg bar-args))))))
                     (command-line-1 '("--foo" "value" "--bar=value"))
                     (list (equal foo-args '("--foo"))
                           (equal bar-args '("--bar=value"))
                           command-line-args-left))"#
        ),
        Value::list([Value::T, Value::T, Value::Nil,])
    );
}

fn assert_list_buffers_keeps_file_visiting_internal_names_addressable() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-buffer-menu-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("sample.txt");
    std::fs::write(&target, "hello\n").unwrap();

    let mut interp = Interpreter::new();
    let expr = format!(
        "(progn \
               (find-file {path:?}) \
               (rename-buffer \" foo\") \
               (list-buffers) \
               (with-current-buffer \"*Buffer List*\" \
                 (buffer-name (Buffer-menu-buffer))))",
        path = target.display().to_string()
    );
    assert_string_value(eval_str_with(&mut interp, &expr), " foo");

    std::fs::remove_file(&target).unwrap();
    std::fs::remove_dir(&root).unwrap();
}

#[test]
fn list_buffers_keeps_file_visiting_internal_names_addressable() {
    run_large_stack_test(assert_list_buffers_keeps_file_visiting_internal_names_addressable);
}

#[test]
fn load_target_prefers_files_over_same_named_directories() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-load-target-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(root.join("sample")).unwrap();
    std::fs::write(root.join("sample.el"), "(provide 'sample)\n").unwrap();

    let mut interp = Interpreter::new();
    interp.set_load_path(vec![root.clone()]);
    let resolved = interp.load_target("sample").unwrap();
    assert_eq!(resolved, root.join("sample.el"));

    std::fs::remove_file(root.join("sample.el")).unwrap();
    std::fs::remove_dir(root.join("sample")).unwrap();
    std::fs::remove_dir(&root).unwrap();
}

#[test]
fn load_target_resolves_repeated_directory_autoload_aliases() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-load-target-alias-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(root.join("srecode")).unwrap();
    std::fs::write(
        root.join("srecode").join("template.el"),
        "(provide 'srecode/template)\n",
    )
    .unwrap();

    let mut interp = Interpreter::new();
    interp.set_load_path(vec![root.clone()]);
    let resolved = interp.load_target("srecode/srecode-template").unwrap();
    assert_eq!(resolved, root.join("srecode").join("template.el"));

    std::fs::remove_file(root.join("srecode").join("template.el")).unwrap();
    std::fs::remove_dir(root.join("srecode")).unwrap();
    std::fs::remove_dir(&root).unwrap();
}

#[test]
fn load_file_strict_sets_lexical_binding_from_file_cookie() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-lexical-binding-{}.el",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(
        &path,
        ";;; lexical-cookie -*- lexical-binding: t -*-\n(provide 'sample)\n",
    )
    .unwrap();

    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(&mut interp, &path).unwrap();
    assert_eq!(
        interp.lookup_var("lexical-binding", &Vec::new()),
        Some(Value::T)
    );

    std::fs::remove_file(path).unwrap();
}

#[test]
fn load_file_strict_prebinds_current_load_list() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-current-load-list-{}.el",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(
        &path,
        "(setq sample-current-load-entry (car (last current-load-list)))\n",
    )
    .unwrap();

    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(&mut interp, &path).unwrap();
    assert_string_value(
        interp
            .lookup_var("sample-current-load-entry", &Vec::new())
            .expect("sample-current-load-entry"),
        &path.display().to_string(),
    );
    assert_eq!(
        interp.lookup_var("current-load-list", &Vec::new()),
        Some(Value::Nil)
    );

    std::fs::remove_file(path).unwrap();
}

#[test]
fn load_in_progress_is_truthy_while_loading_files() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-load-progress-{}.el",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(
        &path,
        "(setq sample-load-in-progress-seen load-in-progress)\n",
    )
    .unwrap();

    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(&mut interp, &path).unwrap();
    assert_eq!(
        interp.lookup_var("sample-load-in-progress-seen", &Vec::new()),
        Some(Value::T)
    );
    assert_eq!(
        interp.lookup_var("load-in-progress", &Vec::new()),
        Some(Value::Nil)
    );

    std::fs::remove_file(path).unwrap();
}

#[test]
fn batch_dump_purify_flag_defaults_to_nil() {
    assert_eq!(eval_str("purify-flag"), Value::Nil);
}

#[test]
fn require_final_newline_matches_batch_default() {
    assert_eq!(eval_str("require-final-newline"), Value::T);
}

#[test]
fn custom_file_matches_batch_default() {
    assert_eq!(eval_str("custom-file"), Value::Nil);
}

#[test]
fn backup_retention_variables_match_batch_defaults() {
    assert_eq!(
        eval_str(
            "(list version-control dired-kept-versions delete-old-versions
                   kept-old-versions kept-new-versions)"
        ),
        Value::list([
            Value::Nil,
            Value::Integer(2),
            Value::Nil,
            Value::Integer(2),
            Value::Integer(2),
        ])
    );
}

#[test]
fn sentence_end_defaults_to_nil_in_batch() {
    assert_eq!(eval_str("sentence-end"), Value::Nil);
}

#[test]
fn page_delimiter_has_standard_default() {
    assert_eq!(
        eval_str("page-delimiter"),
        Value::String("^\u{000c}".into())
    );
}

#[test]
fn adaptive_fill_defaults_are_bound() {
    assert_eq!(eval_str("adaptive-fill-mode"), Value::T);
    assert_eq!(
        eval_str("adaptive-fill-regexp"),
        Value::String("[-–!|#%;>*·•‣⁃◦ \t]*".into())
    );
    assert_eq!(
        eval_str("adaptive-fill-first-line-regexp"),
        Value::String("\\`[ \t]*\\'".into())
    );
}

#[test]
fn null_device_matches_unix_batch_default() {
    assert_eq!(eval_str("null-device"), Value::String("/dev/null".into()));
}

#[test]
fn exec_suffixes_matches_unix_batch_default() {
    assert_eq!(
        eval_str("exec-suffixes"),
        Value::list([Value::String(String::new())])
    );
}

#[test]
fn debug_on_error_defaults_to_nil_in_batch() {
    assert_eq!(eval_str("debug-on-error"), Value::Nil);
}

#[test]
fn locate_file_searches_directories_and_suffixes() {
    let dir = std::env::temp_dir().join(format!(
        "emaxx-locate-file-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&dir).unwrap();
    let rejected = dir.join("sample.el");
    let accepted = dir.join("sample.txt");
    std::fs::write(&rejected, "").unwrap();
    std::fs::write(&accepted, "").unwrap();
    let dir_text = dir.display().to_string();
    let found = eval_str(&format!(
        "(locate-file \"sample\" '(\"{dir_text}\") '(\".el\" \".txt\")
                          (lambda (path) (string-suffix-p \".txt\" path)))"
    ));
    assert_eq!(found, Value::String(accepted.display().to_string()));
    std::fs::remove_file(rejected).unwrap();
    std::fs::remove_file(accepted).unwrap();
    std::fs::remove_dir(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn locate_file_accepts_symbolic_access_predicates() {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!(
        "emaxx-locate-file-executable-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&dir).unwrap();
    let script = dir.join("sample-tool");
    std::fs::write(&script, "#!/bin/sh\n").unwrap();
    let mut permissions = std::fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&script, permissions).unwrap();
    let dir_text = dir.display().to_string();

    assert_eq!(
        eval_str(&format!(
            "(locate-file \"sample-tool\" '(\"{dir_text}\") '(\"\") 'executable)"
        )),
        Value::String(script.display().to_string())
    );

    std::fs::remove_file(script).unwrap();
    std::fs::remove_dir(dir).unwrap();
}

#[test]
fn defcustom_property_scan_stops_at_non_keyword_forms() {
    assert_eq!(
        eval_str(
            "(progn
                   (defcustom sample-custom-value 1
                     \"doc\"
                     :type 'integer
                     (message \"loaded\"))
                   sample-custom-value)"
        ),
        Value::Integer(1)
    );
}

#[test]
fn load_file_strict_preserves_original_load_errors() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-load-error-{}.el",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(
        &path,
        "(require 'mod-test \"/tmp/emaxx-missing-mod-test\")\n",
    )
    .unwrap();

    let mut interp = Interpreter::new();
    let error = crate::lisp::load_file_strict(&mut interp, &path).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Cannot open load file: No such file or directory, /tmp/emaxx-missing-mod-test"
    );

    std::fs::remove_file(path).unwrap();
}

#[test]
fn generic_record_reader_forms_evaluate_to_literal_records() {
    let mut interp = Interpreter::new();
    let value = eval_str_with(&mut interp, "#s(#s(a b) c)");
    let Value::Record(id) = value else {
        panic!("expected a record literal");
    };
    let record = interp.find_record(id).expect("record state");
    assert_eq!(record.type_name, "literal-record");
    assert_eq!(record.slots.len(), 2);
    assert!(matches!(record.slots[0], Value::Record(_)));
    assert_eq!(record.slots[1], Value::Symbol("c".into()));
}

#[test]
fn read_from_string_makes_record_literals_record_like() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((read-circle t)
                       (result (read-from-string "([#s(r a)])"))
                       (x2 (car (car result)))
                       (x3 (aref x2 0)))
                  (list (recordp x3)
                        (length x3)
                        (aref x3 0)
                        (aref x3 1)))
                "#
        ),
        Value::list([
            Value::T,
            Value::Integer(2),
            Value::Symbol("r".into()),
            Value::Symbol("a".into()),
        ])
    );
}

#[test]
fn read_from_string_nested_record_step_shrinks() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((read-circle t)
                       (result (read-from-string "([#s(r ([#s(r a)]))])"))
                       (x0 (car result))
                       (x1 (aref (car x0) 0))
                       (next (aref x1 1))
                       (x2 (aref (car next) 0)))
                  (list (equal x0 next)
                        (recordp x1)
                        (length x1)
                        (aref x1 0)
                        (recordp x2)
                        (length x2)
                        (aref x2 0)
                        (aref x2 1)))
                "#
        ),
        Value::list([
            Value::Nil,
            Value::T,
            Value::Integer(2),
            Value::Symbol("r".into()),
            Value::T,
            Value::Integer(2),
            Value::Symbol("r".into()),
            Value::Symbol("a".into()),
        ])
    );
}

#[test]
fn read_from_string_prints_circular_cons_with_labels() {
    assert_string_value(
        eval_str(
            r##"
                (let* ((read-circle t)
                       (print-circle t)
                       (result (read-from-string "#1=(#1# . #1#)")))
                  (prin1-to-string (car result)))
                "##,
        ),
        "#1=(#1# . #1#)",
    );
}

#[test]
fn read_from_string_prints_circular_vectors_with_labels() {
    assert_string_value(
        eval_str(
            r##"
                (let* ((read-circle t)
                       (print-circle t)
                       (result (read-from-string "#1=[#1# a #1#]")))
                  (prin1-to-string (car result)))
                "##,
        ),
        "#1=[#1# a #1#]",
    );
}

#[test]
fn prin1_to_string_prints_vector_dotted_pair_tails() {
    assert_string_value(
        eval_str(r#"(prin1-to-string '(("testcat1" . [3 0 2 1])))"#),
        "((\"testcat1\" . [3 0 2 1]))",
    );
}

#[test]
fn prin1_to_current_buffer_keeps_saved_restriction_markers_current() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "header\nbody\n")
                  (goto-char 8)
                  (narrow-to-region (point) (point-max))
                  (save-restriction
                    (widen)
                    (goto-char (point-min))
                    (delete-region (line-beginning-position) (line-end-position))
                    (prin1 '(("testcat1" . [3 0 2 1])) (current-buffer)))
                  (buffer-substring (point-min) (point-max)))
                "#
        ),
        Value::String("body\n".into())
    );
}

#[test]
fn read_from_string_roundtrips_lread_circle_cases() {
    for case in [
        "#1=(#1# . #1#)",
        "#1=[#1# a #1#]",
        "#1=(#2=[#1# #2#] . #1#)",
        "#1=(#2=[#1# #2#] . #2#)",
        "#1=[#2=(#1# . #2#)]",
        "#1=(#2=[#3=(#1# . #2#) #4=(#3# . #4#)])",
    ] {
        let program = format!(
            r##"
                (let* ((read-circle t)
                       (print-circle t)
                       (result (read-from-string "{case}")))
                  (prin1-to-string (car result)))
                "##,
        );
        assert_string_value(eval_str(&program), case);
    }
}

#[test]
fn print_circle_2_upstream_case_completes() {
    let value = eval_str(
        r##"
            (let* ((read-circle t)
                   (x (car (read-from-string "(0 . #1=(0 . #1#))"))))
              (list
               (let ((print-circle nil))
                 (prin1-to-string x))
               (let ((print-circle t))
                 (prin1-to-string x))))
            "##,
    );
    let items = value.to_vec().expect("result list");
    assert_eq!(items.len(), 2);
    assert!(
        items[0]
            .as_string()
            .expect("print-circle nil result should be a string")
            .contains(". #")
    );
    assert_eq!(
        items[1]
            .as_string()
            .expect("print-circle t result should be a string"),
        "(0 . #1=(0 . #1#))"
    );
}

#[test]
fn number_sequence_defaults_to_positive_step() {
    assert_eq!(
        eval_str(
            r##"
                (list
                 (number-sequence 1 0)
                 (number-sequence 3 1)
                 (number-sequence 3 1 -1)
                 (number-sequence 7.5 5.5 -1)
                 (number-sequence 1 3 0.5)
                 (number-sequence 1 3.0 1))
                "##,
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::list([Value::Integer(3), Value::Integer(2), Value::Integer(1)]),
            Value::list([Value::Float(7.5), Value::Float(6.5), Value::Float(5.5)]),
            Value::list([
                Value::Integer(1),
                Value::Float(1.5),
                Value::Float(2.0),
                Value::Float(2.5),
                Value::Float(3.0),
            ]),
            Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3)]),
        ])
    );
}

#[test]
fn mouse_wheel_mode_binds_scroll_command() {
    run_with_large_stack(|| {
        let emacs_repo = upstream_emacs_repo();
        let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
        let mut interp = Interpreter::new();
        interp.set_load_path(load_path);
        interp.set_variable("noninteractive", Value::T, &mut Vec::new());
        interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
        let _ = interp.load_target("backquote");
        load_faces_compat(&mut interp);

        assert_eq!(
            eval_str_with(
                &mut interp,
                r##"
                    (progn
                      (require 'mwheel)
                      (with-suppressed-warnings ((obsolete mouse-wheel-up-event))
                        (mouse-wheel-mode 1)
                        (let ((enabled (lookup-key (current-global-map)
                                                   `[,mouse-wheel-up-event])))
                          (mouse-wheel-mode -1)
                          (list mouse-wheel-up-event
                                enabled
                                (lookup-key (current-global-map)
                                            `[,mouse-wheel-up-event])))))
                    "##,
            ),
            Value::list([
                Value::Symbol("mouse-5".into()),
                Value::Symbol("mwheel-scroll".into()),
                Value::Nil,
            ])
        );
    });
}

#[test]
fn subr_introspection_supports_if_special_form() {
    assert_eq!(
        eval_str(
            r##"
                (list
                 (subr-arity (symbol-function 'if))
                 (subr-name (symbol-function 'if)))
                "##,
        ),
        Value::list([
            Value::cons(Value::Integer(2), Value::Symbol("unevalled".into())),
            Value::String("if".into()),
        ])
    );
}

#[test]
fn fboundp_recognizes_special_forms() {
    assert_eq!(
        eval_str(
            r##"
                (list
                 (fboundp 'setq)
                 (subrp (symbol-function 'setq))
                 (subr-arity (symbol-function 'setq)))
                "##,
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::cons(Value::Integer(0), Value::Symbol("unevalled".into())),
        ])
    );
}

#[test]
fn upstream_lread_circle_form_passes() {
    assert_eq!(
        eval_str(
            r##"
                (let ((lread-test-circle-cases
                       '("#1=(#1# . #1#)"
                         "#1=[#1# a #1#]"
                         "#1=(#2=[#1# #2#] . #1#)"
                         "#1=(#2=[#1# #2#] . #2#)"
                         "#1=[#2=(#1# . #2#)]"
                         "#1=(#2=[#3=(#1# . #2#) #4=(#3# . #4#)])")))
                  (catch 'fail
                    (dolist (str lread-test-circle-cases)
                      (let* ((actual
                              (let* ((read-circle t)
                                     (print-circle t)
                                     (val (read-from-string str)))
                                (if (consp val)
                                    (prin1-to-string (car val))
                                  (error "reading %S failed: %S" str val)))))
                        (unless (equal actual str)
                          (throw 'fail (list str actual)))))
                    (condition-case nil
                        (progn
                          (read-from-string "#1=#1#")
                          (throw 'fail 'invalid-case-did-not-signal))
                      (invalid-read-syntax t))
                    t))
                "##,
        ),
        Value::T
    );
}
