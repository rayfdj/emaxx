use super::*;

#[test]
fn letrec_binds_names_before_initializer_evaluation() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (letrec ((x 1)
                         (y x))
                  y)
                "#
        ),
        Value::Integer(1)
    );
}

#[test]
fn letrec_preserves_recursive_lambda_bindings() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (letrec ((countdown
                          (lambda (n)
                            (if (zerop n) 'done (funcall countdown (1- n))))))
                  (funcall countdown 5))
                "#
        ),
        Value::Symbol("done".into())
    );
}

#[test]
fn letrec_preserves_an_uninterned_recursive_binding_like_gnu_subr_el() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (require 'macroexp)
                 (let* ((name (make-symbol "--cl-loop--"))
                        (form
                         `(letrec
                              ((,name
                                (lambda (n)
                                  (if (zerop n)
                                      'done
                                    (funcall ,name (1- n))))))
                            (funcall ,name 3))))
                   (eval form t)))"#,
        ),
        Value::Symbol("done".into())
    );
}

#[test]
fn letrec_preserves_uninterned_binding_across_non_tail_recursive_calls() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (require 'macroexp)
                 (let* ((name (make-symbol "--cl-loop--"))
                        (form
                         `(letrec
                              ((,name
                                (lambda (tree)
                                  (cond
                                   ((eq tree 'needle) t)
                                   ((consp tree)
                                    (or (funcall ,name (car tree))
                                        (funcall ,name (cdr tree))))))))
                            (funcall ,name '(alpha (beta needle) gamma)))))
                   (eval form t)))"#,
        ),
        Value::T
    );
}

#[test]
fn named_let_expands_to_recursive_binding() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (eval (quote (progn
                  (require 'pcase)
                  (require 'macroexp)
                  (require 'subr-x)
                  (named-let loop ((n 3) (acc nil))
                    (if (> n 0)
                        (loop (1- n) (cons n acc))
                      acc)))) t)
                "#
        ),
        Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3),])
    );
}

#[test]
fn named_let_keeps_its_non_tail_recursive_function_binding() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(eval (quote (progn
                 (require 'pcase)
                 (require 'macroexp)
                 (require 'subr-x)
                 (named-let walk ((tree '(alpha (beta needle) gamma))
                                  (depth 10))
                   (cond
                    ((<= depth 0) nil)
                    ((eq tree 'needle) t)
                    ((consp tree)
                     (or (walk (car tree) (1- depth))
                         (walk (cdr tree) (1- depth)))))))) t)
                "#,
        ),
        Value::T
    );
}

#[test]
fn alist_get_supports_equal_test_function() {
    assert_eq!(
        eval_str("(alist-get \"b\" '((\"a\" . 1) (\"b\" . 2)) nil nil #'equal)"),
        Value::Integer(2)
    );
}

#[test]
fn setf_alist_get_updates_and_removes_entries() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((alist '((\"a\" . 1))))
                   (setf (alist-get \"b\" alist nil nil #'equal) 2)
                   alist)"
        ),
        Value::list([
            Value::cons(Value::String("b".into()), Value::Integer(2)),
            Value::cons(Value::String("a".into()), Value::Integer(1)),
        ])
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((alist '((a . 1) (b . 2))))
                   (setf (alist-get 'b alist nil 'remove) nil)
                   alist)"
        ),
        Value::list([Value::cons(Value::Symbol("a".into()), Value::Integer(1),)])
    );
}

#[test]
fn setf_plist_get_updates_and_adds_entries() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((plist '(:host \"example.org\")))
                   (setf (plist-get plist :secret) \"pw\")
                   plist)"
        ),
        // GNU's gv expander prepends missing keys rather than appending
        // like plist-put.
        Value::list([
            Value::Symbol(":secret".into()),
            Value::String("pw".into()),
            Value::Symbol(":host".into()),
            Value::String("example.org".into()),
        ])
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((plist (list \"host\" \"old\")))
                   (setf (plist-get plist \"host\" #'equal) \"new\")
                   plist)"
        ),
        Value::list([Value::String("host".into()), Value::String("new".into()),])
    );
}

#[test]
fn ert_with_temp_file_honors_text_keyword() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            "(ert-with-temp-file sample-file
                   ;; The generated suffix needs a source file name, which
                   ;; string evaluation lacks (GNU --eval fails identically);
                   ;; an explicit suffix keeps the :text coverage portable.
                   :suffix \"-emaxx\"
                   :text \"alpha\\nbeta\\n\"
                   (with-temp-buffer
                     (insert-file-contents sample-file)
                     (buffer-string)))"
        ),
        Value::String("alpha\nbeta\n".into())
    );
}

#[test]
fn with_temp_file_honors_dynamic_default_directory() {
    let directory = std::env::temp_dir().join(format!(
        "emaxx-with-temp-file-default-directory-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::create_dir(&directory).expect("create default-directory test directory");
    let expected = directory.join("relative-output.txt");
    let directory_text = format!("{}/", directory.to_string_lossy());
    let form = format!(
        r#"(let ((default-directory "{directory_text}"))
             (with-temp-file "relative-output.txt"
               (insert "written"))
             (file-exists-p "{expected}"))"#,
        expected = expected.to_string_lossy()
    );
    assert_eq!(eval_str_with_upstream_batch(&form), Value::T);
    let _ = fs::remove_file(expected);
    let _ = fs::remove_dir(directory);
}

#[test]
fn setf_image_property_updates_image_descriptors() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((image '(image :type png :file \"demo.png\")))
                   (setf (image-property image :type) nil)
                   (setf (image-property image :data) \"payload\")
                   image)"
        ),
        Value::list([
            Value::Symbol("image".into()),
            Value::Symbol(":file".into()),
            Value::String("demo.png".into()),
            Value::Symbol(":data".into()),
            Value::String("payload".into()),
        ])
    );
}

#[test]
fn if_let_star_and_when_let_star_short_circuit_on_nil() {
    assert_eq!(
        eval_str_with_upstream_batch("(if-let* ((a 1) (b 2)) (+ a b) 'fallback)"),
        Value::Integer(3)
    );
    assert_eq!(
        eval_str_with_upstream_batch("(if-let* ((a 1) (_ nil) (b 2)) (+ a b) 'fallback)"),
        Value::Symbol("fallback".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch("(when-let* ((a 1) (b 2)) (+ a b))"),
        Value::Integer(3)
    );
}

#[test]
fn if_let_and_when_let_support_single_binding_compat_syntax() {
    assert_eq!(
        eval_str_with_upstream_batch("(if-let (a 3) (+ a 4) 'fallback)"),
        Value::Integer(7)
    );
    assert_eq!(
        eval_str_with_upstream_batch("(if-let ((a nil) (b 2)) (+ a b) 'fallback)"),
        Value::Symbol("fallback".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch("(when-let (a 5) (+ a 6))"),
        Value::Integer(11)
    );
}

#[test]
fn native_when_let_does_not_reexpand_transient_if_let_forms_in_loops() {
    // GNU's `when-let' expands through `if-let' into `if-let*', so a
    // redefined `if-let*' macro takes over and its binding-dropping
    // expansion leaves `value' unbound: direct GNU 30.2 probes of this
    // exact form signal (void-variable value).
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (defvar emaxx-test-if-let-expansions 0)
               (defmacro if-let* (bindings then &rest else)
                 (setq emaxx-test-if-let-expansions
                       (1+ emaxx-test-if-let-expansions))
                 `(if ,(cadar bindings) ,then ,@else))
               (condition-case err
                   (let ((sum 0))
                     (dotimes (i 1000)
                       (when-let ((value i))
                         (setq sum (+ sum value))))
                     sum)
                 (void-variable (list 'void (cadr err)))))"
        ),
        Value::list([Value::Symbol("void".into()), Value::Symbol("value".into())])
    );
}

#[test]
fn if_let_star_keeps_all_bindings_in_scope_for_the_else_branch() {
    // GNU expands this to one `let*': after B fails, C is bound to nil
    // without evaluating its value form, and all three bindings surround
    // the else branch.
    assert_eq!(
        eval_str(
            "(if-let* ((a 1)
                        (b nil)
                        (c (error \"must not run\")))
                 'then
               (list a b c))"
        ),
        Value::list([Value::Integer(1), Value::Nil, Value::Nil])
    );
}

#[test]
fn and_let_star_returns_body_or_last_binding_value() {
    assert_eq!(
        eval_str("(and-let* ((a 1) (b (+ a 2))) (+ a b))"),
        Value::Integer(4)
    );
    assert_eq!(
        eval_str("(and-let* ((a 1) (b nil)) (error \"must not run\"))"),
        Value::Nil
    );
    assert_eq!(
        eval_str("(and-let* ((a 1) (b (+ a 2))))"),
        Value::Integer(3)
    );
    assert_eq!(eval_str("(and-let* nil)"), Value::T);
}

#[test]
fn bound_and_true_p_checks_binding_before_value() {
    assert_eq!(
        // GNU --eval runs lexically, so the let binding is not a dynamic
        // binding and `bound-and-true-p' reports nil (probed on GNU 30.2).
        eval_str_with_upstream_batch("(let ((sample t)) (bound-and-true-p sample))"),
        Value::Nil
    );
    assert_eq!(
        eval_str_with_upstream_batch("(bound-and-true-p missing-symbol)"),
        Value::Nil
    );
}

#[test]
fn numeric_comparisons_support_variadic_chains() {
    assert_eq!(
        eval_str("(list (<= 33 77 47) (<= 33 40 47) (< 32 65 91) (/= 1 2 1))"),
        Value::list([Value::Nil, Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn seq_position_uses_equal_by_default() {
    assert_eq!(
        eval_str_with_upstream_batch("(seq-position '((a a a) (b b b) (c c c)) '(b b b))"),
        Value::Integer(1)
    );
}

#[test]
fn require_ert_uses_builtin_feature_and_skip_alias() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    assert_eq!(
        eval_str_with(&mut interp, "(require 'ert)"),
        Value::Symbol("ert".into())
    );
    eval_str_with(
        &mut interp,
        r#"
            (ert-deftest skip-via-ert-private-alias ()
              (ert--skip-unless nil))
            "#,
    );
    let summary = interp.run_ert_tests_with_selector(None);
    assert_eq!(summary.skipped, 1);
    assert_eq!(
        interp.test_results[0].condition_type.as_deref(),
        Some("ert-test-skipped")
    );
}

#[test]
fn require_and_provide_evaluate_feature_variables() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((feature-name 'cl-lib)) (require feature-name))"
        ),
        Value::Symbol("cl-lib".into())
    );
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((feature-name 'sample-dynamic-feature)) (provide feature-name))"
        ),
        Value::Symbol("sample-dynamic-feature".into())
    );
    assert!(interp.has_feature("sample-dynamic-feature"));
}

#[test]
fn assigning_features_stays_authoritative_across_later_provides() {
    assert_eq!(
        eval_str(
            "(progn
               (provide 'sample-removed-feature)
               (setq features (delq 'sample-removed-feature features))
               (provide 'sample-later-feature)
               (list (featurep 'sample-removed-feature)
                     (featurep 'sample-later-feature)))"
        ),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn dynamic_features_bindings_keep_the_native_feature_index_in_sync() {
    assert_eq!(
        eval_str(
            "(progn
               (provide 'sample-dynamic-feature)
               (list
                (let ((features
                       (delq 'sample-dynamic-feature (copy-sequence features))))
                  (featurep 'sample-dynamic-feature))
                (featurep 'sample-dynamic-feature)))"
        ),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn provide_subfeatures_and_require_noerror_match_gnu_primitive_contracts() {
    assert_eq!(
        eval_str(
            "(progn
               (provide 'sample-contract-feature '(:one :two))
               (list
                 (featurep 'sample-contract-feature :two)
                 (condition-case err
                     (provide 'sample-invalid-subfeatures 'not-a-list)
                   (wrong-type-argument (car err)))
                 (require 'sample-definitely-missing-feature nil t)))"
        ),
        Value::list([
            Value::T,
            Value::Symbol("wrong-type-argument".into()),
            Value::Nil,
        ])
    );
}

#[test]
fn native_file_primitives_use_deterministic_metadata_not_wall_clock_races() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-native-file-contract-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&root).expect("create native file contract directory");
    let older = root.join("older");
    let newer = root.join("newer");
    let alias = root.join("newer-alias");
    let missing = root.join("missing");
    let source = root.join("source.el");
    fs::write(&older, "older").expect("write older file");
    fs::write(&newer, "newer").expect("write newer file");
    fs::write(&source, "source contents\n").expect("write native source file");
    fs::File::open(&older)
        .expect("open older file")
        .set_times(
            fs::FileTimes::new().set_modified(UNIX_EPOCH + std::time::Duration::from_secs(100_000)),
        )
        .expect("set deterministic older timestamp");
    fs::File::open(&newer)
        .expect("open newer file")
        .set_times(
            fs::FileTimes::new().set_modified(UNIX_EPOCH + std::time::Duration::from_secs(200_000)),
        )
        .expect("set deterministic newer timestamp");

    let result = eval_str(&format!(
        "(progn
           (add-name-to-file {newer:?} {alias:?})
           (list
             (file-newer-than-file-p {newer:?} {older:?})
             (file-newer-than-file-p {older:?} {newer:?})
             (file-newer-than-file-p {newer:?} {missing:?})
             (file-newer-than-file-p {missing:?} {newer:?})
             (file-acl {newer:?})
             (set-file-acl {newer:?} nil)
             (file-selinux-context {newer:?})
             (set-file-selinux-context {newer:?} '(nil nil nil nil))
             (comp-el-to-eln-filename {source:?} {root:?})))",
        newer = newer.display().to_string(),
        older = older.display().to_string(),
        alias = alias.display().to_string(),
        missing = missing.display().to_string(),
        source = source.display().to_string(),
        root = root.display().to_string(),
    ));
    let canonical_source = fs::canonicalize(&source).expect("canonicalize native source");
    let source_path_hash = format!(
        "{:x}",
        md5::compute(canonical_source.display().to_string().as_bytes())
    );
    let source_content_hash = format!("{:x}", md5::compute(b"source contents\n"));
    // comp.el places eln files under `comp-native-version-dir'
    // (VERSION-ABIHASH) when that variable is bound.  This build models a
    // GNU without HAVE_NATIVE_COMP: the variable is void (the oracle's own
    // "30.2-adba4e3f" is that binary's per-build identity, not portable
    // state -- audit finding 77), so the eln name has no version
    // subdirectory.  The hash components still match the oracle's.
    let eln_name = format!(
        "source-{}-{}.eln",
        &source_path_hash[..8],
        &source_content_hash[..8]
    );
    assert_eq!(
        result,
        Value::list([
            Value::T,
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::list([Value::Nil, Value::Nil, Value::Nil, Value::Nil]),
            Value::Nil,
            Value::String(root.join(eln_name).display().to_string().into()),
        ])
    );
    fs::write(&alias, "updated through hard link").expect("write hard-link alias");
    assert_eq!(
        fs::read_to_string(&newer).expect("read original hard-link name"),
        "updated through hard link"
    );
    fs::remove_dir_all(root).expect("remove native file contract directory");
}

#[test]
fn native_dired_host_data_and_comparators_keep_their_direct_call_contracts() {
    assert_eq!(
        eval_str(
            "(let ((users (system-users))
                   (groups (system-groups)))
               (list
                 (file-attributes-lessp '(\"A\") '(\"a\"))
                 (file-attributes-lessp '(\"a\") '(\"A\"))
                 (car-less-than-car '(1) '(2))
                 (car-less-than-car '(2.5) '(3))
                 (car-less-than-car
                   '(#x10000000000000000)
                   '(#x20000000000000000))
                 (> (length users) 0)
                 (not (memq nil (mapcar #'stringp users)))
                 (not (memq nil (mapcar #'stringp groups)))
                 (mapcar
                   (lambda (name)
                     (list
                       (subrp (symbol-function name))
                       (func-arity name)))
                   '(file-attributes-lessp
                     system-users
                     system-groups
                     car-less-than-car))))"
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::list([
                Value::list([Value::T, Value::cons(Value::Integer(2), Value::Integer(2)),]),
                Value::list([Value::T, Value::cons(Value::Integer(0), Value::Integer(0)),]),
                Value::list([Value::T, Value::cons(Value::Integer(0), Value::Integer(0)),]),
                Value::list([Value::T, Value::cons(Value::Integer(2), Value::Integer(2)),]),
            ]),
        ])
    );
}

#[cfg(unix)]
#[test]
fn kill_process_is_a_native_command_and_accepts_a_process_name() {
    assert_eq!(
        eval_str(
            "(let* ((process
                     (make-process
                      :name \"native-kill-process\"
                      :command '(\"/bin/sleep\" \"30\")
                      :connection-type 'pipe
                      :sentinel 'ignore))
                    (returned (kill-process \"native-kill-process\")))
               (while (process-live-p process)
                 (accept-process-output process 0.01))
               (list
                (commandp 'kill-process)
                (subrp (symbol-function 'kill-process))
                returned
                (process-status process)
                (process-exit-status process)))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::String("native-kill-process".into()),
            Value::symbol("signal"),
            Value::Integer(libc::SIGKILL.into()),
        ])
    );
}

#[test]
fn ert_with_test_buffer_kills_buffer_after_success() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"(let (buf)
                     (list
                      (ert-with-test-buffer (:name "jit-lock-test")
                        (setq buf (current-buffer))
                        (buffer-name))
                      (buffer-live-p buf)))"#
        ),
        // GNU ert--format-test-buffer-name derives the buffer name from
        // the running test (none here) and the :name form.
        Value::list([
            Value::String("*Test buffer (<anonymous test>): jit-lock-test*".into()),
            Value::Nil,
        ])
    );
}

#[test]
fn ert_with_test_buffer_keeps_buffer_after_error() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"(let (buf)
                     (condition-case nil
                         (ert-with-test-buffer (:name "jit-lock-test")
                           (setq buf (current-buffer))
                           (error "boom"))
                       (error
                        (list (buffer-live-p buf)
                              (buffer-name buf)))))"#
        ),
        Value::list([
            Value::T,
            Value::String("*Test buffer (<anonymous test>): jit-lock-test*".into()),
        ])
    );
}

#[test]
fn require_uses_explicit_file_targets_in_file_missing_errors() {
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let form = Reader::new("(require 'mod-test \"/tmp/emaxx-missing-mod-test\")")
        .read_all()
        .expect("read require")
        .remove(0);
    let error = interp.eval(&form, &mut env).unwrap_err();
    assert_eq!(error.condition_type(), "file-missing");
    assert_eq!(
        error.to_string(),
        "Cannot open load file: No such file or directory, /tmp/emaxx-missing-mod-test"
    );
}

#[test]
fn require_with_explicit_target_requires_provided_feature() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-require-no-provide-{}.el",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, "(setq sample-require-side-effect t)\n").expect("write require target");
    let path_text = path.to_string_lossy();
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let form = Reader::new(&format!(
        r#"(require 'sample-missing-feature "{path_text}")"#
    ))
    .read_all()
    .expect("read require")
    .remove(0);
    let error = interp.eval(&form, &mut env).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed to provide feature \u{2018}sample-missing-feature\u{2019}")
    );
    let _ = fs::remove_file(path);
}

#[test]
fn require_with_extensionless_target_finds_elc_file() {
    let stem = std::env::temp_dir().join(format!(
        "emaxx-require-elc-target-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    let elc_path = stem.with_extension("elc");
    fs::write(&elc_path, "(provide 'sample-elc-feature)\n").expect("write elc target");
    let stem_text = stem.to_string_lossy();
    let form = format!(r#"(require 'sample-elc-feature "{stem_text}")"#);
    assert_eq!(eval_str(&form), Value::Symbol("sample-elc-feature".into()));
    let _ = fs::remove_file(elc_path);
}

#[test]
fn require_with_extensionless_target_uses_elc_when_el_is_empty() {
    let stem = std::env::temp_dir().join(format!(
        "emaxx-require-empty-el-target-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    let el_path = stem.with_extension("el");
    let elc_path = stem.with_extension("elc");
    fs::write(&el_path, "").expect("write empty source stub");
    fs::write(&elc_path, "(provide 'sample-empty-el-feature)\n").expect("write elc target");
    let stem_text = stem.to_string_lossy();
    let form = format!(r#"(require 'sample-empty-el-feature "{stem_text}")"#);
    assert_eq!(
        eval_str(&form),
        Value::Symbol("sample-empty-el-feature".into())
    );
    let _ = fs::remove_file(el_path);
    let _ = fs::remove_file(elc_path);
}

#[test]
fn require_uses_current_load_path_binding() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-require-load-path-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    let library_dir = root.join("resources");
    fs::create_dir_all(&library_dir).expect("create require load-path dir");
    fs::write(
        library_dir.join("sample-load-path.el"),
        "(provide 'sample-load-path)\n",
    )
    .expect("write require target");
    let source_path = root.join("scenario.el");
    fs::write(
        &source_path,
        format!(
            ";;; -*- lexical-binding: t -*-\n\
             (eval-and-compile\n\
               (let ((load-path (cons {:?} load-path)))\n\
                 (require 'sample-load-path)))\n",
            library_dir.display().to_string()
        ),
    )
    .expect("write lexical require caller");

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    crate::lisp::load_file_strict(&mut interp, &source_path)
        .expect("lexical eval-and-compile should see dynamic load-path");
    assert!(interp.has_feature("sample-load-path"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn require_allows_early_provide_cycles_to_finish_defining_their_api() {
    run_with_large_stack(|| {
        let root = std::env::temp_dir().join(format!(
            "emaxx-require-early-provide-cycle-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create require cycle directory");
        fs::write(
            root.join("emaxx-cycle-a.el"),
            "(require 'emaxx-cycle-b)\n\
             (defmacro emaxx-cycle-mark ()\n\
               '(setq emaxx-cycle-result 'expanded))\n\
             (provide 'emaxx-cycle-a)\n",
        )
        .expect("write cycle A");
        fs::write(
            root.join("emaxx-cycle-b.el"),
            "(provide 'emaxx-cycle-b)\n\
             (require 'emaxx-cycle-a)\n\
             (emaxx-cycle-mark)\n",
        )
        .expect("write cycle B");

        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        interp.set_load_path(vec![root.clone()]);
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn\n\
                   (require 'emaxx-cycle-a)\n\
                   (list (featurep 'emaxx-cycle-a)\n\
                         (featurep 'emaxx-cycle-b)\n\
                         (macrop 'emaxx-cycle-mark)\n\
                         emaxx-cycle-result))"
            ),
            Value::list([
                Value::T,
                Value::T,
                Value::T,
                Value::Symbol("expanded".into()),
            ])
        );

        fs::remove_dir_all(root).expect("remove require cycle directory");
    });
}

#[test]
fn require_bounds_recursive_cycles_and_restores_nesting_after_error() {
    run_with_large_stack(|| {
        let root = std::env::temp_dir().join(format!(
            "emaxx-recursive-require-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create recursive require directory");
        let path = root.join("emaxx-recursive-require.el");
        fs::write(
            &path,
            "(require 'emaxx-recursive-require)\n\
             (provide 'emaxx-recursive-require)\n",
        )
        .expect("write recursive require");

        let mut interp = Interpreter::new();
        interp.set_load_path(vec![root.clone()]);
        let mut env = Env::new();
        let form = Reader::new("(require 'emaxx-recursive-require)")
            .read_all()
            .expect("read recursive require")
            .remove(0);
        let error = interp
            .eval(&form, &mut env)
            .expect_err("unbounded require must fail");
        assert_eq!(
            error.to_string(),
            "Recursive `require' for feature `emaxx-recursive-require'"
        );

        fs::write(&path, "(provide 'emaxx-recursive-require)\n").expect("repair recursive require");
        assert_eq!(
            interp.eval(&form, &mut env).expect("require after repair"),
            Value::Symbol("emaxx-recursive-require".into())
        );
        fs::remove_dir_all(root).expect("remove recursive require directory");
    });
}

#[test]
fn source_loaded_ediff_uses_the_shared_early_provide_cycle_contract() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                "(progn\n\
                   (require 'ediff-diff)\n\
                   (list (featurep 'ediff-diff)\n\
                         (featurep 'ediff-init)\n\
                         (featurep 'ediff-util)\n\
                         (macrop 'ediff-defvar-local)\n\
                         (fboundp 'ediff-exec-process)))"
            ),
            Value::list([Value::T, Value::T, Value::T, Value::T, Value::T])
        );
    });
}

#[test]
fn native_ert_runner_publishes_each_tests_gnu_result_record() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize GNU-compatible batch interpreter");
        interp
            .load_target("ert")
            .expect("load GNU ERT before defining result-chain tests");
        eval_str_with(
            &mut interp,
            "(progn
               (ert-deftest result-chain-00-pass () t)
               (ert-deftest result-chain-01-observe-pass ()
                 (should
                  (eq (type-of
                       (aref (ert-get-test 'result-chain-00-pass) 4))
                      'ert-test-passed)))
               (ert-deftest result-chain-02-fail ()
                 :expected-result :failed
                 (should nil))
               (ert-deftest result-chain-03-observe-fail ()
                 (should
                  (eq (type-of
                       (aref (ert-get-test 'result-chain-02-fail) 4))
                      'ert-test-failed)))
               (ert-deftest result-chain-04-skip ()
                 (skip-unless nil))
               (ert-deftest result-chain-05-observe-skip ()
                 (should
                  (eq (type-of
                       (aref (ert-get-test 'result-chain-04-skip) 4))
                      'ert-test-skipped))))",
        );

        let selector = Value::String("^result-chain-".into());
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(summary.total, 6, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 4, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.skipped, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.unexpected, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn skip_unless_records_skip_in_summary() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    eval_str_with(
        &mut interp,
        r#"
            (ert-deftest skipped-test ()
              (skip-unless nil))
            "#,
    );
    let summary = interp.run_ert_tests_with_selector(None);
    assert_eq!(summary.skipped, 1);
    assert_eq!(interp.test_results[0].status, TestStatus::Skipped);
    assert_eq!(
        interp.test_results[0].condition_type.as_deref(),
        Some("ert-test-skipped")
    );
}

#[test]
fn call_interactively_consumes_unread_events_for_k_specs() {
    assert_string_list(
        eval_str(
            "(let ((unread-command-events '(?a ?b))) \
                   (call-interactively \
                     (lambda (a b) \
                       (interactive \"ka\0a: \nkb: \") \
                       (list a b))))",
        ),
        &["a", "b"],
    );
}

#[test]
fn call_interactively_handles_prefix_argument_specs() {
    assert_eq!(
        eval_str(
            "(let ((current-prefix-arg nil)) \
                   (list (call-interactively \
                           (lambda (arg) (interactive \"p\") arg)) \
                         (call-interactively \
                           (lambda (arg) (interactive \"P\") arg))))"
        ),
        Value::list([Value::Integer(1), Value::Nil])
    );
    assert_eq!(
        eval_str(
            "(let ((current-prefix-arg '(4))) \
                   (list (call-interactively \
                           (lambda (arg) (interactive \"p\") arg)) \
                         (call-interactively \
                           (lambda (arg) (interactive \"P\") arg))))"
        ),
        Value::list([Value::Integer(4), Value::list([Value::Integer(4)]),])
    );
}

#[test]
fn prefix_argument_variables_are_dynamic_across_function_calls() {
    assert_eq!(
        eval_str(
            "(progn
               (defun emaxx-test-prefix-state ()
                 (list prefix-arg last-prefix-arg current-prefix-arg))
               (let ((prefix-arg '(4))
                     (last-prefix-arg '-)
                     (current-prefix-arg 7))
                 (list (special-variable-p 'current-prefix-arg)
                       (emaxx-test-prefix-state))))"
        ),
        Value::list([
            Value::T,
            Value::list([
                Value::list([Value::Integer(4)]),
                Value::Symbol("-".into()),
                Value::Integer(7),
            ]),
        ])
    );
}

#[test]
fn read_string_preserves_non_string_defaults_on_empty_input() {
    assert_eq!(
        eval_str(
            "(list
               (let ((unread-command-events '(?\\r)))
                 (read-string \"Port: \" nil nil 6667))
               (let ((unread-command-events '(?\\r)))
                 (read-string \"Choice: \" nil nil '(answer fallback)))
               (let ((unread-command-events '(?\\r)))
                 (read-from-minibuffer \"Raw: \" nil nil nil nil 42)))"
        ),
        Value::list([
            Value::Integer(6667),
            Value::Symbol("answer".into()),
            Value::String(String::new().into()),
        ])
    );
}

#[test]
fn no_event_minibuffer_runs_setup_hook_before_batch_input_and_restores_state() {
    assert_eq!(
        eval_str(
            r#"(let ((minibuffer-setup-hook
                      (list
                        (function
                         (lambda ()
                          (throw 'state
                            (list (minibuffer-depth)
                                  (minibuffer-prompt)
                                  (point)
                                  (minibuffer-contents)
                                  (windowp (active-minibuffer-window)))))))))
                 (list
                  (catch 'state
                    (read-from-minibuffer "Prompt: " "seed"))
                  (minibuffer-depth)
                  (active-minibuffer-window)))"#,
        ),
        Value::list([
            Value::list([
                Value::Integer(1),
                Value::String("Prompt: ".into()),
                Value::Integer(13),
                Value::String("seed".into()),
                Value::T,
            ]),
            Value::Integer(0),
            Value::Nil,
        ])
    );
}

#[test]
fn simulated_minibuffer_keys_preserve_the_callers_prefix_argument() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"(let ((current-prefix-arg '(4)))
                 (ert-simulate-keys "nick\r"
                   (list (read-string "Nick: ") current-prefix-arg)))"#
        ),
        Value::list([
            Value::String("nick".into()),
            Value::list([Value::Integer(4)]),
        ])
    );
}

#[test]
fn simulated_input_translates_symbolic_return_at_gnu_key_boundaries() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"(list
                 (ert-simulate-keys [?b ?2 return]
                   (read-string "Cell: "))
                 (ert-simulate-keys [?b ?2 return]
                   (completing-read "Cell: " nil))
                 (ert-simulate-keys [return] (read-event))
                 (ert-simulate-keys [return] (read-key))
                 (ert-simulate-keys [return] (read-char)))"#
        ),
        Value::list([
            Value::String("b2".into()),
            Value::String("b2".into()),
            Value::Symbol("return".into()),
            Value::Integer(13),
            Value::Integer(13),
        ])
    );
}

#[test]
fn simulated_minibuffer_prefix_commands_repeat_the_following_input() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"(ert-simulate-keys (kbd "C-u C-u c a b RET")
                 (read-string "Text: "))"#
        ),
        Value::String("ccccccccccccccccab".into())
    );
}

#[test]
fn simulated_minibuffer_keys_do_not_run_prompting_buffer_local_hooks() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"(let ((calls 0))
                 (with-temp-buffer
                   (let ((prompting-buffer (current-buffer)))
                     (add-hook 'post-command-hook
                               (lambda () (setq calls (1+ calls))) nil t)
                     (let ((answer
                            (ert-simulate-keys "nick\r"
                              (read-string "Nick: "))))
                       (list answer calls
                             (eq prompting-buffer (current-buffer)))))))"#
        ),
        Value::list([Value::String("nick".into()), Value::Integer(0), Value::T,])
    );
}

#[test]
fn completing_read_consumes_keyboard_macro_input_in_the_minibuffer() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (defvar completion-target-called nil)
                 (defun completion-target ()
                   (interactive)
                   (setq completion-target-called t))
                 (defun completion-driver (name)
                   (interactive
                    (list (completing-read
                           "Command: " '("completion-target"))))
                   (call-interactively (intern name)))
                 (global-set-key (kbd "C-t") 'completion-driver)
                 (with-temp-buffer
                   (execute-kbd-macro
                    (vconcat (kbd "C-t")
                             "completion-target" [return])))
                 completion-target-called)"#
        ),
        Value::T
    );
}

#[test]
fn buffer_undo_list_mutates_saved_head_for_adjacent_insertions_like_gnu() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                 (insert "a")
                 (let ((saved buffer-undo-list))
                   (insert "b")
                   (eq saved buffer-undo-list)))"#
        ),
        Value::T
    );
}

#[test]
fn call_interactively_skips_interactive_guard_prefixes() {
    assert_eq!(
        eval_str(
            "(let ((current-prefix-arg nil))
               (call-interactively
                (lambda (arg) (interactive \"*P\") arg)))"
        ),
        Value::Nil
    );
}

#[test]
fn call_interactively_autoloads_commands_before_collecting_args() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-callint-autoload-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("sample-callint.el");
    std::fs::write(
        &target,
        "(defun sample-callint-command (arg)\n  (interactive (list 42))\n  arg)\n",
    )
    .unwrap();

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    interp.set_load_path(vec![root.clone()]);
    eval_str_with(
        &mut interp,
        "(autoload 'sample-callint-command \"sample-callint\" nil t)",
    );
    assert_eq!(
        eval_str_with(&mut interp, "(call-interactively 'sample-callint-command)"),
        Value::Integer(42)
    );

    std::fs::remove_file(&target).unwrap();
    std::fs::remove_dir(&root).unwrap();
}

#[test]
fn keyboard_quit_signals_quit_condition() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env: Env = Vec::new();
    let form = Reader::new("(keyboard-quit)").read_all().unwrap().remove(0);
    let error = interp.eval(&form, &mut env).unwrap_err();
    assert_eq!(error.condition_type(), "quit");
}

#[test]
fn run_with_timer_returns_a_timer_without_firing_immediately() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((flag nil)
                       (timer (run-with-timer 1 nil (lambda () (setq flag t)))))
                   (list (timerp timer) flag))"
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn timerp_recognizes_loaded_timer_records() {
    assert_eq!(
        eval_str_with_upstream_batch("(progn (require 'timer) (timerp (timer-create)))"),
        Value::T
    );
}

#[test]
fn timer_queue_variables_default_to_empty_lists() {
    assert_eq!(
        eval_str("(list timer-list timer-idle-list)"),
        Value::list([Value::Nil, Value::Nil])
    );
}

#[test]
fn native_timer_queues_are_special_and_waits_drain_the_dynamic_queue() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
                 (require 'timer)
                 (let ((timer-list (copy-sequence timer-list))
                       (timer-idle-list (copy-sequence timer-idle-list))
                       (fired nil))
                   (run-at-time 0.01 nil (lambda () (setq fired t)))
                   (accept-process-output nil 0.05)
                   (list (special-variable-p 'timer-list)
                         (special-variable-p 'timer-idle-list)
                         fired)))"
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn loaded_timer_queue_fires_during_waits() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
                   (require 'timer)
                   (setq fired nil)
                   (run-with-timer 0 nil (lambda () (setq fired t)))
                   (sleep-for 0)
                   fired)"
        ),
        Value::T
    );
}

#[test]
fn timer_callbacks_finish_with_defsubsts_from_their_unloaded_feature() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (load "seq" nil nil)
                 (require 'loadhist)
                 (require 'timer)
                 (let ((file (make-temp-file "emaxx-timer-unload-" nil ".el")))
                   (unwind-protect
                       (progn
                         (write-region
                          "(defsubst emaxx-timer-unload-helper () 'finished)\n(defun emaxx-timer-unload-callback ()\n  (unload-feature 'emaxx-timer-unload)\n  (setq emaxx-timer-unload-result (emaxx-timer-unload-helper)))\n(provide 'emaxx-timer-unload)\n"
                          nil file)
                         (load file nil nil)
                         (setq emaxx-timer-unload-result nil)
                         (setq debug-on-error t)
                         (run-at-time 0 nil #'emaxx-timer-unload-callback)
                         (sleep-for 0.05)
                         (list emaxx-timer-unload-result
                               (featurep 'emaxx-timer-unload)
                               (fboundp 'emaxx-timer-unload-helper)))
                     (delete-file file))))"#
        ),
        Value::list([Value::Symbol("finished".into()), Value::Nil, Value::Nil])
    );
}

#[test]
fn nonlocal_exit_from_timer_preserves_later_due_timers() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
                 (setq later-timer-fired nil)
                 (run-at-time nil nil (lambda () (throw 'timer-stop t)))
                 (run-at-time nil nil (lambda () (setq later-timer-fired t)))
                 (catch 'timer-stop (sleep-for 0))
                 (sleep-for 0)
                 later-timer-fired)"
        ),
        Value::T
    );
}

#[test]
fn recursive_edit_pumps_loaded_elisp_timers_and_propagates_nonlocal_exits() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
                 (require 'timer)
                 (catch 'timer-stop
                   (run-at-time 0 nil (lambda () (throw 'timer-stop 'fired)))
                   (recursive-edit)
                   'missed))"
        ),
        Value::Symbol("fired".into())
    );
}

#[test]
fn repeated_whole_file_load_and_unload_replace_generic_methods_exactly_once() {
    // GNU cl-generic.el keeps the generic function cell after unloading the
    // twice-loaded feature, removes its public method table, and the next call
    // reaches the surviving compiled dispatch closure after the fixture's log
    // variable was unbound.  Assert that real lifecycle directly; the former
    // Emaxx-only specializer property was not a GNU ownership contract.
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-generic",
            r#"(progn
                 (load "seq" nil nil)
                 (require 'loadhist)
                 (let ((file (make-temp-file "emaxx-generic-unload-" nil ".el")))
                   (unwind-protect
                       (progn
                         (with-temp-file file
                           (insert
                            "(defvar emaxx-generic-unload-log nil)\n"
                            "(cl-defgeneric emaxx-generic-unload (x))\n"
                            "(cl-defmethod emaxx-generic-unload :before ((x integer))\n"
                            "  (push 'before emaxx-generic-unload-log))\n"
                            "(cl-defmethod emaxx-generic-unload ((x integer))\n"
                            "  (push 'primary emaxx-generic-unload-log) x)\n"
                            "(provide 'emaxx-generic-unload-feature)\n"))
                         (load file nil nil t)
                         (load file nil nil t)
                         (setq emaxx-generic-unload-log nil)
                         (emaxx-generic-unload 1)
                         (let ((before
                                (reverse emaxx-generic-unload-log)))
                           (unload-feature
                            'emaxx-generic-unload-feature)
                           (list
                            before
                            (length
                             (seq-filter
                              (lambda (entry)
                                (member
                                 '(provide
                                   . emaxx-generic-unload-feature)
                                 (cdr entry)))
                              load-history))
                            (cl--generic-method-table
                             (cl--generic 'emaxx-generic-unload))
                            (fboundp 'emaxx-generic-unload)
                            (condition-case condition
                                (emaxx-generic-unload 1)
                              (error (list (car condition)
                                           (cadr condition)))))))
                     (ignore-errors (delete-file file)))))"#
        ),
        Value::list([
            Value::list([
                Value::Symbol("before".into()),
                Value::Symbol("primary".into()),
            ]),
            Value::Integer(0),
            Value::Nil,
            Value::T,
            Value::list([
                Value::Symbol("void-variable".into()),
                Value::Symbol("emaxx-generic-unload-log".into()),
            ]),
        ])
    );
}

#[test]
fn auto_revert_mode_reloads_changed_file() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-auto-revert-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    let path_text = path.to_string_lossy();
    let form = format!(
        r#"(progn
                 (require 'autorevert)
                 (require 'ert-x)
                 (customize-set-variable 'auto-revert-interval 0.1)
                 (write-region "any text" nil "{path_text}" nil 'no-message)
                 (let ((buf (find-file-noselect "{path_text}")))
                   (with-current-buffer buf
                     (auto-revert-mode 1)
                     (write-region "another text" nil "{path_text}" nil 'no-message)
                     (set-file-times "{path_text}" (time-subtract nil 1))
                     (ert-with-message-capture auto-revert-test-messages
                       (let ((started (current-time)))
                         (while
                             (and
                              (< (float-time (time-subtract nil started)) 0.3)
                              (null
                               (string-match
                                "Reverting buffer"
                                auto-revert-test-messages)))
                           (sleep-for 0.05))))
                     (prog1 (buffer-string)
                       (set-buffer-modified-p nil)
                       (kill-buffer buf)))))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&form),
        Value::String("another text".into())
    );
    let _ = fs::remove_file(path);
}

#[cfg(unix)]
#[test]
fn file_attributes_and_set_file_times_use_unix_timestamp_fields() {
    use std::os::unix::fs::MetadataExt;

    let path = std::env::temp_dir().join(format!(
        "emaxx-file-ctime-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, "ctime").expect("create ctime fixture");
    let metadata = fs::metadata(&path).expect("stat ctime fixture");
    let expected =
        crate::lisp::primitives::unix_time_list_value(metadata.ctime(), metadata.ctime_nsec());
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(&mut interp, &format!("(nth 6 (file-attributes {path:?}))")),
        expected
    );

    let timestamp = 1_700_000_000u64;
    assert_eq!(
        eval_str_with(
            &mut interp,
            &format!("(set-file-times {path:?} {timestamp})")
        ),
        Value::T
    );
    let metadata = fs::metadata(&path).expect("stat retimed fixture");
    assert_eq!(
        metadata
            .accessed()
            .expect("fixture atime")
            .duration_since(UNIX_EPOCH)
            .expect("atime after epoch")
            .as_secs(),
        timestamp
    );
    assert_eq!(
        metadata
            .modified()
            .expect("fixture mtime")
            .duration_since(UNIX_EPOCH)
            .expect("mtime after epoch")
            .as_secs(),
        timestamp
    );
    fs::remove_file(path).expect("remove ctime fixture");
}

#[test]
fn insert_file_contents_replace_never_prompts_about_supersession() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-insert-replace-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, "old").expect("create insert-file-contents source");
    let path_text = path.to_string_lossy();
    let form = format!(
        r#"(progn
                 (let ((buf (find-file-noselect "{path_text}"))
                       (asked nil))
                   (unwind-protect
                       (progn
                         (with-temp-buffer
                           (insert "new")
                           (write-region nil nil "{path_text}" nil 'no-message))
                         (set-file-times
                          "{path_text}"
                          (time-add
                           (with-current-buffer buf (visited-file-modtime))
                           10))
                         (with-current-buffer buf
                           (cl-letf
                               (((symbol-function
                                  'ask-user-about-supersession-threat)
                                 (lambda (&rest _) (setq asked t))))
                             (insert-file-contents
                              "{path_text}" nil nil nil t)
                             (list (buffer-string) asked))))
                     (when (buffer-live-p buf)
                       (with-current-buffer buf (set-buffer-modified-p nil))
                       (kill-buffer buf)))))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch_feature("cl-macs", &form),
        Value::list([Value::String("new".into()), Value::Nil])
    );
    let _ = fs::remove_file(path);
}

#[test]
fn find_file_sets_buffer_local_default_directory() {
    let directory = std::env::temp_dir().join(format!(
        "emaxx-default-directory-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::create_dir(&directory).expect("create default-directory test directory");
    let path = directory.join("visited.txt");
    fs::write(&path, "visited").expect("write default-directory test file");
    let path_text = path.to_string_lossy();
    let expected = format!("{}/", directory.to_string_lossy());
    let form = format!(
        r#"(let ((buf (find-file-noselect "{path_text}")))
              (prog1 (with-current-buffer buf default-directory)
                (kill-buffer buf)))"#
    );
    assert_string_value(eval_str_with_upstream_batch(&form), &expected);
    let _ = fs::remove_file(path);
    let _ = fs::remove_dir(directory);
}

#[test]
fn dired_revert_refreshes_directory_listing() {
    let directory = std::env::temp_dir().join(format!(
        "emaxx-dired-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::create_dir(&directory).expect("create dired test directory");
    let file = directory.join("listed-file");
    fs::write(&file, "contents").expect("write dired test file");
    let directory_text = directory.to_string_lossy();
    let form = format!(
        r#"(let ((buf (dired-noselect "{directory_text}/")))
                 (with-current-buffer buf
                   (let ((before (string-match-p "listed-file" (buffer-string))))
                     (delete-file "{directory_text}/listed-file")
                     (list (not (null before))
                           (dired-buffer-stale-p)
                           (progn
                             (revert-buffer 'ignore-auto 'dont-ask 'preserve-modes)
                             (string-match-p "listed-file" (buffer-string)))))))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&form),
        Value::list([Value::T, Value::T, Value::Nil])
    );
    let _ = fs::remove_dir_all(directory);
}

#[test]
fn file_notifications_drive_global_auto_revert_without_polling() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-notify-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, "").expect("create notification test file");
    let path_text = path.to_string_lossy();
    let form = format!(
        r#"(progn
                 (require 'autorevert)
                 (let ((auto-revert-use-notify t)
                       (auto-revert-avoid-polling t)
                       (auto-revert-notify-exclude-dir-regexp "nothing-to-be-excluded")
                       (buf (find-file-noselect "{path_text}")))
                   (unwind-protect
                       (with-current-buffer buf
                         (global-auto-revert-mode 1)
                         (let ((desc auto-revert-notify-watch-descriptor))
                           (write-region "changed" nil "{path_text}" nil 'no-message)
                           ;; Notifications are delivered when the command
                           ;; loop goes idle, as in GNU Emacs.
                           (sleep-for 0)
                           (list (eq file-notify--library 'kqueue)
                                 (file-notify-valid-p desc)
                                 (equal desc
                                        (buffer-local-value
                                         'auto-revert-notify-watch-descriptor
                                         buf))
                                 (buffer-string))))
                     (global-auto-revert-mode 0)
                     (kill-buffer buf))))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&form),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
            Value::String("changed".into()),
        ])
    );
    let _ = fs::remove_file(path);
}

#[test]
fn file_notifications_keep_callbacks_isolated_and_invalidate_deleted_paths() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-notify-watch-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::create_dir(&root).expect("create notification root");
    let file = root.join("watched-file");
    let directory = root.join("watched-directory");
    fs::write(&file, "contents").expect("create watched file");
    fs::create_dir(&directory).expect("create watched directory");
    let file = serde_json::to_string(&file.display().to_string()).unwrap();
    let directory = serde_json::to_string(&directory.display().to_string()).unwrap();
    let form = format!(
        r#"(progn
             (require 'filenotify)
             (let (events first second directory-watch)
               (setq first
                     (file-notify-add-watch
                      {file} '(change)
                      (lambda (event)
                        (when (eq (cadr event) 'deleted)
                          (push 1 events))))
                     second
                     (file-notify-add-watch
                      {file} '(change)
                      (lambda (event)
                        (when (eq (cadr event) 'deleted)
                          (push 2 events)))))
               (file-notify-rm-watch first)
               (delete-file {file})
               (sleep-for 0)
               (setq directory-watch
                     (file-notify-add-watch
                      {directory} '(change)
                      (lambda (event)
                        (when (eq (cadr event) 'deleted)
                          (push 'directory events)))))
               (delete-directory {directory})
               (sleep-for 0)
               (list (reverse events)
                     (file-notify-valid-p first)
                     (file-notify-valid-p second)
                     (file-notify-valid-p directory-watch))))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&form),
        Value::list([
            Value::list([Value::Integer(2), Value::symbol("directory")]),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn file_notifications_do_not_replay_events_to_later_watches() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-notify-generation-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, "initial").expect("create notification generation test file");
    let path_literal = serde_json::to_string(&path.display().to_string()).unwrap();
    let form = format!(
        r#"(progn
             (require 'filenotify)
             (let (events first second)
               ;; Queue an event before either watch exists.  It must not be
               ;; delivered retroactively when the command loop next idles.
               (write-region "before" nil {path_literal} nil 'no-message)
               (setq first
                     (file-notify-add-watch
                      {path_literal} '(change)
                      (lambda (_event) (push 'first events)))
                     second
                     (file-notify-add-watch
                      {path_literal} '(change)
                      (lambda (_event) (push 'second events))))
               (sleep-for 0)
               (let ((before events))
                 (write-region "after" nil {path_literal} nil 'no-message)
                 (sleep-for 0)
                 (prog1
                     (list before
                           (length events)
                           (not (null (memq 'first events)))
                           (not (null (memq 'second events))))
                   (file-notify-rm-watch first)
                   (file-notify-rm-watch second)))))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&form),
        Value::list([Value::Nil, Value::Integer(2), Value::T, Value::T])
    );
    let _ = fs::remove_file(path);
}

#[test]
fn file_notifications_observe_changes_made_outside_the_interpreter() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-notify-external-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, "before").expect("create external notification test file");
    let path_literal = serde_json::to_string(&path.display().to_string()).unwrap();
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    eval_str_with(
        &mut interp,
        &format!(
            r#"(progn
                 (require 'filenotify)
                 (defvar emaxx-external-notify-events nil)
                 (defvar emaxx-external-notify-watch
                   (file-notify-add-watch
                    {path_literal} '(change)
                    (lambda (event)
                      (push (cadr event) emaxx-external-notify-events)))))"#
        ),
    );

    // Bypass every Lisp file primitive, just as an editor, compiler, or VCS
    // subprocess would when replacing a visited file.
    fs::write(&path, "changed by another process")
        .expect("change watched file outside the interpreter");
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (read-event nil nil 0.1)
               (prog1 emaxx-external-notify-events
                 (file-notify-rm-watch emaxx-external-notify-watch)))",
        ),
        Value::list([Value::symbol("changed")])
    );
    let _ = fs::remove_file(path);
}

#[test]
fn global_auto_revert_adopts_files_opened_after_enable() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-notify-late-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, "").expect("create late notification test file");
    let path_text = path.to_string_lossy();
    let form = format!(
        r#"(progn
                 (require 'autorevert)
                 (let ((auto-revert-use-notify t)
                       (auto-revert-avoid-polling t)
                       (auto-revert-notify-exclude-dir-regexp "nothing-to-be-excluded"))
                   (unwind-protect
                       (progn
                         (global-auto-revert-mode 1)
                         (let ((buf (find-file-noselect "{path_text}")))
                           (with-current-buffer buf
                             (auto-revert-buffers)
                             (not (null auto-revert-notify-watch-descriptor)))))
                     (global-auto-revert-mode 0))))"#
    );
    assert_eq!(eval_str_with_upstream_batch(&form), Value::T);
    let _ = fs::remove_file(path);
}

#[test]
fn make_indirect_buffer_clone_copies_buffer_local_modes() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((base (get-buffer-create " indirect-base")))
                     (with-current-buffer base
                       (setq-local sample-mode t)
                       (let ((cloned (make-indirect-buffer base " indirect-clone" 'clone))
                             (plain (make-indirect-buffer base " indirect-plain" nil)))
                         (unwind-protect
                             (list (buffer-local-value 'sample-mode cloned)
                                   (local-variable-p 'sample-mode cloned)
                                   (local-variable-p 'sample-mode plain))
                           (kill-buffer cloned)
                           (kill-buffer plain)
                           (kill-buffer base)))))"#
        ),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn make_indirect_buffer_runs_local_clone_hooks_in_the_new_buffer() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((base (get-buffer-create " indirect-hook-base")))
                 (unwind-protect
                     (with-current-buffer base
                       (setq-local sample-clone-marker (point-min-marker))
                       (add-hook
                        'clone-indirect-buffer-hook
                        (lambda ()
                          (setq-local
                           sample-clone-marker
                           (copy-marker
                            (marker-position sample-clone-marker))))
                        nil t)
                       (let ((cloned
                              (make-indirect-buffer
                               base " indirect-hook-clone" 'clone)))
                         (unwind-protect
                             (list
                              (eq (marker-buffer sample-clone-marker) base)
                              (with-current-buffer cloned
                                (eq (marker-buffer sample-clone-marker)
                                    cloned))
                              (eq (current-buffer) base))
                           (kill-buffer cloned))))
                   (kill-buffer base)))"#
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn make_indirect_buffer_preserves_text_point_and_restriction() {
    assert_eq!(
        eval_str(
            r#"(let ((base (get-buffer-create " indirect-restriction-base")))
                     (with-current-buffer base
                       (erase-buffer)
                       (insert "header\nfirst\nsecond\n")
                       (goto-char 8)
                       (narrow-to-region 8 20)
                       (let ((clone (make-indirect-buffer base " indirect-restriction-clone")))
                         (unwind-protect
                             (with-current-buffer clone
                               (list (buffer-string)
                                     (point)
                                     (point-min)
                                     (point-max)))
                           (kill-buffer clone)
                           (kill-buffer base)))))"#
        ),
        Value::list([
            Value::String("first\nsecond".into()),
            Value::Integer(8),
            Value::Integer(8),
            Value::Integer(20),
        ])
    );
}

#[test]
fn make_indirect_buffer_does_not_visit_the_base_buffers_file() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-indirect-file-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, "shared text").expect("create indirect buffer source file");
    let path_literal = serde_json::to_string(&path.display().to_string()).unwrap();
    let form = format!(
        r#"(let* ((base (find-file-noselect {path_literal}))
                  (plain (make-indirect-buffer base " indirect-file-plain"))
                  (clone (make-indirect-buffer base " indirect-file-clone" 'clone)))
             (unwind-protect
                 (list (with-current-buffer base buffer-file-name)
                       (with-current-buffer plain
                         (list buffer-file-name buffer-file-truename
                               (buffer-modified-p)))
                       (with-current-buffer clone
                         (list buffer-file-name buffer-file-truename
                               (buffer-modified-p))))
               (kill-buffer plain)
               (kill-buffer clone)
               (kill-buffer base)))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&form),
        Value::list([
            Value::String(path.display().to_string().into()),
            Value::list([Value::Nil, Value::Nil, Value::Nil]),
            Value::list([Value::Nil, Value::Nil, Value::Nil]),
        ])
    );
    let _ = fs::remove_file(path);
}

#[test]
fn buffer_size_ignores_narrowing_like_emacs() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                     (insert "abcdef")
                     (narrow-to-region 2 4)
                     (list (buffer-size) (point-min) (point-max)))"#
        ),
        Value::list([Value::Integer(6), Value::Integer(2), Value::Integer(4)])
    );
}

#[test]
fn buffer_auto_revert_by_notification_defaults_to_nil() {
    assert_eq!(
        eval_str_with_upstream_batch("buffer-auto-revert-by-notification"),
        Value::Nil
    );
}

#[test]
fn format_spec_applies_width_precision_and_flags() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(format-spec "%2a%-3b%.1p%%" '((?a . "") (?b . "-") (?p . "99")))"#
        ),
        Value::String("  -  9%".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(format-spec "%2a%-3b%.1p%%" '((?b . "-") (?p . "99")) 'delete)"#
        ),
        Value::String("-  9%".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(format-spec "%^a %_b %04c %<3d %>3e" '((?a . "abc") (?b . "XYZ") (?c . "7") (?d . "abcdef") (?e . "abcdef")))"#
        ),
        Value::String("ABC xyz 0007 def abc".into())
    );
}

#[test]
fn format_spec_supports_function_values_and_split() {
    assert_eq!(
        eval_str_with_upstream_batch(r#"(format-spec "a%xb" `((?x . ,(lambda () "X"))) nil t)"#),
        Value::list([
            Value::String("a".into()),
            Value::String("X".into()),
            Value::String("b".into()),
        ])
    );
}

#[test]
fn format_spec_renders_buffers_with_princ_semantics() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r##"(with-temp-buffer
                 (rename-buffer "#format-spec-buffer")
                 (format-spec "buffer=%b" `((?b . ,(current-buffer)))))"##
        ),
        Value::String("buffer=#format-spec-buffer".into())
    );
}

#[test]
fn custom_add_choice_extends_choice_types_without_duplicates() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                     (defcustom sample-choice t "Sample."
                       :type '(choice (const :tag "One" one)))
                     (custom-add-choice 'sample-choice '(const :tag "Two" two))
                     (custom-add-choice 'sample-choice '(const :tag "Two" duplicate))
                     (get 'sample-choice 'custom-type))"#
        ),
        // GNU dedups by the complete member, so a same-tag different-value
        // const is still added (probed on GNU 30.2).
        Value::list([
            Value::Symbol("choice".into()),
            Value::list([
                Value::Symbol("const".into()),
                Value::Symbol(":tag".into()),
                Value::String("One".into()),
                Value::Symbol("one".into()),
            ]),
            Value::list([
                Value::Symbol("const".into()),
                Value::Symbol(":tag".into()),
                Value::String("Two".into()),
                Value::Symbol("two".into()),
            ]),
            Value::list([
                Value::Symbol("const".into()),
                Value::Symbol(":tag".into()),
                Value::String("Two".into()),
                Value::Symbol("duplicate".into()),
            ]),
        ])
    );
}

#[test]
fn custom_add_option_records_unique_options() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                     (defcustom sample-hook nil "Sample." :type 'hook)
                     (custom-add-option 'sample-hook 'first)
                     (custom-add-option 'sample-hook 'first)
                     (custom-add-option 'sample-hook 'second)
                     (list (get 'sample-hook 'custom-options)
                           (get 'sample-hook 'custom-type)))"#
        ),
        Value::list([
            // GNU `custom-add-option' pushes, so the newest option leads.
            Value::list([
                Value::Symbol("second".into()),
                Value::Symbol("first".into())
            ]),
            Value::Symbol("hook".into()),
        ])
    );
}

#[test]
fn tab_bar_new_tab_choice_has_preloaded_custom_type() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                     (custom-add-choice 'tab-bar-new-tab-choice
                                        '(const :tag "Bookmark List" bookmark-bmenu-get-buffer))
                     (assoc 'const (cdr (get 'tab-bar-new-tab-choice 'custom-type))))"#
        ),
        Value::list([
            Value::Symbol("const".into()),
            Value::Symbol(":tag".into()),
            Value::String("Current buffer".into()),
            Value::T,
        ])
    );
}

#[test]
fn chinese_gb18030_is_accepted_for_decode_coding_string() {
    assert_eq!(
        eval_str_with_upstream_batch(r#"(coding-system-p 'chinese-gb18030)"#),
        Value::T
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(stringp (decode-coding-string "\xE3\x32\x9A\x36" 'chinese-gb18030))"#
        ),
        Value::T
    );
}

#[test]
fn select_safe_coding_system_uses_default_candidates() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(select-safe-coding-system (point-min) (point-max) (list t 'utf-8-emacs))"
        ),
        Value::Symbol("utf-8-emacs-unix".into())
    );
}

#[test]
fn find_coding_systems_region_internal_accepts_positions_and_exclusions() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (with-temp-buffer
                   (insert "ascii")
                   (let ((ascii
                          (find-coding-systems-region-internal
                           (point-min) (point-max))))
                     (erase-buffer)
                     (insert "❄")
                     (let ((all
                            (find-coding-systems-region-internal
                             (point-min) (point-max)))
                           (excluded
                            (find-coding-systems-region-internal
                             (point-min) (point-max) '(utf-8))))
                       (list ascii
                             (not (null (memq 'utf-8 all)))
                             (not (null (memq 'utf-8 excluded))))))))"#
        ),
        Value::list([Value::T, Value::T, Value::Nil,])
    );
}

#[test]
#[cfg(unix)]
fn user_and_group_identity_primitives_report_effective_and_real_ids() {
    assert_eq!(
        eval_str("(list (user-uid) (user-real-uid) (group-gid) (group-real-gid))"),
        Value::list([
            // SAFETY: these POSIX identity accessors have no preconditions.
            Value::Integer(unsafe { libc::geteuid() } as i64),
            // SAFETY: these POSIX identity accessors have no preconditions.
            Value::Integer(unsafe { libc::getuid() } as i64),
            // SAFETY: these POSIX identity accessors have no preconditions.
            Value::Integer(unsafe { libc::getegid() } as i64),
            // SAFETY: these POSIX identity accessors have no preconditions.
            Value::Integer(unsafe { libc::getgid() } as i64),
        ])
    );
}

#[test]
fn utf8_decoding_preserves_invalid_bytes_as_raw_chars() {
    let decoded = eval_str_with_upstream_batch(r#"(decode-coding-string "\xe3\x32" 'utf-8)"#);
    assert_eq!(primitives::string_text(&decoded).unwrap(), "\u{e0e3}2");
}

#[test]
fn decode_char_supports_eight_bit_charset() {
    assert_eq!(
        eval_str(
            r#"(list (charsetp 'eight-bit)
                        (decode-char 'eight-bit #x41)
                        (decode-char 'eight-bit #x80)
                        (encode-char #x3fff80 'eight-bit)
                        (char-charset (decode-char 'eight-bit #x81))
                        (stringp (char-to-string (decode-char 'eight-bit #x81))))"#
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::Integer(0x3fff80),
            Value::Integer(0x80),
            Value::Symbol("eight-bit".into()),
            Value::T,
        ])
    );
}

#[test]
fn glyphless_char_display_defaults_to_char_table() {
    assert_eq!(
        eval_str(
            r#"(list (char-table-p glyphless-char-display)
                              (char-table-subtype glyphless-char-display))"#
        ),
        Value::list([Value::T, Value::Symbol("glyphless-char-display".into()),])
    );
}

#[test]
fn header_line_indent_mode_defaults_to_nil() {
    assert_eq!(eval_str("header-line-indent-mode"), Value::Nil);
}

#[test]
fn header_line_indent_mode_sets_buffer_local_state() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                     (header-line-indent-mode)
                     (list header-line-indent-mode
                           (string= header-line-indent "")
                           header-line-indent-width
                           (local-variable-p 'header-line-indent-mode)))"#
        ),
        Value::list([Value::T, Value::T, Value::Integer(0), Value::T,])
    );
}

#[test]
fn bidi_string_mark_left_to_right_marks_rtl_strings() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(list (bidi-string-mark-left-to-right "abc")
                              (category-set-mnemonics (char-category-set ?א))
                              (string-match "\\cR" "א")
                              (length (bidi-string-mark-left-to-right "א")))"#
        ),
        Value::list([
            Value::String("abc".into()),
            Value::String(".R".into()),
            Value::Integer(0),
            Value::Integer(2),
        ])
    );
}

#[test]
fn insert_file_contents_visit_marks_buffer_as_visiting_file() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-insert-visit-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, "visited").expect("create insert visit test file");
    let path_text = path.to_string_lossy();
    let form = format!(
        r#"(let ((buf (generate-new-buffer " insert-visit")))
                 (unwind-protect
                     (with-current-buffer buf
                       (insert-file-contents "{path_text}" 'visit)
                       (list buffer-file-name
                             (buffer-modified-p)
                             (verify-visited-file-modtime buf)))
                   (kill-buffer buf)))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&form),
        Value::list([
            Value::String(path_text.to_string().into()),
            Value::Nil,
            Value::T
        ])
    );
    let _ = fs::remove_file(path);
}

#[test]
fn set_visited_file_name_clears_the_recorded_modtime() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-set-visited-modtime-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, b"\0binary\r\nbytes").expect("create set-visited source file");
    let path_text = path.to_string_lossy();
    let form = format!(
        r#"(with-temp-buffer
              (insert-file-contents-literally "{path_text}")
              (set-visited-file-name "{path_text}")
              (set-buffer-modified-p nil)
              (list (visited-file-modtime)
                    (verify-visited-file-modtime (current-buffer))))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&form),
        Value::list([Value::Integer(0), Value::T])
    );
    let _ = fs::remove_file(path);
}

#[test]
fn no_conversion_file_reads_preserve_crlf_bytes() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-no-conversion-read-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, b"first\r\nsecond\r\n").expect("create CRLF source file");
    let path_text = path.to_string_lossy();
    let form = format!(
        r#"(with-temp-buffer
              (let ((coding-system-for-read 'no-conversion))
                (insert-file-contents "{path_text}"))
              (buffer-string))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&form),
        Value::String("first\r\nsecond\r\n".into())
    );
    let _ = fs::remove_file(path);
}

#[test]
fn revert_buffer_refreshes_related_indirect_buffers() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-indirect-revert-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, "old").expect("create indirect revert test file");
    let path_text = path.to_string_lossy();
    let form = format!(
        r#"(let* ((base (find-file-noselect "{path_text}"))
                      (clone (make-indirect-buffer base " indirect-revert-clone" 'clone)))
                 (unwind-protect
                     (with-current-buffer base
                       (write-region "new" nil "{path_text}" nil 'no-message)
                       (revert-buffer 'ignore-auto 'dont-ask 'preserve-modes)
                       (list (buffer-string)
                             (with-current-buffer clone (buffer-string))))
                   (kill-buffer clone)
                   (kill-buffer base)))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&form),
        Value::list([Value::String("new".into()), Value::String("new".into())])
    );
    let _ = fs::remove_file(path);
}

#[test]
fn time_convert_list_accepts_float_precision_loss_like_emacs() {
    assert_eq!(
        eval_str("(time-convert 0.1 'list)"),
        Value::list([
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(100000),
            Value::Integer(0),
        ])
    );
    assert_eq!(
        eval_str("(time-convert -0.1 'list)"),
        Value::list([
            Value::Integer(-1),
            Value::Integer(65535),
            Value::Integer(899999),
            Value::Integer(999999),
        ])
    );
}

#[test]
fn call_interactively_records_declared_history_arguments() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    eval_str_with(
        &mut interp,
        "(defun callint-test-int-args (foo bar &optional zot) \
               (declare (interactive-args (bar 10) (zot 11))) \
               (interactive (list 1 1 1)) \
               (+ foo bar zot))",
    );
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((history-length 1) (command-history ())) \
                   (list (function-get 'callint-test-int-args 'interactive-args) \
                         (call-interactively 'callint-test-int-args t) \
                         command-history))"
        ),
        Value::list([
            Value::list([
                Value::cons(Value::Integer(1), Value::Integer(10)),
                Value::cons(Value::Integer(2), Value::Integer(11)),
            ]),
            Value::Integer(3),
            Value::list([Value::list([
                Value::Symbol("callint-test-int-args".into()),
                Value::Integer(1),
                Value::Integer(10),
                Value::Integer(11),
            ])]),
        ])
    );
}

#[test]
fn call_interactively_reads_region_mark_and_point_codes() {
    // GNU batch answers: (3 8) for "r" with mark 3 and point 8; the two
    // check_mark error messages differ between "m" and "r".
    assert_eq!(
        eval_str(
            "(progn
               (insert \"hello world\")
               (set-marker (mark-marker) 3)
               (goto-char 8)
               (call-interactively (lambda (b e) (interactive \"r\") (list b e))))"
        ),
        Value::list([Value::Integer(3), Value::Integer(8)])
    );
    assert_eq!(
        eval_str(
            "(progn
               (insert \"abc\")
               (goto-char 2)
               (call-interactively (lambda (d) (interactive \"d\") d)))"
        ),
        Value::Integer(2)
    );
    assert_eq!(
        eval_str(
            "(cadr (condition-case e
                       (call-interactively (lambda (m) (interactive \"m\") m))
                     (error e)))"
        ),
        Value::String("The mark is not set now".into())
    );
    assert_eq!(
        eval_str(
            "(cadr (condition-case e
                       (call-interactively (lambda (b e) (interactive \"r\") (list b e)))
                     (error e)))"
        ),
        Value::String("The mark is not set now, so there is no region".into())
    );
}

#[test]
fn call_interactively_star_flag_barfs_on_read_only_buffers() {
    assert_eq!(
        eval_str(
            "(progn
               (setq buffer-read-only t)
               (car (condition-case e
                        (call-interactively (lambda (n) (interactive \"*p\") n))
                      (error e))))"
        ),
        Value::Symbol("buffer-read-only".into())
    );
}

#[test]
fn call_interactively_rejects_invalid_control_letters() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            "(cdr (should-error (call-interactively (lambda () (interactive \"ÿ\")))))"
        ),
        Value::list([Value::String(
            "Invalid control letter `ÿ' (#o377, #x00ff) in interactive calling string".into(),
        )])
    );
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"(cdr (should-error (call-interactively (lambda () (interactive "\xFF")))))"#
        ),
        Value::list([Value::String(
            "Invalid control letter `ÿ' (#o377, #x00ff) in interactive calling string".into(),
        )])
    );
}

#[test]
fn call_interactively_follows_symbol_aliases_for_interactive_specs() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    eval_str_with(
        &mut interp,
        "(defun sample-callint-target (arg)
               (interactive (list 7))
               arg)",
    );
    eval_str_with(
        &mut interp,
        "(defalias 'sample-callint-alias 'sample-callint-target)",
    );
    assert_eq!(
        eval_str_with(&mut interp, "(call-interactively 'sample-callint-alias)"),
        Value::Integer(7)
    );
}

#[test]
fn string_and_region_upcase_share_unicode_special_case_mappings() {
    assert_eq!(
        // GNU 30.2 defines `get-char-code-property' in the Elisp-owned,
        // dumped international/mule-cmds.el.  Exercise that real owner from
        // the reconstructed batch image; a bare interpreter must not grow a
        // Rust substitute merely to make this assertion callable.
        eval_str_with_upstream_batch(
            r#"(with-temp-buffer
                  (insert "Straße ﬁsh")
                  (let ((string (upcase (buffer-string))))
                    (upcase-region (point-min) (point-max))
                    (list string
                          (buffer-string)
                          (get-char-code-property ?ß 'special-uppercase))))"#
        ),
        Value::list([
            Value::String("STRASSE FISH".into()),
            Value::String("STRASSE FISH".into()),
            Value::String("SS".into()),
        ])
    );
}

#[test]
fn loaded_coding_registry_preserves_native_bounds_bom_and_error_contracts() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(list
                 (check-coding-systems-region
                  "aåbγc" nil '(utf-8 iso-latin-1 us-ascii))
                 (encode-char ?γ 'iso-8859-1)
                 (condition-case err
                     (let ((coding-system-for-read 'bogus))
                       (insert-file-contents "/definitely/missing/emaxx")
                       'no-error)
                   (error (car err)))
                 (let ((inhibit-eol-conversion t))
                   (equal (encode-coding-string "a\nb" 'utf-8-dos)
                          "a\nb"))
                 (let* ((string (apply #'string (number-sequence 0 127)))
                        (inhibit-eol-conversion t))
                   (eq (decode-coding-string string 'us-ascii t) string)))"#,
        ),
        Value::list([
            Value::list([
                Value::list([Value::Symbol("iso-latin-1".into()), Value::Integer(3),]),
                Value::list([
                    Value::Symbol("us-ascii".into()),
                    Value::Integer(1),
                    Value::Integer(3),
                ]),
            ]),
            Value::Nil,
            Value::Symbol("coding-system-error".into()),
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn zlib_decompress_region_uses_unibyte_octets_and_is_transactional() {
    let gzip = "'(31 139 8 8 204 39 9 82 0 3 115 109 97 108 108 0 75 203 207 231 2 0 168 101 50 126 4 0 0 0)";
    assert_eq!(
        eval_str(&format!(
            r#"(let ((bytes {gzip}))
                  (list
                   (with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert (apply #'unibyte-string bytes))
                     (list (zlib-decompress-region (point-min) (point-max))
                           (buffer-string)))
                   (with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert (apply #'unibyte-string (butlast bytes 4)))
                     (let ((original (buffer-string)))
                       (list (zlib-decompress-region (point-min) (point-max))
                             (equal original (buffer-string)))))
                   (with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert (apply #'unibyte-string (butlast bytes 4)))
                     (list (zlib-decompress-region (point-min) (point-max) t)
                           (buffer-string)))
                   (with-temp-buffer
                     (condition-case err
                         (zlib-decompress-region (point-min) (point-max))
                       (error (car err))))))"#
        )),
        Value::list([
            Value::list([Value::T, Value::String("foo\n".into())]),
            Value::list([Value::Nil, Value::T]),
            Value::list([Value::Integer(0), Value::String("foo\n".into())]),
            Value::Symbol("error".into()),
        ])
    );
}

#[test]
fn editfns_edge_contracts_preserve_float_character_and_undo_semantics() {
    assert_eq!(
        eval_str(
            r#"(let ((value 18446744073709551616.0))
                  (list (number-to-string value)
                        (prin1-to-string value)
                        (= value (read (format "%s" value)))))"#,
        ),
        Value::list([
            Value::String("1.8446744073709552e+19".into()),
            Value::String("1.8446744073709552e+19".into()),
            Value::T,
        ])
    );
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (let ((table (make-char-table 'translation-table)))
                 (aset table #x3fffff ?*)
                 (insert #x3fffff)
                 (list (translate-region-internal (point-min) (point-max) table)
                       (buffer-string))))",
        ),
        Value::list([Value::Integer(1), Value::String("*".into())])
    );
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert \"1234567890\")
               (setq buffer-undo-list nil)
               (let ((before-change-functions
                      (list (lambda (beg end)
                              (delete-region (1- beg) (1+ end))))))
                 (delete-region 2 5))
               (list (buffer-string)
                     (mapcar (lambda (entry) (type-of (car entry)))
                             buffer-undo-list)))",
        ),
        Value::list([
            Value::String("90".into()),
            Value::list([
                Value::Symbol("string".into()),
                Value::Symbol("string".into()),
                Value::Symbol("marker".into()),
                Value::Symbol("marker".into()),
                Value::Symbol("marker".into()),
            ]),
        ])
    );
}

#[test]
fn non_unicode_buffer_characters_are_typed_state_not_lisp_properties() {
    assert_eq!(
        eval_str(
            r#"
                (let ((source (get-buffer-create "typed-character-source"))
                      (string-target (get-buffer-create "typed-character-string-target"))
                      (buffer-target (get-buffer-create "typed-character-buffer-target")))
                  (set-buffer source)
                  (erase-buffer)
                  (insert "x" #x3fffff "y")
                  (let ((slice (buffer-substring-no-properties 2 3)))
                    (goto-char 1)
                    (insert "p")
                    (delete-region 1 2)
                    (list
                     (list (char-after 2)
                           (text-properties-at 2)
                           (aref (buffer-string) 1))
                     (progn
                       (set-buffer string-target)
                       (erase-buffer)
                       (insert slice)
                       (list (char-after 1) (text-properties-at 1)))
                     (progn
                       (set-buffer buffer-target)
                       (erase-buffer)
                       (insert-buffer-substring source 2 3)
                       (list (char-after 1) (text-properties-at 1))))))
            "#,
        ),
        Value::list([
            Value::list([
                Value::Integer(0x3f_ffff),
                Value::Nil,
                Value::Integer(0x3f_ffff)
            ]),
            Value::list([Value::Integer(0x3f_ffff), Value::Nil]),
            Value::list([Value::Integer(0x3f_ffff), Value::Nil]),
        ])
    );
}

#[test]
fn batch_error_snapshot_keeps_deep_frames_and_signal_site_policy() {
    // `defun' is byte-run.el's macro, so the native snapshot policy is
    // observed through the GNU early-Lisp runtime rather than a bare host.
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Env::new();
    let form = Reader::new(
        "(progn
           (defun batch-error-snapshot-inner () (error \"Boo\"))
           (let ((backtrace-on-error-noninteractive nil))
             (batch-error-snapshot-inner)))",
    )
    .read_all()
    .unwrap()
    .pop()
    .unwrap();
    assert!(interp.eval(&form, &mut env).is_err());
    let snapshot = interp
        .take_batch_error_backtrace()
        .expect("unhandled evaluation keeps its deepest frame snapshot");
    assert!(!snapshot.enabled);
    assert!(snapshot.frames.iter().any(|(_, function, _, _)| {
        matches!(function, Value::Symbol(name) if name == "batch-error-snapshot-inner")
    }));

    let form = Reader::new(
        "(progn
           (defun batch-error-snapshot-first () (error \"first\"))
           (defun batch-error-snapshot-second () (error \"second\"))
           (condition-case nil (batch-error-snapshot-first) (error nil))
           (batch-error-snapshot-second))",
    )
    .read_all()
    .unwrap()
    .pop()
    .unwrap();
    assert!(interp.eval(&form, &mut env).is_err());
    let snapshot = interp
        .take_batch_error_backtrace()
        .expect("a handled error cannot mask the later unhandled snapshot");
    assert!(snapshot.frames.iter().any(|(_, function, _, _)| {
        matches!(function, Value::Symbol(name) if name == "batch-error-snapshot-second")
    }));
    assert!(!snapshot.frames.iter().any(|(_, function, _, _)| {
        matches!(function, Value::Symbol(name) if name == "batch-error-snapshot-first")
    }));
}

#[cfg(unix)]
#[test]
fn local_symlink_targets_are_data_not_file_name_handler_candidates() {
    run_with_large_stack(|| {
        let root = std::env::temp_dir().join(format!(
            "emaxx-symlink-target-data-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let link = root.join("link");
        let expression = format!(
            "(progn
               (require 'files)
               (make-symbolic-link \"/:\" {:?})
               (file-symlink-p {:?}))",
            link.display().to_string(),
            link.display().to_string(),
        );
        let actual = eval_str_with_upstream_batch(&expression);
        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(actual, Value::String("/:".into()));
    });
}

#[cfg(unix)]
#[test]
fn call_process_region_can_delete_entire_buffer() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((shell (executable-find \"sh\"))) \
                   (with-temp-buffer \
                     (insert \"Buffer contents\\n\") \
                     (list \
                       (call-process-region nil nil shell :delete nil nil \"-c\" \"cat >/dev/null\") \
                       (buffer-size))))"
        ),
        Value::list([Value::Integer(0), Value::Integer(0)])
    );
}

#[test]
fn call_process_missing_program_signals_file_error() {
    assert_eq!(
        eval_str(
            r#"
                (condition-case nil
                    (call-process "/definitely/missing/emaxx-program" nil nil nil)
                  (file-error 'caught)
                  (error 'wrong-condition))
                "#
        ),
        Value::Symbol("caught".into())
    );
}

#[test]
fn ignore_error_catches_requested_conditions() {
    assert_eq!(
        eval_str(
            r#"(list
                     (ignore-error wrong-type-argument
                       (signal 'wrong-type-argument nil))
                     (condition-case nil
                         (ignore-error search-failed
                           (signal 'wrong-type-argument nil))
                       (wrong-type-argument 'caught)))"#
        ),
        Value::list([Value::Nil, Value::Symbol("caught".into())])
    );
}

#[test]
fn encode_coding_region_returns_region_text_when_requested() {
    assert_string_value(
        eval_str_with_upstream_batch(
            r#"(with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert "ABC")
                     (encode-coding-region (point-min) (point-max) 'binary t))"#,
        ),
        "ABC",
    );
}

#[test]
fn encode_coding_region_binary_returns_unibyte_string() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(with-temp-buffer
                     (set-buffer-multibyte t)
                     (insert (string #x89 ?A))
                     (let ((encoded (encode-coding-region
                                     (point-min) (point-max) 'binary t)))
                       (list (string-to-list encoded)
                             (multibyte-string-p encoded))))"#
        ),
        Value::list([
            Value::list([Value::Integer(137), Value::Integer(65)]),
            Value::Nil,
        ])
    );
}

#[test]
fn buffer_character_primitives_project_raw_bytes_like_gnu() {
    assert_eq!(
        eval_str(
            r#"(list
                 (with-temp-buffer
                   (set-buffer-multibyte nil)
                   (insert (unibyte-string 255 216))
                   (goto-char (point-min))
                   (list (following-char)
                         (char-after)
                         (progn (forward-char 1) (preceding-char))
                         (char-before)))
                 (with-temp-buffer
                   (insert (unibyte-string 255))
                   (goto-char (point-min))
                   (following-char)))"#
        ),
        Value::list([
            Value::list([
                Value::Integer(255),
                Value::Integer(255),
                Value::Integer(255),
                Value::Integer(255),
            ]),
            Value::Integer(0x3F_FFFF),
        ])
    );
}

#[test]
#[ignore = "Emaxx substitutes SPACE for an unencodable character where GNU substitutes ?; probed: GNU (115 63 108 32 63 32 63 63 63 63)"]
fn encode_coding_string_substitutes_unencodable_ascii_and_latin1_chars() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(list (string-to-list (encode-coding-string "sæl ö всем" 'iso-8859-1))
                     (string-to-list (encode-coding-string "sæl ö всем" 'ascii)))"#
        ),
        Value::list([
            Value::list([
                Value::Integer(115),
                Value::Integer(230),
                Value::Integer(108),
                Value::Integer(32),
                Value::Integer(246),
                Value::Integer(32),
                Value::Integer(32),
                Value::Integer(32),
                Value::Integer(32),
                Value::Integer(32),
            ]),
            Value::list([
                Value::Integer(115),
                Value::Integer(63),
                Value::Integer(108),
                Value::Integer(32),
                Value::Integer(63),
                Value::Integer(32),
                Value::Integer(63),
                Value::Integer(63),
                Value::Integer(63),
                Value::Integer(63),
            ]),
        ])
    );
}

#[test]
fn frame_predicates_track_the_single_live_frame() {
    assert_eq!(eval_str("(framep (selected-frame))"), Value::T);
    assert_eq!(eval_str("(framep nil)"), Value::Nil);
    assert_eq!(eval_str("(frame-live-p (selected-frame))"), Value::T);
    assert_eq!(eval_str("(frame-live-p nil)"), Value::Nil);
}

#[test]
fn url_insert_entities_in_string_escapes_html_markup_chars() {
    assert_eq!(
        eval_str_with_upstream_batch(r#"(url-insert-entities-in-string "<a b=\"c&d\">")"#),
        Value::String("&lt;a b=&quot;c&amp;d&quot;&gt;".into())
    );
}

#[test]
#[ignore = "EUC-JP encoding is unimplemented (GNU encodes \u{3042} as (164 162)); the codec that stood in for it recognised only that one character"]
fn decode_coding_region_rewrites_dos_eol_in_place() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert (encode-coding-string "あ" 'euc-jp) "\r" "\n")
                     (decode-coding-region (point-min) (point-max) 'euc-jp-dos)
                     (string-search "\r" (buffer-string)))"#
        ),
        Value::Nil
    );
}

#[test]
fn decode_coding_region_detects_undecided_text_and_honors_buffer_width() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((raw "\342\235\204\n"))
                 (list
                  (with-temp-buffer
                    (set-buffer-multibyte t)
                    (insert raw)
                    (goto-char (point-min))
                    (list (decode-coding-region
                           (point-min) (point-max) 'undecided)
                          (char-after (point-min))
                          enable-multibyte-characters
                          last-coding-system-used
                          (string-to-list (buffer-string))))
                  (with-temp-buffer
                    (set-buffer-multibyte nil)
                    (insert raw)
                    (list (decode-coding-region
                           (point-min) (point-max) 'undecided)
                          (buffer-size)
                          enable-multibyte-characters
                          last-coding-system-used
                          (string-to-list (buffer-string))))))"#
        ),
        Value::list([
            Value::list([
                Value::Integer(2),
                Value::Integer(10052),
                Value::T,
                Value::Symbol("utf-8-unix".into()),
                Value::list([Value::Integer(10052), Value::Integer(10)]),
            ]),
            Value::list([
                Value::Integer(2),
                Value::Integer(4),
                Value::Nil,
                Value::Symbol("utf-8-unix".into()),
                Value::list([
                    Value::Integer(226),
                    Value::Integer(157),
                    Value::Integer(132),
                    Value::Integer(10),
                ]),
            ]),
        ])
    );
}

#[test]
fn decode_coding_string_normalizes_dos_eol() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((decoded (decode-coding-string "A\r\n" 'utf-8-dos)))
                     (string-search "\r" decoded))"#
        ),
        Value::Nil
    );
}

#[test]
fn base64_decode_string_ignores_wrapped_input() {
    assert_eq!(
        eval_str(
            r#"(let ((decoded (base64-decode-string "SGVsbG8s
IHdvcmxkIQ==")))
                     (list (string-to-list decoded)
                           (multibyte-string-p decoded)))"#
        ),
        Value::list([
            Value::list([
                Value::Integer(72),
                Value::Integer(101),
                Value::Integer(108),
                Value::Integer(108),
                Value::Integer(111),
                Value::Integer(44),
                Value::Integer(32),
                Value::Integer(119),
                Value::Integer(111),
                Value::Integer(114),
                Value::Integer(108),
                Value::Integer(100),
                Value::Integer(33),
            ]),
            Value::Nil,
        ])
    );
}

#[test]
fn base64_decode_string_returns_unibyte_raw_bytes() {
    assert_eq!(
        eval_str(
            r#"(let ((decoded (base64-decode-string "/wA=")))
                     (list (string-to-list decoded)
                           (multibyte-string-p decoded)))"#
        ),
        Value::list([
            Value::list([Value::Integer(255), Value::Integer(0)]),
            Value::Nil,
        ])
    );
}

#[test]
fn base64_decode_string_supports_url_variant_and_invalid_handling() {
    assert_eq!(
        eval_str(
            r#"(list
                    (base64-decode-string "SGVsbG8" t)
                    (condition-case _
                        (base64-decode-string "!")
                      (error 'caught))
                    (string-bytes (base64-decode-string "!" nil t)))"#
        ),
        Value::list([
            Value::String("Hello".into()),
            Value::Symbol("caught".into()),
            Value::Integer(0),
        ])
    );
}

#[test]
fn base64_decode_region_reports_unibyte_byte_count() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert "FPucA9l+")
                     (let ((len (base64-decode-region (point-min) (point-max))))
                       (list len
                             (string-bytes (buffer-string))
                             (string-to-list (buffer-string))
                             (multibyte-string-p (buffer-string)))))"#
        ),
        Value::list([
            Value::Integer(6),
            Value::Integer(6),
            Value::list([
                Value::Integer(20),
                Value::Integer(251),
                Value::Integer(156),
                Value::Integer(3),
                Value::Integer(217),
                Value::Integer(126),
            ]),
            Value::Nil,
        ])
    );
}

#[test]
fn base64_encode_string_rejects_multibyte_non_ascii_input() {
    assert_eq!(
        eval_str(
            r#"(list
                    (condition-case _ (base64-encode-string "ü") (error 'caught))
                    (condition-case _ (base64url-encode-string "ƒ") (error 'caught)))"#
        ),
        Value::list([
            Value::Symbol("caught".into()),
            Value::Symbol("caught".into())
        ])
    );
}

#[test]
fn sha1_and_buffer_hash_match_for_ascii_buffer_contents() {
    assert_eq!(
        eval_str(
            r#"(list
                    (sha1 "foo")
                    (with-temp-buffer
                      (insert "foo")
                      (buffer-hash)))"#
        ),
        Value::list([
            Value::String("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33".into()),
            Value::String("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33".into()),
        ])
    );
}

#[test]
fn secure_hash_supports_core_algorithms_and_iv_auto() {
    let result = eval_str(
        r#"(list
                (secure-hash 'md5 "foobar")
                (secure-hash 'sha1 "foobar")
                (length (secure-hash 'sha512 'iv-auto 100)))"#,
    );
    let items = result.to_vec().expect("hash result list");
    assert_eq!(
        items[0],
        Value::String("3858f62230ac3c915f300c664312c63f".into())
    );
    assert_eq!(
        items[1],
        Value::String("8843d7f92416211de9ebb963ff4ce28125932878".into())
    );
    assert_eq!(items[2], Value::Integer(128));
}

#[test]
fn format_binary_negative() {
    assert_eq!(
        eval_str(r#"(format "%b" #x-5A)"#),
        Value::String("-1011010".into())
    );
    assert_eq!(
        eval_str(r#"(format "%b" #x5A)"#),
        Value::String("1011010".into())
    );
}

#[test]
fn backquote_dotted_pair() {
    assert_eq!(
        eval_str(r#"(car '(#x-5A . "1011010"))"#),
        Value::Integer(-90)
    );
    assert_eq!(
        eval_str(r#"(cdr '(#x-5A . "1011010"))"#),
        Value::String("1011010".into())
    );
}

#[test]
fn dolist_dotted_pairs() {
    assert_string_value(
        eval_str(
            r#"(let ((result nil))
                     (dolist (pair `((1 . "a") (2 . "b")))
                       (setq result (concat (cdr pair) (or result ""))))
                     result)"#,
        ),
        "ba",
    );
}

#[test]
fn nested_backquote_preserves_inner_unquote() {
    assert_eq!(
        eval_str(
            r#"(progn
                     (defmacro nested-single-comma () ``(,x))
                     (let ((x 1))
                       (nested-single-comma)))"#
        ),
        Value::list([Value::Integer(1)])
    );
}

#[test]
fn spelled_out_backquote_is_ordinary_data_inside_a_template() {
    assert_eq!(
        eval_str(
            r#"(let ((definition '(foo bar)))
                 `(apply (backquote ,definition)))"#,
        ),
        Value::list([
            Value::Symbol("apply".into()),
            Value::list([
                Value::Symbol("backquote".into()),
                Value::list([Value::Symbol("foo".into()), Value::Symbol("bar".into())]),
            ]),
        ])
    );
}

#[test]
fn failed_counted_searches_restore_point_unless_noerror_requests_the_bound() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                 (insert "x x")
                 (list
                  (progn (goto-char 1) (search-forward "x" nil t 3) (point))
                  (progn (goto-char 1) (re-search-forward "x" nil t 3) (point))
                  (progn
                    (goto-char 1)
                    (condition-case nil (search-forward "x" nil nil 3)
                      (search-failed nil))
                    (point))
                  (progn
                    (goto-char 1)
                    (condition-case nil (re-search-forward "x" nil nil 3)
                      (search-failed nil))
                    (point))
                  (progn (goto-char 1) (search-forward "x" nil 'move 3) (point))
                  (progn (goto-char 1) (re-search-forward "x" nil 'move 3) (point))))"#,
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(4),
            Value::Integer(4),
        ])
    );
}

#[test]
fn overlay_enumeration_matches_gnu_interval_tree_order() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                 (insert "abcdef")
                 (let ((a (make-overlay 3 5))
                       (b (make-overlay 1 4))
                       (c (make-overlay 2 6))
                       (d (make-overlay 1 3))
                       (e (make-overlay 1 5)))
                   (mapc (lambda (pair) (overlay-put (car pair) 'name (cdr pair)))
                         (list (cons a 'a) (cons b 'b) (cons c 'c)
                               (cons d 'd) (cons e 'e)))
                   (mapcar (lambda (overlay) (overlay-get overlay 'name))
                           (overlays-in 1 6))))"#,
        ),
        Value::list([
            Value::Symbol("e".into()),
            Value::Symbol("d".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
            Value::Symbol("a".into()),
        ])
    );
}

#[test]
fn nested_backquote_decrements_unquote_depth() {
    let expected = Reader::new("`(,1)")
        .read()
        .expect("read succeeds")
        .expect("form is present");
    // GNU (eval FORM) uses a nil lexical environment, so the variable is
    // supplied through eval's LEXICAL alist argument.
    assert_eq!(eval_str(r#"(eval '``(,,x) '((x . 1)))"#), expected);
}

#[test]
fn residual_reader_comma_evaluates_unquote_operand() {
    // GNU has no `comma' function: evaluating a residual reader comma form
    // signals void-function (probed on GNU 30.2, where `(\, ...)' outside a
    // backquote errors identically).
    assert_eq!(
        eval_str_with_upstream_batch(
            "(condition-case err
                 (let ((mode 'c++-mode)) (comma (if (eq mode 'c++-mode) 'matched 'miss)))
               (void-function (list 'void (cadr err))))"
        ),
        Value::list([Value::Symbol("void".into()), Value::Symbol("comma".into())])
    );
}

#[test]
fn nested_backquote_preserves_inner_splice() {
    assert_eq!(
        eval_str(
            r#"(progn
                     (defmacro nested-splice () ``(,@args ,val))
                     (let ((args '(a i))
                           (val 'v))
                       (nested-splice)))"#
        ),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("i".into()),
            Value::Symbol("v".into()),
        ])
    );
}

#[test]
fn format_binary_nonzero_simple() {
    // Simplified version of the ERT test
    assert_eq!(
        eval_str(
            r#"(let* ((n #x-5A) (bits "1011010")
                          (sgn- (if (< n 0) "-" "")))
                     (concat sgn- bits))"#
        ),
        Value::String("-1011010".into())
    );
    // The actual assertion from the test
    assert_eq!(
        eval_str(
            r#"(let* ((n #x-5A) (bits "1011010")
                          (sgn- (if (< n 0) "-" "")))
                     (string-equal (format "%b" n) (concat sgn- bits)))"#
        ),
        Value::T
    );
}

#[test]
fn format_binary_via_dolist() {
    assert_eq!(
        eval_str(
            r#"(let ((ok t))
                     (dolist (nbits `((#x-5A . "1011010")
                                      (#x5A . "1011010")))
                       (let* ((n (car nbits)) (bits (cdr nbits))
                              (sgn- (if (< n 0) "-" "")))
                         (unless (string-equal (format "%b" n) (concat sgn- bits))
                           (setq ok nil))))
                     ok)"#
        ),
        Value::T
    );
}

#[test]
fn backtick_comma_in_dotted_pair() {
    assert_eq!(
        eval_str(r#"`(#xFFF . ,(make-string 12 ?1))"#),
        Value::cons(Value::Integer(0xFFF), Value::String("111111111111".into()))
    );
}

#[test]
fn backquote_comma_quote_embeds_evaluated_data_as_quoted_literal() {
    assert_eq!(
        eval_str(r#"`',(list (cons '$STARTS 231))"#),
        Value::list([
            Value::Symbol("quote".into()),
            Value::list([Value::cons(
                Value::Symbol("$STARTS".into()),
                Value::Integer(231)
            )])
        ])
    );
}

#[test]
fn generated_forms_can_embed_runtime_vectors_as_self_evaluating_data() {
    assert_eq!(
        eval_str(
            r#"(let ((v (vector (cons '$STARTS 231))))
                 (cdr (aref (aref (eval (list 'vector v)) 0) 0)))"#,
        ),
        Value::Integer(231)
    );
}

#[test]
fn backquote_preserves_record_literal_dotted_pair_tails() {
    let mut interp = gnu_early_lisp_interpreter();
    let value = eval_str_with(&mut interp, r#"`(#s(a 1) . #s(b 2))"#);
    let (left, right) = value.cons_values().expect("dotted pair");
    assert!(matches!(left, Value::Record(_)));
    assert!(matches!(right, Value::Record(_)));
}

#[test]
fn macroexpanded_backquote_preserves_record_literal_dotted_pair_tails() {
    let mut interp = gnu_early_lisp_interpreter();
    let value = eval_str_with(
        &mut interp,
        r#"(eval (macroexpand '`((#s(a 1) . #s(b 2)))))"#,
    );
    let pair = value.car().expect("backquoted list element");
    let (left, right) = pair.cons_values().expect("dotted record pair");
    assert!(matches!(left, Value::Record(_)));
    assert!(matches!(right, Value::Record(_)));
}

#[test]
fn backquote_materializes_record_literals() {
    let mut interp = gnu_early_lisp_interpreter();
    let value = eval_str_with(&mut interp, r#"`(#s(a b) #s(#s(c d) e))"#);
    let items = value.to_vec().expect("backquoted list");
    assert_eq!(items.len(), 2);
    let Value::Record(inner_id) = &items[0] else {
        panic!("expected inner record");
    };
    let inner = interp.find_record(*inner_id).expect("inner record");
    assert_eq!(inner.type_tag, Value::symbol("a"));
    assert_eq!(inner.slots, vec![Value::Symbol("b".into())]);
    let Value::Record(outer_id) = &items[1] else {
        panic!("expected outer record");
    };
    let outer = interp.find_record(*outer_id).expect("outer record");
    assert!(matches!(outer.type_tag, Value::Record(_)));
    assert_eq!(outer.slots, vec![Value::symbol("e")]);
}

#[test]
fn eval_while_loop() {
    assert_eq!(
        eval_str("(let ((x 0)) (while (< x 5) (setq x (1+ x))) x)"),
        Value::Integer(5)
    );
}

#[test]
fn overlays_accept_marker_positions() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "abc")
                  (let* ((overlay (make-overlay (copy-marker 2) (copy-marker 4))))
                    (move-overlay overlay (copy-marker 1) (point-max-marker))
                    (list (overlay-start overlay)
                          (overlay-end overlay)
                          (length (overlays-at (copy-marker 2)))
                          (length (overlays-in (copy-marker 1) (copy-marker 4)))
                          (next-overlay-change (point-min-marker))
                          (previous-overlay-change (point-max-marker)))))"#
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(4),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(4),
            Value::Integer(1),
        ])
    );
}

fn assert_overlay_modification_hooks_record_insert_inside_overlay() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (insert "1234")
                  (let ((overlay (make-overlay 2 4)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (goto-char 3)
                    (insert "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
        ),
        Value::list([
            Value::list([
                Value::Symbol("modification-hooks".into()),
                Value::list([
                    Value::T,
                    Value::Integer(3),
                    Value::Integer(4),
                    Value::Integer(0),
                ]),
            ]),
            Value::list([
                Value::Symbol("modification-hooks".into()),
                Value::list([Value::Nil, Value::Integer(3), Value::Integer(3)]),
            ]),
        ])
    );
}

#[test]
fn overlay_modification_hooks_record_insert_inside_overlay() {
    std::thread::Builder::new()
        .stack_size(4 * 1024 * 1024)
        .spawn(assert_overlay_modification_hooks_record_insert_inside_overlay)
        .unwrap()
        .join()
        .unwrap();
}

fn assert_overlay_modification_hooks_record_insert_at_overlay_start() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (insert "1234")
                  (let ((overlay (make-overlay 2 4)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (goto-char 2)
                    (insert "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
        ),
        Value::list([
            Value::list([
                Value::Symbol("insert-in-front-hooks".into()),
                Value::list([
                    Value::T,
                    Value::Integer(2),
                    Value::Integer(3),
                    Value::Integer(0),
                ]),
            ]),
            Value::list([
                Value::Symbol("insert-in-front-hooks".into()),
                Value::list([Value::Nil, Value::Integer(2), Value::Integer(2)]),
            ]),
        ])
    );
}

#[test]
fn overlay_modification_hooks_record_insert_at_overlay_start() {
    std::thread::Builder::new()
        .stack_size(4 * 1024 * 1024)
        .spawn(assert_overlay_modification_hooks_record_insert_at_overlay_start)
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn save_restriction_restores_end_after_insert_at_point_max() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "ab")
                  (save-restriction
                    (narrow-to-region 1 3)
                    (goto-char (point-max))
                    (insert "c"))
                  (buffer-string))
                "#
        ),
        Value::String("abc".into())
    );
}

fn assert_overlay_modification_hooks_record_replace_two_chars() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (insert "1234")
                  (let ((overlay (make-overlay 2 4)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (goto-char (point-min))
                    (search-forward "23")
                    (replace-match "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
        ),
        Value::list([
            Value::list([
                Value::Symbol("modification-hooks".into()),
                Value::list([
                    Value::T,
                    Value::Integer(2),
                    Value::Integer(3),
                    Value::Integer(2),
                ]),
            ]),
            Value::list([
                Value::Symbol("modification-hooks".into()),
                Value::list([Value::Nil, Value::Integer(2), Value::Integer(4)]),
            ]),
        ])
    );
}

#[test]
fn overlay_modification_hooks_record_replace_two_chars() {
    std::thread::Builder::new()
        .stack_size(4 * 1024 * 1024)
        .spawn(assert_overlay_modification_hooks_record_replace_two_chars)
        .unwrap()
        .join()
        .unwrap();
}

fn assert_overlay_modification_hooks_record_zero_length_insert() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (let ((overlay (make-overlay 1 1)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (insert "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
        ),
        Value::list([
            Value::list([
                Value::Symbol("insert-behind-hooks".into()),
                Value::list([
                    Value::T,
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Integer(0),
                ]),
            ]),
            Value::list([
                Value::Symbol("insert-in-front-hooks".into()),
                Value::list([
                    Value::T,
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Integer(0),
                ]),
            ]),
            Value::list([
                Value::Symbol("insert-behind-hooks".into()),
                Value::list([Value::Nil, Value::Integer(1), Value::Integer(1)]),
            ]),
            Value::list([
                Value::Symbol("insert-in-front-hooks".into()),
                Value::list([Value::Nil, Value::Integer(1), Value::Integer(1)]),
            ]),
        ])
    );
}

#[test]
fn overlay_modification_hooks_record_zero_length_insert() {
    std::thread::Builder::new()
        .stack_size(4 * 1024 * 1024)
        .spawn(assert_overlay_modification_hooks_record_zero_length_insert)
        .unwrap()
        .join()
        .unwrap();
}

fn assert_overlay_modification_hooks_data_driven_cases() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((mismatch nil))
                  (dolist (test-case
                           '(((insert-at . 1))
                             ((insert-at . 2)
                              (expected-calls . ((insert-in-front-hooks (nil 2 2))
                                                 (insert-in-front-hooks (t 2 3 0)))))
                             ((insert-at . 3)
                              (expected-calls . ((modification-hooks (nil 3 3))
                                                 (modification-hooks (t 3 4 0)))))
                             ((insert-at . 4)
                              (expected-calls . ((insert-behind-hooks (nil 4 4))
                                                 (insert-behind-hooks (t 4 5 0)))))
                             ((insert-at . 5))
                             ((replace . "1"))
                             ((replace . "2")
                              (expected-calls . ((modification-hooks (nil 2 3))
                                                 (modification-hooks (t 2 3 1)))))
                             ((replace . "3")
                              (expected-calls . ((modification-hooks (nil 3 4))
                                                 (modification-hooks (t 3 4 1)))))
                             ((replace . "4"))
                             ((replace . "4") (overlay-beg . 4))
                             ((replace . "12")
                              (expected-calls . ((modification-hooks (nil 1 3))
                                                 (modification-hooks (t 1 2 2)))))
                             ((replace . "23")
                              (expected-calls . ((modification-hooks (nil 2 4))
                                                 (modification-hooks (t 2 3 2)))))
                             ((replace . "34")
                              (expected-calls . ((modification-hooks (nil 3 5))
                                                 (modification-hooks (t 3 4 2)))))
                             ((replace . "123")
                              (expected-calls . ((modification-hooks (nil 1 4))
                                                 (modification-hooks (t 1 2 3)))))
                             ((replace . "234")
                              (expected-calls . ((modification-hooks (nil 2 5))
                                                 (modification-hooks (t 2 3 3)))))
                             ((replace . "1234")
                              (expected-calls . ((modification-hooks (nil 1 5))
                                                 (modification-hooks (t 1 2 4)))))
                             ((buffer-text . "") (overlay-beg . 1) (overlay-end . 1)
                              (insert-at . 1)
                              (expected-calls . ((insert-in-front-hooks (nil 1 1))
                                                 (insert-behind-hooks (nil 1 1))
                                                 (insert-in-front-hooks (t 1 2 0))
                                                 (insert-behind-hooks (t 1 2 0)))))))
                    (when (null mismatch)
                      (dolist (advance '(nil t))
                        (when (null mismatch)
                          (let-alist test-case
                            (with-temp-buffer
                              (insert (or .buffer-text "1234"))
                              (let ((overlay (make-overlay
                                              (or .overlay-beg 2)
                                              (or .overlay-end 4)
                                              nil
                                              advance advance)))
                                (dolist (hooks-property '(insert-in-front-hooks
                                                          modification-hooks
                                                          insert-behind-hooks))
                                  (overlay-put
                                   overlay
                                   hooks-property
                                   (list (lambda (ov &rest args)
                                           (push (list hooks-property args)
                                                 (overlay-get overlay
                                                              'recorded-modification-hook-calls)))))
                                  (overlay-put overlay 'recorded-modification-hook-calls nil))
                                (when .insert-at
                                  (goto-char .insert-at)
                                  (insert "x"))
                                (when .replace
                                  (goto-char (point-min))
                                  (search-forward .replace)
                                  (replace-match "x"))
                                (let ((actual (reverse (overlay-get overlay 'recorded-modification-hook-calls))))
                                  (unless (equal .expected-calls actual)
                                    (setq mismatch (list test-case advance actual)))))))))))
                  mismatch)"#
        ),
        Value::Nil
    );
}

#[test]
fn overlay_modification_hooks_data_driven_cases() {
    // This data-heavy Lisp form overflows libtest's default stack on macOS-sized threads.
    std::thread::Builder::new()
        .stack_size(4 * 1024 * 1024)
        .spawn(assert_overlay_modification_hooks_data_driven_cases)
        .unwrap()
        .join()
        .unwrap();
}

fn assert_overlay_complex_insert_2_regions() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert (make-string 100 ?\s))
                  (make-overlay 77 7 nil nil t)
                  (make-overlay 21 53 nil t t)
                  (make-overlay 84 14 nil nil nil)
                  (make-overlay 38 69 nil t nil)
                  (make-overlay 93 15 nil nil t)
                  (make-overlay 73 48 nil t t)
                  (make-overlay 96 51 nil t t)
                  (make-overlay 6 43 nil t t)
                  (make-overlay 15 100 nil t t)
                  (make-overlay 22 17 nil nil nil)
                  (make-overlay 72 45 nil t nil)
                  (make-overlay 2 74 nil nil t)
                  (make-overlay 15 29 nil t t)
                  (make-overlay 17 34 nil t t)
                  (make-overlay 101 66 nil t nil)
                  (make-overlay 94 24 nil nil nil)
                  (goto-char 78)
                  (insert "           ")
                  (narrow-to-region 47 19)
                  (goto-char 46)
                  (widen)
                  (narrow-to-region 13 3)
                  (goto-char 9)
                  (delete-char 0)
                  (goto-char 11)
                  (insert "           ")
                  (goto-char 3)
                  (insert "          ")
                  (goto-char 8)
                  (insert "       ")
                  (goto-char 26)
                  (insert "  ")
                  (goto-char 14)
                  (widen)
                  (narrow-to-region 71 35)
                  (sort (mapcar (lambda (ov)
                                  (cons (overlay-start ov)
                                        (overlay-end ov)))
                                (overlays-in (point-min)
                                             (point-max)))
                        (lambda (o1 o2)
                          (or (< (car o1) (car o2))
                              (and (= (car o1) (car o2))
                                   (< (cdr o1) (cdr o2)))))))"#
        ),
        Value::list([
            Value::cons(Value::Integer(2), Value::Integer(104)),
            Value::cons(Value::Integer(23), Value::Integer(73)),
            Value::cons(Value::Integer(24), Value::Integer(107)),
            Value::cons(Value::Integer(44), Value::Integer(125)),
            Value::cons(Value::Integer(45), Value::Integer(59)),
            Value::cons(Value::Integer(45), Value::Integer(134)),
            Value::cons(Value::Integer(45), Value::Integer(141)),
            Value::cons(Value::Integer(47), Value::Integer(52)),
            Value::cons(Value::Integer(47), Value::Integer(64)),
            Value::cons(Value::Integer(51), Value::Integer(83)),
            Value::cons(Value::Integer(54), Value::Integer(135)),
            Value::cons(Value::Integer(68), Value::Integer(99)),
        ])
    );
}

#[test]
fn overlay_complex_insert_2_regions() {
    std::thread::Builder::new()
        .stack_size(4 * 1024 * 1024)
        .spawn(assert_overlay_complex_insert_2_regions)
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn initialized_remove_overlays_uses_subr_el_and_eq_property_matching() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (insert \"abc\")
               (let* ((needle (copy-sequence \"a\"))
                      (equal-but-not-eq (copy-sequence needle))
                      (ov (make-overlay 1 2)))
                 (overlay-put ov nil 4)
                 (overlay-put ov 'tag needle)
                 (remove-overlays nil nil 'tag equal-but-not-eq)
                 ;; The pinned oracle native-compiles remove-overlays
                 ;; (subrp => t there); subr-primitive-p is nil on both.
                 (list (not (subr-primitive-p (symbol-function 'remove-overlays)))
                       (overlay-get ov nil)
                       (length (overlays-in (point-min) (point-max))))))"
        ),
        Value::list([Value::T, Value::Integer(4), Value::Integer(1)])
    );
}

#[test]
fn loaded_with_restriction_uses_the_shared_labeled_restriction_stack() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (insert (make-string 500 ?a))
               (let ((label (list 'outer)))
                 (with-restriction 100 500 :label label
                   (let ((initial (list (point-min) (point-max))))
                     (goto-char (point-max))
                     (insert \"x\")
                     (widen)
                     (let ((wide (list (point-min) (point-max))))
                       (narrow-to-region 50 150)
                       (let ((narrow (list (point-min) (point-max))))
                         (without-restriction :label label
                           (list initial wide narrow
                                 (list (point-min) (point-max))))))))))"
        ),
        Value::list([
            Value::list([Value::Integer(100), Value::Integer(500)]),
            Value::list([Value::Integer(100), Value::Integer(501)]),
            Value::list([Value::Integer(100), Value::Integer(150)]),
            Value::list([Value::Integer(1), Value::Integer(502)]),
        ]),
    );
}

#[test]
fn loaded_with_restriction_restores_labeled_state_after_an_error() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (insert (make-string 20 ?a))
               (condition-case nil
                   (with-restriction 5 15 :label (list 'private)
                     (error \"boom\"))
                 (error nil))
               (widen)
               (list (point-min) (point-max)))"
        ),
        Value::list([Value::Integer(1), Value::Integer(21)]),
    );
}

#[test]
fn font_lock_ensure_and_flush_track_hi_lock_faces() {
    run_large_stack_test(assert_font_lock_ensure_and_flush_track_hi_lock_faces);
}

fn assert_font_lock_ensure_and_flush_track_hi_lock_faces() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
                   (insert \"a A\")
                   (setq font-lock-mode t)
                   (setq hi-lock-interactive-patterns
                         (list
                          (list
                           (lambda (limit)
                             (let ((case-fold-search nil))
                               (re-search-forward \"a\" limit t)))
                           '(0 'hi-yellow prepend))))
                   (font-lock-ensure)
                   (let ((had-face (and (memq 'hi-yellow (get-text-property 1 'face)) t)))
                     (font-lock-flush)
                     (list had-face (get-text-property 1 'face))))"
        ),
        // GNU batch leaves this buffer unfontified: `font-lock-ensure' with
        // hand-rolled hi-lock patterns applies no faces in a headless
        // session (probed on GNU 30.2).
        Value::list([Value::Nil, Value::Nil])
    );
}

#[test]
fn font_lock_flush_reapplies_remaining_hi_lock_faces() {
    run_large_stack_test(assert_font_lock_flush_reapplies_remaining_hi_lock_faces);
}

fn assert_font_lock_flush_reapplies_remaining_hi_lock_faces() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
                   (insert \"ab\")
                   (setq font-lock-mode t
                         font-lock-fontified t)
                   (let* ((match-a
                           (list (lambda (limit) (re-search-forward \"a\" limit t))
                                 '(0 'hi-yellow prepend)))
                          (match-b
                           (list (lambda (limit) (re-search-forward \"b\" limit t))
                                 '(0 'hi-yellow prepend))))
                     (setq hi-lock-interactive-patterns (list match-b match-a))
                     (font-lock-ensure)
                     (setq hi-lock-interactive-patterns (list match-b))
                     (font-lock-flush)
                     (list (get-text-property 1 'face)
                           (and (memq 'hi-yellow (get-text-property 2 'face)) t))))"
        ),
        // GNU batch applies no hi-lock faces here either (probed on GNU
        // 30.2): both positions stay unfontified.
        Value::list([Value::Nil, Value::Nil])
    );
}

#[test]
fn overlay_positions_survive_unibyte_to_multibyte_transition() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (set-buffer-multibyte t)
                  (insert "ääää")
                  (set-buffer-multibyte nil)
                  (let ((nonempty-bob-end (make-overlay 1 2))
                        (nonempty-bob-beg (make-overlay 1 3))
                        (empty-bob        (make-overlay 1 1))
                        (empty-beg        (make-overlay 3 3))
                        (empty-end        (make-overlay 2 2))
                        (nonempty-beg-beg (make-overlay 3 7))
                        (nonempty-beg-end (make-overlay 3 8))
                        (nonempty-end-beg (make-overlay 4 7))
                        (nonempty-end-end (make-overlay 4 8))
                        (nonempty-eob-beg (make-overlay 5 9))
                        (nonempty-eob-end (make-overlay 6 9))
                        (empty-eob        (make-overlay 9 9)))
                    (set-buffer-multibyte t)
                    (list
                     (list (overlay-start nonempty-bob-end) (overlay-end nonempty-bob-end))
                     (list (overlay-start nonempty-bob-beg) (overlay-end nonempty-bob-beg))
                     (list (overlay-start empty-bob) (overlay-end empty-bob))
                     (list (overlay-start empty-beg) (overlay-end empty-beg))
                     (list (overlay-start empty-end) (overlay-end empty-end))
                     (list (overlay-start nonempty-beg-beg) (overlay-end nonempty-beg-beg))
                     (list (overlay-start nonempty-beg-end) (overlay-end nonempty-beg-end))
                     (list (overlay-start nonempty-end-beg) (overlay-end nonempty-end-beg))
                     (list (overlay-start nonempty-end-end) (overlay-end nonempty-end-end))
                     (list (overlay-start nonempty-eob-beg) (overlay-end nonempty-eob-beg))
                     (list (overlay-start nonempty-eob-end) (overlay-end nonempty-eob-end))
                     (list (overlay-start empty-eob) (overlay-end empty-eob)))))
                "#
        ),
        Value::list([
            Value::list([Value::Integer(1), Value::Integer(2)]),
            Value::list([Value::Integer(1), Value::Integer(2)]),
            Value::list([Value::Integer(1), Value::Integer(1)]),
            Value::list([Value::Integer(2), Value::Integer(2)]),
            Value::list([Value::Integer(2), Value::Integer(2)]),
            Value::list([Value::Integer(2), Value::Integer(4)]),
            Value::list([Value::Integer(2), Value::Integer(5)]),
            Value::list([Value::Integer(3), Value::Integer(4)]),
            Value::list([Value::Integer(3), Value::Integer(5)]),
            Value::list([Value::Integer(3), Value::Integer(5)]),
            Value::list([Value::Integer(4), Value::Integer(5)]),
            Value::list([Value::Integer(5), Value::Integer(5)]),
        ])
    );
}

#[test]
fn current_column_uses_lexically_bound_tab_width() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (let ((tab-width 4))
                    (insert "ab\tcd")
                    (goto-char (point-min))
                    (forward-char 3)
                    (current-column)))
                "#
        ),
        Value::Integer(4)
    );
}

#[test]
fn indent_to_uses_tabs_when_indent_tabs_mode_is_non_nil() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (let ((tab-width 4)
                        (indent-tabs-mode t))
                    (insert "a")
                    (list (indent-to 6) (buffer-string) (current-column))))
                "#
        ),
        Value::list([
            Value::Integer(6),
            Value::String("a\t  ".into()),
            Value::Integer(6),
        ])
    );
}

#[test]
fn indent_to_honors_minimum_with_spaces_only() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (let ((tab-width 4)
                        (indent-tabs-mode nil))
                    (insert "abcd")
                    (list (indent-to 2 3) (buffer-string) (current-column))))
                "#
        ),
        Value::list([
            Value::Integer(7),
            Value::String("abcd   ".into()),
            Value::Integer(7),
        ])
    );
}

#[test]
fn indent_line_to_replaces_existing_indentation() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (let ((indent-tabs-mode nil))
                    (insert "    value")
                    (goto-char (point-min))
                    (forward-char 6)
                    (list (indent-line-to 2) (buffer-string) (current-column))))
                "#
        ),
        Value::list([
            Value::Nil,
            Value::String("  value".into()),
            Value::Integer(2),
        ])
    );
}

#[test]
fn default_indent_line_function_is_indent_relative() {
    // indent.c's C default survives as the default value in the loaded
    // image (buffers with a mode override it locally).
    assert_eq!(
        eval_str_with_upstream_batch("(with-temp-buffer indent-line-function)"),
        Value::Symbol("indent-relative".into())
    );
}

#[test]
fn indent_relative_uses_previous_line_indent_points() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (let ((indent-tabs-mode nil))
                    (insert "alpha beta\nx")
                    (goto-char (point-max))
                    (indent-relative)
                    (buffer-string)))
                "#
        ),
        Value::String("alpha beta\nx     ".into())
    );
}

#[test]
fn forward_and_backward_sexp_move_over_balanced_lists() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (insert "(alpha (beta gamma)) tail")
                  (goto-char (point-min))
                  (forward-sexp)
                  (let ((after-forward (point)))
                    (backward-sexp)
                    (list after-forward (point))))
                "#
        ),
        Value::list([Value::Integer(21), Value::Integer(1)])
    );
}

#[test]
fn scan_sexps_uses_syntax_properties_for_comment_boundaries() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (insert "here's an opener (\n"
                          "> here's citing someone with an opener (\n"
                          "and here's a closer )")
                  (goto-char (point-min))
                  (re-search-forward "^>")
                  (let ((start (match-beginning 0)))
                    (add-text-properties
                     start (1+ start)
                     `(syntax-table ,(string-to-syntax "<")))
                    (end-of-line)
                    (add-text-properties
                     (point) (1+ (point))
                     `(syntax-table ,(string-to-syntax ">"))))
                  (setq-local parse-sexp-lookup-properties t)
                  (setq-local parse-sexp-ignore-comments t)
                  (goto-char (point-max))
                  (backward-sexp)
                  (let ((before (buffer-substring-no-properties
                                 (pos-bol) (point))))
                    (forward-sexp)
                    (list before
                          (buffer-substring-no-properties
                           (pos-bol) (point)))))
                "#
        ),
        Value::list([
            Value::String("here's an opener ".into()),
            Value::String("and here's a closer )".into()),
        ])
    );
}

#[test]
fn syntax_ppss_moves_point_to_pos_like_gnu() {
    // GNU syntax-ppss is NOT excursion-saving: point ends at POS
    // (beginning-of-defun-comments depends on this).
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (insert "(alpha\n beta)")
                  (goto-char (point-max))
                  (let ((state (syntax-ppss 8))
                        (after (point)))
                    (list (car state) after)))
                "#
        ),
        Value::list([Value::Integer(1), Value::Integer(8)])
    );
}

#[test]
fn rx_compiles_common_test_patterns() {
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx-to-string '(seq "ab" eos) t)"#),
        Value::String("ab\\'".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((tramp-local-host-names '("foo" "bar")))
                     (rx-to-string `(: bos (| . ,tramp-local-host-names) eos)))"#
        ),
        // GNU sorts the alternation and keeps the shy group here too
        // (probed on GNU 30.2).
        Value::String("\\(?:\\`\\(?:bar\\|foo\\)\\'\\)".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((tramp-local-host-names '("foo" "bar")))
                     (rx-to-string `(: bos (| \, tramp-local-host-names) eos)))"#
        ),
        // GNU regexp-opt sorts the branches and keeps the shy group when
        // the `\,' spelling routes through `rx' dynamic evaluation
        // (probed on GNU 30.2).
        Value::String("\\(?:\\`\\(?:bar\\|foo\\)\\'\\)".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx-to-string '(or) t)"#),
        Value::String("\\`a\\`".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx bot "body" eot)"#),
        Value::String("\\`body\\'".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx "\\(")"#),
        Value::String("\\\\(".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx bos (group (+ digit)) (+ blank) "Hi" eol)"#),
        Value::String("\\`\\([[:digit:]]+\\)[[:blank:]]+Hi$".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (group xdigit xdigit))"#),
        Value::String("\\([[:xdigit:]][[:xdigit:]]\\)".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx bow "SECCOMP" eow)"#),
        Value::String("\\<SECCOMP\\>".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (| "" (: bol "/" (+ digit))))"#),
        Value::String("\\|^/[[:digit:]]+".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (not (any "/:|")))"#),
        Value::String("[^/:|]".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (in " -Z\\^-~"))"#),
        Value::String("[ -Z\\^-~]".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (in alnum "-"))"#),
        Value::String("[[:alnum:]-]".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (1+ (not (any "/|"))))"#),
        Value::String("[^/|]+".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (zero-or-more ?a))"#),
        Value::String("a*".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (one-or-more ?a))"#),
        Value::String("a+".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (zero-or-one ?a))"#),
        Value::String("a?".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (syntax whitespace))"#),
        Value::String("\\s-".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (not-syntax whitespace))"#),
        Value::String("\\S-".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx (group-n 2 (group-n 1 (+ digit)) ":" (+ digit)))"#),
        Value::String("\\(?2:\\(?1:[[:digit:]]+\\):[[:digit:]]+\\)".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(rx bol (regexp "\\(?:\\sw\\|\\s_\\|\\\\.\\)+") eol)"#),
        Value::String("^\\(?:\\(?:\\sw\\|\\s_\\|\\\\.\\)+\\)$".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(let ((part "[[:alpha:]]+")) (rx bos (regexp part) eos))"#),
        Value::String("\\`\\(?:[[:alpha:]]+\\)\\'".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(string-match-p
                    (rx "find " (+ nonl)
                        " \\( \\( -name .svn -or -name .git -or -name .CVS \\)"
                        " -prune -or -true \\)"
                        " \\( \\( \\(" " -name \\*.pl -or -name \\*.pm -or -name \\*.t \\)"
                        " -or -mtime \\+1 \\) -and \\( -fstype nfs -or -fstype ufs \\) \\) ")
                    "find /tmp/ \\( \\( -name .svn -or -name .git -or -name .CVS \\) -prune -or -true \\) \\( \\( \\( -name \\*.pl -or -name \\*.pm -or -name \\*.t \\) -or -mtime \\+1 \\) -and \\( -fstype nfs -or -fstype ufs \\) \\) ")"#
        ),
        Value::Integer(0)
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(string-match-p (rx (in " -Z\\^-~")) "^")"#),
        Value::Integer(0)
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(string-match-p (rx (group (zero-or-more (syntax whitespace))) "=") "  =")"#
        ),
        Value::Integer(0)
    );
}

#[test]
fn rx_supports_pcomplete_help_regex_forms() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(string-match-p (rx "-" (+ (any "-" alnum)) (? "=")) "--tofu-policy=")"#
        ),
        Value::Integer(0)
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(string-match-p (rx (? " ") (seq "<" (+? nonl) ">")) " <path>")"#
        ),
        Value::Integer(0)
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(string-match-p (rx (* nonl) (* "\n" (>= 9 " ") (* nonl)))
                                   " make a signature\n         wrapped")"#
        ),
        Value::Integer(0)
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(string-match-p (rx ", " symbol-start) ", --sign")"#),
        Value::Integer(0)
    );
}

#[test]
fn abbrev_possibly_save_writes_file_and_resets_changed_flag() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("emaxx-abbrev-save-{unique}.el"));
    let path_text = path.to_string_lossy().replace('\\', "\\\\");

    assert_eq!(
        eval_str_with_upstream_batch(&format!(
            r#"
                (require 'abbrev)
                (let ((abbrev-file-name "{path_text}")
                      (save-abbrevs t))
                  (let ((abbrevs-changed t))
                    (list (abbrev--possibly-save nil t)
                          abbrevs-changed
                          (file-exists-p abbrev-file-name))))
                "#
        )),
        Value::list([Value::Nil, Value::Nil, Value::T])
    );

    let _ = std::fs::remove_file(path);
}

#[test]
fn abbrev_possibly_save_honors_simulated_no_response() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (require 'abbrev)
                (require 'ert-x)
                (let ((abbrev-file-name "/tmp/emaxx-abbrev-unused")
                      (save-abbrevs t))
                  (let ((abbrevs-changed t))
                    (ert-simulate-keys '(?n ?\C-m)
                      (list (abbrev--possibly-save nil) abbrevs-changed))))
                "#
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn abbrev_table_obarray_clear_removes_entries() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (require 'abbrev)
                (let ((table (make-abbrev-table)))
                  (define-abbrev table "aa" "alpha")
                  (obarray-clear table)
                  (list (abbrev-expansion "aa" table)
                        (obarrayp table)))
                "#
        ),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn abbrev_table_empty_obarray_symbol_preserves_table_properties() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (require 'abbrev)
                (let ((table (make-abbrev-table)))
                  (abbrev-table-put table :marker 42)
                  (obarray-put table "")
                  (list (abbrev-table-get table :marker)
                        (abbrev-expansion "" table)
                        (abbrev-table-empty-p table)))
                "#
        ),
        Value::list([Value::Integer(42), Value::Nil, Value::T])
    );
}

#[test]
fn abbrev_require_seeds_standard_table_name_list() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (require 'abbrev)
                (list (not (null (memq 'fundamental-mode-abbrev-table
                                        abbrev-table-name-list)))
                      (not (null (memq 'global-abbrev-table
                                        abbrev-table-name-list)))
                      (not (null (memq 'text-mode-abbrev-table
                                        abbrev-table-name-list)))
                      (abbrev-table-p fundamental-mode-abbrev-table)
                      (abbrev-table-p global-abbrev-table)
                      (abbrev-table-p text-mode-abbrev-table))
                "#
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn abbrev_require_preserves_mode_tables_loaded_first() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"
                    (require 'lisp-mode)
                    (require 'abbrev)
                    (list (not (null (memq 'lisp-mode-abbrev-table
                                            abbrev-table-name-list)))
                          (not (null (memq 'fundamental-mode-abbrev-table
                                            abbrev-table-name-list)))
                          (not (null (memq 'global-abbrev-table
                                            abbrev-table-name-list)))
                          (abbrev-table-p lisp-mode-abbrev-table)
                          (abbrev-table-p fundamental-mode-abbrev-table)
                          (abbrev-table-p global-abbrev-table))
                    "#
            ),
            Value::list([Value::T, Value::T, Value::T, Value::T, Value::T, Value::T,])
        );
    });
}

#[test]
fn abbrev_initializes_local_abbrev_table_default() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"
                    (require 'lisp-mode)
                    (require 'abbrev)
                    (let ((initial (with-temp-buffer
                                     (eq local-abbrev-table
                                         fundamental-mode-abbrev-table))))
                      (list initial
                            (with-temp-buffer
                              (eq local-abbrev-table
                                  fundamental-mode-abbrev-table))))
                    "#
            ),
            Value::list([Value::T, Value::T])
        );
    });
}

#[test]
fn translation_table_vector_is_bound_vector_not_abbrev_table() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (require 'abbrev)
                (list (boundp 'translation-table-vector)
                      (and (vectorp translation-table-vector)
                           (= (length translation-table-vector) 16))
                      (abbrev-table-p translation-table-vector))
                "#
        ),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn wrapper_hook_nil_path_runs_body() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (eval (quote (let ((sample-wrapper-hook nil))
                  (subr--with-wrapper-hook-no-warnings sample-wrapper-hook ()
                    'body-ran))) t)
                "#
        ),
        Value::Symbol("body-ran".into())
    );
}

#[test]
fn wrapper_hook_non_nil_wraps_body_through_continuation() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (eval (quote (let* ((calls nil)
                      ;; Under GNU's lexical --eval, a parallel `let' would
                      ;; create this lambda before `calls' is bound and
                      ;; capture nothing; `let*' matches the probed GNU
                      ;; behavior (wrapped (body wrapper)).
                      (sample-wrapper-hook
                       (list (lambda (fun)
                               (push 'wrapper calls)
                               (let ((result (funcall fun)))
                                 (push result calls)
                                 'wrapped)))))
                  (list (subr--with-wrapper-hook-no-warnings sample-wrapper-hook ()
                          'body)
                        calls))) t)
                "#
        ),
        Value::list([
            Value::Symbol("wrapped".into()),
            Value::list([
                Value::Symbol("body".into()),
                Value::Symbol("wrapper".into())
            ]),
        ])
    );
}

#[test]
fn inverse_add_abbrev_skips_trailing_nonword() {
    assert_eq!(
        eval_str_with_upstream_batch_features(
            &["cl-macs", "abbrev"],
            r#"
                (let ((table (make-abbrev-table)))
                  (with-temp-buffer
                    (insert "some text foo ")
                    (cl-letf (((symbol-function 'read-string)
                               (lambda (&rest _) "bar")))
                      (inverse-add-abbrev table "Global" 1)))
                  (string= (abbrev-expansion "foo" table) "bar"))
                "#
        ),
        Value::T
    );
}

#[test]
fn skip_syntax_backward_supports_negated_word_class() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "some text foo ")
                  (skip-syntax-backward "^w")
                  (buffer-substring-no-properties (point) (point-max)))
                "#
        ),
        Value::String(" ".into())
    );
}

#[test]
fn skip_syntax_forward_symbol_class_covers_standard_table_equals() {
    // GNU's standard syntax table classes `=' as a symbol constituent, so
    // the punctuation class does not move but the symbol class does.
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "= a")
                  (goto-char (point-min))
                  (list (skip-syntax-forward ".")
                        (point)
                        (skip-syntax-forward "_")
                        (point)))
                "#
        ),
        Value::list([
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(2),
        ])
    );
}

#[test]
fn abbrev_edit_save_to_file_redefines_tables() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("emaxx-abbrev-edit-save-{unique}.el"));
    let path_text = path.to_string_lossy().replace('\\', "\\\\");

    assert_eq!(
        eval_str_with_upstream_batch(&format!(
            r#"
                (require 'abbrev)
                (defvar emaxx-abbrev-edit-save-table nil)
                (with-temp-buffer
                  (insert "(emaxx-abbrev-edit-save-table)\n")
                  (insert "\n" "\"aa\"\t" "0\t" "\"alpha\"\n")
                  (abbrev-edit-save-to-file "{path_text}")
                  (read-abbrev-file "{path_text}")
                  (equal "alpha"
                         (abbrev-expansion "aa" emaxx-abbrev-edit-save-table)))
                "#
        )),
        Value::T
    );

    let _ = std::fs::remove_file(path);
}

#[test]
fn upstream_abbrev_edit_save_to_file_case() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("emaxx-upstream-abbrev-edit-save-{unique}.el"));
    let path_text = path.to_string_lossy().replace('\\', "\\\\");

    assert_eq!(
        eval_str_with_upstream_batch(&format!(
            r#"
                (require 'ert-x)
                (require 'abbrev)
                (defvar ert-test-abbrevs nil)
                (defvar ert-save-test-table nil)
                (define-abbrev-table 'ert-test-abbrevs '(("a-e-t" "abbrev-ert-test")))
                (with-temp-buffer
                  (goto-char (point-min))
                  (insert "(ert-save-test-table)\n")
                  (insert "\n" "\"s-a-t\"\t" "0\t" "\"save-abbrevs-test\"\n")
                  (and (equal "abbrev-ert-test"
                              (abbrev-expansion "a-e-t" ert-test-abbrevs))
                       (progn (abbrev-edit-save-to-file "{path_text}") t)
                       (not (abbrev-expansion "a-e-t" ert-test-abbrevs))
                       (progn (read-abbrev-file "{path_text}") t)
                       (equal "save-abbrevs-test"
                              (abbrev-expansion "s-a-t" ert-save-test-table))))
                "#
        )),
        Value::T
    );

    let _ = std::fs::remove_file(path);
}

#[test]
fn upstream_abbrev_edit_save_to_file_ert_case_passes() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    eval_str_with(
        &mut interp,
        r#"
            (require 'ert)
            (require 'ert-x)
            (require 'abbrev)

            (defun emaxx-setup-test-abbrev-table ()
              (defvar emaxx-ert-test-abbrevs nil)
              (define-abbrev-table
                'emaxx-ert-test-abbrevs
                '(("a-e-t" "abbrev-ert-test")))
              (abbrev-table-put emaxx-ert-test-abbrevs
                                :ert-test "ert-test-value")
              emaxx-ert-test-abbrevs)

            (ert-deftest emaxx-abbrev-edit-save-to-file-test ()
              (defvar emaxx-ert-save-test-table nil)
              (ert-with-temp-file temp-test-file :suffix ".el"
                (let ((ert-test-abbrevs (emaxx-setup-test-abbrev-table)))
                  (with-temp-buffer
                    (goto-char (point-min))
                    (insert "(emaxx-ert-save-test-table)\n")
                    (insert "\n" "\"s-a-t\"\t" "0\t"
                            "\"save-abbrevs-test\"\n")
                    (should (equal "abbrev-ert-test"
                                   (abbrev-expansion
                                    "a-e-t" ert-test-abbrevs)))
                    (abbrev-edit-save-to-file temp-test-file)
                    (should-not (abbrev-expansion
                                 "a-e-t" ert-test-abbrevs))
                    (read-abbrev-file temp-test-file)
                    (should (equal "save-abbrevs-test"
                                   (abbrev-expansion
                                    "s-a-t"
                                    emaxx-ert-save-test-table)))))))
            "#,
    );
    let selector = Reader::new("emaxx-abbrev-edit-save-to-file-test")
        .read()
        .unwrap()
        .unwrap();
    let summary = interp.run_ert_tests_with_selector(Some(&selector));
    assert_eq!(summary.passed, 1);
    assert_eq!(summary.failed, 0);
}

#[test]
fn bracket_expressions_keep_literal_backslashes_as_members() {
    assert_eq!(
        eval_str(
            r#"
                (let ((pattern (concat "[" (string 92 46) "]"))
                      (text (string 97 92 46 99)))
                  (string-match pattern text)
                  (list (match-beginning 0) (match-end 0)))
                "#
        ),
        Value::list([Value::Integer(1), Value::Integer(2)])
    );
    assert_eq!(
        eval_str(
            r#"
                (let ((pattern (concat "[" (string 92 94 97 98) "]"))
                      (text (string 99 92 100)))
                  (string-match pattern text)
                  (list (match-beginning 0) (match-end 0)))
                "#
        ),
        Value::list([Value::Integer(1), Value::Integer(2)])
    );
    assert_eq!(
        eval_str(
            r#"
                (let ((pattern (concat "[" (string 36 92 40 42 92 41 94) "]*"))
                      (text (string 36 92 40 41 42 94)))
                  (string-match pattern text)
                  (list (match-beginning 0) (match-end 0)))
                "#
        ),
        Value::list([Value::Integer(0), Value::Integer(6)])
    );
}

#[test]
fn regexp_opt_builds_basic_alternations() {
    assert_eq!(
        eval_str_with_upstream_batch(r#"(regexp-opt '(".log" ".aux" ".log"))"#),
        // GNU factors the shared "." prefix (probed on GNU 30.2).
        Value::String("\\(?:\\.\\(?:aux\\|log\\)\\)".into())
    );
    assert_ne!(
        eval_str_with_upstream_batch(r#"(string-match-p "\\(?:[^\\]\\|\\`\\)\\(\"\\)" "\"")"#),
        Value::Nil
    );
}

#[test]
fn regexp_syntax_classes_match_lisp_definition_forms() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    interp
        .load_target("completion")
        .expect("load GNU completion library");

    assert_eq!(
        eval_str_with(&mut interp, r#"(string-match "\\s_" "-")"#),
        Value::Integer(0)
    );

    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
                (mapcar (lambda (text)
                          (and (string-match *lisp-def-regexp* text)
                               (match-end 0)))
                        '("\n(defun foo"
                          "\n(si:def foo"
                          "\n(def-bar foo"
                          "\n(defun (foo"))
                "#
        ),
        Value::list([
            Value::Integer(8),
            Value::Integer(9),
            Value::Integer(10),
            Value::Integer(9),
        ])
    );
}

fn assert_minibuffer_completion_primitives_cover_batch_cases() {
    assert_eq!(
        eval_str_with_upstream_batch(r#"(try-completion "same" '("same" "same"))"#),
        Value::T
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(try-completion "a" '("abc" "abba" "def"))"#),
        Value::String("ab".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(equal (all-completions "a" '("abc" "abba" "def")) '("abc" "abba"))"#
        ),
        Value::T
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn (require 'cl-lib) (null (cl-set-exclusive-or '("abc" "abba") '("abba" "abc") :test #'equal)))"#
        ),
        Value::T
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((ob (obarray-make 7)))
                  (intern "abc" ob)
                  (intern "abba" ob)
                  (equal (all-completions "a" ob) '("abc" "abba")))
                "#
        ),
        Value::T
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((completion-ignore-case t)) (try-completion "bar" '("bAr" "barfoo")))"#
        ),
        Value::String("bAr".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((completion-ignore-case t)) (try-completion "baz" '("baz" "bAz")))"#
        ),
        Value::String("baz".into())
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((table (completion-table-dynamic
                              (lambda (_string)
                                '("ab-one" "ab-two")))))
                  (list (try-completion "ab" table)
                        (all-completions "ab" table)
                        (test-completion "ab-one" table)))
                "#
        ),
        Value::list([
            Value::String("ab-".into()),
            Value::list([
                Value::String("ab-one".into()),
                Value::String("ab-two".into())
            ]),
            Value::T,
        ])
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((ht (make-hash-table :test #'equal)))
                  (puthash "abc" 1 ht)
                  (gethash "abc" ht))
                "#
        ),
        Value::Integer(1)
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((calls 0)
                      (cache (make-hash-table :test #'equal)))
                  (list
                    (with-memoization (gethash "k" cache)
                      (setq calls (+ calls 1))
                      'cached)
                    (with-memoization (gethash "k" cache)
                      (setq calls (+ calls 1))
                      'missed)
                    calls
                    (gethash "k" cache)))
                "#
        ),
        Value::list([
            Value::Symbol("cached".into()),
            Value::Symbol("cached".into()),
            Value::Integer(1),
            Value::Symbol("cached".into()),
        ])
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((place-calls 0)
                      (cache (make-hash-table :test #'equal)))
                  (with-memoization (gethash "k" (progn
                                                  (setq place-calls (+ place-calls 1))
                                                  cache))
                    'cached)
                  (list place-calls
                        (gethash "k" cache)))
                "#
        ),
        Value::list([Value::Integer(1), Value::Symbol("cached".into())])
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(active-minibuffer-window)"#),
        Value::Nil
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(windowp (minibuffer-window))"#),
        Value::T
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(window-minibuffer-p (selected-window))"#),
        Value::Nil
    );
    assert_eq!(
        eval_str_with_upstream_batch(r#"(minibuffer-prompt-end)"#),
        Value::Integer(1)
    );
    assert_eq!(eval_str_with_upstream_batch(r#"case-replace"#), Value::T);
}

#[test]
fn substitute_in_file_name_uses_the_lisp_process_environment() {
    assert_eq!(
        eval_str(
            r#"(let ((process-environment '("EMAXX_DYNAMIC_SUBST=dynamic")))
                  (substitute-in-file-name
                   "${EMAXX_DYNAMIC_SUBST}/$EMAXX_DYNAMIC_SUBST"))"#,
        ),
        Value::String("dynamic/dynamic".into())
    );
}

#[test]
fn completion_observes_lexically_scoped_policy_with_real_cl_letf() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            r#"(progn
                 (with-temp-buffer
                   (insert "foo")
                   (setq-local
                    completion-at-point-functions
                    (list (lambda ()
                            (list (point-min) (point-max)
                                  '("foobar" "foobaz")))))
                   (let ((completion-auto-help nil)
                         message-args)
                     (cl-letf (((symbol-function #'minibuffer-message)
                                (lambda (&rest args)
                                  (setq message-args args))))
                       (completion-at-point)
                       (completion-at-point)
                       (list (buffer-string)
                             message-args
                             (special-variable-p 'completion-auto-help))))))"#,
        ),
        Value::list([
            Value::String("fooba".into()),
            Value::list([Value::String("Next char not unique".into())]),
            Value::T,
        ])
    );
}

#[test]
fn describe_char_observes_preloaded_eldoc_multiline_policy() {
    // descr-text's char-code-property lookups recurse past libtest's
    // default 8 MiB thread stack, and an overflow aborts the whole test
    // binary -- every later test silently never runs.  Carry the large
    // stack explicitly instead of depending on an ambient RUST_MIN_STACK
    // (second audit, B3a).
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"(progn
                     (require 'descr-text)
                     (with-temp-buffer
                       (insert "…")
                       (goto-char (point-min))
                       (list
                        eldoc-echo-area-use-multiline-p
                        (special-variable-p 'eldoc-echo-area-use-multiline-p)
                        (let ((eldoc-echo-area-use-multiline-p t))
                          (describe-char-eldoc 'ignore)))))"#,
            ),
            Value::list([
                Value::Symbol("truncate-sym-name-if-fit".into()),
                Value::T,
                Value::String("U+2026: Horizontal ellipsis (Po: Punctuation, Other)".into()),
            ])
        );
    });
}

#[test]
fn electric_newline_observes_c_basic_offset_binding() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (require 'electric)
                 (require 'elec-pair)
                 (with-temp-buffer
                   (c-mode)
                   (electric-pair-mode 1)
                   (electric-indent-mode 1)
                   (insert "int main {}")
                   (backward-char 1)
                   (let ((c-basic-offset 4))
                     (newline 1 t))
                   (list (buffer-string)
                         (special-variable-p 'c-basic-offset))))"#,
        ),
        Value::list([Value::String("int main {\n    \n}".into()), Value::T,])
    );
}

#[test]
fn intern_primitives_honor_the_dynamically_bound_obarray() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defun emaxx-intern-in-callee (name) (intern name))
                  (let ((obarray (obarray-make)))
                    (let ((symbol (emaxx-intern-in-callee "scoped-symbol")))
                      (list (special-variable-p 'obarray)
                            (eq symbol (intern-soft "scoped-symbol"))
                          (eq symbol (intern-soft "scoped-symbol" obarray))
                          (unintern "scoped-symbol")
                            (null (intern-soft "scoped-symbol" obarray))))))
                "#
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn standard_obarray_intern_soft_stays_indexed_at_scale() {
    let mut interp = Interpreter::new();
    for index in 0..5_000 {
        interp.intern_symbol_name(&format!("emaxx-indexed-obarray-{index}"));
    }
    let obarray = interp
        .lookup_var("obarray", &Vec::new())
        .expect("standard obarray");
    let started = std::time::Instant::now();
    for _ in 0..25 {
        assert_eq!(
            crate::lisp::primitives::intern_soft_in_obarray(
                &interp,
                &obarray,
                "emaxx-indexed-obarray-4999",
            )
            .unwrap(),
            Value::Symbol("emaxx-indexed-obarray-4999".into())
        );
        assert_eq!(
            crate::lisp::primitives::intern_soft_in_obarray(
                &interp,
                &obarray,
                "emaxx-indexed-obarray-missing",
            )
            .unwrap(),
            Value::Nil
        );
    }
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "standard-obarray membership rebuilt the full symbol view: {:?}",
        started.elapsed()
    );
}

#[test]
fn lexical_onload_closure_can_define_a_function_in_a_dynamic_obarray() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            r#"
                (let* ((obarray (obarray-make))
                       (on-load nil)
                       (mk-cmd
                        (lambda (module)
                          (let ((mode (intern (format "erc-%s-mode" module))))
                            (fset mode (lambda (_n) t)))))
                       (add-onload
                        (lambda (module feature installer)
                          (put (intern module) 'erc--feature feature)
                          (push (cons feature
                                      (lambda () (funcall installer module)))
                                on-load))))
                  (funcall add-onload "lo2" 'explicit-feature-lib mk-cmd)
                  (let* ((module (intern-soft "lo2"))
                         (feature (get module 'erc--feature)))
                    (cl-letf (((symbol-function 'require)
                               (lambda (requested &rest _)
                                 (when-let ((handler
                                             (alist-get requested on-load)))
                                   (funcall handler)))))
                      (require feature nil 'noerror))
                    (let ((mode (intern-soft "erc-lo2-mode")))
                      (list (and mode (symbol-name mode))
                            (and mode (fboundp mode))))))
                "#
        ),
        Value::list([Value::String("erc-lo2-mode".into()), Value::T]),
    );
}

#[test]
fn setf_uses_lambda_gv_setter_declarations() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (progn
                  (defun emaxx-cell-value (cell)
                    (declare
                     (gv-setter
                      (lambda (value)
                        `(progn (setcar ,cell ,value) ,value))))
                    (car cell))
                  (let ((cell (list 0)))
                    (list (setf (emaxx-cell-value cell) 7)
                          (emaxx-cell-value cell))))
                "#
        ),
        Value::list([Value::Integer(7), Value::Integer(7)]),
    );
}

#[test]
fn minibuffer_completion_primitives_cover_batch_cases() {
    run_large_stack_test(assert_minibuffer_completion_primitives_cover_batch_cases);
}

#[test]
fn standard_obarray_completion_enumerates_defined_functions() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defun sample-standard-obarray-command () t)
                  (list
                   (obarrayp obarray)
                   (test-completion "sample-standard-obarray-command"
                                    obarray #'functionp)
                   (all-completions "sample-standard-obarray-"
                                    obarray #'functionp)))
            "#
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::list([Value::String("sample-standard-obarray-command".into())]),
        ])
    );
}

#[test]
fn read_interns_ordinary_symbols_in_the_standard_obarray() {
    assert_eq!(
        eval_str(
            r#"
                (let ((name "sample-read-interned-symbol-7a91"))
                  (list
                   (intern-soft name obarray)
                   (read name)
                   (intern-soft name obarray)))
            "#
        ),
        Value::list([
            Value::Nil,
            Value::Symbol("sample-read-interned-symbol-7a91".into()),
            Value::Symbol("sample-read-interned-symbol-7a91".into()),
        ])
    );
}

#[test]
fn eval_buffer_interns_symbols_read_from_loaded_source() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (progn
                  (with-temp-buffer
                    (insert "'sample-loaded-source-symbol-4c82")
                    (eval-buffer))
                  (intern-soft "sample-loaded-source-symbol-4c82" obarray))
            "#
        ),
        Value::Symbol("sample-loaded-source-symbol-4c82".into())
    );
}

#[test]
fn load_file_strict_interns_symbols_read_from_loaded_source() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-load-interns-symbols-{}.el",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(
        &path,
        "(setq sample-loaded-symbol-holder 'sample-loaded-file-symbol-61d9)\n",
    )
    .expect("write loaded symbol source");

    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(&mut interp, &path).expect("load symbol source");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(intern-soft "sample-loaded-file-symbol-61d9" obarray)"#,
        ),
        Value::Symbol("sample-loaded-file-symbol-61d9".into())
    );

    fs::remove_file(path).expect("remove loaded symbol source");
}

#[test]
fn inhibited_interaction_uses_expected_condition_type() {
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let form = Reader::new(r#"(let ((inhibit-interaction t)) (read-from-minibuffer "foo: "))"#)
        .read()
        .unwrap()
        .unwrap();
    let error = interp.eval(&form, &mut env).unwrap_err();
    assert_eq!(error.condition_type(), "inhibited-interaction");
}

#[test]
fn inhibited_interaction_is_dynamic_across_separately_defined_prompt_helpers() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(
        r#"(progn
              (defun emaxx-test-read-string-indirectly ()
                (read-string "foo: "))
              (let ((inhibit-interaction t))
                (condition-case error
                    (emaxx-test-read-string-indirectly)
                  (inhibited-interaction (car error)))))"#,
    )
    .read_all()
    .unwrap()
    .remove(0);
    assert_eq!(
        interp.eval(&form, &mut Vec::new()).unwrap(),
        Value::Symbol("inhibited-interaction".into())
    );
}

#[test]
fn native_comp_capability_probes_are_honest() {
    assert_eq!(eval_str_with_upstream_batch("(featurep 'emacs)"), Value::T);
    assert_eq!(
        eval_str_with_upstream_batch("(native-comp-available-p)"),
        Value::Nil
    );
    // Emaxx models a build without native compilation, so comp.c registers
    // nothing: `native-comp-available-p' and `(featurep 'native-compile)' are
    // both nil and must agree.  Claiming the feature while denying the
    // capability was the inconsistency phase 6 removed.  The pinned oracle is
    // a native-comp build and answers t to both — a documented build
    // divergence, not a target to imitate.
    assert_eq!(
        eval_str_with_upstream_batch("(featurep 'native-compile)"),
        Value::Nil
    );
    assert_eq!(
        eval_str_with_upstream_batch("(native-comp-function-p (symbol-function 'car))"),
        Value::Nil
    );
}

#[test]
fn native_startup_time_cells_are_nil_before_batch_initialization() {
    assert_eq!(
        eval_str("(list (boundp 'before-init-time) (boundp 'after-init-time) after-init-time)"),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn load_average_matches_gnu_integer_and_float_result_contracts() {
    assert_eq!(
        eval_str(
            "(let ((integers (load-average))
                   (floats (load-average t)))
               (list (length integers)
                     (mapcar #'integerp integers)
                     (length floats)
                     (mapcar #'floatp floats)))"
        ),
        Value::list([
            Value::Integer(3),
            Value::list([Value::T, Value::T, Value::T]),
            Value::Integer(3),
            Value::list([Value::T, Value::T, Value::T]),
        ])
    );
}

#[test]
fn char_script_table_is_bound_for_text_fill_runtime() {
    assert_eq!(
        eval_str("(list (char-table-p char-script-table) (char-table-subtype char-script-table))"),
        Value::list([Value::T, Value::Symbol("char-script-table".into())])
    );
}

#[test]
fn nconc_supports_dotted_tails() {
    assert_eq!(
        eval_str("(nconc '(a b) 'tail)"),
        Value::cons(
            Value::symbol("a"),
            Value::cons(Value::symbol("b"), Value::symbol("tail"))
        )
    );
}

#[test]
fn nconc_mutates_existing_aliases() {
    assert_eq!(
        eval_str("(let* ((x (list 'a 'b)) (y x)) (nconc x '(c)) y)"),
        Value::list([Value::symbol("a"), Value::symbol("b"), Value::symbol("c")])
    );
}

#[test]
fn nconc_replaces_dotted_tail_destructively() {
    assert_eq!(
        eval_str("(let* ((x '(a . b)) (y x)) (nconc x '(c)) y)"),
        Value::list([Value::symbol("a"), Value::symbol("c")])
    );
}

#[test]
fn failed_looking_at_preserves_previous_match_data() {
    assert_eq!(
        eval_str(
            r#"
            (with-temp-buffer
              (insert "abc")
              (goto-char (point-min))
              (re-search-forward "ab")
              (let ((before (list (match-beginning 0) (match-end 0))))
                (looking-at "z")
                (list before (match-beginning 0) (match-end 0))))
            "#
        ),
        Value::list([
            Value::list([Value::Integer(1), Value::Integer(3)]),
            Value::Integer(1),
            Value::Integer(3),
        ])
    );
}

#[test]
fn looking_at_sees_left_context_for_zero_width_symbol_boundaries() {
    assert_eq!(
        eval_str(
            r#"
            (with-temp-buffer
              (insert "x")
              (goto-char (point-max))
              (list (looking-at "\\_>")
                    (looking-at "\\_<")
                    (progn
                      (goto-char (point-min))
                      (looking-at "\\=x"))
                    (looking-at "x\\=")))
            "#
        ),
        Value::list([Value::T, Value::Nil, Value::T, Value::Nil])
    );
}

#[test]
fn boundary_heavy_searches_use_linear_candidates_without_weakening_symbol_boundaries() {
    assert_eq!(
        eval_str(
            r#"
            (with-temp-buffer
              (insert (make-string 200000 ?x) "\nM-x mapcar ")
              (goto-char (point-min))
              (let ((news-pattern
                     "'mapcar'\\|M-x[ \t\n]+mapcar\\_>\\|(mapcar)\\|^\\(?:  \\|\t\\)[ \t]*\\(\\(.*[( ']\\)?mapcar\\_>\\)"))
                (list (and (re-search-forward news-pattern nil t)
                           (match-string 0))
                      (string-match "foo\\_>" "foo-bar foo ")
                      (string-match "\\_<foo" "xfoo foo"))))
            "#
        ),
        Value::list([
            Value::String("M-x mapcar".into()),
            Value::Integer(8),
            Value::Integer(5),
        ])
    );
}

#[test]
fn backward_regexp_search_iterates_matches_not_every_buffer_character() {
    assert_eq!(
        eval_str(
            r#"
            (list
             (with-temp-buffer
               (insert "* Changes in Emacs 30.2\n" (make-string 200000 ?x))
               (goto-char (point-max))
               (and (re-search-backward
                     "^\\* \\(?:.* \\)?Emacs \\([0-9.]+[0-9]\\)" nil t)
                    (list (point) (match-string 1))))
             (with-temp-buffer
               (insert "ababa")
               (goto-char (point-max))
               (re-search-backward "aba" nil t)))
            "#
        ),
        Value::list([
            Value::list([Value::Integer(1), Value::String("30.2".into())]),
            Value::Integer(3),
        ])
    );
}

#[test]
fn dumped_help_metadata_keymaps_and_window_entry_points_keep_their_gnu_shape() {
    // A file-less interpreter must not poison the process-wide provenance
    // cache for the upstream-backed interpreter used immediately afterward.
    let _ = eval_str("(subrp (symbol-function 'last))");
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"
                (progn
                 (require 'help)
                 (defvar emaxx-help-test-map nil)
                 (setq emaxx-help-test-map (make-sparse-keymap "Demo"))
                 (let ((map emaxx-help-test-map))
                  (define-key map "a" 'ignore)
                  (let ((canonical (keymap-canonicalize map)))
                    (list
                     (keymap-prompt map)
                     (keymap-prompt canonical)
                     (let ((tail (cdr canonical)))
                       (while (and tail (not (consp (car tail))))
                         (setq tail (cdr tail)))
                       (lookup-key tail "a" t))
                     (not (null
                           (string-match-p
                            "a[[:blank:]]+ignore"
                            (substitute-command-keys
                             "\\{emaxx-help-test-map}"))))
                     (type-of (symbol-function 'last))
                     (symbol-function 'search-forward-regexp)
                     (file-name-nondirectory (symbol-file 'chmod 'defun))
                     (file-name-nondirectory (symbol-file 'posn-window 'defun))
                     (with-temp-buffer
                       (windowp (temp-buffer-window-show (current-buffer))))))))
                "#
            ),
            Value::list([
                Value::String("Demo".into()),
                Value::String("Demo".into()),
                Value::Symbol("ignore".into()),
                Value::T,
                // The sibling GNU build native-compiles `last' into a subr;
                // Emaxx models a no-native-comp GNU (native-comp-available-p
                // is nil), whose dumped Lisp owners are byte-code functions
                // loaded from their compiled `.elc' representation.
                Value::Symbol("byte-code-function".into()),
                Value::BuiltinFunc("re-search-forward".into()),
                Value::String("subr.elc".into()),
                Value::String("subr.elc".into()),
                Value::T,
            ])
        );
    });
}

#[test]
fn batch_startup_preloads_the_gnu_help_surface() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"
                (list (featurep 'keymap)
                      (file-name-nondirectory
                       (symbol-file 'defvar-keymap 'defun))
                      (progn
                        (defvar-keymap emaxx-help-full-map
                          :full t
                          "1" #'ignore
                          "2" #'ignore
                          "M-g M-c" #'forward-char)
                        (list (char-table-p (cadr emaxx-help-full-map))
                              (lookup-key emaxx-help-full-map
                                          (kbd "M-g M-c") t)
                              (length
                               (accessible-keymaps emaxx-help-full-map))
                              (not
                               (null
                                (string-match-p
                                 "M-g M-c[[:blank:]]+forward-char"
                                 (substitute-command-keys
                                  "\\{emaxx-help-full-map}"))))))
                      (featurep 'help)
                      (fboundp 'help--key-description-fontified)
                      (featurep 'minibuffer)
                      (featurep 'elisp-mode)
                      (lookup-key global-map (kbd "C-]") t)
                      (key-description
                       (where-is-internal #'abort-recursive-edit nil t))
                      (substitute-command-keys
                       "\\<minibuffer-local-must-match-map>\\[abort-recursive-edit]")
                      (substitute-command-keys
                       "\\<emacs-lisp-mode-map>\\[eval-defun]")
                      (key-description (where-is-internal #'next-line nil t))
                      (key-description (where-is-internal #'goto-char nil t))
                      (key-description (where-is-internal #'save-buffer nil t))
                      (progn
                        (with-output-to-temp-buffer " *Emaxx Help Output*"
                          (princ "redirected"))
                        (with-current-buffer " *Emaxx Help Output*"
                          (buffer-string)))
                      (let ((text-quoting-style 'curve))
                        (substitute-quotes "`x'"))
                      (let ((map '(keymap
                                   (1 . ignore)
                                   (menu-bar keymap
                                    (foo menu-item "Foo" ignore))))
                            (shadow '((keymap (1 . forward-char)))))
                        (list
                         (length (accessible-keymaps map))
                         (lookup-key shadow "\C-a" t)
                         (eq (lookup-key shadow [] t) shadow)
                         (with-temp-buffer
                           (let ((standard-output (current-buffer)))
                             (help--describe-map-tree
                              map t shadow nil nil nil nil nil nil)
                             (not (null
                                   (string-match-p
                                    "<menu-bar> <foo>[[:blank:]]+ignore"
                                    (buffer-string))))))
                         (with-temp-buffer
                           (let ((standard-output (current-buffer)))
                             (help--describe-map-tree
                              map t shadow nil nil t nil nil nil)
                             (equal (buffer-string) "")))
                         (with-temp-buffer
                           (let ((standard-output (current-buffer)))
                             (help--describe-map-tree
                              map t shadow nil nil t nil nil t)
                             (let ((text (buffer-string)))
                               (and (not (null (string-match-p "C-a" text)))
                                    (not (null
                                          (string-match-p
                                           "this binding is currently shadowed"
                                           text))))))))))
                "#,
            ),
            Value::list([
                Value::T,
                Value::String("keymap.elc".into()),
                Value::list([
                    Value::T,
                    Value::Symbol("forward-char".into()),
                    Value::Integer(4),
                    Value::T,
                ]),
                Value::T,
                Value::T,
                Value::T,
                Value::T,
                Value::Symbol("abort-recursive-edit".into()),
                Value::String("C-]".into()),
                Value::String("C-]".into()),
                Value::String("C-M-x".into()),
                Value::String("C-n".into()),
                Value::String("M-g c".into()),
                Value::String("C-x C-s".into()),
                Value::String("redirected\n".into()),
                Value::String("‘x’".into()),
                Value::list([
                    Value::Integer(2),
                    Value::Symbol("forward-char".into()),
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                ]),
            ])
        );
    });
}

#[test]
fn where_is_first_prefers_a_short_character_binding() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (setq emaxx-substitute-map (make-sparse-keymap))
                 (define-key emaxx-substitute-map "\C-a"
                   'emaxx-substitute-command)
                 (define-key emaxx-substitute-map [home]
                   'emaxx-substitute-command)
                 (define-key emaxx-substitute-map "\C-c\C-a"
                   'emaxx-substitute-command)
                 (list
                  (string=
                   (key-description
                    (where-is-internal
                     'emaxx-substitute-command
                     (list emaxx-substitute-map) t))
                   "C-a")
                  (string=
                   (substitute-command-keys
                    "\\<emaxx-substitute-map>\\[emaxx-substitute-command]")
                   "C-a")))"#,
        ),
        Value::list([Value::T, Value::T]),
    );
}

#[test]
fn batch_native_lisp_callables_preserve_help_arglists() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"
                (let ((let-alist-state
                       (let ((value (let-alist '((value . 42)) .value)))
                         (list (= value 42)
                               (featurep 'let-alist)
                               (autoloadp (symbol-function 'let-alist))
                               (listp (help-function-arglist 'let-alist t)))))
                      (rx-state
                       (let ((regexp (rx "x")))
                         (list (string= regexp "x")
                               (featurep 'rx)
                               (autoloadp (symbol-function 'rx))
                               (listp (help-function-arglist 'rx t))
                               (autoloadp (symbol-function 'rx-define))
                               (listp (help-function-arglist 'rx-define t))))))
                  (require 'shortdoc)
                  ;; GNU: `shortdoc' does not define `cl-oddp'; the cl-lib
                  ;; owner must load before its arity is observable.
                  (require 'cl-lib)
                  (list
                   let-alist-state
                   rx-state
                   (let ((function (indirect-function 'defvar-keymap)))
                     (list (condition-case err (aref function 0)
                             (wrong-type-argument (car err)))
                           (func-arity function)
                           (listp (help-function-arglist 'defvar-keymap t))))
                   (let ((function (indirect-function 'zerop)))
                     (list (aref function 0)
                           (func-arity function)
                           (help-function-arglist 'zerop t)))
                   (let ((function (indirect-function 'cl-oddp)))
                     (list (func-arity function)
                           (help-function-arglist 'cl-oddp t)))))
                "#
            ),
            Value::list([
                Value::list([Value::T, Value::T, Value::Nil, Value::T]),
                Value::list([
                    Value::T,
                    Value::T,
                    Value::Nil,
                    Value::T,
                    Value::Nil,
                    Value::T,
                ]),
                Value::list([
                    // GNU: `defvar-keymap' indirects to (macro . FN), and
                    // `aref' on that cons signals wrong-type-argument.
                    Value::Symbol("wrong-type-argument".into()),
                    Value::cons(Value::Integer(1), Value::Symbol("many".into())),
                    Value::T,
                ]),
                Value::list([
                    // Compiled `zerop' from GNU's subr.elc: slot 0 is the
                    // packed argspec for exactly one required argument.
                    Value::Integer(257),
                    Value::cons(Value::Integer(1), Value::Integer(1)),
                    Value::list([Value::Symbol("number".into())]),
                ]),
                Value::list([
                    Value::cons(Value::Integer(1), Value::Integer(1)),
                    Value::list([Value::Symbol("integer".into())]),
                ]),
            ])
        );
    });
}

#[test]
fn backquote_splicing_uses_a_runtime_keymaps_public_list_shape() {
    assert_eq!(
        eval_str(
            "(let ((map (make-sparse-keymap)))
               (list (keymapp map)
                     `(prefix ,@map suffix)))",
        ),
        Value::list([
            Value::T,
            Value::list([
                Value::Symbol("prefix".into()),
                Value::Symbol("keymap".into()),
                Value::Symbol("suffix".into()),
            ]),
        ])
    );
}

#[test]
fn help_symbol_regexp_uses_current_syntax_table_for_operator_symbols() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"
                (progn
                  (require 'help-mode)
                  (let ((fmt "See also the function ‘%s’."))
                    (mapcar
                     (lambda (fn)
                       (with-temp-buffer
                         (insert (format fmt fn))
                         (goto-char (point-min))
                         (and (re-search-forward help-xref-symbol-regexp nil t)
                              (match-string 9))))
                     '(interactive \` = + - * / %))))
                "#,
            ),
            Value::list(
                ["interactive", "`", "=", "+", "-", "*", "/", "%"]
                    .into_iter()
                    .map(|value| Value::String(value.into())),
            )
        );
    });
}

#[test]
fn regexp_word_atoms_follow_the_current_syntax_table_without_cache_leakage() {
    assert_eq!(
        eval_str(
            r#"
            (let ((syntax-pattern "\\sw")
                  (word-pattern "\\w"))
              (list
               (with-temp-buffer
                 (list (string-match-p syntax-pattern "%")
                       (string-match-p syntax-pattern "_")
                       (string-match-p word-pattern "%")))
               (with-temp-buffer
                 (set-syntax-table (make-syntax-table (standard-syntax-table)))
                 (modify-syntax-entry ?% ".")
                 (modify-syntax-entry ?A ".")
                 (list (char-syntax ?%)
                       (char-syntax ?A)
                       (string-match-p syntax-pattern "%")
                       (string-match-p syntax-pattern "A")))
               (with-temp-buffer
                 (set-syntax-table (make-syntax-table (standard-syntax-table)))
                 (modify-syntax-entry ?_ "w")
                 (list (string-match-p syntax-pattern "_")
                       (string-match-p word-pattern "_")))))
            "#,
        ),
        Value::list([
            Value::list([Value::Integer(0), Value::Nil, Value::Integer(0)]),
            Value::list([
                Value::Integer('.' as i64),
                Value::Integer('.' as i64),
                Value::Nil,
                Value::Nil,
            ]),
            Value::list([Value::Integer(0), Value::Integer(0)]),
        ])
    );
}

#[test]
fn regexp_syntax_atoms_follow_all_effective_table_classes_like_gnu() {
    assert_eq!(
        eval_str(
            r#"
            (with-temp-buffer
              (let* ((table (copy-syntax-table))
                     (cases
                      (list
                       (list 97 45 " ")
                       (list 98 46 ".")
                       (list 99 119 "w")
                       (list 100 95 "_")
                       (list 101 40 "(z")
                       (list 102 41 ")z")
                       (list 103 39 (string 39))
                       (list 104 34 (string 34))
                       (list 105 36 "$")
                       (list 106 92 (string 92))
                       (list 107 47 "/")
                       (list 108 60 "<")
                       (list 109 62 ">")
                       (list 110 64 "@")
                       (list 111 33 "!")
                       (list 112 124 "|"))))
                (mapc
                 (lambda (case)
                   (modify-syntax-entry (nth 0 case) (nth 2 case) table))
                 cases)
                (set-syntax-table table)
                (mapcar
                 (lambda (case)
                   (let* ((character (nth 0 case))
                          (code (nth 1 case))
                          (text (string character)))
                     (list
                      (char-syntax character)
                      (string-match-p (format "\\s%c" code) text)
                      (string-match-p (format "\\S%c" code) text))))
                 cases)))
            "#,
        ),
        Value::list([
            Value::list([Value::Integer(32), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(46), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(119), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(95), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(40), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(41), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(39), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(34), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(36), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(92), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(47), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(60), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(62), Value::Integer(0), Value::Nil]),
            // `@' inherits the standard table's word entry instead of
            // remaining an observable effective syntax class.
            Value::list([Value::Integer(119), Value::Nil, Value::Integer(0)]),
            Value::list([Value::Integer(33), Value::Integer(0), Value::Nil]),
            Value::list([Value::Integer(124), Value::Integer(0), Value::Nil]),
        ])
    );
}

#[test]
fn regexp_whitespace_atom_stops_before_a_table_classed_comment_end_newline() {
    assert_eq!(
        eval_str(
            r#"
            (with-temp-buffer
              (set-syntax-table (copy-syntax-table))
              (modify-syntax-entry ?\n ">")
              (insert "  \nX")
              (goto-char 1)
              (let ((first (looking-at "\\s-*\\(\n\\|\\s>\\)")))
                (list
                 first
                 (mapcar
                  (lambda (index)
                    (cons (match-beginning index) (match-end index)))
                  '(0 1))
                 (progn (goto-char 1) (looking-at "\\s-+"))
                 (cons (match-beginning 0) (match-end 0))
                 (progn (goto-char 3) (looking-at "\\s>"))
                 (cons (match-beginning 0) (match-end 0))
                 (progn (goto-char 3) (looking-at "\\S-"))
                 (cons (match-beginning 0) (match-end 0)))))
            "#,
        ),
        Value::list([
            Value::T,
            Value::list([
                Value::cons(Value::Integer(1), Value::Integer(4)),
                Value::cons(Value::Integer(3), Value::Integer(4)),
            ]),
            Value::T,
            Value::cons(Value::Integer(1), Value::Integer(3)),
            Value::T,
            Value::cons(Value::Integer(3), Value::Integer(4)),
            Value::T,
            Value::cons(Value::Integer(3), Value::Integer(4)),
        ])
    );
}

#[test]
fn regexp_syntax_atom_with_backslash_designator_remains_repeatable() {
    assert_eq!(
        eval_str(
            r#"
            (let ((pattern (concat "\\s" (string 92) "+")))
              (list
               (string-match-p pattern (string 92 92))
               (with-temp-buffer
                 (modify-syntax-entry 92 ".")
                 (string-match-p pattern (string 92 92)))))
            "#,
        ),
        Value::list([Value::Integer(0), Value::Nil])
    );
}

#[test]
fn regexp_word_class_does_not_cache_mutable_syntax_table_entries() {
    assert_eq!(
        eval_str(
            r#"
            (let* ((table (make-syntax-table))
                   (entry (list 2)))
              (set-char-table-range table ?! entry)
              (with-syntax-table table
                (list (char-syntax ?!)
                      (string-match-p "\\w" "!")
                      (progn (setcar entry 1) (char-syntax ?!))
                      (string-match-p "\\w" "!"))))
            "#,
        ),
        Value::list([
            Value::Integer('w' as i64),
            Value::Integer(0),
            Value::Integer('.' as i64),
            Value::Nil,
        ])
    );
}

#[test]
fn regexp_posix_word_class_uses_wide_current_syntax_table_ranges() {
    assert_eq!(
        eval_str(
            r#"
            (let ((table (make-char-table 'syntax-table '(3))))
              (modify-syntax-entry '(#xC0 . #xD6) "w" table)
              (modify-syntax-entry '(#x10000 . #xEFFFF) "w" table)
              (with-syntax-table table
                (mapcar
                 (lambda (character)
                   (let ((text (string character)))
                     (list (char-syntax character)
                           (not (null (string-match-p "[[:word:]_.]" text)))
                           (not (null (string-match-p "[^[:word:]_.]" text))))))
                 '(?A ?_ ?. ?! #xC0 #xEFFFF #xF0000))))
            "#,
        ),
        Value::list([
            Value::list([Value::Integer('_' as i64), Value::Nil, Value::T]),
            Value::list([Value::Integer('_' as i64), Value::T, Value::Nil]),
            Value::list([Value::Integer('_' as i64), Value::T, Value::Nil]),
            Value::list([Value::Integer('_' as i64), Value::Nil, Value::T]),
            Value::list([Value::Integer('w' as i64), Value::T, Value::Nil]),
            Value::list([Value::Integer('w' as i64), Value::T, Value::Nil]),
            Value::list([Value::Integer('_' as i64), Value::Nil, Value::T]),
        ])
    );
}

#[test]
fn regexp_syntax_atoms_honor_position_specific_syntax_properties() {
    assert_eq!(
        eval_str(
            r#"
            (list
             ;; Seed the compiled-regexp cache with the same pattern and
             ;; sentinel scalar, but a different original character.  The
             ;; following buffer must not inherit that translation.
             (with-temp-buffer
               (insert "x")
               (put-text-property 1 2 'syntax-table (string-to-syntax "< c"))
               (let ((parse-sexp-lookup-properties t))
                 (looking-at-p "\\(?:$\\)\\s<")))
             (with-temp-buffer
               (insert "a\nb\n")
               (put-text-property 2 3 'syntax-table (string-to-syntax "< c"))
               (let ((parse-sexp-lookup-properties t))
                 (list
                 (progn
                   (goto-char 1)
                   (re-search-forward "\\(?:$\\)\\s<" nil t)
                   (list (match-beginning 0) (match-end 0)))
                 (progn
                   (goto-char 1)
                   (re-search-forward "\n" nil t))
                 (progn
                   (goto-char 2)
                   (looking-at-p "\\s<"))
                 (progn
                   (goto-char 2)
                   (looking-at-p "."))
                 ;; Quantifying dot must repeat the encoded-newline guard,
                 ;; not check it only once before an otherwise free `.*'.
                 (progn
                   (erase-buffer)
                   (insert "xy\nz\n")
                   (put-text-property 3 4 'syntax-table
                                      (string-to-syntax "< c"))
                   (goto-char 1)
                   (re-search-forward "x.*\\(\n\\)" nil t)
                   (list (point) (match-string 0)))
                 (progn
                   (erase-buffer)
                   (insert "a\nb\n")
                   (put-text-property 2 3 'syntax-table
                                      (string-to-syntax "< c"))
                   (goto-char 2)
                   (looking-at-p "[\n]"))
                 ;; A property-bearing literal elsewhere in the haystack
                 ;; must not rewrite group metadata in \\(?:...\\).
                 (progn
                   (erase-buffer)
                   (set-syntax-table (copy-syntax-table))
                   (modify-syntax-entry ?_ "w")
                   (insert "sub y_max :")
                   (put-text-property 11 12 'syntax-table
                                      (string-to-syntax "\""))
                   (goto-char 1)
                   (re-search-forward
                    "\\<\\(package\\|sub\\)\\>[ \\t]*\\(\\(?:\\sw\\|::\\)+\\)?"
                    nil t)
                   (list (point) (match-string 2)))
                 ;; Explicit group-number metadata has the same `:' hazard.
                 (progn
                   (goto-char 1)
                   (re-search-forward
                    "\\(?1:sub\\) \\(?2:\\sw+\\)"
                    nil t)
                   (list (point) (match-string 1) (match-string 2)))
                 ;; Every syntax-class designator, not just comment syntax,
                 ;; reads the effective per-character entry.
                 (progn
                   (erase-buffer)
                   (insert "x")
                   (put-text-property 1 2 'syntax-table
                                      (string-to-syntax "-"))
                   (goto-char 1)
                   (list (looking-at-p "\\s-")
                         (looking-at-p "\\S-")))
                 ;; Literal preservation is part of the grammar translator:
                 ;; repeat/group metadata must not be rewritten just because
                 ;; the same character has syntax-table properties elsewhere.
                 (progn
                   (erase-buffer)
                   (insert "aa,")
                   (put-text-property 3 4 'syntax-table
                                      (string-to-syntax "< c"))
                   (goto-char 1)
                   (looking-at-p "\\(?:a\\{1,2\\}\\|\\s<\\)"))
                 ;; `$' is a literal away from a branch end and must retain
                 ;; that identity when its haystack occurrence is encoded.
                 (progn
                   (erase-buffer)
                   (insert "a$b")
                   (put-text-property 2 3 'syntax-table
                                      (string-to-syntax "< c"))
                   (goto-char 1)
                   (looking-at-p "a$b\\|\\s<"))
                 ;; Case folding applies to the encoded character's original
                 ;; spelling for both literals and bracket membership.
                 (progn
                   (erase-buffer)
                   (insert "a")
                   (put-text-property 1 2 'syntax-table
                                      (string-to-syntax "< c"))
                   (goto-char 1)
                   (let ((case-fold-search t))
                     (list (looking-at-p "A\\|z\\s<")
                           (looking-at-p "[A]\\|z\\s<")
                           (looking-at-p "[A-Z]\\|z\\s<")
                           (looking-at-p "[^A]\\|z\\s<"))))
                 (progn
                   (erase-buffer)
                   (insert "a\nb\n")
                   (put-text-property 2 3 'syntax-table
                                      (string-to-syntax "< c"))
                   (goto-char 2)
                   (looking-at-p "[^\n]"))
                 (progn
                   (goto-char 2)
                   (looking-at-p "[[:space:]]"))
                 ;; `looking-at' keeps one character of left context.  When
                 ;; that character is syntax-property encoded, its UTF-8 byte
                 ;; width must not be mistaken for the original ASCII width;
                 ;; a zero-width match previously landed inside the sentinel.
                 (progn
                   (erase-buffer)
                   (insert "xy")
                   (put-text-property 1 2 'syntax-table
                                      (string-to-syntax "-"))
                   (goto-char 2)
                   (let ((matched (looking-at "\\s-*")))
                     (list matched (match-beginning 0) (match-end 0))))
                 (progn
                   (erase-buffer)
                   (insert "a\nb\n")
                   (put-text-property 2 3 'syntax-table
                                      (string-to-syntax "< c"))
                   (goto-char 2)
                   (looking-at-p "[^x]"))
                 (progn
                   (goto-char 3)
                   (re-search-forward "\\(?:$\\)\\s<" nil t))))))
            "#,
        ),
        Value::list([
            Value::Nil,
            Value::list([
                Value::list([Value::Integer(2), Value::Integer(3)]),
                Value::Integer(3),
                Value::T,
                Value::Nil,
                Value::list([Value::Integer(4), Value::String("xy\n".into())]),
                Value::T,
                Value::list([Value::Integer(10), Value::String("y_max".into())]),
                Value::list([
                    Value::Integer(10),
                    Value::String("sub".into()),
                    Value::String("y_max".into()),
                ]),
                Value::list([Value::T, Value::Nil]),
                Value::T,
                Value::T,
                Value::list([Value::T, Value::T, Value::T, Value::Nil]),
                Value::Nil,
                Value::T,
                Value::list([Value::T, Value::Integer(2), Value::Integer(2)]),
                Value::T,
                Value::Nil,
            ]),
        ])
    );
}

#[test]
fn copied_syntax_tables_clear_the_root_default_and_inherit_standard_syntax() {
    assert_eq!(
        eval_str(
            r#"
            (let* ((standard (standard-syntax-table))
                   (copy (copy-syntax-table standard))
                   (custom (make-char-table 'syntax-table
                                            (string-to-syntax "w")))
                   (custom-copy (copy-syntax-table custom)))
              (with-temp-buffer
                (set-syntax-table copy)
                (list (char-syntax ?a)
                      (eq (char-table-parent copy) standard)
                      (char-table-range copy nil)
                      (char-table-range custom-copy nil)
                      (eq (char-table-parent custom-copy) standard))))
            "#,
        ),
        Value::list([
            Value::Integer('w' as i64),
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::T,
        ])
    );
}

#[test]
fn copy_syntax_table_without_an_argument_copies_the_standard_table() {
    assert_eq!(
        eval_str(
            r#"
            (with-temp-buffer
              (let ((custom (make-syntax-table)))
                (modify-syntax-entry ?! "w" custom)
                (set-syntax-table custom)
                (let ((copy (copy-syntax-table)))
                  (set-syntax-table copy)
                  (list (char-syntax ?!)
                        (eq (char-table-parent copy)
                            (standard-syntax-table))))))
            "#,
        ),
        Value::list([Value::Integer('.' as i64), Value::T])
    );
}

#[test]
fn backward_forward_comment_honors_property_comment_end_before_whitespace() {
    assert_eq!(
        eval_str(
            r#"
            (with-temp-buffer
              (insert "code;\nBODY\nEND\n\nnext")
              ;; Intermediate newlines carry the mode's ordinary line-comment
              ;; end style; only the property-marked `> c' closes this
              ;; synthetic comment.
              (modify-syntax-entry ?\n "> b")
              (put-text-property 6 7 'syntax-table (string-to-syntax "< c"))
              (put-text-property 15 16 'syntax-table (string-to-syntax "> c"))
              (let ((parse-sexp-lookup-properties t))
                (goto-char 17)
                (let* ((one-result (forward-comment -1))
                       (one-point (point))
                       (one-char (char-after)))
                  (goto-char 17)
                  (let ((many-result (forward-comment (- (point-max)))))
                    (list one-result one-point one-char
                          many-result (point) (char-before))))))
            "#,
        ),
        Value::list([
            Value::T,
            Value::Integer(6),
            Value::Integer('\n' as i64),
            Value::Nil,
            Value::Integer(6),
            Value::Integer(';' as i64),
        ])
    );
}

#[test]
fn font_lock_defaults_syntax_alist_is_scoped_to_fontification() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r##"
            (with-temp-buffer
              (set-syntax-table (make-syntax-table))
              (modify-syntax-entry ?# "<")
              (modify-syntax-entry ?\n ">")
              (insert "# note\nsub y_max")
              (setq font-lock-defaults
                    '((("\\<\\(sub\\)\\>[ \\t]*\\(\\sw+\\)"
                        (1 font-lock-keyword-face)
                        (2 font-lock-function-name-face nil t)))
                      nil nil ((?_ . "w")) nil
                      (font-lock-syntactic-face-function
                       . (lambda (_) 'font-lock-comment-face))))
              (font-lock-ensure)
              (list (get-text-property 2 'face)
                    (get-text-property 12 'face)
                    (get-text-property 14 'face)
                    (char-syntax ?_)))
            "##,
        ),
        Value::list([
            Value::Symbol("font-lock-comment-face".into()),
            Value::Symbol("font-lock-function-name-face".into()),
            Value::Symbol("font-lock-function-name-face".into()),
            Value::Integer('_' as i64),
        ])
    );
}

#[test]
fn regexp_boundaries_honor_per_character_syntax_properties() {
    assert_eq!(
        eval_str(
            r#"
            (list
             (with-temp-buffer
               (insert "a!")
               (put-text-property 2 3 'syntax-table (string-to-syntax "_"))
               (let ((parse-sexp-lookup-properties t))
                 (goto-char 1)
                 (let ((result (re-search-forward "a!?\\_>" nil t)))
                   (list result (match-beginning 0) (match-end 0)))))
             (with-temp-buffer
               (insert "a!")
               (put-text-property 2 3 'syntax-table (string-to-syntax "w"))
               (let ((parse-sexp-lookup-properties t))
                 (goto-char 1)
                 (let ((result (re-search-forward "a!?\\_>" nil t)))
                   (list result (match-beginning 0) (match-end 0))))))
            "#,
        ),
        Value::list([
            Value::list([Value::Integer(3), Value::Integer(1), Value::Integer(3)]),
            Value::list([Value::Integer(3), Value::Integer(1), Value::Integer(3)]),
        ])
    );
}

#[test]
fn regexp_ascii_punct_class_includes_symbols_like_gnu() {
    assert_eq!(
        eval_str(
            r#"
            (list
             (mapcar (lambda (text) (string-match-p "[[:punct:]]" text))
                     '("|" "+" "$" "^" "~" "_" "(" "%" "!"))
             (mapcar (lambda (text) (string-match-p "[[:punct:]]" text))
                     '("a" "0" " ")))
            "#,
        ),
        Value::list([
            Value::list(std::iter::repeat_n(Value::Integer(0), 9)),
            Value::list([Value::Nil, Value::Nil, Value::Nil]),
        ])
    );
}

#[test]
fn font_lock_optional_nil_bounds_and_decoration_levels_match_gnu() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
            (list
             (with-temp-buffer
               (insert "alpha")
               (setq font-lock-mode t
                     font-lock-fontified t
                     font-lock-defaults
                     '(( ("alpha" 0 font-lock-keyword-face) )))
               (font-lock-ensure nil nil)
               (font-lock-flush nil nil)
               (get-text-property 1 'face))
             (with-temp-buffer
               (insert "alpha beta")
               (setq emaxx-test-font-lock-level
                     '(("beta" 0 font-lock-function-name-face))
                     font-lock-defaults '((emaxx-test-font-lock-level))
                     font-lock-maximum-decoration t)
               (font-lock-ensure)
               (get-text-property 7 'face))
             (with-temp-buffer
               (insert "alpha beta")
               (setq emaxx-test-font-lock-level-zero
                     '(("alpha" 0 font-lock-keyword-face))
                     emaxx-test-font-lock-level-one
                     '(("beta" 0 font-lock-function-name-face))
                     font-lock-defaults
                     '((emaxx-test-font-lock-level-zero
                        emaxx-test-font-lock-level-one))
                     font-lock-maximum-decoration 0)
               (font-lock-ensure)
               (list (get-text-property 1 'face)
                     (get-text-property 7 'face)))
             (with-temp-buffer
               (insert "alpha beta")
               ;; Function and variable cells are independent.  GNU's
               ;; font-lock-eval-keywords deliberately prefers the function.
               (setq emaxx-test-font-lock-provider
                     '(("alpha" 0 font-lock-keyword-face))
                     font-lock-defaults
                     '((emaxx-test-font-lock-provider)))
               (fset 'emaxx-test-font-lock-provider
                     (lambda ()
                       '(("beta" 0 font-lock-function-name-face))))
               (font-lock-ensure)
               (list (get-text-property 1 'face)
                     (get-text-property 7 'face))))
            "#,
        ),
        Value::list([
            Value::symbol("font-lock-keyword-face"),
            Value::symbol("font-lock-function-name-face"),
            Value::list([Value::symbol("font-lock-keyword-face"), Value::Nil]),
            Value::list([Value::Nil, Value::symbol("font-lock-function-name-face")]),
        ])
    );
}

#[test]
fn font_lock_keyword_matching_uses_and_restores_its_case_fold_setting() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
            (let ((case-fold-search t))
              (list
               (with-temp-buffer
                 (insert "A a")
                 (setq font-lock-mode t
                       font-lock-defaults
                       '(( ("[A-Z]+" 0 font-lock-type-face) ) nil nil))
                 (font-lock-ensure)
                 (list (get-text-property 1 'face)
                       (get-text-property 3 'face)))
               (with-temp-buffer
                 (insert "A a")
                 (setq font-lock-mode t
                       font-lock-defaults
                       '(( ("[A-Z]+" 0 font-lock-type-face) ) nil t))
                 (font-lock-ensure)
                 (list (get-text-property 1 'face)
                       (get-text-property 3 'face)))
               case-fold-search))
            "#,
        ),
        Value::list([
            Value::list([Value::symbol("font-lock-type-face"), Value::Nil]),
            Value::list([
                Value::symbol("font-lock-type-face"),
                Value::symbol("font-lock-type-face"),
            ]),
            Value::T,
        ])
    );
}

#[test]
fn buffer_list_is_mru_ordered_after_switches() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
            (progn
              (get-buffer-create "first")
              (get-buffer-create "second")
              (switch-to-buffer "first")
              (switch-to-buffer "second")
              (mapcar #'buffer-name (buffer-list)))
            "#
        )
        .to_vec()
        .unwrap()
        .into_iter()
        .take(2)
        .collect::<Vec<_>>(),
        vec![
            Value::String("second".into()),
            Value::String("first".into()),
        ]
    );
}
