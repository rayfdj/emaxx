use super::*;

#[test]
fn letrec_binds_names_before_initializer_evaluation() {
    assert_eq!(
        eval_str(
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
        eval_str(
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
fn named_let_expands_to_recursive_binding() {
    assert_eq!(
        eval_str(
            r#"
                (named-let loop ((n 3) (acc nil))
                  (if (> n 0)
                      (loop (1- n) (cons n acc))
                    acc))
                "#
        ),
        Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3),])
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
            "(ert-with-temp-file sample-file
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
    assert_eq!(eval_str(&form), Value::T);
    let _ = fs::remove_file(expected);
    let _ = fs::remove_dir(directory);
}

#[test]
fn setf_image_property_updates_image_descriptors() {
    assert_eq!(
        eval_str(
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
        eval_str("(if-let* ((a 1) (b 2)) (+ a b) 'fallback)"),
        Value::Integer(3)
    );
    assert_eq!(
        eval_str("(if-let* ((a 1) (_ nil) (b 2)) (+ a b) 'fallback)"),
        Value::Symbol("fallback".into())
    );
    assert_eq!(
        eval_str("(when-let* ((a 1) (b 2)) (+ a b))"),
        Value::Integer(3)
    );
}

#[test]
fn if_let_and_when_let_support_single_binding_compat_syntax() {
    assert_eq!(
        eval_str("(if-let (a 3) (+ a 4) 'fallback)"),
        Value::Integer(7)
    );
    assert_eq!(
        eval_str("(if-let ((a nil) (b 2)) (+ a b) 'fallback)"),
        Value::Symbol("fallback".into())
    );
    assert_eq!(eval_str("(when-let (a 5) (+ a 6))"), Value::Integer(11));
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
        eval_str("(let ((sample t)) (bound-and-true-p sample))"),
        Value::T
    );
    assert_eq!(eval_str("(bound-and-true-p missing-symbol)"), Value::Nil);
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
        eval_str("(seq-position '((a a a) (b b b) (c c c)) '(b b b))"),
        Value::Integer(1)
    );
}

#[test]
fn require_ert_uses_builtin_feature_and_skip_alias() {
    let mut interp = Interpreter::new();
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
    let mut interp = Interpreter::new();
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
fn ert_with_test_buffer_kills_buffer_after_success() {
    assert_eq!(
        eval_str(
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
        eval_str(
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
            .contains("failed to provide feature sample-missing-feature")
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
    let dir = std::env::temp_dir().join(format!(
        "emaxx-require-load-path-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&dir).expect("create require load-path dir");
    fs::write(
        dir.join("sample-load-path.el"),
        "(provide 'sample-load-path)\n",
    )
    .expect("write require target");
    let dir_text = dir.to_string_lossy();
    let form =
        format!(r#"(let ((load-path (cons "{dir_text}" load-path))) (require 'sample-load-path))"#);
    assert_eq!(eval_str(&form), Value::Symbol("sample-load-path".into()));
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn skip_unless_records_skip_in_summary() {
    let mut interp = Interpreter::new();
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

    let mut interp = Interpreter::new();
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
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let form = Reader::new("(keyboard-quit)").read_all().unwrap().remove(0);
    let error = interp.eval(&form, &mut env).unwrap_err();
    assert_eq!(error.condition_type(), "quit");
}

#[test]
fn run_with_timer_returns_a_timer_without_firing_immediately() {
    assert_eq!(
        eval_str(
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
        eval_str_with_upstream_load_path("(progn (require 'timer) (timerp (timer-create)))"),
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
fn loaded_timer_queue_fires_during_waits() {
    assert_eq!(
        eval_str_with_upstream_load_path(
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
fn nonlocal_exit_from_timer_preserves_later_due_timers() {
    assert_eq!(
        eval_str(
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
                 (setq auto-revert-interval 0)
                 (write-region "any text" nil "{path_text}" nil 'no-message)
                 (let ((buf (find-file-noselect "{path_text}")))
                   (with-current-buffer buf
                     (auto-revert-mode 1)
                     (write-region "another text" nil "{path_text}" nil 'no-message)
                     (set-file-times "{path_text}" (time-subtract nil 1))
                     (sleep-for 0)
                     (prog1 (buffer-string)
                       (set-buffer-modified-p nil)
                       (kill-buffer buf)))))"#
    );
    assert_eq!(
        eval_str_with_upstream_load_path(&form),
        Value::String("another text".into())
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
    assert_string_value(eval_str(&form), &expected);
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
        eval_str_with_upstream_load_path(&form),
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
        eval_str_with_upstream_load_path(&form),
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
    assert_eq!(eval_str_with_upstream_load_path(&form), Value::T);
    let _ = fs::remove_file(path);
}

#[test]
fn make_indirect_buffer_clone_copies_buffer_local_modes() {
    assert_eq!(
        eval_str(
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
    assert_eq!(eval_str("buffer-auto-revert-by-notification"), Value::Nil);
}

#[test]
fn format_spec_applies_width_precision_and_flags() {
    assert_eq!(
        eval_str(r#"(format-spec "%2a%-3b%.1p%%" '((?a . "") (?b . "-") (?p . "99")))"#),
        Value::String("  -  9%".into())
    );
    assert_eq!(
        eval_str(r#"(format-spec "%2a%-3b%.1p%%" '((?b . "-") (?p . "99")) 'delete)"#),
        Value::String("-  9%".into())
    );
    assert_eq!(
        eval_str(
            r#"(format-spec "%^a %_b %04c %<3d %>3e" '((?a . "abc") (?b . "XYZ") (?c . "7") (?d . "abcdef") (?e . "abcdef")))"#
        ),
        Value::String("ABC xyz 0007 def abc".into())
    );
}

#[test]
fn format_spec_supports_function_values_and_split() {
    assert_eq!(
        eval_str(r#"(format-spec "a%xb" `((?x . ,(lambda () "X"))) nil t)"#),
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
        eval_str(
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
        eval_str(
            r#"(progn
                     (defcustom sample-choice t "Sample."
                       :type '(choice (const :tag "One" one)))
                     (custom-add-choice 'sample-choice '(const :tag "Two" two))
                     (custom-add-choice 'sample-choice '(const :tag "Two" duplicate))
                     (get 'sample-choice 'custom-type))"#
        ),
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
        ])
    );
}

#[test]
fn custom_add_option_records_unique_options() {
    assert_eq!(
        eval_str(
            r#"(progn
                     (defcustom sample-hook nil "Sample." :type 'hook)
                     (custom-add-option 'sample-hook 'first)
                     (custom-add-option 'sample-hook 'first)
                     (custom-add-option 'sample-hook 'second)
                     (list (get 'sample-hook 'custom-options)
                           (get 'sample-hook 'custom-type)))"#
        ),
        Value::list([
            Value::list([
                Value::Symbol("first".into()),
                Value::Symbol("second".into())
            ]),
            Value::Symbol("hook".into()),
        ])
    );
}

#[test]
fn tab_bar_new_tab_choice_has_preloaded_custom_type() {
    assert_eq!(
        eval_str(
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
    assert_eq!(eval_str(r#"(coding-system-p 'chinese-gb18030)"#), Value::T);
    assert_eq!(
        eval_str(r#"(stringp (decode-coding-string "\xE3\x32\x9A\x36" 'chinese-gb18030))"#),
        Value::T
    );
}

#[test]
fn select_safe_coding_system_uses_default_candidates() {
    assert_eq!(
        eval_str("(select-safe-coding-system (point-min) (point-max) (list t 'utf-8-emacs))"),
        Value::Symbol("utf-8-emacs".into())
    );
}

#[test]
fn utf8_decoding_preserves_invalid_bytes_as_raw_chars() {
    let decoded = eval_str(r#"(decode-coding-string "\xe3\x32" 'utf-8)"#);
    assert_eq!(primitives::string_text(&decoded).unwrap(), "\u{e0e3}2");
}

#[test]
fn decode_char_supports_eight_bit_charset() {
    assert_eq!(
        eval_str(
            r#"(list (charsetp 'eight-bit)
                        (char-charset (decode-char 'eight-bit #x81))
                        (stringp (char-to-string (decode-char 'eight-bit #x81))))"#
        ),
        Value::list([Value::T, Value::Symbol("eight-bit".into()), Value::T,])
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
        eval_str(
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
        eval_str(
            r#"(list (bidi-string-mark-left-to-right "abc")
                              (length (bidi-string-mark-left-to-right "א")))"#
        ),
        Value::list([Value::String("abc".into()), Value::Integer(2)])
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
        eval_str_with_upstream_load_path(&form),
        Value::list([Value::String(path_text.to_string()), Value::Nil, Value::T])
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
        eval_str_with_upstream_load_path(&form),
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
    let mut interp = Interpreter::new();
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
                   (list (call-interactively 'callint-test-int-args t) command-history))"
        ),
        Value::list([
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
fn call_interactively_rejects_invalid_control_letters() {
    assert_eq!(
        eval_str("(cdr (should-error (call-interactively (lambda () (interactive \"ÿ\")))))"),
        Value::list([Value::String(
            "Invalid control letter `ÿ' (#o377, #x00ff) in interactive calling string".into(),
        )])
    );
}

#[test]
fn call_interactively_follows_symbol_aliases_for_interactive_specs() {
    let mut interp = Interpreter::new();
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

#[cfg(unix)]
#[test]
fn call_process_region_can_delete_entire_buffer() {
    assert_eq!(
        eval_str(
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
        eval_str(
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
        eval_str(
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
fn encode_coding_string_substitutes_unencodable_ascii_and_latin1_chars() {
    assert_eq!(
        eval_str(
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
fn framep_accepts_selected_frame_stub() {
    assert_eq!(eval_str("(framep (selected-frame))"), Value::T);
    assert_eq!(eval_str("(framep nil)"), Value::Nil);
}

#[test]
fn url_insert_entities_in_string_escapes_html_markup_chars() {
    assert_eq!(
        eval_str(r#"(url-insert-entities-in-string "<a b=\"c&d\">")"#),
        Value::String("&lt;a b=&quot;c&amp;d&quot;&gt;".into())
    );
}

#[test]
fn decode_coding_region_rewrites_dos_eol_in_place() {
    assert_eq!(
        eval_str(
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
fn decode_coding_string_normalizes_dos_eol() {
    assert_eq!(
        eval_str(
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
    assert_eq!(
        eval_str("(let ((mode 'c++-mode)) (comma (if (eq mode 'c++-mode) 'matched 'miss)))"),
        Value::Symbol("matched".into())
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
    let mut interp = Interpreter::new();
    let value = eval_str_with(&mut interp, r#"`(#s(a 1) . #s(b 2))"#);
    let (left, right) = value.cons_values().expect("dotted pair");
    assert!(matches!(left, Value::Record(_)));
    assert!(matches!(right, Value::Record(_)));
}

#[test]
fn backquote_materializes_record_literals() {
    let mut interp = Interpreter::new();
    let value = eval_str_with(&mut interp, r#"`(#s(a b) #s(#s(c d) e))"#);
    let items = value.to_vec().expect("backquoted list");
    assert_eq!(items.len(), 2);
    let Value::Record(inner_id) = &items[0] else {
        panic!("expected inner record");
    };
    let inner = interp.find_record(*inner_id).expect("inner record");
    assert_eq!(inner.type_name, "a");
    assert_eq!(inner.slots, vec![Value::Symbol("b".into())]);
    let Value::Record(outer_id) = &items[1] else {
        panic!("expected outer record");
    };
    let outer = interp.find_record(*outer_id).expect("outer record");
    assert_eq!(outer.type_name, "literal-record");
    assert_eq!(outer.slots.len(), 2);
    assert!(matches!(outer.slots[0], Value::Record(_)));
    assert!(matches!(outer.slots[1], Value::Symbol(ref symbol) if symbol == "e"));
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
fn remove_overlays_matches_string_properties_by_equal() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
                   (insert \"abc\")
                   (let ((ov-a (make-overlay 1 2))
                         (ov-b (make-overlay 2 3)))
                     (overlay-put ov-a 'tag (copy-sequence \"a\"))
                     (overlay-put ov-b 'tag \"b\")
                     (remove-overlays nil nil 'tag \"a\")
                     (length (overlays-in (point-min) (point-max)))))"
        ),
        Value::Integer(1)
    );
}

#[test]
fn font_lock_ensure_and_flush_track_hi_lock_faces() {
    run_large_stack_test(assert_font_lock_ensure_and_flush_track_hi_lock_faces);
}

fn assert_font_lock_ensure_and_flush_track_hi_lock_faces() {
    assert_eq!(
        eval_str(
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
        Value::list([Value::T, Value::list([Value::Symbol("hi-yellow".into())])])
    );
}

#[test]
fn font_lock_flush_reapplies_remaining_hi_lock_faces() {
    run_large_stack_test(assert_font_lock_flush_reapplies_remaining_hi_lock_faces);
}

fn assert_font_lock_flush_reapplies_remaining_hi_lock_faces() {
    assert_eq!(
        eval_str(
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
        Value::list([Value::Nil, Value::T])
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
        eval_str(
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
            Value::Integer(2),
            Value::String("  value".into()),
            Value::Integer(4),
        ])
    );
}

#[test]
fn default_indent_line_function_is_indent_relative() {
    assert_eq!(
        eval_str("(default-value 'indent-line-function)"),
        Value::Symbol("indent-relative".into())
    );
}

#[test]
fn indent_relative_uses_previous_line_indent_points() {
    assert_eq!(
        eval_str(
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
        eval_str(
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
fn syntax_ppss_moves_point_to_pos_like_gnu() {
    // GNU syntax-ppss is NOT excursion-saving: point ends at POS
    // (beginning-of-defun-comments depends on this).
    assert_eq!(
        eval_str(
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
        eval_str(r#"(rx-to-string '(seq "ab" eos) t)"#),
        Value::String("ab\\'".into())
    );
    assert_eq!(
        eval_str(
            r#"(let ((tramp-local-host-names '("foo" "bar")))
                     (rx-to-string `(: bos (| . ,tramp-local-host-names) eos)))"#
        ),
        Value::String("\\`\\(?:foo\\|bar\\)\\'".into())
    );
    assert_eq!(
        eval_str(
            r#"(let ((tramp-local-host-names '("foo" "bar")))
                     (rx-to-string `(: bos (| \, tramp-local-host-names) eos)))"#
        ),
        Value::String("\\`\\(?:foo\\|bar\\)\\'".into())
    );
    assert_eq!(
        eval_str(r#"(rx-to-string '(or) t)"#),
        Value::String("\\`a\\`".into())
    );
    assert_eq!(
        eval_str(r#"(rx bot "body" eot)"#),
        Value::String("\\`body\\'".into())
    );
    assert_eq!(eval_str(r#"(rx "\\(")"#), Value::String("\\\\(".into()));
    assert_eq!(
        eval_str(r#"(rx bos (group (+ digit)) (+ blank) "Hi" eol)"#),
        Value::String("\\`\\(\\(?:[0-9]\\)+\\)\\(?:[[:blank:]]\\)+Hi$".into())
    );
    assert_eq!(
        eval_str(r#"(rx (group xdigit xdigit))"#),
        Value::String("\\([0-9A-Fa-f][0-9A-Fa-f]\\)".into())
    );
    assert_eq!(
        eval_str(r#"(rx bow "SECCOMP" eow)"#),
        Value::String("\\bSECCOMP\\b".into())
    );
    assert_eq!(
        eval_str(r#"(rx (| "" (: bol "/" (+ digit))))"#),
        Value::String("\\(?:\\|^/\\(?:[0-9]\\)+\\)".into())
    );
    assert_eq!(
        eval_str(r#"(rx (not (any "/:|")))"#),
        Value::String("[^/:|]".into())
    );
    assert_eq!(
        eval_str(r#"(rx (in " -Z\\^-~"))"#),
        Value::String("[ -Z\\^-~]".into())
    );
    assert_eq!(
        eval_str(r#"(rx (in alnum "-"))"#),
        Value::String("[[:alnum:]-]".into())
    );
    assert_eq!(
        eval_str(r#"(rx (1+ (not (any "/|"))))"#),
        Value::String("\\(?:[^/|]\\)+".into())
    );
    assert_eq!(
        eval_str(r#"(rx (zero-or-more ?a))"#),
        Value::String("\\(?:a\\)*".into())
    );
    assert_eq!(
        eval_str(r#"(rx (one-or-more ?a))"#),
        Value::String("\\(?:a\\)+".into())
    );
    assert_eq!(
        eval_str(r#"(rx (zero-or-one ?a))"#),
        Value::String("\\(?:a\\)?".into())
    );
    assert_eq!(
        eval_str(r#"(rx (syntax whitespace))"#),
        Value::String("\\s-".into())
    );
    assert_eq!(
        eval_str(r#"(rx (not-syntax whitespace))"#),
        Value::String("\\S-".into())
    );
    assert_eq!(
        eval_str(r#"(rx (group-n 2 (group-n 1 (+ digit)) ":" (+ digit)))"#),
        Value::String("\\(?2:\\(?1:\\(?:[0-9]\\)+\\):\\(?:[0-9]\\)+\\)".into())
    );
    assert_eq!(
        eval_str(r#"(rx bol (regexp "\\(?:\\sw\\|\\s_\\|\\\\.\\)+") eol)"#),
        Value::String("^\\(?:\\sw\\|\\s_\\|\\\\.\\)+$".into())
    );
    assert_eq!(
        eval_str(r#"(let ((part "[[:alpha:]]+")) (rx bos (regexp part) eos))"#),
        Value::String("\\`[[:alpha:]]+\\'".into())
    );
    assert_eq!(
        eval_str(
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
        eval_str(r#"(string-match-p (rx (in " -Z\\^-~")) "^")"#),
        Value::Integer(0)
    );
    assert_eq!(
        eval_str(r#"(string-match-p (rx (group (zero-or-more (syntax whitespace))) "=") "  =")"#),
        Value::Integer(0)
    );
}

#[test]
fn rx_supports_pcomplete_help_regex_forms() {
    assert_eq!(
        eval_str(r#"(string-match-p (rx "-" (+ (any "-" alnum)) (? "=")) "--tofu-policy=")"#),
        Value::Integer(0)
    );
    assert_eq!(
        eval_str(r#"(string-match-p (rx (? " ") (seq "<" (+? nonl) ">")) " <path>")"#),
        Value::Integer(0)
    );
    assert_eq!(
        eval_str(
            r#"(string-match-p (rx (* nonl) (* "\n" (>= 9 " ") (* nonl)))
                                   " make a signature\n         wrapped")"#
        ),
        Value::Integer(0)
    );
    assert_eq!(
        eval_str(r#"(string-match-p (rx ", " symbol-start) ", --sign")"#),
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
        eval_str_with_upstream_load_path(&format!(
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
        eval_str_with_upstream_load_path(
            r#"
                (require 'abbrev)
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
        eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(
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
            eval_str_with_upstream_load_path(
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
            eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(
            r#"
                (require 'abbrev)
                (list (boundp 'translation-table-vector)
                      (vectorp translation-table-vector)
                      (abbrev-table-p translation-table-vector))
                "#
        ),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn wrapper_hook_nil_path_runs_body() {
    assert_eq!(
        eval_str_with_upstream_load_path(
            r#"
                (let ((sample-wrapper-hook nil))
                  (subr--with-wrapper-hook-no-warnings sample-wrapper-hook ()
                    'body-ran))
                "#
        ),
        Value::Symbol("body-ran".into())
    );
}

#[test]
fn wrapper_hook_non_nil_wraps_body_through_continuation() {
    assert_eq!(
        eval_str_with_upstream_load_path(
            r#"
                (let ((calls nil)
                      (sample-wrapper-hook
                       (list (lambda (fun)
                               (push 'wrapper calls)
                               (let ((result (funcall fun)))
                                 (push result calls)
                                 'wrapped)))))
                  (list (subr--with-wrapper-hook-no-warnings sample-wrapper-hook ()
                          'body)
                        calls))
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
        eval_str_with_upstream_load_path(
            r#"
                (require 'abbrev)
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
        eval_str_with_upstream_load_path(&format!(
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
        eval_str_with_upstream_load_path(&format!(
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
    let mut interp = Interpreter::new();
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
              (ert-with-temp-file temp-test-file
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
        eval_str(r#"(regexp-opt '(".log" ".aux" ".log"))"#),
        Value::String("\\(?:\\.aux\\|\\.log\\)".into())
    );
    assert_ne!(
        eval_str(r#"(string-match-p "\\(?:[^\\]\\|\\`\\)\\(\"\\)" "\"")"#),
        Value::Nil
    );
}

#[test]
fn regexp_syntax_classes_match_lisp_definition_forms() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    let _ = interp.load_target("completion");

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
        eval_str(r#"(try-completion "a" '("abc" "abba" "def"))"#),
        Value::String("ab".into())
    );
    assert_eq!(
        eval_str(r#"(equal (all-completions "a" '("abc" "abba" "def")) '("abc" "abba"))"#),
        Value::T
    );
    assert_eq!(
        eval_str(r#"(null (cl-set-exclusive-or '("abc" "abba") '("abba" "abc") :test #'equal))"#),
        Value::T
    );
    assert_eq!(
        eval_str(
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
        eval_str(r#"(let ((completion-ignore-case t)) (try-completion "bar" '("bAr" "barfoo")))"#),
        Value::String("bAr".into())
    );
    assert_eq!(
        eval_str(r#"(let ((completion-ignore-case t)) (try-completion "baz" '("baz" "bAz")))"#),
        Value::String("baz".into())
    );
    assert_eq!(
        eval_str(
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
        eval_str(
            r#"
                (let ((ht (make-hash-table :test #'equal)))
                  (puthash "abc" 1 ht)
                  (gethash "abc" ht))
                "#
        ),
        Value::Integer(1)
    );
    assert_eq!(
        eval_str(
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
        eval_str(
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
    assert_eq!(eval_str(r#"(active-minibuffer-window)"#), Value::Nil);
    assert_eq!(eval_str(r#"(windowp (minibuffer-window))"#), Value::T);
    assert_eq!(
        eval_str(r#"(window-minibuffer-p (selected-window))"#),
        Value::Nil
    );
    assert_eq!(eval_str(r#"(minibuffer-prompt-end)"#), Value::Integer(1));
    assert_eq!(eval_str(r#"case-replace"#), Value::T);
}

#[test]
fn minibuffer_completion_primitives_cover_batch_cases() {
    run_large_stack_test(assert_minibuffer_completion_primitives_cover_batch_cases);
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
fn native_comp_capability_probes_are_honest() {
    assert_eq!(eval_str("(featurep 'emacs)"), Value::T);
    assert_eq!(eval_str("(native-comp-available-p)"), Value::Nil);
    assert_eq!(eval_str("(featurep 'native-compile)"), Value::T);
    assert_eq!(
        eval_str("(native-comp-function-p (symbol-function 'car))"),
        Value::Nil
    );
}

#[test]
fn startup_time_variables_are_bound_in_batch_runtime() {
    assert_eq!(
        eval_str("(list (boundp 'before-init-time) (boundp 'after-init-time) after-init-time)"),
        Value::list([Value::T, Value::T, Value::T])
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
fn buffer_list_is_mru_ordered_after_switches() {
    assert_eq!(
        eval_str(
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
