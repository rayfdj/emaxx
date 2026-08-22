use super::*;

#[test]
fn reader_interpreted_closures_restore_bindings_and_special_declarations() {
    assert_eq!(
        eval_str(
            "(progn
               (setq x 9)
               (list
                (funcall #[() (captured) ((captured . 7))])
                (let ((x 3)) (funcall #[() (x) (x t)]))))"
        ),
        Value::list([Value::Integer(7), Value::Integer(9)])
    );
}

#[test]
fn nconc_rejects_non_lists_before_last_arg() {
    assert_eq!(
        eval_str("(condition-case err (nconc 'a '(c)) (wrong-type-argument (car err)))"),
        Value::symbol("wrong-type-argument")
    );
}

#[test]
fn nconc_rejects_circular_lists() {
    assert_eq!(
        eval_str(
            "(let ((x (list 'a 'b))) (setcdr (cdr x) x) (condition-case err (nconc x 'tail) (circular-list (car err))))"
        ),
        Value::symbol("circular-list")
    );
}

#[test]
fn mapcan_concatenates_results() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str("(equal (mapcan #'list (list 1 2 3)) '(1 2 3))"),
            Value::T
        );
    });
}

#[test]
fn mapcan_mutates_mapped_lists_destructively() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                "(let ((data (list (list 'foo) (list 'bar))))
                       (and
                        (equal (mapcan #'identity data) '(foo bar))
                        (equal data '((foo bar) (bar)))))"
            ),
            Value::T
        );
    });
}

#[test]
fn copyright_update_updates_last_notice_when_searching_from_end() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"(progn
                     (require 'copyright)
                     (with-temp-buffer
                       (dotimes (_ 2)
                         (insert "Copyright 2021 FSF\n"))
                       (let ((copyright-at-end-flag t)
                             (copyright-query nil))
                         (copyright-update))
                       (buffer-string)))"#
            ),
            Value::String("Copyright 2021 FSF\nCopyright 2021, 2026 FSF\n".into())
        );
    });
}

#[test]
fn define_mail_user_agent_records_mail_properties() {
    assert_eq!(
        eval_str(
            "(progn
               (define-mail-user-agent 'sample-agent 'compose 'send)
               (define-mail-user-agent 'explicit-agent 'compose2 'send2 'abort2 'hook2)
               (list
                (get 'sample-agent 'composefunc)
                (get 'sample-agent 'sendfunc)
                (get 'sample-agent 'abortfunc)
                (get 'sample-agent 'hookvar)
                (get 'explicit-agent 'abortfunc)
                (get 'explicit-agent 'hookvar)))"
        ),
        Value::list([
            Value::Symbol("compose".into()),
            Value::Symbol("send".into()),
            Value::Symbol("kill-buffer".into()),
            Value::Symbol("mail-send-hook".into()),
            Value::Symbol("abort2".into()),
            Value::Symbol("hook2".into()),
        ])
    );
}

#[test]
fn sxhash_eql_matches_equal_bignums() {
    assert_eq!(
        eval_str(
            "(let* ((a (1+ most-positive-fixnum)) (b (+ most-positive-fixnum 1))) (= (sxhash-eql a) (sxhash-eql b)))"
        ),
        Value::T
    );
}

#[test]
fn sort_coding_systems_uses_priority_order() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn (set-coding-system-priority 'utf-8 'iso-latin-1) (sort-coding-systems '(iso-latin-1 undecided utf-8)))"
        ),
        Value::list([
            Value::symbol("utf-8"),
            Value::symbol("iso-latin-1"),
            Value::symbol("undecided"),
        ])
    );
}

#[test]
fn coding_system_type_treats_nil_as_the_no_conversion_designator() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(list (coding-system-type nil) (coding-system-type 'utf-8) (coding-system-type 'raw-text))"
        ),
        Value::list([
            Value::symbol("raw-text"),
            Value::symbol("utf-8"),
            Value::symbol("raw-text"),
        ])
    );
}

#[test]
fn cyrillic_koi8_is_a_single_byte_round_tripping_coding() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((encoded (encode-coding-string \"Русский\" 'cyrillic-koi8)))
               (list (coding-system-type 'cyrillic-koi8)
                     (coding-system-change-eol-conversion
                      'cyrillic-koi8 'unix)
                     (string-bytes encoded)
                     (decode-coding-string encoded 'cyrillic-koi8)))"
        ),
        Value::list([
            Value::Symbol("charset".into()),
            Value::Symbol("cyrillic-koi8-unix".into()),
            Value::Integer(7),
            Value::String("Русский".into()),
        ])
    );
}

#[test]
fn windows_1252_is_a_preloaded_single_byte_round_tripping_coding() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let* ((bytes (unibyte-string
                            100 233 106 224 32 114 97 116 233 32 128))
                    (decoded (decode-coding-string bytes 'windows-1252)))
               (list (coding-system-type 'windows-1252)
                     (coding-system-base 'cp1252)
                     (coding-system-get 'windows-1252 :mime-charset)
                     decoded
                     (multibyte-string-p decoded)
                     (string-to-list
                      (encode-coding-string decoded 'windows-1252))))"
        ),
        Value::list([
            Value::Symbol("charset".into()),
            Value::Symbol("windows-1252".into()),
            Value::Symbol("windows-1252".into()),
            Value::String("déjà raté €".into()),
            Value::T,
            Value::list([
                Value::Integer(100),
                Value::Integer(233),
                Value::Integer(106),
                Value::Integer(224),
                Value::Integer(32),
                Value::Integer(114),
                Value::Integer(97),
                Value::Integer(116),
                Value::Integer(233),
                Value::Integer(32),
                Value::Integer(128),
            ]),
        ])
    );
}

#[test]
fn windows_1251_alias_uses_the_preloaded_single_byte_codec() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let* ((text \"Привет\")
                    (encoded (encode-coding-string text 'cp1251)))
               (list (coding-system-type 'windows-1251)
                     (coding-system-base 'cp1251)
                     (coding-system-get 'windows-1251 :mime-charset)
                     (string-to-list encoded)
                     (decode-coding-string encoded 'windows-1251)))"
        ),
        Value::list([
            Value::Symbol("charset".into()),
            Value::Symbol("windows-1251".into()),
            Value::Symbol("windows-1251".into()),
            Value::list([
                Value::Integer(207),
                Value::Integer(240),
                Value::Integer(232),
                Value::Integer(226),
                Value::Integer(229),
                Value::Integer(242),
            ]),
            Value::String("Привет".into()),
        ])
    );
}

#[test]
fn latin_1_aliases_resolve_to_iso_latin_1() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(list (coding-system-base 'latin-1)
                   (coding-system-change-eol-conversion 'latin-1 'unix)
                   (decode-coding-string
                    (encode-coding-string \"Hyvää päivää\" 'latin-1)
                    'latin-1))"
        ),
        Value::list([
            Value::symbol("iso-latin-1"),
            Value::symbol("iso-latin-1-unix"),
            Value::String("Hyvää päivää".into()),
        ]),
    );
}

#[test]
fn find_composition_keeps_combining_buffer_characters_together() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((old (window-buffer (selected-window))))
               (unwind-protect
                   (with-temp-buffer
                     (set-window-buffer (selected-window) (current-buffer))
                     (insert \"__Åström\")
                     (let ((composition (find-composition 9 10)))
                       (list (car composition) (cadr composition))))
                 (set-window-buffer (selected-window) old)))"
        ),
        Value::list([Value::Integer(8), Value::Integer(10)]),
    );
}

#[test]
fn ascii_case_table_leaves_non_ascii_letters_unchanged() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(with-case-table ascii-case-table (downcase \"ABC 123 ΔΞΩΣ\"))"
        ),
        Value::String("abc 123 ΔΞΩΣ".into()),
    );
}

#[test]
fn read_buffer_simulation_enforces_its_predicate_and_accepts_default() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (get-buffer-create \"#chan\")
               (get-buffer-create \"#fake\")
               (let ((predicate (lambda (name) (string= name \"#chan\"))))
                 (list
                  (let ((unread-command-events
                         (append (kbd \"#chan C-m\")
                                 '(?\\C-g ?\\C-g ?\\C-g))))
                    (read-buffer \"Buffer: \" \"#chan\" t predicate))
                  (let ((unread-command-events
                         (append (kbd \"#fake C-m C-a C-k C-m\")
                                 '(?\\C-g ?\\C-g ?\\C-g))))
                    (read-buffer \"Buffer: \" \"#fake\" t predicate)))))"
        ),
        Value::list([Value::String("#chan".into()), Value::String("#fake".into()),]),
    );
}

#[test]
fn mode_hook_delay_variables_have_default_bindings() {
    assert_eq!(
        eval_str("(list delay-mode-hooks delayed-mode-hooks delayed-after-hook-functions)"),
        Value::list([Value::Nil, Value::Nil, Value::Nil])
    );
}

#[test]
fn delay_mode_hooks_is_dynamically_scoped() {
    assert_eq!(
        eval_str(
            "(progn (defun sample-delay-mode-hooks-value () delay-mode-hooks) (let ((delay-mode-hooks t)) (sample-delay-mode-hooks-value)))"
        ),
        Value::T
    );
}

#[test]
fn loaded_elisp_can_replace_run_mode_hooks_bootstrap_fallback() {
    assert_eq!(
        eval_str(
            "(progn (defun run-mode-hooks (&rest _) 'shadowed) (run-mode-hooks 'sample-hook))"
        ),
        Value::Symbol("shadowed".into())
    );
}

#[test]
fn delay_mode_hooks_evaluates_body_as_special_form() {
    assert_eq!(eval_str("(delay-mode-hooks (+ 1 2))"), Value::Integer(3));
}

#[test]
fn warning_series_variables_have_default_bindings() {
    assert_eq!(
        eval_str(
            "(list warning-series warning-prefix-function warning-fill-prefix warning-type-format warning-suppress-types)"
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::String(" (%s)".into()),
            Value::Nil,
        ])
    );
}

#[test]
fn display_warning_uses_prefix_function_and_explicit_buffer() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(with-temp-buffer
                 (let ((target (buffer-name))
                       (warning-prefix-function
                        (lambda (_level entry)
                          (insert "prefix:")
                          entry)))
                   (display-warning 'check "body" nil target)
                   (buffer-string)))"#
        ),
        Value::String("prefix:Warning (check): body\n".into())
    );
}

#[test]
fn hack_local_variables_accepts_optional_mode_arg() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(list (hack-local-variables) (hack-local-variables 'no-mode))"
        ),
        Value::list([Value::Nil, Value::Nil])
    );
}

#[test]
fn find_file_noselect_delegates_file_local_variable_policy_to_files_el() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let path = std::env::temp_dir().join(format!("emaxx-file-locals-{unique}.el"));
    std::fs::write(
        &path,
        r#";;; -*- lexical-binding: t; -*-
(message "fixture")
;; Local Variables:
;; read-symbol-shorthands: (("s-" . "long-"))
;; End:
"#,
    )
    .unwrap();
    let result = eval_str_with_upstream_batch(&format!(
        r#"(with-current-buffer (find-file-noselect {:?})
             (list read-symbol-shorthands
                   file-local-variables-alist
                   (default-value 'read-symbol-shorthands)))"#,
        path.display().to_string(),
    ));
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        result,
        Value::list([
            Value::list([Value::cons(
                Value::String("s-".into()),
                Value::String("long-".into()),
            )]),
            Value::list([
                Value::cons(Value::Symbol("lexical-binding".into()), Value::T),
                Value::cons(
                    Value::Symbol("read-symbol-shorthands".into()),
                    Value::list([Value::cons(
                        Value::String("s-".into()),
                        Value::String("long-".into()),
                    )]),
                ),
            ]),
            Value::Nil,
        ])
    );
}

#[test]
fn defcustom_safe_metadata_controls_file_local_variable_acceptance() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let option = format!("emaxx-safe-local-{unique}");
    let path = std::env::temp_dir().join(format!("emaxx-safe-local-{unique}.el"));
    std::fs::write(
        &path,
        format!(
            ";;; fixture\n(message \"fixture\")\n;; Local Variables:\n;; {option}: 7\n;; End:\n"
        ),
    )
    .unwrap();
    let result = eval_str_with_upstream_batch(&format!(
        r#"(progn
             (defcustom {option} 1 "Doc."
               :type 'integer
               :safe 'integerp
               :risky t)
             (let ((declaration-owner (macrop 'defcustom)))
               (with-current-buffer (find-file-noselect {:?})
                 (list declaration-owner
                       {option}
                       (local-variable-p '{option})
                       (default-value '{option})
                       (get '{option} 'safe-local-variable)
                       (get '{option} 'risky-local-variable)
                       file-local-variables-alist))))"#,
        path.display().to_string(),
    ));
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        result,
        Value::list([
            Value::T,
            Value::Integer(7),
            Value::T,
            Value::Integer(1),
            Value::symbol("integerp"),
            Value::T,
            Value::list([Value::cons(Value::symbol(&option), Value::Integer(7),)]),
        ])
    );
}

#[test]
fn default_file_modes_can_be_read_and_updated() {
    assert_eq!(
        eval_str(
            "(let ((original (default-file-modes))) (set-default-file-modes #o600) (prog1 (default-file-modes) (set-default-file-modes original)))"
        ),
        Value::Integer(0o600)
    );
}

#[test]
fn list_operation_type_errors_include_original_value() {
    assert_eq!(
        eval_str(
            "(list
               (condition-case err (car 'a) (wrong-type-argument err))
               (condition-case err (nth 1 \"abc\") (wrong-type-argument err)))"
        ),
        Value::list([
            Value::list([
                Value::symbol("wrong-type-argument"),
                Value::symbol("listp"),
                Value::symbol("a"),
            ]),
            Value::list([
                Value::symbol("wrong-type-argument"),
                Value::symbol("listp"),
                Value::String("abc".into()),
            ]),
        ])
    );
}

#[test]
fn read_positioning_symbols_preserves_eq_binding_through_byte_compile() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            "(let* ((sym-with-pos1 (read-positioning-symbols \"sym\"))
                    (sym-with-pos2 (read-positioning-symbols \" sym\"))
                    (without-pos-eq-compiled
                     (byte-compile
                      (lambda (a b)
                        (let ((symbols-with-pos-enabled nil))
                          (eq a b)))))
                    (with-pos-eq-compiled
                     (byte-compile
                      (lambda (a b)
                        (let ((symbols-with-pos-enabled t))
                          (eq a b))))))
               (list (eq sym-with-pos1 sym-with-pos2)
                     (funcall without-pos-eq-compiled sym-with-pos1 sym-with-pos2)
                     (funcall with-pos-eq-compiled sym-with-pos1 sym-with-pos2)
                     (symbol-with-pos-pos sym-with-pos1)
                     (symbol-with-pos-pos sym-with-pos2)))"
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::Integer(0),
            Value::Integer(1),
        ])
    );
}

#[test]
fn condition_case_success_uses_arith_error_condition_value() {
    assert_eq!(
        eval_str(
            "(list
               (condition-case x
                   (/ 1 0)
                 (error (cons 'bad x)))
               (condition-case x
                   (list 42)
                 (error (cons 'bad x))
                 (:success (cons 'good x))))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("bad".into()),
                Value::Symbol("arith-error".into())
            ]),
            Value::list([Value::Symbol("good".into()), Value::Integer(42)]),
        ])
    );
}

#[test]
fn condition_case_handler_closures_follow_gnu_lexical_and_dynamic_unwind() {
    // GNU 30.2 eval.c:internal_lisp_condition_case extends a non-nil
    // interpreter environment lexically, but uses specbind when that
    // environment is nil.  Only the lexical handler closure captures ERR.
    assert_eq!(
        eval_str(
            r#"
                (list
                 (let ((f
                        (eval
                         '(condition-case err
                              (/ 1 0)
                            (arith-error
                             (prog1 (lambda () err)
                               (setq err 'changed))))
                         t)))
                   (funcall f))
                 (let ((f
                        (eval
                         '(condition-case err
                              (/ 1 0)
                            (arith-error
                             (prog1 (lambda () err)
                               (setq err 'changed))))
                         nil)))
                   (condition-case caught
                       (funcall f)
                     (void-variable
                      (list (car caught) (cadr caught))))))
            "#,
        ),
        Value::list([
            Value::Symbol("changed".into()),
            Value::list([
                Value::Symbol("void-variable".into()),
                Value::Symbol("err".into()),
            ]),
        ])
    );
}

#[test]
fn throw_from_handler_inside_function_reaches_matching_catch() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (defun sample-error-frame ()
                 (letrec ((handler (lambda (err) (throw 'sample-tag err))))
                   (catch 'sample-tag
                     (handler-bind ((error handler))
                       (car 'a)))))
               (sample-error-frame))"
        ),
        Value::list([
            Value::symbol("wrong-type-argument"),
            Value::symbol("listp"),
            Value::symbol("a"),
        ])
    );
}

#[test]
fn coding_system_list_is_bound_and_callable() {
    let result = eval_str_with_upstream_batch(
        "(list (boundp 'coding-system-list)
               (fboundp 'coding-system-list)
               (not (null (memq 'utf-8 coding-system-list)))
               (not (null (memq 'utf-8 (coding-system-list))))
               (not (memq 'utf-8-dos (coding-system-list 'base-only))))",
    );
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn set_char_table_range_accepts_t_for_full_range() {
    let result = eval_str(
        "(let ((table (make-char-table nil)))
           (set-char-table-range table t 'all)
           (list (char-table-range table ?A)
                 (char-table-range table #x10ffff)))",
    );
    assert_eq!(
        result,
        Value::list([Value::symbol("all"), Value::symbol("all")])
    );
}

#[test]
fn character_table_reader_literals_preserve_ascii_root_default_and_extras() {
    let mut ascii = vec!["nil"; 128];
    ascii[65] = "upper";
    let ascii = ascii.join(" ");
    let mut roots = vec!["nil"; 64];
    roots[0] = "root";
    let literal = format!(
        "#^[fallback nil purpose #^^[3 0 {ascii}] {} extra]",
        roots.join(" ")
    );
    let inspect = |name: &str| {
        format!(
            "(list (char-table-p {name})
                   (char-table-subtype {name})
                   (aref {name} ?A)
                   (aref {name} ?B)
                   (aref {name} 200)
                   (aref {name} #x10000)
                   (char-table-extra-slot {name} 0))"
        )
    };
    let result = eval_str(&format!(
        "(let ((direct {literal})
               (quoted '{literal})
               (read-back (car (read-from-string \"{literal}\"))))
           (list {} {} {}))",
        inspect("direct"),
        inspect("quoted"),
        inspect("read-back"),
    ));
    let expected_table = Value::list([
        Value::T,
        Value::symbol("purpose"),
        Value::symbol("upper"),
        Value::symbol("fallback"),
        Value::symbol("root"),
        Value::symbol("fallback"),
        Value::symbol("extra"),
    ]);
    assert_eq!(
        result,
        Value::list([
            expected_table.clone(),
            expected_table.clone(),
            expected_table
        ])
    );
}

#[test]
fn upstream_generated_idna_character_table_loads_and_indexes_nested_ranges() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                "(progn
                   (require 'idna-mapping)
                   (list (char-table-p idna-mapping-table)
                         (char-table-subtype idna-mapping-table)
                         (elt idna-mapping-table ?A)
                         (elt idna-mapping-table ?a)
                         (elt idna-mapping-table #xAD)
                         (elt idna-mapping-table #x212A)
                         (elt idna-mapping-table #xFF21)
                         (elt idna-mapping-table #x1D400)
                         (elt idna-mapping-table #xE0100)
                         (elt idna-mapping-table #x10FFFF)))"
            ),
            Value::list([
                Value::T,
                Value::Nil,
                Value::String("a".into()),
                Value::Nil,
                Value::symbol("ignored"),
                Value::String("k".into()),
                Value::String("a".into()),
                Value::String("a".into()),
                Value::symbol("ignored"),
                Value::Nil,
            ])
        );
    });
}

#[test]
fn preloaded_character_property_registry_uses_lisp_policy_and_rust_table_access() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((table (make-char-table 'char-code-property-table)))
               (set-char-table-extra-slot table 0 'emaxx-test-property)
               (aset table ?A 'before)
               (define-char-code-property
                 'emaxx-test-property table \"Test property.\")
               (let ((before (get-char-code-property
                              ?A 'emaxx-test-property)))
                 (put-char-code-property ?A 'emaxx-test-property 'after)
                 (list (fboundp 'define-char-code-property)
                       (eq (cdr (assq 'emaxx-test-property
                                      char-code-property-alist))
                           table)
                       before
                       (get-char-code-property ?A 'emaxx-test-property)
                       (get 'emaxx-test-property
                            'char-code-property-documentation))))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::symbol("before"),
            Value::symbol("after"),
            Value::String("Test property.".into()),
        ])
    );
}

#[test]
fn generated_numeric_property_table_uncompresses_and_decodes_values() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (require 'charprop)
               (list (get-char-code-property ?5 'numeric-value)
                     (get-char-code-property #x0665 'numeric-value)
                     (get-char-code-property #x00BC 'numeric-value)
                     (get-char-code-property #x216B 'numeric-value)
                     (get-char-code-property #x0665 'general-category)))"
        ),
        Value::list([
            Value::Integer(5),
            Value::Integer(5),
            Value::Float(0.25),
            Value::Integer(12),
            Value::symbol("Nd"),
        ])
    );
}

#[test]
fn generated_decomposition_property_table_decodes_word_deltas() {
    let mut interp = Interpreter::new();
    interp.set_load_path(vec![upstream_emacs_repo().join("lisp/international")]);
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (define-char-code-property
                 'decomposition \"uni-decomposition.el\")
               (list (get-char-code-property ?A 'decomposition)
                     (get-char-code-property #x212B 'decomposition)
                     (get-char-code-property #x00C5 'decomposition)
                     (get-char-code-property #x00E5 'decomposition)
                     (get-char-code-property #xFB01 'decomposition)))"
        ),
        Value::list([
            Value::list([Value::Integer(65)]),
            Value::list([Value::Integer(197)]),
            Value::list([Value::Integer(65), Value::Integer(778)]),
            Value::list([Value::Integer(97), Value::Integer(778)]),
            Value::list([
                Value::symbol("compat"),
                Value::Integer(102),
                Value::Integer(105),
            ]),
        ])
    );
}

#[test]
fn translate_region_supports_character_vector_and_sequence_mappings() {
    assert_eq!(
        eval_str(
            "(let ((table (make-char-table 'translation-table)))
               (aset table ?a ?A)
               (aset table ?b [?B ?!])
               (aset table ?c (list (cons [?c ?d] [?C ?D ?!])))
               (with-temp-buffer
                 (insert \"abcdx\")
                 (list (translate-region-internal
                        (point-min) (point-max) table)
                       (buffer-string))))"
        ),
        Value::list([Value::Integer(6), Value::String("AB!CD!x".into()),])
    );
}

#[test]
fn standard_minibuffer_completion_map_is_bound() {
    // minibuffer.el owns this map and GNU preloads it into the dump, so the
    // binding exists in the batch image and not in the early runtime.  GNU
    // answers (t t) here; the early runtime has no minibuffer.el at all.
    let result = eval_str_with_upstream_batch(
        "(list (boundp 'minibuffer-local-completion-map)
               (keymapp minibuffer-local-completion-map))",
    );
    assert_eq!(result, Value::list([Value::T, Value::T]));
}

#[test]
fn completion_style_defaults_are_bound() {
    let result = eval_str_with_upstream_batch(
        "(list (boundp 'completion-styles)
               (not (null (memq 'basic completion-styles)))
               (not (null (assq 'basic completion-styles-alist))))",
    );
    assert_eq!(result, Value::list([Value::T, Value::T, Value::T]));
}

#[test]
fn file_expand_wildcards_returns_existing_matches() {
    let result = eval_str_with_upstream_batch(
        r#"(let ((dir (make-temp-file "emaxx-wildcards-" t)))
             (unwind-protect
                 (progn
                   (make-empty-file (expand-file-name "a.el" dir))
                   (make-empty-file (expand-file-name "b.el" dir))
                   (make-empty-file (expand-file-name "b.txt" dir))
                   (let ((star (file-expand-wildcards
                                (expand-file-name "*.el" dir) t))
                         (question (file-expand-wildcards
                                    (expand-file-name "?.el" dir) t))
                         (class (file-expand-wildcards
                                 (expand-file-name "[ab].el" dir) t)))
                     (list (= (length star) 2)
                           (file-name-absolute-p (car star))
                           (= (length question) 2)
                           (= (length class) 2))))
               (delete-directory dir t)))"#,
    );
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn remote_visit_policy_is_typed_and_derived_from_buffer_identity() {
    let mut interp = Interpreter::new();
    let buffer_id = interp.current_buffer_id();

    assert_eq!(interp.buffer_remote_prefix(buffer_id), None);
    interp.set_current_buffer_file_name(Some("/ssh:user@host:/tmp/file".into()));
    assert_eq!(
        interp.buffer_remote_prefix(buffer_id).as_deref(),
        Some("/ssh:user@host:")
    );

    interp.set_current_buffer_file_name(Some("/tmp/file".into()));
    assert_eq!(interp.buffer_remote_prefix(buffer_id), None);
}

#[test]
fn with_no_warnings_is_one_ordinary_callable_function() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"(list
                 (special-form-p 'with-no-warnings)
                 (with-no-warnings 1 2 3)
                 (funcall #'with-no-warnings 4 5 6)
                 (funcall
                  (byte-compile
                   '(lambda () (with-no-warnings 7 8 9)))))"#
        ),
        Value::list([
            Value::Nil,
            Value::Integer(3),
            Value::Integer(6),
            Value::Integer(9),
        ])
    );
}

#[test]
fn file_replacement_and_offset_writes_share_gnu_file_io_contracts() {
    let result = eval_str_with_upstream_batch(
        r#"(let ((file (make-temp-file "emaxx-file-io-contract-")))
              (unwind-protect
                  (progn
                    (write-region "foobla" nil file)
                    (write-region "baz" nil file 3)
                    (let ((offset-result
                           (with-temp-buffer
                             (insert-file-contents file)
                             (buffer-string))))
                      (write-region "foo" nil file)
                      (list
                       offset-result
                       (with-temp-buffer
                         (insert "fooofoooo")
                         (goto-char (point-min))
                         (list (insert-file-contents
                                file nil nil nil 'replace)
                               (buffer-string)
                               (point)))
                       (with-temp-buffer
                         (insert "bar")
                         (goto-char (point-min))
                         (list (insert-file-contents
                                file nil nil nil 'replace)
                               (buffer-string)
                               (point))))))
                (delete-file file)))"#,
    );
    let file_name = result
        .to_vec()
        .expect("outer result")
        .get(1)
        .expect("first replacement")
        .to_vec()
        .expect("first replacement result")[0]
        .to_vec()
        .expect("insert-file result")[0]
        .clone();
    assert_eq!(
        result,
        Value::list([
            Value::String("foobaz".into()),
            Value::list([
                Value::list([file_name.clone(), Value::Integer(0)]),
                Value::String("foo".into()),
                Value::Integer(1),
            ]),
            Value::list([
                Value::list([file_name, Value::Integer(3)]),
                Value::String("foo".into()),
                Value::Integer(1),
            ]),
        ])
    );
}

#[test]
fn cl_loop_across_iterates_vectors() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop for item across [a b c] collect item)",
        ),
        Value::list([Value::symbol("a"), Value::symbol("b"), Value::symbol("c"),])
    );
}

#[test]
fn plain_vector_is_not_string_like() {
    assert_eq!(
        eval_str(r#"(list (stringp ["a" "b"]) (length ["a" "b"]))"#),
        Value::list([Value::Nil, Value::Integer(2)])
    );
}

#[test]
fn cl_loop_when_collect_filters_items() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop for item in '(1 2 3 4) when (> item 2) collect item)",
        ),
        Value::list([Value::Integer(3), Value::Integer(4)])
    );
}

#[test]
fn cl_loop_when_append_flattens_truthy_results() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop for child in '((a b) nil (c))
                      for matches = child
                      when matches
                      append matches)"
        ),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
        ])
    );
}

#[test]
fn cl_loop_if_collect_else_append_handles_tree_walks() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(equal
              (cl-loop for child in '(\"a\" (b c) \"d\")
                       if (stringp child)
                       collect child
                       else
                       append child)
              '(\"a\" b c \"d\"))"
        ),
        Value::T
    );
}

#[test]
fn cl_defmacro_keyword_default_preserves_quoted_default_form() {
    run_with_large_stack(|| {
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        eval_str_with(&mut interp, "(require 'cl-macs)");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (cl-defmacro sample-cl-default
                       (&key (modes '(quote (ruby-mode js-mode python-mode c-mode))))
                     `(quote ,(eval modes t)))
                   (sample-cl-default))"
            ),
            Value::list([
                Value::Symbol("ruby-mode".into()),
                Value::Symbol("js-mode".into()),
                Value::Symbol("python-mode".into()),
                Value::Symbol("c-mode".into()),
            ])
        );
    });
}

#[test]
fn ruby_and_js_modes_are_callable_prog_modes() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(equal
              (list
               (with-temp-buffer (funcall 'ruby-mode) (list major-mode comment-start))
               (with-temp-buffer (funcall 'js-mode) (list major-mode comment-start)))
              '((ruby-mode \"# \") (js-mode \"// \")))"
        ),
        Value::T
    );
}

#[test]
fn prog_mode_is_callable_without_recording_fundamental_as_a_parent() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (prog-mode)
               (list major-mode
                     mode-name
                     parse-sexp-ignore-comments
                     (derived-mode-p 'prog-mode)
                     (derived-mode-p 'fundamental-mode)
                     (get 'prog-mode 'derived-mode-parent)))"
        ),
        Value::list([
            Value::Symbol("prog-mode".into()),
            Value::String("Prog".into()),
            Value::T,
            // GNU `derived-mode-p' returns the matched mode symbol.
            Value::Symbol("prog-mode".into()),
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn ruby_mode_marks_single_quotes_as_string_delimiters() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (ruby-mode)
               (list (char-syntax ?')
                     (progn
                       (insert \"'x'\")
                       (nth 3 (syntax-ppss 3)))
                     (nth 8 (syntax-ppss 3))))"
        ),
        Value::list([
            Value::Integer('"' as i64),
            Value::Integer('\'' as i64),
            Value::Integer(1),
        ])
    );
}

#[test]
fn tex_mode_is_callable_and_available_as_mode_symbol() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(condition-case err
                 (with-temp-buffer (funcall tex-mode) major-mode)
               (void-variable (list 'void (cadr err))))"
        ),
        // GNU: `tex-mode' is a function, not a variable; funcalling the
        // variable signals (void-variable tex-mode), probed on GNU 30.2.
        Value::list([Value::symbol("void"), Value::symbol("tex-mode")])
    );
}

#[test]
fn upstream_tex_mode_installs_its_lisp_keymap() {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize interpreter");
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list (boundp 'tex-mode-map) (boundp 'texinfo-mode-map))",
        ),
        Value::list([Value::Nil, Value::Nil])
    );
    interp.load_target("tex-mode").expect("load GNU tex-mode");

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list
               (subrp (indirect-function 'tex-mode))
               (lookup-key tex-mode-map \"\\\"\")
               (lookup-key latex-mode-map \"\\\"\")
               (with-temp-buffer
                 (tex-mode)
                 (list major-mode
                       (eq (current-local-map) latex-mode-map)
                       (key-binding \"\\\"\"))))",
        ),
        Value::list([
            Value::Nil,
            Value::symbol("tex-insert-quote"),
            Value::symbol("tex-insert-quote"),
            Value::list([
                Value::symbol("latex-mode"),
                Value::T,
                Value::symbol("tex-insert-quote"),
            ]),
        ])
    );
}

#[test]
fn upstream_electric_layout_uses_the_c_mode_indent_contract() {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize interpreter");
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list custom-delayed-init-variables
                   electric-indent-mode
                   (get 'electric-indent-mode 'custom-set)
                   (fboundp 'electric-indent-mode)
                   (not (null
                         (memq 'electric-indent-post-self-insert-function
                               post-self-insert-hook))))",
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::symbol("custom-set-minor-mode"),
            Value::T,
            Value::T,
        ])
    );
    interp
        .load_target("elec-pair")
        .expect("load GNU Electric Pair");

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (define-derived-mode plainer-c-mode c-mode \"pC\"
                 (c-toggle-electric-state -1)
                 (setq-local electric-indent-local-mode-hook nil)
                 (setq-local electric-indent-mode-hook nil)
                 (electric-indent-local-mode 1)
                 (dolist (key '(?\\\" ?' ?{ ?} ?\\( ?\\) ?[ ?]))
                   (local-set-key (vector key) 'self-insert-command)))
               (with-temp-buffer
                 (plainer-c-mode)
                 (electric-layout-local-mode 1)
                 (electric-pair-local-mode 1)
                 (electric-indent-local-mode 1)
                 (setq-local electric-layout-rules
                             '((?{ . (after)) (?} . (before))))
                 (insert \"int main () \")
                 (let ((last-command-event ?{))
                   (call-interactively
                    (key-binding (vector last-command-event))))
                 (list indent-line-function
                       (buffer-string)
                       electric-indent-mode
                       electric-layout-mode
                       (not (null
                             (memq
                              'electric-indent-post-self-insert-function
                              post-self-insert-hook)))
                       (not (null
                             (memq
                              'electric-layout-post-self-insert-function
                              post-self-insert-hook))))))",
        ),
        Value::list([
            Value::symbol("c-indent-line"),
            Value::String("int main () {\n  \n}".into()),
            Value::T,
            Value::T,
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn upstream_electric_layout_accepts_a_c_mode_style_callback() {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize interpreter");
    interp
        .load_target("elec-pair")
        .expect("load GNU Electric Pair");

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (define-derived-mode plainer-c-mode c-mode \"pC\"
                 (c-toggle-electric-state -1)
                 (setq-local electric-indent-local-mode-hook nil)
                 (setq-local electric-indent-mode-hook nil)
                 (electric-indent-local-mode 1)
                 (dolist (key '(?\\\" ?' ?{ ?} ?\\( ?\\) ?[ ?]))
                   (local-set-key (vector key) 'self-insert-command)))
               (defun electric-layout-for-c-style-du-jour (inserted)
                 (when (memq inserted '(?{ ?}))
                   (save-excursion
                     (backward-char 2)
                     (c-point-syntax)
                     (forward-char)
                     (c-brace-newlines (c-point-syntax)))))
               (with-temp-buffer
                 (plainer-c-mode)
                 (electric-layout-local-mode 1)
                 (electric-pair-local-mode 1)
                 (electric-indent-local-mode 1)
                 (setq-local electric-layout-rules
                             '(electric-layout-for-c-style-du-jour))
                 (insert \"int main () \")
                 (let ((last-command-event ?{))
                   (call-interactively
                    (key-binding (vector last-command-event))))
                 (buffer-string)))",
        ),
        Value::String("int main ()\n{\n  \n}\n".into())
    );
}

#[test]
fn upstream_files_lisp_owns_remote_file_policy() {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize interpreter");

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list
               (subrp (indirect-function 'file-remote-p))
               (subrp (indirect-function 'file-local-name))
               (subrp (indirect-function 'file-local-copy))
               (find-file-name-handler
                \"/ftp:who@foo.com:/whatever\" 'file-remote-p)
               (fboundp 'vc-file-getprop)
               ;; Loading files.el may or may not emit a diagnostic depending
               ;; on the reader warnings enabled by that source revision.
               ;; Neither case belongs to remote-file policy.  What matters
               ;; here is that loading it cannot dirty a save-eligible buffer;
               ;; only the diagnostic Messages buffer may be modified.
               (seq-every-p
                (lambda (buffer)
                  (or (not (buffer-modified-p buffer))
                      (and (equal (buffer-name buffer) \"*Messages*\")
                           (not (buffer-local-value
                                 'buffer-offer-save buffer))
                           (not (local-variable-p
                                 'buffer-offer-save buffer)))))
                (buffer-list))
               (progn
                 (defun sample-remote-handler
                     (operation file &optional _identification _connected)
                   (and (equal file \"/ftp:who@foo.com:/whatever\")
                        (cond
                         ((eq operation 'file-remote-p)
                          (if (eq _identification 'localname)
                              \"/whatever\"
                            \"/ftp:who@foo.com:\"))
                         ((eq operation 'file-local-copy)
                          \"/tmp/local-copy\"))))
                 (let ((file-name-handler-alist
                        '((\"\\\\`/ftp:\" . sample-remote-handler))))
                   (list
                    (file-remote-p \"/ftp:who@foo.com:/whatever\")
                    (file-local-name \"/ftp:who@foo.com:/whatever\")
                    (file-local-copy \"/ftp:who@foo.com:/whatever\")))))",
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::symbol("tramp-autoload-file-name-handler"),
            Value::T,
            Value::T,
            Value::list([
                Value::String("/ftp:who@foo.com:".into()),
                Value::String("/whatever".into()),
                Value::String("/tmp/local-copy".into()),
            ]),
        ])
    );
}

#[test]
fn upstream_nonessential_remote_probe_accepts_an_unknown_method_without_connecting() {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize interpreter");

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list
               (boundp 'non-essential)
               (special-variable-p 'non-essential)
               (file-remote-p \"/method:host:\")
               (file-remote-p \"/method:host:\" 'method)
               (file-remote-p \"/method:host:\" 'host))",
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::String("/method:host:".into()),
            Value::String("method".into()),
            Value::String("host".into()),
        ])
    );
}

#[test]
fn upstream_save_policy_only_queries_buffers_that_offer_to_save() {
    let _permit = crate::test_support::acquire_host_test_permit();
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    eval_str_with(&mut interp, "(require 'cl-macs)");

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list
               (let (buffers prompts)
                 (unwind-protect
                     (progn
                       (dolist (spec '((\"emaxx-offer-1\" t)
                                       (\"emaxx-offer-2\" always)
                                       (\"emaxx-offer-3\" nil)))
                         (let ((buffer (generate-new-buffer (car spec))))
                           (push buffer buffers)
                           (with-current-buffer buffer
                             (setq buffer-offer-save (cadr spec))
                             (insert \"modified\"))))
                       (with-current-buffer (car buffers)
                         (cl-letf (((symbol-function 'read-event)
                                    (lambda (&rest _)
                                      (push t prompts)
                                      ?n))
                                   ((symbol-function 'kill-emacs) #'ignore))
                           (save-buffers-kill-emacs)))
                       (length prompts))
                   (dolist (buffer buffers)
                     (with-current-buffer buffer
                       (set-buffer-modified-p nil))
                     (kill-buffer buffer))))
               (let ((process
                      (make-pipe-process :name \"emaxx-query-on-exit\"))
                     prompts)
                 (unwind-protect
                     (cl-letf (((symbol-function 'yes-or-no-p)
                                (lambda (prompt)
                                  (push prompt prompts)
                                  nil))
                               ((symbol-function 'kill-emacs) #'ignore))
                       (let ((confirm-kill-processes nil))
                         (save-buffers-kill-emacs))
                       prompts)
                   (delete-process process))))",
        ),
        Value::list([Value::Integer(2), Value::Nil,])
    );
}

#[test]
fn text_mode_marks_quotes_as_text_punctuation() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (text-mode)
               (list (char-syntax ?\") (char-syntax ?`) (char-syntax ?')))"
        ),
        Value::list([
            Value::Integer('.' as i64),
            Value::Integer('.' as i64),
            Value::Integer('w' as i64),
        ])
    );
}

#[test]
fn kill_all_local_variables_runs_change_major_mode_hook_before_clearing_locals() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (let (ran)
                 (add-hook 'change-major-mode-hook
                           (lambda () (setq ran (local-variable-p 'sample-local)))
                           nil t)
                 (setq-local sample-local t)
                 (kill-all-local-variables)
                 (list ran (local-variable-p 'sample-local))))"
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn normal_mode_runs_change_major_mode_hook_before_selecting_file_mode() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((file (make-temp-file \"emaxx-normal-mode\" nil \".el\")))
               (unwind-protect
                   (let ((buf (find-file-noselect file)))
                     (with-current-buffer buf
                       (let (ran)
                         (add-hook 'change-major-mode-hook
                                   (lambda () (setq ran (local-variable-p 'sample-local)))
                                   nil t)
                         (setq-local sample-local t)
                         (normal-mode)
                         (prog1
                             (list ran (local-variable-p 'sample-local) major-mode)
                           (kill-buffer buf)))))
                 (ignore-errors (delete-file file))))"
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::Symbol("emacs-lisp-mode".into())
        ])
    );
}

#[test]
fn normal_mode_applies_no_byte_compile_file_local_header() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "files",
            "(let ((file (make-temp-file \"emaxx-no-byte-compile\" nil \".el\")))
               (unwind-protect
                   (progn
                     (write-region \";; -*- no-byte-compile: t; lexical-binding: t; -*-\\n\" nil file nil 'silent)
                     (let ((buf (find-file-noselect file)))
                       (with-current-buffer buf
                         (normal-mode)
                         (prog1 no-byte-compile
                           (kill-buffer buf)))))
                 (ignore-errors (delete-file file))))",
        ),
        Value::T
    );
}

#[test]
fn eval_accepts_explicit_lexical_alist() {
    assert_eq!(
        eval_str("(eval '(+ x y) '((x . 1) (y . 2)))"),
        Value::Integer(3)
    );
}

#[test]
fn eval_t_preserves_gnu_local_defvar_environment_and_macro_dynvars() {
    assert_eq!(
        eval_str(
            "(eval
               '(progn
                  (defvar sample-eval-local-special)
                  (defalias 'sample-eval-local-reader
                    (function (lambda () sample-eval-local-special)))
                  (defalias 'sample-eval-local-call
                    (function
                     (lambda (value)
                       (let ((sample-eval-local-special value))
                         (sample-eval-local-reader)))))
                  (defalias 'sample-eval-local-macro
                    (cons 'macro
                          (function
                           (lambda (&rest ignored)
                             (list 'quote macroexp--dynvars)))))
                  (list (sample-eval-local-call 42)
                        (special-variable-p 'sample-eval-local-special)
                        (aref (symbol-function 'sample-eval-local-reader) 2)
                        (sample-eval-local-macro)))
               t)"
        ),
        Value::list([
            Value::Integer(42),
            Value::Nil,
            Value::list([Value::symbol("sample-eval-local-special"), Value::T,]),
            Value::list([Value::T, Value::symbol("sample-eval-local-special"),]),
        ])
    );
}

#[test]
fn separate_lexical_evals_do_not_share_local_defvar_declarations() {
    assert_eq!(
        eval_str(
            "(progn
               (eval
                '(progn
                   (defvar sample-eval-nonleaking-special)
                   (defalias 'sample-eval-special-owner
                     (function (lambda () sample-eval-nonleaking-special))))
                t)
               (funcall
                (eval
                 '(let ((sample-eval-nonleaking-special 84))
                    (function (lambda () sample-eval-nonleaking-special)))
                 t)))"
        ),
        Value::Integer(84)
    );
}

#[test]
fn earlier_closure_calls_do_not_erase_later_local_defvar_declarations() {
    assert_eq!(
        eval_str(
            "(progn
               (eval
                '(progn
                   (defvar sample-old-special)
                   (defalias 'sample-old-call (function (lambda () nil)))
                   (defvar sample-late-special)
                   (defalias 'sample-late-reader
                     (function (lambda () sample-late-special)))
                   (defalias 'sample-late-call
                     (function
                      (lambda (value)
                        (let ((sample-late-special value))
                          (sample-late-reader))))))
                t)
               (eval '(sample-old-call) t)
               (list
                (eval '(sample-late-call 42) t)
                (aref (symbol-function 'sample-old-call) 2)
                (aref (symbol-function 'sample-late-call) 2)))"
        ),
        Value::list([
            Value::Integer(42),
            Value::list([Value::symbol("sample-old-special"), Value::T]),
            Value::list([
                Value::symbol("sample-late-special"),
                Value::symbol("sample-old-special"),
                Value::T,
            ]),
        ])
    );
}

#[test]
fn local_defvar_lifetimes_follow_gnu_let_and_letstar_environments() {
    assert_eq!(
        eval_str(
            "(progn
               (defalias 'sample-empty-reader
                 (function (lambda () sample-empty-special)))
               (defalias 'sample-scoped-reader
                 (function (lambda () sample-scoped-special)))
               (defalias 'sample-init-reader
                 (function (lambda () sample-init-special)))
               (defalias 'sample-star-reader
                 (function (lambda () sample-star-special)))
               (eval
                '(progn
                   (let () (defvar sample-empty-special))
                   (let ((sample-scope-binding 1))
                     (defvar sample-scoped-special))
                   (setq sample-post-scope-environment
                         (aref (function (lambda () nil)) 2))
                   (let ((sample-init-binding
                          (progn (defvar sample-init-special) 1)))
                     sample-init-binding)
                   (let* ((sample-star-binding
                           (progn (defvar sample-star-special) 1)))
                     sample-star-binding)
                   (list
                    sample-post-scope-environment
                    (let ((sample-empty-special 10))
                      (sample-empty-reader))
                    (condition-case nil
                        (let ((sample-scoped-special 20))
                          (sample-scoped-reader))
                      (void-variable 'void))
                    (let ((sample-init-special 30))
                      (sample-init-reader))
                    sample-star-binding
                    (let ((sample-star-special 40))
                      (sample-star-reader))))
                t))"
        ),
        Value::list([
            Value::list([Value::symbol("sample-empty-special"), Value::T]),
            Value::Integer(10),
            Value::symbol("void"),
            Value::Integer(30),
            Value::Integer(1),
            Value::Integer(40),
        ])
    );
}

#[test]
fn dynamic_functions_hide_lexical_callers_and_dynamically_bind_arguments() {
    assert_eq!(
        eval_str(
            "(progn
               (defalias 'sample-dynamic-reader
                 (eval '(function (lambda () sample-dynamic-value)) nil))
               (defalias 'sample-dynamic-argument
                 (eval
                  '(function
                    (lambda (sample-dynamic-value)
                      (sample-dynamic-reader)))
                  nil))
               (list
                (condition-case nil
                    (eval
                     '(let ((sample-dynamic-value 1))
                        (sample-dynamic-reader))
                     t)
                  (void-variable 'void))
                (eval
                 '(let ((sample-dynamic-value 2))
                    (sample-dynamic-reader))
                 nil)
                (sample-dynamic-argument 3)))"
        ),
        Value::list([Value::symbol("void"), Value::Integer(2), Value::Integer(3),])
    );
}

#[test]
fn noncons_eval_lexical_argument_uses_a_fresh_empty_environment() {
    assert_eq!(
        eval_str(
            "(progn
               (setq sample-eval-fresh-environment 'global)
               (eval
                '(let ((sample-eval-fresh-environment 'outer-lexical))
                   (eval 'sample-eval-fresh-environment 'fresh-lexical))
                t))"
        ),
        Value::symbol("global")
    );
}

#[test]
fn eval_lexical_argument_controls_macroexpander_lexical_binding() {
    assert_eq!(
        eval_str(
            "(progn
               (defmacro sample-eval-lexical-probe () lexical-binding)
               (let ((lexical-binding nil))
                 (list (eval '(sample-eval-lexical-probe) nil)
                       (eval '(sample-eval-lexical-probe) t)
                       (eval '(list lexical-binding
                                    (sample-eval-lexical-probe))
                             t))))"
        ),
        Value::list([Value::Nil, Value::T, Value::list([Value::Nil, Value::T]),])
    );
}

#[test]
fn lexical_eval_does_not_bind_lexical_binding_for_non_macro_calls() {
    assert_eq!(
        eval_str(
            "(progn
               (defvar emaxx-test-macro-probe-events nil)
               (setq emaxx-test-macro-probe-events nil)
               (let ((watcher
                      (lambda (&rest args)
                        (setq emaxx-test-macro-probe-events
                              (cons args emaxx-test-macro-probe-events)))))
                 (add-variable-watcher 'lexical-binding watcher)
                 (eval '(+ 1 2) t)
                 (remove-variable-watcher 'lexical-binding watcher)
                 (length emaxx-test-macro-probe-events)))"
        ),
        Value::Integer(0)
    );
}

#[test]
fn backquote_splices_vector_values_without_internal_marker() {
    assert_eq!(
        eval_str("(let ((vec [ba bb bc])) `(a ,@vec c))"),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("ba".into()),
            Value::Symbol("bb".into()),
            Value::Symbol("bc".into()),
            Value::Symbol("c".into())
        ])
    );
}

#[test]
fn nested_backquote_splices_vector_result_without_internal_marker() {
    assert_eq!(
        eval_str("(let ((lst '(ba bb bc))) `(a ,@`[,@lst] c))"),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("ba".into()),
            Value::Symbol("bb".into()),
            Value::Symbol("bc".into()),
            Value::Symbol("c".into())
        ])
    );
}

#[test]
fn macroexpanded_backquote_preserves_vector_templates() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((lst '(ba bb bc))
                   (vec [ba bb bc]))
               (eval
                (macroexpand-all
                 '(list (equal vec `[,@lst])
                        (equal `(a ,`[,@lst] c) `(a ,vec c))))
                `((lst . ,lst) (vec . ,vec))))"
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn macroexpanded_backquote_preserves_a_dynamic_dotted_vector_tail() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((thingy '(1 2 3)))
               (eval
                (macroexpand-all
                 '(let ((thingy '(1 2 3)))
                    `((abc . [9 ,thingy]) (def))))))"
        ),
        Value::list([
            Value::cons(
                Value::symbol("abc"),
                Value::list([
                    Value::symbol("vector-literal"),
                    Value::Integer(9),
                    Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3),]),
                ]),
            ),
            Value::list([Value::symbol("def")]),
        ])
    );
}

#[test]
fn find_file_noselect_runs_find_file_hook_when_semantic_init_hook_is_nil() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((file (make-temp-file \"emaxx-find-file-hook\")))
               (unwind-protect
                   (progn
                     (defvar semantic-init-hook nil)
                     (defvar find-file-hook nil)
                     (setq emaxx-find-file-hook-ran nil)
                     (let ((semantic-init-hook nil)
                           (find-file-hook
                            (list
                             (lambda ()
                               (setq emaxx-find-file-hook-ran
                                     buffer-file-name)))))
                       (let ((buffer (find-file-noselect file)))
                         (kill-buffer buffer)))
                     (and emaxx-find-file-hook-ran t))
                 (delete-file file)))"
        ),
        Value::T
    );
}

#[test]
fn eql_does_not_compare_distinct_strings_by_contents() {
    assert_eq!(
        eval_str(
            "(let ((a (number-to-string 1))
                   (b (number-to-string 1)))
               (list (eql a b) (equal a b)))"
        ),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn cl_mismatch_key_uses_eql_for_default_test() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (require 'cl-seq)
               (let ((list '(1 2 3 4 5 2 6)))
                 (cl-mismatch list list :key #'number-to-string)))"
        ),
        Value::Integer(0)
    );
}

#[test]
fn cl_substitute_updates_list_copy_through_setf_elt() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (require 'cl-seq)
               (let ((list '(1 2 3 4 5 2 6)))
                 (list (cl-substitute 'b 2 list)
                       list)))"
        ),
        Value::list([
            Value::list([
                Value::Integer(1),
                Value::Symbol("b".into()),
                Value::Integer(3),
                Value::Integer(4),
                Value::Integer(5),
                Value::Symbol("b".into()),
                Value::Integer(6),
            ]),
            Value::list([
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4),
                Value::Integer(5),
                Value::Integer(2),
                Value::Integer(6),
            ]),
        ])
    );
}

#[test]
fn backtrace_get_frames_reports_live_lisp_call_symbols() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (require 'backtrace)
               (defun emaxx-backtrace-make (arg)
                 (emaxx-backtrace-setup))
               (defun emaxx-backtrace-setup ()
                 (mapcar #'backtrace-frame-fun (backtrace-get-frames)))
               (let ((frames (emaxx-backtrace-make 'value)))
                 (list (not (null (memq 'backtrace-get-frames frames)))
                       (not (null (memq 'emaxx-backtrace-setup frames)))
                       (not (null (memq 'emaxx-backtrace-make frames))))))"
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn backtrace_print_marks_last_frame_with_index_property() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (require 'backtrace)
               (defun emaxx-backtrace-print-make (arg)
                 (emaxx-backtrace-print-setup))
               (defun emaxx-backtrace-print-setup ()
                 (backtrace-mode)
                 (setq backtrace-frames (backtrace-get-frames))
                 (let ((this-index))
                   (dotimes (index (length backtrace-frames))
                     (when (eq (backtrace-frame-fun (nth index backtrace-frames))
                               'emaxx-backtrace-print-make)
                       (setq this-index index)))
                   (setq backtrace-frames
                         (seq-subseq backtrace-frames 0 (1+ this-index))))
                 (backtrace-print)
                 (unless (string-match-p \"backtrace-get-frames\" (buffer-string))
                   (error (buffer-string)))
                 (goto-char (point-max))
                 (forward-line -1)
                 (backtrace-get-index))
               (emaxx-backtrace-print-make 'value))"
        ),
        Value::Integer(3)
    );
}

#[test]
fn backtrace_backward_frame_signals_user_error_from_header() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (require 'backtrace)
               (defun emaxx-backtrace-header-make (arg)
                 (emaxx-backtrace-header-setup))
               (defun emaxx-backtrace-header-setup ()
                 (backtrace-mode)
                 (setq backtrace-frames (backtrace-get-frames))
                 (let ((this-index))
                   (dotimes (index (length backtrace-frames))
                     (when (eq (backtrace-frame-fun (nth index backtrace-frames))
                               'emaxx-backtrace-header-make)
                       (setq this-index index)))
                   (setq backtrace-frames
                         (seq-subseq backtrace-frames 0 (1+ this-index))))
                 (let ((inhibit-read-only t))
                   (insert \"Test header\n\"))
                 (backtrace-print)
                 (goto-char 3)
                 (condition-case err
                     (progn (backtrace-backward-frame) 'no-error)
                   (error (car err))))
               (emaxx-backtrace-header-make 'value))"
        ),
        Value::Symbol("user-error".into())
    );
}

#[test]
fn backtrace_backward_frame_should_error_keeps_point() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (load \"../emacs/test/lisp/emacs-lisp/backtrace-tests.el\")
               (ert-with-test-buffer (:name \"backward\")
                 (let ((results (concat backtrace-tests--header
                                        (backtrace-tests--result nil))))
                   (backtrace-tests--make-backtrace nil)
                   (setq backtrace-insert-header-function
                         #'backtrace-tests--insert-header)
                   (backtrace-print)
                   (goto-char (+ (point-min) (/ (length backtrace-tests--header) 2)))
                   (let ((pos (point)))
                     (should-error (backtrace-backward-frame))
                     (= pos (point))))))"
        ),
        Value::T
    );
}

#[test]
fn backtrace_print_includes_unevaluated_setq_frame() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (load \"../emacs/test/lisp/emacs-lisp/backtrace-tests.el\")
               (ert-with-test-buffer (:name \"backward\")
                 (backtrace-tests--make-backtrace nil)
                 (setq backtrace-insert-header-function
                       #'backtrace-tests--insert-header)
                 (backtrace-print)
                 (and (string-match-p
                       \"(setq backtrace-frames (backtrace-get-frames))\"
                       (backtrace-tests--get-substring (point-min) (point-max)))
                      t)))"
        ),
        Value::T
    );
}

#[test]
fn backtrace_locals_show_lambda_arguments_for_requested_frame() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (load \"../emacs/test/lisp/emacs-lisp/backtrace-tests.el\")
               (ert-with-test-buffer (:name \"locals\")
                 (backtrace-tests--make-backtrace 'value)
                 (backtrace-print)
                 (goto-char (point-max))
                 (forward-line -1)
                 (backtrace-toggle-locals)
                 (and (string-match-p
                       \"arg = value\"
                       (backtrace-tests--get-substring (point) (point-max)))
                      t)))"
        ),
        Value::T
    );
}

#[test]
fn backtrace_eval_reads_and_updates_suspended_lexical_bindings() {
    assert_eq!(
        eval_str(
            "(progn
               (defun sample-backtrace-eval (arg)
                 (let ((local 7))
                   (list
                    (backtrace-eval '(list arg local) 0 'backtrace-eval)
                    (progn
                      (backtrace-eval '(setq local 9) 0 'backtrace-eval)
                      local))))
               (sample-backtrace-eval 3))"
        ),
        Value::list([
            Value::list([Value::Integer(3), Value::Integer(7)]),
            Value::Integer(9),
        ])
    );
}

#[test]
fn backtrace_eval_selects_a_debugged_bytecode_callers_lexical_context() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (require 'edebug)
               (defalias 'sample-bytecode-bridge
                 (byte-compile
                  '(lambda (form)
                     (backtrace-eval form 0 'sample-bytecode-bridge))))
               (let ((edebug-entered t))
                 (eval
                  '(let ((lexical 17))
                     (sample-bytecode-bridge 'lexical))
                  t)))"
        ),
        Value::Integer(17)
    );
}

#[test]
fn backtrace_expand_ellipses_reprints_current_frame_without_limit() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (load \"../emacs/test/lisp/emacs-lisp/backtrace-tests.el\")
               (ert-with-test-buffer (:name \"expand\")
                 (let* ((print-level nil)
                        (print-length nil)
                        (backtrace-line-length 300)
                        (arg (make-list 40 (make-string 10 ?a))))
                   (backtrace-tests--make-backtrace arg)
                   (backtrace-print)
                   (goto-char (point-min))
                   (search-forward \"...\")
                   (backward-char)
                   (push-button)
                   (not (string-match-p
                         \"\\\\.\\\\.\\\\.\"
                         (backtrace-tests--get-substring
                          (point-min) (point-max)))))))"
        ),
        Value::T
    );
}

#[test]
fn primitive_print_variables_are_dynamic_across_function_calls() {
    assert_eq!(
        eval_str(
            "(progn
               (defun emaxx-test-prin1 (value) (prin1-to-string value))
               (let ((print-length 1))
                 (emaxx-test-prin1 '(1 2 3))))"
        ),
        Value::String("(1 ...)".into())
    );
}

#[test]
fn field_string_uses_point_and_preserves_text_properties() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert (propertize \"prompt\" 'field 'prompt 'face 'bold)
                       (propertize \"input\" 'field 'input))
               (goto-char 2)
               (list
                (equal-including-properties
                 (field-string)
                 (propertize \"prompt\" 'field 'prompt 'face 'bold))
                (equal (field-string-no-properties nil) \"prompt\")))"
        ),
        Value::list([Value::T, Value::T])
    );

    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert (propertize
                        \"P> \"
                        'field 'prompt
                        'front-sticky '(field)
                        'rear-nonsticky '(field)))
               (insert \"echo\\n\")
               (goto-char 4)
               (list (get-char-property 3 'field)
                     (get-char-property 4 'field)
                     (get-pos-property 4 'field)
                     (field-beginning)
                     (field-end)
                     (field-string-no-properties)))"
        ),
        Value::list([
            Value::Symbol("prompt".into()),
            Value::Nil,
            Value::Nil,
            Value::Integer(4),
            Value::Integer(9),
            Value::String("echo\n".into()),
        ])
    );
}

#[test]
fn line_edge_motion_stops_at_field_boundaries() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(with-temp-buffer
                 (insert (propertize
                          "P> "
                          'field 'prompt
                          'front-sticky '(field)
                          'rear-nonsticky '(field)))
                 (insert "echo hello")
                 (goto-char (point-max))
                 (move-beginning-of-line 1)
                 (let ((beginning (point)))
                   (move-end-of-line 1)
                   (list beginning (point))))"#
        ),
        Value::list([Value::Integer(4), Value::Integer(14)])
    );
}

#[test]
fn move_end_of_line_crosses_a_leading_timestamp_field() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(with-temp-buffer
                 (insert (propertize "[00:00] "
                                     'field 'erc-timestamp
                                     'cursor-intangible t))
                 (insert "Welcome")
                 (goto-char (point-min))
                 (let ((end (pos-eol)))
                   (move-end-of-line 1)
                   (list end (point))))"#
        ),
        Value::list([Value::Integer(16), Value::Integer(16)])
    );
}

#[test]
fn preloaded_mark_whole_buffer_is_interactive_and_sets_region() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(with-temp-buffer
                 (insert "abc")
                 (goto-char 2)
                 (call-interactively 'mark-whole-buffer)
                 (list (point) (mark) mark-active))"#
        ),
        Value::list([Value::Integer(1), Value::Integer(4), Value::T])
    );
}

#[test]
fn get_pos_property_obeys_overlay_endpoint_advancement() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert \"abcde\")
               (let ((ordinary (make-overlay 2 4))
                     (advancing (make-overlay 2 4 nil t t)))
                 (overlay-put ordinary 'ordinary t)
                 (overlay-put advancing 'advancing t)
                 (list (get-pos-property 2 'ordinary)
                       (get-pos-property 4 'ordinary)
                       (get-pos-property 2 'advancing)
                       (get-pos-property 4 'advancing))))"
        ),
        Value::list([Value::T, Value::Nil, Value::Nil, Value::T])
    );
}

#[test]
fn buffer_file_name_binding_crosses_function_calls_but_not_buffers() {
    let mut interp = upstream_lisp_test_interpreter("bookmark-tests.el");
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list
                (special-variable-p 'buffer-file-name)
                (with-temp-buffer
                  (let ((buffer-file-name \"test\")
                        (bookmark-make-record-function
                         (lambda () '(((position . 2))))))
                    (list
                     (bookmark-make-record)
                     (with-temp-buffer buffer-file-name)))))"
        ),
        Value::list([
            Value::T,
            Value::list([
                Value::list([
                    Value::String("test".into()),
                    Value::list([
                        Value::cons(Value::Symbol("position".into()), Value::Integer(2),),
                        Value::list([
                            Value::Symbol("defaults".into()),
                            Value::String("test".into()),
                        ]),
                    ]),
                ]),
                Value::Nil,
            ]),
        ])
    );
}

#[test]
fn comment_region_wraps_c_style_and_prefixes_hash_comments() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(equal
              (list
               (with-temp-buffer
                 (funcall 'c-mode)
                 (insert \"z\")
                 (comment-region (point-min) (point-max))
                 (buffer-string))
               (with-temp-buffer
                 (funcall 'ruby-mode)
                 (insert \"z\")
                 (comment-region (point-min) (point-max))
                 (buffer-string)))
              '(\"/* z */\" \"# z\"))"
        ),
        Value::T
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
              (emacs-lisp-mode)
              (insert \"z\")
              (comment-region (point-min) (point-max))
              (buffer-string))"
        ),
        Value::String(";; z".into())
    );
}

#[test]
fn matching_paren_returns_counterpart_character() {
    assert_eq!(
        eval_str("(list (matching-paren ?\\() (matching-paren ?\\]) (matching-paren ?x))"),
        Value::list([
            Value::Integer(')' as i64),
            Value::Integer('[' as i64),
            Value::Nil,
        ])
    );
}

#[test]
fn syntax_ppss_reports_negative_depth_for_extra_closing_parens() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer (funcall 'c-mode) (insert \" (())) \") (syntax-ppss (point-max)))"
        ),
        Value::list([
            Value::Integer(-1),
            Value::Nil,
            // GNU records the closed (()) list as the last complete sexp
            // even after depth goes negative.
            Value::Integer(2),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Integer(-1),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::list([
                Value::Nil,
                Value::Nil,
                Value::Integer(-1),
                Value::Integer(-1),
            ]),
        ])
    );
}

#[test]
fn parse_partial_sexp_respects_mutated_public_oldstate_depth() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (set-syntax-table (make-syntax-table))
               (modify-syntax-entry ?> \")<\")
               (insert \">x\")
               (let ((state (parse-partial-sexp 1 3 -1)))
                 (setcar state 0)
                 (let ((resumed (parse-partial-sexp (point) 3 -1 nil state)))
                   (list (car state) (car resumed) (point)))))"
        ),
        Value::list([Value::Integer(0), Value::Integer(0), Value::Integer(3)])
    );
}

#[test]
fn font_lock_defaults_honor_syntax_ppss_table_and_syntactic_face_function() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"(progn
                   (require 'syntax)
                   (with-temp-buffer
                     (let ((table (make-syntax-table)))
                       (modify-syntax-entry ?\" "\"" table)
                       (modify-syntax-entry ?' "\"" table)
                       (modify-syntax-entry ?\( "()" table)
                       (modify-syntax-entry ?\) ")(" table)
                       (setq-local syntax-ppss-table table))
                     (insert "(\"a\" 'b') \"text\" 'text'")
                     (setq-local
                      font-lock-defaults
                      '(nil nil nil nil
                        (font-lock-syntactic-face-function
                         . (lambda (state)
                             (and (nth 3 state) (nth 9 state)
                                  'font-lock-string-face)))))
                     (font-lock-ensure)
                     (list
                      (get-text-property 3 'face)
                      (get-text-property 7 'face)
                      (get-text-property 12 'face)
                      (get-text-property 19 'face)
                      (functionp font-lock-syntactic-face-function))))"#
            ),
            Value::list([
                Value::symbol("font-lock-string-face"),
                Value::symbol("font-lock-string-face"),
                Value::Nil,
                Value::Nil,
                Value::T,
            ])
        );
    });
}

#[test]
fn obsolete_labels_rewrites_function_quoted_local_bindings() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"(progn
                   (require 'cl)
                   (with-suppressed-warnings ((obsolete labels))
                     (funcall (labels ((foo () t)) #'foo))))"#
            ),
            Value::T
        );
    });
}

#[test]
fn preloaded_syntax_descriptor_helpers_match_subr_el() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(with-temp-buffer
               (insert \"(a)\")
               (let ((parse-sexp-lookup-properties t))
                 (put-text-property 2 3 'syntax-table (string-to-syntax \"_\"))
                 (list (syntax-class (syntax-after 1))
                       (syntax-class (syntax-after 2))
                       (syntax-class (syntax-after 3))
                       (syntax-after 0)
                       (syntax-after 4)
                       (syntax-class nil))))"
        ),
        Value::list([
            Value::Integer(4),
            Value::Integer(3),
            Value::Integer(5),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn standard_syntax_table_exposes_its_default_punctuation_descriptor() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(&mut interp, "(aref (standard-syntax-table) ?.)"),
        Value::list([Value::Integer(1)])
    );
}

#[test]
fn syntax_ppss_honors_a_syntax_table_valued_text_property() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(with-temp-buffer
               (insert \"(x)\")
               (let ((table (make-syntax-table))
                     (parse-sexp-lookup-properties t))
                 (modify-syntax-entry ?\\( \".\" table)
                 (put-text-property 1 2 'syntax-table table)
                 (butlast (syntax-ppss 3))))"
        ),
        Value::list([
            Value::Integer(0),
            Value::Nil,
            Value::Integer(2),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Integer(0),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn syntax_propertize_extends_a_short_request_to_its_safe_chunk_boundary() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(with-temp-buffer
               (insert \"abc\")
               (let ((syntax-propertize--done (point-min))
                     (syntax-propertize-function
                      (lambda (start end) (setq-local seen (list start end))))
                     (syntax-propertize-extend-region-functions nil))
                 (syntax-propertize 2)
                 (list seen syntax-propertize--done)))"
        ),
        Value::list([
            Value::list([Value::Integer(1), Value::Integer(4)]),
            Value::Integer(4),
        ])
    );
}

#[test]
fn scan_sexps_preserves_match_data_changed_by_lazy_syntax_propertization() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(with-temp-buffer
               (insert \"(x)\")
               (let ((syntax-propertize--done -1)
                     (syntax-propertize-function
                      (lambda (start end)
                        (goto-char start)
                        (re-search-forward \"x\" end t)))
                     (syntax-propertize-extend-region-functions nil))
                 (string-match \"\\\\(a\\\\)\" \"a\")
                 (let ((before (match-data 'integers)))
                   (list (scan-sexps 1 1)
                         before
                         (match-data 'integers)))))"
        ),
        Value::list([
            Value::Integer(4),
            Value::list([
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(0),
                Value::Integer(1),
            ]),
            Value::list([
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(0),
                Value::Integer(1),
            ]),
        ])
    );
}

#[test]
fn c_toggle_electric_state_updates_c_electric_flag() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
              (require 'cc-mode)
              (setq c-electric-flag t)
              (c-toggle-electric-state -1)
              (prog1 c-electric-flag
                (c-toggle-electric-state 1)))"
        ),
        Value::Nil
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn (require 'cc-mode) (setq c-electric-flag nil) (c-toggle-electric-state 1) c-electric-flag)"
        ),
        Value::T
    );
}

#[test]
fn self_insert_command_uses_last_command_event_and_runs_hook() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
              (setq-local post-self-insert-hook
                          (list (lambda () (insert \"!\"))))
              (let ((last-command-event ?x))
                (call-interactively 'self-insert-command))
              (buffer-string))"
        ),
        Value::String("x!".into())
    );
}

#[test]
fn self_insert_command_accepts_an_explicit_character() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (let (seen)
                 (add-hook 'post-self-insert-hook
                           (lambda () (setq seen last-command-event)) nil t)
                 (self-insert-command 2 ?/)
                 (list (buffer-string) seen last-command-event)))"
        ),
        Value::list([
            Value::String("//".into()),
            Value::Integer('/' as i64),
            Value::Integer('/' as i64),
        ])
    );
}

#[test]
fn self_insert_command_expands_an_active_word_abbrev_before_punctuation() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (require 'abbrev)
                 (define-abbrev-table 'sample-self-insert-abbrev-table
                   '(("foo" "expanded")))
                 (let ((noninteractive t))
                   (with-temp-buffer
                     (setq-local local-abbrev-table
                                 sample-self-insert-abbrev-table)
                     (abbrev-mode 1)
                     (insert "foo")
                     (self-insert-command 1 ?\s)
                     (buffer-string))))"#,
        ),
        Value::String("expanded ".into())
    );
}

#[test]
fn beginning_of_line_crosses_an_unterminated_final_line_to_eob() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                 (insert "first\nlast")
                 (goto-char (point-min))
                 (forward-line 1)
                 (let ((forward-result (forward-line 1)))
                   (goto-char (point-min))
                   (forward-line 1)
                   (beginning-of-line 2)
                   (list forward-result (point) (point-max) (eobp))))"#,
        ),
        Value::list([
            Value::Integer(0),
            Value::Integer(11),
            Value::Integer(11),
            Value::T,
        ])
    );
}

#[test]
fn line_beginning_position_crosses_an_unterminated_final_line_to_eob() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                 (insert "last")
                 (goto-char (point-min))
                 (list (line-beginning-position 2)
                       (save-excursion
                         (beginning-of-line 2)
                         (point))
                       (point-max)))"#,
        ),
        Value::list([Value::Integer(5), Value::Integer(5), Value::Integer(5)])
    );
}

#[test]
fn execute_kbd_macro_self_insert_binding_sets_last_command_event() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer (execute-kbd-macro (kbd \"SPC\")) (buffer-string))"
        ),
        Value::String(" ".into())
    );
}

#[test]
fn kmacro_frontier_incremental_search_exposes_each_command_loop_step() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let (commands)
                 (add-hook 'pre-command-hook
                           (lambda () (push this-command commands)))
                 (with-temp-buffer
                   (set-window-buffer nil (current-buffer))
                   (insert "Windows Indic")
                   (goto-char (point-min))
                   (execute-kbd-macro (kbd "C-s Ind ESC"))
                   (list (point)
                         (buffer-substring (- (point) 3) (point))
                         (nreverse commands))))"#
        ),
        Value::list([
            Value::Integer(12),
            Value::String("Ind".into()),
            Value::list([
                Value::Symbol("isearch-forward".into()),
                Value::Symbol("isearch-printing-char".into()),
                Value::Symbol("isearch-printing-char".into()),
                Value::Symbol("isearch-printing-char".into()),
            ]),
        ])
    );
}

#[test]
fn kmacro_frontier_lookup_key_reports_meta_prefixes_in_input_event_units() {
    assert_eq!(
        eval_str("(lookup-key global-map [134217848 ?a])"),
        Value::Integer(1)
    );
}

#[test]
fn kmacro_frontier_literal_angle_bracket_bindings_remain_distinct() {
    assert_eq!(
        eval_str(
            r#"(let ((map (make-sparse-keymap)))
                 (define-key map "<" 'less-command)
                 (define-key map ">" 'greater-command)
                 (list (lookup-key map "<") (lookup-key map ">")))"#
        ),
        Value::list([
            Value::Symbol("less-command".into()),
            Value::Symbol("greater-command".into()),
        ])
    );
}

#[test]
fn legacy_keymaps_accept_nil_as_an_event_without_making_it_a_valid_key_string() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((map (make-sparse-keymap)))
                 (define-key map [nil] 'nil-event)
                 (list (lookup-key map [nil])
                       (lookup-key (make-sparse-keymap) [nil] t)
                       (key-description [nil])
                       (key-valid-p [nil])))"#,
        ),
        Value::list([
            Value::symbol("nil-event"),
            Value::Nil,
            Value::String("<nil>".into()),
            Value::Nil,
        ])
    );
}

#[test]
fn preloaded_string_replace_preserves_gnu_string_identity_and_properties() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let* ((source (propertize "foo" 'source t))
                      (replacement (propertize "Q" 'replacement t))
                      (unchanged (string-replace "z" replacement source))
                      (changed (string-replace "o" replacement source)))
                 (list (eq unchanged source)
                       (get-text-property 0 'source unchanged)
                       (get-text-property 0 'source changed)
                       (get-text-property 1 'source changed)
                       (get-text-property 1 'replacement changed)
                       (get-text-property 2 'replacement changed)))"#,
        ),
        Value::list([Value::T, Value::T, Value::T, Value::Nil, Value::T, Value::T,])
    );
}

#[test]
fn kmacro_frontier_num_input_keys_counts_prefix_events_and_macro_eof() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((num-input-keys 0))
                 (with-temp-buffer
                   (execute-kbd-macro (kbd "a C-u 2 b"))
                   (list (buffer-string) num-input-keys)))"#
        ),
        Value::list([Value::String("abb".into()), Value::Integer(5)])
    );
}

#[test]
fn keyboard_macro_command_cycle_matches_gnu_prefix_phase_order() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"(let (trace)
                     (defun emaxx-prefix-phase-probe (arg)
                       (interactive "P")
                       (push (list 'body arg prefix-arg
                                   current-prefix-arg last-prefix-arg)
                             trace))
                     (global-set-key "a" #'emaxx-prefix-phase-probe)
                     (add-hook 'pre-command-hook
                               (lambda ()
                                 (when (eq this-command
                                           'emaxx-prefix-phase-probe)
                                   (push (list 'pre prefix-arg
                                               current-prefix-arg
                                               last-prefix-arg)
                                         trace))))
                     (add-hook 'post-command-hook
                               (lambda ()
                                 (when (eq this-command
                                           'emaxx-prefix-phase-probe)
                                   (push (list 'post prefix-arg
                                               current-prefix-arg
                                               last-prefix-arg)
                                         trace))))
                     (execute-kbd-macro (kbd "C-2 a a"))
                     (nreverse trace))"#,
            ),
            Value::list([
                Value::list([
                    Value::symbol("pre"),
                    Value::Integer(2),
                    Value::Nil,
                    Value::Nil,
                ]),
                Value::list([
                    Value::symbol("body"),
                    Value::Integer(2),
                    Value::Nil,
                    Value::Integer(2),
                    Value::Nil,
                ]),
                Value::list([
                    Value::symbol("post"),
                    Value::Nil,
                    Value::Integer(2),
                    Value::Integer(2),
                ]),
                Value::list([
                    Value::symbol("pre"),
                    Value::Nil,
                    Value::Integer(2),
                    Value::Integer(2),
                ]),
                Value::list([
                    Value::symbol("body"),
                    Value::Nil,
                    Value::Nil,
                    Value::Nil,
                    Value::Integer(2),
                ]),
                Value::list([Value::symbol("post"), Value::Nil, Value::Nil, Value::Nil,]),
            ])
        );
    });
}

#[test]
fn keyboard_macro_pre_command_hook_can_preserve_the_previous_prefix() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"(let (calls)
                     (defun emaxx-prefix-preservation-probe (arg)
                       (interactive "P")
                       (push arg calls))
                     (global-set-key "a" #'emaxx-prefix-preservation-probe)
                     (add-hook 'pre-command-hook
                               (lambda ()
                                 (when (and
                                        (eq this-command
                                            'emaxx-prefix-preservation-probe)
                                        (not prefix-arg)
                                        current-prefix-arg)
                                   (setq prefix-arg current-prefix-arg))))
                     (execute-kbd-macro (kbd "C-2 a a"))
                     (nreverse calls))"#,
            ),
            Value::list([Value::Integer(2), Value::Integer(2)])
        );
    });
}

#[test]
fn keyboard_macro_prefix_transient_map_falls_through_to_local_binding() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(save-window-excursion
                 (with-temp-buffer
                   (set-window-buffer (selected-window) (current-buffer))
                   (let ((map (make-sparse-keymap)))
                     (define-key map "b"
                       (lambda (arg)
                         (interactive "P")
                         (insert (format "%S" arg))))
                     (use-local-map map)
                     (execute-kbd-macro (kbd "C-u b"))
                     (buffer-string))))"#
        ),
        Value::String("(4)".into())
    );
}

#[test]
fn native_xdisp_truncation_variable_matches_gnu_value_cell_contract() {
    assert_eq!(
        eval_str(
            r#"(eval
                 '(progn
                    (defalias 'sample-read-truncation-setting
                      (function (lambda () truncate-partial-width-windows)))
                    (list
                     (boundp 'truncate-partial-width-windows)
                     truncate-partial-width-windows
                     (default-boundp 'truncate-partial-width-windows)
                     (default-value 'truncate-partial-width-windows)
                     (local-variable-p 'truncate-partial-width-windows)
                     (let ((truncate-partial-width-windows 7))
                       (sample-read-truncation-setting))
                     truncate-partial-width-windows))
                 t)"#,
        ),
        Value::list([
            Value::T,
            Value::Integer(50),
            Value::T,
            Value::Integer(50),
            Value::Nil,
            Value::Integer(7),
            Value::Integer(50),
        ])
    );
}

#[test]
fn keyboard_macro_decimal_prefix_moves_the_requested_number_of_lines() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(save-window-excursion
                 (with-temp-buffer
                   (set-window-buffer (selected-window) (current-buffer))
                   (dotimes (_ 20) (insert "line\n"))
                   (goto-char (point-min))
                   (execute-kbd-macro (kbd "C-u 10 C-n"))
                   (list (line-number-at-pos) (point)
                         prefix-arg current-prefix-arg last-prefix-arg)))"#,
        ),
        Value::list([
            Value::Integer(11),
            Value::Integer(51),
            Value::Nil,
            Value::Integer(10),
            Value::Integer(10),
        ])
    );
}

#[test]
fn execute_kbd_macro_reports_an_undefined_key_sequence() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"
                (ert-with-message-capture messages
                  (execute-kbd-macro "\C-c\C-z")
                  messages)
                "#
        ),
        Value::String("C-c C-z is undefined\n".into())
    );
}

#[test]
fn recursive_keymap_unset_removes_the_nested_binding() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"(let ((map (make-sparse-keymap)))
                 (keymap-set map "C-c C-c" #'ignore)
                 (keymap-unset map "C-c C-c" t)
                 (with-temp-buffer
                   (use-local-map map)
                   (ert-with-message-capture messages
                     (execute-kbd-macro (kbd "C-c C-c"))
                     (list (keymap-lookup map "C-c C-c") messages))))"#
        ),
        Value::list([Value::Nil, Value::String("C-c C-c is undefined\n".into()),])
    );
}

#[test]
fn execute_kbd_macro_propagates_non_minibuffer_command_errors() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (defun emaxx-kbd-user-error ()
                   (interactive)
                   (user-error "boom"))
                 (global-set-key (kbd "C-c e") #'emaxx-kbd-user-error)
                 ;; A customized reporter does not broaden the command
                 ;; loop's explicit `minibuffer-quit' catch set.
                 (let ((command-error-function #'ignore))
                   (condition-case err
                       (progn
                         (execute-kbd-macro (kbd "C-c e"))
                         'no-error)
                     (user-error err))))"#
        ),
        Value::list([
            Value::Symbol("user-error".into()),
            Value::String("boom".into()),
        ])
    );
}

#[test]
fn message_capture_updates_inside_nested_lexical_callbacks() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"(progn
                 (defun sample-message-capture-caller (callback)
                   (funcall callback))
                 (ert-with-message-capture messages
                   (sample-message-capture-caller
                    (lambda ()
                      (setq messages "")
                      (let ((inhibit-message t))
                        (message "Padding"))
                      messages))))"#
        ),
        Value::String("Padding\n".into())
    );
}

#[test]
fn object_intervals_preserve_each_strings_stored_plist_order() {
    assert_eq!(
        eval_str(
            r#"(and
                 (equal
                  (object-intervals
                   #("abc"
                     0 1 (face default foo 1)
                     1 3 (face (default italic) bar "2")))
                  '((0 1 (face default foo 1))
                    (1 3 (face (default italic) bar "2"))))
                 (equal
                  (object-intervals
                   (propertize "a" 'foo 1 'face 'default))
                  '((0 1 (foo 1 face default)))))"#
        ),
        Value::T
    );
}

#[test]
fn return_key_defaults_to_newline_command() {
    // The global map RET reaches is built by preloaded bindings.el, so this
    // is a property of the dumped image: `emacs -Q -batch' answers `newline'.
    assert_eq!(
        eval_str_with_upstream_batch("(key-binding [?\r])"),
        Value::Symbol("newline".into())
    );
}

#[test]
fn c_toggle_comment_style_switches_between_block_and_line_comments() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(equal
              (with-temp-buffer
               (c-mode)
               (let ((initial (list comment-start comment-end c-block-comment-flag)))
                 (c-toggle-comment-style -1)
                 (let ((line (list comment-start comment-end c-block-comment-flag)))
                   (c-toggle-comment-style 1)
                   (list initial line (list comment-start comment-end c-block-comment-flag)))))
              '((\"/* \" \" */\" t) (\"// \" \"\" nil) (\"/* \" \" */\" t)))"
        ),
        Value::T
    );
}

#[test]
fn c_brace_newlines_reports_c_style_layout() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
              (c-mode)
              (insert \"int main () {\")
              (c-brace-newlines (c-point-syntax)))"
        ),
        Value::list([
            Value::Symbol("before".into()),
            Value::Symbol("after".into()),
        ])
    );
}

#[test]
fn syntax_ppss_flush_cache_is_callable() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(list (fboundp 'syntax-ppss-flush-cache) (syntax-ppss-flush-cache (point-min)))"
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn execute_kbd_macro_exposes_this_single_command_keys() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let (seen)
               (add-hook 'post-command-hook
                         (lambda () (setq seen (this-single-command-keys))))
               (with-temp-buffer
                 (execute-kbd-macro (kbd \"a\"))
                 (equal seen [?a])))"
        ),
        Value::T
    );
}

#[test]
fn call_last_kbd_macro_replays_dynamic_last_kbd_macro() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (let ((last-kbd-macro (kbd \"ab\")))
                 (call-last-kbd-macro))
               (buffer-string))"
        ),
        Value::String("ab".into())
    );
}

#[test]
fn ppss_depth_returns_syntax_ppss_depth() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer (insert \"(a (b))\") (ppss-depth (syntax-ppss 6)))"
        ),
        Value::Integer(2)
    );
}

#[test]
fn syntax_ppss_reports_string_start() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer (c-mode) (insert \"\\\"<>\\\"\") (list (nth 3 (syntax-ppss 3)) (nth 8 (syntax-ppss 3))))"
        ),
        Value::list([Value::Integer('"' as i64), Value::Integer(1)])
    );
}

#[test]
fn syntax_ppss_reports_hash_comment_start() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer (python-mode) (insert \"# <>\\n\") (list (nth 4 (syntax-ppss 3)) (nth 8 (syntax-ppss 3))))"
        ),
        Value::list([Value::T, Value::Integer(1)])
    );
}

#[test]
fn syntax_ppss_reports_open_paren_stack() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer (insert \"(a (b))\") (nth 9 (syntax-ppss 6)))"
        ),
        Value::list([Value::Integer(1), Value::Integer(4)])
    );
}

#[test]
fn scan_sexps_signals_premature_close_with_position() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(condition-case err
                 (with-temp-buffer
                   (c-mode)
                   (insert \"( ()]  \")
                   (scan-sexps 2 (point-max)))
               (scan-error
                (list (if (string-match \"ends prematurely\" (nth 1 err)) t nil)
                      (nth 3 err))))"
        ),
        Value::list([Value::T, Value::Integer(6)])
    );
}

#[test]
fn scan_sexps_signals_mixed_delimiter_premature_end() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(condition-case err
                 (with-temp-buffer
                   (c-mode)
                   (insert \"  (])  \")
                   (scan-sexps 2 (point-max)))
               (scan-error
                (list (if (string-match \"ends prematurely\" (nth 1 err)) t nil)
                      (nth 3 err))))"
        ),
        Value::list([Value::T, Value::Integer(6)])
    );
}

#[test]
fn scan_sexps_treats_lisp_prefix_as_part_of_expression() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert \"`((electric-pair-text-syntax-table \\\\, prog-mode-syntax-table))\")
               (list (scan-sexps 1 1)
                     (scan-sexps 2 1)))"
        ),
        Value::list([Value::Integer(63), Value::Integer(63)])
    );
}

#[test]
fn emacs_lisp_syntax_propertize_limits_at_prefix_to_comma_at() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (emacs-lisp-mode)
               (insert \"(a '@)\")
               (let ((quoted (scan-sexps (+ (point-min) 3) 1)))
                 (erase-buffer)
                 (insert \"(a ,@)\")
                 (list quoted
                       (condition-case nil
                           (progn (scan-sexps (+ (point-min) 3) 1) nil)
                         (scan-error t)))))"
        ),
        Value::list([Value::Integer(6), Value::T])
    );
}

#[test]
fn scan_sexps_uses_current_string_quote_delimiter() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (set-syntax-table (make-syntax-table))
               (modify-syntax-entry 39 \"\\\"\")
               (insert (string 39 ?x 39 32 ?y))
               (scan-sexps 1 1))"
        ),
        Value::Integer(4)
    );
}

#[test]
fn syntax_ppss_ignores_escaped_string_quote_start() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (set-syntax-table (make-syntax-table))
               (modify-syntax-entry 39 \"\\\"\")
               (insert \"\\\\''\")
               (list (nth 3 (syntax-ppss 3))
                     (nth 8 (syntax-ppss 3))
                     (nth 3 (syntax-ppss 4))
                     (nth 8 (syntax-ppss 4))))"
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::Integer('\'' as i64),
            Value::Integer(3),
        ])
    );
}

#[test]
fn syntax_ppss_uses_syntax_properties_when_deciding_whether_a_quote_is_escaped() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (insert \"\\\\\\\"x\\\"\")
               (put-text-property 1 2 'syntax-table (string-to-syntax \".\"))
               (let ((parse-sexp-lookup-properties t))
                 (list (nth 3 (syntax-ppss 3))
                       (nth 3 (syntax-ppss 4))
                       (nth 3 (syntax-ppss 5)))))"
        ),
        Value::list([
            Value::Integer('"' as i64),
            Value::Integer('"' as i64),
            Value::Nil,
        ])
    );
}

#[test]
fn syntax_ppss_reports_the_active_nondefault_comment_style() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (let ((table (make-syntax-table)))
                 (set-syntax-table table)
                 (modify-syntax-entry ?! \"< c\" table)
                 (modify-syntax-entry ?? \"> c\" table)
                 (insert \"!inside?\")
                 (list (nth 4 (syntax-ppss 5))
                       (nth 7 (syntax-ppss 5))
                       (nth 4 (syntax-ppss (point-max)))
                       (nth 7 (syntax-ppss (point-max))))))"
        ),
        Value::list([Value::T, Value::Integer(2), Value::Nil, Value::Nil])
    );
}

#[test]
fn anchored_syntax_class_regexp_honors_buffer_syntax_properties() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert \"{\\n(\")
               (put-text-property 1 2 'syntax-table (string-to-syntax \"|\"))
               (let ((parse-sexp-lookup-properties t))
                 (goto-char (point-min))
                 (list (re-search-forward \"^\\\\s(\" nil t)
                       (match-beginning 0))))"
        ),
        Value::list([Value::Integer(4), Value::Integer(3)])
    );
}

#[test]
fn char_syntax_promotes_raw_bytes_in_unibyte_buffers() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            "(with-temp-buffer
               (set-buffer-multibyte nil)
               (let ((table (make-syntax-table))
                     (compiled (byte-compile (lambda (char) (char-syntax char)))))
                 (modify-syntax-entry (unibyte-char-to-multibyte 128) \"_\" table)
                 (set-syntax-table table)
                 (list (char-syntax 128) (funcall compiled 128))))"
        ),
        Value::list([Value::Integer(95), Value::Integer(95)])
    );
}

#[test]
fn standalone_syntax_class_regexps_use_table_and_text_property_entries() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (let ((table (make-syntax-table)))
                 (set-syntax-table table)
                 (modify-syntax-entry ?x \"|\" table)
                 (insert \"abc\")
                 (put-text-property 2 3 'syntax-table (string-to-syntax \"|\"))
                 (let ((parse-sexp-lookup-properties t))
                   (goto-char (point-min))
                   (list (string-match-p \"\\\\s|\" \"a\")
                         (string-match-p \"\\\\s|\" \"x\")
                         (re-search-forward \"\\\\s|\" nil t)
                         (match-beginning 0)
                         (progn (goto-char (point-max))
                                (re-search-backward \"\\\\s|\" nil t))
                         (match-beginning 0)))))"
        ),
        Value::list([
            Value::Nil,
            Value::Integer(0),
            Value::Integer(3),
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(2),
        ])
    );
}

#[test]
fn parse_partial_sexp_continuation_preserves_a_generic_string_fence() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert \"{x}\")
               (put-text-property 1 2 'syntax-table (string-to-syntax \"|\"))
               (put-text-property 3 4 'syntax-table (string-to-syntax \"|\"))
               (let* ((parse-sexp-lookup-properties t)
                      (state (parse-partial-sexp 1 4 nil nil nil 'syntax-table)))
                 (list (point)
                       (nth 3 state)
                       (let ((finished
                              (parse-partial-sexp (point) 4 nil nil state
                                                  'syntax-table)))
                         (list (point) (nth 3 finished))))))"
        ),
        Value::list([
            Value::Integer(2),
            Value::T,
            Value::list([Value::Integer(4), Value::Nil]),
        ])
    );
}

#[test]
fn syntax_ppss_treats_generic_comment_fences_as_comments() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (insert \"{don't}\")
               (put-text-property 1 2 'syntax-table (string-to-syntax \"!\"))
               (put-text-property 7 8 'syntax-table (string-to-syntax \"!\"))
               (let ((parse-sexp-lookup-properties t))
                 (list (nth 3 (syntax-ppss 5))
                       (nth 4 (syntax-ppss 5))
                       (nth 7 (syntax-ppss 5))
                       (nth 8 (syntax-ppss 5))
                       (nth 4 (syntax-ppss (point-max))))))"
        ),
        Value::list([
            Value::Nil,
            Value::T,
            Value::Symbol("syntax-table".into()),
            Value::Integer(1),
            Value::Nil,
        ])
    );
}

#[test]
fn beginning_of_defun_ignores_a_column_zero_opener_inside_a_string() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (insert \"\\\"text\\n(foo\\\"\\n\")
               (goto-char (point-max))
               (let ((defun-prompt-regexp \"^DEF\")
                     (open-paren-in-column-0-is-defun-start t))
                 (list (beginning-of-defun) (point))))"
        ),
        Value::list([Value::Nil, Value::Integer(1)])
    );
}

#[test]
fn syntax_ppss_drops_mismatched_opener_from_stack() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer (c-mode) (insert \"  (])  \") (nth 9 (syntax-ppss 5)))"
        ),
        Value::Nil
    );
}

#[test]
fn replace_match_preserves_save_excursion_point_after_region_replacement() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert \"ab\")
               (goto-char 3)
               (search-backward \"ab\")
               (goto-char 3)
               (save-excursion
                 (replace-match \"x\"))
               (list (buffer-string) (point)))"
        ),
        Value::list([Value::String("x".into()), Value::Integer(2)])
    );
}

#[test]
fn char_before_and_after_nil_default_to_point() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer (insert \"ab\") (goto-char 2) (list (char-before nil) (char-after nil)))"
        ),
        Value::list([Value::Integer('a' as i64), Value::Integer('b' as i64)])
    );
}

#[test]
fn mark_sexp_activates_region_without_moving_point() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((transient-mark-mode t))
               (with-temp-buffer
                 (insert \"foo\")
                 (goto-char 1)
                 (mark-sexp 1)
                 (list (point) (mark) (use-region-p))))"
        ),
        Value::list([Value::Integer(1), Value::Integer(4), Value::T])
    );
}

#[test]
fn transient_mark_mode_uses_the_gnu_batch_default_and_call_contract() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (insert \"abcd\")
               ;; The public command is Elisp-owned; this bare fixture only
               ;; needs an active region to exercise transient-mark-mode.
               (set-marker (mark-marker) 1 (current-buffer))
               (setq mark-active t)
               (goto-char 4)
               (let ((initial transient-mark-mode))
                 (list initial
                       (use-region-p)
                       (progn (transient-mark-mode) transient-mark-mode)
                       (use-region-p)
                       (progn (transient-mark-mode) transient-mark-mode)
                       (use-region-p)
                       (progn (transient-mark-mode 'toggle) transient-mark-mode)
                       (use-region-p))))"
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn gnu_simple_set_mark_updates_the_persistent_buffer_mark_marker() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (insert \"abcd\")
               (let ((marker (mark-marker)))
                 (set-mark 2)
                 (list (eq marker (mark-marker))
                       (mark)
                       mark-active
                       (marker-position marker)
                       (eq (marker-buffer marker) (current-buffer))
                       (progn
                         (set-mark nil)
                         (list (mark t) mark-active
                               (marker-buffer marker))))))"
        ),
        Value::list([
            Value::T,
            Value::Integer(2),
            Value::T,
            Value::Integer(2),
            Value::T,
            Value::list([Value::Nil, Value::Nil, Value::Nil]),
        ])
    );
}

#[test]
fn killing_a_buffer_retires_its_persistent_mark_mapping() {
    let mut interp = Interpreter::new();
    interp.buffer.set_mark(1);
    let buffer_id = interp.current_buffer_id();
    let Value::Marker(marker_id) = interp.buffer_mark_marker_value() else {
        unreachable!("mark-marker always returns a marker")
    };

    assert_eq!(
        interp.buffer_mark_marker_ids.get(&buffer_id),
        Some(&marker_id)
    );
    assert_eq!(
        interp.find_marker(marker_id).unwrap().mark_buffer_id,
        Some(buffer_id)
    );
    assert!(
        interp
            .markers_by_buffer
            .get(&buffer_id)
            .is_some_and(|marker_ids| marker_ids.contains(&marker_id))
    );

    interp.kill_buffer_id(buffer_id);

    assert!(!interp.buffer_mark_marker_ids.contains_key(&buffer_id));
    assert!(!interp.markers_by_buffer.contains_key(&buffer_id));
    let marker = interp.find_marker(marker_id).unwrap();
    assert_eq!(marker.mark_buffer_id, None);
    assert_eq!(marker.buffer_id, None);
    assert_eq!(marker.position, None);
}

#[test]
fn mark_sexp_stops_before_closing_string_quote() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer (c-mode) (insert \"\\\"foo\\\"\") (goto-char 2) (mark-sexp 1) (list (point) (mark)))"
        ),
        Value::list([Value::Integer(2), Value::Integer(5)])
    );
}

#[test]
fn delete_region_accepts_reversed_bounds() {
    assert_eq!(
        eval_str("(with-temp-buffer (insert \"foo\") (delete-region 2 1) (buffer-string))"),
        Value::String("oo".into())
    );
}

#[test]
fn backward_delete_char_untabify_deletes_before_point() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer (insert \"foo\") (goto-char 3) (backward-delete-char-untabify 1) (buffer-string))"
        ),
        Value::String("fo".into())
    );
}

#[test]
fn define_minor_mode_variable_option_toggles_backing_variable() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
              (define-minor-mode sample-global-mode \"doc\" :global t)
              (define-minor-mode sample-local-mode \"doc\"
                :variable (sample-global-mode . (lambda (value) (setq-local sample-global-mode value)))
                (when sample-global-mode
                  (setq sample-local-body-ran t)))
              (sample-local-mode 1)
              (list sample-global-mode (default-value 'sample-global-mode) sample-local-body-ran))"
        ),
        Value::list([Value::T, Value::Nil, Value::T])
    );
}

#[test]
fn define_minor_mode_variable_setter_controls_the_stored_value() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
              (define-minor-mode sample-setter-mode \"doc\"
                :variable
                (sample-setter-state
                 . (lambda (enabled)
                     (setq sample-setter-state
                           (and enabled 'enabled)))))
              (list (sample-setter-mode 1)
                    sample-setter-state
                    (sample-setter-mode -1)
                    sample-setter-state))"
        ),
        Value::list([
            Value::Symbol("enabled".into()),
            Value::Symbol("enabled".into()),
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn define_global_minor_mode_init_value_runs_body() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
              (setq post-self-insert-hook nil)
              (define-minor-mode electric-indent-mode \"doc\"
                :global t
                :init-value t
                (setq sample-init-mode-body-ran t)
                (add-hook 'post-self-insert-hook
                          #'electric-indent-post-self-insert-function
                          60))
              (list electric-indent-mode
                    (bound-and-true-p sample-init-mode-body-ran)
                    post-self-insert-hook))"
        ),
        // GNU does not run a global minor mode's body at definition even
        // with :init-value t: the mode variable becomes t but the body and
        // its hook registration wait for the first mode call (probed on
        // GNU 30.2 → (t nil nil)).
        Value::list([Value::T, Value::Nil, Value::Nil])
    );
}

#[test]
fn internal_cursor_visibility_round_trips() {
    assert_eq!(
        eval_str(
            "(list (internal-show-cursor-p)
                   (progn
                     (internal-show-cursor nil nil)
                     (internal-show-cursor-p))
                   (progn
                     (internal-show-cursor nil t)
                     (internal-show-cursor-p)))"
        ),
        Value::list([Value::T, Value::Nil, Value::T])
    );
}

#[test]
fn process_query_on_exit_flag_defaults_true_and_round_trips() {
    assert_eq!(
        eval_str(
            "(let ((process (make-pipe-process :name \"query-on-exit\")))
               (unwind-protect
                   (list (process-query-on-exit-flag process)
                         (set-process-query-on-exit-flag process nil)
                         (process-query-on-exit-flag process))
                 (delete-process process)))"
        ),
        Value::list([Value::T, Value::Nil, Value::Nil])
    );
}

#[test]
fn atomic_change_group_evaluates_body() {
    assert_eq!(
        eval_str_with_upstream_batch("(let ((x 1)) (atomic-change-group (setq x 2) (+ x 3)))"),
        Value::Integer(5)
    );
}

#[test]
fn atomic_change_group_rolls_back_on_throw() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (insert \"foo\")
               (catch 'done
                 (atomic-change-group
                   (delete-region 1 2)
                   (throw 'done (buffer-string))))
               (buffer-string))"
        ),
        Value::String("foo".into())
    );
}

#[test]
fn push_supports_nthcdr_setf_places() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((items (list 'head 'body))) (push 'neck (nthcdr 1 items)) items)"
        ),
        Value::list([
            Value::Symbol("head".into()),
            Value::Symbol("neck".into()),
            Value::Symbol("body".into()),
        ])
    );
}

#[test]
fn setf_supports_nested_car_places() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let ((items (list (cons 'old 'tail)))) (setf (car (car items)) 'new) items)"
        ),
        Value::list([Value::cons(
            Value::Symbol("new".into()),
            Value::Symbol("tail".into()),
        )])
    );
}

#[test]
fn format_s_honors_print_circle_for_non_strings() {
    assert_eq!(
        eval_str(
            r##"
                (let* ((print-circle t)
                       (items (make-list 2 'a)))
                  (nconc items items)
                  (string-match-p "#1=" (format "%s" items)))
                "##
        ),
        Value::Integer(0)
    );
}

#[test]
fn format_s_honors_print_gensym_for_non_strings() {
    assert_eq!(
        eval_str(r##"(let ((print-gensym t)) (string-match-p "#:" (format "%s" (gensym "g"))))"##),
        Value::Integer(0)
    );
}

#[test]
fn cl_loop_while_collect_without_for_clause() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let ((i 0)) (cl-loop while (< i 3) collect (setq i (1+ i))))",
        ),
        Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3)])
    );
}

#[test]
fn cl_loop_until_collect_do_runs_body_after_collecting() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let ((items '(a b stop c)))
               (list (cl-loop for form in items
                              until (eq form 'stop)
                              collect form
                              do (pop items))
                     items))"
        ),
        Value::list([
            Value::list([Value::Symbol("a".into()), Value::Symbol("b".into())]),
            Value::list([Value::Symbol("stop".into()), Value::Symbol("c".into())]),
        ])
    );
}

#[test]
fn cl_loop_initially_before_while_for_do_collect() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let ((items '(a b c))
                   (seen nil))
               (cl-loop initially (setq seen 'start)
                        while items
                        for name = (car items)
                        do (pop items)
                        collect (cons seen name)))"
        ),
        Value::list([
            Value::cons(Value::Symbol("start".into()), Value::Symbol("a".into())),
            Value::cons(Value::Symbol("start".into()), Value::Symbol("b".into())),
            Value::cons(Value::Symbol("start".into()), Value::Symbol("c".into())),
        ])
    );
}

#[test]
fn cl_loop_vconcat_into_append_into_finally_return() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(equal
               (cl-loop for segment in (list [a b] [c])
                        for thunks in '((x) (y z))
                        vconcat segment into segments
                        append thunks into thunk-list
                        finally return (list segments thunk-list))
               '([a b c] (x y z)))"
        ),
        Value::T
    );
}

#[test]
fn cl_loop_collect_into_finally_return() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop for item in '(a b c)
                      collect (symbol-name item) into names
                      finally return (apply #'vector names))",
        ),
        Value::list([
            Value::symbol("vector-literal"),
            Value::String("a".into()),
            Value::String("b".into()),
            Value::String("c".into()),
        ])
    );
}

#[test]
fn cl_loop_vconcat_accumulates_vector_elements() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop for x in (list 1 2 3 4 5) vconcat (vector (1+ x)))",
        ),
        Value::list([
            Value::symbol("vector-literal"),
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(4),
            Value::Integer(5),
            Value::Integer(6),
        ])
    );
}

#[test]
fn cl_loop_when_binds_it_to_condition_value() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(list
               (cl-loop for i in '(1 2 3 4 5 6)
                        when (and (> i 3) i)
                          collect it)
               (cl-loop for i in '(1 2 3 4 5 6)
                        when (and (> i 3) i)
                          return it))"
        ),
        Value::list([
            Value::list([Value::Integer(4), Value::Integer(5), Value::Integer(6)]),
            Value::Integer(4),
        ])
    );
}

#[test]
fn cl_loop_when_nested_collects_into_else_targets() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            r#"(cl-loop for elt in '(1 a 2 "a" (3 4) 5 6)
                      when (numberp elt)
                        when (cl-evenp elt) collect elt into even
                        else collect elt into odd
                      else
                        when (symbolp elt) collect elt into syms
                        else collect elt into other
                      finally return (list even odd syms other))"#
        ),
        Value::list([
            Value::list([Value::Integer(2), Value::Integer(6)]),
            Value::list([Value::Integer(1), Value::Integer(5)]),
            Value::list([Value::Symbol("a".into())]),
            Value::list([
                Value::String("a".into()),
                Value::list([Value::Integer(3), Value::Integer(4)]),
            ]),
        ])
    );
}

#[test]
fn cl_loop_sequential_when_collect_into_and_do_clauses() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let (seen)
               (cl-loop with found
                        for item in '(start mid stop tail)
                        when found
                          collect item into rest
                        when (eq item 'stop)
                          do (push (cons 'rest rest) seen)
                             (cl-return seen)
                        when (eq item 'start)
                          do (setq found t)))"
        ),
        Value::list([Value::cons(
            Value::symbol("rest"),
            Value::list([Value::symbol("mid"), Value::symbol("stop")]),
        )])
    );
}

#[test]
fn cl_loop_while_supports_equals_then_assignment() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let ((stack '(a b c d e f)))
                   (cl-loop while stack
                            for item = (length stack) then (pop stack)
                            collect item))"
        ),
        Value::list([
            Value::Integer(6),
            Value::Symbol("a".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
            Value::Symbol("d".into()),
            Value::Symbol("e".into()),
            Value::Symbol("f".into()),
        ])
    );
}

#[test]
fn cl_loop_with_sequential_bindings_see_prior_values() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop with a = 1
                      with b = (+ a 2)
                      with c = (+ b 3)
                      return (list a b c))"
        ),
        Value::list([Value::Integer(1), Value::Integer(3), Value::Integer(6)])
    );
}

#[test]
fn cl_loop_with_and_bindings_initialize_in_parallel() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let ((a 5)
                   (b 10))
               (cl-loop with a = 1
                        and b = (+ a 2)
                        and c = (+ b 3)
                        return (list a b c)))"
        ),
        Value::list([Value::Integer(1), Value::Integer(7), Value::Integer(13)])
    );
}

#[test]
fn cl_loop_with_defaults_to_nil_and_splits_do_finally() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(list
               (cl-loop for i below 3
                        with loop-with
                        do (push (* i i) loop-with)
                        finally (cl-return loop-with))
               (boundp 'loop-with))"
        ),
        Value::list([
            Value::list([Value::Integer(4), Value::Integer(1), Value::Integer(0)]),
            Value::Nil,
        ])
    );
}

#[test]
fn cl_loop_when_do_supports_finally_return() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let (seen)
               (cl-loop for item in '(a nil b)
                        when item
                        do (push item seen)
                        finally return seen))"
        ),
        Value::list([Value::symbol("b"), Value::symbol("a")])
    );
}

#[test]
fn cl_loop_if_collect_else_collect() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop for item in '(1 a 2 b)
                      if (numberp item)
                        collect item
                      else
                        collect (list item))"
        ),
        Value::list([
            Value::Integer(1),
            Value::list([Value::symbol("a")]),
            Value::Integer(2),
            Value::list([Value::symbol("b")]),
        ])
    );
}

#[test]
fn cl_loop_if_collect_into_else_collect_into_finally_return() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop for item in '(1 nil 2 nil)
                      if item
                        collect item into present
                      else
                        collect item into absent
                      finally return (list present absent))"
        ),
        Value::list([
            Value::list([Value::Integer(1), Value::Integer(2)]),
            Value::list([Value::Nil, Value::Nil]),
        ])
    );
}

#[test]
fn cl_loop_initially_can_return_before_main_action() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop for item in '(a b)
                      initially (when t (cl-return 'empty))
                      if item
                        do (error \"should not iterate\")
                      else
                        do (error \"should not iterate\")
                      finally return 'done)"
        ),
        Value::symbol("empty")
    );
}

#[test]
fn cl_loop_unless_do_supports_repeated_do_and_finally_progn() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let (seen)
               (cl-loop for item in '(a b)
                        unless (eq item 'a)
                        do (push item seen)
                        do (push 'visited seen)
                        finally
                        (push 'done seen)
                        (cl-return seen)))"
        ),
        Value::list([
            Value::symbol("done"),
            Value::symbol("visited"),
            Value::symbol("b"),
            Value::symbol("visited"),
        ])
    );
}

#[test]
fn cl_loop_unless_count_else_count_finally() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop for item in '(nil a nil b)
                      unless item
                        count t into empty
                      else
                        count t into present
                      finally
                        (cl-return (list empty present)))"
        ),
        Value::list([Value::Integer(2), Value::Integer(2)])
    );
}

#[test]
fn cl_loop_for_in_sees_with_bindings() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop with items = '(a b c)
                      for item in items
                      collect item)"
        ),
        Value::list([Value::symbol("a"), Value::symbol("b"), Value::symbol("c")])
    );
}

#[test]
fn cl_loop_unless_do_supports_finally_return() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let (seen)
               (cl-loop for item in '(a nil b)
                        unless item
                        do (push 'empty seen)
                        finally return (cons 'done seen)))"
        ),
        Value::list([Value::symbol("done"), Value::symbol("empty")])
    );
}

#[test]
fn cl_loop_do_supports_finally_return_keyword() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let (seen)
               (cl-loop for item in '(a b)
                        do (push item seen)
                        finally return seen))"
        ),
        Value::list([Value::symbol("b"), Value::symbol("a")])
    );
}

#[test]
fn cl_loop_if_do_else_do_supports_finally_return() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(let (seen)
               (cl-loop for item in '(a 1 b 2)
                        if (symbolp item)
                        do (push item seen)
                        else
                        do (push 'num seen)
                        finally return seen))"
        ),
        Value::list([
            Value::symbol("num"),
            Value::symbol("b"),
            Value::symbol("num"),
            Value::symbol("a"),
        ])
    );
}

#[test]
fn cl_loop_named_catches_return_from_do_body() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop named main
                      for item in '(a b c)
                      when (eq item 'b)
                      do (cl-return-from main 'hit)
                      finally return 'miss)"
        ),
        Value::Symbol("hit".into())
    );
}

#[test]
fn cl_loop_if_do_is_followed_by_an_unconditional_append_clause() {
    // GNU `cl-loop' parses the `append' as a separate main clause, not as
    // part of the preceding `if'.  It therefore also appends the two nils
    // from the false iteration.
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(cl-loop for item in '(1 nil 3)
                      if item
                      do (setq item (1+ item))
                      append (list item item))",
        ),
        Value::list([
            Value::Integer(2),
            Value::Integer(2),
            Value::Nil,
            Value::Nil,
            Value::Integer(4),
            Value::Integer(4),
        ])
    );
}

#[test]
fn defgroup_tracks_current_group_and_members() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    interp.set_current_load_file(Some("/tmp/custom-group.el".into()));
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (defgroup demo nil \"Doc.\" :prefix \"demo-\")
                   (custom-add-to-group 'demo 'demo-option 'custom-variable)
                   (list (custom-current-group)
                         (equal (get 'demo 'custom-prefix) \"demo-\")
                         (get 'demo 'custom-group)))"
        ),
        Value::list([
            Value::symbol("demo"),
            Value::T,
            Value::list([Value::list([
                Value::symbol("demo-option"),
                Value::symbol("custom-variable"),
            ])]),
        ])
    );
}

#[test]
fn defcustom_records_version_and_group_membership() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (defgroup sample-custom-parent nil \"Doc.\")
               (defcustom sample-custom-versioned nil \"Doc.\"
                 :type 'boolean
                 :version \"31.1\"
                 :group 'sample-custom-parent)
               (list (equal (get 'sample-custom-versioned 'custom-version) \"31.1\")
                     (get 'sample-custom-parent 'custom-group)
                     ;; custom-versions-load-alist is void in emacs -Q batch
                     ;; (probed on both binaries); the old bare reference
                     ;; pinned the retired fallback's invented nil.
                     (boundp 'custom-versions-load-alist)))"
        ),
        Value::list([
            Value::T,
            Value::list([Value::list([
                Value::symbol("sample-custom-versioned"),
                Value::symbol("custom-variable"),
            ])]),
            Value::Nil,
        ])
    );
}

#[test]
fn custom_declare_variable_keeps_compiled_custom_policy_in_elisp() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (custom-declare-group
                 'sample-compiled-group nil \"Group.\" :prefix \"sample-\")
               (custom-declare-variable
                 'sample-compiled-option '(+ 1 2) \"Doc.\"
                 :type 'integer :group 'sample-compiled-group
                 :safe 'integerp :risky t)
               (list sample-compiled-option
                     (get 'sample-compiled-option 'custom-type)
                     (get 'sample-compiled-option 'safe-local-variable)
                     (get 'sample-compiled-option 'risky-local-variable)
                     (special-variable-p 'sample-compiled-option)
                     (get 'sample-compiled-group 'custom-prefix)
                     (get 'sample-compiled-group 'custom-group)))",
        ),
        Value::list([
            Value::Integer(3),
            Value::symbol("integer"),
            Value::symbol("integerp"),
            Value::T,
            Value::T,
            Value::String("sample-".into()),
            Value::list([Value::list([
                Value::symbol("sample-compiled-option"),
                Value::symbol("custom-variable"),
            ])]),
        ])
    );
}

#[test]
fn compiled_subr_and_generic_entry_points_keep_elisp_owners() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (setq sample-compiled-list '(a))
               (funcall #'add-to-list 'sample-compiled-list 'b)
               (setq sample-combined-result
                     (with-temp-buffer
                       (buffer-enable-undo)
                       (insert \"ab\")
                       (funcall #'combine-change-calls-1 1 3
                                (lambda ()
                                  (delete-region 1 3)
                                  (insert \"xy\")))
                       (buffer-string)))
               (cl-generic-define 'sample-runtime-generic '(x) nil)
               (cl-defmethod sample-runtime-generic ((x integer)) (1+ x))
               (list sample-compiled-list
                     sample-combined-result
                     (sample-runtime-generic 4)))",
        ),
        Value::list([
            Value::list([Value::symbol("b"), Value::symbol("a")]),
            Value::String("xy".into()),
            Value::Integer(5),
        ])
    );
}

#[test]
fn compiled_generic_method_entry_point_preserves_arguments_and_next_method() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (cl-generic-define 'sample-compiled-method '(x &optional y) nil)
               (cl-generic-define-method
                 'sample-compiled-method nil '((x t) &optional y) nil
                 (lambda (x &optional y) (list 'base x y)))
               (cl-generic-define-method
                 'sample-compiled-method nil '((x integer) &optional y) 'curried
                 (lambda (next)
                   (lambda (x &optional y)
                     (list 'integer x y (funcall next x y)))))
               (list (sample-compiled-method 'symbol)
                     (sample-compiled-method 'symbol 8)
                     (sample-compiled-method 3 9)))",
        ),
        Value::list([
            Value::list([Value::symbol("base"), Value::symbol("symbol"), Value::Nil,]),
            Value::list([
                Value::symbol("base"),
                Value::symbol("symbol"),
                Value::Integer(8),
            ]),
            Value::list([
                Value::symbol("integer"),
                Value::Integer(3),
                Value::Integer(9),
                Value::list([Value::symbol("base"), Value::Integer(3), Value::Integer(9),]),
            ]),
        ])
    );
}

#[test]
fn real_gnu_cl_generic_exposes_complete_method_introspection() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (cl-defgeneric emaxx-introspection-generic (x)
                 \"generic-doc\")
               (cl-defmethod emaxx-introspection-generic (x)
                 \"default-doc\" x)
               (cl-defmethod emaxx-introspection-generic ((x integer))
                 \"integer-doc\" x)
               (let ((generic (cl--generic
                               'emaxx-introspection-generic)))
                 (list (cl--generic-name generic)
                       (mapcar
                        (lambda (method)
                          (list (cl--generic-method-qualifiers method)
                                (cl--generic-method-specializers method)
                                (cl--generic-method-info method)))
                        (cl--generic-method-table generic))
                       (cl--generic-load-hist-format
                        'emaxx-introspection-generic nil '(integer)))))",
        ),
        Value::list([
            Value::Symbol("emaxx-introspection-generic".into()),
            Value::list([
                Value::list([
                    Value::Nil,
                    Value::list([Value::Symbol("integer".into())]),
                    Value::list([
                        Value::String("".into()),
                        Value::list([Value::list([
                            Value::Symbol("x".into()),
                            Value::Symbol("integer".into()),
                        ])]),
                        Value::String("integer-doc".into()),
                    ]),
                ]),
                Value::list([
                    Value::Nil,
                    Value::list([Value::T]),
                    Value::list([
                        Value::String("".into()),
                        Value::list([Value::Symbol("x".into())]),
                        Value::String("default-doc".into()),
                    ]),
                ]),
            ]),
            Value::list([
                Value::Symbol("emaxx-introspection-generic".into()),
                Value::Nil,
                Value::Symbol("integer".into()),
            ]),
        ])
    );
}

#[test]
fn implicit_native_generic_keeps_gnu_documentation_sentinel() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (cl-defmethod emaxx-implicit-introspection-generic (x)
                 \"default-doc\" x)
               (documentation 'emaxx-implicit-introspection-generic t))",
        ),
        // GNU returns the sole method's docstring here (probed on 30.2).
        Value::String("default-doc".into())
    );
}

#[test]
fn char_access_accepts_out_of_range_negative_integer_positions() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert \"x\")
               (list (char-after -1) (char-before -1)
                     (char-after 1) (char-before 2)))"
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::Integer('x' as i64),
            Value::Integer('x' as i64),
        ])
    );
}

#[test]
fn mapatoms_scans_standard_obarray_symbols() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (progn
                  (defcustom sample-mapatoms-option nil "Doc." :type 'boolean)
                  (intern "sample-mapatoms-interned")
                  (let (found)
                    (mapatoms
                     (lambda (symbol)
                       (when (string-prefix-p "sample-mapatoms-" (symbol-name symbol))
                         (push symbol found))))
                    (sort found (lambda (left right)
                                  (string< (symbol-name left)
                                           (symbol-name right))))))"#
        ),
        Value::list([
            Value::symbol("sample-mapatoms-interned"),
            Value::symbol("sample-mapatoms-option"),
        ])
    );
}

#[test]
fn mapatoms_excludes_symbols_in_private_obarrays() {
    assert_eq!(
        eval_str(
            "(let ((private (obarray-make)) seen)
               (put (intern \"private-propertied-symbol\" private) 'sample-property t)
               (mapatoms
                (lambda (symbol)
                  (when (string= (symbol-name symbol) \"private-propertied-symbol\")
                    (push symbol seen))))
               seen)"
        ),
        Value::Nil
    );
}

#[test]
fn character_property_alias_applies_to_string_lookup_and_changes() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(with-temp-buffer
               (setq-local char-property-alias-alist '((face font-lock-face)))
               (let ((text (copy-sequence \"abc\")))
                 (put-text-property 1 3 'font-lock-face 'bold text)
                 (list char-property-alias-alist
                       (get-text-property 1 'face text)
                       (next-single-property-change 0 'face text)
                       (next-single-property-change 1 'face text 3))))"
        ),
        Value::list([
            Value::list([Value::list([
                Value::Symbol("face".into()),
                Value::Symbol("font-lock-face".into()),
            ])]),
            Value::Symbol("bold".into()),
            Value::Integer(1),
            Value::Integer(3),
        ])
    );
}

#[test]
fn booleanp_matches_nil_and_t_only() {
    assert_eq!(
        eval_str("(list (booleanp nil) (booleanp t) (booleanp 0) (booleanp 'false))"),
        Value::list([Value::T, Value::T, Value::Nil, Value::Nil])
    );
}

#[test]
fn make_obsolete_variable_records_byte_obsolete_property() {
    assert_eq!(
        eval_str(
            "(progn
               (make-obsolete-variable 'sample-old-option 'sample-new-option \"31.1\" 'get)
               (get 'sample-old-option 'byte-obsolete-variable))"
        ),
        Value::list([
            Value::symbol("sample-new-option"),
            Value::symbol("get"),
            Value::String("31.1".into()),
        ])
    );
}

#[test]
fn make_obsolete_rejects_nil_and_t_names() {
    assert_eq!(
        eval_str(
            "(list
               (condition-case err (make-obsolete nil 'sample-new \"31.1\") (error (car err)))
               (condition-case err (make-obsolete t 'sample-new \"31.1\") (error (car err))))"
        ),
        // GNU byte-run.el signals a plain `error' ("Can't make 'nil'
        // obsolete") for these, probed on GNU 30.2.
        Value::list([Value::symbol("error"), Value::symbol("error")])
    );
}

#[test]
fn make_obsolete_variable_rejects_nil_and_t_names() {
    assert_eq!(
        eval_str(
            "(list
               (condition-case err (make-obsolete-variable nil 'sample-new \"31.1\") (error (car err)))
               (condition-case err (make-obsolete-variable t 'sample-new \"31.1\") (error (car err))))"
        ),
        // GNU byte-run.el signals a plain `error' ("Can't make 'nil'
        // obsolete") for these, probed on GNU 30.2.
        Value::list([Value::symbol("error"), Value::symbol("error")])
    );
}

#[test]
fn batch_window_hscroll_defaults_to_zero() {
    assert_eq!(eval_str("(window-hscroll)"), Value::Integer(0));
}

#[test]
fn dolist_with_progress_reporter_uses_dolist_semantics() {
    assert_eq!(
        eval_str(
            "(condition-case err
                 (let ((seen nil) (reporter nil))
                   (dolist-with-progress-reporter (item '(1 2 3) (nreverse seen))
                       (setq reporter 'evaluated)
                     (push (list reporter item) seen)))
               (wrong-type-argument (list (car err) (cadr err) (caddr err))))"
        ),
        // GNU evaluates the reporter argument and then rejects it as a
        // spec list: (wrong-type-argument listp evaluated), probed on
        // GNU 30.2.
        Value::list([
            Value::symbol("wrong-type-argument"),
            Value::symbol("listp"),
            Value::symbol("evaluated"),
        ])
    );
}

#[test]
fn dolist_reuses_own_binding_frame_after_empty_nested_frames() {
    assert_eq!(
        eval_str(
            "(let ((seen nil))
               (dolist (item '(a b) (nreverse seen))
                 (let ()
                   (push item seen))))"
        ),
        Value::list([Value::symbol("a"), Value::symbol("b")])
    );
}

#[test]
fn dolist_binds_original_list_element_for_delq_identity() {
    assert_eq!(
        eval_str(
            r#"(let ((items '("file:///tmp/example")))
                 (dolist (item items items)
                   (setq items (delq item items))))"#
        ),
        Value::Nil
    );
}

#[test]
fn source_string_literals_keep_identity_through_direct_and_quoted_evaluation() {
    assert_eq!(
        eval_str(
            r#"(let ((direct (lambda () "x"))
                     (quoted (lambda () '("x"))))
                 (list (eq (funcall direct) (funcall direct))
                       (eq (car (funcall quoted)) (car (funcall quoted)))
                       (let ((items '("x")))
                         (eq (car items) (car items)))))"#
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn delq_destructively_removes_non_leading_list_cells() {
    assert_eq!(
        eval_str(
            "(let* ((target (list 'remove))
                    (items (list 'keep target 'tail)))
               (delq target items)
               items)"
        ),
        Value::list([Value::Symbol("keep".into()), Value::Symbol("tail".into())])
    );
}

#[test]
fn delq_and_delete_share_gnu_cycle_and_dotted_list_contracts() {
    assert_eq!(
        eval_str(
            "(let ((items (list 'keep 'remove 'tail)))
               (list (eq (delq 'remove items) items)
                     (condition-case err
                         (let ((cycle (list 'remove)))
                           (setcdr cycle cycle)
                           (delq 'remove cycle))
                       (circular-list (car err)))
                     (condition-case err
                         (delete 'missing '(keep . tail))
                       (wrong-type-argument (car err)))))"
        ),
        Value::list([
            Value::T,
            Value::Symbol("circular-list".into()),
            Value::Symbol("wrong-type-argument".into()),
        ])
    );
}

#[test]
fn dnd_multiple_url_handlers_prefer_earlier_equal_precedence_handler() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    interp.load_target("dnd").expect("load dnd");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
            (let ((dnd-protocol-alist
                   '(("^file:///" . dnd-test-local)
                     ("^file:" . error)
                     ("^unrelated-scheme:" . error)))
                  (urls '("file:///usr/openwin/include/pixrect/pr_impl.h"
                          "file:///usr/openwin/include/pixrect/pr_io.h")))
              (put 'dnd-test-local 'dnd-multiple-handler t)
              (defun dnd-test-local (received _action)
                (unless (equal received urls)
                  (error "wrong urls"))
                'copy)
              (dnd-handle-multiple-urls (selected-window) (copy-sequence urls) 'copy))
            "#,
        ),
        Value::Symbol("copy".into())
    );
}

#[test]
fn sort_preserves_order_for_equal_elements() {
    assert_eq!(
        eval_str(
            r#"(mapcar #'car
                      (sort '((first a b) (second c d))
                            (lambda (left right)
                              (> (length (cdr left))
                                 (length (cdr right))))))"#
        ),
        Value::list([
            Value::Symbol("first".into()),
            Value::Symbol("second".into())
        ])
    );
}

#[test]
fn cl_letf_rebinds_symbol_property_get_places() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(progn
               (put 'sample-cl-letf-prop 'tag 'outer)
               (list
                (cl-letf (((get 'sample-cl-letf-prop 'tag) 'inner))
                  (list (get 'sample-cl-letf-prop 'tag)
                        (progn (put 'sample-cl-letf-prop 'tag 'changed)
                               (get 'sample-cl-letf-prop 'tag))))
                (get 'sample-cl-letf-prop 'tag)))"
        ),
        Value::list([
            Value::list([Value::symbol("inner"), Value::symbol("changed")]),
            Value::symbol("outer"),
        ])
    );
}

#[test]
fn setopt_warns_when_value_does_not_match_custom_type() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (progn
                  (defcustom sample-setopt-number 0 "Doc." :type 'number)
                  (with-current-buffer (get-buffer-create "*Warnings*")
                    (let ((inhibit-read-only t))
                      (erase-buffer))
                    (setopt sample-setopt-number :bad)
                    (string-search "does not match type number"
                                   (buffer-string))))"#
        ),
        // GNU's warning uses curly quotes; the quote-free substring sits at
        // offset 30 of the *Warnings* buffer (probed on GNU 30.2).
        Value::Integer(30)
    );
}

#[test]
fn cl_with_gensyms_produces_unique_bindings() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            r#"
                (progn
                  (defmacro sample-cl-with-gensyms (value)
                    (cl-with-gensyms (tmp)
                      `(let ((,tmp ,value))
                         ,tmp)))
                  (let ((tmp 99))
                    (sample-cl-with-gensyms 42)))
                "#
        ),
        Value::Integer(42)
    );
}

#[test]
fn cl_case_matches_atoms_lists_and_fallbacks() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            r#"
                (list
                 (cl-case 'zip
                   ((tar ar) 'other)
                   (zip 'zip))
                 (cl-case 2
                   ((1 3) 'miss)
                   ((2 4) 'hit))
                 (cl-case nil
                   (nil 'impossible)
                   (otherwise 'fallback)))
                "#
        ),
        Value::list([
            Value::symbol("zip"),
            Value::symbol("hit"),
            Value::symbol("fallback"),
        ])
    );
}

#[test]
fn cl_case_evaluates_expression_once() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            r#"
                (let ((count 0))
                  (list
                   (cl-case (progn (setq count (+ count 1)) 'zip)
                     (zip 'matched)
                     (otherwise 'missed))
                   count))
                "#
        ),
        Value::list([Value::symbol("matched"), Value::Integer(1)])
    );
}

#[test]
fn dired_shell_command_confirmation_positions_match_upstream() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (setq noninteractive t)
                 (require 'dired-aux)
                 (list (dired--need-confirm-positions "ls ? ./?" "?")
                       (dired--need-confirm-positions "ls ./? ?" "?")
                       (dired--need-confirm-positions "ls * ./*" "*")
                       (dired--need-confirm-positions "ls * *" "*")
                       (dired--need-confirm-positions "ls ? ?" "?")
                       (dired--need-confirm-positions "ls ? ./`?`" "?")))"#
        ),
        Value::list([
            Value::list([Value::Integer(7)]),
            Value::list([Value::Integer(5)]),
            Value::list([Value::Integer(7)]),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn dired_shell_command_confirms_unsafe_substitution_marks() {
    let dir = std::env::temp_dir().join(format!("emaxx-dired-shell-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let file = dir.join("foo");
    std::fs::write(&file, "contents").expect("write temp file");
    let dir_text = format!("{}/", dir.to_string_lossy()).replace('\\', "\\\\");
    let file_text = file.to_string_lossy().replace('\\', "\\\\");
    let expr = format!(
        r#"(progn
             (setq noninteractive t)
             (require 'ert-x)
             (require 'dired-aux)
             (let ((files (list "{file_text}"))
                   (temporary-file-directory "{dir_text}"))
               (cl-letf (((symbol-function 'read-char-from-minibuffer) 'error)
                         ((symbol-function 'dired-run-shell-command)
                          (lambda (_command) nil)))
                 (dired temporary-file-directory)
                 (dired-goto-file "{file_text}")
                 (list
                  (condition-case err (dired-do-shell-command "ls ? ./?" nil files)
                    (error (car err)))
                  (condition-case err (dired-do-shell-command "ls ./? ?" nil files)
                    (error (car err)))
                  (condition-case err (dired-do-shell-command "ls ? ?" nil files)
                    (error (car err)))
                  (condition-case err (dired-do-shell-command "ls * ./*" nil files)
                    (error (car err)))
                  (condition-case err (dired-do-shell-command "ls * *" nil files)
                    (error (car err)))
                  (condition-case err (dired-do-shell-command "ls ? ./`?`" nil files)
                    (error (car err)))))))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&expr),
        Value::list([
            Value::Symbol("error".into()),
            Value::Symbol("error".into()),
            Value::Nil,
            Value::Symbol("error".into()),
            Value::Nil,
            Value::Nil,
        ])
    );
    std::fs::remove_dir_all(&dir).expect("cleanup temp dir");
}

#[test]
fn dired_create_destination_dirs_controls_copy_and_rename() {
    let dir = std::env::temp_dir().join(format!("emaxx-dired-dest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let dir_text = dir.to_string_lossy().replace('\\', "\\\\");
    let expr = format!(
        r#"(progn
             (setq noninteractive t)
             (require 'dired-aux)
             (let (results)
               (dolist (mode '(always nil ask-yes ask-no))
                 (let* ((scenario-dir (expand-file-name
                                       (symbol-name mode)
                                       "{dir_text}"))
                        (from (make-temp-file "emaxx-dired-from"))
                        (dired-create-destination-dirs
                         (cond ((eq mode 'always) 'always)
                               ((eq mode 'nil) nil)
                               (t 'ask)))
                        (to-cp (expand-file-name
                                "foo-cp"
                                (file-name-as-directory
                                 (expand-file-name
                                  "bar" scenario-dir))))
                        (to-mv (expand-file-name
                                "foo-mv"
                                (file-name-as-directory
                                 (expand-file-name
                                  "qux" scenario-dir)))))
                   (cl-letf (((symbol-function 'yes-or-no-p)
                              (lambda (_prompt) (eq mode 'ask-yes))))
                     (push
                      (list mode
                            (condition-case nil
                                (progn
                                  (dired-copy-file-recursive from to-cp nil)
                                  (file-exists-p to-cp))
                              (error :error))
                            (condition-case nil
                                (progn
                                  (dired-rename-file from to-mv nil)
                                  (file-exists-p to-mv))
                              (error :error)))
                      results))))
               (nreverse results)))"#
    );
    assert_eq!(
        eval_str_with_upstream_batch(&expr),
        Value::list([
            Value::list([Value::symbol("always"), Value::T, Value::T]),
            Value::list([Value::Nil, Value::symbol(":error"), Value::symbol(":error"),]),
            Value::list([Value::symbol("ask-yes"), Value::T, Value::T]),
            Value::list([
                Value::symbol("ask-no"),
                Value::symbol(":error"),
                Value::symbol(":error"),
            ]),
        ])
    );
    std::fs::remove_dir_all(&dir).expect("cleanup temp dir");
}

#[test]
fn dired_do_create_files_recreates_destination_directory() {
    let dir = std::env::temp_dir().join(format!("emaxx-dired-create-files-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let dir_text = dir.to_string_lossy().replace('\\', "\\\\");
    let expr = format!(
        r#"(let ((temporary-file-directory (file-name-as-directory "{dir_text}")))
             (progn
                 (setq noninteractive t)
                 (require 'dired-aux)
                 (let* ((target-dir (make-temp-file "emaxx-dired-target" 'dir))
                        (file1 (make-temp-file "bug30624_file1"))
                        (file2 (make-temp-file "bug30624_file2"))
                        (dired-create-destination-dirs 'always)
                        (inhibit-message t)
                        (buf (dired temporary-file-directory)))
                   (unwind-protect
                       (progn
                         (delete-directory target-dir)
                         (cl-letf (((symbol-function 'dired-mark-read-file-name)
                                    (lambda (&rest _) target-dir)))
                           (dired-revert)
                           (dired-mark-files-regexp "bug30624_file")
                           (condition-case err
                               (dired-do-create-files 'copy 'dired-copy-file "Copy" nil)
                             (error err))))
                     (ignore-errors (delete-directory target-dir 'recursive))
                     (ignore-errors (delete-file file1))
                     (ignore-errors (delete-file file2))
                     (ignore-errors (kill-buffer buf))))))"#,
    );
    let result = eval_str_with_upstream_batch(&expr);
    assert!(result.is_truthy(), "{result:?}");
    std::fs::remove_dir_all(&dir).expect("cleanup temp dir");
}

#[test]
fn dired_highlights_unsubstituted_shell_metacharacters() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (setq noninteractive t)
                 (require 'dired-aux)
                 (let* ((command "sed -r -e 's/oo?/a/' -e 's/oo?/a/' ? `?`")
                        (result (dired--highlight-no-subst-chars
                                 (dired--need-confirm-positions command "?")
                                 command
                                 t))
                        (lines (split-string result "\n"))
                        (line (car lines)))
                   (list (dired--need-confirm-positions command "?")
                         (= (length lines) 2)
                         (string-match-p
                          (regexp-quote "               ^             ^")
                          (cadr lines))
                         (get-text-property 15 'face line)
                         (get-text-property 29 'face line)
                         (text-property-not-all 1 14 'face nil line)
                         (text-property-not-all 16 28 'face nil line)
                         (text-property-not-all 30 (length line) 'face nil line))))"#
        ),
        Value::list([
            Value::list([Value::Integer(29), Value::Integer(15)]),
            Value::T,
            Value::Integer(0),
            Value::symbol("warning"),
            Value::symbol("warning"),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn dired_reuses_directory_buffer_and_preserves_file_point() {
    let result = eval_str_with_upstream_batch(
        r#"(progn
             (setq noninteractive t)
             (require 'dired)
             (require 'ert-x)
             (ert-with-temp-directory test-dir :suffix "-emaxx"
               (let ((dired-auto-revert-buffer t)
                     buffers
                     step)
                 (condition-case err
                     (progn
                       (setq step 'initial)
                       (should-not (dired-buffers-for-dir test-dir))
                       (setq step 'find-directory)
                       (with-current-buffer (find-file-noselect test-dir)
                         (make-directory "test-subdir"))
                       (setq step 'check-eob)
                       (with-current-buffer (car (dired-buffers-for-dir test-dir))
                         (unless (eobp) (error "not at eob")))
                       (setq step 'dired)
                       (push (dired test-dir) buffers)
                       (setq step 'buffer-count)
                       (unless (eq 1 (length (dired-buffers-for-dir test-dir)))
                         (error "extra dired buffer"))
                       (let ((buf (current-buffer))
                             (pt1 (point))
                             (test-file (concat (file-name-as-directory "test-subdir")
                                                "test-file")))
                         (setq step 'write-region)
                         (write-region "Test" nil test-file nil 'silent nil 'excl)
                         (setq step 'file-at-point)
                         (let ((actual (dired-file-name-at-point))
                               (expected (concat test-dir
                                                 (file-name-as-directory "test-subdir"))))
                           (unless (equal actual expected)
                             (error "not on subdir: %S expected %S" actual expected)))
                         (setq step 'find-file)
                         (push (dired-find-file) buffers)
                         (let ((pt2 (point)))
                           (setq step 'pop-back)
                           (pop-to-buffer-same-window buf)
                           (unless (eq (point) pt1)
                             (error "lost directory point"))
                           (setq step 'find-again)
                           (push (dired-find-file) buffers)
                           (list :ok (eq (point) pt2)))))
                   (error (list step err))))))"#,
    );
    assert_eq!(result, Value::list([Value::symbol(":ok"), Value::T]));
}

#[test]
fn dired_delete_empty_marked_directories_removes_entries() {
    let result = eval_str_with_upstream_batch(
        r#"(progn
             (setq noninteractive t)
             (require 'dired)
             (require 'ert-x)
             (ert-with-temp-directory test-dir :suffix "-emaxx"
                 (let* ((dired-deletion-confirmer (lambda (_) "yes"))
                      (inhibit-message t)
                      (default-directory test-dir)
                      (buf nil))
                 (dotimes (i 2) (make-directory (format "empty-dir-%d" i)))
                 (make-directory "zeta-empty-dir")
                 (unwind-protect
                     (progn
                       (setq buf (dired default-directory))
                       (dired-toggle-marks)
                       (let ((before (dired-get-marked-files)))
                         (dired-do-delete nil)
                         (list (= 3 (length before))
                               (dired-get-marked-files)
                               (file-exists-p (expand-file-name "empty-dir-0" test-dir))
                               (file-exists-p (expand-file-name "zeta-empty-dir" test-dir)))))
                   (when (buffer-live-p buf) (kill-buffer buf))))))"#,
    );
    assert_eq!(
        result,
        Value::list([Value::T, Value::Nil, Value::Nil, Value::Nil])
    );
}

#[test]
fn dired_revert_preserves_line_when_header_length_changes() {
    let result = eval_str_with_upstream_batch(
        r#"(progn
             (setq noninteractive t)
             (require 'dired)
             (require 'ert-x)
             (ert-with-temp-directory top-dir :suffix "-emaxx"
               (let* ((subdir (expand-file-name "subdir" top-dir))
                      (header-len-fn (lambda ()
                                       (save-excursion
                                         (goto-char 1)
                                         (forward-line 1)
                                         (- (pos-eol) (point)))))
                      orig-len len diff pos line-nb)
                 (make-directory subdir 'parents)
                 (with-current-buffer (dired-noselect subdir)
                   (setq orig-len (funcall header-len-fn)
                         pos (point)
                         line-nb (line-number-at-pos))
                   (make-directory "subdir" t)
                   (dired-revert)
                   (save-excursion
                     (goto-char 1)
                     (forward-line 1)
                     (let ((inhibit-read-only t)
                           (new-header "  test-bug27968"))
                       (delete-region (point) (pos-eol))
                       (when (= orig-len (length new-header))
                         (setq new-header (concat new-header " :-)")))
                       (insert new-header)))
                   (setq len (funcall header-len-fn)
                         diff (- len orig-len))
                   (list (not (zerop diff))
                         (= line-nb
                            (line-number-at-pos)
                            (line-number-at-pos (+ pos diff)))
                         (dired-get-filename 'local t))))))"#,
    );
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::String("subdir".into())])
    );
}

#[test]
fn line_move_moves_by_logical_lines_in_batch() {
    let result = eval_str_with_upstream_batch(
        r#"(with-temp-buffer
             (insert "alpha\nbeta\ngamma\n")
             (goto-char (point-min))
             (let ((first (line-move 1 t))
                   (second-line (line-number-at-pos))
                   (too-far (line-move 20 t)))
               (list first second-line too-far (line-number-at-pos))))"#,
    );
    assert_eq!(
        result,
        Value::list([Value::T, Value::Integer(2), Value::Nil, Value::Integer(4)])
    );
}

#[test]
fn directory_empty_p_and_temporary_file_directory_match_files_helpers() {
    let result = eval_str_with_upstream_batch(
        r#"(let* ((tmp (temporary-file-directory))
                  (dir (make-temp-file "emaxx-empty-dir-" t)))
             (unwind-protect
                 (let ((missing (expand-file-name "missing" dir)))
                   (list (stringp tmp)
                         (file-name-absolute-p tmp)
                         (directory-empty-p missing)
                         (directory-empty-p dir)
                         (progn
                           (make-empty-file (expand-file-name "child" dir))
                           (directory-empty-p dir))))
               (delete-directory dir t)))"#,
    );
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::Nil, Value::T, Value::Nil])
    );
}

#[test]
fn dired_insert_directory_free_space_uses_target_directory() {
    let result = eval_str_with_upstream_batch_features(
        &["cl-macs", "dired"],
        r#"(let* ((target (make-temp-file "emaxx-insert-dir-target-" t))
                  (other (make-temp-file "emaxx-insert-dir-other-" t))
                  (default-directory other)
                  (dired-free-space 'separate))
             (unwind-protect
                 (progn
                   (make-empty-file (expand-file-name "child" target))
                   (cl-letf (((symbol-function 'file-system-info)
                              (lambda (path)
                                (let ((free (if (equal (file-name-as-directory path)
                                                       (file-name-as-directory target))
                                                10
                                              100)))
                                  (list free free free)))))
                     (with-temp-buffer
                       (dired-insert-directory target "-l" nil nil nil)
                       (let ((output (buffer-string)))
                         (list (and (string-match-p "available 10 B" output) t)
                               (string-match-p "available 100 B" output))))))
               (delete-directory target t)
               (delete-directory other t)))"#,
    );
    assert_eq!(result, Value::list([Value::T, Value::Nil]));
}

#[test]
fn cl_case_rejects_misplaced_otherwise() {
    let _permit = crate::test_support::acquire_host_test_permit();
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    eval_str_with(&mut interp, "(require 'cl-macs)");
    let mut env: Env = Vec::new();
    let form = Reader::new("(cl-case 'zip (otherwise 'fallback) (zip 'hit))")
        .read()
        .unwrap()
        .unwrap();
    let error = interp.eval(&form, &mut env).unwrap_err();
    assert_eq!(error.condition_type(), "error");
    // This diagnostic is emitted by GNU cl-macs.el and retains GNU's curly
    // quotation marks; the removed native parser used ASCII quotes.
    assert_eq!(error.to_string(), "Misplaced t or ‘otherwise’ clause");
}

#[test]
fn sqlite_execute_surfaces_sql_input_errors_as_sqlite_error() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "ert-x",
            r#"
                (let ((db (sqlite-open)))
                  (sqlite-execute db "create table test (a)")
                  (should-error
                   (sqlite-execute db "insert into test values (fake(2))")
                   :type 'sqlite-error))
                "#
        ),
        Value::list([
            Value::symbol("sqlite-error"),
            Value::list([
                Value::String("SQL logic error".into()),
                Value::String("no such function: fake".into()),
                Value::Integer(1),
                Value::Integer(1),
            ]),
        ])
    );
}

#[test]
fn multisession_backends_observe_dynamic_user_init_file_across_library_calls() {
    run_with_large_stack(|| {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(format!("emaxx-multisession-{unique}"));
        std::fs::create_dir_all(&root).unwrap();
        let new_interpreter = || {
            let options = crate::batch::BatchRunOptions {
                load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                    .expect("upstream load path"),
                ..Default::default()
            };
            crate::batch::initialize_batch_interpreter(&options)
                .expect("initialize multisession batch interpreter")
        };

        let mut first = new_interpreter();
        let file_setup = eval_str_with(
            &mut first,
            &format!(
                r#"(progn
                     (require 'multisession)
                     (let ((user-init-file "/tmp/emaxx-multisession-init.el")
                           (multisession-storage 'files)
                           (multisession-directory {root:?}))
                       (define-multisession-variable emaxx-multisession-file 0
                         "" :synchronized t)
                       (list
                        (multisession-value emaxx-multisession-file)
                        (progn
                          (setf (multisession-value emaxx-multisession-file) 1)
                          (multisession-value emaxx-multisession-file))
                        (condition-case err
                            (progn
                              (setf (multisession-value
                                     emaxx-multisession-file)
                                    (make-marker))
                              'no-error)
                          (error (car err)))
                        (file-exists-p
                         (multisession--object-file-name
                          emaxx-multisession-file)))))"#,
                root = root.display().to_string(),
            ),
        );
        assert_eq!(
            file_setup,
            Value::list([
                Value::Integer(0),
                Value::Integer(1),
                Value::symbol("error"),
                Value::T,
            ])
        );

        let mut second = new_interpreter();
        let second_value = eval_str_with(
            &mut second,
            &format!(
                r#"(progn
                     (require 'multisession)
                     (let ((user-init-file "/tmp/emaxx-multisession-init.el")
                           (multisession-storage 'files)
                           (multisession-directory {root:?}))
                       (define-multisession-variable emaxx-multisession-file 0
                         "" :synchronized t)
                       (list
                        (multisession-value emaxx-multisession-file)
                        (progn
                          (setf (multisession-value emaxx-multisession-file) 2)
                          (multisession-value emaxx-multisession-file)))))"#,
                root = root.display().to_string(),
            ),
        );
        assert_eq!(
            second_value,
            Value::list([Value::Integer(1), Value::Integer(2)])
        );
        assert_eq!(
            eval_str_with(
                &mut first,
                &format!(
                    r#"(let ((user-init-file "/tmp/emaxx-multisession-init.el")
                             (multisession-directory {root:?}))
                         (multisession-value emaxx-multisession-file))"#,
                    root = root.display().to_string(),
                ),
            ),
            Value::Integer(2)
        );

        let sqlite_root = root.join("sqlite-backend");
        let sqlite_result = eval_str_with(
            &mut first,
            &format!(
                r#"(let ((user-init-file "/tmp/emaxx-multisession-init.el")
                         (multisession-storage 'sqlite)
                         (multisession-directory {root:?}))
                     (define-multisession-variable emaxx-multisession-sqlite 0
                       "" :synchronized t)
                     (unwind-protect
                         (list
                          (multisession-value emaxx-multisession-sqlite)
                          (progn
                            (setf (multisession-value
                                   emaxx-multisession-sqlite)
                                  1)
                            (multisession-value emaxx-multisession-sqlite))
                          (not (null multisession--db)))
                       (when multisession--db
                         (sqlite-close multisession--db)
                         (setq multisession--db nil))))"#,
                root = sqlite_root.display().to_string(),
            ),
        );
        assert_eq!(
            sqlite_result,
            Value::list([Value::Integer(0), Value::Integer(1), Value::T])
        );

        let _ = std::fs::remove_dir_all(&root);
    });
}

#[test]
fn backtrace_frames_from_current_thread_returns_live_frames() {
    let mut interp = Interpreter::new();
    let current_thread = interp.current_thread_value();
    interp.push_backtrace_frame(Value::Symbol("sample-backtrace-frame".into()), &[]);

    assert_eq!(
        interp.thread_backtrace_frames_snapshot(interp.resolve_thread_id(&current_thread).unwrap()),
        vec![(
            true,
            Value::Symbol("sample-backtrace-frame".into()),
            Vec::new(),
            false,
        )]
    );
}

#[test]
fn unevaluated_backtrace_frame_retains_the_live_source_form() {
    let mut interp = Interpreter::new();
    let form = Value::list([Value::Symbol("before".into()), Value::Integer(1)]);
    interp.push_unevaluated_backtrace_frame(&form);

    form.set_car(Value::Symbol("after".into()))
        .expect("source form is a cons");
    form.cdr()
        .expect("source form has a tail")
        .set_car(Value::Integer(2))
        .expect("source form has an argument cell");

    let (evald, function, args, _) = interp
        .current_backtrace_frame()
        .expect("unevaluated frame should be visible");
    assert!(!evald);
    assert_eq!(function, Value::Symbol("after".into()));
    assert_eq!(args, vec![Value::Integer(2)]);
}

#[test]
fn cached_source_forms_observe_mutation_and_recover_after_errors() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new("(+ 1 2)")
        .read()
        .expect("source form should parse")
        .expect("source form should exist");

    assert_eq!(interp.eval(&form, &mut env).unwrap(), Value::Integer(3));
    form.set_car(Value::symbol("*"))
        .expect("call source should be mutable");
    form.cdr()
        .expect("call should have arguments")
        .set_car(Value::Integer(3))
        .expect("first argument cell should be mutable");
    assert_eq!(interp.eval(&form, &mut env).unwrap(), Value::Integer(6));

    let improper = Value::cons(Value::symbol("+"), Value::Integer(1));
    assert!(matches!(
        interp.eval(&improper, &mut env),
        Err(LispError::TypeError(expected, _)) if expected == "list"
    ));
    assert_eq!(interp.eval(&form, &mut env).unwrap(), Value::Integer(6));
}

#[test]
fn cached_source_dispatch_analysis_observes_head_mutation() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let conditional = Reader::new("(if t 1 2)")
        .read()
        .expect("conditional should parse")
        .expect("conditional should exist");
    assert_eq!(
        interp.eval(&conditional, &mut env).unwrap(),
        Value::Integer(1)
    );
    conditional
        .set_car(Value::symbol("progn"))
        .expect("conditional head should be mutable");
    assert_eq!(
        interp.eval(&conditional, &mut env).unwrap(),
        Value::Integer(2)
    );

    let literal = Reader::new("(vector-literal 23)")
        .read()
        .expect("literal should parse")
        .expect("literal should exist");
    assert_eq!(
        literal.cons_id(),
        interp.eval(&literal, &mut env).unwrap().cons_id()
    );
    literal
        .set_car(Value::symbol("quote"))
        .expect("literal head should be mutable");
    assert_eq!(interp.eval(&literal, &mut env).unwrap(), Value::Integer(23));
}

#[test]
fn cons_mutation_invalidates_all_source_derivations() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let definition = Reader::new("(defmacro mutation-probe (value) (list 'quote value))")
        .read()
        .expect("macro definition should parse")
        .expect("macro definition should exist");
    interp.eval(&definition, &mut env).expect("define macro");

    let macro_call = Reader::new("(mutation-probe first)")
        .read()
        .expect("macro call should parse")
        .expect("macro call should exist");
    assert_eq!(
        interp.eval(&macro_call, &mut env).unwrap(),
        Value::symbol("first")
    );
    macro_call
        .cdr()
        .expect("macro call should have an argument")
        .set_car(Value::symbol("second"))
        .expect("macro argument cell should be mutable");
    assert_eq!(
        interp.eval(&macro_call, &mut env).unwrap(),
        Value::symbol("second")
    );

    let lambda_form = Reader::new("(lambda () 1)")
        .read()
        .expect("lambda should parse")
        .expect("lambda should exist");
    let first_lambda = interp.eval(&lambda_form, &mut env).expect("first lambda");
    lambda_form
        .cdr()
        .expect("lambda should have parameters")
        .cdr()
        .expect("lambda should have a body")
        .set_car(Value::Integer(2))
        .expect("lambda body cell should be mutable");
    let second_lambda = interp.eval(&lambda_form, &mut env).expect("second lambda");
    assert_eq!(
        interp
            .call_function_value(first_lambda, None, &[], &mut env)
            .unwrap(),
        Value::Integer(1)
    );
    assert_eq!(
        interp
            .call_function_value(second_lambda, None, &[], &mut env)
            .unwrap(),
        Value::Integer(2)
    );

    let quote_form = Reader::new("'(plain)")
        .read()
        .expect("quote should parse")
        .expect("quote should exist");
    let quote_template = quote_form
        .cdr()
        .expect("quote should have an argument")
        .car()
        .expect("quote argument should exist");
    assert_eq!(interp.eval(&quote_form, &mut env).unwrap(), quote_template);
    quote_template
        .set_car(Value::symbol("ordinary-symbol"))
        .expect("quote template should be mutable");
    assert_eq!(interp.eval(&quote_form, &mut env).unwrap(), quote_template);
}

#[test]
fn ert_x_remote_temp_directory_loads_after_tramp() {
    // `env.el' is part of GNU's dumped startup image.  Exercise Tramp from
    // the same initialized boundary instead of constructing an impossible
    // half-started interpreter with a load-path but no dumped libraries.
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize batch interpreter");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
              (let ((remote-temp-was-bound
                     (boundp 'ert-remote-temporary-file-directory))
                    (helper-was-defined (fboundp 'ert-filter-string))
                    (missing-home
                     (make-temp-name
                      (expand-file-name "emaxx-missing-home-"
                                        temporary-file-directory))))
                (setenv "HOME" missing-home)
                (require 'tramp)
                (require 'ert-x)
                (list remote-temp-was-bound
                      helper-was-defined
                      (equal (getenv "HOME")
                             (directory-file-name temporary-file-directory))
                      (featurep 'ert-x)
                      (file-remote-p ert-remote-temporary-file-directory)
                      (file-directory-p ert-remote-temporary-file-directory)
                      (file-writable-p ert-remote-temporary-file-directory)))
            "#
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
            Value::String(format!("/mock:{}:", primitives::system_name_value()).into()),
            Value::T,
            Value::T
        ])
    );
}

#[test]
fn dumped_bootstrap_exposes_core_preload_contracts() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list (boundp 'lexical-binding)
                   (special-variable-p 'lexical-binding)
                   lexical-binding
                   (boundp 'char-property-alias-alist)
                   (special-variable-p 'char-property-alias-alist)
                   char-property-alias-alist
                   (mapcar (lambda (function) (fboundp function))
                           '(widget-convert url-generic-parse-url
                             view-mode-enter sh-mode))
                   (boundp 'remote-shell-program)
                   (special-variable-p 'remote-shell-program)
                   (equal remote-shell-program
                          (or (executable-find \"ssh\") \"ssh\"))
                   (boundp 'display-comint-buffer-action)
                   (special-variable-p 'display-comint-buffer-action)
                   (equal display-comint-buffer-action
                          '(display-buffer-same-window
                            (inhibit-same-window . nil)
                            (category . comint)))
                   (fboundp 'file-user-uid)
                   (fboundp 'file-group-gid)
                   (= (file-user-uid) (user-uid))
                   (= (file-group-gid) (group-gid))
                   (fboundp 'exec-path)
                   (equal (exec-path) exec-path)
                   (boundp 'bidi-control-characters)
                   (special-variable-p 'bidi-control-characters)
                   (equal bidi-control-characters
                          '(#x200e #x200f #x061c #x202a #x202b #x202d
                            #x202e #x2066 #x2067 #x2068 #x202c #x2069))
                   (equal (bidi-string-strip-control-characters
                           (string ?a #x202e ?b #x2069 ?c))
                          \"abc\")
                   (not (subrp (symbol-function 'function-get))))",
        ),
        Value::list([
            Value::T,
            Value::T,
            // GNU batch evaluation sees *scratch*'s buffer-local
            // lexical-binding t (probed: (t nil) against the default).
            Value::T,
            Value::T,
            Value::T,
            Value::Nil,
            Value::list([Value::T, Value::T, Value::T, Value::T]),
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
            Value::T,
        ])
    );
}

#[test]
fn unicode_normalization_builtins_cover_canonical_and_compatibility_forms() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();

    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(progn
                 ;; GNU preloads ucs-normalize only in the NS loadup branch;
                 ;; a TTY/batch build (Emaxx's model) must require it.
                 (require 'ucs-normalize)
                 (list (ucs-normalize-NFC-string "LÅRSI")
                     (ucs-normalize-NFD-string "LÅRSI")
                     (ucs-normalize-NFKC-string "LÅRSI")
                     (ucs-normalize-NFKD-string "LÅRSI")))"#,
        ),
        Value::list([
            Value::String("LÅRSI".into()),
            Value::String("LA\u{30a}RSI".into()),
            Value::String("LÅRSI".into()),
            Value::String("LA\u{30a}RSI".into()),
        ])
    );
}

#[test]
fn gnu_batch_runtime_loads_custom_url_view_and_widget_entry_points() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (put 'sample-mode 'custom-mode-group 'sample)
               (put 'fallback 'custom-group '((option custom-variable)))
               (list (custom-group-of-mode 'sample-mode)
                     (custom-group-of-mode 'fallback-mode)
                     (custom-group-of-mode 'missing-mode)
                     (let ((url (url-generic-parse-url
                                 \"ircs://tester@irc.example:6697\")))
                       (url-host url))
                     (car (widget-convert 'string))
                     (equal remote-shell-program
                            (or (executable-find \"ssh\") \"ssh\"))
                     (with-temp-buffer
                       (font-lock-default-function 1)
                       char-property-alias-alist)
                     (with-temp-buffer
                       (view-mode-enter)
                       view-mode)))",
        ),
        Value::list([
            Value::Symbol("sample".into()),
            Value::Symbol("fallback".into()),
            Value::Nil,
            Value::String("irc.example".into()),
            Value::Symbol("string".into()),
            Value::T,
            Value::list([Value::list([
                Value::Symbol("face".into()),
                Value::Symbol("font-lock-face".into()),
            ])]),
            Value::T,
        ])
    );
}

#[test]
fn requiring_url_loads_lisp_setup_before_first_parser_call() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize batch interpreter");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(let ((premature-url-feature (featurep 'url)))
                     (require 'url)
                     (list premature-url-feature
                           (featurep 'url)
                           (fboundp 'url-host)
                           (url-host (url-generic-parse-url "Hello"))
                           (equal
                            (url-host
                             (url-generic-parse-url "https://gnu.org/"))
                            "gnu.org")))"#,
            ),
            Value::list([Value::Nil, Value::T, Value::T, Value::Nil, Value::T,])
        );
    });
}

#[test]
fn requiring_url_http_replaces_autoloads_with_lisp_owned_functions() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize batch interpreter");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(let ((premature-feature (featurep 'url-http))
                          (retrieve-autoload
                           (autoloadp (symbol-function 'url-retrieve))))
                     (require 'url-http)
                     (list premature-feature
                           retrieve-autoload
                           (featurep 'url-http)
                           (autoloadp (symbol-function 'url-retrieve))
                           (fboundp 'url-http)
                           (special-variable-p 'url-gateway-method)))"#,
            ),
            Value::list([
                Value::Nil,
                Value::T,
                Value::T,
                Value::Nil,
                Value::T,
                Value::T
            ])
        );
    });
}

#[test]
fn gnu_batch_runtime_loads_paren_blinking_defaults() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list blink-matching-paren
                   blink-matching-paren-on-screen
                   blink-matching-paren-distance
                   blink-matching-delay
                   blink-matching-paren-dont-ignore-comments
                   blink-matching-paren-highlight-offscreen)",
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::Integer(100 * 1024),
            Value::Integer(1),
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn gnu_batch_runtime_exposes_condition_case_unless_debug_as_a_macro() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list (macrop 'condition-case-unless-debug)
                   (special-form-p 'condition-case-unless-debug)
                   (condition-case-unless-debug err
                       (error \"boom\")
                     (error (car err))))"
        ),
        Value::list([Value::T, Value::Nil, Value::Symbol("error".into()),])
    );
}

#[test]
fn gnu_batch_runtime_exposes_ignore_errors_as_a_preloaded_macro() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list (macrop 'ignore-errors)
                   (special-form-p 'ignore-errors)
                   (ignore-errors (error \"boom\")))"
        ),
        Value::list([Value::T, Value::Nil, Value::Nil])
    );
}

#[test]
fn gnu_batch_runtime_macroexp_file_name_does_not_leak_ert_source() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    let test_file = "/tmp/emaxx-batch-runtime-resource-tests.el";
    interp.set_current_load_file(Some(test_file.into()));
    interp.set_variable(
        "current-load-list",
        Value::list([Value::String(test_file.into())]),
        &mut Vec::new(),
    );
    eval_str_with(
        &mut interp,
        "(ert-deftest batch-runtime-does-not-leak-ert-source ()
           (should-not (macroexp-file-name)))",
    );
    interp.set_variable("current-load-list", Value::Nil, &mut Vec::new());
    interp.set_current_load_file(None);

    assert_eq!(interp.run_ert_tests(), (1, 0, 1));
}

#[test]
fn mailabbrev_builds_aliases_from_the_configured_mailrc() {
    let mailrc = upstream_emacs_repo().join("test/lisp/net/eudc-resources/mailrc");
    assert_eq!(
        eval_str_with_upstream_batch(&format!(
            r#"(let ((mail-personal-alias-file "{}"))
                  (equal (eudc-mailabbrev-query-internal
                          '((email . "lars")))
                         '(((email . "larsi@mail-abbrev.com")
                            (name . "Lars Ingebrigtsen")))))"#,
            mailrc.display()
        )),
        Value::T
    );
}

#[test]
fn gnu_batch_runtime_exposes_with_temp_buffer_as_a_preloaded_macro() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((expanded (macroexpand-all '(with-temp-buffer (insert \"ok\")
                                                  (buffer-string)))))
               (list (macrop 'with-temp-buffer)
                     (special-form-p 'with-temp-buffer)
                     (car expanded)
                     (eval '(with-temp-buffer (insert \"ok\")
                                               (buffer-string)) t)))"
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::Symbol("let".into()),
            Value::String("ok".into()),
        ])
    );
}

#[test]
fn gnu_batch_runtime_exposes_with_temp_message_as_a_preloaded_macro() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((calls 0)
                   (expanded (macroexpand-all
                              '(with-temp-message (progn (setq calls (1+ calls))
                                                         \"working\")
                                 (list 'done calls)))))
               (list (macrop 'with-temp-message)
                     (special-form-p 'with-temp-message)
                     (car expanded)
                     (with-temp-message (progn (setq calls (1+ calls)) \"working\")
                       (list 'done calls))))"
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::Symbol("let".into()),
            Value::list([Value::Symbol("done".into()), Value::Integer(1)]),
        ])
    );
}

#[test]
fn gnu_batch_runtime_exposes_with_delayed_message_as_a_preloaded_macro() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((calls 0))
               (list (macrop 'with-delayed-message)
                     (special-form-p 'with-delayed-message)
                     (with-delayed-message
                         (100 (progn (setq calls (1+ calls)) \"working\"))
                       (list 'done calls))))"
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::list([Value::Symbol("done".into()), Value::Integer(1)]),
        ])
    );
}

#[test]
fn gnu_batch_runtime_exposes_the_font_lock_hook_entry_point() {
    // GNU 30.2 font-core.el owns these functions and deliberately leaves
    // `font-lock-mode' disabled when `noninteractive' is non-nil.
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(with-temp-buffer
               (insert \"abc\")
               (put-text-property 1 4 'font-lock-face 'bold)
               (set-buffer-modified-p nil)
               (narrow-to-region 2 3)
               (font-lock-defontify)
               (let ((properties
                      (save-restriction
                        (widen)
                        (text-properties-at 1))))
                 (list (fboundp 'turn-on-font-lock)
                       (buffer-modified-p)
                       properties
                       font-lock-mode
                       (turn-on-font-lock)
                       font-lock-mode
                       (font-lock-change-mode)
                       font-lock-mode)))"
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn gnu_batch_runtime_loads_subr_shell_process_wrappers() {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize batch interpreter");

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(mapcar #'fboundp
                     '(start-process-shell-command
                       start-file-process-shell-command
                       call-process-shell-command
                       process-file-shell-command))"
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn gnu_batch_runtime_loads_store_match_data_as_the_gnu_alias() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (string-match \"b\" \"abc\")
               (store-match-data '(0 1))
               (list (fboundp 'store-match-data) (match-data)))"
        ),
        Value::list([
            Value::T,
            Value::list([Value::Integer(0), Value::Integer(1)]),
        ])
    );
}

#[test]
fn gnu_batch_runtime_loads_wholenump_as_the_gnu_natnump_alias() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list (eq (symbol-function 'wholenump) 'natnump)
                   (wholenump 0)
                   (wholenump 1000000000000000000000000000000)
                   (wholenump -1)
                   (wholenump 1.0)
                   (wholenump nil))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn preloaded_completing_read_delegates_through_the_gnu_dispatch_variable() {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize batch interpreter");

    assert_eq!(
        eval_str_with(
            &mut interp,
            // GNU does not preload cl-lib: `emacs -Q -batch' signals
            // `void-function cl-letf' for this same program, and returns
            // ("mocked" 8) once cl-lib is required.
            "(progn
              (require 'cl-lib)
              (list
               (cl-letf (((symbol-function 'read-from-minibuffer)
                          (lambda (&rest _args) \"mocked\")))
                 (completing-read \"Prompt: \" nil))
               (let ((completing-read-function
                      (lambda (&rest args) (length args))))
                 (completing-read \"Prompt: \" nil nil t nil nil nil t))))"
        ),
        Value::list([Value::String("mocked".into()), Value::Integer(8)])
    );
}

#[test]
fn preloaded_kbd_preserves_gnu_ascii_string_and_symbolic_vector_results() {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize batch interpreter");

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list (kbd \"RET\")
                   (kbd \"a RET\")
                   (kbd \"<return>\")
                   (kbd \"é\")
                   (stringp (kbd \"RET\"))
                   (vectorp (kbd \"<return>\")))"
        ),
        Value::list([
            Value::String("\r".into()),
            Value::String("a\r".into()),
            Value::list([
                Value::Symbol("vector-literal".into()),
                Value::Symbol("return".into()),
            ]),
            Value::list([
                Value::Symbol("vector-literal".into()),
                Value::Integer('é' as i64),
            ]),
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn gnu_batch_runtime_loads_delete_consecutive_dups_with_destructive_identity() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let* ((items (list 'a 'a 'b 'b 'a))
                    (result (delete-consecutive-dups items)))
               (list (eq items result)
                     result
                     (delete-consecutive-dups (list 'a 'b 'a) t)))"
        ),
        Value::list([
            Value::T,
            Value::list([Value::symbol("a"), Value::symbol("b"), Value::symbol("a"),]),
            Value::list([Value::symbol("a"), Value::symbol("b")]),
        ])
    );
}

#[test]
fn gnu_batch_runtime_preserves_the_real_vc_responsible_backend_autoload() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize batch interpreter");
        let repo = serde_json::to_string(&upstream_emacs_repo().display().to_string())
            .expect("encode upstream path");

        assert_eq!(
            eval_str_with(
                &mut interp,
                &format!(
                    "(list (autoloadp (symbol-function 'vc-responsible-backend))
                           (let ((vc-handled-backends '(Git)))
                             (vc-responsible-backend {repo} t))
                           (featurep 'vc))"
                )
            ),
            Value::list([Value::T, Value::symbol("Git"), Value::T])
        );
    });
}

#[test]
fn batch_startup_preloads_gnu_environment_helpers() {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize batch interpreter");

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list (featurep 'env)
                   (fboundp 'substitute-env-vars)
                   (substitute-env-vars \"$EMAXX_UNDEFINED/x\" 'only-defined))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::String("$EMAXX_UNDEFINED/x".into()),
        ])
    );
}

#[test]
fn batch_startup_preloads_the_gnu_european_coding_owner() {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize batch interpreter");
    interp.set_variable(
        "data-directory",
        Value::String(
            crate::lisp::primitives::path_to_directory_string(&upstream_emacs_repo().join("etc"))
                .into(),
        ),
        &mut Vec::new(),
    );

    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(list (featurep 'european)
                     (coding-system-p 'iso-8859-15)
                     (coding-system-base 'latin-9)
                     (string-to-list
                      (encode-coding-string "€Œ" 'iso-8859-15))
                     (string-to-list
                      (encode-coding-string "Ⓡ" 'iso-8859-15))
                     (decode-coding-string
                      (unibyte-string #xa4 #xbc) 'iso-8859-15))"#
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::symbol("iso-latin-9"),
            Value::list([Value::Integer(0xa4), Value::Integer(0xbc)]),
            Value::list([Value::Integer(b' ' as i64)]),
            Value::String("€Œ".into()),
        ])
    );
}

#[test]
fn gnu_batch_runtime_exposes_iteration_forms_as_macros() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list
               (mapcar (lambda (form)
                         (list (macrop form) (special-form-p form)))
                       '(when unless dolist dotimes dolist-with-progress-reporter
                         dotimes-with-progress-reporter))
               (let (seen)
                 (dolist (elem '(a b c) (nreverse seen))
                   (push elem seen)))
               (let (seen)
                 (dotimes (index 3 (nreverse seen))
                   (push index seen))))"
        ),
        Value::list([
            Value::list([
                Value::list([Value::T, Value::Nil]),
                Value::list([Value::T, Value::Nil]),
                Value::list([Value::T, Value::Nil]),
                Value::list([Value::T, Value::Nil]),
                Value::list([Value::T, Value::Nil]),
                Value::list([Value::T, Value::Nil]),
            ]),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ]),
            Value::list([Value::Integer(0), Value::Integer(1), Value::Integer(2),]),
        ])
    );
}

#[test]
fn gnu_batch_runtime_exposes_completion_table_combinators() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(list
                 (mapcar #'fboundp
                         '(completion-table-with-cache
                           completion-table-with-context
                           completion-table-with-terminator
                           completion-table-with-predicate
                           completion-table-in-turn
                           completion-table-merge))
                 (macrop 'lazy-completion-table)
                 (functionp (completion-table-in-turn '("aa") '("bb")))
                 (completion-table-with-predicate
                  '("aa" "ab") (lambda (value) (equal value "aa"))
                  'strict "a" nil t)
                 (all-completions
                  "b" (completion-table-in-turn '("aa") '("bb")))
                 (all-completions
                  "a" (completion-table-merge '("aa") '("ab")))
                 (completion-table-with-terminator
                  "/" '("dir") "dir" nil nil)
                 (completion-table-with-context
                  "pre-" '("one") "o" nil nil))"#
        ),
        Value::list([
            Value::list([Value::T, Value::T, Value::T, Value::T, Value::T, Value::T,]),
            Value::T,
            Value::T,
            Value::list([Value::String("aa".into())]),
            Value::list([Value::String("bb".into())]),
            Value::list([Value::String("aa".into()), Value::String("ab".into())]),
            Value::String("dir/".into()),
            Value::String("pre-one".into()),
        ])
    );
}

#[test]
fn gnu_batch_runtime_exposes_file_name_completion_table() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let ((directory (make-temp-file "emaxx-file-table-" t)))
                 (unwind-protect
                     (let ((default-directory
                            (file-name-as-directory directory)))
                       (write-region "" nil "file.txt")
                       (list
                        (fboundp 'completion-file-name-table)
                        (completion-file-name-table "fi" nil nil)
                        (completion-file-name-table "fi" nil t)
                        (completion-file-name-table "fi" nil 'metadata)
                        (completion-file-name-table
                         "sub/fi" nil '(boundaries . "/tail"))))
                   (delete-directory directory t)))"#
        ),
        Value::list([
            Value::T,
            Value::String("file.txt".into()),
            Value::list([Value::String("file.txt".into())]),
            Value::list([
                Value::Symbol("metadata".into()),
                Value::cons(
                    Value::Symbol("category".into()),
                    Value::Symbol("file".into())
                ),
            ]),
            Value::cons(
                Value::Symbol("boundaries".into()),
                Value::cons(Value::Integer(4), Value::Integer(0)),
            ),
        ])
    );
}

#[test]
fn completion_at_point_uses_partial_completion_wildcards() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();

    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(with-temp-buffer
                 (insert "fi*.el")
                 (let ((completion-styles '(basic partial-completion emacs22))
                       (completion-at-point-functions
                        (list (lambda ()
                                (list (point-min) (point-max)
                                      '("file.el" "file.txt"))))))
                   (completion-at-point)
                   (buffer-string)))"#,
        ),
        Value::String("file.el".into())
    );
}

#[test]
fn completion_at_point_displays_ambiguous_candidates_after_no_progress() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();

    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(with-temp-buffer
                 (insert "fi")
                 (let ((completion-at-point-functions
                        (list (lambda ()
                                (list (point-min) (point-max)
                                      '("file.el" "file.txt"))))))
                   (completion-at-point)
                   (let ((first (buffer-string))
                         (shown-first (get-buffer-window "*Completions*")))
                     (completion-at-point)
                     (list first
                           shown-first
                           (not (null (get-buffer-window "*Completions*")))))))"#,
        ),
        Value::list([Value::String("file.".into()), Value::Nil, Value::T])
    );
}

fn upstream_lisp_test_interpreter(test_file: &str) -> Interpreter {
    let emacs_repo =
        std::fs::canonicalize(upstream_emacs_repo()).expect("canonical upstream Emacs repository");
    // These callers exercise real upstream test files after normal batch
    // startup; none is a source-bootstrap test.  Reuse the reconstructed
    // compiled GNU image, then load the actual GNU ERT and test sources below.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    // Match the compatibility runner's installation-directory boundary.
    // Source-aware upstream tests must not accidentally inspect the Emaxx
    // workspace just because this faster in-process harness started there.
    let mut env = Vec::new();
    interp.set_variable(
        "source-directory",
        Value::String(crate::lisp::primitives::path_to_directory_string(&emacs_repo).into()),
        &mut env,
    );
    let data_directory = crate::lisp::primitives::path_to_directory_string(&emacs_repo.join("etc"));
    interp.set_variable(
        "data-directory",
        Value::String(data_directory.clone().into()),
        &mut env,
    );
    interp.set_variable(
        "doc-directory",
        Value::String(data_directory.into()),
        &mut env,
    );
    // The compatibility harness passes `-l ert' before loading every test
    // file.  Emaxx also has native bootstrap ERT forms, so relying on the
    // test file's `(require 'ert)' would leave that already-provided feature
    // in place and make fast regressions exercise a different macro surface.
    interp
        .load_target("ert")
        .expect("load upstream GNU ERT before the test file");
    crate::lisp::load_file_strict(
        &mut interp,
        &upstream_emacs_repo().join("test/lisp").join(test_file),
    )
    .expect("load upstream Lisp tests");
    interp
}

#[test]
fn upstream_completion_preview_uses_preloaded_forward_symbol() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("completion-preview-tests.el");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(list (fboundp 'forward-whitespace)
                       (fboundp 'forward-symbol)
                       (fboundp 'forward-same-syntax)
                       (with-temp-buffer
                         (insert \"foo\")
                         (bounds-of-thing-at-point 'symbol))
                       (with-temp-buffer
                         (insert \"foo bar\")
                         (forward-symbol -1)
                         (point))
                       (with-temp-buffer
                         (insert \"  \\ntext\")
                         (goto-char (point-min))
                         (forward-whitespace 2)
                         (point))
                       (with-temp-buffer
                         (insert \"abc \")
                         (goto-char (point-min))
                         (forward-same-syntax)
                         (point)))"
            ),
            Value::list([
                Value::T,
                Value::T,
                Value::T,
                Value::cons(Value::Integer(1), Value::Integer(4)),
                Value::Integer(5),
                Value::Integer(4),
                Value::Integer(4),
            ])
        );

        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.total, 11, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 11, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_elisp_mode_xref_reads_native_generic_metadata() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("progmodes/elisp-mode-tests.el");
        let selector = eval_str_with(
            &mut interp,
            "'(member
               xref-elisp-test-find-defs-defgeneric-co-located-default
               xref-elisp-test-find-defs-defgeneric-implicit-generic
               xref-elisp-test-find-defs-defgeneric-no-default
               xref-elisp-test-find-defs-defgeneric-no-methods
               xref-elisp-test-find-defs-defgeneric-separate-default)",
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(summary.total, 5, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 5, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_descr_text_uses_unicode_property_table_descriptions() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("descr-text-tests.el");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(substring-no-properties
                   (describe-char-eldoc--format #x2026))"
            ),
            Value::String("U+2026: Horizontal ellipsis (Po: Punctuation, Other)".into(),)
        );

        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.total, 3, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 3, "{:#?}", interp.test_results);
    });
}

#[test]
fn dumped_loaddefs_inline_functions_are_available_without_owner_loads() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(list (fboundp 'cvs-dired-noselect)
                   (cvs-dired-noselect temporary-file-directory)
                   (fboundp 'tramp-autoload-file-name-handler)
                   (fboundp 'vc-cvs-registered))"
        ),
        Value::list([Value::T, Value::Nil, Value::T, Value::T])
    );
}

#[test]
fn upstream_dired_does_not_refresh_parent_for_nested_file_changes() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("dired-tests.el");
        let name = "dired-test-bug27243-01";
        let summary = interp.run_ert_tests_with_selector(Some(&Value::Symbol(name.into())));
        assert_eq!(summary.total, 1, "{name}: {:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "{name}: {:#?}", interp.test_results);
    });
}

#[test]
fn upstream_dired_killed_buffer_cleanup_preserves_later_window_display() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("dired-tests.el");
        let selector = eval_str_with(
            &mut interp,
            "'(member dired-test-bug25609
                      files-tests-bug-50630
                      files-tests-insert-directory-shows-files
                      files-tests-insert-directory-shows-free)",
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(summary.total, 4, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 4, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_dnd_mock_remote_transport_handles_file_lifecycle() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("dnd-tests.el");
        let selector = eval_str_with(
            &mut interp,
            "'(member dnd-tests-begin-file-drag
                      dnd-tests-begin-drag-files)",
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(summary.total, 2, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 2, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_files_fast_processes_do_not_leak_into_exit_queries() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("files-tests.el");
        let subprocess = Value::symbol("files-tests-file-name-non-special--subprocess");
        let subprocess_summary = interp.run_ert_tests_with_selector(Some(&subprocess));
        assert_eq!(subprocess_summary.passed, 1, "{:#?}", interp.test_results);
        // Emaxx can reach the next test before these tiny host processes have
        // been scheduled.  Once they have exited, the later process-status
        // must be delivered at the ERT boundary without an explicit Lisp
        // output wait, just as GNU's SIGCHLD-driven runner does.
        std::thread::sleep(std::time::Duration::from_secs(1));
        let selector = eval_str_with(
            &mut interp,
            "'(member files-tests-save-buffers-kill-emacs--asks-to-save-buffers
                      files-tests-save-buffers-kill-emacs--confirm-kill-processes)",
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(summary.total, 2, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 2, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_electric_mode_producers_match_their_gnu_owners() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("electric-tests.el");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(list
                   (with-temp-buffer
                     (plainer-c-mode)
                     (list indent-line-function
                           (progn
                             (insert \"int main () {\\n\\n}\")
                             (goto-char 15)
                             (indent-according-to-mode)
                             (buffer-string))))
                   (subrp (indirect-function 'tex-mode))
                   (with-temp-buffer
                     (tex-mode)
                     (list major-mode
                           (key-binding \"\\\"\")
                           (subrp (indirect-function 'tex-insert-quote)))))",
            ),
            Value::list([
                Value::list([
                    Value::symbol("c-indent-line"),
                    Value::String("int main () {\n  \n}".into()),
                ]),
                Value::Nil,
                Value::list([
                    Value::symbol("latex-mode"),
                    Value::symbol("tex-insert-quote"),
                    Value::Nil,
                ]),
            ])
        );
    });
}

#[test]
fn upstream_custom_theme_uses_native_frame_parameter_updates() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("custom-tests.el");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(subrp (indirect-function 'modify-frame-parameters))"
            ),
            Value::T
        );

        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.total, 9, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 9, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_cl_macs_dynamic_labels_and_symbol_macro_regressions_stay_green() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/cl-macs-tests.el");
        let selector = eval_str_with(
            &mut interp,
            "'(member cl-macs--labels cl-macs--progv cl-macs-test--symbol-macrolet)",
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));

        assert_eq!(summary.total, 3, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 3, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_nested_backquote_regression_stays_green() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/lisp-tests.el");
        let summary = interp
            .run_ert_tests_with_selector(Some(&Value::symbol("core-elisp-tests-3-backquote")));

        assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_ert_resource_macro_finds_faces_theme_from_defining_file() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("faces-tests.el");
        let summary = interp
            .run_ert_tests_with_selector(Some(&Value::symbol("faces--test-extend-with-themes")));

        assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_map_condition_regressions_stay_green() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/map-tests.el");
        let selector = eval_str_with(&mut interp, "'(member test-map-into test-map-merge-empty)");
        let summary = interp.run_ert_tests_with_selector(Some(&selector));

        assert_eq!(summary.total, 2, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 2, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_subr_x_macro_expansion_regression_stays_green() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/subr-x-tests.el");
        // This regression only appeared after earlier macro-expansion tests
        // had run, so preserve the complete per-file execution order.
        let summary = interp.run_ert_tests_with_selector(None);

        assert_eq!(summary.total, 47, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 47, "{:#?}", interp.test_results);
    });
}

#[test]
fn edebug_instrumented_cl_macrolet_preserves_expander_arguments() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/edebug-tests.el");
        let summary = interp
            .run_ert_tests_with_selector(Some(&Value::Symbol("edebug-tests-cl-macrolet".into())));

        assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn edebug_global_break_condition_preserves_the_complete_minibuffer_expression() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/edebug-tests.el");
        let summary = interp.run_ert_tests_with_selector(Some(&Value::Symbol(
            "edebug-tests-set-and-break-on-global-condition".into(),
        )));

        assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn edebug_step_into_generic_method_uses_the_correct_source_stop_points() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/edebug-tests.el");
        let summary = interp.run_ert_tests_with_selector(Some(&Value::Symbol(
            "edebug-tests-step-into-generic-method".into(),
        )));
        let stop_points = eval_str_with(
            &mut interp,
            "(list
               (assoc \"edebug-test-code-emphasize\" edebug-tests-stop-points)
               (assoc \"edebug-test-code-emphasize-1\" edebug-tests-stop-points)
               (assoc \"edebug-test-code-use-methods\" edebug-tests-stop-points))",
        );
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(functionp
                  (cdr (assq 'cl-defmethod find-function-regexp-alist)))",
            ),
            Value::T,
            "cl-generic's real after-load callback must install find-func's method searcher",
        );

        assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
        assert_eq!(
            summary.passed, 1,
            "{:#?}; stop points: {stop_points:#?}",
            interp.test_results
        );
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn edebug_recursive_command_loop_timer_and_macro_minibuffer_state_stay_coherent() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/edebug-tests.el");
        // Keep the upstream ERT order explicit so a cleanup-path corruption
        // is attributed to the test that introduced it rather than reported
        // as a later, unrelated missing function.
        for name in [
            "edebug-tests-error-stepping-into-subr",
            "edebug-tests-error-trying-to-set-breakpoint-in-uninstrumented-code",
            "edebug-tests-evaluate-expressions",
            "edebug-tests-gv-expander",
            "edebug-tests-set-and-break-on-global-condition",
            "edebug-tests-step-into-generic-method",
        ] {
            let summary = interp.run_ert_tests_with_selector(Some(&Value::symbol(name)));
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(list (fboundp 'timerp) (featurep 'timer)
                           (and (symbol-file 'timerp 'defun) t))",
                ),
                Value::list([Value::T, Value::T, Value::T]),
                "{name} cleanup must preserve GNU's preloaded timer.el owner"
            );
            assert_eq!(summary.total, 1, "{name}: {:#?}", interp.test_results);
            assert_eq!(summary.passed, 1, "{name}: {:#?}", interp.test_results);
            assert_eq!(summary.failed, 0, "{name}: {:#?}", interp.test_results);
        }
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(list
                   (length
                    (seq-filter
                     (lambda (entry)
                       (member '(provide . edebug-test-code) (cdr entry)))
                     load-history))
                   (cl--generic-method-table
                    (cl--generic 'edebug-test-code-emphasize)))",
            ),
            Value::list([Value::Integer(0), Value::Nil]),
            "GNU unloads the feature's load-history entry and generic method table"
        );
    });
}

#[test]
fn edebug_sample_code_eval_buffer_reaches_its_provide_form() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/edebug-tests.el");
        let file = eval_str_with(
            &mut interp,
            r#"(let ((file (make-temp-file "emaxx-edebug-code" nil ".el")))
                 (edebug-tests-setup-code-file file)
                 file)"#,
        );
        assert_eq!(
            eval_str_with(
                &mut interp,
                &format!(
                    "(with-current-buffer (find-file-noselect {})
                       (goto-char (point-min))
                       (and (re-search-forward \"!\\\\(\\\\S-+?\\\\)!\" nil t)
                            (match-string-no-properties 0)))",
                    file
                ),
            ),
            Value::Nil,
            "the setup producer must remove every Edebug stop-point annotation"
        );
        assert_eq!(
            eval_str_with(
                &mut interp,
                &format!(
                    "(unwind-protect
                         (progn
                           (with-current-buffer (find-file {})
                             (read-only-mode)
                             (setq lexical-binding t)
                             (syntax-ppss)
                             (eval-buffer))
                           (featurep 'edebug-test-code))
                       (ignore-errors (delete-file {})))",
                    file, file
                ),
            ),
            Value::T
        );
    });
}

#[test]
fn eieio_method_invocation_order_and_next_arguments_stay_coherent() {
    run_with_large_stack(|| {
        let mut interp =
            upstream_lisp_test_interpreter("emacs-lisp/eieio-tests/eieio-test-methodinvoke.el");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(list
                   (cl--class-allparents (cl-find-class 'eitest-B))
                   (cl--class-allparents (cl-find-class 'D))
                   (cl--class-allparents (cl-find-class 'E)))",
            ),
            eval_str(
                "'((eitest-B eitest-B-base1 eitest-B-base2
                    eieio-default-superclass record atom t)
                   (D D-base1 D-base2 D-base0
                    eieio-default-superclass record atom t)
                   (E E-base1 E-base2 E-base0
                    eieio-default-superclass record atom t))",
            ),
            "EIEIO class precedence must preserve direct-parent order"
        );
        let selector = eval_str_with(
            &mut interp,
            r#"'(member
                 eieio-test-cl-generic-1
                 eieio-test-method-order-list-10
                 eieio-test-method-order-list-3
                 eieio-test-method-order-list-4
                 eieio-test-method-order-list-5
                 eieio-test-method-order-list-7
                 eieio-test-method-order-list-8
                 eieio-test-method-order-list-9)"#,
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));

        assert_eq!(summary.total, 8, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 8, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn eieio_persistence_recurses_through_container_values_with_expected_reader_policy() {
    run_with_large_stack(|| {
        let mut interp =
            upstream_lisp_test_interpreter("emacs-lisp/eieio-tests/eieio-test-persist.el");
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(let* ((file (make-temp-file "emaxx-persist-probe-"))
                          (jane (make-instance 'person :name "Jane"))
                          (bob (make-instance 'person :name "Bob"))
                          (object
                           (make-instance
                            'classy
                            :teacher jane
                            :janitors (list [tuesday nil] [friday nil])
                            :random-vector [nil]
                            :file file)))
                     (puthash "Bob" bob (slot-value object 'students))
                     (aset (slot-value object 'random-vector) 0
                           (make-instance 'persistent-random-class))
                     (unwind-protect
                         (let ((save
                                (condition-case error
                                    (progn
                                      (eieio-persistent-save object)
                                      'save-ok)
                                  (error (list 'save-error error)))))
                           (list
                            save
                            (condition-case error
                                (progn
                                  (eieio-persistent-read file 'classy)
                                  'read-ok)
                              (error (list 'read-error error)))))
                       (ignore-errors (delete-file file))))"#,
            ),
            eval_str("'(save-ok read-ok)"),
            "nested persistence must survive both serialization phases"
        );
        let selector = eval_str_with(
            &mut interp,
            r#"'(member
                 eieio-persist-hash-and-vector-backward-compatibility
                 eieio-persist-hash-and-vector-no-backward-compatibility
                 eieio-test-persist-interior-lists-backward-compatibility
                 eieio-test-persist-interior-lists-no-backward-compatibility)"#,
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));

        assert_eq!(summary.total, 4, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 2, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 2, "{:#?}", interp.test_results);
        assert_eq!(summary.unexpected, 0, "{:#?}", interp.test_results);
        assert!(
            interp
                .test_results
                .iter()
                .filter(|outcome| outcome.status == crate::compat::TestStatus::Failed)
                .all(|outcome| outcome.condition_type.as_deref() == Some("invalid-read-syntax")),
            "{:#?}",
            interp.test_results
        );
    });
}

#[test]
fn eieio_core_canonical_suite_preserves_cross_test_object_state() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/eieio-tests/eieio-tests.el");
        let selector = eval_str_with(
            &mut interp,
            "'(not (member eieio-test-37-obsolete-name-in-constructor))",
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));

        assert_eq!(summary.total, 41, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 40, "{:#?}", interp.test_results);
        assert_eq!(summary.skipped, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn ert_font_lock_success_paths_share_the_runners_pass_catch() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/ert-font-lock-tests.el");
        let selector = eval_str_with(
            &mut interp,
            "'(member test-font-lock-test-file--correct
                      test-font-lock-test-string--correct
                      test-macro-test--file)",
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));

        assert_eq!(summary.total, 3, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 3, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn file_name_completion_rejects_a_missing_directory_before_adding_dot_entries() {
    let missing = std::env::temp_dir().join(format!(
        "emaxx-missing-completion-directory-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let expression = format!(
        r#"(condition-case error
               (file-name-all-completions "" {missing:?})
             (file-missing (list (car error) (cadr error))))"#
    );
    assert_eq!(
        eval_str(&expression),
        Value::list([
            Value::Symbol("file-missing".into()),
            Value::String("Opening directory".into()),
        ])
    );
}

#[test]
fn file_name_completion_honors_predicates_case_and_regexps_in_the_requested_directory() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((directory (make-temp-file "emaxx-file-completion-" t)))
                 (unwind-protect
                     (let ((directory (file-name-as-directory directory)))
                       (make-directory (expand-file-name "dir" directory))
                       (write-region "" nil (expand-file-name "file" directory))
                       (list
                        (file-name-completion
                         "" directory
                         (lambda (candidate)
                           (and (equal default-directory directory)
                                (file-directory-p candidate))))
                        (let ((completion-ignore-case t))
                          (file-name-completion "D" directory))
                        (let ((completion-regexp-list '("file")))
                          (file-name-all-completions "" directory))))
                   (delete-directory directory t)))"#
        ),
        Value::list([
            Value::String("dir/".into()),
            Value::String("dir/".into()),
            Value::list([Value::String("file".into())]),
        ])
    );
}

#[test]
fn minibuffer_message_timeout_has_its_native_default_and_special_contract() {
    assert_eq!(
        eval_str(
            "(list minibuffer-message-timeout
                   (special-variable-p 'minibuffer-message-timeout))"
        ),
        Value::list([Value::Integer(2), Value::T])
    );
}

#[test]
fn find_function_suite_uses_preloaded_tag_helpers_and_the_upstream_doc_index() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/find-func-tests.el");
        let summary = interp.run_ert_tests_with_selector(None);

        assert_eq!(summary.total, 6, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 6, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn cl_macrolet_expander_stays_lexical_inside_dynamic_eval() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(eval
               '(cl-macrolet
                    ((sample-expand (value)
                       (funcall
                        (function
                         (lambda () :closure-dont-trim-context value)))))
                  (sample-expand 42)))"
        ),
        Value::Integer(42)
    );
}

#[test]
fn byte_compilation_matches_interpreted_binding_cases() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/bytecomp-tests.el");
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(let (failures)
                     (dolist (lexical-binding '(nil t))
                       (dolist (form
                                (if lexical-binding
                                    (append bytecomp-tests--test-cases-lexbind-only
                                            bytecomp-tests--test-cases)
                                  bytecomp-tests--test-cases))
                         (let ((interpreted
                                (bytecomp-tests--eval-interpreted form))
                               (compiled
                                (bytecomp-tests--eval-compiled form)))
                           (unless (equal interpreted compiled)
                             (push (list lexical-binding form
                                         interpreted compiled)
                                   failures)))))
                     (nreverse failures))"#,
            ),
            Value::Nil
        );
    });
}

#[test]
fn faceup_directory_load_context_matches_across_load_eval_buffer_and_eval_defun() {
    run_with_large_stack(|| {
        let mut interp =
            upstream_lisp_test_interpreter("emacs-lisp/faceup-tests/faceup-test-basics.el");
        let expected_directory = eval_str_with(&mut interp, "faceup-test-resources-directory");
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(let ((file (concat faceup-test-resources-directory
                                      "faceup-test-this-file-directory.el"))
                         (load-file-name nil)
                         values)
                     (makunbound 'faceup-test-this-file-directory)
                     (load file nil :nomessage)
                     (push faceup-test-this-file-directory values)
                     (makunbound 'faceup-test-this-file-directory)
                     (save-excursion
                       (find-file file)
                       (eval-buffer))
                     (push faceup-test-this-file-directory values)
                     (makunbound 'faceup-test-this-file-directory)
                     (save-excursion
                       (find-file file)
                       (save-excursion
                         (goto-char (point-min))
                         (while (not (eobp))
                           (eval-defun nil)
                           (forward-sexp))))
                     (push faceup-test-this-file-directory values)
                     (list (nreverse values)
                           faceup-test-resources-directory))"#,
            ),
            Value::list([
                Value::list([
                    expected_directory.clone(),
                    expected_directory.clone(),
                    expected_directory.clone(),
                ]),
                expected_directory,
            ])
        );
    });
}

fn ert_self_test_interpreter() -> Interpreter {
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp = crate::batch::initialize_batch_interpreter(&options)
        .expect("initialize ERT batch interpreter");
    interp.load_target("ert").expect("load upstream ERT");
    crate::lisp::load_file_strict(
        &mut interp,
        &upstream_emacs_repo().join("test/lisp/emacs-lisp/ert-tests.el"),
    )
    .expect("load upstream ERT self-tests");
    interp
}

#[test]
fn dumped_with_demoted_errors_uses_the_gnu_macro_surface() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize batch interpreter");

        assert_eq!(
            eval_str_with(
                &mut interp,
                "(list (macrop 'with-demoted-errors)\
                       (condition-case nil\
                           (progn\
                             (with-demoted-errors \"FOO: %S\" (error \"Foo\"))\
                             'continued)\
                         (error 'escaped)))",
            ),
            Value::list([Value::T, Value::symbol("continued")])
        );
    });
}

#[test]
fn ert_explain_equal_self_test_runs_with_its_recursive_lexical_helpers() {
    run_with_large_stack(|| {
        let mut interp = ert_self_test_interpreter();
        assert_eq!(
            eval_str_with(&mut interp, "(ert--explain-equal nil 'foo)"),
            Value::list([
                Value::symbol("different-atoms"),
                Value::Nil,
                Value::symbol("foo"),
            ])
        );
        let summary =
            interp.run_ert_tests_with_selector(Some(&Value::symbol("ert-test-explain-equal")));
        assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "{:#?}", interp.test_results);
    });
}

#[test]
fn loaded_ert_assertion_macros_preserve_nested_test_result_protocol() {
    run_with_large_stack(|| {
        let mut interp = ert_self_test_interpreter();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(list
                     (type-of
                      (ert-run-test
                       (make-ert-test
                        :body (lambda () (skip-when t)))))
                     (type-of
                      (ert-run-test
                       (make-ert-test
                        :body (lambda () (skip-unless nil))))))"#,
            ),
            // GNU: `skip-when'/`skip-unless' outside `ert-deftest' produce
            // failed results, not skips (probed on GNU 30.2).
            Value::list([
                Value::symbol("ert-test-failed"),
                Value::symbol("ert-test-failed"),
            ])
        );
        let selector = eval_str_with(
            &mut interp,
            r#"'(member ert-test-should
                       ert-test-should-not
                       ert-test-should-with-macrolet
                       ert-test-should-error
                       ert-test-should-error-subtypes
                       ert-test-should-failure-debugging
                       ert-test-list-of-should-forms
                       ert-test-list-of-should-forms-no-deep-copy
                       ert-test-list-of-should-forms-observers-should-not-stack
                       ert-test-skip-unless
                       ert-test-skip-when)"#,
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(summary.total, 11, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 11, "{:#?}", interp.test_results);
    });
}

#[test]
fn remaining_ert_prefix_self_tests_stay_green_in_the_native_runner() {
    run_with_large_stack(|| {
        let mut interp = ert_self_test_interpreter();
        let selector = eval_str_with(
            &mut interp,
            r#"'(member ert--pp-with-indentation-and-newline
                       ert-test-deftest
                       ert-test-explain-equal-keymaps
                       ert-test-explain-equal-strings
                       ert-test-get-explainer
                       ert-test-run-tests-batch
                       ert-test-run-tests-batch-expensive
                       ert-test-special-operator-p
                       ert-test-with-demoted-errors)"#,
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(summary.total, 9, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 9, "{:#?}", interp.test_results);
    });
}

#[test]
fn loaded_ert_self_test_file_stays_green_in_the_native_runner() {
    run_with_large_stack(|| {
        let mut interp = ert_self_test_interpreter();
        let selector = eval_str_with(
            &mut interp,
            "'(not (or (tag :expensive-test) (tag :unstable)))",
        );
        let discovered_before_run = interp.discovered_tests();
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(discovered_before_run.len(), 55);
        assert_eq!(summary.total, 55, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 55, "{:#?}", interp.test_results);
    });
}

#[test]
fn gv_child_process_diagnostics_decode_as_multibyte_text() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("emacs-lisp/gv-tests.el");
        // The regular Rust test binary is not an Emacs command-line driver.
        // Point this one child-process regression at the already-built local
        // GNU oracle without mutating the process environment shared by
        // parallel Rust tests.
        let oracle = upstream_emacs_repo().join("src/emacs");
        interp.set_global_binding(
            "invocation-name",
            Value::String(
                oracle
                    .file_name()
                    .expect("oracle executable name")
                    .to_string_lossy()
                    .into_owned()
                    .into(),
            ),
        );
        interp.set_global_binding(
            "invocation-directory",
            Value::String(
                format!(
                    "{}/",
                    oracle
                        .parent()
                        .expect("oracle executable directory")
                        .display()
                )
                .into(),
            ),
        );
        let summary = interp.run_ert_tests_with_selector(Some(&Value::Symbol(
            "gv-dont-define-expander-other-file".into(),
        )));

        assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "{:#?}", interp.test_results);
    });
}

#[test]
fn erc_channel_user_struct_setters_remain_generalized_variables() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize ERC batch interpreter");
        interp
            .load_target("lisp-mode")
            .expect("load GNU dumped lisp-mode contract");
        interp
            .load_target("ert")
            .expect("load GNU ERT macros used by the exact harness");
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("test/lisp/erc/erc-tests.el"),
        )
        .expect("load upstream ERC tests");
        let summary =
            interp.run_ert_tests_with_selector(Some(&Value::Symbol("erc-channel-user".into())));

        assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "{:#?}", interp.test_results);
    });
}

#[test]
fn loaded_gnu_setf_sees_lambda_gv_setter_declarations() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"(progn
                     (require 'gv)
                     (defun emaxx-gv-cell-value (cell)
                       (declare
                        (gv-setter
                         (lambda (value)
                           `(progn (setcar ,cell ,value) ,value))))
                       (car cell))
                     (let ((cell (list 0)))
                       (list (setf (emaxx-gv-cell-value cell) 7)
                             (emaxx-gv-cell-value cell))))"#,
            ),
            Value::list([Value::Integer(7), Value::Integer(7)]),
        );
    });
}

#[test]
fn loaded_ert_special_operator_probe_preserves_uninterned_argument_values() {
    run_with_large_stack(|| {
        let mut interp = ert_self_test_interpreter();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(let ((function-name (cl-gensym)))
                   (list (ert--special-operator-p function-name)
                         (progn (fset function-name 'if)
                                (ert--special-operator-p function-name))))",
            ),
            Value::list([Value::Nil, Value::T])
        );
    });
}

#[test]
fn lisp_indentation_aligns_continued_data_with_the_first_argument() {
    run_with_large_stack(|| {
        let mut interp = ert_self_test_interpreter();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(with-temp-buffer
                     (emacs-lisp-mode)
                     (insert "(:one \"1\" :three \"3\"\n:two \"2\")")
                     (goto-char (point-min))
                     (forward-char 1)
                     (let ((symbol-match (looking-at "\\s_"))
                           (word-match (looking-at "\\sw")))
                       (forward-line 1)
                       (lisp-indent-line)
                       (list symbol-match
                             word-match
                             (current-indentation)
                             (buffer-string))))"#,
            ),
            Value::list([
                Value::T,
                Value::Nil,
                Value::Integer(6),
                Value::String("(:one \"1\" :three \"3\"\n      :two \"2\")".into()),
            ])
        );
    });
}

#[test]
fn ewoc_footer_survives_replacing_header_and_node_text() {
    run_with_large_stack(|| {
        let mut interp = ert_self_test_interpreter();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(with-temp-buffer
                     (ert-results-mode)
                     (let ((inhibit-read-only t))
                       (erase-buffer)
                       (let* ((ewoc
                               (ewoc-create
                                (lambda (data)
                                  (when data
                                    (insert "    ")
                                    (ert--pp-with-indentation-and-newline data)
                                    (insert "\n")))
                                nil nil t))
                              (node (ewoc-enter-last ewoc nil)))
                         (ewoc-set-hf ewoc "header-1\n" "\n")
                         (ewoc-set-data
                          node
                          '(condition
                            (nested list whose representation wraps
                                    across more than one output line)))
                         (ewoc-invalidate ewoc node)
                         (ewoc-set-hf ewoc "header-2-longer\n" "\n")
                         (let* ((text (buffer-string))
                                (footer
                                 (marker-position
                                  (ewoc-location (ewoc--footer ewoc))))
                                (end
                                 (marker-position
                                  (ewoc--node-start-marker
                                   (ewoc--dll ewoc)))))
                           (list
                            (substring text (- (length text) 3))
                            (= end (1+ footer))
                            (> footer
                               (marker-position
                                (ewoc-location node))))))))"#,
            ),
            Value::list([Value::String("\n\n\n".into()), Value::T, Value::T,])
        );
    });
}

fn eshell_test_interpreter(test_file: &str) -> Interpreter {
    let mut interp = upstream_lisp_test_interpreter(&format!("eshell/{test_file}"));
    // Upstream's five-second bound targets an optimized Emacs process.  The
    // native Rust regressions intentionally run the interpreter in a much
    // larger debug test binary, with several upstream suites in parallel.
    // Keep the upstream observable-condition wait, but give that event loop
    // a debug-suite deadline instead of turning scheduler pressure into a
    // false semantic failure.  The complete all-targets gate concurrently
    // runs another large-stack interpreter and native child-process probes;
    // focused stress runs stay green but can exceed one minute per wait.
    interp.set_variable(
        "eshell-test--max-wait-time",
        Value::Integer(300),
        &mut Vec::new(),
    );
    // `with-temp-eshell' expands `ert-with-temp-directory', whose suffix
    // generator needs a source file name that string evaluation lacks
    // (GNU --eval fails identically).  A non-nil `ert-temp-file-suffix'
    // short-circuits the generator at expansion time.
    interp.set_variable(
        "ert-temp-file-suffix",
        Value::String("-emaxx".into()),
        &mut Vec::new(),
    );
    interp
}

#[test]
fn upstream_eshell_script_regressions_stay_green() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-script-tests.el");
        let selector = eval_str_with(
            &mut interp,
            "'(member em-script-test/execute-file/output-file
                      em-script-test/source-script/background)",
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));

        assert_eq!(summary.total, 2, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 2, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_eshell_unix_regressions_stay_green_in_file_order() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-unix-tests.el");
        let summary = interp.run_ert_tests_with_selector(None);

        assert_eq!(summary.total, 5, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 5, "{:#?}", interp.test_results);
    });
}

#[test]
fn upstream_eshell_command_regressions_stay_green() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("esh-cmd-tests.el");
        let selector = eval_str_with(
            &mut interp,
            "'(member esh-cmd-test/background/kill
                      esh-cmd-test/background/simple-command
                      esh-cmd-test/background/subcommand
                      esh-cmd-test/if-else-statement-lisp-form-2
                      esh-cmd-test/quoted-lisp-form
                      esh-cmd-test/which/plain/eshell-builtin)",
        );
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        let process_diagnostics = eval_str_with(
            &mut interp,
            "(list eshell-test--max-wait-time
                   eshell-process-list
                   (mapcar (lambda (process)
                             (list (process-name process)
                                   (process-status process)
                                   (process-command process)
                                   (process-thread process)))
                           (process-list))
                   (eshell-get-debug-logs))",
        );

        assert_eq!(summary.total, 6, "{:#?}", interp.test_results);
        assert_eq!(
            summary.passed, 6,
            "tests: {:#?}\nprocesses: {process_diagnostics:#?}",
            interp.test_results
        );
    });
}

#[test]
fn upstream_eshell_external_regressions_stay_green_in_file_order() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("esh-ext-tests.el");
        let summary = interp.run_ert_tests_with_selector(None);

        assert_eq!(summary.total, 5, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 5, "{:#?}", interp.test_results);
    });
}

#[test]
fn eshell_matching_input_navigation_crosses_nonsticky_prompts() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-prompt-tests.el");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (em-prompt-test/forward-backward-matching-input-1)
                   t)"
            ),
            Value::T
        );
    });
}

#[test]
fn eshell_paragraph_navigation_dynamically_inhibits_field_motion() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-prompt-tests.el");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(list
                   (special-variable-p 'inhibit-field-text-motion)
                   (progn
                     (em-prompt-test/forward-backward-paragraph-1)
                     t))"
            ),
            Value::list([Value::T, Value::T])
        );
    });
}

#[test]
fn icalendar_recurring_date_round_trip_uses_historical_local_offset() {
    let mut interp = upstream_lisp_test_interpreter("calendar/icalendar-tests.el");
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (icalendar-tests--test-cycle
                \"UID:4711\\nDTSTART;VALUE=DATE:19190909\\nDTEND;VALUE=DATE:19190910\\nRRULE:FREQ=YEARLY;INTERVAL=1;BYMONTH=09;BYMONTHDAY=09\\nSUMMARY:and diary-anniversary\\n\")
               t)"
        ),
        Value::T
    );
}

#[test]
fn todo_month_edits_observe_dynamic_prefix_argument() {
    let mut interp = upstream_lisp_test_interpreter("calendar/todo-mode-tests.el");
    let test_file = upstream_emacs_repo()
        .join("test/lisp/calendar/todo-mode-tests.el")
        .display()
        .to_string();
    interp.set_current_load_file(Some(test_file.clone()));
    // GNU's real `macroexp-file-name' reads the final file entry from
    // `current-load-list'.  Keep this direct (non-ERT-runner) probe in the
    // same macro-expansion context as the loaded upstream test body.
    interp.set_variable(
        "current-load-list",
        Value::list([Value::String(test_file.into())]),
        &mut Vec::new(),
    );
    let value = eval_str_with(
        &mut interp,
        r#"(with-todo-test
             (todo-test--show 4)
             (let ((get-date
                    (lambda ()
                      (save-excursion
                        (todo-date-string-matcher (pos-eol))
                        (buffer-substring-no-properties
                         (match-beginning 1) (match-end 0)))))
                   (dates nil)
                   (current-prefix-arg t))
               (push (funcall get-date) dates)
               (dolist (increment '(0 1 -1 -1 1 12 -12 -13 7 6 23 -23
                                    24 -24 25 -25))
                 (todo-edit-item--header 'month increment)
                 (push (funcall get-date) dates))
               (nreverse dates)))"#,
    );
    assert_eq!(
        value,
        Value::list(
            [
                "Jan 1, 2020",
                "Jan 1, 2020",
                "Feb 1, 2020",
                "Jan 1, 2020",
                "Dec 1, 2019",
                "Jan 1, 2020",
                "Jan 1, 2021",
                "Jan 1, 2020",
                "Dec 1, 2018",
                "Jul 1, 2019",
                "Jan 1, 2020",
                "Dec 1, 2021",
                "Jan 1, 2020",
                "Jan 1, 2022",
                "Jan 1, 2020",
                "Feb 1, 2022",
                "Jan 1, 2020",
            ]
            .into_iter()
            .map(|date| Value::String(date.into())),
        )
    );
}

#[test]
fn eshell_glob_completion_inserts_its_single_match() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-cmpl-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(with-temp-eshell
                     (ert-with-temp-directory default-directory :suffix "-emaxx"
                       (write-region nil nil (expand-file-name "file.txt"))
                       (write-region nil nil (expand-file-name "file.el"))
                       (eshell-insert-and-complete "echo fi*.el")))"#
            ),
            Value::String("echo file.el ".into())
        );
    });
}

#[test]
fn eshell_ambiguous_completion_displays_candidates_on_second_attempt() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-cmpl-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(with-temp-eshell
                     (ert-with-temp-directory default-directory :suffix "-emaxx"
                       (write-region nil nil (expand-file-name "file.txt"))
                       (write-region nil nil (expand-file-name "file.el"))
                       (let ((first (eshell-insert-and-complete "echo fi")))
                         (completion-at-point)
                         (list first
                               (not (null
                                     (get-buffer-window
                                      "*Completions*")))))))"#,
            ),
            Value::list([Value::String("echo file.".into()), Value::T])
        );
    });
}

#[test]
fn eshell_completes_lisp_function_names_in_forms_and_subcommands() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-cmpl-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(list
                     (with-temp-eshell
                       (eshell-insert-and-complete "echo (eshell/ech"))
                     (with-temp-eshell
                       (eshell-insert-and-complete "echo $(eshell/ech")))"#,
            ),
            Value::list([
                Value::String("echo (eshell/echo".into()),
                Value::String("echo $(eshell/echo".into()),
            ])
        );
    });
}

#[test]
fn eshell_completes_function_quoted_and_backquoted_lisp_symbols() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-cmpl-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(list
                     (with-temp-eshell
                       (eshell-insert-and-complete "echo #'system-nam"))
                     (with-temp-eshell
                       (eshell-insert-and-complete "echo `system-nam")))"#,
            ),
            Value::list([
                Value::String("echo #'system-name ".into()),
                Value::String("echo `system-name ".into()),
            ])
        );
    });
}

#[test]
fn eshell_completes_marker_buffer_references_in_all_supported_forms() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-cmpl-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(let (bufname)
                     (with-temp-buffer
                       (setq bufname (rename-buffer "my-buffer" t))
                       (list
                        (with-temp-eshell
                          (eshell-insert-and-complete
                           "echo hi > #<marker 1 my-buf"))
                        (with-temp-eshell
                          (eshell-insert-and-complete
                           "echo hi > #<marker 1 #<my-buf"))
                        (with-temp-eshell
                          (eshell-insert-and-complete
                           "echo hi > #<marker 1 #<buffer my-buf")))))"#,
            ),
            Value::list([
                Value::String("echo hi > #<marker 1 my-buffer> ".into()),
                Value::String("echo hi > #<marker 1 #<my-buffer>> ".into()),
                Value::String("echo hi > #<marker 1 #<buffer my-buffer>> ".into()),
            ])
        );
    });
}

#[test]
fn eshell_cd_can_list_files_without_replacing_last_command_metadata() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-dirs-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(let ((eshell-list-files-after-cd t))
                     (ert-with-temp-directory tmpdir :suffix "-emaxx"
                       (write-region "text" nil
                                     (expand-file-name "file.txt" tmpdir))
                       (with-temp-eshell
                         (eshell-insert-command (format "cd '%s'" tmpdir))
                         (eshell-wait-for-subprocess)
                         (list (eshell-last-output)
                               (equal default-directory tmpdir)
                               eshell-last-command-name
                               (equal eshell-last-arguments
                                      (list tmpdir))))))"#,
            ),
            Value::list([
                Value::String("file.txt\n".into()),
                Value::T,
                Value::String("#<function eshell/cd>".into()),
                Value::T,
            ])
        );
    });
}

#[test]
fn eshell_directory_module_cases_pass_in_native_runner() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-dirs-tests.el");
        let summary = interp.run_ert_tests_with_selector(None);

        assert_eq!(summary.total, 11, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 11, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn eshell_external_pipeline_finishes_redirected_output_before_returning() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-extpipe-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(ert-with-temp-file temp :suffix "-emaxx"
                     (with-temp-eshell
                       (eshell-insert-command
                        (format "echo \"bar\" *| rev >%s" temp))
                       (eshell-wait-for-subprocess)
                       (list (eshell-last-output)
                             (with-temp-buffer
                               (insert-file-contents temp)
                               (buffer-string)))))"#,
            ),
            Value::list([
                Value::String(String::new().into()),
                Value::String("rab\n".into()),
            ])
        );
    });
}

#[test]
fn eshell_external_pipeline_parser_only_shells_the_external_segment() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-extpipe-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(with-temp-eshell
                     (let ((shell-file-name "sh")
                           (shell-command-switch "-c"))
                       (prin1-to-string
                        (cadadr
                         (eshell-parse-command
                          "echo \"bar\" | rev *>temp")))))"#,
            ),
            Value::String(
                "(eshell-execute-pipeline '((eshell-named-command \"echo\" (list (eshell-escape-arg \"bar\"))) (eshell-named-command \"sh\" (list \"-c\" \"rev >temp\"))))"
                    .into()
            )
        );
    });
}

#[test]
fn eshell_internal_command_feeds_external_pipeline_before_returning() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-extpipe-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(ert-with-temp-file temp :suffix "-emaxx"
                     (with-temp-eshell
                       (eshell-insert-command
                        (format "echo \"bar\" | rev *>%s" temp))
                       (eshell-wait-for-subprocess)
                       (list (eshell-last-output)
                             (with-temp-buffer
                               (insert-file-contents temp)
                               (buffer-string)))))"#,
            ),
            Value::list([
                Value::String(String::new().into()),
                Value::String("rab\n".into()),
            ])
        );
    });
}

#[test]
fn eshell_remote_user_directory_is_not_misread_as_a_glob() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-glob-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(and (eshell-tests-remote-accessible-p)
                         (let* ((default-directory
                                  ert-remote-temporary-file-directory)
                                (remote (file-remote-p default-directory))
                                (path (format "%s~/file.txt" remote))
                                (eshell-error-if-no-glob t))
                           (equal (eshell-extended-glob path) path)))"#,
            ),
            Value::T
        );
    });
}

#[test]
fn eshell_history_module_cases_pass_in_native_runner() {
    run_exclusive_with_large_stack(|| {
        let mut interp = eshell_test_interpreter("em-hist-tests.el");
        let summary = interp.run_ert_tests_with_selector(None);

        assert_eq!(summary.total, 9, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 9, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn calc_file_cases_remain_order_independent_in_native_runner() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("calc/calc-tests.el");
        let summary = interp.run_ert_tests_with_selector(None);

        assert_eq!(summary.total, 25, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 25, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn ert_source_diagnostic_loop_evaluates_function_alias_chain() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize batch interpreter");
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("lisp/emacs-lisp/ert.el"),
        )
        .expect("load upstream ERT source");

        assert_eq!(
            eval_str_with(&mut interp, "(ert--get-explainer 'equal)"),
            Value::symbol("ert--explain-equal")
        );
    });
}

#[test]
fn comint_password_function_survives_the_temporary_process_buffer() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("comint-tests.el");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(progn
                     (defun emaxx-comint-password-roundtrip
                         (password-function)
                       (let ((comint-password-function password-function))
                         (with-temp-buffer
                           (cl-letf (((symbol-function 'read-passwd)
                                      (lambda (&rest _args) "fallback")))
                             (comint-mode)
                             (let ((process
                                    (make-pipe-process
                                     :name "emaxx-comint-binding"
                                     :buffer (current-buffer)
                                     :noquery t))
                                   sent)
                               (unwind-protect
                                   (let ((comint-input-sender
                                          (lambda (_process string)
                                            (setq sent string))))
                                     (comint-send-invisible "Password: ")
                                     sent)
                                 (delete-process process)))))))
                     (emaxx-comint-password-roundtrip
                      (lambda (&rest _args) "alternate")))"#,
            ),
            Value::String("alternate".into())
        );
    });
}

#[test]
fn erc_process_input_line_preserves_spaces_in_the_flood_queue() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize ERC batch interpreter");
        interp
            .load_target("lisp-mode")
            .expect("load GNU dumped lisp-mode contract");
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("test/lisp/erc/resources/erc-tests-common.el"),
        )
        .expect("load focused ERC test helper");
        let queued = eval_str_with(
            &mut interp,
            r##"(with-temp-buffer
                 (rename-buffer " *temp*-erc-process-input" t)
                 (let ((server (erc-tests-common-make-server-buf)))
                   (unwind-protect
                       (progn
                         (setq erc-server-current-nick "tester")
                         (with-current-buffer (erc--open-target "#chan")
                           ;; The upstream helper's fake server is `sleep 1'.
                           ;; Under a saturated Rust suite it can exit before
                           ;; this focused queue assertion reaches ERC.  Keep
                           ;; process scheduling outside this string-shape
                           ;; regression while retaining the real queue path.
                           (cl-letf (((symbol-function
                                      'erc-server-process-alive)
                                     (lambda () t))
                                    ((symbol-function
                                      'erc-server-send-queue)
                                     #'ignore))
                             (erc-process-input-line
                              "/msg #chan hi you\n")
                             (erc-with-server-buffer
                               (pop erc-server-flood-queue)))))
                     (when (process-live-p erc-server-process)
                       (delete-process erc-server-process))
                     (when (get-buffer "#chan")
                       (kill-buffer "#chan")))))"##,
        );
        assert_eq!(
            queued,
            Value::cons(
                Value::String("PRIVMSG #chan :hi you\r\n".into()),
                Value::symbol("utf-8"),
            )
        );
    });
}

#[test]
fn dabbrev_cross_buffer_cases_remain_order_independent_in_native_runner() {
    run_with_large_stack(|| {
        let mut interp = upstream_lisp_test_interpreter("dabbrev-tests.el");
        let expected_passes = [
            "dabbrev-expand-after-killing-buffer",
            "dabbrev-expand-test-minibuffer-4",
            "dabbrev-expand-test-other-buffer-1",
            "dabbrev-expand-test-other-buffer-2",
            "dabbrev-expand-test-other-buffer-3",
            "dabbrev-expand-test-other-buffer-4",
        ];
        let mut test_names = interp
            .discovered_tests()
            .into_iter()
            .map(|test| test.name)
            .collect::<Vec<_>>();
        test_names.sort();

        assert_eq!(test_names.len(), 16);
        for name in test_names {
            let summary =
                interp.run_ert_tests_with_selector(Some(&Value::Symbol(name.clone().into())));
            assert_eq!(summary.total, 1, "{name}: {:#?}", interp.test_results);
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(list (buffer-live-p (window-buffer (selected-window)))\
                           (buffer-live-p (window-buffer (minibuffer-window))))",
                ),
                Value::list([Value::T, Value::T]),
                "{name} left a window displaying a dead buffer"
            );
            if expected_passes.contains(&name.as_str()) {
                let outcome = &interp.test_results[0];
                assert!(
                    matches!(outcome.status, crate::compat::TestStatus::Passed),
                    "{name}: {outcome:#?}"
                );
            }
        }
    });
}

#[test]
fn killing_a_noncurrent_buffer_does_not_retarget_the_selected_window() {
    assert_eq!(
        eval_str(
            r#"(let ((displayed (get-buffer-create " *displayed*"))
                     (current (get-buffer-create " *current*"))
                     (victim (get-buffer-create " *victim*")))
                 (unwind-protect
                     (progn
                       (set-window-buffer nil displayed)
                       (set-buffer current)
                       (kill-buffer victim)
                       (eq (window-buffer) displayed))
                   (kill-buffer displayed)
                   (kill-buffer current)
                   (kill-buffer victim)))"#,
        ),
        Value::T
    );
}
