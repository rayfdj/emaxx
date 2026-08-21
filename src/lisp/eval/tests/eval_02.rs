use super::*;

#[test]
fn define_derived_mode_creates_the_complete_mode_state_contract() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "derived",
            "(progn
                   (define-derived-mode sample-derived-mode fundamental-mode \"Sample\")
                   (list (keymapp sample-derived-mode-map)
                         (eq (char-table-subtype
                              sample-derived-mode-syntax-table)
                             'syntax-table)
                         (abbrev-table-p sample-derived-mode-abbrev-table)
                         (special-variable-p 'sample-derived-mode-hook)
                         (special-variable-p 'sample-derived-mode-map)
                         (special-variable-p 'sample-derived-mode-syntax-table)
                         (special-variable-p 'sample-derived-mode-abbrev-table)
                         (with-temp-buffer
                           (sample-derived-mode)
                           (list (eq (syntax-table)
                                     sample-derived-mode-syntax-table)
                                 (eq local-abbrev-table
                                     sample-derived-mode-abbrev-table)))))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::list([Value::T, Value::T]),
        ])
    );
}

#[test]
fn define_derived_mode_preserves_a_predefined_abbrev_table() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "derived",
            "(progn
               (require 'abbrev)
               (define-abbrev-table 'sample-parent-abbrev-table
                 '((\"kw\" \"keyword\")))
               (define-abbrev-table 'sample-preserved-mode-abbrev-table nil
                 :parents (list sample-parent-abbrev-table))
               (define-derived-mode sample-preserved-mode fundamental-mode
                 \"Preserved\")
               (list
                (eq sample-parent-abbrev-table
                    (car (abbrev-table-get
                          sample-preserved-mode-abbrev-table :parents)))
                (abbrev-expansion
                 \"kw\" sample-preserved-mode-abbrev-table)))"
        ),
        Value::list([Value::T, Value::String("keyword".into())])
    );
}

#[test]
fn loaded_derived_mode_owner_wires_parent_mode_tables_at_activation() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "derived",
            r#"(progn
                 (define-derived-mode sample-parent-mode fundamental-mode "Parent"
                   (modify-syntax-entry ?% "<"))
                 (define-derived-mode sample-child-mode sample-parent-mode "Child")
                 (with-temp-buffer
                   (sample-child-mode)
                   (insert "x % comment\ny")
                   (list
                    (featurep 'derived)
                    (eq (char-table-parent sample-child-mode-syntax-table)
                        sample-parent-mode-syntax-table)
                    (eq (keymap-parent sample-child-mode-map)
                        sample-parent-mode-map)
                    (eq (car (abbrev-table-get
                              sample-child-mode-abbrev-table :parents))
                        sample-parent-mode-abbrev-table)
                    (progn
                      (goto-char 8)
                      (nth 4 (syntax-ppss))))))"#,
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn source_loaded_tex_mode_inherits_its_parent_syntax_table() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (require 'tex-mode)
                 (with-temp-buffer
                   (latex-mode)
                   (insert "x % \\cite{ignored}\ny")
                   (list
                    ;; Compiled tex-mode expands `define-derived-mode' at
                    ;; compile time and never loads derived.el at runtime;
                    ;; GNU batch reports (featurep 'derived) => nil here.
                    (featurep 'derived)
                    (eq (char-table-parent latex-mode-syntax-table)
                        tex-mode-syntax-table)
                    (eq (car (syntax-after 3)) 11)
                    (progn
                      (goto-char 10)
                      (nth 4 (syntax-ppss))))))"#,
        ),
        Value::list([Value::Nil, Value::T, Value::T, Value::T])
    );
}

#[test]
fn eager_define_derived_mode_lowering_replaces_its_search_stub_with_the_real_mode() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "derived",
            "(progn
               (eval (macroexpand
                      '(define-derived-mode sample-eager-mode fundamental-mode
                         \"Eager\"
                         (setq-local sample-eager-body-ran t))))
               (with-temp-buffer
                 (sample-eager-mode)
                 (list major-mode
                       (derived-mode-p 'sample-eager-mode)
                       sample-eager-body-ran)))"
        ),
        Value::list([
            Value::Symbol("sample-eager-mode".into()),
            Value::Symbol("sample-eager-mode".into()),
            Value::T,
        ])
    );
}

#[test]
fn real_outline_library_loads_over_the_runtime_keymap_list_adapter() {
    let emacs_repo = upstream_emacs_repo();
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&emacs_repo).expect("upstream load path"),
    );
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    crate::lisp::load_file_strict(&mut interp, &emacs_repo.join("lisp/outline.el"))
        .expect("load real outline");

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list (featurep 'outline)
                   (boundp 'outline-mode-syntax-table)
                   (eq (char-table-subtype outline-mode-syntax-table)
                       'syntax-table)
                   (keymapp outline-minor-mode-menu-bar-map))"
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn preloaded_fundamental_mode_runs_the_elisp_owned_reset_lifecycle() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (defvar-local emaxx-test-fundamental-reset nil)
               (with-temp-buffer
                 (setq-local emaxx-test-fundamental-reset 'stale)
                 (fundamental-mode)
                 (list major-mode
                       mode-name
                       emaxx-test-fundamental-reset
                       (local-variable-p 'emaxx-test-fundamental-reset))))",
        ),
        Value::list([
            Value::symbol("fundamental-mode"),
            Value::String("Fundamental".into()),
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn dumped_word_motion_and_auto_fill_controls_are_complete() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
                (with-temp-buffer
                  (insert "alpha beta")
                  (list
                   (progn (goto-char (point-min))
                          (forward-word-strictly)
                          (point))
                   (progn (goto-char (point-max))
                          (backward-word-strictly)
                          (point))
                   (progn (turn-on-auto-fill) auto-fill-function)
                   (progn (turn-off-auto-fill) auto-fill-function)
                   (progn (abbrev-mode 1) abbrev-mode)
                   (progn (abbrev-mode -1) abbrev-mode)
                   (char-table-p word-move-empty-char-table)))
                "#,
        ),
        Value::list([
            Value::Integer(6),
            Value::Integer(7),
            Value::Symbol("do-auto-fill".into()),
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::T,
        ])
    );
}

#[test]
fn forward_word_uses_boundary_functions_and_reports_buffer_edges_like_gnu() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
              (progn
                (fset 'emaxx-test-word-boundary
                      (lambda (pos limit)
                        (if (< pos limit)
                            (min (+ pos 3) limit)
                          (max (- pos 2) limit))))
                (with-temp-buffer
                  (insert "fooBar baz")
                  (let ((table (make-char-table nil nil)))
                    (set-char-table-range table t 'emaxx-test-word-boundary)
                    (setq-local find-word-boundary-function-table table)
                    (list
                     (progn (goto-char 1) (list (forward-word) (point)))
                     (list (forward-word) (point))
                     (list (forward-word) (point))
                     (list (forward-word) (point))
                     (progn (goto-char (point-max))
                            (list (forward-word -1) (point)))
                     (progn (goto-char (point-min))
                            (list (forward-word -1) (point)))
                     (progn (goto-char (point-min))
                            (list (forward-word nil) (point)))))))
              "#,
        ),
        Value::list([
            Value::list([Value::T, Value::Integer(4)]),
            Value::list([Value::T, Value::Integer(7)]),
            Value::list([Value::T, Value::Integer(11)]),
            Value::list([Value::Nil, Value::Integer(11)]),
            Value::list([Value::T, Value::Integer(8)]),
            Value::list([Value::Nil, Value::Integer(1)]),
            Value::list([Value::T, Value::Integer(4)]),
        ])
    );
}

#[test]
fn strict_word_motion_bypasses_mode_specific_boundary_functions() {
    let mut interp = Interpreter::new();
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
              (progn
                (fset 'emaxx-test-word-boundary
                      (lambda (pos limit)
                        (if (< pos limit)
                            (min (+ pos 3) limit)
                          (max (- pos 2) limit))))
                (with-temp-buffer
                  (insert "fooBar")
                  (let ((table (make-char-table nil nil)))
                    (set-char-table-range table t 'emaxx-test-word-boundary)
                    (setq-local find-word-boundary-function-table table)
                    (goto-char (point-min))
                    (list (forward-word-strictly) (point)))))
              "#,
        ),
        Value::list([Value::T, Value::Integer(7)])
    );
}

#[test]
fn word_boundary_table_is_special_across_lexical_function_calls() {
    // `setq-local' expands through preloaded macroexp.el in GNU's dump, so
    // the reconstructed batch image is the honest runtime here.
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
              (progn
                (fset 'emaxx-test-ordinary-word-motion
                      (eval '(lambda (position)
                               (save-excursion
                                 (goto-char position)
                                 (forward-word 1)
                                 (point)))
                            t))
                (fset 'emaxx-test-reentrant-boundary
                      (eval '(lambda (position _limit)
                               (let ((find-word-boundary-function-table
                                      (make-char-table nil nil)))
                                 (emaxx-test-ordinary-word-motion position)))
                            t))
                (with-temp-buffer
                  (insert "fooBar")
                  (let ((table (make-char-table nil nil)))
                    (set-char-table-range table t 'emaxx-test-reentrant-boundary)
                    (setq-local find-word-boundary-function-table table)
                    (goto-char (point-min))
                    (list
                     (special-variable-p 'find-word-boundary-function-table)
                     (forward-word)
                     (point)))))
              "#,
        ),
        Value::list([Value::T, Value::T, Value::Integer(7)])
    );
}

#[test]
fn cl_defstruct_generates_constructor_accessors_and_setf() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
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
        eval_str_with_upstream_batch_feature(
            "cl-macs",
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
fn cl_defstruct_honors_explicit_predicate_name() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(progn
                   (cl-defstruct (sample-pred
                                  (:constructor nil)
                                  (:predicate sample-pred-object-p)
                                  (:constructor make-sample-pred (value)))
                     value)
                   (let ((sample (make-sample-pred 42)))
                     (list
                      (fboundp 'sample-pred-p)
                      (sample-pred-object-p sample)
                      (sample-pred-value sample))))"
        ),
        Value::list([Value::Nil, Value::T, Value::Integer(42)])
    );
}

#[test]
fn cl_defstruct_type_list_accessors_accept_nil_and_lists() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(progn
                   (cl-defstruct (sample-list-struct
                                  (:type list)
                                  (:predicate nil))
                     alpha beta)
                   (list (sample-list-struct-alpha nil)
                         (sample-list-struct-alpha '(left right))
                         (sample-list-struct-beta '(left right))))"
        ),
        Value::list([
            Value::Nil,
            Value::Symbol("left".into()),
            Value::Symbol("right".into()),
        ])
    );
}

#[test]
fn cl_getf_places_update_plists() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-extra",
            "(progn
               (let ((plist '(x 1 y)))
                 (list
                  (cl-incf (cl-getf plist 'x 10) 2)
                  plist
                  (cl-incf (cl-getf plist 'y 10) 4)
                  plist)))"
        ),
        Value::list([
            Value::Integer(3),
            Value::list([
                Value::Symbol("x".into()),
                Value::Integer(3),
                Value::Symbol("y".into()),
            ]),
            Value::Integer(14),
            Value::list([
                Value::Symbol("y".into()),
                Value::Integer(14),
                Value::Symbol("x".into()),
                Value::Integer(3),
                Value::Symbol("y".into()),
            ]),
        ])
    );
}

#[test]
fn cl_getf_handles_malformed_plists_like_cl_extra() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-extra",
            "(progn
               (let ((plist '(x 1 y . 2)))
                 (list
                  (cl-getf plist 'x)
                  (cl-incf (cl-getf plist 'x 10) 2)
                  plist
                  (condition-case err
                      (cl-getf plist 'y :none)
                    (wrong-type-argument (car err)))
                  (condition-case err
                      (cl-getf plist 'z :none)
                    (wrong-type-argument (car err))))))"
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(3),
            Value::cons(
                Value::Symbol("x".into()),
                Value::cons(
                    Value::Integer(3),
                    Value::cons(Value::Symbol("y".into()), Value::Integer(2)),
                ),
            ),
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol("wrong-type-argument".into()),
        ])
    );
}

#[test]
fn cl_defstruct_constructor_respects_optional_marker() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
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
        eval_str_with_upstream_batch_feature(
            "cl-macs",
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
        eval_str_with_upstream_batch_feature(
            "cl-macs",
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
fn cl_defstruct_constructor_aux_can_reference_constructor_args() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(progn
               (cl-defstruct (aux-arg-struct
                              (:constructor make-aux-arg-struct
                                            (value &aux
                                                   (integer (integerp value))
                                                   (values (unless integer
                                                             (list value)))
                                                   (range (when integer
                                                            `((,value . ,value)))))))
                 integer values range)
               (let ((from-int (make-aux-arg-struct 3))
                     (from-symbol (make-aux-arg-struct 'alpha)))
                 (list (aux-arg-struct-integer from-int)
                       (aux-arg-struct-range from-int)
                       (aux-arg-struct-integer from-symbol)
                       (aux-arg-struct-values from-symbol))))"
        ),
        Value::list([
            Value::T,
            Value::list([Value::cons(Value::Integer(3), Value::Integer(3))]),
            Value::Nil,
            Value::list([Value::Symbol("alpha".into())]),
        ])
    );
}

#[test]
fn cl_defstruct_named_constructors_keep_default_constructor() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(progn
               (cl-defstruct (multi-ctor-struct
                              (:constructor multi-ctor-from-value (value)))
                 value)
               (list
                (fboundp 'make-multi-ctor-struct)
                (multi-ctor-struct-value
                 (make-multi-ctor-struct :value 'default))
                (multi-ctor-struct-value
                 (multi-ctor-from-value 'named))))"
        ),
        Value::list([Value::T, Value::symbol("default"), Value::symbol("named"),])
    );
}

#[test]
fn cl_defstruct_constructor_arglists_ignore_aux_bindings() {
    assert_eq!(
        eval_str_with_upstream_batch_features(
            &["cl-macs", "pcase"],
            "(progn
               (cl-defstruct (arglist-struct
                              (:constructor make-arglist-empty (&aux (abc 1)))
                              (:constructor make-arglist-optional (&optional def)))
                 (abc 5) def)
               (list
                (help-function-arglist 'make-arglist-empty)
                (pcase (help-function-arglist 'make-arglist-optional)
                  (`(&optional ,_) t))))"
        ),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn loaded_gnu_help_reads_real_cl_struct_constructor_arglists() {
    run_with_large_stack(|| {
        // The subject is Help/CL behavior after startup, not source loading.
        // Use the same real GNU owners through the reconstructed compiled
        // batch image instead of rebuilding unrelated bootstrap Lisp here.
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        eval_str_with(&mut interp, "(require 'cl-macs)");
        eval_str_with(&mut interp, "(require 'pcase)");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                (progn
                  (require 'help)
                  (cl-defstruct
                      (loaded-arglist-struct
                       (:constructor loaded-arglist-empty (&aux (abc 1)))
                       (:constructor loaded-arglist-optional (&optional def)))
                    (abc 5)
                    def)
                  (list
                   (help-function-arglist 'loaded-arglist-empty)
                   (pcase (help-function-arglist 'loaded-arglist-optional)
                     (`(&optional ,_) t))
                   (loaded-arglist-struct-abc (loaded-arglist-empty))
                   (loaded-arglist-struct-def
                    (loaded-arglist-optional 'value))))"#
            ),
            Value::list([
                Value::Nil,
                Value::T,
                Value::Integer(1),
                Value::Symbol("value".into()),
            ])
        );
    });
}

#[test]
fn abbrev_expansion_respects_table_props_and_parent_tables() {
    assert_eq!(
        eval_str_with_upstream_batch(
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
    let mut interp = gnu_early_lisp_interpreter();
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
    let mut interp = gnu_early_lisp_interpreter();
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
        Value::String(expected.into())
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
    assert_eq!(eval_str(&expr), Value::String(expected.into()));
}

#[test]
fn custom_current_group_alist_defaults_to_nil() {
    assert_eq!(eval_str("custom-current-group-alist"), Value::Nil);
}

#[test]
fn emacs_lisp_mode_syntax_table_is_the_elisp_specific_child() {
    // GNU's table inherits the Lisp-data punctuation entries but removes the
    // generic prefix flag from `@'; syntax-propertize restores it for `,@'.
    assert_eq!(
        eval_str("emacs-lisp-mode-syntax-table"),
        Value::CharTable(4)
    );
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (set-syntax-table (copy-syntax-table emacs-lisp-mode-syntax-table))
               (list (char-syntax ?.) (char-syntax ?@)))"
        ),
        Value::list([Value::Integer('_' as i64), Value::Integer('_' as i64),])
    );
}

#[test]
fn emacs_lisp_mode_map_defaults_to_keymap() {
    // lisp-mode.el owns `emacs-lisp-mode-map'; GNU has it because the map is
    // in the dumped image, not because C creates one.
    assert_eq!(
        eval_str_with_upstream_batch("(keymapp emacs-lisp-mode-map)"),
        Value::T
    );
}

#[test]
fn cl_loop_supports_across_with_unbounded_from() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
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
fn byte_compile_symbol_preserves_function_attributes() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
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
    let result = eval_str_with_upstream_batch_feature(
        "bytecomp",
        r#"
                (progn
                  (require 'subr-x)
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
                        (not (null (member '(byte-constant 111) lap)))
                        (not (null (member '(byte-constant 222) lap)))
                        (member '(byte-constant 333) lap)
                        (not (null (member '(byte-constant 444) lap)))
                        (let* ((bc2 (byte-compile
                                     '(lambda (x)
                                        (cond ((eql x #x10000000000000000) 111)
                                              ((eql x #x10000000000000001) 222)
                                              ((eql x #x10000000000000000) 333)
                                              ((eql x #x10000000000000002) 444)))))
                               (lap2 (byte-decompile-bytecode (aref bc2 1) (aref bc2 2)))
                               (table2 (cadr (assq 'byte-constant lap2))))
                          (mapcar #'numberp (hash-table-keys table2))))))
                "#,
    );
    assert_eq!(
        result,
        Value::list([
            Value::T,
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ]),
            Value::T,
            Value::T,
            Value::Nil,
            Value::T,
            Value::list([Value::T, Value::T, Value::T]),
        ])
    );
}

#[test]
fn byte_compile_warns_for_malformed_defcustom_types() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (progn
                  (defun test--bytecomp-defcustom-type-matches-p (pattern form)
                    (with-current-buffer (get-buffer-create "*Compile-Log*")
                      (let ((inhibit-read-only t))
                        (erase-buffer)))
                    (let ((text-quoting-style 'grave)
                          (macroexp--warned
                           (make-hash-table :test #'equal :weakness 'key)))
                      (byte-compile form))
                    (with-current-buffer "*Compile-Log*"
                      (not (null (re-search-forward pattern nil t)))))
                  (mapcar
                   (lambda (case)
                     (test--bytecomp-defcustom-type-matches-p (car case) (cadr case)))
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
fn byte_compile_warning_logging_preserves_the_callers_buffer_point() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (with-current-buffer (get-buffer-create "*Compile-Log*")
                    (let ((inhibit-read-only t))
                      (erase-buffer))
                    (byte-compile
                     '(defcustom test--malformed-custom nil "doc"
                        :type ''integer :group 'test))
                    (list (= (point) (point-min))
                          (equal byte-compile-log-buffer "*Compile-Log*")
                          (not (null
                                (string-match "type should not be quoted"
                                              (buffer-string))))))
                "#,
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn byte_compile_warns_for_missing_defcustom_type_and_group() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (progn
                  (defun test--bytecomp-defcustom-warning-p (pattern form)
                    (with-current-buffer (get-buffer-create "*Compile-Log*")
                      (let ((inhibit-read-only t))
                        (erase-buffer)))
                    (let ((text-quoting-style 'grave)
                          (macroexp--warned
                           (make-hash-table :test #'equal :weakness 'key)))
                      (byte-compile form))
                    (with-current-buffer "*Compile-Log*"
                      (not (null (re-search-forward pattern nil t)))))
                  (list
                   (test--bytecomp-defcustom-warning-p
                    "fails to specify containing group"
                    '(defcustom mytest nil "doc" :type 'boolean))
                   (test--bytecomp-defcustom-warning-p
                    "missing :type keyword parameter"
                    '(defcustom mytest nil "doc" :group 'test))))
                "#
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn byte_compile_warns_for_extra_format_arguments() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (progn
                  (with-current-buffer (get-buffer-create "*Compile-Log*")
                    (let ((inhibit-read-only t))
                      (erase-buffer)))
                  (byte-compile '(message "%s" 1 2))
                  (with-current-buffer "*Compile-Log*"
                    (not (null (re-search-forward
                                "called with 2 arguments to fill 1 format field"
                                nil t)))))
                "#
        ),
        Value::T
    );
}

#[test]
fn byte_compile_warns_for_free_vars_and_interactive_only_forms() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (progn
                  (defun test--bytecomp-warning-p (pattern form)
                    (with-current-buffer (get-buffer-create "*Compile-Log*")
                      (let ((inhibit-read-only t))
                        (erase-buffer)))
                    (let ((text-quoting-style 'grave)
                          (macroexp--warned
                           (make-hash-table :test #'equal :weakness 'key)))
                      (byte-compile form))
                    (with-current-buffer "*Compile-Log*"
                      (not (null (re-search-forward pattern nil t)))))
                  (defvar test--bytecomp-obsolete-var nil)
                  (make-obsolete-variable 'test--bytecomp-obsolete-var nil "31.1")
                  (list
                   (test--bytecomp-warning-p
                    "free.*foo"
                    '(setq foo 'bar))
                   (test--bytecomp-warning-p
                    "free variable .bar"
                    '(defun sample-free-ref () bar))
                   (test--bytecomp-warning-p
                    "make-variable-buffer-local. not called at toplevel"
                    '(defun sample-buffer-local () (make-variable-buffer-local 'foobar)))
                   (test--bytecomp-warning-p
                    "next-line.*interactive use only.*forward-line"
                    '(defun sample-next-line () (next-line)))
                   (test--bytecomp-warning-p
                    "malformed .interactive. specification"
                    '(defun sample-bad-interactive ()
                       (interactive "foo" "bar")))
                   (test--bytecomp-warning-p
                    "test--bytecomp-obsolete-var.*obsolete variable.*31.1"
                    '(defun sample-obsolete-var ()
                       test--bytecomp-obsolete-var))
                   (test--bytecomp-warning-p
                    "with-current.*rather than save-excursion"
                    '(defun sample-set-buffer ()
                       (save-excursion
                         (set-buffer (current-buffer)))))
                   (test--bytecomp-warning-p
                    "let-bind constant"
                    '(defun sample-let-constant ()
                       (let ((t 1)) t)))
                   (test--bytecomp-warning-p
                    "let-bind nonvariable"
                    '(defun sample-let-nonvariable ()
                       (let (('t 1)) t)))
                   (test--bytecomp-warning-p
                    "attempt to set constant"
                    '(defun sample-set-constant ()
                       (setq t nil)))
                   (test--bytecomp-warning-p
                    "attempt to set non-variable"
                    '(defun sample-set-nonvariable ()
                       (setq (a) nil)))
                   (test--bytecomp-warning-p
                    "odd number of arguments"
                    '(defun sample-setq-odd (a b)
                       (setq a 1 b)))))
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
            Value::T
        ])
    );
}

#[test]
fn byte_compile_suppresses_prefixless_defvar_warning() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (progn
                  (with-current-buffer (get-buffer-create "*Compile-Log*")
                    (let ((inhibit-read-only t))
                      (erase-buffer)))
                  (byte-compile '(with-suppressed-warnings ((lexical prefixless))
                                   (defvar prefixless)))
                  (and
                   (with-current-buffer "*Compile-Log*"
                     (not (string-match "global/dynamic var .prefixless. lacks"
                                        (buffer-string))))
                   (equal (byte-compile '(defvar prefixless))
                          (byte-compile '(with-suppressed-warnings ((lexical prefixless))
                                           (defvar prefixless))))))
                "#
        ),
        Value::T
    );
}

#[test]
fn byte_compile_warning_suppression_accepts_positioned_symbols_like_gnu() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"(let* ((symbols-with-pos-enabled t)
                      (warning (read-positioning-symbols "lexical"))
                      (subject (read-positioning-symbols "prefixless"))
                      (byte-compile--suppressed-warnings
                       (list (list warning subject))))
                 (list (eq warning 'lexical)
                       (not (null (memq subject '(prefixless))))
                       (byte-compile-warning-enabled-p 'lexical subject)))"#,
        ),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn upstream_byte_compiler_publishes_gnu_compiler_state_variables() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            "(list (boundp 'byte-compile-unresolved-functions) \
                   byte-compile-unresolved-functions)",
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn byte_compile_records_unresolved_calls_for_individual_lambdas() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (let ((byte-compile-unresolved-functions nil)
                      (byte-compile-log-buffer
                       (generate-new-buffer " *Compile-Log*")))
                  (byte-compile '(lambda () (test--missing-bytecomp-function)))
                  (list
                   (not (null (assq 'test--missing-bytecomp-function
                                    byte-compile-unresolved-functions)))
                   (with-current-buffer byte-compile-log-buffer
                     (= (buffer-size) 0))))
                "#
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn byte_compile_records_unresolved_calls_introduced_by_macroexpansion() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (progn
                  (defmacro test--bytecomp-missing-expander ()
                    '(test--missing-after-macroexpansion))
                  (let ((byte-compile-unresolved-functions nil)
                        (byte-compile-log-buffer
                         (generate-new-buffer " *Compile-Log*")))
                    (byte-compile '(lambda ()
                                     (test--bytecomp-missing-expander)))
                    (list
                     (not (null (assq 'test--missing-after-macroexpansion
                                      byte-compile-unresolved-functions)))
                     (with-current-buffer byte-compile-log-buffer
                       (= (buffer-size) 0)))))
                "#
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn byte_compile_records_unresolved_calls_in_function_quoted_nested_lambdas() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (let ((byte-compile-unresolved-functions nil)
                      (byte-compile-log-buffer
                       (generate-new-buffer " *Compile-Log*")))
                  (byte-compile
                   '(lambda ()
                      (let ((worker
                             #'(lambda ()
                                 (test--missing-in-nested-bytecomp-lambda))))
                        (funcall worker))))
                  (list
                   (not (null (assq 'test--missing-in-nested-bytecomp-lambda
                                    byte-compile-unresolved-functions)))
                   (with-current-buffer byte-compile-log-buffer
                     (= (buffer-size) 0))))
                "#
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn byte_compile_keeps_ordinary_macroexpansion_messages_out_of_compile_log() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (progn
                  (defmacro test--bytecomp-message-expander ()
                    (message "Warning: generated macro diagnostic")
                    t)
                  (let ((byte-compile-log-buffer
                         (generate-new-buffer " *Compile-Log*"))
                        (messages (get-buffer-create "*Messages*")))
                    (with-current-buffer messages
                      (let ((inhibit-read-only t))
                        (erase-buffer)))
                    (byte-compile
                     '(lambda () (test--bytecomp-message-expander)))
                    (list
                     (with-current-buffer byte-compile-log-buffer
                       (= (buffer-size) 0))
                     (with-current-buffer messages
                       (not (null
                             (string-match "Warning: generated macro diagnostic"
                                           (buffer-string))))))))
                "#
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn byte_compile_from_buffer_warns_for_unresolved_calls_outside_feature_guards() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*")))
                      (with-temp-buffer
                    (insert "\(defun foo ()\n"
                            "  (an-undefined-function))\n"
                            "\(defun foo1 ()\n"
                            "  (if (featurep 'xemacs)\n"
                            "      (some-undefined-function-if)))\n"
                            "\(defun foo2 ()\n"
                            "  (and (featurep 'xemacs)\n"
                            "       (some-undefined-function-and)))\n"
                            "\(defun foo3 ()\n"
                            "  (if (not (featurep 'emacs))\n"
                            "      (some-undefined-function-not)))\n"
                            "\(defun foo4 ()\n"
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
                (list (not (null
                            (string-match
                             "global/dynamic var .prefixless. lacks" warn-log)))
                      (with-current-buffer byte-compile-log-buffer
                        (= (buffer-size) 0))
                      (file-exists-p warn-dest)
                      (file-exists-p suppressed-dest))))
            "#,
        warn_src = warn_src.display().to_string(),
        warn_dest = warn_dest.display().to_string(),
        suppressed_src = suppressed_src.display().to_string(),
        suppressed_dest = suppressed_dest.display().to_string(),
    );

    let result = eval_str_with_upstream_batch_feature("bytecomp", &source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn byte_compile_file_loads_macro_expanded_function_bodies() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-macroexpand-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let source_path = dir.join("macroexpand.el");
    let dest_path = dir.join("macroexpand.elc");
    std::fs::write(
        &source_path,
        ";;; -*- lexical-binding: t -*-\n(eval-and-compile\n  (defmacro sample-compile-macro () -7)\n  (defun sample-compile-def () (sample-compile-macro)))\n",
    )
    .unwrap();

    let source = format!(
        r#"
            (let ((byte-compile-dest-file-function (lambda (_) {dest_path:?})))
              (byte-compile-file {source_path:?})
              (load {dest_path:?} nil 'nomessage)
              (sample-compile-def))
            "#,
        source_path = source_path.display().to_string(),
        dest_path = dest_path.display().to_string(),
    );

    let result = eval_str_with_upstream_batch_feature("bytecomp", &source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(result, Value::Integer(-7));
}

#[test]
fn package_upgrade_reloads_previously_loaded_library_before_compiling() {
    run_with_large_stack(|| {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(format!("emaxx-package-reload-{unique}"));
        let old_dir = root.join("reload-sample-1.0");
        let new_dir = root.join("reload-sample-2.0");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        std::fs::write(
            old_dir.join("reload-sample-aux.el"),
            ";;; -*- lexical-binding: t -*-\n(defun reload-sample-aux-1 (&rest forms) \"Description\" `(progn ,@forms))\n(provide 'reload-sample-aux)\n",
        )
        .unwrap();
        std::fs::write(
            old_dir.join("reload-sample.el"),
            ";;; -*- lexical-binding: t -*-\n(require 'reload-sample-aux)\n(defmacro reload-sample-1 (&rest forms) \"Description\" `(progn ,@forms))\n(defun reload-sample-value () \"\" (reload-sample-1 'a 'b) (reload-sample-aux-1 'a 'b))\n(provide 'reload-sample)\n",
        )
        .unwrap();
        std::fs::write(
            new_dir.join("reload-sample-aux.el"),
            ";;; -*- lexical-binding: t -*-\n(defmacro reload-sample-aux-1 (&rest forms) \"Description\" `(progn ,@forms))\n(provide 'reload-sample-aux)\n",
        )
        .unwrap();
        std::fs::write(
            new_dir.join("reload-sample.el"),
            ";;; reload-sample.el --- package reload test -*- lexical-binding: t -*-\n;; Version: 2.0\n;; Keywords: tools\n;;; Code:\n(require 'reload-sample-aux)\n(defmacro reload-sample-1 (&rest forms) \"Description\" `(progn ,(cadr (car forms))))\n(defun reload-sample-value () \"\" (list (reload-sample-1 '1 'b) (reload-sample-aux-1 'a 'b)))\n(provide 'reload-sample)\n;;; reload-sample.el ends here\n",
        )
        .unwrap();
        // GNU's package owner runs inside the dumped batch image.  Reconstruct
        // that image from the real GNU loadup sequence before requiring it.
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let result = eval_str_with(
            &mut interp,
            &format!(
                r#"
                (progn
                  (require 'package)
                  (let ((load-path (cons {old_dir:?} load-path)))
                    (byte-recompile-directory {old_dir:?} 0 t)
                    (delete-file {old_aux:?})
                    (delete-file {old_main:?})
                    (load "reload-sample")
                    (let ((before (reload-sample-value))
                          (package-user-dir {package_user_dir:?})
                          package--initialized
                          package-alist
                          package-selected-packages)
                      (package-install-file {new_dir:?})
                      (list before (reload-sample-value)))))
                "#,
                old_dir = old_dir.display().to_string(),
                old_aux = old_dir.join("reload-sample-aux.el").display().to_string(),
                old_main = old_dir.join("reload-sample.el").display().to_string(),
                new_dir = new_dir.display().to_string(),
                package_user_dir = root.join("packages").display().to_string(),
            ),
        );

        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(
            result,
            Value::list([
                Value::list([
                    Value::Symbol("progn".into()),
                    Value::Symbol("a".into()),
                    Value::Symbol("b".into())
                ]),
                Value::list([Value::Integer(1), Value::Symbol("b".into())])
            ])
        );
    });
}

#[test]
fn cl_macrolet_expands_defun_body_before_local_macro_exits() {
    let result = eval_str_with_upstream_batch_feature(
        "cl-macs",
        r#"
        (progn
          (cl-macrolet ((sample-cl-macrolet-macro () 4))
            (defmacro sample-cl-macrolet-macro () 5)
            (defun sample-cl-macrolet-def () (sample-cl-macrolet-macro)))
          (sample-cl-macrolet-def))
        "#,
    );

    assert_eq!(result, Value::Integer(4));
}

#[test]
fn defmacro_replaces_a_materialized_macro_function_cell() {
    // GNU byte-run.el owns `defmacro'; the early-loadup fixture executes that
    // real definition before this function-cell test begins.
    let mut interp = gnu_early_lisp_interpreter();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (defmacro sample-redefined-macro () 1)
               (symbol-function 'sample-redefined-macro)
               (eval '(defmacro sample-redefined-macro () -1))
               (macroexpand '(sample-redefined-macro)))",
        ),
        Value::Integer(-1)
    );
}

#[test]
fn cl_macrolet_outranks_a_materialized_global_macro_cell() {
    let _permit = crate::test_support::acquire_host_test_permit();
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    eval_str_with(&mut interp, "(require 'cl-macs)");
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (defmacro sample-local-priority () 5)
               (symbol-function 'sample-local-priority)
               (cl-macrolet ((sample-local-priority () 4))
                 (defun sample-local-priority-user ()
                   (sample-local-priority)))
               (sample-local-priority-user))",
        ),
        Value::Integer(4)
    );
}

#[test]
fn cl_macrolet_expands_setf_places() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            "(progn
               (cl-defstruct sample-macrolet-place alpha)
               (let ((obj (make-sample-macrolet-place)))
                 (cl-macrolet ((slot (x) (list 'sample-macrolet-place-alpha x)))
                   (setf (slot obj) 9)
                   (sample-macrolet-place-alpha obj))))"
        ),
        Value::Integer(9)
    );
}

#[test]
fn byte_compile_file_suppression_survives_compile_and_load_flow() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-suppress-flow-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let unsuppressed = dir.join("unsuppressed.el");
    let suppressed = dir.join("suppressed.el");
    let unsuppressed_dest = dir.join("unsuppressed.elc");
    let suppressed_dest = dir.join("suppressed.elc");
    std::fs::write(
        &unsuppressed,
        ";;; -*- lexical-binding: t -*-\n(defvar prefixless)\n",
    )
    .unwrap();
    std::fs::write(
        &suppressed,
        ";;; -*- lexical-binding: t -*-\n(with-suppressed-warnings ((lexical prefixless))\n  (defvar prefixless))\n",
    )
    .unwrap();

    let source = format!(
        r#"
            (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*")))
              (let ((byte-compile-dest-file-function (lambda (_) {unsuppressed_dest:?})))
                (byte-compile-file {unsuppressed:?}))
              (with-current-buffer byte-compile-log-buffer
                (let ((inhibit-read-only t))
                  (erase-buffer)))
              (let ((byte-compile-dest-file-function (lambda (_) {suppressed_dest:?})))
                (byte-compile-file {suppressed:?}))
              (load {suppressed:?} nil 'nomessage)
              (with-current-buffer byte-compile-log-buffer
                (not (string-match "global/dynamic var .prefixless. lacks"
                                   (buffer-string)))))
            "#,
        unsuppressed = unsuppressed.display().to_string(),
        suppressed = suppressed.display().to_string(),
        unsuppressed_dest = unsuppressed_dest.display().to_string(),
        suppressed_dest = suppressed_dest.display().to_string(),
    );

    let result = eval_str_with_upstream_batch_feature("bytecomp", &source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(result, Value::T);
}

#[test]
fn byte_compile_file_uses_dynamic_log_buffer_across_helper_call() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-dynamic-log-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let source_path = dir.join("suppressed.el");
    let warning_path = dir.join("warning.el");
    let output_path = dir.join("suppressed.elc");
    let warning_output_path = dir.join("warning.elc");
    std::fs::write(
        &warning_path,
        ";;; -*- lexical-binding: t -*-\n(defvar prefixless)\n",
    )
    .unwrap();
    std::fs::write(
        &source_path,
        ";;; -*- lexical-binding: t -*-\n(with-suppressed-warnings ((lexical prefixless))\n  (defvar prefixless))\n",
    )
    .unwrap();

    let source = format!(
        r#"
            (let ((lexical-binding t))
              (defun emaxx-bytecomp-helper (src dest)
                (let ((byte-compile-dest-file-function (lambda (_) dest)))
                  (byte-compile-file src)))
              (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*")))
                (emaxx-bytecomp-helper {warning_path:?} {warning_output_path:?})
                (with-current-buffer byte-compile-log-buffer
                  (unless (string-match "global/dynamic var .prefixless. lacks"
                                        (buffer-string))
                    (error "missing unsuppressed warning: %s" (buffer-string))))
                (with-current-buffer byte-compile-log-buffer
                  (let ((inhibit-read-only t))
                    (erase-buffer)))
                (emaxx-bytecomp-helper {source_path:?} {output_path:?})
                (with-current-buffer byte-compile-log-buffer
                  (not (string-match "global/dynamic var .prefixless. lacks"
                                     (buffer-string))))))
            "#,
        source_path = source_path.display().to_string(),
        warning_path = warning_path.display().to_string(),
        output_path = output_path.display().to_string(),
        warning_output_path = warning_output_path.display().to_string(),
    );

    let result = eval_str_with_upstream_batch_feature("bytecomp", &source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(result, Value::T);
}

#[test]
fn bytecomp_tests_suppression_helper_matches_prefixless_defvar() {
    run_with_large_stack(|| {
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        interp.set_global_binding("noninteractive", Value::T);
        let path = upstream_emacs_repo().join("test/lisp/emacs-lisp/bytecomp-tests.el");
        crate::lisp::load_file_strict(&mut interp, &path).unwrap();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                (test-suppression
                 '(defvar prefixless)
                 '((lexical prefixless))
                 "global/dynamic var .prefixless. lacks")
                "#
            ),
            Value::T
        );
    });
}

#[test]
fn bytecomp_tests_suppression_case_passes_in_default_ert_run() {
    run_with_large_stack(|| {
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        interp.set_global_binding("noninteractive", Value::T);
        let path = upstream_emacs_repo().join("test/lisp/emacs-lisp/bytecomp-tests.el");
        crate::lisp::load_file_strict(&mut interp, &path).unwrap();
        let selector =
            crate::lisp::reader::Reader::new("(not (or (tag :expensive-test) (tag :unstable)))")
                .read()
                .unwrap()
                .unwrap();
        let _ = interp.run_ert_tests_with_selector(Some(&selector));
        let outcome = interp
            .test_results
            .iter()
            .find(|result| result.name == "bytecomp-test--with-suppressed-warnings")
            .expect("selected suppression test");
        assert_eq!(outcome.status, TestStatus::Passed, "{outcome:?}");
    });
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
                    (list (and (string-search "file has no" missing-log)
                               (not (null (string-search "lexical-binding" missing-log))))
                          (string-search "file has no" lexical-t-log)
                          (string-search "file has no" lexical-nil-log))))))
            "#,
        dest = dest.display().to_string(),
        missing = missing.display().to_string(),
        lexical_t = lexical_t.display().to_string(),
        lexical_nil = lexical_nil.display().to_string(),
    );
    let result = eval_str_with_upstream_batch_feature("bytecomp", &source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(result, Value::list([Value::T, Value::Nil, Value::Nil]));
}

#[test]
fn byte_compile_file_reads_and_interns_file_local_symbol_shorthands() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-shorthand-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let source_path = dir.join("source.el");
    std::fs::write(
        &source_path,
        r#";;; -*- lexical-binding: t; -*-
(defun s-target () 42)
;; Local Variables:
;; read-symbol-shorthands: (("s-" . "long-"))
;; End:
"#,
    )
    .unwrap();

    let result = eval_str_with_upstream_batch_feature(
        "bytecomp",
        &format!(
            r#"(progn
             (byte-compile-file {:?})
             (list (intern-soft "long-target")
                   (intern-soft "s-target")))"#,
            source_path.display().to_string(),
        ),
    );
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        result,
        Value::list([Value::Symbol("long-target".into()), Value::Nil])
    );
}

#[cfg(unix)]
#[test]
fn byte_compile_file_reports_an_unwritable_target_like_gnu() {
    use std::os::unix::fs::PermissionsExt;

    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-readonly-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let source_path = dir.join("source.el");
    let source_elc = dir.join("source.elc");
    std::fs::write(
        &source_path,
        ";;; -*-lexical-binding:t-*-\n(defun sample () 1)\n",
    )
    .unwrap();
    let original_perms = std::fs::metadata(&dir).unwrap().permissions();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let result = eval_str_with_upstream_batch_feature(
        "bytecomp",
        &format!(
            r#"
            (condition-case error
                (list 'unexpected-success
                      (byte-compile-file {source_path:?}))
              (file-missing
               (list (car error)
                     (nth 1 error)
                     (nth 2 error)
                     (string-suffix-p "/source.elc" (nth 3 error))
                     (file-exists-p {source_elc:?}))))
        "#,
            source_path = source_path.display().to_string(),
            source_elc = source_elc.display().to_string(),
        ),
    );

    std::fs::set_permissions(&dir, original_perms).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        result,
        Value::list([
            Value::Symbol("file-missing".into()),
            Value::String("Opening output file".into()),
            Value::String("Directory not writable or nonexistent".into()),
            Value::T,
            Value::Nil,
        ])
    );
}

#[test]
fn byte_compile_file_warns_for_defsubst_callargs() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-defsubst-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let source_path = dir.join("source.el");
    let dest_path = dir.join("source.elc");
    std::fs::write(
        &source_path,
        ";;; -*-lexical-binding:t-*-\n(defsubst sample-defsubst (_x) nil)\n(defun caller () (sample-defsubst 1 2))\n",
    )
    .unwrap();

    let result = eval_str_with_upstream_batch_feature(
        "bytecomp",
        &format!(
            r#"
            (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*"))
                  (byte-compile-dest-file-function (lambda (_) {dest_path:?})))
              (byte-compile-file {source_path:?})
              (with-current-buffer byte-compile-log-buffer
                (not (null (string-match "with 2 arguments, but accepts only 1" (buffer-string))))))
        "#,
            source_path = source_path.display().to_string(),
            dest_path = dest_path.display().to_string(),
        ),
    );

    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(result, Value::T);
}

#[test]
fn byte_compile_file_reports_unescaped_character_literal_errors_and_returns_nil() {
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
                  (byte-compile-log-buffer (generate-new-buffer " *Compile-Log*"))
                  (byte-compile-dest-file-function (lambda (_) {dest_path:?})))
              (list (byte-compile-file {source_path:?})
                    (file-exists-p {dest_path:?})
                    (with-current-buffer byte-compile-log-buffer
                      (and (string-search "unescaped character literals" (buffer-string))
                           t))))
            "#,
        source_path = source_path.display().to_string(),
        dest_path = dest_path.display().to_string(),
    );
    let result = eval_str_with_upstream_batch_feature("bytecomp", &source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(result, Value::list([Value::Nil, Value::Nil, Value::T]));
}

#[test]
fn read_errors_surface_as_invalid_read_syntax_with_gnu_condition_data() {
    assert_eq!(
        eval_str(
            r##"(mapcar
                 (lambda (source)
                   (condition-case error
                       (read source)
                     (invalid-read-syntax error)))
                 '("?\\N{}" "#b" "#1=#1#"))"##
        ),
        Value::list([
            Value::list([
                Value::Symbol("invalid-read-syntax".into()),
                Value::String("Empty character name".into()),
            ]),
            Value::list([
                Value::Symbol("invalid-read-syntax".into()),
                Value::String("integer, radix 2".into()),
            ]),
            Value::list([
                Value::Symbol("invalid-read-syntax".into()),
                Value::String("nonsensical self-reference".into()),
            ]),
        ])
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
    let result = eval_str_with_upstream_batch_feature("bytecomp", &source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(result, Value::list([Value::T, Value::T, Value::Nil]));
}

#[test]
fn byte_compile_file_applies_function_put_before_macro_expansion() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-function-put-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let source_path = dir.join("source.el");
    let dest_path = dir.join("source.elc");

    let source = format!(
        r#"
            (with-temp-buffer
              (insert ";;; -*-lexical-binding:t-*-\n")
              (dolist (form '((function-put 'sample-bytecomp-foo 'foo 1)
                              (function-put 'sample-bytecomp-foo 'bar 2)
                              (defmacro sample-bytecomp-foobar ()
                                `(cons ,(function-get 'sample-bytecomp-foo 'foo)
                                       ,(function-get 'sample-bytecomp-foo 'bar)))
                              (defvar sample-bytecomp-foobar 1)
                              (setq sample-bytecomp-foobar (sample-bytecomp-foobar))))
                (print form (current-buffer)))
              (write-region (point-min) (point-max) {source_path:?} nil 'silent))
            (let ((byte-compile-dest-file-function (lambda (_) {dest_path:?})))
              (byte-compile-file {source_path:?})
              (load {source_path:?})
              sample-bytecomp-foobar)
            "#,
        source_path = source_path.display().to_string(),
        dest_path = dest_path.display().to_string(),
    );
    let result = eval_str_with_upstream_batch_feature("bytecomp", &source);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(result, Value::cons(Value::Integer(1), Value::Integer(2)));
}

#[test]
fn byte_compile_file_applies_gv_expanders_before_later_top_level_forms() {
    let _permit = crate::test_support::acquire_host_test_permit();
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-byte-compile-gv-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    let source_path = dir.join("gv-source.el");
    let dest_path = dir.join("gv-source.elc");
    std::fs::write(
        &source_path,
        ";;; -*- lexical-binding: t -*-\n\
         (gv-define-setter sample-bytecomp-gv (newval cons)\n\
           `(setcar ,cons ,newval))\n\
         (defvar sample-bytecomp-gv-pair (cons 1 2))\n\
         (setf (sample-bytecomp-gv sample-bytecomp-gv-pair) 99)\n",
    )
    .unwrap();

    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut compiler =
        crate::batch::initialize_batch_interpreter_with_load_preference(&options, true)
            .expect("initialize compiled-owner compiler interpreter");
    eval_str_with(
        &mut compiler,
        &format!(
            "(byte-compile-file {:?})",
            source_path.display().to_string()
        ),
    );
    let mut loader =
        crate::batch::initialize_batch_interpreter_with_load_preference(&options, true)
            .expect("initialize fresh compiled-owner loader interpreter");
    crate::lisp::load_file_strict(&mut loader, &dest_path)
        .expect("compiled GV file should load in a fresh interpreter");
    let actual = eval_str_with(&mut loader, "sample-bytecomp-gv-pair");

    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(actual, Value::cons(Value::Integer(99), Value::Integer(2)));
}

#[test]
fn byte_compile_warns_for_unused_args_and_ignored_assq_values() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*")))
                  (with-current-buffer byte-compile-log-buffer
                    (let ((inhibit-read-only t))
                      (erase-buffer)))
                  (byte-compile '(lambda (y) 6))
                  (let ((unused-arg-log (with-current-buffer byte-compile-log-buffer
                                          (buffer-string))))
                    (with-current-buffer byte-compile-log-buffer
                      (let ((inhibit-read-only t))
                        (erase-buffer)))
                    (byte-compile '(lambda (y) (ignore y) 6))
                    (let ((ignored-arg-log (with-current-buffer byte-compile-log-buffer
                                             (buffer-string))))
                      (with-current-buffer byte-compile-log-buffer
                        (let ((inhibit-read-only t))
                          (erase-buffer)))
                      (byte-compile '(lambda (x y) (progn (assq x y) 5)))
                      (let ((assq-log (with-current-buffer byte-compile-log-buffer
                                        (buffer-string))))
                        (with-current-buffer byte-compile-log-buffer
                          (let ((inhibit-read-only t))
                            (erase-buffer)))
                        (byte-compile '(lambda (x y) (progn (ignore (assq x y)) 5)))
                        (let ((ignored-assq-log (with-current-buffer byte-compile-log-buffer
                                                  (buffer-string))))
                          (list (not (null (string-match "unused" unused-arg-log)))
                                ignored-arg-log
                                (not (null (string-match "assq" assq-log)))
                                ignored-assq-log))))))
            "#
        ),
        Value::list([
            Value::T,
            Value::String(String::new().into()),
            Value::T,
            Value::String(String::new().into())
        ])
    );
}

#[test]
fn byte_compile_warns_for_dodgy_eq_and_eql_literal_args() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*")))
                  (with-current-buffer byte-compile-log-buffer
                    (let ((inhibit-read-only t))
                      (erase-buffer)))
                  (dolist (form '((eq '(a) 'x)
                                  (eq 'x "a")
                                  (eq 'x [a])
                                  (eq 'x (lambda () 1))
                                  (eq 'x #'(lambda () 1))
                                  (eq 'x #x10000000000)
                                  (eq 'x 1.0)
                                  (eql '(a) 'x)
                                  (eql 'x "a")
                                  (eql 'x [a])
                                  (eql 'x (lambda () 1))
                                  (eql 'x #'(lambda () 1))))
                    (let ((text-quoting-style 'grave)
                          (macroexp--warned
                           (make-hash-table :test #'equal :weakness 'key)))
                      (byte-compile form)))
                  (let ((warn-log (with-current-buffer byte-compile-log-buffer
                                    (buffer-string))))
                    (with-current-buffer byte-compile-log-buffer
                      (let ((inhibit-read-only t))
                        (erase-buffer)))
                    (let ((text-quoting-style 'grave)
                          (macroexp--warned
                           (make-hash-table :test #'equal :weakness 'key)))
                      (byte-compile '(eql 'x #x10000000000))
                      (byte-compile '(eql 'x 1.0)))
                    (let ((numeric-eql-log (with-current-buffer byte-compile-log-buffer
                                             (buffer-string))))
                      (list (not (null (string-match "`eq'.*list.*arg 1" warn-log)))
                            (not (null (string-match "`eq'.*string.*arg 2" warn-log)))
                            (not (null (string-match "`eq'.*vector.*arg 2" warn-log)))
                            (not (null (string-match "`eq'.*function.*arg 2" warn-log)))
                            (not (null (string-match "`eq'.*integer.*arg 2" warn-log)))
                            (not (null (string-match "`eq'.*float.*arg 2" warn-log)))
                            (not (null (string-match "`eql'.*list.*arg 1" warn-log)))
                            (not (null (string-match "`eql'.*string.*arg 2" warn-log)))
                            (not (null (string-match "`eql'.*vector.*arg 2" warn-log)))
                            (not (null (string-match "`eql'.*function.*arg 2" warn-log)))
                            numeric-eql-log))))
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
            Value::String(String::new().into())
        ])
    );
}

#[test]
fn byte_compile_warns_for_dodgy_identity_member_literal_args() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*")))
                  (with-current-buffer byte-compile-log-buffer
                    (let ((inhibit-read-only t))
                      (erase-buffer)))
                  (dolist (form '((memq '(a) '(x))
                                  (memq "a" '(x))
                                  (memq [a] '(x))
                                  (memq (lambda () 1) '(x))
                                  (memq #'(lambda () 1) '(x))
                                  (memq #x10000000000 '(x))
                                  (memq 1.0 '(x))
                                  (memq 'x '(a "b" c))
                                  (memq 'x '(a ''b c))
                                  (assq 'x '((a . 1) ("b" . 2) (c . 3)))
                                  (rassq 'x '((1 . a) (2 . "b") (3 . c)))))
                    (let ((text-quoting-style 'grave)
                          (macroexp--warned
                           (make-hash-table :test #'equal :weakness 'key)))
                      (byte-compile form)))
                  (let ((warn-log (with-current-buffer byte-compile-log-buffer
                                    (buffer-string))))
                    (with-current-buffer byte-compile-log-buffer
                      (let ((inhibit-read-only t))
                        (erase-buffer)))
                    (let ((text-quoting-style 'grave)
                          (macroexp--warned
                           (make-hash-table :test #'equal :weakness 'key)))
                      (byte-compile '(memql #x10000000000 '(x)))
                      (byte-compile '(memql 1.0 '(x))))
                    (let ((numeric-memql-log (with-current-buffer byte-compile-log-buffer
                                               (buffer-string))))
                      (list (not (null (string-match "`memq'.*list.*arg 1" warn-log)))
                            (not (null (string-match "`memq'.*string.*arg 1" warn-log)))
                            (not (null (string-match "`memq'.*vector.*arg 1" warn-log)))
                            (not (null (string-match "`memq'.*function.*arg 1" warn-log)))
                            (not (null (string-match "`memq'.*integer.*arg 1" warn-log)))
                            (not (null (string-match "`memq'.*float.*arg 1" warn-log)))
                            (not (null (string-match "`memq'.*string.*element 2 of arg 2" warn-log)))
                            (not (null (string-match "`memq'.*list.*element 2 of arg 2" warn-log)))
                            (not (null (string-match "`assq'.*string.*element 2 of arg 2" warn-log)))
                            (not (null (string-match "`rassq'.*string.*element 2 of arg 2" warn-log)))
                            numeric-memql-log))))
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
            Value::String(String::new().into())
        ])
    );
}

#[test]
fn cl_labels_functions_can_call_local_labels() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            r#"
                (cl-labels ((double (x) (+ x x))
                            (quadruple (x) (double (double x))))
                  (quadruple 7))
            "#
        ),
        Value::Integer(28)
    );
}

#[test]
fn loaded_cl_labels_keeps_nested_pcase_commas_inside_backquote_patterns() {
    run_with_large_stack(|| {
        // GNU cl-labels expands its binding parser through pcase patterns
        // containing nested raw `\,' symbols.  A constant-suffix backquote
        // optimization must not turn those pattern markers into variables.
        assert_eq!(
            eval_str_with_upstream_batch(
                r#"(progn
                     (require 'cl-macs)
                     (cl-labels
                         ((prn3 (x y z)
                            (prin1-to-string (list x y z)))
                          (cat3 (x y z)
                            (concat "(" x " " y " " z ")")))
                       (prn3 nil nil nil)))"#,
            ),
            Value::String("(nil nil nil)".into())
        );
    });
}

#[test]
fn byte_compile_warns_for_quoted_condition_names() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (let ((byte-compile-log-buffer (generate-new-buffer " *Compile-Log*")))
                  (with-current-buffer byte-compile-log-buffer
                    (let ((inhibit-read-only t))
                      (erase-buffer)))
                  (let ((text-quoting-style 'grave)
                        (macroexp--warned
                         (make-hash-table :test #'equal :weakness 'key)))
                    (byte-compile '(condition-case nil
                                       (abc)
                                     ('arith-error "ugh")))
                    (byte-compile '(ignore-error 'error (abc))))
                  (let ((warn-log (with-current-buffer byte-compile-log-buffer
                                    (buffer-string))))
                    (list (not (null (string-match "`condition-case'.*'arith-error" warn-log)))
                          (not (null (string-match "`ignore-error'.*'error" warn-log))))))
            "#
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn byte_compile_wide_docstring_ignores_function_arg_lists() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (list
                 (byte-compile--wide-docstring-p
                  "\(dbus-register-property BUS SERVICE PATH INTERFACE PROPERTY ACCESS [TYPE] VALUE &optional EMITS-SIGNAL DONT-REGISTER-SERVICE)"
                  fill-column)
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
fn byte_compile_warns_for_wide_docstrings_in_definition_forms() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "bytecomp",
            r#"
                (progn
                  (defun test--bytecomp-warning-p (pattern form)
                    (with-current-buffer (get-buffer-create "*Compile-Log*")
                      (let ((inhibit-read-only t))
                        (erase-buffer)))
                    (let ((text-quoting-style 'grave)
                          (macroexp--warned
                           (make-hash-table :test #'equal :weakness 'key)))
                      (byte-compile form))
                    (with-current-buffer "*Compile-Log*"
                      (not (null (re-search-forward pattern nil t)))))
                  (list
                   (test--bytecomp-warning-p
                    "defvar .sample-wide-var. docstring wider"
                    '(defvar sample-wide-var nil
                       "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
                   (test--bytecomp-warning-p
                    "docstring wider"
                    '(defun sample-wide-function ()
                       "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                       nil))
                   (not
                    (test--bytecomp-warning-p
                     "docstring wider"
                     '(defun sample-signature-only ()
                        "(fn NAME FIXTURE INPUT &key SKIP-PAIR-STRING EXPECTED-STRING EXPECTED-POINT BINDINGS (MODES \\='\\='(ruby-mode js-mode python-mode)) (TEST-IN-COMMENTS t) (TEST-IN-STRINGS t) (TEST-IN-CODE t))"
                        nil)))))
                "#
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn upstream_byte_compiler_preserves_gnu_dynamic_scope_boundaries() {
    crate::test_support::run_with_large_stack(|| {
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        eval_str_with(&mut interp, "(require 'bytecomp)");

        assert_eq!(
            eval_str_with(
                &mut interp,
                "(type-of (byte-compile (lambda (x) (char-syntax x))))",
            ),
            Value::Symbol("byte-code-function".into()),
            "GNU bytecomp.el must produce a byte-code-function closure",
        );
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(funcall (byte-compile #'(lambda (x) (1+ x))) 41)",
            ),
            Value::Integer(42),
            "GNU bytecomp.el must accept an interpreted function value",
        );
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                    (let ((function
                           (byte-compile
                            #'(lambda (required &optional optional)
                                (list required optional)))))
                      (list (aref function 0)
                            (func-arity function)
                            (funcall function 'value)))
                "#,
            ),
            Value::list([
                Value::Integer(513),
                Value::cons(Value::Integer(1), Value::Integer(2)),
                Value::list([Value::Symbol("value".into()), Value::Nil]),
            ]),
            "compiled argument descriptors and callable arity must agree",
        );

        // GNU bytecomp.el's `byte-compile' (around line 2955) compiles a
        // quoted lambda dynamically, but reifies the environment of an
        // interpreted lexical closure.  The dynamic variable must actually
        // be declared special; merely binding the variable named
        // `lexical-binding' does not retroactively change the evaluator
        // environment around this form.
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                (progn
                  (defvar sample-byte-dynamic nil)
                  (let ((sample-byte-dynamic 1))
                  (list
                   (let ((lexical-binding nil)
                         (compiled
                          (byte-compile
                           '(lambda () sample-byte-dynamic))))
                     (let ((sample-byte-dynamic 2))
                       (funcall compiled)))
                   (let ((compiled
                          (let ((sample-byte-lexical 3))
                            (byte-compile
                             (lambda () sample-byte-lexical)))))
                     (let ((sample-byte-lexical 4))
                       (funcall compiled))))))
                "#,
            ),
            Value::list([Value::Integer(2), Value::Integer(3)]),
            "byte-compile must distinguish declared-special dynamic lookup from a reified lexical closure",
        );

        // GNU eval.c dynamically binds a condition-case variable and unwinds
        // that binding when the handler exits.  A dynamic lambda returned by
        // the handler therefore cannot capture the variable after the unwind.
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                (let ((form '(funcall
                              (condition-case x
                                  (/ 1 0)
                                (arith-error
                                 (prog1 (lambda (y) (+ y x))
                                   (setq x 10))))
                              4))
                      (lexical-binding nil))
                  (list
                   (condition-case err
                       (eval form lexical-binding)
                     (void-variable (car err)))
                   (condition-case err
                       (funcall
                       (byte-compile (list 'lambda nil form)))
                     (void-variable (car err)))))
                "#,
            ),
            Value::list([
                Value::Symbol("void-variable".into()),
                Value::Symbol("void-variable".into()),
            ]),
            "condition-case's dynamic error binding must be gone before a returned lambda is called",
        );

        // GNU eval.c's dynamic lambdas do not close over their arguments.
        // The nested `g' therefore resolves the currently active dynamic x.
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                (let ((form '(let ((f (lambda (x)
                                        (lambda nil
                                          (let ((g (lambda nil x)))
                                            (let ((x 'a))
                                              (list x (funcall g))))))))
                               (funcall (funcall f 'b))))
                      (lexical-binding nil))
                  (list (eval form lexical-binding)
                        (funcall (byte-compile (list 'lambda nil form)))))
                "#,
            ),
            Value::list([
                Value::list([Value::Symbol("a".into()), Value::Symbol("a".into())]),
                Value::list([Value::Symbol("a".into()), Value::Symbol("a".into())]),
            ]),
            "interpreted and compiled dynamic lambdas must resolve the same active binding",
        );
    });
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
                 (string-match-p "\\s_" "=")
                 (string-match-p "\\s>" "\n")
                 (string-match-p "\\S>" "n")
                 (string-match-p "\\S(" "a"))
                "#
        ),
        // GNU's standard syntax table assigns no character the comment-end
        // class (newline is whitespace there); `\s>' matches nothing and
        // `\S>' matches anything outside a mode that sets up comments.
        // `=' is symbol class there (syntax.c gives "_-+*/&|<>=" Ssymbol),
        // so `\s.' rejects it and `\s_' matches.
        Value::list([
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Nil,
            Value::Integer(0),
            Value::Nil,
            Value::Integer(0),
            Value::Integer(0),
        ])
    );
}

#[test]
fn regexp_string_syntax_class_uses_the_current_table_not_a_literal_quote() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (let ((table (make-syntax-table)))
                    (modify-syntax-entry ?' "\"" table)
                    (set-syntax-table table)
                    (insert "'")
                    (goto-char 1)
                    (list (looking-at-p "\\s\"")
                          (looking-at-p "\\S\""))))
                "#,
        ),
        Value::list([Value::T, Value::Nil])
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
fn invisible_p_honors_symbolic_overlay_categories() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "ab")
                  (let ((hidden (make-overlay 1 2))
                        (highlight (make-overlay 1 2)))
                    (overlay-put hidden 'invisible 'outline)
                    (overlay-put highlight 'priority 1001)
                    (overlay-put highlight 'face 'isearch)
                    (setq buffer-invisibility-spec '(outline))
                    (list (invisible-p 1)
                          (invisible-p 2)
                          (progn
                            (setq buffer-invisibility-spec nil)
                            (invisible-p 1)))))
                "#
        ),
        Value::list([Value::T, Value::Nil, Value::Nil])
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
fn forward_comment_uses_absolute_positions_in_a_narrowed_buffer() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (set-syntax-table (make-syntax-table))
                  (modify-syntax-entry ?/ ". 124b")
                  (modify-syntax-entry ?* ". 23")
                  (insert "prefix text\n  /* inside */")
                  (narrow-to-region 13 (point-max))
                  (goto-char (point-max))
                  (list (forward-comment -1) (point) (point-min)))
                "#
        ),
        Value::list([Value::T, Value::Integer(15), Value::Integer(13)])
    );
}

#[test]
fn backward_forward_comment_does_not_find_comment_openers_inside_strings() {
    assert_eq!(
        eval_str(
            r##"
                (list
                 (with-temp-buffer
                   (modify-syntax-entry ?# "< b")
                   (modify-syntax-entry ?\n "> b")
                   (insert "\"bar#x\"\n")
                   (goto-char (point-max))
                   (list (forward-comment (- (point))) (point)))
                 (with-temp-buffer
                   (modify-syntax-entry ?# "< b")
                   (modify-syntax-entry ?\n "> b")
                   (insert "#x\n")
                   (goto-char (point-max))
                   (list (forward-comment -1) (point))))
                "##,
        ),
        Value::list([
            Value::list([Value::Nil, Value::Integer(8)]),
            Value::list([Value::T, Value::Integer(1)]),
        ])
    );
}

#[test]
fn forward_comment_lazily_applies_position_specific_syntax() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r##"
                (with-temp-buffer
                  (modify-syntax-entry ?# "<")
                  (modify-syntax-entry ?\n ">")
                  (setq-local syntax-propertize-function
                              (lambda (start end)
                                (goto-char start)
                                (when (search-forward "#" end t)
                                  (put-text-property
                                   (1- (point)) (point) 'syntax-table
                                   (string-to-syntax "_")))
                                (goto-char end)))
                  (insert "foo#zot")
                  (goto-char (point-max))
                  (list (forward-comment (- (point)))
                        (point)))
                "##,
        ),
        Value::list([Value::Nil, Value::Integer(8),])
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
fn scan_lists_skips_escaped_line_and_block_comment_endings() {
    assert_eq!(
        eval_str(
            r#"
                (list
                 (with-temp-buffer
                   (setq parse-sexp-ignore-comments t
                         comment-end-can-be-escaped t)
                   (modify-syntax-entry ?{ "(}")
                   (modify-syntax-entry ?} "){")
                   (modify-syntax-entry ?/ ". 124b")
                   (modify-syntax-entry ?* ". 23")
                   (modify-syntax-entry ?\n "> b")
                   (insert "{ // x \\\n} ignored\n}")
                   (let ((end (point-max)))
                     (list (scan-lists 1 1 0) end)))
                 (with-temp-buffer
                   (setq parse-sexp-ignore-comments t
                         comment-end-can-be-escaped t)
                   (modify-syntax-entry ?{ "(}")
                   (modify-syntax-entry ?} "){")
                   (modify-syntax-entry ?/ ". 124b")
                   (modify-syntax-entry ?* ". 23")
                   (modify-syntax-entry ?\n "> b")
                   (insert "{ /* x \\*/ } ignored\n*/ }")
                   (let ((end (point-max)))
                     (list (scan-lists 1 1 0) end))))
                "#,
        ),
        Value::list([
            Value::list([Value::Integer(21), Value::Integer(21)]),
            Value::list([Value::Integer(26), Value::Integer(26)]),
        ])
    );
}

#[test]
fn forward_list_moves_over_syntax_table_brace_lists() {
    assert_eq!(
        eval_str_with_upstream_batch(
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
fn blank_temporary_syntax_tables_and_narrowed_scans_use_gnu_coordinates() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (insert "prefix (alpha\n beta) suffix")
                  (goto-char 8)
                  (let* ((op (char-after))
                         (cl (cdr (aref (syntax-table) op)))
                         parse-sexp-lookup-properties
                         (syntax-propertize-function nil))
                    (with-syntax-table (make-char-table 'syntax-table nil)
                      (let ((blank-classes
                             (list (char-syntax ?a)
                                   (char-syntax ?[)
                                   (char-syntax ?\s))))
                        (modify-syntax-entry op
                                             (concat "(" (char-to-string cl)))
                        (modify-syntax-entry cl
                                             (concat ")" (char-to-string op)))
                        (modify-syntax-entry ?\\ "\\")
                        (narrow-to-region 8 21)
                        (forward-list)
                        (list blank-classes (point) (point-min) (point-max))))))
                "#,
        ),
        Value::list([
            Value::list([
                Value::Integer(' ' as i64),
                Value::Integer(' ' as i64),
                Value::Integer(' ' as i64),
            ]),
            Value::Integer(21),
            Value::Integer(8),
            Value::Integer(21),
        ])
    );
}

#[test]
fn skip_syntax_honors_effective_syntax_properties_when_enabled() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "%")
                  (put-text-property 1 2 'syntax-table
                                     (string-to-syntax "|"))
                  (list
                   (let ((parse-sexp-lookup-properties nil))
                     (goto-char 1)
                     (list (skip-syntax-forward ".")
                           (progn (goto-char 1)
                                  (skip-syntax-forward "|"))))
                   (let ((parse-sexp-lookup-properties t))
                     (goto-char 1)
                     (list (skip-syntax-forward ".")
                           (progn (goto-char 1)
                                  (skip-syntax-forward "|"))))))
                "#,
        ),
        Value::list([
            Value::list([Value::Integer(0), Value::Integer(0)]),
            Value::list([Value::Integer(0), Value::Integer(1)]),
        ])
    );
}

#[test]
fn backward_sexp_uses_effective_syntax_when_skipping_prefix_chars() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (let ((table (make-syntax-table)))
                    (modify-syntax-entry ?: "' p" table)
                    (set-syntax-table table)
                    (setq-local parse-sexp-lookup-properties t)
                    (insert ":()")
                    ;; Ruby uses this shape to suppress the base prefix
                    ;; syntax of a ternary colon immediately before `('.
                    (put-text-property 1 2 'syntax-table '(1))
                    (goto-char (point-max))
                    (backward-sexp)
                    (point)))
                "#,
        ),
        Value::Integer(2)
    );
}

#[test]
fn down_and_up_list_move_through_nested_lists() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (insert "(outer (inner value)) tail")
                  (goto-char (point-min))
                  (down-list)
                  (let ((after-down (point)))
                    (down-list)
                    (up-list)
                    (list after-down (point))))
                "#
        ),
        Value::list([Value::Integer(2), Value::Integer(21)])
    );
}

#[test]
fn beginning_of_defun_raw_moves_between_column_zero_lists() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (with-temp-buffer
                  (insert "(defun first () nil)\n\n(defun second () nil)\n")
                  (goto-char (point-max))
                  (let ((backward (beginning-of-defun-raw))
                        (first-point (point)))
                    (beginning-of-defun-raw)
                    (let ((second-point (point)))
                      (beginning-of-defun-raw -1)
                      (list backward first-point second-point (point)))))
                "#
        ),
        Value::list([
            Value::T,
            Value::Integer(23),
            Value::Integer(1),
            Value::Integer(23),
        ])
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
fn forward_comment_moves_over_style_c_double_slash_comments() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (let ((table (make-syntax-table)))
                    (modify-syntax-entry ?/ ". 12c" table)
                    (modify-syntax-entry ?\n "> c" table)
                    (set-syntax-table table))
                  (insert "// comment\ncode")
                  (goto-char (point-min))
                  (list (forward-comment 1) (point)))
                "#
        ),
        Value::list([Value::T, Value::Integer(12)])
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
fn re_search_backward_preserves_context_after_the_search_point() {
    assert_eq!(
        eval_str(
            r#"
                (list
                  (with-temp-buffer
                    (insert "a\nb")
                    (goto-char 3)
                    (list (re-search-backward "^[ \t]*$" nil t)
                          (point)))
                  (with-temp-buffer
                    (insert "a\nb")
                    (goto-char 3)
                    (list (re-search-backward "$" nil t)
                          (point)
                          (match-beginning 0)
                          (match-end 0)))
                  (with-temp-buffer
                    (insert "a\nb")
                    (goto-char 3)
                    (list (re-search-backward "\\'" nil t)
                          (point)))
                  (with-temp-buffer
                    (insert "a\nb")
                    (goto-char 3)
                    (list (re-search-backward "\\=" nil t)
                          (point)
                          (match-beginning 0)
                          (match-end 0)))
                  (with-temp-buffer
                    (insert "a\nb")
                    (goto-char 3)
                    (list (re-search-backward "\\=b" nil t)
                          (point)))
                  (with-temp-buffer
                    (insert "a\nb")
                    (goto-char 4)
                    (list (re-search-backward "b\\=" nil t)
                          (point)
                          (match-beginning 0)
                          (match-end 0)))
                  (with-temp-buffer
                    (insert "@defun x\nText")
                    (goto-char 10)
                    (let ((regexp
                           "\\(@[a-zA-Z]+\\)[ \t\n]\\|^[ \t]*$\\|\f"))
                      (list (re-search-backward regexp nil t)
                            (match-beginning 0)
                            (match-end 0))))
                  (with-temp-buffer
                    (insert "a\nb")
                    (goto-char 3)
                    (list (posix-search-backward "^[ \t]*$" nil t)
                          (point)))
                  (with-temp-buffer
                    (insert "a\nb")
                    (goto-char 3)
                    (list (posix-search-backward "$" nil t)
                          (point)
                          (match-beginning 0)
                          (match-end 0)))
                  (with-temp-buffer
                    (insert "a\nb")
                    (goto-char 3)
                    (list (posix-search-backward "\\'" nil t)
                          (point)))
                  (with-temp-buffer
                    (insert "a\nb")
                    (goto-char 3)
                    (list (posix-search-backward "\\=" nil t)
                          (point)
                          (match-beginning 0)
                          (match-end 0))))
                "#,
        ),
        Value::list([
            Value::list([Value::Nil, Value::Integer(3)]),
            Value::list([
                Value::Integer(2),
                Value::Integer(2),
                Value::Integer(2),
                Value::Integer(2),
            ]),
            Value::list([Value::Nil, Value::Integer(3)]),
            Value::list([
                Value::Integer(3),
                Value::Integer(3),
                Value::Integer(3),
                Value::Integer(3),
            ]),
            Value::list([Value::Nil, Value::Integer(3)]),
            Value::list([
                Value::Integer(3),
                Value::Integer(3),
                Value::Integer(3),
                Value::Integer(4),
            ]),
            Value::list([Value::Integer(1), Value::Integer(1), Value::Integer(8)]),
            Value::list([Value::Nil, Value::Integer(3)]),
            Value::list([
                Value::Integer(2),
                Value::Integer(2),
                Value::Integer(2),
                Value::Integer(2),
            ]),
            Value::list([Value::Nil, Value::Integer(3)]),
            Value::list([
                Value::Integer(3),
                Value::Integer(3),
                Value::Integer(3),
                Value::Integer(3),
            ]),
        ])
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
fn clearing_match_data_leaves_unmatched_register_queries_defined() {
    assert_eq!(
        eval_str(
            "(progn
               (string-match \"a\" \"a\")
               (set-match-data nil)
               (list (match-data) (match-beginning 0) (match-end 0)))"
        ),
        Value::list([Value::Nil, Value::Nil, Value::Nil])
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
fn single_char_property_changes_preserve_string_overlay_identity() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "abcdefgh")
                  (let ((overlay (make-overlay 1 9)))
                    (overlay-put overlay 'url (format "https://%s" "example.test"))
                    (list (previous-single-char-property-change 2 'url)
                          (next-single-char-property-change 1 'url))))
                "#
        ),
        Value::list([Value::Integer(1), Value::Integer(9)])
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
fn forward_comment_finds_line_comment_before_unterminated_nested_marker() {
    assert_eq!(
        eval_str(
            r##"
                (with-temp-buffer
                  (modify-syntax-entry ?# ". 14")
                  (modify-syntax-entry ?| ". 23n")
                  (modify-syntax-entry ?\; "< b")
                  (modify-syntax-entry ?\n "> b")
                  (insert "; #|\n")
                  (goto-char (point-max))
                  (list (forward-comment -1) (point)))
                "##,
        ),
        Value::list([Value::T, Value::Integer(1)])
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
fn emacs_version_variable_matches_the_pinned_gnu_release() {
    // GNU: (format "%s|%s|%s" emacs-version emacs-major-version
    // emacs-minor-version) => "30.2|30|2".  A non-empty check here once hid
    // the crate's three-component semver leaking into `emacs-version'.
    assert_eq!(
        eval_str("(format \"%s|%s|%s\" emacs-version emacs-major-version emacs-minor-version)"),
        Value::String("30.2|30|2".into())
    );
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let version = eval_str_with(&mut interp, "emacs-version");
    let configuration = eval_str_with(&mut interp, "system-configuration");
    let description = eval_str_with(&mut interp, "(emacs-version)");
    let version = primitives::string_text(&version).expect("emacs-version variable is a string");
    let configuration =
        primitives::string_text(&configuration).expect("system-configuration is a string");
    let description = primitives::string_text(&description)
        .expect("GNU version.el emacs-version returns a string");
    assert!(description.contains(&version));
    assert!(description.contains(&configuration));
}

#[test]
fn process_identity_supports_desktop_lock_checks() {
    // `alist-get' and `file-name-nondirectory' are supplied by GNU's dumped
    // Lisp runtime, while the process identity operations remain host-owned.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut load_path = interp.configured_load_path().to_vec();
    load_path.insert(0, temp.clone());
    interp.set_load_path(load_path);
    assert_eq!(
        eval_str_with(&mut interp, "(locate-library \"sample-lib\")"),
        Value::String(library.display().to_string().into())
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
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "custom",
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
            eval_str_with_upstream_batch_feature(
                "time-date",
                r#"(progn
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
            eval_str_with_upstream_batch(
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
            eval_str_with_upstream_batch_feature(
                "parse-time",
                r#"(progn
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
fn cl_parse_integer_handles_keyword_bounds_through_real_cl_extra() {
    run_with_large_stack(|| {
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        eval_str_with(&mut interp, "(require 'cl-extra)");
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(progn
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
            eval_str_with_upstream_batch_feature(
                "parse-time",
                r#"(progn
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
fn time_zone_rule_is_interpreter_local_and_setenv_tz_updates_it() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "env",
            r#"
                (let ((winter (encode-time '(0 0 12 1 1 2020 nil nil t)))
                      (summer (encode-time '(0 0 12 1 7 2020 nil nil t))))
                  (set-time-zone-rule "CET-1CEST,M3.5.0/2,M10.5.0/3")
                  (list
                   (format-time-string "%z %Z" winter)
                   (format-time-string "%z %Z" summer)
                   (progn
                     (setenv "TZ" "UTC0")
                     (format-time-string "%z %Z" winter))))
                "#
        ),
        Value::list([
            Value::String("+0100 CET".into()),
            Value::String("+0200 CEST".into()),
            Value::String("+0000 UTC".into()),
        ])
    );
}

#[test]
fn decoded_time_accessors_read_list_fields() {
    assert_eq!(
        eval_str_with_upstream_batch(
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
            eval_str_with_upstream_batch(
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
fn loaded_gnu_setf_mutates_decoded_time_places() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize GNU-compatible batch interpreter");
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("lisp/emacs-lisp/gv.el"),
        )
        .expect("load GNU gv");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(let ((time (decode-time 0 \"UTC0\" 'integer)))
                   (setf (decoded-time-hour time) 23
                         (decoded-time-second time) 30)
                   (list (decoded-time-hour time)
                         (decoded-time-second time)))"
            ),
            Value::list([Value::Integer(23), Value::Integer(30)])
        );
    });
}

#[test]
fn macro_expansion_cache_tracks_symbol_property_mutations() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize GNU-compatible batch interpreter");
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("lisp/emacs-lisp/gv.el"),
        )
        .expect("load GNU gv");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (defun emaxx-test-late-place--cmacro (_form time)
                     (list 'nth 2 time))
                   (defun emaxx-test-set-late-place (time)
                     (setf (emaxx-test-late-place time) 23))
                   (let ((time (decode-time 0 \"UTC0\" 'integer)))
                     (condition-case nil
                         (emaxx-test-set-late-place time)
                       (error nil))
                     (put 'emaxx-test-late-place
                          'compiler-macro
                          'emaxx-test-late-place--cmacro)
                     (emaxx-test-set-late-place time)
                     (decoded-time-hour time)))"
            ),
            Value::Integer(23)
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
fn encode_time_obsolescent_calls_use_the_last_argument_as_zone() {
    assert_eq!(
        eval_str(
            r#"(mapcar (lambda (time) (format-time-string "%FT%T" time t))
                       (list (encode-time 52 27 18 10 3 2008 t)
                             (encode-time 52 27 18 10 3 2008 nil t)
                             (encode-time 52 27 18 10 3 2008 nil nil t)
                             (encode-time '(52 27 18 10 3 2008 nil nil t))))"#,
        ),
        Value::list([
            Value::String("2008-03-10T18:27:52".into()),
            Value::String("2008-03-10T18:27:52".into()),
            Value::String("2008-03-10T18:27:52".into()),
            Value::String("2008-03-10T18:27:52".into()),
        ])
    );
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
fn format_time_string_supports_case_flags_widths_and_calendar_fields() {
    run_with_large_stack(|| {
        assert_string_value(
            eval_str(
                r#"(let ((time (encode-time 5 4 15 2 1 2006 nil nil 0)))
                     (mapconcat
                      (lambda (format)
                        (format-time-string format time t))
                      '("%^A" "%#A" "%#p" "%#Z"
                        "%5a" "%05a" "%-5a" "%_5a"
                        "%3m" "%_3m" "%-3m"
                        "%U" "%W" "%V" "%G" "%g" "%q" "%k" "%l"
                        "%Ec" "%Od")
                      "|"))"#,
            ),
            "MONDAY|MONDAY|pm|utc|  Mon|00Mon|Mon|  Mon|001|  1|1|\
             01|01|01|2006|06|1|15| 3|Mon Jan  2 15:04:05 2006|02",
        );
    });
}

#[test]
fn startup_mail_host_address_is_a_special_nil_binding() {
    assert_eq!(
        eval_str(
            r#"(let ((read-mail-host (lambda () mail-host-address)))
                 (list mail-host-address
                       (let ((mail-host-address "mail.example"))
                         (funcall read-mail-host))
                       mail-host-address))"#,
        ),
        Value::list([Value::Nil, Value::String("mail.example".into()), Value::Nil,])
    );
}

#[test]
fn user_identity_primitives_honor_dynamic_cells_and_gnu_uid_inputs() {
    assert_eq!(
        eval_str(
            r#"(let ((read-current-identities
                       (lambda ()
                         (list (user-full-name) (user-full-name nil)
                               (user-login-name) (user-login-name nil)
                               (user-real-login-name)))))
                 (list
                  (let ((user-full-name "Full Name")
                        (user-login-name "login")
                        (user-real-login-name "real"))
                    (funcall read-current-identities))
                  (equal (user-login-name 0) (user-login-name 0.0))
                  (equal (user-login-name 0) (user-login-name '(0 . 0)))
                  (condition-case error
                      (user-login-name 1.5)
                    (error (error-message-string error)))
                  (condition-case error
                      (user-full-name 'not-a-uid)
                    (error (error-message-string error)))))"#,
        ),
        Value::list([
            Value::list([
                Value::String("Full Name".into()),
                Value::String("Full Name".into()),
                Value::String("login".into()),
                Value::String("login".into()),
                Value::String("real".into()),
            ]),
            Value::T,
            Value::T,
            Value::String("Not an in-range integer, integral float, or cons of integers".into()),
            Value::String("Invalid UID specification".into()),
        ])
    );
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    eval_str_with(
        &mut interp,
        r#"(setenv "TZ" "EET-2EEST,M3.5.0/3,M10.5.0/4")"#,
    );
    assert_eq!(
        interp.local_time_zone_rule,
        Value::String("EET-2EEST,M3.5.0/3,M10.5.0/4".into())
    );
    let result = eval_str_with(
        &mut interp,
        r#"(list
              (decode-time
               (encode-time '(0 0 10 1 1 2013 nil -1 nil))
               nil 'integer)
              (decode-time
               (encode-time '(0 0 10 1 8 2013 nil -1 nil))
               nil 'integer)
              (decode-time
               (encode-time '(0 0 10 1 1 2013 nil -1 t))
               nil 'integer))"#,
    );
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
fn loaded_todo_mode_resource_state_survives_real_ert_macro() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("emaxx-todo-mode-{unique}"));
    let todo_directory = root.join("todo-mode-resources");
    std::fs::create_dir_all(&todo_directory).expect("create Todo resource directory");
    let test_file = root.join("todo-mode-tests.el");
    std::fs::copy(
        upstream_emacs_repo().join("test/lisp/calendar/todo-mode-tests.el"),
        &test_file,
    )
    .expect("copy Todo test");
    std::fs::copy(
        upstream_emacs_repo().join("test/lisp/calendar/todo-mode-resources/todo-test-1.todo"),
        todo_directory.join("todo-test-1.todo"),
    )
    .expect("copy Todo fixture");
    std::fs::copy(
        upstream_emacs_repo().join("test/lisp/calendar/todo-mode-resources/todo-test-1.toda"),
        todo_directory.join("todo-test-1.toda"),
    )
    .expect("copy Todo archive fixture");
    let noselect_file = root.join("noselect.txt");
    std::fs::write(&noselect_file, "noselect\n").expect("write noselect fixture");
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp = crate::batch::initialize_batch_interpreter(&options)
        .expect("initialize GNU-compatible batch interpreter");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(list (featurep 'window)
                     (fboundp 'cl--assertion-failed)
                     (subrp (symbol-function 'window-body-width))
                     (subrp (indirect-function 'window-width))
                     (subrp (indirect-function 'window-fringes))
                     (subrp (indirect-function 'set-window-fringes))
                     (= (window-width) (frame-width))
                     (= (window-height) (frame-height))
                     (= (window-pixel-width) (frame-width))
                     (= (window-pixel-height) (frame-height))
                     (= (window-pixel-left) 0)
                     (= (window-pixel-top) 0)
                     (equal
                      (list (frame-internal-border-width)
                            (frame-native-width)
                            (frame-native-height)
                            (frame-pixel-width)
                            (frame-pixel-height)
                            (frame-text-width)
                            (frame-text-height)
                            (frame-text-cols)
                            (frame-text-lines)
                            (frame-fringe-width)
                            (frame-scroll-bar-width)
                            (frame-scroll-bar-height))
                      '(0 80 25 80 25 80 25 80 25 0 0 0))
                     (equal
                      (list (window-mode-line-height)
                            (window-header-line-height)
                            (window-tab-line-height)
                            (window-right-divider-width)
                            (window-bottom-divider-width)
                            (window-scroll-bar-width)
                            (window-scroll-bar-height))
                      '(1 0 0 0 0 0 0))
                     (let ((mode-line-format nil))
                       (= (window-mode-line-height) 0))
                     (equal
                      (list (window-body-width)
                            (window-body-height)
                            (window-total-width)
                            (window-total-height)
                            (window-pixel-edges)
                            (window-body-pixel-edges)
                            (window-inside-pixel-edges)
                            (window-left-column)
                            (window-top-line)
                            (window-old-pixel-width)
                            (window-old-pixel-height)
                            (window-old-body-pixel-width)
                            (window-old-body-pixel-height))
                      '(80 23 80 24
                        (0 0 80 24) (0 0 80 23) (0 0 80 23)
                        0 0 0 0 0 0))
                     (let ((window (selected-window)))
                       (and
                        (equal (window-fringes window) '(0 0 nil nil))
                        (null (set-window-fringes window 3 4 t t))
                        (equal (window-fringes window) '(0 0 nil nil))
                        (equal (window-scroll-bars window)
                               '(nil 0 t nil 0 t nil))
                        (null
                         (set-window-scroll-bars
                          window 3 'left 2 'bottom t))
                        (equal (window-scroll-bars window)
                               '(nil 0 t nil 0 t nil))
                        (eq (window-cursor-type window) t)
                        (eq (set-window-cursor-type window 'bar) 'bar)
                        (eq (window-cursor-type window) 'bar)
                        (eq (set-window-display-table window 'sentinel)
                            'sentinel)
                        (eq (window-display-table window) 'sentinel)
                        (eq (set-window-cursor-type window t) t)
                        (null (set-window-display-table window nil))))
                     (let ((window (selected-window)))
                       (and
                        (let ((initial
                               (list (window-new-pixel window)
                                     (window-new-total window)
                                     (window-new-normal window)
                                     (window-normal-size window)
                                     (window-normal-size window t))))
                          (or (equal initial '(0 0 0 1.0 1.0))
                              (error "initial resize state differs: %S"
                                     initial)))
                        (= (set-window-new-pixel window 24) 24)
                        (= (set-window-new-pixel window -1 t) 23)
                        (= (set-window-new-pixel window 1 t) 24)
                        (= (set-window-new-total window 24) 24)
                        (= (set-window-new-total window -1 t) 23)
                        (= (set-window-new-total window 1 t) 24)
                        (eq (set-window-new-normal window 'pending) 'pending)
                        (eq (window-new-normal window) 'pending)
                        (window-resize-apply)
                        (window-resize-apply-total)
                        (null (set-window-new-normal window))
                        (null (window-new-normal window))))
                     (let* ((old (selected-window))
                            (new (split-window-internal old 12 nil 0.5))
                            (root (frame-root-window)))
                       (unwind-protect
                           (and
                            (window-valid-p root)
                            (not (window-live-p root))
                            (null (window-buffer root))
                            (eq (window-top-child root) old)
                            (null (window-left-child root))
                            (null (window-combination-limit root))
                            (eq (set-window-combination-limit root t) t)
                            (eq (window-combination-limit root) t)
                            (eq (window-parent old) root)
                            (eq (window-parent new) root)
                            (eq (window-next-sibling old) new)
                            (eq (window-prev-sibling new) old)
                            (= (window-pixel-height old) 12)
                            (= (window-pixel-height new) 12)
                            (= (window-pixel-top old) 0)
                            (= (window-pixel-top new) 12)
                            (= (length (window-list)) 2)
                            (eq (next-window old) new)
                            (eq (previous-window new) old)
                            (equal (window-buffer old) (window-buffer new))
                            (let (seen)
                              (let ((window-scroll-functions
                                     (list
                                      (lambda (window start)
                                        (setq seen
                                              (list window start
                                                    (eq (current-buffer)
                                                        (window-buffer
                                                         window))))))))
                                (run-window-scroll-functions new))
                              (equal seen (list new 1 t)))
                            (progn
                              (delete-window-internal new)
                              (and
                               (windowp new)
                               (not (window-valid-p new))
                               (not (window-live-p new))
                               (eq (frame-root-window) old)
                               (null (window-parent old))
                               (= (window-pixel-height old) 24)
                               (= (length (window-list)) 1))))
                         (when (window-valid-p new)
                           (delete-window-internal new))))
                     (let ((actual
                            (list temp-buffer-show-function
                                  minibuffer-scroll-window
                                  mode-line-in-non-selected-windows
                                  other-window-scroll-buffer
                                  other-window-scroll-default
                                  auto-window-vscroll
                                  next-screen-context-lines
                                  scroll-preserve-screen-position
                                  window-point-insertion-type
                                  window-buffer-change-functions
                                  window-size-change-functions
                                  window-selection-change-functions
                                  window-state-change-functions
                                  window-state-change-hook
                                  window-configuration-change-hook
                                  window-restore-killed-buffer-windows
                                  recenter-redisplay
                                  window-combination-resize
                                  window-combination-limit
                                  window-persistent-parameters
                                  window-resize-pixelwise
                                  fast-but-imprecise-scrolling)))
                       (or (equal actual
                                  '(nil nil t nil nil t 2 nil nil nil nil nil nil
                                    nil (window--adjust-process-windows) nil tty
                                    nil window-size
                                    ((context . writable) (clone-of . t))
                                    nil nil))
                           (error "dumped window defaults differ: %S" actual)))
                     (= (frame-char-height) 1)
                     (= (frame-right-divider-width) 0)
                     (= (frame-bottom-divider-width) 0)
                     (window-valid-p (selected-window))
                     (null
                      (delq nil
                            (list
                             (window-parent (selected-window))
                             (window-prev-sibling (selected-window))
                             (window-next-sibling (selected-window))
                             (window-top-child (selected-window))
                             (window-left-child (selected-window))))))"#,
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            // GNU's initial TTY root window excludes the minibuffer line.
            Value::Nil,
            Value::T,
            Value::Nil,
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
    let window_history_probe = eval_str_with(
        &mut interp,
        r#"(let ((original (current-buffer))
                 (one (get-buffer-create "emaxx-window-one"))
                 (two (get-buffer-create "emaxx-window-two")))
             (set-window-buffer nil one)
             (set-buffer two)
             (set-window-buffer nil two)
             (prog1
                 (buffer-name (caar (window-prev-buffers)))
               (set-window-buffer nil original)
               (set-buffer original)
               (set-window-prev-buffers nil nil)
               (set-window-next-buffers nil nil)
               (kill-buffer one)
               (kill-buffer two)))"#,
    );
    assert_eq!(
        window_history_probe,
        Value::String("emaxx-window-one".into())
    );
    interp.set_global_binding(
        "find-file-noselect-probe",
        Value::String(noselect_file.display().to_string().into()),
    );
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let ((current (current-buffer))
                     (shown (window-buffer)))
                 (let ((visited
                        (find-file-noselect find-file-noselect-probe)))
                   (prog1
                       (list (eq (current-buffer) current)
                             (eq (window-buffer) shown))
                     (kill-buffer visited))))"#,
        ),
        Value::list([Value::T, Value::T])
    );
    crate::lisp::load_file_strict(&mut interp, &test_file).expect("load copied Todo tests");
    let selector = Value::list([
        Value::symbol("member"),
        Value::symbol("todo-test-item-insertion-with-priority-1"),
        Value::symbol("todo-test-item-insertion-with-priority-2"),
        Value::symbol("todo-test-raise-lower-priority"),
        Value::symbol("todo-test-revert-buffer01"),
        Value::symbol("todo-test-todo-mark-unmark-category"),
    ]);
    let summary = interp.run_ert_tests_with_selector(Some(&selector));
    let results = format!("{:#?}", interp.test_results);
    assert_eq!(summary.total, 5, "{results}");
    assert_eq!(summary.passed, 5, "{results}");
    let todo_lock = todo_directory.join(".#todo-test-1.todo");
    let archive_lock = todo_directory.join(".#todo-test-1.toda");
    assert!(
        !todo_lock.exists() && !archive_lock.exists(),
        "Todo state tests left a resource lock"
    );
    let first_quit_selector = Value::list([
        Value::symbol("member"),
        Value::symbol("todo-test-todo-quit01"),
    ]);
    let first_quit_summary = interp.run_ert_tests_with_selector(Some(&first_quit_selector));
    let first_quit_results = format!("{:#?}", interp.test_results);
    assert_eq!(first_quit_summary.total, 1, "{first_quit_results}");
    assert_eq!(first_quit_summary.passed, 1, "{first_quit_results}");
    let live_buffers = eval_str_with(
        &mut interp,
        r#"(mapcar (lambda (buffer)
                    (with-current-buffer buffer
                      (list (buffer-name)
                            (buffer-file-name)
                            (buffer-modified-p))))
                  (buffer-list))"#,
    );
    assert!(
        !todo_lock.exists() && !archive_lock.exists(),
        "Todo quit left a resource lock; live buffers: {live_buffers:#?}"
    );
    let quit_selector = Value::list([
        Value::symbol("member"),
        Value::symbol("todo-test-todo-quit02"),
    ]);
    let quit_summary = interp.run_ert_tests_with_selector(Some(&quit_selector));
    let quit_results = format!("{:#?}", interp.test_results);
    let header_selector = Value::list([
        Value::symbol("member"),
        Value::symbol("todo-test-toggle-item-header06"),
    ]);
    let header_summary = interp.run_ert_tests_with_selector(Some(&header_selector));
    let header_results = format!("{:#?}", interp.test_results);
    std::fs::remove_dir_all(root).expect("remove copied Todo test tree");
    assert_eq!(quit_summary.total, 1, "{quit_results}");
    assert_eq!(quit_summary.passed, 1, "{quit_results}");
    assert_eq!(header_summary.total, 1, "{header_results}");
    assert_eq!(header_summary.passed, 1, "{header_results}");
}

#[test]
fn modified_state_transitions_and_buffer_kill_release_owned_file_locks() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("emaxx-lock-lifecycle-{unique}"));
    std::fs::create_dir_all(&root).expect("create lock lifecycle directory");
    let visited = root.join("visited.txt");
    let lock = root.join(".#visited.txt");
    std::fs::write(&visited, "original\n").expect("write visited file");

    let result = eval_str_with_upstream_batch(&format!(
        r#"(let ((path "{}")
                  buffer
                  locked-with-hidden-file-name
                  locked-before-restore
                  locked-after-restore
                  locked-before-kill
                  restore-result)
              (unwind-protect
                  (progn
                    (setq buffer (find-file-noselect path))
                    (with-current-buffer buffer
                      (goto-char (point-max))
                      (let ((buffer-file-name nil))
                        (insert "internal change"))
                      (setq locked-with-hidden-file-name
                            (file-exists-p (make-lock-file-name path)))
                      (restore-buffer-modified-p nil)
                      (insert "changed")
                      (setq locked-before-restore
                            (file-exists-p (make-lock-file-name path)))
                      (setq restore-result (restore-buffer-modified-p nil))
                      (setq locked-after-restore
                            (file-exists-p (make-lock-file-name path))))
                    (kill-buffer buffer)
                    (setq buffer (find-file-noselect path))
                    (with-current-buffer buffer
                      (goto-char (point-max))
                      (insert "changed again")
                      (setq locked-before-kill
                            (file-exists-p (make-lock-file-name path))))
                    (let ((original-yes-or-no-p
                           (symbol-function 'yes-or-no-p)))
                      (unwind-protect
                          (progn
                            (fset 'yes-or-no-p (lambda (&rest _) t))
                            (kill-buffer buffer))
                        (fset 'yes-or-no-p original-yes-or-no-p)))
                    (list locked-with-hidden-file-name
                          locked-before-restore
                          restore-result
                          locked-after-restore
                          locked-before-kill
                          (file-exists-p (make-lock-file-name path))
                          (buffer-live-p buffer)))
                (when (buffer-live-p buffer)
                  (with-current-buffer buffer
                    (restore-buffer-modified-p nil))
                  (kill-buffer buffer))))"#,
        visited.display()
    ));

    assert_eq!(
        result,
        Value::list([
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::Nil,
        ])
    );
    assert!(!lock.exists(), "owned lock survived buffer teardown");
    std::fs::remove_dir_all(root).expect("remove lock lifecycle directory");
}

#[test]
fn char_width_matches_string_width_for_single_characters() {
    assert_eq!(
        eval_str(
            "(let ((tab-width 4))
                   (list (char-width ?a)
                         (char-width ?\t)
                         (string-width \"\t\")
                         (char-width ?界)
                         (string-width \"界\")))"
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(4),
            Value::Integer(4),
            Value::Integer(2),
            Value::Integer(2),
        ])
    );
}

#[test]
fn truncate_string_to_width_uses_display_columns() {
    // GNU preloads international/mule-util.el, the Elisp owner of
    // `truncate-string-to-width', in its dumped image.
    assert_eq!(
        eval_str_with_upstream_batch(
            "(list (truncate-string-to-width \"abcdef\" 3)
                       (truncate-string-to-width \"界a\" 2)
                       (truncate-string-to-width \"a\" 3 0 ?.)
                       (truncate-string-to-width \"abcdef\" 4 2)
                       (truncate-string-to-width \"hun2\" 2 0 nil t)
                       (truncate-string-to-width \"hi\" 2 0 nil t))"
        ),
        Value::list([
            Value::String("abc".into()),
            Value::String("界".into()),
            Value::String("a..".into()),
            Value::String("cd".into()),
            Value::String("h…".into()),
            Value::String("hi".into()),
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
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        eval_str_with(&mut interp, "(require 'cl-extra)");
        eval_str_with(&mut interp, "(require 'cl-macs)");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                       (list (cl--class-name (cl-find-class 'fixnum))
                             (built-in-class-p (cl-find-class 'fixnum))
                             (eq (get 'fixnum 'cl--class)
                                 (cl-find-class 'fixnum))
                             (mapcar #'cl--class-name
                                     (cl--class-parents (cl-find-class 'fixnum)))
                             (cl--class-allparents (get 'fixnum 'cl--class))
                             (cl-typep 10 'fixnum)))"
            ),
            Value::list([
                Value::Symbol("fixnum".into()),
                Value::T,
                Value::T,
                Value::list([Value::Symbol("integer".into())]),
                Value::list([
                    Value::Symbol("fixnum".into()),
                    Value::Symbol("integer".into()),
                    Value::Symbol("number".into()),
                    Value::Symbol("integer-or-marker".into()),
                    Value::Symbol("number-or-marker".into()),
                    Value::Symbol("atom".into()),
                    Value::T,
                ]),
                Value::T,
            ])
        );
    });
}

#[test]
fn builtin_class_schema_matches_gnu_parentage_and_predicates() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch_feature(
                "cl-extra",
                "(progn
                   (require 'cl-extra)
                   (list
                    (mapcar #'cl--class-name
                            (cl--class-parents (cl-find-class 'null)))
                    (cl--class-allparents (cl-find-class 'null))
                    (mapcar #'cl--class-name
                            (cl--class-parents
                             (cl-find-class 'symbol-with-pos)))
                    (cl--class-allparents (cl-find-class 'symbol-with-pos))
                    (mapcar #'cl--class-name
                            (cl--class-parents
                             (cl-find-class 'primitive-function)))
                    (cl--class-allparents
                     (cl-find-class 'primitive-function))
                    (mapcar (lambda (class)
                              (get class 'cl-deftype-satisfies))
                            '(array integer-or-marker module-function
                              primitive-function sequence symbol-with-pos
                              user-ptr condvar finalizer))))"
            ),
            Value::list([
                Value::list([
                    Value::Symbol("boolean".into()),
                    Value::Symbol("list".into())
                ]),
                Value::list([
                    Value::Symbol("null".into()),
                    Value::Symbol("boolean".into()),
                    Value::Symbol("symbol".into()),
                    Value::Symbol("atom".into()),
                    Value::Symbol("list".into()),
                    Value::Symbol("sequence".into()),
                    Value::T,
                ]),
                Value::list([Value::Symbol("symbol".into())]),
                Value::list([
                    Value::Symbol("symbol-with-pos".into()),
                    Value::Symbol("symbol".into()),
                    Value::Symbol("atom".into()),
                    Value::T,
                ]),
                Value::list([
                    Value::Symbol("subr".into()),
                    Value::Symbol("compiled-function".into()),
                ]),
                Value::list([
                    Value::Symbol("primitive-function".into()),
                    Value::Symbol("subr".into()),
                    Value::Symbol("compiled-function".into()),
                    Value::Symbol("function".into()),
                    Value::Symbol("atom".into()),
                    Value::T,
                ]),
                Value::list([
                    Value::Symbol("arrayp".into()),
                    Value::Symbol("integer-or-marker-p".into()),
                    Value::Symbol("module-function-p".into()),
                    Value::Symbol("primitive-function-p".into()),
                    Value::Symbol("sequencep".into()),
                    Value::Symbol("symbol-with-pos-p".into()),
                    Value::Symbol("user-ptrp".into()),
                    Value::Nil,
                    Value::Nil,
                ]),
            ])
        );
    });
}

#[test]
fn macrop_recognizes_defined_and_autoloaded_macros() {
    run_with_large_stack(|| {
        let mut interp = gnu_early_lisp_interpreter();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                       (defmacro sample-live-macro () nil)
                       (defalias 'sample-alias-macro 'sample-live-macro)
                       (list
                        (macrop 'sample-live-macro)
                        (macrop (symbol-function 'sample-live-macro))
                        (macrop 'sample-alias-macro)
                        (progn
                          (autoload 'sample-auto-macro \"sample-auto\" nil nil 'macro)
                          (macrop 'sample-auto-macro))
                        (sample-alias-macro)
                        (macrop 'car)))"
            ),
            Value::list([
                Value::T,
                Value::T,
                Value::T,
                // GNU subr.el's `macrop' deliberately returns the matching
                // `(macro t)' tail from `memq' for an autoload object.
                Value::list([Value::symbol("macro"), Value::T]),
                Value::Nil,
                Value::Nil,
            ])
        );
    });
}

#[test]
fn apropos_internal_filters_symbols_by_regexp_and_predicate() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str_with_upstream_batch_feature(
                "rx",
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
        assert_eq!(
            eval_str_with_upstream_batch_feature(
                "custom",
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
        assert_eq!(
            eval_str(
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
fn advertised_calling_convention_is_keyed_by_function_identity() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                "(progn
                   (defun sample-old-adv-cc (arg) arg)
                   (set-advertised-calling-convention
                    'sample-old-adv-cc '(old-arg) \"31.1\")
                   (let ((old-function (symbol-function 'sample-old-adv-cc)))
                     (defun sample-old-adv-cc (replacement) replacement)
                     (list
                      (get-advertised-calling-convention old-function)
                      (get-advertised-calling-convention
                       (symbol-function 'sample-old-adv-cc)))))",
            ),
            Value::list([Value::list([Value::Symbol("old-arg".into())]), Value::T,])
        );
    });
}

#[test]
fn save_buffer_uses_the_dumped_files_el_policy() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((before (autoloadp (symbol-function 'save-buffer)))
                      (read-file-name-function
                       (lambda (&rest _ignore)
                         (error "Prompted for a file name")))
                      first-local)
                  (with-temp-buffer
                    (setq write-contents-functions (lambda () t))
                    (setq first-local
                          (local-variable-p 'write-contents-functions))
                    (set-buffer-modified-p t)
                    (save-buffer))
                  (with-temp-buffer
                    (set-buffer-modified-p t)
                    (list before
                          first-local
                          (buffer-modified-p)
                          write-contents-functions
                          (condition-case nil
                              (progn (save-buffer) 'did-not-prompt)
                            (error 'prompted))
                          write-contents-functions
                          buffer-file-name
                          (autoloadp (symbol-function 'save-buffer)))))
                "#
        ),
        Value::list([
            Value::Nil,
            Value::T,
            Value::T,
            Value::Nil,
            Value::Symbol("prompted".into()),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn write_region_visit_marks_the_buffer_saved() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((path (make-temp-file "emaxx-write-region-visit")))
                  (unwind-protect
                      (with-temp-buffer
                        (insert "saved")
                        (let ((result (write-region nil nil path nil t)))
                          (list result
                                (equal buffer-file-name path)
                                (buffer-modified-p)
                                (with-temp-buffer
                                  (insert-file-contents path)
                                  (buffer-string)))))
                    (delete-file path)))
                "#
        ),
        Value::list([
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::String("saved".into()),
        ])
    );
}

#[test]
fn replace_buffer_contents_preserves_matching_text_and_source_properties() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert #("source" 2 4 (prop 7)))
                  (let ((source (current-buffer)))
                    (with-temp-buffer
                      (insert "before dest after")
                      (let ((marker (set-marker (make-marker) 14)))
                        (save-restriction
                          (narrow-to-region 8 12)
                          (replace-buffer-contents source))
                        (list (buffer-string)
                              (marker-position marker)
                              (point)
                              (get-text-property 10 'prop))))))
                "#
        ),
        Value::list([
            Value::String("before source after".into()),
            Value::Integer(16),
            Value::Integer(9),
            Value::Integer(7),
        ])
    );
}

#[test]
fn fine_grained_revert_uses_native_non_destructive_buffer_replacement() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((path (make-temp-file "emaxx-fine-grain-revert")))
                  (unwind-protect
                      (with-temp-buffer
                        (insert "saved contents")
                        (write-file path)
                        (erase-buffer)
                        (insert "changed")
                        (let ((result (revert-buffer-with-fine-grain t t)))
                          (list result (buffer-string) (buffer-modified-p))))
                    (delete-file path)))
                "#
        ),
        Value::list([Value::T, Value::String("saved contents".into()), Value::Nil,])
    );
}

#[test]
fn compressed_file_visit_preserves_detected_coding_across_mode_and_save() {
    let resource = upstream_emacs_repo().join("test/lisp/files-resources/files-bug18141.el.gz");
    let resource = resource.display().to_string();
    assert_eq!(
        eval_str_with_upstream_batch(&format!(
            r#"
                (let ((path (make-temp-file "emaxx-bug-18141" nil ".gz")))
                  (unwind-protect
                      (progn
                        (copy-file "{resource}" path t)
                        (with-current-buffer (find-file-noselect path)
                          (let ((before buffer-file-coding-system)
                                (last-before last-coding-system-used))
                            (set-buffer-modified-p t)
                            (save-buffer)
                            (prog1
                                (list (subrp (indirect-function 'save-buffer))
                                      before last-before
                                      buffer-file-coding-system
                                      last-coding-system-used
                                      (buffer-modified-p)
                                      (buffer-live-p
                                       (get-buffer " *jka-compr-wr-temp*"))
                                      (with-current-buffer
                                          " *jka-compr-wr-temp*"
                                        (buffer-modified-p)))
                              (kill-buffer (current-buffer))))))
                    (delete-file path)))
                "#
        )),
        Value::list([
            Value::Nil,
            Value::Symbol("iso-2022-7bit-unix".into()),
            Value::Symbol("iso-2022-7bit-unix".into()),
            Value::Symbol("iso-2022-7bit-unix".into()),
            Value::Symbol("iso-2022-7bit-unix".into()),
            Value::Nil,
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn gnus_group_loads_against_dumped_simple_shell_state() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (require 'gnus-group)
                (gnus-short-group-name
                 "nnimap+email@example.com:archives/2020/03")
                "#
        ),
        Value::String("email@example:a/2/03".into())
    );
}

#[test]
fn pp_to_string_autoloads_from_clean_startup() {
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
                "(list (featurep 'pp) (pp-to-string '(a b)) (featurep 'pp))",
            ),
            Value::list([Value::Nil, Value::String("(a b)\n".into()), Value::T,])
        );
    });
}

#[test]
fn customize_set_value_autoloads_the_elisp_implementation() {
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
                "(progn (defvar sample-custom-value nil)\
                        (list (featurep 'cus-edit)\
                              (customize-set-value 'sample-custom-value 42)\
                              sample-custom-value\
                              (featurep 'cus-edit)))",
            ),
            Value::list([Value::Nil, Value::Integer(42), Value::Integer(42), Value::T,])
        );
    });
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

    let mut interp = gnu_early_lisp_interpreter();
    interp.set_load_path(vec![root.clone()]);
    eval_str_with(
        &mut interp,
        "(autoload 'sample-autoload \"sample-autoload\")",
    );
    assert_eq!(
        eval_str_with(&mut interp, "(funcall 'sample-autoload)"),
        Value::Integer(42)
    );
    eval_str_with(
        &mut interp,
        "(progn
           (fmakunbound 'sample-autoload)
           (autoload 'sample-autoload \"sample-autoload\"))",
    );
    assert_eq!(
        eval_str_with(&mut interp, "(sample-autoload)"),
        Value::Integer(42)
    );

    std::fs::remove_file(&target).unwrap();
    std::fs::remove_dir(&root).unwrap();
}

#[test]
fn function_get_autoloads_before_reading_declared_property() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-function-get-autoload-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("sample-function-property.el");
    std::fs::write(
        &target,
        "(put 'sample-function-property 'sample-property 'ready)\n\
         (defun sample-function-property () t)\n",
    )
    .unwrap();

    let mut interp = gnu_early_lisp_interpreter();
    interp.set_load_path(vec![root.clone()]);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (autoload 'sample-function-property \"sample-function-property\")
               (list
                (function-get 'sample-function-property
                              'sample-property 'macro)
                (autoloadp (symbol-function 'sample-function-property))
                (function-get 'sample-function-property
                              'sample-property t)
                (autoloadp (symbol-function 'sample-function-property))))"
        ),
        Value::list([
            Value::Nil,
            Value::T,
            Value::Symbol("ready".into()),
            Value::Nil,
        ])
    );

    std::fs::remove_file(&target).unwrap();
    std::fs::remove_dir(&root).unwrap();
}

#[test]
fn symbol_plist_is_the_live_mutable_property_list() {
    assert_eq!(
        eval_str(
            r#"
            (let* ((symbol (make-symbol "live-plist"))
                   (plist (list 'a 1 'b 2 'c 3)))
              (setplist symbol plist)
              (let ((same-before (eq plist (symbol-plist symbol))))
                (setcar (cdr plist) 9)
                (setcdr (cdr plist) (nthcdr 4 plist))
                (list same-before
                      (eq plist (symbol-plist symbol))
                      (get symbol 'a)
                      (get symbol 'b)
                      (symbol-plist symbol))))"#
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::Integer(9),
            Value::Nil,
            Value::list([
                Value::Symbol("a".into()),
                Value::Integer(9),
                Value::Symbol("c".into()),
                Value::Integer(3),
            ]),
        ])
    );
}

#[test]
fn ordered_global_and_property_indexes_survive_middle_removal() {
    let mut interp = Interpreter::new();
    for (name, value) in [
        ("emaxx-index-a", Value::Integer(1)),
        ("emaxx-index-b", Value::Integer(2)),
        ("emaxx-index-c", Value::Integer(3)),
    ] {
        interp.set_global_binding(name, value);
        interp.put_symbol_property(name, "sample", Value::T);
    }

    interp.remove_global_binding("emaxx-index-b");
    interp
        .set_symbol_plist("emaxx-index-b", Value::Nil)
        .unwrap();

    assert_eq!(
        interp.global_binding_value("emaxx-index-a"),
        Some(Value::Integer(1))
    );
    assert_eq!(interp.global_binding_value("emaxx-index-b"), None);
    assert_eq!(
        interp.global_binding_value("emaxx-index-c"),
        Some(Value::Integer(3))
    );
    assert_eq!(
        interp.get_symbol_property("emaxx-index-a", "sample"),
        Some(Value::T)
    );
    assert_eq!(interp.get_symbol_property("emaxx-index-b", "sample"), None);
    assert_eq!(
        interp.get_symbol_property("emaxx-index-c", "sample"),
        Some(Value::T)
    );

    interp.set_global_binding("emaxx-index-c", Value::Integer(30));
    interp.put_symbol_property("emaxx-index-c", "sample", Value::Integer(30));
    assert_eq!(
        interp.global_binding_value("emaxx-index-c"),
        Some(Value::Integer(30))
    );
    assert_eq!(
        interp.get_symbol_property("emaxx-index-c", "sample"),
        Some(Value::Integer(30))
    );
    assert_eq!(
        interp
            .globals
            .iter()
            .filter(|(name, _)| name.as_str() == "emaxx-index-c")
            .count(),
        1
    );
    assert_eq!(
        interp
            .symbol_properties
            .iter()
            .filter(|(name, _)| name == "emaxx-index-c")
            .count(),
        1
    );
}

#[test]
fn loaded_gnu_cl_remprop_mutates_non_head_live_plist_cells() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp =
            crate::batch::initialize_batch_interpreter(&options).expect("batch interpreter");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                (progn
                  (require 'cl-extra)
                  (let ((symbol (make-symbol "cl-remprop-live")))
                    (put symbol 'a 1)
                    (put symbol 'b 2)
                    (put symbol 'c 3)
                    (put symbol 'd 4)
                    (list
                     (cl-remprop symbol 'c)
                     (copy-sequence (symbol-plist symbol))
                     (cl-remprop symbol 'd)
                     (copy-sequence (symbol-plist symbol))
                     (cl-remprop symbol 'a)
                     (copy-sequence (symbol-plist symbol)))))"#
            ),
            Value::list([
                Value::T,
                Value::list([
                    Value::Symbol("a".into()),
                    Value::Integer(1),
                    Value::Symbol("b".into()),
                    Value::Integer(2),
                    Value::Symbol("d".into()),
                    Value::Integer(4),
                ]),
                Value::T,
                Value::list([
                    Value::Symbol("a".into()),
                    Value::Integer(1),
                    Value::Symbol("b".into()),
                    Value::Integer(2),
                ]),
                Value::T,
                Value::list([Value::Symbol("b".into()), Value::Integer(2)]),
            ])
        );
    });
}

#[test]
fn eager_macroexpansion_treats_setf_method_names_as_definition_syntax() {
    run_with_large_stack(|| {
        let mut bare = Interpreter::new();
        assert_eq!(
            eval_str_with(&mut bare, "(get 'cl-no-method 'error-conditions)"),
            Value::Nil,
            "the C-only runtime must not pre-seed cl-generic.el's conditions"
        );

        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp =
            crate::batch::initialize_batch_interpreter(&options).expect("batch interpreter");
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(list (get 'cl-no-method 'error-conditions)
                       (get 'cl-no-next-method 'error-conditions))"
            ),
            Value::list([
                Value::list([
                    Value::Symbol("cl-no-method".into()),
                    Value::Symbol("error".into()),
                ]),
                Value::list([
                    Value::Symbol("cl-no-next-method".into()),
                    Value::Symbol("cl-no-method".into()),
                    Value::Symbol("error".into()),
                ]),
            ]),
            "the reconstructed GNU image must obtain conditions from cl-generic.el"
        );

        eval_str_with(
            &mut interp,
            r#"
            (progn
              (setq lexical-binding t)
              (require 'ert)
              (require 'gv)
              (cl-defgeneric (setf emaxx-eager-method-place) (value object))
              (ert-deftest emaxx-eager-setf-method-name ()
                (cl-defmethod (setf emaxx-eager-method-place)
                    (value (object t))
                  (list value object))
                (should
                 (equal (setf (emaxx-eager-method-place 'target) 'stored)
                        '(stored target)))))"#,
        );

        let summary = interp.run_ert_tests_with_selector(Some(&Value::Symbol(
            "emaxx-eager-setf-method-name".into(),
        )));
        assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn autoloaded_handler_function_quote_resolves_on_dispatch() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-autoload-handler-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let target = root.join("sample-autoload-handler.el");
    std::fs::write(
        &target,
        "(defun sample-autoload-handler (err) (throw 'handled err))\n",
    )
    .unwrap();

    let mut interp = gnu_early_lisp_interpreter();
    interp.set_load_path(vec![root.clone()]);
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (autoload 'sample-autoload-handler \"sample-autoload-handler\")
               (catch 'handled
                 (handler-bind ((error #'sample-autoload-handler))
                   (error \"boom\"))))"
        ),
        Value::list([Value::Symbol("error".into()), Value::String("boom".into())])
    );

    std::fs::remove_file(&target).unwrap();
    std::fs::remove_dir(&root).unwrap();
}

#[test]
fn preloaded_eval_defun_evaluates_current_definition() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (with-temp-buffer
                 (insert \"(defun sample-eval-defun () 42)\\n\")
                 (goto-char (point-min))
                 (re-search-forward \"sample-eval-defun\")
                 (eval-defun nil))
               (sample-eval-defun))"
        ),
        Value::Integer(42)
    );
}

#[test]
fn defmacro_source_docstring_does_not_hide_following_declarations() {
    assert_eq!(
        eval_str(
            "(progn
               (defmacro sample-declared-macro (form)
                 \"A source docstring with reader identity.\"
                 (declare (debug (form)))
                 form)
               (get 'sample-declared-macro 'edebug-form-spec))"
        ),
        Value::list([Value::Symbol("form".into())])
    );
}

#[test]
fn edebug_instrumentation_generates_unique_nested_definition_names() {
    let emacs_repo = upstream_emacs_repo();
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&emacs_repo)
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("initialize batch interpreter");
    interp.load_target("ert").expect("load ERT");
    interp
        .load_target(
            emacs_repo
                .join("test/lisp/emacs-lisp/edebug-tests.el")
                .to_str()
                .expect("UTF-8 test path"),
        )
        .expect("load edebug tests");
    let cl_flet_spec = eval_str_with(&mut interp, "(get 'cl-flet 'edebug-form-spec)");
    assert!(cl_flet_spec.is_truthy(), "missing cl-flet Edebug spec");
    let summary =
        interp.run_ert_tests_with_selector(Some(&Value::Symbol("edebug-tests-cl-flet".into())));
    assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
    assert_eq!(summary.passed, 1, "{:#?}", interp.test_results);
    assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
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

        let mut interp = gnu_early_lisp_interpreter();
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

        let mut interp = gnu_early_lisp_interpreter();
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

        let mut interp = gnu_early_lisp_interpreter();
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

    let mut interp = gnu_early_lisp_interpreter();
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
fn list_tail_functions_treat_nil_as_default_and_nbutlast_mutates() {
    assert_eq!(
        eval_str(
            "(let* ((original (list 'a 'b 'c))
                    (copy (butlast original nil))
                    (mutated (list 'a 'b 'c))
                    (result (nbutlast mutated nil)))
               (list copy
                     (eq copy original)
                     result
                     (eq result mutated)
                     mutated
                     (last '(a . b) -1)
                     (nbutlast (list 'a 'b) 2)))"
        ),
        Value::list([
            Value::list([Value::Symbol("a".into()), Value::Symbol("b".into())]),
            Value::Nil,
            Value::list([Value::Symbol("a".into()), Value::Symbol("b".into())]),
            Value::T,
            Value::list([Value::Symbol("a".into()), Value::Symbol("b".into())]),
            Value::Nil,
            Value::Nil,
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
        load_gnu_batch_runtime(&mut interp);
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
        eval_str_with_upstream_batch_features(
            &["macroexp", "cl-macs"],
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
        eval_str_with_upstream_batch_feature(
            "macroexp",
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
        eval_str_with_upstream_batch_feature(
            "disp-table",
            "(let ((table (make-display-table))) \
                   (list (char-table-p table) (char-table-subtype table)))"
        ),
        Value::list([Value::T, Value::Symbol("display-table".into())])
    );
}

#[test]
fn translate_region_uses_char_tables() {
    // `translate-region' is mule.el's preloaded Elisp wrapper around the
    // native `translate-region-internal' (editfns.c).
    let value = eval_str_with_upstream_batch(
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
fn preloaded_point_to_register_owner_is_fboundp() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    assert_eq!(
        eval_str_with(&mut interp, "(fboundp 'point-to-register)"),
        Value::T
    );
}

#[test]
fn preloaded_point_to_register_stores_the_current_location() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let ((register-alist nil))
                     (with-temp-buffer
                       (insert "abc")
                       (goto-char 2)
                       (point-to-register ?a)
                       (marker-position (get-register ?a))))"#
        ),
        Value::Integer(2)
    );
}

#[test]
fn preloaded_command_line_1_processes_command_switch_alist() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let expr = format!(
        "(progn \
               (find-file {path:?}) \
               (rename-buffer \" foo\") \
               (list-buffers) \
               (with-current-buffer \"*Buffer List*\" \
                 (buffer-name (Buffer-menu-buffer))))",
        path = target.display().to_string()
    );
    assert_string_value(eval_str_with_upstream_batch(&expr), " foo");

    std::fs::remove_file(&target).unwrap();
    std::fs::remove_dir(&root).unwrap();
}

#[test]
fn list_buffers_keeps_file_visiting_internal_names_addressable() {
    run_large_stack_test(assert_list_buffers_keeps_file_visiting_internal_names_addressable);
}

#[test]
fn buffer_name_reads_the_live_name_from_an_existing_buffer_object() {
    assert_eq!(
        eval_str(
            "(let ((buffer (generate-new-buffer \"emaxx-old-name\")))
               (unwind-protect
                   (progn
                     (with-current-buffer buffer
                       (rename-buffer \"emaxx-new-name\"))
                     (let ((live-name (buffer-name buffer)))
                       (kill-buffer buffer)
                       (list live-name (buffer-name buffer))))
                 (when (buffer-live-p buffer)
                   (kill-buffer buffer))))"
        ),
        Value::list([Value::String("emaxx-new-name".into()), Value::Nil])
    );
}

#[test]
fn rename_buffer_notifies_the_preloaded_uniquify_owner() {
    assert_eq!(
        eval_str(
            "(let (notification)
               (defun uniquify--rename-buffer-advice (requested unique)
                 (setq notification (list requested unique)))
               (with-temp-buffer
                 (list (rename-buffer \"emaxx-renamed\" 'unique)
                       notification)))"
        ),
        Value::list([
            Value::String("emaxx-renamed".into()),
            Value::list([
                Value::String("emaxx-renamed".into()),
                Value::Symbol("unique".into()),
            ]),
        ])
    );
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
fn selected_gnu_elc_never_falls_back_to_its_sibling_source() {
    run_with_large_stack(|| {
        let root = std::env::temp_dir().join(format!(
            "emaxx-gnu-elc-fallback-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let source = root.join("sample.el");
        let compiled = root.join("sample.elc");
        std::fs::write(
            &source,
            ";;; -*- lexical-binding: t -*-\n\
             (defalias 'sample-function (function (lambda () 'source)))\n\
             (provide 'sample)\n",
        )
        .unwrap();
        std::fs::write(
            &compiled,
            b";ELC\x1e\0\0\0\n(defalias 'sample-function #[0 \"\\300\\207\" [compiled] 1])\n(provide 'sample)\n",
        )
        .unwrap();

        let mut source_interp = Interpreter::new();
        source_interp.set_load_path(vec![root.clone()]);
        assert_eq!(source_interp.load_target("sample").unwrap(), source);
        assert_eq!(
            eval_str_with(&mut source_interp, "(sample-function)"),
            Value::symbol("source")
        );

        let mut preferred_interp = Interpreter::new();
        preferred_interp.set_prefer_compiled_loads(true);
        preferred_interp.set_load_path(vec![root.clone()]);
        assert_eq!(preferred_interp.load_target("sample").unwrap(), compiled);
        assert_eq!(
            eval_str_with(&mut preferred_interp, "(sample-function)"),
            Value::symbol("compiled")
        );

        // Explicit `.elc' names execute that file regardless of the resolver's
        // default suffix preference.
        let mut compiled_interp = Interpreter::new();
        compiled_interp.set_load_path(vec![root.clone()]);
        assert_eq!(compiled_interp.load_target("sample.elc").unwrap(), compiled);
        assert_eq!(
            eval_str_with(&mut compiled_interp, "(sample-function)"),
            Value::symbol("compiled")
        );

        std::fs::remove_file(root.join("sample.el")).unwrap();
        std::fs::remove_file(root.join("sample.elc")).unwrap();
        std::fs::remove_dir(&root).unwrap();
    });
}

#[test]
fn headered_textual_elc_executes_instead_of_its_empty_source_stub() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-headered-textual-elc-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let source = root.join("sample.el");
    let compiled = root.join("sample.elc");
    std::fs::write(&source, "").unwrap();
    std::fs::write(
        &compiled,
        b";ELC\x1e\0\0\0\n;;; Compiled\n(provide 'headered-textual-sample)\n",
    )
    .unwrap();

    let mut interp = Interpreter::new();
    interp.set_load_path(vec![root.clone()]);
    assert_eq!(interp.load_target("sample").unwrap(), compiled);
    assert!(interp.has_feature("headered-textual-sample"));

    std::fs::remove_file(source).unwrap();
    std::fs::remove_file(root.join("sample.elc")).unwrap();
    std::fs::remove_dir(root).unwrap();
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
fn load_file_strict_scopes_lexical_binding_to_the_loaded_file() {
    run_with_large_stack(|| {
        let path = std::env::temp_dir().join(format!(
            "emaxx-lexical-binding-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            ";;; lexical-cookie -*- lexical-binding: t -*-\n(if lexical-binding nil (error \"missing lexical binding\"))\n(provide 'sample)\n",
        )
        .unwrap();

        let mut interp = Interpreter::new();
        crate::lisp::load_file_strict(&mut interp, &path).unwrap();
        assert_eq!(
            interp.lookup_var("lexical-binding", &Vec::new()),
            Some(Value::Nil)
        );

        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn load_file_strict_scopes_bare_defvar_and_macro_dynvars_to_one_lexical_file() {
    run_with_large_stack(|| {
        let root = std::env::temp_dir().join(format!(
            "emaxx-file-local-defvar-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let owner = root.join("owner.el");
        let unrelated = root.join("unrelated.el");
        std::fs::write(
            &owner,
            ";;; -*- lexical-binding: t -*-\n\
             (defvar sample-file-local-special)\n\
             (defalias 'sample-file-local-reader\n\
               (function (lambda () sample-file-local-special)))\n\
             (defalias 'sample-file-local-call\n\
               (function\n\
                (lambda (value)\n\
                  (let ((sample-file-local-special value))\n\
                    (sample-file-local-reader)))))\n\
             (defalias 'sample-file-local-macro\n\
               (cons 'macro\n\
                     (function\n\
                      (lambda (&rest ignored)\n\
                        (list 'quote macroexp--dynvars)))))\n\
             (setq sample-file-local-macro-result (sample-file-local-macro))\n",
        )
        .unwrap();
        std::fs::write(
            &unrelated,
            ";;; -*- lexical-binding: t -*-\n\
             (defalias 'sample-unrelated-lexical-maker\n\
               (function\n\
                (lambda (value)\n\
                  (let ((sample-file-local-special value))\n\
                    (function (lambda () sample-file-local-special))))))\n",
        )
        .unwrap();

        let mut interp = Interpreter::new();
        crate::lisp::load_file_strict(&mut interp, &owner).unwrap();
        crate::lisp::load_file_strict(&mut interp, &unrelated).unwrap();
        let metadata = eval_str_with(
            &mut interp,
            "(list
               (special-variable-p 'sample-file-local-special)
               (aref (symbol-function 'sample-file-local-reader) 2)
               (aref (symbol-function 'sample-file-local-call) 2)
               sample-file-local-macro-result
               macroexp--dynvars
               (aref (symbol-function 'sample-unrelated-lexical-maker) 2))",
        );
        assert_eq!(
            metadata,
            Value::list([
                Value::Nil,
                Value::list([Value::symbol("sample-file-local-special"), Value::T,]),
                Value::list([Value::symbol("sample-file-local-special"), Value::T,]),
                Value::list([Value::T, Value::symbol("sample-file-local-special"),]),
                Value::Nil,
                Value::list([Value::T]),
            ])
        );

        let result = eval_str_with(
            &mut interp,
            "(list
               (sample-file-local-call 42)
               (funcall (sample-unrelated-lexical-maker 84)))",
        );
        assert_eq!(
            result,
            Value::list([Value::Integer(42), Value::Integer(84)])
        );

        std::fs::remove_dir_all(root).unwrap();
    });
}

#[test]
fn load_file_strict_prebinds_current_load_list() {
    run_with_large_stack(|| {
        let path = std::env::temp_dir().join(format!(
            "emaxx-current-load-list-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            "(setq sample-current-load-entry (car current-load-list))\n",
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
    });
}

#[test]
fn source_load_records_require_for_an_already_loaded_feature() {
    run_with_large_stack(|| {
        let path = std::env::temp_dir().join(format!(
            "emaxx-require-load-history-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            "(eval-when-compile (require 'cl-preloaded))\n\
             (require 'emacs)\n\
             (provide 'sample-requirer)\n",
        )
        .unwrap();

        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        crate::lisp::load_file_strict(&mut interp, &path).unwrap();
        // GNU records a `require' under the loading file even when the
        // feature is already present, and even when the call happens inside
        // `eval-when-compile's load-time evaluation: direct GNU 30.2 probes
        // of this fixture return both members.
        assert_eq!(
            eval_str_with(
                &mut interp,
                &format!(
                    "(let ((entry (assoc {path:?} load-history)))
                       (list (and (member '(require . emacs) entry) t)
                             (and (member '(require . cl-preloaded) entry) t)))"
                )
            ),
            Value::list([Value::T, Value::T])
        );

        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn source_definitions_restore_autoloads_without_stale_function_cells() {
    run_with_large_stack(|| {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let autoload_path =
            std::env::temp_dir().join(format!("emaxx-function-history-autoload-{nonce}.el"));
        let definition_path =
            std::env::temp_dir().join(format!("emaxx-function-history-definition-{nonce}.el"));
        std::fs::write(
            &autoload_path,
            "(autoload 'emaxx-history-probe \"emaxx-history-probe\")\n",
        )
        .unwrap();
        std::fs::write(
            &definition_path,
            "(defun emaxx-history-probe () :defined)\n",
        )
        .unwrap();

        let mut interp = gnu_early_lisp_interpreter();
        crate::lisp::load_file_strict(&mut interp, &autoload_path).unwrap();
        crate::lisp::load_file_strict(&mut interp, &definition_path).unwrap();
        assert_eq!(
            eval_str_with(
                &mut interp,
                &format!(
                    "(let ((history (get 'emaxx-history-probe 'function-history)))
                       (list (equal (car history) {definition_path:?})
                             (autoloadp (cadr history))
                             (not (null (member '(defun . emaxx-history-probe)
                                                (assoc {autoload_path:?} load-history))))))"
                )
            ),
            Value::list([Value::T, Value::T, Value::T])
        );

        // This is the two unload paths in loadhist-unload-element: restore
        // the hidden autoload, then void it when its owning file unloads.
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(let ((history (get 'emaxx-history-probe 'function-history)))
                   (defalias 'emaxx-history-probe (cadr history))
                   (put 'emaxx-history-probe 'function-history (cddr history))
                   (defalias 'emaxx-history-probe nil)
                   (fboundp 'emaxx-history-probe))"
            ),
            Value::Nil
        );

        std::fs::remove_file(autoload_path).unwrap();
        std::fs::remove_file(definition_path).unwrap();
    });
}

#[test]
fn symbol_file_finds_defun_recorded_by_source_load() {
    run_with_large_stack(|| {
        let path = std::env::temp_dir().join(format!(
            "emaxx-symbol-file-defun-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "(defun emaxx-loaded-source-probe () t)\n").unwrap();

        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        crate::lisp::load_file_strict(&mut interp, &path).unwrap();
        assert_string_value(
            eval_str_with(
                &mut interp,
                "(symbol-file 'emaxx-loaded-source-probe 'defun)",
            ),
            &path.display().to_string(),
        );

        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn load_file_strict_records_cl_defmethod_files() {
    run_with_large_stack(|| {
        let path = std::env::temp_dir().join(format!(
            "sample-cl-defmethod-load-history-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            "(cl-defgeneric sample-load-method (x))\n\
             (cl-defmethod sample-load-method (x) x)\n\
             (cl-defmethod sample-load-method ((x string)) x)\n\
             (cl-defmethod sample-load-method ((x integer)) x)\n",
        )
        .unwrap();

        let path = std::fs::canonicalize(path).expect("canonicalize method fixture");
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        eval_str_with(&mut interp, "(require 'cl-generic)");
        crate::lisp::load_file_strict(&mut interp, &path).unwrap();
        assert_eq!(
            eval_str_with(
                &mut interp,
                &format!(
                    "(let ((files (cl--generic-method-files 'sample-load-method))\
                           (path {path:?}))\
                       (and (equal (length files) 3)\
                            (equal (symbol-file 'sample-load-method 'defun) path)\
                            (equal (symbol-file
                                    '(sample-load-method nil string)
                                    'cl-defmethod)
                                   path)\
                            (equal (mapcar #'car files) (list path path path))\
                            (equal (mapcar #'cadr files)\
                                   '(sample-load-method sample-load-method
                                     sample-load-method))\
                            (equal (mapcar #'cddr files)\
                                   '((nil integer) (nil string) (nil t)))))"
                )
            ),
            Value::T
        );

        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn cl_generic_describe_prints_quoted_eql_specializers() {
    run_with_large_stack(|| {
        let path = std::env::temp_dir().join(format!(
            "sample-cl-generic-describe-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            "(cl-defgeneric sample-describe-method (function))\n\
             (cl-defmethod sample-describe-method ((function (eql '4))) (+ function 1))\n",
        )
        .unwrap();

        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        eval_str_with(&mut interp, "(require 'cl-generic)");
        crate::lisp::load_file_strict(&mut interp, &path).unwrap();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(with-temp-buffer
                   (cl--generic-describe 'sample-describe-method)
                   (list (not (re-search-forward \"#'\" nil t))
                         (progn
                           (goto-char (point-min))
                           (not (null (re-search-forward \"(eql '4)\" nil t))))))"
            ),
            Value::list([Value::T, Value::T])
        );

        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn load_in_progress_is_truthy_while_loading_files() {
    run_with_large_stack(|| {
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
    });
}

#[test]
fn batch_dump_purify_flag_defaults_to_nil() {
    assert_eq!(eval_str("purify-flag"), Value::Nil);
}

#[test]
fn require_final_newline_matches_batch_default() {
    assert_eq!(eval_str("require-final-newline"), Value::Nil);
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
fn sentence_end_double_space_defaults_to_t() {
    assert_eq!(eval_str("sentence-end-double-space"), Value::T);
}

#[test]
fn core_definition_forms_have_doc_string_slots() {
    assert_eq!(
        eval_str(
            "(list (function-get 'defun 'doc-string-elt)
                   (function-get 'defmacro 'doc-string-elt)
                   (function-get 'defvar 'doc-string-elt)
                   (function-get 'define-category 'doc-string-elt))"
        ),
        Value::list([
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(2),
        ])
    );
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
    assert_eq!(eval_str("use-hard-newlines"), Value::Nil);
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
        Value::list([Value::String(String::new().into())])
    );
}

#[test]
fn debug_on_error_defaults_to_nil_in_batch() {
    assert_eq!(eval_str("debug-on-error"), Value::Nil);
    assert_eq!(eval_str("eval-expression-debug-on-error"), Value::T);
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let found = eval_str_with(
        &mut interp,
        &format!(
            "(locate-file \"sample\" '(\"{dir_text}\") '(\".el\" \".txt\")
                          (lambda (path) (string-suffix-p \".txt\" path)))"
        ),
    );
    assert_eq!(found, Value::String(accepted.display().to_string().into()));
    std::fs::remove_file(rejected).unwrap();
    std::fs::remove_file(accepted).unwrap();
    std::fs::remove_dir(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn locate_file_access_predicates_cover_public_and_internal_paths() {
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();

    assert_eq!(
        eval_str_with(
            &mut interp,
            &format!("(locate-file \"sample-tool\" '(\"{dir_text}\") '(\"\") 'executable)"),
        ),
        Value::String(script.display().to_string().into())
    );
    assert_eq!(
        eval_str_with(
            &mut interp,
            &format!("(locate-file-internal \"sample-tool\" '(\"{dir_text}\") '(\"\") 1)"),
        ),
        Value::String(script.display().to_string().into())
    );

    std::fs::remove_file(script).unwrap();
    std::fs::remove_dir(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn executable_find_observes_dynamic_exec_path_and_empty_path_entries() {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!(
        "emaxx-executable-find-{}",
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
    let directory = primitives::path_to_directory_string(&dir);
    let expected = Value::String(script.display().to_string().into());

    assert_eq!(
        eval_str_with_upstream_batch(&format!(
            r#"
                (load "files")
                (list
                  (let ((exec-path '("{directory}")))
                    (executable-find "sample-tool"))
                  (let ((default-directory "{directory}")
                        (exec-path nil))
                    (executable-find "sample-tool")))
                "#
        )),
        Value::list([expected.clone(), expected])
    );
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (require 'ert)
                (require 'ert-x)
                (require 'nadvice)
                (require 'cl-lib)
                (require 'bytecomp)
                (require 'dired)
                (require 'filenotify)
                (load "../emacs/test/lisp/files-tests.el")
                (ert-with-temp-file tmpfile
                  :suffix (car exec-suffixes)
                  (set-file-modes tmpfile #o755)
                  (list
                   (let ((exec-path `(,temporary-file-directory)))
                     (equal tmpfile
                            (executable-find
                             (file-name-nondirectory tmpfile))))
                   (let ((default-directory temporary-file-directory)
                         (exec-path nil))
                     (equal tmpfile
                            (executable-find
                             (file-name-nondirectory tmpfile))))
                   (let ((default-directory "/ssh::")
                         (exec-path
                          (append exec-path
                                  `("." ,temporary-file-directory))))
                     (equal tmpfile
                            (executable-find
                             (file-name-nondirectory tmpfile))))))
                "#
        ),
        Value::list([Value::T, Value::T, Value::T])
    );

    std::fs::remove_file(script).unwrap();
    std::fs::remove_dir(dir).unwrap();
}

#[test]
fn quoted_default_directory_preserves_shell_and_file_process_output() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((directory (make-temp-file "emaxx-process-directory" t)))
                  (unwind-protect
                      (list
                       (with-temp-buffer
                         (let ((default-directory
                                (file-name-quote
                                 (file-name-as-directory directory))))
                           (shell-command "printf 30.2.0" (current-buffer))
                           (string= (buffer-string) "30.2.0")))
                       (with-temp-buffer
                         (let* ((default-directory
                                 (file-name-quote
                                  (file-name-as-directory directory)))
                                (process
                                 (start-file-process
                                  "emaxx-process-output" (current-buffer)
                                  "/bin/echo" "30.2.0")))
                           (unwind-protect
                               (progn
                                 (accept-process-output process)
                                 (string-match-p "30\\.2\\.0" (buffer-string)))
                             (set-process-query-on-exit-flag process nil)
                             (delete-process process))))
                       (with-temp-buffer
                         (let ((default-directory
                                (file-name-quote
                                 (concat (file-name-as-directory directory)
                                         "missing/"))))
                           (condition-case nil
                               (progn
                                 (shell-command "printf unexpected"
                                 (current-buffer))
                                 nil)
                             (error t)))))
                    (delete-directory directory t)))
                "#
        ),
        Value::list([Value::T, Value::Integer(0), Value::T])
    );
}

#[test]
fn defcustom_rejects_non_keyword_property_forms_after_evaluating_them() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "custom",
            "(let (evaluated)
               (condition-case err
                   (defcustom sample-custom-value 1
                     \"doc\"
                     :type 'integer
                     (setq evaluated 'loaded))
                 (error
                  (list evaluated
                        (boundp 'sample-custom-value)
                        (car err)))))"
        ),
        Value::list([Value::symbol("loaded"), Value::Nil, Value::symbol("error")])
    );
}

#[test]
fn load_file_strict_preserves_original_load_errors() {
    run_with_large_stack(|| {
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
    });
}

#[test]
fn generic_record_reader_forms_evaluate_to_literal_records() {
    let mut interp = Interpreter::new();
    let value = eval_str_with(&mut interp, "#s(#s(a b) c)");
    let Value::Record(id) = value else {
        panic!("expected a record literal");
    };
    let record = interp.find_record(id).expect("record state");
    let Value::Record(type_id) = record.type_tag else {
        panic!("GNU preserves the nested record as the exact type descriptor");
    };
    assert_eq!(record.slots, vec![Value::Symbol("c".into())]);
    let descriptor = interp.find_record(type_id).expect("type descriptor record");
    assert_eq!(descriptor.type_tag, Value::symbol("a"));
    assert_eq!(descriptor.slots, vec![Value::symbol("b")]);
}

#[test]
fn record_primitives_preserve_arbitrary_type_descriptors() {
    assert_eq!(
        eval_str(
            r#"(let* ((tag (record 'a 'b))
                       (value (record tag 'c))
                       (copy (copy-sequence value)))
                  (list (type-of value)
                        (cl-type-of value)
                        (type-of (aref value 0))
                        (aref (aref value 0) 1)
                        (equal value copy)
                        (eq (aref value 0) (aref copy 0))
                        (prin1-to-string value)))"#,
        ),
        Value::list([
            Value::symbol("b"),
            Value::symbol("b"),
            Value::symbol("a"),
            Value::symbol("b"),
            Value::T,
            Value::T,
            Value::string("#s(#s(a b) c)"),
        ])
    );
}

#[test]
fn make_record_and_aset_preserve_arbitrary_type_descriptors() {
    assert_eq!(
        eval_str(
            r#"(let* ((tag (record 'a 'b))
                       (value (make-record tag 2 'z))
                       (replacement (record 'c 'd)))
                  (aset value 0 replacement)
                  (list (type-of value)
                        (cl-type-of value)
                        (aref value 1)
                        (aref value 2)
                        (prin1-to-string value)))"#,
        ),
        Value::list([
            Value::symbol("d"),
            Value::symbol("d"),
            Value::symbol("z"),
            Value::symbol("z"),
            Value::string("#s(#s(c d) z z)"),
        ])
    );
}

#[test]
fn quoted_bytecode_reader_forms_are_materialized_as_records() {
    let mut interp = Interpreter::new();
    let value = eval_str_with(&mut interp, r#"'(macro #[0 "\300\207" [nil] 1])"#);
    let items = value.to_vec().expect("quoted macro list");

    assert_eq!(items.first(), Some(&Value::Symbol("macro".into())));
    assert!(matches!(items.get(1), Some(Value::Record(_))));
}

#[test]
fn compiled_interactive_metadata_is_shared_by_command_queries() {
    assert_eq!(
        eval_str(
            r#"(let ((fn #[257 "\300\207" [nil] 1 nil "P"]))
                  (list (commandp fn) (interactive-form fn)))"#
        ),
        Value::list([
            Value::T,
            Value::list([Value::symbol("interactive"), Value::String("P".into())]),
        ])
    );
}

#[test]
fn native_face_variables_exist_before_lisp_libraries_load() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list (internal-get-lisp-face-attribute 'default :foreground nil)
                   (internal-get-lisp-face-attribute 'default :background nil)
                   (internal-get-lisp-face-attribute 'default :foreground t)
                   (internal-get-lisp-face-attribute 'default :background t))"
        ),
        Value::list([
            Value::String("unspecified-fg".into()),
            Value::String("unspecified-bg".into()),
            Value::Symbol("unspecified".into()),
            Value::Symbol("unspecified".into()),
        ])
    );
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list (hash-table-p face--new-frame-defaults)\n\
                   (gethash 'default face--new-frame-defaults)\n\
                   face-filters-always-match face-default-stipple\n\
                   scalable-fonts-allowed face-ignored-fonts\n\
                   face-remapping-alist face-font-rescale-alist\n\
                   face-near-same-color-threshold\n\
                   face-font-lax-matched-attributes)"
        )
        .to_vec()
        .expect("face variable result list")[0],
        Value::T
    );
    let values = eval_str_with(
        &mut interp,
        "(list (car (gethash 'default face--new-frame-defaults))\n\
               face-filters-always-match face-default-stipple\n\
               scalable-fonts-allowed face-ignored-fonts\n\
               face-remapping-alist face-font-rescale-alist\n\
               face-near-same-color-threshold\n\
               face-font-lax-matched-attributes)",
    );
    assert_eq!(
        values,
        Value::list([
            Value::Integer(0),
            Value::Nil,
            Value::String("gray3".into()),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Integer(30_000),
            Value::T,
        ])
    );
    let entry = eval_str_with(
        &mut interp,
        "(progn (internal-make-lisp-face 'emaxx-test-face nil)\n\
                (gethash 'emaxx-test-face face--new-frame-defaults))",
    );
    let (id, vector) = entry.cons_values().expect("new face defaults entry");
    assert_eq!(id, Value::Integer(1));
    assert!(crate::lisp::primitives::vector_items(&vector).is_ok());
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
fn equal_compares_circular_cons_graphs() {
    assert_eq!(
        eval_str(
            r##"
                (let* ((read-circle t)
                       (x (car (read-from-string "#1=(a #1#)")))
                       (y (car (read-from-string "#1=(a #1#)")))
                       (z (car (read-from-string "#1=(b #1#)"))))
                  (list (equal x x) (equal x y) (equal x z)))
                "##,
        ),
        Value::list([Value::T, Value::T, Value::Nil])
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
fn prin1_to_string_uses_gnu_closure_reader_syntax_for_byte_code() {
    assert_eq!(
        eval_str(
            "(let ((printed (prin1-to-string (make-byte-code 0 \"\" [] 0))))
               (list (substring printed 0 2) (substring printed -1)))"
        ),
        Value::list([Value::String("#[".into()), Value::String("]".into())])
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
        load_gnu_batch_runtime(&mut interp);

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
                 (subr-name (symbol-function 'if))
                 ;; GNU's subr.el owns `dlet' as a macro, so `subr-arity'
                 ;; signals wrong-type-argument with the subrp predicate and
                 ;; the macro object itself.
                 (condition-case err
                     (subr-arity (symbol-function 'dlet))
                   (error (list (car err)
                                (cadr err)
                                (eq (nth 2 err) (symbol-function 'dlet))))))
                "##,
        ),
        Value::list([
            Value::cons(Value::Integer(2), Value::Symbol("unevalled".into())),
            Value::String("if".into()),
            Value::list([
                Value::Symbol("wrong-type-argument".into()),
                Value::Symbol("subrp".into()),
                Value::T,
            ]),
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
fn subr_arity_reports_regular_builtin_functions() {
    assert_eq!(
        eval_str("(subr-arity (symbol-function 'identity))"),
        Value::cons(Value::Integer(1), Value::Integer(1))
    );
}

#[test]
fn subr_arity_uses_the_generated_gnu_native_manifest() {
    assert_eq!(
        eval_str(
            "(mapcar (lambda (function)
                       (subr-arity (symbol-function function)))
                     '(get-buffer-create
                       modify-category-entry
                       define-category
                       category-docstring
                       copy-category-table))"
        ),
        Value::list([
            Value::cons(Value::Integer(1), Value::Integer(2)),
            Value::cons(Value::Integer(2), Value::Integer(4)),
            Value::cons(Value::Integer(2), Value::Integer(3)),
            Value::cons(Value::Integer(1), Value::Integer(2)),
            Value::cons(Value::Integer(0), Value::Integer(1)),
        ])
    );
}

#[test]
fn commandp_uses_the_generated_gnu_native_interactive_manifest() {
    assert_eq!(
        eval_str(
            "(list
               (commandp 'self-insert-command)
               (equal
                (interactive-form #'self-insert-command)
                '(interactive
                  (list (prefix-numeric-value current-prefix-arg)
                        last-command-event))))"
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn prefix_numeric_value_accepts_every_lisp_object_like_gnu() {
    assert_eq!(
        eval_str(
            r#"
              (list (prefix-numeric-value nil)
                    (prefix-numeric-value '-)
                    (prefix-numeric-value 'sqltest)
                    (prefix-numeric-value 7)
                    (prefix-numeric-value '(4 5))
                    (prefix-numeric-value '(4 . tail))
                    (prefix-numeric-value '(symbol))
                    (prefix-numeric-value 1.5)
                    (prefix-numeric-value "x")
                    (prefix-numeric-value 1000000000000000000000000000000)
                    (prefix-numeric-value
                     '(1000000000000000000000000000000)))
            "#,
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(-1),
            Value::Integer(1),
            Value::Integer(7),
            Value::Integer(4),
            Value::Integer(4),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
        ])
    );
}

#[test]
fn string_affix_helpers_preserve_subr_el_length_short_circuits() {
    assert_eq!(
        eval_str(
            r#"
              (list (string-prefix-p " " nil)
                    (string-suffix-p " " nil)
                    (string-prefix-p '(not-a-string) nil)
                    (string-suffix-p '(not-a-string) nil)
                    (string-prefix-p "AB" "abc" t)
                    (string-suffix-p "BC" "abc" t)
                    (string-prefix-p "abcd" "abc")
                    (string-suffix-p "abcd" "abc"))
            "#,
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn cl_assert_signals_condition_with_asserted_form() {
    let _permit = crate::test_support::acquire_host_test_permit();
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    eval_str_with(&mut interp, "(require 'cl-macs)");
    let form = Reader::new("(cl-assert lexical-binding)")
        .read()
        .unwrap()
        .unwrap();
    interp.set_variable("lexical-binding", Value::Nil, &mut Vec::new());
    let error = interp.eval(&form, &mut Vec::new()).unwrap_err();
    let LispError::SignalValue(value) = error else {
        panic!("expected cl assertion signal");
    };
    assert_eq!(
        value,
        Value::list([
            Value::Symbol("cl-assertion-failed".into()),
            Value::Symbol("lexical-binding".into())
        ])
    );
}

#[test]
fn load_file_strict_keeps_lexical_binding_for_cl_iter_defun() {
    run_with_large_stack(|| {
        let path = std::env::temp_dir().join(format!(
            "sample-cl-iter-defun-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            ";;; -*- lexical-binding: t -*-\n(require 'cl-macs)\n(require 'generator)\n(cl-iter-defun sample-cl-iter-defun ()\n  (:documentation (concat \"sample\"))\n  (iter-yield 'ok))\n",
        )
        .unwrap();

        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let result = crate::lisp::load_file_strict(&mut interp, &path);
        let _ = std::fs::remove_file(path);
        result.unwrap();
    });
}

#[test]
fn lexical_iter_defun_keeps_dolist_variables_in_value_position() {
    run_with_large_stack(|| {
        let path = std::env::temp_dir().join(format!(
            "emaxx-iter-defun-dolist-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            ";;; -*- lexical-binding: t -*-\n(require 'generator)\n(iter-defun sample-iter-defun (items)\n  (dolist (elem items)\n    (when (listp elem)\n      (iter-yield elem))))\n",
        )
        .unwrap();

        let mut interp = Interpreter::new();
        interp.set_load_path(vec![
            std::path::PathBuf::from("../emacs/lisp"),
            std::path::PathBuf::from("../emacs/lisp/emacs-lisp"),
        ]);
        crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
        let result = crate::lisp::load_file_strict(&mut interp, &path);
        let _ = std::fs::remove_file(path);
        result.unwrap();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(let ((iterator (sample-iter-defun '(skip (one) (two)))))\n                   (list (iter-next iterator) (iter-next iterator)))"
            ),
            Value::list([
                Value::list([Value::Symbol("one".into())]),
                Value::list([Value::Symbol("two".into())]),
            ])
        );
    });
}

#[test]
fn load_file_strict_preserves_outer_lexical_binding_and_restores_default() {
    run_with_large_stack(|| {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("emaxx-nested-lexical-binding-{unique}"));
        std::fs::create_dir_all(&root).unwrap();
        let inner = root.join("inner-lexical.el");
        let outer = root.join("outer-lexical.el");
        std::fs::write(
            &inner,
            ";;; -*- lexical-binding: nil -*-\n(provide 'inner-lexical)\n",
        )
        .unwrap();
        std::fs::write(
            &outer,
            ";;; -*- lexical-binding: t -*-\n(require 'cl-lib)\n(require 'generator)\n(require 'inner-lexical)\n(cl-iter-defun sample-nested-cl-iter-defun ()\n  (iter-yield 'ok))\n",
        )
        .unwrap();

        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        interp.set_variable("lexical-binding", Value::Nil, &mut Vec::new());
        let mut load_path = interp.configured_load_path().to_vec();
        load_path.insert(0, root.clone());
        interp.set_load_path(load_path);
        let result = crate::lisp::load_file_strict(&mut interp, &outer);
        let _ = std::fs::remove_dir_all(&root);
        result.unwrap();
        assert_eq!(
            interp.lookup_var("lexical-binding", &Vec::new()),
            Some(Value::Nil)
        );
    });
}

#[test]
fn lexical_ert_body_keeps_macro_context_in_its_temporary_buffer() {
    run_with_large_stack(|| {
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        eval_str_with(&mut interp, "(require 'cl-macs)");
        eval_str_with(&mut interp, "(require 'ert)");
        interp.set_variable("lexical-binding", Value::T, &mut Vec::new());
        eval_str_with(
            &mut interp,
            r#"(eval
                 '(progn
                    (defmacro emaxx-lexical-ert-probe ()
                      (cl-assert lexical-binding)
                      t)
                    (ert-deftest emaxx-lexical-ert-context ()
                      (should lexical-binding)
                      (should (emaxx-lexical-ert-probe))))
                 t)"#,
        );

        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.total, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "{:#?}", interp.test_results);
        assert_eq!(summary.failed, 0, "{:#?}", interp.test_results);
    });
}

#[test]
fn evaluated_cl_macrolet_expands_nested_lambda_bodies_before_scope_exit() {
    assert_eq!(
        eval_str_with_upstream_batch_feature(
            "cl-macs",
            r#"(let (function)
                 (cl-macrolet ((local-value () ''expanded))
                   (setq function (lambda () (local-value))))
                 (funcall function))"#
        ),
        Value::symbol("expanded")
    );
}

#[test]
fn mode_reset_detaches_active_buffer_local_special_bindings_like_gnu() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (defvar-local emaxx-test-mode-reset-a 'global)
               (defvar-local emaxx-test-mode-reset-b 'global)
               (list
                (with-temp-buffer
                  (setq-local emaxx-test-mode-reset-a 'local)
                  (list
                   (let ((emaxx-test-mode-reset-a 'bound))
                     (kill-all-local-variables)
                     (list emaxx-test-mode-reset-a
                           (local-variable-p 'emaxx-test-mode-reset-a)))
                   emaxx-test-mode-reset-a
                   (local-variable-p 'emaxx-test-mode-reset-a)))
                (with-temp-buffer
                  (setq-local emaxx-test-mode-reset-b 'local)
                  (list
                   (let ((emaxx-test-mode-reset-b 'bound))
                     (kill-all-local-variables)
                     (setq emaxx-test-mode-reset-b 'new)
                     (list emaxx-test-mode-reset-b
                           (local-variable-p 'emaxx-test-mode-reset-b)))
                   emaxx-test-mode-reset-b
                   (local-variable-p 'emaxx-test-mode-reset-b)))))"
        ),
        Value::list([
            Value::list([
                Value::list([Value::symbol("global"), Value::Nil]),
                Value::symbol("global"),
                Value::Nil,
            ]),
            Value::list([
                Value::list([Value::symbol("new"), Value::T]),
                Value::symbol("local"),
                Value::T,
            ]),
        ])
    );
}

#[test]
fn mode_reset_honors_permanent_locals_and_kill_permanent_argument() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (defvar-local emaxx-test-permanent-reset-a 'global)
               (defvar-local emaxx-test-permanent-reset-b 'global)
               (put 'emaxx-test-permanent-reset-a 'permanent-local t)
               (put 'emaxx-test-permanent-reset-b 'permanent-local t)
               (list
                (with-temp-buffer
                  (setq-local emaxx-test-permanent-reset-a 'local)
                  (let ((emaxx-test-permanent-reset-a 'bound))
                    (kill-all-local-variables)
                    (list emaxx-test-permanent-reset-a
                          (local-variable-p 'emaxx-test-permanent-reset-a))))
                (with-temp-buffer
                  (setq-local emaxx-test-permanent-reset-b 'local)
                  (list
                   (let ((emaxx-test-permanent-reset-b 'bound))
                     (kill-all-local-variables t)
                     (list emaxx-test-permanent-reset-b
                           (local-variable-p 'emaxx-test-permanent-reset-b)))
                   emaxx-test-permanent-reset-b
                   (local-variable-p 'emaxx-test-permanent-reset-b)))))"
        ),
        Value::list([
            Value::list([Value::symbol("bound"), Value::T]),
            Value::list([
                Value::list([Value::symbol("global"), Value::Nil]),
                Value::symbol("global"),
                Value::Nil,
            ]),
        ])
    );
}

#[test]
fn mode_reset_notifies_watchers_before_removing_even_permanent_locals() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (defvar-local emaxx-test-watched-reset 'global)
               (put 'emaxx-test-watched-reset 'permanent-local t)
               (let (events)
                 (with-temp-buffer
                   (add-variable-watcher
                    'emaxx-test-watched-reset
                    (lambda (_symbol new operation where)
                      (push (list new operation (eq where (current-buffer))) events)))
                   (setq-local emaxx-test-watched-reset 'local)
                   (setq events nil)
                   (kill-all-local-variables)
                   (list emaxx-test-watched-reset (nreverse events)))))"
        ),
        Value::list([
            Value::symbol("local"),
            Value::list([Value::list([
                Value::Nil,
                Value::symbol("makunbound"),
                Value::T,
            ])]),
        ])
    );
}

#[test]
fn cconv_closure_convert_captures_simple_free_variable() {
    run_with_large_stack(|| {
        let result = eval_str_with_upstream_batch_feature(
            "cconv",
            r#"
            (progn
              (setq lexical-binding t)
              (require 'bytecomp)
              (defun sample-cconv-intern-all (x)
                (cond ((symbolp x) (intern (symbol-name x)))
                      ((consp x) (cons (sample-cconv-intern-all (car x))
                                       (sample-cconv-intern-all (cdr x))))
                      (t x)))
              (sample-cconv-intern-all
               (cconv-closure-convert '#'(lambda (x) #'(lambda () x)))))
            "#,
        );

        assert_eq!(
            result,
            eval_str(
                "'#'(lambda (x) (internal-make-closure nil (x) nil (internal-get-closed-var 0)))"
            )
        );
    });
}

#[test]
fn loaded_gnu_cl_generic_method_keeps_generic_documentation_public() {
    run_with_large_stack(|| {
        // This checks cl-generic's public behavior, not source bootstrapping.
        // Reuse the faithfully reconstructed compiled GNU batch image so the
        // test executes the same GNU Elisp owner without repeatedly paying
        // the unrelated source-load cost.
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        eval_str_with(&mut interp, "(require 'cl-generic)");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                (progn
                  (setq lexical-binding t)
                  (require 'cl-lib)
                  (fmakunbound 'sample-loaded-generic)
                  (setplist 'sample-loaded-generic nil)
                  (cl-defgeneric sample-loaded-generic (n)
                    (:documentation (concat "loaded generic" " documentation")))
                  (cl-defmethod sample-loaded-generic ((n integer))
                    (:documentation "loaded method documentation")
                    (1+ n))
                  (require 'help-fns)
                  (let ((description
                         (describe-function 'sample-loaded-generic)))
                    (list
                     (and (string-match-p "loaded generic documentation"
                                          description)
                          t)
                     (and (string-match-p "loaded method documentation"
                                          description)
                          t)
                        (sample-loaded-generic 10)
                        (and
                         (string-match-p
                          "\\`loaded generic documentation"
                          (documentation 'sample-loaded-generic))
                         t))))"#
            ),
            Value::list([Value::T, Value::T, Value::Integer(11), Value::T,])
        );
    });
}

#[test]
fn cconv_closure_convert_remaps_shadowed_lambda_lifted_variable() {
    run_with_large_stack(|| {
        let result = eval_str_with_upstream_batch_feature(
            "cconv",
            r#"
            (progn
              (setq lexical-binding t)
              (require 'bytecomp)
              (defun sample-cconv-intern-all (x)
                (cond ((symbolp x) (intern (symbol-name x)))
                      ((consp x) (cons (sample-cconv-intern-all (car x))
                                       (sample-cconv-intern-all (cdr x))))
                      (t x)))
              (sample-cconv-intern-all
               (cconv-closure-convert
                '#'(lambda (x)
                     (let ((f #'(lambda () x)))
                       (let ((x 'b))
                         (list x (funcall f))))))))
            "#,
        );

        assert_eq!(
            result,
            eval_str(
                "'#'(lambda (x)
                      (let ((f #'(lambda (x) x)))
                        (let ((x 'b)
                              (closed-x x))
                          (list x (funcall f closed-x)))))"
            )
        );
    });
}

#[test]
fn cconv_analyze_keeps_simple_captured_argument_unmutated() {
    run_with_large_stack(|| {
        let result = eval_str_with_upstream_batch_feature(
            "cconv",
            r#"
            (progn
              (setq lexical-binding t)
              (require 'bytecomp)
              (defvar cconv-var-classification nil)
              (defvar cconv-freevars-alist nil)
              (let ((cconv-var-classification nil)
                    (byte-compile-lexical-variables nil)
                    (cconv--interactive-form-funs (make-hash-table))
                    cconv-freevars-alist cconv--dynbound-variables)
                (cconv-analyze-form '#'(lambda (x) #'(lambda () x)) nil)
                cconv-var-classification))
            "#,
        );

        assert_eq!(result, Value::Nil);
    });
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
