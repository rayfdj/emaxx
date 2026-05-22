use super::*;

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
        eval_str(
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
fn defgroup_tracks_current_group_and_members() {
    let mut interp = Interpreter::new();
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
        eval_str(
            "(progn
               (defgroup sample-custom-parent nil \"Doc.\")
               (defcustom sample-custom-versioned nil \"Doc.\"
                 :type 'boolean
                 :version \"31.1\"
                 :group 'sample-custom-parent)
               (list (equal (get 'sample-custom-versioned 'custom-version) \"31.1\")
                     (get 'sample-custom-parent 'custom-group)
                     custom-versions-load-alist))"
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
fn mapatoms_scans_standard_obarray_symbols() {
    assert_eq!(
        eval_str(
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
fn batch_window_hscroll_defaults_to_zero() {
    assert_eq!(eval_str("(window-hscroll)"), Value::Integer(0));
}

#[test]
fn dolist_with_progress_reporter_uses_dolist_semantics() {
    assert_eq!(
        eval_str(
            "(let ((seen nil) (reporter nil))
               (dolist-with-progress-reporter (item '(1 2 3) (nreverse seen))
                   (setq reporter 'evaluated)
                 (push (list reporter item) seen)))"
        ),
        Value::list([
            Value::list([Value::symbol("evaluated"), Value::Integer(1)]),
            Value::list([Value::symbol("evaluated"), Value::Integer(2)]),
            Value::list([Value::symbol("evaluated"), Value::Integer(3)]),
        ])
    );
}

#[test]
fn cl_letf_rebinds_symbol_property_get_places() {
    assert_eq!(
        eval_str(
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
        eval_str(
            r#"
                (progn
                  (defcustom sample-setopt-number 0 "Doc." :type 'number)
                  (with-current-buffer (get-buffer-create "*Warnings*")
                    (let ((inhibit-read-only t))
                      (erase-buffer))
                    (setopt sample-setopt-number :bad)
                    (string-search "Value `:bad' does not match type number"
                                   (buffer-string))))"#
        ),
        Value::Integer(9)
    );
}

#[test]
fn cl_with_gensyms_produces_unique_bindings() {
    assert_eq!(
        eval_str(
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
        eval_str(
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
        eval_str(
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
fn cl_case_rejects_misplaced_otherwise() {
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let form = Reader::new("(cl-case 'zip (otherwise 'fallback) (zip 'hit))")
        .read()
        .unwrap()
        .unwrap();
    let error = interp.eval(&form, &mut env).unwrap_err();
    assert_eq!(error.condition_type(), "error");
    assert_eq!(error.to_string(), "Misplaced t or `otherwise' clause");
}

#[test]
fn sqlite_execute_surfaces_sql_input_errors_as_sqlite_error() {
    assert_eq!(
        eval_str(
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
fn backtrace_frames_from_current_thread_returns_live_frames() {
    let mut interp = Interpreter::new();
    let current_thread = interp.current_thread_value();
    interp.push_backtrace_frame(Value::Symbol("sample-backtrace-frame".into()), Vec::new());

    assert_eq!(
        interp.thread_backtrace_frames_snapshot(interp.resolve_thread_id(&current_thread).unwrap()),
        vec![(
            Value::Symbol("sample-backtrace-frame".into()),
            Vec::new(),
            false,
        )]
    );
}
