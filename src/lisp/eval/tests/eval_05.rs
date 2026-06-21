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
fn coding_system_type_reports_known_coding_kind() {
    assert_eq!(
        eval_str(
            "(list (coding-system-type nil) (coding-system-type 'utf-8) (coding-system-type 'raw-text))"
        ),
        Value::list([
            Value::Nil,
            Value::symbol("utf-8"),
            Value::symbol("raw-text"),
        ])
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
fn run_mode_hooks_preserves_builtin_definition() {
    assert_eq!(
        eval_str(
            "(progn (defun run-mode-hooks (&rest _) 'shadowed) (run-mode-hooks 'sample-hook))"
        ),
        Value::Nil
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
        eval_str(
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
        eval_str("(list (hack-local-variables) (hack-local-variables 'no-mode))"),
        Value::list([Value::Nil, Value::Nil])
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
        eval_str(
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
fn throw_from_handler_inside_function_reaches_matching_catch() {
    assert_eq!(
        eval_str(
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
    let result = eval_str(
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
fn standard_minibuffer_completion_map_is_bound() {
    let result = eval_str(
        "(list (boundp 'minibuffer-local-completion-map)
               (keymapp minibuffer-local-completion-map))",
    );
    assert_eq!(result, Value::list([Value::T, Value::T]));
}

#[test]
fn completion_style_defaults_are_bound() {
    let result = eval_str(
        "(list (boundp 'completion-styles)
               (not (null (memq 'basic completion-styles)))
               (not (null (assq 'basic completion-styles-alist))))",
    );
    assert_eq!(result, Value::list([Value::T, Value::T, Value::T]));
}

#[test]
fn file_expand_wildcards_returns_existing_matches() {
    let result = eval_str(
        r#"(let ((dir (make-temp-file "emaxx-wildcards-" t)))
             (unwind-protect
                 (progn
                   (make-empty-file (expand-file-name "a.el" dir))
                   (make-empty-file (expand-file-name "b.txt" dir))
                   (let ((matches (file-expand-wildcards
                                   (expand-file-name "*.el" dir)
                                   t)))
                     (list (= (length matches) 1)
                           (file-name-absolute-p (car matches))
                           (not (null (string-match-p "a\\.el\\'" (car matches)))))))
               (delete-directory dir t)))"#,
    );
    assert_eq!(result, Value::list([Value::T, Value::T, Value::T]));
}

#[test]
fn mock_tramp_file_operations_use_localname() {
    let result = eval_str(
        r#"(let ((dir (make-temp-file "emaxx-mock-tramp-" t)))
             (unwind-protect
                 (let* ((remote (concat "/mock::" dir))
                        (remote-file (expand-file-name "sample.txt" remote)))
                   (write-region "sample" nil remote-file)
                   (let ((copy (file-local-copy remote-file)))
                     (prog1
                         (list (file-remote-p remote)
                               (file-directory-p remote)
                               (file-writable-p remote)
                               (and copy
                                    (not (file-remote-p copy))
                                    (file-exists-p copy)))
                       (when copy
                         (delete-file copy)))))
               (delete-directory dir t)))"#,
    );
    assert_eq!(
        result,
        Value::list([
            Value::String("/mock::".into()),
            Value::T,
            Value::T,
            Value::T
        ])
    );
}

#[test]
fn cl_loop_across_iterates_vectors() {
    assert_eq!(
        eval_str("(cl-loop for item across [a b c] collect item)"),
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
        eval_str("(cl-loop for item in '(1 2 3 4) when (> item 2) collect item)"),
        Value::list([Value::Integer(3), Value::Integer(4)])
    );
}

#[test]
fn cl_loop_when_append_flattens_truthy_results() {
    assert_eq!(
        eval_str(
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
        eval_str(
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
        let mut interp = Interpreter::new();
        interp.set_load_path(
            crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
        );
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (require 'cl-lib)
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
        eval_str(
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
fn prog_mode_is_callable_and_derived_from_fundamental_mode() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer (prog-mode) (list major-mode mode-name (derived-mode-p 'prog-mode 'fundamental-mode)))"
        ),
        Value::list([
            Value::Symbol("prog-mode".into()),
            Value::String("Prog".into()),
            Value::T,
        ])
    );
}

#[test]
fn ruby_mode_marks_single_quotes_as_string_delimiters() {
    assert_eq!(
        eval_str(
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
        eval_str(
            "(with-temp-buffer (funcall tex-mode) (equal (list major-mode comment-start) '(tex-mode \"%\")))"
        ),
        Value::T
    );
}

#[test]
fn text_mode_marks_quotes_as_text_punctuation() {
    assert_eq!(
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
            "(let ((file (make-temp-file \"emaxx-no-byte-compile\" nil \".el\")))
               (unwind-protect
                   (progn
                     (write-region \";; -*- no-byte-compile: t; lexical-binding: t; -*-\\n\" nil file nil 'silent)
                     (let ((buf (find-file-noselect file)))
                       (with-current-buffer buf
                         (normal-mode)
                         (prog1 no-byte-compile
                           (kill-buffer buf)))))
                 (ignore-errors (delete-file file))))"
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
        eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(
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
fn backtrace_expand_ellipses_reprints_current_frame_without_limit() {
    assert_eq!(
        eval_str_with_upstream_load_path(
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
fn comment_region_wraps_c_style_and_prefixes_hash_comments() {
    assert_eq!(
        eval_str(
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
        eval_str(
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
        eval_str(
            "(with-temp-buffer (funcall 'c-mode) (insert \" (())) \") (syntax-ppss (point-max)))"
        ),
        Value::list([
            Value::Integer(-1),
            Value::Nil,
            Value::Nil,
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
fn c_toggle_electric_state_updates_c_electric_flag() {
    assert_eq!(
        eval_str(
            "(progn
              (setq c-electric-flag t)
              (c-toggle-electric-state -1)
              (prog1 c-electric-flag
                (c-toggle-electric-state 1)))"
        ),
        Value::Nil
    );
    assert_eq!(
        eval_str("(progn (setq c-electric-flag nil) (c-toggle-electric-state 1) c-electric-flag)"),
        Value::T
    );
}

#[test]
fn self_insert_command_uses_last_command_event_and_runs_hook() {
    assert_eq!(
        eval_str(
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
fn execute_kbd_macro_self_insert_binding_sets_last_command_event() {
    assert_eq!(
        eval_str("(with-temp-buffer (execute-kbd-macro (kbd \"SPC\")) (buffer-string))"),
        Value::String(" ".into())
    );
}

#[test]
fn return_key_defaults_to_newline_command() {
    assert_eq!(
        eval_str("(key-binding [?\r])"),
        Value::Symbol("newline".into())
    );
}

#[test]
fn c_toggle_comment_style_switches_between_block_and_line_comments() {
    assert_eq!(
        eval_str(
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
        eval_str(
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
        eval_str("(list (fboundp 'syntax-ppss-flush-cache) (syntax-ppss-flush-cache (point-min)))"),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn ppss_depth_returns_syntax_ppss_depth() {
    assert_eq!(
        eval_str("(with-temp-buffer (insert \"(a (b))\") (ppss-depth (syntax-ppss 6)))"),
        Value::Integer(2)
    );
}

#[test]
fn syntax_ppss_reports_string_start() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer (c-mode) (insert \"\\\"<>\\\"\") (list (nth 3 (syntax-ppss 3)) (nth 8 (syntax-ppss 3))))"
        ),
        Value::list([Value::Integer('"' as i64), Value::Integer(1)])
    );
}

#[test]
fn syntax_ppss_reports_hash_comment_start() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer (python-mode) (insert \"# <>\\n\") (list (nth 4 (syntax-ppss 3)) (nth 8 (syntax-ppss 3))))"
        ),
        Value::list([Value::T, Value::Integer(1)])
    );
}

#[test]
fn syntax_ppss_reports_open_paren_stack() {
    assert_eq!(
        eval_str("(with-temp-buffer (insert \"(a (b))\") (nth 9 (syntax-ppss 6)))"),
        Value::list([Value::Integer(1), Value::Integer(4)])
    );
}

#[test]
fn scan_sexps_signals_premature_close_with_position() {
    assert_eq!(
        eval_str(
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
        eval_str(
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
        eval_str(
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
fn syntax_ppss_drops_mismatched_opener_from_stack() {
    assert_eq!(
        eval_str("(with-temp-buffer (c-mode) (insert \"  (])  \") (nth 9 (syntax-ppss 5)))"),
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
        eval_str(
            "(with-temp-buffer (insert \"foo\") (goto-char 1) (mark-sexp 1) (list (point) (mark) (use-region-p)))"
        ),
        Value::list([Value::Integer(1), Value::Integer(4), Value::T])
    );
}

#[test]
fn mark_sexp_stops_before_closing_string_quote() {
    assert_eq!(
        eval_str(
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
        eval_str(
            "(with-temp-buffer (insert \"foo\") (goto-char 3) (backward-delete-char-untabify 1) (buffer-string))"
        ),
        Value::String("fo".into())
    );
}

#[test]
fn define_minor_mode_variable_option_toggles_backing_variable() {
    assert_eq!(
        eval_str(
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
fn define_global_minor_mode_init_value_runs_body() {
    assert_eq!(
        eval_str(
            "(progn
              (define-minor-mode electric-indent-mode \"doc\"
                :global t
                :init-value t
                (setq sample-init-mode-body-ran t))
              (list electric-indent-mode sample-init-mode-body-ran))"
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn atomic_change_group_evaluates_body() {
    assert_eq!(
        eval_str("(let ((x 1)) (atomic-change-group (setq x 2) (+ x 3)))"),
        Value::Integer(5)
    );
}

#[test]
fn atomic_change_group_rolls_back_on_throw() {
    assert_eq!(
        eval_str(
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
        eval_str("(let ((items (list 'head 'body))) (push 'neck (nthcdr 1 items)) items)"),
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
        eval_str("(let ((items (list (cons 'old 'tail)))) (setf (car (car items)) 'new) items)"),
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
        eval_str("(let ((i 0)) (cl-loop while (< i 3) collect (setq i (1+ i))))"),
        Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3)])
    );
}

#[test]
fn cl_loop_collect_into_finally_return() {
    assert_eq!(
        eval_str(
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
        eval_str("(cl-loop for x in (list 1 2 3 4 5) vconcat (vector (1+ x)))"),
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
        eval_str(
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
fn cl_loop_if_do_append_runs_body_before_append() {
    assert_eq!(
        eval_str(
            "(cl-loop for item in '(1 nil 3)
                      if item
                      do (setq item (1+ item))
                      append (list item item))",
        ),
        Value::list([
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(4),
            Value::Integer(4),
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
fn make_obsolete_rejects_nil_and_t_names() {
    assert_eq!(
        eval_str(
            "(list
               (condition-case err (make-obsolete nil 'sample-new \"31.1\") (wrong-type-argument (car err)))
               (condition-case err (make-obsolete t 'sample-new \"31.1\") (wrong-type-argument (car err))))"
        ),
        Value::list([
            Value::symbol("wrong-type-argument"),
            Value::symbol("wrong-type-argument"),
        ])
    );
}

#[test]
fn make_obsolete_variable_rejects_nil_and_t_names() {
    assert_eq!(
        eval_str(
            "(list
               (condition-case err (make-obsolete-variable nil 'sample-new \"31.1\") (wrong-type-argument (car err)))
               (condition-case err (make-obsolete-variable t 'sample-new \"31.1\") (wrong-type-argument (car err))))"
        ),
        Value::list([
            Value::symbol("wrong-type-argument"),
            Value::symbol("wrong-type-argument"),
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
fn dnd_multiple_url_handlers_prefer_earlier_equal_precedence_handler() {
    let mut interp = Interpreter::new();
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
fn dired_shell_command_confirmation_positions_match_upstream() {
    assert_eq!(
        eval_str_with_upstream_load_path(
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
        eval_str_with_upstream_load_path(&expr),
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
        eval_str_with_upstream_load_path(&expr),
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
    let result = eval_str_with_upstream_load_path(&expr);
    assert!(result.is_truthy(), "{result:?}");
    std::fs::remove_dir_all(&dir).expect("cleanup temp dir");
}

#[test]
fn dired_highlights_unsubstituted_shell_metacharacters() {
    assert_eq!(
        eval_str_with_upstream_load_path(
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
    let result = eval_str_with_upstream_load_path(
        r#"(progn
             (setq noninteractive t)
             (require 'dired)
             (ert-with-temp-directory test-dir
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
    let result = eval_str_with_upstream_load_path(
        r#"(progn
             (setq noninteractive t)
             (require 'dired)
             (ert-with-temp-directory test-dir
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
    let result = eval_str_with_upstream_load_path(
        r#"(progn
             (setq noninteractive t)
             (require 'dired)
             (ert-with-temp-directory top-dir
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
    let result = eval_str(
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
    let result = eval_str(
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
fn insert_directory_free_space_uses_target_directory() {
    let result = eval_str(
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
                       (insert-directory target "-l" nil nil)
                       (let ((output (buffer-string)))
                         (list (string-match-p "available 10 B" output)
                               (string-match-p "available 100 B" output))))))
               (delete-directory target t)
               (delete-directory other t)))"#,
    );
    assert_eq!(result, Value::list([Value::Integer(0), Value::Nil]));
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
            true,
            Value::Symbol("sample-backtrace-frame".into()),
            Vec::new(),
            false,
        )]
    );
}

#[test]
fn ert_x_remote_temp_directory_loads_after_tramp() {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    interp.set_variable("noninteractive", Value::T, &mut Vec::new());
    interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
              (progn
                (require 'tramp)
                (require 'ert-x)
                (list (featurep 'ert-x)
                      (file-remote-p ert-remote-temporary-file-directory)
                      (file-directory-p ert-remote-temporary-file-directory)
                      (file-writable-p ert-remote-temporary-file-directory)))
            "#
        ),
        Value::list([
            Value::T,
            Value::String("/mock::".into()),
            Value::T,
            Value::T
        ])
    );
}
