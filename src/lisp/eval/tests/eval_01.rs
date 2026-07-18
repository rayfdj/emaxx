use super::*;

#[test]
fn eval_atoms() {
    assert_eq!(eval_str("42"), Value::Integer(42));
    assert_eq!(eval_str("\"hello\""), Value::String("hello".into()));
    assert_eq!(eval_str("nil"), Value::Nil);
    assert_eq!(eval_str("t"), Value::T);
}

#[test]
fn unwind_protect_cleanup_nonlocal_exit_supersedes_the_protected_result() {
    assert_eq!(
        eval_str(
            r#"(let (cleaned log)
                 (list
                  (catch 'done
                    (unwind-protect 'protected
                      (throw 'done 'cleanup)))
                  (condition-case err
                      (unwind-protect (error "protected")
                        (error "cleanup"))
                    (error (cadr err)))
                  (catch 'done
                    (unwind-protect 'protected
                      (push 'first log)
                      (throw 'done (nreverse log))
                      (push 'late log)))
                  (catch 'done
                    (unwind-protect (throw 'done 'protected)
                      (setq cleaned t)))
                  cleaned))"#,
        ),
        Value::list([
            Value::Symbol("cleanup".into()),
            Value::String("cleanup".into()),
            Value::list([Value::Symbol("first".into())]),
            Value::Symbol("protected".into()),
            Value::T,
        ])
    );
}

#[test]
fn subprocess_exit_is_event_driven_and_notifies_stderr_before_primary_once() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r#"(let* ((shell (executable-find "sh"))
                         (command (list shell "-c" "printf err >&2"))
                         (events nil)
                         (stderr
                          (make-pipe-process
                           :name "event-order-stderr"
                           :sentinel
                           (lambda (_process _event)
                             (setq events (append events '(stderr))))))
                         (child
                          (make-process
                           :name "event-order-primary"
                           :command command
                           :stderr stderr
                           :sentinel
                           (lambda (_process _event)
                             (setq events (append events '(primary))))))
                         (initially-live (process-live-p child)))
                    (while (process-live-p child)
                      (sit-for 0.01))
                    ;; A second pump must not repeat either terminal event.
                    (sit-for 0.01)
                    (list initially-live
                          events
                          (process-exit-status child)
                          (process-live-p child)
                          (equal (process-command child) command)))"#,
            ),
            Value::list([
                Value::T,
                Value::list([
                    Value::Symbol("stderr".into()),
                    Value::Symbol("primary".into()),
                ]),
                Value::Integer(0),
                Value::Nil,
                Value::T,
            ])
        );
    });
}

#[test]
fn process_send_eof_keeps_linked_stderr_separate_from_stdout() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r#"(let* ((shell (executable-find "sh"))
                         (stdout "")
                         (stderr-output "")
                         (stderr
                          (make-pipe-process
                           :name "send-eof-stderr"
                           :filter
                           (lambda (_process text)
                             (setq stderr-output
                                   (concat stderr-output text)))))
                         (child
                          (make-process
                           :name "send-eof-primary"
                           :command
                           (list shell "-c"
                                 "input=$(cat); printf 'out:%s' \"$input\"; printf err >&2")
                           :stderr stderr
                           :filter
                           (lambda (_process text)
                             (setq stdout (concat stdout text))))))
                    (process-send-string child "value")
                    (process-send-eof child)
                    (list stdout stderr-output))"#,
            ),
            Value::list([
                Value::String("out:value".into()),
                Value::String("err".into()),
            ])
        );
    });
}

#[test]
fn with_demoted_errors_returns_nil_after_catching_errors() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str("(with-demoted-errors \"%S\" (error \"boom\"))"),
            Value::Nil
        );
    });
}

#[test]
fn prin1_writes_to_buffer_streams() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer \
                   (prin1 '(alpha \"beta\") (current-buffer)) \
                   (buffer-string))"
        ),
        Value::String("(alpha \"beta\")".into())
    );
}

#[test]
fn read_accepts_buffer_and_marker_streams() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer \
                   (insert \"(1 2)\") \
                   (goto-char 1) \
                   (list (read (current-buffer)) (point) (read (point-min-marker))))"
        ),
        Value::list([
            Value::list([Value::Integer(1), Value::Integer(2)]),
            Value::Integer(6),
            Value::list([Value::Integer(1), Value::Integer(2)]),
        ])
    );
}

#[test]
fn runtime_read_returns_raw_backquote_symbols_and_accepts_dot_comma() {
    assert_eq!(
        eval_str(
            "(let* ((value (car (read-from-string \"`(t .,t)\")))
                    (body (car (cdr value))))
               (and (eq (car value) (intern \"`\"))
                    (eq (car body) t)
                    (eq (car (cdr body)) (intern \",\"))
                    (eq (car (cdr (cdr body))) t)
                    (null (cdr (cdr (cdr body))))))"
        ),
        Value::T
    );
}

#[test]
fn defun_does_not_shadow_preferred_builtin_overrides() {
    assert_eq!(
        eval_str(
            "(progn
               (defun tool-bar-local-item-from-menu (&rest _args) 'shadowed)
               (tool-bar-local-item-from-menu 'command \"icon\" 'target-map))"
        ),
        Value::Symbol("target-map".into())
    );
}

#[test]
fn md5_accepts_buffer_sources_and_coding_symbols() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer \
                   (insert \"abc\") \
                   (md5 (current-buffer) nil nil 'utf-8-emacs-unix))"
        ),
        Value::String("900150983cd24fb0d6963f7d28e17f72".into())
    );
}

#[test]
fn intern_soft_accepts_symbol_arguments() {
    assert_eq!(
        eval_str(
            "(list (intern-soft 'sample-symbol)\
                   (intern-soft (make-symbol \"sample-symbol\")))"
        ),
        Value::list([Value::Symbol("sample-symbol".into()), Value::Nil])
    );
}

#[test]
fn intern_preserves_canonical_nil_and_t_values() {
    assert_eq!(
        eval_str(
            "(list (intern \"nil\") (intern \"t\") (intern-soft \"nil\") (intern-soft \"t\"))"
        ),
        Value::list([Value::Nil, Value::T, Value::Nil, Value::T])
    );
}

#[test]
fn handler_bind_errors_skip_inner_condition_case() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (condition-case nil
                (handler-bind
                    ((error (lambda (_err)
                              (signal 'wrong-type-argument nil))))
                  (list 'result
                        (condition-case nil
                            (user-error "hello")
                          (wrong-type-argument 'inner-handler))))
              (wrong-type-argument 'wrong-type-argument))
            "#,
    )
    .read_all()
    .unwrap();
    let result = interp.eval(&forms[0], &mut env).unwrap();
    assert_eq!(result, Value::Symbol("wrong-type-argument".into()));
}

#[test]
fn full_handler_bind_regression_sequence() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (equal (catch 'tag
                       (handler-bind ((error (lambda (_err) (throw 'tag 'wow))))
                         'noerror))
                     'noerror)
              (equal (catch 'tag
                       (handler-bind ((error (lambda (_err) (throw 'tag 'err))))
                         (list 'inner-catch
                               (catch 'tag
                                 (user-error "hello")))))
                     '(inner-catch err))
              (condition-case nil
                  (handler-bind
                      ((error (lambda (_err)
                                (signal 'wrong-type-argument nil))))
                    (list 'result
                          (condition-case nil
                              (user-error "hello")
                            (wrong-type-argument 'inner-handler))))
                (wrong-type-argument 'wrong-type-argument)))
            "#,
    )
    .read_all()
    .unwrap();
    let result = interp.eval(&forms[0], &mut env).unwrap();
    assert_eq!(result, Value::Symbol("wrong-type-argument".into()));
}

#[test]
fn handler_bind_preserves_error_object_identity_for_condition_case() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (let* (inner-error
                   (outer-error
                    (condition-case err
                        (handler-bind ((error (lambda (err) (setq inner-error err))))
                          (car 1))
                      (error err))))
              (eq inner-error outer-error))
            "#,
    )
    .read_all()
    .unwrap();
    let result = interp.eval(&forms[0], &mut env).unwrap();
    assert_eq!(result, Value::T);
}

#[test]
fn aref_out_of_range_signals_args_out_of_range_condition() {
    assert_eq!(
        eval_str(
            "(condition-case nil (aref [1] 1) (args-out-of-range 'caught) (error 'plain-error))"
        ),
        Value::Symbol("caught".into())
    );
}

#[test]
fn handler_bind_handlers_do_not_apply_inside_handlers() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (condition-case nil
                (handler-bind
                    ((error (lambda (_err)
                              (signal 'wrong-type-argument nil)))
                     (wrong-type-argument
                      (lambda (_err) (user-error "wrong-type-argument"))))
                  (user-error "hello"))
              (wrong-type-argument 'wrong-type-argument)
              (error 'plain-error))
            "#,
    )
    .read_all()
    .unwrap();
    let result = interp.eval(&forms[0], &mut env).unwrap();
    assert_eq!(result, Value::Symbol("wrong-type-argument".into()));
}

#[test]
fn lambda_without_body_still_reports_invalid_function_for_bad_args() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(r#"(eval '(funcall (lambda (&rest &optional))) nil)"#)
        .read_all()
        .unwrap();
    let error = interp.eval(&forms[0], &mut env).unwrap_err();
    assert_eq!(error.condition_type(), "invalid-function");
}

#[test]
fn lambda_with_only_string_body_returns_that_string() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(r#"(funcall (lambda () "foo"))"#)
        .read()
        .unwrap()
        .unwrap();
    let result = interp.eval(&form, &mut env).unwrap();
    assert_eq!(result, Value::String("foo".into()));
}

#[test]
fn lambda_rest_ignores_missing_optional_arguments() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str("(funcall (lambda (a &optional b c &rest rest) (list a b c rest)) 1)"),
            Value::list([Value::Integer(1), Value::Nil, Value::Nil, Value::Nil,])
        );
    });
}

#[test]
fn with_connection_local_variables_uses_lisp_macro_definition() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (defmacro with-connection-local-variables (&rest body)
                     `(progn ,@body))
                   (with-connection-local-variables 1 2 3))",
        ),
        Value::Integer(3)
    );
}

#[test]
fn with_eval_after_load_runs_forms_when_feature_is_provided() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (setq emaxx-after-load-events nil)
                  (with-eval-after-load 'sample-after-load
                    (push 'deferred emaxx-after-load-events))
                  (with-eval-after-load 'emaxx
                    (push 'immediate emaxx-after-load-events))
                  (provide 'sample-after-load)
                  emaxx-after-load-events)
                "#
        ),
        Value::list([
            Value::Symbol("deferred".into()),
            Value::Symbol("immediate".into()),
        ])
    );
}

#[test]
fn mutating_if_tail_reports_void_variable() {
    assert_eq!(
        eval_str(
            r#"
                (let ((if-tail (list '(setcdr if-tail "abc") t)))
                  (list
                   (condition-case nil
                       (progn (eval (cons 'if if-tail) nil) 'ok)
                     (void-variable 'void-variable)
                     (wrong-type-argument 'wrong-type-argument))
                   (condition-case nil
                       (progn (eval (cons 'if if-tail) t) 'ok)
                     (void-variable 'void-variable)
                     (wrong-type-argument 'wrong-type-argument))))
                "#
        ),
        Value::list([
            Value::Symbol("void-variable".into()),
            Value::Symbol("void-variable".into()),
        ])
    );
}

#[test]
fn eval_arithmetic() {
    assert_eq!(eval_str("(+ 1 2)"), Value::Integer(3));
    assert_eq!(eval_str("(- 10 3)"), Value::Integer(7));
    assert_eq!(eval_str("(* 4 5)"), Value::Integer(20));
    assert_eq!(eval_str("(/ 2)"), Value::Integer(0));
    assert_eq!(eval_str("(/ -1)"), Value::Integer(-1));
    assert_eq!(eval_str("(/ 2.0)"), Value::Float(0.5));
    {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let form = Reader::new("(/ 0)").read().unwrap().unwrap();
        assert!(matches!(
            interp.eval(&form, &mut env),
            Err(LispError::SignalValue(value))
                if value == Value::list([Value::Symbol("arith-error".into())])
        ));
    }
    assert_eq!(eval_str("(+ 1 2 3 4)"), Value::Integer(10));
    assert_eq!(eval_str("(1+ 5)"), Value::Integer(6));
    assert_eq!(eval_str("(1- 5)"), Value::Integer(4));
    assert_eq!(eval_str("(logand)"), Value::Integer(-1));
    assert_eq!(eval_str("(logand 7 3 1)"), Value::Integer(1));
    assert_eq!(eval_str("(logior 1 2 4)"), Value::Integer(7));
    assert_eq!(eval_str("(logxor 1 2 3)"), Value::Integer(0));
    assert_eq!(eval_str("(lognot 5)"), Value::Integer(-6));
    assert_eq!(
        eval_str("(list (cl-evenp 4) (cl-oddp 5) (cl-evenp -2) (cl-oddp -3))"),
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
    assert_eq!(
        eval_str(
            "(and (< (abs (- (degrees-to-radians 180) float-pi)) 0.000000001)
                      (< (abs (- (radians-to-degrees float-pi) 180.0)) 0.000000001))"
        ),
        Value::T
    );
}

#[test]
fn aref_reads_strings_bound_in_lexical_variables() {
    assert_eq!(
        eval_str("(let ((buf (make-string 4 0))) (aref buf 0))"),
        Value::Integer(0)
    );
}

#[test]
fn aset_mutates_make_string_storage() {
    assert_eq!(
        eval_str("(let ((buf (make-string 2 ?x))) (aset buf 0 ?a) (equal buf \"ax\"))"),
        Value::T
    );
}

#[test]
fn setf_supports_aref_places_bound_in_lexical_variables() {
    assert_eq!(
        eval_str("(let ((stats (vector 0 0)) (i 1)) (setf (aref stats (mod i 2)) 7) stats)"),
        Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Integer(0),
            Value::Integer(7),
        ])
    );
}

#[test]
fn setf_uses_symbol_gv_setter_declarations() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defstruct sample-gv alpha beta)
                   (defun sample-alpha-setter (object value)
                     (setf (sample-gv-alpha object) value)
                     value)
                   (defun sample-alpha-view (object)
                     (declare (gv-setter sample-alpha-setter))
                     (sample-gv-alpha object))
                   (let ((object (make-sample-gv :alpha 1 :beta 2)))
                     (list
                      (setf (sample-alpha-view object) 9)
                      (sample-gv-alpha object)
                      (sample-gv-beta object))))"
        ),
        Value::list([Value::Integer(9), Value::Integer(9), Value::Integer(2),])
    );
}

#[test]
fn setf_resolves_conditional_places() {
    assert_eq!(
        eval_str(
            "(let ((left nil) (right nil) (choose-right t))
                   (setf (cond (choose-right right) (t left)) '(stored))
                   (list left right))"
        ),
        Value::list([Value::Nil, Value::list([Value::Symbol("stored".into())])])
    );
}

#[test]
fn aset_mutates_vectors_bound_in_lexical_variables() {
    assert_eq!(
        eval_str("(let ((stats (make-vector 2 nil))) (aset stats 1 'ok) stats)"),
        Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Nil,
            Value::Symbol("ok".into()),
        ])
    );
}

#[test]
fn prog1_returns_vectors_after_in_place_mutation() {
    assert_eq!(
        eval_str("(let ((stats (make-vector 2 nil))) (prog1 stats (aset stats 1 'ok)))"),
        Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Nil,
            Value::Symbol("ok".into()),
        ])
    );
}

#[test]
fn prog2_returns_second_form_after_evaluating_remaining_body() {
    assert_eq!(
        eval_str(
            "(let ((events nil))
                   (list
                    (prog2
                        (push 'first events)
                        (push 'second events)
                      (push 'third events))
                    events))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("second".into()),
                Value::Symbol("first".into())
            ]),
            Value::list([
                Value::Symbol("third".into()),
                Value::Symbol("second".into()),
                Value::Symbol("first".into()),
            ]),
        ])
    );
}

#[test]
fn prog2_returns_vectors_after_in_place_mutation() {
    assert_eq!(
        eval_str(
            "(let ((stats (make-vector 2 nil)))
                   (prog2 'ignored stats (aset stats 1 'ok)))"
        ),
        Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Nil,
            Value::Symbol("ok".into()),
        ])
    );
}

#[test]
fn make_temp_name_preserves_prefix_and_changes_across_calls() {
    assert_eq!(
        eval_str(
            r#"(let ((a (make-temp-name "x-dnd-test-"))
                         (b (make-temp-name "x-dnd-test-")))
                     (list (string-prefix-p "x-dnd-test-" a)
                           (string-prefix-p "x-dnd-test-" b)
                           (equal a b)))"#
        ),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn temporary_file_directory_names_a_directory_with_trailing_separator() {
    assert_eq!(
        eval_str(
            r#"(list (string-suffix-p "/" temporary-file-directory)
                     (equal temporary-file-directory
                            (file-name-as-directory temporary-file-directory)))"#
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn write_region_accepts_string_data_even_with_numeric_end_argument() {
    assert_eq!(
        eval_str(
            r#"(let ((path (make-temp-name temporary-file-directory)))
                     (unwind-protect
                         (progn
                           (write-region "" 0 path)
                           (file-exists-p path))
                       (ignore-errors (delete-file path))))"#
        ),
        Value::T
    );
}

#[test]
fn url_encode_url_preserves_reserved_chars_and_escapes_spaces() {
    assert_eq!(
        eval_str(r#"(url-encode-url "file:///tmp/a b?x=1#frag")"#),
        Value::String("file:///tmp/a%20b?x=1#frag".into())
    );
}

#[test]
fn url_scheme_get_property_reports_standard_default_ports() {
    assert_eq!(
        eval_str(
            "(list
                   (url-scheme-get-property \"https\" 'default-port)
                   (url-scheme-get-property \"http\" 'default-port)
                   (url-scheme-get-property \"unknown\" 'default-port))"
        ),
        Value::list([Value::Integer(443), Value::Integer(80), Value::Integer(0)])
    );
}

#[test]
fn assoc_matches_strings_bound_in_lexical_variables() {
    assert_eq!(
        eval_str(
            "(let ((key \"--foo\") (alist (list (cons \"--foo\" 1)))) (cdr (assoc key alist)))"
        ),
        Value::Integer(1)
    );
}

#[test]
fn assoc_and_assq_ignore_non_cons_alist_entries() {
    assert_eq!(
        eval_str(
            "(list (assoc 'target '(dummy (other . 1) (target . 2)))
                       (assq 'target '(dummy (other . 1) (target . 2))))"
        ),
        Value::list([
            Value::cons(Value::Symbol("target".into()), Value::Integer(2)),
            Value::cons(Value::Symbol("target".into()), Value::Integer(2)),
        ])
    );
}

#[test]
fn cl_list_accessors_return_positional_elements() {
    assert_eq!(
        eval_str("(list (cl-first '(a b c)) (cl-second '(a b c)) (cl-third '(a b c)))"),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
        ])
    );
}

#[test]
fn cl_list_accessors_cover_first_ten_elements() {
    assert_eq!(
        eval_str(
            "(list
               (cl-fourth '(1 2 3 4 5 6 7 8 9 10))
               (cl-fifth '(1 2 3 4 5 6 7 8 9 10))
               (cl-sixth '(1 2 3 4 5 6 7 8 9 10))
               (cl-seventh '(1 2 3 4 5 6 7 8 9 10))
               (cl-eighth '(1 2 3 4 5 6 7 8 9 10))
               (cl-ninth '(1 2 3 4 5 6 7 8 9 10))
               (cl-tenth '(1 2 3 4 5 6 7 8 9 10))
               (condition-case err
                   (cl-fourth \"1234\")
                 (wrong-type-argument (car err))))"
        ),
        Value::list([
            Value::Integer(4),
            Value::Integer(5),
            Value::Integer(6),
            Value::Integer(7),
            Value::Integer(8),
            Value::Integer(9),
            Value::Integer(10),
            Value::Symbol("wrong-type-argument".into()),
        ])
    );
}

#[test]
fn cl_endp_distinguishes_empty_lists_from_cons_cells() {
    assert_eq!(
        eval_str(
            "(list
               (cl-endp nil)
               (cl-endp '(1))
               (condition-case err
                   (cl-endp 1)
                 (wrong-type-argument (car err)))
               (condition-case err
                   (cl-endp [1])
                 (wrong-type-argument (car err))))"
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol("wrong-type-argument".into()),
        ])
    );
}

#[test]
fn proper_list_p_returns_length_for_proper_lists_only() {
    assert_eq!(
        eval_str(
            "(list
                   (proper-list-p nil)
                   (proper-list-p '(a b))
                   (proper-list-p '(a . b))
                   (let ((x (list 'a)))
                     (setcdr x x)
                     (proper-list-p x)))"
        ),
        Value::list([Value::Integer(0), Value::Integer(2), Value::Nil, Value::Nil])
    );
}

#[test]
fn rassq_delete_all_filters_matching_alist_values() {
    assert_eq!(
        eval_str("(rassq-delete-all 'drop '(noise (a . drop) (b . keep) (c . drop)))"),
        Value::list([
            Value::Symbol("noise".into()),
            Value::cons(Value::Symbol("b".into()), Value::Symbol("keep".into())),
        ])
    );
}

#[test]
fn format_prompt_uses_first_default_choice() {
    run_large_stack_test(assert_format_prompt_uses_first_default_choice);
}

#[test]
fn warn_formats_message_and_returns_nil() {
    assert_eq!(
        eval_str(
            "(progn
                   (warn \"sample %s\" \"warning\")
                   (list (current-message)
                         (warn \"ignored\")))"
        ),
        Value::list([Value::String("Warning: sample warning".into()), Value::Nil])
    );
}

#[test]
fn run_hook_with_args_until_success_returns_first_truthy_result() {
    assert_eq!(
        eval_str(
            "(progn
                   (defvar sample-success-hook nil)
                   (setq sample-success-hook
                         (list
                          (lambda (value) nil)
                          (lambda (value) (list 'hit value))
                          (lambda (value) (error \"must not run\"))))
                   (run-hook-with-args-until-success 'sample-success-hook 7))"
        ),
        Value::list([Value::Symbol("hit".into()), Value::Integer(7)])
    );
}

#[test]
fn run_hook_with_args_until_failure_stops_at_first_nil_result() {
    assert_eq!(
        eval_str(
            "(progn
                   (defvar sample-failure-hook nil)
                   (setq sample-failure-hook
                         (list
                          (lambda (value) t)
                          (lambda (value) nil)
                          (lambda (value) (error \"must not run\"))))
                   (run-hook-with-args-until-failure 'sample-failure-hook 7))"
        ),
        Value::Nil
    );
    assert_eq!(
        eval_str(
            "(progn
                   (defvar sample-no-failure-hook nil)
                   (setq sample-no-failure-hook
                         (list (lambda () t) (lambda () 'ok)))
                   (run-hook-with-args-until-failure 'sample-no-failure-hook))"
        ),
        Value::T
    );
}

#[test]
fn add_hook_orders_functions_by_gnu_depth() {
    assert_eq!(
        eval_str(
            "(progn
               (defvar depth-order-hook nil)
               (defvar depth-order-log nil)
               (setq depth-order-hook nil depth-order-log nil)
               (defun depth-order-f70 () (push 'f70 depth-order-log))
               (defun depth-order-f60 () (push 'f60 depth-order-log))
               (defun depth-order-fzero () (push 'fzero depth-order-log))
               (defun depth-order-fneg () (push 'fneg depth-order-log))
               (defun depth-order-flate () (push 'flate depth-order-log))
               (add-hook 'depth-order-hook #'depth-order-f70 70)
               (add-hook 'depth-order-hook #'depth-order-f60 60)
               (add-hook 'depth-order-hook #'depth-order-fzero)
               (add-hook 'depth-order-hook #'depth-order-fneg -10)
               (add-hook 'depth-order-hook #'depth-order-flate 'append)
               (list depth-order-hook
                     (progn (run-hooks 'depth-order-hook)
                            (nreverse depth-order-log))
                     (symbolp (get 'depth-order-hook 'hook--depth-alist))))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("depth-order-fneg".into()),
                Value::Symbol("depth-order-fzero".into()),
                Value::Symbol("depth-order-f60".into()),
                Value::Symbol("depth-order-f70".into()),
                Value::Symbol("depth-order-flate".into()),
            ]),
            Value::list([
                Value::Symbol("fneg".into()),
                Value::Symbol("fzero".into()),
                Value::Symbol("f60".into()),
                Value::Symbol("f70".into()),
                Value::Symbol("flate".into()),
            ]),
            Value::T,
        ])
    );
}

#[test]
fn local_hook_depth_splices_the_default_at_depth_zero() {
    assert_eq!(
        eval_str(
            "(progn
               (defvar depth-splice-hook nil)
               (defvar depth-splice-log nil)
               (setq depth-splice-hook nil depth-splice-log nil)
               (defun depth-splice-global () (push 'global depth-splice-log))
               (defun depth-splice-before () (push 'before depth-splice-log))
               (defun depth-splice-after () (push 'after depth-splice-log))
               (add-hook 'depth-splice-hook #'depth-splice-global)
               (with-temp-buffer
                 (add-hook 'depth-splice-hook #'depth-splice-after 60 t)
                 (add-hook 'depth-splice-hook #'depth-splice-before -60 t)
                 (list depth-splice-hook
                       (progn (run-hooks 'depth-splice-hook)
                              (nreverse depth-splice-log)))))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("depth-splice-before".into()),
                Value::T,
                Value::Symbol("depth-splice-after".into()),
            ]),
            Value::list([
                Value::Symbol("before".into()),
                Value::Symbol("global".into()),
                Value::Symbol("after".into()),
            ]),
        ])
    );
}

#[test]
fn dynamically_filtered_local_hook_still_splices_the_default() {
    assert_eq!(
        eval_str(
            "(progn
               (defvar dynamic-splice-hook nil)
               (defvar dynamic-splice-log nil)
               (defun dynamic-splice-global ()
                 (push 'global dynamic-splice-log))
               (defun dynamic-splice-a () (push 'a dynamic-splice-log))
               (defun dynamic-splice-b () (push 'b dynamic-splice-log))
               (add-hook 'dynamic-splice-hook #'dynamic-splice-global)
               (with-temp-buffer
                 (add-hook 'dynamic-splice-hook #'dynamic-splice-a nil t)
                 (add-hook 'dynamic-splice-hook #'dynamic-splice-b nil t)
                 (let ((dynamic-splice-hook
                        (remq #'dynamic-splice-a dynamic-splice-hook)))
                   (run-hooks 'dynamic-splice-hook)
                   (list dynamic-splice-hook
                         (nreverse dynamic-splice-log)))))"
        ),
        Value::list([
            Value::list([Value::Symbol("dynamic-splice-b".into()), Value::T]),
            Value::list([Value::Symbol("b".into()), Value::Symbol("global".into()),]),
        ])
    );
}

#[test]
fn advice_member_p_defaults_to_nil_for_untracked_advice() {
    assert_eq!(
        eval_str("(advice-member-p 'sample-advice 'sample-function)"),
        Value::Nil
    );
}

#[test]
fn advice_add_allows_forward_target_symbols() {
    assert_eq!(
        eval_str(
            "(progn
                   (defun sample-forward-advice (&rest _) nil)
                   (advice-add 'sample-forward-target :around #'sample-forward-advice)
                   (fboundp 'sample-forward-target))"
        ),
        Value::Nil
    );
}

fn assert_format_prompt_uses_first_default_choice() {
    assert_eq!(
        eval_str(r#"(format-prompt "Regexp to unhighlight" '("a" "b"))"#),
        Value::String("Regexp to unhighlight (default a): ".into())
    );
}

#[test]
fn ngettext_selects_singular_only_for_one() {
    assert_eq!(
        eval_str(
            r#"(list (ngettext "item" "items" 1)
                         (ngettext "item" "items" 0)
                         (ngettext "item" "items" 2))"#
        ),
        Value::list([
            Value::String("item".into()),
            Value::String("items".into()),
            Value::String("items".into()),
        ])
    );
}

#[test]
fn assoc_string_matches_symbols_single_strings_and_case_fold() {
    assert_eq!(
        eval_str(
            "(list (assoc-string 'foo '((bar . 1) (foo . 2)))
                       (assoc-string \"foo\" '(dummy \"foo\"))
                       (assoc-string \"FOO\" '((\"foo\" . 3)) t))"
        ),
        Value::list([
            Value::cons(Value::Symbol("foo".into()), Value::Integer(2)),
            Value::String("foo".into()),
            Value::cons(Value::String("foo".into()), Value::Integer(3)),
        ])
    );
}

#[test]
fn assoc_string_handles_nil_t_and_empty_alists_like_symbols() {
    assert_eq!(
        eval_str(
            r#"(list (assoc-string nil nil)
                         (assoc-string 1 nil)
                         (assoc-string nil '((nil . nil-value) ("nil" . string-value)))
                         (assoc-string t '((t . t-value) ("t" . string-value)))
                         (assoc-string "nil" '((nil . nil-value)))
                         (assoc-string "t" '((t . t-value))))"#
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::cons(Value::Nil, Value::Symbol("nil-value".into())),
            Value::cons(Value::T, Value::Symbol("t-value".into())),
            Value::cons(Value::Nil, Value::Symbol("nil-value".into())),
            Value::cons(Value::T, Value::Symbol("t-value".into())),
        ])
    );
}

#[test]
fn assoc_string_case_fold_avoids_multi_character_uppercase_matches() {
    let value =
        eval_str("(assoc-string \"ß\" '((\"ss\" . wrong) (\"ß\" . right) (\"ẞ\" . upper)) t)");
    let Some((key, result)) = value.cons_values() else {
        panic!("assoc-string should return an alist entry");
    };
    assert_string_value(key, "ß");
    assert_eq!(result, Value::Symbol("right".into()));
}

#[test]
fn cl_delete_if_filters_matching_items() {
    run_large_stack_test(assert_cl_delete_if_filters_matching_items);
}

fn assert_cl_delete_if_filters_matching_items() {
    assert_eq!(
        eval_str("(cl-delete-if #'numberp '(a 1 b 2 c))"),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
        ])
    );
}

#[test]
fn remove_filters_lists_vectors_and_strings() {
    let value = eval_str(
        "(list
               (remove 'a '(a b a c))
               (remove 2 [1 2 3 2])
               (remove ?a \"aba\"))",
    );
    let items = value.to_vec().unwrap();
    assert_eq!(
        items[0],
        Value::list([Value::Symbol("b".into()), Value::Symbol("c".into()),])
    );
    assert_eq!(
        items[1],
        Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Integer(1),
            Value::Integer(3),
        ])
    );
    assert_eq!(primitives::string_text(&items[2]).unwrap(), "b");
}

#[test]
fn unibyte_string_sequences_return_byte_values() {
    assert_eq!(
        eval_str("(let ((s (unibyte-string 225 16))) (aref s 0))"),
        Value::Integer(225)
    );
    assert_eq!(
        eval_str("(let ((s (unibyte-string 225 16))) (string-to-list s))"),
        Value::list([Value::Integer(225), Value::Integer(16)])
    );
}

#[test]
fn eval_comparisons() {
    assert_eq!(eval_str("(= 3 3)"), Value::T);
    assert_eq!(eval_str("(= 3 4)"), Value::Nil);
    assert_eq!(eval_str("(< 1 2)"), Value::T);
    assert_eq!(eval_str("(> 1 2)"), Value::Nil);
    assert_eq!(eval_str("(<= 3 3)"), Value::T);
    assert_eq!(eval_str("(>= 4 3)"), Value::T);
}

#[test]
fn eval_let() {
    assert_eq!(eval_str("(let ((x 10)) x)"), Value::Integer(10));
    assert_eq!(eval_str("(let ((x 2) (y 3)) (+ x y))"), Value::Integer(5));
}

#[test]
fn eval_if() {
    assert_eq!(eval_str("(if t 1 2)"), Value::Integer(1));
    assert_eq!(eval_str("(if nil 1 2)"), Value::Integer(2));
    assert_eq!(eval_str("(if t 1)"), Value::Integer(1));
    assert_eq!(eval_str("(if nil 1)"), Value::Nil);
}

#[test]
fn eval_progn() {
    assert_eq!(eval_str("(progn 1 2 3)"), Value::Integer(3));
}

#[test]
fn eval_and_or() {
    assert_eq!(eval_str("(and 1 2 3)"), Value::Integer(3));
    assert_eq!(eval_str("(and 1 nil 3)"), Value::Nil);
    assert_eq!(eval_str("(or nil nil 3)"), Value::Integer(3));
    assert_eq!(eval_str("(or nil nil)"), Value::Nil);
}

#[test]
fn eval_defun_and_call() {
    let mut interp = Interpreter::new();
    eval_str_with(&mut interp, "(defun double (x) (* x 2))");
    assert_eq!(
        eval_str_with(&mut interp, "(double 21)"),
        Value::Integer(42)
    );
}

#[test]
fn defun_without_body_returns_nil() {
    assert_eq!(
        eval_str("(progn (defun sample-empty-function (arg)) (sample-empty-function 1))"),
        Value::Nil
    );
}

#[test]
fn defmacro_without_body_expands_to_nil() {
    assert_eq!(
        eval_str("(progn (defmacro sample-empty-macro (arg)) (sample-empty-macro value))"),
        Value::Nil
    );
}

#[test]
fn macro_missing_required_args_signals_wrong_number_of_arguments() {
    // GNU signals wrong-number-of-arguments when a macro call omits
    // required parameters.
    assert_eq!(
        eval_str(
            "(progn (defmacro sample-macro (required &rest rest) rest)
                    (condition-case e (eval '(sample-macro) t) (error (car e))))"
        ),
        Value::Symbol("wrong-number-of-arguments".into())
    );
}

fn assert_eval_string_ops() {
    assert_eq!(
        eval_str(r#"(concat "hello" " " "world")"#),
        Value::String("hello world".into())
    );
    assert_eq!(eval_str(r#"(string= "abc" "abc")"#), Value::T);
    assert_eq!(eval_str(r#"(string= "abc" "def")"#), Value::Nil);
    assert_eq!(eval_str(r#"(string= "4" nil)"#), Value::Nil);
    assert_eq!(eval_str(r#"(string= nil nil)"#), Value::T);
    assert_eq!(eval_str(r#"(string< 'a 'b)"#), Value::T);
    assert_eq!(eval_str(r#"(length "hello")"#), Value::Integer(5));
    assert_string_value(eval_str(r#"(reverse "stressed")"#), "desserts");
    assert_string_value(eval_str(r#"(nreverse "drawer")"#), "reward");
    assert_string_value(eval_str(r#"(substring-no-properties "hello" 1 4)"#), "ell");
    assert_string_value(eval_str(r#"(substring "hello" 0 -1)"#), "hell");
    assert_eq!(
        eval_str(r#"(substring [255 99 98 97] 1 4)"#),
        Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Integer(99),
            Value::Integer(98),
            Value::Integer(97),
        ])
    );
    assert_eq!(eval_str(r#"(string-to-number "1e-1")"#), Value::Float(0.1));
    assert_eq!(
        eval_str(r#"(string-to-number ".1..e1")"#),
        Value::Float(0.1)
    );
    assert_eq!(
        eval_str(r#"(string-to-number "1e+1.1")"#),
        Value::Float(10.0)
    );
    assert_eq!(
        eval_str(r#"(string-to-number "ffzz" 16)"#),
        Value::Integer(255)
    );
    assert_eq!(
        eval_str(r#"(int-to-string 12345)"#),
        Value::String("12345".into())
    );
    assert_string_value(
        eval_str(r#"(internal--format-docstring-line "Return non-nil if %S is ready." 'object)"#),
        "Return non-nil if object is ready.",
    );
    assert_string_value(
        eval_str(r#"(replace-regexp-in-string "\\([a-z]+\\)" "<\\1>" "abc 123")"#),
        "<abc> 123",
    );
    assert_string_value(
        eval_str(r#"(replace-regexp-in-string "[0-9]+" "x" "a1b22" t t)"#),
        "axbx",
    );
    assert_string_value(
        eval_str(r#"(subst-char-in-string ?/ ?! "/home/me/src")"#),
        "!home!me!src",
    );
    assert_eq!(
        eval_str(
            r#"(let ((s (copy-sequence "a/b/c")))
                     (and (eq (subst-char-in-string ?/ ?! s t) s)
                          (string= s "a!b!c")))"#
        ),
        Value::T
    );
    assert_string_value(
        eval_str(
            r#"(replace-regexp-in-string "%[[:xdigit:]][[:xdigit:]]"
                                              (lambda (_match) "/")
                                              "file:///tmp/a%20b"
                                              t t)"#,
        ),
        "file:///tmp/a/b",
    );
    assert_string_value(
        eval_str(
            r#"(replace-regexp-in-string "\\`\\([ACMHSs]-\\)*"
                                              "\\&down-"
                                              "S-mouse-2"
                                              t)"#,
        ),
        "S-down-mouse-2",
    );
    assert_eq!(
        eval_str(r#"(string-join '("foo" "bar" "zot") " ")"#),
        Value::String("foo bar zot".into())
    );
    assert_eq!(
        eval_str(r#"(equal (mapcar #'reverse '("abc" "abd")) '("cba" "dba"))"#),
        Value::T
    );
    assert_eq!(
        eval_str(r#"(compiled-function-p (lambda (x) x))"#),
        Value::Nil
    );
    assert_eq!(
        eval_str(r#"(equal (sort '(3 1 2) #'< :in-place t) '(1 2 3))"#),
        Value::T
    );
    assert_eq!(
        eval_str(
            r#"(list (isearch-no-upper-case-p "abc" t)
                         (isearch-no-upper-case-p "Abc" t)
                         (isearch-no-upper-case-p "A\\b" t)
                         (isearch-no-upper-case-p "[:upper:]" t)
                         (with-temp-buffer
                           (insert "a A")
                           (goto-char (point-min))
                           (let ((search-spaces-regexp search-whitespace-regexp))
                             (re-search-forward "a   a" nil t))))"#
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Integer(4),
        ])
    );
}

#[test]
fn eval_string_ops() {
    run_large_stack_test(assert_eval_string_ops);
}

#[test]
fn string_match_failure_preserves_existing_match_data() {
    let value = eval_str(
        r#"
            (progn
              (string-match "a\\(b\\)" "ab")
              (string-match "z" "ab")
              (match-string 1 "ab"))
            "#,
    );
    assert_eq!(
        primitives::string_text(&value).expect("match-string result"),
        "b"
    );
}

#[test]
fn string_match_treats_reversed_bracket_ranges_as_empty() {
    assert_eq!(
        eval_str(
            r#"(list (string-match "[z-a]" "z")
                         (string-match "[az-c]" "a")
                         (string-match "[az-c]" "z")
                         (string-match "[^z-a]" "z")
                         (string-match "[^az-c]" "a")
                         (string-match "[^az-c]" "z"))"#
        ),
        Value::list([
            Value::Nil,
            Value::Integer(0),
            Value::Nil,
            Value::Integer(0),
            Value::Nil,
            Value::Integer(0),
        ])
    );
}

#[test]
fn string_match_handles_leading_hyphen_bracket_ranges() {
    assert_eq!(
        eval_str(
            r#"(list (string-match "[--/]" ".")
                     (string-match "[--/]" "a")
                     (string-match "[^---]" "-ab")
                     (string-match "X[^---]Y" "X-YXaYXbY"))"#
        ),
        Value::list([
            Value::Integer(0),
            Value::Nil,
            Value::Integer(1),
            Value::Integer(3),
        ])
    );
}

#[test]
fn newline_inserts_requested_line_breaks() {
    assert_string_value(
        eval_str(r#"(with-temp-buffer (insert "a") (newline 2) (insert "b") (buffer-string))"#),
        "a\n\nb",
    );
}

#[test]
fn completing_read_uses_default_without_interaction() {
    assert_eq!(
        eval_str(r#"(completing-read "File: " '("one" "two") nil nil nil nil "two")"#),
        Value::String("two".into())
    );
}

#[test]
fn completing_read_falls_back_to_first_candidate() {
    assert_eq!(
        eval_str(r#"(completing-read "File: " '("one" "two"))"#),
        Value::String("one".into())
    );
}

#[test]
fn re_search_failure_preserves_existing_match_data() {
    let value = eval_str(
        r#"
            (with-temp-buffer
              (insert "ab")
              (goto-char (point-min))
              (re-search-forward "a\\(b\\)")
              (re-search-forward "z" nil t)
              (match-string 1))
            "#,
    );
    assert_eq!(
        primitives::string_text(&value).expect("match-string result"),
        "b"
    );
}

#[test]
fn re_search_forward_respects_limit_argument() {
    let value = eval_str(
        r#"
            (with-temp-buffer
              (insert "ab ab")
              (goto-char (point-min))
              (re-search-forward "a\\(b\\)")
              (goto-char (point-min))
              (list (re-search-forward "a\\(b\\)" 2 t)
                    (match-string 1)))
            "#,
    );
    let items = value.to_vec().unwrap();
    assert_eq!(items[0], Value::Nil);
    assert_eq!(primitives::string_text(&items[1]).unwrap(), "b");
}

#[test]
fn re_search_forward_honors_nested_point_assertion_at_search_start() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                 (insert "xxx| rev *>temp")
                 (goto-char 4)
                 (list
                  (re-search-forward
                   "\\(?:\\=\\|[^*]\\|\\S-\\*\\)\\(|\\)" nil t)
                  (match-beginning 0)
                  (match-beginning 1)
                  (match-end 0)))"#,
        ),
        Value::list([
            Value::Integer(5),
            Value::Integer(4),
            Value::Integer(4),
            Value::Integer(5),
        ])
    );
}

#[test]
fn re_search_forward_respects_positive_count_argument() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "a\nb\nc\n")
                  (goto-char (point-min))
                  (list (re-search-forward "^[abc]" nil t 2)
                        (match-beginning 0)
                        (point)))
                "#
        ),
        Value::list([Value::Integer(4), Value::Integer(3), Value::Integer(4),])
    );
}

#[test]
fn re_search_forward_count_zero_sets_empty_match_at_point() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "a\nb\n")
                  (goto-char (point-min))
                  (list (re-search-forward "^[ab]" nil t 0)
                        (point)
                        (match-beginning 0)
                        (match-end 0)))
                "#
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
        ])
    );
}

#[test]
fn re_search_forward_negative_count_searches_backward() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "a\nb\n")
                  (goto-char (point-max))
                  (list (re-search-forward "^[ab]" nil t -1)
                        (point)
                        (match-beginning 0)
                        (match-end 0)))
                "#
        ),
        Value::list([
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(4),
        ])
    );
}

#[test]
fn re_search_backward_negative_count_searches_forward() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "a\nb\n")
                  (goto-char (point-min))
                  (list (re-search-backward "^[ab]" nil t -1)
                        (point)
                        (match-beginning 0)
                        (match-end 0)))
                "#
        ),
        Value::list([
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(1),
            Value::Integer(2),
        ])
    );
}

#[test]
fn re_search_backward_clamps_below_min_bound() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "foo")
                  (goto-char (point-max))
                  (list (re-search-backward "foo" -100 t)
                        (point)
                        (match-beginning 0)
                        (match-end 0)))
                "#
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(4),
        ])
    );
}

#[test]
fn match_string_no_properties_reads_existing_match_data() {
    let value = eval_str(
        r#"
            (progn
              (string-match "a\\(b\\)" "ab")
              (match-string-no-properties 1 "ab"))
            "#,
    );
    assert_eq!(
        primitives::string_text(&value).expect("match-string-no-properties result"),
        "b"
    );
}

#[test]
fn eval_list_ops() {
    assert_eq!(eval_str("(car '(1 2 3))"), Value::Integer(1));
    assert_eq!(eval_str("(cadr '(1 2 3))"), Value::Integer(2));
    assert_eq!(
        eval_str("(cddr '(1 2 3))"),
        Value::list([Value::Integer(3)])
    );
    assert_eq!(eval_str("(identity 'ok)"), Value::Symbol("ok".into()));
    assert_eq!(eval_str("(length '(1 2 3))"), Value::Integer(3));
}

#[test]
fn dlet_binds_values_for_evalled_body_forms() {
    assert_eq!(
        eval_str(
            "(dlet ((day \"25\") (month \"10\") (year \"1917\"))
                   (mapconcat #'eval '(year \"-\" month \"-\" day) \"\"))"
        ),
        Value::String("1917-10-25".into())
    );
}

#[test]
fn eval_symbol_with_escaped_trailing_space() {
    assert_eq!(eval_str("'GNU\\ "), Value::Symbol("GNU ".into()));
    assert_eq!(eval_str("(eq 'GNU\\  'GNU\\ )"), Value::T);
}

#[test]
fn eval_font_get_returns_xlfd_foundry_symbol() {
    assert_eq!(
        eval_str(
            "(equal (font-get (font-spec :name \"-GNU -FreeSans-semibold-italic-normal-*-*-*-*-*-*-0-iso10646-1\") :foundry) 'GNU\\ )"
        ),
        Value::T
    );
}

#[test]
fn eval_buffer_ops() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        r#"(with-temp-buffer
                 (insert "hello")
                 (should (= (point) 6))
                 (should (string= (buffer-string) "hello")))"#,
    );
}

#[test]
fn c_mode_sets_c_comment_defaults() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (c-mode)
                  (equal
                   (list major-mode mode-name comment-start comment-end
                         comment-start-skip comment-end-skip comment-use-syntax
                         comment-style comment-multi-line)
                   '(c-mode "C" "/* " " */"
                     "\\(?://+\\|/\\*+\\)\\s *"
                     "[ \t]*\\*+/"
                     t indent t)))
                "#
        ),
        Value::T
    );
}

#[test]
fn js_mode_sets_electric_layout_defaults() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (js-mode)
                  (list c-basic-offset
                        (cdr (assq ?{ electric-layout-rules))
                        (cdr (assq ?} electric-layout-rules))
                        (memq ?{ electric-indent-chars)))
                "#
        ),
        Value::list([
            Value::Integer(4),
            Value::Symbol("after".into()),
            Value::Symbol("before".into()),
            Value::list([
                Value::Integer('{' as i64),
                Value::Integer('}' as i64),
                Value::Integer('(' as i64),
                Value::Integer(')' as i64),
                Value::Integer(':' as i64),
                Value::Integer(';' as i64),
                Value::Integer(',' as i64),
                Value::Integer('\n' as i64),
            ]),
        ])
    );
}

#[test]
fn emacs_lisp_mode_sets_minimal_font_lock_defaults() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (emacs-lisp-mode)
                  (insert ";x\n")
                  (equal
                   (list major-mode mode-name comment-start comment-end
                         comment-use-syntax font-lock-defaults
                         (nth 4 (syntax-ppss 3)))
                   '(emacs-lisp-mode "Emacs-Lisp" ";" "" t t t)))
                "#
        ),
        Value::T
    );
}

#[test]
fn eval_ert_deftest_and_run() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        r#"
            (ert-deftest basic-insert ()
              (with-temp-buffer
                (insert "hello")
                (should (= (point) 6))
                (should (string= (buffer-string) "hello"))))
            "#,
    );
    let (passed, failed, total) = interp.run_ert_tests();
    assert_eq!(total, 1);
    assert_eq!(passed, 1);
    assert_eq!(failed, 0);
}

#[test]
fn ert_resource_file_uses_test_defining_file_during_execution() {
    let mut interp = Interpreter::new();
    let test_file = "/tmp/emaxx-pcmpl-linux-tests.el";
    let expected = "/tmp/emaxx-pcmpl-linux-resources/fs";
    interp.set_current_load_file(Some(test_file.into()));
    eval_str_with(
        &mut interp,
        &format!(
            r#"
                (ert-deftest ert-resource-file-keeps-defining-file ()
                  (should (string= (ert-resource-file "fs") "{expected}")))
                "#
        ),
    );
    interp.set_current_load_file(None);
    let (passed, failed, total) = interp.run_ert_tests();
    assert_eq!(total, 1);
    assert_eq!(passed, 1);
    assert_eq!(failed, 0);
}

#[test]
fn keyword_symbols_self_evaluate() {
    assert_eq!(eval_str(":default"), Value::Symbol(":default".into()));
}

#[test]
fn defconst_binds_global_like_defvar() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(&mut interp, "(defconst sample-constant 42)"),
        Value::Nil
    );
    assert_eq!(
        eval_str_with(&mut interp, "sample-constant"),
        Value::Integer(42)
    );
}

#[test]
fn defconst_reinitializes_existing_binding() {
    assert_eq!(
        eval_str("(progn (defvar sample-constant 1) (defconst sample-constant 2) sample-constant)"),
        Value::Integer(2)
    );
}

#[test]
fn defvar_without_initializer_keeps_variable_void() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(&mut interp, "(defvar sample-unbound)"),
        Value::Nil
    );
    assert_eq!(
        eval_str_with(&mut interp, "(boundp 'sample-unbound)"),
        Value::Nil
    );
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(condition-case err sample-unbound (void-variable (car err)))",
        ),
        Value::symbol("void-variable")
    );
}

#[test]
fn defvar_nil_initializer_binds_variable() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(&mut interp, "(defvar sample-nil-bound (when nil 1))"),
        Value::Nil
    );
    assert_eq!(
        eval_str_with(&mut interp, "(boundp 'sample-nil-bound)"),
        Value::T
    );
    assert_eq!(eval_str_with(&mut interp, "sample-nil-bound"), Value::Nil);
}

#[test]
fn file_remote_p_parses_tramp_style_names() {
    assert_eq!(
        eval_str(
            r#"(list
                     (file-remote-p "/ssh:user@host:/tmp/x")
                     (file-remote-p "/ssh:user@host:/tmp/x" 'method)
                     (file-remote-p "/ssh:user@host:/tmp/x" 'user)
                     (file-remote-p "/ssh:user@host:/tmp/x" 'host)
                     (file-remote-p "/ssh:user@host:/tmp/x" 'localname)
                     (file-remote-p "/mock::/tmp/x" 'method)
                     (file-remote-p "/mock::/tmp/x" 'localname))"#,
        ),
        Value::list([
            Value::String("/ssh:user@host:".into()),
            Value::String("ssh".into()),
            Value::String("user".into()),
            Value::String("host".into()),
            Value::String("/tmp/x".into()),
            Value::String("mock".into()),
            Value::String("/tmp/x".into()),
        ])
    );
    assert_eq!(
        eval_str(
            r#"(list
                     (file-local-name "/tmp/local")
                     (file-local-name "/ssh:user@host:/tmp/x"))"#,
        ),
        Value::list([
            Value::String("/tmp/local".into()),
            Value::String("/tmp/x".into()),
        ])
    );
}

#[test]
fn mock_remote_home_localname_uses_the_local_host_home() {
    assert_eq!(
        eval_str(
            r#"(let* ((remote "/mock::~/file.txt")
                       (expected (expand-file-name "~/file.txt")))
                  (list (equal (file-local-name remote) expected)
                        (equal (file-remote-p remote 'localname) expected)
                        (file-directory-p "/mock::~/")))"#,
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn copy_alist_copies_entry_cells() {
    assert_eq!(
        eval_str(
            "(let* ((orig (list (cons 'a 1)))
                        (copy (copy-alist orig)))
                   (setcdr (car copy) 2)
                   (list orig copy))"
        ),
        Value::list([
            Value::list([Value::cons(Value::symbol("a"), Value::int(1))]),
            Value::list([Value::cons(Value::symbol("a"), Value::int(2))]),
        ])
    );
}

#[test]
fn copy_alist_rejects_vectors_and_strings() {
    assert!(matches!(
        eval_str("(condition-case err (copy-alist [(a . 1)]) (wrong-type-argument 'caught))"),
        Value::Symbol(name) if name == "caught"
    ));
    assert!(matches!(
        eval_str("(condition-case err (copy-alist \"abc\") (wrong-type-argument 'caught))"),
        Value::Symbol(name) if name == "caught"
    ));
}

#[test]
fn float_constants_are_available_as_builtin_variables() {
    assert_eq!(eval_str("float-e"), Value::Float(std::f64::consts::E));
    assert_eq!(eval_str("float-pi"), Value::Float(std::f64::consts::PI));
}

#[test]
fn preloaded_system_name_variable_matches_the_host_primitive() {
    assert_eq!(
        eval_str(
            "(list (boundp 'system-name) (stringp system-name)\
                        (equal system-name (system-name)))"
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn gc_counter_variables_are_available_for_benchmark() {
    assert_eq!(
        eval_str("(list gcs-done gc-elapsed)"),
        Value::list([Value::Integer(0), Value::Float(0.0)])
    );
}

#[test]
fn case_fold_search_is_special_and_auto_buffer_local() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(progn
                     (defun case-fold-helper ()
                       (string-match-p "A" "a"))
                     (list
                      (let ((case-fold-search nil))
                        (case-fold-helper))
                      (with-temp-buffer
                        (setq case-fold-search nil)
                        (default-value 'case-fold-search))
                      case-fold-search))"#
        ),
        Value::list([Value::Nil, Value::T, Value::T])
    );
}

#[test]
fn line_formats_are_bound_special_and_auto_buffer_local() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let ((default-mode-line mode-line-format))
                 (list
                  (mapcar (lambda (variable)
                            (list (boundp variable)
                                  (special-variable-p variable)
                                  (local-variable-if-set-p variable)))
                          '(mode-line-format header-line-format tab-line-format))
                  (with-temp-buffer
                    (setq mode-line-format '(temporary))
                    (list mode-line-format
                          (equal (default-value 'mode-line-format)
                                 default-mode-line)))
                  (equal mode-line-format default-mode-line)))"#
        ),
        Value::list([
            Value::list([
                Value::list([Value::T, Value::T, Value::T]),
                Value::list([Value::T, Value::T, Value::T]),
                Value::list([Value::T, Value::T, Value::T]),
            ]),
            Value::list([Value::list([Value::Symbol("temporary".into())]), Value::T,]),
            Value::T,
        ])
    );
}

#[test]
fn editing_command_state_defaults_are_bound() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(list
                    buffer-read-only
                    this-command
                    last-command
                    (with-temp-buffer
                      (setq buffer-read-only t)
                      (default-value 'buffer-read-only))
                    buffer-read-only)"#
        ),
        Value::list([Value::Nil, Value::Nil, Value::Nil, Value::Nil, Value::Nil])
    );
}

#[test]
fn process_runtime_defaults_match_dumped_process_c_state() {
    assert_eq!(
        eval_str(
            "(list delete-exited-processes
                   default-process-coding-system
                   process-connection-type
                   process-adaptive-read-buffering
                   process-prioritize-lower-fds
                   interrupt-process-functions
                   signal-process-functions
                   internal--daemon-sockname
                   read-process-output-max
                   fast-read-process-output
                   process-error-pause-time)"
        ),
        Value::list([
            Value::T,
            Value::cons(
                Value::Symbol("utf-8-unix".into()),
                Value::Symbol("utf-8-unix".into()),
            ),
            Value::T,
            Value::T,
            Value::Nil,
            Value::list([Value::Symbol("internal-default-interrupt-process".into())]),
            Value::list([Value::Symbol("internal-default-signal-process".into())]),
            Value::Nil,
            Value::Integer(65_536),
            Value::T,
            Value::Integer(1),
        ])
    );
}

#[test]
fn coding_system_eol_type_treats_nil_as_no_conversion() {
    assert_eq!(eval_str("(coding-system-eol-type nil)"), Value::Integer(0));
}

#[test]
fn buffer_read_only_let_binding_is_local_to_its_buffer() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                  (let ((buffer-read-only t))
                    (list
                     (condition-case err
                         (progn (insert "blocked") 'wrong)
                       (buffer-read-only (car err)))
                     (with-temp-buffer
                       (insert "writable")
                       (buffer-string))
                     buffer-read-only)))"#
        ),
        Value::list([
            Value::Symbol("buffer-read-only".into()),
            Value::String("writable".into()),
            Value::T,
        ])
    );
}

#[test]
fn new_buffers_inherit_default_directory_but_not_read_only() {
    assert_eq!(
        eval_str(
            r#"(let ((default-directory "/tmp/emaxx-inherited-directory/")
                     (buffer-read-only t))
                  (with-temp-buffer
                    (list default-directory buffer-read-only)))"#,
        ),
        Value::list([
            Value::String("/tmp/emaxx-inherited-directory/".into()),
            Value::Nil,
        ])
    );
}

#[test]
fn trimmed_closure_frame_does_not_alias_same_shaped_caller_frame() {
    let mut interp = Interpreter::new();
    interp.push_lambda_eval_context(true, true);
    let result = eval_str_with(
        &mut interp,
        r#"(let* ((make-inner
                    (lambda (string pred action)
                      (lambda () action)))
                   (inner (funcall make-inner "captured" nil nil))
                   (make-caller
                    (lambda (string pred action)
                      (lambda ()
                        (ignore string pred action)
                        (funcall inner))))
                   (caller
                    (funcall make-caller "current" nil 'lambda)))
              (funcall caller))"#,
    );
    interp.pop_lambda_capture_override();

    assert_eq!(result, Value::Nil);
}

#[test]
fn letstar_initializer_closure_does_not_capture_a_later_binding() {
    let mut interp = Interpreter::new();
    interp.set_global_binding("later-binding", Value::Symbol("global".into()));
    interp.push_lambda_eval_context(true, false);
    let result = eval_str_with(
        &mut interp,
        r#"(let* ((reader (lambda () later-binding))
                      (later-binding 'local))
                 (funcall reader))"#,
    );
    interp.pop_lambda_capture_override();

    assert_eq!(result, Value::Symbol("global".into()));
}

#[test]
fn locate_user_emacs_file_uses_user_emacs_directory() {
    assert_eq!(
        eval_str(r#"(locate-user-emacs-file "ido.last" ".ido.last")"#),
        Value::String("/nonexistent/.emacs.d/ido.last".into())
    );
}

fn assert_seq_some_returns_first_truthy_result() {
    assert_eq!(
        eval_str(r#"(seq-some #'identity '(nil nil ok))"#),
        Value::Symbol("ok".into())
    );
}

#[test]
fn seq_some_returns_first_truthy_result() {
    run_large_stack_test(assert_seq_some_returns_first_truthy_result);
}

#[test]
fn remove_function_is_a_safe_noop_for_nil_function_slots() {
    assert_eq!(
        eval_str(
            r#"(progn
                     (setq read-file-name-function nil)
                     (remove-function read-file-name-function #'ignore)
                     read-file-name-function)"#
        ),
        Value::Nil
    );
}

#[test]
fn directory_listing_regexp_matches_common_ls_output() {
    assert_ne!(
        eval_str(
            r#"(string-match-p directory-listing-before-filename-regexp
                                    "-rw-r--r--@    1 alpha  staff      0 Mar 16 04:57 foo.c")"#
        ),
        Value::Nil
    );
}

#[test]
fn defvar_local_loads_like_defvar() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(&mut interp, "(defvar-local sample-local :default)"),
        Value::Nil
    );
    assert_eq!(
        eval_str_with(&mut interp, "sample-local"),
        Value::Symbol(":default".into())
    );
}

#[test]
fn defcustom_loads_like_defvar() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(defcustom treesit-max-buffer-size 42 \"doc\")"
        ),
        Value::Nil
    );
    assert_eq!(
        eval_str_with(&mut interp, "treesit-max-buffer-size"),
        Value::Integer(42)
    );
}

#[test]
fn setopt_runs_defcustom_setter() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (defun sample-setter (symbol value)
                     (set-default symbol value)
                     (setq sample-setter-result value))
                   (defcustom sample-option nil \"doc\" :set #'sample-setter :type 'boolean)
                   (setopt sample-option t)
                   (list sample-option
                         sample-setter-result
                         (get 'sample-option 'custom-set)
                         (get 'sample-option 'custom-type)))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::Symbol("sample-setter".into()),
            Value::Symbol("boolean".into()),
        ])
    );
}

#[test]
fn customize_set_variable_runs_defcustom_setter() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        eval_str_with(
            &mut interp,
            "(defun sample-setter (symbol value)
                   (set-default symbol value)
                   (setq sample-setter-result value))",
        );
        eval_str_with(
            &mut interp,
            "(defcustom sample-option nil \"doc\" :set #'sample-setter :type 'boolean)",
        );

        let forms = Reader::new("(customize-set-variable 'sample-option t)")
            .read_all()
            .expect("parse customize-set-variable form");
        assert_eq!(interp.eval(&forms[0], &mut env).unwrap(), Value::T);
        assert_eq!(interp.lookup("sample-option", &env).unwrap(), Value::T);
        assert_eq!(
            interp.lookup("sample-setter-result", &env).unwrap(),
            Value::T
        );
    });
}

#[test]
fn switch_to_buffer_accepts_bound_string_values() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((buffer-name \"foo\"))
                   (switch-to-buffer buffer-name)
                   (buffer-name))"
        ),
        Value::String("foo".into())
    );
}

#[test]
fn window_buffer_tracks_selected_buffer() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (switch-to-buffer \"foo\")
                   (buffer-name (window-buffer (selected-window))))"
        ),
        Value::String("foo".into())
    );
}

#[test]
fn with_temp_buffer_does_not_change_the_selected_windows_buffer() {
    assert_eq!(
        eval_str(
            "(let ((shown (window-buffer)))
               (with-temp-buffer
                 (list (eq (window-buffer) (current-buffer))
                       (eq (window-buffer) shown))))"
        ),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn set_window_buffer_accepts_nil_for_selected_window() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((buffer (get-buffer-create \" set-window-buffer-target\")))
                   (set-window-buffer nil buffer)
                   (eq (window-buffer) buffer))"
        ),
        Value::T
    );
}

#[test]
fn window_parameters_round_trip_and_support_setf() {
    assert_eq!(
        eval_str(
            "(let ((window (selected-window)))
                 (set-window-parameter window 'alpha 1)
                 (setf (window-parameter nil 'beta) 2)
                 (list (window-parameter window 'alpha)
                       (window-parameter nil 'beta)
                       (progn
                         (set-window-parameter window 'alpha nil)
                         (window-parameter window 'alpha))
                       (assq 'beta (window-parameters window))))"
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(2),
            Value::Nil,
            Value::cons(Value::Symbol("beta".into()), Value::Integer(2)),
        ])
    );
}

#[test]
fn killing_selected_window_buffer_moves_window_to_live_buffer() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((victim (get-buffer-create \" kill-window-buffer-victim\")))
                   (set-window-buffer nil victim)
                   (kill-buffer victim)
                   (buffer-live-p (window-buffer)))"
        ),
        Value::T
    );
}

#[test]
fn kill_buffer_nil_kills_the_current_buffer() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let ((victim (get-buffer-create \" kill-buffer-nil-victim\")))
               (switch-to-buffer victim)
               (list (kill-buffer nil) (buffer-live-p victim)))"
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn visual_line_mode_sets_buffer_local_state() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
                   (visual-line-mode)
                   (list visual-line-mode
                         (local-variable-p 'visual-line-mode)
                         (progn (visual-line-mode 0) visual-line-mode)))"
        ),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn ert_simulate_command_runs_hooks_and_tracks_commands() {
    assert_eq!(
        eval_str(
            "(let (events)
                   (fset 'emaxx-test-command
                         (lambda (arg)
                           (interactive)
                           (push (list 'command arg this-command) events)
                           'done))
                   (add-hook 'pre-command-hook
                             (lambda () (push (list 'pre this-command) events)))
                   (add-hook 'post-command-hook
                             (lambda () (push (list 'post this-command) events)))
                   (list (ert-simulate-command '(emaxx-test-command 7))
                         last-command
                         real-last-command
                         (nreverse events)))"
        ),
        Value::list([
            Value::Symbol("done".into()),
            Value::Symbol("emaxx-test-command".into()),
            Value::Symbol("emaxx-test-command".into()),
            Value::list([
                Value::list([
                    Value::Symbol("pre".into()),
                    Value::Symbol("emaxx-test-command".into()),
                ]),
                Value::list([
                    Value::Symbol("command".into()),
                    Value::Integer(7),
                    Value::Symbol("emaxx-test-command".into()),
                ]),
                Value::list([
                    Value::Symbol("post".into()),
                    Value::Symbol("emaxx-test-command".into()),
                ]),
            ]),
        ])
    );
}

#[test]
fn display_warning_records_message_and_returns_nil() {
    assert_eq!(
        eval_str(
            r#"(progn
                     (display-warning 'todo "check this" :warning)
                     (current-message))"#
        ),
        Value::String("Warning (todo): check this".into())
    );
}

#[test]
fn deactivate_mark_clears_active_region() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                     (insert "abc")
                     (set-mark 1)
                     (goto-char 3)
                     (list (region-active-p)
                           (deactivate-mark)
                           (region-active-p)))"#
        ),
        Value::list([Value::T, Value::Nil, Value::Nil])
    );
}

#[test]
fn bury_buffer_moves_buffer_to_end_and_selects_next_buffer() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let ((first (current-buffer))
                         (second (get-buffer-create " *bury-second*")))
                     (switch-to-buffer second)
                     (bury-buffer)
                     (list (eq (current-buffer) first)
                           (eq (car (last (buffer-list))) second)))"#
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn scroll_up_moves_point_with_window_when_point_would_scroll_off_top() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(with-temp-buffer
                   (insert \"\\n\\n\\n\")
                   (goto-char (point-min))
                   (switch-to-buffer (current-buffer))
                   (scroll-up 1)
                   (list (window-start)
                         (save-excursion (move-to-window-line 0) (point))
                         (point)))"
        ),
        Value::list([Value::Integer(2), Value::Integer(2), Value::Integer(2)])
    );
}

#[test]
fn scroll_up_respects_scroll_preserve_screen_position() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(with-temp-buffer
                   (insert \"\\n\\n\\n\")
                   (goto-char (+ (point-min) 1))
                   (switch-to-buffer (current-buffer))
                   (let ((scroll-preserve-screen-position 'always))
                     (scroll-up 1)
                     (list (window-start)
                           (save-excursion (move-to-window-line 1) (point))
                           (point))))"
        ),
        Value::list([Value::Integer(2), Value::Integer(3), Value::Integer(3)])
    );
}

#[test]
fn define_minor_mode_enables_buffer_local_state_and_runs_body() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (define-minor-mode sample-mode \"doc\"
                     (setq sample-mode-ran sample-mode))
                   (sample-mode 1)
                   (let ((enabled sample-mode)
                         (ran sample-mode-ran))
                     (switch-to-buffer \"other\")
                     (list enabled sample-mode ran)))"
        ),
        Value::list([Value::T, Value::Nil, Value::T])
    );
}

#[test]
fn define_global_minor_mode_uses_default_value_even_if_variable_becomes_buffer_local() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (define-minor-mode sample-global-mode \"doc\" :global t)
                   (make-variable-buffer-local 'sample-global-mode)
                   (sample-global-mode 1)
                   (switch-to-buffer \"other\")
                   (list sample-global-mode
                         (default-value 'sample-global-mode)
                         (local-variable-p 'sample-global-mode)))"
        ),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn defcustom_local_options_are_buffer_local_and_permanent_when_requested() {
    assert_eq!(
        eval_str(
            "(progn
                   (defcustom sample-local-option 'initial \"doc\" :local t)
                   (defcustom sample-permanent-option 'initial \"doc\" :local 'permanent)
                   (with-temp-buffer
                     (setq sample-local-option 'changed)
                     (setq sample-permanent-option 'changed)
                     (let ((before (list sample-local-option
                                         sample-permanent-option
                                         (default-value 'sample-local-option)
                                         (default-value 'sample-permanent-option))))
                       (kill-all-local-variables)
                       (list before
                             sample-local-option
                             sample-permanent-option
                             (default-value 'sample-local-option)
                             (default-value 'sample-permanent-option)))))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("changed".into()),
                Value::Symbol("changed".into()),
                Value::Symbol("initial".into()),
                Value::Symbol("initial".into()),
            ]),
            Value::Symbol("initial".into()),
            Value::Symbol("changed".into()),
            Value::Symbol("initial".into()),
            Value::Symbol("initial".into()),
        ])
    );
}

#[test]
fn defcustom_uses_stashed_non_user_theme_value_once() {
    assert_eq!(
        eval_str(
            "(progn
                   (put 'sample-themed-option 'theme-value
                        '((sample-theme 'theme-value)))
                   (put 'sample-themed-option 'saved-value
                        '('theme-value))
                   (defcustom sample-themed-option 'standard \"doc\")
                   (list sample-themed-option
                         (eval (car (get 'sample-themed-option 'standard-value)))
                         (get 'sample-themed-option 'saved-value)))"
        ),
        Value::list([
            Value::Symbol("theme-value".into()),
            Value::Symbol("standard".into()),
            Value::Nil,
        ])
    );
}

#[test]
fn define_minor_mode_call_without_arg_enables_instead_of_toggling() {
    assert_eq!(
        eval_str(
            "(progn
                   (define-minor-mode sample-mode \"doc\")
                   (sample-mode)
                   (sample-mode)
                   sample-mode)"
        ),
        Value::T
    );
}

#[test]
fn define_global_minor_mode_call_without_arg_enables_instead_of_toggling() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (define-minor-mode sample-global-mode \"doc\" :global t)
                   (make-variable-buffer-local 'sample-global-mode)
                   (sample-global-mode)
                   (switch-to-buffer \"other\")
                   (sample-global-mode)
                   (list sample-global-mode
                         (default-value 'sample-global-mode)))"
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn defvar_keymap_supports_custom_setters_toggling_bindings() {
    assert_eq!(
        eval_str_with_upstream_load_path(
            "(progn
                   (defun sample-option-setter (symbol value)
                     (if value
                         (keymap-unset sample-map \"C-c <left>\")
                       (keymap-set sample-map \"C-c <left>\" #'sample-command))
                     (set-default symbol value))
                   (defcustom sample-flag nil \"doc\" :set #'sample-option-setter)
                   (defvar-keymap sample-map :doc \"doc\")
                   (setopt sample-flag sample-flag)
                   (list
                    (keymap-lookup sample-map \"C-c <left>\")
                    (progn
                      (setopt sample-flag t)
                      (keymap-lookup sample-map \"C-c <left>\"))
                    (progn
                      (setopt sample-flag nil)
                      (keymap-lookup sample-map \"C-c <left>\"))))"
        ),
        Value::list([
            Value::Symbol("sample-command".into()),
            Value::Nil,
            Value::Symbol("sample-command".into()),
        ])
    );
}

#[test]
fn defvar_keymap_supports_read_only_filter_bindings() {
    assert_eq!(
        eval_str(
            "(progn
                   (defvar-keymap sample-read-only-map
                     \"a\" (keymap-read-only-bind #'ignore))
                   (list
                    (lookup-key sample-read-only-map \"a\")
                    (progn
                      (setq buffer-read-only t)
                      (lookup-key sample-read-only-map \"a\"))))"
        ),
        Value::list([Value::Nil, Value::Symbol("ignore".into())])
    );
}

#[test]
fn declaration_stub_forms_do_not_error_during_loads() {
    assert_eq!(
        eval_str(
            "(progn
                   (defgroup treesit nil \"doc\")
                   (defface treesit-face '((t :inherit default)) \"doc\")
                   (defvar-keymap treesit-map :doc \"doc\")
                   (define-minor-mode treesit-mode \"doc\")
                   (define-globalized-minor-mode global-treesit-mode treesit-mode ignore)
                   (define-derived-mode treesit-derived fundamental-mode \"TS\")
                   (cl-defstruct (ppss (:constructor make-ppss) (:type list)) depth)
                   (and (keymapp treesit-map)
                        (boundp 'treesit-mode)
                        (fboundp 'treesit-mode)
                        (boundp 'global-treesit-mode)
                        (fboundp 'treesit-derived)))"
        ),
        Value::T
    );
}

#[test]
fn face_attribute_tracks_runtime_values_and_inheritance() {
    let value = eval_str(
        "(progn
               (defface parent-face '((t (:foreground \"white\"))) \"doc\")
               (defface child-face '((t (:inherit default))) \"doc\")
               (set-face-attribute 'parent-face nil :foreground \"blue\")
               (set-face-attribute 'child-face nil :inherit 'parent-face)
               (list
                (face-attribute 'tool-bar :foreground)
                (face-attribute 'parent-face :foreground)
                (face-attribute 'child-face :foreground nil t)
                (face-attribute 'child-face :inherit)))",
    );
    let items = value.to_vec().unwrap();
    assert_eq!(items[0], Value::Symbol("unspecified".into()));
    assert_eq!(primitives::string_text(&items[1]).unwrap(), "blue");
    assert_eq!(primitives::string_text(&items[2]).unwrap(), "blue");
    assert_eq!(items[3], Value::Symbol("parent-face".into()));
}

#[test]
fn facep_recognizes_defined_faces() {
    assert_eq!(
        eval_str(
            "(progn
                   (defface sample-face '((t (:foreground \"red\"))) \"doc\")
                   (list
                    (facep 'sample-face)
                    (facep \"sample-face\")
                    (face-name 'sample-face)
                    (facep 'missing-face)))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::String("sample-face".into()),
            Value::Nil,
        ])
    );
}

#[test]
fn face_list_includes_defined_faces() {
    assert_eq!(
        eval_str(
            "(progn
                   (defface sample-listed-face '((t (:foreground \"red\"))) \"doc\")
                   (and (memq 'default (face-list))
                        (memq 'sample-listed-face (face-list))
                        t))"
        ),
        Value::T
    );
}

#[test]
fn cl_typep_recognizes_nil_and_cons_as_lists() {
    assert_eq!(
        eval_str("(list (cl-typep nil 'list) (cl-typep '(a b) 'list) (cl-typep 'a 'list))"),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn cl_typep_recognizes_eieio_records_as_eieio_objects() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-eieio-root nil nil)
                   (cl-typep (make-instance 'sample-eieio-root) 'eieio-object))"
        ),
        Value::T
    );
}

#[test]
fn eieio_internal_object_class_reports_record_class_name() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-eieio-class-name nil nil)
                   (eieio--class-name
                    (eieio--object-class
                     (make-instance 'sample-eieio-class-name))))"
        ),
        Value::Symbol("sample-eieio-class-name".into())
    );
}

#[test]
fn eieio_object_p_and_slot_boundp_accept_record_backed_instances() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-eieio-slot-bound nil
                     ((known :initform nil)))
                   (let ((object (make-instance 'sample-eieio-slot-bound)))
                     (list (eieio-object-p object)
                           (slot-boundp object 'known)
                           (slot-boundp object 'missing))))"
        ),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn eieio_slots_without_initform_start_unbound() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-eieio-unbound nil
                     ((missing-initform)
                      (explicit-nil :initform nil)))
                   (let ((object (make-instance 'sample-eieio-unbound)))
                     (list (slot-boundp object 'missing-initform)
                           (slot-boundp object 'explicit-nil)
                           (condition-case err
                               (eieio-oref object 'missing-initform)
                             (unbound-slot (car err))))))"
        ),
        Value::list([Value::Nil, Value::T, Value::Symbol("unbound-slot".into())])
    );
}

#[test]
fn eieio_clone_copies_record_backed_instances_and_applies_initargs() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-eieio-clone nil
                     ((name :initarg :name :initform \"old\")
                      (count :initarg :count :initform 1)))
                   (let* ((object (make-instance 'sample-eieio-clone :name \"old\"))
                          (copy (clone object :name \"new\")))
                     (list (not (eq object copy))
                           (eieio-oref object 'name)
                           (eieio-oref copy 'name)
                           (eieio-oref copy 'count))))"
        ),
        Value::list([
            Value::T,
            Value::String("old".into()),
            Value::String("new".into()),
            Value::Integer(1),
        ])
    );
}

#[test]
fn set_face_attribute_rejects_unknown_faces() {
    assert_eq!(
        eval_str(
            "(condition-case err
                     (progn
                       (set-face-attribute 'runtime-face nil :foreground \"blue\")
                       'ok)
                   (error err))"
        ),
        Value::list([
            Value::Symbol("error".into()),
            Value::String("Invalid face".into()),
            Value::Symbol("runtime-face".into()),
        ])
    );
}

#[test]
fn defface_only_records_default_display_clauses() {
    assert_eq!(
            eval_str(
                "(progn
                   (defface sample-nongraphic-face '((((type graphic)) :foreground \"red\")) \"doc\")
                   (face-attribute 'sample-nongraphic-face :foreground))"
            ),
            Value::Symbol("unspecified".into())
        );
}

#[test]
fn defface_records_nested_default_plists() {
    assert_eq!(
        eval_str(
            "(progn
                   (defface sample-nested-face '((t (:weight bold :extend t))) \"doc\")
                   (list
                    (face-attribute 'sample-nested-face :weight)
                    (face-attribute 'sample-nested-face :extend)))"
        ),
        Value::list([Value::Symbol("bold".into()), Value::T])
    );
}

#[test]
fn faces_compat_provides_face_ids_and_colors_at_point() {
    let mut interp = Interpreter::new();
    load_faces_compat(&mut interp);

    let value = eval_str_with(
        &mut interp,
        "(progn
               (defface sample-face '((t :foreground \"red\" :background \"blue\")) \"doc\")
               (with-temp-buffer
                 (insert (propertize \"x\" 'face '(sample-face)))
                 (goto-char 1)
                 (list
                  (face-id 'sample-face)
                  (face-id 'tooltip)
                  (foreground-color-at-point)
                  (background-color-at-point))))",
    );
    let items = value.to_vec().unwrap();
    assert_eq!(items[0], Value::Integer(2));
    assert_eq!(items[1], Value::Integer(1));
    assert_string_value(items[2].clone(), "red");
    assert_string_value(items[3].clone(), "blue");
}

#[test]
fn faces_compat_preserves_builtin_user_themes() {
    let mut interp = Interpreter::new();
    load_faces_compat(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(and (memq 'user custom-known-themes)
                  (memq 'changed custom-known-themes)
                  t)"
        ),
        Value::T
    );
}

#[test]
fn set_frame_parameter_accepts_batch_theme_updates() {
    let mut interp = Interpreter::new();
    load_faces_compat(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list
              (window-system 'frame)
              (face-set-after-frame-default 'frame)
              (frame-terminal 'frame)
              (set-frame-parameter 'frame 'background-color \"white\"))"
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::Symbol("terminal".into()),
            Value::String("white".into()),
        ])
    );
}

#[test]
fn faces_compat_color_at_point_skips_unspecified_faces() {
    let mut interp = Interpreter::new();
    load_faces_compat(&mut interp);

    let value = eval_str_with(
        &mut interp,
        r#"(progn
                 (defface sample-color-face
                   '((t :background "black" :foreground "black"))
                   "doc")
                 (defface sample-box-face '((t :box 1)) "doc")
                 (with-temp-buffer
                   (insert (propertize "STRING"
                                       'face
                                       '(sample-box-face sample-color-face)))
                   (goto-char (point-min))
                   (list (background-color-at-point)
                         (foreground-color-at-point))))"#,
    );
    let items = value.to_vec().unwrap();
    assert_string_value(items[0].clone(), "black");
    assert_string_value(items[1].clone(), "black");
}

#[test]
fn faces_compat_color_at_point_matches_upstream_cases() {
    let mut interp = Interpreter::new();
    load_faces_compat(&mut interp);

    let value = eval_str_with(
        &mut interp,
        r#"(progn
                 (defface sample-color-face
                   '((t :background "black" :foreground "black"))
                   "doc")
                 (defface sample-box-face '((t :box 1)) "doc")
                 (list
                  (with-temp-buffer
                    (insert (propertize "STRING"
                                        'face
                                        '(sample-box-face sample-color-face)))
                    (goto-char (point-min))
                    (list (background-color-at-point)
                          (foreground-color-at-point)))
                  (with-temp-buffer
                    (insert (propertize "STRING"
                                        'face
                                        '(:foreground "black" :background "black")))
                    (goto-char (point-min))
                    (list (background-color-at-point)
                          (foreground-color-at-point)))
                  (with-temp-buffer
                    (emacs-lisp-mode)
                    (setq-local font-lock-comment-face 'sample-color-face)
                    (setq-local font-lock-constant-face 'sample-box-face)
                    (insert ";; `symbol'")
                    (font-lock-fontify-region (point-min) (point-max))
                    (goto-char (point-min))
                    (let ((comment (list (background-color-at-point)
                                         (foreground-color-at-point))))
                      (goto-char 6)
                      (list comment
                            (list (background-color-at-point)
                                  (foreground-color-at-point)))))))"#,
    );
    let cases = value.to_vec().unwrap();
    assert_eq!(cases.len(), 3);
    for pair in cases[0].to_vec().unwrap() {
        assert_string_value(pair, "black");
    }
    for pair in cases[1].to_vec().unwrap() {
        assert_string_value(pair, "black");
    }
    let font_lock_cases = cases[2].to_vec().unwrap();
    for pair in font_lock_cases[0].to_vec().unwrap() {
        assert_string_value(pair, "black");
    }
    for pair in font_lock_cases[1].to_vec().unwrap() {
        assert_string_value(pair, "black");
    }
}

#[test]
fn faces_compat_load_theme_recomputes_theme_faces() {
    run_large_stack_test(assert_faces_compat_load_theme_recomputes_theme_faces);
}

fn assert_faces_compat_load_theme_recomputes_theme_faces() {
    let mut interp = Interpreter::new();
    load_faces_compat(&mut interp);

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let theme_dir = std::env::temp_dir().join(format!("emaxx-theme-{unique}"));
    std::fs::create_dir_all(&theme_dir).unwrap();
    let theme_file = theme_dir.join("sample-theme-theme.el");
    std::fs::write(
        &theme_file,
        "(deftheme sample-theme \"doc\")\n\
             (custom-theme-set-faces 'sample-theme '(sample-base ((t (:extend t)))))\n\
             (provide-theme 'sample-theme)\n",
    )
    .unwrap();

    let theme_dir_literal = serde_json::to_string(&theme_dir.display().to_string()).unwrap();
    let program = format!(
        "(progn
               (defface sample-base '((t :background \"grey\")) \"doc\")
               (defface sample-child '((t :inherit sample-base)) \"doc\")
               (setq custom-theme-load-path (list {theme_dir_literal}))
               (load-theme 'sample-theme t t)
               (list
                (face-attribute 'sample-child :extend nil t)
                (progn
                  (enable-theme 'sample-theme)
                  (face-attribute 'sample-child :extend nil t))
                (progn
                  (disable-theme 'sample-theme)
                  (face-attribute 'sample-child :extend nil t))))"
    );

    assert_eq!(
        eval_str_with(&mut interp, &program),
        Value::list([
            Value::Symbol("unspecified".into()),
            Value::T,
            Value::Symbol("unspecified".into()),
        ])
    );
}

#[test]
fn hash_table_iteration_and_mutation_primitives_cover_ert_cases() {
    run_large_stack_test(assert_hash_table_iteration_and_mutation_primitives_cover_ert_cases);
}

#[test]
fn hash_table_eq_test_uses_cons_identity() {
    assert_eq!(
        eval_str(
            "(let ((ht (make-hash-table :test #'eq))
                       (left (list 'a))
                       (right (list 'a)))
                   (puthash left 'left ht)
                   (list (gethash left ht)
                         (gethash right ht 'missing)
                         (hash-table-count ht)))"
        ),
        Value::list([
            Value::Symbol("left".into()),
            Value::Symbol("missing".into()),
            Value::Integer(1),
        ])
    );
}

#[test]
fn eq_distinguishes_cons_cells_that_share_car_storage() {
    assert_eq!(
        eval_str(
            "(let* ((cell (list 'a))
                        (left (cons (car cell) nil))
                        (right (cons (car cell) nil)))
                   (eq left right))"
        ),
        Value::Nil
    );
}

fn assert_hash_table_iteration_and_mutation_primitives_cover_ert_cases() {
    assert_eq!(
        eval_str(
            "(let ((ht (make-hash-table :test #'equal))
                       (seen nil))
                   (puthash \"a\" 1 ht)
                   (puthash \"b\" 2 ht)
                   (list
                    (maphash (lambda (key value)
                               (push (cons key value) seen))
                             ht)
                    (progn
                      (remhash \"a\" ht)
                      (hash-table-count ht))
                    (gethash \"a\" ht 'missing)
                    (let ((cleared (clrhash ht)))
                      (list (hash-table-p cleared)
                            (hash-table-count ht)))
                    (length seen)))"
        ),
        Value::list([
            Value::Nil,
            Value::Integer(1),
            Value::Symbol("missing".into()),
            Value::list([Value::T, Value::Integer(0)]),
            Value::Integer(2),
        ])
    );
}

#[test]
fn hash_table_copy_and_clear_string_cover_password_cache_cases() {
    let result = eval_str(
        "(let ((original (make-hash-table :test #'equal)))
               (puthash \"foo\" 1 original)
               (let ((copy (copy-hash-table original))
                     (secret (copy-sequence \"bar\")))
                 (puthash \"bar\" 2 copy)
                 (clear-string secret)
                 (list
                  (hash-table-contains-p \"foo\" copy)
                  (hash-table-contains-p \"bar\" original)
                  (hash-table-count copy)
                  (hash-table-count original)
                  secret)))",
    );
    let items = result.to_vec().unwrap();
    assert_eq!(
        items,
        vec![
            Value::T,
            Value::Nil,
            Value::Integer(2),
            Value::Integer(1),
            items[4].clone(),
        ]
    );
    assert_string_value(items[4].clone(), "\0\0\0");
}

#[test]
fn thread_join_executes_zero_arg_lambda_with_closure_state() {
    assert_eq!(
        eval_str("(let ((value 42)) (thread-join (make-thread (lambda () value))))"),
        Value::Integer(42)
    );
}

#[test]
fn custom_hash_table_tests_are_registered_and_used_for_lookup() {
    assert_eq!(
        eval_str(
            "(let ((calls 0))
                   (defun my-cmp (a b)
                     (setq calls (1+ calls))
                     (equal a b))
                   (defun my-hash (_value) 0)
                   (let ((spec (define-hash-table-test 'my-test 'my-cmp 'my-hash))
                         (table (make-hash-table :test 'my-test)))
                     (puthash \"a\" 1 table)
                     (list spec
                           (hash-table-test table)
                           (gethash (copy-sequence \"a\") table 'missing)
                           (> calls 0))))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("my-cmp".into()),
                Value::Symbol("my-hash".into()),
            ]),
            Value::Symbol("my-test".into()),
            Value::Integer(1),
            Value::T,
        ])
    );
}

#[test]
fn custom_hash_table_hash_functions_cannot_mutate_their_table() {
    assert_eq!(
        eval_str(
            "(progn
                   (define-hash-table-test 'badeq 'eq 'bad-hash)
                   (let ((h (make-hash-table :test 'badeq :size 1 :rehash-size 1)))
                     (defun bad-hash (k)
                       (if (eq k 100)
                           (clrhash h))
                       (sxhash-eq k))
                     (should-error
                      (dotimes (k 200)
                        (puthash k k h)))
                     (hash-table-count h)))"
        ),
        Value::Integer(100)
    );
}

#[test]
fn assoc_honors_optional_test_function() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                "(let ((alist '((\"a\" . 1) (\"b\" . 2))))
                       (list
                        (assoc \"a\" alist #'ignore)
                        (eq (assoc \"b\" alist #'string-equal) (cadr alist))
                        (assoc \"b\" alist #'eq)))"
            ),
            Value::list([Value::Nil, Value::T, Value::Nil])
        );
    });
}

#[test]
fn garbage_collect_prunes_synthetic_weak_hash_table_entries() {
    assert_eq!(
        eval_str(
            "(let ((table (make-hash-table :test 'equal :weakness 'key)))
                   (puthash \"00-key-alive\" \"00-val-alive\" table)
                   (puthash \"01-key-dead\" \"01-val-alive\" table)
                   (garbage-collect)
                   (list (hash-table-count table)
                         (gethash \"00-key-alive\" table)
                         (gethash \"01-key-dead\" table 'missing)))"
        ),
        Value::list([
            Value::Integer(1),
            Value::String("00-val-alive".into()),
            Value::Symbol("missing".into()),
        ])
    );
}

#[test]
fn require_edmacro_supports_edmacro_parse_keys_cases() {
    std::thread::Builder::new()
        .stack_size(4 * 1024 * 1024)
        .spawn(assert_require_edmacro_supports_edmacro_parse_keys_cases)
        .unwrap()
        .join()
        .unwrap();
}

fn assert_require_edmacro_supports_edmacro_parse_keys_cases() {
    assert_eq!(
        eval_str(
            "(progn
                   (require 'edmacro)
                    (and
                    (equal (edmacro-parse-keys \"\") [])
                    (equal (edmacro-parse-keys \"x ;; ignored\") [?x])
                    (equal (edmacro-parse-keys \"<<goto-line>>\")
                           [?\\M-x ?g ?o ?t ?o ?- ?l ?i ?n ?e ?\\r])
                    (equal (edmacro-parse-keys \"3*C-m\") [?\\C-m ?\\C-m ?\\C-m])
                    (equal (edmacro-parse-keys \"10*foo\")
                           (apply #'vconcat (make-list 10 [?f ?o ?o])))))"
        ),
        Value::T
    );
}

#[test]
fn let_alist_binds_dotted_pair_keys() {
    assert_string_value(
        eval_str("(let ((x '((buffer-text . \"hi\")))) (let-alist x .buffer-text))"),
        "hi",
    );
}

#[test]
fn cl_loop_supports_simple_for_from_to_do() {
    assert_eq!(
        eval_str("(let ((n 0)) (cl-loop for i from 1 to 3 do (setq n (+ n i))) n)"),
        Value::Integer(6)
    );
}

#[test]
fn cl_loop_supports_step_and_unless_collect() {
    assert_eq!(
        eval_str(
            "(cl-loop for i below 6 by 2
                          unless (memq i '(2))
                          collect (nth i '(:alpha \"sample\" :max 1 :omega 22)))"
        ),
        Value::list([
            Value::Symbol(":alpha".into()),
            Value::Symbol(":omega".into())
        ])
    );
}

#[test]
fn cl_loop_supports_repeat_collect() {
    assert_eq!(
        eval_str("(let ((n 0)) (cl-loop repeat 3 collect (setq n (1+ n))))"),
        Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3)])
    );
}

#[test]
fn cl_loop_supports_parallel_in_thereis_until() {
    assert_eq!(
        eval_str("(cl-loop for a in '(1 2 3) for b in '(1 3 2) thereis (< a b) until (> a b))"),
        Value::T
    );
}

#[test]
fn cl_loop_supports_destructuring_with_and_return() {
    assert_eq!(
        eval_str("(cl-loop with (a b c) = '(1 2 3) return (+ a b c))"),
        Value::Integer(6)
    );
}

#[test]
fn cl_loop_supports_dotted_pair_destructuring_for_in() {
    assert_eq!(
        eval_str("(cl-loop for (k . v) in '((a . 1) (b . 2)) collect (list k v))"),
        Value::list([
            Value::list([Value::Symbol("a".into()), Value::Integer(1)]),
            Value::list([Value::Symbol("b".into()), Value::Integer(2)]),
        ])
    );
}

#[test]
fn cl_loop_supports_when_collect_into_finally_return() {
    assert_eq!(
        eval_str(
            "(cl-loop for item in '((\"one\" . 1) (\"two\" . 2) (\"other\" . 3))
                          when (string-match \"^t\" (car item))
                          collect item into matches
                          finally return matches)"
        ),
        Value::list([Value::cons(Value::String("two".into()), Value::Integer(2))])
    );
}

#[test]
fn assq_delete_all_filters_matching_alist_keys() {
    assert_eq!(
        eval_str("(assq-delete-all 'drop '(noise (drop . a) (keep . b) (drop . c)))"),
        Value::list([
            Value::Symbol("noise".into()),
            Value::cons(Value::Symbol("keep".into()), Value::Symbol("b".into())),
        ])
    );
}

#[test]
fn assoc_delete_all_filters_matching_alist_keys_with_equal() {
    assert_eq!(
        eval_str("(assoc-delete-all \"drop\" '(noise (\"drop\" . a) (\"keep\" . b)))"),
        Value::list([
            Value::Symbol("noise".into()),
            Value::cons(Value::String("keep".into()), Value::Symbol("b".into())),
        ])
    );
}

#[test]
fn add_to_list_updates_quoted_variable() {
    let mut interp = Interpreter::new();
    eval_str_with(&mut interp, "(setq sample-list '(b c))");
    assert_eq!(
        eval_str_with(&mut interp, "(add-to-list 'sample-list 'a)"),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
        ])
    );
    assert_eq!(
        eval_str_with(&mut interp, "sample-list"),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
        ])
    );
    assert_eq!(
        eval_str_with(&mut interp, "(add-to-list 'sample-list 'c t)"),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
        ])
    );
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
                   (setq sample-strings '(\"a\"))
                   (add-to-list 'sample-strings (symbol-name 'a) t)
                   sample-strings)"
        ),
        Value::list([Value::String("a".into())])
    );
}

#[test]
fn cl_pushnew_supports_key_and_test_not() {
    assert_eq!(
        eval_str(
            "(let ((list '((1 2) (3 4))))
                   (cl-pushnew '(3 7) list :key #'cdr)
                   list)"
        ),
        Value::list([
            Value::list([Value::Integer(3), Value::Integer(7)]),
            Value::list([Value::Integer(1), Value::Integer(2)]),
            Value::list([Value::Integer(3), Value::Integer(4)]),
        ])
    );
    assert_eq!(
        eval_str(
            "(let ((list '((1 2) (3 4))))
                   (cl-pushnew '(3 5) list :test-not #'equal)
                   list)"
        ),
        Value::list([
            Value::list([Value::Integer(1), Value::Integer(2)]),
            Value::list([Value::Integer(3), Value::Integer(4)]),
        ])
    );
}

#[test]
fn push_uses_generalized_place_updates() {
    assert_eq!(
        eval_str(
            "(let ((cell '(root tail)))
                   (push 'middle (cdr cell))
                   cell)"
        ),
        Value::list([
            Value::Symbol("root".into()),
            Value::Symbol("middle".into()),
            Value::Symbol("tail".into()),
        ])
    );
    assert_eq!(
        eval_str(
            "(let ((ht (make-hash-table :test #'eq)))
                   (puthash 'item '(b) ht)
                   (push 'a (gethash 'item ht))
                   (gethash 'item ht))"
        ),
        Value::list([Value::Symbol("a".into()), Value::Symbol("b".into())])
    );
}

#[test]
fn generalized_place_subforms_are_evaluated_once_for_push() {
    assert_eq!(
        eval_str(
            "(let ((n 0)
                       (cell (list nil)))
                   (push 'x (car (progn (setq n (1+ n)) cell)))
                   (list n cell))"
        ),
        Value::list([
            Value::Integer(1),
            Value::list([Value::list([Value::Symbol("x".into())])]),
        ])
    );
}

#[test]
fn setf_nth_mutates_existing_list_cell() {
    assert_eq!(
        eval_str(
            "(let ((state (list 'depth 'last 'old)))
                   (setf (nth 2 state) 'new)
                   state)"
        ),
        Value::list([
            Value::Symbol("depth".into()),
            Value::Symbol("last".into()),
            Value::Symbol("new".into()),
        ])
    );
}

#[test]
fn define_abbrev_table_creates_real_runtime_table() {
    assert_eq!(
        eval_str(
            "(progn
                   (defvar sample-abbrevs nil)
                   (define-abbrev-table 'sample-abbrevs '((\"a\" \"alpha\" nil :case-fixed t)))
                   (abbrev-table-put sample-abbrevs :marker 'ok)
                   (list
                    (abbrev-table-p sample-abbrevs)
                    (abbrev-expansion \"a\" sample-abbrevs)
                    (abbrev-table-get sample-abbrevs :marker)
                    (abbrev-table-name sample-abbrevs)))"
        ),
        Value::list([
            Value::T,
            Value::String("alpha".into()),
            Value::Symbol("ok".into()),
            Value::Symbol("sample-abbrevs".into()),
        ])
    );
}

#[test]
fn derived_mode_add_parents_updates_runtime_mode_hierarchy() {
    assert_eq!(
        eval_str(
            "(progn
                   (define-derived-mode sample-parent fundamental-mode \"Parent\")
                   (define-derived-mode sample-child sample-parent \"Child\")
                   (defalias 'sample-alias #'sample-child)
                   (derived-mode-add-parents 'sample-parent '(sample-alias))
                   (setq major-mode 'sample-child)
                   (list
                    (derived-mode-p 'sample-parent)
                    (derived-mode-p 'sample-alias)
                    (derived-mode-p 'fundamental-mode)))"
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn derived_mode_all_parents_reports_parent_alias_and_extra_modes() {
    assert_eq!(
        eval_str(
            "(progn
                   (define-derived-mode sample-parent fundamental-mode \"Parent\")
                   (define-derived-mode sample-child sample-parent \"Child\")
                   (defalias 'sample-alias #'sample-child)
                   (derived-mode-add-parents 'sample-parent '(sample-alias))
                   (derived-mode-all-parents 'sample-child))"
        ),
        Value::list([
            Value::Symbol("sample-child".into()),
            Value::Symbol("sample-parent".into()),
            Value::Symbol("fundamental-mode".into()),
            Value::Symbol("sample-alias".into()),
        ])
    );
}

#[test]
fn defun_navigation_delegates_to_bound_mode_functions() {
    assert_eq!(
        eval_str(
            "(let (bod-param (eod-calls 0)
                       (beginning-of-defun-function
                        (lambda (arg) (setq bod-param arg) 'bod-result))
                       (end-of-defun-function
                        (lambda () (setq eod-calls (1+ eod-calls)))))
                   (let ((bod-value (beginning-of-defun 3))
                         (first-bod-param bod-param))
                     (end-of-defun)
                     (list bod-value first-bod-param bod-param eod-calls)))"
        ),
        Value::list([
            Value::T,
            Value::Integer(3),
            Value::Integer(-1),
            Value::Integer(2),
        ])
    );
}

#[test]
fn cl_letf_binds_special_variables_dynamically() {
    assert_eq!(
        eval_str(
            "(progn
                   (defvar cl-letf-probe-var nil)
                   (defun cl-letf-probe () cl-letf-probe-var)
                   (list (cl-letf ((cl-letf-probe-var t))
                           (let ((cl-letf-probe-var 'inner))
                             (cl-letf-probe)))
                         cl-letf-probe-var))"
        ),
        Value::list([Value::Symbol("inner".into()), Value::Nil])
    );
}

#[test]
fn special_forms_resolve_through_function_cells() {
    assert_eq!(
        eval_str("(list (fboundp 'while) (macrop 'while) (eventp 1) (eventp ?A) (eventp 'foo))"),
        Value::list([Value::T, Value::Nil, Value::T, Value::T, Value::T])
    );
}

#[test]
fn defun_navigation_defaults_bracket_the_current_top_level_form() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
                   (insert \"(defun one ()\\n  1)\\n\\n(defun two ()\\n  2)\\n\\n(defun three ()\\n  3)\\n\")
                   (goto-char (point-min))
                   (search-forward \"defun two\")
                   (let (positions)
                     (end-of-defun)
                     (push (point) positions)
                     (beginning-of-defun)
                     (push (point) positions)
                     (nreverse positions)))"
        ),
        Value::list([Value::Integer(40), Value::Integer(21)])
    );
}

#[test]
fn forward_sexp_honors_syntax_table_category_properties() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
                   (set-syntax-table (make-syntax-table))
                   (modify-syntax-entry ?< \".\")
                   (modify-syntax-entry ?> \".\")
                   (put 'open-angle 'syntax-table '(4 . ?>))
                   (put 'close-angle 'syntax-table '(5 . ?<))
                   (insert \"<()>\")
                   (put-text-property 1 2 'category 'open-angle)
                   (put-text-property 4 5 'category 'close-angle)
                   (goto-char (point-min))
                   (forward-sexp)
                   (point))"
        ),
        Value::Integer(5)
    );
}

#[test]
fn define_derived_mode_installs_callable_mode_body() {
    let value = eval_str(
        "(progn
               (defun sample-parent-mode ()
                 (setq-local parent-ran t))
               (define-derived-mode sample-child-mode sample-parent-mode \"Child\"
                 (setq-local child-ran t))
               (with-temp-buffer
                 (sample-child-mode)
                 (list major-mode mode-name parent-ran child-ran)))",
    );
    let items = value.to_vec().unwrap();
    assert_eq!(items[0], Value::Symbol("sample-child-mode".into()));
    assert_string_value(items[1].clone(), "Child");
    assert_eq!(items[2], Value::T);
    assert_eq!(items[3], Value::T);
}

#[test]
fn define_derived_mode_delays_parent_hooks_and_runs_after_hooks() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (define-derived-mode sample-parent-mode fundamental-mode "P"
                   :after-hook
                   (let ((f (let ((x "S")) (lambda () x))))
                     (insert (format "AFP=%s " (let ((x "D")) x (funcall f)))))
                   (insert "PB "))
                 (define-derived-mode sample-child-mode sample-parent-mode "C"
                   :after-hook
                   (let ((f (let ((x "S")) (lambda () x))))
                     (insert (format "AFC=%s " (let ((x "D")) x (funcall f)))))
                   (insert "CB "))
                 (with-temp-buffer
                   (let ((sample-child-mode-hook (lambda () (insert "MH "))))
                     (sample-child-mode)
                     (buffer-string))))"#
        ),
        Value::String("PB CB MH AFP=S AFC=S ".into())
    );
}

#[test]
fn font_lock_add_keywords_accumulates_derived_mode_keywords() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (define-derived-mode mode-a fundamental-mode "mode-a"
                   (font-lock-add-keywords nil `(("a" 0 'font-lock-keyword-face))))
                 (define-derived-mode mode-b mode-a "mode-b"
                   (font-lock-add-keywords nil `(("b" 0 'font-lock-builtin-face))))
                 (define-derived-mode mode-c mode-b "mode-c"
                   (font-lock-add-keywords nil `(("c" 0 'font-lock-constant-face))))
                 (with-temp-buffer
                   (mode-c)
                   (equal font-lock-keywords
                          '(t (("c" 0 'font-lock-constant-face)
                               ("b" 0 'font-lock-builtin-face)
                               ("a" 0 'font-lock-keyword-face))
                              ("c" (0 'font-lock-constant-face))
                              ("b" (0 'font-lock-builtin-face))
                              ("a" (0 'font-lock-keyword-face))))))"#
        ),
        Value::T
    );
}

#[test]
fn face_alias_predicates_and_fringe_bitmap_fallback_load() {
    assert_eq!(
        eval_str(
            "(progn
               (defface sample-face '((t :foreground \"red\")) \"doc\")
               (list
                (face-equal 'sample-face 'sample-face)
                (face-equal 'sample-face 'default)
                (face-differs-from-default-p 'sample-face)
                (face-differs-from-default-p 'default)
                (define-obsolete-face-alias 'old-sample-face 'sample-face \"31.1\")
                (define-fringe-bitmap 'sample-bitmap [0 1 2])))"
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}
