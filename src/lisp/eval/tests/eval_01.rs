use super::*;

#[test]
fn eval_atoms() {
    assert_eq!(eval_str("42"), Value::Integer(42));
    assert_eq!(eval_str("\"hello\""), Value::String("hello".into()));
    assert_eq!(eval_str("nil"), Value::Nil);
    assert_eq!(eval_str("t"), Value::T);
}

#[test]
fn unmatched_throw_signals_no_catch_but_outer_catches_still_receive_throws() {
    assert_eq!(
        eval_str(
            r#"(list
                 (condition-case err
                     (throw 'missing 7)
                   (no-catch err))
                 (catch 'outer
                   (condition-case err
                       (throw 'outer 9)
                     (no-catch (list 'wrong err))))
                 (condition-case err
                     (let ((caught (copy-sequence "tag"))
                           (thrown (copy-sequence "tag")))
                       (catch caught (throw thrown 11)))
                   (no-catch (car err))))"#,
        ),
        Value::list([
            Value::list([
                Value::Symbol("no-catch".into()),
                Value::Symbol("missing".into()),
                Value::Integer(7),
            ]),
            Value::Integer(9),
            Value::Symbol("no-catch".into()),
        ])
    );
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
                         (initially-live (process-live-p child))
                         (pid (process-id child)))
                    (while (process-live-p child)
                      (sit-for 0.01))
                    ;; A second pump must not repeat either terminal event.
                    (sit-for 0.01)
                    (list initially-live
                          (and (integerp pid) (> pid 0)
                               (= (process-id child) pid))
                          (null (get-process "event-order-primary"))
                          events
                          (process-exit-status child)
                          (process-live-p child)
                          (equal (process-command child) command)))"#,
            ),
            Value::list([
                Value::T,
                Value::T,
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
                           :connection-type 'pipe
                           :stderr stderr
                           :filter
                           (lambda (_process text)
                             (setq stdout (concat stdout text))))))
                    (process-send-string child "value")
                    (process-send-eof child)
                    (while (process-live-p child)
                      (accept-process-output child 0.1))
                    ;; `process-live-p' can observe exit while moving final
                    ;; bytes into the pending queues; pump once more to run
                    ;; both filters.
                    (accept-process-output child 0.1)
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
            "(let* ((private (obarray-make))\
                    (private-symbol (intern \"private-symbol\" private))\
                    (sample-symbol (intern \"sample-symbol\")))\
               (list (intern-soft nil)\
                     (intern-soft t)\
                     (intern-soft sample-symbol)\
                     (intern-soft (make-symbol \"sample-symbol\"))\
                     (intern-soft nil private)\
                     (intern-soft t private)\
                     (eq private-symbol (intern-soft private-symbol private))\
                     (null (intern-soft sample-symbol private))))"
        ),
        Value::list([
            Value::Nil,
            Value::T,
            Value::Symbol("sample-symbol".into()),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
        ])
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
fn runtime_readers_honor_dynamic_symbol_shorthands_and_escapes() {
    assert_eq!(
        eval_str(
            r##"(let ((read-symbol-shorthands '(("s-" . "long-")
                                                ("-" . "fooey-"))))
                 (list
                  (car (read-from-string "s-name"))
                  (with-temp-buffer
                    (insert "s-buffer")
                    (goto-char (point-min))
                    (read (current-buffer)))
                  (car (read-from-string "(/= (-name))"))
                  (car (read-from-string "#_s-raw"))))"##,
        ),
        Value::list([
            Value::Symbol("long-name".into()),
            Value::Symbol("long-buffer".into()),
            Value::list([
                Value::Symbol("/=".into()),
                Value::list([Value::Symbol("fooey-name".into())]),
            ]),
            Value::Symbol("s-raw".into()),
        ])
    );
}

#[test]
fn standard_obarray_unintern_detaches_membership_until_reinterned() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun emaxx-standard-unintern-target () 42)
                 (let ((symbol 'emaxx-standard-unintern-target))
                   (list (unintern symbol)
                         (intern-soft symbol)
                         (fboundp symbol)
                         (unintern "emaxx-standard-unintern-missing")
                         (progn
                           (intern "emaxx-standard-unintern-target")
                           (intern-soft "emaxx-standard-unintern-target")))))"#,
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::Symbol("emaxx-standard-unintern-target".into()),
        ])
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
fn handler_bind_1_accepts_gnu_condition_lists_and_multiple_pairs() {
    assert_eq!(
        eval_str(
            "(let (seen)
               (list
                (condition-case err
                    (handler-bind-1
                     (lambda () (signal 'wrong-type-argument '(x)))
                     '(wrong-type-argument error)
                     (lambda (err) (push (car err) seen))
                     '(wrong-type-argument)
                     (lambda (_err) (push 'second seen)))
                  (wrong-type-argument
                   (list (nreverse seen) (car err))))
                (handler-bind-1
                 (lambda () 'ok)
                 nil
                 (lambda (_err) 'bad))
                (condition-case err
                    (handler-bind-1 (lambda () t) '(error))
                  (error (car err)))))",
        ),
        Value::list([
            Value::list([
                Value::list([
                    Value::symbol("wrong-type-argument"),
                    Value::symbol("second"),
                ]),
                Value::symbol("wrong-type-argument"),
            ]),
            Value::symbol("ok"),
            Value::symbol("error"),
        ])
    );
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
fn integer_arithmetic_preserves_exact_results_across_checked_overflow() {
    assert_eq!(
        eval_str(
            "(list
               (= (+ 9223372036854775807 1) 9223372036854775808)
               (= (+ 9223372036854775807 1 -1) 9223372036854775807)
               (= (- -9223372036854775808 1) -9223372036854775809)
               (= (* 3037000500 3037000500) 9223372037000250000)
               (= (* 3037000500 3037000500 0) 0)
               (= (1+ 9223372036854775807) 9223372036854775808)
               (= (1- -9223372036854775808) -9223372036854775809)
               (< 9223372036854775807 9223372036854775808)
               (> -9223372036854775808 -9223372036854775809))"
        ),
        Value::list(std::iter::repeat_n(Value::T, 9))
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
fn aset_is_a_normal_primitive_with_left_to_right_argument_evaluation() {
    assert_eq!(
        eval_str(
            "(let ((buf (vector 0)) events)
               (aset buf
                     (progn (push 'index events) 0)
                     (progn (push 'value events) 7))
               (list buf events
                     (special-form-p 'aset)
                     (subrp (symbol-function 'aset))))"
        ),
        Value::list([
            Value::list([Value::Symbol("vector-literal".into()), Value::Integer(7),]),
            Value::list([Value::Symbol("value".into()), Value::Symbol("index".into()),]),
            Value::Nil,
            Value::T,
        ])
    );
}

#[test]
fn aset_mutates_shared_string_identity_without_rebinding_its_variable() {
    assert_eq!(
        eval_str(
            "(let* ((string \"abc\")
                    (alias string))
               (aset string 0 ?x)
               (list string alias (eq string alias)))"
        ),
        Value::list([
            Value::String("xbc".into()),
            Value::String("xbc".into()),
            Value::T,
        ])
    );
}

#[test]
fn eval_throw_and_call_interactively_keep_their_gnu_function_boundary() {
    assert_eq!(
        eval_str(
            "(mapcar
               (lambda (name)
                 (list name
                       (special-form-p name)
                       (subrp (symbol-function name))))
               '(eval throw call-interactively))"
        ),
        Value::list([
            Value::list([Value::Symbol("eval".into()), Value::Nil, Value::T,]),
            Value::list([Value::Symbol("throw".into()), Value::Nil, Value::T,]),
            Value::list([
                Value::Symbol("call-interactively".into()),
                Value::Nil,
                Value::T,
            ]),
        ])
    );
}

#[test]
fn defalias_provide_and_require_keep_their_gnu_function_boundary() {
    assert_eq!(
        eval_str(
            "(mapcar
               (lambda (name)
                 (list name
                       (special-form-p name)
                       (subrp (symbol-function name))))
               '(defalias provide require))"
        ),
        Value::list([
            Value::list([Value::Symbol("defalias".into()), Value::Nil, Value::T,]),
            Value::list([Value::Symbol("provide".into()), Value::Nil, Value::T,]),
            Value::list([Value::Symbol("require".into()), Value::Nil, Value::T,]),
        ])
    );
}

#[test]
fn defalias_provide_and_require_evaluate_arguments_left_to_right() {
    assert_eq!(
        eval_str(
            "(let (events)
               (defalias
                 (progn (push 'alias-name events) 'sample-ordered-alias)
                 (progn (push 'definition events) #'ignore)
                 (progn (push 'docstring events) \"ordered alias\"))
               (provide
                 (progn (push 'feature events) 'sample-ordered-feature)
                 (progn (push 'subfeatures events) '(:first :second)))
               (require
                 (progn (push 'required-feature events)
                        'sample-ordered-feature)
                 (progn (push 'filename events) nil)
                 (progn (push 'noerror events) t))
               (list events
                     (featurep 'sample-ordered-feature :second)
                     (get 'sample-ordered-alias
                          'function-documentation)))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("noerror".into()),
                Value::Symbol("filename".into()),
                Value::Symbol("required-feature".into()),
                Value::Symbol("subfeatures".into()),
                Value::Symbol("feature".into()),
                Value::Symbol("docstring".into()),
                Value::Symbol("definition".into()),
                Value::Symbol("alias-name".into()),
            ]),
            Value::T,
            Value::String("ordered alias".into()),
        ])
    );
}

#[test]
fn migrated_gnu_c_primitives_keep_native_metadata_and_headless_results() {
    assert_eq!(
        eval_str(
            "(list
               (mapcar
                 (lambda (name)
                   (list name
                         (special-form-p name)
                         (subrp (symbol-function name))))
                 '(documentation-stringp current-idle-time posn-at-point))
               (mapcar #'documentation-stringp
                       (list \"doc\" 1 (ash 1 100) '(\"file.elc\" . 2)
                             (cons \"file.elc\" (ash 1 100)) nil
                             '(\"file.elc\" 2)))
               (current-idle-time)
               (posn-at-point))"
        ),
        Value::list([
            Value::list([
                Value::list([
                    Value::Symbol("documentation-stringp".into()),
                    Value::Nil,
                    Value::T,
                ]),
                Value::list([
                    Value::Symbol("current-idle-time".into()),
                    Value::Nil,
                    Value::T,
                ]),
                Value::list([Value::Symbol("posn-at-point".into()), Value::Nil, Value::T,]),
            ]),
            Value::list([
                Value::T,
                Value::T,
                Value::Nil,
                Value::T,
                Value::Nil,
                Value::Nil,
                Value::Nil,
            ]),
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn interactive_is_the_gnu_unevaluated_declaration_primitive() {
    assert_eq!(
        eval_str(
            "(list
               (special-form-p 'interactive)
               (subrp (symbol-function 'interactive))
               (interactive (error \"must remain unevaluated\")))"
        ),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn native_table_function_and_runtime_introspection_matches_gnu() {
    assert_eq!(
        eval_str(
            "(let ((case (make-char-table 'case-table))
                   (syntax (make-char-table 'syntax-table))
                   (other (make-char-table 'other)))
               (list
                 (case-table-p case)
                 (syntax-table-p syntax)
                 (case-table-p syntax)
                 (syntax-table-p other)
                 (progn
                   (set-char-table-extra-slot case 0 t)
                   (case-table-p case))
                 (subr-type (symbol-function 'car))
                 (condition-case err
                     (subr-type (lambda () t))
                   (wrong-type-argument (car err)))
                 (let ((make (lambda (x) (lambda () x))))
                   (function-equal (funcall make 1) (funcall make 2)))
                 (function-equal (lambda () t) (lambda () t))
                 (let ((runtime (get-internal-run-time)))
                   (list (length runtime) (mapcar #'natnump runtime)))
                 (flush-standard-output)))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Symbol("wrong-type-argument".into()),
            Value::T,
            Value::Nil,
            Value::list([
                Value::Integer(4),
                Value::list([Value::T, Value::T, Value::T, Value::T]),
            ]),
            Value::Nil,
        ])
    );
}

#[test]
fn lambda_code_identity_rejects_recycled_source_cons_addresses() {
    assert_eq!(
        eval_str(
            "(let ((first (eval (list 'lambda nil 0) t))
                   second)
               (dotimes (index 128)
                 (setq second
                       (eval (list 'lambda nil (1+ index)) t)))
               (list
                 (funcall first)
                 (funcall second)
                 (function-equal first second)))"
        ),
        Value::list([Value::Integer(0), Value::Integer(128), Value::Nil])
    );
}

#[test]
fn native_property_change_primitives_use_interval_boundaries() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert \"abcd\")
               (add-text-properties 2 4 '(face bold))
               (let ((overlay (make-overlay 3 4)))
                 (overlay-put overlay 'help-echo 'from-overlay)
                 (list
                   (next-property-change 1)
                   (next-property-change 2)
                   (previous-property-change 5)
                   (next-property-change 1 nil 3)
                   (next-property-change 1 nil t)
                   (let ((found
                          (get-char-property-and-overlay 3 'help-echo)))
                     (list (car found) (eq (cdr found) overlay)))
                   (get-char-property-and-overlay 2 'face)
                   (next-property-change
                     0 (propertize \"abc\" 'face 'bold) t))))"
        ),
        Value::list([
            Value::Integer(2),
            Value::Integer(4),
            Value::Integer(4),
            Value::Integer(2),
            Value::Integer(2),
            Value::list([Value::Symbol("from-overlay".into()), Value::T]),
            Value::list([Value::Symbol("bold".into())]),
            Value::Integer(3),
        ])
    );
}

#[test]
fn native_char_property_changes_merge_overlay_and_text_boundaries() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (insert \"abcdef\")
               (add-text-properties 3 5 '(face bold))
               (let ((overlay (make-overlay 2 4)))
                 (list
                   (mapcar #'next-char-property-change
                           (number-sequence 1 7))
                   (mapcar #'previous-char-property-change
                           (number-sequence 1 7))
                   (next-char-property-change 1 99)
                   (next-char-property-change 1 2)
                   (previous-char-property-change 7 -9)
                   (previous-char-property-change 7 5)
                   (list
                     (subrp (symbol-function
                              'next-char-property-change))
                     (func-arity 'next-char-property-change)))))"
        ),
        Value::list([
            Value::list([2, 3, 4, 5, 7, 7, 7].map(Value::Integer)),
            Value::list([1, 1, 2, 3, 4, 5, 5].map(Value::Integer)),
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(5),
            Value::Integer(5),
            Value::list([Value::T, Value::cons(Value::Integer(1), Value::Integer(2)),]),
        ])
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
fn setf_aref_mutates_record_slots_without_losing_identity() {
    assert_eq!(
        eval_str(
            "(let ((object (record 'sample 1 '(2))))
               (cl-pushnew 3 (aref object 2))
               (list (recordp object) (aref object 0) (aref object 2)))"
        ),
        Value::list([
            Value::T,
            Value::Symbol("sample".into()),
            Value::list([Value::Integer(3), Value::Integer(2)]),
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
fn preferred_native_fallback_delegates_to_loaded_gnu_url_owner() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(progn
                 (require 'url-expand)
                 (list
                  (url-scheme-get-property "file" 'expand-file-name)
                  (featurep 'url-file)
                  (url-expand-file-name "bar.html"
                                        "file:///a/b/c/foo.html")))"#,
        ),
        Value::list([
            Value::Symbol("url-file-expand-file-name".into()),
            Value::T,
            Value::String("file:///a/b/c/bar.html".into()),
        ])
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
fn global_write_file_hooks_remain_visible_in_new_buffers() {
    assert_eq!(
        eval_str(
            "(progn
               (setq-default write-file-functions nil)
               (let ((calls 0))
                 (add-hook 'write-file-functions
                           (lambda () (setq calls (1+ calls)) nil))
                 (with-temp-buffer
                   (list (local-variable-p 'write-file-functions)
                         (length write-file-functions)
                         (run-hook-with-args-until-success
                          'write-file-functions)
                         calls
                         (length (default-value 'write-file-functions))
                         (local-variable-p 'write-contents-functions)
                         (progn
                           (setq write-contents-functions nil)
                           (local-variable-p
                            'write-contents-functions))))))"
        ),
        Value::list([
            Value::Nil,
            Value::Integer(1),
            Value::Nil,
            Value::Integer(1),
            Value::Integer(1),
            Value::Nil,
            Value::T,
        ])
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
fn add_hook_respects_a_dynamic_binding_of_a_buffer_local_hook() {
    assert_eq!(
        eval_str(
            "(progn
               (defvar sample-dynamic-local-hook nil)
               (setq sample-dynamic-local-hook nil)
               (with-temp-buffer
                 (add-hook 'sample-dynamic-local-hook #'sample-local nil t)
                 (let ((during
                        (let ((sample-dynamic-local-hook nil))
                          (add-hook 'sample-dynamic-local-hook #'sample-temporary)
                          (list sample-dynamic-local-hook
                                (default-value 'sample-dynamic-local-hook)))))
                   (list during sample-dynamic-local-hook
                         (default-value 'sample-dynamic-local-hook)))))"
        ),
        Value::list([
            Value::list([
                Value::list([Value::Symbol("sample-temporary".into())]),
                Value::Nil,
            ]),
            Value::list([Value::Symbol("sample-local".into()), Value::T]),
            Value::Nil,
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
fn byte_to_string_preserves_octets_as_unibyte_strings() {
    assert_eq!(
        eval_str(
            r#"(list
                 (mapcar (lambda (byte)
                           (let ((string (byte-to-string byte)))
                             (list (multibyte-string-p string)
                                   (length string)
                                   (aref string 0))))
                         '(0 127 128 195 255))
                 (decode-coding-string
                  (concat (byte-to-string 195) (byte-to-string 167))
                  'utf-8))"#,
        ),
        Value::list([
            Value::list([
                Value::list([Value::Nil, Value::Integer(1), Value::Integer(0)]),
                Value::list([Value::Nil, Value::Integer(1), Value::Integer(127)]),
                Value::list([Value::Nil, Value::Integer(1), Value::Integer(128)]),
                Value::list([Value::Nil, Value::Integer(1), Value::Integer(195)]),
                Value::list([Value::Nil, Value::Integer(1), Value::Integer(255)]),
            ]),
            Value::String("ç".into()),
        ])
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
    assert_eq!(
        eval_str(
            r#"(progn
                 (string-match "\\(before\\)" "before")
                 (let ((saved (match-data)))
                   (list
                    (replace-regexp-in-string
                     "x\\([0-9]+\\)"
                     (lambda (matched) (match-string 1 matched))
                     "pre x42 post")
                    (equal (match-data) saved))))"#,
        ),
        Value::list([Value::String("pre 42 post".into()), Value::T])
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
fn reader_printer_and_string_to_number_share_gnu_special_float_syntax() {
    let positive_infinity = eval_str(r#"(string-to-number "1.0e+INFjunk")"#);
    assert!(matches!(positive_infinity, Value::Float(value) if value == f64::INFINITY));
    let negative_infinity = eval_str(r#"(string-to-number "-2.e+INF")"#);
    assert!(matches!(negative_infinity, Value::Float(value) if value == f64::NEG_INFINITY));

    let positive_nan = eval_str(r#"(string-to-number "2.e+NaNjunk")"#);
    assert!(
        matches!(positive_nan, Value::Float(value) if value.is_nan() && value.is_sign_positive())
    );
    let negative_nan = eval_str(r#"(string-to-number "-2.e+NaN")"#);
    assert!(
        matches!(negative_nan, Value::Float(value) if value.is_nan() && value.is_sign_negative())
    );

    assert_eq!(
        eval_str(r#"(string-to-number "1e-INF")"#),
        Value::Integer(1)
    );
    assert_eq!(eval_str("'1e-INF"), Value::Symbol("1e-INF".into()));
    assert!(matches!(eval_str("1e+INF"), Value::Float(value) if value == f64::INFINITY));
    assert_string_value(eval_str(r#"(prin1-to-string (intern "1e-INF"))"#), "1e-INF");
    assert_string_value(
        eval_str(r#"(prin1-to-string (intern "1e+INF"))"#),
        r"\1e+INF",
    );
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
fn posix_string_match_chooses_longest_match_and_honors_inhibit_modify() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (posix-string-match "\\(a\\|aa\\)\\(a*\\)" "xaaa" 1)
                 (let ((longest (list (match-beginning 0) (match-end 0)
                                      (match-beginning 1) (match-end 1)
                                      (match-beginning 2) (match-end 2))))
                   (string-match "z" "z")
                   (posix-string-match "\\(a\\|aa\\)" "aa" nil t)
                   (list longest (match-beginning 0) (match-end 0))))"#,
        ),
        Value::list([
            Value::list([
                Value::Integer(1),
                Value::Integer(4),
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(2),
                Value::Integer(4),
            ]),
            Value::Integer(0),
            Value::Integer(1),
        ])
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
fn string_match_handles_leading_closing_bracket_ranges() {
    assert_eq!(
        eval_str(
            r#"(list (string-match "[]-a]" "^")
                     (string-match "[]-a]" "b")
                     (string-match "[^]-~]" "^")
                     (string-match "[^]-~]" "!"))"#
        ),
        Value::list([Value::Integer(0), Value::Nil, Value::Nil, Value::Integer(0),])
    );
}

#[test]
fn string_match_treats_incomplete_posix_class_openers_as_literal_members() {
    assert_eq!(
        eval_str(
            r#"(let ((regexp "[ \t\n[:?;{=*/%&|,<>!@+-]"))
                 (mapcar (lambda (string) (string-match-p regexp string))
                         '("[" ":" "x" "-")))"#
        ),
        Value::list([
            Value::Integer(0),
            Value::Integer(0),
            Value::Nil,
            Value::Integer(0),
        ])
    );
}

#[test]
fn string_match_rejects_complete_malformed_posix_classes() {
    assert_eq!(
        eval_str(
            r#"
                (list
                 (string-match "[[:alpha:]]" "a")
                 (string-match "[x[:y]]" "x]")
                 (condition-case err
                     (string-match "[[:unknown:]]" "x")
                   (invalid-regexp (list (car err) (cadr err))))
                 (condition-case err
                     (string-match "a[[:]:]]b" "ab")
                   (invalid-regexp (list (car err) (cadr err)))))
                "#,
        ),
        Value::list([
            Value::Integer(0),
            Value::Integer(0),
            Value::list([
                Value::Symbol("invalid-regexp".into()),
                Value::String("Invalid character class name".into()),
            ]),
            Value::list([
                Value::Symbol("invalid-regexp".into()),
                Value::String("Invalid character class name".into()),
            ]),
        ])
    );
}

#[test]
fn string_match_reports_ascii_and_multibyte_capture_positions_in_characters() {
    assert_eq!(
        eval_str(
            r#"
            (list
             (progn
               (string-match "\\(b+\\)" "aaabbb")
               (match-data))
             (progn
               (string-match "\\(β+\\)" "ééββ")
               (match-data)))
            "#,
        ),
        Value::list([
            Value::list([
                Value::Integer(3),
                Value::Integer(6),
                Value::Integer(3),
                Value::Integer(6),
            ]),
            Value::list([
                Value::Integer(2),
                Value::Integer(4),
                Value::Integer(2),
                Value::Integer(4),
            ]),
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
fn search_spaces_regexp_binding_crosses_function_calls() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun emaxx-test-search-spaces ()
                   (with-temp-buffer
                     (insert "a \n\t b")
                     (goto-char (point-min))
                     (re-search-forward "a b" nil t)))
                 (let ((search-spaces-regexp "\\s-+"))
                   (list (special-variable-p 'search-spaces-regexp)
                         (emaxx-test-search-spaces))))"#
        ),
        Value::list([Value::T, Value::Integer(7)])
    );
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
fn macroexp_file_name_survives_nested_ert_macro_expansion() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    let test_file = "/tmp/emaxx-nested-resource-tests.el";
    interp.set_current_load_file(Some(test_file.into()));
    interp.set_variable(
        "current-load-list",
        Value::list([Value::String(test_file.into())]),
        &mut Vec::new(),
    );
    eval_str_with(
        &mut interp,
        &format!(
            r#"
                (defmacro emaxx-test-call-site-file ()
                  `(quote ,(macroexp-file-name)))
                (ert-deftest macroexp-file-name-keeps-defining-file ()
                  (should (string= (emaxx-test-call-site-file) "{test_file}")))
                "#
        ),
    );
    interp.set_variable("current-load-list", Value::Nil, &mut Vec::new());
    interp.set_current_load_file(None);
    let (passed, failed, total) = interp.run_ert_tests();
    assert_eq!((passed, failed, total), (1, 0, 1));
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
fn missing_function_and_variable_cells_signal_distinct_conditions() {
    assert_eq!(
        eval_str(
            r#"(list
                 (condition-case err
                     emaxx-test-unbound
                   (void-variable (list (car err) (cadr err))))
                 (condition-case err
                     (emaxx-test-unbound)
                   (void-function (list (car err) (cadr err)))))"#
        ),
        Value::list([
            Value::list([
                Value::symbol("void-variable"),
                Value::symbol("emaxx-test-unbound"),
            ]),
            Value::list([
                Value::symbol("void-function"),
                Value::symbol("emaxx-test-unbound"),
            ]),
        ])
    );
}

#[test]
fn preloaded_list_processes_keeps_lisp_policy_over_native_process_records() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"(let ((process
                       (make-pipe-process
                        :name "emaxx-process-list"
                        :buffer (current-buffer)
                        :noquery nil)))
                 (unwind-protect
                     (list
                      (list-processes t)
                      (with-current-buffer "*Process List*"
                        (string-match-p "emaxx-process-list"
                                        (buffer-string))))
                   (delete-process process)))"#
        ),
        Value::list([Value::Nil, Value::Integer(0)])
    );
}

#[test]
fn frame_c_default_frame_alist_is_bound_nil_and_special() {
    assert_eq!(
        eval_str(
            "(list (boundp 'default-frame-alist)
                   default-frame-alist
                   (special-variable-p 'default-frame-alist))"
        ),
        Value::list([Value::T, Value::Nil, Value::T])
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
fn file_name_handlers_honor_precedence_operations_and_inhibition() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (put 'emaxx-handler-late 'operations '(copy-file))
                 (let ((file-name-handler-alist
                        '(("^/ssh:" . emaxx-handler-broad)
                          ("host:" . emaxx-handler-late))))
                   (list
                    (find-file-name-handler
                     "/ssh:user@host:/tmp" 'copy-file)
                    (find-file-name-handler
                     "/ssh:user@host:/tmp" 'start-file-process)
                    (let ((inhibit-file-name-operation 'copy-file)
                          (inhibit-file-name-handlers
                           '(emaxx-handler-late)))
                      (find-file-name-handler
                       "/ssh:user@host:/tmp" 'copy-file))
                    (let ((inhibit-file-name-operation
                           'start-file-process)
                          (inhibit-file-name-handlers
                           '(emaxx-handler-late)))
                      (find-file-name-handler
                       "/ssh:user@host:/tmp" 'copy-file)))))"#,
        ),
        Value::list([
            Value::symbol("emaxx-handler-late"),
            Value::symbol("emaxx-handler-broad"),
            Value::symbol("emaxx-handler-broad"),
            Value::symbol("emaxx-handler-late"),
        ])
    );
}

#[test]
fn file_name_handler_match_cache_reuses_stable_scans() {
    crate::lisp::primitives::reset_file_name_handler_scan_count();
    assert_eq!(
        eval_str(
            r#"(let ((file-name-handler-alist
                      '(("cache-target" . emaxx-cache-handler))))
                 (list (find-file-name-handler
                        "/tmp/cache-target" 'file-exists-p)
                       (find-file-name-handler
                        "/tmp/cache-target" 'file-exists-p)))"#,
        ),
        Value::list([
            Value::symbol("emaxx-cache-handler"),
            Value::symbol("emaxx-cache-handler"),
        ])
    );
    assert_eq!(crate::lisp::primitives::file_name_handler_scan_count(), 1);
}

#[test]
fn file_name_handler_match_cache_tracks_every_mutable_authority() {
    assert_eq!(
        eval_str(
            r#"(let* ((entry (cons "cache-one" 'emaxx-cache-first))
                      (file-name-handler-alist (list entry)))
                 (list
                  (find-file-name-handler "/tmp/cache-one" 'file-exists-p)
                  (progn
                    (setcar entry "cache-two")
                    (find-file-name-handler "/tmp/cache-one" 'file-exists-p))
                  (find-file-name-handler "/tmp/cache-two" 'file-exists-p)
                  (let ((file-name-handler-alist
                         '(("cache-two" . emaxx-cache-second))))
                    (find-file-name-handler "/tmp/cache-two" 'file-exists-p))
                  (find-file-name-handler "/tmp/cache-two" 'file-exists-p)
                  (progn
                    (put 'emaxx-cache-first 'operations '(copy-file))
                    (find-file-name-handler "/tmp/cache-two" 'file-exists-p))
                  (find-file-name-handler "/tmp/cache-two" 'copy-file)))"#,
        ),
        Value::list([
            Value::symbol("emaxx-cache-first"),
            Value::Nil,
            Value::symbol("emaxx-cache-first"),
            Value::symbol("emaxx-cache-second"),
            Value::symbol("emaxx-cache-first"),
            Value::Nil,
            Value::symbol("emaxx-cache-first"),
        ])
    );

    // Mutable Lisp regexps are validated against their cached text snapshot:
    // `aset' can change one without replacing its enclosing cons cells.
    assert_eq!(
        eval_str(
            r#"(let* ((pattern (copy-sequence "mutable-a"))
                      (file-name-handler-alist
                       (list (cons pattern 'emaxx-mutable-handler))))
                 (list
                  (find-file-name-handler "/tmp/mutable-a" 'file-exists-p)
                  (progn
                    (aset pattern 8 ?b)
                    (list
                     (find-file-name-handler "/tmp/mutable-a" 'file-exists-p)
                     (find-file-name-handler
                      "/tmp/mutable-b" 'file-exists-p)))))"#,
        ),
        Value::list([
            Value::symbol("emaxx-mutable-handler"),
            Value::list([Value::Nil, Value::symbol("emaxx-mutable-handler")]),
        ])
    );
}

#[test]
fn autoloaded_file_name_handlers_keep_their_symbol_identity() {
    let unique = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(format!("emaxx-file-handler-autoload-{unique}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("emaxx-sample-handler.el"),
        "(defun emaxx-sample-handler (operation &rest _args)\n  (eq operation 'file-exists-p))\n(provide 'emaxx-sample-handler-feature)\n",
    )
    .unwrap();

    let mut interp = Interpreter::new();
    interp.set_load_path(vec![dir.clone()]);
    let result = eval_str_with(
        &mut interp,
        r#"(progn
             (autoload 'emaxx-sample-handler "emaxx-sample-handler")
             (let ((file-name-handler-alist
                    '(("\\`/emaxx-autoload-handler" . emaxx-sample-handler))))
               (list (featurep 'emaxx-sample-handler-feature)
                     (file-exists-p "/emaxx-autoload-handler")
                     (featurep 'emaxx-sample-handler-feature))))"#,
    );
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(result, Value::list([Value::Nil, Value::T, Value::T]));
}

#[test]
fn file_name_handler_insertion_rejoins_the_common_coding_tail() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun emaxx-insert-handler (operation filename &rest _args)
                   (when (eq operation 'insert-file-contents)
                     (insert "handled\n")
                     (setq last-coding-system-used 'iso-2022-7bit-unix)
                     (list filename 8)))
                 (defun format-decode (_format inserted _visit) inserted)
                 (defun emaxx-after-insert (inserted)
                   (setq emaxx-after-insert-count inserted)
                   inserted)
                 (let ((file-name-handler-alist
                        '(("\\`/emaxx-insert-handler" . emaxx-insert-handler)))
                       (after-insert-file-functions '(emaxx-after-insert))
                       emaxx-after-insert-count)
                   (with-temp-buffer
                     (let ((result
                            (insert-file-contents "/emaxx-insert-handler")))
                       (list result
                             (buffer-string)
                             buffer-file-coding-system
                             emaxx-after-insert-count)))))"#,
        ),
        Value::list([
            Value::list([
                Value::String("/emaxx-insert-handler".into()),
                Value::Integer(8),
            ]),
            Value::String("handled\n".into()),
            Value::Symbol("iso-2022-7bit-unix".into()),
            Value::Integer(8),
        ])
    );
}

#[test]
fn file_name_handlers_drive_real_io_and_quoted_names_reach_only_the_host_path() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let* ((path (make-temp-file "emaxx-handler-io"))
                      (quoted (file-name-quote path))
                      (link (concat path ".link"))
                      (magic (concat path ".magic"))
                      (magic-handler
                       (lambda (operation &rest arguments)
                         (let ((file-name-handler-alist nil))
                           (setcar arguments
                                   (substring (car arguments) 0 -6))
                           (apply operation arguments)))))
                 (unwind-protect
                     (let ((file-name-handler-alist
                            (cons (cons "\\.magic\\'" magic-handler)
                                  file-name-handler-alist)))
                       (list (file-name-quoted-p quoted)
                             (file-exists-p quoted)
                             (file-exists-p magic)
                             (progn (add-name-to-file quoted link)
                                    (prog1 (and (file-exists-p link)
                                                (null (access-file link "test")))
                                      (delete-file link)))
                             (equal (file-name-directory quoted)
                                    (file-name-quote
                                     (file-name-directory path)))
                             (with-temp-buffer
                               (insert-file-contents quoted :visit)
                               (let* ((actual nil)
                                      (base-handlers file-name-handler-alist)
                                      (log (lambda (&rest arguments)
                                             (setq actual arguments)
                                             (let ((file-name-handler-alist base-handlers))
                                               (apply #'file-name-non-special arguments))))
                                      (file-name-handler-alist
                                       (cons (cons "\\`/:" log)
                                             file-name-handler-alist))
                                      (visiting (current-buffer)))
                                 (and (equal buffer-file-name quoted)
                                      (file-name-quoted-p buffer-file-name)
                                      (with-temp-buffer
                                        (verify-visited-file-modtime visiting))
                                      (verify-visited-file-modtime)
                                      actual)))
                             (find-file-name-handler
                              quoted 'file-exists-p)))
                   (delete-file path)))"#,
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::list([
                Value::Symbol("verify-visited-file-modtime".into()),
                Value::Nil,
            ]),
            Value::Symbol("file-name-non-special".into()),
        ])
    );
}

#[test]
fn preloaded_file_name_transforms_and_temp_directory_are_dynamic() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(progn
                 (defun sample-make-temp-file () (make-temp-file "child"))
                 (with-temp-buffer
                   (setq buffer-file-name "/tmp/foo.txt")
                   (let* ((directory (make-temp-file "emaxx-temp-scope" t))
                          (temporary-file-directory
                           (file-name-as-directory directory))
                          (child (sample-make-temp-file)))
                     (unwind-protect
                         (list (make-auto-save-file-name)
                               (let ((auto-save-file-name-transforms
                                      '(("\\`/.*/\\([^/]+\\)\\'"
                                         "/var/tmp/\\1" t))))
                                 (make-auto-save-file-name))
                               (let ((lock-file-name-transforms nil))
                                 (make-lock-file-name buffer-file-name))
                               (string-prefix-p temporary-file-directory child))
                       (delete-file child)
                       (delete-directory directory)))))"#,
        ),
        Value::list([
            Value::String("/tmp/#foo.txt#".into()),
            Value::String("/var/tmp/#!tmp!foo.txt#".into()),
            Value::String("/tmp/.#foo.txt".into()),
            Value::T,
        ])
    );
}

#[test]
fn quoted_visited_buffers_use_local_host_paths_without_losing_lisp_spelling() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let* ((path (make-temp-file "emaxx-quoted-visit"))
                      (quoted (file-name-quote path))
                      (buffer (find-file-noselect quoted)))
                 (unwind-protect
                     (with-current-buffer buffer
                       (list (file-name-quoted-p buffer-file-name)
                             (stringp (make-auto-save-file-name))
                             (null (set-visited-file-modtime))))
                   (kill-buffer buffer)
                   (delete-file path)))"#,
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn quoted_visited_buffers_compose_with_earlier_file_name_handlers() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (progn
                  (require 'ert-x)
                  (defconst emaxx-test-special-extension ".special")
                  (defconst emaxx-test-special-regexp "\\.special\\'")
                  (defun emaxx-test-special-handler (operation &rest args)
                    (let ((args (copy-sequence args))
                          (file-name-handler-alist
                           (delete
                            (rassoc 'emaxx-test-special-handler
                                    file-name-handler-alist)
                            file-name-handler-alist)))
                      (let ((tail args))
                        (while tail
                          (when (and (stringp (car tail))
                                     (not (file-name-quoted-p (car tail)))
                                     (string-match emaxx-test-special-regexp
                                                   (car tail)))
                            (setcar tail (replace-match "" nil nil (car tail))))
                          (setq tail (cdr tail))))
                      (apply operation args)))
                  (let* ((directory (make-temp-file "emaxx-special-quoted" t))
                         (temporary-file-directory
                          (file-name-as-directory directory))
                         (file-name-handler-alist
                          `((,emaxx-test-special-regexp
                             . emaxx-test-special-handler)
                            . ,file-name-handler-alist))
                         (actual (make-temp-file "emaxx-special-file"))
                         (name (concat actual emaxx-test-special-extension))
                         (quoted (file-name-quote name))
                         quoted-buffer normal-buffer)
                    (unwind-protect
                        (progn
                          (setq quoted-buffer (find-file-noselect quoted))
                          (let ((quoted-auto-save
                                 (with-current-buffer quoted-buffer
                                   (make-auto-save-file-name)))
                                (quoted-name
                                 (buffer-file-name quoted-buffer)))
                            (kill-buffer quoted-buffer)
                            (setq quoted-buffer nil)
                            (setq normal-buffer (find-file-noselect name))
                            (with-current-buffer normal-buffer
                              (list (equal quoted-name quoted)
                                    (stringp quoted-auto-save)
                                    (null (set-visited-file-modtime))
                                    (equal buffer-file-name actual)
                                    (not (equal quoted-auto-save
                                                (make-auto-save-file-name)))))))
                      (when (bufferp quoted-buffer) (kill-buffer quoted-buffer))
                      (when (bufferp normal-buffer) (kill-buffer normal-buffer))
                      (when (file-exists-p actual) (delete-file actual))
                      (delete-directory directory t))))
                "#
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn unix_file_metadata_buffer_lookup_and_exec_path_use_host_facts() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let ((path (make-temp-file "emaxx-host-file")))
                 (unwind-protect
                     (progn
                       (set-file-modes path #o755)
                       (with-temp-buffer
                         (insert-file-contents path :visit)
                         (list (= (file-attribute-user-id
                                   (file-attributes path 'integer))
                                  (user-uid))
                               (consp (visited-file-modtime))
                               (eq (find-buffer 'buffer-file-truename
                                                (file-truename path))
                                   (current-buffer))
                               (let ((exec-path
                                      (list (file-name-directory path))))
                                 (equal path
                                        (executable-find
                                         (file-name-nondirectory path)))))))
                   (delete-file path)))"#,
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn preloaded_file_buffer_policy_is_bound_and_truly_buffer_local() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun emaxx-test-dialog-policy () use-dialog-box)
                 (let ((first (generate-new-buffer " *emaxx-policy-1*"))
                     (second (generate-new-buffer " *emaxx-policy-2*")))
                 (unwind-protect
                     (progn
                       (with-current-buffer first
                         (setq buffer-offer-save t vc-mode 'Git))
                       (list (boundp 'uniquify-trailing-separator-p)
                             uniquify-trailing-separator-p
                             (with-current-buffer first buffer-offer-save)
                             (with-current-buffer second buffer-offer-save)
                             (with-current-buffer first vc-mode)
                             (with-current-buffer second vc-mode)
                             (let ((use-dialog-box nil))
                               (emaxx-test-dialog-policy))
                             (fboundp 'uniquify--create-file-buffer-advice)
                             (fboundp 'vc-before-save)
                             (null (string-match mounted-file-systems
                                                 "/private/var/tmp"))
                             (numberp (string-match mounted-file-systems
                                                    "/mnt/share"))))
                   (kill-buffer first)
                   (kill-buffer second))))"#,
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::Symbol("Git".into()),
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn normal_mode_uses_gnu_cookie_directory_interpreter_and_magic_precedence() {
    assert_eq!(
        eval_str(
            r##"(let ((directory (make-temp-file "emaxx-mode-pipeline" t)))
                 (unwind-protect
                     (let ((file (expand-file-name "sample.quux" directory))
                           (locals (expand-file-name ".dir-locals.el" directory)))
                       (with-temp-file locals
                         (insert "((auto-mode-alist . ((\"\\\\.quux\\\\'\" . tcl-mode))))"))
                       (with-temp-file file (insert "puts hello"))
                       (list
                        (with-temp-buffer
                          (insert "-*- mode: notexist; mode: text -*-")
                          (normal-mode)
                          major-mode)
                        (with-temp-buffer
                          (insert "Local variables:\nmode: text\nmode: outline\nend:\n")
                          (normal-mode)
                          major-mode)
                        (with-current-buffer (find-file-noselect file) major-mode)
                        (with-temp-buffer
                          (insert "#!/bin/bash")
                          (normal-mode)
                          (list (derived-mode-p 'sh-base-mode) sh-shell))
                        (let ((magic-mode-alist '(("my-tag" . text-mode))))
                          (with-temp-buffer
                            (insert "my-tag")
                            (normal-mode)
                            major-mode))
                        (with-temp-buffer
                          (insert "<!doctype html>")
                          (normal-mode)
                          major-mode)))
                   (delete-directory directory t)))"##,
        ),
        Value::list([
            Value::Symbol("text-mode".into()),
            Value::Symbol("outline-mode".into()),
            Value::Symbol("tcl-mode".into()),
            Value::list([Value::T, Value::Symbol("bash".into())]),
            Value::Symbol("text-mode".into()),
            Value::Symbol("mhtml-mode".into()),
        ])
    );
}

#[test]
fn mock_remote_operations_use_the_local_mock_handler() {
    assert_eq!(
        eval_str(
            "(let ((file-name-handler-alist nil))
               (mapcar (lambda (operation)
                         (find-file-name-handler
                          \"/mock::/tmp/\" operation))
                       '(exec-path expand-file-name file-group-gid
                         file-local-copy file-user-uid make-process
                         start-file-process)))"
        ),
        Value::list([
            Value::symbol("emaxx-mock-file-name-handler"),
            Value::symbol("emaxx-mock-file-name-handler"),
            Value::symbol("emaxx-mock-file-name-handler"),
            Value::symbol("emaxx-mock-file-name-handler"),
            Value::symbol("emaxx-mock-file-name-handler"),
            Value::symbol("emaxx-mock-file-name-handler"),
            Value::symbol("emaxx-mock-file-name-handler"),
        ])
    );
}

#[test]
fn expand_file_name_dispatches_a_relative_name_through_its_remote_base() {
    assert_eq!(
        eval_str(
            r#"(let ((default-directory "/mock::/tmp/work/"))
                 (list
                  (find-file-name-handler
                   default-directory 'expand-file-name)
                  (expand-file-name "sh")
                  (expand-file-name "../bin/sh" default-directory)
                  (expand-file-name
                   "child" "/mock:host:/:/tmp/quoted-base")
                  (expand-file-name "~")
                  (expand-file-name "/bin/sh")))"#
        ),
        Value::list([
            Value::symbol("emaxx-mock-file-name-handler"),
            Value::String("/mock::/tmp/work/sh".into()),
            Value::String("/mock::/tmp/bin/sh".into()),
            Value::String("/mock:host:/:/tmp/quoted-base/child".into()),
            Value::String(
                std::env::var("HOME")
                    .expect("test host has a home directory")
                    .into(),
            ),
            Value::String("/bin/sh".into()),
        ])
    );
}

#[test]
fn mock_remote_path_policies_share_the_typed_handler_registry() {
    assert_eq!(
        eval_str(
            r#"(let ((default-directory "/mock::/tmp/work/")
                     (exec-path '("/bin" "/usr/bin"))
                     (tramp-remote-path '("/" "/" "/definitely-missing")))
                 (list
                  (expand-file-name ".." "./")
                  (let ((tramp-tolerate-tilde t))
                    (expand-file-name "/mock::~"))
                  (funcall
                   (find-file-name-handler default-directory 'exec-path)
                   'exec-path)
                  (let ((handler
                         (find-file-name-handler
                          "/mock::/tmp/" 'file-system-info)))
                    (list handler
                          (length
                           (funcall handler 'file-system-info
                                    "/mock::/tmp/"))))
                  (let ((tramp-mode nil))
                    (find-file-name-handler
                     "/mock::/tmp/work/" 'expand-file-name))))"#
        ),
        Value::list([
            Value::String("/mock::/tmp".into()),
            Value::String("/mock::/:~".into()),
            Value::list([
                Value::String("/".into()),
                Value::String("/tmp/work/".into()),
            ]),
            Value::list([
                Value::symbol("emaxx-mock-file-name-handler"),
                Value::Integer(3),
            ]),
            Value::Nil,
        ])
    );
}

#[test]
fn mock_remote_abbreviation_applies_directory_rules_before_home() {
    assert_eq!(
        eval_str(
            r#"(let ((process-environment
                      '("HOME=/tmp/emaxx-remote-home"))
                     (directory-abbrev-alist
                      '(("\\`/mock::/tmp/emaxx-remote-home/foo"
                         . "/mock::/tmp/emaxx-remote-home/f"))))
                 (list
                  (abbreviate-file-name
                   "/mock::/tmp/emaxx-remote-home/other")
                  (abbreviate-file-name
                   "/mock::/tmp/emaxx-remote-home/foo/bar")))"#
        ),
        Value::list([
            Value::String("/mock::~/other".into()),
            Value::String("/mock::~/f/bar".into()),
        ])
    );
}

#[test]
fn remote_default_directory_does_not_reclassify_absolute_or_remote_probe_names() {
    assert_eq!(
        eval_str(
            r#"(let ((default-directory "/mock::/tmp/work/"))
                 (list (file-remote-p "sh")
                       (file-executable-p "/bin/sh")
                       (file-regular-p "/bin/sh")))"#
        ),
        Value::list([Value::Nil, Value::T, Value::T])
    );
}

#[test]
fn mock_predicates_accept_relative_names_selected_through_the_remote_base() {
    assert_eq!(
        eval_str(
            r#"(let ((default-directory "/mock::/tmp/work/"))
                 (list
                  (file-exists-p "emaxx-definitely-missing")
                  (file-regular-p "emaxx-definitely-missing")
                  (file-directory-p "emaxx-definitely-missing")
                  (file-accessible-directory-p "emaxx-definitely-missing")
                  (file-executable-p "emaxx-definitely-missing")
                  (file-readable-p "emaxx-definitely-missing")
                  (file-writable-p "emaxx-definitely-missing")))"#
        ),
        Value::list([
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
fn mock_truename_preserves_the_connection_and_canonicalizes_its_local_part() {
    assert_eq!(
        eval_str(
            r#"(let ((default-directory "/mock::/tmp/work/"))
                 (let ((relative (file-truename "missing"))
                       (remote (file-truename "/mock::/bin/sh")))
                   (list
                    (equal (file-remote-p relative 'method) "mock")
                    (equal (file-remote-p relative 'host) (system-name))
                    (equal (file-remote-p relative 'localname)
                           (file-truename "/tmp/work/missing"))
                    (equal (file-remote-p remote 'method) "mock")
                    (equal (file-remote-p remote 'host) (system-name))
                    (equal (file-remote-p remote 'localname)
                           (file-truename "/bin/sh"))
                    (equal (file-truename "/bin/sh") "/bin/sh"))))"#
        ),
        Value::list([
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
fn mock_make_process_uses_the_shared_native_process_path() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r#"(let* ((default-directory "/mock::")
                          (output "")
                          (process
                           (make-process
                            :name "emaxx-mock-make-process"
                            :command
                            (list "/bin/sh" "-c"
                                  "printf %s \"$INSIDE_EMACS\"")
                            :file-handler t
                            :filter
                            (lambda (_process text)
                              (setq output (concat output text))))))
                     (while (process-live-p process)
                       (accept-process-output process 0.05))
                     (list (processp process)
                           (not (null
                                 (string-match-p ",tramp\\'" output)))))"#
            ),
            Value::list([Value::T, Value::T])
        );
    });
}

#[test]
fn mock_shell_command_uses_the_same_typed_transport_registry() {
    assert_eq!(
        eval_str(
            r#"(let ((default-directory "/mock::/tmp/"))
                 (list
                  (eq (find-file-name-handler default-directory 'shell-command)
                      'emaxx-mock-file-name-handler)
                  (eq (find-file-name-handler default-directory 'process-file)
                      'emaxx-mock-file-name-handler)
                  (with-temp-buffer
                    (shell-command
                     "printf '%s' \"$INSIDE_EMACS\"" (current-buffer))
                    (buffer-string))
                  (with-temp-buffer
                    (list
                     (process-file "/bin/sh" nil (current-buffer) nil
                                   "-c" "printf '%s' \"$INSIDE_EMACS\"")
                     (buffer-string)))))"#
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::String("emaxx,tramp".into()),
            Value::list([Value::Integer(0), Value::String("emaxx,tramp".into())]),
        ])
    );
}

#[test]
fn buffer_lock_lifecycle_uses_the_public_file_handler_route() {
    assert_eq!(
        eval_str(
            r#"(let (operations)
                  (defun emaxx-test-lock-handler (operation &rest _args)
                    (push operation operations)
                    nil)
                  (let ((file-name-handler-alist
                         '(("\\`/sample:" . emaxx-test-lock-handler))))
                    (with-temp-buffer
                      (setq buffer-file-name "/sample:host:/tmp/file"
                            buffer-file-truename buffer-file-name)
                      (insert "x")
                      (set-buffer-modified-p nil)))
                  (nreverse operations))"#,
        ),
        Value::list([Value::symbol("lock-file"), Value::symbol("unlock-file"),])
    );
}

#[test]
fn mock_backup_policy_preserves_the_logical_connection_spelling() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun find-backup-file-name (file)
                   (let ((handler
                          (find-file-name-handler
                           file 'find-backup-file-name)))
                     (if handler
                         (funcall handler 'find-backup-file-name file)
                       (list (concat file "~")))))
                 (defun tramp-handle-find-backup-file-name (file)
                   (find-backup-file-name file))
                 (let ((tramp-mode t))
                   (list
                    (car (find-backup-file-name
                          "/mock::/tmp/emaxx-backup-source"))
                    (car (find-backup-file-name
                          "/mock:host:/:/tmp/emaxx-backup-source")))))"#,
        ),
        Value::list([
            Value::String("/mock::/tmp/emaxx-backup-source~".into()),
            Value::String("/mock:host:/:/tmp/emaxx-backup-source~".into()),
        ])
    );
}

#[test]
fn mock_remote_metadata_reads_delegate_connection_lifecycle_to_loaded_tramp() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defvar opened-mock-connections nil)
                 (defun tramp-dissect-file-name (file) file)
                 (defun tramp-maybe-open-connection (vector)
                   (push vector opened-mock-connections))
                 (let ((tramp-mode t))
                   (file-symlink-p
                    "/mock::/tmp/emaxx-missing-symlink-probe")
                   (list (length opened-mock-connections)
                         (car opened-mock-connections))))"#,
        ),
        Value::list([
            Value::Integer(1),
            Value::String("/mock::/tmp/emaxx-missing-symlink-probe".into()),
        ])
    );
}

#[test]
fn mock_full_directory_entries_retain_the_implicit_remote_directory() {
    assert_eq!(
        eval_str(
            r#"(let ((directory (make-temp-file "emaxx-mock-listing-" t))
                     (tramp-mode t))
                 (unwind-protect
                     (progn
                       (make-empty-file (expand-file-name "sample" directory))
                       (let ((default-directory
                              (concat "/mock::" directory "/")))
                         (let ((entry (car (directory-files
                                            "." t "\\`sample\\'"))))
                           (and (string-prefix-p "/mock::" entry)
                                (string-suffix-p "/sample" entry)))))
                   (delete-directory directory t)))"#,
        ),
        Value::T
    );
}

#[test]
fn mock_processes_overlay_the_remote_environment_with_lisp_precedence() {
    assert_eq!(
        eval_str(
            r#"(list
                 (let ((default-directory "/mock:localhost#11111:/tmp/")
                       (tramp-mode t)
                       (tramp-remote-process-environment
                        '("EMAXX_REMOTE_PORT=11111")))
                   (with-temp-buffer
                     (shell-command
                      "printf %s \"$EMAXX_REMOTE_PORT\"" t)
                     (buffer-string)))
                 (let ((process-environment
                        '("EMAXX_ENV_ORDER=first"
                          "EMAXX_ENV_ORDER=last")))
                   (with-temp-buffer
                     (shell-command
                      "printf %s \"$EMAXX_ENV_ORDER\"" t)
                     (buffer-string))))"#,
        ),
        Value::list([Value::String("11111".into()), Value::String("first".into()),])
    );
}

#[test]
fn lexical_file_name_operations_ignore_a_remote_default_directory() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun sample-unreachable-file-handler
                     (&rest _args)
                   (error "lexical operation reached remote handler"))
                 (let ((default-directory "/mock::/tmp/work/")
                       (file-name-handler-alist
                        '(("\\`/mock:" . sample-unreachable-file-handler))))
                   (list (file-name-nondirectory "sh")
                         (file-name-directory "bin/sh")
                         (file-name-as-directory "bin"))))"#
        ),
        Value::list([
            Value::String("sh".into()),
            Value::String("bin/".into()),
            Value::String("bin/".into()),
        ])
    );
}

#[test]
fn mock_remote_abbreviation_stays_inside_the_native_transport() {
    assert_eq!(
        eval_str(
            r#"(let ((file-name-handler-alist
                      '(("\\`/mock:" . sample-unreachable-file-handler))))
                 (list
                  (find-file-name-handler
                   "/mock::/tmp/work/" 'abbreviate-file-name)
                  (abbreviate-file-name "/mock::/tmp/work/")))"#
        ),
        Value::list([
            Value::symbol("emaxx-mock-file-name-handler"),
            Value::String("/mock::/tmp/work/".into()),
        ])
    );
}

#[test]
fn files_facade_recognizes_a_bare_mock_connection_prefix() {
    let prefix = format!("/mock:{}:", crate::lisp::primitives::system_name_value());
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun sample-files-file-remote-p
                     (file &optional identification connected)
                   (let ((handler
                          (find-file-name-handler file 'file-remote-p)))
                     (and handler
                          (funcall handler 'file-remote-p file
                                   identification connected))))
                 (let ((default-directory "/mock::")
                       (command "sh"))
                   (list
                    (sample-files-file-remote-p default-directory)
                    (or (and (stringp command)
                             (sample-files-file-remote-p command))
                        (sample-files-file-remote-p
                         default-directory)))))"#
        ),
        Value::list([
            Value::String(prefix.clone().into()),
            Value::String(prefix.into()),
        ])
    );
}

#[test]
fn default_directory_binding_is_visible_to_called_functions() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun sample-called-default-directory () default-directory)
                 (let ((default-directory "/mock::/tmp/work/"))
                   (list (special-variable-p 'default-directory)
                         (sample-called-default-directory))))"#
        ),
        Value::list([Value::T, Value::String("/mock::/tmp/work/".into()),])
    );
}

#[test]
fn mock_remote_predicates_do_not_fall_through_to_registered_remote_handlers() {
    assert_eq!(
        eval_str(
            "(let ((file-name-handler-alist
                    '((\".*\" . should-not-handle-native-mock))))
               (mapcar (lambda (operation)
                         (find-file-name-handler
                          \"/mock::/tmp/\" operation))
                       '(file-accessible-directory-p file-directory-p
                         file-executable-p file-exists-p file-readable-p
                         file-regular-p file-writable-p)))"
        ),
        Value::list(std::iter::repeat_n(
            Value::symbol("emaxx-mock-file-name-handler"),
            7
        ))
    );
}

#[test]
fn mock_remote_home_localname_uses_the_local_host_home() {
    assert_eq!(
        eval_str(
            r#"(let* ((remote "/mock::~/file.txt")
                       (qualified (format "/mock:%s:~/file.txt" (system-name)))
                       (expected (expand-file-name "~/file.txt")))
                  (list (equal (file-local-name remote) expected)
                        (equal (file-local-name qualified) expected)
                        (equal (file-remote-p remote 'localname) expected)
                        (eq (find-file-name-handler
                             qualified 'file-remote-p)
                            'emaxx-mock-file-name-handler)
                        (equal
                         (funcall
                          (find-file-name-handler qualified 'file-remote-p)
                          'file-remote-p qualified 'localname)
                         expected)
                        (file-directory-p "/mock::~/")))"#,
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::T, Value::T,])
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
fn native_window_scroll_policy_is_bound_and_dynamically_special() {
    assert_eq!(
        eval_str(
            "(progn
               (defun sample-window-scroll-targets ()
                 (list minibuffer-scroll-window
                       other-window-scroll-buffer
                       other-window-scroll-default))
               (list
                (mapcar #'boundp '(minibuffer-scroll-window
                                    other-window-scroll-buffer
                                    other-window-scroll-default))
                (mapcar #'special-variable-p '(minibuffer-scroll-window
                                                other-window-scroll-buffer
                                                other-window-scroll-default))
                (let ((minibuffer-scroll-window 'mini)
                      (other-window-scroll-buffer 'buffer)
                      (other-window-scroll-default 'fallback))
                  (sample-window-scroll-targets))))"
        ),
        Value::list([
            Value::list([Value::T, Value::T, Value::T]),
            Value::list([Value::T, Value::T, Value::T]),
            Value::list([
                Value::symbol("mini"),
                Value::symbol("buffer"),
                Value::symbol("fallback"),
            ]),
        ])
    );
}

#[test]
fn format_mode_line_is_a_batch_no_op_before_inspecting_the_format() {
    assert_eq!(
        eval_str(
            "(list (format-mode-line 'missing-mode-line-variable)
                   (format-mode-line '(\"ignored\" (:eval (error \"unreachable\")))
                                     t nil nil))"
        ),
        Value::list([
            Value::String(String::new().into()),
            Value::String(String::new().into())
        ])
    );
}

#[test]
fn buffer_modified_p_honors_its_optional_buffer_argument() {
    assert_eq!(
        eval_str(
            "(let ((clean (get-buffer-create \" *clean*\"))
                   (dirty (get-buffer-create \" *dirty*\")))
               (with-current-buffer clean
                 (insert \"clean\")
                 (set-buffer-modified-p nil))
               (with-current-buffer dirty
                 (insert \"dirty\")
                 (list (buffer-modified-p clean)
                       (buffer-modified-p dirty)
                       (buffer-modified-p))))"
        ),
        Value::list([Value::Nil, Value::T, Value::T])
    );
}

#[test]
fn native_buffer_ticks_are_signed_distinct_and_honor_buffer_arguments() {
    assert_eq!(
        eval_str(
            "(let* ((current (current-buffer))
                    (before (buffer-modified-tick current))
                    (other (get-buffer-create \" *tick-contract*\")))
               (internal--set-buffer-modified-tick -3 other)
               (list
                 (buffer-modified-tick other)
                 (buffer-chars-modified-tick other)
                 (with-current-buffer other
                   (insert \"x\")
                   (list
                     (buffer-modified-tick)
                     (buffer-chars-modified-tick)))
                 (= before (buffer-modified-tick current))
                 (list
                   (subrp
                     (symbol-function
                       'internal--set-buffer-modified-tick))
                   (func-arity
                     'internal--set-buffer-modified-tick))))"
        ),
        Value::list([
            Value::Integer(-3),
            Value::Integer(1),
            Value::list([Value::Integer(-2), Value::Integer(-2)]),
            Value::T,
            Value::list([Value::T, Value::cons(Value::Integer(1), Value::Integer(2)),]),
        ])
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
fn header_line_format_accepts_a_quoted_eval_form_as_data() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (setq header-line-format
                     '(:eval (get-text-property (point-min) 'header-line)))
               header-line-format)"
        ),
        Value::list([
            Value::Symbol(":eval".into()),
            Value::list([
                Value::Symbol("get-text-property".into()),
                Value::list([Value::Symbol("point-min".into())]),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("header-line".into()),
                ]),
            ]),
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
fn command_loop_c_variables_are_dynamic_across_function_calls() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun emaxx-test-command-loop-state ()
                   (list this-command
                         real-this-command
                         this-original-command
                         last-command))
                 (let ((this-command 'self-insert-command)
                       (real-this-command 'self-insert-command)
                       (this-original-command 'self-insert-command)
                       (last-command 'previous-command))
                   (list
                    (emaxx-test-command-loop-state)
                    (memq this-command '(self-insert-command delete-backward-char))
                    (mapcar #'special-variable-p
                            '(this-command real-this-command
                              this-original-command last-command)))))"#
        ),
        Value::list([
            Value::list([
                Value::Symbol("self-insert-command".into()),
                Value::Symbol("self-insert-command".into()),
                Value::Symbol("self-insert-command".into()),
                Value::Symbol("previous-command".into()),
            ]),
            Value::list([
                Value::Symbol("self-insert-command".into()),
                Value::Symbol("delete-backward-char".into()),
            ]),
            Value::list([Value::T, Value::T, Value::T, Value::T]),
        ])
    );
}

#[test]
fn installation_directory_c_variables_are_dynamic_across_function_calls() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun emaxx-test-installation-directories ()
                   (list source-directory data-directory doc-directory
                         configure-info-directory))
                 (let ((source-directory "/source/")
                       (data-directory "/data/")
                       (doc-directory "/doc/")
                       (configure-info-directory "/info/"))
                   (list
                    (emaxx-test-installation-directories)
                    (mapcar #'special-variable-p
                            '(source-directory data-directory doc-directory
                              configure-info-directory)))))"#
        ),
        Value::list([
            Value::list([
                Value::String("/source/".into()),
                Value::String("/data/".into()),
                Value::String("/doc/".into()),
                Value::String("/info/".into()),
            ]),
            Value::list([Value::T, Value::T, Value::T, Value::T]),
        ])
    );
}

#[test]
fn recursive_minibuffer_c_policy_is_bound_and_special() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun emaxx-test-recursive-minibuffer-policy ()
                   enable-recursive-minibuffers)
                 (list enable-recursive-minibuffers
                       (special-variable-p 'enable-recursive-minibuffers)
                       (let ((enable-recursive-minibuffers t))
                         (emaxx-test-recursive-minibuffer-policy))))"#
        ),
        Value::list([Value::Nil, Value::T, Value::T])
    );
}

#[test]
fn file_completion_minibuffer_policy_is_bound_and_special() {
    assert_eq!(
        eval_str(
            "(list minibuffer-completing-file-name
                   (special-variable-p
                    'minibuffer-completing-file-name))"
        ),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn native_minibuffer_completion_session_state_is_bound_and_special() {
    assert_eq!(
        eval_str(
            "(mapcar (lambda (name)
                       (list (boundp name)
                             (symbol-value name)
                             (special-variable-p name)))
                     '(minibuffer-completion-table
                       minibuffer-completion-predicate
                       minibuffer-completion-confirm))"
        ),
        Value::list(std::iter::repeat_n(
            Value::list([Value::T, Value::Nil, Value::T]),
            3
        ))
    );
}

#[test]
fn evaluator_debugger_policy_is_bound_and_dynamic_across_function_calls() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defun emaxx-test-active-debugger-policy ()
                   (list debugger debug-on-error debug-on-quit debug-on-signal
                         debugger-may-continue debug-on-next-call
                         backtrace-on-error-noninteractive))
                 (list
                  (emaxx-test-active-debugger-policy)
                  (mapcar #'special-variable-p
                          '(debugger debug-on-error debug-on-quit debug-on-signal
                            debugger-may-continue debug-on-next-call
                            backtrace-on-error-noninteractive))
                  (let ((debugger 'emaxx-test-debugger)
                        (debug-on-error t)
                        (debug-on-quit t)
                        (debug-on-signal t)
                        (debugger-may-continue nil)
                        (debug-on-next-call t)
                        (backtrace-on-error-noninteractive nil))
                    (emaxx-test-active-debugger-policy))))"#
        ),
        Value::list([
            Value::list([
                Value::Symbol("debug".into()),
                Value::Nil,
                Value::Nil,
                Value::Nil,
                Value::T,
                Value::Nil,
                Value::T,
            ]),
            Value::list([
                Value::T,
                Value::T,
                Value::T,
                Value::T,
                Value::T,
                Value::T,
                Value::T,
            ]),
            Value::list([
                Value::Symbol("emaxx-test-debugger".into()),
                Value::T,
                Value::T,
                Value::T,
                Value::Nil,
                Value::T,
                Value::Nil,
            ]),
        ])
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
fn native_per_buffer_manifest_is_special_and_automatically_local() {
    let interp = Interpreter::new();
    let env = Env::new();
    let mut unbound = Vec::new();
    for variable in GNU_NATIVE_PER_BUFFER_VARIABLES {
        let name = variable.name;
        assert!(
            interp.is_per_buffer_special(name),
            "GNU DEFVAR_PER_BUFFER `{name}` must retain native forwarding semantics"
        );
        assert!(
            interp.is_auto_buffer_local(name),
            "GNU DEFVAR_PER_BUFFER `{name}` must become local when assigned"
        );
        assert_eq!(
            interp.is_always_buffer_local_special(name),
            variable.always_local,
            "GNU DEFVAR_PER_BUFFER `{name}` has the wrong default-inheriting/always-local subtype"
        );
        assert_eq!(
            interp
                .get_symbol_property(name, "permanent-local")
                .is_some_and(|value| value.is_truthy()),
            variable.permanent,
            "GNU DEFVAR_PER_BUFFER `{name}` has the wrong permanence metadata"
        );
        if interp.lookup_var(name, &env).is_none() {
            unbound.push(name);
        }
    }
    assert!(
        unbound.is_empty(),
        "GNU DEFVAR_PER_BUFFER slots cannot be void: {unbound:?}"
    );
    let mut unique = std::collections::HashSet::new();
    assert!(
        GNU_NATIVE_PER_BUFFER_VARIABLES
            .iter()
            .all(|variable| unique.insert(variable.name)),
        "native per-buffer manifest contains a duplicate entry"
    );
}

#[test]
fn builtin_startup_defaults_are_intrinsically_special() {
    let interp = Interpreter::new();
    for name in [
        "resize-mini-windows",
        "max-mini-window-height",
        "inhibit-point-motion-hooks",
        "inhibit-x-resources",
        "tab-bar-mode",
        "auto-resize-tab-bars",
        "auto-raise-tab-bar-buttons",
        "auto-resize-tool-bars",
        "auto-raise-tool-bar-buttons",
        "tab-bar-border",
        "tab-bar-button-margin",
        "tab-bar-button-relief",
        "tool-bar-border",
        "tool-bar-button-margin",
        "tool-bar-button-relief",
        "read-minibuffer-restore-windows",
    ] {
        assert!(
            interp.builtin_var_value(name).is_some(),
            "missing `{name}` default"
        );
        assert!(
            interp.is_special_variable(name),
            "builtin startup variable `{name}` did not inherit special binding semantics"
        );
    }
    assert_eq!(
        eval_str(
            "(progn
               (defun emaxx-test-native-startup-bindings ()
                 (list resize-mini-windows max-mini-window-height
                       inhibit-point-motion-hooks inhibit-x-resources
                       tab-bar-mode))
               (let ((resize-mini-windows nil)
                     (max-mini-window-height 7)
                     (inhibit-point-motion-hooks nil)
                     (inhibit-x-resources nil)
                     (tab-bar-mode t))
                 (emaxx-test-native-startup-bindings)))"
        ),
        Value::list([
            Value::Nil,
            Value::Integer(7),
            Value::Nil,
            Value::Nil,
            Value::T,
        ])
    );
    assert_eq!(
        eval_str(
            "(list auto-resize-tab-bars auto-raise-tab-bar-buttons
                   auto-resize-tool-bars auto-raise-tool-bar-buttons
                   tab-bar-border tab-bar-button-margin tab-bar-button-relief
                   tool-bar-border tool-bar-button-margin tool-bar-button-relief
                   read-minibuffer-restore-windows)"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::Symbol("internal-border-width".into()),
            Value::Integer(1),
            Value::Integer(1),
            Value::Symbol("internal-border-width".into()),
            Value::Integer(4),
            Value::Integer(1),
            Value::T,
        ])
    );
}

#[test]
fn startup_global_values_and_special_declarations_share_one_registry() {
    let interp = Interpreter::new();
    for (name, _) in &interp.globals {
        assert!(
            interp.is_special_variable(name),
            "startup global `{name}` was not registered as special"
        );
    }
    let names = interp.special_variable_names();
    let unique = names.iter().collect::<std::collections::HashSet<_>>();
    assert_eq!(
        unique.len(),
        names.len(),
        "special-variable registry contains duplicate declarations"
    );
}

#[test]
fn startup_features_own_their_capability_metadata_in_one_manifest() {
    let interp = Interpreter::new();
    let mut unique = std::collections::HashSet::new();
    for feature in STARTUP_FEATURES {
        assert!(
            unique.insert(feature.name),
            "startup feature manifest contains duplicate `{}`",
            feature.name
        );
        assert!(
            interp.has_feature(feature.name),
            "startup feature `{}` was not provided",
            feature.name
        );
        if let Some(subfeatures) = feature.subfeatures {
            assert_eq!(
                interp.get_symbol_property(feature.name, "subfeatures"),
                Some(subfeatures()),
                "startup feature `{}` lost its subfeatures",
                feature.name
            );
        }
    }
}

#[test]
fn dumped_auto_buffer_locals_share_their_defaults_and_locality_manifest() {
    let interp = Interpreter::new();
    let mut unique = std::collections::HashSet::new();
    for variable in DUMPED_AUTO_BUFFER_LOCALS {
        assert!(
            unique.insert(variable.name),
            "dumped auto-buffer-local manifest contains duplicate `{}`",
            variable.name
        );
        assert_eq!(
            interp.builtin_var_value(variable.name),
            Some(variable.default.value()),
            "dumped auto-buffer-local `{}` lost its default",
            variable.name
        );
        assert!(
            interp.is_auto_buffer_local(variable.name),
            "dumped auto-buffer-local `{}` lost its locality",
            variable.name
        );
        assert!(interp.is_special_variable(variable.name));
    }
}

#[test]
fn native_change_hook_controls_are_bound_and_dynamically_special() {
    let interp = Interpreter::new();
    let env = Env::new();
    for name in GNU_CHANGE_HOOK_SPECIAL_VARIABLES {
        assert_eq!(interp.lookup_var(name, &env), Some(Value::Nil));
        assert!(
            interp.is_special_variable(name),
            "GNU native change-hook variable `{name}` must be dynamically special"
        );
    }
}

#[test]
fn native_permanent_buffer_locals_survive_major_mode_reset() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
               (setq truncate-lines t
                     buffer-file-coding-system 'iso-2022-7bit)
               (kill-all-local-variables)
               (list truncate-lines
                     buffer-file-coding-system
                     (local-variable-p 'truncate-lines)
                     (local-variable-p 'buffer-file-coding-system)))"
        ),
        Value::list([
            Value::T,
            Value::Symbol("iso-2022-7bit".into()),
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn builtin_text_codings_expose_complete_eol_variant_families() {
    let interp = Interpreter::new();
    for base in [
        "undecided",
        "us-ascii",
        "iso-latin-1",
        "cyrillic-koi8",
        "utf-8",
        "utf-8-with-signature",
        "utf-16",
        "utf-16le",
        "utf-16be",
        "utf-8-auto",
        "prefer-utf-8",
        "raw-text",
        "mac-roman",
        "euc-jp",
        "iso-2022-7bit",
        "sjis",
        "big5",
        "chinese-gb18030",
    ] {
        for suffix in ["unix", "dos", "mac"] {
            let variant = format!("{base}-{suffix}");
            assert!(
                interp.has_coding_system(&variant),
                "GNU coding-system variant `{variant}` must be registered"
            );
        }
    }
    for suffix in ["unix", "dos", "mac"] {
        assert!(!interp.has_coding_system(&format!("no-conversion-{suffix}")));
    }
}

#[test]
fn dumped_simple_shell_command_state_is_available_at_startup() {
    assert_eq!(
        eval_str(
            "(list shell-command-buffer-name
                   shell-command-buffer-name-async
                   shell-command-history
                   shell-command-default-error-buffer
                   async-shell-command-buffer
                   async-shell-command-display-buffer
                   async-shell-command-width
                   shell-command-prompt-show-cwd
                   shell-command-dont-erase-buffer
                   shell-command-saved-pos)"
        ),
        Value::list([
            Value::String("*Shell Command Output*".into()),
            Value::String("*Async Shell Command*".into()),
            Value::Nil,
            Value::Nil,
            Value::Symbol("confirm-new-buffer".into()),
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn gnu_bindings_host_feature_manifest_is_preprovided() {
    assert_eq!(
        eval_str(
            "(list (featurep 'base64)
                   (featurep 'md5)
                   (featurep 'sha1)
                   (mapcar (lambda (subfeature)
                             (featurep 'overlay subfeature))
                           '(display syntax-table field))
                   (mapcar (lambda (subfeature)
                             (featurep 'text-properties subfeature))
                           '(display syntax-table field point-entered)))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
            Value::list([Value::T, Value::T, Value::T]),
            Value::list([Value::T, Value::T, Value::T, Value::T]),
        ])
    );
}

#[test]
fn gnu_callproc_host_program_manifest_is_bound_and_special() {
    assert_eq!(
        eval_str(
            "(mapcar (lambda (entry)
                       (let ((name (car entry))
                             (expected (cdr entry)))
                         (list name
                               (boundp name)
                               (equal (symbol-value name) expected)
                               (special-variable-p name))))
                     '((ctags-program-name . \"ctags\")
                       (etags-program-name . \"etags\")
                       (hexl-program-name . \"hexl\")
                       (emacsclient-program-name . \"emacsclient\")
                       (movemail-program-name . \"movemail\")
                       (ebrowse-program-name . \"ebrowse\")
                       (rcs2log-program-name . \"rcs2log\")))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("ctags-program-name".into()),
                Value::T,
                Value::T,
                Value::T
            ]),
            Value::list([
                Value::Symbol("etags-program-name".into()),
                Value::T,
                Value::T,
                Value::T
            ]),
            Value::list([
                Value::Symbol("hexl-program-name".into()),
                Value::T,
                Value::T,
                Value::T
            ]),
            Value::list([
                Value::Symbol("emacsclient-program-name".into()),
                Value::T,
                Value::T,
                Value::T
            ]),
            Value::list([
                Value::Symbol("movemail-program-name".into()),
                Value::T,
                Value::T,
                Value::T
            ]),
            Value::list([
                Value::Symbol("ebrowse-program-name".into()),
                Value::T,
                Value::T,
                Value::T
            ]),
            Value::list([
                Value::Symbol("rcs2log-program-name".into()),
                Value::T,
                Value::T,
                Value::T
            ]),
        ])
    );
}

#[test]
fn category_primitives_honor_gnu_optional_tables_ranges_and_reset() {
    assert_eq!(
        eval_str(
            "(let ((table (make-category-table)))
               (set-category-table table)
               ;; These omitted TABLE arguments are the form used by GNU's
               ;; dumped character and word-wrap setup.
               (define-category ?x \"category x\")
               (define-category ?y \"category y\")
               (modify-category-entry ?B ?y)
               ;; A range update must retain the narrower pre-existing value.
               (modify-category-entry '(?A . ?C) ?x)
               ;; Category tables span GNU's complete internal character
               ;; range, not only Unicode scalar values.
               (modify-category-entry #x200020 ?x)
               ;; Nil TABLE also means the current buffer's table.
               (modify-category-entry ?B ?x nil t)
               (list (category-docstring ?x)
                     (mapcar
                      (lambda (character)
                        (let ((set (char-category-set character)))
                          (list (aref set ?x) (aref set ?y))))
                      '(?A ?B ?C))
                     (aref (char-category-set #x200020) ?x)
                     (category-table-p (copy-category-table))))"
        ),
        Value::list([
            Value::String("category x".into()),
            Value::list([
                Value::list([Value::T, Value::Nil]),
                Value::list([Value::Nil, Value::T]),
                Value::list([Value::T, Value::Nil]),
            ]),
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn regexp_category_atoms_use_live_buffer_and_standard_category_tables() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                 (let ((table (make-category-table))
                       (case-fold-search t))
                   (define-category ?x "category x" table)
                   (set-category-table table)
                   (insert "A")
                   (goto-char (point-min))
                   (list
                    (looking-at "\\cx")
                    (progn
                      (modify-category-entry ?A ?x)
                      (looking-at "\\cx"))
                    (progn
                      (modify-category-entry ?A ?x nil t)
                      (looking-at "\\Cx"))
                    ;; String matching uses the standard category table,
                    ;; not this buffer's replacement table.
                    (string-match-p "\\cx" "A"))))"#,
        ),
        Value::list([Value::Nil, Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn self_insert_calls_internal_auto_fill_only_for_configured_characters() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (setq emaxx-test-auto-fill-calls nil)
                 (fset 'internal-auto-fill
                       (lambda ()
                         (setq emaxx-test-auto-fill-calls
                               (cons (list (point) (buffer-string))
                                     emaxx-test-auto-fill-calls))
                         t))
                 (with-temp-buffer
                   (setq-local auto-fill-function 'ignore)
                   (self-insert-command 1 ?x)
                   (self-insert-command 1 ?\s)
                   (self-insert-command 1 ?\n)
                   (list (nreverse emaxx-test-auto-fill-calls)
                         (point)
                         (buffer-string))))"#,
        ),
        Value::list([
            Value::list([
                Value::list([Value::Integer(3), Value::String("x ".into())]),
                Value::list([Value::Integer(3), Value::String("x \n".into())]),
            ]),
            Value::Integer(4),
            Value::String("x \n".into()),
        ])
    );
}

#[test]
fn get_unused_category_scans_the_complete_printable_category_range() {
    assert_eq!(
        eval_str(
            "(let ((table (make-category-table)))
               (prog1
                   (list
                     (get-unused-category table)
                     (progn
                       (define-category 32 \"space\" table)
                       (get-unused-category table))
                     (subrp
                       (symbol-function 'get-unused-category)))
                 (let ((category 33))
                   (while (<= category 126)
                     (define-category category \"used\" table)
                     (setq category (1+ category)))
                   (unless (null (get-unused-category table))
                     (error \"category range was not exhausted\")))))"
        ),
        Value::list([Value::Integer(32), Value::Integer(33), Value::T])
    );
}

#[test]
fn dumped_define_prefix_command_sets_function_and_requested_value_cell() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (define-prefix-command 'emaxx-test-prefix
                                      'emaxx-test-prefix-map
                                      \"Test menu\")
               (list (keymapp (symbol-function 'emaxx-test-prefix))
                     (keymapp emaxx-test-prefix-map)
                     (eq (symbol-function 'emaxx-test-prefix)
                         emaxx-test-prefix-map)
                     (boundp 'emaxx-test-prefix)))"
        ),
        Value::list([Value::T, Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn gnu_lread_c_variables_are_declared_special() {
    let interp = Interpreter::new();
    for name in GNU_LREAD_SPECIAL_VARIABLES {
        assert!(
            interp.is_special_variable(name),
            "GNU lread.c DEFVAR `{name}` must remain dynamically scoped"
        );
        assert!(
            interp.lookup_var(name, &Vec::new()).is_some(),
            "GNU lread.c DEFVAR `{name}` must have a startup value"
        );
    }
}

#[test]
fn gnu_keyboard_c_startup_policy_has_one_bound_special_value_cell() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (defun emaxx-read-echo-keystrokes () echo-keystrokes)
               (list
                echo-keystrokes
                echo-keystrokes-help
                polling-period
                double-click-time
                double-click-fuzz
                (let ((echo-keystrokes 0))
                  (emaxx-read-echo-keystrokes))
                (special-variable-p 'echo-keystrokes)))"
        ),
        Value::list([
            Value::Integer(1),
            Value::T,
            Value::Float(2.0),
            Value::Integer(500),
            Value::Integer(3),
            Value::Integer(0),
            Value::T,
        ])
    );
}

#[test]
fn gnu_allocator_emergency_state_exists_before_jit_lock_loads() {
    assert_eq!(
        eval_str(
            "(list
               memory-full
               memory-signal-data
               (special-variable-p 'memory-full)
               (special-variable-p 'memory-signal-data))"
        ),
        Value::list([
            Value::Nil,
            Value::list([
                Value::symbol("error"),
                Value::String(
                    "Memory exhausted--use M-x save-some-buffers then exit and restart Emacs"
                        .into(),
                ),
            ]),
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn gnu_emacs_c_locale_variables_exist_before_lisp_startup_policy_runs() {
    let interp = Interpreter::new();
    for name in GNU_EMACS_LOCALE_SPECIAL_VARIABLES {
        assert!(
            interp.is_special_variable(name),
            "GNU emacs.c locale DEFVAR `{name}` must remain dynamically scoped"
        );
        assert_eq!(
            interp.lookup_var(name, &Vec::new()),
            Some(Value::Nil),
            "GNU emacs.c locale DEFVAR `{name}` must start bound to nil"
        );
    }
}

#[test]
fn dumped_lread_loader_policy_defaults_are_complete() {
    #[cfg(target_os = "macos")]
    let dynamic_suffixes =
        Value::list([Value::String(".dylib".into()), Value::String(".so".into())]);
    #[cfg(all(unix, not(target_os = "macos")))]
    let dynamic_suffixes = Value::list([Value::String(".so".into())]);
    #[cfg(windows)]
    let dynamic_suffixes = Value::list([Value::String(".dll".into())]);

    let suffix_vector = dynamic_suffixes
        .to_vec()
        .expect("dynamic suffix list")
        .into_iter()
        .chain([Value::String(".elc".into()), Value::String(".el".into())]);
    assert_eq!(
        eval_str(
            "(list load-suffixes
                   dynamic-library-suffixes
                   load-file-rep-suffixes
                   load-source-file-function
                   load-force-doc-strings
                   load-convert-to-unibyte
                   load-dangerous-libraries
                   force-load-messages
                   load-prefer-newer
                   load-no-native
                   read-symbol-shorthands)"
        ),
        Value::list([
            Value::list(suffix_vector),
            dynamic_suffixes,
            Value::list([
                Value::String(String::new().into()),
                Value::String(".gz".into()),
            ]),
            Value::Symbol("load-with-code-conversion".into()),
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
fn fill_column_binding_crosses_lexical_function_boundaries_and_assigns_locally() {
    assert_eq!(
        eval_str(
            r#"(progn
                  (defun emaxx-test-read-fill-column () fill-column)
                  (let ((original (current-buffer)))
                    (list
                     (special-variable-p 'fill-column)
                     (let ((fill-column 5))
                       (list (emaxx-test-read-fill-column)
                             (with-temp-buffer
                               (emaxx-test-read-fill-column))))
                     (let ((buffer-file-name "outer"))
                       (with-temp-buffer buffer-file-name))
                     (progn
                       (setq fill-column 30)
                       (with-temp-buffer
                         (list fill-column
                               (progn (setq fill-column 40) fill-column)
                               (with-current-buffer original fill-column)))))))"#,
        ),
        Value::list([
            Value::T,
            Value::list([Value::Integer(5), Value::Integer(5)]),
            Value::Nil,
            Value::list([Value::Integer(70), Value::Integer(40), Value::Integer(30),]),
        ])
    );
}

#[test]
fn host_noninteractive_flag_is_dynamically_visible_across_function_calls() {
    assert_eq!(
        eval_str(
            r#"(progn
                  (setq noninteractive t)
                  (defun emaxx-test-read-noninteractive () noninteractive)
                  (list (let ((noninteractive nil))
                          (emaxx-test-read-noninteractive))
                        noninteractive))"#,
        ),
        Value::list([Value::Nil, Value::T])
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
    for listing in [
        "-rw-r--r--@    1 alpha  staff      0 Mar 16 04:57 foo.c",
        "-rw-r--r--     1 501    20    238779 06-12 13:35 src/alloc.c",
        "-rw-r--r--     1 alpha  staff      0 2026-06-12 13:35 foo.c",
    ] {
        assert_ne!(
            eval_str(&format!(
                "(string-match-p directory-listing-before-filename-regexp {listing:?})"
            )),
            Value::Nil,
            "listing should match: {listing}"
        );
    }
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
fn defcustom_initializer_runs_setter_at_declaration_time() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(progn
                 (defun sample-custom-initialize-reset (symbol exp)
                   (funcall (get symbol 'custom-set)
                            symbol
                            (if (boundp symbol) (symbol-value symbol) (eval exp t))))
                 (defvar sample-dependent-value nil)
                 (defun sample-option-setter (symbol value)
                   (set-default symbol value)
                   (setq sample-dependent-value (concat "[" value "]")))
                 (defcustom sample-option "abc" "doc"
                   :set #'sample-option-setter
                   :initialize #'sample-custom-initialize-reset
                   :type 'string)
                 (list sample-option sample-dependent-value))"#
        ),
        Value::list([Value::String("abc".into()), Value::String("[abc]".into()),])
    );
}

#[test]
fn kmacro_frontier_defcustom_uses_the_standard_reset_initializer_by_default() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(progn
                 (defvar sample-default-initializer-side-effect nil)
                 (defun sample-default-initializer-setter (symbol value)
                   (set-default symbol value)
                   (setq sample-default-initializer-side-effect value))
                 (defcustom sample-default-initializer-option 42 "doc"
                   :set #'sample-default-initializer-setter
                   :type 'integer)
                 (list sample-default-initializer-option
                       sample-default-initializer-side-effect))"#
        ),
        Value::list([Value::Integer(42), Value::Integer(42)])
    );
}

#[test]
fn delayed_defcustom_initializer_runs_setter_after_startup() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(progn
                 (defun sample-delayed-setter (symbol value)
                   (set-default symbol value)
                   (setq sample-delayed-side-effect
                         (concat "[" value "]")))
                 (defcustom sample-delayed-option
                   (concat "runtime-" "default")
                   "doc"
                   :set #'sample-delayed-setter
                   :initialize #'custom-initialize-delay
                   :type 'string)
                 (list sample-delayed-option
                       sample-delayed-side-effect
                       custom-delayed-init-variables))"#
        ),
        Value::list([
            Value::String("runtime-default".into()),
            Value::String("[runtime-default]".into()),
            Value::T,
        ])
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
fn killing_buffer_replaces_it_in_every_window() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(let* ((original (current-buffer))
                    (victim (get-buffer-create \" kill-other-window-buffer-victim\"))
                    (other (split-window-internal
                            (selected-window) 10 'below 0)))
               (set-window-buffer other victim)
               (kill-buffer victim)
               (list (window-live-p other)
                     (buffer-live-p (window-buffer other))
                     (eq (window-buffer other) original)))"
        ),
        Value::list([Value::T, Value::T, Value::T])
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
fn native_visual_line_mode_exposes_the_complete_minor_mode_contract() {
    assert_eq!(
        eval_str(
            "(let (events)
               (with-temp-buffer
                   (add-hook 'visual-line-mode-hook
                             (lambda () (push visual-line-mode events))
                             nil t)
                   (visual-line-mode)
                   (list visual-line-mode
                         (local-variable-p 'visual-line-mode)
                         (keymapp visual-line-mode-map)
                         (eq (lookup-key visual-line-mode-map
                                         [remap kill-line])
                             'kill-visual-line)
                         (eq (cdr (assq 'visual-line-mode
                                        minor-mode-map-alist))
                             visual-line-mode-map)
                         (not (null (memq 'visual-line-mode
                                          local-minor-modes)))
                         (progn (visual-line-mode 0) visual-line-mode)
                         events)))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::Nil,
            Value::list([Value::Nil, Value::T]),
        ])
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
            r#"(let ((transient-mark-mode t))
                  (with-temp-buffer
                    (insert "abc")
                    ;; This bare interpreter fixture does not preload GNU
                    ;; simple.el.  Install the mark through native marker
                    ;; storage; `deactivate-mark' is the behavior under test.
                    (set-marker (mark-marker) 1 (current-buffer))
                    (setq mark-active t)
                    (goto-char 3)
                    (list (region-active-p)
                          (deactivate-mark)
                          (region-active-p))))"#
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
fn only_defcustom_declarations_gain_a_standard_value() {
    assert_eq!(
        eval_str(
            "(progn
               (defvar sample-ordinary-variable nil)
               (defvar-local sample-local-variable nil)
               (defcustom sample-custom-variable nil \"doc\" :type 'boolean)
               (list (get 'sample-ordinary-variable 'standard-value)
                     (get 'sample-local-variable 'standard-value)
                     (not (null (get 'sample-custom-variable
                                     'standard-value)))))"
        ),
        Value::list([Value::Nil, Value::Nil, Value::T])
    );
}

#[test]
fn define_minor_mode_registers_mode_line_and_keymap_metadata() {
    assert_eq!(
        eval_str(
            "(progn
               (defvar-keymap sample-mode-map \"x\" 'ignore)
               (defvar sample-mode-name \" Sample\")
               (define-minor-mode sample-mode \"doc\"
                 :lighter sample-mode-name
                 :keymap sample-mode-map)
               (list (assq 'sample-mode minor-mode-alist)
                     (eq (cdr (assq 'sample-mode minor-mode-map-alist))
                         sample-mode-map)
                     (car minor-mode-list)))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("sample-mode".into()),
                Value::Symbol("sample-mode-name".into()),
            ]),
            Value::T,
            Value::Symbol("sample-mode".into()),
        ])
    );
}

#[test]
fn startup_does_not_claim_lisp_owned_url_features() {
    assert_eq!(
        eval_str(
            "(list (featurep 'url)
                     (featurep 'url-http)
                     (autoloadp (symbol-function 'url-retrieve))
                     url-configuration-directory
                     url-redirect-buffer
                     url-retrieve-number-of-calls
                     url-asynchronous
                     url-dead-buffer-list
                     (equal url-configuration-directory
                            (locate-user-emacs-file \"url/\" \".url/\")))"
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::String("/nonexistent/.emacs.d/url/".into()),
            Value::Nil,
            Value::Integer(0),
            Value::T,
            Value::Nil,
            Value::T,
        ])
    );
}

#[test]
fn generated_dumped_variable_defaults_include_loaddefs_value_cells() {
    assert_eq!(
        eval_str(
            "(list (car image-file-name-extensions)
                   (member \"webp\" image-file-name-extensions)
                   image-file-name-regexps
                   (special-variable-p 'mail-personal-alias-file)
                   package-user-dir
                   package-directory-list
                   package-quickstart-file
                   rmail-spool-directory)"
        ),
        Value::list([
            Value::String("png".into()),
            Value::list([Value::String("webp".into())]),
            Value::Nil,
            Value::T,
            Value::String("~/.emacs.d/elpa".into()),
            Value::Nil,
            Value::String("~/.emacs.d/package-quickstart.el".into()),
            Value::String("/var/mail/".into()),
        ])
    );
}

#[test]
fn set_buffer_major_mode_uses_the_default_without_selecting_the_buffer() {
    assert_eq!(
        eval_str(
            "(let ((original (current-buffer))
                   (target (generate-new-buffer \"set-major-mode-target\")))
               (unwind-protect
                   (progn
                     (set-buffer-major-mode target)
                     (list (eq original (current-buffer))
                           (buffer-local-value 'major-mode target)))
                 (kill-buffer target)))"
        ),
        Value::list([Value::T, Value::Symbol("fundamental-mode".into())])
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
fn face_list_uses_the_native_face_registry_for_runtime_faces() {
    assert_eq!(
        eval_str(
            "(let ((define-runtime-face
                    (lambda (name)
                      (eval (list 'defface name
                                  ''((t :extend t)) \"doc\")))))
               (funcall define-runtime-face 'sample-runtime-face)
               (and (facep 'sample-runtime-face)
                    (memq 'sample-runtime-face (face-list))
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
fn eieio_object_type_does_not_leak_the_host_record_representation() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-eieio-root nil nil)
                   (let ((object (make-instance 'sample-eieio-root))
                         (table (make-hash-table))
                         (plain-record (record 'sample-plain-record)))
                     (list
                      (cl-typep object 'eieio-object)
                      (eieio-object-p object)
                      (recordp (eieio--object-class object))
                      (cl-typep table 'eieio-object)
                      (eieio-object-p table)
                      (cl-typep plain-record 'eieio-object)
                      (eieio-object-p plain-record)
                      (cl-typep [] 'eieio-object)
                      (eieio-object-p []))))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
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
fn defface_clauses_choose_by_the_terminal_color_count() {
    let spec = "(defface sample-min-colors-face
                  '((((class color) (min-colors 88)) :foreground \"red\")
                    (((class color) (min-colors 8)) :foreground \"blue\")
                    (t :weight bold))
                  \"doc\")";
    let probe = "(list (face-attribute 'sample-min-colors-face :foreground)
                       (face-attribute 'sample-min-colors-face :weight))";

    let mut color = Interpreter::new();
    color.set_tty_display_colors(8);
    eval_str_with(&mut color, spec);
    let values = eval_str_with(&mut color, probe).to_vec().unwrap();
    assert_string_value(values[0].clone(), "blue");
    assert_eq!(
        values[1],
        Value::Symbol("unspecified".into()),
        "only the first matching clause applies, not later ones"
    );

    let mut mono = Interpreter::new();
    eval_str_with(&mut mono, spec);
    let values = eval_str_with(&mut mono, probe).to_vec().unwrap();
    assert_eq!(
        values[0],
        Value::Symbol("unspecified".into()),
        "color clauses cannot match a colorless terminal"
    );
    assert_eq!(values[1], Value::Symbol("bold".into()));
}

#[test]
fn defface_default_clause_contributes_leading_defaults() {
    let mut interp = Interpreter::new();
    interp.set_tty_display_colors(8);
    let values = eval_str_with(
        &mut interp,
        "(progn
           (defface sample-defaulted-face
             '((default :underline t)
               (((class color) (background light)) :foreground \"blue\")
               (((class color) (background dark)) :foreground \"red\"))
             \"doc\")
           (list (face-attribute 'sample-defaulted-face :underline)
                 (face-attribute 'sample-defaulted-face :foreground)))",
    )
    .to_vec()
    .unwrap();
    assert_eq!(values[0], Value::T);
    assert_string_value(values[1].clone(), "blue");
}

#[test]
fn tty_face_attrs_resolve_through_the_face_machinery() {
    let mut interp = Interpreter::new();
    interp.set_tty_display_colors(8);
    // The real tty-color-translate lives in GNU's tty-colors.el, outside a
    // unit test's load path; a table stub exercises the same call channel.
    eval_str_with(
        &mut interp,
        "(progn
           (defun tty-color-translate (color &optional frame)
             (cdr (assoc color '((\"cyan\" . 6) (\"magenta\" . 5)))))
           (defface sample-parent-face '((t :background \"magenta\")) \"doc\")
           (defface sample-resolved-face
             '((t :foreground \"cyan\" :weight bold :extend t
                  :inherit sample-parent-face))
             \"doc\"))",
    );
    let mut env: Env = Vec::new();
    let attrs = crate::lisp::primitives::resolve_tty_face_attrs(
        &mut interp,
        &mut env,
        &Value::Symbol("sample-resolved-face".into()),
    );
    assert_eq!(attrs.foreground, Some(6));
    assert_eq!(
        attrs.background,
        Some(5),
        "unspecified attributes merge from the :inherit parent"
    );
    assert!(attrs.bold && attrs.extend);
    assert!(!attrs.reverse && !attrs.underline);
}

#[test]
fn window_face_spans_layer_text_properties_region_and_overlays() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        "(progn
           (insert \"abcdefghij\")
           (put-text-property 2 4 'face 'bold)
           (setq transient-mark-mode t)
           (set-marker (mark-marker) 5)
           (setq mark-active t)
           (goto-char 8)
           (let ((overlay (make-overlay 6 9)))
             (overlay-put overlay 'face 'isearch)
             (overlay-put overlay 'priority 1001)))",
    );
    let mut env: Env = Vec::new();
    let buffer_id = interp.current_buffer_id();
    let spans =
        crate::lisp::primitives::window_face_spans(&mut interp, &mut env, buffer_id, 1, 11, true);
    assert_eq!(
        spans,
        vec![
            (2, 4, Value::Symbol("bold".into())),
            (5, 8, Value::Symbol("region".into())),
            (6, 9, Value::Symbol("isearch".into())),
        ],
        "text properties first, then the active region, overlays above"
    );

    let without_region =
        crate::lisp::primitives::window_face_spans(&mut interp, &mut env, buffer_id, 1, 11, false);
    assert!(
        without_region
            .iter()
            .all(|(_, _, face)| !matches!(face, Value::Symbol(s) if s == "region")),
        "non-selected windows do not paint the region"
    );
}

#[test]
fn propertized_messages_carry_face_spans_to_the_echo_area() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        "(message \"%sabc\" (propertize \"I-search: \" 'face 'minibuffer-prompt))",
    );
    let (text, spans) = crate::lisp::primitives::echo_area_message_with_spans()
        .expect("message sets the echo area");
    assert_eq!(text, "I-search: abc");
    assert_eq!(
        spans,
        vec![(0, 10, Value::Symbol("minibuffer-prompt".into()))],
        "format keeps the argument's face properties in char offsets"
    );
    eval_str_with(&mut interp, "(message \"plain\")");
    let (_, spans) = crate::lisp::primitives::echo_area_message_with_spans().unwrap();
    assert!(spans.is_empty(), "plain messages carry no spans");
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
fn faces_compat_reports_named_faces_at_point_in_precedence_order() {
    let mut interp = Interpreter::new();
    load_faces_compat(&mut interp);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (defface sample-point-face-a '((t :weight bold)) \"a\")
               (defface sample-point-face-b '((t :slant italic)) \"b\")
               (list
                (with-temp-buffer
                  (insert \"x\")
                  (let ((overlay (make-overlay 1 2)))
                    (overlay-put overlay 'face 'sample-point-face-a)
                    (goto-char 1)
                    (face-at-point)))
                (with-temp-buffer
                  (insert (propertize
                           \"x\" 'face
                           '(sample-point-face-a (:weight bold)
                             sample-point-face-b sample-point-face-a)))
                  (goto-char 1)
                  (face-at-point nil t))
                (with-temp-buffer
                  (insert (propertize
                           \"x\" 'face 'sample-point-face-a
                           'read-face-name 'sample-point-face-b))
                  (goto-char 1)
                  (face-at-point))
                (face-list-p '(sample-point-face-a sample-point-face-b))
                (face-list-p '(:weight bold))))",
        ),
        Value::list([
            Value::symbol("sample-point-face-a"),
            Value::list([
                Value::symbol("sample-point-face-a"),
                Value::symbol("sample-point-face-b"),
            ]),
            Value::symbol("sample-point-face-b"),
            Value::T,
            Value::Nil,
        ])
    );
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
            "(let ((frame (selected-frame)))
              (list
              (window-system frame)
              (face-set-after-frame-default frame)
              (terminal-live-p (frame-terminal frame))
              (set-frame-parameter frame 'background-color \"white\")
              (frame-parameter nil 'background-color)
              (cdr (assq 'background-color (frame-parameters)))))"
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::String("white".into()),
            Value::String("white".into()),
        ])
    );
}

#[test]
fn modify_frame_parameters_persists_values_and_first_duplicate_wins() {
    assert_eq!(
        eval_str(
            "(list
               (subrp (indirect-function 'modify-frame-parameters))
               (modify-frame-parameters
                nil
                '((sample-parameter . first)
                  (sample-parameter . second)
                  (foreground-color . \"green\")))
               (frame-parameter nil 'sample-parameter)
               (cdr (assq 'sample-parameter (frame-parameters)))
               (frame-parameter nil 'foreground-color)
               (cdr (assq 'foreground-color (frame-parameters)))
               (frame-parameter nil 'menu-bar-lines))"
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::Symbol("first".into()),
            Value::Symbol("first".into()),
            Value::String("green".into()),
            Value::String("green".into()),
            Value::Integer(1),
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
fn let_alist_duplicate_keys_keep_the_first_assq_value() {
    assert_string_value(
        eval_str("(let-alist '((port . \"7070\") (port . \"70\")) .port)"),
        "7070",
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
        Value::list([Value::T, Value::T, Value::Nil])
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
fn define_derived_mode_is_a_command_unless_interactive_is_disabled() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (define-derived-mode sample-command-mode fundamental-mode "Command")
                 (define-derived-mode sample-function-mode fundamental-mode "Function"
                   :interactive nil)
                 (list (commandp 'sample-command-mode)
                       (commandp 'sample-function-mode)))"#
        ),
        Value::list([Value::T, Value::Nil])
    );
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
fn face_alias_predicates_and_fringe_bitmap_registry_load() {
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
            Value::Symbol("sample-bitmap".into()),
        ])
    );
}

#[test]
fn loaded_with_current_buffer_window_macro_owns_its_display_lifecycle() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defmacro with-current-buffer-window
                     (_buffer action _quit &rest body)
                   `(list :macro
                          ,action
                          (progn ,@body)))
                 (with-current-buffer-window
                     "*sample-output*"
                     (progn (setq sample-display-action :evaluated)
                            :action)
                     nil
                   :body))"#
        ),
        Value::list([
            Value::Symbol(":macro".into()),
            Value::Symbol(":action".into()),
            Value::Symbol(":body".into()),
        ])
    );
}

#[test]
fn dumped_with_current_buffer_window_runs_setup_body_display_and_quit() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"(let ((original (current-buffer))
                     (target-name "*sample-current-buffer-window*")
                     phases)
                 (let ((result
                        (with-current-buffer-window
                            target-name
                            (progn (push :action-evaluated phases) nil)
                            (lambda (window value)
                              (list (windowp window)
                                    value
                                    (not (null
                                          (get-buffer-window target-name)))))
                          (push :body phases)
                          (list (eq standard-output (current-buffer))
                                (buffer-name)))))
                   (list result
                         (eq (current-buffer) original)
                         (nreverse phases))))"#
        ),
        Value::list([
            Value::list([
                Value::T,
                Value::list([
                    Value::T,
                    Value::String("*sample-current-buffer-window*".into()),
                ]),
                Value::T,
            ]),
            Value::T,
            Value::list([
                Value::Symbol(":action-evaluated".into()),
                Value::Symbol(":body".into()),
            ]),
        ])
    );
}

#[test]
fn dumped_simple_completion_policies_exist_before_minibuffer_display() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    assert_eq!(
        eval_str_with(
            &mut interp,
            "(list completion-auto-wrap
                   completion-auto-select
                   completion-show-help
                   (not (null
                         (custom-variable-p 'completion-auto-select))))"
        ),
        Value::list([Value::T, Value::Nil, Value::T, Value::T])
    );
}
