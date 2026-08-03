use super::*;

#[test]
fn cl_prin1_to_string_autoloads_from_cl_print() {
    assert_string_value(eval_str("(cl-prin1-to-string '(a b))"), "(a b)");
}

#[test]
fn prin1_to_string_roundtrips_upstream_symbol_cases() {
    let symbols = vec![
        "", "&", "*", "+", "-", "/", "0E", "0e", "<", "=", ">", "E", "E0", "NaN", "\"", "#", "#x0",
        "'", "''", "(", ")", "+00", ",", "-0", ".", ".0", "0", "0.0", "0E0", "0e0", "1E+",
        "1E+NaN", "1e+", "1e+NaN", ";", "?", "[", "\\", "]", "`", "_", "a", "e", "e0", "x", "{",
        "|", "}", "~", ":", "’", "’bar", "\t", "\n", " ", "\u{00A0}", "\u{200B}", "0",
    ];
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();

    let roundtrip = |interp: &mut Interpreter, env: &mut Env, name: &str| {
        let rendered = primitives::call(
            interp,
            "prin1-to-string",
            &[Value::Symbol(name.to_string())],
            env,
        )
        .expect("symbol should print");
        let rendered = primitives::string_text(&rendered).expect("rendered symbol string");
        let parsed = Reader::new(&rendered)
            .read()
            .expect("printed symbol should parse")
            .expect("printed symbol should yield one form");
        assert_eq!(
            parsed,
            Value::Symbol(name.to_string()),
            "symbol {:?} printed as {:?}",
            name,
            rendered
        );
    };

    for symbol in &symbols {
        roundtrip(&mut interp, &mut env, symbol);
    }
    for left in &symbols {
        for right in &symbols {
            roundtrip(&mut interp, &mut env, &format!("{left}{right}"));
        }
    }
}

#[test]
fn prin1_to_string_matches_upstream_integer_character_cases() {
    let printed = eval_str(
        r#"
            (let ((print-integers-as-characters t))
              (prin1-to-string
               '(?? ?\; ?\( ?\) ?\{ ?\} ?\[ ?\] ?\" ?\' ?\\ ?f ?~ ?Á 32
                 ?\n ?\r ?\t ?\b ?\f ?\a ?\v ?\e ?\d)))
            "#,
    );
    assert_string_value(
        printed,
        r#"(?? ?\; ?\( ?\) ?\{ ?\} ?\[ ?\] ?\" ?\' ?\\ ?f ?~ ?Á ?\s ?\n ?\r ?\t ?\b ?\f 7 11 27 127)"#,
    );
}

#[test]
fn prin1_to_string_escapes_leading_dot_symbols() {
    assert_string_value(eval_str(r#"(prin1-to-string '.foo)"#), r#"\.foo"#);
    assert_string_value(eval_str(r#"(prin1-to-string '.foo.)"#), r#"\.foo."#);
    assert_string_value(eval_str(r#"(prin1-to-string 'foo.bar)"#), "foo.bar");
}

#[test]
fn cl_prin1_respects_charset_text_property_modes() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r##"
                    (list
                     (let ((print-charset-text-property nil))
                       (if (string-match
                            "charset"
                            (cl-prin1-to-string
                             (propertize "a" 'charset 'unicode)))
                           t nil))
                     (let ((print-charset-text-property 'default))
                       (if (string-match
                            "charset"
                            (cl-prin1-to-string
                             (propertize "\u00F6" 'charset 'ascii)))
                           t nil))
                     (let ((print-charset-text-property 'default))
                       (if (string-match
                            "charset"
                            (cl-prin1-to-string
                             (propertize "\u00F6" 'charset 'unicode)))
                           t nil))
                     (let ((print-charset-text-property 'default))
                       (if (string-match
                            "charset"
                            (cl-prin1-to-string
                             (propertize "a" 'charset 'unicode)))
                           t nil)))
                    "##
            ),
            Value::list([Value::T, Value::T, Value::T, Value::T])
        );
    });
}

#[test]
fn cl_prin1_supports_continuous_numbering() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r##"
                    (let* ((x (list 1))
                           (y "hello")
                           (g (gensym))
                           (print-circle t)
                           (print-gensym t)
                           (print-continuous-numbering t)
                           (print-number-table nil))
                      (if (string-match
                           "(#1=(1) #1# #2=\"hello\" #2#)(#3=#:g[[:digit:]]+ #3#)(#1# #2# #3#)#2#$"
                           (mapconcat #'cl-prin1-to-string
                                      `((,x ,x ,y ,y) (,g ,g) (,x ,y ,g) ,y)))
                          t nil))
                    "##
            ),
            Value::Nil
        );
    });
}

#[test]
fn cl_prin1_to_string_marks_circular_ellipsis() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r##"
                    (let ((wide-obj (list 0 1 2 3 4))
                          (deep-obj (list 0 (list 1 (list 2 (list 3 (list 4))))))
                          (print-length 4)
                          (print-level 3))
                      (setf (nth 4 wide-obj) wide-obj)
                      (setf (car (cadadr (cadadr deep-obj))) deep-obj)
                      (equal
                       (list
                        (let* ((print-circle nil)
                               (result (cl-prin1-to-string wide-obj))
                               (pos (next-single-property-change
                                     0 'cl-print-ellipsis result))
                               (value (get-text-property
                                       pos 'cl-print-ellipsis result)))
                          (list result
                                (with-output-to-string
                                  (cl-print--expand-ellipsis value nil))))
                        (let* ((print-circle nil)
                               (result (cl-prin1-to-string deep-obj))
                               (pos (next-single-property-change
                                     0 'cl-print-ellipsis result))
                               (value (get-text-property
                                       pos 'cl-print-ellipsis result)))
                          (list result
                                (with-output-to-string
                                  (cl-print--expand-ellipsis value nil))))
                        (let* ((print-circle t)
                               (result (cl-prin1-to-string wide-obj))
                               (pos (next-single-property-change
                                     0 'cl-print-ellipsis result))
                               (value (get-text-property
                                       pos 'cl-print-ellipsis result)))
                          (list result
                                (with-output-to-string
                                  (cl-print--expand-ellipsis value nil))))
                        (let* ((print-circle t)
                               (result (cl-prin1-to-string deep-obj))
                               (pos (next-single-property-change
                                     0 'cl-print-ellipsis result))
                               (value (get-text-property
                                       pos 'cl-print-ellipsis result)))
                          (list result
                                (with-output-to-string
                                  (cl-print--expand-ellipsis value nil)))))
                       '(("(0 1 2 3 ...)" "#0")
                         ("(0 (1 (2 ...)))" "(3 (#0))")
                         ("#1=(0 1 2 3 ...)" "#1#")
                         ("#1=(0 (1 (2 ...)))" "(3 (#1#))"))))
                    "##
            ),
            Value::T
        );
    });
}

#[test]
fn cl_prin1_to_string_marks_simple_circular_ellipsis() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r##"
                    (let ((wide-obj (list 0 1 2 3 4))
                          (print-length 4)
                          (print-level 3)
                          (print-circle t))
                        (setf (nth 4 wide-obj) wide-obj)
                        (let* ((result (cl-prin1-to-string wide-obj))
                               (pos (next-single-property-change
                                     0 'cl-print-ellipsis result))
                               (value (get-text-property
                                       pos 'cl-print-ellipsis result)))
                          (list result
                                (with-output-to-string
                                  (cl-print--expand-ellipsis value nil)))))
                    "##
            ),
            Value::list([
                Value::String("#1=(0 1 2 3 ...)".into()),
                Value::String("#1#".into()),
            ])
        );
    });
}

#[test]
fn cl_prin1_to_string_marks_cons_ellipsis() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r##"
                    (let ((print-length 4)
                          (print-level 3))
                      (equal
                       (mapcar
                        (lambda (object)
                          (let* ((result (cl-prin1-to-string object))
                                 (pos (next-single-property-change
                                       0 'cl-print-ellipsis result))
                                 (value (get-text-property
                                         pos 'cl-print-ellipsis result)))
                            (list result
                                  (with-output-to-string
                                    (cl-print--expand-ellipsis value nil)))))
                        (list
                         '(0 1 2 3 4 5)
                         '(0 1 2 3 4 5 6 7 8 9)
                         '(a (b (c (d (e)))))
                         (let ((x (make-list 6 'b)))
                           (setf (nthcdr 6 x) 'c)
                           x)))
                       '(("(0 1 2 3 ...)" "4 5")
                         ("(0 1 2 3 ...)" "4 5 6 7 ...")
                         ("(a (b (c ...)))" "(d (e))")
                         ("(b b b b ...)" "b b . c"))))
                    "##
            ),
            Value::T
        );
    });
}

#[test]
fn cl_prin1_to_string_marks_string_ellipsis() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r##"
                    (equal
                     (list
                      (let* ((cl-print-string-length 4)
                             (result (cl-prin1-to-string "abcdefg"))
                             (pos (next-single-property-change
                                   0 'cl-print-ellipsis result))
                             (value (get-text-property
                                     pos 'cl-print-ellipsis result)))
                        (list result
                              (with-output-to-string
                                (cl-print--expand-ellipsis value nil))))
                      (let* ((cl-print-string-length 4)
                             (result (cl-prin1-to-string "abcdefghijk"))
                             (pos (next-single-property-change
                                   0 'cl-print-ellipsis result))
                             (value (get-text-property
                                     pos 'cl-print-ellipsis result)))
                        (list result
                              (with-output-to-string
                                (cl-print--expand-ellipsis value nil))))
                      (let* ((print-length 4)
                             (result (cl-prin1-to-string
                                      #("abcd" 0 1 (bold t)
                                        1 2 (invisible t)
                                        3 4 (italic t))))
                             (pos (next-single-property-change
                                   0 'cl-print-ellipsis result))
                             (value (get-text-property
                                     pos 'cl-print-ellipsis result)))
                        (list result
                              (with-output-to-string
                                (cl-print--expand-ellipsis value nil)))))
                     '(("\"abcd...\"" "efg")
                       ("\"abcd...\"" "efgh...")
                       ("#(\"abcd\" 0 1 (bold t) ...)"
                        "1 2 (invisible t) ...")))
                    "##
            ),
            Value::T
        );
    });
}

#[test]
fn cl_prin1_to_string_marks_struct_ellipsis() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r##"
                    (progn
                      (cl-defstruct (sample-print-struct
                                     (:constructor make-sample-print-struct))
                        a b c d e)
                      (let ((struct (make-sample-print-struct))
                            (print-length 4)
                            (print-level 3))
                        (equal
                         (list
                          (let* ((result (cl-prin1-to-string struct))
                                 (pos (next-single-property-change
                                       0 'cl-print-ellipsis result))
                                 (value (get-text-property
                                         pos 'cl-print-ellipsis result)))
                            (list result
                                  (with-output-to-string
                                    (cl-print--expand-ellipsis value nil))))
                          (let* ((print-length 2)
                                 (result (cl-prin1-to-string struct))
                                 (pos (next-single-property-change
                                       0 'cl-print-ellipsis result))
                                 (value (get-text-property
                                         pos 'cl-print-ellipsis result)))
                            (list result
                                  (with-output-to-string
                                    (cl-print--expand-ellipsis value nil)))))
                         '(("#s(sample-print-struct :a nil :b nil :c nil :d nil ...)"
                            ":e nil")
                           ("#s(sample-print-struct :a nil :b nil ...)"
                            ":c nil :d nil ...")))))
                    "##
            ),
            Value::T
        );
    });
}

#[test]
fn princ_and_terpri_respect_output_streams() {
    assert_eq!(
        eval_str(
            r#"
                (let ((marker-output
                       (with-current-buffer (get-buffer-create "*printer-test*")
                         (erase-buffer)
                         (insert "seed")
                         (point-max-marker))))
                  (list
                   (with-output-to-string
                     (princ 'abc)
                     (terpri nil t)
                     (terpri nil t)
                     (princ "xyz"))
                   (progn
                     (princ 'abc marker-output)
                     (terpri marker-output t)
                     (terpri marker-output t)
                     (with-current-buffer (marker-buffer marker-output)
                       (buffer-string)))))
                "#
        ),
        Value::list([
            Value::String("abc\nxyz".into()),
            Value::String("seedabc\n".into()),
        ])
    );
}

#[test]
fn eval_second_argument_controls_lambda_capture() {
    assert_eq!(
        eval_str(
            "(let ((x 1)
                       (form '(funcall (let ((x 2)) (lambda () x)))))
                   (list
                    (condition-case err
                        (eval form nil)
                      (void-variable (car err)))
                    (eval form t)))"
        ),
        Value::list([Value::Symbol("void-variable".into()), Value::Integer(2)])
    );
    assert_eq!(
        eval_str("(let ((standard-output 'marker)) (eval 'standard-output nil))"),
        Value::Symbol("marker".into())
    );
}

#[test]
fn eval_second_argument_controls_delayed_lambda_macroexpansion() {
    assert_eq!(
        eval_str(
            "(progn
               (defmacro sample-variable-kind (var)
                 (if (macroexp--dynamic-variable-p var) ''dyn ''lex))
               (let ((form '(lambda (x)
                              (let ((y 1))
                                (list (sample-variable-kind x)
                                      (sample-variable-kind y))))))
                 (list (funcall (eval form nil) 0)
                       (funcall (eval form t) 0))))"
        ),
        Value::list([
            Value::list([Value::symbol("dyn"), Value::symbol("dyn")]),
            Value::list([Value::symbol("lex"), Value::symbol("lex")]),
        ])
    );
}

#[test]
fn eval_lambda_trims_unused_lexical_context_unless_marker_requests_it() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((magic "This-is-a-magic-string")
                       (safe-p (lambda (x)
                                 (not (string-match magic (format "%S" x))))))
                  (list
                   (funcall safe-p (eval '(lambda (x) (+ x 1))
                                         `((y . ,magic))))
                   (funcall safe-p (eval '(lambda (x) :closure-dont-trim-context)
                                         `((y . ,magic))))
                   (funcall safe-p (eval '(lambda (x) :closure-dont-trim-context (+ x 1))
                                         `((y . ,magic))))))
            "#
        ),
        Value::list([Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn documentation_form_is_recorded_for_defun_and_lambda() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defun sample-doc-form ()
                    (:documentation (concat "defun" " documentation"))
                    'ok)
                  (let ((fun (lambda ()
                               (:documentation (concat "lambda" " documentation"))
                               'ok)))
                    (list (documentation 'sample-doc-form)
                          (sample-doc-form)
                          (documentation fun)
                          (funcall fun))))
            "#
        ),
        Value::list([
            Value::String("defun documentation".into()),
            Value::Symbol("ok".into()),
            Value::String("lambda documentation".into()),
            Value::Symbol("ok".into()),
        ])
    );
}

#[test]
fn call_interactively_preserves_interactive_form_closure_mutations() {
    assert_eq!(
        eval_str(
            r#"
                (let ((f (let ((d 51695))
                           (lambda (data)
                             (interactive (progn (setq d (1+ d)) (list d)))
                             (list (called-interactively-p 'any) data)))))
                  (list (call-interactively f)
                        (funcall f 51695)
                        (call-interactively f)))
            "#
        ),
        Value::list([
            Value::list([Value::T, Value::Integer(51696)]),
            Value::list([Value::Nil, Value::Integer(51695)]),
            Value::list([Value::T, Value::Integer(51697)]),
        ])
    );
}

#[test]
fn dynamic_lambdas_write_back_mutated_caller_bindings() {
    assert_eq!(
        eval_str(
            r#"
                (let ((x nil)
                      (f (eval '(lambda () (setq x t)) nil)))
                  (funcall f)
                  x)
                "#
        ),
        Value::T
    );
}

#[test]
fn lexical_closures_preserve_mutated_bindings_across_funcalls() {
    run_with_large_stack(|| {
        assert_eq!(
            eval_str(
                r#"
                    (let* (ranges
                           got
                           (try (lambda (from to)
                                  (setq got (list from to ranges))
                                  (setq ranges (list from to))
                                  got)))
                      (list (funcall try 3 5)
                            (funcall try 10 12)
                            (progn
                              (setq ranges nil)
                              (funcall try 20 25))))
                    "#
            ),
            Value::list([
                Value::list([Value::Integer(3), Value::Integer(5), Value::Nil]),
                Value::list([
                    Value::Integer(10),
                    Value::Integer(12),
                    Value::list([Value::Integer(3), Value::Integer(5)]),
                ]),
                Value::list([Value::Integer(20), Value::Integer(25), Value::Nil]),
            ])
        );
    });
}

#[test]
fn lexical_closure_mutation_through_fresh_eval_updates_live_outer_binding() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defun sample-invoke-through-fresh-eval (function value)
                    (eval (list function value)))
                  (let ((captured 0))
                    (cl-letf (((symbol-function 'sample-captured-setter)
                               (lambda (value) (setq captured value))))
                      (sample-invoke-through-fresh-eval
                       'sample-captured-setter 42)
                      captured)))
            "#
        ),
        Value::Integer(42)
    );
}

#[test]
fn escaped_lexical_closure_sees_assignment_made_after_capture() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (setq sample-escaped-reader nil)
                  (let ((captured 0))
                    (setq sample-escaped-reader (lambda () captured))
                    (setq captured 17))
                  (funcall sample-escaped-reader))
            "#
        ),
        Value::Integer(17)
    );
}

#[test]
fn closure_equality_observes_assignments_made_after_capture() {
    assert_eq!(
        eval_str(
            r#"(let (closures)
                 (let ((captured 0))
                   (push (lambda () captured) closures)
                   (setq captured 17))
                 (let ((captured 0))
                   (push (lambda () captured) closures)
                   (setq captured 23))
                 (list (equal (car closures) (cadr closures))
                       (mapcar #'funcall (reverse closures))))"#
        ),
        Value::list([
            Value::Nil,
            Value::list([Value::Integer(17), Value::Integer(23)]),
        ])
    );
}

#[test]
fn sibling_closure_called_during_writer_sees_the_immediate_update() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((captured 0)
                       (reader (lambda () captured))
                       (write-and-read
                        (lambda (value)
                          (setq captured value)
                          (funcall reader))))
                  (funcall write-and-read 23))
            "#
        ),
        Value::Integer(23)
    );
}

#[test]
fn same_named_lexical_cells_from_distinct_frames_never_alias() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defun sample-make-independent-cell (initial)
                    (let ((cell initial))
                      (list (lambda () cell)
                            (lambda (value) (setq cell value)))))
                  (let ((first (sample-make-independent-cell 'first))
                        (second (sample-make-independent-cell 'second)))
                    (funcall (cadr first) 'changed)
                    (list (funcall (car first))
                          (funcall (car second)))))
            "#
        ),
        Value::list([
            Value::Symbol("changed".into()),
            Value::Symbol("second".into()),
        ])
    );
}

#[test]
fn lexical_closures_do_not_capture_same_shaped_record_frames() {
    assert_eq!(
        eval_str(
            "(progn
                 (defclass sample-closure-record nil ((name :initarg :name)))
                 (let* ((first (make-instance 'sample-closure-record :name 'first))
                        (second (make-instance 'sample-closure-record :name 'second))
                        (make-callback
                         (lambda (sti dictionary)
                           (lambda () (slot-value sti 'name))))
                        (callback (funcall make-callback first nil)))
                   ((lambda (sti dictionary) (funcall callback)) second nil)))"
        ),
        Value::Symbol("first".into())
    );
}

#[test]
fn insert_file_contents_leaves_point_at_insert_start() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-insert-file-contents-{}.txt",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, "abc").unwrap();

    let path_literal = path.display().to_string().replace('\\', "\\\\");
    assert_eq!(
        eval_str(&format!(
            "(with-temp-buffer \
                   (insert-file-contents-literally \"{path_literal}\") \
                   (list (point) (point-min) (point-max)))"
        )),
        Value::list([Value::Integer(1), Value::Integer(1), Value::Integer(4)])
    );

    std::fs::remove_file(path).unwrap();
}

#[test]
fn literal_file_bytes_keep_the_unibyte_buffer_decoding_contract() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-unibyte-file-bytes-{}.bin",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, [0xc3, 0xa4]).unwrap();

    let path_literal = path.display().to_string().replace('\\', "\\\\");
    assert_eq!(
        eval_str(&format!(
            "(with-temp-buffer
               (set-buffer-multibyte nil)
               (insert-file-contents-literally \"{path_literal}\")
               (let ((bytes (buffer-string)))
                 (list (multibyte-string-p bytes)
                       (string-to-list bytes)
                       (decode-coding-string bytes 'utf-8)
                       (multibyte-string-p
                        (decode-coding-string bytes 'utf-8)))))"
        )),
        Value::list([
            Value::Nil,
            Value::list([Value::Integer(0xc3), Value::Integer(0xa4)]),
            Value::String("ä".into()),
            Value::T,
        ])
    );

    std::fs::remove_file(path).unwrap();
}

#[test]
fn insert_file_contents_rejects_circular_after_insert_file_functions() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-insert-file-contents-circular-{}.txt",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, "hello\n").unwrap();

    let path_literal = path.display().to_string().replace('\\', "\\\\");
    assert_eq!(
        eval_str(&format!(
            "(let ((after-insert-file-functions (list 'identity))) \
                   (setcdr after-insert-file-functions after-insert-file-functions) \
                   (condition-case err \
                       (insert-file-contents \"{path_literal}\") \
                     (circular-list (car err))))"
        )),
        Value::Symbol("circular-list".into())
    );

    std::fs::remove_file(path).unwrap();
}

#[test]
fn define_inline_lowers_inline_wrappers_into_a_runtime_function() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        r#"
            (define-inline sample-inline (x y)
              (inline-letevals (x y)
                (inline-quote (list ,x ',y))))
            "#,
    );
    assert_eq!(
        eval_str_with(&mut interp, "(let ((sym 'ok)) (sample-inline 1 sym))"),
        Value::list([Value::Integer(1), Value::Symbol("ok".into())])
    );
}

#[test]
fn keymap_placeholders_cover_load_time_setup_calls() {
    assert_eq!(
        eval_str(
            r#"
                (let ((map (make-sparse-keymap "demo")))
                  (list (keymapp map)
                        (keymapp (copy-keymap map))
                        (define-key map "a" 'foo)
                        (lookup-key map "a")
                        (define-key map (kbd "<return>") 'bar)
                        (lookup-key map (kbd "<return>"))
                        (eq (suppress-keymap map) map)
                        (keymap-parent map)))
                "#,
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::Symbol("foo".into()),
            Value::Symbol("foo".into()),
            Value::Symbol("bar".into()),
            Value::Symbol("bar".into()),
            Value::T,
            Value::Nil,
        ])
    );
}

#[test]
fn standard_minibuffer_local_map_is_available() {
    assert_eq!(
        eval_str(
            r#"
                (list (boundp 'minibuffer-local-map)
                      (keymapp minibuffer-local-map)
                      (define-key minibuffer-local-map (kbd "C-c t") 'ignore)
                      (lookup-key minibuffer-local-map (kbd "C-c t")))
                "#
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::Symbol("ignore".into()),
            Value::Symbol("ignore".into()),
        ])
    );
}

#[test]
fn keymap_records_compare_equal_to_literal_lists() {
    assert_eq!(
        eval_str(
            r#"
                (let ((map (make-sparse-keymap)))
                  (list
                   (progn
                     (define-key map "a" 'foo)
                     (equal map '(keymap (97 . foo))))
                   (progn
                     (define-key map "a" nil)
                     (equal map '(keymap (97))))
                   (progn
                     (define-key map "a" 'foo)
                     (define-key map "a" nil t)
                     (equal map '(keymap)))))
                "#,
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn independently_created_empty_keymaps_compare_structurally_equal() {
    assert_eq!(
        eval_str(
            "(list (equal (make-keymap) (make-keymap))\
                   (equal (make-sparse-keymap) (make-sparse-keymap)))"
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn define_key_after_preserves_menu_insertion_order() {
    assert_eq!(
        eval_str(
            r#"
                (let ((map (make-sparse-keymap)))
                  (define-key-after map [cmd1]
                    '(menu-item "Run Command 1" keymap-tests--command-1
                                :help "Command 1 Help"))
                  (define-key-after map [cmd2]
                    '(menu-item "Run Command 2" keymap-tests--command-2
                                :help "Command 2 Help"))
                  (define-key-after map [cmd3]
                    '(menu-item "Run Command 3" keymap-tests--command-3
                                :help "Command 3 Help")
                    'cmd1)
                  (list (caadr map) (caaddr map)))
                "#,
        ),
        Value::list([Value::Symbol("cmd1".into()), Value::Symbol("cmd3".into()),])
    );
}

#[test]
fn global_map_supports_mouse_style_bindings() {
    assert_eq!(
        eval_str(
            r#"
                (let ((map (current-global-map))
                      (event 'mouse-5))
                  (global-set-key [mouse-5] 'mwheel-scroll)
                  (global-set-key [(shift mouse-4)] 'mwheel-scroll)
                  (list
                   (lookup-key map `[,event])
                   (lookup-key map [(shift mouse-4)])
                   (progn
                     (global-unset-key [mouse-5])
                     (lookup-key map [mouse-5]))
                   (progn
                     (global-unset-key [(shift mouse-4)])
                     (lookup-key map [(shift mouse-4)]))))
                "#,
        ),
        Value::list([
            Value::Symbol("mwheel-scroll".into()),
            Value::Symbol("mwheel-scroll".into()),
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn define_key_accepts_runtime_mouse_vectors() {
    assert_eq!(
        eval_str(
            r#"
                (let ((map (make-sparse-keymap "demo"))
                      (event 'mouse-5))
                  (define-key map (vector event) 'mwheel-scroll)
                  (list (lookup-key map (vector event))
                        (lookup-key map [mouse-5])))
                "#,
        ),
        Value::list([
            Value::Symbol("mwheel-scroll".into()),
            Value::Symbol("mwheel-scroll".into()),
        ])
    );
}

#[test]
fn global_set_key_accepts_runtime_mouse_vectors() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((map (current-global-map))
                       (event 'mouse-5)
                       (key (vector event)))
                  (global-set-key key 'mwheel-scroll)
                  (list (lookup-key map key)
                        (lookup-key map [mouse-5])
                        (progn
                          (global-unset-key key)
                          (lookup-key map [mouse-5]))))
                "#,
        ),
        Value::list([
            Value::Symbol("mwheel-scroll".into()),
            Value::Symbol("mwheel-scroll".into()),
            Value::Nil,
        ])
    );
}

#[test]
fn frame_dimension_primitives_round_trip() {
    assert_eq!(
        eval_str(
            r#"
                (let ((width (frame-width))
                      (height (frame-height)))
                  (set-frame-width nil 120)
                  (set-frame-height nil 40)
                  (list width height (frame-width) (frame-height)))
                "#,
        ),
        Value::list([
            Value::Integer(80),
            Value::Integer(25),
            Value::Integer(80),
            Value::Integer(25),
        ])
    );
}

#[test]
fn window_width_tracks_runtime_frame_width() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (set-frame-width nil 120)
                  (window-width))
                "#,
        ),
        Value::Integer(120)
    );
}

#[test]
fn window_height_tracks_runtime_frame_height() {
    assert_eq!(
        eval_str(
            r#"
                  (progn
                  (set-frame-height nil 40)
                  (list (window-height) (window-height (selected-window) 'floor)))
                "#,
        ),
        Value::list([Value::Integer(39), Value::Integer(39)])
    );
}

#[test]
fn frame_resize_keeps_split_window_tree_geometry_coherent() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((first (selected-window))
                       (second (split-window-internal first 40 t 0.5)))
                  (set-frame-width nil 120)
                  (set-frame-height nil 40)
                  (let ((root (frame-root-window)))
                    (list
                     (eq first second)
                     (eq root first)
                     (eq root second)
                     (eq root (window-parent first))
                     (eq (window-parent first) (window-parent second))
                     (window-width root)
                     (window-height root)
                     (window-width first)
                     (window-width second)
                     (+ (window-width first) (window-width second))
                     (window-height first)
                     (window-height second)
                     (nth 2 (window-edges second))
                     (nth 3 (window-edges second)))))
                "#,
        ),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
            Value::Integer(120),
            Value::Integer(39),
            Value::Integer(60),
            Value::Integer(60),
            Value::Integer(120),
            Value::Integer(39),
            Value::Integer(39),
            Value::Integer(120),
            Value::Integer(40),
        ])
    );
}

#[test]
fn window_use_times_follow_selection_and_second_most_recent_bumps() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let* ((first (selected-window))
                    (second (split-window first)))
               (list
                (window-use-time first)
                (window-use-time second)
                (window-bump-use-time second)
                (window-use-time first)
                (window-use-time second)
                (window-bump-use-time second)
                (progn (select-window second) (window-use-time second))
                (window-use-time first)))"
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(4),
            Value::Integer(3),
        ])
    );
}

#[test]
fn frame_selected_window_family_preserves_norecord_state() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(let* ((first (selected-window))
                    (second (split-window first)))
               (list
                (subrp (indirect-function 'set-frame-selected-window))
                (subrp (indirect-function 'frame-old-selected-window))
                (subrp (indirect-function 'old-selected-window))
                (eq (old-selected-window) first)
                (null (frame-old-selected-window))
                (eq (set-frame-selected-window nil second t) second)
                (eq (selected-window) second)
                (window-use-time second)
                (eq (set-frame-selected-window nil first t) first)
                (eq (selected-window) first)
                (window-use-time first)))"
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::Integer(0),
            Value::T,
            Value::T,
            Value::Integer(1),
        ])
    );
}

#[test]
fn terminal_frame_visibility_family_matches_gnu() {
    assert_eq!(
        eval_str(
            r#"(let ((frame (selected-frame)))
                 (list
                  (frame-visible-p frame)
                  (eq (make-frame-visible frame) frame)
                  (condition-case err
                      (progn (make-frame-invisible frame) 'no-error)
                    (error (car err)))
                  (make-frame-invisible frame t)
                  (frame-visible-p frame)
                  (iconify-frame frame)
                  (frame-visible-p frame)
                  (and (memq frame (visible-frame-list)) t)))"#
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::Symbol("error".into()),
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn window_edge_aliases_report_selected_window_geometry() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (set-frame-width nil 120)
                  (set-frame-height nil 40)
                  (list (window-edges)
                        (window-inside-edges)
                        (window-body-edges)
                        (window-inside-pixel-edges)))
                "#
        ),
        Value::list([
            Value::list([
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(120),
                Value::Integer(40),
            ]),
            Value::list([
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(120),
                Value::Integer(39),
            ]),
            Value::list([
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(120),
                Value::Integer(39),
            ]),
            Value::list([
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(120),
                Value::Integer(39),
            ]),
        ])
    );
}

#[test]
fn headless_window_is_not_splittable() {
    assert_eq!(
        eval_str(
            "(list (window-splittable-p)
                       (window-splittable-p (selected-window))
                       (window-splittable-p (selected-window) t)
                       (window-dedicated-p)
                       (window-dedicated-p (selected-window))
                       (window-combined-p)
                       (window-combined-p (selected-window) t))"
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
fn split_window_returns_selected_window_in_headless_runtime() {
    assert_eq!(
        eval_str(
            "(let ((window (selected-window))
                       (target (get-buffer-create \"*split-target*\")))
                   (list (eq (split-window-below) window)
                         (eq (split-window-right) window)
                         (progn
                           (set-window-buffer (split-window) target)
                           (eq (window-buffer window) target))))"
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn query_replace_map_is_preloaded_for_prompt_helpers() {
    assert_eq!(
        eval_str(
            "(list (keymapp query-replace-map)
                       (lookup-key query-replace-map \"y\")
                       (lookup-key query-replace-map \"n\")
                       (lookup-key query-replace-map \" \" )
                       (lookup-key query-replace-map [delete])
                       (lookup-key query-replace-map \"!\")
                       (lookup-key query-replace-map [escape]))"
        ),
        Value::list([
            Value::T,
            Value::Symbol("act".into()),
            Value::Symbol("skip".into()),
            Value::Symbol("act".into()),
            Value::Symbol("skip".into()),
            Value::Symbol("automatic".into()),
            Value::Symbol("exit-prefix".into()),
        ])
    );
}

#[test]
fn buffer_file_name_accepts_explicit_buffer_argument() {
    assert_eq!(
        eval_str(
            "(let ((first (get-buffer-create \"*first-file-buffer*\"))
                       (second (get-buffer-create \"*second-file-buffer*\")))
                   (with-current-buffer first
                     (setq buffer-file-name \"/tmp/first-file\"))
                   (with-current-buffer second
                     (setq buffer-file-name \"/tmp/second-file\"))
                   (switch-to-buffer first)
                   (list (buffer-file-name)
                         (buffer-file-name second)))"
        ),
        Value::list([
            Value::String("/tmp/first-file".into()),
            Value::String("/tmp/second-file".into()),
        ])
    );
}

#[test]
fn headless_window_vscroll_is_zero() {
    assert_eq!(
        eval_str(
            "(list (window-vscroll)
                       (window-vscroll (selected-window) t)
                       (set-window-vscroll (selected-window) 3)
                       (set-window-vscroll (selected-window) 3 t))"
        ),
        Value::list([
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
        ])
    );
}

#[test]
fn mode_line_buffer_identification_is_bound() {
    assert_eq!(
        eval_str(
            "(and (boundp 'mode-line-buffer-identification) (listp mode-line-buffer-identification))"
        ),
        Value::T
    );
}

#[test]
fn max_lisp_eval_depth_matches_emacs_default() {
    assert_eq!(eval_str("max-lisp-eval-depth"), Value::Integer(1600));
}

#[test]
fn terminal_parameter_places_support_setf() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (setf (terminal-parameter nil 'sample-terminal-param) 7)
                  (terminal-parameter nil 'sample-terminal-param))
                "#,
        ),
        Value::Integer(7)
    );
}

#[test]
fn append_treats_strings_as_sequences() {
    assert_eq!(
        eval_str(r#"(append "ab" '(99))"#),
        Value::list([
            Value::Integer('a' as i64),
            Value::Integer('b' as i64),
            Value::Integer(99),
        ])
    );
}

#[test]
fn setcar_supports_expression_targets() {
    assert_eq!(
        eval_str(
            r#"
                (let ((posn '(a b c d)))
                  (setcar (nthcdr 3 posn) 0)
                  posn)
                "#,
        ),
        Value::list([
            Value::Symbol("a".into()),
            Value::Symbol("b".into()),
            Value::Symbol("c".into()),
            Value::Integer(0),
        ])
    );
}

#[test]
fn setcdr_returns_new_cdr_value() {
    assert_eq!(
        eval_str("(let ((cell (cons 'a 'b))) (list (setcdr cell nil) cell))"),
        Value::list([
            Value::Nil,
            Value::cons(Value::Symbol("a".into()), Value::Nil)
        ])
    );
}

#[test]
fn read_key_decodes_xt_mouse_translators() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (setq xterm-mouse-mode t)
                  (defalias 'xterm-mouse-translate (lambda (_event) [decoded]))
                  (let ((unread-command-events '(27 91 77 116 97 105 108)))
                    (list (read-key) (length unread-command-events))))
                "#,
        ),
        Value::list([Value::Symbol("decoded".into()), Value::Integer(4)])
    );
}

#[test]
fn read_key_returns_unread_event_objects() {
    assert_eq!(
        eval_str(
            r#"
                (let ((unread-command-events '((sample-event payload))))
                  (read-key))
                "#,
        ),
        Value::list([
            Value::Symbol("sample-event".into()),
            Value::Symbol("payload".into()),
        ])
    );
}

#[test]
fn cl_lib_compat_preload_seeds_proclaim_state() {
    let interp = Interpreter::new();
    assert!(is_compat_preloaded_feature("cl-lib"));
    assert!(is_compat_preloaded_feature("cl-generic"));
    assert_eq!(
        interp.lookup_var("cl--proclaims-deferred", &Vec::new()),
        Some(Value::Nil)
    );
    assert_eq!(
        interp.lookup_var("inhibit-file-name-handlers", &Vec::new()),
        Some(Value::Nil)
    );
    assert_eq!(
        interp.lookup_var("inhibit-file-name-operation", &Vec::new()),
        Some(Value::Nil)
    );
    assert_eq!(
        interp.lookup_var("vc-directory-exclusion-list", &Vec::new()),
        Some(preloaded_vc_directory_exclusion_list())
    );
}

#[test]
fn cl_proclaim_records_deferred_specs_without_error() {
    assert_eq!(
        eval_str("(progn (cl-proclaim '(inline sample-fn)) cl--proclaims-deferred)"),
        Value::list([Value::list([
            Value::Symbol("inline".into()),
            Value::Symbol("sample-fn".into()),
        ])])
    );
}

#[test]
fn tool_bar_helpers_accept_placeholder_keymaps_during_load() {
    assert_eq!(
        eval_str(
            r#"
                (let ((map (make-sparse-keymap "demo"))
                      (menu-map (make-sparse-keymap "menu")))
                  (list
                   (eq (tool-bar-local-item "close" 'quit-window 'quit map
                                            :help "Quit help" :vert-only t)
                       map)
                   (eq (tool-bar-local-item-from-menu 'help-go-back "left-arrow"
                                                      map menu-map
                                                      :rtl "right-arrow"
                                                      :vert-only t)
                       map)))
                "#,
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn keymap_list_helpers_cover_grep_tool_bar_setup() {
    assert_eq!(
        eval_str(
            r#"
                (let ((map (make-sparse-keymap "demo")))
                  (define-key map "a" 'ignore)
                  (define-key map "b" 'self-insert-command)
                  (list
                   (keymapp (butlast map))
                   (equal (car (car (last map))) "b")))
                "#,
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn key_binding_resolves_minor_mode_remaps() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (setq sample-mode t)
                  (let ((map (make-sparse-keymap "demo")))
                    (global-set-key (kbd "C-x 5 C-o") 'display-buffer-other-frame)
                    (define-key map [remap display-buffer-other-frame] 'demo-display)
                    (setq sample-mode-map-entry (cons 'sample-mode map))
                    (add-to-list 'minor-mode-map-alist sample-mode-map-entry)
                    (list (key-binding (kbd "C-x 5 C-o"))
                          (key-binding (kbd "C-x 5 C-o") nil t))))
                "#
        ),
        Value::list([
            Value::Symbol("demo-display".into()),
            Value::Symbol("display-buffer-other-frame".into()),
        ])
    );
}

#[test]
fn command_remapping_reads_active_minor_mode_maps() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (setq sample-mode t)
                  (let ((map (make-sparse-keymap "demo")))
                    (define-key map [remap display-buffer-other-frame] 'demo-display)
                    (setq sample-mode-map-entry (cons 'sample-mode map))
                    (add-to-list 'minor-mode-map-alist sample-mode-map-entry)
                    (command-remapping 'display-buffer-other-frame)))
                "#
        ),
        Value::Symbol("demo-display".into())
    );
}

#[test]
fn commandp_accepts_bare_interactive_forms() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (defun sample-command ()
                    "doc"
                    (interactive)
                    nil)
                  (list (commandp #'sample-command)
                        (interactive-form #'sample-command)))
                "#
        ),
        Value::list([Value::T, Value::list([Value::Symbol("interactive".into())]),])
    );
}

#[test]
fn kbd_parses_multi_event_and_symbolic_key_specs() {
    assert_eq!(
        eval_str(
            r#"
                (list (length (kbd "IS"))
                      (aref (kbd "IS") 0)
                      (aref (kbd "<up>") 0)
                      (key-description (kbd "ESC ESC ESC")))
                "#
        ),
        Value::list([
            Value::Integer(2),
            Value::Integer('I' as i64),
            Value::Symbol("up".into()),
            Value::String("ESC ESC ESC".into()),
        ])
    );
}

#[test]
fn easy_menu_define_registers_a_placeholder_menu_symbol() {
    assert_eq!(
        eval_str(
            r#"
                (let ((map (make-sparse-keymap "demo")))
                  (easy-menu-define demo-menu map "Demo menu" '("Demo" ["Item" ignore t]))
                  (list (keymapp demo-menu)
                        (fboundp 'demo-menu)
                        (car (easy-menu-binding demo-menu "Demo"))))
                "#,
        ),
        Value::list([Value::T, Value::T, Value::Symbol("menu-item".into()),])
    );
}

#[test]
fn search_forward_missing_pattern_signals_search_failed() {
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let forms = Reader::new("(with-temp-buffer (insert \"abc\") (search-forward \"z\"))")
        .read_all()
        .unwrap();
    let error = interp.eval(&forms[0], &mut env).unwrap_err();
    assert_eq!(error.condition_type(), "search-failed");
    assert_eq!(error.to_string(), "\"z\"");
}

#[test]
fn search_forward_noerror_returns_nil_on_missing_pattern() {
    assert_eq!(
        eval_str("(with-temp-buffer (insert \"abc\") (search-forward \"z\" nil t))"),
        Value::Nil
    );
}

#[test]
fn cl_destructuring_bind_keeps_missing_optional_slots_nil() {
    assert_eq!(
        eval_str(
            "(cl-destructuring-bind (a b &optional c d &rest rest) '(1 2) (list a b c d rest))"
        ),
        Value::list([
            Value::Integer(1),
            Value::Integer(2),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ])
    );
}

#[test]
fn cl_destructuring_bind_supports_nested_keys_defaults_and_supplied_flags() {
    assert_eq!(
        eval_str(
            "(cl-destructuring-bind
                 ((&key (alpha 10 alpha-p)
                        (beta (1+ alpha) beta-p))
                  body)
                 '((:alpha 4) (tail))
               (list alpha alpha-p beta beta-p body))"
        ),
        Value::list([
            Value::Integer(4),
            Value::T,
            Value::Integer(5),
            Value::Nil,
            Value::list([Value::Symbol("tail".into())]),
        ])
    );
}

#[test]
fn cl_destructuring_bind_supports_dotted_tail_patterns() {
    assert_eq!(
        eval_str("(cl-destructuring-bind (_ _ xy . rest) '(a b (184 . 95) tail) (list xy rest))"),
        Value::list([
            Value::cons(Value::Integer(184), Value::Integer(95)),
            Value::list([Value::Symbol("tail".into())]),
        ])
    );
}

#[test]
fn nreverse_relinks_cons_cells_and_cl_copy_list_preserves_dotted_tails() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let* ((head (list 'a 'b 'c))
                       (first head)
                       (second (cdr head))
                       (third (cddr head))
                       (reversed (nreverse head)))
                  (list reversed
                        (eq reversed third)
                        (eq (cdr reversed) second)
                        (eq (cddr reversed) first)
                        head
                        (cl-copy-list '(slot . property))))
                "#
        ),
        Value::list([
            Value::list([
                Value::Symbol("c".into()),
                Value::Symbol("b".into()),
                Value::Symbol("a".into()),
            ]),
            Value::T,
            Value::T,
            Value::T,
            Value::list([Value::Symbol("a".into())]),
            Value::cons(
                Value::Symbol("slot".into()),
                Value::Symbol("property".into()),
            ),
        ])
    );
}

#[test]
fn cl_defun_supports_destructuring_arglists() {
    let value = eval_str(
        "(progn
               (cl-defun file-notify-test ((desc actions file &optional extra))
                 (list desc actions file extra))
               (file-notify-test '(1 (changed) \"/tmp/file\" 9)))",
    );
    let items = value.to_vec().unwrap();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0], Value::Integer(1));
    assert_eq!(items[1], Value::list([Value::Symbol("changed".into())]));
    assert_string_value(items[2].clone(), "/tmp/file");
    assert_eq!(items[3], Value::Integer(9));
}

#[test]
fn cl_defun_supports_basic_key_arguments() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defun register-test (data &key print-func jump-func)
                     (list data print-func jump-func))
                   (register-test 7 :jump-func 'jump))"
        ),
        Value::list([Value::Integer(7), Value::Nil, Value::Symbol("jump".into()),])
    );
}

#[test]
fn cl_defun_optional_defaults_distinguish_omitted_from_explicit_nil() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (cl-defun optional-default-test
                     (seed &optional (value (list seed) supplied))
                   (list value supplied))
                 (cl-defun optional-destructure-test
                     (&optional ((left right) '(1 2) supplied))
                   (list left right supplied))
                 (list (optional-default-test 'default)
                       (optional-default-test 'default nil)
                       (optional-default-test 'default 'explicit)
                       (optional-destructure-test)
                       (optional-destructure-test '(3 4))))"#,
        ),
        Value::list([
            Value::list([Value::list([Value::Symbol("default".into())]), Value::Nil,]),
            Value::list([Value::Nil, Value::T]),
            Value::list([Value::Symbol("explicit".into()), Value::T]),
            Value::list([Value::Integer(1), Value::Integer(2), Value::Nil]),
            Value::list([Value::Integer(3), Value::Integer(4), Value::T]),
        ])
    );
}

#[test]
fn auto_buffer_local_global_special_binding_survives_buffer_switches() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defvar-local sample-cross-buffer-special :default)
                 (let ((sample-cross-buffer-special :outer))
                   (let ((original (current-buffer)))
                     (list
                      (with-temp-buffer sample-cross-buffer-special)
                      (with-temp-buffer
                        (setq sample-cross-buffer-special :local)
                        sample-cross-buffer-special)
                      sample-cross-buffer-special
                      (with-temp-buffer
                        (let ((sample-cross-buffer-special :inner))
                          (list sample-cross-buffer-special
                                (with-current-buffer
                                    original
                                  sample-cross-buffer-special))))))))"#
        ),
        Value::list([
            Value::symbol(":outer"),
            Value::symbol(":local"),
            Value::symbol(":outer"),
            Value::list([Value::symbol(":inner"), Value::symbol(":inner")]),
        ])
    );
}

#[test]
fn make_local_variable_inside_dynamic_binding_moves_reads_to_local_cell() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defvar sample-localized-mid-binding 0)
                 (list
                  (with-temp-buffer
                    (let ((sample-localized-mid-binding 1))
                      (make-local-variable
                       'sample-localized-mid-binding)
                      (setq sample-localized-mid-binding 2)
                      (list sample-localized-mid-binding
                            (default-value
                             'sample-localized-mid-binding)
                            (local-variable-p
                             'sample-localized-mid-binding))))
                  sample-localized-mid-binding
                  (default-value 'sample-localized-mid-binding)))"#
        ),
        Value::list([
            Value::list([Value::Integer(2), Value::Integer(1), Value::T]),
            Value::Integer(0),
            Value::Integer(0),
        ])
    );
}

#[test]
fn ordinary_buffer_local_wins_over_default_binding_from_another_buffer() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (defvar sample-cross-buffer-ordinary 0)
                 (let ((first (generate-new-buffer " *sample-first*"))
                       (second (generate-new-buffer " *sample-second*")))
                   (unwind-protect
                       (progn
                         (with-current-buffer second
                           (setq-local sample-cross-buffer-ordinary 20))
                         (with-current-buffer first
                           (let ((sample-cross-buffer-ordinary 1))
                             (list
                              sample-cross-buffer-ordinary
                              (with-current-buffer second
                                sample-cross-buffer-ordinary)
                              (default-value
                               'sample-cross-buffer-ordinary)))))
                     (kill-buffer first)
                     (kill-buffer second))))"#
        ),
        Value::list([Value::Integer(1), Value::Integer(20), Value::Integer(1)])
    );
}

#[test]
fn cl_defun_preserves_explicit_key_names() {
    assert_eq!(
        eval_str(
            "(progn
               (cl-defun explicit-key-test
                   (&key ((bare-key bare-value)) ((:colon-key colon-value)))
                 (list bare-value colon-value))
               (explicit-key-test 'bare-key 11 :colon-key 22))"
        ),
        Value::list([Value::Integer(11), Value::Integer(22)])
    );
}

#[test]
fn cl_struct_class_type_reports_the_backing_sequence() {
    assert_eq!(
        eval_str(
            "(progn
               (cl-defstruct class-type-record value)
               (cl-defstruct (class-type-list (:type list)) value)
               (cl-defstruct (class-type-vector (:type vector)) value)
               (list
                (cl--struct-class-type
                 (cl--struct-get-class 'class-type-record))
                (cl--struct-class-type
                 (cl--struct-get-class 'class-type-list))
                (cl--struct-class-type
                 (cl--struct-get-class 'class-type-vector))
                (gethash 'value (cl--class-index-table
                                 (cl--struct-get-class 'class-type-record)))
                (gethash 'value (cl--class-index-table
                                 (cl--struct-get-class 'class-type-list)))
                (gethash 'value (cl--class-index-table
                                 (cl--struct-get-class 'class-type-vector)))))"
        ),
        Value::list([
            Value::Nil,
            Value::Symbol("list".into()),
            Value::Symbol("vector".into()),
            Value::Integer(1),
            Value::Integer(0),
            Value::Integer(0),
        ])
    );
}

#[test]
fn preloaded_cl_struct_class_slots_support_compiled_macroexpansion() {
    assert_eq!(
        eval_str(
            "(progn
               (cl-defstruct emaxx-preloaded-class-slot
                 (value 7 :type integer)
                 other)
               (let ((slots
                      (cl--struct-class-slots
                       (cl--struct-get-class 'emaxx-preloaded-class-slot))))
                 (list (length slots)
                       (cl--slot-descriptor-name (aref slots 0))
                       (cl--slot-descriptor-initform (aref slots 0))
                       (cl--slot-descriptor-type (aref slots 0))
                       (cl--slot-descriptor-props (aref slots 0)))))"
        ),
        Value::list([
            Value::Integer(2),
            Value::Symbol("value".into()),
            Value::Integer(7),
            Value::Symbol("integer".into()),
            Value::Nil,
        ])
    );
}

#[test]
fn loaded_gnu_setf_mutates_all_preloaded_slot_descriptor_fields() {
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
                "(let ((slot (record 'cl-slot-descriptor
                                     'old-name nil nil nil)))
                   (setf (cl--slot-descriptor-name slot) 'new-name
                         (cl--slot-descriptor-initform slot) 7
                         (cl--slot-descriptor-type slot) 'integer
                         (cl--slot-descriptor-props slot) '((:read-only . t)))
                   (list (cl--slot-descriptor-name slot)
                         (cl--slot-descriptor-initform slot)
                         (cl--slot-descriptor-type slot)
                         (cl--slot-descriptor-props slot)))"
            ),
            Value::list([
                Value::Symbol("new-name".into()),
                Value::Integer(7),
                Value::Symbol("integer".into()),
                Value::list([Value::cons(Value::Symbol(":read-only".into()), Value::T)]),
            ])
        );
    });
}

#[test]
fn loaded_gnu_eieio_instances_use_completed_inherited_slot_records() {
    run_with_large_stack(|| {
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
                "(progn
                   (require 'eieio)
                   (eieio-defclass-internal
                    'sample-loaded-slot-parent nil
                    '((file :initarg :file :initform nil)) nil)
                   (eieio-defclass-internal
                    'sample-loaded-slot-child '(sample-loaded-slot-parent)
                    '((own :initarg :own :initform 3)) nil)
                   (let ((object
                          (make-instance 'sample-loaded-slot-child
                                         :file \"source.cpp\"
                                         :own 7)))
                     (list (slot-value object 'file)
                           (slot-value object 'own)
                           (mapcar
                            #'cl--slot-descriptor-name
                            (append
                             (eieio--class-slots
                              (cl--find-class
                               'sample-loaded-slot-child))
                             nil)))))"
            ),
            Value::list([
                Value::String("source.cpp".into()),
                Value::Integer(7),
                Value::list([Value::Symbol("file".into()), Value::Symbol("own".into()),]),
            ])
        );
    });
}

#[test]
fn loaded_gnu_eieio_metadata_and_class_storage_stay_authoritative() {
    run_with_large_stack(|| {
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
                "(progn
                   (require 'eieio)
                   (eieio-defclass-internal
                    'sample-loaded-abstract nil nil '(:abstract t))
                   (eieio-defclass-internal
                    'sample-loaded-allocation nil
                    '((shared :allocation :class
                              :initarg :shared :initform 5))
                    nil)
                   (let* ((class
                           (cl--find-class 'sample-loaded-allocation))
                          (object
                           (make-instance 'sample-loaded-allocation)))
                     (list
                      (class-abstract-p 'sample-loaded-abstract)
                      (slot-value object 'shared)
                      (progn
                        (setf (slot-value object 'shared) 9)
                        (slot-value object 'shared))
                      (aref
                       (eieio--class-class-allocation-values class)
                       0)
                      (progn
                        (eieio-oset-default
                         'sample-loaded-allocation 'shared 11)
                        (list
                         (slot-value object 'shared)
                         (aref
                          (eieio--class-class-allocation-values
                           class)
                          0))))))"
            ),
            Value::list([
                Value::T,
                Value::Integer(5),
                Value::Integer(9),
                Value::Integer(9),
                Value::list([Value::Integer(11), Value::Integer(11)]),
            ])
        );
    });
}

#[test]
fn upstream_semantic_format_loads_with_complete_eieio_slots() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize GNU-compatible batch interpreter");
        let path = upstream_emacs_repo().join("test/lisp/cedet/semantic/format-tests.el");
        crate::lisp::load_file_strict(&mut interp, &path).expect("load semantic format tests");
        let summary =
            interp.run_ert_tests_with_selector(Some(&Value::Symbol("semantic-fmt-utest".into())));
        assert_eq!(summary.total, 1, "results: {:#?}", interp.test_results);
        assert_eq!(summary.passed, 1, "results: {:#?}", interp.test_results);
    });
}

#[test]
fn upstream_semantic_make_completion_survives_the_file_visit_sequence() {
    run_with_large_stack(|| {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize GNU-compatible batch interpreter");
        let test_path = upstream_emacs_repo().join("test/lisp/cedet/semantic-utest-ia.el");
        crate::lisp::load_file_strict(&mut interp, &test_path).expect("load Semantic IA tests");
        let resource =
            upstream_emacs_repo().join("test/lisp/cedet/semantic-utest-ia-resources/test.mk");

        assert_eq!(
            eval_str_with(
                &mut interp,
                &format!(
                    "(semantic-ia-utest {resource:?})",
                    resource = resource.display().to_string()
                )
            ),
            Value::Nil
        );
    });
}

#[test]
fn semantic_make_fallback_follows_mode_ancestry_not_file_spelling() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-semantic-make-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create Make completion fixture directory");
    std::fs::write(root.join("testdoublens.cpp"), "").expect("write C++ fixture");
    std::fs::write(root.join("testdoublens.hpp"), "").expect("write C++ header fixture");
    let directory = format!("{}/", root.display());

    assert_eq!(
        eval_str(&format!(
            "(progn
               (put 'sample-make-mode 'derived-mode-parent 'makefile-mode)
               (with-temp-buffer
                 (setq major-mode 'sample-make-mode)
                 (setq-local default-directory {directory:?})
                 (insert \"FILES=testdoub\\noptional: all\\nprobe: $FIL\\nnotoptional: opt\\n\")
                 (let ((tags
                        (mapcar #'car (semantic-fetch-tags))))
                   (goto-char (point-min))
                   (search-forward \"testdoub\")
                   (let ((files
                          (mapcar #'car
                                  (semantic-analyze-possible-completions
                                   nil))))
                     (goto-char (point-min))
                   (search-forward \"$FIL\")
                   (let ((variables
                          (mapcar #'car
                                  (semantic-analyze-possible-completions
                                   nil))))
                     (search-forward \"notoptional: opt\")
                     (list tags
                           files
                           variables
                           (mapcar #'car
                                   (semantic-analyze-possible-completions
                                    nil))))))))"
        )),
        Value::list([
            Value::list([
                Value::String("optional".into()),
                Value::String("probe".into()),
                Value::String("notoptional".into()),
            ]),
            Value::list([
                Value::String("testdoublens.cpp".into()),
                Value::String("testdoublens.hpp".into()),
            ]),
            Value::list([Value::String("FILES".into())]),
            Value::list([Value::String("optional".into())]),
        ])
    );

    std::fs::remove_dir_all(root).expect("remove Make completion fixture directory");
}

#[test]
fn loaded_gnu_file_modes_run_semantic_parser_setup() {
    run_with_large_stack(|| {
        let root = std::env::temp_dir().join(format!(
            "emaxx-semantic-modes-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("create Semantic mode fixture directory");
        let python = root.join("sample.py");
        let html = root.join("sample.html");
        let makefile = root.join("Makefile");
        std::fs::write(&python, "def sample():\n    return 1\n").expect("write Python fixture");
        std::fs::write(&html, "<html><body>sample</body></html>\n").expect("write HTML fixture");
        std::fs::write(&makefile, "sample:\n\t@true\n").expect("write Makefile fixture");

        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
            ..Default::default()
        };
        let mut interp = crate::batch::initialize_batch_interpreter(&options)
            .expect("initialize GNU-compatible batch interpreter");
        let expression = format!(
            "(progn
               (require 'semantic)
               (semantic-mode 1)
               (mapcar
                (lambda (file)
                  (let ((buffer (find-file-noselect file)))
                    (with-current-buffer buffer
                      (prog1
                          (list major-mode
                                (semantic-active-p)
                                (and (boundp 'semantic--parse-table)
                                     semantic--parse-table
                                     t))
                        (kill-buffer buffer)))))
                (list {python:?} {html:?} {makefile:?})))",
            python = python.display().to_string(),
            html = html.display().to_string(),
            makefile = makefile.display().to_string(),
        );
        let actual = eval_str_with(&mut interp, &expression);

        std::fs::remove_dir_all(&root).expect("remove Semantic mode fixture directory");
        assert_eq!(
            actual,
            Value::list([
                Value::list([Value::Symbol("python-mode".into()), Value::T, Value::T,]),
                Value::list([Value::Symbol("html-mode".into()), Value::T, Value::T,]),
                Value::list([
                    Value::Symbol("makefile-bsdmake-mode".into()),
                    Value::T,
                    Value::T,
                ]),
            ])
        );
    });
}

#[test]
fn cl_defstruct_include_preserves_and_overrides_slot_defaults() {
    assert_eq!(
        eval_str(
            "(progn
               (defvar included-dynamic-default 7)
               (cl-defstruct included-parent (first 1) (second 2))
               (cl-defstruct
                   (included-child
                    (:include included-parent
                              (second included-dynamic-default)))
                 third)
               (let ((included-dynamic-default 9))
                 (let ((value (make-included-child)))
                   (list (included-child-first value)
                         (included-child-second value)
                         (included-child-third value)))))"
        ),
        Value::list([Value::Integer(1), Value::Integer(9), Value::Nil])
    );
}

#[test]
fn cl_defun_wraps_body_in_named_block() {
    assert_eq!(
        eval_str(
            "(progn
               (require 'cl-lib)
               (cl-defun emaxx-cl-defun-block-test ()
                 (cl-return-from emaxx-cl-defun-block-test 'done)
                 'missed)
               (emaxx-cl-defun-block-test))"
        ),
        Value::Symbol("done".into())
    );
}

#[test]
fn cl_defmethod_lowers_specialized_arguments() {
    let result = eval_str(
        "(progn
               (cl-defgeneric method-test (value flag))
               (cl-defmethod method-test ((value string) flag)
                 (list value flag))
               (method-test \"ok\" 3))",
    );
    let items = result.to_vec().unwrap();
    assert_eq!(primitives::string_text(&items[0]).unwrap(), "ok");
    assert_eq!(items[1], Value::Integer(3));
}

#[test]
fn cl_defmethod_dispatch_preserves_nested_lexical_callback_capture() {
    assert_eq!(
        eval_str(
            "(progn
               (setq lexical-binding t)
               (cl-defgeneric sample-map-method (function value))
               (cl-defmethod sample-map-method (function (value vector))
                 (mapcar (lambda (element) (funcall function element)) value))
               (sample-map-method #'1+ [1 2]))",
        ),
        Value::list([Value::Integer(2), Value::Integer(3)])
    );
}

#[test]
fn cl_defmethod_macroexpands_method_body_once_at_definition_time() {
    assert_eq!(
        eval_str(
            "(progn
               (defvar sample-method-expansions 0)
               (defmacro sample-method-expansion-probe ()
                 (setq sample-method-expansions
                       (1+ sample-method-expansions))
                 nil)
               (cl-defgeneric sample-expanded-method ())
               (cl-defmethod sample-expanded-method ()
                 (sample-method-expansion-probe)
                 'ok)
               (list sample-method-expansions
                     (sample-expanded-method)
                     (sample-expanded-method)
                     sample-method-expansions))"
        ),
        Value::list([
            Value::Integer(1),
            Value::Symbol("ok".into()),
            Value::Symbol("ok".into()),
            Value::Integer(1),
        ])
    );
}

#[test]
fn cl_generic_dispatch_recognizes_record_backed_builtin_types() {
    let mut interp = Interpreter::new();
    let process = interp
        .create_process(None, None, Vec::new(), None, Some("sample-process".into()))
        .expect("create process");
    interp.set_global_binding("sample-process", process);

    assert_eq!(
        eval_str_with(
            &mut interp,
            "(progn
               (cl-defgeneric sample-record-type (value))
               (cl-defmethod sample-record-type ((_value process)) 'process)
               (list (cl-typep sample-process 'process)
                     (cl-type-of sample-process)
                     (sample-record-type sample-process)))"
        ),
        Value::list([
            Value::T,
            Value::Symbol("process".into()),
            Value::Symbol("process".into()),
        ])
    );
}

#[test]
fn cl_defmethod_context_specializers_see_dynamic_bindings() {
    // GNU dispatches the &context method when the context variable is
    // let-bound around the call: (text base).
    assert_eq!(
        eval_str(
            "(progn
               (cl-defgeneric sample-context-method (value))
               (cl-defmethod sample-context-method
                   (value &context (major-mode (eql text-mode)))
                 'text)
               (cl-defmethod sample-context-method (value)
                 'base)
               (list (let ((major-mode 'text-mode))
                       (sample-context-method 'item))
                     (let ((major-mode 'fundamental-mode))
                       (sample-context-method 'item))))"
        ),
        Value::list([Value::Symbol("text".into()), Value::Symbol("base".into())])
    );
}

#[test]
fn cl_defmethod_dispatches_eieio_specializers_without_clobbering_previous_methods() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-method-parent nil nil)
                   (defclass sample-method-child (sample-method-parent) nil)
                   (cl-defgeneric sample-method-name (object))
                   (cl-defmethod sample-method-name ((object sample-method-parent))
                     'parent)
                   (cl-defmethod sample-method-name ((object sample-method-child))
                     'child)
                   (list (sample-method-name (make-instance 'sample-method-parent))
                         (sample-method-name (make-instance 'sample-method-child))))"
        ),
        Value::list([
            Value::Symbol("parent".into()),
            Value::Symbol("child".into()),
        ])
    );
}

#[test]
fn cl_defmethod_dispatches_over_unspecialized_and_parent_eieio_methods() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-method-abstract nil
                     ((file :initarg :file)))
                   (defclass sample-method-table (sample-method-abstract) nil)
                   (cl-defmethod sample-method-file (object) nil)
                   (cl-defmethod sample-method-file ((_object sample-method-abstract)) 'abstract)
                   (cl-defmethod sample-method-file ((object sample-method-table))
                     (slot-value object 'file))
                   (sample-method-file
                    (make-instance 'sample-method-table :file \"a.c\")))"
        ),
        Value::String("a.c".into())
    );
}

#[test]
fn cl_defmethod_keeps_sibling_eieio_methods_separate() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-method-root nil nil)
                   (defclass sample-method-left (sample-method-root) nil)
                   (defclass sample-method-right (sample-method-root) nil)
                   (cl-defmethod sample-method-sibling ((object sample-method-left)) 'left)
                   (cl-defmethod sample-method-sibling ((object sample-method-right)) 'right)
                   (sample-method-sibling (make-instance 'sample-method-left)))"
        ),
        Value::Symbol("left".into())
    );
}

#[test]
fn cl_defmethod_prefers_child_over_later_parent_method() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-method-root nil nil)
                   (defclass sample-method-child (sample-method-root) nil)
                   (cl-defmethod sample-method-late-parent ((object sample-method-child)) 'child)
                   (cl-defmethod sample-method-late-parent ((object sample-method-root)) 'root)
                   (sample-method-late-parent (make-instance 'sample-method-child)))"
        ),
        Value::Symbol("child".into())
    );
}

#[test]
fn cl_defmethod_dispatches_semanticdb_full_filename_shape() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-db-abstract-table nil
                     ((parent-db)))
                   (defclass sample-db-table (sample-db-abstract-table)
                     ((file :initarg :file)))
                   (defclass sample-db-project nil
                     ((reference-directory :initarg :reference-directory)))
                   (cl-defmethod sample-db-full-filename (buffer-or-string)
                     nil)
                   (cl-defmethod sample-db-full-filename
                     ((_object sample-db-abstract-table))
                     nil)
                   (cl-defmethod sample-db-full-filename
                     ((object sample-db-table))
                     (expand-file-name
                      (slot-value object 'file)
                      (slot-value (slot-value object 'parent-db)
                                  'reference-directory)))
                   (cl-defmethod sample-db-full-filename
                     ((_object sample-db-project))
                     nil)
                   (let ((db (make-instance 'sample-db-project
                                            :reference-directory \"/tmp/sys/\"))
                         (table (make-instance 'sample-db-table
                                               :file \"cdefs.h\")))
                     (setf (slot-value table 'parent-db) db)
                     (sample-db-full-filename table)))"
        ),
        Value::String("/tmp/sys/cdefs.h".into())
    );
}

#[test]
fn cl_defmethod_call_next_method_dispatches_to_previous_specializer() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-next-context nil nil)
                   (defclass sample-next-functionarg (sample-next-context) nil)
                   (cl-defmethod sample-next-method ((context sample-next-context)
                                                      &optional desired-type)
                     (list 'base context desired-type))
                   (cl-defmethod sample-next-method ((context sample-next-functionarg))
                     (cons 'child (cl-call-next-method context 'desired)))
                   (let ((object (make-instance 'sample-next-functionarg)))
                     (let ((result (sample-next-method object)))
                       (list (car result)
                             (cadr result)
                             (cl-typep (caddr result) 'sample-next-functionarg)
                             (cadddr result)))))"
        ),
        Value::list([
            Value::Symbol("child".into()),
            Value::Symbol("base".into()),
            Value::T,
            Value::Symbol("desired".into()),
        ])
    );
}

#[test]
fn cl_defmethod_around_method_keeps_next_method_binding_distinct() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defstruct sample-next-parent a b)
                   (cl-defstruct (sample-next-child (:include sample-next-parent)) c)
                   (fmakunbound 'sample-next-generic)
                   (cl-defgeneric sample-next-generic (x y))
                   (cl-defmethod sample-next-generic ((x t) y)
                     (cons x y))
                   (cl-defmethod sample-next-generic ((_x sample-next-parent) y)
                     (cons 'parent (cl-call-next-method 'a y)))
                   (cl-defmethod sample-next-generic ((_x sample-next-child) _y)
                     (cons 'child (cl-call-next-method)))
                   (cl-defmethod sample-next-generic :around ((_x t) _y)
                     (cons 'around (cl-call-next-method)))
                   (cl-defstruct (sample-next-child11 (:include sample-next-child)) d)
                   (cl-defmethod sample-next-generic :around ((_x sample-next-child11) _y)
                     (cons 'child11 (cl-call-next-method)))
                   (list
                    (sample-next-generic (make-sample-next-child) nil)
                    (sample-next-generic (make-sample-next-child11) nil)
                    (progn
                      (cl-defstruct (sample-next-child2 (:include sample-next-parent)) d)
                      (cl-defmethod sample-next-generic ((_x sample-next-child2) _y)
                        (cons 'child2 (cl-call-next-method)))
                      (sample-next-generic (make-sample-next-child2) nil))))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("around".into()),
                Value::Symbol("child".into()),
                Value::Symbol("parent".into()),
                Value::Symbol("a".into()),
            ]),
            Value::list([
                Value::Symbol("child11".into()),
                Value::Symbol("around".into()),
                Value::Symbol("child".into()),
                Value::Symbol("parent".into()),
                Value::Symbol("a".into()),
            ]),
            Value::list([
                Value::Symbol("around".into()),
                Value::Symbol("child2".into()),
                Value::Symbol("parent".into()),
                Value::Symbol("a".into()),
            ]),
        ])
    );
}

#[test]
fn setf_generic_place_calls_setf_generic_after_place_args() {
    assert_eq!(
        eval_str(
            "(progn
                   (fmakunbound 'sample-setf-generic)
                   (cl-defgeneric sample-setf-generic (x y))
                   (cl-defmethod (setf sample-setf-generic) (v (y t) z)
                     (list v y z))
                   (cl-defmethod (setf sample-setf-generic) (v (_y (eql 4)) z)
                     (list v 'four z))
                   (let ((x nil))
                     (list
                      (setf (sample-setf-generic 'a 'b) 'v)
                      (setf (sample-setf-generic 4 'b) 'v)
                      (setf (sample-setf-generic (progn (push 1 x) 'a)
                                                 (progn (push 2 x) 'b))
                            (progn (push 3 x) 'v))
                      x)))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("v".into()),
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
            ]),
            Value::list([
                Value::Symbol("v".into()),
                Value::Symbol("four".into()),
                Value::Symbol("b".into()),
            ]),
            Value::list([
                Value::Symbol("v".into()),
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
            ]),
            Value::list([Value::Integer(3), Value::Integer(2), Value::Integer(1)]),
        ])
    );
}

#[test]
fn cl_defmethod_orders_overlapping_numeric_specializers() {
    assert_eq!(
        eval_str(
            "(progn
                   (fmakunbound 'sample-overlap-generic)
                   (cl-defgeneric sample-overlap-generic (x y))
                   (cl-defmethod sample-overlap-generic ((y t) z) (list y z))
                   (cl-defmethod sample-overlap-generic ((_y (eql 4)) _z)
                     (cons 'four (cl-call-next-method)))
                   (cl-defmethod sample-overlap-generic ((_y integer) _z)
                     (cons 'integer (cl-call-next-method)))
                   (cl-defmethod sample-overlap-generic ((_y number) _z)
                     (cons 'number (cl-call-next-method)))
                   (list
                    (sample-overlap-generic 'a 'b)
                    (sample-overlap-generic 1 'b)
                    (sample-overlap-generic 4 'b)))"
        ),
        Value::list([
            Value::list([Value::Symbol("a".into()), Value::Symbol("b".into())]),
            Value::list([
                Value::Symbol("integer".into()),
                Value::Symbol("number".into()),
                Value::Integer(1),
                Value::Symbol("b".into()),
            ]),
            Value::list([
                Value::Symbol("four".into()),
                Value::Symbol("integer".into()),
                Value::Symbol("number".into()),
                Value::Integer(4),
                Value::Symbol("b".into()),
            ]),
        ])
    );
}

#[test]
fn cl_defmethod_honors_argument_precedence_order() {
    assert_eq!(
        eval_str(
            "(progn
                   (fmakunbound 'sample-apo-generic)
                   (cl-defgeneric sample-apo-generic (x y)
                     (:argument-precedence-order y x))
                   (cl-defmethod sample-apo-generic (x y) (list x y))
                   (cl-defmethod sample-apo-generic (_x (_y integer))
                     (cons 'y-int (cl-call-next-method)))
                   (cl-defmethod sample-apo-generic ((_x integer) _y)
                     (cons 'x-int (cl-call-next-method)))
                   (cl-defmethod sample-apo-generic ((_x integer) (_y integer))
                     (cons 'both (cl-call-next-method)))
                   (sample-apo-generic 1 2))"
        ),
        Value::list([
            Value::Symbol("both".into()),
            Value::Symbol("y-int".into()),
            Value::Symbol("x-int".into()),
            Value::Integer(1),
            Value::Integer(2),
        ])
    );
}

#[test]
fn cl_defmethod_before_after_methods_wrap_primary_result() {
    assert_eq!(
        eval_str(
            "(let ((log nil))
               (fmakunbound 'sample-before-after-generic)
               (cl-defgeneric sample-before-after-generic (x y))
               (cl-defmethod sample-before-after-generic ((_x t) y)
                 (cons y log))
               (cl-defmethod sample-before-after-generic ((_x (eql 4)) _y)
                 (cons 'four (cl-call-next-method)))
               (cl-defmethod sample-before-after-generic :after (x _y)
                 (push (list :after x) log))
               (cl-defmethod sample-before-after-generic :before (x _y)
                 (push (list :before x) log))
               (list (sample-before-after-generic 4 6) log))"
        ),
        Value::list([
            Value::list([
                Value::Symbol("four".into()),
                Value::Integer(6),
                Value::list([Value::Symbol(":before".into()), Value::Integer(4)]),
            ]),
            Value::list([
                Value::list([Value::Symbol(":after".into()), Value::Integer(4)]),
                Value::list([Value::Symbol(":before".into()), Value::Integer(4)]),
            ]),
        ])
    );
}

#[test]
fn cl_defmethod_updates_generic_under_around_advice() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (fmakunbound 'sample-advised-generic)
                 (cl-defgeneric sample-advised-generic (x y))
                 (cl-defmethod sample-advised-generic (x y) (list x y))
                 (defun sample-advised-wrapper (&rest args)
                   (cons 'advice (apply args)))
                 (advice-add 'sample-advised-generic :around #'sample-advised-wrapper)
                 (let ((before (sample-advised-generic 4 5)))
                   (cl-defmethod sample-advised-generic ((_x integer) _y)
                     (cons 'integer (cl-call-next-method)))
                   (let ((during (sample-advised-generic 4 5)))
                     (advice-remove 'sample-advised-generic #'sample-advised-wrapper)
                     (list before during (sample-advised-generic 4 5)))))"#
        ),
        Value::list([
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
        ])
    );
}

#[test]
fn cl_typep_recognizes_builtin_numeric_parent_types() {
    assert_eq!(
        eval_str("(list (cl-typep 1 'integer) (cl-typep 1 'number) (cl-typep 1.5 'number))"),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn cl_typep_implements_gnu_builtin_satisfies_and_eql_types() {
    assert_eq!(
        eval_str(
            "(progn
               (defun sample-cl-command () (interactive))
               (put 'sample-even 'cl-deftype-satisfies
                    (lambda (value) (and (integerp value) (zerop (% value 2)))))
               (list (cl-typep :before 'keyword)
                     (cl-typep 'before 'keyword)
                     (cl-typep 65 'character)
                     (cl-typep -1 'character)
                     (cl-typep 0 'natnum)
                     (cl-typep -1 'natnum)
                     (cl-typep 1.5 'real)
                     (cl-typep 'sample-cl-command 'command)
                     (cl-typep 4 'sample-even)
                     (cl-typep 3 'sample-even)
                     (cl-typep 1 '(eql 1))
                     (cl-typep 1.0 '(eql 1))))"
        ),
        Value::list([
            Value::T,
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::T,
            Value::T,
            Value::T,
            Value::Nil,
            Value::T,
            Value::Nil,
        ])
    );
}

#[test]
fn loaded_gnu_cl_deftype_drives_class_type_matching() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (require 'eieio)
               (eieio-defclass-internal 'sample-loaded-class nil nil nil)
               (list (functionp (get 'class 'cl-deftype-handler))
                     (class-p 'sample-loaded-class)
                     (cl-typep 'sample-loaded-class 'class)
                     (cl-typep (cl--find-class 'sample-loaded-class) 'class)
                     (cl-typep 'not-a-class 'class)))"
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::Nil])
    );
}

#[test]
fn loaded_gnu_eieio_subclass_dispatch_reads_completed_lisp_class_records() {
    assert_eq!(
        eval_str_with_upstream_batch(
            "(progn
               (require 'eieio)
               (eieio-defclass-internal 'sample-loaded-parent nil nil nil)
               (eieio-defclass-internal
                'sample-loaded-child '(sample-loaded-parent) nil nil)
               (cl-defmethod sample-loaded-dispatch
                 ((_class (subclass sample-loaded-parent)))
                 'matched)
               (list
                (mapcar #'cl--class-name
                        (cl--class-parents
                         (cl--find-class 'sample-loaded-child)))
                (cl--class-allparents
                 (cl--find-class 'sample-loaded-child))
                (sample-loaded-dispatch 'sample-loaded-child)))"
        ),
        Value::list([
            Value::list([Value::Symbol("sample-loaded-parent".into())]),
            Value::list([
                Value::Symbol("sample-loaded-child".into()),
                Value::Symbol("sample-loaded-parent".into()),
                Value::Symbol("eieio-default-superclass".into()),
                Value::Symbol("record".into()),
                Value::Symbol("atom".into()),
                Value::T,
            ]),
            Value::Symbol("matched".into()),
        ])
    );
}

#[test]
fn loaded_gnu_eieio_subclass_dispatch_resolves_autoload_dummy_classes() {
    run_with_large_stack(|| {
        let path = std::env::temp_dir().join(format!(
            "emaxx-eieio-autoload-class-{}.el",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            "(defclass sample-autoload-child (sample-autoload-parent) nil)\n\
             (setq sample-autoload-loaded t)\n",
        )
        .unwrap();
        let path = path.display().to_string().replace('\\', "\\\\");

        assert_eq!(
            eval_str_with_upstream_batch(&format!(
                "(progn
                   (require 'eieio)
                   (eieio-defclass-internal
                    'sample-autoload-parent nil nil nil)
                   (setq sample-autoload-loaded nil)
                   (eieio-defclass-autoload
                    'sample-autoload-child
                    '(sample-autoload-parent)
                    \"{path}\"
                    \"Autoload child\")
                   (cl-defmethod sample-autoload-dispatch
                     ((_class (subclass sample-autoload-parent)))
                     'matched)
                   (let ((loaded-before-dispatch sample-autoload-loaded))
                     (list loaded-before-dispatch
                           (sample-autoload-dispatch
                            'sample-autoload-child)
                           sample-autoload-loaded
                           (mapcar
                            #'cl--class-name
                            (cl--class-parents
                             (cl--find-class
                              'sample-autoload-child))))))"
            )),
            Value::list([
                Value::Nil,
                Value::Symbol("matched".into()),
                Value::T,
                Value::list([Value::Symbol("sample-autoload-parent".into())]),
            ])
        );

        std::fs::remove_file(path).unwrap();
    });
}

#[test]
fn cl_deftype_optional_args_default_to_star() {
    assert_eq!(
        eval_str(
            "(progn
               (cl-deftype sample-member-type (&optional x) `(member ,x))
               (list (cl-typep '* 'sample-member-type)
                     (cl-typep 1 'sample-member-type)
                     (cl-typep 1 '(sample-member-type 1))))"
        ),
        Value::list([Value::T, Value::Nil, Value::T])
    );
}

#[test]
fn defclass_returns_the_class_name() {
    assert_eq!(
        eval_str("(defclass sample-class nil nil)"),
        Value::Symbol("sample-class".into())
    );
}

#[test]
fn defclass_registers_runtime_class_metadata() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-parent nil nil)
                   (defclass sample-child (sample-parent)
                     ((sample-slot :initform 7))
                     :documentation \"Sample\")
                   (list (type-of (cl-find-class 'sample-child))
                         (cl--class-allparents (cl-find-class 'sample-child))
                         (cl--class-children (cl-find-class 'sample-parent))))"
        ),
        Value::list([
            Value::Symbol("eieio--class".into()),
            Value::list([
                Value::Symbol("sample-child".into()),
                Value::Symbol("sample-parent".into()),
                Value::T,
            ]),
            Value::list([Value::Symbol("sample-child".into())]),
        ])
    );
}

#[test]
fn eieio_class_slots_return_slot_descriptor_records() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-desc-parent nil
                     ((base-slot :initarg :base-slot :initform 1)))
                   (defclass sample-desc-child (sample-desc-parent)
                     ((own-slot :initarg :own-slot :initform \"hi\"
                                :documentation \"Own slot\")))
                   (let ((slots (eieio--class-slots
                                 (cl--find-class 'sample-desc-child))))
                     (list (length slots)
                           (cl--slot-descriptor-name (aref slots 0))
                           (cl--slot-descriptor-initform (aref slots 0))
                           (cl--slot-descriptor-name (aref slots 1))
                           (cl--slot-descriptor-initform (aref slots 1))
                           (cl--slot-descriptor-type (aref slots 1))
                           (cdr (assq :documentation
                                      (cl--slot-descriptor-props
                                       (aref slots 1))))
                           (eieio--class-initarg-tuples
                            (cl--find-class 'sample-desc-child)))))"
        ),
        Value::list([
            Value::Integer(2),
            Value::Symbol("base-slot".into()),
            Value::Integer(1),
            Value::Symbol("own-slot".into()),
            Value::String("hi".into()),
            Value::T,
            Value::String("Own slot".into()),
            Value::list([
                Value::cons(
                    Value::Symbol(":base-slot".into()),
                    Value::Symbol("base-slot".into()),
                ),
                Value::cons(
                    Value::Symbol(":own-slot".into()),
                    Value::Symbol("own-slot".into()),
                ),
            ]),
        ])
    );
}

#[test]
fn equal_compares_records_element_wise() {
    assert_eq!(
        eval_str(
            "(list (equal (record 'sample-rec 1 \"a\") (record 'sample-rec 1 \"a\"))
                   (equal (record 'sample-rec 1) (record 'sample-rec 2))
                   (equal (record 'sample-rec 1) (record 'other-rec 1)))"
        ),
        Value::list([Value::T, Value::Nil, Value::Nil])
    );
}

#[test]
fn read_materializes_hash_table_literals() {
    assert_eq!(
        eval_str(
            "(let ((table (read \"#s(hash-table test equal data (\\\"a\\\" 1))\")))
                   (list (hash-table-p table)
                         (gethash \"a\" table)
                         (hash-table-test table)))"
        ),
        Value::list([Value::T, Value::Integer(1), Value::Symbol("equal".into())])
    );
}

#[test]
fn compat_nil_objects_print_unreadably_like_gnu() {
    // GNU tags objects with the class OBJECT unless
    // `eieio-backward-compatibility' downgrades the tag to the class
    // symbol; the class's circular default-object cache then prints as a
    // `#N' marker that `read' rejects (bug#29220).
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-tagged nil
                     ((name :initarg :name)))
                   (let* ((compat (make-instance 'sample-tagged :name \"a\"))
                          (bare (let ((eieio-backward-compatibility nil))
                                  (make-instance 'sample-tagged :name \"a\"))))
                     (list (equal (prin1-to-string compat)
                                  \"#s(sample-tagged \\\"a\\\")\")
                           (not (equal (prin1-to-string bare)
                                       \"#s(sample-tagged \\\"a\\\")\"))
                           (condition-case err
                               (progn (read (prin1-to-string bare)) nil)
                             (invalid-read-syntax t)))))"
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn cl_defmethod_reregistration_replaces_in_place() {
    // GNU replaces a re-registered method (same qualifiers/specializers);
    // splicing a duplicate wrapper used to loop the dispatch chain.
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-replace-base nil nil)
                   (defclass sample-replace-child (sample-replace-base) nil)
                   (cl-defmethod sample-replace-fn ((_x sample-replace-base)) 'base)
                   (cl-defmethod sample-replace-fn ((_x sample-replace-child)) 'child-1)
                   (cl-defmethod sample-replace-fn ((_x sample-replace-child))
                     (list 'child-2 (cl-call-next-method)))
                   (sample-replace-fn (sample-replace-child)))"
        ),
        Value::list([
            Value::Symbol("child-2".into()),
            Value::Symbol("base".into()),
        ])
    );
}

#[test]
fn cl_generic_exhausted_dispatch_signals_like_gnu() {
    // A single-method generic checks its specializers (unmatched calls
    // reach the no-applicable hook) and `cl-call-next-method' with no next
    // method reaches the no-next hook; both error without the hooks'
    // eieio-compat methods installed.
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-hooks-a nil nil)
                   (cl-defmethod sample-hooks-fn ((_x sample-hooks-a))
                     (cl-call-next-method))
                   (list (condition-case nil (sample-hooks-fn 5)
                           (error 'no-applicable))
                         (condition-case nil (sample-hooks-fn (sample-hooks-a))
                           (error 'no-next))))"
        ),
        Value::list([
            Value::Symbol("no-applicable".into()),
            Value::Symbol("no-next".into()),
        ])
    );
}

#[test]
fn setf_updates_eieio_class_parent_metadata() {
    assert_eq!(
        eval_str(
            "(let ((parent (record 'eieio--class))
                       (child (record 'eieio--class)))
                   (setf (cl--find-class 'sample-autoload-parent) parent)
                   (setf (cl--class-parents child) (list parent))
                   (setf (cl--find-class 'sample-autoload-child) child)
                   (list (eq (cl-find-class 'sample-autoload-child) child)
                         (cl--class-allparents child)
                         (eq (car (cl--class-parents child)) parent)))"
        ),
        Value::list([
            Value::T,
            Value::list([
                Value::Symbol("sample-autoload-child".into()),
                Value::Symbol("sample-autoload-parent".into()),
                Value::T,
            ]),
            Value::T,
        ])
    );
}

#[test]
fn complete_lisp_eieio_record_is_authoritative_for_class_children() {
    assert_eq!(
        eval_str(
            "(let ((parent (make-record 'eieio--class 11 nil)))
               (aset parent 1 'sample-live-parent)
               (aset parent 6 '(sample-live-child))
               (setf (cl--find-class 'sample-live-parent) parent)
               (eieio-class-children 'sample-live-parent))"
        ),
        Value::list([Value::Symbol("sample-live-child".into())])
    );
}

#[test]
fn loaded_gnu_setf_updates_raw_cl_class_parent_slot() {
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
                "(let ((parent (record 'eieio--class
                                       'parent nil nil nil nil))
                       (child (record 'eieio--class
                                      'child nil nil nil nil)))
                   (setf (cl--class-parents child) (list parent))
                   (list (eq (car (cl--class-parents child)) parent)
                         (equal (aref child 3) (list parent))))"
            ),
            Value::list([Value::T, Value::T])
        );
    });
}

#[test]
fn defclass_registers_instance_predicate() {
    // GNU's generated `NAME-p' matches the exact class only
    // (`eieio-make-class-predicate'); `NAME--eieio-childp' accepts
    // subclasses.
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-parent nil nil)
                   (defclass sample-child (sample-parent) nil)
                   (let ((child (make-instance 'sample-child)))
                     (list (sample-child-p child)
                           (sample-parent-p child)
                           (sample-parent--eieio-childp child)
                           (sample-child-p 'not-an-object))))"
        ),
        Value::list([Value::T, Value::Nil, Value::T, Value::Nil])
    );
}

#[test]
fn defclass_constructor_initializes_and_updates_slots() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-backend nil
                     ((type :initarg :type :initform 'netrc)
                      (source :initarg :source)
                      (host :initarg :host :initform t)))
                   (let ((backend (sample-backend \"obsolete-name\"
                                                  :source \".\"
                                                  :type 'password-store)))
                     (eieio-oset backend 'host \"example.org\")
                     (list
                      (type-of backend)
                      (slot-value backend 'type)
                      (slot-value backend :source)
                      (eieio-oref backend 'source)
                      (eieio-oref backend 'host))))"
        ),
        Value::list([
            Value::Symbol("sample-backend".into()),
            Value::Symbol("password-store".into()),
            Value::String(".".into()),
            Value::String(".".into()),
            Value::String("example.org".into()),
        ])
    );
}

#[test]
fn defclass_installs_slot_accessors() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-accessor nil
                     ((tags :initarg :tags :accessor sample-accessor-tags)))
                   (sample-accessor-tags
                    (make-instance 'sample-accessor :tags '(a b))))"
        ),
        Value::list([Value::Symbol("a".into()), Value::Symbol("b".into()),])
    );
}

#[test]
fn make_instance_uses_class_slot_defaults() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-instance nil
                     ((alpha :initarg :alpha :initform 7)
                      (beta :initarg :beta :initform (+ 2 3))))
                   (let ((object (make-instance 'sample-instance :alpha 11)))
                     (list
                      (slot-value object 'alpha)
                      (slot-value object 'beta))))"
        ),
        Value::list([Value::Integer(11), Value::Integer(5)])
    );
}

#[test]
fn setf_slot_value_updates_eieio_instances() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-setf-slot nil
                     ((name :initarg :name)
                      (path :initform nil)))
                   (let ((object (make-instance 'sample-setf-slot :name \"old\")))
                     (setf (slot-value object 'name) \"new\"
                           (slot-value object 'path) \"/tmp/sample\")
                     (list (slot-value object 'name)
                           (slot-value object 'path))))"
        ),
        Value::list([
            Value::String("new".into()),
            Value::String("/tmp/sample".into()),
        ])
    );
}

#[test]
fn macroexpanded_setf_follows_preferred_builtin_function_alias() {
    assert_eq!(
        eval_str_with_upstream_load_path(
            "(progn
               (require 'eieio)
               (require 'macroexp)
               (defclass sample-expanded-setf-slot nil
                 ((name :initarg :name)))
               (let* ((object (make-instance 'sample-expanded-setf-slot
                                             :name \"old\"))
                      (expanded
                       (macroexpand-all
                        `(setf (slot-value ',object 'name) \"new\"))))
                 (eval expanded t)
                 (list (symbol-function 'slot-value)
                       (slot-value object 'name))))"
        ),
        Value::list([
            Value::Symbol("eieio-oref".into()),
            Value::String("new".into()),
        ])
    );
}

#[test]
fn macroexpanded_with_slots_setq_uses_the_generalized_setter() {
    assert_eq!(
        eval_str_with_upstream_load_path(
            "(progn
               (require 'eieio)
               (defclass sample-virtual-slot nil
                 ((base :initarg :base)))
               (cl-defmethod slot-missing
                 ((object sample-virtual-slot) slot operation
                  &optional new-value)
                 (if (eq slot 'derived)
                     (with-slots (base) object
                       (if (eq operation 'oref)
                           (1+ base)
                         (setq base (1- new-value))))
                   (cl-call-next-method)))
               (let ((object (sample-virtual-slot :base 1)))
                 (eieio-oset object 'derived 5)
                 (list (eieio-oref object 'base)
                       (eieio-oref object 'derived))))"
        ),
        Value::list([Value::Integer(4), Value::Integer(5)])
    );
}

#[test]
fn cl_symbol_macrolet_reads_and_writes_slot_backed_symbols() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-symbol-macrolet-slot nil
                     ((bounds :initarg :bounds)))
                   (let ((object (make-instance 'sample-symbol-macrolet-slot
                                                :bounds '(1 . 2))))
                     (cl-symbol-macrolet ((bounds (slot-value object 'bounds)))
                       (setq bounds '(3 . 4))
                       bounds)))"
        ),
        Value::cons(Value::Integer(3), Value::Integer(4))
    );
}

#[test]
fn cl_symbol_macrolet_respects_lexical_shadowing() {
    assert_eq!(
        eval_str(
            "(cl-symbol-macrolet ((bounds 'outer))
                   (list bounds
                         (let ((bounds 'inner)) bounds)
                         ((lambda (bounds) bounds) 'argument)))"
        ),
        Value::list([
            Value::Symbol("outer".into()),
            Value::Symbol("inner".into()),
            Value::Symbol("argument".into()),
        ])
    );
}

#[test]
fn cl_symbol_macrolet_preserves_function_call_position() {
    assert_eq!(
        eval_str(
            "(cl-symbol-macrolet ((f (+ x 6)))
                   (cl-flet ((f (x) (+ x 5)))
                     (let ((x 5))
                       (f f))))"
        ),
        Value::Integer(16)
    );
}

#[test]
fn cl_symbol_macrolet_hides_behind_lexical_bindings() {
    assert_eq!(
        eval_str(
            "(let ((y 5))
                   (cl-symbol-macrolet ((x y))
                     (list x
                           (let ((x 6)) (list x y))
                           (cl-letf ((x 6)) (list x y))
                           (apply (lambda (x) (+ x 1)) (list 8)))))"
        ),
        Value::list([
            Value::Integer(5),
            Value::list([Value::Integer(6), Value::Integer(5)]),
            Value::list([Value::Integer(6), Value::Integer(6)]),
            Value::Integer(9),
        ])
    );
}

#[test]
fn cl_symbol_macrolet_preserves_invalid_setq_places() {
    assert_eq!(
        eval_str(
            "(condition-case err
                 (let ((l (list 1)))
                   (cl-symbol-macrolet ((x 1))
                     (setq (car l) 0)))
               (error (car err)))"
        ),
        Value::Symbol("wrong-type-argument".into())
    );
}

#[test]
fn cl_symbol_macrolet_supports_gv_synthetic_place_in_incf() {
    assert_eq!(
        eval_str(
            "(let ((l (list 0)))
               (let ((cl (car l)))
                 (cl-symbol-macrolet
                     ((p (gv-synthetic-place cl (lambda (v) `(setcar l ,v)))))
                   (cl-incf p)))
               l)"
        ),
        Value::list([Value::Integer(1)])
    );
}

#[test]
fn cl_letf_supports_gv_synthetic_place_restore() {
    assert_eq!(
        eval_str(
            "(let ((x 1))
               (list x
                     (cl-letf (((gv-synthetic-place (+ 1 2)
                                                      (lambda (v) `(setq x ,v)))
                                7))
                       x)
                     x))"
        ),
        Value::list([Value::Integer(1), Value::Integer(7), Value::Integer(3)])
    );
}

#[test]
fn cl_old_struct_compat_mode_types_tagged_vectors() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defstruct sample-old-struct x)
                   (let ((x (vector 'cl-struct-sample-old-struct))
                         (saved cl-old-struct-compat-mode))
                     (cl-old-struct-compat-mode -1)
                     (let ((disabled (type-of x)))
                       (cl-old-struct-compat-mode 1)
                       (defvar cl-struct-sample-old-struct)
                       (let ((cl-struct-sample-old-struct
                              (cl--struct-get-class 'sample-old-struct)))
                         (setf (symbol-function 'cl-struct-sample-old-struct)
                               :quick-object-witness-check)
                         (prog1
                             (list disabled
                                   (type-of x)
                                   (type-of (vector 'sample-old-struct)))
                           (cl-old-struct-compat-mode (if saved 1 -1)))))))"
        ),
        Value::list([
            Value::Symbol("vector".into()),
            Value::Symbol("sample-old-struct".into()),
            Value::Symbol("vector".into()),
        ])
    );
}

#[test]
fn cl_struct_define_legacy_type_enables_old_struct_mode() {
    assert_eq!(
        eval_str(
            "(let ((saved cl-old-struct-compat-mode))
                   (cl-old-struct-compat-mode -1)
                   (cl-struct-define 'sample-old-define \"\" 'cl-structure-object
                                     nil nil nil
                                     'cl-struct-sample-old-define-tags
                                     'cl-struct-sample-old-define t)
                   (prog1
                       (list cl-old-struct-compat-mode
                             cl-struct-sample-old-define-tags
                             (symbol-function 'cl-struct-sample-old-define))
                     (cl-old-struct-compat-mode (if saved 1 -1))))"
        ),
        Value::list([
            Value::T,
            Value::list([Value::Symbol("cl-struct-sample-old-define".into())]),
            Value::Symbol(":quick-object-witness-check".into()),
        ])
    );
}

#[test]
fn cl_struct_define_rejects_builtin_type_names() {
    assert_eq!(
        eval_str(
            "(condition-case err
                 (cl-struct-define 'hash-table nil nil 'record nil nil
                                   'cl-preloaded-tests-tag
                                   'cl-preloaded-tests nil)
               (wrong-type-argument err))"
        ),
        Value::list([
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol("cl--struct-name-p".into()),
            Value::Symbol("hash-table".into()),
            Value::Symbol("name".into()),
        ])
    );
}

#[test]
fn cl_generic_define_generalizer_registers_runtime_value() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-generic-define-generalizer sample-generalizer
                     9
                     (lambda (arg) arg)
                     (lambda (_tag) nil))
                   (type-of sample-generalizer))"
        ),
        Value::Symbol("cl--generic-generalizer".into())
    );
}

#[test]
fn cl_defmethod_accepts_extra_qualifiers_before_lambda_list() {
    assert_string_value(
        eval_str(
            "(progn
                   (cl-defgeneric qualified-method (value))
                   (cl-defmethod qualified-method :extra \"tag\" ((value string))
                     value)
                   (qualified-method \"ok\"))",
        ),
        "ok",
    );
}

#[test]
fn cl_defmethod_allows_empty_body_without_edebug_notification() {
    // Evaluating a cl-defmethod form directly (not through edebug's
    // reader) does not notify `edebug-new-definition-function'; only
    // instrumentation via the edebug spec does.
    assert_eq!(
        eval_str(
            r#"(let* ((edebug-all-defs t)
                      (defined-symbols nil)
                      (edebug-new-definition-function
                       (lambda (def-name)
                         (push def-name defined-symbols))))
                 (cl-defmethod sample-edebug-method ((_ number)))
                 (cl-defmethod sample-edebug-method :around ((_ number)))
                 defined-symbols)"#
        ),
        Value::Nil
    );
}

#[test]
fn cl_defmethod_dispatches_eql_specializers() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (cl-defgeneric sample-eql-method (op head &rest args))
                  (cl-defmethod sample-eql-method (op (_ (eql 'alpha)) value &rest rest)
                    (list 'alpha value rest))
                  (cl-defmethod sample-eql-method (op (_ (eql :beta)) value &rest rest)
                    (list 'beta value rest))
                  (equal (list (sample-eql-method 'op 'alpha 1 2 3)
                               (apply #'sample-eql-method 'op '(:beta 4 5 6)))
                         '((alpha 1 (2 3)) (beta 4 (5 6)))))
                "#
        ),
        Value::T
    );
}

#[test]
fn cl_defmethod_specializes_extra_fixed_arg_from_generic_rest() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (cl-defgeneric sample-rest-specializer (x &rest r))
                  (cl-defmethod sample-rest-specializer (x &rest r)
                    (cons x r))
                  (cl-defmethod sample-rest-specializer (x (y integer) &rest r)
                    (list 'integer y x r))
                  (equal (list (sample-rest-specializer 'a 'b)
                               (sample-rest-specializer 1 2))
                         '((a b) (integer 2 1 nil))))
                "#
        ),
        Value::T
    );
}

#[test]
fn cl_defmethod_rewrites_next_method_p() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (fmakunbound 'sample-next-p-generic)
                  (cl-defgeneric sample-next-p-generic (x y))
                  (cl-defmethod sample-next-p-generic ((x t) y)
                    (list x y
                          (with-suppressed-warnings ((obsolete cl-next-method-p))
                            (cl-next-method-p))))
                  (cl-defmethod sample-next-p-generic ((_x (eql 4)) _y)
                    (cons 'four
                          (cons (with-suppressed-warnings ((obsolete cl-next-method-p))
                                  (cl-next-method-p))
                                (cl-call-next-method))))
                  (sample-next-p-generic 4 5))
                "#
        ),
        Value::list([
            Value::Symbol("four".into()),
            Value::T,
            Value::Integer(4),
            Value::Integer(5),
            Value::Nil,
        ])
    );
}

#[test]
fn cl_defmethod_dispatches_context_specializers() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (fmakunbound 'sample-context-generic)
                  (cl-defgeneric sample-context-generic ())
                  (cl-defmethod sample-context-generic (&context (overwrite-mode (eql t)))
                    (list 'is-t (cl-call-next-method)))
                  (cl-defmethod sample-context-generic (&context (overwrite-mode (eql nil)))
                    (list 'is-nil (cl-call-next-method)))
                  (cl-defmethod sample-context-generic () 'any)
                  (list (let ((overwrite-mode t)) (sample-context-generic))
                        (let ((overwrite-mode nil)) (sample-context-generic))
                        (let ((overwrite-mode 1)) (sample-context-generic))))
                "#
        ),
        Value::list([
            Value::list([Value::Symbol("is-t".into()), Value::Symbol("any".into())]),
            Value::list([Value::Symbol("is-nil".into()), Value::Symbol("any".into()),]),
            Value::Symbol("any".into()),
        ])
    );
}

#[test]
fn cl_defmethod_dispatches_head_specializers() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (fmakunbound 'sample-head-generic)
                  (cl-defgeneric sample-head-generic (x y))
                  (cl-defmethod sample-head-generic ((x t) y) (cons x y))
                  (cl-defmethod sample-head-generic ((_x (head 4)) _y)
                    (cons "quatre" (cl-call-next-method)))
                  (cl-defmethod sample-head-generic ((_x (head 5)) _y)
                    (cons "cinq" (cl-call-next-method)))
                  (cl-defmethod sample-head-generic ((_x (head 6)) y)
                    (cons "six" (cl-call-next-method 'a y)))
                  (list (sample-head-generic 'a nil)
                        (sample-head-generic '(4) nil)
                        (sample-head-generic '(5) nil)
                        (sample-head-generic '(6) nil)))
                "#
        ),
        Value::list([
            Value::list([Value::Symbol("a".into())]),
            Value::list([
                Value::String("quatre".into()),
                Value::list([Value::Integer(4)]),
            ]),
            Value::list([
                Value::String("cinq".into()),
                Value::list([Value::Integer(5)]),
            ]),
            Value::list([Value::String("six".into()), Value::Symbol("a".into())]),
        ])
    );
}

#[test]
fn cl_defgeneric_records_advertised_calling_convention() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (fmakunbound 'sample-acc-generic)
                  (cl-defgeneric sample-acc-generic (x &optional y)
                    (declare (advertised-calling-convention (x) "671.2")))
                  (cl-defmethod sample-acc-generic ((x float)) (+ x 5.0))
                  (list
                   (get-advertised-calling-convention
                    (indirect-function 'sample-acc-generic))
                   (condition-case err
                       (let ((byte-compile-error-on-warn t))
                         (byte-compile
                          '(cl-defmethod sample-acc-generic ((x list))
                             (declare (advertised-calling-convention (y) "1.1"))
                             (cons x '(5 5 5 5 5))))
                         nil)
                     (error
                      (and (eq 'error (car err))
                           (string-match "Stray.*declare" (cadr err)))))))
                "#
        ),
        Value::list([Value::list([Value::Symbol("x".into())]), Value::Integer(0)])
    );
}

#[test]
fn bindat_pack_val_round_trips_integer_representation() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        interp.set_load_path(
            crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
        );
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("test/lisp/emacs-lisp/bindat-tests.el"),
        )
        .expect("load bindat tests");
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                    (cl-loop for n in '(0 42 125 126 127 128 150 255 5000 65535 65536 8769786876)
                             always (equal (bindat-unpack bindat-test--int-websocket-type
                                                          (bindat-pack bindat-test--int-websocket-type n))
                                           n))
                    "#
            ),
            Value::T
        );
    });
}

#[test]
fn bindat_recursive_leb128_round_trips_integers() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        interp.set_load_path(
            crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
        );
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("test/lisp/emacs-lisp/bindat-tests.el"),
        )
        .expect("load bindat tests");
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                    (cl-loop for n in '(0 1 42 127 128 255 256 16384 1048575)
                             always (equal (bindat-unpack bindat-test--LEB128
                                                          (bindat-pack bindat-test--LEB128 n))
                                           n))
                    "#
            ),
            Value::T
        );
    });
}

#[test]
fn bindat_signed_integer_types_round_trip_wide_values() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        interp.set_load_path(
            crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
        );
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("test/lisp/emacs-lisp/bindat-tests.el"),
        )
        .expect("load bindat tests");
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                    (eval
                     '(let* ((bitlen 72)
                             (stype (bindat-type sint bitlen nil))
                             (values (list -1 0 42 (1- (ash 1 63)) (- (ash 1 63)))))
                        (cl-loop for n in values
                                 always (equal (bindat-unpack stype
                                                              (bindat-pack stype n))
                                               n)))
                     t)
                    "#
            ),
            Value::T
        );
    });
}

#[test]
fn bindat_str_fields_unpack_from_vector_bytes() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        interp.set_load_path(
            crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
        );
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("test/lisp/emacs-lisp/bindat-tests.el"),
        )
        .expect("load bindat tests");
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                    (let* ((spec (bindat-type
                                   (first u8)
                                   (string str 3)
                                   (last uint 16)))
                           (unpacked (bindat-unpack spec [#xff #x63 #x62 #x61 #xff #xff])))
                      (and (equal (bindat-get-field unpacked 'string) "cba")
                           (equal (bindat-get-field unpacked 'first) 255)
                           (equal (bindat-get-field unpacked 'last) 65535)))
                    "#
            ),
            Value::T
        );
    });
}

#[test]
fn bindat_formats_vector_ip_addresses() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        interp.set_load_path(
            crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
        );
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("test/lisp/emacs-lisp/bindat-tests.el"),
        )
        .expect("load bindat tests");
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(equal (bindat-ip-to-string [192 168 0 1]) "192.168.0.1")"#
            ),
            Value::T
        );
    });
}

#[test]
fn bindat_packet_spec_packs_to_expected_bytes() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        interp.set_load_path(
            crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
        );
        crate::lisp::load_file_strict(
            &mut interp,
            &upstream_emacs_repo().join("test/lisp/emacs-lisp/bindat-tests.el"),
        )
        .expect("load bindat tests");
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                    (equal
                     (append (bindat-pack packet-bindat-spec struct-bindat) nil)
                     '(192 168 1 100 192 168 1 101 01 28 21 32 2 0 0 0
                          2 3 5 0 ?A ?B ?C ?D ?E ?F 0 0 1 2 3 4 5 0 0 0
                          1 4 7 0 ?B ?C ?D ?E ?F ?G 0 0 6 7 8 9 10 11 12 0))
                    "#
            ),
            Value::T
        );
    });
}

#[test]
fn evaluated_lambdas_bind_uninterned_parameters() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((var (make-symbol "v"))
                       (fun (eval `(lambda (,var) ,var) t)))
                  (funcall fun 42))
                "#
        ),
        Value::Integer(42)
    );
}

#[test]
fn nested_evaluated_lambdas_bind_uninterned_parameters() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((var (make-symbol "v"))
                       (inner `(lambda (,var) ,var))
                       (outer (eval `(lambda (arg) (funcall ,inner arg)) t)))
                  (funcall outer 42))
                "#
        ),
        Value::Integer(42)
    );
}

#[test]
fn dotimes_nested_lambdas_bind_uninterned_parameters() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((var (make-symbol "v"))
                       (inner `(lambda (,var) (setq seen ,var)))
                       (outer (eval `(lambda (seq)
                                       (dotimes (i (length seq))
                                         (funcall ,inner (elt seq i)))
                                       seen)
                                    t))
                       seen)
                  (funcall outer [42]))
                "#
        ),
        Value::Integer(42)
    );
}

#[test]
fn dotimes_reuses_its_binding_across_large_loops() {
    assert_eq!(
        eval_str("(dotimes (unicode-codepoint 250000 unicode-codepoint))"),
        Value::Integer(250_000)
    );
}

#[test]
fn cl_defmethod_supports_setf_function_names() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defmethod (setf sample-slot) (store object)
                     store)
                   (funcall #'(setf sample-slot) 42 nil))"
        ),
        Value::Integer(42)
    );
}

#[test]
fn cl_defgeneric_keeps_its_default_body() {
    assert_eq!(
        eval_str(
            "(progn
                   (cl-defgeneric sample-generic (xs)
                     (length xs))
                   (sample-generic '(a b c)))"
        ),
        Value::Integer(3)
    );
}

#[test]
fn cl_defgeneric_does_not_notify_edebug_on_plain_eval() {
    // Like GNU: evaluating cl-defgeneric/:method forms directly (not
    // through edebug's reader) does not call
    // `edebug-new-definition-function'.
    assert_eq!(
        eval_str(
            "(let ((edebug-all-defs t)
                   (instrumented-names nil)
                   (edebug-new-definition-function
                    (lambda (name) (push name instrumented-names))))
               (cl-defgeneric cl-defgeneric/edebug/method/1 (_)
                 (:method ((_ number)) 1)
                 (:method ((_ string)) 2)
                 (:method :around ((_ number)) 3))
               (cl-defgeneric cl-defgeneric/edebug/method/2 (_)
                 (:method ((_ number)) 3))
               (reverse instrumented-names))"
        ),
        Value::Nil
    );
}

#[test]
fn oclosure_lambda_lowers_to_plain_lambda() {
    assert_eq!(
        eval_str(
            "(let ((object (oclosure-lambda (sample-type) (x) x)))
               (list (funcall object 7)
                     (oclosure-type object)
                     (oclosure-type (lambda () nil))))"
        ),
        Value::list([Value::Integer(7), Value::symbol("sample-type"), Value::Nil])
    );
}

#[test]
fn oclosure_methods_precede_callable_representation_methods() {
    assert_eq!(
        eval_str(
            "(progn
               (oclosure-define sample-dispatch-oclosure (value :mutable t))
               (cl-defmethod sample-oclosure-dispatch
                 ((object sample-dispatch-oclosure)) 'specific)
               (cl-defgeneric sample-oclosure-dispatch (object))
               (cl-defmethod sample-oclosure-dispatch (object) 'default)
               (cl-defgeneric sample-oclosure-dispatch (object) 'redefined-default)
               (cl-defmethod sample-oclosure-dispatch
                 ((object interpreted-function)) 'representation)
               (cl-defmethod cl-print-object
                 ((object sample-dispatch-oclosure) stream)
                 (princ \"#f(sample)\" stream))
               (cl-defgeneric cl-print-object (object stream)
                 (prin1 object stream))
               (cl-defmethod cl-print-object
                 ((object interpreted-function) stream)
                 (princ \"#f(lambda)\" stream))
               (let ((object
                      (oclosure-lambda
                          (sample-dispatch-oclosure (value 7)) ()
                        value)))
                 (list (cl-typep object 'interpreted-function)
                       (sample-oclosure-dispatch object)
                       (cl-prin1-to-string object)
                       (progn
                         (cl-incf (sample-dispatch-oclosure--value object) 2)
                         (funcall object)))))"
        ),
        Value::list([
            Value::T,
            Value::symbol("specific"),
            Value::string("#f(sample)"),
            Value::Integer(9),
        ])
    );
}

#[test]
fn function_quote_returns_non_lambda_list_objects_literally() {
    assert_eq!(
        eval_str("#'(1 2)"),
        Value::list([Value::int(1), Value::int(2)])
    );
}

#[test]
fn zerop_rejects_non_numbers_instead_of_treating_them_as_nonzero() {
    assert_eq!(
        eval_str(
            "(condition-case err
                 (zerop \"not-a-number\")
               (wrong-type-argument (list (car err) (cadr err))))"
        ),
        Value::list([
            Value::symbol("wrong-type-argument"),
            Value::symbol("number-or-marker-p"),
        ])
    );
}

#[test]
fn align_c_variable_declaration_regex_matches_resource_lines() {
    let result = eval_str(
        r#"(list
                 (progn
                   (string-match
                    "[*&0-9A-Za-z_]>?[][&*]*\\(\\s-+[*&]*\\)[A-Za-z_][][0-9A-Za-z:_]*\\s-*\\(\\()\\|=[^=\n].*\\|(.*)\\|\\(\\[.*\\]\\)*\\)\\s-*[;,]\\|)\\s-*$\\)"
                    "main (int argc,")
                   (list (match-beginning 0) (match-beginning 1) (match-end 1)))
                 (progn
                   (string-match
                    "[*&0-9A-Za-z_]>?[][&*]*\\(\\s-+[*&]*\\)[A-Za-z_][][0-9A-Za-z:_]*\\s-*\\(\\()\\|=[^=\n].*\\|(.*)\\|\\(\\[.*\\]\\)*\\)\\s-*[;,]\\|)\\s-*$\\)"
                    "char *argv[]);")
                   (list (match-beginning 0) (match-beginning 1) (match-end 1))))"#,
    );
    assert_eq!(
        result,
        Value::list([
            Value::list([Value::Integer(8), Value::Integer(9), Value::Integer(10)]),
            Value::list([Value::Integer(3), Value::Integer(4), Value::Integer(6)]),
        ])
    );
}

#[test]
fn align_c_function_declaration_matches_resource_output() {
    run_with_large_stack(|| {
        let emacs_repo = upstream_emacs_repo();
        let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
        let mut interp = Interpreter::new();
        interp.set_load_path(load_path);
        interp.set_variable("noninteractive", Value::T, &mut Vec::new());
        interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
        let _ = interp.load_target("backquote");
        let _ = interp.load_target("seq");
        load_faces_compat(&mut interp);

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(progn
                         (require 'ert)
                         (require 'ert-x)
                         (require 'align)
                         (with-temp-buffer
                           (c-mode)
                           (insert "int\nmain (int argc,\n      char *argv[]);\n")
                           (align (point-min) (point-max))
                           (buffer-string)))"#
            ),
            Value::String("int\nmain (int\t argc,\n      char\t*argv[]);\n".into())
        );
    });
}

#[test]
fn align_c_variable_declaration_rule_is_runnable_and_valid() {
    run_with_large_stack(|| {
        let emacs_repo = upstream_emacs_repo();
        let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
        let mut interp = Interpreter::new();
        interp.set_load_path(load_path);
        interp.set_variable("noninteractive", Value::T, &mut Vec::new());
        interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
        let _ = interp.load_target("backquote");
        let _ = interp.load_target("seq");
        load_faces_compat(&mut interp);

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(progn
                         (require 'align)
                         (let ((rule (assq 'c-variable-declaration align-rules-list)))
                           (with-temp-buffer
                             (c-mode)
                             (insert "main (int argc,\n      char *argv[]);\n")
                             (goto-char (point-min))
                             (re-search-forward (cdr (assq 'regexp rule)))
                             (list major-mode
                                   font-lock-mode
                                   indent-tabs-mode
                                   align-to-tab-stop
                                   (align--rule-should-run rule)
                                   (funcall (cdr (assq 'valid rule)))))))"#
            ),
            Value::list([
                Value::Symbol("c-mode".into()),
                Value::T,
                Value::T,
                Value::Symbol("indent-tabs-mode".into()),
                Value::T,
                Value::T,
            ])
        );
    });
}

#[test]
fn align_css_declaration_rule_matches_only_declarations() {
    run_with_large_stack(|| {
        let emacs_repo = upstream_emacs_repo();
        let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
        let mut interp = Interpreter::new();
        interp.set_load_path(load_path);
        interp.set_variable("noninteractive", Value::T, &mut Vec::new());
        interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
        let _ = interp.load_target("backquote");
        let _ = interp.load_target("seq");
        load_faces_compat(&mut interp);

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(progn
                         (require 'align)
                         (let ((rule (assq 'css-declaration align-rules-list)))
                           (with-temp-buffer
                             (css-mode)
                             (list (align--rule-should-run rule)
                                   (string-match (cdr (assq 'regexp rule)) "  color: red;")
                                   (string-match (cdr (assq 'regexp rule)) "p.center {")))))"#
            ),
            Value::list([Value::T, Value::Integer(0), Value::Nil])
        );
    });
}

#[test]
fn align_css_declaration_search_positions_match_buffer_lines() {
    run_with_large_stack(|| {
        let emacs_repo = upstream_emacs_repo();
        let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
        let mut interp = Interpreter::new();
        interp.set_load_path(load_path);
        interp.set_variable("noninteractive", Value::T, &mut Vec::new());
        interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
        let _ = interp.load_target("backquote");
        let _ = interp.load_target("seq");
        load_faces_compat(&mut interp);

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(progn
                         (require 'align)
                         (let ((rule (assq 'css-declaration align-rules-list)))
                           (with-temp-buffer
                             (css-mode)
                             (insert "  border: 1px solid black;\n  padding: 25px 50px 75px 100px;\n")
                             (goto-char (point-min))
                             (re-search-forward (cdr (assq 'regexp rule)))
                             (let ((first (list (match-beginning 0) (match-end 0)
                                                (match-beginning 1) (match-end 1))))
                               (re-search-forward (cdr (assq 'regexp rule)))
                               (list first
                                     (list (match-beginning 0) (match-end 0)
                                           (match-beginning 1) (match-end 1)))))))"#
            ),
            Value::list([
                Value::list([
                    Value::Integer(1),
                    Value::Integer(27),
                    Value::Integer(10),
                    Value::Integer(11),
                ]),
                Value::list([
                    Value::Integer(28),
                    Value::Integer(60),
                    Value::Integer(38),
                    Value::Integer(39),
                ]),
            ])
        );
    });
}

#[test]
fn align_region_separator_finds_brace_line_between_css_blocks() {
    run_with_large_stack(|| {
        let emacs_repo = upstream_emacs_repo();
        let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
        let mut interp = Interpreter::new();
        interp.set_load_path(load_path);
        interp.set_variable("noninteractive", Value::T, &mut Vec::new());
        interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
        let _ = interp.load_target("backquote");
        let _ = interp.load_target("seq");
        load_faces_compat(&mut interp);

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(progn
                         (require 'align)
                         (with-temp-buffer
                           (insert "div {\n  border: 1px solid black;\n  padding: 25px 50px 75px 100px;\n  background-color: lightblue;\n}\np.center {\n  text-align: center;\n  color: red;\n}\n")
                           (align-new-section-p 86 124 align-region-separate)))"#
            ),
            Value::Integer(99)
        );
    });
}

#[test]
fn align_region_separator_accepts_marker_bounds() {
    run_with_large_stack(|| {
        let emacs_repo = upstream_emacs_repo();
        let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
        let mut interp = Interpreter::new();
        interp.set_load_path(load_path);
        interp.set_variable("noninteractive", Value::T, &mut Vec::new());
        interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
        let _ = interp.load_target("backquote");
        let _ = interp.load_target("seq");
        load_faces_compat(&mut interp);

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(progn
                         (require 'align)
                         (with-temp-buffer
                           (insert "div {\n  border: 1px solid black;\n  padding: 25px 50px 75px 100px;\n  background-color: lightblue;\n}\np.center {\n  text-align: center;\n  color: red;\n}\n")
                           (align-new-section-p (copy-marker 86 t)
                                                (copy-marker 124 t)
                                                align-region-separate)))"#
            ),
            Value::Integer(99)
        );
    });
}

#[test]
fn align_css_resource_case_matches_output() {
    run_with_large_stack(|| {
        let emacs_repo = upstream_emacs_repo();
        let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
        let mut interp = Interpreter::new();
        interp.set_load_path(load_path);
        interp.set_variable("noninteractive", Value::T, &mut Vec::new());
        interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
        let _ = interp.load_target("backquote");
        let _ = interp.load_target("seq");
        load_faces_compat(&mut interp);

        assert_eq!(
                eval_str_with(
                    &mut interp,
                    r#"(progn
                         (require 'align)
                         (with-temp-buffer
                           (let ((indent-tabs-mode nil))
                             (css-mode)
                             (insert "div {\n  border: 1px solid black;\n  padding: 25px 50px 75px 100px;\n  background-color: lightblue;\n}\np.center {\n  text-align: center;\n  color: red;\n}\n")
                             (align (point-min) (point-max))
                             (buffer-string))))"#
                ),
                Value::String(
                    "div {\n  border:           1px solid black;\n  padding:          25px 50px 75px 100px;\n  background-color: lightblue;\n}\np.center {\n  text-align: center;\n  color:      red;\n}\n"
                        .into()
                )
            );
    });
}

#[test]
fn buffer_match_data_restores_live_marker_positions() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                     (insert "aa bb")
                     (goto-char (point-min))
                     (re-search-forward "aa\\( \\)bb")
                     (let ((data (match-data)))
                       (goto-char (point-min))
                       (insert "XX")
                       (set-match-data data)
                       (list (match-beginning 1) (match-end 1))))"#
        ),
        Value::list([Value::Integer(5), Value::Integer(6)])
    );
}

#[test]
fn save_match_data_restores_live_buffer_positions() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                     (insert "aa bb")
                     (goto-char (point-min))
                     (re-search-forward "aa\\( \\)bb")
                     (save-match-data
                       (goto-char (point-min))
                       (insert "XX"))
                     (list (match-beginning 1) (match-end 1)))"#
        ),
        Value::list([Value::Integer(5), Value::Integer(6)])
    );
}

#[test]
fn re_search_forward_anchor_keeps_context_after_point() {
    assert_eq!(
        eval_str(
            r#"(with-temp-buffer
                     (insert "a\nb\n")
                     (goto-char 2)
                     (re-search-forward "^b")
                     (list (match-beginning 0) (match-end 0) (point)))"#
        ),
        Value::list([Value::Integer(3), Value::Integer(4), Value::Integer(4)])
    );
}

#[test]
fn allout_range_overlaps_keeps_prior_ranges_when_appending() {
    run_with_large_stack(|| {
        let emacs_repo = upstream_emacs_repo();
        let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
        let mut interp = Interpreter::new();
        interp.set_load_path(load_path);
        interp.set_variable("noninteractive", Value::T, &mut Vec::new());
        interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
        let _ = interp.load_target("backquote");
        let _ = interp.load_target("seq");
        load_faces_compat(&mut interp);

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                    (progn
                      (require 'allout-widgets)
                      (allout-range-overlaps 10 12 '((3 5))))
                    "#
            ),
            Value::list([
                Value::Nil,
                Value::list([
                    Value::list([Value::Integer(3), Value::Integer(5)]),
                    Value::list([Value::Integer(10), Value::Integer(12)]),
                ]),
            ])
        );
    });
}

#[test]
fn cl_letf_supports_symbol_places() {
    assert_eq!(
        eval_str(
            "(progn
                   (defvar cl-letf-temp 'outer)
                   (list
                     (cl-letf ((cl-letf-temp 'inner))
                       (setq cl-letf-temp 'changed)
                       cl-letf-temp)
                     cl-letf-temp))"
        ),
        Value::list([
            Value::Symbol("changed".into()),
            Value::Symbol("outer".into()),
        ])
    );
}

#[test]
fn cl_letf_can_mix_variable_and_function_rebinding() {
    assert_eq!(
        eval_str(
            "(progn
                   (defvar cl-letf-temp 'outer)
                   (fset 'cl-letf-temp-fn #'identity)
                   (list
                     (cl-letf (((symbol-function 'cl-letf-temp-fn) #'ignore)
                               (cl-letf-temp 'inner))
                       (list (cl-letf-temp-fn 'value) cl-letf-temp))
                     (cl-letf-temp-fn 'value)
                     cl-letf-temp))"
        ),
        Value::list([
            Value::list([Value::Nil, Value::Symbol("inner".into())]),
            Value::Symbol("value".into()),
            Value::Symbol("outer".into()),
        ])
    );
}

#[test]
fn cl_letf_can_temporarily_override_native_read_event() {
    assert_eq!(
        eval_str(
            "(cl-letf (((symbol-function 'read-event) \
                         (lambda (&rest _) ?n))) \
                 (read-event))"
        ),
        Value::Integer('n' as i64)
    );
}

#[test]
fn map_y_or_n_p_honors_a_temporarily_overridden_read_event() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (require 'map-ynp)
                (let ((use-dialog-box nil)
                      (reads 0))
                  (cl-letf (((symbol-function 'read-event)
                             (lambda (&rest _)
                               (setq reads (1+ reads))
                               ?n))
                            ((symbol-function 'sit-for)
                             (lambda (&rest _)
                               (error "map-y-or-n-p retried a valid answer"))))
                    (list (map-y-or-n-p "%s? " #'ignore '(item)) reads)))
                "#
        ),
        Value::list([Value::Integer(0), Value::Integer(1)])
    );
}

#[test]
fn pcase_dolist_binds_backquoted_variables() {
    assert_eq!(
        eval_str(
            "(let (pairs) \
                   (pcase-dolist (`(,left ,right) '((1 2) (3 4))) \
                     (push (list left right) pairs)) \
                   (nreverse pairs))"
        ),
        Value::list([
            Value::list([Value::Integer(1), Value::Integer(2)]),
            Value::list([Value::Integer(3), Value::Integer(4)]),
        ])
    );
}

#[test]
fn pcase_backquote_requires_exact_list_shape() {
    assert_eq!(
        eval_str(
            "(list (pcase '(1 2) (`(,left ,middle ,right) 'match) (_ 'miss)) \
                       (pcase '(3 4 5 6) (`(,left ,middle ,right) 'match) (_ 'miss)))"
        ),
        Value::list([Value::Symbol("miss".into()), Value::Symbol("miss".into()),])
    );
}

#[test]
fn pcase_backquote_treats_plain_symbols_as_literals() {
    assert_eq!(
        eval_str(
            "(list
                   (pcase '(float 141421356237 -11)
                     (`(frac ,p ,q) 'wrong)
                     (`(float ,m ,e) (list m e))
                     (_ 'miss))
                   (pcase '(frac 1 2)
                     (`(frac ,p ,q) (/ (float p) q))
                     (_ 'miss))
                   (pcase '(_ value)
                     (`(_ ,x) x)
                     (_ 'miss)))"
        ),
        Value::list([
            Value::list([Value::Integer(141421356237), Value::Integer(-11)]),
            Value::Float(0.5),
            Value::Symbol("value".into()),
        ])
    );
}

#[test]
fn pcase_backquote_treats_t_and_nil_as_literals() {
    assert_eq!(
        eval_str(
            "(list
                   (pcase '(binder read nil t nil)
                     (`(,binder ,_ t t ,_) 'wrong)
                     (_ 'other))
                   (pcase '(binder nil nil t nil)
                     (`(,binder nil ,_ ,_ nil) 'unused)
                     (_ 'other)))"
        ),
        Value::list([
            Value::Symbol("other".into()),
            Value::Symbol("unused".into()),
        ])
    );
}

#[test]
fn pcase_backquote_comma_matches_nested_patterns() {
    assert_eq!(
        eval_str(
            "(pcase '(let ((f 1)) body)
               (`(,(and letsym (or 'let* 'let)) ,binders . ,body)
                (list letsym binders body))
               (_ 'miss))"
        ),
        Value::list([
            Value::Symbol("let".into()),
            Value::list([Value::list([Value::Symbol("f".into()), Value::Integer(1)])]),
            Value::list([Value::Symbol("body".into())]),
        ])
    );
}

#[test]
fn pcase_let_lenient_backquoted_lists_bind_missing_nil_and_ignore_extra() {
    assert_eq!(
        eval_str(
            "(list (pcase-let ((`(,a ,b ,c) '(1 2))) (list a b c)) \
                       (pcase-let ((`(,a ,b) '(1 2 3))) (list a b)) \
                       (pcase-let ((`(,a ,b) nil)) (list a b)))"
        ),
        Value::list([
            Value::list([Value::Integer(1), Value::Integer(2), Value::Nil]),
            Value::list([Value::Integer(1), Value::Integer(2)]),
            Value::list([Value::Nil, Value::Nil]),
        ])
    );
}

#[test]
fn pcase_let_matches_cl_struct_slot_patterns() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (cl-defstruct sample-pcase-state stack ppss ppss-point)
                  (pcase-let* (((cl-struct sample-pcase-state
                                            (stack indent-stack)
                                            ppss ppss-point)
                                 (make-sample-pcase-state
                                  :stack '(cached)
                                  :ppss '(0 nil)
                                  :ppss-point 7)))
                    (list indent-stack ppss ppss-point)))
                "#
        ),
        Value::list([
            Value::list([Value::Symbol("cached".into())]),
            Value::list([Value::Integer(0), Value::Nil]),
            Value::Integer(7),
        ])
    );
}

#[test]
fn bool_vector_literals_eval_to_runtime_values() {
    assert_eq!(
        eval_str(
            r#"(let ((vec #&8"\1"))
                         (list (bool-vector-count-population vec)
                               (aref vec 0)
                               (aref vec 7)))"#
        ),
        Value::list([Value::Integer(1), Value::T, Value::Nil])
    );
}

#[test]
fn pcase_matches_quoted_symbols_and_wildcards() {
    assert_eq!(
        eval_str(
            "(list (pcase 'gnu/linux ('gnu/linux 1) (_ 2)) \
                       (pcase 'other ('gnu/linux 1) (_ 2)))"
        ),
        Value::list([Value::Integer(1), Value::Integer(2)])
    );
}

#[test]
fn pcase_matches_keyword_symbols_as_constants() {
    assert_eq!(
        eval_str(
            "(list
                   (pcase :captured+mutated
                     (:captured+mutated 'hit)
                     (_ 'miss))
                   (pcase nil
                     (:captured+mutated 'wrong)
                     (_ 'other)))"
        ),
        Value::list([Value::Symbol("hit".into()), Value::Symbol("other".into()),])
    );
}

#[test]
fn pcase_matches_or_patterns() {
    assert_eq!(
        eval_str(
            "(list (pcase 3 ((or 1 3 5) 'odd) (_ 'other)) \
                       (pcase 2 ((or 1 3 5) 'odd) (_ 'other)))"
        ),
        Value::list([Value::Symbol("odd".into()), Value::Symbol("other".into())])
    );
}

#[test]
fn pcase_matches_predicate_patterns() {
    assert_eq!(
        eval_str(
            "(list
                   (pcase 'list ((pred symbolp) 'symbol) (_ 'other))
                   (pcase '(1 2) ((pred listp) 'list) (_ 'other))
                   (pcase 3 ((pred (not symbolp)) 'number) (_ 'other)))"
        ),
        Value::list([
            Value::Symbol("symbol".into()),
            Value::Symbol("list".into()),
            Value::Symbol("number".into()),
        ])
    );
}

#[test]
fn cl_struct_predicate_is_available_to_pcase_patterns() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (cl-defstruct sample-pcase-struct value)
                 (let ((object (make-sample-pcase-struct :value 7)))
                   (list (cl-struct-p object)
                         (cl-struct-p '(sample-pcase-struct 7))
                         (pcase object
                           ((pred cl-struct-p) 'struct)
                           (_ 'other)))))"#
        ),
        Value::list([Value::T, Value::Nil, Value::symbol("struct")])
    );
}

#[test]
fn string_comparison_uses_visible_names_of_uninterned_symbols() {
    assert_eq!(
        eval_str(
            r#"(let ((symbol (make-symbol "same")))
                 (list (eq 'same symbol)
                       (string= 'same symbol)
                       (string< 'same symbol)
                       (string< symbol 'same)))"#
        ),
        Value::list([Value::Nil, Value::T, Value::Nil, Value::Nil])
    );
}

#[test]
fn raw_byte_characters_build_multibyte_strings_across_character_apis() {
    assert_eq!(
        eval_str(
            r#"(let ((formatted (format "?%c" #x3fffff))
                     (built (string #x3fffff))
                     (single (char-to-string #x3fffff)))
                 (and (multibyte-string-p formatted)
                      (multibyte-string-p built)
                      (multibyte-string-p single)
                      (equal formatted (string-to-multibyte "?\xff"))
                      (equal (append formatted nil) '(63 #x3fffff))
                      (equal (append built nil) '(#x3fffff))
                      (equal built single)))"#,
        ),
        Value::T
    );
}

#[test]
fn print_quoted_does_not_consume_an_elided_print_level() {
    assert_eq!(
        eval_str(
            "(let ((print-level 1)
                   (print-quoted t))
               (prin1-to-string ''(a (b))))"
        ),
        Value::String("'(a ...)".into())
    );
}

#[test]
fn pcase_predicate_patterns_append_value_to_function_forms() {
    assert_eq!(
        eval_str(
            "(let ((target (quote (a b))))
               (list
                (pcase (quote (a b)) ((pred (equal target)) (quote hit)) (_ (quote miss)))
                (pcase (quote (a c)) ((pred (equal target)) (quote wrong)) (_ (quote other)))))"
        ),
        Value::list([Value::Symbol("hit".into()), Value::Symbol("other".into())])
    );
}

#[test]
fn pcase_defmacro_registers_a_macroexpander_property() {
    assert_eq!(
        eval_str(
            "(progn
                   (pcase-defmacro sample (pattern) pattern)
                   (list (get 'sample 'pcase-macroexpander)
                         (fboundp 'sample--pcase-macroexpander)))"
        ),
        Value::list([
            Value::Symbol("sample--pcase-macroexpander".into()),
            Value::T
        ])
    );
}

#[test]
fn pcase_dolist_lenient_backquoted_lists_bind_missing_nil_and_ignore_extra() {
    assert_eq!(
        eval_str(
            "(let (pairs) \
                   (pcase-dolist (`(,left ,middle ,right) \
                                  '((1 2) (3 4 5) (6 7 8 9))) \
                     (push (list left middle right) pairs)) \
                   (nreverse pairs))"
        ),
        Value::list([
            Value::list([Value::Integer(1), Value::Integer(2), Value::Nil]),
            Value::list([Value::Integer(3), Value::Integer(4), Value::Integer(5)]),
            Value::list([Value::Integer(6), Value::Integer(7), Value::Integer(8)]),
        ])
    );
}

#[test]
fn pcase_and_let_patterns_evaluate_expressions_with_bindings() {
    assert_eq!(
        eval_str(
            "(let ((f (lambda (x) \
                            (pcase 'dummy \
                              ((and (let var x) (guard var)) 'left) \
                              ((and (let var (not x)) (guard var)) 'right))))) \
                   (list (funcall f t) (funcall f nil)))"
        ),
        Value::list([Value::Symbol("left".into()), Value::Symbol("right".into())])
    );
}

#[test]
fn pcase_dolist_matches_or_and_let_nil_patterns() {
    assert_eq!(
        eval_str(
            "(let (pairs) \
                   (pcase-dolist ((or `(,min . ,max) (and min (let max nil))) \
                                  '(\"0.9\" (\"1.0\" . \"2.0\"))) \
                     (push (list min max) pairs)) \
                   (nreverse pairs))"
        ),
        Value::list([
            Value::list([Value::String("0.9".into()), Value::Nil]),
            Value::list([Value::String("1.0".into()), Value::String("2.0".into()),]),
        ])
    );
}

#[test]
fn version_lte_rejects_invalid_version_strings() {
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let forms = Reader::new("(version<= \"foo\" \"1.0\")")
        .read_all()
        .expect("read version comparison form");
    let error = interp
        .eval(&forms[0], &mut env)
        .expect_err("invalid version syntax should signal");
    assert!(matches!(
        error,
        LispError::Signal(message)
            if message == "Invalid version syntax: `foo' (must start with a number)"
    ));
}

#[test]
fn version_lte_honors_prerelease_qualifiers() {
    assert_eq!(eval_str("(version<= \"1.0pre1\" \"1.0\")"), Value::T);
    assert_eq!(eval_str("(version<= \"1.0\" \"1.0pre1\")"), Value::Nil);
    assert_eq!(eval_str("(version<= \"1.0.1alpha\" \"1.0.1\")"), Value::T);
    assert_eq!(eval_str("(version<= \"1.0\" \"1.0.0\")"), Value::T);
}

#[test]
fn version_to_list_exposes_parsed_version_components() {
    assert_eq!(
        eval_str("(version-to-list \"2.7.3.30.2\")"),
        Value::list([
            Value::Integer(2),
            Value::Integer(7),
            Value::Integer(3),
            Value::Integer(30),
            Value::Integer(2),
        ])
    );
    assert_eq!(
        eval_str("(version-to-list \"1.0pre2\")"),
        Value::list([
            Value::Integer(1),
            Value::Integer(0),
            Value::Integer(-1),
            Value::Integer(2),
        ])
    );
}

#[test]
fn lexical_symbol_variables_do_not_shadow_function_namespace() {
    assert_eq!(
        eval_str("(let ((append 'append) (car 'cdr)) (list (append '(1) '(2)) (car '(3 . 4))))"),
        Value::list([
            Value::list([Value::Integer(1), Value::Integer(2)]),
            Value::Integer(3),
        ])
    );
}

#[test]
fn replace_match_updates_match_data_for_subexpressions() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (let (mismatch)
                    (pcase-dolist (`(,pre ,post) '(("" "")
                                                   ("a" "")
                                                   ("" "b")
                                                   ("a" "b")))
                      (unless mismatch
                        (erase-buffer)
                        (insert "hello ")
                        (save-excursion (insert pre post " world"))
                        (looking-at
                         (concat "\\(\\)" pre "\\(\\)\\(\\(\\)\\)\\(\\)" post "\\(\\)"))
                        (let* ((beg0 (match-beginning 0))
                               (beg4 (+ beg0 (length pre)))
                               (end4 (+ beg4 (length "BOO")))
                               (end0 (+ end4 (length post))))
                          (replace-match "BOO" t t nil 4)
                          (unless (and (equal (match-beginning 0) beg0)
                                       (equal (match-end 0) end0))
                            (setq mismatch
                                  (list pre post
                                        (match-beginning 0)
                                        (match-end 0)
                                        beg0
                                        end0))))))
                    mismatch))"#,
        ),
        Value::Nil
    );
}

#[test]
fn save_excursion_restores_current_buffer_after_switching() {
    assert_eq!(
        eval_str(
            r#"
                (let ((origin (current-buffer)))
                  (save-excursion
                    (switch-to-buffer " *save-excursion-other*"))
                  (eq (current-buffer) origin))
                "#
        ),
        Value::T
    );
}

#[test]
fn current_buffer_scopes_never_restore_by_displaying_a_buffer() {
    assert_eq!(
        eval_str(
            r#"
                (let* ((origin (current-buffer))
                       (other (get-buffer-create " *current-buffer-scope-other*"))
                       (window (selected-window))
                       (inside
                        (with-current-buffer other
                          (list (eq (current-buffer) other)
                                (eq (window-buffer window) origin))))
                       after-save-current
                       after-save-excursion
                       after-error)
                  (save-current-buffer
                    (switch-to-buffer other))
                  (setq after-save-current
                        (list (eq (current-buffer) origin)
                              (eq (window-buffer window) other)))
                  (switch-to-buffer origin)
                  (save-excursion
                    (switch-to-buffer other))
                  (setq after-save-excursion
                        (list (eq (current-buffer) origin)
                              (eq (window-buffer window) other)))
                  (switch-to-buffer origin)
                  (condition-case nil
                      (save-current-buffer
                        (switch-to-buffer other)
                        (error "boom"))
                    (error nil))
                  (setq after-error
                        (list (eq (current-buffer) origin)
                              (eq (window-buffer window) other)))
                  (list inside after-save-current after-save-excursion after-error))
                "#
        ),
        Value::list([
            Value::list([Value::T, Value::T]),
            Value::list([Value::T, Value::T]),
            Value::list([Value::T, Value::T]),
            Value::list([Value::T, Value::T]),
        ])
    );
}

#[test]
fn save_restriction_restores_the_original_buffer_restriction() {
    assert_eq!(
        eval_str(
            r#"
                (with-temp-buffer
                  (insert "abcdef")
                  (narrow-to-region 2 5)
                  (let ((origin (current-buffer)))
                    (save-restriction
                      (switch-to-buffer " *save-restriction-other*"))
                    (with-current-buffer origin
                      (list (point-min) (point-max)))))
                "#
        ),
        Value::list([Value::Integer(2), Value::Integer(5)])
    );
}

#[test]
fn string_match_supports_explicitly_numbered_groups() {
    assert_string_value(
        eval_str(
            r#"
                (progn
                  (string-match
                   "\\$\\(?:\\(?1:[[:alnum:]_]+\\)\\|{\\(?1:[^{}]+\\)}\\|\\$\\)"
                   "${HOME}")
                  (match-string 1 "${HOME}"))"#,
        ),
        "HOME",
    );
}

#[test]
fn save_window_excursion_restores_current_buffer() {
    assert_eq!(
        eval_str(
            r#"
                (let ((original (current-buffer))
                      (other (get-buffer-create "*save-window-excursion*")))
                  (save-window-excursion
                    (set-buffer other)
                    (current-buffer))
                  (eq (current-buffer) original))
                "#
        ),
        Value::T
    );
}

#[test]
fn save_window_excursion_restores_window_start() {
    assert_eq!(
        eval_str(
            r#"
                (let ((window (selected-window))
                      (original (current-buffer)))
                  (insert "a\nb\nc\nd\n")
                  (set-window-start window 3)
                  (save-window-excursion
                    (set-window-start window 5)
                    (set-buffer (get-buffer-create "*save-window-excursion*")))
                  (list (window-start window)
                        (eq (current-buffer) original)))
                "#
        ),
        Value::list([Value::Integer(3), Value::T])
    );
}

#[test]
fn preloaded_window_contract_restores_context_and_defines_resize_mode() {
    assert_eq!(
        eval_str_with_upstream_batch(
            r#"
                (let ((original-buffer (current-buffer))
                      (original-window (selected-window))
                      (other (get-buffer-create "*save-selected-window*")))
                  (list
                   (macrop 'save-selected-window)
                   (boundp 'temp-buffer-resize-mode)
                   temp-buffer-resize-mode
                   (save-selected-window
                     (set-buffer other)
                     'body-value)
                   (eq (current-buffer) original-buffer)
                   (eq (selected-window) original-window)))
                "#
        ),
        Value::list([
            Value::T,
            Value::T,
            Value::Nil,
            Value::symbol("body-value"),
            Value::T,
            Value::T,
        ])
    );
}

#[test]
fn window_configuration_equality_ignores_view_position_but_compares_layout() {
    assert_eq!(
        eval_str(
            r#"(progn
                 (insert "one\ntwo\nthree\n")
                 (let ((before (current-window-configuration)))
                   (goto-char (point-max))
                   (set-window-start (selected-window) 5)
                   (let ((moved (current-window-configuration)))
                     (switch-to-buffer (get-buffer-create "*other-layout*"))
                     (list (window-configuration-equal-p before moved)
                           (window-configuration-equal-p
                            before (current-window-configuration))))))"#
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn vertical_motion_moves_by_lines_and_reports_actual_motion() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (insert "a\nb\nc\n")
                  (goto-char (point-min))
                  (list (vertical-motion 2)
                        (line-number-at-pos)
                        (vertical-motion 5)
                        (line-number-at-pos)
                        (vertical-motion -1)
                        (line-number-at-pos)))
                "#
        ),
        Value::list([
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(1),
            Value::Integer(4),
            Value::Integer(-1),
            Value::Integer(3),
        ])
    );
}

#[test]
fn pos_visible_in_window_p_checks_selected_window_range() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (insert "a\nb\nc\n")
                  (set-window-start (selected-window) 3)
                  (list (pos-visible-in-window-p 1)
                        (pos-visible-in-window-p 3)
                        (pos-visible-in-window-p (point-max) (selected-window))))
                "#
        ),
        Value::list([Value::Nil, Value::T, Value::T])
    );
}

#[test]
fn pos_visible_in_window_p_respects_window_height() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (dotimes (_ 30)
                    (insert "line\n"))
                  (set-window-start (selected-window) 1)
                  (list (pos-visible-in-window-p 1)
                        (pos-visible-in-window-p (point-max))))
                "#
        ),
        Value::list([Value::T, Value::Nil])
    );
}

#[test]
fn pos_visible_in_window_p_is_nil_when_noninteractive() {
    assert_eq!(
        eval_str(
            r#"
                (progn
                  (setq noninteractive t)
                  (insert "line\n")
                  (pos-visible-in-window-p 1))
                "#
        ),
        Value::Nil
    );
}

#[test]
fn display_buffer_preserves_current_buffer_and_updates_window_buffer() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
                (let ((original (current-buffer))
                      (other (get-buffer-create "*display-buffer-target*")))
                  (with-current-buffer other
                    (erase-buffer)
                    (insert "a\nb\nc\n"))
                  (display-buffer other)
                  (set-window-start (selected-window) 3)
                  (list (eq (current-buffer) original)
                        (eq (window-buffer (selected-window)) other)
                        (= (window-start (selected-window)) 3)
                        (= (window-end (selected-window))
                           (with-current-buffer other (point-max)))))"#
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn switch_to_buffer_displays_a_target_that_is_already_current() {
    assert_eq!(
        eval_str(
            r#"
                (let ((visible (current-buffer))
                      (hidden (get-buffer-create " *set-buffer-hidden*"))
                      (window (selected-window)))
                  (set-buffer hidden)
                  (let ((before (and (eq (current-buffer) hidden)
                                     (eq (window-buffer window) visible))))
                    (switch-to-buffer hidden)
                    (list before
                          (eq (current-buffer) hidden)
                          (eq (window-buffer window) hidden))))
                "#
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn display_buffer_respects_inhibit_same_window_action() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
                (let ((original (current-buffer))
                      (other (get-buffer-create "*display-buffer-no-same-window*")))
                  (list (display-buffer other '((inhibit-same-window . t)))
                        (eq (window-buffer (selected-window)) original)))"#
        ),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn display_buffer_alist_matches_modes_and_merges_actions() {
    assert_eq!(
        eval_str(
            r#"(eval
                '(let ((target (get-buffer-create " display-alist-target")) calls)
                 (with-current-buffer target (setq major-mode 'erc-mode))
                 (cl-letf (((symbol-function 'match-target)
                            (lambda (_buffer action)
                              (push (list 'matched
                                          (alist-get 'bar (cdr action)))
                                    calls)
                              t))
                           ((symbol-function 'show-target)
                            (lambda (_buffer action)
                              (push (list 'shown
                                          (alist-get 'foo action)
                                          (alist-get 'bar action))
                                    calls)
                              (selected-window))))
                   (let ((display-buffer-alist
                          '(((and (major-mode . erc-mode) match-target)
                             show-target (foo . 42)))))
                     (display-buffer target '(nil (bar . 7)))))
                 (nreverse calls))
                t)"#
        ),
        Value::list([
            Value::list([Value::Symbol("matched".into()), Value::Integer(7)]),
            Value::list([
                Value::Symbol("shown".into()),
                Value::Integer(42),
                Value::Integer(7),
            ]),
        ])
    );
}

#[test]
fn preloaded_display_buffer_actions_remain_dynamic_across_function_calls() {
    assert_eq!(
        eval_str(
            r#"(eval
                '(defun emaxx-test-display-buffer-from-separate-function (buffer)
                   (display-buffer buffer))
                t)
               (eval
                '(let ((target (get-buffer-create " display-dynamic-target")) calls)
                     (with-current-buffer target (setq major-mode 'erc-mode))
                     (cl-letf (((symbol-function 'emaxx-test-display-predicate)
                                (lambda (buffer action)
                                  (push (list 'matched buffer
                                              (alist-get 'bar (cdr action)))
                                        calls)))
                               ((symbol-function 'emaxx-test-display-action)
                                (lambda (buffer action)
                                  (push (list 'shown buffer
                                              (alist-get 'foo action))
                                        calls)
                                  (selected-window))))
                       (let ((display-buffer-alist
                              '(((and (major-mode . erc-mode)
                                      emaxx-test-display-predicate)
                                 emaxx-test-display-action (foo . 42)))))
                         (emaxx-test-display-buffer-from-separate-function target)))
                     (list (special-variable-p 'display-buffer-alist)
                           (mapcar #'car (nreverse calls))))
                t)"#
        ),
        Value::list([
            Value::T,
            Value::list([
                Value::Symbol("matched".into()),
                Value::Symbol("shown".into()),
            ]),
        ])
    );
}

#[test]
fn gnu_add_function_composes_nested_advice_on_a_lexical_variable() {
    assert_eq!(
        eval_str_with_upstream_load_path(
            r#"(eval
                '(let ((target (lambda (value)
                                 (list 'base value))))
                   (mapc
                    (lambda (tag)
                      (let ((captured-tag nil))
                        (let ((layer
                               (lambda (oldfun value)
                                 (cons captured-tag
                                       (funcall oldfun value)))))
                          ;; The closure already exists when this shared
                          ;; lexical cell diverges from the next layer's.
                          (setq captured-tag tag)
                          (add-function :around (var target) layer))))
                    '(outer inner))
                   (funcall target 7))
                t)"#,
        ),
        Value::list([
            Value::Symbol("inner".into()),
            Value::Symbol("outer".into()),
            Value::Symbol("base".into()),
            Value::Integer(7),
        ])
    );
}

#[test]
fn gnu_add_function_composes_nested_advice_on_a_dynamic_variable() {
    assert_eq!(
        eval_str_with_upstream_load_path(
            r#"(eval
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
                t)"#,
        ),
        Value::list([
            Value::Symbol("inner".into()),
            Value::Symbol("outer".into()),
            Value::Symbol("base".into()),
            Value::Integer(7),
        ])
    );
}

#[test]
fn quit_window_buries_current_buffer_without_killing_it() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
                (let ((target (get-buffer-create "*quit-window-target*")))
                  (switch-to-buffer target)
                  (quit-window)
                  (list (not (eq (current-buffer) target))
                        (buffer-live-p target)))"#
        ),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn quit_window_returns_to_previous_pop_to_buffer_target() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
                (let ((origin (get-buffer-create "*quit-origin*"))
                      (first (get-buffer-create "*quit-first*"))
                      (second (get-buffer-create "*quit-second*")))
                  (switch-to-buffer origin)
                  (pop-to-buffer first)
                  (pop-to-buffer second)
                  (quit-window)
                  (list (eq (current-buffer) first)
                        (eq (window-buffer (selected-window)) first)
                        (buffer-live-p second)))"#
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn quit_window_does_not_reselect_buffer_that_quit_to_restored_buffer() {
    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            r#"
                (let ((todo (get-buffer-create "*quit-todo*"))
                      (dir (get-buffer-create "*quit-dir*")))
                  (switch-to-buffer dir)
                  (switch-to-buffer todo)
                  (quit-window)
                  (let ((first-quit-buffer (current-buffer)))
                    (quit-window)
                    (list (eq first-quit-buffer dir)
                          (not (eq (current-buffer) todo))
                          (buffer-live-p todo)
                          (buffer-live-p dir))))"#
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn region_extension_function_variables_are_bound() {
    assert_eq!(
        eval_str(
            r#"
                (list (boundp 'region-extract-function)
                      (boundp 'region-insert-function)
                      (boundp 'redisplay-highlight-region-function)
                      (boundp 'redisplay-unhighlight-region-function))"#
        ),
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn built_in_prefix_keymaps_are_full_keymaps() {
    assert_eq!(
        eval_str(
            r#"
                (list (char-table-p (nth 1 esc-map))
                      (char-table-p (nth 1 ctl-x-map))
                      (char-table-p (nth 1 global-map)))"#
        ),
        Value::list([Value::T, Value::T, Value::T])
    );
}

#[test]
fn meta_prefix_char_defaults_to_escape() {
    assert_eq!(eval_str("meta-prefix-char"), Value::Integer(27));
}

#[test]
fn push_resolves_progn_place_once() {
    assert_eq!(
        eval_str(
            "(let ((events nil) (cell '(1)))
                   (push 0 (progn (push 'seen events) cell))
                   events)"
        ),
        Value::list([Value::Symbol("seen".into())])
    );
}

#[test]
fn seq_mapcat_flattens_sequence_results() {
    run_large_stack_test(assert_seq_mapcat_flattens_sequence_results);
}

fn assert_seq_mapcat_flattens_sequence_results() {
    assert_eq!(
        eval_str("(seq-mapcat 'list '(1 2 3))"),
        Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3),])
    );
}

#[test]
fn rx_define_registers_custom_atoms_for_rx() {
    assert_eq!(
        eval_str("(progn (rx-define sample-rx \"ab\") (rx sample-rx))"),
        Value::String("ab".into())
    );
}

#[test]
fn rx_repeat_supports_exact_repetition() {
    assert_eq!(
        eval_str("(rx (repeat 3 \"ab\"))"),
        Value::String("\\(?:ab\\)\\{3\\}".into())
    );
    assert_eq!(
        eval_str("(rx (= 3 digit))"),
        Value::String("\\(?:[0-9]\\)\\{3\\}".into())
    );
    assert_eq!(
        eval_str(
            r#"(list
                     (string-match-p (rx bos (= 3 digit) eos) "123")
                     (string-match-p (rx bos (= 3 digit) eos) "12")
                     (string-match-p (rx bos (= 3 digit) eos) "1234"))"#
        ),
        Value::list([Value::Integer(0), Value::Nil, Value::Nil])
    );
}

#[test]
fn rx_literal_evaluates_and_quotes_string_forms() {
    assert_eq!(
        eval_str(
            r#"
                (let ((needle "a.b"))
                  (list
                   (rx (literal needle))
                   (rx bol (literal (concat needle "?")) eol)
                   (string-match-p (rx (literal needle)) "a.b")
                   (string-match-p (rx (literal needle)) "axb")))
                "#
        ),
        Value::list([
            Value::String("a\\.b".into()),
            Value::String("^a\\.b\\?$".into()),
            Value::Integer(0),
            Value::Nil,
        ])
    );
}

#[test]
fn replace_match_returns_updated_string_for_string_targets() {
    assert_string_value(
        eval_str(
            r#"
                (let ((text "foo_${HOME}_bar"))
                  (string-match
                   "\\$\\(?:\\(?1:[[:alnum:]_]+\\)\\|{\\(?1:[^{}]+\\)}\\|\\$\\)"
                   text)
                  (replace-match "qux" t t text))"#,
        ),
        "foo_qux_bar",
    );
}

#[test]
fn with_environment_variables_binds_process_environment_dynamically() {
    assert_string_value(
        eval_str(
            r#"
                (progn
                  (defun emaxx-test-current-env (name)
                    (getenv-internal name))
                  (let ((name "EMAXX_DYNAMIC_ENV_TEST")
                        (value "value"))
                    (with-environment-variables ((name value))
                      (emaxx-test-current-env name))))"#,
        ),
        "value",
    );
}

#[test]
fn setenv_is_lisp_local_and_does_not_mutate_the_host_process() {
    let name = format!(
        "EMAXX_SETENV_HOST_ISOLATION_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    assert!(std::env::var_os(&name).is_none());

    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            &format!(
                r#"(let ((name {name:?}))
                     (list
                      (getenv name)
                      (let ((process-environment
                             (copy-sequence process-environment)))
                        (setenv name "lisp-only")
                        (getenv name))
                      (getenv name)))"#
            )
        ),
        Value::list([Value::Nil, Value::String("lisp-only".into()), Value::Nil])
    );
    assert!(std::env::var_os(&name).is_none());
}

#[test]
fn expand_file_name_uses_lisp_home_environment() {
    let home = std::env::temp_dir().join(format!(
        "emaxx-lisp-home-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let home = home.display().to_string();
    let expected_child = PathBuf::from(&home)
        .join("base")
        .join("child")
        .display()
        .to_string();

    let mut interp = Interpreter::new();
    assert_eq!(
        eval_str_with(
            &mut interp,
            &format!(
                r#"(let ((process-environment
                           (copy-sequence process-environment)))
                     (setenv "HOME" {home:?})
                     (list (expand-file-name "~")
                           (expand-file-name "child" "~/base/")))"#
            )
        ),
        Value::list([Value::String(home), Value::String(expected_child)])
    );
}

#[test]
fn missing_process_and_buffer_designators_are_non_live() {
    assert_eq!(
        eval_str(
            r#"(list (get-buffer-process " *emaxx-no-such-buffer*")
                      (process-live-p "emaxx-no-such-process")
                      (process-live-p 'undef)
                      (process-live-p 42)
                      (process-live-p nil))"#
        ),
        Value::list([Value::Nil, Value::Nil, Value::Nil, Value::Nil, Value::Nil,])
    );
}

#[test]
fn ert_selector_excludes_expensive_tests_by_tag() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        r#"
            (ert-deftest cheap-test ()
              (should t))
            (ert-deftest expensive-test ()
              :tags '(:expensive-test)
              (should t))
            "#,
    );
    let selector = Reader::new("(not (or (tag :expensive-test) (tag :unstable)))")
        .read_all()
        .unwrap()
        .remove(0);
    let summary = interp.run_ert_tests_with_selector(Some(&selector));
    assert_eq!(summary.total, 1);
    assert_eq!(interp.last_selected_tests, vec!["cheap-test".to_string()]);
}

#[test]
fn ert_deftest_evaluates_conditional_tag_expressions() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        r#"
            (progn
              (setenv "EMAXX_ERT_CONDITIONAL_TAG" nil)
              (ert-deftest conditional-tag-test ()
                :tags (and (null (getenv "EMAXX_ERT_CONDITIONAL_TAG"))
                           '(:unstable))
                (should t)))
            "#,
    );
    let selector = Reader::new("(not (tag :unstable))")
        .read_all()
        .unwrap()
        .remove(0);
    let summary = interp.run_ert_tests_with_selector(Some(&selector));
    assert_eq!(summary.total, 0);
    assert!(interp.last_selected_tests.is_empty());
    assert_eq!(interp.discovered_tests()[0].tags, vec![":unstable"]);
}

#[test]
fn ert_deftest_preserves_source_string_object_docstrings() {
    assert_string_value(
        eval_str(
            r#"(progn
                  (ert-deftest documented-source-test ()
                    "The source docstring survives reader object identity."
                    (should t))
                  (aref (get 'documented-source-test 'ert--test) 2))"#,
        ),
        "The source docstring survives reader object identity.",
    );
}

#[test]
fn should_error_checks_error_type() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        r#"
            (ert-deftest typed-error ()
              (should-error (car 1) :type 'wrong-type-argument))
            "#,
    );
    let summary = interp.run_ert_tests_with_selector(None);
    assert_eq!(summary.passed, 1);
    assert_eq!(summary.failed, 0);
}

#[test]
fn should_not_failures_report_ert_test_failed() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        r#"
            (ert-deftest should-not-failure ()
              (should-not t))
            "#,
    );
    let summary = interp.run_ert_tests_with_selector(None);
    assert_eq!(summary.failed, 1);
    assert_eq!(
        interp.test_results[0].condition_type.as_deref(),
        Some("ert-test-failed")
    );
}

#[test]
fn ert_runner_exposes_current_test_frame_to_backtrace_queries() {
    let mut interp = Interpreter::new();
    eval_str_with(
        &mut interp,
        r#"
            (ert-deftest backtrace-thread-frame ()
              (let* ((frames (backtrace--frames-from-thread (current-thread)))
                     (found nil))
                (dolist (frame frames)
                  (when (and (consp frame)
                             (memq (car frame) '(t nil))
                             (functionp (cadr frame)))
                    (setq found t)))
                (should found)))
            "#,
    );
    let summary = interp.run_ert_tests_with_selector(None);
    assert_eq!(summary.passed, 1);
    assert_eq!(summary.failed, 0);
}

#[test]
fn defalias_can_reference_incf_via_function_quote() {
    assert_eq!(
        eval_str(
            "(progn \
                   (defalias 'cl-incf #'incf) \
                   (let ((n 0)) \
                     (cl-incf n)))"
        ),
        Value::Integer(1)
    );
    assert_eq!(
        eval_str(
            "(progn \
                   (defalias 'cl-decf #'decf) \
                   (let ((n 2)) \
                     (cl-decf n)))"
        ),
        Value::Integer(1)
    );
}

#[test]
fn defalias_can_reference_list_primitives_via_function_quote() {
    assert_eq!(
        eval_str(
            "(progn
               (defalias 'sample-values #'list)
               (defalias 'sample-nth-value #'nth)
               (let ((vals (sample-values 2 3)))
                 (list (sample-nth-value 0 vals)
                       (sample-nth-value 1 vals)
                       (sample-nth-value 2 vals))))"
        ),
        Value::list([Value::Integer(2), Value::Integer(3), Value::Nil])
    );
}

#[test]
fn cl_lib_multiple_value_aliases_load_from_upstream() {
    assert_eq!(
        eval_str_with_upstream_load_path(
            "(progn
               (require 'cl-lib)
               (let ((vals (cl-values 2 3)))
                 (list (cl-nth-value 0 vals)
                       (cl-nth-value 1 vals)
                       (cl-nth-value 2 vals))))"
        ),
        Value::list([Value::Integer(2), Value::Integer(3), Value::Nil])
    );
}

#[test]
fn fset_can_define_function_aliases() {
    assert_eq!(
        eval_str("(progn (fset 'sample-head #'car) (sample-head '(1 2 3)))"),
        Value::Integer(1)
    );
}

#[test]
fn named_lisp_calls_share_immutable_function_code() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let definition = Reader::new("(defun emaxx-test-shared-function-code (value) (+ value 1))")
        .read()
        .expect("shared-code function should parse")
        .expect("shared-code definition should exist");
    interp
        .eval(&definition, &mut env)
        .expect("define the shared-code function");

    let first = interp
        .lookup_function("emaxx-test-shared-function-code", &env)
        .expect("first function lookup");
    let second = interp
        .lookup_function("emaxx-test-shared-function-code", &env)
        .expect("second function lookup");
    let (Value::Lambda(_, first_body, _), Value::Lambda(_, second_body, _)) = (&first, &second)
    else {
        panic!("named definition should remain a Lisp lambda");
    };
    assert!(
        std::rc::Rc::ptr_eq(first_body, second_body),
        "function lookup must share immutable code rather than cloning its AST"
    );

    let call = Reader::new("(emaxx-test-shared-function-code 41)")
        .read()
        .expect("shared-code call should parse")
        .expect("shared-code call should exist");
    for _ in 0..20_000 {
        assert_eq!(
            interp
                .eval(&call, &mut env)
                .expect("repeated shared-code call should execute"),
            Value::Integer(42)
        );
    }
}

#[test]
fn dumped_string_equal_alias_is_visible_to_metadata_consumers() {
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load simple compat");
    assert_eq!(
        eval_str_with(&mut interp, "(function-alias-p 'string=)"),
        Value::list([Value::Symbol("string-equal".into())])
    );
}

#[test]
fn uninterned_symbols_have_independent_function_cells() {
    assert_eq!(
        eval_str(
            "(let ((function-name (make-symbol \"G\")))
               (list (fboundp function-name)
                     (progn (fset function-name 'if)
                            (fboundp function-name))
                     (eq (indirect-function function-name)
                         (indirect-function 'if))))"
        ),
        Value::list([Value::Nil, Value::T, Value::T])
    );
}

#[test]
fn defalias_evaluates_symbol_definition_forms() {
    assert_eq!(
        eval_str(
            "(progn \
                   (defvar sample-definition 'car) \
                   (defalias 'sample-head sample-definition) \
                   (sample-head '(1 2 3)))"
        ),
        Value::Integer(1)
    );
}

#[test]
fn defalias_evaluates_computed_names_and_optional_docstrings() {
    assert_eq!(
        eval_str(
            r#"
                (let ((entry '(sample-computed-alias . ignored))
                      (documentation "computed alias documentation"))
                  (list
                   (defalias (car entry) #'ignore documentation)
                   (funcall (car entry))
                   (get (car entry) 'function-documentation)))
                "#
        ),
        Value::list([
            Value::Symbol("sample-computed-alias".into()),
            Value::Nil,
            Value::String("computed alias documentation".into()),
        ])
    );
}

#[test]
fn function_quote_allows_forward_symbol_references() {
    assert_eq!(
        eval_str(
            "(progn
                   (defvar before-change-functions nil)
                   (add-hook 'before-change-functions #'syntax-ppss-flush-cache)
                   (defun syntax-ppss-flush-cache (&rest _) 'ok)
                   (funcall (car before-change-functions)))"
        ),
        Value::Symbol("ok".into())
    );
}

#[test]
fn functionp_and_funcall_accept_quoted_lambda_expressions() {
    assert_eq!(
        eval_str(
            "(list
                   (functionp '(lambda () t))
                   (cl-functionp '(lambda () t))
                   (funcall '(lambda (value) (concat value \"bar\")) \"foo\"))"
        ),
        Value::list([Value::T, Value::T, Value::String("foobar".into())])
    );
}

#[test]
fn functionp_accepts_function_autoload_symbols_but_not_macros_or_special_forms() {
    assert_eq!(
        eval_str(
            "(progn
               (autoload 'sample-autoloaded-function \"sample-function\")
               (autoload 'sample-autoloaded-macro
                         \"sample-macro\" nil nil 'macro)
               (list (functionp 'sample-autoloaded-function)
                     (functionp 'sample-autoloaded-macro)
                     (functionp 'if)))"
        ),
        Value::list([Value::T, Value::Nil, Value::Nil])
    );
}

#[test]
fn upstream_script_modes_own_their_derived_mode_contracts() {
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
               (with-temp-buffer
                 (sh-mode)
                 (list major-mode
                       (derived-mode-p 'sh-base-mode)
                       (subrp (indirect-function 'sh-mode))
                       (local-variable-p 'sh-shell)))
               (with-temp-buffer
                 (python-mode)
                 (list major-mode
                       (derived-mode-p 'python-base-mode)
                       (subrp (indirect-function 'python-mode))))
               (list
                (cdr (assoc \"python[0-9.]*\" interpreter-mode-alist))
                (cdr (assoc \"gawk\" interpreter-mode-alist))))",
        ),
        Value::list([
            Value::list([
                Value::Symbol("sh-mode".into()),
                Value::T,
                Value::Nil,
                Value::T,
            ]),
            Value::list([Value::Symbol("python-mode".into()), Value::T, Value::Nil,]),
            Value::list([
                Value::Symbol("python-mode".into()),
                Value::Symbol("awk-mode".into()),
            ]),
        ])
    );
}

#[test]
fn treesit_language_available_defaults_to_nil() {
    assert_eq!(eval_str("(treesit-language-available-p 'json)"), Value::Nil);
}

#[test]
fn treesit_linecol_helpers_report_positions() {
    assert_eq!(
        eval_str(
            "(with-temp-buffer
                   (insert \"a\\n\")
                   (treesit--linecol-cache-set 1 0 1)
                   (list (treesit--linecol-cache)
                         (treesit--linecol-at 2)
                         (treesit--linecol-at 3)))"
        ),
        Value::list([
            Value::list([
                Value::Symbol(":line".into()),
                Value::Integer(1),
                Value::Symbol(":col".into()),
                Value::Integer(0),
                Value::Symbol(":bytepos".into()),
                Value::Integer(1),
            ]),
            Value::cons(Value::Integer(1), Value::Integer(1)),
            Value::cons(Value::Integer(2), Value::Integer(0)),
        ])
    );
}

#[test]
fn copy_tree_preserves_nested_list_structure() {
    assert_eq!(
        eval_str("(copy-tree '((a . b) (c d)))"),
        Value::list([
            Value::cons(Value::Symbol("a".into()), Value::Symbol("b".into())),
            Value::list([Value::Symbol("c".into()), Value::Symbol("d".into())]),
        ])
    );
}

#[test]
fn copy_tree_does_not_alias_mutable_cons_cells() {
    assert_eq!(
        eval_str(
            "(let* ((orig '((a . b) (c d)))
                        (copy (copy-tree orig)))
                   (setcdr (car copy) 'z)
                   (list orig copy))"
        ),
        Value::list([
            Value::list([
                Value::cons(Value::Symbol("a".into()), Value::Symbol("b".into())),
                Value::list([Value::Symbol("c".into()), Value::Symbol("d".into())]),
            ]),
            Value::list([
                Value::cons(Value::Symbol("a".into()), Value::Symbol("z".into())),
                Value::list([Value::Symbol("c".into()), Value::Symbol("d".into())]),
            ]),
        ])
    );
}

#[test]
fn pop_supports_generalized_places() {
    assert_eq!(
        eval_str(
            "(let ((xs (list 1 2 3)))
                   (list (pop (cdr xs)) xs))"
        ),
        Value::list([
            Value::Integer(2),
            Value::list([Value::Integer(1), Value::Integer(3)]),
        ])
    );
}

#[test]
fn pcase_seq_pattern_supports_seq_let_rest() {
    run_with_large_stack(|| {
        let mut interp = Interpreter::new();
        interp.set_load_path(
            crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
        );
        let _ = interp.load_target("seq");

        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(seq-let (beg end table &rest plist)
                       '(1 4 ("foobarbaz") :display-sort-function identity)
                     (list beg end table plist))"#
            ),
            Value::list([
                Value::Integer(1),
                Value::Integer(4),
                Value::list([Value::String("foobarbaz".into())]),
                Value::list([
                    Value::Symbol(":display-sort-function".into()),
                    Value::Symbol("identity".into()),
                ]),
            ])
        );
    });
}
