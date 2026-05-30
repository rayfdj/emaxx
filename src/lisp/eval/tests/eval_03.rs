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
                r#"
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
                    "#
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
                r#"
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
                    "#
            ),
            Value::Nil
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
            Value::Integer(24),
            Value::Integer(120),
            Value::Integer(40),
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
        Value::list([Value::Integer(40), Value::Integer(40)])
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
                Value::Integer(0),
                Value::Integer(120),
                Value::Integer(40),
            ]),
            Value::list([
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(120),
                Value::Integer(40),
            ]),
            Value::list([
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(120),
                Value::Integer(40),
            ]),
            Value::list([
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(960),
                Value::Integer(640),
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
                       (define-key query-replace-map \" \" 'ignore))"
        ),
        Value::list([Value::T, Value::Symbol("ignore".into())])
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
                    (define-key map [remap display-buffer-other-frame] 'demo-display)
                    (setq sample-mode-map-entry (cons 'sample-mode map))
                    (add-to-list 'minor-mode-map-alist sample-mode-map-entry)
                    (key-binding (kbd "C-x 5 C-o"))))
                "#
        ),
        Value::Symbol("demo-display".into())
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
                Value::Symbol("t".into()),
            ]),
            Value::list([Value::Symbol("sample-child".into())]),
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
                Value::Symbol("t".into()),
            ]),
            Value::T,
        ])
    );
}

#[test]
fn defclass_registers_instance_predicate() {
    assert_eq!(
        eval_str(
            "(progn
                   (defclass sample-parent nil nil)
                   (defclass sample-child (sample-parent) nil)
                   (let ((child (make-instance 'sample-child)))
                     (list (sample-child-p child)
                           (sample-parent-p child)
                           (sample-child-p 'not-an-object))))"
        ),
        Value::list([Value::T, Value::T, Value::Nil])
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
fn bindat_pack_val_round_trips_integer_representation() {
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
}

#[test]
fn bindat_recursive_leb128_round_trips_integers() {
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
}

#[test]
fn bindat_signed_integer_types_round_trip_wide_values() {
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
                (let* ((bitlen 72)
                       (stype (bindat-type sint bitlen nil))
                       (values (list -1 0 42 (1- (ash 1 63)) (- (ash 1 63)))))
                  (cl-loop for n in values
                           always (equal (bindat-unpack stype (bindat-pack stype n)) n)))
                "#
        ),
        Value::T
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
fn oclosure_lambda_lowers_to_plain_lambda() {
    assert_eq!(
        eval_str("(funcall (oclosure-lambda (sample-type) (x) x) 7)"),
        Value::Integer(7)
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
fn fset_can_define_function_aliases() {
    assert_eq!(
        eval_str("(progn (fset 'sample-head #'car) (sample-head '(1 2 3)))"),
        Value::Integer(1)
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
fn preloaded_sh_mode_sets_imenu_configuration() {
    let value = eval_str(
        "(with-temp-buffer
               (funcall #'sh-mode)
               (list major-mode
                     mode-name
                     imenu-case-fold-search
                     imenu-create-index-function
                     imenu-generic-expression))",
    );
    let items = value.to_vec().unwrap();
    assert_eq!(items[0], Value::Symbol("sh-mode".into()));
    assert_string_value(items[1].clone(), "Shell-script");
    assert_eq!(items[2], Value::Nil);
    assert_eq!(
        items[3],
        Value::Symbol("imenu-default-create-index-function".into())
    );
    assert_eq!(
        items[4],
        Value::list([
            Value::list([
                Value::Nil,
                Value::String("^[ \t]*function[ \t]+\\([A-Za-z_][A-Za-z0-9_]*\\)".into()),
                Value::Integer(1),
            ]),
            Value::list([
                Value::Nil,
                Value::String("^[ \t]*\\([A-Za-z_][A-Za-z0-9_]*\\)[ \t]*()".into()),
                Value::Integer(1),
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
}
