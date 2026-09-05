use super::*;

pub(super) fn gnu_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../emacs")
        .canonicalize()
        .expect("the unchanged GNU checkout is available")
}

pub(super) fn source_callback(
    interp: &mut Interpreter,
    code: &[u8],
    constants: Vec<Value>,
) -> Value {
    super::super::call(
        interp,
        "make-byte-code",
        &[
            Value::Integer(1028), // bytecode.c: four mandatory, four non-rest arguments.
            crate::lisp::primitives::coding::bytes_to_unibyte_value(code),
            Value::vector(constants),
            Value::Integer(8),
        ],
        &mut Env::new(),
    )
    .expect("construct source callback from GNU bytecode instructions")
}

#[test]
fn eval_buffer_uses_the_supplied_history_filename_without_visiting_it() {
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    interp.buffer.file = Some("buffer-visited-name.el".into());
    let filename = Value::string("explicit-history-name.el");
    let outer = Value::list([Value::string("outer-load.el")]);
    interp.set_variable("current-load-list", outer.clone(), &mut env);
    let result = super::super::call(
        &mut interp,
        "eval-buffer",
        &[Value::Nil, Value::Nil, filename.clone()],
        &mut env,
    )
    .expect("an empty buffer still records its explicit history filename");
    assert_eq!(result, Value::Nil);
    let history = interp
        .lookup_var("load-history", &env)
        .expect("load history");
    assert_eq!(
        history.car().expect("new history entry"),
        Value::list([filename])
    );
    assert!(values_eq_in_env(
        &interp,
        &interp
            .lookup_var("current-load-list", &env)
            .expect("restored outer load list"),
        &outer,
        &env,
    ));
    assert_eq!(
        interp.buffer.file.as_deref(),
        Some("buffer-visited-name.el")
    );
}

#[test]
fn eval_buffer_checks_a_non_nil_history_filename_even_for_an_empty_buffer() {
    let mut interp = Interpreter::new();
    let result = super::super::call(
        &mut interp,
        "eval-buffer",
        &[Value::Nil, Value::Nil, Value::Integer(42)],
        &mut Env::new(),
    );
    assert!(
        matches!(
            result,
            Err(LispError::WrongTypeArgument(predicate, Value::Integer(42))) if predicate == "stringp"
        ),
        "readevalloop checks a non-nil source name before reading any forms"
    );
}

#[test]
fn eval_buffer_nil_filename_records_an_independent_nil_history_entry() {
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    let outer = Value::list([Value::string("outer-file.el")]);
    interp.set_variable("current-load-list", outer.clone(), &mut env);
    for _ in 0..2 {
        super::super::call(&mut interp, "eval-buffer", &[], &mut env)
            .expect("nil filename is a valid read/eval-loop source name");
        assert_eq!(
            interp.lookup_var("load-history", &env),
            Some(Value::list([Value::list([Value::Nil]),]))
        );
        assert!(values_eq_in_env(
            &interp,
            &interp
                .lookup_var("current-load-list", &env)
                .expect("restored outer load list"),
            &outer,
            &env,
        ));
    }
}

#[test]
fn eval_buffer_loads_unchanged_gnu_source_before_the_macroexpander_is_defined() {
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Env::new();
    assert_eq!(
        super::super::call(
            &mut interp,
            "fboundp",
            &[Value::symbol("internal-macroexpand-for-load")],
            &mut env,
        )
        .expect("check the early-runtime precondition"),
        Value::Nil
    );
    let source = fs::read_to_string(gnu_root().join("test/src/comp-resources/comp-test-45603.el"))
        .expect("read unchanged GNU source");
    interp.buffer.insert(&source);
    super::super::call(&mut interp, "eval-buffer", &[], &mut env)
        .expect("readevalloop directly evaluates when the eager owner is undefined");
    assert!(interp.has_feature("comp-test-45603"));
}

#[test]
fn eval_buffer_uses_the_history_suffix_to_disable_eager_macroexpansion() {
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Env::new();
    // A defined eager owner that signals proves it was not called. This is
    // an ordinary C defalias to a C primitive, not an authored Lisp helper.
    super::super::call(
        &mut interp,
        "defalias",
        &[
            Value::symbol("internal-macroexpand-for-load"),
            Value::symbol("error"),
        ],
        &mut env,
    )
    .expect("install the deliberately failing callback");
    let source = fs::read_to_string(gnu_root().join("test/src/comp-resources/comp-test-45603.el"))
        .expect("read unchanged GNU source");
    interp.buffer.insert(&source);
    interp.buffer.file = Some("visited-source.el".into());
    super::super::call(
        &mut interp,
        "eval-buffer",
        &[
            Value::Nil,
            Value::Nil,
            Value::string("supplied-history.elc"),
        ],
        &mut env,
    )
    .expect("a .elc source name disables the eager owner even for a .el buffer");
    assert!(interp.has_feature("comp-test-45603"));
}

#[test]
fn load_source_callback_receives_gnu_arguments_and_owns_the_return_value() {
    let directory = gnu_root().join("test/src/comp-resources");
    let file = directory.join("comp-test-45603.el");
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    interp.set_load_path(vec![directory]);
    interp.set_variable("load-source-file-function", Value::symbol("list"), &mut env);
    for (purifying, force) in [(false, false), (true, false), (false, true)] {
        interp.set_variable(
            "purify-flag",
            if purifying { Value::T } else { Value::Nil },
            &mut env,
        );
        interp.set_variable(
            "force-load-messages",
            if force { Value::T } else { Value::Nil },
            &mut env,
        );
        let actual = super::super::call(
            &mut interp,
            "load",
            &[
                Value::string("comp-test-45603.el"),
                Value::Integer(7),
                Value::Integer(9),
            ],
            &mut env,
        )
        .expect("source loader returns its callback result");
        let fullname = Value::string(&file.display().to_string());
        assert_eq!(
            actual,
            Value::list([
                fullname.clone(),
                if purifying {
                    Value::string("comp-test-45603.el")
                } else {
                    fullname
                },
                Value::T,
                if force { Value::Nil } else { Value::T },
            ])
        );
    }
    assert!(
        !interp.has_feature("comp-test-45603"),
        "the callback, not a second Rust reader, owns source execution"
    );
}

#[test]
fn load_source_callback_observes_and_restores_the_outer_c_bindings() {
    let file = gnu_root().join("test/src/comp-resources/comp-test-45603.el");
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    let old_warning = Value::list([Value::Integer(63)]);
    interp.set_variable("lexical-binding", Value::T, &mut env);
    interp.set_variable(
        "lread--unescaped-character-literals",
        old_warning.clone(),
        &mut env,
    );
    let callback = source_callback(
        &mut interp,
        &[8, 9, 68, 135],
        vec![
            Value::symbol("lexical-binding"),
            Value::symbol("lread--unescaped-character-literals"),
        ],
    );
    interp.set_variable("load-source-file-function", callback, &mut env);
    let result = super::super::call(
        &mut interp,
        "load",
        &[
            Value::string(&file.display().to_string()),
            Value::Nil,
            Value::T,
        ],
        &mut env,
    )
    .expect("GNU source callback executes inside C-owned bindings");
    assert_eq!(result, Value::list([Value::Nil, Value::Nil]));
    assert_eq!(interp.lookup_var("lexical-binding", &env), Some(Value::T));
    assert_eq!(
        interp.lookup_var("lread--unescaped-character-literals", &env),
        Some(old_warning)
    );
}

#[test]
fn load_source_callback_nonlocal_exit_unwinds_even_with_noerror() {
    let file = gnu_root().join("test/src/comp-resources/comp-test-45603.el");
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    let tag = Value::symbol("load-exit");
    let callback = source_callback(
        &mut interp,
        &[192, 193, 194, 34, 135],
        vec![Value::symbol("throw"), tag.clone(), Value::Integer(23)],
    );
    interp.set_variable("load-source-file-function", callback, &mut env);
    interp.set_variable("lexical-binding", Value::T, &mut env);
    interp.push_active_catch_tag(tag.clone());
    let result = super::super::call(
        &mut interp,
        "load",
        &[
            Value::string(&file.display().to_string()),
            Value::T,
            Value::T,
        ],
        &mut env,
    );
    interp.pop_active_catch_tag();
    assert!(
        matches!(result, Err(LispError::Throw(actual, Value::Integer(23))) if values_eql(&actual, &tag))
    );
    assert_eq!(interp.lookup_var("lexical-binding", &env), Some(Value::T));
    assert!(interp.loads_in_progress.is_nil());
}

#[test]
fn load_source_callback_reads_the_detached_c_slot() {
    let file = gnu_root().join("test/src/comp-resources/comp-test-45603.el");
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    interp.set_variable("load-source-file-function", Value::symbol("list"), &mut env);
    super::super::call(
        &mut interp,
        "makunbound",
        &[Value::symbol("load-source-file-function")],
        &mut env,
    )
    .expect("detach the Lisp symbol from the C field");
    interp.set_symbol_value_cell("load-source-file-function", Value::Nil);
    let fullname = Value::string(&file.display().to_string());
    let result = super::super::call(
        &mut interp,
        "load",
        &[fullname.clone(), Value::Nil, Value::T],
        &mut env,
    )
    .expect("Fload reads Vload_source_file_function, not the detached symbol");
    assert_eq!(
        result,
        Value::list([fullname.clone(), fullname, Value::Nil, Value::T])
    );
    assert!(interp.loads_in_progress.is_nil());
}

#[test]
fn load_recursion_limit_counts_only_the_same_file_and_restores_the_stack() {
    let file = Value::string(
        &gnu_root()
            .join("test/src/comp-resources/comp-test-45603.el")
            .display()
            .to_string(),
    );
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    interp.set_variable("load-source-file-function", Value::symbol("list"), &mut env);
    for count in [3, 4] {
        let saved = Value::list(
            std::iter::repeat_n(file.clone(), count).chain([Value::string("unrelated-file.el")]),
        );
        interp.loads_in_progress = saved.clone();
        let result = super::super::call(
            &mut interp,
            "load",
            &[file.clone(), Value::T, Value::T],
            &mut env,
        );
        if count == 3 {
            assert!(
                result.is_ok(),
                "GNU permits three previous occurrences: {result:?}"
            );
        } else {
            let Err(LispError::SignalValue(error)) = result else {
                panic!("four previous occurrences must signal even with NOERROR");
            };
            assert_eq!(
                error,
                Value::list([
                    Value::symbol("error"),
                    Value::string("Recursive load"),
                    Value::cons(file.clone(), saved.clone()),
                ])
            );
        }
        assert!(values_eq_in_env(
            &interp,
            &interp.loads_in_progress,
            &saved,
            &env
        ));
    }
}

#[test]
fn load_search_obeys_the_gnu_suffix_list_instead_of_a_private_vm_preference() {
    let root = gnu_root().join("lisp/emacs-lisp");
    for suffix in [".elc", ".el"] {
        assert!(root.join(format!("seq{suffix}")).is_file());
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        interp.set_load_path(vec![root.clone()]);
        interp.set_variable(
            "load-suffixes",
            Value::list([Value::string(suffix)]),
            &mut env,
        );
        assert_eq!(
            resolve_load_target_in_env(&mut interp, "seq", &env).expect("search GNU suffixes"),
            Some(root.join(format!("seq{suffix}"))),
            "Fload takes its suffixes from Fget_load_suffixes"
        );
    }
}

#[test]
fn load_search_does_not_rewrite_a_missing_repeated_directory_filename() {
    let mut interp = Interpreter::new();
    interp.set_load_path(vec![gnu_root().join("lisp")]);
    assert_eq!(
        resolve_load_target_in_env(&mut interp, "emacs-lisp/emacs-lisp-seq.el", &Env::new())
            .expect("search missing name"),
        None,
        "openp never rewrites this missing name to emacs-lisp/seq.el"
    );
}

#[test]
fn load_search_expands_home_from_the_lisp_process_environment() {
    let root = gnu_root();
    let relative = "test/src/comp-resources/comp-test-45603.el";
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    interp.set_load_path(vec![root.clone()]);
    interp.set_variable(
        "process-environment",
        Value::list([Value::string(&format!("HOME={}", root.display()))]),
        &mut env,
    );
    assert_eq!(
        resolve_load_target_in_env(&mut interp, &format!("~/{relative}"), &env)
            .expect("expand and search"),
        Some(root.join(relative)),
        "openp calls Fexpand_file_name rather than opening the raw tilde name"
    );
}

#[test]
fn load_search_nil_path_uses_the_current_buffers_default_directory() {
    let root = gnu_root().join("test/src/comp-resources");
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    interp.set_variable("load-path", Value::Nil, &mut env);
    interp.set_variable(
        "default-directory",
        Value::string(&format!("{}/", root.display())),
        &mut env,
    );
    assert_eq!(
        resolve_load_target_in_env(&mut interp, "comp-test-45603.el", &env)
            .expect("use buffer directory"),
        Some(root.join("comp-test-45603.el")),
        "openp's nil-path branch expands against the buffer, not process cwd"
    );
}

#[test]
fn openp_t_predicate_opens_the_file_instead_of_calling_t_as_a_function() {
    let file = gnu_root().join("test/src/comp-resources/comp-test-45603.el");
    let mut interp = Interpreter::new();
    assert_eq!(
        locate_file_internal(
            &mut interp,
            &Value::string(&file.display().to_string()),
            &Value::Nil,
            &Value::Nil,
            &Value::T,
            &mut Env::new(),
        )
        .expect("openp's binary-file predicate does not invoke Lisp"),
        Value::string(&file.display().to_string())
    );
}

#[test]
fn openp_function_predicate_skips_a_directory_without_dir_ok() {
    let directory = gnu_root().join("test/src/comp-resources");
    let mut interp = Interpreter::new();
    assert_eq!(
        locate_file_internal(
            &mut interp,
            &Value::string(&directory.display().to_string()),
            &Value::Nil,
            &Value::Nil,
            &Value::symbol("identity"),
            &mut Env::new(),
        )
        .expect("a truthy predicate alone does not admit directories"),
        Value::Nil
    );
}

#[test]
fn openp_directory_predicate_must_return_the_dir_ok_symbol() {
    let directory = Value::string(
        &gnu_root()
            .join("test/src/comp-resources")
            .display()
            .to_string(),
    );
    let mut interp = Interpreter::new();
    // bytecode.c: one mandatory/non-rest argument (257), constant 0, return.
    let predicate = super::super::call(
        &mut interp,
        "make-byte-code",
        &[
            Value::Integer(257),
            crate::lisp::primitives::coding::bytes_to_unibyte_value(&[192, 135]),
            Value::vector(vec![Value::symbol("dir-ok")]),
            Value::Integer(2),
        ],
        &mut Env::new(),
    )
    .expect("construct ordinary bytecode predicate");
    let found = locate_file_internal(
        &mut interp,
        &directory,
        &Value::Nil,
        &Value::Nil,
        &predicate,
        &mut Env::new(),
    )
    .expect("dir-ok permits the directory");
    assert_eq!(found, directory);
}

#[test]
fn openp_validates_all_suffix_cars_but_does_not_require_a_proper_list() {
    let file = Value::string(
        &gnu_root()
            .join("test/src/comp-resources/comp-test-45603.el")
            .display()
            .to_string(),
    );
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    let suffixes = Value::list([Value::string(""), Value::Integer(7)]);
    assert!(matches!(
        locate_file_internal(
            &mut interp,
            &file,
            &Value::Nil,
            &suffixes,
            &Value::Nil,
            &mut env
        ),
        Err(LispError::WrongTypeArgument(_, _)) | Err(LispError::TypeError(_, _))
    ));
    let suffixes = Value::cons(Value::string(""), Value::Integer(7));
    assert_eq!(
        locate_file_internal(
            &mut interp,
            &file,
            &Value::Nil,
            &suffixes,
            &Value::Nil,
            &mut env
        )
        .expect("FOR_EACH_TAIL_SAFE stops at a non-cons tail"),
        file
    );
    assert!(
        locate_file_internal(
            &mut interp,
            &file,
            &Value::Nil,
            &Value::string(""),
            &Value::Nil,
            &mut env
        )
        .expect("non-list suffixes have no conses to search")
        .is_nil()
    );
}

#[test]
fn load_suffix_product_reads_c_slots_and_obeys_gnu_tail_iteration() {
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    // concat2 accepts character sequences, not just strings.
    interp.set_variable(
        "load-suffixes",
        Value::cons(
            Value::list([Value::Integer(46), Value::Integer(101), Value::Integer(108)]),
            Value::Integer(7),
        ),
        &mut env,
    );
    interp.set_variable(
        "load-file-rep-suffixes",
        Value::cons(Value::string(""), Value::T),
        &mut env,
    );
    assert_eq!(
        get_load_suffixes_value(&mut interp, &mut env).expect("GNU concat2 product"),
        Value::list([Value::string(".el")])
    );
    for name in ["load-suffixes", "load-file-rep-suffixes"] {
        super::super::call(&mut interp, "makunbound", &[Value::symbol(name)], &mut env)
            .expect("detach public symbol from C slot");
        interp.set_symbol_value_cell(name, Value::Nil);
    }
    assert_eq!(
        get_load_suffixes_value(&mut interp, &mut env).expect("read detached C slots"),
        Value::list([Value::string(".el")])
    );
}

#[test]
fn locate_file_internal_accepts_its_two_required_arguments() {
    let file = Value::string(
        &gnu_root()
            .join("test/src/comp-resources/comp-test-45603.el")
            .display()
            .to_string(),
    );
    let mut interp = Interpreter::new();
    assert_eq!(
        super::super::call(
            &mut interp,
            "locate-file-internal",
            &[file.clone(), Value::Nil],
            &mut Env::new()
        )
        .expect("GNU arity is 2..4"),
        file
    );
}

#[test]
fn load_search_newer_stays_in_first_directory_and_preserves_suffix_ties() {
    let root = std::env::temp_dir().join(format!(
        "load-search-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let first = root.join("first");
    let second = root.join("second");
    fs::create_dir_all(&first).expect("create isolated fixture directory");
    fs::create_dir(&second).expect("create second fixture directory");
    let source = gnu_root().join("test/src/comp-resources/comp-test-45603.el");
    for (path, seconds) in [
        (first.join("sample.elc"), 10),
        (first.join("sample.el"), 20),
        (second.join("sample.el"), 30),
    ] {
        fs::copy(&source, &path).expect("copy unchanged GNU bytes; only searching, not executing");
        fs::File::open(path)
            .expect("open fixture")
            .set_times(
                fs::FileTimes::new()
                    .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds)),
            )
            .expect("set deterministic fixture mtime");
    }
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    interp.set_load_path(vec![first.clone(), second]);
    interp.set_variable("load-prefer-newer", Value::T, &mut env);
    assert_eq!(
        resolve_load_target_in_env(&mut interp, "sample", &env).expect("newest in first entry"),
        Some(first.join("sample.el"))
    );
    fs::File::open(first.join("sample.elc"))
        .expect("open fixture")
        .set_times(
            fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(20)),
        )
        .expect("tie mtimes");
    assert_eq!(
        resolve_load_target_in_env(&mut interp, "sample", &env).expect("earlier suffix wins tie"),
        Some(first.join("sample.elc"))
    );
    interp.set_variable("load-prefer-newer", Value::Nil, &mut env);
    assert_eq!(
        resolve_load_target_in_env(&mut interp, "sample", &env).expect("normal suffix order"),
        Some(first.join("sample.elc"))
    );
    assert!(
        resolve_load_file_in_env(&mut interp, "sample", &env, true, false)
            .expect("nosuffix")
            .0
            .is_none()
    );
    fs::copy(&source, first.join("plain")).expect("copy unchanged GNU file without a suffix");
    assert!(
        resolve_load_file_in_env(&mut interp, "plain", &env, false, true)
            .expect("mustsuffix")
            .0
            .is_none()
    );
    assert_eq!(
        resolve_load_file_in_env(&mut interp, "./plain", &env, false, true)
            .expect("directory component cancels mustsuffix")
            .0,
        Some(first.join("plain"))
    );
    fs::remove_dir_all(root).expect("remove only this test's isolated fixture copies");
}
