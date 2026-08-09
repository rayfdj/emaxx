use super::*;
use crate::lisp::reader::Reader;
use std::io::Write;

fn upstream_emacs_repo() -> PathBuf {
    crate::compat::project_root().join("../emacs")
}

fn assert_upstream_primitive_contract(program: &str, expected: &str) {
    let binary = upstream_emacs_repo().join("src/emacs");
    let output = std::process::Command::new(&binary)
        .args(["--batch", "-Q", "--eval", program])
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "run primitive-contract oracle {}: {error}",
                binary.display()
            )
        });
    assert!(
        output.status.success(),
        "primitive-contract oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout), expected);
}

fn assert_upstream_primitive_contract_with_stdin(program: &str, stdin: &str, expected: &str) {
    let binary = upstream_emacs_repo().join("src/emacs");
    let mut child = std::process::Command::new(&binary)
        .args(["--batch", "-Q", "--eval", program])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| {
            panic!(
                "run primitive-contract oracle {}: {error}",
                binary.display()
            )
        });
    child
        .stdin
        .as_mut()
        .expect("GNU primitive-contract stdin should be piped")
        .write_all(stdin.as_bytes())
        .expect("write GNU primitive-contract stdin");
    let output = child
        .wait_with_output()
        .expect("wait for GNU primitive-contract oracle");
    assert!(
        output.status.success(),
        "primitive-contract oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout), expected);
}

#[test]
fn record_literal_detection_does_not_traverse_vector_storage() {
    let vector = Value::list([
        Value::symbol("vector-literal"),
        Value::Integer(1),
        Value::Integer(2),
    ]);
    let (_, tail) = vector
        .cons_cells()
        .expect("the vector facade should have a tagged cons root");
    let _exclusive_tail_borrow = tail.borrow_mut();

    // Holding the tail exclusively makes any attempted traversal panic.
    // Record detection must reject the vector solely from its distinct tag.
    assert!(record_literal_items(&vector).is_none());
}

#[test]
fn compare_buffer_substrings_accepts_current_buffer_and_bounds_as_nil() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    call(
        &mut interp,
        "insert",
        &[Value::String("abcabc".into())],
        &mut env,
    )
    .expect("insert comparison text");

    assert_eq!(
        call(
            &mut interp,
            "compare-buffer-substrings",
            &[
                Value::Nil,
                Value::Nil,
                Value::Integer(4),
                Value::Nil,
                Value::Integer(4),
                Value::Nil,
            ],
            &mut env,
        )
        .expect("compare current-buffer halves"),
        Value::Integer(0)
    );
}

#[test]
fn redisplay_defaults_match_native_terminal_and_input_state() {
    let mut interp = Interpreter::new();
    let form = Reader::new(
        "(list baud-rate
               (let ((baud-rate 9600)) baud-rate)
               baud-rate
               (= (window-start nil) (window-start))
               (= (window-end nil t) (window-end))
               input-method-function
               (let ((input-method-function nil)) input-method-function)
               input-method-function)",
    )
    .read()
    .expect("read redisplay defaults probe")
    .expect("redisplay defaults probe should contain one form");

    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate redisplay defaults probe"),
        Value::list([
            Value::Integer(0),
            Value::Integer(9600),
            Value::Integer(0),
            Value::T,
            Value::T,
            Value::Symbol("list".into()),
            Value::Nil,
            Value::Symbol("list".into()),
        ])
    );
}

#[test]
fn sort_recognizes_an_evaluated_numeric_lambda_without_interpreting_each_comparison() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new("(lambda (x y) (< x y))")
        .read()
        .expect("the comparator should parse")
        .expect("the comparator form should be present");
    let comparator = interp
        .eval(&form, &mut env)
        .expect("the comparator should evaluate");

    assert!(
        direct_sort_comparator(&interp, &comparator, &env).is_some(),
        "ordinary numeric comparator was not recognized: {comparator:?}"
    );
}

#[test]
fn native_kill_emacs_is_noncatchable_runs_hooks_and_preserves_c_exit_mapping() {
    let mut interp = Interpreter::new();
    let form = Reader::new(
        r#"(progn
             (setq emaxx-kill-seen nil)
             (setq kill-emacs-hook
                   (list
                    (lambda ()
                      (setq emaxx-kill-seen
                            (cons 'first emaxx-kill-seen)))
                    (lambda () (error "ignored shutdown error"))
                    (lambda ()
                      (setq emaxx-kill-seen
                            (cons 'third emaxx-kill-seen)))))
             (unwind-protect
                 (condition-case nil
                     (kill-emacs 7)
                   (t
                    (setq emaxx-kill-seen
                          (cons 'caught emaxx-kill-seen))))
               (setq emaxx-kill-seen
                     (cons 'cleanup emaxx-kill-seen))))"#,
    )
    .read_all()
    .expect("read kill-emacs nonlocal-control contract")
    .remove(0);
    let error = interp
        .eval(&form, &mut Vec::new())
        .expect_err("kill-emacs must not return into Lisp");
    assert!(matches!(
        error,
        LispError::Terminate(EmacsTermination {
            exit_code: 7,
            restart: false
        })
    ));
    assert_eq!(
        interp.lookup_var("emaxx-kill-seen", &Vec::new()),
        Some(Value::list([
            Value::symbol("third"),
            Value::symbol("first")
        ])),
        "ordinary hook errors are demoted, while condition handlers and unwind cleanup never run"
    );
    assert_eq!(
        interp.take_pending_termination(),
        Some(EmacsTermination {
            exit_code: 7,
            restart: false,
        })
    );

    let termination_for = |args: &[Value]| {
        let mut interp = Interpreter::new();
        match call(&mut interp, "kill-emacs", args, &mut Vec::new())
            .expect_err("native kill-emacs must request process termination")
        {
            LispError::Terminate(termination) => termination,
            other => panic!("unexpected kill-emacs outcome: {other}"),
        }
    };
    assert_eq!(
        termination_for(&[]),
        EmacsTermination {
            exit_code: 0,
            restart: false,
        }
    );
    assert_eq!(
        termination_for(&[Value::Integer(-1)]),
        EmacsTermination {
            exit_code: -1,
            restart: false,
        }
    );
    assert_eq!(
        termination_for(&[Value::Integer(i64::MAX)]),
        EmacsTermination {
            exit_code: i32::MAX,
            restart: false,
        }
    );
    assert_eq!(
        termination_for(&[Value::big_integer(BigInt::from(i64::MAX) + 1)]),
        EmacsTermination {
            exit_code: 0,
            restart: false,
        },
        "emacs.c uses FIXNUMP, so a bignum is not an exit status"
    );
    assert_eq!(
        termination_for(&[Value::String("terminal input".into()), Value::T]),
        EmacsTermination {
            exit_code: 0,
            restart: true,
        }
    );
}

#[test]
fn native_user_ptr_predicate_is_exhaustive_over_the_module_free_value_model() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let string_object = interp
        .eval(
            &Reader::new("\"heap string\"")
                .read_all()
                .expect("read string literal")
                .remove(0),
            &mut env,
        )
        .expect("evaluate string literal");
    let lambda = interp
        .eval(
            &Reader::new("(lambda (value) value)")
                .read_all()
                .expect("read lambda")
                .remove(0),
            &mut env,
        )
        .expect("evaluate lambda");
    let representatives = vec![
        Value::Nil,
        Value::T,
        Value::Integer(1),
        Value::big_integer(BigInt::from(i64::MAX) + 1),
        Value::Float(1.5),
        Value::String("inline string".into()),
        string_object,
        Value::symbol("symbol"),
        Value::cons(Value::Integer(1), Value::Nil),
        Value::BuiltinFunc("car".into()),
        lambda,
        Value::buffer(1, "*scratch*"),
        Value::Marker(1),
        Value::Overlay(1),
        Value::CharTable(1),
        Value::Record(1),
        Value::Finalizer(1),
        Value::Unbound,
    ];

    for value in representatives {
        assert_eq!(
            call(
                &mut interp,
                "user-ptrp",
                std::slice::from_ref(&value),
                &mut env,
            )
            .unwrap_or_else(|error| panic!("user-ptrp rejected {value}: {error}")),
            Value::Nil,
            "module-free Emaxx cannot construct GNU's PVEC_USER_PTR"
        );
    }
}

#[test]
fn native_comp_pure_introspection_family_matches_gnu_and_the_backend_boundary() {
    let signature_program = r#"
        (mapcar
         (lambda (name)
           (comp--subr-signature (symbol-function name)))
         '(car + concat if let))"#;
    let expected_signatures = r#"("car(1 . 1)" "+(0 . many)" "concat(0 . many)" "if(2 . unevalled)" "let(1 . unevalled)")"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {signature_program})"),
        expected_signatures,
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(signature_program)
        .read()
        .expect("native-comp signature program should parse")
        .expect("native-comp signature program should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("native-comp signatures should evaluate");
    let expected = Reader::new(expected_signatures)
        .read()
        .expect("native-comp expected signatures should parse")
        .expect("native-comp expected signatures should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "comp--subr-signature differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );

    let capability_form = Reader::new(
        r#"(list (native-comp-available-p)
                  (comp-native-driver-options-effective-p)
                  (comp-native-compiler-options-effective-p)
                  (comp-libgccjit-version))"#,
    )
    .read()
    .expect("native-comp capability program should parse")
    .expect("native-comp capability program should contain a form");
    assert_eq!(
        interp
            .eval(&capability_form, &mut env)
            .expect("native-comp capability queries should evaluate"),
        Value::list([Value::Nil, Value::Nil, Value::Nil, Value::Nil]),
        "compiler capability helpers must agree that Emaxx has no native-comp backend"
    );

    let error = call(
        &mut interp,
        "comp--subr-signature",
        &[Value::Integer(1)],
        &mut env,
    )
    .expect_err("comp--subr-signature must reject non-subrs");
    assert_eq!(error.condition_type(), "wrong-type-argument");
}

#[test]
fn native_comp_source_names_hash_canonical_paths_and_real_contents() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-native-comp-names-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&root).expect("create native-comp name fixture");
    let source = root.join("sample.el");
    let compressed = root.join("sample.el.gz");
    let missing = root.join("missing.el");
    let base = root.join("eln-cache");
    let contents = b"(message \"native source\")\n";
    fs::write(&source, contents).expect("write native-comp source fixture");
    let mut encoder = flate2::write::GzEncoder::new(
        fs::File::create(&compressed).expect("create compressed native-comp source"),
        flate2::Compression::default(),
    );
    encoder
        .write_all(contents)
        .expect("compress native-comp source");
    encoder.finish().expect("finish compressed source");

    let relative_name = |path: &Path| {
        let canonical = fs::canonicalize(path).expect("canonicalize native-comp source");
        let canonical = canonical.display().to_string();
        let hash_path = canonical.strip_suffix(".gz").unwrap_or(&canonical);
        let path_hash = format!("{:x}", md5::compute(hash_path.as_bytes()));
        let content_hash = format!("{:x}", md5::compute(contents));
        format!("sample-{}-{}.eln", &path_hash[..8], &content_hash[..8])
    };
    let source_relative = relative_name(&source);
    let compressed_relative = relative_name(&compressed);
    let absolute = base.join("test-abi").join(&source_relative);
    let program = format!(
        r#"
        (let ((comp-native-version-dir "test-abi"))
          (list
           (comp-el-to-eln-rel-filename {source:?})
           (comp-el-to-eln-rel-filename {compressed:?})
           (comp-el-to-eln-filename {source:?} {base:?})
           (condition-case error-data
               (comp-el-to-eln-rel-filename 42)
             (error error-data))
           (condition-case error-data
               (comp-el-to-eln-rel-filename {missing:?})
             (error error-data))))"#,
        source = source.display().to_string(),
        compressed = compressed.display().to_string(),
        base = base.display().to_string(),
        missing = missing.display().to_string(),
    );
    let expected = format!(
        "({source_relative:?} {compressed_relative:?} {absolute:?} \
         (wrong-type-argument stringp 42) (file-missing {missing:?}))",
        absolute = absolute.display().to_string(),
        missing = missing.display().to_string(),
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), &expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(&program)
        .read()
        .expect("native-comp filename contract should parse")
        .expect("native-comp filename contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("native-comp filename contract should evaluate");
    let expected_value = Reader::new(&expected)
        .read()
        .expect("native-comp expected filenames should parse")
        .expect("native-comp expected filenames should exist");
    assert!(
        values_equal(&interp, &actual, &expected_value),
        "native-comp filename result differs from GNU:\nactual: {actual:?}\nexpected: {expected_value:?}"
    );

    fs::remove_dir_all(root).expect("remove native-comp name fixture");
}

#[test]
fn native_comp_mutating_entry_points_report_the_unavailable_backend_honestly() {
    let missing = "/definitely/missing/emaxx-native.eln";
    let upstream_program = format!(
        r#"
        (list
         (comp--release-ctxt)
         (condition-case error-data
             (comp--compile-ctxt-to-file0 42)
           (error error-data))
         (condition-case error-data
             (native-elisp-load 42)
           (error error-data))
         (condition-case error-data
             (native-elisp-load {missing:?})
           (error error-data)))"#
    );
    let upstream_expected = format!(
        "(t (wrong-type-argument stringp 42) \
         (wrong-type-argument stringp 42) \
         (native-lisp-load-failed \"file does not exists\" {missing:?}))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {upstream_program})"), &upstream_expected);

    let source = fs::canonicalize("src/lisp/simple_compat.el")
        .expect("canonicalize existing non-ELN fixture")
        .display()
        .to_string();
    let program = format!(
        r#"
        (list
         (native-comp-available-p)
         (comp--release-ctxt)
         (condition-case error-data (comp--init-ctxt) (error error-data))
         (condition-case error-data
             (comp--compile-ctxt-to-file0 "output.eln")
           (error error-data))
         (condition-case error-data
             (comp--install-trampoline 'car (symbol-function 'cdr))
           (error error-data))
         (condition-case error-data
             (comp--register-lambda nil nil nil nil nil nil nil)
           (error error-data))
         (condition-case error-data
             (comp--register-subr nil nil nil nil nil nil nil)
           (error error-data))
         (condition-case error-data
             (comp--late-register-subr nil nil nil nil nil nil nil)
           (error error-data))
         (condition-case error-data
             (native-elisp-load {source:?})
           (error error-data)))"#
    );
    let unavailable = "(error \"Native compiler backend is unavailable\")";
    let expected = format!(
        "(nil t {unavailable} {unavailable} {unavailable} {unavailable} \
         {unavailable} {unavailable} \
         (native-lisp-load-failed {source:?} \
          \"Native compiler backend is unavailable\"))"
    );
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(&program)
        .read()
        .expect("native-comp backend boundary should parse")
        .expect("native-comp backend boundary should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("native-comp backend boundary should evaluate");
    let expected = Reader::new(&expected)
        .read()
        .expect("native-comp backend expected result should parse")
        .expect("native-comp backend expected result should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "native-comp backend boundary was not explicit:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn portable_dump_introspection_and_backend_boundary_are_honest() {
    let sort_program = r#"
        (list
         (dump-emacs-portable--sort-predicate '(first 1) '(second 2))
         (dump-emacs-portable--sort-predicate '(first 2) '(second 1))
         (dump-emacs-portable--sort-predicate '(first 1) '(second 1)))"#;
    let expected = "(t nil nil)";
    assert_upstream_primitive_contract(&format!("(prin1 {sort_program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(sort_program)
        .read()
        .expect("portable-dump sort program should parse")
        .expect("portable-dump sort program should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("portable-dump sort predicate should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("portable-dump sort result should parse")
        .expect("portable-dump sort result should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "portable-dump relocation ordering differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );

    assert_upstream_primitive_contract(
        r#"(let ((stats (pdumper-stats)))
              (prin1
               (or (null stats)
                   (and (eq (alist-get 'dumped-with-pdumper stats) t)
                        (numberp (alist-get 'load-time stats))
                        (stringp (alist-get 'dump-file-name stats))))))"#,
        "t",
    );
    assert_eq!(
        call(&mut interp, "pdumper-stats", &[], &mut env)
            .expect("a directly initialized Emaxx has valid dump statistics"),
        Value::Nil,
        "Emaxx must not claim it restored from a portable dump"
    );

    let contract_program = r#"
        (list
         (subr-arity (symbol-function 'dump-emacs-portable))
         (subr-arity
          (symbol-function 'dump-emacs-portable--sort-predicate-copied))
         (condition-case error-data
             (dump-emacs-portable 42)
           (error error-data)))"#;
    let contract_expected = "((1 . 2) (2 . 2) (wrong-type-argument stringp 42))";
    assert_upstream_primitive_contract(&format!("(prin1 {contract_program})"), contract_expected);

    let boundary_program = r#"
        (list
         (subr-arity (symbol-function 'dump-emacs-portable))
         (subr-arity
          (symbol-function 'dump-emacs-portable--sort-predicate-copied))
         (condition-case error-data
             (dump-emacs-portable 42)
           (error error-data))
         (condition-case error-data
             (dump-emacs-portable "must-not-be-created.pdmp" t)
           (error error-data))
         (condition-case error-data
             (dump-emacs-portable--sort-predicate-copied nil nil)
           (error error-data)))"#;
    let unavailable = "(error \"Portable dumper backend is unavailable\")";
    let boundary_expected = format!(
        "((1 . 2) (2 . 2) (wrong-type-argument stringp 42) \
         {unavailable} {unavailable})"
    );
    let form = Reader::new(boundary_program)
        .read()
        .expect("portable-dumper boundary program should parse")
        .expect("portable-dumper boundary program should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("portable-dumper boundary should be catchable");
    let expected = Reader::new(&boundary_expected)
        .read()
        .expect("portable-dumper boundary expectation should parse")
        .expect("portable-dumper boundary expectation should contain a form");
    assert!(
        values_equal(&interp, &actual, &expected),
        "portable-dumper boundary was not explicit:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn compiled_regexp_introspection_preserves_the_real_backend_boundary() {
    let contract_program = r#"
        (list
         (subr-arity (symbol-function 're--describe-compiled))
         (condition-case error-data
             (re--describe-compiled "[" t)
           (error (car error-data)))
         (let ((raw (re--describe-compiled "a" t)))
           (list (stringp raw)
                 (multibyte-string-p raw)
                 (string-to-list raw))))"#;
    let contract_expected = "((1 . 2) invalid-regexp (t nil (2 1 97 1)))";
    assert_upstream_primitive_contract(&format!("(prin1 {contract_program})"), contract_expected);

    let boundary_program = r#"
        (list
         (subr-arity (symbol-function 're--describe-compiled))
         (condition-case error-data
             (re--describe-compiled "[" t)
           (error (car error-data)))
         (condition-case error-data
             (re--describe-compiled "a")
           (error error-data))
         (condition-case error-data
             (re--describe-compiled "a" t)
           (error error-data)))"#;
    let unavailable =
        "(error \"Compiled regexp introspection is unavailable from the fancy-regex backend\")";
    let boundary_expected = format!("((1 . 2) invalid-regexp {unavailable} {unavailable})");
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(boundary_program)
        .read()
        .expect("compiled-regexp boundary program should parse")
        .expect("compiled-regexp boundary program should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("compiled-regexp boundary should be catchable");
    let expected = Reader::new(&boundary_expected)
        .read()
        .expect("compiled-regexp boundary expectation should parse")
        .expect("compiled-regexp boundary expectation should contain a form");
    assert!(
        values_equal(&interp, &actual, &expected),
        "compiled-regexp boundary was not explicit:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn memory_use_counts_exposes_the_allocation_telemetry_boundary_honestly() {
    let contract_program = r#"
        (let ((counts (memory-use-counts)))
          (list
           (subr-arity (symbol-function 'memory-use-counts))
           (length counts)
           (mapcar #'integerp counts)))"#;
    let contract_expected = "((0 . 0) 7 (t t t t t t t))";
    assert_upstream_primitive_contract(&format!("(prin1 {contract_program})"), contract_expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        r#"
        (list
         (subr-arity (symbol-function 'memory-use-counts))
         (condition-case error-data
             (memory-use-counts)
           (error error-data)))"#,
    )
    .read()
    .expect("allocation-counter boundary program should parse")
    .expect("allocation-counter boundary program should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("allocation-counter boundary should be catchable");
    let expected = Reader::new(
        "((0 . 0) \
         (error \"GNU allocation counters are unavailable in the Rust ownership backend\"))",
    )
    .read()
    .expect("allocation-counter boundary expectation should parse")
    .expect("allocation-counter boundary expectation should contain a form");
    assert!(
        values_equal(&interp, &actual, &expected),
        "allocation-counter boundary was not explicit:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[cfg(unix)]
#[test]
fn module_load_validates_real_libraries_without_fabricating_the_gnu_value_abi() {
    use std::sync::atomic::{AtomicU64, Ordering};

    static PROBE_ID: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "emaxx-module-probe-{}-{}",
        std::process::id(),
        PROBE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&directory).expect("module probe directory should be created");
    let source = directory.join("probe.c");
    std::fs::write(
        &source,
        "int plugin_is_GPL_compatible;\n\
         int emacs_module_init(void *runtime) { (void) runtime; return 0; }\n",
    )
    .expect("module probe source should be written");
    #[cfg(target_os = "macos")]
    let library = directory.join("probe.dylib");
    #[cfg(not(target_os = "macos"))]
    let library = directory.join("probe.so");
    let mut compiler = std::process::Command::new(
        std::env::var_os("CC").unwrap_or_else(|| std::ffi::OsString::from("cc")),
    );
    #[cfg(target_os = "macos")]
    compiler.arg("-dynamiclib");
    #[cfg(not(target_os = "macos"))]
    compiler.args(["-shared", "-fPIC"]);
    let status = compiler
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .status()
        .expect("module probe compiler should run");
    assert!(status.success(), "module probe should compile");

    let contract_program = r#"
        (list
         (subr-arity (symbol-function 'module-load))
         (condition-case error-data
             (module-load 1)
           (error (car error-data)))
         (condition-case error-data
             (module-load "/definitely/missing/emaxx-module.so")
           (error (car error-data)))
         (get 'module-open-failed 'error-conditions)
         (get 'module-not-gpl-compatible 'error-conditions)
         (get 'missing-module-init-function 'error-conditions)
         (get 'module-init-failed 'error-conditions))"#;
    let contract_expected = "((1 . 1) wrong-type-argument module-open-failed \
        (module-open-failed module-load-failed error) \
        (module-not-gpl-compatible module-load-failed error) \
        (missing-module-init-function module-load-failed error) \
        (module-init-failed module-load-failed error))";
    assert_upstream_primitive_contract(&format!("(prin1 {contract_program})"), contract_expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let contract_form = Reader::new(contract_program)
        .read()
        .expect("dynamic-module contract should parse")
        .expect("dynamic-module contract should contain a form");
    let contract_actual = interp
        .eval(&contract_form, &mut env)
        .expect("dynamic-module validation conditions should be catchable");
    let contract_expected = Reader::new(contract_expected)
        .read()
        .expect("dynamic-module expectation should parse")
        .expect("dynamic-module expectation should contain a form");
    assert!(
        values_equal(&interp, &contract_actual, &contract_expected),
        "dynamic-module validation conditions diverged:\nactual: {contract_actual:?}"
    );

    let form = Value::list([
        Value::symbol("condition-case"),
        Value::symbol("error-data"),
        Value::list([
            Value::symbol("module-load"),
            Value::String(library.display().to_string().into()),
        ]),
        Value::list([Value::symbol("error"), Value::symbol("error-data")]),
    ]);
    let actual = interp
        .eval(&form, &mut env)
        .expect("dynamic-module ABI boundary should be catchable");
    let expected = Value::list([
        Value::symbol("error"),
        Value::string("GNU dynamic module ABI is unavailable in the Rust value backend"),
    ]);
    assert!(
        values_equal(&interp, &actual, &expected),
        "module initializer must not receive a fabricated runtime:\nactual: {actual:?}"
    );
    std::fs::remove_dir_all(directory).expect("module probe directory should be removed");
}

#[test]
fn every_claimed_gnu_c_primitive_mirror_has_an_exact_native_surface_contract() {
    use super::generated_gnu_c_primitives::{
        GNU_C_PRIMITIVE_AVAILABLE_COUNT, GNU_C_PRIMITIVE_SOURCE_COUNT, GNU_C_PRIMITIVES,
    };

    assert_eq!(GNU_C_PRIMITIVES.len(), GNU_C_PRIMITIVE_SOURCE_COUNT);
    assert_eq!(
        GNU_C_PRIMITIVES
            .iter()
            .filter(|contract| contract.arity.is_some())
            .count(),
        GNU_C_PRIMITIVE_AVAILABLE_COUNT
    );
    assert!(
        GNU_C_PRIMITIVES
            .windows(2)
            .all(|pair| pair[0].name < pair[1].name),
        "generated GNU C primitive inventory must stay sorted and unique"
    );

    let available = GNU_C_PRIMITIVES
        .iter()
        .filter(|contract| contract.arity.is_some())
        .collect::<Vec<_>>();
    let is_native_mirror = |name: &str| is_builtin(name) || is_special_form_name(name);
    let missing = available
        .iter()
        .copied()
        .filter(|contract| !is_native_mirror(contract.name))
        .collect::<Vec<_>>();
    let mut mirrored = Vec::new();
    let mut issues = Vec::new();
    for contract in available
        .iter()
        .copied()
        .filter(|contract| is_native_mirror(contract.name))
    {
        mirrored.push(contract.name);
        if generated_builtin_arities::generated_builtin_arity(contract.name) != contract.arity {
            issues.push(format!(
                "{} [{}]: arity {:?}, expected {:?}",
                contract.name,
                contract.origins,
                generated_builtin_arities::generated_builtin_arity(contract.name),
                contract.arity
            ));
        }
        if is_special_form_name(contract.name) != contract.special_form {
            issues.push(format!(
                "{} [{}]: special_form {}, expected {}",
                contract.name,
                contract.origins,
                is_special_form_name(contract.name),
                contract.special_form
            ));
        }
        if generated_builtin_arities::generated_builtin_command_p(contract.name) != contract.command
        {
            issues.push(format!(
                "{} [{}]: command {}, expected {}",
                contract.name,
                contract.origins,
                generated_builtin_arities::generated_builtin_command_p(contract.name),
                contract.command
            ));
        }
        if !contract.special_form && !has_dispatch_handler(contract.name) {
            issues.push(format!(
                "{} [{}]: claimed native without a Rust dispatch route",
                contract.name, contract.origins
            ));
        }
    }

    let fingerprint = |names: &[&str]| {
        // Stable FNV-1a over the sorted, NUL-separated names.  The exact
        // fingerprint prevents a removed dispatch arm from silently moving
        // from the mirrored inventory into the missing inventory.
        names.iter().fold(0xcbf29ce484222325_u64, |mut hash, name| {
            for byte in name.bytes().chain(std::iter::once(0)) {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100000001b3);
            }
            hash
        })
    };
    let missing_names = missing
        .iter()
        .map(|contract| contract.name)
        .collect::<Vec<_>>();
    assert_eq!(
        (mirrored.len(), fingerprint(&mirrored)),
        (1_420, 4_253_707_965_298_194_171),
        "GNU C mirror inventory changed; audit the exact addition/removal before updating this snapshot"
    );
    assert_eq!(
        (missing_names.len(), fingerprint(&missing_names)),
        (0, 14_695_981_039_346_656_037),
        "GNU C missing-primitive inventory changed; audit the exact addition/removal before updating this snapshot"
    );
    if std::env::var_os("EMAXX_PRINT_NATIVE_PRIMITIVE_AUDIT").is_some() {
        let mut by_origin = std::collections::BTreeMap::<&str, Vec<&str>>::new();
        for contract in missing {
            by_origin
                .entry(contract.origins)
                .or_default()
                .push(contract.name);
        }
        for (origins, names) in by_origin {
            eprintln!("{} ({})\n  {}", origins, names.len(), names.join(" "));
        }
    }
    assert!(
        issues.is_empty(),
        "{} GNU C primitive surface mismatches across {} Emaxx mirrors:\n{}",
        issues.len(),
        mirrored.len(),
        issues.join("\n")
    );
}

#[test]
fn generated_rust_manifests_never_contain_trailing_whitespace() {
    for (name, source) in [
        (
            "dumped autoloads",
            include_str!("../eval/generated_autoloads.rs"),
        ),
        (
            "builtin arities",
            include_str!("generated_builtin_arities.rs"),
        ),
    ] {
        for (line_index, line) in source.lines().enumerate() {
            assert!(
                !line.ends_with(' ') && !line.ends_with('\t'),
                "{name} generator emitted trailing whitespace on line {}",
                line_index + 1
            );
        }
    }
}

#[test]
fn builtin_metadata_covers_the_whole_c_manifest_without_lisp_string_leakage() {
    use super::generated_gnu_c_primitives::GNU_C_PRIMITIVES;

    for contract in GNU_C_PRIMITIVES {
        let Some(arity) = contract.arity else {
            continue;
        };
        assert_eq!(
            generated_builtin_arities::generated_builtin_arity(contract.name),
            Some(arity),
            "{} [{}] is missing from generated C primitive arities",
            contract.name,
            contract.origins
        );
        assert_eq!(
            generated_builtin_arities::generated_builtin_command_p(contract.name),
            contract.command,
            "{} [{}] has the wrong generated command identity",
            contract.name,
            contract.origins
        );
    }

    // These are dumped Lisp commands in GNU, not C primitives.  Merely
    // mentioning their condition symbols inside a Rust dispatcher must never
    // move them across the Lisp/native boundary.
    for name in ["beginning-of-buffer", "end-of-buffer"] {
        assert_eq!(
            generated_builtin_arities::generated_builtin_arity(name),
            None,
            "{name} leaked into C metadata from a Rust string literal"
        );
        assert!(
            !generated_builtin_arities::generated_builtin_command_p(name),
            "{name} leaked into C command metadata from a Rust string literal"
        );
    }
}

#[test]
fn gnu_c_primitive_boundary_is_not_reimplemented_in_simple_compat_lisp() {
    use super::generated_gnu_c_primitives::GNU_C_PRIMITIVES;

    let available = GNU_C_PRIMITIVES
        .iter()
        .filter(|contract| contract.arity.is_some())
        .map(|contract| contract.name)
        .collect::<std::collections::HashSet<_>>();
    let mut violations = include_str!("../simple_compat.el")
        .lines()
        .filter_map(|line| {
            let form = line
                .strip_prefix("(defun ")
                .or_else(|| line.strip_prefix("(defmacro "))?;
            let name = form
                .split(|ch: char| ch.is_ascii_whitespace() || ch == '(' || ch == ')')
                .next()?;
            available.contains(name).then_some(name)
        })
        .collect::<Vec<_>>();
    violations.sort_unstable();

    assert!(
        violations.is_empty(),
        "GNU C primitives must stay on Emaxx's Rust side of the host/Lisp boundary:\n{}",
        violations.join("\n")
    );
}

#[test]
fn regex_resource_helper_patterns_compile() {
    for literal in [
        r#""\\(?:^\\|[^\\]\\)\\(?:\\\\\\\\\\)*\\\\""#,
        r#""\\(?:^\\|[^\\]\\)\\(?:\\\\\\\\\\)*\\\\.\\=""#,
    ] {
        let pattern = Reader::new(literal)
            .read()
            .expect("pattern literal should parse")
            .expect("pattern literal should contain a value");
        let pattern = string_text(&pattern).expect("pattern literal should be a string");
        let translated = regexp::translate_elisp_regex(&pattern);
        let rendered = format!("(?m:{translated})");
        assert!(
            FancyRegex::new(&rendered).is_ok(),
            "failed to compile `{pattern}` as `{rendered}`"
        );
    }
}

#[test]
fn translate_elisp_regex_handles_rx_generated_hyphen_classes() {
    let pattern = r"-\(?:[\-[:alnum:]]\)+\(?:=\)?";
    let translated = regexp::translate_elisp_regex(pattern);
    let rendered = format!("(?m:{translated})");
    let regex = FancyRegex::new(&rendered).expect("translated regex should compile");
    assert!(
        regex
            .is_match("--tofu-policy=")
            .expect("match result should be available"),
        "translated `{pattern}` into `{translated}`"
    );
}

#[test]
fn translate_elisp_regex_elides_repeated_empty_shy_groups() {
    let pattern = r"\(?:\)?x\(?:\)*";
    let translated = regexp::translate_elisp_regex(pattern);
    let rendered = format!("(?m:{translated})");
    let regex = FancyRegex::new(&rendered).expect("translated regexp should compile");
    assert!(regex.is_match("x").expect("match should complete"));
}

#[test]
fn subregexp_context_rejects_classes_bounds_and_trailing_escape() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "subregexp-context-p",
            &[Value::String("a[b]".into()), Value::Integer(2)],
            &mut env,
        )
        .expect("inside a character class is not a subregexp context"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "subregexp-context-p",
            &[Value::String(r"a\(".into()), Value::Integer(3)],
            &mut env,
        )
        .expect("unfinished group is still a subregexp context"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "subregexp-context-p",
            &[Value::String(r"a\".into()), Value::Integer(2)],
            &mut env,
        )
        .expect("trailing escape is not a subregexp context"),
        Value::Nil
    );
}

#[test]
fn string_to_syntax_encodes_classes_flags_and_matching_characters() {
    // Keep this public-value contract tied to the pinned GNU implementation,
    // not merely to hand-copied Rust expectations.  This probe is small
    // enough for the fast suite and covers the related char-table boundary.
    assert_upstream_primitive_contract(
        r#"(let ((table (make-syntax-table)) mapped)
              (modify-syntax-entry ?% ". c" table)
              (map-char-table
               (lambda (range syntax)
                 (when (or (equal range ?%)
                           (and (consp range)
                                (<= (car range) ?%)
                                (>= (cdr range) ?%)))
                   (setq mapped syntax)))
               table)
              (prin1
               (list (string-to-syntax ".")
                     (string-to-syntax ". 1234")
                     (string-to-syntax "(] 1234")
                     (string-to-syntax "@")
                     (string-to-syntax ". c")
                     (aref table ?%)
                     (char-table-range table ?%)
                     mapped
                     (condition-case error
                         (string-to-syntax "z")
                       (error (list (car error)
                                    (error-message-string error)))))))"#,
        "((1) (983041) (983044 . 93) nil (8388609) (8388609) (8388609) (8388609) (error \"Invalid syntax description letter: z\"))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "string-to-syntax",
            &[Value::String(".".into())],
            &mut env,
        )
        .expect("punctuation syntax"),
        Value::list([Value::Integer(1)])
    );
    assert_eq!(
        call(
            &mut interp,
            "string-to-syntax",
            &[Value::String(". 1234".into())],
            &mut env,
        )
        .expect("comment flag syntax"),
        Value::list([Value::Integer(983041)])
    );
    assert_eq!(
        call(
            &mut interp,
            "string-to-syntax",
            &[Value::String(". nb".into())],
            &mut env,
        )
        .expect("nested style-b syntax"),
        Value::list([Value::Integer(6291457)])
    );
    assert_eq!(
        call(
            &mut interp,
            "string-to-syntax",
            &[Value::String("(] 1234".into())],
            &mut env,
        )
        .expect("matching paren syntax"),
        Value::cons(Value::Integer(983044), Value::Integer(']' as i64))
    );
    assert_eq!(
        call(
            &mut interp,
            "string-to-syntax",
            &[Value::String("@".into())],
            &mut env,
        )
        .expect("inherit syntax"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "string-to-syntax",
            &[Value::String(". c".into())],
            &mut env,
        )
        .expect("comment style-c syntax"),
        Value::list([Value::Integer(8_388_609)])
    );
    assert_eq!(
        call(
            &mut interp,
            "string-to-syntax",
            &[Value::String("z".into())],
            &mut env,
        )
        .expect_err("invalid syntax descriptors must signal")
        .to_string(),
        "Invalid syntax description letter: z"
    );
}

#[test]
fn internal_char_font_matches_the_headless_gnu_font_boundary() {
    assert_upstream_primitive_contract(
        r#"(prin1 (list (internal-char-font nil ?A)
                         (with-temp-buffer
                           (insert "A")
                           (internal-char-font 1))))"#,
        "(nil nil)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    assert_eq!(
        call(
            &mut interp,
            "internal-char-font",
            &[Value::Nil, Value::Integer('A' as i64)],
            &mut env,
        )
        .expect("query the headless default font"),
        Value::Nil
    );
    interp.buffer.insert("A");
    assert_eq!(
        call(
            &mut interp,
            "internal-char-font",
            &[Value::Integer(1)],
            &mut env,
        )
        .expect("query an undisplayed buffer position"),
        Value::Nil
    );
}

#[test]
fn fontp_matches_the_gnu_font_record_contract() {
    assert_upstream_primitive_contract(
        r#"(let ((font (font-spec :name "x")))
              (prin1
               (list (fontp nil)
                     (fontp font)
                     (fontp font 'font-spec)
                     (fontp font 'font-object)
                     (condition-case error
                         (fontp font 'bogus)
                       (error (car error))))))"#,
        "(nil t t nil wrong-type-argument)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let font = call(
        &mut interp,
        "font-spec",
        &[Value::Symbol(":name".into()), Value::String("x".into())],
        &mut env,
    )
    .expect("font-spec should create a font record");
    assert_eq!(
        call(&mut interp, "fontp", &[Value::Nil], &mut env).expect("nil is not a font"),
        Value::Nil
    );
    assert_eq!(
        call(&mut interp, "fontp", std::slice::from_ref(&font), &mut env)
            .expect("font-spec is a font"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "fontp",
            &[font.clone(), Value::Symbol("font-spec".into())],
            &mut env,
        )
        .expect("font-spec subtype should match"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "fontp",
            &[font.clone(), Value::Symbol("font-object".into())],
            &mut env,
        )
        .expect("font-object subtype should not match"),
        Value::Nil
    );
    let error = call(
        &mut interp,
        "fontp",
        &[font, Value::Symbol("bogus".into())],
        &mut env,
    )
    .expect_err("invalid font subtype must signal");
    assert_eq!(error.condition_type(), "wrong-type-argument");
}

#[test]
fn native_frame_identity_and_single_tty_traversal_match_gnu() {
    let program = r#"
        (let ((frame (selected-frame)))
          (list
           (type-of frame)
           (recordp frame)
           (symbolp frame)
           (framep frame)
           (frame-live-p frame)
           (eq (car (frame-list)) frame)
           (eq (car (visible-frame-list)) frame)
           (eq (next-frame frame) frame)
           (eq (previous-frame frame) frame)
           (eq (old-selected-frame) frame)
           (frame-parent frame)
           (frame-ancestor-p frame frame)))"#;
    let expected = "(frame nil nil t t t t t t t nil nil)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("frame identity contract should parse")
        .expect("frame identity contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("frame identity contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("frame identity expected value should parse")
        .expect("frame identity expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "frame identity differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_frame_geometry_parameters_and_state_flags_match_gnu() {
    let program = r#"
        (let ((frame (selected-frame)))
          (list
           (frame-char-width frame)
           (frame-char-height frame)
           (frame-native-width frame)
           (frame-native-height frame)
           (frame-text-width frame)
           (frame-text-height frame)
           (frame-text-cols frame)
           (frame-text-lines frame)
           (frame-total-cols frame)
           (frame-total-lines frame)
           (frame-internal-border-width frame)
           (frame-fringe-width frame)
           (frame-scroll-bar-width frame)
           (frame-scroll-bar-height frame)
           (frame-right-divider-width frame)
           (frame-bottom-divider-width frame)
           (frame-child-frame-border-width frame)
           (tool-bar-pixel-width frame)
           (frame-scale-factor frame)
           (frame-position frame)
           (frame-windows-min-size frame nil nil nil)
           (frame-windows-min-size frame t nil nil)
           (frame-windows-min-size frame nil nil t)
           (list
            (frame-parameter frame 'width)
            (frame-parameter frame 'height)
            (frame-parameter frame 'name))
           (progn
             (modify-frame-parameters
              frame
              '((emaxx-probe . first)
                (emaxx-probe . second)))
             (frame-parameter frame 'emaxx-probe))
           (progn
             (set-frame-width frame 90)
             (set-frame-height frame 30)
             (list
              (frame-native-width frame)
              (frame-native-height frame)))
           (progn
             (set-frame-size frame 70 20)
             (list
              (frame-native-width frame)
              (frame-native-height frame)))
           (progn (set-frame-size frame 80 25) nil)
           (set-frame-position frame 3 -4)
           (frame-position frame)
           (frame-window-state-change frame)
           (set-frame-window-state-change frame t)
           (frame-window-state-change frame)
           (set-frame-window-state-change frame nil)
           (frame--set-was-invisible frame t)
           (frame--set-was-invisible frame nil)
           (frame-after-make-frame frame nil)
           (frame-after-make-frame frame t)))"#;
    let expected = r#"
        (1 1 80 25 80 25 80 25 80 25
         0 0 0 0 0 0 0 0 1.0 (0 . 0)
         8 10 5 (80 25 "F1") first
         (90 31) (70 21) nil t (0 . 0)
         nil t t nil t nil nil t)"#;
    let expected_printed = r#"(1 1 80 25 80 25 80 25 80 25 0 0 0 0 0 0 0 0 1.0 (0 . 0) 8 10 5 (80 25 "F1") first (90 31) (70 21) nil t (0 . 0) nil t t nil t nil nil t)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("frame geometry contract should parse")
        .expect("frame geometry contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("frame geometry contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("frame geometry expected value should parse")
        .expect("frame geometry expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "frame geometry differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn window_configuration_restore_keeps_the_current_frame_size_like_gnu() {
    let program = r#"
        (let (configuration)
          (set-frame-height nil 30)
          (setq configuration (current-window-configuration))
          (set-frame-height nil 10)
          (set-window-configuration configuration)
          (list
           (frame-native-height)
           (frame-text-height)
           (window-pixel-top (frame-root-window))
           (window-pixel-height (frame-root-window))
           (window-pixel-top (minibuffer-window))
           (window-configuration-equal-p
            configuration
            (current-window-configuration))))"#;
    let expected = r#"(11 10 1 9 10 nil)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("window configuration contract should parse")
        .expect("window configuration contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("window configuration contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("window configuration expected value should parse")
        .expect("window configuration expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "window configuration restore differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_frame_focus_mouse_geometry_and_headless_errors_match_gnu() {
    let program = r#"
        (let ((frame (selected-frame)))
          (list
           (eq (select-frame frame) frame)
           (eq
            (handle-switch-frame
             (list 'switch-frame frame))
            frame)
           (eq (make-frame-visible frame) frame)
           (condition-case error-data
               (make-frame-invisible frame)
             (error (car error-data)))
           (make-frame-invisible frame t)
           (iconify-frame frame)
           (frame-visible-p frame)
           (let ((position (mouse-position)))
             (list
              (eq (car position) frame)
              (cdr position)))
           (let ((position (mouse-pixel-position)))
             (list
              (eq (car position) frame)
              (cdr position)))
           (set-mouse-position frame 2 3)
           (set-mouse-pixel-position frame 2 3)
           (raise-frame frame)
           (lower-frame frame)
           (frame-focus frame)
           (redirect-frame-focus frame frame)
           (eq (frame-focus frame) frame)
           (redirect-frame-focus frame nil)
           (frame-focus frame)
           (condition-case error-data
               (x-focus-frame frame)
             (error (car error-data)))
           (frame-pointer-visible-p frame)
           (condition-case error-data
               (delete-frame frame)
             (error (car error-data)))
           (condition-case error-data
               (delete-frame frame t)
             (error (car error-data)))
           (condition-case error-data
               (make-terminal-frame nil)
             (error (car error-data)))
           (condition-case error-data
               (reconsider-frame-fonts frame)
             (error (car error-data)))
           (condition-case error-data
               (x-get-resource "a" "b")
             (error (car error-data)))
           (x-parse-geometry "80x24+1-2")
           (x-parse-geometry "+3-4")
           (x-parse-geometry "bogus")
           (mapcar
            (lambda (thunk)
              (condition-case error-data
                  (funcall thunk)
                (error (car error-data))))
            (list
             (lambda () (select-frame 1))
             (lambda ()
               (set-mouse-position frame 1.0 2))
             (lambda () (frame-visible-p nil))
             (lambda () (x-parse-geometry 1))))))"#;
    let expected = r#"
        (t t t error nil nil t
         (t (nil)) (t (nil))
         nil nil nil nil nil nil t nil nil error t
         error error error error error
         ((height . 24) (width . 80)
          (top . -2) (left . 1))
         ((top . -4) (left . 3))
         nil
         (wrong-type-argument wrong-type-argument
          wrong-type-argument wrong-type-argument))"#;
    let expected_printed = "(t t t error nil nil t (t (nil)) (t (nil)) nil nil nil nil nil nil t nil nil error t error error error error error ((height . 24) (width . 80) (top . -2) (left . 1)) ((top . -4) (left . 3)) nil (wrong-type-argument wrong-type-argument wrong-type-argument wrong-type-argument))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("frame headless contract should parse")
        .expect("frame headless contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("frame headless contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("frame headless expected value should parse")
        .expect("frame headless expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "frame headless contract differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_font_spec_state_and_headless_lookup_family_matches_gnu() {
    let program = r#"
        (let ((spec
               (font-spec
                :foundry "misc"
                :family "Mono"
                :weight 'bold
                :slant 'italic
                :width 'condensed
                :size 12
                :dpi 96
                :spacing 'M
                :avgwidth 80
                :custom 42)))
          (list
           (mapcar
            (lambda (key) (font-get spec key))
            '(:foundry :family :weight :slant :width
              :size :dpi :spacing :avgwidth :custom :name))
           (font-put spec :family "Serif")
           (font-get spec :family)
           (font-xlfd-name spec)
           (font-xlfd-name spec t)
           (font-match-p (font-spec :family "Serif") spec)
           (font-match-p (font-spec :family "Mono") spec)
           (list-fonts spec)
           (find-font spec)
           (font-family-list)
           (frame-font-cache)
           (clear-font-cache)))"#;
    let expected = r#"
        ((misc Mono bold italic condensed 12 96 100 80 42 nil)
         "Serif"
         Serif
         "-misc-Serif-bold-italic-condensed-*-12-*-96-96-m-80-*-*"
         "-misc-Serif-bold-italic-condensed-*-12-*-96-96-m-80-*"
         t nil nil nil nil nil nil)"#;
    let expected_printed = "((misc Mono bold italic condensed 12 96 100 80 42 nil) \"Serif\" Serif \"-misc-Serif-bold-italic-condensed-*-12-*-96-96-m-80-*-*\" \"-misc-Serif-bold-italic-condensed-*-12-*-96-96-m-80-*\" t nil nil nil nil nil nil)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("font.c pure family contract should parse")
        .expect("font.c pure family contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("font.c pure family contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("font.c expected value should parse")
        .expect("font.c expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "font.c result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_font_spec_validation_and_name_normalization_match_gnu() {
    let program = r#"
        (let ((spec (font-spec :family "Mono")))
          (list
           (list
            (font-put spec :script "latin")
            (font-get spec :script))
           (let ((named
                  (font-spec :name "Mono-12:bold")))
             (list
              (font-get named :family)
              (font-get named :size)
              (font-get named :weight)
              (font-get named :name)))
           (mapcar
            (lambda (thunk)
              (condition-case error-data
                  (funcall thunk)
                (error (car error-data))))
            (list
             (lambda () (font-spec :weight 100))
             (lambda () (font-spec :size -1))
             (lambda () (font-spec :weight))
             (lambda () (font-spec 1 2))
             (lambda ()
               (font-put spec :width 'bogus))
             (lambda () (font-get spec 1))
             (lambda ()
               (list-fonts spec nil "one"))))))"#;
    let expected = r#"
        (("latin" latin)
         (Mono 12.0 bold "Mono-12:bold")
         (error error error wrong-type-argument
          error wrong-type-argument wrong-type-argument))"#;
    let expected_printed = "((\"latin\" latin) (Mono 12.0 bold \"Mono-12:bold\") (error error error wrong-type-argument error wrong-type-argument wrong-type-argument))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("font validation contract should parse")
        .expect("font validation contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("font validation contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("font validation expected value should parse")
        .expect("font validation expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "font validation differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_font_at_and_info_match_the_headless_gnu_boundary() {
    let program = r#"
        (progn
          (erase-buffer)
          (insert "Aλ")
          (list
           (font-at 1)
           (font-at 2)
           (font-at 0 nil "Aλ")
           (font-at 1 nil "Aλ")
           (mapcar
            (lambda (thunk)
              (condition-case error-data
                  (funcall thunk)
                (error (car error-data))))
            (list
             (lambda () (font-at 0))
             (lambda () (font-at 2 nil "A"))
             (lambda () (font-at 'x nil "A"))
             (lambda () (font-at 1 7))
             (lambda ()
               (with-temp-buffer
                 (insert "A")
                 (font-at 1)))
             (lambda () (font-info "Mono"))))))"#;
    let expected = "(nil nil nil nil (args-out-of-range args-out-of-range wrong-type-argument wrong-type-argument error error))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("headless font boundary should parse")
        .expect("headless font boundary should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("headless font boundary should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("headless font result should parse")
        .expect("headless font result should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "headless font result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_font_backend_boundary_and_glyph_validation_match_gnu() {
    let program = r#"
        (let* ((spec (font-spec))
               (gstring
                (composition-get-gstring 0 1 nil "a")))
          (list
           (condition-case error-data
               (font-shape-gstring nil nil)
             (error error-data))
           (condition-case error-data
               (font-shape-gstring [] nil)
             (error error-data))
           (progn
             (aset gstring 1 7)
             (eq (font-shape-gstring gstring 'bogus)
                 gstring))
           (progn
             (aset gstring 1 nil)
             (condition-case error-data
                 (font-shape-gstring gstring nil)
               (error error-data)))
           (condition-case error-data
               (font-variation-glyphs nil 65)
             (error error-data))
           (condition-case error-data
               (font-variation-glyphs spec 65)
             (error
              (list (car error-data)
                    (cadr error-data)
                    (eq (caddr error-data) spec))))
           (condition-case error-data
               (close-font nil)
             (error error-data))
           (condition-case error-data
               (close-font spec)
             (error
              (list (car error-data)
                    (cadr error-data)
                    (eq (caddr error-data) spec))))
           (condition-case error-data
               (query-font nil)
             (error error-data))
           (condition-case error-data
               (query-font spec)
             (error
              (list (car error-data)
                    (cadr error-data)
                    (eq (caddr error-data) spec))))
           (condition-case error-data
               (font-has-char-p nil 65)
             (error error-data))
           (condition-case error-data
               (font-has-char-p spec "x")
             (error
              (list (car error-data)
                    (cadr error-data)
                    (eq (caddr error-data) "x"))))
           (condition-case error-data
               (font-has-char-p spec 65 t)
             (error error-data))
           (condition-case error-data
               (font-get-glyphs nil 1 1)
             (error error-data))
           (condition-case error-data
               (font-get-glyphs spec 1 1)
             (error
              (list (car error-data)
                    (cadr error-data)
                    (eq (caddr error-data) spec))))
           (condition-case error-data
               (font-face-attributes nil t)
             (error error-data))
           (condition-case error-data
               (open-font nil nil t)
             (error error-data))))"#;
    let expected = concat!(
        "((error \"Invalid glyph-string: \") ",
        "(error \"Invalid glyph-string: \" []) t ",
        "(wrong-type-argument font-object utf-8-unix) ",
        "(wrong-type-argument font-object nil) ",
        "(wrong-type-argument font-object t) ",
        "(wrong-type-argument font-object nil) ",
        "(wrong-type-argument font-object t) ",
        "(wrong-type-argument font-object nil) ",
        "(wrong-type-argument font-object t) ",
        "(wrong-type-argument font nil) ",
        "(wrong-type-argument characterp nil) ",
        "(wrong-type-argument framep t) ",
        "(wrong-type-argument font-object nil) ",
        "(wrong-type-argument font-object t) ",
        "(wrong-type-argument frame-live-p t) ",
        "(wrong-type-argument frame-live-p t))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("font backend contract should parse")
        .expect("font backend contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("font backend contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("font backend expected value should parse")
            .expect("font backend expected value should exist")
    );

    let headless = Reader::new(
        r#"
        (let ((spec (font-spec)))
          (mapcar
           (lambda (thunk)
             (condition-case error-data
                 (funcall thunk)
               (error
                (list (car error-data)
                      (cadr error-data)))))
           (list
            (lambda () (font-face-attributes spec))
            (lambda () (open-font nil))
            (lambda () (font-has-char-p spec 65)))))"#,
    )
    .read()
    .expect("catchable headless font contract should parse")
    .expect("catchable headless font contract should contain a form");
    assert_eq!(
        interp
            .eval(&headless, &mut env)
            .expect("headless font failures should be catchable"),
        Value::list([
            Value::list([
                Value::symbol("error"),
                Value::string("Window system frame should be used"),
            ]),
            Value::list([
                Value::symbol("error"),
                Value::string("Window system frame should be used"),
            ]),
            Value::list([
                Value::symbol("error"),
                Value::string("Window system frame should be used"),
            ]),
        ])
    );
}

#[test]
fn native_fontset_registry_family_matches_gnu() {
    let program = r#"
        (let ((name
               "-*-*-*-*-*-*-*-*-*-*-*-*-fontset-emaxx-native"))
          (list
           (fontset-list)
           (condition-case error-data
               (query-fontset "fontset-default")
             (error (car error-data)))
           (condition-case error-data
               (fontset-info t)
             (error (car error-data)))
           (new-fontset
            name
            (list
             (list
              'greek
              (font-spec
               :family "Greek"
               :registry "iso10646-1"))))
           (fontset-font name 955)
           (fontset-font name 65)
           (set-fontset-font
            name 955
            (font-spec
             :family "One"
             :registry "iso10646-1"))
           (set-fontset-font
            name 955
            (font-spec
             :family "Two"
             :registry "iso10646-1")
            nil 'append)
           (set-fontset-font
            name 955
            (font-spec
             :family "Zero"
             :registry "iso10646-1")
            nil 'prepend)
           (set-fontset-font
            name '(1024 . 1030)
            '("Range" . "iso10646-1"))
           (set-fontset-font
            name nil
            (font-spec
             :family "Fallback"
             :registry "iso10646-1"))
           (fontset-font name 955)
           (fontset-font name 955 t)
           (fontset-font name 1026 t)
           (fontset-font name 128 t)
           (fontset-list)
           (condition-case error-data
               (set-fontset-font
                name 65
                (font-spec :family "No"))
             (error (car error-data)))
           (condition-case error-data
               (new-fontset "bad" nil)
             (error (car error-data)))
           (new-fontset name nil)
           (fontset-font name 955 t)
           (fontset-list)))"#;
    let expected = r#"
        (("-*-*-*-*-*-*-*-*-*-*-*-*-fontset-default")
         error error
         "-*-*-*-*-*-*-*-*-*-*-*-*-fontset-emaxx-native"
         ("Greek" . "iso10646-1")
         nil nil nil nil nil nil
         ("Zero" . "iso10646-1")
         (("Zero" . "iso10646-1")
          ("One" . "iso10646-1")
          ("Two" . "iso10646-1")
          ("Fallback" . "iso10646-1"))
         (("Range" . "iso10646-1")
          ("Fallback" . "iso10646-1"))
         (("Fallback" . "iso10646-1"))
         ("-*-*-*-*-*-*-*-*-*-*-*-*-fontset-emaxx-native"
          "-*-*-*-*-*-*-*-*-*-*-*-*-fontset-default")
         error error
         "-*-*-*-*-*-*-*-*-*-*-*-*-fontset-emaxx-native"
         (("Fallback" . "iso10646-1"))
         ("-*-*-*-*-*-*-*-*-*-*-*-*-fontset-emaxx-native"
          "-*-*-*-*-*-*-*-*-*-*-*-*-fontset-default"))"#;
    let expected_printed = "((\"-*-*-*-*-*-*-*-*-*-*-*-*-fontset-default\") error error \"-*-*-*-*-*-*-*-*-*-*-*-*-fontset-emaxx-native\" (\"Greek\" . \"iso10646-1\") nil nil nil nil nil nil (\"Zero\" . \"iso10646-1\") ((\"Zero\" . \"iso10646-1\") (\"One\" . \"iso10646-1\") (\"Two\" . \"iso10646-1\") (\"Fallback\" . \"iso10646-1\")) ((\"Range\" . \"iso10646-1\") (\"Fallback\" . \"iso10646-1\")) ((\"Fallback\" . \"iso10646-1\")) (\"-*-*-*-*-*-*-*-*-*-*-*-*-fontset-emaxx-native\" \"-*-*-*-*-*-*-*-*-*-*-*-*-fontset-default\") error error \"-*-*-*-*-*-*-*-*-*-*-*-*-fontset-emaxx-native\" ((\"Fallback\" . \"iso10646-1\")) (\"-*-*-*-*-*-*-*-*-*-*-*-*-fontset-emaxx-native\" \"-*-*-*-*-*-*-*-*-*-*-*-*-fontset-default\"))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("fontset.c family contract should parse")
        .expect("fontset.c family contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("fontset.c family contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("fontset.c expected value should parse")
        .expect("fontset.c expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "fontset.c result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn nil_coding_system_queries_match_the_gnu_primitive_contract() {
    assert_upstream_primitive_contract(
        "(prin1 (list (coding-system-p nil)
                       (coding-system-type nil)
                       (coding-system-base nil)
                       (coding-system-eol-type nil)
                       (coding-system-equal nil nil)
                       (plist-get (coding-system-plist nil) :coding-type)))",
        "(t raw-text no-conversion 0 t raw-text)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let mut actual = [
        "coding-system-p",
        "coding-system-type",
        "coding-system-base",
        "coding-system-eol-type",
    ]
    .into_iter()
    .map(|name| {
        call(&mut interp, name, &[Value::Nil], &mut env)
            .unwrap_or_else(|error| panic!("{name} nil: {error}"))
    })
    .collect::<Vec<_>>();
    actual.push(
        call(
            &mut interp,
            "coding-system-equal",
            &[Value::Nil, Value::Nil],
            &mut env,
        )
        .expect("nil coding systems should compare equal"),
    );
    let plist = call(&mut interp, "coding-system-plist", &[Value::Nil], &mut env)
        .expect("nil coding system plist should resolve to no-conversion");
    actual.push(
        call(
            &mut interp,
            "plist-get",
            &[plist, Value::Symbol(":coding-type".into())],
            &mut env,
        )
        .expect("no-conversion plist should expose its public coding type"),
    );
    assert_eq!(
        Value::list(actual),
        Value::list([
            Value::T,
            Value::Symbol("raw-text".into()),
            Value::Symbol("no-conversion".into()),
            Value::Integer(0),
            Value::T,
            Value::Symbol("raw-text".into()),
        ])
    );
}

#[test]
fn bootstrap_coding_plists_expose_gnu_display_and_keyboard_metadata() {
    assert_upstream_primitive_contract(
        "(prin1 (mapcar
                   (lambda (coding)
                     (list coding
                           (coding-system-get coding :ascii-compatible-p)
                           (coding-system-get coding :charset-list)))
                   '(utf-8 utf-8-unix us-ascii iso-latin-1 raw-text undecided)))",
        "((utf-8 t (unicode)) (utf-8-unix t (unicode)) (us-ascii t (ascii)) (iso-latin-1 t (iso-8859-1)) (raw-text t nil) (undecided t (ascii)))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    for (coding, charset) in [
        ("utf-8", Some("unicode")),
        ("utf-8-unix", Some("unicode")),
        ("us-ascii", Some("ascii")),
        ("iso-latin-1", Some("iso-8859-1")),
        ("raw-text", None),
        ("undecided", Some("ascii")),
    ] {
        let plist = call(
            &mut interp,
            "coding-system-plist",
            &[Value::Symbol(coding.into())],
            &mut env,
        )
        .unwrap_or_else(|error| panic!("coding-system-plist {coding}: {error}"));
        assert_eq!(
            call(
                &mut interp,
                "plist-get",
                &[plist.clone(), Value::Symbol(":ascii-compatible-p".into())],
                &mut env,
            )
            .unwrap_or_else(|error| panic!("ascii-compatible {coding}: {error}")),
            Value::T,
        );
        assert_eq!(
            call(
                &mut interp,
                "plist-get",
                &[plist, Value::Symbol(":charset-list".into())],
                &mut env,
            )
            .unwrap_or_else(|error| panic!("charset-list {coding}: {error}")),
            charset
                .map(|name| Value::list([Value::Symbol(name.into())]))
                .unwrap_or(Value::Nil),
        );
    }
}

#[test]
fn coding_system_eol_type_exposes_base_variant_vectors() {
    assert_upstream_primitive_contract(
        "(prin1 (mapcar (lambda (coding)
                          (list coding (coding-system-eol-type coding)))
                        '(utf-8 utf-8-unix undecided no-conversion raw-text)))",
        "((utf-8 [utf-8-unix utf-8-dos utf-8-mac]) (utf-8-unix 0) (undecided [undecided-unix undecided-dos undecided-mac]) (no-conversion 0) (raw-text [raw-text-unix raw-text-dos raw-text-mac]))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    for (coding, expected) in [
        (
            "utf-8",
            Value::list([
                Value::Symbol("vector-literal".into()),
                Value::Symbol("utf-8-unix".into()),
                Value::Symbol("utf-8-dos".into()),
                Value::Symbol("utf-8-mac".into()),
            ]),
        ),
        ("utf-8-unix", Value::Integer(0)),
        (
            "undecided",
            Value::list([
                Value::Symbol("vector-literal".into()),
                Value::Symbol("undecided-unix".into()),
                Value::Symbol("undecided-dos".into()),
                Value::Symbol("undecided-mac".into()),
            ]),
        ),
        ("no-conversion", Value::Integer(0)),
    ] {
        assert_eq!(
            call(
                &mut interp,
                "coding-system-eol-type",
                &[Value::Symbol(coding.into())],
                &mut env,
            )
            .unwrap_or_else(|error| panic!("coding-system-eol-type {coding}: {error}")),
            expected,
        );
    }
}

#[test]
fn read_coding_system_matches_the_gnu_coding_primitive_contract() {
    assert_upstream_primitive_contract(
        r#"(progn
              (require 'ert-x)
              (prin1
               (list (ert-simulate-keys [?u ?t ?f ?- ?8 return]
                       (read-coding-system "Coding: " nil))
                     (ert-simulate-keys [return]
                       (read-coding-system "Coding: " 'utf-8))
                     (ert-simulate-keys [return]
                       (read-coding-system "Coding: " nil)))))"#,
        "(utf-8 utf-8 nil)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_global_binding("executing-kbd-macro", Value::T);
    for (events, default, expected) in [
        (
            vec![b'u', b't', b'f', b'-', b'8', 13],
            Value::Nil,
            Value::Symbol("utf-8".into()),
        ),
        (
            vec![13],
            Value::Symbol("utf-8".into()),
            Value::Symbol("utf-8".into()),
        ),
        (vec![13], Value::Nil, Value::Nil),
    ] {
        interp.set_global_binding(
            "unread-command-events",
            Value::list(events.into_iter().map(|event| Value::Integer(event.into()))),
        );
        assert_eq!(
            call(
                &mut interp,
                "read-coding-system",
                &[Value::String("Coding: ".into()), default],
                &mut env,
            )
            .expect("read-coding-system should consume simulated minibuffer input"),
            expected,
        );
    }

    assert_eq!(
        call(
            &mut interp,
            "command-error-default-function",
            &[
                Value::list([Value::Symbol("error".into()), Value::String("boom".into())]),
                Value::String(String::new().into()),
                Value::Nil,
            ],
            &mut env,
        )
        .expect("dumped help.el must be able to delegate to the host error reporter"),
        Value::Nil,
    );
}

#[test]
fn map_char_table_exposes_public_syntax_descriptors() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (let ((table (make-syntax-table))
                  punctuation)
              (modify-syntax-entry ?% "." table)
              (map-char-table
               (lambda (range syntax)
                 (when (or (and (integerp range) (= range ?%))
                           (and (consp range)
                                (<= (car range) ?%)
                                (>= (cdr range) ?%)))
                   (setq punctuation syntax)))
               table)
              (list (string-to-syntax ".") punctuation))"#,
    )
    .read_all()
    .expect("syntax-table mapping test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("syntax-table callbacks should receive public descriptors");
    assert_eq!(
        result,
        Value::list([
            Value::list([Value::Integer(1)]),
            Value::list([Value::Integer(1)]),
        ])
    );
}

#[test]
fn eval_region_preserves_point_and_uses_the_supplied_reader() {
    let program = r#"(progn
          (makunbound 'emaxx-eval-region-sample)
          (let ((reads 0))
            (with-temp-buffer
              (insert "(setq emaxx-eval-region-sample 1)\n(setq emaxx-eval-region-sample 42)\n")
              (goto-char 2)
              (let ((before (point))
                    (result
                     (eval-region
                      (point-min) (point-max) nil
                      (lambda (stream)
                        (setq reads (1+ reads))
                        (read stream)))))
                (list result before (point) reads
                      (symbol-value 'emaxx-eval-region-sample))))))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(nil 2 2 2 42)");

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let result = Reader::new(program)
        .read_all()
        .expect("eval-region contract should parse")
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("eval-region should evaluate the bounded input");
    assert_eq!(
        result,
        Value::list([
            Value::Nil,
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(42),
        ])
    );
}

#[test]
fn headless_terminal_queries_match_the_upstream_batch_terminal() {
    assert_upstream_primitive_contract(
        "(prin1 (list (tty-type) (tty-display-color-p)\
                      (tty-display-color-cells) (controlling-tty-p)\
                      (tty-top-frame)))",
        "(nil nil 0 nil nil)",
    );

    let mut interp = Interpreter::new();
    assert_eq!(
        [
            "tty-type",
            "tty-display-color-p",
            "tty-display-color-cells",
            "controlling-tty-p",
            "tty-top-frame",
        ]
        .into_iter()
        .map(|name| call(&mut interp, name, &[], &mut Vec::new())
            .unwrap_or_else(|error| panic!("query {name}: {error}")))
        .collect::<Vec<_>>(),
        vec![
            Value::Nil,
            Value::Nil,
            Value::Integer(0),
            Value::Nil,
            Value::Nil,
        ]
    );
}

#[test]
fn native_terminal_state_and_tty_controls_share_the_gnu_headless_contract() {
    let program = r#"(let* ((terminal (frame-terminal))
                            (capture
                             (lambda (thunk)
                               (condition-case err
                                   (list 'ok (funcall thunk))
                                 (error err)))))
                       (list
                        (tty-no-underline)
                        (funcall capture
                                 (lambda () (tty-no-underline 'bogus)))
                        (funcall capture (lambda () (suspend-tty)))
                        (funcall capture (lambda () (resume-tty terminal)))
                        (funcall capture
                                 (lambda () (tty--output-buffer-size)))
                        (funcall capture
                                 (lambda ()
                                   (tty--set-output-buffer-size 0 terminal)))
                        (funcall capture
                                 (lambda () (tty--set-output-buffer-size -1)))
                        (terminal-parameter terminal 'emaxx-native-probe)
                        (set-terminal-parameter
                         terminal 'emaxx-native-probe 1)
                        (set-terminal-parameter
                         terminal 'emaxx-native-probe 2)
                        (terminal-parameter terminal 'emaxx-native-probe)
                        (assq 'emaxx-native-probe
                              (terminal-parameters terminal))
                        (terminal-name terminal)
                        (terminal-live-p terminal)
                        (terminal-live-p 'bogus)))"#;
    let expected = r#"(nil (wrong-type-argument terminal-live-p bogus) (error "Attempt to suspend a non-text terminal device") (error "Attempt to resume a non-text terminal device") (error "Not a tty terminal") (error "Attempt to suspend a non-text terminal device") (error "Invalid output buffer size") nil nil 1 2 (emaxx-native-probe . 2) "initial_terminal" t nil)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let form = Reader::new(program)
        .read_all()
        .expect("read native terminal contract")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate native terminal contract");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn native_terminal_identity_is_opaque_and_shared_with_the_frame() {
    let program = r#"
        (let* ((frame (selected-frame))
               (terminal (frame-terminal frame)))
          (list
           (type-of terminal)
           (symbolp terminal)
           (recordp terminal)
           (eq terminal (car (terminal-list)))
           (terminal-live-p terminal)
           (eq terminal (frame-terminal frame))
           (terminal-name terminal)))"#;
    let expected = r#"(terminal nil nil t t t "initial_terminal")"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let form = Reader::new(program)
        .read()
        .expect("terminal identity contract should parse")
        .expect("terminal identity contract should contain a form");
    let actual = interp
        .eval(&form, &mut Vec::new())
        .expect("terminal identity contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("terminal identity expected value should parse")
        .expect("terminal identity expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "terminal identity differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_delete_terminal_tracks_liveness_and_runs_its_hook_before_removal() {
    let program = r#"(let ((terminal (car (terminal-list)))
                           seen)
                       (list
                        (terminal-live-p terminal)
                        (condition-case error-data
                            (delete-terminal terminal)
                          (error
                           (list (car error-data)
                                 (cadr error-data))))
                        (progn
                          (add-hook
                           'delete-terminal-functions
                           (lambda (argument)
                             (setq seen
                                   (list (eq argument terminal)
                                         (terminal-live-p argument)))))
                          (delete-terminal terminal t))
                        seen
                        (terminal-live-p terminal)
                        (terminal-list)))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        r#"(t (error "Attempt to delete the sole active display terminal") nil (t t) nil nil)"#,
    );

    let mut interp = Interpreter::new();
    let form = Reader::new(program)
        .read_all()
        .expect("read delete-terminal contract")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate delete-terminal contract");
    assert_eq!(
        result,
        Value::list([
            Value::T,
            Value::list([
                Value::symbol("error"),
                Value::String("Attempt to delete the sole active display terminal".into()),
            ]),
            Value::Nil,
            Value::list([Value::T, Value::T]),
            Value::Nil,
            Value::Nil,
        ])
    );

    let mut no_op_interp = Interpreter::new();
    assert_eq!(
        call(
            &mut no_op_interp,
            "delete-terminal",
            &[Value::symbol("bogus"), Value::T],
            &mut Vec::new(),
        )
        .expect("an object that does not designate a terminal is a no-op"),
        Value::Nil
    );
    assert!(no_op_interp.terminal_live());
}

#[test]
fn native_dispnew_family_tracks_menu_state_and_headless_redisplay_contracts() {
    let state_program = r#"(progn
          (defvar emaxx-disp-state nil)
          (list
           (frame-or-buffer-changed-p 'emaxx-disp-state)
           (vectorp emaxx-disp-state)
           (frame-or-buffer-changed-p 'emaxx-disp-state)
           (progn
             (set-buffer-modified-p t)
             (frame-or-buffer-changed-p 'emaxx-disp-state))
           (frame-or-buffer-changed-p 'emaxx-disp-state)
           (progn
             (set-buffer-modified-p nil)
             (frame-or-buffer-changed-p 'emaxx-disp-state))
           (frame-or-buffer-changed-p 'emaxx-disp-state)
           (progn
             (get-buffer-create "visible")
             (frame-or-buffer-changed-p 'emaxx-disp-state))
           (frame-or-buffer-changed-p 'emaxx-disp-state)
           (progn
             (kill-buffer "visible")
             (frame-or-buffer-changed-p 'emaxx-disp-state))
           (progn
             (get-buffer-create " hidden")
             (frame-or-buffer-changed-p 'emaxx-disp-state))
           (progn
             (kill-buffer " hidden")
             (frame-or-buffer-changed-p 'emaxx-disp-state))
           (progn
             (aset emaxx-disp-state 0 'tampered)
             (frame-or-buffer-changed-p 'emaxx-disp-state))))"#;
    let expected_state = "(t t nil t nil t nil t nil t nil nil t)";
    assert_upstream_primitive_contract(&format!("(prin1 {state_program})"), expected_state);

    let mut interp = Interpreter::new();
    let state_form = Reader::new(state_program)
        .read_all()
        .expect("read dispnew state contract")
        .remove(0);
    assert_eq!(
        interp
            .eval(&state_form, &mut Vec::new())
            .expect("evaluate dispnew state contract")
            .to_string(),
        expected_state
    );

    let surface_program = r#"(list
          (redisplay)
          (redisplay t)
          (redraw-frame)
          (redraw-frame nil)
          (redraw-frame (selected-frame))
          (redraw-display)
          (condition-case error-data
              (open-termscript nil)
            (error error-data))
          (condition-case error-data
              (open-termscript "ignored")
            (error error-data))
          (condition-case error-data
              (display--update-for-mouse-movement 1.0 2)
            (error error-data)))"#;
    let expected_surface = r#"(t t nil nil nil nil (error "Current frame is not on a tty device") (error "Current frame is not on a tty device") (wrong-type-argument fixnump 1.0))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {surface_program})"), expected_surface);
    let surface_form = Reader::new(surface_program)
        .read_all()
        .expect("read dispnew headless surface")
        .remove(0);
    assert_eq!(
        interp
            .eval(&surface_form, &mut Vec::new())
            .expect("evaluate dispnew headless surface")
            .to_string(),
        expected_surface
    );
    assert_eq!(
        call(
            &mut interp,
            "display--update-for-mouse-movement",
            &[Value::Integer(1), Value::Integer(2)],
            &mut Vec::new(),
        )
        .expect("valid fixnum mouse coordinates"),
        Value::Nil
    );
}

#[test]
fn headless_input_mode_family_matches_the_upstream_batch_terminal() {
    let program = "(list (current-input-mode)
                          (progn (set-input-meta-mode 8)
                                 (current-input-mode))
                          (progn (set-input-mode nil t nil ?x)
                                 (current-input-mode))
                          (progn (set-input-mode t nil 'encoded ?\\C-g)
                                 (current-input-mode)))";
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "((t nil t 7) (t nil t 7) (nil nil t 7) (t nil t 7))",
    );

    let mut interp = Interpreter::new();
    let form = Reader::new(program)
        .read_all()
        .expect("read input-mode family probe")
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate input-mode family probe"),
        Value::list([
            Value::list([Value::T, Value::Nil, Value::T, Value::Integer(7)]),
            Value::list([Value::T, Value::Nil, Value::T, Value::Integer(7)]),
            Value::list([Value::Nil, Value::Nil, Value::T, Value::Integer(7)]),
            Value::list([Value::T, Value::Nil, Value::T, Value::Integer(7)]),
        ])
    );
}

#[test]
fn parse_font_names_matches_core_upstream_cases() {
    let cases = [
        (" ", Some(" "), None, None, None, None, None),
        ("Monospace", Some("Monospace"), None, None, None, None, None),
        (
            "Monospace Serif",
            Some("Monospace Serif"),
            None,
            None,
            None,
            None,
            None,
        ),
        ("Foo1", Some("Foo1"), None, None, None, None, None),
        ("12", None, Some(12.0), None, None, None, None),
        ("12 ", Some("12 "), None, None, None, None, None),
        ("Foo:", Some("Foo"), None, None, None, None, None),
        ("Foo-8", Some("Foo"), Some(8.0), None, None, None, None),
        ("Foo-18:", Some("Foo"), Some(18.0), None, None, None, None),
        (
            "Foo-18:light",
            Some("Foo"),
            Some(18.0),
            Some("light"),
            None,
            None,
            None,
        ),
        (
            "Foo 10:weight=bold",
            Some("Foo 10"),
            None,
            Some("bold"),
            None,
            None,
            None,
        ),
        (
            "Foo-12:weight=bold",
            Some("Foo"),
            Some(12.0),
            Some("bold"),
            None,
            None,
            None,
        ),
        (
            "Foo 8-20:slant=oblique",
            Some("Foo 8"),
            Some(20.0),
            None,
            Some("oblique"),
            None,
            None,
        ),
        (
            "Foo:light:roman",
            Some("Foo"),
            None,
            Some("light"),
            Some("roman"),
            None,
            None,
        ),
        (
            "Foo:italic:roman",
            Some("Foo"),
            None,
            None,
            Some("roman"),
            None,
            None,
        ),
        (
            "Foo 12:light:oblique",
            Some("Foo 12"),
            None,
            Some("light"),
            Some("oblique"),
            None,
            None,
        ),
        (
            "Foo-12:demibold:oblique",
            Some("Foo"),
            Some(12.0),
            Some("demibold"),
            Some("oblique"),
            None,
            None,
        ),
        (
            "Foo:black:proportional",
            Some("Foo"),
            None,
            Some("black"),
            None,
            Some(0),
            None,
        ),
        (
            "Foo-10:black:proportional",
            Some("Foo"),
            Some(10.0),
            Some("black"),
            None,
            Some(0),
            None,
        ),
        (
            "Foo:weight=normal",
            Some("Foo"),
            None,
            Some("normal"),
            None,
            None,
            None,
        ),
        (
            "Foo:weight=bold",
            Some("Foo"),
            None,
            Some("bold"),
            None,
            None,
            None,
        ),
        (
            "Foo:weight=bold:slant=italic",
            Some("Foo"),
            None,
            Some("bold"),
            Some("italic"),
            None,
            None,
        ),
        (
            "Foo:weight=bold:slant=italic:mono",
            Some("Foo"),
            None,
            Some("bold"),
            Some("italic"),
            Some(100),
            None,
        ),
        (
            "Foo-10:demibold:slant=normal",
            Some("Foo"),
            Some(10.0),
            Some("demibold"),
            Some("normal"),
            None,
            None,
        ),
        (
            "Foo 11-16:oblique:weight=bold",
            Some("Foo 11"),
            Some(16.0),
            Some("bold"),
            Some("oblique"),
            None,
            None,
        ),
        (
            "Foo:oblique:randomprop=randomtag:weight=bold",
            Some("Foo"),
            None,
            Some("bold"),
            Some("oblique"),
            None,
            None,
        ),
        (
            "Foo:randomprop=randomtag:bar=baz",
            Some("Foo"),
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "Foo Book Light:bar=baz",
            Some("Foo Book Light"),
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "Foo Book Light 10:bar=baz",
            Some("Foo Book Light 10"),
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "Foo Book Light-10:bar=baz",
            Some("Foo Book Light"),
            Some(10.0),
            None,
            None,
            None,
            None,
        ),
        ("Oblique", None, None, None, Some("oblique"), None, None),
        ("Bold 17", None, Some(17.0), Some("bold"), None, None, None),
        ("17 Bold", Some("17"), None, Some("bold"), None, None, None),
        (
            "Book Oblique 2",
            None,
            Some(2.0),
            Some("book"),
            Some("oblique"),
            None,
            None,
        ),
        ("Bar 7", Some("Bar"), Some(7.0), None, None, None, None),
        (
            "Bar Ultra-Light",
            Some("Bar"),
            None,
            Some("ultra-light"),
            None,
            None,
            None,
        ),
        (
            "Bar Light 8",
            Some("Bar"),
            Some(8.0),
            Some("light"),
            None,
            None,
            None,
        ),
        (
            "Bar Book Medium 9",
            Some("Bar"),
            Some(9.0),
            Some("medium"),
            None,
            None,
            None,
        ),
        (
            "Bar Semi-Bold Italic 10",
            Some("Bar"),
            Some(10.0),
            Some("semi-bold"),
            Some("italic"),
            None,
            None,
        ),
        (
            "Bar Semi-Condensed Bold Italic 11",
            Some("Bar"),
            Some(11.0),
            Some("bold"),
            Some("italic"),
            None,
            None,
        ),
        (
            "Foo 10 11",
            Some("Foo 10"),
            Some(11.0),
            None,
            None,
            None,
            None,
        ),
        (
            "Foo 1985 Book",
            Some("Foo 1985"),
            None,
            Some("book"),
            None,
            None,
            None,
        ),
        (
            "Foo 1985 A Book",
            Some("Foo 1985 A"),
            None,
            Some("book"),
            None,
            None,
            None,
        ),
        (
            "Foo 1 Book 12",
            Some("Foo 1"),
            Some(12.0),
            Some("book"),
            None,
            None,
            None,
        ),
        (
            "Foo A Book 12 A",
            Some("Foo A Book 12 A"),
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "Foo 1985 Book 12 Oblique",
            Some("Foo 1985 Book 12"),
            None,
            None,
            Some("oblique"),
            None,
            None,
        ),
        (
            "Foo 1985 Book 12 Italic 10",
            Some("Foo 1985 Book 12"),
            Some(10.0),
            None,
            Some("italic"),
            None,
            None,
        ),
        (
            "Foo Book Bar 6 Italic",
            Some("Foo Book Bar 6"),
            None,
            None,
            Some("italic"),
            None,
            None,
        ),
        (
            "Foo Book Bar Bold",
            Some("Foo Book Bar"),
            None,
            Some("bold"),
            None,
            None,
            None,
        ),
        (
            "-GNU -FreeSans-semibold-italic-normal-*-*-*-*-*-*-0-iso10646-1",
            Some("FreeSans"),
            None,
            Some("semi-bold"),
            None,
            None,
            Some("GNU "),
        ),
        (
            "-Take-mikachan-PS-normal-normal-normal-*-*-*-*-*-*-0-iso10646-1",
            Some("mikachan-PS"),
            None,
            Some("normal"),
            None,
            None,
            Some("Take"),
        ),
        (
            "-foundry-name-with-lots-of-dashes-normal-normal-normal-*-*-*-*-*-*-0-iso10646-1",
            Some("name-with-lots-of-dashes"),
            None,
            Some("normal"),
            None,
            None,
            Some("foundry"),
        ),
    ];

    for (name, family, size, weight, slant, spacing, foundry) in cases {
        let actual = parse_font_name(name);
        assert_eq!(
            actual.family.as_deref(),
            family,
            "family mismatch for {name:?}"
        );
        assert_eq!(actual.size, size, "size mismatch for {name:?}");
        assert_eq!(
            actual.weight.as_deref(),
            weight,
            "weight mismatch for {name:?}"
        );
        assert_eq!(
            actual.slant.as_deref(),
            slant,
            "slant mismatch for {name:?}"
        );
        assert_eq!(actual.spacing, spacing, "spacing mismatch for {name:?}");
        assert_eq!(
            actual.foundry.as_deref(),
            foundry,
            "foundry mismatch for {name:?}"
        );
    }
}

#[test]
fn file_name_path_helpers_match_core_unix_cases() {
    assert_eq!(file_name_directory("/abc"), Some("/".into()));
    assert_eq!(file_name_directory("/abc/"), Some("/abc/".into()));
    assert_eq!(file_name_directory("abc"), None);

    assert_eq!(file_name_as_directory(""), "./");
    assert_eq!(file_name_as_directory("/abc"), "/abc/");
    assert_eq!(file_name_as_directory("/abc/"), "/abc/");

    assert_eq!(directory_file_name("/"), "/");
    assert_eq!(directory_file_name("//"), "//");
    assert_eq!(directory_file_name("///"), "/");
    assert_eq!(directory_file_name("/abc/"), "/abc");

    assert_eq!(file_name_concat(&["foo".into(), "bar".into()]), "foo/bar");
    assert_eq!(file_name_concat(&["foo/".into(), "bar".into()]), "foo/bar");
    assert_eq!(
        file_name_concat(&["foo//".into(), "bar".into()]),
        "foo//bar"
    );
    assert_eq!(expand_file_name("/abc/", None), "/abc/");
    assert_eq!(expand_file_name("abc/", Some("/tmp/")), "/tmp/abc/");
    assert!(file_name_absolute_p("/tmp/example"));
    assert!(file_name_absolute_p("~/example"));
    assert!(!"~/example".starts_with('/'));
}

#[test]
fn ert_resource_directory_prefers_sibling_resources_dir() {
    assert_eq!(
        ert_resource_directory_for("/tmp/example-tests.el"),
        "/tmp/example-resources/"
    );
    assert_eq!(
        ert_resource_directory_for("/Users/example/projects/emacs/test/src/syntax-tests.el"),
        "/Users/example/projects/emacs/test/src/syntax-resources/"
    );
}

#[test]
fn ert_resource_directory_trims_test_suffixes_like_emacs() {
    assert_eq!(
        ert_resource_directory_for("/tmp/foo-test.el"),
        "/tmp/foo-resources/"
    );
    assert_eq!(
        ert_resource_directory_for("/tmp/foo-tests.el"),
        "/tmp/foo-resources/"
    );
    assert_eq!(
        ert_resource_directory_for("/tmp/bookmark.el"),
        "/tmp/bookmark-resources/"
    );
}

#[test]
fn ert_gcc_is_clang_matches_upstream_apple_markers() {
    assert_eq!(
        apple_gcc_version_match("Apple LLVM version 10.0.0 (clang-1000.10.44.4)"),
        Some(0)
    );
    assert!(
        apple_gcc_version_match("gcc wrapper\nInstalledDir: /Applications/Xcode.app/Contents")
            .is_some()
    );
    assert_eq!(
        apple_gcc_version_match("gcc (Homebrew GCC 15.2.0) 15.2.0"),
        None
    );
}

#[test]
fn directory_files_returns_mutable_sorted_names_with_dot_entries() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!("emaxx-directory-files-{unique}"));
    std::fs::create_dir_all(directory.join("ext4")).expect("create ext4 fixture");

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let result = call(
        &mut interp,
        "directory-files",
        &[Value::String(directory.display().to_string().into())],
        &mut env,
    )
    .expect("directory-files should succeed");

    assert_eq!(
        result,
        Value::list([
            Value::String(".".into()),
            Value::String("..".into()),
            Value::String("ext4".into()),
        ])
    );

    interp.set_variable(
        "default-directory",
        Value::String(format!("{}/", directory.display()).into()),
        &mut env,
    );
    let relative_full = call(
        &mut interp,
        "directory-files",
        &[
            Value::String(".".into()),
            Value::T,
            Value::String("\\`[^.]".into()),
        ],
        &mut env,
    )
    .expect("relative directory-files should honor dynamic default-directory");
    assert_eq!(
        relative_full,
        Value::list([Value::String(
            directory.join("ext4").display().to_string().into(),
        )])
    );

    let file_name = result.to_vec().expect("directory entries")[2].clone();
    call(
        &mut interp,
        "add-text-properties",
        &[
            Value::Integer(0),
            Value::Integer(4),
            Value::list([Value::Symbol("face".into()), Value::Symbol("bold".into())]),
            file_name.clone(),
        ],
        &mut env,
    )
    .expect("directory file names should accept text properties");
    assert_eq!(
        call(
            &mut interp,
            "get-text-property",
            &[Value::Integer(0), Value::Symbol("face".into()), file_name,],
            &mut env,
        )
        .expect("read file-name text property"),
        Value::Symbol("bold".into())
    );

    let _ = std::fs::remove_dir_all(&directory);
}

#[test]
fn seq_uniq_preserves_first_occurrence_order() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let result = call(
        &mut interp,
        "seq-uniq",
        &[Value::list([
            Value::String("/".into()),
            Value::String("/proc".into()),
            Value::String("/".into()),
            Value::String("/dev".into()),
            Value::String("/proc".into()),
        ])],
        &mut env,
    )
    .expect("seq-uniq should succeed");

    assert_eq!(
        result,
        Value::list([
            Value::String("/".into()),
            Value::String("/proc".into()),
            Value::String("/dev".into()),
        ])
    );
}

#[test]
fn charset_helpers_cover_ascii_unicode_and_priority_mutation() {
    let mut interp = Interpreter::new();
    assert!(interp.has_charset("ascii"));
    assert_eq!(interp.charset_id("iso-8859-1"), Some(1));
    assert_eq!(interp.charset_id("unicode"), Some(2));
    assert_eq!(interp.charset_id("emacs"), Some(3));
    assert_eq!(interp.charset_id("eight-bit"), Some(4));
    assert_eq!(charset_for_char('A' as u32), "ascii");
    assert_eq!(charset_for_char('あ' as u32), "unicode");

    interp
        .define_charset_alias("latin", "ascii")
        .expect("ascii alias should be accepted");
    assert!(interp.has_charset("latin"));

    interp.set_charset_priority(&["ascii".into(), "unicode".into()]);
    assert_eq!(
        interp.charset_priority_list(),
        vec!["ascii", "unicode", "eight-bit"]
    );
    assert_eq!(
        charsets_for_text("Aあ", &interp),
        vec![
            Value::Symbol("ascii".into()),
            Value::Symbol("unicode".into())
        ]
    );
}

#[test]
fn unicode_char_property_helpers_cover_names_and_general_categories() {
    assert_upstream_primitive_contract(
        "(prin1 (mapcar (lambda (u)
                          (list u
                                (get-char-code-property u 'name)
                                (get-char-code-property u 'general-category)))
                        '(#x16100 #x1CC00 #x14646 #x2FFC #x4E00
                          #xD800 #xE000 #x10FFFF)))",
        "((90368 nil Cn) (117760 nil Cn) (83526 \"ANATOLIAN HIEROGLYPH A530\" Lo) (12284 \"IDEOGRAPHIC DESCRIPTION CHARACTER SURROUND FROM RIGHT\" So) (19968 \"CJK IDEOGRAPH-4E00\" Lo) (55296 \"HIGH SURROGATE-D800\" Cs) (57344 nil Co) (1114111 nil Cn))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let name = call(
        &mut interp,
        "get-char-code-property",
        &[
            Value::Integer('\u{2026}' as i64),
            Value::Symbol("name".into()),
        ],
        &mut env,
    )
    .expect("get-char-code-property should return Unicode names");
    assert_eq!(name, Value::String("HORIZONTAL ELLIPSIS".into()));

    let category = call(
        &mut interp,
        "get-char-code-property",
        &[
            Value::Integer('\u{2026}' as i64),
            Value::Symbol("general-category".into()),
        ],
        &mut env,
    )
    .expect("get-char-code-property should return Unicode general categories");
    assert_eq!(category, Value::Symbol("Po".into()));

    let description = call(
        &mut interp,
        "char-code-property-description",
        &[Value::Symbol("general-category".into()), category],
        &mut env,
    )
    .expect("char-code-property-description should describe general categories");
    assert_eq!(description, Value::String("Punctuation, Other".into()));

    for (code, expected_name, expected_category) in [
        (0x16100, None, "Cn"),
        (0x1CC00, None, "Cn"),
        (0x14646, Some("ANATOLIAN HIEROGLYPH A530"), "Lo"),
        (
            0x2FFC,
            Some("IDEOGRAPHIC DESCRIPTION CHARACTER SURROUND FROM RIGHT"),
            "So",
        ),
        (0x4E00, Some("CJK IDEOGRAPH-4E00"), "Lo"),
        (0xD800, Some("HIGH SURROGATE-D800"), "Cs"),
        (0xE000, None, "Co"),
        (0x10FFFF, None, "Cn"),
    ] {
        assert_eq!(
            call(
                &mut interp,
                "get-char-code-property",
                &[Value::Integer(code), Value::Symbol("name".into())],
                &mut env,
            )
            .expect("read the Unicode 15.1 name property"),
            expected_name.map_or(Value::Nil, |name| Value::String(name.into()))
        );
        assert_eq!(
            call(
                &mut interp,
                "get-char-code-property",
                &[
                    Value::Integer(code),
                    Value::Symbol("general-category".into()),
                ],
                &mut env,
            )
            .expect("read the Unicode 15.1 category property"),
            Value::Symbol(expected_category.into())
        );
    }
}

#[test]
fn max_char_distinguishes_the_internal_and_unicode_character_spaces() {
    // Pin the producer contract that bounds Unicode-wide Lisp loops.  A
    // regression here multiplies every `(dotimes (u (1+ (max-char 'ucs))))'
    // scan by almost four before any property lookup is even considered.
    assert_upstream_primitive_contract(
        "(prin1 (list (max-char) (max-char 'ucs) (max-char t) (max-char nil)))",
        "(4194303 1114111 1114111 4194303)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    assert_eq!(
        call(&mut interp, "max-char", &[], &mut env).expect("internal character ceiling"),
        Value::Integer(0x3f_ffff)
    );
    for unicode in [Value::Symbol("ucs".into()), Value::T] {
        assert_eq!(
            call(&mut interp, "max-char", &[unicode], &mut env).expect("Unicode character ceiling"),
            Value::Integer(0x10_ffff)
        );
    }
    assert_eq!(
        call(&mut interp, "max-char", &[Value::Nil], &mut env)
            .expect("nil retains the internal character ceiling"),
        Value::Integer(0x3f_ffff)
    );
}

#[test]
fn unicode_property_tables_are_stable_and_preserve_overrides() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let property = Value::Symbol("name".into());
    let table = call(
        &mut interp,
        "unicode-property-table-internal",
        std::slice::from_ref(&property),
        &mut env,
    )
    .expect("create the lazy Unicode name table");

    // This is a hot primitive in Unicode-wide scans.  Stable identity both
    // matches GNU's char-code-property-alist cache and prevents per-character
    // char-table allocation from returning unnoticed.
    for _ in 0..10_000 {
        assert_eq!(
            call(
                &mut interp,
                "unicode-property-table-internal",
                std::slice::from_ref(&property),
                &mut env,
            )
            .expect("reuse the Unicode name table"),
            table
        );
    }

    call(
        &mut interp,
        "put-unicode-property-internal",
        &[
            table.clone(),
            Value::Integer('A' as i64),
            Value::String("EMAXX TEST OVERRIDE".into()),
        ],
        &mut env,
    )
    .expect("override a Unicode table entry");
    assert_eq!(
        call(
            &mut interp,
            "get-unicode-property-internal",
            &[table, Value::Integer('A' as i64)],
            &mut env,
        )
        .expect("read the Unicode table override"),
        Value::String("EMAXX TEST OVERRIDE".into())
    );
}

#[test]
fn plain_regexp_cache_hits_skip_syntax_table_rendering() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let pattern = Value::String("-[0-9A-F]+\\'".into());
    let haystack = Value::String("CJK IDEOGRAPH-4E00".into());

    regexp::reset_regexp_syntax_class_render_count();
    for _ in 0..10_000 {
        assert_eq!(
            call(
                &mut interp,
                "string-match-p",
                &[pattern.clone(), haystack.clone()],
                &mut env,
            )
            .expect("match a table-independent regexp"),
            Value::Integer(13)
        );
    }
    assert_eq!(
        regexp::regexp_syntax_class_render_count(),
        0,
        "plain compiled-regexp cache hits must not rebuild syntax-table classes"
    );
}

#[test]
fn equal_string_hash_tables_scale_without_losing_public_semantics() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let table = call(
        &mut interp,
        "make-hash-table",
        &[
            Value::Symbol(":test".into()),
            Value::Symbol("equal".into()),
            Value::Symbol(":size".into()),
            Value::Integer(20_000),
        ],
        &mut env,
    )
    .expect("create an equal string hash table");

    for index in 0..20_000 {
        call(
            &mut interp,
            "puthash",
            &[
                Value::String(format!("UNICODE NAME {index}").into()),
                Value::Integer(index),
                table.clone(),
            ],
            &mut env,
        )
        .expect("insert an indexed string key");
    }
    for index in [0, 9_999, 19_999] {
        assert_eq!(
            call(
                &mut interp,
                "gethash",
                &[
                    Value::String(format!("UNICODE NAME {index}").into()),
                    table.clone(),
                ],
                &mut env,
            )
            .expect("look up an indexed string key"),
            Value::Integer(index)
        );
    }
    call(
        &mut interp,
        "puthash",
        &[
            make_shared_string_value_with_multibyte("SHARED UNICODE NAME".into(), Vec::new(), true),
            Value::Integer(20_000),
            table.clone(),
        ],
        &mut env,
    )
    .expect("insert a shared Lisp string through the same index");
    assert_eq!(
        call(
            &mut interp,
            "gethash",
            &[Value::String("SHARED UNICODE NAME".into()), table.clone(),],
            &mut env,
        )
        .expect("plain and shared strings compare equal as hash keys"),
        Value::Integer(20_000)
    );
    assert_eq!(
        call(
            &mut interp,
            "hash-table-count",
            std::slice::from_ref(&table),
            &mut env,
        )
        .expect("count indexed entries"),
        Value::Integer(20_001)
    );

    let copy = call(
        &mut interp,
        "copy-hash-table",
        std::slice::from_ref(&table),
        &mut env,
    )
    .expect("copy the indexed table");
    call(
        &mut interp,
        "puthash",
        &[
            Value::String("UNICODE NAME 9999".into()),
            Value::Integer(-1),
            table,
        ],
        &mut env,
    )
    .expect("replace an entry in the original table");
    assert_eq!(
        call(
            &mut interp,
            "gethash",
            &[Value::String("UNICODE NAME 9999".into()), copy,],
            &mut env,
        )
        .expect("the copied table remains independent"),
        Value::Integer(9_999)
    );
}

#[test]
fn equal_structured_hash_tables_use_structural_buckets() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let table = call(
        &mut interp,
        "make-hash-table",
        &[
            Value::Symbol(":test".into()),
            Value::Symbol("equal".into()),
            Value::Symbol(":size".into()),
            Value::Integer(5_000),
        ],
        &mut env,
    )
    .expect("create an equal structured-key hash table");

    let started = Instant::now();
    for index in 0..5_000 {
        let key = Value::list([
            Value::Symbol("macroexp-warning".into()),
            Value::list([
                Value::Integer(index),
                Value::String(format!("generated form {index}").into()),
            ]),
        ]);
        call(
            &mut interp,
            "puthash",
            &[key, Value::Integer(index), table.clone()],
            &mut env,
        )
        .expect("insert a structurally indexed form");
    }
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "structured equal keys fell back to a quadratic scan: {:?}",
        started.elapsed()
    );

    for index in [0, 2_499, 4_999] {
        let equivalent_key = Value::list([
            Value::Symbol("macroexp-warning".into()),
            Value::list([
                Value::Integer(index),
                Value::String(format!("generated form {index}").into()),
            ]),
        ]);
        assert_eq!(
            call(
                &mut interp,
                "gethash",
                &[equivalent_key, table.clone()],
                &mut env,
            )
            .expect("look up a separately allocated equal form"),
            Value::Integer(index)
        );
    }

    call(
        &mut interp,
        "puthash",
        &[
            Value::Integer(7),
            Value::Symbol("number".into()),
            table.clone(),
        ],
        &mut env,
    )
    .expect("insert a fixnum key");
    assert_eq!(
        call(
            &mut interp,
            "gethash",
            &[Value::big_integer(BigInt::from(7)), table],
            &mut env,
        )
        .expect("equal fixnum and bignum representations share a bucket"),
        Value::Symbol("number".into())
    );
}

#[test]
fn ordinary_memq_skips_symbol_with_position_mode_resolution() {
    let mut interp = Interpreter::new();
    let mut env = vec![vec![("symbols-with-pos-enabled".into(), Value::T)].into()];
    let symbols = Value::list(
        (0..2_048).map(|index| Value::Symbol(format!("ordinary-symbol-{index}").into())),
    );

    reset_symbol_with_pos_flag_read_count();
    for _ in 0..256 {
        assert_eq!(
            call(
                &mut interp,
                "memq",
                &[Value::Symbol("absent-symbol".into()), symbols.clone()],
                &mut env,
            )
            .expect("scan ordinary symbols"),
            Value::Nil
        );
    }
    assert_eq!(
        symbol_with_pos_flag_read_count(),
        0,
        "ordinary symbol identity must not resolve the dynamic symbol-with-position mode"
    );

    let positioned = call(
        &mut interp,
        "position-symbol",
        &[Value::Symbol("ordinary-symbol-1".into()), Value::Integer(7)],
        &mut env,
    )
    .expect("make a positioned symbol");
    assert_eq!(
        call(
            &mut interp,
            "eq",
            &[positioned, Value::Symbol("ordinary-symbol-1".into())],
            &mut env,
        )
        .expect("compare a positioned symbol"),
        Value::T
    );
    assert_eq!(
        symbol_with_pos_flag_read_count(),
        1,
        "positioned-symbol equality must still honor the dynamic mode"
    );
}

#[test]
fn preloaded_undo_keeps_gnu_lisp_command_ownership_and_behavior() {
    let program = r#"
          (list
           (commandp 'undo)
           (subrp (symbol-function 'undo))
           (car (interactive-form 'undo))
           (with-temp-buffer
             (buffer-enable-undo)
             (insert "first")
             (undo-boundary)
             (insert " second")
             ;; The command loop (and `ert-simulate-command') closes each
             ;; completed command with this boundary before invoking undo.
             (undo-boundary)
             (undo)
             (buffer-string)))
        "#;
    let form = Reader::new(program)
        .read()
        .expect("read preloaded undo contract")
        .expect("preloaded undo contract form");
    let expected = Reader::new("(t nil interactive \"first\")")
        .read()
        .expect("read preloaded undo expectation")
        .expect("preloaded undo expectation");
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load the Emaxx preload compatibility layer");
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate preloaded undo contract");
    assert_eq!(result, expected);
}

#[test]
fn dynamic_buffer_undo_list_binding_restores_native_history() {
    let program = r#"
          (with-temp-buffer
            (buffer-enable-undo)
            (insert "before")
            (undo-boundary)
            (let ((original buffer-undo-list)
                  temporary-result)
              (let ((buffer-undo-list nil))
                (insert " temporary")
                (let ((pending buffer-undo-list)
                      (undo-in-progress t))
                  (setq pending (primitive-undo 1 pending))
                  (setq temporary-result (list (buffer-string) pending))))
              (list temporary-result (eq buffer-undo-list original))))
        "#;
    let form = Reader::new(program)
        .read()
        .expect("read dynamic undo binding contract")
        .expect("dynamic undo binding form");
    let expected = Reader::new("((\"before\" nil) t)")
        .read()
        .expect("read dynamic undo binding expectation")
        .expect("dynamic undo binding expectation");
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load the Emaxx preload compatibility layer");
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate dynamic undo binding contract");
    assert_eq!(result, expected);
}

#[test]
fn buffer_undo_list_assignment_preserves_cons_identity() {
    let program = r#"
          (with-temp-buffer
            (buffer-enable-undo)
            (insert "before")
            (let* ((original buffer-undo-list)
                   (extended (cons nil original)))
              (setq buffer-undo-list extended)
              (let ((extended-is-visible (eq buffer-undo-list extended)))
                (setq buffer-undo-list original)
                (list extended-is-visible
                      (eq buffer-undo-list original)))))
        "#;
    let form = Reader::new(program)
        .read()
        .expect("read undo-list identity contract")
        .expect("undo-list identity form");
    let mut interp = Interpreter::new();
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate undo-list identity contract");
    assert_eq!(result, Value::list([Value::T, Value::T]));
}

#[test]
fn primitive_undo_consumes_marker_adjustments_with_their_deletion() {
    let program = r#"
          (with-temp-buffer
            (buffer-enable-undo)
            (insert "abc")
            (setq buffer-undo-list nil)
            (let ((marker (copy-marker 2)))
              (delete-region 1 4)
              (let ((pending buffer-undo-list)
                    (undo-in-progress t))
                (setq pending (primitive-undo 1 pending))
                (list (buffer-string) (marker-position marker) pending))))
        "#;
    let form = Reader::new(program)
        .read()
        .expect("read marker-adjustment undo contract")
        .expect("marker-adjustment undo form");
    let mut interp = Interpreter::new();
    crate::lisp::load_file_strict(
        &mut interp,
        &crate::compat::project_root().join("src/lisp/simple_compat.el"),
    )
    .expect("load the Emaxx preload compatibility layer");
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate marker-adjustment undo contract");
    assert_eq!(
        result,
        Value::list([Value::String("abc".into()), Value::Integer(2), Value::Nil])
    );
}

#[test]
fn substitute_in_file_name_expands_shell_style_env_vars() {
    let old = std::env::var("EMAXX_SUBST_TEST").ok();
    unsafe {
        std::env::set_var("EMAXX_SUBST_TEST", "value");
    }
    assert_eq!(
        substitute_in_file_name("$EMAXX_SUBST_TEST/${EMAXX_SUBST_TEST}/$$"),
        "value/value/$"
    );
    assert_eq!(substitute_in_file_name("/path///file"), "/file");
    assert_eq!(substitute_in_file_name("path//file"), "/file");
    if let Some(value) = old {
        unsafe {
            std::env::set_var("EMAXX_SUBST_TEST", value);
        }
    } else {
        unsafe {
            std::env::remove_var("EMAXX_SUBST_TEST");
        }
    }
}

#[test]
fn compat_paths_follow_emacs_test_directory_layout() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let repo_root = std::env::temp_dir().join(format!("emaxx-compat-paths-{unique}"));
    let test_dir = repo_root.join("test");
    let src_dir = repo_root.join("src");
    let lib_src_dir = repo_root.join("lib-src");
    std::fs::create_dir_all(&test_dir).expect("create test directory");
    std::fs::create_dir_all(&src_dir).expect("create src directory");
    std::fs::create_dir_all(&lib_src_dir).expect("create lib-src directory");
    std::fs::write(src_dir.join("emacs"), "").expect("write fake emacs binary");
    std::fs::write(lib_src_dir.join("emacsclient"), "").expect("write fake emacsclient binary");

    let test_directory = test_dir.display().to_string();
    assert_eq!(
        compat_emacsclient_path_from_test_directory(&test_directory),
        Some(lib_src_dir.join("emacsclient"))
    );
    let old = std::env::var("EMACS_TEST_DIRECTORY").ok();
    unsafe {
        std::env::set_var("EMACS_TEST_DIRECTORY", &test_directory);
    }
    assert_eq!(
        current_invocation_path(),
        std::env::current_exe().expect("current test executable"),
        "EMACS_TEST_DIRECTORY must never redirect Emaxx subprocesses to the GNU oracle"
    );
    assert_eq!(
        compat_installation_directory(),
        Some(path_to_directory_string(&repo_root))
    );
    if let Some(value) = old {
        unsafe {
            std::env::set_var("EMACS_TEST_DIRECTORY", value);
        }
    } else {
        unsafe {
            std::env::remove_var("EMACS_TEST_DIRECTORY");
        }
    }

    let _ = std::fs::remove_dir_all(&repo_root);
}

#[test]
fn insert_file_contents_reports_missing_input_as_file_missing() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let path = std::env::temp_dir().join(format!(
        "emaxx-missing-input-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let error = call(
        &mut interp,
        "insert-file-contents",
        &[Value::String(path.display().to_string().into())],
        &mut env,
    )
    .expect_err("a nonexistent input file must signal");

    assert_eq!(error.condition_type(), "file-missing");

    let path = path.display().to_string();
    let error = call(
        &mut interp,
        "insert-file-contents",
        &[Value::String(path.clone().into()), Value::T],
        &mut env,
    )
    .expect_err("visiting a nonexistent input file must still signal");

    assert_eq!(error.condition_type(), "file-missing");
    assert_eq!(interp.buffer.file.as_deref(), Some(path.as_str()));
    assert!(!interp.buffer.is_modified());
}

#[test]
fn system_move_file_to_trash_preserves_gnu_missing_file_contract() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let path = std::env::temp_dir().join(format!(
        "emaxx-missing-trash-input-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let path = path.display().to_string();
    let error = call(
        &mut interp,
        "system-move-file-to-trash",
        &[Value::String(path.clone().into())],
        &mut env,
    )
    .expect_err("moving a nonexistent file to trash must signal");
    let LispError::SignalValue(condition) = error else {
        panic!("expected a structured file-missing condition");
    };
    assert_eq!(
        condition,
        Value::list([
            Value::Symbol("file-missing".into()),
            Value::String("Removing old name".into()),
            Value::String("No such file or directory".into()),
            Value::String(path.into()),
        ])
    );
}

#[cfg(unix)]
#[test]
fn process_lines_uses_default_directory_as_subprocess_cwd() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let cwd = std::env::temp_dir().join(format!("emaxx-process-lines-{unique}"));
    std::fs::create_dir_all(&cwd).expect("create temp cwd");
    let expected = std::fs::canonicalize(&cwd)
        .expect("canonical temp cwd")
        .display()
        .to_string();

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable(
        "default-directory",
        Value::String(cwd.display().to_string().into()),
        &mut env,
    );

    let result = call(
        &mut interp,
        "process-lines",
        &[Value::String("/bin/pwd".into())],
        &mut env,
    )
    .expect("process-lines should succeed");

    assert_eq!(result, Value::list([Value::String(expected.into())]));

    let _ = std::fs::remove_dir_all(&cwd);
}

#[cfg(unix)]
#[test]
fn process_send_string_and_region_route_output_to_the_process_buffer() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*process-output*");
    let buffer = Value::buffer(buffer_id, buffer_name);

    let process = call(
        &mut interp,
        "start-process",
        &[
            Value::String("cat".into()),
            buffer.clone(),
            Value::String("/bin/cat".into()),
        ],
        &mut env,
    )
    .expect("start-process should succeed");

    assert_eq!(
        call(
            &mut interp,
            "processp",
            std::slice::from_ref(&process),
            &mut env
        )
        .expect("processp should succeed"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "get-buffer-process",
            std::slice::from_ref(&buffer),
            &mut env,
        )
        .expect("get-buffer-process should succeed"),
        process
    );

    call(
        &mut interp,
        "process-send-string",
        &[process.clone(), Value::String("secret\n".into())],
        &mut env,
    )
    .expect("process-send-string should succeed");
    call(
        &mut interp,
        "process-send-string",
        &[process.clone(), Value::String("second\n".into())],
        &mut env,
    )
    .expect("second process-send-string should succeed");
    interp.insert_current_buffer("prefix\nregion\nsuffix");
    call(
        &mut interp,
        "process-send-region",
        &[process.clone(), Value::Integer(8), Value::Integer(15)],
        &mut env,
    )
    .expect("process-send-region should succeed");

    // Process output is asynchronous.  Wait explicitly, as Lisp callers
    // must, before asserting on its buffer.  The long deadline does not slow
    // the normal case (accept returns on delivery), but avoids mistaking CPU
    // starvation in the parallel fast suite for a process semantic failure.
    let current_contents = interp
        .get_buffer_by_id(buffer_id)
        .expect("process buffer")
        .buffer_substring(
            1,
            interp
                .get_buffer_by_id(buffer_id)
                .expect("process buffer")
                .point_max(),
        )
        .expect("process output");
    if current_contents != "secret\nsecond\nregion\n" {
        call(
            &mut interp,
            "accept-process-output",
            &[process.clone(), Value::Integer(10)],
            &mut env,
        )
        .expect("accept-process-output should receive the echo");
    }

    let contents = interp
        .get_buffer_by_id(buffer_id)
        .expect("process buffer")
        .buffer_substring(
            1,
            interp
                .get_buffer_by_id(buffer_id)
                .expect("process buffer")
                .point_max(),
        )
        .expect("process output");
    assert_eq!(contents, "secret\nsecond\nregion\n");
    assert_eq!(
        call(
            &mut interp,
            "process-status",
            std::slice::from_ref(&process),
            &mut env,
        )
        .expect("process-status should succeed"),
        Value::Symbol("run".into())
    );
}

#[test]
fn process_list_is_newest_first_and_excludes_deleted_processes() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let first = call(
        &mut interp,
        "make-pipe-process",
        &[Value::Symbol(":name".into()), Value::String("first".into())],
        &mut env,
    )
    .expect("create first pipe process");
    let second = call(
        &mut interp,
        "make-pipe-process",
        &[
            Value::Symbol(":name".into()),
            Value::String("second".into()),
        ],
        &mut env,
    )
    .expect("create second pipe process");

    assert_eq!(
        call(&mut interp, "process-list", &[], &mut env).expect("list live processes"),
        Value::list([second.clone(), first.clone()])
    );
    call(
        &mut interp,
        "delete-process",
        std::slice::from_ref(&second),
        &mut env,
    )
    .expect("delete second process");
    assert_eq!(
        call(&mut interp, "process-list", &[], &mut env).expect("list remaining process"),
        Value::list([first])
    );
}

#[test]
fn process_sentinel_can_delete_its_own_process_exactly_once() {
    let program = r#"(let ((calls 0) process)
                       (setq
                        process
                        (make-pipe-process
                         :name "self-delete"
                         :sentinel
                         (lambda (process _event)
                           (setq calls (1+ calls))
                           (delete-process process))))
                       (delete-process process)
                       (list calls
                             (process-live-p process)
                             (process-status process)))"#;
    let expected = "(1 nil closed)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("self-deleting process sentinel contract should parse")
        .expect("self-deleting process sentinel contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("self-deleting process sentinel should terminate"),
        Reader::new(expected)
            .read()
            .expect("self-deleting sentinel result should parse")
            .expect("self-deleting sentinel result should exist")
    );
}

#[cfg(unix)]
fn process_connection_probe(connection_type: Value, name: &str) -> String {
    process_connection_probe_with_default(Some(connection_type), Value::T, name)
}

#[cfg(unix)]
fn process_connection_probe_with_default(
    connection_type: Option<Value>,
    default_connection_type: Value,
    name: &str,
) -> String {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("process-connection-type", default_connection_type, &mut env);
    let (buffer_id, buffer_name) = interp.create_buffer(&format!("*{name}*"));
    let mut process_args = vec![
        Value::Symbol(":name".into()),
        Value::String(name.into()),
        Value::Symbol(":buffer".into()),
        Value::buffer(buffer_id, buffer_name),
        Value::Symbol(":command".into()),
        Value::list([
            Value::String("/bin/sh".into()),
            Value::String("-c".into()),
            Value::String(
                "printf x; [ -t 0 ] && printf i; [ -t 1 ] && printf o; [ -t 2 ] && printf e >&2"
                    .into(),
            ),
        ]),
        Value::Symbol(":sentinel".into()),
        Value::symbol("ignore"),
    ];
    if let Some(connection_type) = connection_type {
        process_args.extend([Value::Symbol(":connection-type".into()), connection_type]);
    }
    let process = call(&mut interp, "make-process", &process_args, &mut env)
        .expect("create connection probe");
    let process_id = interp
        .resolve_process_id(&process)
        .expect("probe process id");
    while interp.process_is_live(process_id) {
        call(
            &mut interp,
            "accept-process-output",
            std::slice::from_ref(&process),
            &mut env,
        )
        .expect("wait for probe process activity");
    }
    pump_external_process_output(&mut interp, &mut env).expect("drain probe output");
    interp
        .get_buffer_by_id(buffer_id)
        .expect("probe buffer")
        .buffer_string()
}

#[cfg(unix)]
#[test]
fn make_process_honors_split_pipe_and_pty_connection_types() {
    assert_eq!(
        process_connection_probe(
            Value::cons(Value::Nil, Value::Symbol("pipe".into())),
            "input-pty"
        ),
        "xi"
    );
    assert_eq!(process_connection_probe(Value::Nil, "all-pty"), "xioe");
    assert_eq!(
        process_connection_probe(
            Value::cons(Value::Symbol("pipe".into()), Value::Nil),
            "output-pty"
        ),
        "xoe"
    );
    assert_eq!(
        process_connection_probe(Value::Symbol("pipe".into()), "all-pipe"),
        "x"
    );
}

#[cfg(unix)]
#[test]
fn make_process_nil_or_omitted_connection_type_uses_the_dynamic_default() {
    assert_eq!(
        process_connection_probe_with_default(None, Value::T, "omitted-default-pty"),
        "xioe"
    );
    assert_eq!(
        process_connection_probe_with_default(Some(Value::Nil), Value::Nil, "nil-default-pipe"),
        "x"
    );
}

#[cfg(unix)]
#[test]
fn process_send_eof_uses_the_pty_eof_character_and_drains_final_output() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*pty-eof*");
    let process = call(
        &mut interp,
        "make-process",
        &[
            Value::Symbol(":name".into()),
            Value::String("pty-eof".into()),
            Value::Symbol(":buffer".into()),
            Value::buffer(buffer_id, buffer_name),
            Value::Symbol(":command".into()),
            Value::list([Value::String("/bin/cat".into())]),
            Value::Symbol(":connection-type".into()),
            Value::Nil,
            Value::Symbol(":sentinel".into()),
            Value::symbol("ignore"),
        ],
        &mut env,
    )
    .expect("create PTY cat");
    call(
        &mut interp,
        "process-send-string",
        &[process.clone(), Value::String("hello\n".into())],
        &mut env,
    )
    .expect("send PTY input");
    call(
        &mut interp,
        "process-send-eof",
        std::slice::from_ref(&process),
        &mut env,
    )
    .expect("send PTY EOF");
    let process_id = interp.resolve_process_id(&process).expect("PTY process id");
    while interp.process_is_live(process_id) {
        call(
            &mut interp,
            "accept-process-output",
            &[process.clone(), Value::Float(0.1)],
            &mut env,
        )
        .expect("wait for PTY output");
    }
    pump_external_process_output(&mut interp, &mut env).expect("drain final PTY output");

    assert_eq!(
        interp
            .get_buffer_by_id(buffer_id)
            .expect("PTY process buffer")
            .buffer_string(),
        "hello\n"
    );
    assert!(!interp.process_is_live(process_id));
}

#[cfg(unix)]
#[test]
fn process_send_eof_keeps_a_split_input_pty_alive_until_the_child_reads_eof() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*split-pty-eof*");
    let process = call(
        &mut interp,
        "make-process",
        &[
            Value::Symbol(":name".into()),
            Value::String("split-pty-eof".into()),
            Value::Symbol(":buffer".into()),
            Value::buffer(buffer_id, buffer_name),
            Value::Symbol(":command".into()),
            Value::list([Value::String("/bin/cat".into())]),
            Value::Symbol(":connection-type".into()),
            Value::cons(Value::Nil, Value::Symbol("pipe".into())),
            Value::Symbol(":sentinel".into()),
            Value::symbol("ignore"),
        ],
        &mut env,
    )
    .expect("create cat with PTY input and pipe output");
    call(
        &mut interp,
        "process-send-string",
        &[process.clone(), Value::String("hello\n".into())],
        &mut env,
    )
    .expect("send split PTY input");
    call(
        &mut interp,
        "process-send-eof",
        std::slice::from_ref(&process),
        &mut env,
    )
    .expect("send split PTY EOF");
    let process_id = interp
        .resolve_process_id(&process)
        .expect("split PTY process id");
    while interp.process_is_live(process_id) {
        call(
            &mut interp,
            "accept-process-output",
            &[process.clone(), Value::Float(0.1)],
            &mut env,
        )
        .expect("wait for split PTY output");
    }
    pump_external_process_output(&mut interp, &mut env).expect("drain split PTY output");

    assert_eq!(
        interp
            .get_buffer_by_id(buffer_id)
            .expect("split PTY process buffer")
            .buffer_string(),
        "hello\n"
    );
}

#[cfg(unix)]
#[test]
fn signal_process_preserves_os_signal_status_and_sentinel_event() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("captured-signal-event", Value::Nil, &mut env);
    let process = call(
        &mut interp,
        "start-process",
        &[
            Value::String("signal-target".into()),
            Value::Nil,
            Value::String("/bin/sleep".into()),
            Value::String("30".into()),
        ],
        &mut env,
    )
    .expect("start signal target");
    let sentinel = Value::list([
        Value::Symbol("lambda".into()),
        Value::list([
            Value::Symbol("_process".into()),
            Value::Symbol("event".into()),
        ]),
        Value::list([
            Value::Symbol("setq".into()),
            Value::Symbol("captured-signal-event".into()),
            Value::Symbol("event".into()),
        ]),
    ]);
    call(
        &mut interp,
        "set-process-sentinel",
        &[process.clone(), sentinel],
        &mut env,
    )
    .expect("install signal sentinel");
    call(
        &mut interp,
        "signal-process",
        &[process.clone(), Value::Symbol("SIGPIPE".into())],
        &mut env,
    )
    .expect("signal child process");
    let process_id = interp
        .resolve_process_id(&process)
        .expect("target process id");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        pump_external_process_output(&mut interp, &mut env).expect("pump signal target");
        if !interp.process_is_live(process_id) {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(!interp.process_is_live(process_id));
    assert_eq!(
        call(
            &mut interp,
            "process-status",
            std::slice::from_ref(&process),
            &mut env,
        )
        .expect("signaled process status"),
        Value::Symbol("signal".into())
    );
    assert_eq!(
        call(
            &mut interp,
            "process-exit-status",
            std::slice::from_ref(&process),
            &mut env,
        )
        .expect("signaled process exit status"),
        Value::Integer(libc::SIGPIPE.into())
    );
    let event = interp
        .lookup_var("captured-signal-event", &env)
        .and_then(|value| string_like(&value).map(|string| string.text))
        .expect("captured signal sentinel event");
    assert!(
        event.starts_with("broken pipe"),
        "unexpected event: {event:?}"
    );
    assert!(event.ends_with('\n'), "unexpected event: {event:?}");
}

#[cfg(unix)]
#[test]
fn deleted_process_is_not_returned_for_buffer() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*deleted-process*");
    let buffer = Value::buffer(buffer_id, buffer_name);
    let process = call(
        &mut interp,
        "start-process",
        &[
            Value::String("cat".into()),
            buffer.clone(),
            Value::String("/bin/cat".into()),
        ],
        &mut env,
    )
    .expect("start-process should succeed");

    call(
        &mut interp,
        "delete-process",
        std::slice::from_ref(&process),
        &mut env,
    )
    .expect("delete-process should succeed");

    assert_eq!(
        call(
            &mut interp,
            "get-buffer-process",
            std::slice::from_ref(&buffer),
            &mut env,
        )
        .expect("get-buffer-process should succeed"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "process-buffer",
            std::slice::from_ref(&process),
            &mut env,
        )
        .expect("process-buffer should succeed"),
        buffer
    );
}

#[test]
fn string_limit_supports_end_flag() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "string-limit",
            &[Value::String("foobar".into()), Value::Integer(3)],
            &mut env,
        )
        .expect("string-limit should succeed"),
        Value::String("foo".into())
    );
    assert_eq!(
        call(
            &mut interp,
            "string-limit",
            &[Value::String("foobar".into()), Value::Integer(3), Value::T,],
            &mut env,
        )
        .expect("string-limit with end flag should succeed"),
        Value::String("bar".into())
    );
}

#[test]
fn run_at_time_callbacks_fire_on_accept_process_output() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let callback = Value::lambda(
        Vec::new().into(),
        vec![
            Value::list([
                Value::Symbol("setq".into()),
                Value::Symbol("timer-fired".into()),
                Value::T,
            ]),
            Value::T,
        ]
        .into(),
        shared_env(Vec::new()),
    );

    call(
        &mut interp,
        "run-at-time",
        &[Value::Integer(0), Value::Nil, callback],
        &mut env,
    )
    .expect("run-at-time should succeed");
    assert_eq!(interp.lookup_var("timer-fired", &env), None);

    call(&mut interp, "accept-process-output", &[], &mut env)
        .expect("accept-process-output should succeed");

    assert_eq!(interp.lookup_var("timer-fired", &env), Some(Value::T));
}

#[test]
fn run_with_timer_callbacks_fire_on_accept_process_output() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let callback = Value::lambda(
        Vec::new().into(),
        vec![
            Value::list([
                Value::Symbol("setq".into()),
                Value::Symbol("timer-fired".into()),
                Value::T,
            ]),
            Value::T,
        ]
        .into(),
        shared_env(Vec::new()),
    );

    call(
        &mut interp,
        "run-with-timer",
        &[Value::Integer(0), Value::Nil, callback],
        &mut env,
    )
    .expect("run-with-timer should succeed");
    assert_eq!(interp.lookup_var("timer-fired", &env), None);

    call(&mut interp, "accept-process-output", &[], &mut env)
        .expect("accept-process-output should succeed");

    assert_eq!(interp.lookup_var("timer-fired", &env), Some(Value::T));
}

#[test]
fn accept_process_output_honors_seconds_with_no_millis_argument() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let buffer = Value::buffer(interp.current_buffer_id(), String::new());
    let process = call(
        &mut interp,
        "start-process",
        &[
            Value::String("accept-output-test".into()),
            buffer,
            Value::String("sh".into()),
            Value::String("-c".into()),
            Value::String("printf ready".into()),
        ],
        &mut env,
    )
    .expect("start-process should launch a writer");

    // This is a deadline, not a sleep: the process normally returns at
    // once.  Keep it generous because the full parallel suite can leave a
    // newly spawned shell unscheduled for more than one second on a busy
    // host.  The exact seconds-only parsing contract is asserted below.
    assert_eq!(
        call(
            &mut interp,
            "accept-process-output",
            &[process, Value::Integer(10)],
            &mut env,
        )
        .expect("accept-process-output should wait for output"),
        Value::T
    );
    assert_eq!(interp.buffer.full_buffer_string(), "ready");
    assert_eq!(
        wait_duration(&[Value::Integer(10)]).expect("ten-second wait should be valid"),
        std::time::Duration::from_secs(10)
    );
    call(
        &mut interp,
        "accept-process-output",
        &[Value::Nil, Value::Integer(0), Value::Nil, Value::T],
        &mut env,
    )
    .expect("an explicit nil MILLISEC should mean zero milliseconds");
    assert_eq!(
        wait_duration(&[Value::Integer(10), Value::Nil])
            .expect("nil optional milliseconds should be zero"),
        std::time::Duration::from_secs(10)
    );
}

#[test]
fn accept_process_output_without_timeout_waits_for_requested_process() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let buffer = Value::buffer(interp.current_buffer_id(), String::new());
    let process = call(
        &mut interp,
        "start-process",
        &[
            Value::String("accept-output-no-timeout".into()),
            buffer,
            Value::String("sh".into()),
            Value::String("-c".into()),
            Value::String("printf ready".into()),
        ],
        &mut env,
    )
    .expect("start-process should launch a writer");

    assert_eq!(
        call(
            &mut interp,
            "accept-process-output",
            std::slice::from_ref(&process),
            &mut env,
        )
        .expect("accept-process-output should wait without an explicit deadline"),
        Value::T
    );
    assert_eq!(interp.buffer.full_buffer_string(), "ready");
}

#[test]
fn accept_process_output_ignores_distractor_output_until_target_delivers() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let target_buffer = call(
        &mut interp,
        "generate-new-buffer",
        &[Value::String(" *accept-target*".into())],
        &mut env,
    )
    .expect("create target buffer");
    let target_buffer_id = interp
        .resolve_buffer_id(&target_buffer)
        .expect("resolve target buffer");
    let distractor_buffer = call(
        &mut interp,
        "generate-new-buffer",
        &[Value::String(" *accept-distractor*".into())],
        &mut env,
    )
    .expect("create distractor buffer");
    let target = call(
        &mut interp,
        "start-process",
        &[
            Value::String("accept-target".into()),
            target_buffer,
            Value::String("sh".into()),
            Value::String("-c".into()),
            Value::String("sleep 0.15; printf target".into()),
        ],
        &mut env,
    )
    .expect("start delayed target");
    call(
        &mut interp,
        "start-process",
        &[
            Value::String("accept-distractor".into()),
            distractor_buffer,
            Value::String("sh".into()),
            Value::String("-c".into()),
            Value::String("printf distractor".into()),
        ],
        &mut env,
    )
    .expect("start immediate distractor");

    assert_eq!(
        call(
            &mut interp,
            "accept-process-output",
            // Wait for the requested process event itself.  A wall-clock
            // deadline turns host scheduler contention into a false failure,
            // while returning early for the distractor is still caught by
            // the target-buffer assertion below.
            &[target],
            &mut env,
        )
        .expect("wait for requested process"),
        Value::T
    );
    assert_eq!(
        interp
            .get_buffer_by_id(target_buffer_id)
            .expect("live target buffer")
            .full_buffer_string(),
        "target"
    );
}

#[test]
fn make_network_process_ipv4_family_prefers_an_ipv4_listener() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let process = call(
        &mut interp,
        "make-network-process",
        &[
            Value::Symbol(":name".into()),
            Value::String("ipv4-listener".into()),
            Value::Symbol(":server".into()),
            Value::T,
            Value::Symbol(":family".into()),
            Value::Symbol("ipv4".into()),
            Value::Symbol(":host".into()),
            Value::String("localhost".into()),
            Value::Symbol(":service".into()),
            Value::T,
        ],
        &mut env,
    )
    .expect("IPv4 listener should bind");

    let local = call(
        &mut interp,
        "process-contact",
        &[process.clone(), Value::Symbol(":local".into())],
        &mut env,
    )
    .expect("process-contact should expose the listener address");
    assert_eq!(
        call(
            &mut interp,
            "length",
            std::slice::from_ref(&local),
            &mut env,
        )
        .expect("address vector length"),
        Value::Integer(5)
    );

    call(
        &mut interp,
        "delete-process",
        std::slice::from_ref(&process),
        &mut env,
    )
    .expect("listener cleanup should succeed");
}

#[test]
fn make_network_process_ipv6_family_uses_an_ipv6_listener() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let process = call(
        &mut interp,
        "make-network-process",
        &[
            Value::Symbol(":name".into()),
            Value::String("ipv6-listener".into()),
            Value::Symbol(":server".into()),
            Value::T,
            Value::Symbol(":family".into()),
            Value::Symbol("ipv6".into()),
            Value::Symbol(":host".into()),
            Value::Symbol("local".into()),
            Value::Symbol(":service".into()),
            Value::T,
        ],
        &mut env,
    )
    .expect("IPv6 loopback listener should bind");

    let local = call(
        &mut interp,
        "process-contact",
        &[process.clone(), Value::Symbol(":local".into())],
        &mut env,
    )
    .expect("process-contact should expose the IPv6 listener address");
    assert_eq!(
        call(
            &mut interp,
            "length",
            std::slice::from_ref(&local),
            &mut env,
        )
        .expect("IPv6 address vector length"),
        Value::Integer(9)
    );
    assert!(
        call(&mut interp, "aref", &[local, Value::Integer(8)], &mut env,)
            .expect("IPv6 listener port")
            .as_integer()
            .is_ok_and(|port| port > 0)
    );

    call(
        &mut interp,
        "delete-process",
        std::slice::from_ref(&process),
        &mut env,
    )
    .expect("IPv6 listener cleanup should succeed");
}

#[test]
fn make_network_process_coding_precedence_matches_gnu() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        r#"
        (let ((coding-system-for-read 'binary)
              (coding-system-for-write 'utf-8-unix))
          (let ((implicit (make-network-process
                           :name "coding-implicit" :server t :noquery t
                           :family 'ipv4 :service t :host 'local))
                (nil-coding (make-network-process
                             :name "coding-nil" :server t :noquery t
                             :family 'ipv4 :service t :coding nil :host 'local))
                (explicit (make-network-process
                           :name "coding-explicit" :server t :noquery t
                           :family 'ipv4 :service t
                           :coding 'georgian-academy :host 'local)))
            (unwind-protect
                (list (process-coding-system implicit)
                      (process-coding-system nil-coding)
                      (process-coding-system explicit))
              (delete-process implicit)
              (delete-process nil-coding)
              (delete-process explicit))))"#,
    )
    .read()
    .expect("network coding precedence probe should parse")
    .expect("network coding precedence probe should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("network coding precedence probe should evaluate");
    let expected = Reader::new(
        "((binary . utf-8-unix) (binary . utf-8-unix) \
         (georgian-academy . georgian-academy))",
    )
    .read()
    .expect("network coding precedence expectation should parse")
    .expect("network coding precedence expectation should contain a form");
    assert!(
        values_equal(&interp, &actual, &expected),
        "network coding precedence differed from GNU:\nactual: {actual:?}"
    );
}

#[test]
fn localhost_family_fallback_opens_without_polluting_the_process_buffer() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let server = call(
        &mut interp,
        "make-network-process",
        &[
            Value::Symbol(":name".into()),
            Value::String("clean-open-server".into()),
            Value::Symbol(":server".into()),
            Value::T,
            Value::Symbol(":family".into()),
            Value::Symbol("ipv4".into()),
            Value::Symbol(":host".into()),
            Value::Symbol("local".into()),
            Value::Symbol(":service".into()),
            Value::T,
        ],
        &mut env,
    )
    .expect("server should bind");
    let port = call(
        &mut interp,
        "process-contact",
        &[server.clone(), Value::Symbol(":service".into())],
        &mut env,
    )
    .expect("server should expose its port");
    let buffer = call(
        &mut interp,
        "generate-new-buffer",
        &[Value::String(" *clean-network-open*".into())],
        &mut env,
    )
    .expect("client buffer should be created");
    let buffer_id = interp
        .resolve_buffer_id(&buffer)
        .expect("client buffer should resolve");
    let client = call(
        &mut interp,
        "make-network-process",
        &[
            Value::Symbol(":name".into()),
            Value::String("clean-open-client".into()),
            Value::Symbol(":buffer".into()),
            buffer,
            Value::Symbol(":host".into()),
            Value::String("localhost".into()),
            Value::Symbol(":service".into()),
            port,
        ],
        &mut env,
    )
    .expect("client should connect");

    assert_eq!(
        interp
            .get_buffer_by_id(buffer_id)
            .expect("client buffer should remain live")
            .full_buffer_string(),
        ""
    );
    for process in [client, server] {
        call(&mut interp, "delete-process", &[process], &mut env)
            .expect("network process cleanup should succeed");
    }
}

#[test]
fn make_network_process_nowait_opens_on_the_next_event_pump() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let server = call(
        &mut interp,
        "make-network-process",
        &[
            Value::Symbol(":name".into()),
            Value::String("nowait-server".into()),
            Value::Symbol(":server".into()),
            Value::T,
            Value::Symbol(":family".into()),
            Value::Symbol("ipv4".into()),
            Value::Symbol(":host".into()),
            Value::String("127.0.0.1".into()),
            Value::Symbol(":service".into()),
            Value::T,
        ],
        &mut env,
    )
    .expect("server should bind");
    let port = call(
        &mut interp,
        "process-contact",
        &[server.clone(), Value::Symbol(":service".into())],
        &mut env,
    )
    .expect("server should expose its port");
    let sentinel = Value::lambda(
        vec!["process".into(), "event".into()].into(),
        vec![Value::list([
            Value::Symbol("setq".into()),
            Value::Symbol("nowait-event".into()),
            Value::Symbol("event".into()),
        ])]
        .into(),
        shared_env(Vec::new()),
    );
    let client = call(
        &mut interp,
        "make-network-process",
        &[
            Value::Symbol(":name".into()),
            Value::String("nowait-client".into()),
            Value::Symbol(":family".into()),
            Value::Symbol("ipv4".into()),
            Value::Symbol(":host".into()),
            Value::String("127.0.0.1".into()),
            Value::Symbol(":service".into()),
            port,
            Value::Symbol(":nowait".into()),
            Value::T,
            Value::Symbol(":sentinel".into()),
            sentinel,
        ],
        &mut env,
    )
    .expect("client should begin connecting");

    assert_eq!(
        call(
            &mut interp,
            "process-status",
            std::slice::from_ref(&client),
            &mut env,
        )
        .expect("initial process status"),
        Value::Symbol("connect".into())
    );
    assert_eq!(interp.lookup_var("nowait-event", &env), None);

    call(
        &mut interp,
        "accept-process-output",
        &[Value::Nil, Value::Float(0.05)],
        &mut env,
    )
    .expect("event pump should report the connection");
    assert_eq!(
        call(
            &mut interp,
            "process-status",
            std::slice::from_ref(&client),
            &mut env,
        )
        .expect("opened process status"),
        Value::Symbol("open".into())
    );
    assert_eq!(
        interp.lookup_var("nowait-event", &env),
        Some(Value::String("open\n".into()))
    );
}

#[test]
fn process_command_reports_child_argv_and_nil_for_connection_records() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let child = interp
        .create_process(
            None,
            Some("sh".into()),
            vec!["-c".into(), "printf child".into()],
            None,
            Some("child".into()),
        )
        .expect("create child process record");
    let connection = interp
        .create_process(None, None, Vec::new(), None, Some("pipe".into()))
        .expect("create connection process record");

    assert_eq!(
        call(
            &mut interp,
            "process-command",
            std::slice::from_ref(&child),
            &mut env,
        )
        .expect("read child command"),
        Value::list([
            Value::String("sh".into()),
            Value::String("-c".into()),
            Value::String("printf child".into()),
        ])
    );
    assert_eq!(
        call(&mut interp, "process-command", &[connection], &mut env,)
            .expect("read connection command"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "process-tty-name",
            &[child.clone(), Value::Symbol("stdin".into())],
            &mut env,
        )
        .expect("pipe-backed child has no tty"),
        Value::Nil
    );
    assert_eq!(
        call(&mut interp, "process-coding-system", &[child], &mut env,)
            .expect("read child coding systems"),
        Value::cons(
            Value::Symbol("utf-8-unix".into()),
            Value::Symbol("utf-8-unix".into()),
        )
    );
}

#[test]
fn indent_rigidly_shifts_each_line_in_region() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "a\nb\n");
    interp.buffer.goto_char(interp.buffer.point_max());
    let mut env = Vec::new();

    call(
        &mut interp,
        "indent-rigidly",
        &[Value::Integer(1), Value::Integer(5), Value::Integer(2)],
        &mut env,
    )
    .expect("indent-rigidly should succeed");

    assert_eq!(
        interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
            .expect("buffer contents"),
        "  a\n  b\n"
    );
    assert_eq!(interp.buffer.point(), 9);
}

#[test]
fn inhibit_read_only_allows_buffer_read_only_edits() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "abc");
    let mut env = Vec::new();
    interp.set_variable("buffer-read-only", Value::T, &mut env);
    interp.set_variable("inhibit-read-only", Value::T, &mut env);
    interp.buffer.goto_char(1);

    call(&mut interp, "delete-char", &[Value::Integer(1)], &mut env)
        .expect("delete-char should ignore buffer-read-only when inhibited");

    assert_eq!(
        interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
            .expect("buffer contents"),
        "bc"
    );
}

#[test]
fn insert_signals_buffer_read_only_unless_inhibited() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "");
    let mut env = Vec::new();
    interp.set_variable("buffer-read-only", Value::T, &mut env);

    assert!(matches!(
        call(&mut interp, "insert", &[Value::String("x".into())], &mut env),
        Err(LispError::SignalValue(value))
            if matches!(value.to_vec().ok().as_deref(), Some([
                Value::Symbol(name),
                Value::Buffer(_),
            ]) if name == "buffer-read-only")
    ));

    interp.set_variable("inhibit-read-only", Value::T, &mut env);
    call(
        &mut interp,
        "insert",
        &[Value::String("x".into())],
        &mut env,
    )
    .expect("inhibit-read-only should allow insertion");
    assert_eq!(
        interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
            .expect("buffer contents"),
        "x"
    );
}

#[test]
fn failed_search_with_move_noerror_moves_to_bound() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "abc def");
    let mut env = Vec::new();
    interp.buffer.goto_char(1);

    assert_eq!(
        call(
            &mut interp,
            "search-forward",
            &[
                Value::String("z".into()),
                Value::Integer(5),
                Value::Symbol("move".into()),
            ],
            &mut env,
        )
        .expect("search-forward should return nil when noerror is move"),
        Value::Nil
    );
    assert_eq!(interp.buffer.point(), 5);

    interp.buffer.goto_char(1);
    assert_eq!(
        call(
            &mut interp,
            "re-search-forward",
            &[
                Value::String("z+".into()),
                Value::Integer(6),
                Value::Symbol("move".into()),
            ],
            &mut env,
        )
        .expect("re-search-forward should return nil when noerror is move"),
        Value::Nil
    );
    assert_eq!(interp.buffer.point(), 6);
}

#[test]
fn delete_line_removes_the_current_line() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "one\ntwo\nthree\n");
    let mut env = Vec::new();
    interp.buffer.goto_char(6);

    call(&mut interp, "delete-line", &[], &mut env).expect("delete-line should succeed");

    assert_eq!(
        interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
            .expect("buffer contents"),
        "one\nthree\n"
    );
}

#[test]
fn make_button_ignores_incomplete_ranges() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "button");
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "make-button",
            &[
                Value::Nil,
                Value::Integer(3),
                Value::Symbol("type".into()),
                Value::Symbol("sample".into()),
            ],
            &mut env,
        )
        .expect("make-button should ignore nil positions"),
        Value::Nil
    );
}

#[test]
fn looking_at_p_preserves_existing_match_data() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "abc");
    let mut env = Vec::new();
    call(
        &mut interp,
        "re-search-forward",
        &[Value::String("a".into())],
        &mut env,
    )
    .expect("re-search-forward should set match data");
    let saved = interp.last_match_data.clone();
    interp.buffer.goto_char(1);

    let result = call(
        &mut interp,
        "looking-at-p",
        &[Value::String("z".into())],
        &mut env,
    )
    .expect("looking-at-p should return nil for a failed match");

    assert_eq!(result, Value::Nil);
    assert_eq!(interp.last_match_data, saved);
}

#[test]
fn native_posix_buffer_search_and_search_state_helpers_match_gnu_contracts() {
    let program = r#"
        (with-temp-buffer
          (insert "aa")
          (goto-char 1)
          (let ((here (current-buffer))
                ordinary-inhibited inhibited longest forward backward translated metadata)
            (set-match-data '(9 10))
            (setq ordinary-inhibited
                  (and (looking-at "a" t)
                       (equal (match-data t) '(9 10))))
            (set-match-data '(9 10))
            (setq inhibited
                  (and (posix-looking-at "\\(a\\|aa\\)" t)
                       (equal (match-data t) '(9 10))))
            (posix-looking-at "\\(a\\|aa\\)")
            (setq longest
                  (and (equal (butlast (match-data t)) '(1 3 1 3))
                       (eq (car (last (match-data t))) here)))
            (goto-char 1)
            (setq forward
                  (and (= (posix-search-forward "\\(a\\|aa\\)") 3)
                       (equal (butlast (match-data t)) '(1 3 1 3))))
            (goto-char 3)
            (setq backward
                  (and (= (posix-search-backward "\\(a\\|aa\\)") 2)
                       (equal (butlast (match-data t)) '(2 3 2 3))))
            (set-match-data '(2 7 nil nil 4 5))
            (setq translated
                  (and (null (match-data--translate -3))
                       (equal (match-data t) '(0 4 nil nil 1 2))))
            (setq metadata
                  (list (subrp (symbol-function 'posix-looking-at))
                        (subrp (symbol-function 'posix-search-forward))
                        (subrp (symbol-function 'posix-search-backward))
                        (subrp (symbol-function 'match-data--translate))
                        (subrp (symbol-function 'newline-cache-check))
                        (equal (help-function-arglist 'posix-looking-at)
                               '(arg1 &optional arg2))
                        (commandp 'posix-search-forward)
                        (commandp 'posix-search-backward)))
            (list ordinary-inhibited inhibited longest forward backward translated
                  (null (newline-cache-check))
                  (null (newline-cache-check here))
                  metadata)))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "(t t t t t t t t (t t t t t t t t))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("POSIX search contract should parse")
        .expect("POSIX search contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("POSIX search native family should match GNU"),
        Value::list(
            std::iter::repeat_n(Value::T, 8).chain(std::iter::once(Value::list(vec![Value::T; 8]))),
        )
    );
}

#[test]
fn match_data_preserves_gnu_source_reuse_reseat_and_elision_contracts() {
    let program = r#"
        (with-temp-buffer
          (insert "a")
          (goto-char 1)
          (looking-at "\\(a\\)\\(z\\)?")
          (let* ((here (current-buffer))
                 (integers (match-data t))
                 (reuse (list nil nil nil nil nil 'spare))
                 (marker-data (match-data))
                 (old-marker (car marker-data))
                 restored-source reuse-result reseated)
            (setq reuse-result
                  (and (eq (match-data t reuse) reuse)
                       (equal (list (nth 0 reuse) (nth 1 reuse)
                                    (nth 2 reuse) (nth 3 reuse))
                              '(1 2 1 2))
                       (eq (nth 4 reuse) here)
                       (null (nth 5 reuse))))
            (setq reseated
                  (and (eq (match-data nil marker-data t) marker-data)
                       (null (marker-buffer old-marker))))
            (set-match-data integers)
            (setq restored-source
                  (and (equal (butlast (match-data t)) '(1 2 1 2))
                       (eq (car (last (match-data t))) here)))
            (list (equal (butlast integers) '(1 2 1 2))
                  (eq (car (last integers)) here)
                  reuse-result reseated restored-source)))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(t t t t t)");

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("match-data state contract should parse")
        .expect("match-data state contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("match-data should retain GNU state semantics"),
        Value::list(vec![Value::T; 5])
    );
}

#[test]
fn native_sqlite_columns_uses_the_live_result_set_schema() {
    let program = r#"
        (let* ((db (sqlite-open))
               (set (sqlite-select
                     db "select 1 as alpha, 2 as beta" nil 'set))
               result)
          (unwind-protect
              (let ((before (sqlite-columns set))
                    (row (sqlite-next set))
                    (after (sqlite-columns set))
                    (finalized (sqlite-finalize set)))
                (setq result
                      (list (equal before '("alpha" "beta"))
                            (equal row '(1 2))
                            (equal after '("alpha" "beta"))
                            finalized
                            (condition-case nil
                                (progn (sqlite-columns set) nil)
                              (error t))
                            (subrp (symbol-function 'sqlite-columns))
                            (equal (help-function-arglist 'sqlite-columns)
                                   '(arg1)))))
            (sqlite-close db))
          result)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(t t t t t t t)");

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("sqlite-columns contract should parse")
        .expect("sqlite-columns contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("sqlite-columns should expose the result-set schema"),
        Value::list(vec![Value::T; 7])
    );
}

#[test]
fn native_file_lock_primitives_share_the_buffer_lock_state_machine() {
    let program = r#"
        (let ((path (make-temp-file "emaxx-lock-primitive-")))
          (unwind-protect
              (let ((locked (lock-file path))
                    (owner (file-locked-p path))
                    (unlocked (unlock-file path))
                    (after (file-locked-p path))
                    disabled)
                (let ((create-lockfiles nil))
                  (lock-file path)
                  (setq disabled (file-locked-p path)))
                (list locked owner unlocked after disabled
                      (subrp (symbol-function 'lock-file))
                      (subrp (symbol-function 'unlock-file))
                      (help-function-arglist 'lock-file)))
            (ignore-errors (unlock-file path))
            (delete-file path)))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "(nil t nil nil nil t t (arg1))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("file lock primitive contract should parse")
        .expect("file lock primitive contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("file lock primitives should use the shared lock implementation"),
        Value::list([
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
            Value::list([Value::Symbol("arg1".into())]),
        ])
    );
}

#[test]
fn native_message_dialog_fallbacks_share_the_headless_message_contract() {
    let program = r#"
        (list (message-or-box "value=%d" 7)
              (message-box "value=%d" 8)
              (message-or-box nil)
              (message-box nil)
              (subrp (symbol-function 'message-or-box))
              (subrp (symbol-function 'message-box))
              (help-function-arglist 'message-or-box))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "(\"value=7\" \"value=8\" nil nil t t (arg1 &rest rest))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("message dialog fallback contract should parse")
        .expect("message dialog fallback contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("headless dialog messages should use the message path"),
        Value::list([
            Value::String("value=7".into()),
            Value::String("value=8".into()),
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
            Value::list([
                Value::Symbol("arg1".into()),
                Value::Symbol("&rest".into()),
                Value::Symbol("rest".into()),
            ]),
        ])
    );
}

#[test]
fn native_mutex_name_reads_the_shared_mutex_state() {
    let program = r#"
        (let ((named (make-mutex "gate"))
              (unnamed (make-mutex)))
          (list (mutex-name named)
                (mutex-name unnamed)
                (subrp (symbol-function 'mutex-name))
                (help-function-arglist 'mutex-name)))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(\"gate\" nil t (arg1))");

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("mutex-name contract should parse")
        .expect("mutex-name contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("mutex-name should expose the stored mutex name"),
        Value::list([
            Value::String("gate".into()),
            Value::Nil,
            Value::T,
            Value::list([Value::Symbol("arg1".into())]),
        ])
    );
}

#[test]
fn native_menu_activity_predicate_is_false_without_a_graphical_menu() {
    let program = r#"
        (list (menu-or-popup-active-p)
              (subrp (symbol-function 'menu-or-popup-active-p))
              (help-function-arglist 'menu-or-popup-active-p))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(nil t nil)");

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("menu activity contract should parse")
        .expect("menu activity contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("headless menu activity should be false"),
        Value::list([Value::Nil, Value::T, Value::Nil])
    );
}

#[test]
fn native_imagep_validates_the_shared_image_specification_shape() {
    let program = r#"
        (list
         (mapcar
          #'imagep
          '(nil
            (image)
            (image :type xpm)
            (image :type xpm :data "")
            (image :type png :file "not-loaded-by-imagep")
            (image :type nope :data "")
            (image :type xpm :data "" :type png)
            (image :type png :data "" :file "both")
            (image :type xpm :data "" extra)))
         (subrp (symbol-function 'imagep))
         (help-function-arglist 'imagep))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "((nil nil nil t t nil nil nil nil) t (arg1))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("imagep contract should parse")
        .expect("imagep contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("imagep should validate image property lists"),
        Value::list([
            Value::list([
                Value::Nil,
                Value::Nil,
                Value::Nil,
                Value::T,
                Value::T,
                Value::Nil,
                Value::Nil,
                Value::Nil,
                Value::Nil,
            ]),
            Value::T,
            Value::list([Value::Symbol("arg1".into())]),
        ])
    );
}

#[test]
fn native_image_cache_family_matches_the_headless_frame_contract() {
    let program = r#"
        (let ((valid (list 'image :type (car image-types) :data "x")))
          (list
           (condition-case err (clear-image-cache) (error (car err)))
           (clear-image-cache t)
           (clear-image-cache "x")
           (clear-image-cache nil (cons 'image valid))
           (condition-case err
               (clear-image-cache nil 7)
             (error (car err)))
           (image-cache-size)
           (image-transforms-p)
           (condition-case err (image-flush valid) (error (car err)))
           (image-flush valid t)
           (condition-case err (image-flush 'nope) (error (car err)))
           (subrp (symbol-function 'clear-image-cache))
           (help-function-arglist 'clear-image-cache)
           (subrp (symbol-function 'image-cache-size))
           (help-function-arglist 'image-cache-size)
           (subrp (symbol-function 'image-flush))
           (help-function-arglist 'image-flush)
           (subrp (symbol-function 'image-transforms-p))
           (help-function-arglist 'image-transforms-p)))"#;
    let expected = "(error nil nil nil wrong-type-argument 0 nil error nil error t (&optional arg1 arg2) t nil t (arg1 &optional arg2) t (&optional arg1))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("image cache contract should parse")
        .expect("image cache contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("image cache operations should respect the headless frame"),
        Reader::new(expected)
            .read()
            .expect("expected image cache result should parse")
            .expect("expected image cache result should exist")
    );
}

#[test]
fn native_image_variables_match_the_gnu_image_c_contract() {
    let program = r#"
        (progn
          (defun emaxx-test-image-scaling-factor () image-scaling-factor)
          (list
           (boundp 'image-types)
           max-image-size
           cross-disabled-images
           x-bitmap-file-path
           image-cache-eviction-delay
           image-scaling-factor
           (mapcar #'special-variable-p
                   '(image-types max-image-size cross-disabled-images
                     x-bitmap-file-path image-cache-eviction-delay
                     image-scaling-factor))
           (let ((image-scaling-factor 2.0))
             (emaxx-test-image-scaling-factor))))"#;
    let expected = "(t 10.0 nil (\".\") 300 auto (t t t t t t) 2.0)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("image variable contract should parse")
        .expect("image variable contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("native image variables should match image.c"),
        Reader::new(expected)
            .read()
            .expect("expected image variable result should parse")
            .expect("expected image variable result should exist")
    );
}

#[test]
fn native_fringe_bitmap_registry_family_matches_gnu() {
    let program = r#"
        (let ((name 'emaxx-test-fringe))
          (unwind-protect
              (list
               (list
                (length fringe-bitmaps)
                (get 'question-mark 'fringe)
                (get 'empty-line 'fringe))
               (let* ((before fringe-bitmaps)
                      (defined
                       (define-fringe-bitmap
                        name [1 2 511 -1])))
                 (list
                  (eq defined name)
                  (integerp (get name 'fringe))
                  (and (memq name fringe-bitmaps) t)
                  (= (length fringe-bitmaps)
                     (1+ (length before)))
                  (set-fringe-bitmap-face name 'warning)
                  (eq
                   (define-fringe-bitmap
                    name "\1\2" 7 16 '(bottom t))
                   name)
                  (= (length fringe-bitmaps)
                     (1+ (length before)))
                  (destroy-fringe-bitmap name)
                  (get name 'fringe)
                  (and (memq name fringe-bitmaps) t)))
               (mapcar
                (lambda (thunk)
                  (condition-case error-data
                      (funcall thunk)
                    (error
                     (list
                      (car error-data)
                      (cadr error-data)))))
                (list
                 (lambda ()
                   (define-fringe-bitmap 3 []))
                 (lambda ()
                   (define-fringe-bitmap name 3))
                 (lambda ()
                   (define-fringe-bitmap name [] nil 0))
                 (lambda ()
                   (define-fringe-bitmap name [] nil 17))
                 (lambda ()
                   (define-fringe-bitmap
                    name [] nil 8 'middle))
                 (lambda ()
                   (set-fringe-bitmap-face name nil))
                 (lambda ()
                   (destroy-fringe-bitmap 3))))
               (with-temp-buffer
                 (insert "abc")
                 (set-window-buffer
                  (selected-window) (current-buffer))
                 (list
                  (fringe-bitmaps-at-pos)
                  (fringe-bitmaps-at-pos 1)
                  (condition-case error-data
                      (fringe-bitmaps-at-pos 99)
                    (error (car error-data)))
                  (condition-case error-data
                      (fringe-bitmaps-at-pos nil 3)
                    (error (car error-data))))))
            (destroy-fringe-bitmap name)))"#;
    let expected = r#"((24 1 24)
                       (t t t t nil t t nil nil nil)
                       ((wrong-type-argument symbolp)
                        (wrong-type-argument arrayp)
                        (args-out-of-range 0)
                        (args-out-of-range 17)
                        (error "Bad align argument")
                        (error "Undefined fringe bitmap")
                        (wrong-type-argument symbolp))
                       (nil nil args-out-of-range
                        wrong-type-argument))"#;
    let expected_printed = "((24 1 24) (t t t t nil t t nil nil nil) ((wrong-type-argument symbolp) (wrong-type-argument arrayp) (args-out-of-range 0) (args-out-of-range 17) (error \"Bad align argument\") (error \"Undefined fringe bitmap\") (wrong-type-argument symbolp)) (nil nil args-out-of-range wrong-type-argument))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("fringe.c family contract should parse")
        .expect("fringe.c family contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("fringe.c family contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("fringe.c expected value should parse")
        .expect("fringe.c expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "fringe.c result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_composite_c_family_and_text_property_identity_match_gnu() {
    let program = r#"
        (list
         (clear-composition-cache)
         (composition-sort-rules
          (list [a 2 c] [d 5 f] [g 2 i]))
         (let ((string (copy-sequence "abcd"))
               before)
           (compose-string-internal
            string 1 3 nil 'ignore)
           (setq before
                 (copy-tree
                  (get-text-property
                   1 'composition string)))
           (list
            before
            (find-composition-internal
             1 nil string nil)
            (find-composition-internal
             1 nil string t)
            (get-text-property
             1 'composition string)))
         (let ((string (copy-sequence "ab"))
               (left (list 1))
               (right (list 1)))
           (put-text-property 0 1 'x left string)
           (put-text-property 1 2 'x right string)
           (list
            (next-single-property-change
             0 'x string)
            (eq
             (get-text-property 0 'x string)
             (get-text-property 1 'x string))
            (equal
             (get-text-property 0 'x string)
             (get-text-property 1 'x string))))
         (with-temp-buffer
           (insert "abcd")
           (compose-region-internal
            4 2 ?X 'ignore)
           (list
            (get-text-property 2 'composition)
            (find-composition-internal
             2 nil nil t)
            (find-composition-internal
             1 4 nil nil)
            (find-composition-internal
             5 1 nil nil)))
         (list
          (let ((string (copy-sequence "abc")))
            (compose-string-internal
             string 0 3 [65 12 66] 'ignore)
            (find-composition-internal
             0 nil string t))
          (let ((string (copy-sequence "abc")))
            (compose-string-internal
             string 0 3 "XY" 'ignore)
            (find-composition-internal
             0 nil string t)))
         (composition-get-gstring 0 2 nil "é")
         (find-composition-internal 0 -9 "abc" nil)
         (mapcar
          (lambda (thunk)
            (condition-case error-data
                (funcall thunk)
              (error
               (list
                (car error-data)
                (if (bufferp (cadr error-data))
                    'buffer
                  (cadr error-data))))))
          (list
           (lambda ()
             (compose-region-internal 0 1))
           (lambda ()
             (compose-region-internal 1 1 1.5))
           (lambda ()
             (composition-get-gstring 0 0 nil ""))
           (lambda ()
             (composition-get-gstring 0 1 3 "a"))
           (lambda ()
             (find-composition-internal 9 nil "a" nil))
           (lambda ()
             (composition-sort-rules
              (list [a -1 c] [b 1 d]))))))"#;
    let expected = r#"
        (nil
         ([d 5 f] [a 2 c] [g 2 i])
         (((2) . ignore)
          (1 3 t)
          (1 3 [98 99] t ignore 1)
          (0 2 [98 99] . ignore))
         (1 nil t)
         ((1 2 [88] . ignore)
          (2 4 [88] t ignore 1)
          (2 4 t)
          (2 4 t))
         ((0 3 [65 12 66] nil ignore 2)
          (0 3 [88 89] t ignore 1))
         [[utf-8-unix 101 769]
          nil
          [0 0 101 101 1 0 1 1 0 nil]
          [1 1 769 769 0 0 0 1 0 nil]
          nil nil nil nil nil nil]
         nil
         ((args-out-of-range buffer)
          (wrong-type-argument vectorp)
          (error "Attempt to shape zero-length text")
          (wrong-type-argument terminal-live-p)
          (args-out-of-range "a")
          (error
           "Invalid composition rule in RULES argument")))"#;
    let expected_printed = "(nil ([d 5 f] [a 2 c] [g 2 i]) (((2) . ignore) (1 3 t) (1 3 [98 99] t ignore 1) (0 2 [98 99] . ignore)) (1 nil t) ((1 2 [88] . ignore) (2 4 [88] t ignore 1) (2 4 t) (2 4 t)) ((0 3 [65 12 66] nil ignore 2) (0 3 [88 89] t ignore 1)) [[utf-8-unix 101 769] nil [0 0 101 101 1 0 1 1 0 nil] [1 1 769 769 0 0 0 1 0 nil] nil nil nil nil nil nil] nil ((args-out-of-range buffer) (wrong-type-argument vectorp) (error \"Attempt to shape zero-length text\") (wrong-type-argument terminal-live-p) (args-out-of-range \"a\") (error \"Invalid composition rule in RULES argument\")))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("composite.c family contract should parse")
        .expect("composite.c family contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("composite.c family contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("composite.c expected value should parse")
        .expect("composite.c expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "composite.c result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_doc_c_family_uses_one_doc_file_index_and_resolution_contract() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should follow the Unix epoch")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!("emaxx-native-doc-{unique}"));
    std::fs::create_dir_all(&directory).expect("create synthetic DOC directory");
    let filename = "fixture-DOC";
    let doc = b"\x1fSfixture.o\n\
\x1fFcar\n\
Raw \\[forward-char] docs.\n\n(fn LIST)\
\x1fVcase-fold-search\n\
*Variable \\[forward-char] docs.\
\x1fSafter.o\n";
    std::fs::write(directory.join(filename), doc).expect("write synthetic DOC file");
    let doc_directory = format!("{}/", directory.display());
    let program = format!(
        r#"
        (let ((doc-directory {doc_directory:?}))
          (put 'doc-zero
               'variable-documentation 0)
          (defvar doc-base 1)
          (defvaralias 'doc-alias 'doc-base)
          (put 'doc-base
               'variable-documentation
               "Alias docs.")
          (list
           (Snarf-documentation {filename:?})
           internal-doc-file-name
           (internal-subr-documentation
            (symbol-function 'car))
           (get 'case-fold-search
                'variable-documentation)
           (documentation 'car t)
           (documentation 'car)
           (documentation-property
            'case-fold-search
            'variable-documentation
            t)
           (documentation-property
            'case-fold-search
            'variable-documentation)
           (internal-subr-documentation
            (lambda () "Lisp docs"))
           (condition-case error-data
               (Snarf-documentation 1)
             (error (car error-data)))
           (documentation-property
            'doc-zero 'variable-documentation t)
           (documentation-property
            'doc-alias 'variable-documentation t)))"#
    );
    let expected = r#"
        (nil
         "fixture-DOC"
         18
         -73
         "Raw \\[forward-char] docs.

(fn LIST)"
         #("Raw C-f docs.

(fn LIST)"
           4 7
           (font-lock-face help-key-binding
            face help-key-binding))
         "*Variable \\[forward-char] docs."
         #("*Variable C-f docs."
           10 13
           (font-lock-face help-key-binding
            face help-key-binding))
         t
         wrong-type-argument
         nil
         "Alias docs.")"#;
    let expected_printed = r#"(nil "fixture-DOC" 18 -73 "Raw \\[forward-char] docs.

(fn LIST)" #("Raw C-f docs.

(fn LIST)" 4 7 (font-lock-face help-key-binding face help-key-binding)) "*Variable \\[forward-char] docs." #("*Variable C-f docs." 10 13 (font-lock-face help-key-binding face help-key-binding)) t wrong-type-argument nil "Alias docs.")"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(&program)
        .read()
        .expect("doc.c family contract should parse")
        .expect("doc.c family contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("doc.c family contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("doc.c expected value should parse")
        .expect("doc.c expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "doc.c result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn native_xfaces_lisp_face_registry_family_matches_gnu() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should follow the Unix epoch")
        .as_nanos();
    let color_file = std::env::temp_dir().join(format!("emaxx-native-colors-{unique}"));
    std::fs::write(
        &color_file,
        "1 2 3 alpha\n255 0 16 two words\nnot a color\n",
    )
    .expect("write synthetic xfaces color file");
    let color_file = serde_json::to_string(&color_file.display().to_string())
        .expect("serialize synthetic color filename");
    let program = format!(
        r##"
        (let* ((name 'emaxx-native-face)
               (copy 'emaxx-native-face-copy)
               (frame (selected-frame))
               (global
                (internal-make-lisp-face name nil))
               (local
                (internal-make-lisp-face name frame)))
          (list
           (length global)
           (aref global 0)
           (vectorp global)
           (and (internal-lisp-face-p name nil) t)
           (and (internal-lisp-face-p name frame) t)
           (internal-lisp-face-empty-p name t)
           (internal-lisp-face-empty-p name frame)
           (internal-set-lisp-face-attribute
            name :foreground "red" t)
           (internal-get-lisp-face-attribute
            name :foreground t)
           (internal-lisp-face-empty-p name t)
           (internal-copy-lisp-face
            name copy t nil)
           (internal-get-lisp-face-attribute
            copy :foreground t)
           (internal-lisp-face-equal-p
            name copy t)
           (progn
             (aset global 9 "blue")
             (internal-get-lisp-face-attribute
              name :foreground t))
           (progn
             (internal-set-lisp-face-attribute
              name :height 120 frame)
             (list
              (internal-get-lisp-face-attribute
               name :height frame)
              (internal-get-lisp-face-attribute
               name :height t)))
           (progn
             (internal-set-lisp-face-attribute
              name :inherit 'default t)
             (internal-get-lisp-face-attribute
              name :inherit t))
           (let ((attrs
                  (face-attributes-as-vector
                   '(:height 1.5
                     :foreground "cyan"))))
             (list
              (length attrs)
              (aref attrs 0)
              (aref attrs 4)
              (aref attrs 9)
              (aref attrs 16)))
           (list
            (face-attribute-relative-p
             :height 1.5)
            (face-attribute-relative-p
             :height 100)
            (face-attribute-relative-p
             :foreground 'unspecified)
            (merge-face-attribute
             :height 1.5 100)
            (merge-face-attribute
             :height 1.5 2.0)
            (merge-face-attribute
             :foreground 'unspecified "black")
            (merge-face-attribute
             :foreground "white" "black"))
           (list
            (internal-lisp-face-attribute-values
             :underline)
            (internal-lisp-face-attribute-values
             :height))
           (list
            (bitmap-spec-p "bitmap")
            (bitmap-spec-p
             (list 8 2 (unibyte-string 0 0)))
            (bitmap-spec-p
             (list 9 2 (unibyte-string 0 0)))
            (bitmap-spec-p '(0 1 "")))
           (list
            (color-gray-p "black")
            (color-gray-p "#808080")
            (color-gray-p "red")
            (color-supported-p "red")
            (color-supported-p "not-a-color"))
           (progn
             (internal-set-lisp-face-attribute
              name :weight 'bold t)
             (internal-set-lisp-face-attribute
              name :slant 'italic t)
             (list
              (face-font name t)
              (face-font name frame)))
           (internal-set-font-selection-order
            '(:width :height :weight :slant))
           (internal-set-alternative-font-family-alist
            '(("Foo" "Bar")))
           (internal-set-alternative-font-registry-alist
            '(("ISO" "Foo")))
           (list
            (internal-set-lisp-face-attribute-from-resource
             name :height "140" t)
            (internal-get-lisp-face-attribute
             name :height t))
           (progn
             (internal-set-lisp-face-attribute
              copy :foreground "purple" t)
             (internal-set-lisp-face-attribute
              copy :foreground "green" frame)
             (list
              (internal-merge-in-global-face
               copy frame)
              (internal-get-lisp-face-attribute
               copy :foreground frame)))
           (x-family-fonts)
           (condition-case error-data
               (x-list-fonts "*")
             (error (car error-data)))
           (x-load-color-file {color_file})
           (tty-suppress-bold-inverse-default-colors
            t)
           (clear-face-cache)
           (mapcar
            (lambda (thunk)
              (condition-case error-data
                  (funcall thunk)
                (error (car error-data))))
            (list
             (lambda ()
               (internal-make-lisp-face 1 nil))
             (lambda ()
               (internal-get-lisp-face-attribute
                name :no-such-attribute t))
             (lambda ()
               (internal-set-lisp-face-attribute
                name :weight 'not-a-weight t))
             (lambda ()
               (internal-set-lisp-face-attribute
                name :inherit '(default 1) t))
             (lambda ()
               (internal-set-font-selection-order
                '(:width :height :weight)))))))"##
    );
    let expected = r#"
        (20 face t t t t t
         emaxx-native-face "red" nil
         emaxx-native-face-copy "red" t "blue"
         (120 unspecified)
         default
         (20 unspecified 1.5 "cyan" unspecified)
         (t nil t 150 3.0 "black" "white")
         ((t nil) nil)
         (t t nil nil)
         (t t nil t nil)
         ((italic bold) nil)
         nil
         ((Foo Bar))
         (("iso" "foo"))
         (emaxx-native-face 140)
         (nil "purple")
         nil error
         (("two words" . 16711696)
          ("alpha" . 66051))
         t nil
         (wrong-type-argument error error error error))"#;
    let expected_printed = "(20 face t t t t t emaxx-native-face \"red\" nil emaxx-native-face-copy \"red\" t \"blue\" (120 unspecified) default (20 unspecified 1.5 \"cyan\" unspecified) (t nil t 150 3.0 \"black\" \"white\") ((t nil) nil) (t t nil nil) (t t nil t nil) ((italic bold) nil) nil ((Foo Bar)) ((\"iso\" \"foo\")) (emaxx-native-face 140) (nil \"purple\") nil error ((\"two words\" . 16711696) (\"alpha\" . 66051)) t nil (wrong-type-argument error error error error))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(&program)
        .read()
        .expect("xfaces.c family contract should parse")
        .expect("xfaces.c family contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("xfaces.c family contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("xfaces.c expected value should parse")
        .expect("xfaces.c expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "xfaces.c result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
    let _ = std::fs::remove_file(
        serde_json::from_str::<String>(&color_file).expect("deserialize color filename"),
    );
}

#[test]
fn native_xfaces_frame_table_and_resource_boundary_match_gnu() {
    let program = r#"
        (let* ((name 'emaxx-frame-table-face)
               (frame (selected-frame))
               (table (frame--face-hash-table frame))
               (_global (internal-make-lisp-face name nil))
               (before (gethash name table 'missing))
               (local (internal-make-lisp-face name frame)))
          (internal-set-lisp-face-attribute
           name :foreground "orange" frame)
          (list
           (hash-table-p table)
           (eq table (frame--face-hash-table))
           (hash-table-test table)
           before
           (eq (gethash name table) local)
           (aref (gethash name table) 9)
           (condition-case error-data
               (frame--face-hash-table 42)
             (error (car error-data)))
           (condition-case error-data
               (internal-face-x-get-resource nil "Class")
             (error (car error-data)))
           (condition-case error-data
               (internal-face-x-get-resource "resource" nil)
             (error (car error-data)))))"#;
    let expected =
        "(t t eq missing t \"orange\" wrong-type-argument wrong-type-argument wrong-type-argument)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("xfaces frame table contract should parse")
        .expect("xfaces frame table contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("xfaces frame table contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("xfaces frame table expected value should parse")
            .expect("xfaces frame table expected value should exist")
    );

    let resource = Reader::new(
        r#"
        (condition-case error-data
            (internal-face-x-get-resource "foreground" "Foreground")
          (error (list (car error-data) (cadr error-data))))"#,
    )
    .read()
    .expect("headless face resource contract should parse")
    .expect("headless face resource contract should contain a form");
    assert_eq!(
        interp
            .eval(&resource, &mut env)
            .expect("headless face resource error should be catchable"),
        Value::list([
            Value::symbol("error"),
            Value::string("Window system is not in use or not initialized"),
        ])
    );
}

#[test]
fn native_gnutls_digest_catalog_and_hashing_use_rustcrypto() {
    let program = r#"
        (let ((names '(SHA1 SHA224 SHA256 SHA384 SHA512 MD5
                       STREEBOG-256 STREEBOG-512 GOSTR341194)))
          (list
           (gnutls-digests)
           (mapcar
            (lambda (name)
              (let ((value (gnutls-hash-digest name "abc")))
                (list name
                      (length value)
                      (string-bytes value)
                      (multibyte-string-p value)
                      (secure-hash 'sha256 value))))
            names)
           (let ((descriptor (cdr (assq 'SHA256 (gnutls-digests)))))
             (list
              (equal (gnutls-hash-digest 'SHA256 "abc")
                     (gnutls-hash-digest "SHA256" "abc"))
              (equal (gnutls-hash-digest 'SHA256 "abc")
                     (gnutls-hash-digest descriptor "abc"))
              (equal (gnutls-hash-digest 'SHA256 "abc")
                     (gnutls-hash-digest 6 "abc"))))
           (mapcar
            (lambda (method)
              (condition-case error-data
                  (gnutls-hash-digest method "abc")
                (error error-data)))
            '(BOGUS nil 999))
           (mapcar
            (lambda (input)
              (secure-hash
               'sha256
               (gnutls-hash-digest 'SHA256 input)))
            (list (list "abcdef" 1 4)
                  (list "abcdef" nil 3)
                  (list "abcdef" 3 nil)
                  (list "abcdef" nil nil)))
           (condition-case error-data
               (gnutls-hash-digest 'SHA256 42)
             (error error-data))))"#;
    let expected = concat!(
        "(((STREEBOG-512 :digest-algorithm-id 17 :type gnutls-digest-algorithm ",
        ":digest-algorithm-length 64) (STREEBOG-256 :digest-algorithm-id 16 ",
        ":type gnutls-digest-algorithm :digest-algorithm-length 32) ",
        "(GOSTR341194 :digest-algorithm-id 15 :type gnutls-digest-algorithm ",
        ":digest-algorithm-length 32) (MD5 :digest-algorithm-id 2 :type ",
        "gnutls-digest-algorithm :digest-algorithm-length 16) (SHA224 ",
        ":digest-algorithm-id 9 :type gnutls-digest-algorithm ",
        ":digest-algorithm-length 28) (SHA512 :digest-algorithm-id 8 :type ",
        "gnutls-digest-algorithm :digest-algorithm-length 64) (SHA384 ",
        ":digest-algorithm-id 7 :type gnutls-digest-algorithm ",
        ":digest-algorithm-length 48) (SHA256 :digest-algorithm-id 6 :type ",
        "gnutls-digest-algorithm :digest-algorithm-length 32) (SHA1 ",
        ":digest-algorithm-id 3 :type gnutls-digest-algorithm ",
        ":digest-algorithm-length 20)) ((SHA1 20 20 nil ",
        "\"2c8e065d764096572cda7bd0923710a18e1b2985bf1b918b6fe0c0a21aa7f8b9\") ",
        "(SHA224 28 28 nil ",
        "\"a05a17b2ee93714f17d1d1cdcf7366729149ebc8f7198a467d8c6b0338f3ee54\") ",
        "(SHA256 32 32 nil ",
        "\"4f8b42c22dd3729b519ba6f68d2da7cc5b2d606d05daed5ad5128cc03e6c6358\") ",
        "(SHA384 48 48 nil ",
        "\"c47cc088b7f8657a65899f33c4a4192fc43dd4b10307d5fe45d0410b1cb6ef51\") ",
        "(SHA512 64 64 nil ",
        "\"2b8e2baefea41ddf88d7ccd66550cb9493970ea7854d2e74eb33e57cd3c73d9c\") ",
        "(MD5 16 16 nil ",
        "\"46e7e78bfc6972ccb3a94d62b387cd63bad9a94946df9c7caba1948664db0c62\") ",
        "(STREEBOG-256 32 32 nil ",
        "\"4c515144bceafec3517bcf6d7358ebac7550b2179fab64a3c988a1fb84d8e2f2\") ",
        "(STREEBOG-512 64 64 nil ",
        "\"3e54ae0f0792e194465c7a989d81fc121f64753c2e69c1082e538ab6f7355d17\") ",
        "(GOSTR341194 32 32 nil ",
        "\"788c46e0988fae25fe03f974ed4d4178ca017caff866599ef7f6b72132867661\")) ",
        "(t t t) ((error \"GnuTLS digest-method is invalid or not found\" BOGUS) ",
        "(error \"GnuTLS digest-method is invalid or not found\" nil) ",
        "(error \"GnuTLS digest-method is invalid or not found\" 999)) ",
        "(\"d93d8d21f0ecc7d040f6effe20c7ceb6347c814df496b74c028fea6754567d0a\" ",
        "\"4f8b42c22dd3729b519ba6f68d2da7cc5b2d606d05daed5ad5128cc03e6c6358\" ",
        "\"9f9d6d0ed77b6f7c21197ac03559feac1518ac1eaae18c47047d6ef950d25183\" ",
        "\"ce65d4756128f0035cba4d8d7fae4e9fa93cf7fdf12c0f83ee4a0e84064bef8a\") ",
        "(wrong-type-argument consp 42))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("GnuTLS digest contract should parse")
        .expect("GnuTLS digest contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("GnuTLS digest contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("GnuTLS digest expected value should parse")
            .expect("GnuTLS digest expected value should exist")
    );
}

#[test]
fn native_gnutls_advertises_loaded_host_capabilities() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        "(let ((capabilities (gnutls-available-p)))
           (if (and (equal (secure-hash-algorithms)
                           '(md5 sha1 sha224 sha256 sha384 sha512))
                    (memq 'gnutls3 capabilities)
                    (memq 'digests capabilities)
                    (memq 'macs capabilities)
                    (memq 'ciphers capabilities))
               t nil))",
    )
    .read()
    .expect("GnuTLS availability contract should parse")
    .expect("GnuTLS availability contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("GnuTLS availability contract should evaluate"),
        Value::T
    );
    let globals = Reader::new(
        "(list (integerp libgnutls-version)
               (> libgnutls-version 0)
               gnutls-log-level
               (special-variable-p 'libgnutls-version)
               (special-variable-p 'gnutls-log-level))",
    )
    .read()
    .expect("GnuTLS native globals probe should parse")
    .expect("GnuTLS native globals probe should contain a form");
    assert_eq!(
        interp
            .eval(&globals, &mut env)
            .expect("GnuTLS native globals should evaluate"),
        Value::list([Value::T, Value::T, Value::Integer(0), Value::T, Value::T])
    );
}

#[test]
fn native_gnutls_catalogs_and_error_diagnostics_use_the_host_library() {
    let program = r#"
        (progn
          (put 'custom-fatal 'gnutls-code -10)
          (put 'custom-again 'gnutls-code -28)
          (put 'custom-zero 'gnutls-code 0)
          (put 'custom-float 'gnutls-code 1.5)
          (let ((ciphers (gnutls-ciphers))
                (macs (gnutls-macs))
                (errors (list t 'custom-fatal 'custom-again 'custom-zero
                              -10 -28 0 'missing 1.5 "x")))
            (list
             (list
              (length ciphers)
              (car ciphers)
              (car (last ciphers))
              (assq 'CHACHA20-POLY1305 ciphers)
              (assq 'AES-128-GCM ciphers))
             (list
              (length macs)
              (car macs)
              (car (last macs))
              (assq 'AES-GMAC-128 macs)
              (assq 'SHA256 macs))
             (mapcar
              (lambda (error)
                (condition-case error-data
                    (gnutls-error-fatalp error)
                  (error error-data)))
              errors)
             (mapcar #'gnutls-error-string errors))))"#;
    let expected = concat!(
        "((44 (RC2-40 :cipher-id 17 :type gnutls-symmetric-cipher ",
        ":cipher-aead-capable nil :cipher-tagsize 0 :cipher-blocksize 8 ",
        ":cipher-keysize 5 :cipher-ivsize 8) (AES-256-CBC :cipher-id 5 ",
        ":type gnutls-symmetric-cipher :cipher-aead-capable nil ",
        ":cipher-tagsize 0 :cipher-blocksize 16 :cipher-keysize 32 ",
        ":cipher-ivsize 16) (CHACHA20-POLY1305 :cipher-id 23 :type ",
        "gnutls-symmetric-cipher :cipher-aead-capable t :cipher-tagsize 16 ",
        ":cipher-blocksize 64 :cipher-keysize 32 :cipher-ivsize 12) ",
        "(AES-128-GCM :cipher-id 10 :type gnutls-symmetric-cipher ",
        ":cipher-aead-capable t :cipher-tagsize 16 :cipher-blocksize 16 ",
        ":cipher-keysize 16 :cipher-ivsize 12)) (21 (PBMAC1 ",
        ":mac-algorithm-id 213 :type gnutls-mac-algorithm ",
        ":mac-algorithm-length 0 :mac-algorithm-keysize 0 ",
        ":mac-algorithm-noncesize 0) (SHA1 :mac-algorithm-id 3 :type ",
        "gnutls-mac-algorithm :mac-algorithm-length 20 ",
        ":mac-algorithm-keysize 20 :mac-algorithm-noncesize 0) ",
        "(AES-GMAC-128 :mac-algorithm-id 205 :type gnutls-mac-algorithm ",
        ":mac-algorithm-length 16 :mac-algorithm-keysize 16 ",
        ":mac-algorithm-noncesize 12) (SHA256 :mac-algorithm-id 6 :type ",
        "gnutls-mac-algorithm :mac-algorithm-length 32 ",
        ":mac-algorithm-keysize 32 :mac-algorithm-noncesize 0)) ",
        "(nil t nil nil t nil nil ",
        "(error \"Symbol has no numeric gnutls-code property\") ",
        "(error \"Not an error symbol or code\") ",
        "(error \"Not an error symbol or code\")) ",
        "(\"Not an error\" ",
        "\"The specified session has been invalidated for some reason.\" ",
        "\"Resource temporarily unavailable, try again.\" \"Success.\" ",
        "\"The specified session has been invalidated for some reason.\" ",
        "\"Resource temporarily unavailable, try again.\" \"Success.\" ",
        "\"Symbol has no numeric gnutls-code property\" ",
        "\"Not an error symbol or code\" \"Not an error symbol or code\"))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("GnuTLS catalog and error contract should parse")
        .expect("GnuTLS catalog and error contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("host GnuTLS catalog and error contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("GnuTLS catalog and error result should parse")
        .expect("GnuTLS catalog and error result should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "host GnuTLS catalog/error result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_gnutls_formats_x509_certificates_with_the_host_library() {
    let certificate = fs::read_to_string(
        upstream_emacs_repo().join("test/lisp/net/network-stream-resources/cert.pem"),
    )
    .expect("read upstream X.509 certificate fixture");
    let program = format!(
        r#"
        (let ((formatted (gnutls-format-certificate {certificate:?})))
          (list
           (length formatted)
           (secure-hash 'sha256 formatted)
           (string-prefix-p
            "X.509 Certificate Information:\n\tVersion: 3\n"
            formatted)
           (mapcar
            (lambda (cert)
              (condition-case error-data
                  (gnutls-format-certificate cert)
                (error error-data)))
            '(42 "" "not a certificate"))))"#
    );
    let expected = concat!(
        "(2863 \"2354c81d5fca4d5d2259514652d1254626f8722b6f682178cc9fce21b094fb26\" t ",
        "((wrong-type-argument stringp 42) ",
        "(error \"gnutls-format-certificate error: Base64 unexpected header error.\") ",
        "(error \"gnutls-format-certificate error: Base64 unexpected header error.\")))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(&program)
        .read()
        .expect("GnuTLS certificate-format contract should parse")
        .expect("GnuTLS certificate-format contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("host GnuTLS certificate-format contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("GnuTLS certificate expected result should parse")
        .expect("GnuTLS certificate expected result should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "host GnuTLS certificate result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_gnutls_mac_uses_the_host_crypto_and_zeroizes_keys() {
    let program = r#"
        (let* ((key (copy-sequence "test"))
               (descriptor (cdr (assq 'SHA256 (gnutls-macs))))
               (first (gnutls-hash-mac 'SHA256 key "hello\n"))
               (reference (gnutls-hash-mac 'SHA256 "test" "hello\n")))
          (list
           (secure-hash 'sha256 first)
           (string-to-list key)
           (list
            (equal reference (gnutls-hash-mac "SHA256" "test" "hello\n"))
            (equal reference (gnutls-hash-mac descriptor "test" "hello\n"))
            (equal reference (gnutls-hash-mac 6 "test" "hello\n")))
           (secure-hash
            'sha256
            (gnutls-hash-mac
             'SHA256 '("--test++" 2 6) '("xxhello\nyy" 2 8)))
           (mapcar
            (lambda (method)
              (condition-case error-data
                  (gnutls-hash-mac method "test" "hello\n")
                (error error-data)))
            '(BOGUS "BOGUS" nil 999))
           (condition-case error-data
               (gnutls-hash-mac 'SHA256 42 "x")
             (error error-data))
           (condition-case error-data
               (gnutls-hash-mac 'SHA256 "x" 42)
             (error error-data))))"#;
    let expected = concat!(
        "(\"b76ce0731f8b3aaca7e3e1d0130c68cecd68ada35627dd8f7dad0c3b8e9339a0\" ",
        "(0 0 0 0) (t t t) ",
        "\"b76ce0731f8b3aaca7e3e1d0130c68cecd68ada35627dd8f7dad0c3b8e9339a0\" ",
        "((error \"GnuTLS MAC-method is invalid or not found\" BOGUS) ",
        "(error \"GnuTLS MAC-method is invalid or not found\" BOGUS) ",
        "(error \"GnuTLS MAC-method is invalid or not found\" nil) ",
        "(error \"GnuTLS MAC-method is invalid or not found\" 999)) ",
        "(wrong-type-argument consp 42) (wrong-type-argument consp 42))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("GnuTLS MAC contract should parse")
        .expect("GnuTLS MAC contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("host GnuTLS MAC contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("GnuTLS MAC expected result should parse")
        .expect("GnuTLS MAC expected result should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "host GnuTLS MAC result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_gnutls_symmetric_crypto_round_trips_block_and_aead_ciphers() {
    let program = r#"
        (let* ((block (cdr (assq 'AES-128-CBC (gnutls-ciphers))))
               (block-key (copy-sequence "0123456789abcdef"))
               (block-dec-key (copy-sequence "0123456789abcdef"))
               (block-out
                (gnutls-symmetric-encrypt
                 block block-key "0123456789abcdef" "abcdefghijklmnop"))
               (block-back
                (gnutls-symmetric-decrypt
                 block block-dec-key (cadr block-out) (car block-out)))
               (aead (cdr (assq 'AES-128-GCM (gnutls-ciphers))))
               (aead-key (copy-sequence "0123456789abcdef"))
               (aead-dec-key (copy-sequence "0123456789abcdef"))
               (aead-out
                (gnutls-symmetric-encrypt
                 aead aead-key "0123456789ab" "plaintext" "auth"))
               (aead-back
                (gnutls-symmetric-decrypt
                 aead aead-dec-key (cadr aead-out) (car aead-out) "auth"))
               (auto
                (gnutls-symmetric-encrypt
                 'AES-128-CBC "0123456789abcdef"
                 '(iv-auto 16) "abcdefghijklmnop")))
          (list
           (list
            (secure-hash 'sha256 (car block-out))
            (cadr block-out)
            (car block-back)
            (string= block-key (make-string 16 0))
            (string= block-dec-key (make-string 16 0))
            (equal
             (car block-out)
             (car (gnutls-symmetric-encrypt
                   "AES-128-CBC" "0123456789abcdef"
                   "0123456789abcdef" "abcdefghijklmnop")))
            (equal
             (car block-out)
             (car (gnutls-symmetric-encrypt
                   4 "0123456789abcdef"
                   "0123456789abcdef" "abcdefghijklmnop"))))
           (list
            (length (car aead-out))
            (secure-hash 'sha256 (car aead-out))
            (cadr aead-out)
            (car aead-back)
            (string= aead-key (make-string 16 0))
            (string= aead-dec-key (make-string 16 0)))
           (list
            (length (cadr auto))
            (equal
             (car (gnutls-symmetric-decrypt
                   'AES-128-CBC "0123456789abcdef"
                   (cadr auto) (car auto)))
             "abcdefghijklmnop"))
           (list
            (condition-case error-data
                (gnutls-symmetric-encrypt
                 'BOGUS "0123456789abcdef"
                 "0123456789abcdef" "abcdefghijklmnop")
              (error error-data))
            (condition-case error-data
                (gnutls-symmetric-encrypt
                 'AES-128-CBC "short"
                 "0123456789abcdef" "abcdefghijklmnop")
              (error error-data))
            (condition-case error-data
                (gnutls-symmetric-encrypt
                 'AES-128-CBC "0123456789abcdef"
                 "short" "abcdefghijklmnop")
              (error error-data))
            (condition-case error-data
                (gnutls-symmetric-encrypt
                 'AES-128-CBC "0123456789abcdef"
                 "0123456789abcdef" "short")
              (error error-data)))))"#;
    let expected = concat!(
        "((\"ee1a7e1e074ccb5430bd445f97f07b3f11dce56177c94252f41b532333e72817\" ",
        "\"0123456789abcdef\" \"abcdefghijklmnop\" t t t t) ",
        "(25 \"b53ae704131446902e9463827175e729d436cbf300882ddf2b25cc098c912952\" ",
        "\"0123456789ab\" \"plaintext\" t t) (16 t) ",
        "((error \"GnuTLS cipher is invalid or not found\" BOGUS) ",
        "(error \"GnuTLS cipher AES-128-CBC/encrypt key length 5 is not equal ",
        "to the required 16\") ",
        "(error \"GnuTLS cipher AES-128-CBC/encrypt IV length 5 is not equal ",
        "to the required 16\") ",
        "(error \"GnuTLS cipher AES-128-CBC/encrypt input block length 5 is ",
        "not a multiple of the required 16\")))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("GnuTLS symmetric contract should parse")
        .expect("GnuTLS symmetric contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("host GnuTLS symmetric contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("GnuTLS symmetric expected result should parse")
        .expect("GnuTLS symmetric expected result should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "host GnuTLS symmetric result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_gnutls_pre_session_state_warnings_and_error_predicate_match_gnu() {
    let program = r#"
        (let ((process (make-pipe-process :name "gnutls-state" :noquery t)))
          (unwind-protect
              (list
               (list
                (gnutls-get-initstage process)
                (gnutls-asynchronous-parameters
                 process '(:hostname "example.test"))
                (gnutls-get-initstage process)
                (gnutls-peer-status process)
                (process-plist process)
                (gnutls-deinit process)
                (gnutls-get-initstage process)
                (gnutls-deinit process))
               (mapcar
                (lambda (status)
                  (list status
                        (gnutls-peer-status-warning-describe status)))
                '(:invalid :revoked :self-signed :unknown-ca :not-ca
                  :insecure :not-activated :expired :no-host-match
                  :signature-failure :revocation-data-superseded
                  :revocation-data-issued-in-future
                  :signer-constraints-failure :purpose-mismatch
                  :missing-ocsp-status :invalid-ocsp-status :bogus))
               (mapcar
                (lambda (error)
                  (list error (gnutls-errorp error)))
                '(t gnutls-e-again nil -1 0 foo 42 "x"))
               (condition-case error-data
                   (gnutls-peer-status-warning-describe 1)
                 (error error-data))
               (condition-case error-data
                   (gnutls-get-initstage t)
                 (error error-data))
               (condition-case error-data
                   (gnutls-asynchronous-parameters t nil)
                 (error error-data))
               (condition-case error-data
                   (gnutls-deinit t)
                 (error error-data))
               (condition-case error-data
                   (gnutls-peer-status t)
                 (error error-data)))
            (delete-process process)))"#;
    let expected = concat!(
        "((0 nil 0 nil nil nil 0 nil) ",
        "((:invalid \"certificate could not be verified\") ",
        "(:revoked \"certificate was revoked (CRL)\") ",
        "(:self-signed \"certificate signer was not found (self-signed)\") ",
        "(:unknown-ca \"the certificate was signed by an unknown and therefore ",
        "untrusted authority\") (:not-ca \"certificate signer is not a CA\") ",
        "(:insecure \"certificate was signed with an insecure algorithm\") ",
        "(:not-activated \"certificate is not yet activated\") ",
        "(:expired \"certificate has expired\") ",
        "(:no-host-match \"certificate host does not match hostname\") ",
        "(:signature-failure \"certificate signature could not be verified\") ",
        "(:revocation-data-superseded \"certificate revocation data are old and ",
        "have been superseded\") (:revocation-data-issued-in-future ",
        "\"certificate revocation data have a future issue date\") ",
        "(:signer-constraints-failure \"certificate signer constraints were ",
        "violated\") (:purpose-mismatch \"certificate does not match the intended ",
        "purpose\") (:missing-ocsp-status \"certificate requires the server to ",
        "send a OCSP certificate status, but no status was received\") ",
        "(:invalid-ocsp-status \"the received OCSP certificate status is invalid\") ",
        "(:bogus nil)) ((t nil) (gnutls-e-again nil) (nil t) (-1 t) (0 t) ",
        "(foo t) (42 t) (\"x\" t)) (wrong-type-argument symbolp 1) ",
        "(wrong-type-argument processp t) (wrong-type-argument processp t) ",
        "(wrong-type-argument processp t) (wrong-type-argument processp t))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("GnuTLS pre-session contract should parse")
        .expect("GnuTLS pre-session contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("GnuTLS pre-session contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("GnuTLS pre-session expected value should parse")
            .expect("GnuTLS pre-session expected value should exist")
    );
}

#[test]
fn native_gnutls_boot_and_bye_preserve_gnu_validation_contracts() {
    let program = r#"
        (let ((process (make-pipe-process :name "gnutls-validation" :noquery t)))
          (unwind-protect
              (list
               (subr-arity (symbol-function 'gnutls-boot))
               (subr-arity (symbol-function 'gnutls-bye))
               (mapcar
                (lambda (symbol) (get symbol 'gnutls-code))
                '(gnutls-e-interrupted gnutls-e-again
                  gnutls-e-invalid-session gnutls-e-not-ready-for-handshake))
               (condition-case error-data
                   (gnutls-boot 1 'gnutls-x509pki nil)
                 (error error-data))
               (condition-case error-data
                   (gnutls-boot process 1 nil)
                 (error error-data))
               (condition-case error-data
                   (gnutls-boot process 'gnutls-x509pki 1)
                 (error error-data))
               (condition-case error-data
                   (gnutls-boot process 'bogus nil)
                 (error error-data))
               (condition-case error-data
                   (gnutls-boot process 'gnutls-x509pki nil)
                 (error error-data))
               (condition-case error-data
                   (gnutls-bye 1 nil)
                 (error error-data)))
            (delete-process process)))"#;
    let expected = concat!(
        "((3 . 3) (2 . 2) (-52 -28 -10 -65500) ",
        "(wrong-type-argument processp 1) ",
        "(wrong-type-argument symbolp 1) (wrong-type-argument listp 1) ",
        "(error \"Invalid GnuTLS credential type\") ",
        "(error \"gnutls-boot: invalid :hostname parameter (not a string)\") ",
        "(wrong-type-argument processp 1))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("GnuTLS lifecycle validation should parse")
        .expect("GnuTLS lifecycle validation should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("GnuTLS lifecycle validation should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("GnuTLS lifecycle expectation should parse")
        .expect("GnuTLS lifecycle expectation should contain a form");
    assert!(
        values_equal(&interp, &actual, &expected),
        "GnuTLS lifecycle validation differs from GNU:\nactual: {actual:?}"
    );
}

fn wait_for_local_test_server(child: &mut std::process::Child, port: u16, description: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        assert!(
            child.try_wait().expect("poll local test server").is_none(),
            "{description} exited before accepting a connection"
        );
        if std::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).is_ok() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{description} did not start listening"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
fn native_gnutls_session_encrypts_process_io_and_closes_the_same_transport() {
    struct Server(std::process::Child);
    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("reserve localhost port for GnuTLS test server");
    let port = listener.local_addr().expect("reserved address").port();
    drop(listener);
    let child = std::process::Command::new("gnutls-serv")
        .args([
            "--quiet",
            "--echo",
            "--priority",
            "NORMAL:+ANON-ECDH",
            "--port",
            &port.to_string(),
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("gnutls-serv is required for the transport regression");
    let mut server = Server(child);
    wait_for_local_test_server(&mut server.0, port, "GnuTLS test server");

    let program = format!(
        r#"
        (let* ((buffer (generate-new-buffer " *gnutls-transport*"))
               (process (make-network-process
                         :name "gnutls-transport"
                         :buffer buffer
                         :host "127.0.0.1"
                         :service {port}
                         :family 'ipv4
                         :coding 'binary
                         :sentinel #'ignore
                         :noquery t)))
          (unwind-protect
              (list
               (gnutls-boot
                process 'gnutls-anon
               '(:hostname "localhost"
                  :priority "NORMAL:+ANON-ECDH"
                  :complete-negotiation t))
               (gnutls-get-initstage process)
               (let ((status (gnutls-peer-status process)))
                 (list (stringp (plist-get status :key-exchange))
                       (stringp (plist-get status :protocol))
                       (stringp (plist-get status :cipher))
                       (stringp (plist-get status :mac))
                       (plist-get status :warnings)))
               (progn
                 (process-send-string process "encrypted round trip\n")
                 (accept-process-output process 2)
                 (with-current-buffer buffer (buffer-string)))
               (gnutls-bye process t)
               (gnutls-deinit process)
               (gnutls-get-initstage process))
            (delete-process process)
            (kill-buffer buffer)))"#
    );
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(&program)
        .read()
        .expect("GnuTLS transport program should parse")
        .expect("GnuTLS transport program should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("GnuTLS transport program should evaluate");
    let expected = Reader::new("(t 9 (t t t t nil) \"encrypted round trip\\n\" t t 3)")
        .read()
        .expect("GnuTLS transport expectation should parse")
        .expect("GnuTLS transport expectation should contain a form");
    assert!(
        values_equal(&interp, &actual, &expected),
        "GnuTLS process transport did not round-trip through one live session:\nactual: {actual:?}"
    );

    let asynchronous = format!(
        r#"
        (let* ((buffer (generate-new-buffer " *gnutls-async-transport*"))
               (process (make-network-process
                         :name "gnutls-async-transport"
                         :buffer buffer
                         :host "127.0.0.1"
                         :service {port}
                         :family 'ipv4
                         :coding 'binary
                         :sentinel #'ignore
                         :noquery t
                         :nowait t
                         :tls-parameters
                         '(gnutls-anon
                           :hostname "localhost"
                           :priority "NORMAL:+ANON-ECDH"))))
          (unwind-protect
              (let ((initial-status (process-status process))
                    (tries 0))
                (while (and (eq (process-status process) 'connect)
                            (< (setq tries (1+ tries)) 100))
                  (sit-for 0.01))
                (let ((peer (gnutls-peer-status process)))
                  (list initial-status
                        (process-status process)
                        (gnutls-get-initstage process)
                        (list (stringp (plist-get peer :key-exchange))
                              (stringp (plist-get peer :protocol))
                              (stringp (plist-get peer :cipher))
                              (stringp (plist-get peer :mac)))
                        (progn
                          (process-send-string process "async encrypted round trip\n")
                          (accept-process-output process 2)
                          (with-current-buffer buffer (buffer-string))))))
            (delete-process process)
            (kill-buffer buffer)))"#
    );
    let form = Reader::new(&asynchronous)
        .read()
        .expect("asynchronous GnuTLS transport program should parse")
        .expect("asynchronous GnuTLS transport program should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("asynchronous GnuTLS transport program should evaluate");
    let expected = Reader::new("(connect open 9 (t t t t) \"async encrypted round trip\\n\")")
        .read()
        .expect("asynchronous GnuTLS transport expectation should parse")
        .expect("asynchronous GnuTLS transport expectation should contain a form");
    assert!(
        values_equal(&interp, &actual, &expected),
        "asynchronous GnuTLS negotiation did not complete in the process event loop:\nactual: {actual:?}"
    );
}

#[test]
fn native_gnutls_x509_verifies_explicit_trust_and_rejects_hostname_mismatch() {
    struct Server(std::process::Child);
    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("reserve localhost port for X.509 server");
    let port = listener.local_addr().expect("reserved address").port();
    drop(listener);
    let directory = std::env::temp_dir().join(format!("emaxx-gnutls-x509-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("create GnuTLS X.509 fixture directory");
    let key = directory.join("key.pem");
    let certificate = directory.join("certificate.pem");
    let client_key = directory.join("client-key.pem");
    let client_certificate = directory.join("client-certificate.pem");
    let status = std::process::Command::new("openssl")
        .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout"])
        .arg(&key)
        .arg("-out")
        .arg(&certificate)
        .args([
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("openssl is required for the X.509 transport regression");
    assert!(status.success(), "generate self-signed X.509 fixture");
    let status = std::process::Command::new("openssl")
        .args(["req", "-x509", "-newkey", "rsa:2048", "-keyout"])
        .arg(&client_key)
        .arg("-out")
        .arg(&client_certificate)
        .args([
            "-passout",
            "pass:emaxx-secret",
            "-days",
            "1",
            "-subj",
            "/CN=emaxx-client",
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("openssl is required for the encrypted-key regression");
    assert!(status.success(), "generate encrypted client-key fixture");

    let child = std::process::Command::new("gnutls-serv")
        .args(["--quiet", "--echo", "--port", &port.to_string()])
        .arg("--x509keyfile")
        .arg(&key)
        .arg("--x509certfile")
        .arg(&certificate)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("gnutls-serv is required for the X.509 regression");
    let mut server = Server(child);
    wait_for_local_test_server(&mut server.0, port, "X.509 server");

    let program = format!(
        r#"
        (let ((trusted
               (make-network-process :name "gnutls-x509-trusted"
                                     :host "127.0.0.1" :service {port}
                                     :family 'ipv4 :sentinel #'ignore :noquery t))
              (mismatch nil))
          (unwind-protect
              (let* ((boot
                      (gnutls-boot
                       trusted 'gnutls-x509pki
                       '(:hostname "localhost"
                         :trustfiles ("{certificate}")
                         :keylist (("{client_key}" "{client_certificate}"))
                         :pass "emaxx-secret"
                         :flags (unknown-flag)
                         :loglevel 0
                         :verify-flags 0
                         :min-prime-bits 1024
                         :verify-error t
                         :complete-negotiation t)))
                     (peer (gnutls-peer-status trusted)))
                (gnutls-bye trusted t)
                (gnutls-deinit trusted)
                (delete-process trusted)
                (setq mismatch
                      (make-network-process :name "gnutls-x509-mismatch"
                                            :host "127.0.0.1" :service {port}
                                            :family 'ipv4 :sentinel #'ignore :noquery t))
                (list boot
                      (stringp (plist-get peer :protocol))
                      (plist-get peer :warnings)
                      (let ((certificate (plist-get peer :certificate)))
                        (list (consp (plist-get peer :certificates))
                              (stringp (plist-get certificate :issuer))
                              (stringp (plist-get certificate :subject))))
                      (condition-case error-data
                          (gnutls-boot
                           mismatch 'gnutls-x509pki
                           '(:hostname "wrong.example"
                             :trustfiles ("{certificate}")
                             :verify-error (:hostname)
                             :complete-negotiation t))
                        (error error-data))))
            (delete-process trusted)
            (when mismatch (delete-process mismatch))))"#,
        certificate = certificate.display(),
        client_key = client_key.display(),
        client_certificate = client_certificate.display()
    );
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(&program)
        .read()
        .expect("X.509 verification program should parse")
        .expect("X.509 verification program should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("X.509 verification program should evaluate");
    let items = actual
        .to_vec()
        .expect("X.509 verification result should be a list");
    assert_eq!(items.first(), Some(&Value::T));
    assert_eq!(items.get(1), Some(&Value::T));
    assert_eq!(items.get(2), Some(&Value::Nil));
    assert_eq!(
        items.get(3),
        Some(&Value::list([Value::T, Value::T, Value::T]))
    );
    assert!(
        matches!(
            items.get(4).and_then(|error| error.car().ok()),
            Some(Value::Symbol(symbol)) if symbol == "error"
        ),
        "hostname mismatch should be a catchable error: {actual:?}"
    );
    std::fs::remove_dir_all(directory).expect("remove X.509 fixture directory");
}

#[test]
fn native_conditional_gc_and_memory_info_match_the_host_contract() {
    let program = r#"
        (let ((memory (memory-info)))
          (list
           (garbage-collect-maybe 0)
           (garbage-collect-maybe 1)
           (condition-case nil
               (progn (garbage-collect-maybe -1) nil)
             (wrong-type-argument t))
           (or (null memory)
               (and (= (length memory) 4)
                    (integerp (nth 0 memory))
                    (integerp (nth 1 memory))
                    (integerp (nth 2 memory))
                    (integerp (nth 3 memory))))
           (subrp (symbol-function 'garbage-collect-maybe))
           (subrp (symbol-function 'memory-info))
           (help-function-arglist 'garbage-collect-maybe)
           (help-function-arglist 'memory-info)))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "(nil nil t t t t (arg1) nil)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("conditional GC and memory contract should parse")
        .expect("conditional GC and memory contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("host memory and GC policy should match GNU's shape"),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::list([Value::Symbol("arg1".into())]),
            Value::Nil,
        ])
    );
}

#[test]
fn native_invocation_queries_copy_host_values_and_daemon_finalization_rejects_batch() {
    let program = r#"
        (list
         (equal (invocation-name) invocation-name)
         (eq (invocation-name) invocation-name)
         (equal (invocation-directory) invocation-directory)
         (eq (invocation-directory) invocation-directory)
         (condition-case err (daemon-initialized) (error (car err)))
         (subrp (symbol-function 'invocation-name))
         (help-function-arglist 'invocation-name)
         (subrp (symbol-function 'invocation-directory))
         (help-function-arglist 'invocation-directory)
         (subrp (symbol-function 'daemon-initialized))
         (help-function-arglist 'daemon-initialized))"#;
    let expected = "(t nil t nil error t nil t nil t nil)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("invocation contract should parse")
        .expect("invocation contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("invocation queries should copy native host strings"),
        Reader::new(expected)
            .read()
            .expect("expected invocation result should parse")
            .expect("expected invocation result should exist")
    );
}

#[test]
fn native_syntax_description_decodes_the_shared_descriptor_bits() {
    let program = r#"
        (let
            ((results
              (mapcar
               (lambda (case)
                 (with-temp-buffer
                   (let* ((value (car case))
                          (returned
                           (internal-describe-syntax-value value)))
                     (and (eq returned value)
                          (equal (buffer-string) (cadr case))))))
               (list
                (list nil "default")
                (list (standard-syntax-table) "deeper char-table ...")
                (list 42 "invalid")
                (list (string-to-syntax ".")
                      ". 	which means: punctuation")
                (list (string-to-syntax "(]")
                      "(]	which means: open, matches ]")
                (list
                 (string-to-syntax ". 1234pbnc")
                 ". 1234pbcn	which means: punctuation,
	  is the first character of a comment-start sequence,
	  is the second character of a comment-start sequence,
	  is the first character of a comment-end sequence,
	  is the second character of a comment-end sequence (comment style b) (comment style c) (nestable),
	  is a prefix character for ‘backward-prefix-chars’")))))
          (list results
                (subrp
                 (symbol-function 'internal-describe-syntax-value))
                (help-function-arglist
                 'internal-describe-syntax-value)))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "((t t t t t t) t (arg1))");

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("syntax description contract should parse")
        .expect("syntax description contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("syntax descriptions should decode native descriptor bits"),
        Value::list([
            Value::list(vec![Value::T; 6]),
            Value::T,
            Value::list([Value::Symbol("arg1".into())]),
        ])
    );
}

#[test]
fn canonical_combining_classes_come_from_complete_unicode_data() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    for (character, expected) in [(0x0307, 230), (0x0323, 220)] {
        assert_eq!(
            call(
                &mut interp,
                "get-char-code-property",
                &[
                    Value::Integer(character),
                    Value::Symbol("canonical-combining-class".into()),
                ],
                &mut env,
            )
            .expect("read canonical combining class"),
            Value::Integer(expected)
        );
    }
}

#[test]
fn text_quoting_policy_is_shared_by_the_query_and_substitution_primitives() {
    let program = r#"
        (mapcar
         (lambda (style)
           (let ((text-quoting-style style))
             (list style
                   (text-quoting-style)
                   (substitute-command-keys "`foo'"))))
         '(nil grave straight curve))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "((nil curve \"‘foo’\") (grave grave \"`foo'\") (straight straight \"'foo'\") (curve curve \"‘foo’\"))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("text quoting contract should parse")
        .expect("text quoting contract should contain a form");
    let result = interp
        .eval(&form, &mut env)
        .expect("text quoting query and substitution should share policy");
    assert_eq!(
        result,
        Value::list([
            Value::list([
                Value::Nil,
                Value::Symbol("curve".into()),
                Value::String("‘foo’".into()),
            ]),
            Value::list([
                Value::Symbol("grave".into()),
                Value::Symbol("grave".into()),
                Value::String("`foo'".into()),
            ]),
            Value::list([
                Value::Symbol("straight".into()),
                Value::Symbol("straight".into()),
                Value::String("'foo'".into()),
            ]),
            Value::list([
                Value::Symbol("curve".into()),
                Value::Symbol("curve".into()),
                Value::String("‘foo’".into()),
            ]),
        ])
    );
}

#[test]
fn format_message_quotes_only_format_literals_with_the_effective_text_style() {
    let program = r#"
        (mapcar
         (lambda (style)
           (let ((text-quoting-style style))
             (list style
                   (format-message "`%s'" "`arg'")
                   (format "`%s'" "`arg'"))))
         '(nil grave straight curve))"#;
    let expected = r#"((nil "‘`arg'’" "``arg''") (grave "``arg''" "``arg''") (straight "'`arg''" "``arg''") (curve "‘`arg'’" "``arg''"))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let form = Reader::new(program)
        .read_all()
        .expect("read format-message quoting contract")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate format-message quoting contract");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn selected_global_keymap_is_distinct_from_the_global_map_variable() {
    let program = r#"
        (let ((old-global (current-global-map))
              (old-local (current-local-map))
              (new-global (make-sparse-keymap))
              (new-local (make-sparse-keymap)))
          (define-key new-global "x" 'selected-command)
          (unwind-protect
              (list (use-global-map new-global)
                    (eq (current-global-map) new-global)
                    (eq global-map new-global)
                    (key-binding "x")
                    (use-local-map new-local)
                    (eq (current-local-map) new-local)
                    (subrp (symbol-function 'use-global-map))
                    (help-function-arglist 'use-global-map))
            (use-global-map old-global)
            (use-local-map old-local)))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "(nil t nil selected-command nil t t (arg1))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("selected global keymap contract should parse")
        .expect("selected global keymap contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("global keymap selection should drive key lookup"),
        Value::list([
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::Symbol("selected-command".into()),
            Value::Nil,
            Value::T,
            Value::T,
            Value::list([Value::Symbol("arg1".into())]),
        ])
    );
}

#[test]
fn minor_mode_keymap_consumers_share_gnu_order_replacement_and_default_rules() {
    let program = r#"
        (progn
          (defvar emaxx-test-mode-a nil)
          (defvar emaxx-test-mode-b nil)
          (defvar emaxx-test-mode-c nil)
          (defvar emaxx-test-emulation nil)
          (let* ((map-a (make-sparse-keymap))
                 (map-b (make-sparse-keymap))
                 (map-c (make-sparse-keymap))
                 (prefix-a (make-sparse-keymap))
                 (prefix-c (make-sparse-keymap))
                 (emaxx-test-mode-a t)
                 (emaxx-test-mode-b t)
                 (emaxx-test-mode-c t)
                 (minor-mode-map-alist
                  (list (cons 'emaxx-test-mode-a map-a)
                        (cons 'emaxx-test-mode-b map-b)))
                 (minor-mode-overriding-map-alist
                  (list (cons 'emaxx-test-mode-b map-c)))
                 (emaxx-test-emulation
                  (list (cons 'emaxx-test-mode-c map-a)))
                 (emulation-mode-map-alists '(emaxx-test-emulation)))
            (define-key map-a "x" prefix-a)
            (define-key map-b "x" 'hidden)
            (define-key map-c "x" prefix-c)
            (list
             (mapcar (lambda (map)
                       (cond ((eq map map-a) 'a)
                             ((eq map map-b) 'b)
                             ((eq map map-c) 'c)))
                     (current-minor-mode-maps))
             (mapcar (lambda (entry)
                       (cons (car entry)
                             (cond ((eq (cdr entry) prefix-a) 'pa)
                                   ((eq (cdr entry) prefix-c) 'pc))))
                     (minor-mode-key-binding "x"))
             (let ((defaults (make-sparse-keymap))
                   (emaxx-test-mode-c t)
                   (minor-mode-map-alist nil)
                   (minor-mode-overriding-map-alist nil)
                   (emulation-mode-map-alists nil))
               (define-key defaults [t] 'fallback)
               (let ((minor-mode-map-alist
                      (list (cons 'emaxx-test-mode-c defaults))))
                 (list (lookup-key defaults "z")
                       (lookup-key defaults "z" t)
                       (minor-mode-key-binding "z")
                       (minor-mode-key-binding "z" t))))
             (list (subrp (symbol-function 'current-minor-mode-maps))
                   (help-function-arglist 'current-minor-mode-maps)
                   (subrp (symbol-function 'minor-mode-key-binding))
                   (help-function-arglist 'minor-mode-key-binding)))))"#;
    let expected = "((a c a) ((emaxx-test-mode-c . pa) (emaxx-test-mode-b . pc) (emaxx-test-mode-a . pa)) (nil fallback nil ((emaxx-test-mode-c . fallback))) (t nil t (arg1 &optional arg2)))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("minor-mode keymap contract should parse")
        .expect("minor-mode keymap contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("all minor-mode keymap consumers should share one stack"),
        Reader::new(expected)
            .read()
            .expect("expected minor-mode keymap result should parse")
            .expect("expected minor-mode keymap result should exist")
    );
}

#[test]
fn map_keymap_internal_visits_only_direct_bindings_and_returns_the_parent() {
    let program = r#"
        (let* ((parent (make-sparse-keymap))
               (map (make-sparse-keymap))
               seen result)
          (define-key parent "p" 'parent-command)
          (define-key map "a" 'child-command)
          (set-keymap-parent map parent)
          (setq result
                (map-keymap-internal
                 (lambda (key value) (push (cons key value) seen))
                 map))
          (list (eq result parent)
                seen
                (subrp (symbol-function 'map-keymap-internal))
                (help-function-arglist 'map-keymap-internal)
                (help-function-arglist 'map-keymap)))"#;
    let expected = "(t ((97 . child-command)) t (arg1 arg2) (arg1 arg2 &optional arg3))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("internal keymap walker contract should parse")
        .expect("internal keymap walker contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("internal keymap walker should preserve the parent boundary"),
        Reader::new(expected)
            .read()
            .expect("expected internal keymap result should parse")
            .expect("expected internal keymap result should exist")
    );
}

#[test]
fn describe_vector_groups_equal_ranges_and_shares_standard_output_with_describers() {
    let program = r#"
        (let ((table (make-char-table nil nil)))
          (set-char-table-range table (cons 65 67) 'foo)
          (set-char-table-range table 70 'bar)
          (list
           (with-temp-buffer
             (list (describe-vector [foo foo nil bar bar bar])
                   (buffer-string)))
           (with-temp-buffer
             (list (describe-vector
                    [foo nil bar]
                    (lambda (value) (insert (format "<%S>" value))))
                   (buffer-string)))
           (with-temp-buffer
             (list (describe-vector table) (buffer-string)))
           (subrp (symbol-function 'describe-vector))
           (help-function-arglist 'describe-vector)))"#;
    let expected = "((nil \"\nC-@ .. C-a\tfoo\nC-c .. C-e\tbar\n\") (nil \"\nC-@\t\t<foo>\nC-b\t\t<bar>\n\") (nil \"\nA .. C\t\tfoo\nF\t\tbar\n\") t (arg1 &optional arg2))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("vector description contract should parse")
        .expect("vector description contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("vector and char-table descriptions should match GNU"),
        Reader::new(expected)
            .read()
            .expect("expected vector description result should parse")
            .expect("expected vector description result should exist")
    );
}

#[test]
fn internal_buffer_completion_preserves_hidden_filtering_metadata_and_predicate_values() {
    let program = r#"
        (progn
          (get-buffer-create " zz-hidden")
          (get-buffer-create "zz-visible")
          (let ((predicate
                 (lambda (entry)
                   (member (car entry) '(" zz-hidden" "zz-visible")))))
            (list
             (internal-complete-buffer "" predicate t)
             (internal-complete-buffer " " predicate t)
             (internal-complete-buffer "zz-v" predicate nil)
             (internal-complete-buffer "zz-visible" predicate 'lambda)
             (internal-complete-buffer "" predicate 'metadata)
             (internal-complete-buffer "" predicate 'other)
             (subrp (symbol-function 'internal-complete-buffer))
             (help-function-arglist 'internal-complete-buffer))))"#;
    let expected = "((\"zz-visible\") (\" zz-hidden\") \"zz-visible\" (\"zz-visible\") (metadata (category . buffer) (cycle-sort-function . identity)) nil t (arg1 arg2 arg3))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("buffer completion contract should parse")
        .expect("buffer completion contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("buffer completion should preserve GNU's collection protocol"),
        Reader::new(expected)
            .read()
            .expect("expected buffer completion result should parse")
            .expect("expected buffer completion result should exist")
    );
}

#[test]
fn native_command_and_variable_readers_normalize_defaults_and_intern_results() {
    let oracle_program = r#"
        (progn
          (defun emaxx-test-readable-command () (interactive))
          (defcustom emaxx-test-readable-option nil "test"
            :type 'boolean :group 'emacs)
          (prin1
           (list
            (read-command "Command: " 'emaxx-test-readable-command)
            (read-command "Command: "
                          '("emaxx-test-readable-command" "ignore"))
            (read-command "Command: ")
            (read-variable "Variable: " 'emaxx-test-readable-option)
            (read-variable "Variable: "
                           '("emaxx-test-readable-option" "other"))
            (subrp (symbol-function 'read-command))
            (help-function-arglist 'read-command)
            (subrp (symbol-function 'read-variable))
            (help-function-arglist 'read-variable))))"#;
    let result = "(emaxx-test-readable-command emaxx-test-readable-command ## emaxx-test-readable-option emaxx-test-readable-option t (arg1 &optional arg2) t (arg1 &optional arg2))";
    assert_upstream_primitive_contract_with_stdin(
        oracle_program,
        "\n\n\n\n\n",
        &format!("Command: Command: Command: Variable: Variable: {result}"),
    );

    let emaxx_program = r#"
        (progn
          (defun emaxx-test-readable-command () (interactive))
          (defcustom emaxx-test-readable-option nil "test"
            :type 'boolean :group 'emacs)
          (list
           (read-command "Command: " 'emaxx-test-readable-command)
           (read-command "Command: "
                         '("emaxx-test-readable-command" "ignore"))
           (let ((unread-command-events '(13)))
             (read-command "Command: "))
           (read-variable "Variable: " 'emaxx-test-readable-option)
           (read-variable "Variable: "
                          '("emaxx-test-readable-option" "other"))
           (subrp (symbol-function 'read-command))
           (help-function-arglist 'read-command)
           (subrp (symbol-function 'read-variable))
           (help-function-arglist 'read-variable)))"#;
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(emaxx_program)
        .read()
        .expect("native symbol reader contract should parse")
        .expect("native symbol reader contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("native symbol readers should use completion and intern"),
        Reader::new(result)
            .read()
            .expect("expected native symbol reader result should parse")
            .expect("expected native symbol reader result should exist")
    );
}

#[test]
fn set_minibuffer_window_validates_and_updates_the_shared_window_state() {
    let program = r#"
        (let ((mini (minibuffer-window))
              (ordinary (selected-window)))
          (list (eq (set-minibuffer-window mini) mini)
                (eq (minibuffer-window) mini)
                (condition-case err
                    (set-minibuffer-window ordinary)
                  (error (car err)))
                (condition-case err
                    (set-minibuffer-window 7)
                  (error (car err)))
                (subrp (symbol-function 'set-minibuffer-window))
                (help-function-arglist 'set-minibuffer-window)))"#;
    let expected = "(t t error wrong-type-argument t (arg1))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("minibuffer window contract should parse")
        .expect("minibuffer window contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("minibuffer window selection should share window state"),
        Reader::new(expected)
            .read()
            .expect("expected minibuffer window result should parse")
            .expect("expected minibuffer window result should exist")
    );
}

#[test]
fn looking_back_matches_text_before_point_with_limit() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "alpha beta");
    interp.buffer.goto_char(11);
    let mut env = Vec::new();

    let result = call(
        &mut interp,
        "looking-back",
        &[Value::String("b\\(eta\\)".into()), Value::Integer(7)],
        &mut env,
    )
    .expect("looking-back should match text ending at point");

    assert_eq!(result, Value::T);
    assert!(matches!(
        interp.last_match_data.as_ref(),
        Some(data) if data.get(1).copied().flatten() == Some((8, 11))
    ));
    assert_eq!(
        call(
            &mut interp,
            "looking-back",
            &[Value::String("alpha".into()), Value::Integer(7)],
            &mut env,
        )
        .expect("looking-back should honor limit"),
        Value::Nil
    );
}

#[test]
fn set_text_properties_replaces_existing_properties() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "abc");
    let mut env = Vec::new();

    call(
        &mut interp,
        "add-text-properties",
        &[
            Value::Integer(1),
            Value::Integer(3),
            Value::list([
                Value::Symbol("face".into()),
                Value::Symbol("bold".into()),
                Value::Symbol("mouse-face".into()),
                Value::Symbol("highlight".into()),
            ]),
        ],
        &mut env,
    )
    .expect("add-text-properties should seed buffer props");

    call(
        &mut interp,
        "set-text-properties",
        &[
            Value::Integer(1),
            Value::Integer(3),
            Value::list([Value::Symbol("face".into()), Value::Symbol("italic".into())]),
        ],
        &mut env,
    )
    .expect("set-text-properties should replace buffer props");

    assert_eq!(
        interp.buffer.text_properties_at(1),
        vec![("face".into(), Value::Symbol("italic".into()))]
    );

    let string = call(
        &mut interp,
        "buffer-substring",
        &[Value::Integer(1), Value::Integer(3)],
        &mut env,
    )
    .expect("buffer-substring should preserve text properties");

    call(
        &mut interp,
        "set-text-properties",
        &[
            Value::Integer(0),
            Value::Integer(2),
            Value::list([
                Value::Symbol("face".into()),
                Value::Symbol("underline".into()),
            ]),
            string.clone(),
        ],
        &mut env,
    )
    .expect("set-text-properties should replace substring props");

    let props = call(
        &mut interp,
        "text-properties-at",
        &[Value::Integer(0), string],
        &mut env,
    )
    .expect("text-properties-at should read string props");
    assert_eq!(
        props,
        Value::list([
            Value::Symbol("face".into()),
            Value::Symbol("underline".into()),
        ])
    );
}

#[test]
fn buffer_substring_accepts_reversed_bounds() {
    let mut interp = Interpreter::new();
    interp.buffer.insert("abcdef");
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "buffer-substring-no-properties",
            &[Value::Integer(5), Value::Integer(2)],
            &mut env,
        )
        .expect("buffer-substring-no-properties should accept reversed bounds"),
        Value::String("bcd".into())
    );
}

#[test]
fn delete_and_extract_region_preserves_text_properties() {
    let mut interp = Interpreter::new();
    interp.buffer.insert("abcdef");
    let mut env = Vec::new();
    call(
        &mut interp,
        "put-text-property",
        &[
            Value::Integer(2),
            Value::Integer(5),
            Value::Symbol("face".into()),
            Value::Symbol("bold".into()),
        ],
        &mut env,
    )
    .expect("put-text-property should install buffer props");

    let extracted = call(
        &mut interp,
        "delete-and-extract-region",
        &[Value::Integer(5), Value::Integer(2)],
        &mut env,
    )
    .expect("delete-and-extract-region should accept reversed bounds");

    assert_eq!(string_text(&extracted).expect("extracted text"), "bcd");
    assert_eq!(
        interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
            .expect("remaining buffer text"),
        "aef"
    );
    assert_eq!(
        call(
            &mut interp,
            "text-properties-at",
            &[Value::Integer(0), extracted],
            &mut env,
        )
        .expect("extracted string should retain properties"),
        Value::list([Value::Symbol("face".into()), Value::Symbol("bold".into()),])
    );
}

#[test]
fn buffer_substring_preserves_properties_with_reversed_bounds() {
    let mut interp = Interpreter::new();
    interp.buffer.insert("abcdef");
    let mut env = Vec::new();
    call(
        &mut interp,
        "set-text-properties",
        &[
            Value::Integer(2),
            Value::Integer(5),
            Value::list([Value::Symbol("face".into()), Value::Symbol("bold".into())]),
        ],
        &mut env,
    )
    .expect("set-text-properties should install buffer props");

    let string = call(
        &mut interp,
        "buffer-substring",
        &[Value::Integer(5), Value::Integer(2)],
        &mut env,
    )
    .expect("buffer-substring should accept reversed bounds");

    let props = call(
        &mut interp,
        "text-properties-at",
        &[Value::Integer(0), string],
        &mut env,
    )
    .expect("text-properties-at should read reversed substring props");
    assert_eq!(
        props,
        Value::list([Value::Symbol("face".into()), Value::Symbol("bold".into())])
    );
}

#[test]
fn next_single_property_change_uses_string_positions() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let string = call(
        &mut interp,
        "propertize",
        &[
            Value::String("ab".into()),
            Value::Symbol("seed".into()),
            Value::T,
        ],
        &mut env,
    )
    .expect("propertize should create a mutable string object");
    call(
        &mut interp,
        "set-text-properties",
        &[
            Value::Integer(0),
            Value::Integer(1),
            Value::list([Value::Symbol("help-echo".into()), Value::T]),
            string.clone(),
        ],
        &mut env,
    )
    .expect("set-text-properties should add a string property at position zero");

    let change = call(
        &mut interp,
        "next-single-property-change",
        &[Value::Integer(0), Value::Symbol("help-echo".into()), string],
        &mut env,
    )
    .expect("next-single-property-change should scan strings from position zero");

    assert_eq!(change, Value::Integer(1));
}

#[test]
fn next_single_property_change_returns_nil_for_uniform_string_property() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let string = call(
        &mut interp,
        "propertize",
        &[
            Value::String("abc".into()),
            Value::Symbol("help-echo".into()),
            Value::T,
        ],
        &mut env,
    )
    .expect("propertize should return a propertized string");

    let change = call(
        &mut interp,
        "next-single-property-change",
        &[Value::Integer(0), Value::Symbol("help-echo".into()), string],
        &mut env,
    )
    .expect("next-single-property-change should return nil when a string property is uniform");

    assert_eq!(change, Value::Nil);
}

#[test]
fn property_change_helpers_accept_markers() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "abc");
    let mut env = Vec::new();

    call(
        &mut interp,
        "put-text-property",
        &[
            Value::Integer(1),
            Value::Integer(3),
            Value::Symbol("button".into()),
            Value::T,
        ],
        &mut env,
    )
    .expect("put-text-property should set button text properties");

    let marker = interp.make_marker();
    let Value::Marker(marker_id) = marker else {
        unreachable!("make_marker returns a marker");
    };
    interp
        .set_marker(marker_id, Some(2), Some(interp.current_buffer_id()))
        .expect("set-marker should accept a live buffer");

    let previous = call(
        &mut interp,
        "previous-single-property-change",
        &[Value::Marker(marker_id), Value::Symbol("button".into())],
        &mut env,
    )
    .expect("previous-single-property-change should accept markers");
    let next = call(
        &mut interp,
        "next-single-property-change",
        &[Value::Marker(marker_id), Value::Symbol("button".into())],
        &mut env,
    )
    .expect("next-single-property-change should accept markers");

    assert_eq!(previous, Value::Integer(1));
    assert_eq!(next, Value::Integer(3));
}

#[test]
fn get_text_property_inherits_from_category_symbol() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "abc");
    interp.put_symbol_property(
        "sample-button-category",
        "type",
        Value::Symbol("sample-button-type".into()),
    );
    let mut env = Vec::new();

    call(
        &mut interp,
        "put-text-property",
        &[
            Value::Integer(1),
            Value::Integer(2),
            Value::Symbol("category".into()),
            Value::Symbol("sample-button-category".into()),
        ],
        &mut env,
    )
    .expect("put-text-property should assign a category");

    assert_eq!(
        call(
            &mut interp,
            "get-text-property",
            &[Value::Integer(1), Value::Symbol("type".into())],
            &mut env,
        )
        .expect("get-text-property should inherit button properties"),
        Value::Symbol("sample-button-type".into())
    );
}

#[test]
fn overlay_get_inherits_from_category_symbol() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "abc");
    interp.put_symbol_property(
        "sample-button-category",
        "type",
        Value::Symbol("sample-button-type".into()),
    );
    let mut env = Vec::new();

    let overlay = call(
        &mut interp,
        "make-overlay",
        &[Value::Integer(1), Value::Integer(2)],
        &mut env,
    )
    .expect("make-overlay should create an overlay");
    call(
        &mut interp,
        "overlay-put",
        &[
            overlay.clone(),
            Value::Symbol("category".into()),
            Value::Symbol("sample-button-category".into()),
        ],
        &mut env,
    )
    .expect("overlay-put should assign a category");

    assert_eq!(
        call(
            &mut interp,
            "overlay-get",
            &[overlay, Value::Symbol("type".into())],
            &mut env,
        )
        .expect("overlay-get should inherit button properties"),
        Value::Symbol("sample-button-type".into())
    );
}

#[test]
fn copy_overlay_clones_region_and_properties_with_new_identity() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "abcdef");
    let mut env = Vec::new();

    let overlay = call(
        &mut interp,
        "make-overlay",
        &[Value::Integer(2), Value::Integer(5)],
        &mut env,
    )
    .expect("make-overlay should create an overlay");
    call(
        &mut interp,
        "overlay-put",
        &[
            overlay.clone(),
            Value::Symbol("display".into()),
            Value::String("".into()),
        ],
        &mut env,
    )
    .expect("overlay-put should assign a display property");

    let copy = call(
        &mut interp,
        "copy-overlay",
        std::slice::from_ref(&overlay),
        &mut env,
    )
    .expect("copy-overlay should clone a live overlay");

    assert_ne!(copy, overlay);
    assert_eq!(
        call(
            &mut interp,
            "overlay-start",
            std::slice::from_ref(&copy),
            &mut env
        )
        .expect("copy should have a start"),
        Value::Integer(2)
    );
    assert_eq!(
        call(
            &mut interp,
            "overlay-end",
            std::slice::from_ref(&copy),
            &mut env
        )
        .expect("copy should have an end"),
        Value::Integer(5)
    );
    assert_eq!(
        call(
            &mut interp,
            "overlay-get",
            &[copy, Value::Symbol("display".into())],
            &mut env,
        )
        .expect("copy should keep properties"),
        Value::String("".into())
    );
}

#[test]
fn substitute_command_keys_uses_explicit_keymaps() {
    let mut interp = Interpreter::new();
    let keymap = make_runtime_keymap(&mut interp, Some("button-tests--map"));
    interp.set_global_binding("button-tests--map", keymap.clone());
    keymap_define_binding(&mut interp, &keymap, "x", Value::Symbol("ignore".into()))
        .expect("keymap bindings should accept runtime keymaps");

    let mut env = Vec::new();
    let substituted = call(
        &mut interp,
        "substitute-command-keys",
        &[Value::String(
            "text: \\<button-tests--map>\\[ignore]".into(),
        )],
        &mut env,
    )
    .expect("substitute-command-keys should expand explicit keymaps");

    assert_eq!(substituted, Value::String("text: x".into()));
}

#[test]
fn add_face_text_property_preserves_other_string_properties() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let string = call(
        &mut interp,
        "propertize",
        &[
            Value::String("button text".into()),
            Value::Symbol("help-echo".into()),
            Value::String("help text".into()),
        ],
        &mut env,
    )
    .expect("propertize should create a mutable string");
    call(
        &mut interp,
        "add-face-text-property",
        &[
            Value::Integer(0),
            Value::Integer(11),
            Value::Symbol("button".into()),
            Value::T,
            string.clone(),
        ],
        &mut env,
    )
    .expect("add-face-text-property should preserve unrelated props");

    assert_eq!(
        string_property_at(&string, 0, "help-echo"),
        Some(Value::String("help text".into()))
    );
    assert_eq!(
        string_property_at(&string, 0, "face"),
        Some(Value::Symbol("button".into()))
    );
}

#[test]
fn propertize_preserves_existing_string_properties() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let seed = call(
        &mut interp,
        "propertize",
        &[
            Value::String("button text".into()),
            Value::Symbol("help-echo".into()),
            Value::String("help text".into()),
        ],
        &mut env,
    )
    .expect("propertize should seed a string property");
    let updated = call(
        &mut interp,
        "propertize",
        &[seed, Value::Symbol("button".into()), Value::T],
        &mut env,
    )
    .expect("propertize should preserve existing properties");

    assert_eq!(
        string_property_at(&updated, 0, "help-echo"),
        Some(Value::String("help text".into()))
    );
    assert_eq!(string_property_at(&updated, 0, "button"), Some(Value::T));
}

#[test]
fn make_symbol_creates_distinct_symbols_with_stable_visible_names() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let left = call(
        &mut interp,
        "make-symbol",
        &[Value::String("help".into())],
        &mut env,
    )
    .expect("make-symbol should return a symbol");
    let right = call(
        &mut interp,
        "make-symbol",
        &[Value::String("help".into())],
        &mut env,
    )
    .expect("make-symbol should return a distinct symbol");

    assert_ne!(left, right);
    assert_eq!(
        call(
            &mut interp,
            "symbol-name",
            std::slice::from_ref(&left),
            &mut env
        )
        .expect("symbol-name should preserve the visible name"),
        Value::String("help".into())
    );
    assert_eq!(
        call(
            &mut interp,
            "symbol-name",
            std::slice::from_ref(&right),
            &mut env
        )
        .expect("symbol-name should preserve the visible name"),
        Value::String("help".into())
    );
}

#[test]
fn text_property_search_helpers_find_matches_and_gaps() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "abcd");
    let mut env = Vec::new();

    call(
        &mut interp,
        "put-text-property",
        &[
            Value::Integer(1),
            Value::Integer(3),
            Value::Symbol("fontified".into()),
            Value::T,
        ],
        &mut env,
    )
    .expect("put-text-property should set buffer props");

    assert_eq!(
        call(
            &mut interp,
            "text-property-any",
            &[
                Value::Integer(1),
                Value::Integer(5),
                Value::Symbol("fontified".into()),
                Value::T,
            ],
            &mut env,
        )
        .expect("text-property-any should find the first matching position"),
        Value::Integer(1)
    );
    assert_eq!(
        call(
            &mut interp,
            "text-property-not-all",
            &[
                Value::Integer(1),
                Value::Integer(5),
                Value::Symbol("fontified".into()),
                Value::T,
            ],
            &mut env,
        )
        .expect("text-property-not-all should find the first gap"),
        Value::Integer(3)
    );
}

#[test]
fn font_lock_mode_enables_minimal_jit_lock_state() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(&mut interp, "font-lock-mode", &[], &mut env)
            .expect("font-lock-mode should enable font-lock"),
        Value::T
    );
    let buffer_id = interp.current_buffer_id();
    assert_eq!(
        interp.buffer_local_value(buffer_id, "font-lock-mode"),
        Some(Value::T)
    );
    assert_eq!(
        interp.buffer_local_value(buffer_id, "jit-lock-mode"),
        Some(Value::T)
    );
    assert_eq!(
        interp.buffer_local_value(buffer_id, "jit-lock-functions"),
        Some(Value::list([Value::Symbol("ignore".into())]))
    );
}

#[test]
fn backtrace_frame_internal_honors_depth_relative_to_base() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.push_backtrace_frame(Value::Symbol("outer-frame".into()), &[Value::Integer(7)]);
    interp.push_backtrace_frame(Value::Symbol("base-frame".into()), &[]);
    interp.push_backtrace_frame(Value::Symbol("inner-frame".into()), &[]);

    assert_eq!(
        call(
            &mut interp,
            "backtrace-frame--internal",
            &[
                Value::Symbol("list".into()),
                Value::Integer(1),
                Value::Symbol("base-frame".into()),
            ],
            &mut env,
        )
        .expect("select a frame relative to the requested base"),
        Value::list([
            Value::T,
            Value::Symbol("outer-frame".into()),
            Value::list([Value::Integer(7)]),
            Value::Nil,
        ])
    );
}

#[test]
fn set_buffer_redisplay_is_a_callable_variable_watcher() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let watcher = interp
        .lookup_function("set-buffer-redisplay", &env)
        .expect("xdisp watcher primitive should be prebound");
    assert_eq!(
        call(
            &mut interp,
            "add-variable-watcher",
            &[Value::Symbol("header-line-format".into()), watcher.clone(),],
            &mut env,
        )
        .expect("install redisplay watcher"),
        watcher
    );
    let form = Reader::new(
        "(setq header-line-format
               '(:eval (get-text-property (point-min) 'header-line)))",
    )
    .read_all()
    .expect("read redisplay watcher assignment")
    .remove(0);
    assert!(
        interp.eval(&form, &mut env).is_ok(),
        "the GNU redisplay watcher must accept assignments"
    );
}

#[test]
fn font_lock_text_property_helpers_keep_anonymous_faces_atomic() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "foo");
    let mut env = Vec::new();

    call(
        &mut interp,
        "add-text-properties",
        &[
            Value::Integer(1),
            Value::Integer(3),
            Value::list([Value::Symbol("face".into()), Value::Symbol("italic".into())]),
        ],
        &mut env,
    )
    .expect("add-text-properties should seed a face property");

    call(
        &mut interp,
        "font-lock-append-text-property",
        &[
            Value::Integer(1),
            Value::Integer(3),
            Value::Symbol("face".into()),
            Value::list([Value::Symbol(":strike-through".into()), Value::T]),
        ],
        &mut env,
    )
    .expect("font-lock-append-text-property should accept an omitted object");

    assert_eq!(
        interp.buffer.text_property_at(1, "face"),
        Some(Value::list([
            Value::Symbol("italic".into()),
            Value::list([Value::Symbol(":strike-through".into()), Value::T,]),
        ]))
    );

    call(
        &mut interp,
        "font-lock-prepend-text-property",
        &[
            Value::Integer(1),
            Value::Integer(3),
            Value::Symbol("face".into()),
            Value::list([Value::Symbol(":underline".into()), Value::T]),
        ],
        &mut env,
    )
    .expect("font-lock-prepend-text-property should accept an omitted object");

    assert_eq!(
        interp.buffer.text_property_at(1, "face"),
        Some(Value::list([
            Value::list([Value::Symbol(":underline".into()), Value::T]),
            Value::Symbol("italic".into()),
            Value::list([Value::Symbol(":strike-through".into()), Value::T,]),
        ]))
    );
}

#[test]
fn bidi_override_positions_match_upstream_cases() {
    let cases = [
        (
            "int main() {\n  bool isAdmin = false;\n  /*\u{202e} }\u{2066}if (isAdmin)\u{2069} \u{2066} begin admins only */\n  printf(\"You are an admin.\\\\n\");\n  /* end admins only \u{202e} { \u{2066}*/\n  return 0;\n}",
            Some(46),
        ),
        (
            "#define is_restricted_user(user)\t\t\t\\\\\n  !strcmp (user, \"root\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"admin\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"superuser\u{202e}\u{2066}? 0 : 1\u{2069} \u{2066}\")\u{2069}\u{202c}\n\nint main () {\n  printf (\"root: %d\\\\n\", is_restricted_user (\"root\"));\n  printf (\"admin: %d\\\\n\", is_restricted_user (\"admin\"));\n  printf (\"superuser: %d\\\\n\", is_restricted_user (\"superuser\"));\n  printf (\"luser: %d\\\\n\", is_restricted_user (\"luser\"));\n  printf (\"nobody: %d\\\\n\", is_restricted_user (\"nobody\"));\n}",
            None,
        ),
        (
            "#define is_restricted_user(user)\t\t\t\\\\\n  !strcmp (user, \"root\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"admin\") ? 0 :\t\t\t\\\\\n  !strcmp (user, \"superuser\u{202e}\u{2066}? '#' : '!'\u{2069} \u{2066}\")\u{2069}\u{202c}\n\nint main () {\n  printf (\"root: %d\\\\n\", is_restricted_user (\"root\"));\n  printf (\"admin: %d\\\\n\", is_restricted_user (\"admin\"));\n  printf (\"superuser: %d\\\\n\", is_restricted_user (\"superuser\"));\n  printf (\"luser: %d\\\\n\", is_restricted_user (\"luser\"));\n  printf (\"nobody: %d\\\\n\", is_restricted_user (\"nobody\"));\n}",
            None,
        ),
    ];

    for (index, (text, expected_exact)) in cases.into_iter().enumerate() {
        let mut interp = Interpreter::new();
        interp.buffer = crate::buffer::Buffer::from_text("*test*", text);
        let found = find_bidi_override(
            &interp,
            interp.buffer.point_min(),
            interp.buffer.point_max(),
        );
        if let Some(expected) = expected_exact {
            assert_eq!(found, Some(expected));
        } else {
            assert!(
                found.is_some(),
                "case {index} should report a suspicious bidi override position"
            );
        }
    }

    // A directional isolate changes presentation without overriding the
    // surrounding logical order.  Textsec appends the mixed-direction suffix
    // before asking the primitive, so exercise the exact balanced shape that
    // previously produced a false positive.
    let balanced = "אבגד \u{2067}שונה\u{2069} מרגילa1א:!";
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", balanced);
    assert_eq!(
        find_bidi_override(
            &interp,
            interp.buffer.point_min(),
            interp.buffer.point_max(),
        ),
        None
    );
}

#[test]
fn key_description_formats_follow_prefix_defaults() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let result = call(
        &mut interp,
        "key-description",
        &[Value::String("\u{3}.".into())],
        &mut env,
    )
    .expect("key-description should accept control-char strings");
    assert_eq!(result, Value::String("C-c .".into()));
}

#[test]
fn key_description_matches_upstream_string_and_vector_cases() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let prefixed = call(
        &mut interp,
        "key-description",
        &[
            Value::list([
                Value::Symbol("vector-literal".into()),
                Value::Symbol("right".into()),
            ]),
            Value::list([Value::Symbol("vector-literal".into()), Value::Integer(0x18)]),
        ],
        &mut env,
    )
    .expect("key-description should format prefixed vector keys");
    assert_eq!(prefixed, Value::String("C-x <right>".into()));

    let meta_prefixed = call(
        &mut interp,
        "key-description",
        &[
            Value::list([
                Value::Symbol("vector-literal".into()),
                Value::Integer('<' as i64),
            ]),
            Value::list([
                Value::Symbol("vector-literal".into()),
                Value::Integer(KEY_DESCRIPTION_META_PREFIX),
            ]),
        ],
        &mut env,
    )
    .expect("key-description should combine an ESC prefix with the next event");
    assert_eq!(meta_prefixed, Value::String("M-<".into()));

    let nested_meta_prefixed = call(
        &mut interp,
        "key-description",
        &[
            Value::list([
                Value::Symbol("vector-literal".into()),
                Value::Integer('c' as i64),
            ]),
            Value::list([
                Value::Symbol("vector-literal".into()),
                Value::Integer(KEY_DESCRIPTION_META_PREFIX),
                Value::Integer('g' as i64),
                Value::Integer(KEY_DESCRIPTION_META_PREFIX),
            ]),
        ],
        &mut env,
    )
    .expect("key-description should combine nested ESC prefixes");
    assert_eq!(nested_meta_prefixed, Value::String("M-g M-c".into()));

    let raw_byte = call(
        &mut interp,
        "key-description",
        &[bytes_to_unibyte_value(&[0xE1])],
        &mut env,
    )
    .expect("key-description should normalize raw unibyte meta bytes");
    assert_eq!(raw_byte, Value::String("M-a".into()));

    let list_event = call(
        &mut interp,
        "key-description",
        &[Value::list([
            Value::Symbol("vector-literal".into()),
            Value::list([Value::Symbol("control".into()), Value::Symbol("x".into())]),
            Value::list([Value::Symbol("control".into()), Value::Symbol("f".into())]),
        ])],
        &mut env,
    )
    .expect("key-description should normalize list-form control events");
    assert_eq!(list_event, Value::String("C-x C-f".into()));
}

#[test]
fn key_sequence_binding_parts_preserve_control_prefixes() {
    assert_eq!(
        key_sequence_binding_parts(&Value::String("\x03,\x17".into()))
            .expect("raw control-byte key sequence should parse"),
        vec!["C-c".to_string(), ",".to_string(), "C-w".to_string()]
    );
    assert_eq!(
        key_sequence_keymap_parts(&Value::String("\x03,\x17".into()))
            .expect("raw control-byte key sequence should retain every event"),
        vec!["C-c".to_string(), ",".to_string(), "C-w".to_string()]
    );
    assert_eq!(
        key_sequence_binding_parts(&Value::String("C-c g".into()))
            .expect("raw strings should remain raw key sequences"),
        vec!["C", "-", "c", "SPC", "g"]
    );
    assert_eq!(
        textual_key_sequence_binding_parts(&Value::String("C-c g".into()))
            .expect("textual control-prefixed key should parse"),
        vec!["C-c".to_string(), "g".to_string()]
    );
    assert_eq!(
        key_sequence_binding_parts(&Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Integer(3),
            Value::Integer('g' as i64),
        ]))
        .expect("vector control-prefixed key should parse"),
        vec!["C-c".to_string(), "g".to_string()]
    );
    assert_eq!(
        textual_key_sequence_keymap_parts(&Value::String("M-v".into()))
            .expect("Meta character key should normalize for keymap storage"),
        vec!["ESC".to_string(), "v".to_string()]
    );
    assert_eq!(
        textual_key_sequence_keymap_parts(&Value::String("M-<up>".into()))
            .expect("Meta function key should remain one symbolic event"),
        vec!["M-up".to_string()]
    );
}

#[test]
fn define_key_preserves_raw_space_events_in_shared_prefixes() {
    assert_upstream_primitive_contract(
        r#"(let ((map (make-sparse-keymap)))
              (define-key map "\C-c, " 'semantic-complete-analyze-inline)
              (define-key map "\C-c,\C-w" 'senator-kill-tag)
              (prin1 (list (lookup-key map "\C-c, ")
                           (lookup-key map "\C-c,\C-w"))))"#,
        "(semantic-complete-analyze-inline senator-kill-tag)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"(let ((map (make-sparse-keymap)))
              (define-key map "\C-c, " 'semantic-complete-analyze-inline)
              (define-key map "\C-c,\C-w" 'senator-kill-tag)
              (list (lookup-key map "\C-c, ")
                    (lookup-key map "\C-c,\C-w")))"#,
    )
    .read_all()
    .expect("shared-prefix keymap regression should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("raw-space and sibling control bindings should coexist");
    assert_eq!(
        result,
        Value::list([
            Value::Symbol("semantic-complete-analyze-inline".into()),
            Value::Symbol("senator-kill-tag".into()),
        ])
    );
}

#[test]
fn define_key_creates_a_local_prefix_over_an_inherited_non_prefix_binding() {
    assert_upstream_primitive_contract(
        r#"(let ((parent (make-sparse-keymap))
                 (map (make-keymap)))
              (define-key parent "s" 'inherited-command)
              (set-keymap-parent map parent)
              (suppress-keymap map t)
              (define-key map "s?" 'prefix-help)
              (define-key map "sc" 'prefix-command)
              (prin1 (list (lookup-key map "s?")
                           (lookup-key map "sc")
                           (lookup-key parent "s"))))"#,
        "(prefix-help prefix-command inherited-command)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"(let ((parent (make-sparse-keymap))
                 (map (make-keymap)))
              (define-key parent "s" 'inherited-command)
              (set-keymap-parent map parent)
              (suppress-keymap map t)
              (define-key map "s?" 'prefix-help)
              (define-key map "sc" 'prefix-command)
              (list (lookup-key map "s?")
                    (lookup-key map "sc")
                    (lookup-key parent "s")))"#,
    )
    .read_all()
    .expect("full-map nil-prefix regression should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("an inherited non-prefix binding should be shadowed locally");
    assert_eq!(
        result,
        Value::list([
            Value::Symbol("prefix-help".into()),
            Value::Symbol("prefix-command".into()),
            Value::Symbol("inherited-command".into()),
        ])
    );
}

#[test]
fn define_key_creates_a_specific_prefix_over_a_default_binding() {
    assert_upstream_primitive_contract(
        r#"(let ((map (make-sparse-keymap)))
              (define-key map [t] 'fallback-command)
              (define-key map [27 t] 'meta-fallback-command)
              (prin1 (list (lookup-key map [t])
                           (lookup-key map [27 t]))))"#,
        "(fallback-command meta-fallback-command)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"(let ((map (make-sparse-keymap)))
              (define-key map [t] 'fallback-command)
              (define-key map [27 t] 'meta-fallback-command)
              (list (lookup-key map [t])
                    (lookup-key map [27 t])))"#,
    )
    .read_all()
    .expect("default-binding prefix regression should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("a default binding should not block a specific prefix");
    assert_eq!(
        result,
        Value::list([
            Value::Symbol("fallback-command".into()),
            Value::Symbol("meta-fallback-command".into()),
        ])
    );
}

#[test]
fn keymap_set_where_is_internal_preserves_control_prefixes() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let keymap = make_runtime_keymap(&mut interp, Some("test-map"));
    call(
        &mut interp,
        "keymap-set",
        &[
            keymap.clone(),
            Value::String("C-c g".into()),
            Value::Symbol("keymap-tests-command".into()),
        ],
        &mut env,
    )
    .expect("keymap-set should accept control-prefixed textual specs");
    assert_eq!(
        where_is_internal(
            &mut interp,
            "keymap-tests-command",
            &[keymap],
            false,
            &mut env,
        )
        .expect("where-is-internal should find control-prefixed binding"),
        vec![Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Integer(3),
            Value::Integer('g' as i64),
        ])]
    );
}

#[test]
fn mapcar_iterates_runtime_keymaps_as_lisp_keymap_lists() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let keymap = make_runtime_keymap(&mut interp, Some("test-map"));
    call(
        &mut interp,
        "keymap-set",
        &[
            keymap.clone(),
            Value::String("C-c g".into()),
            Value::Symbol("keymap-tests-command".into()),
        ],
        &mut env,
    )
    .expect("keymap-set should populate the runtime keymap");

    let mapped = call(
        &mut interp,
        "mapcar",
        &[Value::Symbol("identity".into()), keymap],
        &mut env,
    )
    .expect("mapcar should see the Lisp keymap list representation");

    let items = mapped.to_vec().expect("mapcar returns a list");
    assert_eq!(items.first(), Some(&Value::Symbol("keymap".into())));
    assert!(
        items
            .iter()
            .any(|item| item.to_string().contains("keymap-tests-command")),
        "mapped keymap items should include runtime bindings: {items:?}"
    );
}

#[test]
fn case_tables_apply_explicit_byte8_mappings_to_raw_unibyte_strings() {
    let mut interp = Interpreter::new();
    interp.set_load_path(vec![upstream_emacs_repo().join("lisp")]);
    interp.load_target("case-table").expect("load case-table");
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (let* ((tab (copy-case-table (standard-case-table)))
                   (byte8 #x3FFF00)
                   (input (decode-coding-string "\xff\xff\xef Foo baR \xcf\xcf" 'binary)))
              (set-case-table tab)
              (set-case-syntax-pair (+ byte8 #xef) (+ byte8 #xff) tab)
              (list (upcase input)
                    (downcase input)
                    (capitalize input)
                    (upcase-initials input)))
            "#,
    )
    .read_all()
    .expect("case-table byte8 test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("case-table byte8 forms should evaluate");
    let actual = result.to_vec().expect("result list");
    let expected = [
        bytes_to_unibyte_value(b"\xef\xef\xef FOO BAR \xcf\xcf"),
        bytes_to_unibyte_value(b"\xff\xff\xff foo bar \xcf\xcf"),
        bytes_to_unibyte_value(b"\xef\xff\xff Foo Bar \xcf\xcf"),
        bytes_to_unibyte_value(b"\xef\xff\xef Foo BaR \xcf\xcf"),
    ];

    for (actual, expected) in actual.iter().zip(expected.iter()) {
        let actual_string = string_like(actual).expect("actual string");
        let expected_string = string_like(expected).expect("expected string");
        assert_eq!(actual_string.text, expected_string.text);
        assert!(!actual_string.multibyte);
    }
}

#[test]
fn capitalize_uses_current_syntax_table_word_boundaries() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (with-temp-buffer
              (fundamental-mode)
              (list (char-syntax ?%)
                    (capitalize "padding (%d)")
                    (let ((case-symbols-as-words nil))
                      (capitalize "FOO-BAR"))
                    (let ((case-symbols-as-words t))
                      (capitalize "FOO-BAR"))
                    (progn
                      (modify-syntax-entry ?% ".")
                      (capitalize "padding (%d)"))
                    (progn
                      (modify-syntax-entry ?A ".")
                      (capitalize "xA XAX"))))
            "#,
    )
    .read_all()
    .expect("syntax-aware capitalize test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("syntax-aware capitalize forms should evaluate");

    assert_eq!(
        result.to_string(),
        r#"(119 "Padding (%d)" "Foo-Bar" "Foo-bar" "Padding (%D)" "Xa XaX")"#
    );
}

#[test]
fn case_tables_apply_explicit_byte8_mappings_to_raw_unibyte_regions() {
    let mut interp = Interpreter::new();
    interp.set_load_path(vec![upstream_emacs_repo().join("lisp")]);
    interp.load_target("case-table").expect("load case-table");
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (let* ((tab (copy-case-table (standard-case-table)))
                   (byte8 #x3FFF00)
                   (input (decode-coding-string "\xff\xff\xef Foo baR \xcf\xcf" 'binary)))
              (with-temp-buffer
                (set-case-table tab)
                (set-case-syntax-pair (+ byte8 #xef) (+ byte8 #xff) tab)
                (toggle-enable-multibyte-characters)
                (insert input)
                (upcase-region (point-min) (point-max))
                (list (buffer-string) (multibyte-string-p (buffer-string)))))
            "#,
    )
    .read_all()
    .expect("case-table byte8 region test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("case-table byte8 region forms should evaluate");
    let items = result.to_vec().expect("region result list");
    let actual = string_like(&items[0]).expect("region result string");
    let expected = string_like(&bytes_to_unibyte_value(b"\xef\xef\xef FOO BAR \xcf\xcf"))
        .expect("expected string");

    assert_eq!(actual.text, expected.text);
    assert!(!actual.multibyte);
    assert_eq!(items[1], Value::Nil);
}

#[test]
fn single_key_description_matches_symbol_cases() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let home = call(
        &mut interp,
        "single-key-description",
        &[Value::Symbol("home".into())],
        &mut env,
    )
    .expect("single-key-description should wrap event symbols");
    assert_eq!(home, Value::String("<home>".into()));

    let plain = call(
        &mut interp,
        "single-key-description",
        &[Value::Symbol("home".into()), Value::T],
        &mut env,
    )
    .expect("single-key-description should honor no-angles");
    assert_eq!(plain, Value::String("home".into()));
}

#[test]
fn keymap_bindings_accept_t_vector_events() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        "(let ((map (make-sparse-keymap \"demo\")))
               (define-key map [t] 'fallback-command)
               (eq (lookup-key map [t]) 'fallback-command))",
    )
    .read_all()
    .expect("keymap test form should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("[t] key events should be accepted in keymaps");
    assert_eq!(result, Value::T);
}

#[test]
fn map_keymap_visits_runtime_keymap_bindings() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (let ((map (make-sparse-keymap))
                  seen)
              (define-key map "x" 'sample-command)
              (map-keymap (lambda (key value)
                            (setq seen (cons (cons key value) seen)))
                          map)
              seen)"#,
    )
    .read_all()
    .expect("map-keymap test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("map-keymap should visit bindings");
    assert_eq!(
        result,
        Value::list([Value::cons(
            Value::Integer(i64::from(b'x')),
            Value::Symbol("sample-command".into()),
        )])
    );
}

#[test]
fn keymaps_nest_multi_event_bindings_and_report_full_map_ranges() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (let ((map (make-keymap))
                  seen
                  public-seen)
              (define-key map [remap foo] 'bar)
              (define-key map (kbd "M-v") 'meta-command)
              (dolist (key '("1" "2" "3" "4"))
                (define-key map (kbd key) 'range-command))
              (define-key map [(65 . 67)] 'public-range-command)
              (map-keymap (lambda (key value)
                            (setq seen (cons (cons key value) seen)))
                          map)
              (let ((public (list 'keymap (cadr map))))
                (map-keymap (lambda (key value)
                              (setq public-seen
                                    (cons (cons key value) public-seen)))
                            public)
                (list (lookup-key map [remap foo] t)
                      (length (accessible-keymaps map))
                      (keymapp (lookup-key map (kbd "ESC") t))
                      (lookup-key map (kbd "M-v") t)
                      (not (null (member '((49 . 52) . range-command) seen)))
                      (lookup-key public "B" t)
                      (not (null
                                 (member '((65 . 67) . public-range-command)
                                         public-seen))))))"#,
    )
    .read_all()
    .expect("keymap range test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("keymap prefixes and ranges should be observable");
    assert_eq!(
        result,
        Value::list([
            Value::Symbol("bar".into()),
            Value::Integer(3),
            Value::T,
            Value::Symbol("meta-command".into()),
            Value::T,
            Value::Symbol("public-range-command".into()),
            Value::T,
        ])
    );
}

#[test]
fn keymap_walkers_follow_prefix_command_symbols_and_run_leaf_menu_filters() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (setq keymap-tests-filter-buffer nil)
              (let ((root (make-sparse-keymap))
                    (prefix (make-sparse-keymap)))
                (fset 'keymap-tests-prefix prefix)
                (define-key root (kbd "a") 'keymap-tests-prefix)
                (define-key prefix (kbd "b")
                  `(menu-item "Identity" identity
                              :filter ,(lambda (command)
                                         (setq keymap-tests-filter-buffer
                                               (current-buffer))
                                         command)))
                (with-temp-buffer
                  (let ((here (current-buffer))
                        (lookup (lookup-key root (kbd "a b") t))
                        (accessible (length (accessible-keymaps root))))
                    (setq keymap-tests-filter-buffer nil)
                    (list lookup
                          accessible
                          (key-description
                           (where-is-internal #'identity root t))
                          (eq keymap-tests-filter-buffer here))))))"#,
    )
    .read_all()
    .expect("prefix-command keymap test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("all keymap walkers should traverse prefix-command symbols");
    assert_eq!(
        result,
        Value::list([
            Value::Symbol("identity".into()),
            Value::Integer(2),
            Value::String("a b".into()),
            Value::T,
        ])
    );
}

#[test]
fn completion_predicates_preserve_string_list_membership() {
    assert_upstream_primitive_contract(
        r#"(let* ((abcdef '("abc" "def"))
                  (pred (lambda (elt) (memq elt abcdef))))
             (prin1
              (list (try-completion "a" abcdef pred)
                    (all-completions "a" abcdef pred)
                    (test-completion "abc" abcdef pred))))"#,
        "(\"abc\" (\"abc\") (\"abc\" \"def\"))",
    );
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let mut interp = Interpreter::new();
            let mut env = Vec::new();
            let forms = Reader::new(
                "(let* ((abcdef '(\"abc\" \"def\"))
                            (pred (lambda (elt) (memq elt abcdef))))
                       (list (try-completion \"a\" abcdef pred)
                             (all-completions \"a\" abcdef pred)
                             (test-completion \"abc\" abcdef pred)))",
            )
            .read_all()
            .expect("completion test form should parse");
            let result = forms
                .iter()
                .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
                .expect("string-list completion predicates should keep matching members");
            let expected = Value::list([
                Value::String("abc".into()),
                Value::list([Value::String("abc".into())]),
                Value::list([Value::String("abc".into()), Value::String("def".into())]),
            ]);
            assert!(
                values_equal(&interp, &result, &expected),
                "expected {expected}, got {result}"
            );
        })
        .expect("spawn large-stack test thread")
        .join()
        .expect("join large-stack test thread");
}

#[test]
fn shell_quote_argument_matches_upstream_batch_cases() {
    assert_eq!(shell_quote_argument("*.pl"), r"\*.pl");
    assert_eq!(shell_quote_argument("nfs"), "nfs");
    assert_eq!(
        shell_quote_argument("/Users/example/projects/emaxx/"),
        "/Users/example/projects/emaxx/"
    );
    assert_eq!(shell_quote_argument("foo bar"), r"foo\ bar");
    assert_eq!(shell_quote_argument(""), "''");
}

#[test]
fn split_string_supports_regexp_separators() {
    let interp = Interpreter::new();
    let env = Vec::new();
    assert_eq!(
        regexp::split_string_impl(
            &interp,
            &Value::String("-k basename".into()),
            Some(&Value::String("\\s-+".into())),
            None,
            &env,
        )
        .expect("split-string should accept regexp separators"),
        Value::list([Value::String("-k".into()), Value::String("basename".into()),])
    );
}

#[test]
fn split_string_supports_multibyte_regexp_separators() {
    let interp = Interpreter::new();
    let env = Vec::new();
    assert_eq!(
        regexp::split_string_impl(
            &interp,
            &Value::String("2¦4¦bb*¦abbbc¦".into()),
            Some(&Value::String("¦".into())),
            None,
            &env,
        )
        .expect("split-string should accept multibyte regexp separators"),
        Value::list([
            Value::String("2".into()),
            Value::String("4".into()),
            Value::String("bb*".into()),
            Value::String("abbbc".into()),
            Value::String("".into()),
        ])
    );
}

#[test]
fn completion_results_accept_text_properties() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"(let* ((matches (all-completions "foo" '("foobar") nil))
                  (candidate (car matches)))
             (set-text-properties 0 1 '(face completion-preview) candidate)
             (get-text-property 0 'face candidate))"#,
    )
    .read_all()
    .expect("completion property test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("completion strings should accept text properties");
    assert_eq!(result, Value::Symbol("completion-preview".into()));
}

#[test]
fn substring_of_completion_result_accepts_text_properties() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"(let* ((matches (all-completions "foo" '("foobar") nil))
                  (candidate (car matches))
                  (suffix (substring candidate 3)))
             (set-text-properties 0 1 '(face completion-preview) suffix)
             (get-text-property 0 'face suffix))"#,
    )
    .read_all()
    .expect("completion substring property test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("completion substring should accept text properties");
    assert_eq!(result, Value::Symbol("completion-preview".into()));
}

#[test]
fn native_process_callbacks_types_and_coding_flags_share_one_gnu_state_model() {
    assert_upstream_primitive_contract(
        r#"(with-temp-buffer
             (insert "head")
             (let* ((p (make-pipe-process :name "audit" :buffer (current-buffer)))
                    (m (copy-marker (point-max))))
               (goto-char (point-min))
               (let ((surface
                      (list
                       (process-filter p)
                       (process-sentinel p)
                       (set-process-filter p nil)
                       (process-filter p)
                       (set-process-sentinel p nil)
                       (process-sentinel p)
                       (process-type p)
                       (process-type (process-name p))
                       (process-type (current-buffer))
                       (process-inherit-coding-system-flag p)
                       (set-process-inherit-coding-system-flag p 'yes)
                       (process-inherit-coding-system-flag p)
                       (set-process-coding-system p nil nil)
                       (set-process-window-size p 24 80))))
                 (internal-default-process-filter p "out")
                 (let ((after-filter
                        (list (buffer-string) (point)
                              (marker-position m)
                              (marker-position (process-mark p)))))
                   (set-process-sentinel p 'ignore)
                   (delete-process p)
                   (internal-default-process-sentinel p "finished\n")
                   (prin1
                    (list surface after-filter
                          (buffer-string) (point)
                          (marker-position m)
                          (marker-position (process-mark p))))))))"#,
        "((internal-default-process-filter internal-default-process-sentinel internal-default-process-filter internal-default-process-filter internal-default-process-sentinel internal-default-process-sentinel pipe pipe pipe nil yes t nil nil) (\"headout\" 1 8 8) \"headout\nProcess audit finished\n\" 1 8 32)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, _) = interp.create_buffer(" *native-process-audit*");
    interp
        .switch_to_buffer_id(buffer_id)
        .expect("switch to process audit buffer");
    interp.insert_current_buffer("head");
    let buffer = interp
        .buffer_identity_value(buffer_id)
        .expect("process audit buffer identity");
    let process = call(
        &mut interp,
        "make-pipe-process",
        &[
            Value::symbol(":name"),
            Value::String("audit".into()),
            Value::symbol(":buffer"),
            buffer.clone(),
        ],
        &mut env,
    )
    .expect("create pipe process");

    let mut surface = Vec::new();
    for (function, arguments) in [
        ("process-filter", vec![process.clone()]),
        ("process-sentinel", vec![process.clone()]),
        ("set-process-filter", vec![process.clone(), Value::Nil]),
        ("process-filter", vec![process.clone()]),
        ("set-process-sentinel", vec![process.clone(), Value::Nil]),
        ("process-sentinel", vec![process.clone()]),
        ("process-type", vec![process.clone()]),
        ("process-type", vec![Value::String("audit".into())]),
        ("process-type", vec![buffer]),
        ("process-inherit-coding-system-flag", vec![process.clone()]),
        (
            "set-process-inherit-coding-system-flag",
            vec![process.clone(), Value::symbol("yes")],
        ),
        ("process-inherit-coding-system-flag", vec![process.clone()]),
        (
            "set-process-coding-system",
            vec![process.clone(), Value::Nil, Value::Nil],
        ),
        (
            "set-process-window-size",
            vec![process.clone(), Value::Integer(24), Value::Integer(80)],
        ),
    ] {
        surface.push(
            call(&mut interp, function, &arguments, &mut env)
                .unwrap_or_else(|error| panic!("{function}: {error}")),
        );
    }
    assert_eq!(
        Value::list(surface),
        Value::list([
            Value::symbol("internal-default-process-filter"),
            Value::symbol("internal-default-process-sentinel"),
            Value::symbol("internal-default-process-filter"),
            Value::symbol("internal-default-process-filter"),
            Value::symbol("internal-default-process-sentinel"),
            Value::symbol("internal-default-process-sentinel"),
            Value::symbol("pipe"),
            Value::symbol("pipe"),
            Value::symbol("pipe"),
            Value::Nil,
            Value::symbol("yes"),
            Value::T,
            Value::Nil,
            Value::Nil,
        ])
    );

    let marker = interp
        .copy_marker_value(&Value::Integer(interp.buffer.point_max() as i64), false)
        .expect("copy marker at process output boundary");
    interp.buffer.goto_char(interp.buffer.point_min());
    call(
        &mut interp,
        "internal-default-process-filter",
        &[process.clone(), Value::String("out".into())],
        &mut env,
    )
    .expect("run native default process filter");
    let Value::Marker(marker_id) = marker else {
        unreachable!("copy-marker returns a marker")
    };
    let process_id = interp
        .resolve_process_id(&process)
        .expect("pipe process id");
    let process_mark = interp.process_mark_id(process_id).expect("process mark");
    assert_eq!(interp.buffer.buffer_string(), "headout");
    assert_eq!(interp.buffer.point(), 1);
    assert_eq!(interp.marker_position(marker_id), Some(8));
    assert_eq!(interp.marker_position(process_mark), Some(8));

    call(
        &mut interp,
        "set-process-sentinel",
        &[process.clone(), Value::symbol("ignore")],
        &mut env,
    )
    .expect("suppress automatic delete message");
    call(
        &mut interp,
        "delete-process",
        std::slice::from_ref(&process),
        &mut env,
    )
    .expect("delete pipe process");
    call(
        &mut interp,
        "internal-default-process-sentinel",
        &[process, Value::String("finished\n".into())],
        &mut env,
    )
    .expect("run native default process sentinel");
    assert_eq!(
        interp.buffer.buffer_string(),
        "headout\nProcess audit finished\n"
    );
    assert_eq!(interp.buffer.point(), 1);
    assert_eq!(interp.marker_position(marker_id), Some(8));
    assert_eq!(interp.marker_position(process_mark), Some(32));
}

#[cfg(unix)]
#[test]
fn native_connection_control_and_pid_signals_follow_gnu_process_c() {
    let program = r#"(let ((p (make-pipe-process :name "control" :sentinel 'ignore)))
                       (unwind-protect
                           (list
                            (eq (stop-process p) p)
                            (process-status p)
                            (process-command p)
                            (eq (continue-process p) p)
                            (process-status p)
                            (process-command p)
                            (internal-default-signal-process "not-a-pid" 'TERM)
                            (signal-process (emacs-pid) 0))
                         (when (process-live-p p)
                           (delete-process p))))"#;
    let expected = "(t stop t t open nil nil 0)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("native process control contract should parse")
        .expect("native process control contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("native process control contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("native process control result should parse")
            .expect("native process control result should exist")
    );
}

#[cfg(target_os = "macos")]
#[test]
fn native_signal_names_share_the_platform_codec_used_by_signal_process() {
    let program = r#"(let ((names (signal-names)))
                       (list
                        names
                        (catch 'invalid
                          (dolist (name names t)
                            (unless
                                (= -1
                                   (internal-default-signal-process
                                    2147483647 (intern name)))
                              (throw 'invalid nil))))))"#;
    let expected = r#"(("USR2" "USR1" "INFO" "WINCH" "PROF" "VTALRM" "XFSZ" "XCPU" "IO" "TTOU" "TTIN" "CHLD" "CONT" "TSTP" "STOP" "URG" "TERM" "ALRM" "PIPE" "SYS" "SEGV" "BUS" "KILL" "FPE" "EMT" "ABRT" "TRAP" "ILL" "QUIT" "INT" "HUP" "EXIT") t)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("native signal-name contract should parse")
        .expect("native signal-name contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("native signal-name contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("native signal-name result should parse")
            .expect("native signal-name result should exist")
    );
}

#[cfg(unix)]
#[test]
fn process_filters_can_observe_that_read_event_is_waiting_for_user_input() {
    let program = r#"(progn
                       (setq emaxx-test-wait-seen nil
                             emaxx-test-wait-received nil)
                       (let*
                         ((p
                           (make-process
                            :name "wait-input"
                            :command '("/bin/cat")
                            :connection-type 'pipe
                            :filter
                            (lambda (_process text)
                              (setq
                               emaxx-test-wait-received text
                               emaxx-test-wait-seen
                               (waiting-for-user-input-p)))
                            :sentinel 'ignore)))
                         (unwind-protect
                             (progn
                               (process-send-string p "x")
                               (list
                               (read-event nil nil 10)
                                emaxx-test-wait-seen
                                emaxx-test-wait-received
                                (waiting-for-user-input-p)))
                           (when (process-live-p p)
                             (delete-process p)))))"#;
    let expected = "(nil t \"x\" nil)";
    // With null stdin GNU's read-event returns before a delayed filter can
    // run, so the subprocess oracle cannot reliably exercise this timing
    // state.  Its outside-wait baseline remains exact; the t-during-filter
    // assertion below covers process.c's documented read_kbd contract.
    assert_upstream_primitive_contract("(prin1 (waiting-for-user-input-p))", "nil");

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("read-event waiting contract should parse")
        .expect("read-event waiting contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("read-event waiting contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("read-event waiting result should parse")
            .expect("read-event waiting result should exist")
    );
}

#[test]
fn native_process_thread_ownership_matches_gnu_descriptor_locking() {
    let program = r#"(let*
                         ((p (make-pipe-process
                              :name "locked" :sentinel 'ignore))
                          (owner
                           (make-thread
                            (lambda () (sleep-for .01))
                            "owner")))
                       (unwind-protect
                           (list
                            (eq (process-thread p) (current-thread))
                            (set-process-thread p nil)
                            (process-thread p)
                            (eq
                             (set-process-thread p (current-thread))
                             (current-thread))
                            (eq (process-thread p) (current-thread))
                            (eq (set-process-thread p owner) owner)
                            (condition-case error
                                (accept-process-output p 0)
                              (error (cadr error)))
                            (progn
                              (thread-join owner)
                              (thread-live-p owner))
                            (process-thread p))
                         (set-process-thread p nil)
                         (delete-process p)))"#;
    let expected = r#"(t nil nil t t t "Attempt to accept output from process locked locked to thread owner" nil nil)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("process thread ownership contract should parse")
        .expect("process thread ownership contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("process thread ownership contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("process thread ownership result should parse")
            .expect("process thread ownership result should exist")
    );
}

#[test]
fn native_network_lookup_uses_platform_address_vectors_and_gnu_validation() {
    let program = r#"(list
                       (network-lookup-address-info
                        "127.0.0.1" nil 'numeric)
                       (network-lookup-address-info
                        "127.0.0.1" 'ipv4 'numeric)
                       (network-lookup-address-info
                        "::1" 'ipv6 'numeric)
                       (network-lookup-address-info
                        "127.0.0.1" 'ipv6 'numeric)
                       (condition-case error
                           (network-lookup-address-info "x" 'bogus)
                         (error (cdr error)))
                       (condition-case error
                           (network-lookup-address-info "x" nil 'bogus)
                         (error (cdr error))))"#;
    let expected = r#"(([127 0 0 1 0]) ([127 0 0 1 0]) ([0 0 0 0 0 0 0 1 0]) nil ("Unsupported family") ("Unsupported hints value"))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("network lookup contract should parse")
        .expect("network lookup contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("network lookup contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("network lookup result should parse")
            .expect("network lookup result should exist")
    );
}

#[test]
fn native_network_interface_list_reports_the_ipv4_loopback_subnet() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let interfaces = call(
        &mut interp,
        "network-interface-list",
        &[Value::T, Value::symbol("ipv4")],
        &mut env,
    )
    .expect("enumerate IPv4 interfaces")
    .to_vec()
    .expect("interface result should be a list");
    let loopback = Reader::new("[127 0 0 1 0]")
        .read()
        .expect("loopback vector should parse")
        .expect("loopback vector should exist");
    let entry = interfaces
        .iter()
        .filter_map(|entry| entry.to_vec().ok())
        .find(|entry| entry.get(1) == Some(&loopback))
        .expect("IPv4 loopback interface should be present");
    let broadcast = Reader::new("[127 255 255 255 0]")
        .read()
        .expect("loopback broadcast vector should parse")
        .expect("loopback broadcast vector should exist");
    let mask = Reader::new("[255 0 0 0 0]")
        .read()
        .expect("loopback mask vector should parse")
        .expect("loopback mask vector should exist");
    assert_eq!(entry[2], broadcast);
    assert_eq!(entry[3], mask);
}

#[test]
fn native_serial_process_validation_matches_gnu_process_c() {
    let program = r#"(list
                       (make-serial-process)
                       (condition-case error
                           (make-serial-process :speed 9600)
                         (error (cdr error)))
                       (condition-case error
                           (make-serial-process :port "/does/not/exist")
                         (error (cdr error)))
                       (let ((pipe (make-pipe-process :name "not-serial")))
                         (unwind-protect
                             (condition-case error
                                 (serial-process-configure
                                  :process pipe :speed 9600)
                               (error (cdr error)))
                           (delete-process pipe))))"#;
    let expected =
        r#"(nil ("No port specified") (":speed not specified") ("Not a serial process"))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("serial validation contract should parse")
        .expect("serial validation contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("serial validation contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("serial validation result should parse")
            .expect("serial validation result should exist")
    );
}

#[test]
fn native_minibuffer_stack_queries_match_gnu_minibuf_c() {
    let inactive = r#"(let ((buffer (window-buffer (minibuffer-window))))
                        (list
                         (buffer-name buffer)
                         (minibufferp buffer)
                         (minibufferp buffer t)
                         (with-current-buffer buffer
                           (list
                            (minibuffer-depth)
                            (minibuffer-prompt)
                            (minibuffer-prompt-end)
                            (innermost-minibuffer-p)
                            (minibuffer-innermost-command-loop-p)))
                         (innermost-minibuffer-p " *Minibuf-0*")
                         (condition-case error
                             (abort-minibuffers)
                           (error (cdr error)))))"#;
    let inactive_expected = r#"(" *Minibuf-0*" t nil (0 nil 1 t nil) nil ("Not in a minibuffer"))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {inactive})"), inactive_expected);

    let active = r#"(catch 'state
                      (let ((minibuffer-setup-hook
                             (list
                              (lambda ()
                                (throw
                                 'state
                                 (list
                                  (minibuffer-depth)
                                  (minibuffer-prompt)
                                  (innermost-minibuffer-p)
                                  (minibuffer-innermost-command-loop-p)
                                  (minibuffer-prompt-end)
                                  (point)
                                  (minibuffer-contents)
                                  (windowp (active-minibuffer-window))
                                  (minibufferp nil t)
                                  (eq (current-local-map)
                                      minibuffer-local-completion-map)
                                  (equal minibuffer-completion-table '("a"))))))))
                        (let ((executing-kbd-macro t))
                          (completing-read "Prompt: " '("a")))))"#;
    let active_expected = r#"(1 "Prompt: " t t 9 9 "" t t t t)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {active})"), active_expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    for (program, expected) in [(inactive, inactive_expected), (active, active_expected)] {
        let form = Reader::new(program)
            .read()
            .expect("minibuffer stack contract should parse")
            .expect("minibuffer stack contract should contain a form");
        let expected = Reader::new(expected)
            .read()
            .expect("minibuffer stack result should parse")
            .expect("minibuffer stack result should exist");
        assert_eq!(
            interp
                .eval(&form, &mut env)
                .expect("minibuffer stack contract should evaluate"),
            expected
        );
    }
}

#[test]
fn native_condition_wait_releases_and_restores_recursive_mutex_ownership() {
    let validation = r#"(let* ((mutex (make-mutex "m"))
                               (condition
                                (make-condition-variable mutex "c")))
                          (list
                           (condition-case error
                               (condition-wait condition)
                             (error (cdr error)))
                           (condition-case error
                               (condition-notify condition)
                             (error (cdr error)))))"#;
    let validation_expected = "((\"Condition variable’s mutex is not held by current thread\") (\"Condition variable’s mutex is not held by current thread\"))";
    assert_upstream_primitive_contract(&format!("(prin1 {validation})"), validation_expected);

    let synchronization = r#"(let* ((mutex (make-mutex "m"))
                                    (condition
                                     (make-condition-variable mutex "c"))
                                    (flag nil)
                                    (notifier
                                     (make-thread
                                      (lambda ()
                                        (with-mutex mutex
                                          (setq flag 'notified)
                                          (condition-notify condition))))))
                               (mutex-lock mutex)
                               (mutex-lock mutex)
                               (let ((wait-result (condition-wait condition))
                                     (owned-at-depth-two
                                      (condition-notify condition)))
                                 (mutex-unlock mutex)
                                 (let ((owned-at-depth-one
                                        (condition-notify condition)))
                                   (mutex-unlock mutex)
                                   (thread-join notifier)
                                   (list
                                    (null wait-result)
                                    (null owned-at-depth-two)
                                    (null owned-at-depth-one)
                                    (equal
                                     (condition-case error
                                         (condition-notify condition)
                                       (error (cdr error)))
                                     '("Condition variable’s mutex is not held by current thread"))))))"#;
    let synchronization_expected = "(t t t t)";
    assert_upstream_primitive_contract(
        &format!("(prin1 {synchronization})"),
        synchronization_expected,
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    for (program, expected) in [
        (validation, validation_expected),
        (synchronization, synchronization_expected),
    ] {
        let form = Reader::new(program)
            .read()
            .expect("condition-variable contract should parse")
            .expect("condition-variable contract should contain a form");
        let expected = Reader::new(expected)
            .read()
            .expect("condition-variable result should parse")
            .expect("condition-variable result should exist");
        assert_eq!(
            interp
                .eval(&form, &mut env)
                .expect("condition-variable contract should evaluate"),
            expected
        );
    }
}

#[test]
fn native_combined_after_change_merges_ranges_before_running_hooks() {
    let program = r#"(progn
                       (defun emaxx-test-after-change
                           (begin end old-length)
                         (push
                          (list
                           begin end old-length (buffer-string))
                          emaxx-test-after-change-events))
                       (with-temp-buffer
                         (setq emaxx-test-after-change-events nil)
                         (add-hook
                          'after-change-functions
                          'emaxx-test-after-change nil t)
                         (let
                             ((before
                               (let ((combine-after-change-calls t))
                                 (insert "ab")
                                 (goto-char 2)
                                 (delete-char 1)
                                 (insert "XYZ")
                                 emaxx-test-after-change-events)))
                           (list
                            before
                            (combine-after-change-execute)
                            emaxx-test-after-change-events
                            (buffer-string)))))"#;
    let expected = r#"(nil nil ((1 5 0 "aXYZ")) "aXYZ")"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("combined after-change contract should parse")
        .expect("combined after-change contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("combined after-change contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("combined after-change result should parse")
            .expect("combined after-change result should exist")
    );
}

#[test]
fn combine_change_calls_coalesces_hooks_and_tracks_the_updated_end() {
    let program = r#"(progn
                       (defun emaxx-test-combine-before (begin end)
                         (push (list 'before begin end (buffer-string))
                               emaxx-test-combine-events))
                       (defun emaxx-test-combine-after (begin end old-length)
                         (push (list 'after begin end old-length (buffer-string))
                               emaxx-test-combine-events))
                       (with-temp-buffer
                         (setq emaxx-test-combine-events nil)
                         (add-hook 'before-change-functions
                                   #'emaxx-test-combine-before nil t)
                         (add-hook 'after-change-functions
                                   #'emaxx-test-combine-after nil t)
                         (let ((result
                                (combine-change-calls (point-min) (point-max)
                                  (insert "a")
                                  (combine-change-calls (point) (point)
                                    (insert "b"))
                                  'body-result)))
                           (list result
                                 (nreverse emaxx-test-combine-events)
                                 (buffer-string)))))"#;
    let expected = r#"(body-result ((before 1 1 "") (after 1 3 0 "ab")) "ab")"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("combine-change-calls contract should parse")
        .expect("combine-change-calls contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("combine-change-calls contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("combine-change-calls result should parse")
            .expect("combine-change-calls result should exist")
    );
}

#[test]
fn native_keyboard_macro_family_matches_gnu_recording_and_execution_contracts() {
    let contracts = [
        (
            r#"(let ((last-kbd-macro [old]))
                 (list
                  (store-kbd-macro-event 'ignored)
                  (cancel-kbd-macro-events)
                  (start-kbd-macro t t)
                  defining-kbd-macro
                  (store-kbd-macro-event 'pending)
                  (cancel-kbd-macro-events)
                  (end-kbd-macro)
                  last-kbd-macro))"#,
            "(nil nil nil t nil nil nil [old])",
        ),
        (
            r#"(progn
                 (defalias 'emaxx-macro-alias [97])
                 (let ((iterations 0)
                       (terminations 0))
                   (add-hook
                    'kbd-macro-termination-hook
                    (lambda ()
                      (setq terminations (1+ terminations))))
                   (with-temp-buffer
                     (execute-kbd-macro
                      'emaxx-macro-alias 3
                      (lambda ()
                        (setq iterations (1+ iterations))
                        (< iterations 3)))
                     (list
                      (buffer-string)
                      iterations
                      terminations
                      executing-kbd-macro
                      executing-kbd-macro-index))))"#,
            r#"("aa" 3 1 nil 0)"#,
        ),
        (
            r#"(list
                (condition-case error-data
                    (let ((last-kbd-macro nil))
                      (call-last-kbd-macro)
                      'missed)
                  (error (car error-data)))
                (condition-case error-data
                    (let ((last-kbd-macro [])
                          (defining-kbd-macro t))
                      (call-last-kbd-macro)
                      'missed)
                  (error (car error-data)))
                (let ((last-kbd-macro [97]))
                  (with-temp-buffer
                    (call-last-kbd-macro 2)
                    (buffer-string))))"#,
            r#"(error error "aa")"#,
        ),
        (
            r#"(let ((last-kbd-macro [old]))
                 (start-kbd-macro t t)
                 (let
                     ((error-kind
                       (condition-case error-data
                           (end-kbd-macro "bad")
                         (error (car error-data)))))
                   (prog1
                       (list
                        error-kind
                        defining-kbd-macro
                        last-kbd-macro)
                     (cancel-kbd-macro-events)
                     (end-kbd-macro))))"#,
            "(wrong-type-argument t [old])",
        ),
        (
            r#"(progn
                 (defun emaxx-test-store-command ()
                   (interactive)
                   (store-kbd-macro-event 'kept))
                 (global-set-key [f5] 'emaxx-test-store-command)
                 (let ((last-kbd-macro [old]))
                   (start-kbd-macro t t)
                   (execute-kbd-macro [f5])
                   (store-kbd-macro-event 'pending)
                   (cancel-kbd-macro-events)
                   (end-kbd-macro)
                   last-kbd-macro))"#,
            "[old kept]",
        ),
    ];

    for (program, expected) in contracts {
        assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let form = Reader::new(program)
            .read()
            .expect("keyboard-macro contract should parse")
            .expect("keyboard-macro contract should contain a form");
        assert_eq!(
            interp
                .eval(&form, &mut env)
                .expect("keyboard-macro contract should evaluate"),
            Reader::new(expected)
                .read()
                .expect("keyboard-macro expected value should parse")
                .expect("keyboard-macro expected value should exist"),
            "keyboard-macro contract diverged: {program}"
        );
    }
}

#[test]
fn native_keyboard_input_family_matches_gnu_kboard_contracts() {
    let contracts = [
        (
            r#"(list
                (event-convert-list '(control a))
                (event-convert-list '(shift a))
                (event-convert-list '(meta control x))
                (event-convert-list '(hyper f5))
                (event-convert-list '(double down mouse-1))
                (condition-case error-data
                    (event-convert-list '(a b))
                  (error (car error-data))))"#,
            "(1 65 134217752 H-f5 double-down-mouse-1 error)",
        ),
        (
            r#"(list
                (internal-event-symbol-parse-modifiers 'M-C-S-f5)
                (internal-event-symbol-parse-modifiers
                 'double-down-mouse-1)
                (internal-event-symbol-parse-modifiers 'mouse-1))"#,
            "((f5 meta control shift) (mouse-1 double down) (mouse-1 click))",
        ),
        (
            r#"(let ((track-mouse 'outside)
                     seen)
                 (list
                  (internal--track-mouse
                   (lambda ()
                     (setq seen track-mouse)
                     42))
                  seen
                  track-mouse
                  (condition-case error-data
                      (internal--track-mouse
                       (lambda () (error "boom")))
                    (error (car error-data)))
                  track-mouse))"#,
            "(42 t outside error outside)",
        ),
        (
            r#"(list
                (internal-handle-focus-in
                 (list 'focus-in (selected-frame)))
                (condition-case error-data
                    (internal-handle-focus-in '(focus-in nope))
                  (error (car error-data)))
                (condition-case error-data
                    (suspend-emacs 1)
                  (error (car error-data))))"#,
            "(nil error wrong-type-argument)",
        ),
        (
            r#"(progn
                 (set--this-command-keys "ab")
                 (let
                     ((before
                       (list
                        (this-command-keys)
                        (this-command-keys-vector)
                        (this-single-command-keys))))
                   (clear-this-command-keys t)
                   (list
                    before
                    (this-command-keys)
                    (this-command-keys-vector))))"#,
            r#"(("ab" [97 98] [97 98]) "" [])"#,
        ),
        (
            r#"(let ((unread-command-events '(97)))
                 (list
                  (read-key-sequence nil)
                  (this-command-keys)
                  (this-command-keys-vector)
                  (this-single-command-keys)
                  (this-single-command-raw-keys)
                  (recent-keys)))"#,
            r#"("a" "a" [97] [97] [97] [97])"#,
        ),
        (
            r#"(let ((unread-command-events '(f5)))
                 (list
                  (read-key-sequence-vector nil)
                  (this-command-keys)
                  (this-command-keys-vector)
                  (this-single-command-raw-keys)
                  (recent-keys)))"#,
            "([f5] [f5] [f5] [f5] [f5])",
        ),
        (
            r#"(let ((last-kbd-macro [old])
                     (unread-command-events '(97)))
                 (start-kbd-macro t t)
                 (store-kbd-macro-event 'pending)
                 (discard-input)
                 (list
                  last-kbd-macro
                  defining-kbd-macro
                  unread-command-events))"#,
            "([old] nil nil)",
        ),
        (
            r#"(let ((old-size (lossage-size)))
                 (unwind-protect
                     (list
                      old-size
                      (lossage-size 100)
                      (condition-case error-data
                          (lossage-size 99)
                        (user-error
                         (list
                          (car error-data)
                          (cadr error-data)))))
                   (lossage-size old-size)))"#,
            r#"(300 100 (user-error "Value must be >= 100"))"#,
        ),
    ];

    for (program, expected) in contracts {
        assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let form = Reader::new(program)
            .read()
            .expect("keyboard-input contract should parse")
            .expect("keyboard-input contract should contain a form");
        assert_eq!(
            interp
                .eval(&form, &mut env)
                .expect("keyboard-input contract should evaluate"),
            Reader::new(expected)
                .read()
                .expect("keyboard-input expected value should parse")
                .expect("keyboard-input expected value should exist"),
            "keyboard-input contract diverged: {program}"
        );
    }
}

#[test]
fn native_open_dribble_file_creates_and_closes_a_private_file_like_gnu() {
    let path = std::env::temp_dir().join(format!("emaxx-native-dribble-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let program = format!(
        r#"(let ((file {path:?}))
             (unwind-protect
                 (progn
                   (open-dribble-file file)
                   (open-dribble-file nil)
                   (list
                    (file-exists-p file)
                    (file-modes file)
                    (nth 7 (file-attributes file))))
               (ignore-errors (open-dribble-file nil))
               (ignore-errors (delete-file file))))"#,
        path = path.to_string_lossy()
    );
    let expected = "(t 384 0)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(&program)
        .read()
        .expect("dribble-file contract should parse")
        .expect("dribble-file contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("dribble-file contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("dribble-file expected value should parse")
            .expect("dribble-file expected value should exist")
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn native_libxml_family_uses_strict_xml_and_tolerant_html_dom_contracts() {
    let program = r#"(list
                       (with-temp-buffer
                         (insert
                          "<p CLASS=x>Hello<br>world<!--c-->")
                         (libxml-parse-html-region))
                       (with-temp-buffer
                         (insert "<b>one<i>two</b>three")
                         (libxml-parse-html-region nil nil nil t))
                       (with-temp-buffer
                         (insert
                          "<!--top--><root b=\"2\" a=\"1\"> x <!--inside--><child/> </root><!--after-->")
                         (list
                          (libxml-parse-xml-region)
                          (libxml-parse-xml-region nil nil nil t)
                          (libxml-parse-xml-region
                           (point-max) (point-min))))
                       (with-temp-buffer
                         (insert "<broken>")
                         (libxml-parse-xml-region))
                       (with-temp-buffer
                         (insert "<p>x")
                         (condition-case error-data
                             (libxml-parse-html-region nil nil 3)
                           (error (car error-data)))))"#;
    let expected = r#"((html nil
                         (body nil
                          (p ((class . "x"))
                           "Hello" (br nil) "world"
                           (comment nil "c"))))
                       (html nil
                        (body nil
                         (b nil "one" (i nil "two"))
                         "three"))
                       ((top nil
                         (comment nil "top")
                         (root ((b . "2") (a . "1"))
                          " x " (comment nil "inside") (child nil) " ")
                         (comment nil "after"))
                        (root ((b . "2") (a . "1"))
                         " x " (comment nil "inside") (child nil) " ")
                        (top nil
                         (comment nil "top")
                         (root ((b . "2") (a . "1"))
                          " x " (comment nil "inside") (child nil) " ")
                         (comment nil "after")))
                       nil
                       wrong-type-argument)"#;
    let expected_printed = "((html nil (body nil (p ((class . \"x\")) \"Hello\" (br nil) \"world\" (comment nil \"c\")))) (html nil (body nil (b nil \"one\" (i nil \"two\")) \"three\")) ((top nil (comment nil \"top\") (root ((b . \"2\") (a . \"1\")) \" x \" (comment nil \"inside\") (child nil) \" \") (comment nil \"after\")) (root ((b . \"2\") (a . \"1\")) \" x \" (comment nil \"inside\") (child nil) \" \") (top nil (comment nil \"top\") (root ((b . \"2\") (a . \"1\")) \" x \" (comment nil \"inside\") (child nil) \" \") (comment nil \"after\"))) nil wrong-type-argument)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("libxml family contract should parse")
        .expect("libxml family contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("libxml family contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("libxml expected value should parse")
            .expect("libxml expected value should exist")
    );
}

#[test]
fn native_headless_window_geometry_and_hscroll_match_gnu_c_contracts() {
    let program = r#"(let ((window (selected-window))
                            (buffer (current-buffer)))
                        (unwind-protect
                            (progn
                              (set-window-hscroll window 0)
                              (set-window-margins window 3 4)
                              (list
                               (list
                                (window-body-width window)
                                (window-text-width window)
                                (window-text-width window t)
                                (window-text-height window)
                                (window-text-height window t))
                               (mapcar
                                (lambda (xy)
                                  (coordinates-in-window-p xy window))
                                (list
                                 (cons 0 0) (cons 2 0) (cons 3 0)
                                 (cons 75 0) (cons 76 0) (cons 79 0)
                                 (cons 0 23)))
                               (list
                                (window-hscroll window)
                                (set-window-hscroll window 5)
                                (scroll-left 3)
                                (scroll-right 4)
                                (scroll-right 99)
                                (scroll-left nil)
                                (scroll-right nil))
                               (list
                                (window-line-height nil window)
                                (window-lines-pixel-dimensions window))
                               (eq
                                (window-configuration-frame
                                 (current-window-configuration))
                                (selected-frame))
                               (list
                                (force-window-update)
                                (force-window-update window)
                                (force-window-update buffer)
                                (force-window-update (buffer-name buffer))
                                (force-window-update "missing")
                                (force-window-update 42))
                               (progn
                                 (setq-local tab-line-format "t"
                                             header-line-format "h")
                                 (list
                                  (coordinates-in-window-p
                                   (cons 3 0) window)
                                  (coordinates-in-window-p
                                   (cons 3 1) window)
                                  (coordinates-in-window-p
                                   (cons 3 2) window)
                                  (window-text-height window)))))
                          (set-window-margins window nil nil)
                          (set-window-hscroll window 0)))"#;
    let expected = r#"((73 73 73 23 23)
                        (left-margin left-margin (0 . 0) (72 . 0)
                         right-margin right-margin mode-line)
                        (0 5 8 4 0 71 0)
                        (nil nil)
                        t
                        (t t t t nil nil)
                        (tab-line header-line (0 . 2) 21))"#;
    let expected_printed = "((73 73 73 23 23) (left-margin left-margin (0 . 0) (72 . 0) right-margin right-margin mode-line) (0 5 8 4 0 71 0) (nil nil) t (t t t t nil nil) (tab-line header-line (0 . 2) 21))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("window geometry contract should parse")
        .expect("window geometry contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("window geometry contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("window geometry expected value should parse")
            .expect("window geometry expected value should exist")
    );
}

#[test]
fn native_indent_c_motion_and_line_number_width_family_matches_gnu() {
    let program = r#"(list
                       (with-temp-buffer
                         (insert "abcdef\nxy\tZ")
                         (setq tab-width 4
                               truncate-lines nil
                               truncate-partial-width-windows nil)
                         (let ((window (selected-window)))
                           (set-window-buffer window (current-buffer))
                           (set-window-start window (point-min))
                           (list
                            (compute-motion
                             1 '(0 . 0) (point-max) nil 4 nil window)
                            (compute-motion
                             1 '(0 . 0) (point-max) '(0 . 1) 4 nil window)
                            (compute-motion
                             1 '(0 . 0) (point-max) '(0 . 2) 4 nil window)
                            (compute-motion
                             1 '(0 . 0) (point-max) '(2 . 1) 4 nil window)
                            (compute-motion
                             1 '(0 . 0) 5 nil 4 nil window)
                            (compute-motion
                             1 '(0 . 0) 6 nil 4 nil window)
                            (compute-motion
                             1 '(0 . 0) (point-max) nil 4 '(1 . 0)
                             window))))
                       (with-temp-buffer
                         (dotimes (_ 1234) (insert "x\n"))
                         (goto-char (point-min))
                         (forward-line 998)
                         (let ((start (point))
                               (window (selected-window)))
                           (setq-local display-line-numbers t)
                           (set-window-buffer window (current-buffer))
                           (set-window-start window start)
                           (list
                            (line-number-at-pos start)
                            (window-body-height window)
                            (line-number-display-width)
                            (line-number-display-width t)
                            (line-number-display-width 'columns)
                            (let ((display-line-numbers-width 7))
                              (list
                               (line-number-display-width)
                               (line-number-display-width t)))
                            (let ((display-line-numbers nil))
                              (list
                               (line-number-display-width)
                               (line-number-display-width t)
                               (line-number-display-width
                                'columns)))))))"#;
    let expected = r#"(((12 1 3 4 t)
                        (5 0 1 4 t)
                        (8 0 2 2 nil)
                        (7 2 1 1 nil)
                        (5 0 1 4 t)
                        (6 1 1 1 nil)
                        (12 4 1 4 nil))
                       (999 23 4 6 6.0 (7 9) (0 0 0.0)))"#;
    let expected_printed = "(((12 1 3 4 t) (5 0 1 4 t) (8 0 2 2 nil) (7 2 1 1 nil) (5 0 1 4 t) (6 1 1 1 nil) (12 4 1 4 nil)) (999 23 4 6 6.0 (7 9) (0 0 0.0)))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("indent.c family contract should parse")
        .expect("indent.c family contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("indent.c family contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("indent.c expected value should parse")
            .expect("indent.c expected value should exist")
    );
}

#[test]
fn native_xdisp_headless_query_family_matches_gnu() {
    let program = r#"(list
                       (with-temp-buffer
                         (insert "abc\n")
                         (let ((window (selected-window)))
                           (set-window-buffer window (current-buffer))
                           (list
                            (line-pixel-height)
                            (display--line-is-continued-p)
                            (tab-bar-height)
                            (tab-bar-height nil t)
                            (tool-bar-height)
                            (tool-bar-height nil t)
                            (long-line-optimizations-p)
                            (bidi-resolved-levels)
                            (bidi-resolved-levels 0))))
                       (with-temp-buffer
                         (insert (make-string 200 ?x))
                         (let ((window (selected-window)))
                           (set-window-buffer window (current-buffer))
                           (setq truncate-lines nil
                                 truncate-partial-width-windows nil)
                           (mapcar
                            (lambda (position)
                              (goto-char position)
                              (list
                               position
                               (display--line-is-continued-p)
                               (point)))
                            '(1 80 159 201))))
                       (with-temp-buffer
                         (insert "abc אבג\n")
                         (goto-char 6)
                         (list
                          (current-bidi-paragraph-direction)
                          (let ((bidi-paragraph-direction
                                 'right-to-left))
                            (current-bidi-paragraph-direction))
                          (let ((bidi-display-reordering nil))
                            (current-bidi-paragraph-direction))
                          (let ((other
                                 (generate-new-buffer
                                  " *bidi-other*")))
                            (unwind-protect
                                (progn
                                  (with-current-buffer other
                                    (insert "אבג"))
                                  (current-bidi-paragraph-direction
                                   other))
                              (kill-buffer other)))))
                       (mapcar
                        (lambda (fixture)
                          (with-temp-buffer
                            (insert fixture)
                            (goto-char (point-max))
                            (current-bidi-paragraph-direction
                             (current-buffer))))
                        '("אבג\nabc"
                          "אבג\n\nabc"
                          "abc\nאבג"
                          "abc\n\nאבג"
                          "אבג\n\n"))
                       (with-temp-buffer
                         (insert "אבג")
                         (let ((window (selected-window)))
                           (set-window-buffer window (current-buffer))
                           (mapcar
                            (lambda (case)
                              (goto-char (car case))
                              (list
                               (car case)
                               (cdr case)
                               (condition-case error-data
                                   (move-point-visually (cdr case))
                                 (error (car error-data)))))
                            '((1 . -1) (1 . 1)
                              (2 . -1) (2 . 1)
                              (3 . -1) (3 . 1)
                              (4 . -1) (4 . 1)))))
                       (let ((map
                              '(((rect .
                                      ((0 . 0) . (10 . 10)))
                                 rect-id (:help "r"))
                                ((circle . ((20 . 20) . 5))
                                 circle-id nil)
                                ((poly .
                                       [30 30 40 30 40 40 30 40])
                                 poly-id nil))))
                         (mapcar
                          (lambda (xy)
                            (lookup-image-map
                             map (car xy) (cdr xy)))
                          '((0 . 0) (5 . 5) (10 . 10)
                            (11 . 5) (20 . 20) (24 . 20)
                            (26 . 20) (35 . 35) (41 . 35)))))"#;
    let expected = r#"((1 nil 0 0 0 0 nil nil nil)
                       ((1 t 1) (80 t 80)
                        (159 nil 159) (201 nil 201))
                       (left-to-right right-to-left
                        left-to-right right-to-left)
                       (right-to-left left-to-right
                        left-to-right right-to-left
                        right-to-left)
                       ((1 -1 2)
                        (1 1 beginning-of-buffer)
                        (2 -1 3) (2 1 1)
                        (3 -1 4) (3 1 2)
                        (4 -1 end-of-buffer) (4 1 3))
                       (((rect (0 . 0) 10 . 10)
                         rect-id (:help "r"))
                        ((rect (0 . 0) 10 . 10)
                         rect-id (:help "r"))
                        ((rect (0 . 0) 10 . 10)
                         rect-id (:help "r"))
                        nil
                        ((circle (20 . 20) . 5)
                         circle-id nil)
                        ((circle (20 . 20) . 5)
                         circle-id nil)
                        nil
                        ((poly .
                               [30 30 40 30 40 40 30 40])
                         poly-id nil)
                        nil))"#;
    let expected_printed = "((1 nil 0 0 0 0 nil nil nil) ((1 t 1) (80 t 80) (159 nil 159) (201 nil 201)) (left-to-right right-to-left left-to-right right-to-left) (right-to-left left-to-right left-to-right right-to-left right-to-left) ((1 -1 2) (1 1 beginning-of-buffer) (2 -1 3) (2 1 1) (3 -1 4) (3 1 2) (4 -1 end-of-buffer) (4 1 3)) (((rect (0 . 0) 10 . 10) rect-id (:help \"r\")) ((rect (0 . 0) 10 . 10) rect-id (:help \"r\")) ((rect (0 . 0) 10 . 10) rect-id (:help \"r\")) nil ((circle (20 . 20) . 5) circle-id nil) ((circle (20 . 20) . 5) circle-id nil) nil ((poly . [30 30 40 30 40 40 30 40]) poly-id nil) nil))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("xdisp.c family contract should parse")
        .expect("xdisp.c family contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("xdisp.c family contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("xdisp.c expected value should parse")
            .expect("xdisp.c expected value should exist")
    );
}

#[test]
fn native_x_display_queries_observe_the_headless_backend_boundary() {
    let program = r#"
        (list
         (x-display-list)
         (x-hide-tip)
         (mapcar
          (lambda (name)
            (condition-case error-data
                (funcall name)
              (error (car error-data))))
          '(x-display-backing-store
            x-display-color-cells
            x-display-grayscale-p
            x-display-mm-height
            x-display-mm-width
            x-display-pixel-height
            x-display-pixel-width
            x-display-planes
            x-display-save-under
            x-display-screens
            x-display-visual-class
            x-server-max-request-size
            x-server-vendor
            x-server-version
            xw-display-color-p))
         (condition-case error-data
             (xw-color-defined-p "red")
           (error (car error-data)))
         (condition-case error-data
             (xw-color-values "red")
           (error (car error-data))))"#;
    let expected = r#"
        (nil nil
         (error error error error error
          error error error error error
          error error error error error)
         error error)"#;
    let expected_printed = "(nil nil (error error error error error error error error error error error error error error error) error error)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("X display query contract should parse")
        .expect("X display query contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("headless X display queries should preserve GNU's boundary"),
        Reader::new(expected)
            .read()
            .expect("X display query result should parse")
            .expect("X display query result should exist")
    );
}

#[test]
fn native_gui_creation_tip_and_chooser_boundary_matches_gnu() {
    let program = r#"
        (list
         (condition-case error-data
             (x-create-frame 1)
           (error error-data))
         (condition-case error-data
             (x-create-frame nil)
           (error (car error-data)))
         (condition-case error-data
             (x-show-tip 1)
           (error error-data))
         (condition-case error-data
             (x-show-tip "x" t)
           (error error-data))
         (condition-case error-data
             (x-show-tip "x")
           (error error-data))
         (condition-case error-data
             (x-file-dialog 1 2)
           (error error-data))
         (condition-case error-data
             (x-select-font t)
           (error error-data))
         (condition-case error-data
             (x-select-font)
           (error error-data)))"#;
    let expected = concat!(
        "((wrong-type-argument listp 1) error ",
        "(wrong-type-argument stringp 1) ",
        "(wrong-type-argument frame-live-p t) ",
        "(error \"Window system frame should be used\") ",
        "(error \"Window system is not in use or not initialized\") ",
        "(wrong-type-argument frame-live-p t) ",
        "(error \"Window system frame should be used\"))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("headless GUI action contract should parse")
        .expect("headless GUI action contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("headless GUI action failures should be catchable");
    let expected = Reader::new(expected)
        .read()
        .expect("headless GUI action expected value should parse")
        .expect("headless GUI action expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "headless GUI action result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_headless_menu_and_drag_actions_preserve_gnu_boundaries() {
    let program = r#"
        (let ((menu
               (list "Title"
                     (list "Pane" (cons "One" 1) "break" nil))))
          (list
           (x-popup-menu nil 42)
           (condition-case error-data
               (x-popup-menu t 42)
             (error error-data))
           (condition-case error-data
               (x-popup-menu t nil)
             (error error-data))
           (condition-case error-data
               (x-popup-menu t (list 1))
             (error error-data))
           (x-popup-menu
            (list (list 0 0) (selected-frame))
            menu)
           (x-popup-dialog
            t
            (list "Question" (cons "Yes" t)))
           (condition-case error-data
               (x-popup-dialog 42 nil)
             (error error-data))
           (condition-case error-data
               (x-popup-dialog t 42)
             (error error-data))
           (condition-case error-data
               (menu-bar-menu-at-x-y 0 0 42)
             (error error-data))))"#;
    let expected = concat!(
        "(nil (wrong-type-argument listp 42) ",
        "(wrong-type-argument stringp nil) ",
        "(wrong-type-argument stringp 1) nil nil ",
        "(wrong-type-argument windowp nil) ",
        "(wrong-type-argument listp 42) ",
        "(wrong-type-argument framep 42))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("headless menu contract should parse")
        .expect("headless menu contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("headless menu contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("headless menu expected value should parse")
        .expect("headless menu expected value should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "headless menu result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );

    let boundary = Reader::new(
        r#"
        (list
         (menu-bar-menu-at-x-y 0 0)
         (condition-case error-data
             (menu-bar-menu-at-x-y nil 0)
           (error error-data))
         (condition-case error-data
             (x-begin-drag nil)
           (error error-data))
         (condition-case error-data
             (x-begin-drag 42 nil nil)
           (error error-data)))"#,
    )
    .read()
    .expect("headless menu/drag boundary should parse")
    .expect("headless menu/drag boundary should contain a form");
    assert_eq!(
        interp
            .eval(&boundary, &mut env)
            .expect("headless menu/drag errors should be catchable"),
        Value::list([
            Value::Nil,
            Value::list([
                Value::symbol("wrong-type-argument"),
                Value::symbol("fixnump"),
                Value::Nil,
            ]),
            Value::list([
                Value::symbol("error"),
                Value::string("Window system frame should be used"),
            ]),
            Value::list([
                Value::symbol("error"),
                Value::string("Window system frame should be used"),
            ]),
        ])
    );
}

#[test]
fn native_display_connection_management_stops_at_the_headless_backend() {
    let validation = r#"
        (list
         (condition-case error-data
             (x-open-connection 42)
           (error error-data))
         (condition-case error-data
             (x-close-connection 42)
           (error error-data)))"#;
    let expected = "((wrong-type-argument stringp 42) (wrong-type-argument frame-live-p 42))";
    assert_upstream_primitive_contract(&format!("(prin1 {validation})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let validation = Reader::new(validation)
        .read()
        .expect("display connection validation should parse")
        .expect("display connection validation should contain a form");
    let actual = interp
        .eval(&validation, &mut env)
        .expect("display connection validation errors should be catchable");
    let expected = Reader::new(expected)
        .read()
        .expect("display connection validation result should parse")
        .expect("display connection validation result should exist");
    assert!(
        values_equal(&interp, &actual, &expected),
        "display connection validation differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );

    let boundary = Reader::new(
        r#"
        (mapcar
         (lambda (operation)
           (condition-case error-data
               (funcall operation)
             (error error-data)))
         (list
          (lambda () (x-open-connection "display"))
          (lambda () (x-open-connection "display" 42 t))
          (lambda () (x-close-connection nil))
          (lambda () (x-close-connection (selected-frame)))
          (lambda () (x-close-connection (frame-terminal)))
          (lambda () (x-close-connection "missing"))))"#,
    )
    .read()
    .expect("headless display connection boundary should parse")
    .expect("headless display connection boundary should contain a form");
    let unavailable = Value::list([
        Value::symbol("error"),
        Value::string("Window system is not in use or not initialized"),
    ]);
    assert_eq!(
        interp
            .eval(&boundary, &mut env)
            .expect("headless display connection errors should be catchable"),
        Value::list(std::iter::repeat_n(unavailable, 6))
    );
}

#[test]
fn native_treesit_runtime_capabilities_and_query_predicates_match_gnu() {
    let program = r#"
        (let* ((detail
                (treesit-language-available-p
                 'emaxx-definitely-missing t))
               (lazy
                (treesit-query-compile
                 'emaxx-definitely-missing
                 "(identifier) @id")))
          (list
           (treesit-available-p)
           (treesit-library-abi-version)
           (treesit-library-abi-version t)
           (treesit-language-abi-version)
           (treesit-language-abi-version 'emaxx-definitely-missing)
           (treesit-language-available-p 'emaxx-definitely-missing)
           (list (car detail) (cadr detail))
           (treesit-parser-p nil)
           (treesit-node-p nil)
           (treesit-compiled-query-p nil)
           (treesit-query-p "(identifier) @id")
           (treesit-query-p '((identifier) @id))
           (treesit-query-p [])
           (treesit-query-p nil)
           (treesit-compiled-query-p lazy)
           (treesit-query-p lazy)
           (treesit-query-language lazy)
           (eq lazy (treesit-query-compile 'other-language lazy))
           (condition-case error-data
               (treesit-query-compile
                'emaxx-definitely-missing "(identifier)" t)
             (error (car error-data)))
           (condition-case error-data
               (treesit-query-language nil)
             (error (list (car error-data) (cadr error-data))))
           (condition-case error-data
               (treesit-node-parser nil)
             (error (list (car error-data) (cadr error-data))))
           (condition-case error-data
               (treesit-parser-create 'emaxx-definitely-missing)
             (error (list (car error-data) (cadr error-data))))
           (treesit-parser-list)))"#;
    let expected = r#"
        (t 15 13 nil nil nil (nil not-found)
         nil nil nil t t nil nil
         t t emaxx-definitely-missing t treesit-load-language-error
         (wrong-type-argument treesit-compiled-query-p)
         (wrong-type-argument treesit-node-p)
         (treesit-load-language-error not-found) nil)"#;
    let expected_printed = "(t 15 13 nil nil nil (nil not-found) nil nil nil t t nil nil t t emaxx-definitely-missing t treesit-load-language-error (wrong-type-argument treesit-compiled-query-p) (wrong-type-argument treesit-node-p) (treesit-load-language-error not-found) nil)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("Tree-sitter capability contract should parse")
        .expect("Tree-sitter capability contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("Tree-sitter runtime introspection should evaluate"),
        Reader::new(expected)
            .read()
            .expect("Tree-sitter capability result should parse")
            .expect("Tree-sitter capability result should exist")
    );
}

#[test]
fn native_treesit_parser_lifecycle_and_real_json_nodes_use_official_runtime() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*json*", r#"{"hello": [1, true]}"#);
    interp.register_treesit_language_for_test("json", tree_sitter_json::LANGUAGE.into());

    let program = r#"
        (let* ((parser (treesit-parser-create 'json nil nil 'main))
               (reused (treesit-parser-create 'json nil nil 'main))
               (second (treesit-parser-create 'json nil t 'main)))
          (treesit-parser-add-notifier parser 'first)
          (treesit-parser-add-notifier parser 'second)
          (treesit-parser-add-notifier parser 'first)
          (treesit-parser-set-included-ranges parser '((1 . 21)))
          (let* ((root (treesit-parser-root-node parser))
                 (object (treesit-node-child root 0 t))
                 (pair (treesit-node-child object 0 t))
                 (key (treesit-node-child-by-field-name pair "key"))
                 (value (treesit-node-child-by-field-name pair "value"))
                 (same-key (treesit-node-child-by-field-name pair "key")))
            (let ((before
                   (list
               (treesit-language-available-p 'json)
               (treesit-language-available-p 'json t)
               (treesit-language-abi-version 'json)
               (treesit-parser-p parser)
               (eq parser reused)
               (not (eq parser second))
               (eq (treesit-parser-buffer parser) (current-buffer))
               (treesit-parser-language parser)
               (treesit-parser-tag parser)
               (treesit-parser-included-ranges parser)
               (treesit-parser-notifiers parser)
               (length (treesit-parser-list nil 'json 'main))
               (treesit-node-p root)
               (eq (treesit-node-parser root) parser)
               (treesit-node-type root)
               (treesit-node-start root)
               (treesit-node-end root)
               (treesit-node-string root)
               (treesit-node-child-count root t)
               (treesit-node-type object)
               (treesit-node-type pair)
               (treesit-node-field-name-for-child pair 0)
               (treesit-node-field-name-for-child pair 2)
               (treesit-node-type key)
               (treesit-node-start key)
               (treesit-node-end key)
               (treesit-node-type value)
               (treesit-node-eq key same-key)
               (treesit-node-eq key value)
               (treesit-node-check key 'named)
               (treesit-node-check key 'missing)
               (treesit-node-check key 'has-error)
               (treesit-node-type (treesit-node-parent key))
               (treesit-node-type (treesit-node-prev-sibling pair))
               (treesit-node-type (treesit-node-next-sibling pair))
               (treesit-node-next-sibling pair t))))
              (goto-char (point-max))
              (insert " ")
              (let ((new-root (treesit-parser-root-node parser)))
                (treesit-parser-delete second)
                (append
                 before
                 (list
                  (treesit-node-check root 'outdated)
                  (treesit-node-type new-root)
                  (treesit-node-end new-root)
                  (length (treesit-parser-list nil 'json 'main))
                  (treesit-parser-p second)
                  (condition-case error-data
                      (treesit-parser-language second)
                    (error (car error-data)))
                  (condition-case error-data
                      (treesit-node-type root)
                    (error (car error-data)))))))))"#;
    let expected = r#"
        (t (t) 14 t t t t json main ((1 . 21)) (first second) 2
         t t "document" 1 21
         "(document (object (pair key: (string (string_content)) value: (array (number) (true)))))"
         1 "object" "pair" "key" "value" "string" 2 9 "array"
         t nil t nil nil "pair" "{" "}" nil t "document" 21 1 t
         treesit-parser-deleted treesit-node-outdated)"#;
    let form = Reader::new(program)
        .read()
        .expect("Tree-sitter parser lifecycle program should parse")
        .expect("Tree-sitter parser lifecycle form should exist");
    let actual = interp
        .eval(&form, &mut Vec::new())
        .expect("official Tree-sitter JSON parser lifecycle should evaluate");
    interp.set_global_binding("emaxx-treesit-result", actual);
    let comparison = Reader::new(&format!("(equal emaxx-treesit-result '{expected})"))
        .read()
        .expect("Tree-sitter parser lifecycle comparison should parse")
        .expect("Tree-sitter parser lifecycle comparison should exist");
    assert_eq!(
        interp
            .eval(&comparison, &mut Vec::new())
            .expect("Tree-sitter parser lifecycle result should compare"),
        Value::T
    );
}

#[test]
fn native_treesit_queries_and_traversal_use_official_runtime() {
    let expansion_program = r#"
        (list
         (treesit-pattern-expand :anchor)
         (treesit-pattern-expand :equal)
         (treesit-pattern-expand
          '(type field: (_) @capture :anchor))
         (treesit-pattern-expand '[(_) "return"])
         (treesit-query-expand
          '((type field: (_) @capture :anchor)
            :? :* :+ "return"))
         (treesit-pattern-expand "a\nb\rc\td\0e\"f\1g\\h\fi"))"#;
    let expansion_expected =
        r##"("." "#equal" "(type field: (_) @capture .)" "[(_) \"return\"]" "(type field: (_) @capture .) ? * + \"return\"" "\"a\\nb\\rc\\td\\0e\\\"f\1g\\\\h\fi\"")"##
            .replace(r"\1", "\u{1}")
            .replace(r"\f", "\u{c}");
    assert_upstream_primitive_contract(
        &format!("(prin1 {expansion_program})"),
        &expansion_expected,
    );

    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*json-query*", r#"{"hello": [1, true]}"#);
    interp.register_treesit_language_for_test("json", tree_sitter_json::LANGUAGE.into());

    let program = r#"
        (progn
          (fset 'emaxx-treesit-last-p
                (lambda (node)
                  (not (treesit-node-next-sibling node t))))
          (let* ((treesit-thing-settings
                  '((json (scalar (or "number" "true")))))
                 (parser (treesit-parser-create 'json))
                 (root (treesit-parser-root-node parser))
                 (object (treesit-node-child root 0 t))
                 (pair (treesit-node-child object 0 t))
                 (array
                  (treesit-node-child-by-field-name pair "value"))
                 (number (treesit-node-child array 0 t))
                 (truth (treesit-node-child array 1 t))
                 (query
                  "((string_content) @hello (#match \"^hello$\" @hello))
((number) @one (#equal \"1\" @one))
((true) @last (#pred emaxx-treesit-last-p @last))")
                 (compiled
                  (treesit-query-compile 'json query t)))
            (list
             (treesit-query-language compiled)
             (mapcar
              (lambda (capture)
                (cons (car capture)
                      (treesit-node-type (cdr capture))))
              (treesit-query-capture root compiled))
             (mapcar
              #'treesit-node-type
              (treesit-query-capture
               parser
               "(number) @number
(true) @truth"
               12 13 t))
             (mapcar
              #'treesit-node-type
              (treesit-query-capture
               'json "(true) @truth" nil nil t))
             (treesit-node-type
              (treesit-node-first-child-for-pos object 1))
             (treesit-node-type
              (treesit-node-first-child-for-pos object 2 t))
             (treesit-node-type
              (treesit-node-descendant-for-range root 12 13 t))
             (treesit-node-match-p number "number")
             (treesit-node-match-p number '(not "number"))
             (treesit-node-match-p truth 'scalar)
             (treesit-node-match-p truth 'missing t)
             (treesit-node-type
              (treesit-search-subtree array "true"))
             (treesit-node-type
              (treesit-search-subtree array "number" t))
             (treesit-node-type
              (treesit-search-forward number "true"))
             (treesit-node-type
              (treesit-search-forward truth "number" t))
             (treesit-induce-sparse-tree
              root 'scalar #'treesit-node-type)
             (treesit-subtree-stat array)
             (condition-case error-data
                 (treesit-query-compile 'json "(missing-node)" t)
               (error (car error-data))))))"#;
    let expected = r#"
        (json
         ((hello . "string_content") (one . "number") (last . "true"))
         ("number") ("true")
         "{" "pair" "number"
         t nil t nil
         "true" "number" "true" "number"
         (nil ("number") ("true"))
         (1 5 6)
         treesit-query-error)"#;
    let form = Reader::new(program)
        .read()
        .expect("Tree-sitter query and traversal program should parse")
        .expect("Tree-sitter query and traversal form should exist");
    let actual = interp
        .eval(&form, &mut Vec::new())
        .expect("official Tree-sitter query and traversal runtime should evaluate");
    interp.set_global_binding("emaxx-treesit-query-result", actual);
    let comparison = Reader::new(&format!("(equal emaxx-treesit-query-result '{expected})"))
        .read()
        .expect("Tree-sitter query result comparison should parse")
        .expect("Tree-sitter query result comparison should exist");
    assert_eq!(
        interp
            .eval(&comparison, &mut Vec::new())
            .expect("Tree-sitter query and traversal result should compare"),
        Value::T
    );
}

#[test]
fn native_window_change_state_hooks_and_minibuffer_resize_match_gnu() {
    let program = r#"(list
                       (list (window-old-buffer) (window-old-point))
                       (list
                        (condition-case error-data
                            (other-window-for-scrolling)
                          (error (car error-data)))
                        (let ((other-window-scroll-default
                               (lambda () (minibuffer-window))))
                          (eq
                           (other-window-for-scrolling)
                           (minibuffer-window))))
                       (let ((old
                              (default-value
                               'window-configuration-change-hook)))
                         (unwind-protect
                             (progn
                               (fset
                                'emaxx-w-local
                                (lambda ()
                                  (setq emaxx-w-events
                                        (cons 'local emaxx-w-events))))
                               (fset
                                'emaxx-w-global
                                (lambda ()
                                  (setq emaxx-w-events
                                        (cons 'global emaxx-w-events))))
                               (setq emaxx-w-events nil)
                               (setq-default
                                window-configuration-change-hook
                                (list 'emaxx-w-global))
                               (add-hook
                                'window-configuration-change-hook
                                'emaxx-w-local nil t)
                               (run-window-configuration-change-hook)
                               (nreverse emaxx-w-events))
                           (setq-default
                            window-configuration-change-hook old)
                           (kill-local-variable
                            'window-configuration-change-hook)
                           (fmakunbound 'emaxx-w-local)
                           (fmakunbound 'emaxx-w-global)))
                       (let* ((mini (minibuffer-window))
                              (root (frame-root-window)))
                         (list
                          (window-total-height root)
                          (window-total-height mini)
                          (progn
                            (set-window-new-pixel root 23)
                            (set-window-new-pixel mini 2)
                            (resize-mini-window-internal mini))
                          (window-total-height root)
                          (window-total-height mini))))"#;
    let expected = "((nil 1) (error t) (local global) (24 1 t 23 2))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("window state contract should parse")
        .expect("window state contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("window state contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("window state expected value should parse")
            .expect("window state expected value should exist")
    );
}

#[cfg(unix)]
fn serial_test_pty() -> (serialport::TTYPort, String) {
    use serialport::SerialPort;

    let (master, slave) = serialport::TTYPort::pair().expect("create serial test PTY");
    let path = slave
        .name()
        .expect("serial test PTY should have a slave path");
    drop(slave);
    (master, path)
}

#[cfg(unix)]
fn serial_surface_program(path: &str) -> String {
    format!(
        r#"(let* ((port {path:?})
                  (process
                   (make-serial-process
                    :name "serial-audit"
                    :buffer " *serial-audit*"
                    :port port
                    :speed 9600
                    :bytesize nil
                    :parity nil
                    :stopbits nil
                    :flowcontrol nil
                    :noquery t
                    :stop t
                    :sentinel 'ignore)))
             (unwind-protect
                 (list
                  (eq (process-type process) 'serial)
                  (eq (process-status process) 'stop)
                  (null (process-id process))
                  (equal (process-contact process) (list port 9600))
                  (= (process-contact process :speed) 9600)
                  (= (process-contact process :bytesize) 8)
                  (null (process-contact process :parity))
                  (= (process-contact process :stopbits) 1)
                  (null (process-contact process :flowcontrol))
                  (equal (process-contact process :summary) "8N1")
                  (null (process-query-on-exit-flag process))
                  (progn
                    (continue-process process)
                    (eq (process-status process) 'open))
                  (progn
                    (stop-process process)
                    (eq (process-status process) 'stop))
                  (progn
                    (serial-process-configure
                     :process process
                     :bytesize nil
                     :parity nil
                     :stopbits nil
                     :flowcontrol nil)
                    (equal (process-contact process :summary) "8N1"))
                  (progn
                    (delete-process process)
                    (and
                     (eq (process-type process) 'serial)
                     (eq (process-status process) 'closed))))
               (when (process-live-p process)
                 (delete-process process))))"#
    )
}

#[cfg(unix)]
#[test]
fn native_serial_process_surface_and_configuration_match_gnu() {
    let (gnu_master, gnu_path) = serial_test_pty();
    let gnu_program = serial_surface_program(&gnu_path);
    let expected = "(t t t t t t t t t t t t t t t)";
    assert_upstream_primitive_contract(&format!("(prin1 {gnu_program})"), expected);
    drop(gnu_master);

    let (_emaxx_master, emaxx_path) = serial_test_pty();
    let emaxx_program = serial_surface_program(&emaxx_path);
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(&emaxx_program)
        .read()
        .expect("serial surface contract should parse")
        .expect("serial surface contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("serial surface contract should evaluate"),
        Value::list(std::iter::repeat_n(Value::T, 15))
    );
}

#[cfg(unix)]
#[test]
fn native_serial_speed_nil_preserves_the_unconfigured_gnu_contract() {
    let (gnu_master, gnu_path) = serial_test_pty();
    let program = |path: &str| {
        format!(
            r#"(let ((process
                      (make-serial-process
                       :name "serial-unconfigured"
                       :port {path:?}
                       :speed nil
                       :sentinel 'ignore)))
                 (unwind-protect
                     (list
                      (eq (process-type process) 'serial)
                      (null (process-contact process :speed))
                      (null (process-contact process :summary))
                      (null
                       (serial-process-configure
                        :process process :speed 9600))
                      (null (process-contact process :speed))
                      (null (process-contact process :summary)))
                   (delete-process process)))"#
        )
    };
    let expected = "(t t t t t t)";
    assert_upstream_primitive_contract(&format!("(prin1 {})", program(&gnu_path)), expected);
    drop(gnu_master);

    let (_emaxx_master, emaxx_path) = serial_test_pty();
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(&program(&emaxx_path))
        .read()
        .expect("unconfigured serial contract should parse")
        .expect("unconfigured serial contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("unconfigured serial contract should evaluate"),
        Value::list(std::iter::repeat_n(Value::T, 6))
    );
}

#[cfg(unix)]
#[test]
fn native_serial_process_pumps_and_sends_bytes_over_a_real_pty() {
    use serialport::SerialPort;

    let (mut master, path) = serial_test_pty();
    master
        .set_timeout(Duration::from_secs(1))
        .expect("set serial test PTY timeout");
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let filter_form = Reader::new("(lambda (_process text) (setq emaxx-test-serial-input text))")
        .read()
        .expect("serial filter should parse")
        .expect("serial filter should exist");
    let filter = interp
        .eval(&filter_form, &mut env)
        .expect("serial filter should evaluate");
    let process = call(
        &mut interp,
        "make-serial-process",
        &[
            Value::symbol(":name"),
            Value::String("serial-io".into()),
            Value::symbol(":port"),
            Value::String(path.into()),
            Value::symbol(":speed"),
            Value::Integer(9600),
            Value::symbol(":filter"),
            filter,
            Value::symbol(":sentinel"),
            Value::symbol("ignore"),
        ],
        &mut env,
    )
    .expect("open serial process on PTY");

    master
        .write_all(b"from-master")
        .expect("write PTY master to serial process");
    call(
        &mut interp,
        "accept-process-output",
        &[process.clone(), Value::Float(1.0)],
        &mut env,
    )
    .expect("pump serial process input");
    assert_eq!(
        interp.lookup_var("emaxx-test-serial-input", &env),
        Some(Value::String("from-master".into()))
    );

    call(
        &mut interp,
        "process-send-string",
        &[process.clone(), Value::String("from-emaxx".into())],
        &mut env,
    )
    .expect("send serial process output");
    let mut output = [0_u8; 10];
    std::io::Read::read_exact(&mut master, &mut output)
        .expect("read serial process output from PTY master");
    assert_eq!(&output, b"from-emaxx");
    call(&mut interp, "delete-process", &[process], &mut env).expect("delete serial process");
}

#[test]
fn native_datagram_addresses_track_udp_peer_state_and_contact_metadata() {
    let program = r#"(let
                         ((udp
                           (make-network-process
                            :name "udp-client"
                            :type 'datagram
                            :family 'ipv4
                            :host "127.0.0.1"
                            :service 9
                            :sentinel 'ignore))
                          (pipe
                           (make-pipe-process
                            :name "not-datagram"
                            :sentinel 'ignore)))
                       (unwind-protect
                           (list
                            (process-status udp)
                            (process-type udp)
                            (process-datagram-address udp)
                            (set-process-datagram-address
                             udp [127 0 0 1 10])
                            (process-datagram-address udp)
                            (process-contact udp :local)
                            (process-contact udp :remote)
                            (set-process-datagram-address
                             udp [0 0 0 0 0 0 0 1 10])
                            (process-datagram-address pipe)
                            (set-process-datagram-address
                             pipe [127 0 0 1 10]))
                         (delete-process udp)
                         (delete-process pipe)))"#;
    let expected = "(open network [127 0 0 1 9] [127 0 0 1 10] [127 0 0 1 10] [0 0 0 0 0] [127 0 0 1 10] nil nil nil)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("datagram address contract should parse")
        .expect("datagram address contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("datagram address contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("datagram address result should parse")
            .expect("datagram address result should exist")
    );
}

#[test]
fn native_network_socket_errors_preserve_gnu_file_conditions() {
    let server = crate::lisp::eval::error_condition_value(&processes::network_server_error(
        &std::io::Error::from_raw_os_error(libc::EPERM),
    ))
    .to_vec()
    .expect("server error should be a condition list");
    assert_eq!(server[0], Value::symbol("file-error"));
    assert_eq!(server[1], Value::string("Cannot bind server socket"));
    assert!(
        !string_text(&server[2])
            .expect("server detail should be a string")
            .contains("(os error")
    );

    let args = [Value::symbol(":name"), Value::string("probe")];
    let client = crate::lisp::eval::error_condition_value(&processes::network_client_error(
        &std::io::Error::from_raw_os_error(libc::ENOENT),
        &args,
    ))
    .to_vec()
    .expect("client error should be a condition list");
    assert_eq!(client[0], Value::symbol("file-missing"));
    assert_eq!(client[1], Value::string("make client process failed"));
    assert!(
        !string_text(&client[2])
            .expect("client detail should be a string")
            .contains("(os error")
    );
    assert_eq!(&client[3..], args.as_slice());
}

#[test]
fn native_udp_event_pump_preserves_datagrams_and_updates_the_reply_peer() {
    let program = r#"(progn
                       (setq emaxx-test-udp-received nil)
                       (let*
                           ((server
                             (make-network-process
                              :name "udp-server"
                              :type 'datagram
                              :family 'ipv4
                              :server t
                              :host "127.0.0.1"
                              :service t
                              :filter
                              (lambda (_process text)
                                (setq emaxx-test-udp-received text))
                              :sentinel 'ignore))
                            (local (process-contact server :local))
                            (client
                             (make-network-process
                              :name "udp-sender"
                              :type 'datagram
                              :family 'ipv4
                              :host "127.0.0.1"
                              :service (aref local 4)
                              :sentinel 'ignore)))
                         (unwind-protect
                             (progn
                               (process-send-string client "ping")
                               (let ((attempts 0))
                                 (while
                                     (and
                                      (null emaxx-test-udp-received)
                                      (< attempts 50))
                                   (accept-process-output server .02)
                                   (setq attempts (1+ attempts))))
                               (let
                                   ((peer
                                     (process-datagram-address server)))
                                 (list
                                  (equal
                                   emaxx-test-udp-received "ping")
                                  (equal
                                   peer
                                   (process-contact server :remote))
                                  (and
                                   (= (aref peer 0) 127)
                                   (= (aref peer 1) 0)
                                   (= (aref peer 2) 0)
                                   (= (aref peer 3) 1))
                                  (> (aref peer 4) 0))))
                           (delete-process client)
                           (delete-process server))))"#;
    let expected = "(t t t t)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("UDP event-pump contract should parse")
        .expect("UDP event-pump contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("UDP event-pump contract should evaluate"),
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
}

#[cfg(unix)]
#[test]
fn native_subprocess_job_control_uses_child_groups_and_reaps_signal_states() {
    fn start_sleep(interp: &mut Interpreter, env: &mut Env, name: &str) -> Value {
        call(
            interp,
            "make-process",
            &[
                Value::symbol(":name"),
                Value::String(name.into()),
                Value::symbol(":command"),
                Value::list([
                    Value::String("/bin/sleep".into()),
                    Value::String("30".into()),
                ]),
                Value::symbol(":connection-type"),
                Value::symbol("pipe"),
                Value::symbol(":sentinel"),
                Value::symbol("ignore"),
            ],
            env,
        )
        .unwrap_or_else(|error| panic!("start {name}: {error}"))
    }

    fn wait_for_status(interp: &mut Interpreter, env: &mut Env, process: &Value, expected: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let status = call(interp, "process-status", std::slice::from_ref(process), env)
                .expect("read controlled subprocess status");
            if status == Value::symbol(expected) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "subprocess never reached {expected}; last status was {status:?}"
            );
            pump_external_process_output(interp, env).expect("pump controlled subprocess");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let stopped = start_sleep(&mut interp, &mut env, "stopped-child");
    let stopped_id = interp
        .resolve_process_id(&stopped)
        .expect("stopped child process id");
    let stopped_pid = interp
        .process_os_id(stopped_id)
        .expect("stopped child OS pid") as libc::pid_t;
    // SAFETY: getpgid reads kernel metadata for a known live child pid.
    assert_eq!(unsafe { libc::getpgid(stopped_pid) }, stopped_pid);
    assert_eq!(
        call(
            &mut interp,
            "internal-default-signal-process",
            &[stopped.clone(), Value::symbol("STOP")],
            &mut env,
        )
        .expect("force child stop"),
        Value::Integer(0)
    );
    wait_for_status(&mut interp, &mut env, &stopped, "stop");
    assert_eq!(
        call(
            &mut interp,
            "continue-process",
            std::slice::from_ref(&stopped),
            &mut env,
        )
        .expect("continue stopped child"),
        stopped
    );
    wait_for_status(&mut interp, &mut env, &stopped, "run");
    assert_eq!(
        call(
            &mut interp,
            "kill-process",
            std::slice::from_ref(&stopped),
            &mut env,
        )
        .expect("kill continued child"),
        stopped
    );
    wait_for_status(&mut interp, &mut env, &stopped, "signal");
    assert_eq!(
        call(
            &mut interp,
            "process-exit-status",
            std::slice::from_ref(&stopped),
            &mut env,
        )
        .expect("killed child signal"),
        Value::Integer(libc::SIGKILL.into())
    );

    for (name, function, signal) in [
        ("interrupted-child", "interrupt-process", libc::SIGINT),
        ("quit-child", "quit-process", libc::SIGQUIT),
    ] {
        let process = start_sleep(&mut interp, &mut env, name);
        assert_eq!(
            call(
                &mut interp,
                function,
                std::slice::from_ref(&process),
                &mut env,
            )
            .unwrap_or_else(|error| panic!("{function}: {error}")),
            process
        );
        wait_for_status(&mut interp, &mut env, &process, "signal");
        assert_eq!(
            call(
                &mut interp,
                "process-exit-status",
                std::slice::from_ref(&process),
                &mut env,
            )
            .expect("controlled child signal"),
            Value::Integer(signal.into())
        );
    }
}

#[test]
fn native_system_process_inventory_and_attributes_share_the_host_snapshot() {
    let program = r#"(let* ((pid (emacs-pid))
                            (ids (list-system-processes))
                            (a (process-attributes pid)))
                       (list
                        (or (null ids) (and (listp ids) (memq pid ids) t))
                        (integerp (alist-get 'euid a))
                        (stringp (alist-get 'user a))
                        (integerp (alist-get 'egid a))
                        (stringp (alist-get 'group a))
                        (stringp (alist-get 'comm a))
                        (stringp (alist-get 'state a))
                        (integerp (alist-get 'ppid a))
                        (integerp (alist-get 'pgrp a))
                        (consp (alist-get 'start a))
                        (integerp (alist-get 'vsize a))
                        (integerp (alist-get 'rss a))
                        (consp (alist-get 'etime a))
                        (stringp (alist-get 'args a))
                        (process-attributes -1)))"#;
    let expected = "(t t t t t t t t t t t t t t nil)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("process inventory assertion should parse")
        .expect("process inventory assertion form");
    let result = interp
        .eval(&form, &mut env)
        .expect("process inventory assertion should evaluate");
    assert_eq!(
        result,
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
            Value::Nil,
        ])
    );
}

#[test]
fn native_system_process_inventory_matches_oracle_availability() {
    let binary = upstream_emacs_repo().join("src/emacs");
    let output = std::process::Command::new(&binary)
        .args([
            "--batch",
            "-Q",
            "--eval",
            "(prin1 (null (list-system-processes)))",
        ])
        .output()
        .unwrap_or_else(|error| panic!("run process-inventory oracle: {error}"));
    assert!(output.status.success());
    let oracle_is_empty = output.stdout == b"t";

    let mut interp = Interpreter::new();
    let inventory = call(&mut interp, "list-system-processes", &[], &mut Vec::new())
        .expect("list native system processes");
    assert_eq!(inventory.is_nil(), oracle_is_empty);
}

#[cfg(unix)]
#[test]
fn process_filter_t_holds_os_output_until_the_default_filter_is_restored() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, _) = interp.create_buffer(" *held-process-output*");
    let buffer = interp
        .buffer_identity_value(buffer_id)
        .expect("held-output buffer identity");
    let process = call(
        &mut interp,
        "make-process",
        &[
            Value::symbol(":name"),
            Value::String("held-output".into()),
            Value::symbol(":buffer"),
            buffer,
            Value::symbol(":command"),
            Value::list([Value::String("/bin/cat".into())]),
            Value::symbol(":connection-type"),
            Value::symbol("pipe"),
            Value::symbol(":sentinel"),
            Value::symbol("ignore"),
        ],
        &mut env,
    )
    .expect("start held-output process");
    call(
        &mut interp,
        "set-process-filter",
        &[process.clone(), Value::T],
        &mut env,
    )
    .expect("hold process output");
    call(
        &mut interp,
        "process-send-string",
        &[process.clone(), Value::String("held\n".into())],
        &mut env,
    )
    .expect("send held output");
    assert_eq!(
        call(
            &mut interp,
            "accept-process-output",
            &[process.clone(), Value::Float(0.05)],
            &mut env,
        )
        .expect("wait while output is held"),
        Value::Nil
    );
    assert_eq!(
        interp
            .get_buffer_by_id(buffer_id)
            .expect("held-output buffer")
            .buffer_string(),
        ""
    );

    call(
        &mut interp,
        "set-process-filter",
        &[process.clone(), Value::Nil],
        &mut env,
    )
    .expect("restore default process filter");
    // The full debug suite runs heavyweight Eshell interpreters and several
    // native subprocess probes concurrently.  Keep polling the actual
    // delivery condition, but do not make host scheduler latency a semantic
    // failure.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let delivered = call(
            &mut interp,
            "accept-process-output",
            &[process.clone(), Value::Float(0.05)],
            &mut env,
        )
        .expect("wait for resumed output");
        if delivered == Value::T {
            break;
        }
    }
    assert_eq!(
        interp
            .get_buffer_by_id(buffer_id)
            .expect("held-output buffer")
            .buffer_string(),
        "held\n"
    );
    call(
        &mut interp,
        "delete-process",
        std::slice::from_ref(&process),
        &mut env,
    )
    .expect("delete held-output process");
}

#[cfg(unix)]
#[test]
fn native_process_window_and_foreground_queries_follow_pty_ownership() {
    assert_upstream_primitive_contract(
        r#"(let ((pipe (make-process :name "pipe"
                                     :command '("/bin/cat")
                                     :connection-type 'pipe))
                  (pty (make-process :name "pty"
                                    :command '("/bin/cat")
                                    :connection-type 'pty)))
             (unwind-protect
                 (prin1
                  (list
                   (set-process-window-size pipe 24 80)
                   (set-process-window-size pty 24 80)
                   (let ((v (process-running-child-p pipe)))
                     (or (eq v t) (integerp v)))
                   (null (process-running-child-p pty))))
               (set-process-sentinel pipe 'ignore)
               (set-process-sentinel pty 'ignore)
               (delete-process pipe)
               (delete-process pty)))"#,
        "(nil t t t)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let make_process =
        |interp: &mut Interpreter, env: &mut Env, name: &str, connection_type: &str| {
            call(
                interp,
                "make-process",
                &[
                    Value::symbol(":name"),
                    Value::String(name.into()),
                    Value::symbol(":command"),
                    Value::list([Value::String("/bin/cat".into())]),
                    Value::symbol(":connection-type"),
                    Value::symbol(connection_type),
                    Value::symbol(":sentinel"),
                    Value::symbol("ignore"),
                ],
                env,
            )
            .unwrap_or_else(|error| panic!("start {connection_type} process: {error}"))
        };
    let pipe = make_process(&mut interp, &mut env, "pipe", "pipe");
    let pty = make_process(&mut interp, &mut env, "pty", "pty");

    assert_eq!(
        call(
            &mut interp,
            "set-process-window-size",
            &[pipe.clone(), Value::Integer(24), Value::Integer(80)],
            &mut env,
        )
        .expect("pipe window size"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "set-process-window-size",
            &[pty.clone(), Value::Integer(24), Value::Integer(80)],
            &mut env,
        )
        .expect("PTY window size"),
        Value::T
    );
    let pipe_foreground = call(
        &mut interp,
        "process-running-child-p",
        std::slice::from_ref(&pipe),
        &mut env,
    )
    .expect("pipe foreground query");
    assert!(pipe_foreground == Value::T || matches!(pipe_foreground, Value::Integer(_)));
    assert_eq!(
        call(
            &mut interp,
            "process-running-child-p",
            std::slice::from_ref(&pty),
            &mut env,
        )
        .expect("PTY foreground query"),
        Value::Nil
    );
    for process in [pipe, pty] {
        call(
            &mut interp,
            "delete-process",
            std::slice::from_ref(&process),
            &mut env,
        )
        .expect("delete process query target");
    }
}
