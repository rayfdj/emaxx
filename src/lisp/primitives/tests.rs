use super::*;
use crate::lisp::reader::Reader;
use std::io::{Read, Write};

fn upstream_emacs_repo() -> PathBuf {
    crate::compat::canonicalize_path(&crate::compat::project_root().join("../emacs"))
        .expect("canonical sibling GNU checkout")
}

/// Call NAME through the interpreter's function cell, exactly as GNU
/// `funcall' would — reaching Lisp definitions (subr.el's start-process,
/// keymap.el's keymap-set) instead of the C-only dispatch table.
fn call_via_lisp(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let function = interp.lookup_function(name, env)?;
    interp.call_function_value(function, Some(name), args, env)
}

fn upstream_primitive_contract_output(program: &str) -> String {
    crate::test_support::mark_process_test();
    let binary = upstream_emacs_repo().join("src/emacs");
    let program = crate::test_support::oracle_program_ascii(program);
    let output = std::process::Command::new(&binary)
        .args(["--batch", "-Q", "--eval", &program])
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
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn assert_upstream_primitive_contract(program: &str, expected: &str) {
    assert_eq!(
        upstream_primitive_contract_output(program),
        expected,
        "oracle disagreed; program sent was:\n{program}"
    );
}

/// Ask the pinned oracle a question and return its stdout verbatim.  For
/// contract elements that are properties of the oracle's OWN build or host
/// libraries (configure-time paths, linked-library versions, per-build
/// fboundp), the honest expectation is the oracle's live answer, not a
/// literal transcribed from whichever container the test was written on.
fn upstream_oracle_stdout(program: &str) -> String {
    crate::test_support::mark_process_test();
    let binary = upstream_emacs_repo().join("src/emacs");
    let program = crate::test_support::oracle_program_ascii(program);
    let output = std::process::Command::new(&binary)
        .args(["--batch", "-Q", "--eval", &program])
        .output()
        .unwrap_or_else(|error| panic!("run oracle probe {}: {error}", binary.display()));
    assert!(
        output.status.success(),
        "oracle probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn assert_upstream_primitive_contract_with_stdin(program: &str, stdin: &str, expected: &str) {
    crate::test_support::mark_process_test();
    let binary = upstream_emacs_repo().join("src/emacs");
    let program = crate::test_support::oracle_program_ascii(program);
    let mut child = std::process::Command::new(&binary)
        .args(["--batch", "-Q", "--eval", &program])
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
fn character_table_literal_materializes_nested_bytecode_decoder() {
    let decoder = Value::ReaderForm(std::rc::Rc::new(crate::lisp::types::ReaderForm::Closure {
        kind: crate::lisp::types::ReaderClosureKind::ByteCode,
        slots: vec![
            Value::Integer(0),
            Value::String(String::new().into()),
            Value::list([Value::symbol("vector-literal")]),
            Value::Integer(0),
        ],
    }));
    let mut fields = vec![Value::Nil; 70];
    fields[2] = Value::symbol("char-code-property-table");
    fields[68] = Value::symbol("name");
    fields[69] = decoder;
    let literal = Value::ReaderForm(std::rc::Rc::new(
        crate::lisp::types::ReaderForm::CharTable { fields },
    ));

    let mut interp = Interpreter::new();
    let Value::CharTable(table_id) = materialize_read_char_table_literals(&mut interp, &literal)
        .expect("materialize a character table with a bytecode decoder")
    else {
        panic!("character-table syntax must produce a typed table");
    };
    let Some(Value::Record(decoder_id)) = interp.char_table_extra_slot(table_id, 1) else {
        panic!("the nested decoder must be a typed byte-code-function object");
    };
    assert_eq!(
        interp.find_record(decoder_id).map(|record| record.kind),
        Some(crate::lisp::eval::RecordKind::Closure)
    );
}

#[test]
fn bytecode_closure_aref_and_func_arity_preserve_gnu_argument_descriptors() {
    // GNU 30.2 data.c:Faref returns CLOSURE_ARGLIST verbatim, while
    // eval.c:lambda_arity delegates integer descriptors to
    // bytecode.c:get_byte_code_arity.  Cover packed fixed/rest descriptors
    // and the legacy dynamic-binding arglist independently.
    let contract = r#"
        (let ((zero (make-byte-code 0 "" [] 0))
              (fixed (make-byte-code 513 "" [] 0))
              (rest (make-byte-code 128 "" [] 0))
              (legacy
               (make-byte-code '(a &optional b &rest c) "" [] 0)))
          (list (aref zero 0) (func-arity zero)
                (aref fixed 0) (func-arity fixed)
                (aref rest 0) (func-arity rest)
                (aref legacy 0) (func-arity legacy)))
    "#;
    let expected = "(0 (0 . 0) 513 (1 . 2) 128 (0 . many) (a &optional b &rest c) (1 . many))";
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);

    let mut interp = Interpreter::new();
    let form = Reader::new(contract)
        .read()
        .expect("bytecode argument descriptor contract should parse")
        .expect("bytecode argument descriptor contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("bytecode argument descriptor contract should evaluate")
            .to_string(),
        expected
    );
}

#[test]
fn compiled_time_string_results_keep_text_property_mutation() {
    // GNU timefns.c allocates mutable Lisp strings: multibyte for
    // Fformat_time_string and unibyte for Fcurrent_time_string.  A
    // byte-compiled lexical local does not pass through the interpreter's
    // value-storage upgrade, so returning immutable host text here silently
    // discarded ERC's timestamp properties.
    let program = r#"
        (progn
          (require 'bytecomp)
          (funcall
           (byte-compile
            (lambda ()
              (let ((formatted (format-time-string "%H:%M" 704591940 t))
                    (current (current-time-string 704591940 t)))
                (dolist (s (list formatted current))
                  (put-text-property 0 (length s) 'invisible 'timestamp s))
                (list
                 (list (get-text-property 0 'invisible formatted)
                       (object-intervals formatted)
                       (multibyte-string-p formatted))
                 (list (get-text-property 0 'invisible current)
                       (object-intervals current)
                       (multibyte-string-p current))))))))
    "#;
    let expected = concat!(
        "((timestamp ((0 5 (invisible timestamp))) t) ",
        "(timestamp ((0 24 (invisible timestamp))) nil))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read compiled time-string program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate compiled time-string program");
    assert_eq!(result.to_string(), expected);
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
fn compare_buffer_substrings_uses_dynamic_case_folding_and_the_canonical_table() {
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(
        r#"
        (with-temp-buffer
          (insert "Aza")
          (list
           (let ((case-fold-search nil))
             (compare-buffer-substrings nil 1 2 nil 3 4))
           (let ((case-fold-search t))
             (compare-buffer-substrings nil 1 2 nil 3 4))
           (let ((case-fold-search t))
             (compare-buffer-substrings nil 1 3 nil 3 4))
           (let ((case-fold-search t))
             (compare-buffer-substrings nil 3 4 nil 1 3))
           (let* ((table (copy-sequence (current-case-table)))
                  (canonical (make-char-table 'case-table)))
             (set-char-table-range canonical ?A ?z)
             (set-char-table-range canonical ?z ?z)
             (set-char-table-extra-slot table 1 canonical)
             (set-case-table table)
             (let ((case-fold-search t))
               (compare-buffer-substrings nil 1 2 nil 2 3)))))
        "#,
    )
    .read_all()
    .expect("case-folded substring comparison probe should parse")
    .remove(0);

    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("case-folded substring comparison should evaluate"),
        Value::list([
            Value::Integer(-1),
            Value::Integer(0),
            Value::Integer(2),
            Value::Integer(-2),
            Value::Integer(0),
        ])
    );
}

#[test]
fn subr_frontier_compare_strings_uses_gnu_simple_upcase_canonicalization() {
    let program = r#"(prin1 (list
      (compare-strings "Όσος" nil nil "ΌΣΟΣ" nil nil t)
      (compare-strings "ẞ" nil nil "ß" nil nil t)))"#;
    assert_upstream_primitive_contract(program, "(t t)");

    let mut interp = Interpreter::new();
    let form = Reader::new(&program[7..program.len() - 1])
        .read()
        .expect("comparison contract should parse")
        .expect("comparison contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("comparison contract should evaluate"),
        Value::list([Value::T, Value::T])
    );
}

#[test]
fn subr_frontier_reader_decodes_the_complete_classic_string_escape_table() {
    let contract = r#"(list (string-to-list "\a\b\d\e\f\n\r\t\v")
                  (split-string "vd jc"))"#;
    let expected = "((7 8 127 27 12 10 13 9 11) (\"vd\" \"jc\"))";
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(contract)
        .read()
        .expect("classic reader escape contract should parse")
        .expect("classic reader escape contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("classic reader escape contract should evaluate")
            .to_string(),
        expected
    );
}

#[test]
fn subr_frontier_delete_reuses_retained_cons_cells_and_copies_other_sequences() {
    let contract = r#"(let* ((xs (list "a" "a" "b" "b" "a" "c"))
                  (first-b (cddr xs))
                  (result (delete "a" xs)))
             (list (eq result first-b)
                   (equal result '("b" "b" "c"))
                   (delete ?a "aba")
                   (delete 'a [a b a])))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), "(t t \"b\" [b])");

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let form = Reader::new(contract)
        .read()
        .expect("delete contract should parse")
        .expect("delete contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("delete contract should evaluate")
            .to_string(),
        r#"(t t "b" [b])"#
    );
}

#[test]
fn subr_frontier_buffer_local_value_signals_when_no_binding_exists() {
    let contract = r#"(let ((buffer (generate-new-buffer "boundp-owner")))
             (unwind-protect
                 (condition-case error
                     (buffer-local-value 'emaxx-never-bound buffer)
                   (void-variable (car error)))
               (kill-buffer buffer)))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), "void-variable");

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let form = Reader::new(contract)
        .read()
        .expect("buffer-local-value contract should parse")
        .expect("buffer-local-value contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("void-variable should be caught"),
        Value::Symbol("void-variable".into())
    );
}

#[test]
fn subr_frontier_mapbacktrace_with_an_absent_base_is_an_empty_traversal() {
    let contract = r#"(let ((called nil) (base (make-symbol "absent")))
             (list (mapbacktrace (lambda (&rest _) (setq called t)) base)
                   called))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), "(nil nil)");

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let form = Reader::new(contract)
        .read()
        .expect("mapbacktrace contract should parse")
        .expect("mapbacktrace contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("mapbacktrace contract should evaluate"),
        Value::list([Value::Nil, Value::Nil])
    );
}

#[test]
fn subr_frontier_replace_match_applies_gnu_case_adaptation() {
    let contract = r#"(let ((source "Beta"))
             (string-match "B\\(..\\)a" source)
             (list (replace-match "carrot" nil nil source)
                   (replace-match "carrot" t nil source)
                   (replace-match "m\\1a" nil nil source)))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {contract})"),
        "(\"Carrot\" \"carrot\" \"Meta\")",
    );

    let mut interp = Interpreter::new();
    let form = Reader::new(contract)
        .read()
        .expect("replace-match contract should parse")
        .expect("replace-match contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("replace-match contract should evaluate")
            .to_string(),
        r#"("Carrot" "carrot" "Meta")"#
    );
}

#[test]
fn subr_frontier_recordp_does_not_expose_hash_table_runtime_storage() {
    let contract = "(list (recordp (make-hash-table)) (recordp #s(sample value)))";
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), "(nil t)");

    let mut interp = Interpreter::new();
    let form = Reader::new(contract)
        .read()
        .expect("record predicate contract should parse")
        .expect("record predicate contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("record predicate contract should evaluate"),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn subr_frontier_direct_vector_evaluation_materializes_nested_record_literals() {
    let contract = r#"(let ((vector [#s(sample value)]))
             (list (recordp (aref vector 0))
                   (prin1-to-string vector)))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {contract})"),
        r#"(t "[#s(sample value)]")"#,
    );

    let mut interp = Interpreter::new();
    let form = Reader::new(contract)
        .read()
        .expect("nested record vector contract should parse")
        .expect("nested record vector contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("nested record vector contract should evaluate")
            .to_string(),
        r#"(t "[#s(sample value)]")"#
    );
}

#[test]
fn subr_frontier_aset_promotes_ascii_unibyte_strings_like_gnu() {
    let contract = r#"(let ((ascii (string-as-unibyte "a"))
                   (byte8 (string-as-unibyte "a"))
                   (raw (unibyte-string #x80))
                   (multi (string-to-multibyte "é")))
             (aset ascii 0 ?ƒ)
             (aset byte8 0 #x3fffc9)
             (aset multi 0 ?a)
             (list ascii
                   (multibyte-string-p ascii)
                   (aref ascii 0)
                   (multibyte-string-p byte8)
                   (aref byte8 0)
                   multi
                   (multibyte-string-p multi)
                   (condition-case error
                       (aset raw 0 ?ƒ)
                     (args-out-of-range
                      (list (car error) (nth 2 error))))))"#;
    let expected = r#"("ƒ" t 402 t 4194249 "a" t (args-out-of-range 402))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);

    let mut interp = Interpreter::new();
    let form = Reader::new(contract)
        .read()
        .expect("aset string-promotion contract should parse")
        .expect("aset string-promotion contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("aset string-promotion contract should evaluate")
            .to_string(),
        expected
    );
}

#[test]
fn make_string_and_aset_share_the_internal_character_encoding() {
    assert_upstream_primitive_contract(
        "(let ((s (make-string 1 ?a)) (c (max-char)))\
           (aset s 0 c)\
           (prin1 (list (equal s (make-string 1 c)) (aref s 0))))",
        "(t 4194303)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        "(let ((s (make-string 1 ?a)) (c (max-char)))\
           (aset s 0 c)\
           (list (equal s (make-string 1 c)) (aref s 0)))",
    )
    .read_all()
    .expect("read internal-character string contract")
    .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("construct and mutate the same internal character"),
        Value::list([Value::T, Value::Integer(0x3f_ffff)]),
    );
}

#[test]
fn overlay_properties_accept_nil_keys_and_accessible_endpoints() {
    assert_upstream_primitive_contract(
        "(with-temp-buffer\
           (insert \"foo\")\
           (let ((end (make-overlay (point-max) (point-max)))\
                 (middle (make-overlay 2 2)))\
             (overlay-put middle nil 4)\
             (narrow-to-region 1 2)\
             (prin1 (list (overlay-get middle nil)\
                          (length (overlays-in 2 2))\
                          (length (overlays-in 4 4))))))",
        "(4 1 0)",
    );

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(
        "(with-temp-buffer\
           (insert \"foo\")\
           (let ((end (make-overlay (point-max) (point-max)))\
                 (middle (make-overlay 2 2)))\
             (overlay-put middle nil 4)\
             (narrow-to-region 1 2)\
             (list (overlay-get middle nil)\
                   (length (overlays-in 2 2))\
                   (length (overlays-in 4 4)))))",
    )
    .read_all()
    .expect("read overlay endpoint contract")
    .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("query nil overlay property and accessible endpoint"),
        Value::list([Value::Integer(4), Value::Integer(1), Value::Integer(0)]),
    );
}

#[test]
fn overlay_property_keys_use_lisp_identity() {
    assert_upstream_primitive_contract(
        "(with-temp-buffer
           (let* ((overlay (make-overlay 1 1))
                  (key (copy-sequence \"key\"))
                  (equal-key (copy-sequence key)))
             (overlay-put overlay key 4)
             (prin1 (list (overlay-get overlay key)
                          (overlay-get overlay equal-key)))))",
        "(4 nil)",
    );

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(
        "(with-temp-buffer
           (let* ((overlay (make-overlay 1 1))
                  (key (copy-sequence \"key\"))
                  (equal-key (copy-sequence key)))
             (overlay-put overlay key 4)
             (list (overlay-get overlay key)
                   (overlay-get overlay equal-key))))",
    )
    .read_all()
    .expect("read overlay property-key identity contract")
    .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("preserve overlay property-key identity"),
        Value::list([Value::Integer(4), Value::Nil]),
    );
}

#[test]
fn kill_buffer_queries_before_the_interactive_modified_prompt() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(
        "(progn (require 'cl-lib)\
         (let ((victim (get-buffer-create \" kill-query-order\")) events)\
           (with-current-buffer victim\
             (setq buffer-file-name \"visited\")\
             (insert \"changed\")\
             (add-hook 'kill-buffer-query-functions\
                       (lambda () (push 'query events) t) nil t))\
           (cl-letf (((symbol-function 'kill-buffer--possibly-save)\
                      (lambda (_) (push 'modified events) t)))\
             (call-interactively\
              (lambda () (interactive) (kill-buffer victim))))\
           (nreverse events)))",
    )
    .read_all()
    .expect("read kill-buffer query ordering contract")
    .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("query hooks precede the modified-buffer prompt"),
        Value::list([
            Value::Symbol("query".into()),
            Value::Symbol("modified".into()),
        ]),
    );
}

#[test]
fn noninteractive_kill_buffer_does_not_prompt_for_a_modified_file() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(
        "(progn (require 'cl-lib)\
         (let ((victim (get-buffer-create \" kill-no-prompt\")) prompted)\
           (with-current-buffer victim\
             (setq buffer-file-name \"visited\")\
             (insert \"changed\"))\
           (cl-letf (((symbol-function 'kill-buffer--possibly-save)\
                      (lambda (_) (setq prompted t) nil)))\
             (list (kill-buffer victim) prompted (buffer-live-p victim)))))",
    )
    .read_all()
    .expect("read noninteractive kill-buffer contract")
    .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("kill a modified file buffer without an interactive prompt"),
        Value::list([Value::T, Value::Nil, Value::Nil]),
    );
}

#[test]
fn kill_buffer_restores_the_current_buffer_when_a_query_signals() {
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(
        "(let ((caller (current-buffer))
               (victim (get-buffer-create \" kill-query-error\")))
           (with-current-buffer victim
             (add-hook 'kill-buffer-query-functions
                       (lambda () (error \"query failed\")) nil t))
           (condition-case nil
               (kill-buffer victim)
             (error nil))
           (list (eq (current-buffer) caller) (buffer-live-p victim)))",
    )
    .read_all()
    .expect("read kill-buffer error-unwind contract")
    .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("restore caller after a kill query signals"),
        Value::list([Value::T, Value::T]),
    );
}

#[test]
fn subr_frontier_replace_match_distinguishes_string_and_buffer_escapes() {
    let contract = r#"(let ((source "aba"))
             (string-match "a" source)
             (list
              (replace-match "\\\\,\\?" nil nil source)
              (condition-case error
                  (replace-match "\\x" nil nil source)
                (error (car error)))
              (condition-case error
                  (with-temp-buffer
                    (insert "a")
                    (goto-char (point-min))
                    (re-search-forward "a")
                    (replace-match "\\?" nil nil)
                    (buffer-string))
                (error (car error)))))"#;
    let expected = r#"("\\,\\?ba" error error)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let form = Reader::new(contract)
        .read()
        .expect("replace-match escape contract should parse")
        .expect("replace-match escape contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("replace-match escape contract should evaluate"),
        Value::list([
            Value::String("\\,\\?ba".into()),
            Value::Symbol("error".into()),
            Value::Symbol("error".into()),
        ])
    );
}

#[test]
fn subr_frontier_fundamental_mode_is_not_a_stored_derived_parent() {
    let contract = r#"(progn
             (define-derived-mode emaxx-sample-root fundamental-mode "Root")
             (define-derived-mode emaxx-sample-child emaxx-sample-root "Child")
             (list (get 'emaxx-sample-root 'derived-mode-parent)
                   (derived-mode-all-parents 'emaxx-sample-child)))"#;
    let expected = "(nil (emaxx-sample-child emaxx-sample-root))";
    assert_upstream_primitive_contract(
        &format!("(progn (require 'derived) (prin1 {contract}))"),
        expected,
    );

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(contract)
        .read()
        .expect("derived-mode parent contract should parse")
        .expect("derived-mode parent contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("derived-mode parent contract should evaluate")
            .to_string(),
        expected
    );
}

#[test]
fn subr_frontier_backquote_folds_the_constant_suffix_like_gnu() {
    let contract = "(macroexpand '`(a ,x b ,y 0 font-lock-keyword-face))";
    let expected = "(cons 'a (cons x (cons 'b (cons y '(0 font-lock-keyword-face)))))";
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let form = Reader::new(contract)
        .read()
        .expect("backquote suffix contract should parse")
        .expect("backquote suffix contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("backquote suffix contract should evaluate")
            .to_string(),
        expected
    );
}

#[test]
fn subr_frontier_hook_execution_uses_elisp_owned_local_value_cells() {
    let contract = r#"(progn
             (setq emaxx-hook-log nil)
             (fset 'emaxx-global-hook
                   (lambda () (push 'global emaxx-hook-log)))
             (fset 'emaxx-local-before
                   (lambda () (push 'local-before emaxx-hook-log)))
             (fset 'emaxx-local-after
                   (lambda () (push 'local-after emaxx-hook-log)))
             (setq-default emaxx-owner-hook '(emaxx-global-hook))
             (with-temp-buffer
               (make-local-variable 'emaxx-owner-hook)
               ;; Deliberately bypass native add-hook bookkeeping, as GNU's
               ;; complete subr.el owner is entitled to do.
               (setq emaxx-owner-hook
                     '(emaxx-local-before t emaxx-local-after))
               (run-hooks 'emaxx-owner-hook)
               (nreverse emaxx-hook-log)))"#;
    let expected = "(local-before global local-after)";
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let form = Reader::new(contract)
        .read()
        .expect("Elisp-owned hook contract should parse")
        .expect("Elisp-owned hook contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("Elisp-owned hook contract should evaluate")
            .to_string(),
        expected
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let source = fs::canonicalize("../emacs/lisp/subr.el")
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
    let is_native_mirror = |name: &str| has_dispatch_handler(name) || is_special_form_name(name);
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
fn every_literal_native_dispatch_arm_is_owned_by_gnu_c() {
    let quoted_name =
        regex::Regex::new(r#"\"([^\"]+)\""#).expect("compile native-dispatch pattern extractor");
    let mut patterns = Vec::new();
    super::dispatch::visit_handled_patterns(&mut |module, pattern| {
        patterns.push((module, pattern))
    });

    let mut names = std::collections::BTreeMap::new();
    let mut nonliteral_patterns = Vec::new();
    for (module, pattern) in patterns {
        let extracted = quoted_name
            .captures_iter(pattern)
            .map(|capture| capture[1].to_string())
            .collect::<Vec<_>>();
        if extracted.is_empty() {
            nonliteral_patterns.push(format!("{module}: {pattern}"));
        } else {
            for name in extracted {
                names.insert(name, module);
            }
        }
    }
    assert!(
        nonliteral_patterns.is_empty(),
        "native dispatch contains patterns that the ownership audit cannot enumerate: {}",
        nonliteral_patterns.join(", ")
    );

    // Dispatch source is a cross-platform union.  An arm must belong to at
    // least one contracted GNU host, while `is_builtin' separately selects
    // only the current host's generated ownership manifest.
    let non_gnu = names
        .iter()
        .filter(|(name, _)| {
            super::generated_gnu_c_primitives::generated_gnu_c_primitive_available(name)
                != Some(true)
                && super::generated_gnu_c_primitives_linux::generated_gnu_c_primitive_available(
                    name,
                ) != Some(true)
        })
        .map(|(name, module)| format!("{module}: {name}"))
        .collect::<Vec<_>>();
    assert!(
        non_gnu.is_empty(),
        "{} native dispatch arms are not owned by GNU C:\n{}",
        non_gnu.len(),
        non_gnu.join("\n")
    );
}

#[test]
fn file_notification_primitives_follow_the_host_contract() {
    #[cfg(target_os = "macos")]
    {
        assert!(is_builtin("kqueue-add-watch"));
        assert!(!is_builtin("inotify-add-watch"));
    }
    #[cfg(target_os = "linux")]
    {
        assert!(is_builtin("inotify-add-watch"));
        assert!(!is_builtin("kqueue-add-watch"));
    }
}

#[test]
fn native_dispatch_fails_closed_at_the_gnu_c_boundary() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    for name in [
        "semantic-go-to-tag",
        "srecode-template-get-table",
        "bounds-of-thing-at-point",
        "advice-add",
        "advice-remove",
        "advice-member-p",
        "add-function",
        "remove-function",
        "define-advice",
        concat!("emaxx", "--apply-around-advice"),
        concat!("emaxx", "--apply-after-advice"),
        concat!("emaxx", "--cl-generic-remove-loadhist-method"),
        concat!("emaxx", "--oclosure-slot"),
    ] {
        assert!(
            !is_builtin(name),
            "{name} is not a GNU C primitive and must not be Lisp-callable Rust"
        );
        assert!(
            !matches!(
                interp.raw_function_binding(name, &env),
                Some(Value::BuiltinFunc(_))
            ),
            "{name} acquired a native function cell outside GNU's C manifest"
        );
        assert!(
            matches!(
                call(&mut interp, name, &[], &mut env),
                Err(LispError::Signal(message)) if message == format!("Unknown function: {name}")
            ),
            "direct dispatch bypassed the GNU C ownership boundary for {name}"
        );
    }
}

#[test]
fn generated_rust_manifests_never_contain_trailing_whitespace() {
    for (name, source) in [(
        "builtin arities",
        include_str!("generated_builtin_arities.rs"),
    )] {
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
fn project_does_not_ship_lisp_compatibility_facades() {
    let source_dir = crate::compat::project_root().join("src/lisp");
    let mut facades = walkdir::WalkDir::new(&source_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.into_path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "el"))
        .collect::<Vec<_>>();
    facades.sort();
    assert!(
        facades.is_empty(),
        "project-owned Lisp compatibility facades can mask missing GNU owners: {facades:?}"
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();

    assert_eq!(
        call_via_lisp(
            &mut interp,
            "subregexp-context-p",
            &[Value::String("a[b]".into()), Value::Integer(2)],
            &mut env,
        )
        .expect("inside a character class is not a subregexp context"),
        Value::Nil
    );
    assert_eq!(
        call_via_lisp(
            &mut interp,
            "subregexp-context-p",
            &[Value::String(r"a\(".into()), Value::Integer(3)],
            &mut env,
        )
        .expect("unfinished group is still a subregexp context"),
        Value::T
    );
    assert_eq!(
        call_via_lisp(
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
                     (string-to-syntax "-")
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
        "((1) (0) (983041) (983044 . 93) nil (8388609) (8388609) (8388609) (8388609) (error \"Invalid syntax description letter: z\"))",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "string-to-syntax",
            &[Value::String("-".into())],
            &mut env,
        )
        .expect("hyphen whitespace syntax alias"),
        Value::list([Value::Integer(0)])
    );
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

    // `frame-windows-min-size' is window.el Lisp in GNU; only the Darwin
    // contract lists it with an arity.  On a host whose C contract does not
    // own it, the interpreter needs the dumped Lisp to answer, as GNU does.
    let mut interp = if is_builtin("frame-windows-min-size") {
        Interpreter::new()
    } else {
        crate::test_support::initialized_upstream_batch_interpreter()
    };
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
        (progn
          (set-terminal-coding-system-internal 'utf-8-unix)
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
             (error error-data)))))"#;
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let mut actual = [
        "coding-system-p",
        "coding-system-type",
        "coding-system-base",
        "coding-system-eol-type",
    ]
    .into_iter()
    .map(|name| {
        // mule.el owns these accessors over the C-owned plist; reach them
        // through the function cell as GNU `funcall' does.
        call_via_lisp(&mut interp, name, &[Value::Nil], &mut env)
            .unwrap_or_else(|error| panic!("{name} nil: {error}"))
    })
    .collect::<Vec<_>>();
    actual.push(
        call_via_lisp(
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let result = call_via_lisp(
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
    assert_eq!(char_charset(&interp, 'A' as u32).0, "ascii");
    assert_eq!(char_charset(&interp, 'あ' as u32).0, "unicode");

    interp
        .define_charset_alias("latin", "ascii")
        .expect("ascii alias should be accepted");
    assert!(interp.has_charset("latin"));

    interp.set_charset_priority(&["ascii".into(), "unicode".into()]);
    // charset.c Fset_charset_priority moves the given charsets to the
    // front and keeps every other charset in its old relative order.
    assert_eq!(
        interp.charset_priority_list(),
        vec!["ascii", "unicode", "iso-8859-1", "emacs", "eight-bit"]
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
    crate::test_support::run_with_large_stack(
        unicode_char_property_helpers_cover_names_and_general_categories_inner,
    );
}

fn unicode_char_property_helpers_cover_names_and_general_categories_inner() {
    assert_upstream_primitive_contract(
        "(prin1 (mapcar (lambda (u)
                          (list u
                                (get-char-code-property u 'name)
                                (get-char-code-property u 'general-category)))
                        '(#x16100 #x1CC00 #x14646 #x2FFC #x4E00
                          #xD800 #xE000 #x10FFFF)))",
        "((90368 nil Cn) (117760 nil Cn) (83526 \"ANATOLIAN HIEROGLYPH A530\" Lo) (12284 \"IDEOGRAPHIC DESCRIPTION CHARACTER SURROUND FROM RIGHT\" So) (19968 \"CJK IDEOGRAPH-4E00\" Lo) (55296 \"HIGH SURROGATE-D800\" Cs) (57344 nil Co) (1114111 nil Cn))",
    );

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();

    let name = crate::test_support::call_lisp_function(
        &mut interp,
        &mut env,
        "get-char-code-property",
        &[
            Value::Integer('\u{2026}' as i64),
            Value::Symbol("name".into()),
        ],
    )
    .expect("get-char-code-property should return Unicode names");
    assert_eq!(name, Value::String("HORIZONTAL ELLIPSIS".into()));

    let category = crate::test_support::call_lisp_function(
        &mut interp,
        &mut env,
        "get-char-code-property",
        &[
            Value::Integer('\u{2026}' as i64),
            Value::Symbol("general-category".into()),
        ],
    )
    .expect("get-char-code-property should return Unicode general categories");
    assert_eq!(category, Value::Symbol("Po".into()));

    let description = crate::test_support::call_lisp_function(
        &mut interp,
        &mut env,
        "char-code-property-description",
        &[Value::Symbol("general-category".into()), category],
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
            crate::test_support::call_lisp_function(
                &mut interp,
                &mut env,
                "get-char-code-property",
                &[Value::Integer(code), Value::Symbol("name".into())],
            )
            .expect("read the Unicode 15.1 name property"),
            expected_name.map_or(Value::Nil, |name| Value::String(name.into()))
        );
        assert_eq!(
            crate::test_support::call_lisp_function(
                &mut interp,
                &mut env,
                "get-char-code-property",
                &[
                    Value::Integer(code),
                    Value::Symbol("general-category".into()),
                ],
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
    crate::test_support::run_with_large_stack(
        unicode_property_tables_are_stable_and_preserve_overrides_inner,
    );
}

fn unicode_property_tables_are_stable_and_preserve_overrides_inner() {
    assert_upstream_primitive_contract(
        "(let* ((property 'general-category)
                 (first (unicode-property-table-internal property))
                 (second (unicode-property-table-internal property)))
            (put-unicode-property-internal first ?A 'Po)
            (prin1 (list (eq first second)
                         (get-unicode-property-internal first ?A)
                         (get-char-code-property ?A property))))",
        "(t Po Po)",
    );

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let property = Value::Symbol("general-category".into());
    let table = call(
        &mut interp,
        "unicode-property-table-internal",
        std::slice::from_ref(&property),
        &mut env,
    )
    .expect("create the lazy Unicode category table");

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
            Value::Symbol("Po".into()),
        ],
        &mut env,
    )
    .expect("override a Unicode table entry");
    assert_eq!(
        call(
            &mut interp,
            "get-unicode-property-internal",
            &[table.clone(), Value::Integer('A' as i64)],
            &mut env,
        )
        .expect("read the Unicode table override"),
        Value::Symbol("Po".into())
    );
    assert_eq!(
        crate::test_support::call_lisp_function(
            &mut interp,
            &mut env,
            "get-char-code-property",
            &[Value::Integer('A' as i64), property],
        )
        .expect("read the override through the GNU Elisp owner"),
        Value::Symbol("Po".into())
    );
}

#[test]
fn unicode_property_internal_encoders_follow_gnu_decision_table() {
    let program = r#"
        (let ((character (make-char-table 'char-code-property-table))
              (run (make-char-table 'char-code-property-table))
              (numeric (make-char-table 'char-code-property-table))
              (plain (make-char-table 'char-code-property-table)))
          (set-char-table-extra-slot character 2 0)
          (put-unicode-property-internal character ?A ?B)
          (set-char-table-extra-slot run 1 0)
          (set-char-table-extra-slot run 2 1)
          (set-char-table-extra-slot run 4 [nil Lu Po])
          (put-unicode-property-internal run ?A 'Po)
          (set-char-table-extra-slot numeric 1 0)
          (set-char-table-extra-slot numeric 2 2)
          (set-char-table-extra-slot numeric 4 [10 20])
          (put-unicode-property-internal numeric ?A 99)
          (put-unicode-property-internal plain ?A 'raw)
          (list
           (get-unicode-property-internal character ?A)
           (condition-case error
               (put-unicode-property-internal character ?A 'bad)
             (error error))
           (aref run ?A)
           (get-unicode-property-internal run ?A)
           (condition-case error
               (put-unicode-property-internal run ?A 'Cc)
             (error error))
           (aref numeric ?A)
           (char-table-extra-slot numeric 4)
           (get-unicode-property-internal numeric ?A)
           (condition-case error
               (put-unicode-property-internal numeric ?A 1.5)
             (error error))
           (get-unicode-property-internal plain ?A)))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "(66 (wrong-type-argument integerp bad) 2 Po (wrong-type-argument \"Unicode property value\" Cc) 2 [10 20 2] 2 (wrong-type-argument fixnump 1.5) raw)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("Unicode encoder contract should parse")
        .expect("Unicode encoder contract should contain a form");
    let result = interp
        .eval(&form, &mut env)
        .expect("Unicode encoders should follow the GNU decision table");
    let expected = Reader::new(
        "(66 (wrong-type-argument integerp bad) 2 Po
             (wrong-type-argument \"Unicode property value\" Cc)
             2 [10 20 2] 2 (wrong-type-argument fixnump 1.5) raw)",
    )
    .read()
    .expect("expected Unicode encoder result should parse")
    .expect("expected Unicode encoder result should contain a form");
    assert_eq!(result, expected);
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
                "string-match",
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
fn syntax_word_class_rendering_is_shared_and_invalidated_at_table_mutation() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    regexp::reset_regexp_syntax_class_render_count();
    for pattern in ["\\w", "\\W", "[[:word:]]", "\\sw"] {
        call(
            &mut interp,
            "string-match",
            &[Value::String(pattern.into()), Value::String("word!".into())],
            &mut env,
        )
        .expect("match a syntax-table-dependent regexp");
    }
    assert_eq!(
        regexp::regexp_syntax_class_render_count(),
        1,
        "different patterns must share one current-table word-class rendering"
    );

    call(
        &mut interp,
        "modify-syntax-entry",
        &[Value::Integer('!' as i64), Value::String("w".into())],
        &mut env,
    )
    .expect("mutate the current syntax table");
    assert_eq!(
        call(
            &mut interp,
            "string-match",
            &[Value::String("\\w".into()), Value::String("!".into())],
            &mut env,
        )
        .expect("match through the updated syntax table"),
        Value::Integer(0)
    );
    assert_eq!(
        regexp::regexp_syntax_class_render_count(),
        2,
        "any table mutation must invalidate the derived rendering"
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
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
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
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    crate::test_support::replace_with_gnu_batch_runtime(&mut interp);
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
fn dumped_directory_family_ignores_the_test_harness_variable() {
    // Finding 102: data-directory, doc-directory, installation-directory
    // and emacsclient-program-name were derived from EMACS_TEST_DIRECTORY
    // -- exactly the rule source-directory's own comment bans.  In GNU
    // they are epaths.h constants fixed when the binary is built; here
    // that means the pinned sibling checkout's paths, whatever the
    // harness environment says.
    let repo = crate::compat::canonicalize_path(&upstream_emacs_repo())
        .expect("sibling GNU checkout")
        .display()
        .to_string();
    let program = concat!(
        "(prin1 (list data-directory doc-directory installation-directory ",
        "emacsclient-program-name))"
    );
    let expected = format!("(\"{repo}/etc/\" \"{repo}/etc/\" \"{repo}/\" \"emacsclient\")");
    assert_upstream_primitive_contract(program, &expected);

    // A hostile EMACS_TEST_DIRECTORY pointing at a fake repo layout --
    // complete with the binaries the old derivation looked for -- must
    // change none of them.
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let fake_root = std::env::temp_dir().join(format!("emaxx-compat-paths-{unique}"));
    let test_dir = fake_root.join("test");
    std::fs::create_dir_all(&test_dir).expect("create test directory");
    std::fs::create_dir_all(fake_root.join("src")).expect("create src directory");
    std::fs::create_dir_all(fake_root.join("lib-src")).expect("create lib-src directory");
    std::fs::create_dir_all(fake_root.join("etc")).expect("create etc directory");
    std::fs::write(fake_root.join("src/emacs"), "").expect("write fake emacs binary");
    std::fs::write(fake_root.join("lib-src/emacsclient"), "")
        .expect("write fake emacsclient binary");

    let _env_write = crate::compat::lock_boot_environment_for_write();
    let old = std::env::var("EMACS_TEST_DIRECTORY").ok();
    unsafe {
        std::env::set_var("EMACS_TEST_DIRECTORY", test_dir.display().to_string());
    }
    assert_eq!(
        current_invocation_path(),
        std::env::current_exe().expect("current test executable"),
        "EMACS_TEST_DIRECTORY must never redirect Emaxx subprocesses to the GNU oracle"
    );
    assert_eq!(compat_data_directory(), Some(format!("{repo}/etc/")));
    assert_eq!(compat_installation_directory(), Some(format!("{repo}/")));
    if let Some(value) = old {
        unsafe {
            std::env::set_var("EMACS_TEST_DIRECTORY", value);
        }
    } else {
        unsafe {
            std::env::remove_var("EMACS_TEST_DIRECTORY");
        }
    }
    let _ = std::fs::remove_dir_all(&fake_root);
    drop(_env_write);

    // The interpreter's dumped bindings answer the same oracle row.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let values = interp
        .eval(
            &Reader::new(
                "(list data-directory doc-directory installation-directory \
                 emacsclient-program-name)",
            )
            .read_all()
            .expect("read directory-family list")
            .remove(0),
            &mut Vec::new(),
        )
        .expect("evaluate directory-family list");
    assert_eq!(values.to_string(), expected);
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
fn insert_file_contents_replace_collapses_point_in_the_differing_middle() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let path = std::env::temp_dir().join(format!(
        "emaxx-replace-point-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::write(&path, "alpha beta\n").expect("write replace fixture");
    interp.buffer.insert("changed alpha beta\n");
    interp.buffer.goto_char(9);

    call(
        &mut interp,
        "insert-file-contents",
        &[
            Value::String(path.display().to_string().into()),
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::Symbol("if-regular".into()),
        ],
        &mut env,
    )
    .expect("replace buffer contents");

    assert_eq!(interp.buffer.buffer_string(), "alpha beta\n");
    assert_eq!(interp.buffer.point(), interp.buffer.point_min());
    let _ = std::fs::remove_file(path);
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
    // Only the w32 and NS builds DEFUN this primitive; on a host whose C
    // contract lacks it (the X oracle), GNU and Emaxx both answer
    // `void-function'.
    if !is_builtin("system-move-file-to-trash") {
        let condition = crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            "(condition-case e (system-move-file-to-trash \"/nonexistent/zz\") (error e))",
        )
        .expect("void-function must be catchable")
        .to_string();
        assert_eq!(condition, "(void-function system-move-file-to-trash)");
        drop(error);
        return;
    }
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    interp.set_variable(
        "default-directory",
        Value::String(cwd.display().to_string().into()),
        &mut env,
    );

    let result = call_via_lisp(
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
    // This timing-sensitive real subprocess test failed only while competing
    // with the marked GNU-library integration class.  Share that class's
    // explicit permit; unrelated primitive tests remain parallel.
    let _permit = crate::test_support::acquire_exclusive_host_test_permit();
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*process-output*");
    let buffer = Value::buffer(buffer_id, buffer_name);

    let process = call_via_lisp(
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
    // starvation or endpoint scanning in the full parallel suite for a
    // process semantic failure.
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
            &[process.clone(), Value::Integer(60)],
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    // The probe's `ignore' sentinel is subr.el Lisp; run on the early
    // GNU-Lisp runtime rather than the file-less host.
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
fn make_process_accepts_nil_coding_like_emacs() {
    let _permit = crate::test_support::acquire_exclusive_host_test_permit();
    let program = r#"(let* ((default-directory temporary-file-directory)
                            (process
                             (make-process
                              :name "nil-coding"
                              :command '("/usr/bin/true")
                              :coding nil
                              :sentinel 'ignore)))
                       (unwind-protect
                           (processp process)
                         (delete-process process)))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "t");

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let form = Reader::new(program)
        .read()
        .expect("nil-coding process contract should parse")
        .expect("nil-coding process contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("nil :coding should use process coding defaults"),
        Value::T
    );
}

#[cfg(unix)]
#[test]
fn process_send_eof_uses_the_pty_eof_character_and_drains_final_output() {
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    interp.set_variable("captured-signal-event", Value::Nil, &mut env);
    let process = call_via_lisp(
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
fn explicit_process_filter_uses_and_restores_the_callers_current_buffer() {
    let program = r#"(let* ((observed nil)
                            (origin (current-buffer))
                            (other (get-buffer-create "*filter-other*"))
                            (process-buffer
                             (get-buffer-create "*filter-process*"))
                            (process
                            (make-process
                              :name "filter-current-buffer"
                              :buffer process-buffer
                              :command (list shell-file-name "-c" "printf x")
                              :connection-type 'pipe
                              :filter
                              (lambda (_process _output)
                                (setq observed (current-buffer))
                                (set-buffer other)))))
                       (while (process-live-p process)
                         (accept-process-output process 0.1))
                       (accept-process-output process 0.1)
                       (list (eq observed origin)
                             (eq (current-buffer) origin)
                             (eq (process-buffer process) process-buffer)))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(t t t)");

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("process filter buffer contract should parse")
        .expect("process filter buffer contract should contain a form");
    let expected = Reader::new("(t t t)")
        .read()
        .expect("expected filter buffer result should parse")
        .expect("expected filter buffer result should exist");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("process filter buffer contract should evaluate"),
        expected
    );
}

#[cfg(unix)]
#[test]
fn deleted_process_is_not_returned_for_buffer() {
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*deleted-process*");
    let buffer = Value::buffer(buffer_id, buffer_name);
    let process = call_via_lisp(
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();

    assert_eq!(
        call_via_lisp(
            &mut interp,
            "string-limit",
            &[Value::String("foobar".into()), Value::Integer(3)],
            &mut env,
        )
        .expect("string-limit should succeed"),
        Value::String("foo".into())
    );
    assert_eq!(
        call_via_lisp(
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    call_via_lisp(
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    call_via_lisp(
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
    let _permit = crate::test_support::acquire_exclusive_host_test_permit();
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let buffer = Value::buffer(interp.current_buffer_id(), String::new());
    let process = call_via_lisp(
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

    // Wait for the process event itself.  A wall-clock deadline makes this
    // test depend on when the host schedules a newly spawned shell during
    // the complete parallel suite; the exact seconds-only parsing contract
    // is asserted deterministically below.
    let accepted = call(
        &mut interp,
        "accept-process-output",
        std::slice::from_ref(&process),
        &mut env,
    )
    .expect("accept-process-output should wait for output");
    if accepted != Value::T {
        let process_id = interp
            .resolve_process_id(&process)
            .expect("resolve timed process");
        let (pending_stdout, pending_stderr) = interp
            .poll_process_output(process_id)
            .expect("inspect timed process output");
        panic!(
            "accept-process-output timed out: status={:?}, deliveries={:?}, pending_stdout={pending_stdout:?}, pending_stderr={pending_stderr:?}",
            interp.process_status_value(process_id),
            interp.process_output_delivery_count(process_id),
        );
    }
    // GNU's own contract here is timing-dependent (oracle probe apo1.el,
    // this container: 4/5 runs deliver the exit sentinel in the SAME
    // accept-process-output call as the output, 1/5 split them), so the
    // sentinel line is accepted but not required.
    let text = interp.buffer.full_buffer_string();
    let text = text
        .strip_suffix("\nProcess accept-output-test finished\n")
        .unwrap_or(&text);
    assert_eq!(text, "ready");
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let buffer = Value::buffer(interp.current_buffer_id(), String::new());
    let process = call_via_lisp(
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
    // GNU's own contract here is timing-dependent (oracle probe apo1.el,
    // this container: 4/5 runs deliver the exit sentinel in the SAME
    // accept-process-output call as the output, 1/5 split them), so the
    // sentinel line is accepted but not required.
    let text = interp.buffer.full_buffer_string();
    let text = text
        .strip_suffix("\nProcess accept-output-no-timeout finished\n")
        .unwrap_or(&text);
    assert_eq!(text, "ready");
}

#[test]
fn accept_process_output_ignores_distractor_output_until_target_delivers() {
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let target_buffer = call_via_lisp(
        &mut interp,
        "generate-new-buffer",
        &[Value::String(" *accept-target*".into())],
        &mut env,
    )
    .expect("create target buffer");
    let target_buffer_id = interp
        .resolve_buffer_id(&target_buffer)
        .expect("resolve target buffer");
    let distractor_buffer = call_via_lisp(
        &mut interp,
        "generate-new-buffer",
        &[Value::String(" *accept-distractor*".into())],
        &mut env,
    )
    .expect("create distractor buffer");
    let target = call_via_lisp(
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
    call_via_lisp(
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
    // GNU's own contract here is timing-dependent (oracle probe apo1.el,
    // this container: 4/5 runs deliver the exit sentinel in the SAME
    // accept-process-output call as the output, 1/5 split them), so the
    // sentinel line is accepted but not required.
    let text = interp
        .get_buffer_by_id(target_buffer_id)
        .expect("live target buffer")
        .full_buffer_string();
    let text = text
        .strip_suffix("\nProcess accept-target finished\n")
        .unwrap_or(&text);
    assert_eq!(text, "target");
}

#[test]
fn accept_process_output_just_this_one_suspends_distractor_filters_like_emacs() {
    let _permit = crate::test_support::acquire_exclusive_host_test_permit();
    let program = r#"(let* ((target-buffer (generate-new-buffer " *apo-target*"))
                            (distractor-buffer
                             (generate-new-buffer " *apo-distractor*"))
                            (target
                             (make-process
                              :name "apo-target" :buffer target-buffer
                              :command (list shell-file-name shell-command-switch
                                             "sleep 0.15; printf target")
                              :noquery t :sentinel #'ignore))
                            ;; The distractor must outlive the wait.  A
                            ;; distractor that exits during it changes the
                            ;; measurement: process.c's status_notify reads
                            ;; any output remaining from a process whose
                            ;; status changed, JUST-THIS-ONE or not, and
                            ;; whether that exit lands inside the 0.15 s
                            ;; window is host timing (the Linux oracle
                            ;; answers "distractor" there, Darwin "").
                            (distractor
                             (make-process
                              :name "apo-distractor" :buffer distractor-buffer
                              :command (list shell-file-name shell-command-switch
                                             "printf distractor; sleep 2")
                              :noquery t :sentinel #'ignore)))
                       (unwind-protect
                           (list
                            (accept-process-output target nil nil t)
                            (with-current-buffer distractor-buffer (buffer-string))
                            (with-current-buffer target-buffer (buffer-string))
                            (accept-process-output distractor 2)
                            (with-current-buffer distractor-buffer (buffer-string)))
                         (ignore-errors (delete-process target))
                         (ignore-errors (delete-process distractor))
                         (kill-buffer target-buffer)
                         (kill-buffer distractor-buffer)))"#;
    let expected = "(t \"\" \"target\" t \"distractor\")";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("JUST-THIS-ONE contract should parse")
        .expect("JUST-THIS-ONE contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("JUST-THIS-ONE contract should evaluate"),
        Value::list([
            Value::T,
            Value::String("".into()),
            Value::String("target".into()),
            Value::T,
            Value::String("distractor".into()),
        ])
    );
}

#[test]
fn zero_duration_sleep_does_not_dispatch_ready_process_output_like_emacs() {
    let _permit = crate::test_support::acquire_exclusive_host_test_permit();
    let program = r#"(let* ((buffer (generate-new-buffer " *sleep-zero*"))
                            (process
                             (make-process
                              :name "sleep-zero" :buffer buffer
                              :command (list shell-file-name shell-command-switch
                                             "printf ready")
                              :noquery t :sentinel #'ignore)))
                       (unwind-protect
                           (progn
                             (call-process "sleep" nil nil nil "0.1")
                             (sleep-for 0)
                             (list
                              (with-current-buffer buffer (buffer-string))
                              (accept-process-output process 1)
                              (with-current-buffer buffer (buffer-string))))
                         (ignore-errors (delete-process process))
                         (kill-buffer buffer)))"#;
    let expected = "(\"\" t \"ready\")";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("zero-duration sleep contract should parse")
        .expect("zero-duration sleep contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("zero-duration sleep contract should evaluate"),
        Value::list([
            Value::String("".into()),
            Value::T,
            Value::String("ready".into()),
        ])
    );
}

#[test]
fn accept_process_output_does_not_count_an_outputless_exit_as_delivery() {
    let program = r#"(let ((process
                            (make-process
                             :name "quiet-exit"
                             :command (list shell-file-name "-c" "exit 0")
                             :noquery t
                             :sentinel #'ignore)))
                       (list (accept-process-output nil 1)
                             (process-status process)))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(nil exit)");

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("outputless-exit contract should parse")
        .expect("outputless-exit contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("outputless-exit contract should evaluate"),
        Value::list([Value::Nil, Value::symbol("exit")])
    );
}

#[test]
fn make_network_process_requires_the_gnu_name_contract() {
    let program = "(prin1 (list (make-network-process)\
                   (condition-case err (make-network-process :server t) (error err))\
                   (condition-case err (make-network-process :name nil) (error err))))";
    let expected =
        "(nil (error \"Missing :name keyword parameter\") (error \":name value not a string\"))";
    assert_upstream_primitive_contract(program, expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    assert_eq!(
        call(&mut interp, "make-network-process", &[], &mut env)
            .expect("zero arguments follow GNU's nil fast path"),
        Value::Nil
    );

    let Err(LispError::Signal(missing)) = call(
        &mut interp,
        "make-network-process",
        &[Value::Symbol(":server".into()), Value::T],
        &mut env,
    ) else {
        panic!("a nonempty argument list without :name must fail");
    };
    assert_eq!(missing, "Missing :name keyword parameter");

    let Err(LispError::Signal(not_string)) = call(
        &mut interp,
        "make-network-process",
        &[Value::Symbol(":name".into()), Value::Nil],
        &mut env,
    ) else {
        panic!("a non-string :name must fail");
    };
    assert_eq!(not_string, ":name value not a string");
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
    // GNU's network suites guard IPv6 scenarios with `skip-unless' on a
    // trial bind; some containers have no IPv6 stack at all, and GNU
    // itself cannot bind there either ("Address family not supported by
    // protocol").  Probe the host live and skip the same way.
    if std::net::TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, 0)).is_err() {
        eprintln!("skipping: this host cannot bind an IPv6 loopback listener");
        return;
    }
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let buffer = call_via_lisp(
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "a\nb\n");
    interp.buffer.goto_char(interp.buffer.point_max());
    let mut env = Vec::new();

    call_via_lisp(
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "one\ntwo\nthree\n");
    let mut env = Vec::new();
    interp.buffer.goto_char(6);

    call_via_lisp(&mut interp, "delete-line", &[], &mut env).expect("delete-line should succeed");

    assert_eq!(
        interp
            .buffer
            .buffer_substring(interp.buffer.point_min(), interp.buffer.point_max())
            .expect("buffer contents"),
        "one\nthree\n"
    );
}

#[test]
fn make_button_signals_on_an_incomplete_range() {
    // GNU probe: (make-button nil 3 'type 'sample) signals
    // wrong-type-argument — button.el hands BEG to the C text-property
    // primitives, which reject a nil position.  An earlier Emaxx facade
    // returned nil here instead.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "button");
    let mut env = Vec::new();

    let error = call_via_lisp(
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
    .expect_err("a nil button start must signal");
    assert_eq!(error.condition_type(), "wrong-type-argument");
}

#[test]
fn looking_at_p_preserves_existing_match_data() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let result = call_via_lisp(
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
fn native_sqlite_errors_publish_the_gnu_condition_hierarchy() {
    let program = r#"
        (let ((db (sqlite-open)))
          (unwind-protect
              (progn
                (sqlite-execute db "create table test (a)")
                (list
                 (get 'sqlite-error 'error-conditions)
                 (get 'sqlite-error 'error-message)
                 (get 'sqlite-locked-error 'error-conditions)
                 (get 'sqlite-locked-error 'error-message)
                 (condition-case err
                     (sqlite-execute db
                                     "insert into test values (fake(2))")
                   (sqlite-error (car err)))))
            (sqlite-close db)))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "((sqlite-error error) \"Database error\" (sqlite-locked-error sqlite-error error) \"Database locked\" sqlite-error)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("SQLite condition contract should parse")
        .expect("SQLite condition contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("SQLite conditions should publish their GNU hierarchy")
            .to_string(),
        r#"((sqlite-error error) "Database error" (sqlite-locked-error sqlite-error error) "Database locked" sqlite-error)"#
    );
}

#[test]
fn native_file_lock_primitives_share_the_buffer_lock_state_machine() {
    let oracle_program = r#"
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
                      (subrp (symbol-function 'unlock-file))))
            (ignore-errors (unlock-file path))
            (delete-file path)))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {oracle_program})"),
        "(nil t nil nil nil t t)",
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let path = std::env::temp_dir().join(format!(
        "lock-primitive-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::write(&path, []).expect("create file-lock fixture");
    let path_value = Value::String(path.display().to_string().into());

    let locked = call(
        &mut interp,
        "lock-file",
        std::slice::from_ref(&path_value),
        &mut env,
    )
    .expect("lock fixture");
    let owner = call(
        &mut interp,
        "file-locked-p",
        std::slice::from_ref(&path_value),
        &mut env,
    )
    .expect("inspect owned fixture lock");
    let unlocked = call(
        &mut interp,
        "unlock-file",
        std::slice::from_ref(&path_value),
        &mut env,
    )
    .expect("unlock fixture");
    let after = call(
        &mut interp,
        "file-locked-p",
        std::slice::from_ref(&path_value),
        &mut env,
    )
    .expect("inspect unlocked fixture");
    interp.set_global_binding("create-lockfiles", Value::Nil);
    call(
        &mut interp,
        "lock-file",
        std::slice::from_ref(&path_value),
        &mut env,
    )
    .expect("disabled locking is a no-op");
    let disabled = call(
        &mut interp,
        "file-locked-p",
        std::slice::from_ref(&path_value),
        &mut env,
    )
    .expect("disabled locking leaves no lock");

    let lock_function = call(
        &mut interp,
        "symbol-function",
        &[Value::Symbol("lock-file".into())],
        &mut env,
    )
    .expect("read lock-file definition");
    let unlock_function = call(
        &mut interp,
        "symbol-function",
        &[Value::Symbol("unlock-file".into())],
        &mut env,
    )
    .expect("read unlock-file definition");
    assert_eq!(
        Value::list([
            locked,
            owner,
            unlocked,
            after,
            disabled,
            call(&mut interp, "subrp", &[lock_function], &mut env).expect("lock-file is a subr"),
            call(&mut interp, "subrp", &[unlock_function], &mut env)
                .expect("unlock-file is a subr"),
        ]),
        Value::list([
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
        ])
    );
    fs::remove_file(path).expect("remove file-lock fixture");
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
fn native_thread_and_synchronization_handles_compare_by_identity() {
    let program = r#"
        (let* ((thread-1 (make-thread 'ignore))
               (thread-2 (make-thread 'ignore))
               (mutex-1 (make-mutex))
               (mutex-2 (make-mutex))
               (condition-1 (make-condition-variable mutex-1))
               (condition-2 (make-condition-variable mutex-1))
               (record-thread-1 (record 'thread 1))
               (record-thread-2 (record 'thread 1))
               (record-mutex-1 (record 'mutex 1))
               (record-mutex-2 (record 'mutex 1)))
          (list (recordp thread-1)
                (recordp mutex-1)
                (recordp condition-1)
                (equal thread-1 thread-2)
                (equal thread-1 thread-1)
                (equal mutex-1 mutex-2)
                (equal mutex-1 mutex-1)
                (equal condition-1 condition-2)
                (equal condition-1 condition-1)
                (recordp record-thread-1)
                (equal record-thread-1 record-thread-2)
                (recordp record-mutex-1)
                (equal record-mutex-1 record-mutex-2)
                (threadp record-thread-1)
                (mutexp record-mutex-1)))
    "#;
    let expected = "(nil nil nil nil t nil t nil t t t t t nil nil)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("opaque-handle equality contract should parse")
        .expect("opaque-handle equality contract should contain a form");
    let expected = Reader::new(expected)
        .read()
        .expect("opaque-handle equality result should parse")
        .expect("opaque-handle equality result should exist");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("opaque handles should compare by identity"),
        expected
    );
}

#[test]
fn native_menu_activity_predicate_is_false_without_a_graphical_menu() {
    let program = r#"
        (list (menu-or-popup-active-p)
              (subrp (symbol-function 'menu-or-popup-active-p))
              (help-function-arglist 'menu-or-popup-active-p))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(nil t nil)");

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
    // `x-bitmap-file-path' is decode_env_path over epaths.h's PATH_BITMAPS,
    // a configure-time constant: it is ("/usr/include/X11/bitmaps") when the
    // oracle's configure found X headers and (".") when it did not.  The
    // oracle is asked for its own build's value live; Emaxx models the
    // X-less batch build and keeps (".") — the divergence is recorded in
    // docs/honesty-audit-2026-08-18.md.
    let oracle_bitmap_path = upstream_oracle_stdout("(prin1 x-bitmap-file-path)");
    let expected_with_path = |path: &str| format!("(t 10.0 nil {path} 300 auto (t t t t t t) 2.0)");
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        &expected_with_path(oracle_bitmap_path.trim()),
    );

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("image variable contract should parse")
        .expect("image variable contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("native image variables should match image.c"),
        Reader::new(&expected_with_path("(\".\")"))
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
        (progn
         (set-terminal-coding-system-internal 'no-conversion)
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
              (list [a -1 c] [b 1 d])))))))"#;
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
         [[us-ascii 101 769]
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
    let expected_printed = "(nil ([d 5 f] [a 2 c] [g 2 i]) (((2) . ignore) (1 3 t) (1 3 [98 99] t ignore 1) (0 2 [98 99] . ignore)) (1 nil t) ((1 2 [88] . ignore) (2 4 [88] t ignore 1) (2 4 t) (2 4 t)) ((0 3 [65 12 66] nil ignore 2) (0 3 [88 89] t ignore 1)) [[us-ascii 101 769] nil [0 0 101 101 1 0 1 1 0 nil] [1 1 769 769 0 0 0 1 0 nil] nil nil nil nil nil nil] nil ((args-out-of-range buffer) (wrong-type-argument vectorp) (error \"Attempt to shape zero-length text\") (wrong-type-argument terminal-live-p) (args-out-of-range \"a\") (error \"Invalid composition rule in RULES argument\")))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);

    // fill_gstring_body reads glyph widths from `char-width-table', whose
    // combining-character entries are set by international/characters.el at
    // dump time; the expected literal is the dumped oracle's answer, so the
    // in-process arm must carry the same dumped Lisp surface.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
fn find_composition_reports_the_automatic_composition_for_a_displayed_buffer() {
    // composite.c find_automatic_composition: the decomposed
    // "__A<U+030A>stro<U+0308>m" of erc-tests' `erc--split-line'.  The
    // combining pair composes through composition-function-table's Mn rule
    // and `compose-gstring-for-terminal' -- but ONLY while a window shows
    // the buffer, because the C returns 0 when Fget_buffer_window does.
    // Both halves are asserted, and the expectation is the oracle's own
    // live answer rather than a transcribed literal.
    let program = r#"
        (let ((old (window-buffer (selected-window)))
              (line (concat "__A" (string #x30A) "stro" (string #x308) "m")))
          (unwind-protect
              (list
               (with-temp-buffer
                 (set-window-buffer (selected-window) (current-buffer))
                 (insert line)
                 (find-composition 9 10))
               (with-temp-buffer
                 (insert line)
                 (find-composition 9 10)))
            (set-window-buffer (selected-window) old)))"#;
    let expected = upstream_oracle_stdout(&format!("(prin1 {program})"));
    assert!(
        expected.starts_with("((8 10 [[us-ascii 111 776]") && expected.ends_with(") nil)"),
        "oracle reported an unexpected composition shape: {expected}"
    );

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read()
        .expect("automatic composition contract should parse")
        .expect("automatic composition contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("automatic composition contract should evaluate")
            .to_string(),
        expected
    );
}

#[test]
fn char_charset_restriction_narrows_to_charset_list_or_coding_system() {
    // charset.c Fchar_charset with RESTRICTION: a list names charsets to
    // try in order (each validated with CHECK_CHARSET_GET_CHARSET); any
    // other non-nil value goes through coding_system_charset_list, which
    // signals (coding-system-error NAME) for an unknown coding system.
    // `compose-gstring-for-terminal' leans on the coding-system form to
    // decide whether the terminal can render each glyph.
    let program = r#"
        (list
         (char-charset 776 'us-ascii)
         (char-charset 776 'utf-8)
         (char-charset 111 'us-ascii)
         (char-charset 97 'latin-1)
         (char-charset 776 '(ascii unicode))
         (char-charset 40 '(unicode ascii))
         (char-charset 12354 'japanese-shift-jis)
         (char-charset 776 'no-conversion)
         (char-charset 97 'no-conversion)
         (condition-case e (char-charset 776 '(nosuch)) (error e))
         (condition-case e (char-charset 776 'nosuch) (error e))
         (condition-case e (char-charset 'x 'utf-8) (error e)))"#;
    let expected = "(nil unicode ascii iso-8859-1 unicode unicode japanese-jisx0208 unicode ascii \
                    (wrong-type-argument charsetp nosuch) (coding-system-error nosuch) \
                    (wrong-type-argument characterp x))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read()
        .expect("char-charset restriction contract should parse")
        .expect("char-charset restriction contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("char-charset restriction contract should evaluate")
            .to_string(),
        expected
    );
}

#[test]
fn composition_gstring_uses_the_effective_terminal_coding_system() {
    let program = r#"
        (let ((string (string 101 769)))
          (list
           (progn
             (set-terminal-coding-system-internal nil)
             (aref (aref (composition-get-gstring 0 2 nil string) 0) 0))
           (progn
             (set-terminal-coding-system-internal 'no-conversion)
             (aref (aref (composition-get-gstring 0 2 nil string) 0) 0))
           (progn
             (set-terminal-coding-system-internal 'utf-8-unix)
             (aref (aref (composition-get-gstring 0 2 nil string) 0) 0))))"#;
    let expected = "(us-ascii us-ascii utf-8-unix)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read()
        .expect("terminal gstring contract should parse")
        .expect("terminal gstring contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("terminal gstring contract should evaluate")
            .to_string(),
        expected
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
    // `x-load-color-file' is DEFUNed only in builds whose configure compiled
    // the X color machinery; on an oracle built without it the rest of this
    // family still holds, so that one element is stubbed out of the oracle's
    // program while Emaxx (which models the X-compiled headless build) keeps
    // parsing the color file — recorded in docs/honesty-audit-2026-08-18.md.
    let oracle_loads_color_files = upstream_oracle_stdout("(prin1 (fboundp 'x-load-color-file))");
    let color_file_call = format!("(x-load-color-file {color_file})");
    let color_file_rows = "((\"two words\" . 16711696) (\"alpha\" . 66051))";
    if oracle_loads_color_files.trim() == "t" {
        assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected_printed);
    } else {
        assert_upstream_primitive_contract(
            &format!(
                "(prin1 {})",
                program.replace(&color_file_call, "'emaxx-oracle-lacks-x-load-color-file")
            ),
            &expected_printed.replace(color_file_rows, "emaxx-oracle-lacks-x-load-color-file"),
        );
    }

    // Dispatch follows the host's C contract, so on a host whose oracle
    // lacks `x-load-color-file' Emaxx lacks it too; stub the same element
    // out of both sides there instead of asserting the Darwin build's row.
    let (host_program, host_expected) = if is_builtin("x-load-color-file") {
        (program.clone(), expected_printed.to_string())
    } else {
        (
            program.replace(&color_file_call, "'emaxx-oracle-lacks-x-load-color-file"),
            expected_printed.replace(color_file_rows, "emaxx-oracle-lacks-x-load-color-file"),
        )
    };
    let _ = expected;
    // The color primitives resolve names through term/tty-colors.el's
    // `tty-color-desc' over the colors startup.el registers with
    // `tty-register-default-colors' (xfaces.c tty_lookup_color), so the
    // Emaxx side runs on the same batch image the oracle answered from.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(&host_program)
        .read()
        .expect("xfaces.c family contract should parse")
        .expect("xfaces.c family contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("xfaces.c family contract should evaluate");
    let expected = Reader::new(&host_expected)
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
fn native_xfaces_set_attribute_frame_and_creation_contract_matches_gnu() {
    let program = r#"
        (let ((existing 'sample-existing-face)
              (global-missing 'sample-global-missing-face)
              (zero-missing 'sample-zero-missing-face)
              (local 'sample-local-face)
              (frame (selected-frame)))
          (internal-make-lisp-face existing nil)
          (list
           (condition-case error-data
               (internal-set-lisp-face-attribute
                "default" :foreground "blue" t)
             (error error-data))
           (condition-case error-data
               (internal-set-lisp-face-attribute
                global-missing :bogus 1 t)
             (error error-data))
           (condition-case error-data
               (internal-set-lisp-face-attribute
                zero-missing :foreground "blue" 0)
             (error error-data))
           (condition-case error-data
               (internal-set-lisp-face-attribute
                existing :foreground "blue" 17)
             (error error-data))
           (condition-case error-data
               (internal-set-lisp-face-attribute
                local :bogus 1 nil)
             (error error-data))
           (and (internal-lisp-face-p local frame) t)
           (internal-set-lisp-face-attribute
            existing :foreground 'unspecified t)
           (internal-get-lisp-face-attribute
            existing :foreground t)
           (internal-set-lisp-face-attribute
            existing :foreground 'unspecified frame)
           (internal-get-lisp-face-attribute
            existing :foreground frame)))"#;
    let expected = r#"((wrong-type-argument symbolp "default")
                       (error "Invalid face" sample-global-missing-face)
                       (error "Invalid face" sample-zero-missing-face)
                       (wrong-type-argument frame-live-p 17)
                       (error "Invalid face attribute name" :bogus)
                       t
                       sample-existing-face unspecified
                       sample-existing-face unspecified)"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "((wrong-type-argument symbolp \"default\") (error \"Invalid face\" sample-global-missing-face) (error \"Invalid face\" sample-zero-missing-face) (wrong-type-argument frame-live-p 17) (error \"Invalid face attribute name\" :bogus) t sample-existing-face unspecified sample-existing-face unspecified)",
    );

    let mut interp = Interpreter::new();
    let form = Reader::new(program)
        .read()
        .expect("xfaces set-attribute contract should parse")
        .expect("xfaces set-attribute contract should contain a form");
    let actual = interp
        .eval(&form, &mut Vec::new())
        .expect("xfaces set-attribute contract should evaluate");
    let expected = Reader::new(expected)
        .read()
        .expect("xfaces set-attribute expectation should parse")
        .expect("xfaces set-attribute expectation should contain a value");
    assert!(
        values_equal(&interp, &actual, &expected),
        "xfaces set-attribute result differs from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
}

#[test]
fn native_internal_lisp_face_p_is_a_total_predicate() {
    let program = r#"
        (let ((name 'emaxx-internal-face-p)
              (alias 'emaxx-internal-face-p-alias))
          (internal-make-lisp-face name nil)
          (put alias 'face-alias name)
          (list (internal-lisp-face-p nil)
                (internal-lisp-face-p 7)
                (internal-lisp-face-p '(x))
                (and (internal-lisp-face-p name) t)
                (and (internal-lisp-face-p "emaxx-internal-face-p") t)
                (and (internal-lisp-face-p alias) t)
                (internal-lisp-face-p 'emaxx-no-such-face)))
    "#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(nil nil nil t t t nil)");

    let mut interp = Interpreter::new();
    let form = Reader::new(program)
        .read()
        .expect("internal-lisp-face-p contract should parse")
        .expect("internal-lisp-face-p contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("internal-lisp-face-p contract should evaluate"),
        Value::list([
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::T,
            Value::T,
            Value::Nil,
        ])
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    // Compared live-to-live, like gnutls_digests_are_queried_from_the_library
    // below: both runtimes dlopen the SAME host libgnutls, so the cipher and
    // mac catalogues (their length included) are properties of that library,
    // not constants to transcribe from whichever container the test was
    // written on.  Emaxx renders its answer and the oracle must print the
    // identical text; the anchors underneath keep the answer from being
    // trivially empty.
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(&format!("(prin1-to-string {program})"))
        .read()
        .expect("GnuTLS catalog and error contract should parse")
        .expect("GnuTLS catalog and error contract should contain a form");
    let rendered = interp
        .eval(&form, &mut env)
        .expect("host GnuTLS catalog and error contract should evaluate");
    let rendered = string_text(&rendered).expect("prin1-to-string returns a string");
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), &rendered);
    for anchor in [
        ":cipher-aead-capable",
        ":mac-algorithm-id",
        "\"Symbol has no numeric gnutls-code property\"",
        "\"Not an error symbol or code\"",
        "(nil t nil nil t nil nil ",
    ] {
        assert!(
            rendered.contains(anchor),
            "expected {anchor} inside the GnuTLS catalog/error result, got {rendered}"
        );
    }
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
        .spawn();
    // GNU guards its gnutls-serv scenarios with `skip-unless
    // (executable-find "gnutls-serv")'; skip the same way on a host
    // without the tool instead of failing on the missing binary.
    let child = match child {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping: gnutls-serv is not installed on this host");
            return;
        }
        Err(error) => panic!("spawn gnutls-serv: {error}"),
    };
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
        .spawn();
    // Same `skip-unless (executable-find "gnutls-serv")' discipline as the
    // transport regression above.
    let child = match child {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping: gnutls-serv is not installed on this host");
            return;
        }
        Err(error) => panic!("spawn gnutls-serv: {error}"),
    };
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
                        (require 'nsm)
                        (list (consp (plist-get peer :certificates))
                              (stringp (plist-get certificate :issuer))
                              (stringp (plist-get certificate :subject))
                              (integerp (plist-get certificate :version))
                              (stringp (plist-get certificate :serial-number))
                              (stringp (plist-get certificate :valid-from))
                              (stringp (plist-get certificate :valid-to))
                              (stringp (plist-get certificate :public-key-algorithm))
                              (stringp (plist-get certificate :certificate-security-level))
                              (stringp (plist-get certificate :signature-algorithm))
                              (stringp (plist-get certificate :pem))
                              (null (nsm-protocol-check--sha1-sig
                                     "localhost" {port} peer))
                              (null (nsm-protocol-check--md5-sig
                                     "localhost" {port} peer))))
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
        Some(&Value::list(std::iter::repeat_n(Value::T, 13)))
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
    // Note: this contract is checked by handing the program to the oracle
    // through `--eval', and GNU decodes command-line arguments with the
    // locale's coding system -- so a literal curved quote in the program
    // text would not survive a non-UTF-8 locale.  The two quotes in the
    // expected description are therefore spelled as `\u' escapes, keeping
    // the argument pure ASCII, and the style binding below pins how
    // `internal-describe-syntax-value' renders them.
    let program = r#"
        (let*
            (;; Pin the quoting style: a nil `text-quoting-style' means
             ;; grave outside a UTF-8 locale, so the curved quotes in the
             ;; expected description below would otherwise depend on the
             ;; ambient LANG.
             (text-quoting-style 'curve)
             (results
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
	  is a prefix character for \u2018backward-prefix-chars\u2019")))))
          (list results
                (subrp
                 (symbol-function 'internal-describe-syntax-value))
                (help-function-arglist
                 'internal-describe-syntax-value)))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "((t t t t t t) t (arg1))");

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
    crate::test_support::run_with_large_stack(
        canonical_combining_classes_come_from_complete_unicode_data_inner,
    );
}

fn canonical_combining_classes_come_from_complete_unicode_data_inner() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    for (character, expected) in [(0x0307, 230), (0x0323, 220)] {
        assert_eq!(
            crate::test_support::call_lisp_function(
                &mut interp,
                &mut env,
                "get-char-code-property",
                &[
                    Value::Integer(character),
                    Value::Symbol("canonical-combining-class".into()),
                ],
            )
            .expect("read canonical combining class"),
            Value::Integer(expected)
        );
    }
}

// macOS only, matching the `#[cfg]` on the code it covers: on GNU/Linux the
// Emaxx arm is still the acknowledged bare-nil cheat and these interface names
// do not exist, so the assertions below would fail against a tree that is
// behaving exactly as documented.
#[cfg(target_os = "macos")]
#[test]
fn network_interface_info_reports_the_real_interface() {
    // process.c:4459 returns (ADDR BCAST NETMASK HWADDR FLAGS), IPv4 only,
    // and nil for an interface that answers no query.  The arm this replaces
    // was a bare nil beside a genuinely implemented `network-interface-list'
    // (audit finding 111).
    //
    // The interfaces are NAMED rather than enumerated: `network-interface-list'
    // itself disagrees with GNU about which interfaces exist (finding 118), so
    // driving the sweep from it would compare two different sets.  A name that
    // does not exist on some host answers nil on BOTH sides, so this stays
    // correct off this machine -- it just tests less there.
    //
    // `bridge0' is not decoration.  Its 7-character name is what makes
    // LLADDR's pointer arithmetic differ from indexing the libc-declared
    // 12-byte sdl_data, and the first version of this code PANICKED on it.
    // The first version of this test named only lo0, which has no MAC at all,
    // so the entire hardware-address path went unexecuted and the crash shipped
    // to the gate unnoticed.
    let program = r#"
        (prin1-to-string
         (list (network-interface-info "lo0")
               (network-interface-info "en0")
               (network-interface-info "bridge0")
               (network-interface-info "nosuchdev0")
               (condition-case error
                   (network-interface-info "averyveryverylongname0123456789")
                 (error (cadr error)))
               (condition-case error
                   (network-interface-info 42)
                 (error (car error)))))"#;

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read interface-info program")
        .remove(0);
    let rendered = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate interface-info program");
    let rendered = string_text(&rendered).expect("prin1-to-string returns a string");

    // Compare Lisp-rendered text on both sides.  Rust's `Display' for a cons
    // whose CDR is a vector flattens it -- `(18 vector-literal 74 ...)' -- while
    // `prin1' correctly writes `(18 . [74 ...])'; only the Lisp printer is the
    // contract, and only it is what GNU is being compared against.
    assert_upstream_primitive_contract(&format!("(princ {program})"), &rendered);

    let rows = Reader::new(&rendered)
        .read()
        .expect("re-read rendered result")
        .expect("rendered result exists")
        .to_vec()
        .expect("interface-info list");
    assert!(
        !rows[0].is_nil(),
        "lo0 must report real data, not the constant nil this replaced"
    );
    assert!(rows[3].is_nil(), "a nonexistent interface answers nil");
    // The panic needed BOTH a hardware address and a name of 7+ bytes, since
    // it was an overrun past the libc-declared 12-byte sdl_data at offset
    // sdl_nlen.  Asserting merely that "some interface has a MAC" is satisfied
    // by en0, whose 3-byte name never reaches the overflowing offsets -- which
    // is how the crash shipped in the first place.  Require the long-named one
    // specifically, and fail loudly rather than silently testing less if this
    // host has no such device.
    let long_named_with_mac = rows[2]
        .to_vec()
        .is_ok_and(|parts| parts.get(3).is_some_and(|hw| !hw.is_nil()));
    assert!(
        long_named_with_mac,
        "bridge0 (a 7-byte name with a MAC) reported none, so the LLADDR \
         pointer arithmetic that once panicked went unexercised"
    );
}

#[test]
fn core_libraries_are_not_shadowed_by_cedet_subdirectories() {
    // Finding 123.  Emaxx discovers test-helper directories with a recursive
    // walk when EMACS_TEST_DIRECTORY is set -- which the compatibility harness
    // sets for every child it measures -- and those 77 extra directories (66
    // under test/, 11 under lisp/) used to sit AHEAD of the standard library.
    // ELEVEN core names then resolved into CEDET, quail and fixture
    // directories; five of them are checked here:
    //
    //     chart comp debug generic map
    //
    // `(require 'map)' loaded cedet/srecode/map.el, which provides
    // `srecode/map', and failed.  Five manifest files require one of THESE
    // five, worth at least 324 outcomes -- including the 177 of
    // test/src/comp-tests.el.  The full eleven-name exposure is larger and
    // has not been counted.
    //
    // THIS TEST DOES NOT PIN THE ORDERING FIX, and an audit caught it
    // claiming to.  `initialized_upstream_batch_interpreter' is built with
    // `load_path' ALREADY set to GNU's 25 directories (test_support.rs), and
    // those sit at the head under either ordering, so this passed with the fix
    // reverted.  The real regression test is
    // `standard_library_precedes_the_discovered_test_tree' in batch.rs, which
    // asserts the order directly and does fail when reverted (verified).
    //
    // What this one is still worth: it confirms against the live oracle that
    // these requires SUCCEED and agree with GNU, which is the user-visible
    // half.  Note the shadow set was ELEVEN names, not the five here --
    // compile, cpp, emoji, etags, grep and python were shadowed too.
    let program = r#"
        (list (require 'map nil t)
              (require 'comp nil t)
              (require 'chart nil t)
              (require 'generic nil t)
              (and (fboundp 'map-elt) (fboundp 'map-put!) t))"#;
    let expected = "(map comp chart generic t)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read core-library program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate core-library program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn threads_get_their_own_bindings_handlers_and_join_semantics() {
    // The real content behind stale finding 99, all mirroring thread.c:
    //
    //   - a body error is caught by the thread itself (thread.c:815), kept
    //     for `thread-last-error', and `thread-join' returns NIL; only a
    //     `thread-signal'-delivered kill re-raises out of the join
    //     (thread.c:1081/1088);
    //   - each thread owns its dynamic bindings (unbind/rebind_for_thread_
    //     switch, thread.c:87-100): a child sees GLOBALS, its setq writes
    //     globals, and the parent's let-exit restores the child-written
    //     value; nested grandchildren must not see the grandparent's lets
    //     either (the first version of the swap walked the whole shared
    //     stack and re-exposed them);
    //   - each thread owns its handler list: ERT wraps test bodies in
    //     `handler-bind' (ert.el:803), and with a shared stack a child's
    //     error ran the PARENT's handler, whose cl-return-from died at the
    //     boundary as (no-catch --cl-block-error-- nil).
    let program = r#"
        (progn
          (defvar zz-tt 'global)
          (list
           ;; join of an errored thread: nil, error retrievable
           (thread-join (make-thread (lambda () (car 42))))
           (thread-last-error 'cleanup)
           ;; child sees the global, not the parent's let
           (let ((zz-tt 'parent))
             (thread-join (make-thread (lambda () zz-tt))))
           ;; child setq writes the global; parent's let-exit restores it
           (progn (let ((zz-tt 'parent))
                    (thread-join (make-thread (lambda () (setq zz-tt 'child)))))
                  zz-tt)
           ;; grandchild sees the global through two levels of lets
           (let ((zz-tt 'p1))
             (thread-join (make-thread (lambda ()
               (let ((zz-tt 'p2))
                 (thread-join (make-thread (lambda () zz-tt))))))))
           ;; parent handler-bind must not see the child's error
           (let ((zz-hb nil))
             (handler-bind ((error (lambda (_) (setq zz-hb 'parent-saw))))
               (thread-join (make-thread (lambda () (car 42)))))
             (list zz-hb (car (thread-last-error 'cleanup))))
           ;; a delivered signal comes back out of the join
           (let ((m (make-mutex)) (started nil))
             (mutex-lock m)
             (let ((th (make-thread (lambda () (setq started t) (mutex-lock m)))))
               (while (not started) (thread-yield))
               (thread-signal th 'quit nil)
               (condition-case e (thread-join th) (quit 'quit-resignalled))))))"#;
    // Rows interact by design: row 4's child setq legitimately leaves the
    // GLOBAL as `child' (the parent's let-exit restores the child-written
    // value -- the two-way swap), so row 5's grandchild correctly reads
    // `child'.  The first draft of this expectation said `global' for both
    // and the ORACLE corrected it.
    let expected = concat!(
        "(nil (wrong-type-argument listp 42) global child child ",
        "(nil wrong-type-argument) quit-resignalled)"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read thread semantics program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate thread semantics program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn thread_join_leaves_unrelated_timers_pending() {
    let program = r#"
        (progn
          (setq zz-join-timer-fired nil)
          (run-at-time 0 nil (lambda () (setq zz-join-timer-fired t)))
          (thread-join (make-thread (lambda () 'done)))
          (list zz-join-timer-fired
                (progn (input-pending-p t) zz-join-timer-fired)))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(nil t)");

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read thread-join timer program")
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate thread-join timer program"),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn subprocess_cwd_uses_native_unhandled_directory_mechanism() {
    let program = r#"
        (let ((saved (symbol-function 'unhandled-file-name-directory))
              (replacement-called nil))
          (unwind-protect
              (progn
                (fset 'unhandled-file-name-directory
                      (lambda (_file)
                        (setq replacement-called t)
                        "/path-that-must-not-be-used/"))
                (let ((default-directory "/"))
                  (list replacement-called
                        (call-process shell-file-name nil nil nil "-c" "exit 0"))))
            (fset 'unhandled-file-name-directory saved)))"#;
    let expected = "(nil 0)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read subprocess cwd dispatch program")
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate subprocess cwd dispatch program")
            .to_string(),
        expected
    );
}

#[test]
fn process_tty_name_rejects_unknown_streams() {
    let program = r#"
        (let ((process (make-pipe-process :name "emaxx-tty-stream-contract")))
          (unwind-protect
              (condition-case error
                  (process-tty-name process 'bogus-stream)
                (error error))
            (delete-process process)))"#;
    let expected = "(error \"Unknown stream\" bogus-stream)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read process tty stream program")
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate process tty stream program")
            .to_string(),
        expected
    );
}

#[cfg(unix)]
#[test]
fn dropping_an_interpreter_terminates_and_reaps_its_child() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let program = r#"
        (make-process
         :name "emaxx-drop-child-contract"
         :command (list shell-file-name "-c" "read line")
         :connection-type 'pipe
         :noquery t)"#;
    let form = Reader::new(program)
        .read_all()
        .expect("read child-drop lifecycle program")
        .remove(0);
    let process = interp
        .eval(&form, &mut Vec::new())
        .expect("create child for interpreter-drop lifecycle");
    let process_id = interp
        .resolve_process_id(&process)
        .expect("resolve lifecycle child");
    let pid = interp
        .process_os_id(process_id)
        .expect("lifecycle child has an operating-system pid") as libc::pid_t;

    drop(interp);

    // RunningProcess::drop waits for reaping, so this is a deterministic
    // lifecycle assertion rather than a sleep-and-hope race probe.
    // SAFETY: signal zero only queries whether PID still names a process.
    let result = unsafe { libc::kill(pid, 0) };
    assert_eq!(result, -1, "interpreter drop left child pid {pid} alive");
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH),
        "interpreter drop did not reap child pid {pid}"
    );
}

#[test]
fn buffer_file_name_primitive_observes_current_buffer_dynamic_binding() {
    let program = r#"(with-temp-buffer
        (let ((buffer-file-name "/tmp/emaxx-dynamic-file"))
          (list buffer-file-name
                (buffer-file-name)
                (buffer-file-name (current-buffer)))))"#;
    let expected =
        "(\"/tmp/emaxx-dynamic-file\" \"/tmp/emaxx-dynamic-file\" \"/tmp/emaxx-dynamic-file\")";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read dynamically bound buffer-file-name program")
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate dynamically bound buffer-file-name program")
            .to_string(),
        expected
    );
}

#[cfg(target_os = "macos")]
#[test]
fn kqueue_directory_watch_reports_external_child_creation() {
    let program = r#"
        (progn
          (require 'filenotify)
          (let* ((directory (make-temp-file "emaxx-kqueue-contract" t))
                 (child (expand-file-name "created-outside-emaxx" directory))
                 (events nil)
                 (descriptor
                  (file-notify-add-watch
                   directory '(change)
                   (lambda (event)
                     (push (list (cadr event)
                                 (file-name-nondirectory (caddr event)))
                           events))))
                 (touch (executable-find "touch"))
                 (process
                  (make-process :name "external-file-creator"
                                :command (list touch child)
                                :noquery t
                                :connection-type 'pipe)))
            (unwind-protect
                (progn
                  (while (process-live-p process)
                    (accept-process-output process 0.05))
                  (let ((deadline (+ (float-time) 2.0)))
                    (while (and (null events) (< (float-time) deadline))
                      (read-event nil nil 0.01)))
                  (nreverse events))
              (file-notify-rm-watch descriptor)
              (delete-directory directory t))))"#;
    let expected = "((created \"created-outside-emaxx\"))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read external kqueue directory program")
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate external kqueue directory program")
            .to_string(),
        expected
    );
}

#[cfg(target_os = "linux")]
#[test]
fn inotify_directory_watch_reports_external_child_creation() {
    let program = r#"
        (progn
          (require 'filenotify)
          (let* ((directory (make-temp-file "emaxx-inotify-contract" t))
                 (child (expand-file-name "created-outside-emaxx" directory))
                 (events nil)
                 (descriptor
                  (file-notify-add-watch
                   directory '(change)
                   (lambda (event)
                     (push (list (cadr event)
                                 (file-name-nondirectory (caddr event)))
                           events))))
                 (touch (executable-find "touch"))
                 (process
                  (make-process :name "external-file-creator"
                                :command (list touch child)
                                :noquery t
                                :connection-type 'pipe)))
            (unwind-protect
                (progn
                  (while (process-live-p process)
                    (accept-process-output process 0.05))
                  (let ((deadline (+ (float-time) 2.0)))
                    (while (and (null events) (< (float-time) deadline))
                      (read-event nil nil 0.01)))
                  (nreverse events))
              (file-notify-rm-watch descriptor)
              (delete-directory directory t))))"#;
    let expected = "((created \"created-outside-emaxx\"))";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read external inotify directory program")
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate external inotify directory program")
            .to_string(),
        expected
    );
}

/// Pin PROGRAM's printed value against the live Linux oracle and then
/// against a fresh interpreter, so the literal cannot drift from GNU.
#[cfg(target_os = "linux")]
fn assert_linux_inotify_contract(program: &str, expected: &str, label: &str) {
    assert_oracle_contract_matches_interpreter(program, expected, label);
}

/// Pin PROGRAM's printed value against the oracle, then evaluate the same
/// program in an initialized in-process interpreter and require the same
/// text.
fn assert_oracle_contract_matches_interpreter(program: &str, expected: &str, label: &str) {
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .unwrap_or_else(|_| panic!("read {label} program"))
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .unwrap_or_else(|_| panic!("evaluate {label} program"))
            .to_string(),
        expected
    );
}

#[cfg(target_os = "linux")]
#[test]
fn process_attributes_follows_sysdep_procfs() {
    // sysdep.c system_process_attributes (GNU_LINUX) conses 31 attributes
    // from /proc/PID: owner ids and names, the `stat' fields, jiffies as
    // old-style times, /proc/uptime-derived start/etime/pcpu, and the
    // escaped command line.  The child is a fresh `sh -c "sleep 5"' that
    // ignores its extra arguments and stays alive (a `sleep' handed those
    // arguments exits at once, and reading /proc then raced its death:
    // an empty cmdline and a zombie state), so the parent
    // linkage, the child-accounting fields and the argument escaping are
    // exact; the live counters are pinned by type.
    let program = r#"
        (let* ((p (start-process "s" nil "sh" "-c" "sleep 5" "a b" "c\\d"))
               (a (process-attributes (process-id p)))
               (keys '(euid user egid group comm state ppid pgrp sess ttname tpgid
                       minflt majflt cminflt cmajflt utime stime time cutime cstime
                       ctime start etime pcpu pri nice thcount vsize rss pmem args)))
          (prog1
              (list (mapcar #'car a)
                    (mapcar (lambda (k) (type-of (cdr (assq k a)))) keys)
                    (cdr (assq 'comm a))
                    (and (member (cdr (assq 'state a)) '("R" "S")) t)
                    (= (cdr (assq 'ppid a)) (emacs-pid))
                    (= (cdr (assq 'euid a)) (user-uid))
                    (equal (cdr (assq 'user a)) (user-login-name))
                    (cdr (assq 'cminflt a)) (cdr (assq 'cmajflt a))
                    (cdr (assq 'cutime a)) (cdr (assq 'cstime a)) (cdr (assq 'ctime a))
                    (length (cdr (assq 'start a))) (length (cdr (assq 'etime a)))
                    (cdr (assq 'thcount a))
                    (let ((args (cdr (assq 'args a))))
                      (list (file-name-absolute-p args)
                            (string-suffix-p "/sh -c sleep\\ 5 a\\ b c\\\\d" args)))
                    (process-attributes 0))
            (delete-process p)))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "((args pmem rss vsize thcount nice pri pcpu etime start ctime cstime cutime time stime \
         utime cmajflt cminflt majflt minflt tpgid ttname sess pgrp ppid state comm group egid \
         user euid) (integer string integer string string string integer integer integer string \
         integer integer integer integer integer cons cons cons cons cons cons cons cons float \
         integer integer integer integer integer float string) \"sh\" t t t t 0 0 (0 0 0 0) \
         (0 0 0 0) (0 0 0 0) 4 4 1 (t t) nil)",
        "process-attributes",
    );
}

#[cfg(unix)]
#[test]
fn program_search_follows_openp_over_exec_path() {
    // process.c Fmake_process and callproc.c Fcall_process locate the
    // program with openp (X_OK over `exec-path' and `exec-suffixes'), report
    // a miss as "Searching for program" with openp's errno (ENOENT, EISDIR
    // for a directory, EACCES), and make-process rejects an absolute
    // directory outright.  fileio.c's `file-executable-p' is a plain
    // faccessat, so a searchable directory qualifies.
    let program = r#"
        (list
         (condition-case e (start-process "x" nil "no-such-emaxx-program") (error e))
         (let ((exec-path nil))
           (condition-case e (start-process "x" nil "sleep" "1") (error e)))
         (let ((exec-path '("/usr/bin")))
           (condition-case e (start-process "x" nil ".") (error e)))
         (condition-case e (start-process "x" nil "/usr/bin") (error e))
         (condition-case e (start-process "x" nil "./no-such-emaxx-program") (error e))
         (let ((exec-path nil))
           (condition-case e (call-process "sleep" nil nil nil "0") (error e)))
         (condition-case e (call-process "/etc/passwd") (error e))
         (let ((exec-path '("/usr/bin")))
           (condition-case e (call-process ".") (error e)))
         (file-executable-p "/usr/bin")
         (file-executable-p "/etc/passwd")
         (with-temp-buffer
           (call-process "sh" nil t nil "-c" "echo $0")
           (equal (buffer-string) (concat (executable-find "sh") "\n")))
         (let* ((b (generate-new-buffer "argv0"))
                (p (start-process "argv0" b "sh" "-c" "echo $0")))
           (while (process-live-p p) (sleep-for 0.05))
           (equal (car (split-string (with-current-buffer b (buffer-string)) "\n"))
                  (executable-find "sh"))))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "((file-missing \"Searching for program\" \"No such file or directory\" \
         \"no-such-emaxx-program\") (file-missing \"Searching for program\" \
         \"No such file or directory\" \"sleep\") (file-error \"Searching for program\" \
         \"Is a directory\" \".\") (error \"Specified program for new process is a directory\") \
         (file-missing \"Searching for program\" \"No such file or directory\" \
         \"./no-such-emaxx-program\") (file-missing \"Searching for program\" \
         \"No such file or directory\" \"sleep\") (permission-denied \"Searching for program\" \
         \"Permission denied\" \"/etc/passwd\") (file-error \"Searching for program\" \
         \"Is a directory\" \".\") t nil t t)",
        "program search",
    );
}

#[cfg(unix)]
#[test]
fn exec_failure_follows_emacs_spawn() {
    // callproc.c emacs_spawn: with a pseudo-terminal the vfork child reports
    // a failed exec itself ("<emacs>: <program>: <strerror>" on its stderr,
    // then _exit 127 for ENOENT or 126 otherwise), so Lisp gets a process
    // that exits with that code; without a pty posix_spawn hands the errno
    // to the parent, which signals "Doing vfork" naming no file.  The
    // diagnostic's first field is the running Emacs's own argv[0], so the
    // comparison starts after it.
    let program = r#"
        (let ((results nil))
          (dolist (case '(("pty-missing" t "/no/such/emaxx-program")
                          ("pty-denied" t "/etc/passwd")
                          ("pipe-missing" nil "/no/such/emaxx-program")
                          ("pipe-denied" nil "/etc/passwd")))
            (let ((process-connection-type (nth 1 case))
                  (b (generate-new-buffer (nth 0 case))))
              (push (condition-case e
                        (let ((p (start-process (nth 0 case) b (nth 2 case))))
                          (while (process-live-p p) (sleep-for 0.05))
                          (while (accept-process-output p 0.1))
                          (list (process-exit-status p) (process-status p)
                                (cdr (split-string (with-current-buffer b (buffer-string))
                                                   ": "))))
                      (error e))
                    results)))
          (nreverse results))"#;
    // Darwin uses fork for the PTY path, and the pinned GNU 30.2 build keeps
    // the same child exit codes without delivering exec_failed's diagnostic
    // through the process buffer.  Keep both host contracts explicit rather
    // than treating the Linux-oracle result as Unix-wide.
    #[cfg(target_os = "macos")]
    assert_oracle_contract_matches_interpreter(
        program,
        "((127 exit nil) (126 exit nil) (file-missing \"Doing vfork\" \
         \"No such file or directory\") (permission-denied \"Doing vfork\" \
         \"Permission denied\"))",
        "exec failure",
    );
    #[cfg(not(target_os = "macos"))]
    assert_oracle_contract_matches_interpreter(
        program,
        "((127 exit (\"/no/such/emaxx-program\" \"No such file or directory\n\nProcess \
         pty-missing exited abnormally with code 127\n\")) (126 exit (\"/etc/passwd\" \
         \"Permission denied\n\nProcess pty-denied exited abnormally with code 126\n\")) \
         (file-missing \"Doing vfork\" \"No such file or directory\") (permission-denied \
         \"Doing vfork\" \"Permission denied\"))",
        "exec failure",
    );
}

#[test]
fn sleep_for_zero_returns_without_waiting_or_running_timers() {
    // dispnew.c Fsleep_for enters wait_reading_process_output only for a
    // positive duration; a zero one returns nil at once and leaves a due
    // timer unrun.  In batch `sit-for' is that same call.
    let program = r#"
        (list
         (let (x) (run-at-time 0 nil (lambda () (setq x t))) (list (sleep-for 0) x))
         (let (x) (run-at-time 0 nil (lambda () (setq x t))) (list (sleep-for 0.01) x))
         (let (x) (run-at-time 0 nil (lambda () (setq x t))) (list (sit-for 0) x)))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "((nil nil) (nil t) (t nil))",
        "sleep-for 0",
    );
}

#[test]
fn minibuffer_reads_under_a_keyboard_macro_follow_read_minibuf() {
    // minibuf.c read_minibuf takes the stdin reader only while
    // `noninteractive' and no keyboard macro executes; with
    // `executing-kbd-macro' bound the recursive command loop reads, whose
    // read_char drains `unread-command-events' first and then the macro
    // (advancing `executing-kbd-macro-index'), ends the read at the end of
    // the macro with the minibuffer's contents, and afterwards adds the
    // value, or DEFAULT for an empty one, to HIST.
    let program = r#"
        (progn
          (defvar emaxx-test-h1 nil) (defvar emaxx-test-h2 nil)
          (defvar emaxx-test-h3 nil) (defvar emaxx-test-h4 nil)
          (list
           (let ((executing-kbd-macro []) (executing-kbd-macro-index 0))
             (list (read-from-minibuffer "p: " nil nil nil 'emaxx-test-h1 "dflt")
                   emaxx-test-h1))
           (let ((executing-kbd-macro "abc\r") (executing-kbd-macro-index 0))
             (list (read-from-minibuffer "p: " nil nil nil 'emaxx-test-h2 "d5")
                   emaxx-test-h2 executing-kbd-macro-index))
           (let ((executing-kbd-macro "\r") (executing-kbd-macro-index 0))
             (list (read-string "p: " nil 'emaxx-test-h3 "d6")
                   emaxx-test-h3 executing-kbd-macro-index))
           (let ((executing-kbd-macro "\r") (executing-kbd-macro-index 0))
             (list (read-string "p: " "init" 'emaxx-test-h3 "d6") (copy-sequence emaxx-test-h3)))
           (let ((executing-kbd-macro "q\r") (executing-kbd-macro-index 0)
                 (unread-command-events (listify-key-sequence "z")))
             (list (read-string "p: " nil 'emaxx-test-h3) (copy-sequence emaxx-test-h3)))
           (let ((executing-kbd-macro t)
                 (unread-command-events (listify-key-sequence "ab\r")))
             (list (read-string "p: " nil 'emaxx-test-h4) emaxx-test-h4))
           (let ((executing-kbd-macro t)
                 (unread-command-events (listify-key-sequence "ab")))
             (read-string "p: " nil 'emaxx-test-h4))
           (let ((executing-kbd-macro t))
             (read-string "p: " nil 'emaxx-test-h4))))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "((\"\" (\"dflt\")) (\"abc\" (\"abc\") 4) (\"d6\" (\"d6\") 1) (\"init\" (\"init\" \"d6\")) \
         (\"zq\" (\"zq\" \"init\" \"d6\")) (\"ab\" (\"ab\")) \"ab\" \"\")",
        "minibuffer under macro",
    );
}

#[cfg(target_os = "linux")]
#[test]
fn just_this_one_wait_still_notifies_an_exited_distractor() {
    // process.c status_notify runs for every process whose status changed,
    // wait_proc or not: it reads any output that remains and runs the
    // sentinel.  So a JUST-THIS-ONE wait on TARGET still delivers a
    // distractor's leftover output once the distractor exits, while a
    // distractor that stays alive keeps its output unread (the companion
    // contract).  Whether the exit lands inside the 0.15 s window is host
    // scheduling; the Linux oracle answers deterministically.
    let _permit = crate::test_support::acquire_exclusive_host_test_permit();
    let program = r#"
        (let* ((target-buffer (generate-new-buffer " *apo-target*"))
               (distractor-buffer (generate-new-buffer " *apo-distractor*"))
               (target (make-process
                        :name "apo-target" :buffer target-buffer
                        :command (list shell-file-name shell-command-switch
                                       "sleep 0.15; printf target")
                        :noquery t :sentinel #'ignore))
               (distractor (make-process
                            :name "apo-distractor" :buffer distractor-buffer
                            :command (list shell-file-name shell-command-switch
                                           "printf distractor")
                            :noquery t :sentinel #'ignore)))
          (unwind-protect
              (list (accept-process-output target nil nil t)
                    (with-current-buffer distractor-buffer (buffer-string))
                    (with-current-buffer target-buffer (buffer-string))
                    (accept-process-output distractor 2)
                    (with-current-buffer distractor-buffer (buffer-string))
                    (process-status distractor))
            (ignore-errors (delete-process target))
            (ignore-errors (delete-process distractor))
            (kill-buffer target-buffer)
            (kill-buffer distractor-buffer)))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "(t \"distractor\" \"target\" nil \"distractor\" exit)",
        "JUST-THIS-ONE status_notify drain",
    );
}

#[test]
fn lisp_eval_depth_counts_ffuncall_entries_like_eval_c() {
    // eval.c increments `lisp_eval_depth' in eval_sub and again in
    // Ffuncall: a direct call costs one unit per level, while `funcall'
    // (of a quoted symbol or of a `function' form -- the interpreter
    // never rewrites the latter into a direct call), `apply' and `mapc'
    // (call1) cost two.  Pinned as the difference between two limits,
    // which is independent of the base depth at which the probe starts.
    let program = r#"
        (progn
          (defvar emaxx-depth-reached 0)
          (defun emaxx-depth-ff (n) (setq emaxx-depth-reached n) (funcall #'emaxx-depth-ff (1+ n)))
          (defun emaxx-depth-gg (n) (setq emaxx-depth-reached n) (emaxx-depth-gg (1+ n)))
          (defun emaxx-depth-hh (n) (setq emaxx-depth-reached n) (apply #'emaxx-depth-hh (list (1+ n))))
          (defun emaxx-depth-kk (n) (setq emaxx-depth-reached n) (funcall 'emaxx-depth-kk (1+ n)))
          (defun emaxx-depth-mm (n) (setq emaxx-depth-reached n) (mapc #'emaxx-depth-mm (list (1+ n))))
          (defun emaxx-depth-probe (f lim)
            (let ((max-lisp-eval-depth lim))
              (setq emaxx-depth-reached 0)
              (condition-case nil (funcall f 0) (error emaxx-depth-reached))))
          (mapcar (lambda (f) (- (emaxx-depth-probe f 400) (emaxx-depth-probe f 200)))
                  '(emaxx-depth-ff emaxx-depth-gg emaxx-depth-hh emaxx-depth-kk emaxx-depth-mm)))"#;
    assert_oracle_contract_matches_interpreter(program, "(100 200 100 100 100)", "eval depth");
}

#[test]
fn timers_run_inside_a_child_threads_sleep_with_its_bindings() {
    // thread.c: a child blocked in `sleep-for' sits in
    // wait_reading_process_output, so the timers that come due during its
    // sleep run there, on the child's own specpdl -- the joiner's dynamic
    // `let' is swapped out (the child sees the global) and the child's own
    // `let' is visible.  `thread-join' itself runs nothing.
    let program = r#"
        (progn
          (defvar emaxx-tv 'global)
          (defvar emaxx-seen nil)
          (list
           (let ((emaxx-tv 'main))
             (setq emaxx-seen nil)
             (thread-join (make-thread (lambda () (setq emaxx-seen emaxx-tv))))
             emaxx-seen)
           (let ((emaxx-tv 'main))
             (setq emaxx-seen nil)
             (run-at-time 0 nil (lambda () (setq emaxx-seen (list 'timer emaxx-tv))))
             (thread-join (make-thread (lambda () (sleep-for 0.2))))
             emaxx-seen)
           (let ((emaxx-tv 'main))
             (setq emaxx-seen nil)
             (run-at-time 0.05 nil (lambda () (setq emaxx-seen (list 'timer emaxx-tv))))
             (thread-join (make-thread (lambda () (sleep-for 0.2))))
             emaxx-seen)
           (let ((emaxx-tv 'main))
             (setq emaxx-seen nil)
             (let ((th (make-thread (lambda () (sleep-for 0.2)))))
               (run-at-time 0 nil (lambda () (setq emaxx-seen (list 'timer emaxx-tv))))
               (thread-join th)
               emaxx-seen))
           (let ((emaxx-tv 'main))
             (setq emaxx-seen nil)
             (run-at-time 0 nil (lambda () (setq emaxx-seen (list 'timer emaxx-tv))))
             (thread-join (make-thread (lambda () (let ((emaxx-tv 'kid)) (sleep-for 0.2)))))
             emaxx-seen)
           (let ((x 1))
             (setq emaxx-seen nil)
             (run-at-time 0 nil (lambda () (setq emaxx-seen t)))
             (thread-join (make-thread
                           (lambda ()
                             (let ((end (+ (float-time) 0.2)))
                               (while (< (float-time) end))))))
             (list emaxx-seen (progn (sleep-for 0.05) emaxx-seen)))))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "(global (timer global) (timer global) (timer global) (timer kid) (nil t))",
        "timers in threads",
    );
}

#[test]
fn thread_signal_queues_a_thread_event_for_the_main_thread() {
    // thread.c Fthread_signal: the current thread signals itself at once;
    // aimed at the main thread from a child it stores a THREAD_EVENT
    // `(thread-event THREAD ERROR-SYMBOL DATA)' in the input queue, which
    // read_char later hands to `special-event-map's thread-handle-event
    // (its "Error ..." message, with the data intact), and the read itself
    // returns nothing.
    let program = r#"
        (list
         (let ((th (make-thread (lambda () (thread-signal main-thread 'error '("hi"))))))
           (thread-join th)
           (sleep-for 0.1)
           (condition-case e (read-event nil nil 0.1) (error (list 'caught e))))
         (with-current-buffer "*Messages*"
           (string-match-p "Error #<thread [^>]*>: (error (\"hi\"))" (buffer-string)))
         (condition-case e (thread-signal main-thread 'error '("self")) (error (list 'self e)))
         (condition-case e (thread-signal (current-thread) 'error '("cur")) (error (list 'cur e))))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "(nil 0 (self (error \"self\")) (cur (error \"cur\")))",
        "thread-signal",
    );
}

#[test]
fn eval_region_delegates_to_load_read_function_without_reinterning() {
    // lread.c readevalloop: a nil READ-FUNCTION means `load-read-function',
    // and a Lisp reader interns nothing beyond what it interned itself, so
    // an `unintern'-ed symbol it returns stays dead.  Feval_buffer returns
    // nil whatever the last form produced.
    let program = r#"
        (list
         (progn
           (intern "emaxx-gone-zz")
           (let ((s (intern-soft "emaxx-gone-zz")) (calls 0))
             (unintern s obarray)
             (with-temp-buffer
               (insert "x")
               (let ((load-read-function
                      (lambda (&optional _stream)
                        (setq calls (1+ calls))
                        (goto-char (point-max))
                        (list 'quote s))))
                 (eval-region (point-min) (point-max))))
             (list calls (intern-soft "emaxx-gone-zz"))))
         (progn
           (intern "emaxx-gone-yy")
           (let ((s (intern-soft "emaxx-gone-yy")) (calls 0))
             (unintern s obarray)
             (with-temp-buffer
               (insert "x")
               (let ((load-read-function
                      (lambda (&optional _stream)
                        (setq calls (1+ calls))
                        (goto-char (point-max))
                        (list 'quote s))))
                 (list (eval-buffer) calls (intern-soft "emaxx-gone-yy")))))))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "((1 nil) (nil 1 nil))",
        "eval-region reader",
    );
}

#[test]
fn gnu_c_bool_variables_match_fresh_grep() {
    // The DEFVAR_BOOL table is every `DEFVAR_BOOL ("name"' in the pinned
    // oracle's src/*.c, sorted by byte order and deduplicated; the
    // coercion in prepare_variable_assignment keys on it, so it must not
    // drift from the sources.
    let source_root = upstream_emacs_repo().join("src");
    let pattern = regex::Regex::new(r#"DEFVAR_BOOL \("([^"]+)""#).expect("valid pattern");
    let mut names = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(&source_root).expect("read oracle src directory") {
        let path = entry.expect("directory entry").path();
        if path.extension().is_some_and(|extension| extension == "c") {
            // A few C sources carry non-UTF-8 bytes in comments.
            let bytes = std::fs::read(&path).expect("read C source");
            let text = String::from_utf8_lossy(&bytes);
            for capture in pattern.captures_iter(&text) {
                names.insert(capture[1].to_string());
            }
        }
    }
    let fresh = names.into_iter().collect::<Vec<_>>();
    assert_eq!(
        crate::lisp::primitives::generated_gnu_c_bool_variables::GNU_C_BOOL_VARIABLES,
        fresh.as_slice(),
        "regenerate src/lisp/primitives/generated_gnu_c_bool_variables.rs from the oracle sources"
    );
}

#[test]
fn defvar_bool_stores_coerce_and_makunbound_detaches() {
    // data.c store_symval_forwarding stores `!NILP (newval)' into a
    // DEFVAR_BOOL slot through every store path; DEFVAR_LISP slots keep
    // the object.  set_internal turns `makunbound' of a forwarded symbol
    // into a detached void symbol: `boundp' nil, a void-variable read, and
    // later stores plain (a bool slot no longer coerces).  doc.c keeps
    // reading its own C variable, so `text-quoting-style' the function
    // still answers grave under LANG=C.
    let program = r#"
        (list
         (progn (setq internal--text-quoting-flag 42) internal--text-quoting-flag)
         (let ((internal--text-quoting-flag 'foo)) internal--text-quoting-flag)
         (progn (setq internal--text-quoting-flag nil) internal--text-quoting-flag)
         (let ((inhibit-read-only 7)) inhibit-read-only)
         (progn (setq-default print-escape-newlines 'q) print-escape-newlines)
         (progn (set 'print-escape-newlines 0) print-escape-newlines)
         (progn (set-default 'print-escape-newlines 0) print-escape-newlines)
         (progn (setq print-escape-newlines nil) print-escape-newlines)
         (progn (makunbound 'delete-exited-processes)
                (list (boundp 'delete-exited-processes)
                      (condition-case e delete-exited-processes (error e))
                      (progn (setq delete-exited-processes 5) delete-exited-processes)))
         (progn (makunbound 'gc-cons-threshold)
                (list (boundp 'gc-cons-threshold)
                      (progn (setq gc-cons-threshold 800000) gc-cons-threshold)))
         (progn (makunbound 'text-quoting-style)
                (list (boundp 'text-quoting-style)
                      (condition-case e text-quoting-style (error e))
                      (text-quoting-style))))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "(t t nil 7 t t t nil (nil (void-variable delete-exited-processes) 5) (nil 800000) \
         (nil (void-variable text-quoting-style) grave))",
        "DEFVAR_BOOL",
    );
}

#[test]
fn commandp_follows_fcommandp_order_and_property_error() {
    // eval.c Fcommandp: a void symbol is nil; keyboard macros answer t; a
    // list `lambda' answers from its body and never reaches the property
    // walk; an interpreted closure, a primitive or an alias chain without
    // an interactive spec walks the symbols and an `interactive-form'
    // property there is (error "Found an 'interactive-form' property!").
    let program = r#"
        (list
         (progn (fset 'emaxx-foo-list '(lambda () 1))
                (put 'emaxx-foo-list 'interactive-form '(interactive))
                (condition-case e (commandp 'emaxx-foo-list) (error e)))
         (progn (fset 'emaxx-foo-cmd (lambda () 1))
                (put 'emaxx-foo-cmd 'interactive-form '(interactive))
                (defalias 'emaxx-foo-alias 'emaxx-foo-cmd)
                (condition-case e (commandp 'emaxx-foo-alias) (error e)))
         (progn (put 'car 'interactive-form '(interactive))
                (condition-case e (commandp 'car) (error e)))
         (progn (put 'ignore 'interactive-form '(interactive))
                (condition-case e (commandp 'ignore) (error e)))
         (progn (fset 'emaxx-foo-int (lambda () (interactive) 1))
                (put 'emaxx-foo-int 'interactive-form '(interactive))
                (condition-case e (commandp 'emaxx-foo-int) (error e)))
         (progn (put 'emaxx-foo-void 'interactive-form '(interactive))
                (condition-case e (commandp 'emaxx-foo-void) (error e)))
         (progn (fset 'emaxx-foo-str "abc")
                (put 'emaxx-foo-str 'interactive-form '(interactive))
                (list (commandp 'emaxx-foo-str) (commandp 'emaxx-foo-str t))))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "(nil (error \"Found an 'interactive-form' property!\") (error \"Found an \
         'interactive-form' property!\") t t nil (t nil))",
        "commandp",
    );
}

#[test]
fn tty_color_primitives_follow_xfaces_c() {
    // xfaces.c on a tty frame: color-gray-p and color-supported-p go
    // through tty_defined_color (Lisp `tty-color-desc'), color-distance
    // takes (R G B) lists or names, calls METRIC with the lists, and uses
    // Riemersma's metric otherwise; color-values-from-color-spec parses the
    // numeric X forms only and never names.
    let program = r##"
        (list
         (color-gray-p "gray50") (color-gray-p "snow") (color-gray-p "#818080")
         (color-gray-p "#123456") (color-gray-p "nosuchcolor")
         (color-supported-p "dark slate gray") (color-supported-p "nosuchcolor")
         (color-supported-p "#123456") (color-supported-p "red" nil t)
         (color-distance "red" "blue") (color-distance "#ff0000" "#0000ff")
         (color-distance "red" "red") (color-distance "white" "black")
         (color-distance "gray50" "snow")
         (color-distance '(1000 2000 3000) '(4000 5000 60000))
         (color-distance "red" "blue" nil (lambda (a b) (list a b)))
         (condition-case e (color-distance "nosuch" "red") (error e))
         (condition-case e (color-distance "red" 5) (error e))
         (color-values-from-color-spec "red")
         (color-values-from-color-spec "#ff0000")
         (color-values-from-color-spec "rgb:ff/00/00")
         (color-values-from-color-spec "rgbi:1.0/0.5/0")
         (color-values-from-color-spec "#f00")
         (color-values-from-color-spec "#ffff00000000")
         (color-values-from-color-spec "RED")
         (color-values-from-color-spec "#12345"))"##;
    assert_oracle_contract_matches_interpreter(
        program,
        "(t t t nil nil t nil t t 327669 327669 0 589805 589805 147664 ((65535 0 0) (0 0 65535)) \
         (error \"Invalid color\" \"nosuch\") (error \"Invalid color\" 5) nil (65535 0 0) \
         (65535 0 0) (65535 32768 0) (65535 0 0) (65535 0 0) nil nil)",
        "tty colors",
    );
}

#[test]
fn punct_class_beyond_ascii_follows_buffer_syntax() {
    // regex-emacs.c ISPUNCT: printable non-alphanumeric ASCII, and for
    // every other character `BUFFER_SYNTAX (c) != Sword'.  Both the regexp
    // engine and skip-chars share it.
    let program = r#"
        (list
         (mapcar (lambda (c) (string-match-p "[[:punct:]]" (string c)))
                 '(#xa0 #x3000 #x200b #x202f ?a ?, #x2020 #xb7 #xe9 #x3042))
         (with-temp-buffer
           (insert (string #xa0 #x2020 #xe9 ?a))
           (goto-char (point-min))
           (list (skip-chars-forward "[:punct:]") (point)))
         (with-temp-buffer
           (insert (string #xe9 ?a))
           (modify-syntax-entry #xe9 "." (syntax-table))
           (string-match-p "[[:punct:]]" (string #xe9))))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "((0 0 0 0 nil 0 0 0 nil nil) (2 3) 0)",
        "[:punct:]",
    );
}

#[test]
fn quoting_style_reaches_message_error_text_and_display_table() {
    // editfns.c Fmessage formats through Fformat_message; print.c passes
    // the error-message property through `substitute-command-keys'; doc.c
    // default_to_grave_quoting_style also reads `standard-display-table'
    // (U+2018 shown as [?`] means grave) once the locale flag is set; and
    // fns.c Frequire names the file `load-history' records.
    let program = r#"
        (list
         (let ((text-quoting-style 'curve))
           (list (with-temp-buffer (message "`m'"))
                 (condition-case e (emaxx-undefined-fn-zz) (error (error-message-string e)))
                 (condition-case e (symbol-value 'emaxx-undefined-var-zz)
                   (error (error-message-string e)))))
         (let ((text-quoting-style 'straight)) (message "`m'"))
         (let ((text-quoting-style 'grave)) (message "`m'"))
         (let ((internal--text-quoting-flag t)) (text-quoting-style))
         (let ((internal--text-quoting-flag t)
               (standard-display-table (make-display-table)))
           (aset standard-display-table ?\N{LEFT SINGLE QUOTATION MARK} [?`])
           (text-quoting-style))
         (let ((internal--text-quoting-flag t)
               (standard-display-table (make-display-table)))
           (aset standard-display-table ?\N{LEFT SINGLE QUOTATION MARK} [?` ?`])
           (text-quoting-style))
         (let ((d (make-temp-file "qlp" t)))
           (with-temp-file (expand-file-name "qnp.el" d) (insert "(provide 'other)\n"))
           (let ((load-path (cons d load-path)))
             (condition-case e (require 'qnp)
               (error (list (car e) (string-match-p "\\`Loading file .*/qnp\\.el failed to provide feature `qnp'\\'" (cadr e))))))))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        "((\"\u{2018}m\u{2019}\" \"Symbol\u{2019}s function definition is void: emaxx-undefined-fn-zz\" \
         \"Symbol\u{2019}s value as variable is void: emaxx-undefined-var-zz\") \"'m'\" \"`m'\" \
         curve grave curve (error 0))",
        "quoting style",
    );
}

#[cfg(target_os = "linux")]
#[test]
fn inotify_error_data_follows_report_file_notify_error() {
    // fileio.c report_file_notify_error always places the rendered errno
    // between the message and the offending object, splicing a list (or nil)
    // object in as the tail.  Aspects are converted before FILE-NAME is
    // type-checked, a well-formed but stale descriptor removes nothing and
    // returns t, and IDs for one inode are the lowest free ones.  The
    // "Invalid descriptor " text depends on the errno left by the previous
    // host call in GNU as well, so only its shape is pinned.
    let program = r#"
        (list
         (condition-case e (inotify-add-watch "/tmp" '(bogus) #'ignore)
           (file-notify-error e))
         (condition-case e (inotify-add-watch "/tmp" '(create 7) #'ignore)
           (file-notify-error e))
         (condition-case e (inotify-add-watch "/tmp" '(nil) #'ignore)
           (file-notify-error e))
         (condition-case e (inotify-add-watch 42 '(bogus) #'ignore) (error e))
         (condition-case e (inotify-add-watch 42 '(create) #'ignore) (error e))
         (condition-case e
             (inotify-add-watch "/nonexistent/zz-audit" '(create) #'ignore)
           (file-notify-error e))
         (let ((e (condition-case e (inotify-rm-watch 5) (file-notify-error e))))
           (list (car e) (cadr e) (stringp (nth 2 e)) (nthcdr 3 e)))
         (let ((e (condition-case e (inotify-rm-watch '(a . 1))
                    (file-notify-error e))))
           (list (car e) (cadr e) (stringp (nth 2 e)) (nthcdr 3 e)))
         (inotify-rm-watch '(123456 . 0))
         (let* ((a (inotify-add-watch "/tmp" '(create) #'ignore))
                (b (inotify-add-watch "/tmp" '(delete) #'ignore))
                (c (progn (inotify-rm-watch a)
                          (inotify-add-watch "/tmp" '(attrib) #'ignore))))
           (prog1 (list (eq (car a) (car b)) (cdr a) (cdr b) (cdr c)
                        (inotify-valid-p a) (inotify-valid-p b)
                        (inotify-rm-watch (cons (car b) 99)) (inotify-valid-p b))
             (inotify-rm-watch b) (inotify-rm-watch c)))
         (let ((d (inotify-add-watch "/tmp" '(create) #'ignore)))
           (inotify-rm-watch d)
           (list (inotify-rm-watch d) (inotify-valid-p d))))"#;
    let expected = concat!(
        "((file-notify-error \"Unknown aspect\" \"Invalid argument\" bogus) ",
        "(file-notify-error \"Unknown aspect\" \"Invalid argument\" 7) ",
        "(file-notify-error \"Unknown aspect\" \"Invalid argument\") ",
        "(file-notify-error \"Unknown aspect\" \"Invalid argument\" bogus) ",
        "(wrong-type-argument stringp 42) ",
        "(file-notify-error \"Could not add watch for file\" ",
        "\"No such file or directory\" \"/nonexistent/zz-audit\") ",
        "(file-notify-error \"Invalid descriptor \" t (5)) ",
        "(file-notify-error \"Invalid descriptor \" t (a . 1)) ",
        "t (t 0 1 0 t t t t) (t nil))"
    );
    assert_linux_inotify_contract(program, expected, "inotify error data");
}

#[cfg(target_os = "linux")]
#[test]
fn inotify_events_reach_lisp_only_through_keyboard_reads() {
    // process.c registers the inotify descriptor with
    // add_keyboard_wait_descriptor, so a READ_KBD 0 wait such as
    // accept-process-output or sleep-for never reads it, input-pending-p
    // neither reads nor dispatches, and the callback runs from read_char.
    let program = r#"
        (let* ((events nil)
               (file (make-temp-file "zz-audit-stage"))
               (d (inotify-add-watch file '(modify)
                                     (lambda (ev) (push ev events)))))
          (unwind-protect
              (progn
                (with-temp-file file (insert "y"))
                (list (input-pending-p) (length events)
                      (progn (accept-process-output nil 0.05) (length events))
                      (progn (sleep-for 0.02) (length events))
                      (progn (input-pending-p t) (length events))
                      (progn (read-event nil nil 0.05) (length events))
                      (mapcar (lambda (ev)
                                (list (equal (car ev) d) (cadr ev)
                                      (equal (caddr ev) file) (nth 3 ev)))
                              events)))
            (inotify-rm-watch d)
            (delete-file file)))"#;
    assert_linux_inotify_contract(
        program,
        "(nil 0 0 0 0 1 ((t (modify) t 0)))",
        "inotify delivery stage",
    );
}

#[cfg(target_os = "linux")]
#[test]
fn file_notification_callback_errors_propagate_from_read_event() {
    // read_char executes the special-event binding without a condition
    // handler: the signal leaves read-event, and the buffered events behind
    // it (here the second watch's callback for the same kernel event) are
    // delivered by the next read.  GNU has no "Error in file notification"
    // message; that text was an emaxx invention.
    let program = r#"
        (let* ((events nil)
               (create-lockfiles nil)
               (dir (make-temp-file "zz-audit-err" t))
               (d1 (inotify-add-watch dir '(create) (lambda (_) (error "boom"))))
               (d2 (inotify-add-watch dir '(create)
                                      (lambda (ev) (push (caddr ev) events)))))
          (unwind-protect
              (progn
                (with-temp-file (expand-file-name "a" dir) nil)
                (list (condition-case err (read-event nil nil 0.2) (error err))
                      (length events)
                      (progn (read-event nil nil 0.1) events)
                      (cdr d1) (cdr d2)))
            (inotify-rm-watch d1)
            (inotify-rm-watch d2)
            (delete-directory dir t)))"#;
    assert_linux_inotify_contract(
        program,
        "((error \"boom\") 0 (\"a\") 0 1)",
        "file notification error propagation",
    );
}

#[cfg(target_os = "linux")]
#[test]
fn filenotify_scenarios_match_the_oracle_through_keyboard_reads() {
    // The same watch-isolation, invalidation and no-replay scenarios that
    // the eval tests exercise, run through filenotify.el against the live
    // oracle with read-event waits.  Their earlier `sleep-for' form encoded
    // delivery inside a READ_KBD 0 wait, which GNU never performs.
    let program = r#"
        (progn
          (require 'filenotify)
          (let* ((root (make-temp-file "zz-audit-iso" t))
                 (file (expand-file-name "watched-file" root))
                 (directory (expand-file-name "watched-directory" root))
                 (generation (make-temp-file "zz-audit-gen"))
                 (create-lockfiles nil)
                 events first second directory-watch)
            (unwind-protect
                (progn
                  (with-temp-file file (insert "contents"))
                  (make-directory directory)
                  (setq first (file-notify-add-watch
                               file '(change)
                               (lambda (event)
                                 (when (eq (cadr event) 'deleted) (push 1 events))))
                        second (file-notify-add-watch
                                file '(change)
                                (lambda (event)
                                  (when (eq (cadr event) 'deleted) (push 2 events)))))
                  (file-notify-rm-watch first)
                  (delete-file file)
                  (read-event nil nil 0.1)
                  (setq directory-watch
                        (file-notify-add-watch
                         directory '(change)
                         (lambda (event)
                           (when (eq (cadr event) 'deleted) (push 'directory events)))))
                  (delete-directory directory)
                  (read-event nil nil 0.1)
                  (let ((isolation (list (reverse events)
                                         (file-notify-valid-p first)
                                         (file-notify-valid-p second)
                                         (file-notify-valid-p directory-watch))))
                    (setq events nil)
                    (write-region "before" nil generation nil 'no-message)
                    (setq first (file-notify-add-watch
                                 generation '(change)
                                 (lambda (_event) (push 'first events)))
                          second (file-notify-add-watch
                                  generation '(change)
                                  (lambda (_event) (push 'second events))))
                    (read-event nil nil 0.1)
                    (let ((before events))
                      (write-region "after" nil generation nil 'no-message)
                      (read-event nil nil 0.1)
                      (list isolation
                            (list before (length events)
                                  (not (null (memq 'first events)))
                                  (not (null (memq 'second events))))))))
              (ignore-errors (file-notify-rm-watch first))
              (ignore-errors (file-notify-rm-watch second))
              (delete-file generation)
              (delete-directory root t))))"#;
    assert_linux_inotify_contract(
        program,
        "(((2 directory) nil nil nil) (nil 2 t t))",
        "filenotify keyboard-read scenarios",
    );
}

#[test]
fn copy_family_native_path_uses_handler_expanded_names() {
    // fileio.c's Fadd_name_to_file, Fcopy_file, Frename_file and
    // Fmake_symbolic_link expand their names (Fexpand_file_name and
    // expand_cp_target, both handler-aware) BEFORE the handler lookup and
    // then run the native body on those expanded names.  A handler that
    // rewrites names during expansion and therefore no longer matches must
    // see its rewrite honored; Emaxx used to re-resolve the raw arguments
    // and link the unrewritten name (files-tests' `.special` handler).
    let program = r#"
        (progn
          (defun zz-special-handler (operation &rest args)
            (let ((arg args)
                  (file-name-handler-alist
                   (delete (rassoc 'zz-special-handler file-name-handler-alist)
                           file-name-handler-alist)))
              (while arg
                (when (and (stringp (car arg))
                           (not (file-name-quoted-p (car arg)))
                           (string-match "\\.special\\'" (car arg)))
                  (setcar arg (replace-match "" nil nil (car arg))))
                (setq arg (cdr arg)))
              (apply operation args)))
          (let* ((dir (file-name-as-directory (make-temp-file "zz-cpfam" t)))
                 (real (expand-file-name "base" dir))
                 (file (concat real ".special"))
                 (file-name-handler-alist
                  (cons (cons "\\.special\\'" 'zz-special-handler)
                        file-name-handler-alist)))
            (unwind-protect
                (progn
                  (with-temp-file real (insert "x"))
                  (list
                   (progn (add-name-to-file file (expand-file-name "added.special" dir))
                          (list (file-exists-p (expand-file-name "added" dir))
                                (file-exists-p (expand-file-name "added.special" dir))))
                   (progn (copy-file file (expand-file-name "copied.special" dir))
                          (list (file-exists-p (expand-file-name "copied" dir))
                                (file-exists-p (expand-file-name "copied.special" dir))))
                   (progn (make-directory (expand-file-name "sub" dir))
                          (copy-file file (file-name-as-directory
                                           (expand-file-name "sub" dir)))
                          (directory-files (expand-file-name "sub" dir) nil "^[^.]"))
                   (progn (rename-file (expand-file-name "copied.special" dir)
                                       (expand-file-name "moved.special" dir))
                          (list (file-exists-p (expand-file-name "copied" dir))
                                (file-exists-p (expand-file-name "moved" dir))))
                   (progn (make-symbolic-link "base" (expand-file-name "link.special" dir))
                          (list (file-symlink-p (expand-file-name "link" dir))
                                (file-exists-p (expand-file-name "link.special" dir))))
                   (condition-case e
                       (add-name-to-file file (expand-file-name "added.special" dir))
                     (error (car e)))
                   ;; Fmake_symbolic_link keeps TARGET verbatim; only a
                   ;; fixnum OK-IF-ALREADY-EXISTS expands `~' or drops `/:'.
                   (progn
                     (make-symbolic-link "~/zz-target" (expand-file-name "l1" dir))
                     (make-symbolic-link "~/zz-target" (expand-file-name "l2" dir) 1)
                     (make-symbolic-link "/:/tmp/zz-q" (expand-file-name "l3" dir) 1)
                     (make-symbolic-link "/:/tmp/zz-q" (expand-file-name "l4" dir))
                     (list (file-symlink-p (expand-file-name "l1" dir))
                           (equal (file-symlink-p (expand-file-name "l2" dir))
                                  (expand-file-name "~/zz-target"))
                           (file-symlink-p (expand-file-name "l3" dir))
                           (file-symlink-p (expand-file-name "l4" dir))))
                   ;; Relative names resolve against the Lisp
                   ;; `default-directory' (Fexpand_file_name's nil rule),
                   ;; never the process working directory.  The Linux
                   ;; frozen run caught the cwd form through arc-mode and
                   ;; bytecomp tests that bind default-directory.
                   (let ((default-directory dir))
                     (copy-file "base" "rel-copy")
                     (rename-file "rel-copy" "rel-moved")
                     (add-name-to-file "base" "rel-name")
                     (make-symbolic-link "base" "rel-link")
                     (list (file-exists-p (expand-file-name "rel-moved" dir))
                           (file-exists-p (expand-file-name "rel-name" dir))
                           (file-symlink-p (expand-file-name "rel-link" dir))))))
              (delete-directory dir t))))"#;
    let expected = concat!(
        "((t t) (t t) (\"base\") (nil t) (\"base\" t) file-already-exists ",
        "(\"~/zz-target\" t \"/tmp/zz-q\" \"/:/tmp/zz-q\") (t t \"base\"))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read copy-family handler program")
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate copy-family handler program")
            .to_string(),
        expected
    );
}

#[cfg(target_os = "linux")]
#[test]
fn subr_arity_answers_the_host_inotify_primitives() {
    // The Darwin-derived arity table has no inotify rows; `subr-arity' and
    // `func-arity' must consult the host's C contract, or loading
    // inotify-tests.el fails inside macroexpansion before any test runs.
    let program = r#"
        (list (subr-arity (symbol-function 'inotify-valid-p))
              (subr-arity (symbol-function 'inotify-add-watch))
              (func-arity 'inotify-rm-watch)
              (featurep 'inotify) (fboundp 'kqueue-add-watch))"#;
    assert_linux_inotify_contract(
        program,
        "((1 . 1) (3 . 3) (1 . 1) t nil)",
        "host inotify arity",
    );
}

#[test]
fn skip_chars_word_class_includes_ascii_digits() {
    // The SkipSyntaxSnapshot classifies each segment by sampling its START,
    // which is only correct where the class is uniform across the window.
    // `default_syntax_entry' varies per character INSIDE ASCII, and with
    // boundaries drawn only from explicit char-table entries the window
    // holding the digits sampled as punctuation: `skip-chars-forward
    // "[:word:]"' stopped dead at ?0 while `char-syntax' answered w.  This
    // was the single regression between the 2026-08-25 and 2026-08-27 frozen
    // baselines (regex-tests-word-character-class).
    let program = r#"
        (list (with-temp-buffer (insert "abcABC012\N{U+2620}-, \t\n")
                (goto-char (point-min))
                (skip-chars-forward "[:word:]") (point))
              (with-temp-buffer (insert "012a")
                (goto-char (point-min))
                (skip-chars-forward "[:word:]") (point))
              (with-temp-buffer (insert "C012")
                (goto-char (point-min))
                (skip-chars-forward "[:word:]") (point))
              (char-syntax ?0))"#;
    let expected = "(11 5 5 119)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read skip-word program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate skip-word program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn euc_jp_codec_follows_the_oracle_contract() {
    // The real euc-jp codec behind finding 106's second half (the stub
    // signalled for every non-ASCII character): JIS X 0208 as the high
    // bank, halfwidth katakana behind SS2, latin-jisx0201 designated with
    // ESC ( J and restored before controls/eol, space for unencodable,
    // raw-byte resynchronization on invalid sequences, and encode-char /
    // decode-char through the now-honoured `unify-charset' state.  Every
    // row is the oracle's own answer.
    let program = r#"
        (list
         (append (encode-coding-string (string 12354) 'euc-jp) nil)
         (append (encode-coding-string (string 65393) 'euc-jp) nil)
         (append (encode-coding-string (string 165) 'euc-jp) nil)
         (append (encode-coding-string (string 165 10 97) 'euc-jp) nil)
         (append (encode-coding-string (string 8364) 'euc-jp) nil)
         (append (encode-coding-string (string 97 12354 10 98 12450) 'euc-jp-dos) nil)
         (append (decode-coding-string (unibyte-string 164 162) 'euc-jp) nil)
         (append (decode-coding-string (unibyte-string 142 177) 'euc-jp) nil)
         (append (decode-coding-string (unibyte-string 143 203 174) 'euc-jp) nil)
         (append (decode-coding-string (unibyte-string 164 65 255) 'euc-jp) nil)
         (append (decode-coding-string (unibyte-string 164) 'euc-jp) nil)
         (encode-char 12354 'japanese-jisx0208)
         (decode-char 'japanese-jisx0208 9250)
         (encode-char 65393 'katakana-jisx0201)
         (encode-char 165 'latin-jisx0201))"#;
    let expected = concat!(
        "((164 162) (142 177) (27 40 74 92 27 40 66) (27 40 74 92 27 40 66 10 97) (32) ",
        "(97 164 162 13 10 98 165 162) (12354) (65393) (29476) (4194212 65 4194303) ",
        "(4194212) 9250 12354 49 92)"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read euc-jp program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate euc-jp program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn unify_charset_validates_and_deunifies_like_charset_c() {
    // charset.c:1330 Funify_charset.  Only offset-method charsets whose
    // code-offset lies beyond Unicode can unify; a bad map argument
    // signals (error "Bad unify-map" 42) in a fresh session (GNU's
    // early return is gated on the lazily loaded deunifier, so it does
    // not fire before any encode); de-unification restores the raw
    // code-offset conversion, indexed through :code-space -- jisx0212's
    // code 0x4B2E is index 0x8F79, hence 1347449, not offset + 0x4B2E.
    let program = r#"
        (list
         (unify-charset 'japanese-jisx0208)
         (let ((text-quoting-style 'straight))
           (condition-case e (unify-charset 'ascii) (error (cadr e))))
         (condition-case e (unify-charset 'japanese-jisx0208 42) (error e))
         (progn (unify-charset 'japanese-jisx0212 nil t)
                (prog1 (list (decode-char 'japanese-jisx0212 19246)
                             (encode-char 29476 'japanese-jisx0212))
                  (unify-charset 'japanese-jisx0212)))
         (decode-char 'japanese-jisx0212 19246))"#;
    let expected = concat!(
        "(nil \"Can't unify charset: ascii\" (error \"Bad unify-map\" 42) ",
        "(1347449 nil) 29476)"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read unify-charset program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate unify-charset program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn file_reads_consult_the_coding_alist_and_name_like_the_oracle() {
    // Three findings in one contract, every row the oracle's answer:
    //   - insert-file-contents consults `file-coding-system-alist' via
    //     find-operation-coding-system (fileio.c), so a pure-ASCII .el
    //     file reads as prefer-utf-8-unix, not undecided-unix;
    //   - a detection that concludes bare `undecided' (pure ASCII, no
    //     eol byte anywhere) leaves buffer-file-coding-system nil, while
    //     LF upgrades it to undecided-unix and a BOM-less no-newline
    //     UTF-8 file becomes bare utf-8 in last-coding-system-used but
    //     utf-8-unix in the buffer; an explicit request keeps its own
    //     spelling (`unix', `binary') unless detection resolved a
    //     charset or an eol the request left open;
    //   - non-UTF-8 8-bit junk without C1 controls detects as
    //     iso-latin-1, and a byte in 0x80..0x9F forces raw-text.
    let program = r#"
        (progn
          ;; The default buffer-file-coding-system is the oracle host's
          ;; locale (utf-8 on a desktop, nil under LANG=C); pin it so the
          ;; nil-vs-set contract below is environment-independent.
          (setq-default buffer-file-coding-system nil)
          (defun zz-cf-read (name content wcoding rcoding)
            (let ((f (make-temp-file "emaxx-coding" nil name)))
              (unwind-protect
                  (progn
                    (let ((coding-system-for-write wcoding))
                      (with-temp-file f (insert content)))
                    (with-temp-buffer
                      (let ((coding-system-for-read rcoding))
                        (insert-file-contents f))
                      (list last-coding-system-used buffer-file-coding-system)))
                (delete-file f))))
          (list
           (zz-cf-read ".el" "alpha" 'utf-8 nil)
           (zz-cf-read ".el" "alpha\n" 'utf-8 nil)
           (zz-cf-read ".txt" "alpha" 'utf-8 nil)
           (zz-cf-read ".txt" "alpha\n" 'utf-8 nil)
           (zz-cf-read ".txt" (string 97 192) 'utf-8 nil)
           (zz-cf-read ".txt" (string 97 13 10) 'binary nil)
           (zz-cf-read ".txt" "alpha" 'utf-8 'unix)
           (zz-cf-read ".txt" "alpha" 'utf-8 'utf-8)
           (zz-cf-read ".txt" "alpha
" 'utf-8 'binary)
           (zz-cf-read ".txt" (string 12354 10) 'euc-jp 'euc-jp)
           (zz-cf-read ".txt" (apply #'unibyte-string '(97 255 254)) 'binary nil)
           (zz-cf-read ".txt" (apply #'unibyte-string '(97 129 10)) 'binary nil)
           ;; a unibyte buffer suppresses every conversion except eol:
           ;; valid UTF-8 stays as its bytes and the read is raw-text
           (let ((f (make-temp-file "emaxx-coding" nil ".txt")))
             (unwind-protect
                 (progn
                   (let ((coding-system-for-write 'binary))
                     (with-temp-file f (insert (unibyte-string 195 128 10))))
                   (with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert-file-contents f)
                     (list last-coding-system-used buffer-file-coding-system
                           (append (buffer-string) nil))))
               (delete-file f)))
           (car (find-operation-coding-system 'insert-file-contents "any.el"))))"#;
    let expected = concat!(
        "((prefer-utf-8 prefer-utf-8-unix) (prefer-utf-8-unix prefer-utf-8-unix) ",
        "(undecided nil) (undecided-unix undecided-unix) (utf-8 utf-8-unix) ",
        "(undecided-dos undecided-dos) (unix undecided-unix) (utf-8 utf-8-unix) ",
        "(binary no-conversion) (japanese-iso-8bit-unix japanese-iso-8bit-unix) ",
        "(iso-latin-1 iso-latin-1-unix) (raw-text-unix raw-text-unix) ",
        "(raw-text-unix raw-text-unix (195 128 10)) prefer-utf-8)"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read file-coding program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate file-coding program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn string_decode_names_last_coding_system_used_like_the_oracle() {
    // coding.c's decoder records the coding it actually used: the
    // requested spelling survives verbatim (the euc-jp alias stays
    // `euc-jp', canonicalizing was this port's invention), pure-ASCII
    // input without a CR never re-resolves the name (LF included), and
    // only a decode that both converted something and saw an eol byte
    // gains the canonical eol subsidiary (japanese-iso-8bit-unix).
    let program = r#"
        (list
         (progn (decode-coding-string "alpha" 'undecided) last-coding-system-used)
         (progn (decode-coding-string "alpha\n" 'undecided) last-coding-system-used)
         (progn (decode-coding-string (unibyte-string 97 13 10) 'undecided)
                last-coding-system-used)
         (progn (decode-coding-string (unibyte-string 195 128) 'undecided)
                last-coding-system-used)
         (progn (decode-coding-string "alpha\n" 'utf-8) last-coding-system-used)
         (progn (decode-coding-string (unibyte-string 164 162) 'euc-jp)
                last-coding-system-used)
         (progn (decode-coding-string (unibyte-string 164 162 10) 'euc-jp)
                last-coding-system-used)
         (progn (decode-coding-string (unibyte-string 164 162) 'euc-jp-unix)
                last-coding-system-used)
         (progn (encode-coding-string "a" 'euc-jp) last-coding-system-used)
         (progn (decode-coding-string (unibyte-string 97 255 254) 'undecided)
                last-coding-system-used))"#;
    let expected = concat!(
        "(undecided undecided undecided-dos utf-8 utf-8 euc-jp ",
        "japanese-iso-8bit-unix euc-jp-unix euc-jp iso-latin-1)"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read lcsu program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate lcsu program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn undecided_decode_detects_shift_jis_like_coding_c() {
    // coding.c:detect_coding_sjis accepts 0x81..0x9F leads only with a
    // 0x40..0xFC trail other than 0x7F, and rejects an incomplete lead in
    // the final block.  The first row is the sole residual from the round-2
    // 132-case decode matrix; its 0x81 0x62 pair is JIS #x2143 (U+FF5C).
    // The overlap rows pin the category order which an SJIS-only matrix
    // missed: the live Latin-extra table admits 0x91..0x96, valid Emacs-Mule
    // wins before SJIS, and private or unmappable Mule forms preserve bytes.
    let program = r#"
        (progn
          (defun zz-sjis-detect (bytes)
            (let ((decoded
                   (decode-coding-string
                    (apply #'unibyte-string bytes) 'undecided)))
              (list (append decoded nil) last-coding-system-used)))
          (list
           (zz-sjis-detect '(97 129 98))
           (progn (decode-coding-string (unibyte-string 129) 'undecided)
                  last-coding-system-used)
           (zz-sjis-detect '(129 64))
           (progn (decode-coding-string (unibyte-string 129 127) 'undecided)
                  last-coding-system-used)
           (zz-sjis-detect '(129 160))
           (zz-sjis-detect '(130 160))
           (zz-sjis-detect '(131 160))
           (zz-sjis-detect '(137 160))
           (zz-sjis-detect '(139 160))
           (zz-sjis-detect '(144 160))
           (zz-sjis-detect '(144 160 160))
           (zz-sjis-detect '(145 160))
           (zz-sjis-detect '(150 64))
           (zz-sjis-detect '(151 160))
           (zz-sjis-detect '(154 160 160))
           (zz-sjis-detect '(156 160 160 160))
           (zz-sjis-detect '(137 161))
           (let ((old (aref latin-extra-code-table 129)))
             (unwind-protect
                 (progn
                   (aset latin-extra-code-table 129 t)
                   (zz-sjis-detect '(129 64)))
               (aset latin-extra-code-table 129 old)))))
    "#;
    let expected = concat!(
        "(((97 65372) japanese-shift-jis) raw-text ",
        "((12288) japanese-shift-jis) raw-text ",
        "((160) emacs-mule) ((160) emacs-mule) ((160) emacs-mule) ",
        "((4194185 4194208) emacs-mule) ((20384) japanese-shift-jis) ",
        "((25722) japanese-shift-jis) ",
        "((4194192 4194208 4194208) emacs-mule) ",
        "((145 160) iso-latin-1) ((150 64) iso-latin-1) ",
        "((35023) japanese-shift-jis) ",
        "((4194202 4194208 4194208) emacs-mule) ",
        "((4194204 4194208 4194208 4194208) emacs-mule) ",
        "((65377) emacs-mule) ((129 64) iso-latin-1))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read Shift-JIS detection program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate Shift-JIS detection program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn string_byte_conversions_use_the_internal_encoding() {
    // character.c: `string-as-unibyte' exposes the INTERNAL (UTF-8)
    // bytes and `string-as-multibyte' reads them back (what stood here
    // used the latin-1 byte below 0x100, signalled above it, and
    // panicked on any 8-bit byte in as-multibyte); `string-make-unibyte'
    // takes the low byte of a character with no unibyte equivalent, and
    // `string-make-multibyte' turns non-ASCII bytes into eight-bit
    // characters, not latin-1.  Oracle answers throughout.
    let program = r#"
        (with-suppressed-warnings ((obsolete string-as-unibyte
                                             string-as-multibyte
                                             string-make-unibyte
                                             string-make-multibyte))
          (list
           (append (string-as-unibyte (string 192)) nil)
           (append (string-as-unibyte (string 12354)) nil)
           (append (string-to-multibyte (string-as-unibyte (string 192))) nil)
           (append (string-as-multibyte (unibyte-string 195 128)) nil)
           (append (string-as-multibyte (unibyte-string 195)) nil)
           (append (string-make-unibyte (string 192)) nil)
           (append (string-make-unibyte (string 12354)) nil)
           (append (string-make-multibyte (unibyte-string 192)) nil)))"#;
    let expected = concat!(
        "((195 128) (227 129 130) (4194243 4194176) (192) (4194243) ",
        "(192) (66) (4194240))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read byte-conversion program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate byte-conversion program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn sjis_conversion_follows_the_oracle_contract() {
    // The real Shift-JIS behind finding 107 (the stubs knew one pair).
    // The sjis-char primitives convert through Vsjis_coding_system --
    // the LAST defined shift-jis system, japanese-shift-jis-2004, so
    // their kanji bank is JIS X 0213 plane 1 (#x8940 is 38498 and the
    // euro sign ENCODES as #x8540 through JISX2131's 0x2921), while the
    // `sjis' string codec stays on japanese-shift-jis's JIS X 0208.
    // encode-sjis-char pushes whatever code the charset search found
    // through JIS_TO_SJIS -- a katakana code 0x31 becomes 0x70AF.
    // (encode-sjis-char #xA5) is NOT probed: GNU's unencodable path
    // aborts the oracle binary; see the ledger.
    let program = r#"
        (list
         (decode-sjis-char #x82A0)
         (decode-sjis-char #x8940)
         (decode-sjis-char #xB1)
         (condition-case e (decode-sjis-char #x8040) (error e))
         (condition-case e (decode-sjis-char #xA0) (error e))
         (condition-case e (decode-sjis-char -1) (error e))
         (condition-case e (decode-sjis-char (ash 1 30)) (error e))
         (encode-sjis-char #x3042)
         (encode-sjis-char #xFF71)
         (encode-sjis-char #x20AC)
         (append (encode-coding-string (string #x3042 #xFF71 10) 'sjis) nil)
         (append (encode-coding-string (string #x3042 10) 'sjis-dos) nil)
         (append (encode-coding-string (string #x20AC) 'sjis) nil)
         (append (decode-coding-string (unibyte-string #x82 #xA0 #xB1 10) 'sjis) nil)
         (append (decode-coding-string (unibyte-string #xA0 #x41) 'sjis) nil)
         (append (decode-coding-string (unibyte-string #x82 #x7F #x41) 'sjis) nil)
         (append (decode-coding-string (unibyte-string #x82) 'sjis) nil))"#;
    let expected = concat!(
        "(12354 38498 65393 (error \"Invalid code: 32832\") ",
        "(error \"Invalid code: 160\") (wrong-type-argument wholenump -1) ",
        "(error \"Invalid code: 1073741824\") 33440 28847 34112 ",
        "(130 160 177 10) (130 160 13 10) (32) (12354 65393 10) ",
        "(4194208 65) (4194178 127 65) (4194178))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read sjis program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate sjis program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn big5_conversion_follows_the_oracle_contract() {
    // Big5 through the BIG5 charset map.  decode-big5-char reproduces
    // coding.c's own bug: Fdecode_big5_char masks the second byte with
    // 0x7F before validating, so every code whose low byte has bit 7
    // set -- #xA4A4 among them -- signals "Invalid code" while the
    // encode direction produces exactly that code.  The string codec
    // has no such mask.  Every row is the oracle's answer.
    let program = r#"
        (list
         (decode-big5-char #xA440)
         (condition-case e (decode-big5-char #xA4A4) (error e))
         (condition-case e (decode-big5-char #xA000) (error e))
         (encode-big5-char #x4E2D)
         (encode-big5-char #x20AC)
         (append (encode-coding-string (string #x4E2D 10) 'big5) nil)
         (append (encode-coding-string (string #x20AC) 'big5) nil)
         (append (encode-coding-string (string #x3042) 'big5) nil)
         (append (decode-coding-string (unibyte-string #xA4 #xA4 10) 'big5) nil)
         (append (decode-coding-string (unibyte-string #xA1 #x30 #x41) 'big5) nil)
         (append (decode-coding-string (unibyte-string #xA1 #xA1) 'big5) nil)
         (append (decode-coding-string (encode-coding-string "a中€" 'big5) 'big5) nil)
         (progn (decode-coding-string (unibyte-string #xA4 #xA4) 'big5)
                last-coding-system-used))"#;
    let expected = concat!(
        "(19968 (error \"Invalid code: 42148\") (error \"Invalid code: 40960\") ",
        "42148 41953 (164 164 10) (163 225) (32) (20013 10) ",
        "(4194209 48 65) (65115) (97 20013 8364) big5)"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read big5 program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate big5 program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn garbage_collect_reports_the_live_census() {
    // Finding 110: every count was a fabricated 0.  The alist shape --
    // nine rows, this exact order, these exact lengths, integer columns
    // throughout -- is GNU's (alloc.c, oracle-confirmed); the numbers are
    // emaxx's own live reachability census, which no oracle row can pin
    // (GNU's counts are its allocator's heap state).
    let shape_program = r#"
        (mapcar (lambda (entry)
                  (list (car entry)
                        (length entry)
                        (null (delq t (mapcar #'integerp (cdr entry))))))
                (garbage-collect))"#;
    let expected = concat!(
        "((conses 4 t) (symbols 4 t) (strings 4 t) (string-bytes 3 t) ",
        "(vectors 3 t) (vector-slots 4 t) (floats 4 t) (intervals 4 t) ",
        "(buffers 3 t))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {shape_program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(shape_program)
        .read_all()
        .expect("read garbage-collect shape program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate garbage-collect shape program");
    assert_eq!(result.to_string(), expected);

    // The census itself: a booted image holds its preloaded Lisp, so the
    // discriminating counts cannot be anywhere near the old zeros.
    let counts_form = Reader::new(
        r#"(mapcar (lambda (entry) (cons (car entry) (nth 2 entry)))
                   (garbage-collect))"#,
    )
    .read_all()
    .expect("read garbage-collect counts program")
    .remove(0);
    let counts = interp
        .eval(&counts_form, &mut Vec::new())
        .expect("evaluate garbage-collect counts program")
        .to_string();
    let count_of = |key: &str| -> i64 {
        let marker = format!("({key} . ");
        let start = counts.find(&marker).map(|at| at + marker.len());
        start
            .and_then(|at| counts[at..].split(')').next())
            .and_then(|digits| digits.parse().ok())
            .unwrap_or_else(|| panic!("no {key} count in {counts}"))
    };
    assert!(count_of("conses") > 10_000, "conses: {counts}");
    assert!(count_of("symbols") > 1_000, "symbols: {counts}");
    assert!(count_of("strings") > 100, "strings: {counts}");
    assert!(count_of("string-bytes") > 10_000, "string-bytes: {counts}");
    assert!(count_of("buffers") >= 1, "buffers: {counts}");
}

#[test]
fn overriding_keymaps_follow_the_oracle_contract() {
    // Finding 109: dispatch suppressed local and minor maps under
    // overriding-terminal-local-map unless subr.el's `add-keymap-witness'
    // marker was present -- a rule keymap.c does not have in either
    // direction.  GNU 30.2 (keymap.c:1657): overriding-terminal-local-map
    // suppresses NOTHING and rides on top of the keymap-property, minor
    // and local maps; overriding-local-map replaces them, and only while
    // overriding-terminal-local-map is nil; where-is searches with the
    // overriding maps out of force entirely (keymap.c:2653).
    let program = r#"
        (let ((transient (make-sparse-keymap))
              (local (make-sparse-keymap))
              (minor (make-sparse-keymap))
              (result ()))
          (define-key transient "t" 'zz-transient-cmd)
          (define-key local "l" 'zz-local-cmd)
          (define-key minor "m" 'zz-minor-cmd)
          (define-key minor "l" 'zz-minor-l-cmd)
          (with-temp-buffer
            (use-local-map local)
            (setq-local zz-keymap-witness-minor-mode t)
            (let ((minor-mode-map-alist
                   (list (cons 'zz-keymap-witness-minor-mode minor))))
              (push (list :baseline (key-binding "l") (key-binding "m")
                          (key-binding "t"))
                    result)
              (let ((overriding-terminal-local-map transient))
                (push (list :otlp (key-binding "l") (key-binding "m")
                            (key-binding "t") (key-binding "a"))
                      result))
              (let ((overriding-terminal-local-map nil))
                (internal-push-keymap transient
                                      'overriding-terminal-local-map)
                (push (list :witness (key-binding "l") (key-binding "t"))
                      result))
              (let ((overriding-local-map transient))
                (push (list :olp (key-binding "l") (key-binding "m")
                            (key-binding "t") (key-binding "a"))
                      result))
              (let ((overriding-terminal-local-map transient)
                    (overriding-local-map local))
                (push (list :both (key-binding "l") (key-binding "t"))
                      result))
              (push (list :where-is
                          (let ((overriding-terminal-local-map transient))
                            (where-is-internal 'zz-transient-cmd nil t)))
                    result)))
          (nreverse result))"#;
    let expected = concat!(
        "((:baseline zz-minor-l-cmd zz-minor-cmd self-insert-command) ",
        "(:otlp zz-minor-l-cmd zz-minor-cmd zz-transient-cmd self-insert-command) ",
        "(:witness zz-minor-l-cmd zz-transient-cmd) ",
        "(:olp self-insert-command self-insert-command zz-transient-cmd ",
        "self-insert-command) ",
        "(:both zz-minor-l-cmd zz-transient-cmd) ",
        "(:where-is nil))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read overriding-keymap program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate overriding-keymap program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn max_lisp_eval_depth_is_honoured_dynamically() {
    // eval.c:2504-2509.  The limit came from the GLOBAL cell, so a `let' was
    // invisible; it was then multiplied by 384 and floored at 307200, so the
    // variable could not lower it at all; and exceeding it raised a plain
    // `error' rather than `excessive-lisp-nesting' (audit finding 105).
    //
    // The first row is the discriminating one: under a deliberately small
    // binding a runaway recursion ran to completion and returned `done'.
    // The second pins eval.c:2506's floor -- a limit below 100 is RAISED to
    // 100, not rejected -- and the third pins that ordinary recursion under
    // the default limit still succeeds, which is what the old scaling was
    // there to protect.
    let program = r#"
        (progn
          (defun zz-depth-rec (n) (if (> n 0) (zz-depth-rec (1- n)) 'done))
          (list (condition-case error
                    (let ((max-lisp-eval-depth 100)) (zz-depth-rec 500))
                  (error (car error)))
                (condition-case error
                    (let ((max-lisp-eval-depth 5)) (zz-depth-rec 50))
                  (error (car error)))
                (condition-case error (zz-depth-rec 200) (error (car error)))
                ;; A NEGATIVE limit floors to 100 like any sub-100 value.
                ;; Converting before clamping turned it into the 1600 default
                ;; -- larger than requested, where GNU makes it smaller.
                (condition-case error
                    (let ((max-lisp-eval-depth -5)) (zz-depth-rec 500))
                  (error (car error)))
                (get 'excessive-lisp-nesting 'error-conditions)))"#;
    let expected = concat!(
        "(excessive-lisp-nesting excessive-lisp-nesting done ",
        "excessive-lisp-nesting (excessive-lisp-nesting recursion-error error))"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read depth program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate depth program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn get_unused_iso_final_char_scans_the_registered_charsets() {
    // charset.c:1406 scans `0'..`?' for a final char no charset of this
    // DIMENSION and CHARS has claimed.  This returned the constant ?0 with
    // both arguments unread (audit finding 104).
    //
    // The (1 94) row is the discriminating one twice over: it answers ?6, so
    // a constant ?0 fails, AND getting there requires GNU's actual bucketing
    // rule -- charset.c:1395 reduces CHARS to the BOOLEAN `chars == 96', so
    // `arabic-digit', whose charset-chars is 9, shares the 94 bucket and
    // claims ?2.  Comparing the numbers for equality answers ?2 here instead.
    let program = r#"
        (list (get-unused-iso-final-char 1 94)
              (get-unused-iso-final-char 1 96)
              (get-unused-iso-final-char 2 94)
              (get-unused-iso-final-char 2 96)
              (get-unused-iso-final-char 3 94)
              (get-unused-iso-final-char 3 96)
              (condition-case error (get-unused-iso-final-char 1 ?0)
                (error (cadr error)))
              (condition-case error (get-unused-iso-final-char 9 94)
                (error (cadr error)))
              ;; The WHOLE error object, not just its car: charset.c:1387 is
              ;; CHECK_FIXNUM, which names `fixnump', and comparing only the
              ;; condition symbol let `integerp' pass here undetected.
              (condition-case error (get-unused-iso-final-char "x" 94)
                (error error))
              (condition-case error (get-unused-iso-final-char 1 "x")
                (error error))
              (condition-case error (get-unused-iso-final-char 1.0 94)
                (error error))
              ;; charset.c:1440 writes equivalence declarations into the same
              ;; table this primitive reads, so declaring one must consume the
              ;; slot it names.
              (progn (declare-equiv-charset 1 94 ?6 'ascii)
                     (get-unused-iso-final-char 1 94)))"#;
    let expected = concat!(
        "(54 51 50 48 48 48 \"Invalid CHARS 48, it should be 94 or 96\" ",
        "\"Invalid DIMENSION 9, it should be 1, 2, or 3\" ",
        "(wrong-type-argument fixnump \"x\") ",
        "(wrong-type-argument fixnump \"x\") ",
        "(wrong-type-argument fixnump 1.0) 55)"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read iso-final-char program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate iso-final-char program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn gnutls_digests_are_queried_from_the_library() {
    // gnutls.c:2713 walks `gnutls_digest_list' and asks the library for each
    // algorithm's name and hash length.  Emaxx carried a transcribed 9-entry
    // table in the oracle's exact order while its cipher and mac neighbours
    // in the same file were queried live through dlopen (audit finding 100).
    //
    // Compared against the live oracle rather than a literal: the catalogue is
    // a property of the host's GnuTLS, so a hardcoded expectation here would
    // just be the same transcription in a different file.
    //
    // HONEST LIMIT: this pins CORRECTNESS, not liveness.  The table it
    // replaced happened to match this host's GnuTLS exactly -- that is how it
    // was built -- so restoring the constant would leave this test green.
    // Only a host whose GnuTLS lists a different set would tell them apart.
    // That the query is live rests on reading the code, as with finding 101.
    let program = "(prin1-to-string (gnutls-digests))";
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read gnutls-digests program")
        .remove(0);
    let rendered = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate gnutls-digests program");
    let rendered = string_text(&rendered).expect("prin1-to-string returns a string");
    assert_upstream_primitive_contract(&format!("(princ {program})"), &rendered);

    // A catalogue that came from the library has entries; an empty answer
    // would satisfy an oracle comparison only if GnuTLS were missing, and
    // this host has it.
    assert!(
        rendered.starts_with("((") && rendered.contains(":digest-algorithm-length"),
        "expected a populated digest catalogue, got {rendered}"
    );
}

#[test]
fn set_network_process_option_applies_the_option_or_refuses() {
    // process.c:2962.  The arm this replaces resolved the process, confirmed
    // it was a network process, and returned t WITHOUT EVER READING THE
    // OPTION (audit finding 103) -- so an unsupported option reported success
    // and nothing was set.  It also accepted 2 arguments where GNU needs 3.
    //
    // The `:unknown' row is the one that discriminates: fabricated success
    // returns t there where GNU signals.
    // SO_BINDTODEVICE reaches the real kernel, and what comes back is the
    // host's answer: the loopback's name, whether this caller may bind to
    // it (Linux wants CAP_NET_RAW), and the errno text all vary by
    // platform and privilege.  Those rows are wrapped so both runtimes
    // catch the same outcome, the device is the host's own loopback name,
    // and the result is compared live-to-live instead of pinning one
    // container's socket behavior; the anchors underneath hold the
    // platform-free discriminating rows (finding 103) in place.
    let loopback = if cfg!(target_os = "linux") {
        "lo"
    } else {
        "lo0"
    };
    let program = format!(
        r#"
        (let ((server (make-network-process
                       :name "emaxx-sockopt" :server t :host 'local
                       :service t :family 'ipv4)))
          (prog1
              (list (set-network-process-option server :broadcast t)
                    (plist-get (process-contact server t) :broadcast)
                    (condition-case error
                        (set-network-process-option server :nosuchopt t)
                      (error (cadr error)))
                    (set-network-process-option server :nosuchopt t t)
                    (condition-case error
                        (set-network-process-option server :broadcast)
                      (error (car error)))
                    (condition-case error
                        (set-network-process-option server 42 t)
                      (error (car error)))
                    ;; process.c:2881 matches by NAME, so an uninterned
                    ;; keyword is still :broadcast.  Matching Emaxx's raw
                    ;; symbol name would answer "Unknown" here.
                    (set-network-process-option
                     server (make-symbol ":broadcast") t)
                    ;; SO_BINDTODEVICE is defined on this platform and GNU
                    ;; accepts it; omitting it regressed the option to an
                    ;; error, which the old cheat had matched by accident.
                    ;; Whether THIS caller may bind to the loopback is the
                    ;; kernel's decision (Linux wants CAP_NET_RAW), so both
                    ;; runtimes catch the same outcome instead of pinning it.
                    (condition-case error
                        (set-network-process-option
                         server :bindtodevice "{loopback}")
                      (error (list 'refused (car error))))
                    (plist-get (process-contact server t) :bindtodevice)
                    (condition-case error
                        (set-network-process-option server :bindtodevice 42)
                      (error (cadr error)))
                    ;; process.c:2940 raises a file-error carrying the option
                    ;; and value as DATA, not an error with them in the text.
                    (condition-case error
                        (set-network-process-option
                         server :bindtodevice "nosuchdev0")
                      (error error))
                    ;; process.c:2846 compiles :priority only where
                    ;; SO_PRIORITY exists (GNU/Linux yes, Darwin no), so the
                    ;; same rows read "applied" on one platform and "Unknown
                    ;; or unsupported option" on the other -- which is
                    ;; exactly what live-to-live comparison verifies.
                    (condition-case error
                        (set-network-process-option server :priority 3)
                      (error (cadr error)))
                    (plist-get (process-contact server t) :priority)
                    (condition-case error
                        (set-network-process-option server :priority "x")
                      (error (cadr error)))
                    ;; Out-of-int-range linger: GNU ignores it rather than
                    ;; truncating it into the kernel.
                    (set-network-process-option server :linger 3000000000)
                    ;; process.c:2990 stores the CALLER's symbol and plist_put
                    ;; compares with EQ, so a foreign-obarray keyword sets the
                    ;; option yet stays invisible to a plist-get with the
                    ;; interned one.  Storing a reconstructed symbol answered t
                    ;; here where GNU answers nil.
                    (let ((elsewhere (obarray-make)))
                      (set-network-process-option
                       server (intern ":keepalive" elsewhere) t))
                    (plist-get (process-contact server t) :keepalive)
                    ;; process.c:2954 `list2 (opt, val)' carries the CALLER's
                    ;; symbol, so `eq' against it succeeds and `eq' against the
                    ;; interned keyword fails.  Rebuilding the symbol in the
                    ;; error data inverted both.
                    (let* ((elsewhere (obarray-make))
                           (key (intern ":bindtodevice" elsewhere)))
                      (condition-case error
                          (set-network-process-option server key "nosuchdev0")
                        (file-error (list (eq (nth 3 error) key)
                                          (eq (nth 3 error) :bindtodevice))))))
            (delete-process server)))"#
    );

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(&program)
        .read_all()
        .expect("read socket-option program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate socket-option program");
    let rendered = result.to_string();
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), &rendered);
    assert!(
        rendered.starts_with(
            "(t t \"Unknown or unsupported option\" nil \
             wrong-number-of-arguments wrong-type-argument t "
        ),
        "platform-free socket-option rows changed shape: {rendered}"
    );
    assert!(
        rendered.contains("\"Bad option value for :bindtodevice\""),
        "SOPT_STR type check row changed shape: {rendered}"
    );
    assert!(
        rendered.ends_with(" t t nil (t nil))"),
        "identity and linger rows changed shape: {rendered}"
    );
    // GNU/Linux compiles SO_PRIORITY in; oracle probe sopri.el pins the
    // applied/recorded/refused triple for this platform.
    #[cfg(target_os = "linux")]
    assert!(
        rendered.contains(" t 3 \"Bad option value for :priority\" "),
        "SO_PRIORITY rows changed shape: {rendered}"
    );
}

#[test]
fn file_name_case_insensitivity_is_asked_of_the_filesystem() {
    // fileio.c:2689 walks up the tree until pathconf answers.  A constant --
    // which is what this returned before audit finding 108 -- satisfies
    // files-tests-file-name-non-special-file-name-case-insensitive-p
    // trivially, because that test compares the predicate against itself.
    // Every row below discriminates: on a case-insensitive volume /tmp is t,
    // so constant nil fails, and a missing path is nil, so constant t fails.
    let program = r#"
        (list (file-name-case-insensitive-p "/tmp")
              (file-name-case-insensitive-p "/")
              (file-name-case-insensitive-p "/nonexistent-zzz/deep/file")
              (condition-case error
                  (file-name-case-insensitive-p 42)
                (error (car error))))"#;
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read case-insensitivity program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate case-insensitivity program");
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), &result.to_string());

    // Pin the discrimination itself, so the contract cannot be satisfied by a
    // constant even if the oracle and Emaxx ever agreed on one.  Only on a
    // case-INsensitive volume: on a case-sensitive APFS volume (a supported
    // macOS setup) or on Linux, every answer is legitimately nil and this
    // would fail against a CORRECT implementation.
    #[cfg(target_os = "macos")]
    if crate::lisp::primitives::file_name_case_insensitive_err("/tmp") < 0 {
        let answers = result.to_vec().expect("case-insensitivity list");
        assert_ne!(
            answers[0], answers[2],
            "an existing path and a missing one must not answer alike"
        );
    }
}

#[test]
fn operating_system_release_is_wired_to_the_uname_syscall() {
    // editfns.c:136-141 fills this from the `uname' syscall.  It used to be
    // the literal "25.6.0" -- this host's own release (audit finding 101).
    //
    // BE CLEAR ABOUT WHAT THIS CAN AND CANNOT SHOW.  No on-host test can
    // distinguish a transcription of THIS machine's release from a computed
    // one: the oracle says "25.6.0", `uname' says "25.6.0", and so did the
    // hardcoded literal.  This test pins the WIRING -- that the binding
    // answers whatever `uname_field' answers -- and would catch the value
    // drifting away from the syscall or the binding being dropped.  It would
    // NOT fail if someone reintroduced the literal today.  That the cheat is
    // gone rests on reading eval.rs, not on this assertion.
    let expected =
        crate::lisp::primitives::uname_field(crate::lisp::primitives::UnameField::Release)
            .expect("uname(2) should answer on a supported host");
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new("operating-system-release")
        .read_all()
        .expect("read operating-system-release")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate operating-system-release");
    assert_eq!(result, Value::String(expected.into()));
}

#[test]
fn text_quoting_default_is_derived_from_the_process_locale() {
    // The rest of the quoting tests pin `internal--text-quoting-flag' so they
    // do not inherit the ambient LANG.  That leaves the derivation itself --
    // emacs.c:1665 `text_quoting_flag = using_utf8 ()' -- untested, which is
    // the whole premise of the change.  Pin it here instead: with NO bindings
    // at all the answer must track the locale probe, and the variable must be
    // a real global (GNU's DEFVAR_BOOL answers t to `default-boundp').
    let utf8 = crate::lisp::primitives::values::locale_uses_utf8();

    // Pin the probe itself against the environment, not just the wiring that
    // reads it.  POSIX precedence: LC_ALL, then LC_CTYPE, then LANG.
    let locale = ["LC_ALL", "LC_CTYPE", "LANG"]
        .into_iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()));
    match locale.as_deref() {
        Some(name) if name.to_ascii_uppercase().contains("UTF-8") => assert!(
            utf8,
            "locale {name} names UTF-8 but the mbrtowc probe disagreed"
        ),
        Some("C" | "POSIX") => assert!(
            !utf8,
            "locale {locale:?} is the C locale but the mbrtowc probe claimed UTF-8"
        ),
        // Any other named locale, or none at all, is not decidable from the
        // name alone -- leave the probe unchallenged rather than guess.
        _ => {}
    }

    let expected_flag = if utf8 { "t" } else { "nil" };
    let expected_style = if utf8 { "curve" } else { "grave" };
    let expected = format!("({expected_flag} {expected_style} t t)");

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(
        "(list internal--text-quoting-flag
               (text-quoting-style)
               (default-boundp 'internal--text-quoting-flag)
               (special-variable-p 'internal--text-quoting-flag))",
    )
    .read_all()
    .expect("read locale-derived quoting program")
    .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate locale-derived quoting program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn interactive_form_prefers_the_property_over_advice_and_walks_aliases() {
    // data.c:1141-1151 consults an `interactive-form' property FIRST, walking
    // the symbol-function alias chain, before inspecting the function object.
    // Widening the OClosure test to compiled advice objects made that ordering
    // observable for every advised function, so pin it.  `commandp' is asked
    // only about symbols WITHOUT the property: eval.c:2282-2291 signals
    // outright when one is present, which is a separate divergence.
    let program = r#"
        (progn
          (defun emaxx-adv-cmd (x) (interactive "p") x)
          (defun emaxx-adv-plain (x) x)
          (advice-add 'emaxx-adv-cmd :around (lambda (orig &rest a) (apply orig a)))
          (advice-add 'emaxx-adv-plain :around (lambda (orig &rest a) (apply orig a)))
          (defun emaxx-adv-target (x) x)
          (put 'emaxx-adv-target 'interactive-form '(interactive "M"))
          (defalias 'emaxx-adv-alias 'emaxx-adv-target)
          (defun emaxx-adv-both (x) (interactive "p") x)
          (advice-add 'emaxx-adv-both :around (lambda (orig &rest a) (apply orig a)))
          (put 'emaxx-adv-both 'interactive-form '(interactive "P"))
          (list (interactive-form 'emaxx-adv-cmd)
                (commandp 'emaxx-adv-cmd)
                (interactive-form 'emaxx-adv-plain)
                (commandp 'emaxx-adv-plain)
                (interactive-form 'emaxx-adv-alias)
                (interactive-form 'emaxx-adv-both)
                (interactive-form 'emaxx-adv-nosuch)))"#;
    let expected = concat!(
        "((interactive \"p\") t nil nil (interactive \"M\") ",
        "(interactive \"P\") nil)"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read advice interactive-form program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate advice interactive-form program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn text_quoting_style_follows_the_locale_flag_only_for_a_nil_setting() {
    // doc.c:679: NIL consults `default_to_grave_quoting_style' (whose first
    // test is the locale-derived `internal--text-quoting-flag', emacs.c:1665
    // `text_quoting_flag = using_utf8 ()'), `grave' and `straight' are
    // returned as-is, and "any other value is treated as `curve'".  Routing
    // unmatched values through the flag made a bogus style answer grave in a
    // non-UTF-8 locale, which is how the compatibility harness runs.
    let program = r#"
        (mapcar
         (lambda (case)
           (let ((internal--text-quoting-flag (car case))
                 (text-quoting-style (cadr case)))
             (list (text-quoting-style) (format-message "`a'"))))
         '((t nil) (nil nil) (t grave) (nil curve) (t straight)
           (nil foo) (t foo) (nil 42)))"#;
    let expected = concat!(
        "((curve \"\u{2018}a\u{2019}\") (grave \"`a'\") (grave \"`a'\") ",
        "(curve \"\u{2018}a\u{2019}\") (straight \"'a'\") ",
        "(curve \"\u{2018}a\u{2019}\") (curve \"\u{2018}a\u{2019}\") ",
        "(curve \"\u{2018}a\u{2019}\"))"
    );
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read text-quoting policy program")
        .remove(0);
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("evaluate text-quoting policy program");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn text_quoting_policy_is_shared_by_the_query_and_substitution_primitives() {
    // Pin `internal--text-quoting-flag': a nil `text-quoting-style' means
    // grave in a non-UTF-8 locale (doc.c:653 via emacs.c's using_utf8), so
    // without this the nil row would depend on the ambient LANG.  GNU
    // exposes the same variable and honours a binding of it.
    let program = r#"
        (mapcar
         (lambda (style)
           (let ((internal--text-quoting-flag t)
                 (text-quoting-style style))
             (list style
                   (text-quoting-style)
                   (substitute-command-keys "`foo'"))))
         '(nil grave straight curve))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "((nil curve \"‘foo’\") (grave grave \"`foo'\") (straight straight \"'foo'\") (curve curve \"‘foo’\"))",
    );

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
    // See the note in the sibling test: pin the locale-derived flag so the
    // nil row is deterministic.
    let program = r#"
        (mapcar
         (lambda (style)
           (let ((internal--text-quoting-flag t)
                 (text-quoting-style style))
             (list style
                   (format-message "`%s'" "`arg'")
                   (format "`%s'" "`arg'"))))
         '(nil grave straight curve))"#;
    let expected = r#"((nil "‘`arg'’" "``arg''") (grave "``arg''" "``arg''") (straight "'`arg''" "``arg''") (curve "‘`arg'’" "``arg''"))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    // The bare runtime has no preloaded Lisp, so drive the same C-owned
    // describe-vector through C buffer primitives instead of subr.el's
    // `with-temp-buffer' and skip help-fns.el's `help-function-arglist'.
    let bare_program = r#"
        (let ((table (make-char-table nil nil)))
          (set-char-table-range table (cons 65 67) 'foo)
          (set-char-table-range table 70 'bar)
          (list
           (let ((buf (get-buffer-create (generate-new-buffer-name " *temp*"))))
             (save-current-buffer
               (set-buffer buf)
               (list (describe-vector [foo foo nil bar bar bar])
                     (buffer-string))))
           (let ((buf (get-buffer-create (generate-new-buffer-name " *temp*"))))
             (save-current-buffer
               (set-buffer buf)
               (list (describe-vector
                      [foo nil bar]
                      #'(lambda (value) (insert (format "<%S>" value))))
                     (buffer-string))))
           (let ((buf (get-buffer-create (generate-new-buffer-name " *temp*"))))
             (save-current-buffer
               (set-buffer buf)
               (list (describe-vector table) (buffer-string))))
           (subrp (symbol-function 'describe-vector))))"#;
    let bare_expected = "((nil \"\nC-@ .. C-a\tfoo\nC-c .. C-e\tbar\n\") (nil \"\nC-@\t\t<foo>\nC-b\t\t<bar>\n\") (nil \"\nA .. C\t\tfoo\nF\t\tbar\n\") t)";
    assert_upstream_primitive_contract(&format!("(prin1 {bare_program})"), bare_expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(bare_program)
        .read()
        .expect("vector description contract should parse")
        .expect("vector description contract should contain a form");
    interp.intern_symbols_in_value(&form);
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("vector and char-table descriptions should match GNU"),
        Reader::new(bare_expected)
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

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    // GNU's minibuffer reads real stdin in batch, which an in-process
    // interpreter cannot prime; the input-driven half of this comparison
    // therefore lives in tests/cli.rs
    // (`batch_symbol_readers_answer_piped_stdin_like_gnu'), where Emaxx runs
    // as a process with the same piped answers as the oracle above.  What
    // remains here is the input-free C surface.
    let emaxx_program = r#"
        (list
         (subrp (symbol-function 'read-command))
         (help-function-arglist 'read-command)
         (subrp (symbol-function 'read-variable))
         (help-function-arglist 'read-variable))"#;
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(emaxx_program)
        .read()
        .expect("native symbol reader contract should parse")
        .expect("native symbol reader contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("native symbol readers expose GNU's subr surface"),
        Reader::new("(t (arg1 &optional arg2) t (arg1 &optional arg2))")
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
                (func-arity (symbol-function 'set-minibuffer-window))))"#;
    let expected = "(t t error wrong-type-argument t (1 . 1))";
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "alpha beta");
    interp.buffer.goto_char(11);
    let mut env = Vec::new();

    let result = call_via_lisp(
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
        call_via_lisp(
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    let copy = call_via_lisp(
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    crate::test_support::eval_lisp(
        &mut interp,
        &mut env,
        "(progn (defvar button-tests--map (make-sparse-keymap))
                (define-key button-tests--map \"x\" 'ignore))",
    )
    .expect("define test keymap through C-owned primitives");
    let substituted = interp
        .call_function_value(
            Value::symbol("substitute-command-keys"),
            Some("substitute-command-keys"),
            &[Value::String(
                "text: \\<button-tests--map>\\[ignore]".into(),
            )],
            &mut env,
        )
        .expect("GNU help.el substitute-command-keys should expand explicit keymaps");

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
fn mapconcat_result_preserves_gnu_mutable_string_identity() {
    let contract = r#"(let* ((string (mapconcat #'identity '("a" "b") ""))
                             (alias string))
                        (aset string 1 ?x)
                        (add-face-text-property 0 1 'bold nil string)
                        (list (equal alias "ax") (eq string alias)
                              (get-text-property 0 'face alias)))"#;
    let expected = "(t t bold)";
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);

    let mut interp = Interpreter::new();
    let form = Reader::new(contract)
        .read()
        .expect("mutable mapconcat contract should parse")
        .expect("mutable mapconcat contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("mutable mapconcat contract should evaluate")
            .to_string(),
        expected
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
fn font_lock_mode_declines_to_enable_in_a_batch_session() {
    // GNU probe (`emacs -Q -batch'): (font-lock-mode) returns nil and leaves
    // font-lock-mode, jit-lock-mode, jit-lock-functions and
    // font-lock-fontified nil — font-lock.el refuses to turn itself on in a
    // noninteractive session.  An earlier Emaxx facade fabricated an enabled
    // jit-lock state here instead.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();

    assert_eq!(
        call_via_lisp(&mut interp, "font-lock-mode", &[], &mut env)
            .expect("font-lock-mode should evaluate"),
        Value::Nil
    );
    let buffer_id = interp.current_buffer_id();
    for name in [
        "font-lock-mode",
        "jit-lock-mode",
        "jit-lock-functions",
        "font-lock-fontified",
    ] {
        assert!(
            interp
                .buffer_local_value(buffer_id, name)
                .is_none_or(|value| value.is_nil()),
            "{name} must stay off in batch"
        );
    }
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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

    call_via_lisp(
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

    call_via_lisp(
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter_with(&["keymap"]);
    let mut env = Vec::new();
    let keymap = make_runtime_keymap(&mut interp, Some("test-map"));
    call_via_lisp(
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
            &Value::Symbol("keymap-tests-command".into()),
            &[keymap],
            false,
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
fn keymap_character_contracts_share_gnu_control_and_full_map_storage() {
    let program = r#"
        (let ((map (make-keymap)))
          (define-key map "(" 'literal-open)
          (define-key map [(32 . 32)] 'space-range)
          (list (lookup-key map "(")
                (lookup-key map [32])
                (lookup-key (current-global-map) ["C-x C-f"])))"#;
    let expected = "(literal-open space-range find-file)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    // The C-x C-f resolution comes from files.el in the dumped image.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("keymap character contract should parse")
        .expect("keymap character contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("keymap character contracts should match GNU"),
        Reader::new(expected)
            .read()
            .expect("expected keymap character contract should parse")
            .expect("expected keymap character contract should exist")
    );
}

#[test]
fn mapcar_iterates_runtime_keymaps_as_lisp_keymap_lists() {
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter_with(&["keymap"]);
    let mut env = Vec::new();
    let keymap = make_runtime_keymap(&mut interp, Some("test-map"));
    call_via_lisp(
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
fn string_case_conversion_preserves_properties_until_character_count_changes() {
    let program = r#"
        (let* ((stable (concat (propertize "al" 'face 'bold)
                               (propertize "pha" 'face 'italic)))
               (upper (upcase stable))
               (lower (downcase stable))
               (capitalized (capitalize stable))
               (initials (upcase-initials stable))
               (expanded (upcase (propertize "aßc" 'face 'bold))))
          (list
           (substring-no-properties upper)
           (text-properties-at 0 upper) (text-properties-at 2 upper)
           (substring-no-properties lower)
           (text-properties-at 0 lower) (text-properties-at 2 lower)
           (substring-no-properties capitalized)
           (text-properties-at 0 capitalized) (text-properties-at 2 capitalized)
           (substring-no-properties initials)
           (text-properties-at 0 initials) (text-properties-at 2 initials)
           (substring-no-properties expanded)
           (text-properties-at 0 expanded)))
    "#;
    let expected = concat!(
        "(\"ALPHA\" (face bold) (face italic) ",
        "\"alpha\" (face bold) (face italic) ",
        "\"Alpha\" (face bold) (face italic) ",
        "\"Alpha\" (face bold) (face italic) \"ASSC\" nil)"
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read()
        .expect("case-property contract should parse")
        .expect("case-property contract should contain a form");
    let result = interp
        .eval(&form, &mut Vec::new())
        .expect("case-property contract should evaluate");
    assert_eq!(result.to_string(), expected);
}

#[test]
fn capitalize_uses_current_syntax_table_word_boundaries() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter_with(&["keymap"]);
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter_with(&["keymap"]);
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
            let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
fn case_folded_try_completion_preserves_unextended_input_spelling() {
    let program = r#"(let ((completion-ignore-case t))
                       (list
                        (try-completion "A" '("alpha" "alpine" "amber"))
                        (try-completion "AL" '("alpha" "alpine" "amber"))
                        (try-completion "aL" '("alpha" "alpine" "amber"))
                        (try-completion "AM" '("alpha" "alpine" "amber"))))"#;
    let expected = r#"("A" "alp" "alp" "amber")"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("case-folded completion contract should parse")
        .expect("case-folded completion contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("case-folded completion contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("case-folded completion result should parse")
            .expect("case-folded completion result should exist")
    );
}

#[test]
fn all_completions_preserves_propertized_string_candidate_identity() {
    let program = r#"(let* ((candidate (propertize "alphaValue" 'payload 7))
                            (matches (all-completions "alp" (list candidate))))
                       (list (eq (car matches) candidate)
                             (equal (car matches) candidate)
                             (get-text-property 0 'payload (car matches))))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(t t 7)");

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("propertized completion identity contract should parse")
        .expect("propertized completion identity contract should contain a form");
    let expected = Reader::new("(t t 7)")
        .read()
        .expect("expected completion identity result should parse")
        .expect("expected completion identity result should exist");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("propertized completion identity contract should evaluate"),
        expected
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
                            :command '("/bin/sh" "-c"
                                       "printf ready; exec /bin/cat")
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
                               ;; Establish child readiness before starting
                               ;; the semantic timeout.  Host executable
                               ;; scanning may otherwise consume that entire
                               ;; timeout before /bin/cat reaches its read.
                               (accept-process-output p)
                               (setq emaxx-test-wait-seen nil
                                     emaxx-test-wait-received nil)
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
fn native_network_lookup_delegates_numeric_syntax_to_the_host_resolver() {
    let program = r#"(let ((addresses
                            '("localhost" "343.1.2.3" "1.2.3.4.5"
                              "127.0.0.1" "127.0.1" "127.1" "127" "1" "0"
                              "0xe3010203" "0xe3.1.2.3" "227.0x1.2.3"
                              "034300201003" "0343.1.2.3" "227.001.2.3"
                              "fe80:1" "e301:203:1" "e301::203::1"
                              "1:2:3:4:5:6:7:8:9" "0xe301:203::1"
                              "343:10001:2::3" "fe80::1" "e301::203:1"
                              "e301:0203::1" "::1" "::0"
                              "0343:1:2::3" "343:001:2::3")))
                       (mapcar
                        (lambda (address)
                          (list address
                                (network-lookup-address-info
                                 address nil 'numeric)
                                (network-lookup-address-info
                                 address 'ipv4 'numeric)
                                (network-lookup-address-info
                                 address 'ipv6 'numeric)))
                        addresses))"#;
    let oracle = upstream_oracle_stdout(&format!("(prin1 {program})"));
    let expected = Reader::new(&oracle)
        .read()
        .expect("numeric lookup oracle result should parse")
        .expect("numeric lookup oracle result should exist");

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("numeric lookup contract should parse")
        .expect("numeric lookup contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("numeric lookup contract should evaluate"),
        expected
    );
}

fn one_shot_http_fixture() -> (
    u16,
    std::sync::mpsc::Receiver<Vec<u8>>,
    std::thread::JoinHandle<()>,
) {
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("bind one-shot HTTP fixture");
    let port = listener
        .local_addr()
        .expect("read one-shot HTTP fixture address")
        .port();
    listener
        .set_nonblocking(true)
        .expect("make one-shot HTTP fixture nonblocking");
    let (request_tx, request_rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "editor did not connect to the one-shot HTTP fixture"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(error) => panic!("accept one-shot HTTP fixture connection: {error}"),
            }
        };
        stream
            .set_nonblocking(false)
            .expect("make accepted HTTP fixture connection blocking");
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .expect("set HTTP fixture read timeout");
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = stream.read(&mut chunk).expect("read HTTP fixture request");
            assert_ne!(count, 0, "HTTP request ended before its header terminator");
            request.extend_from_slice(&chunk[..count]);
            assert!(
                request.len() < 64 * 1024,
                "HTTP fixture request is too large"
            );
        }
        request_tx
            .send(request)
            .expect("publish HTTP fixture request");

        let body = b"network fixture\nline two\n";
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\nX-Contract-Fixture: exact\r\n\r\n",
            body.len()
        )
        .expect("write HTTP fixture headers");
        stream.write_all(body).expect("write HTTP fixture body");
        stream.flush().expect("flush HTTP fixture response");
    });
    (port, request_rx, server)
}

fn http_retrieval_program(port: u16) -> String {
    format!(
        r#"(progn
              (require 'url)
              (let ((buffer
                     (url-retrieve-synchronously
                      "http://127.0.0.1:{port}/fixture?mode=exact" t t 10))
                    result)
                (unwind-protect
                    (with-current-buffer buffer
                      (goto-char (point-min))
                      (re-search-forward
                       "^X-Contract-Fixture: \\([^\r\n]+\\)\r?$")
                      (let ((fixture-header (match-string 1)))
                        (re-search-forward "\r?\n\r?\n")
                        (let ((body
                             (buffer-substring-no-properties
                              (point) (point-max))))
                          (setq result
                                (list url-http-response-status
                                      fixture-header
                                      (length body)
                                      (secure-hash 'sha256 body)
                                      (buffer-substring-no-properties
                                       (point) (+ (point) 7)))))))
                  (when (buffer-live-p buffer)
                    (kill-buffer buffer)))
                (list result (buffer-live-p buffer))))"#
    )
}

#[test]
fn url_retrieve_synchronously_matches_gnu_over_a_real_local_http_connection() {
    let (oracle_port, oracle_request_rx, oracle_server) = one_shot_http_fixture();
    let oracle_program = http_retrieval_program(oracle_port);
    let oracle = upstream_oracle_stdout(&format!("(prin1 {oracle_program})"));
    let pinned = "((200 \"exact\" 25 \"bdf31c61b3e3c2a24d92212baf213640a6c53aa75563d8691e609a146dae30f4\" \"network\") nil)";
    assert_eq!(
        oracle, pinned,
        "GNU did not return the pinned one-shot HTTP fixture record"
    );
    let expected = Reader::new(&oracle)
        .read()
        .expect("HTTP retrieval oracle result should parse")
        .expect("HTTP retrieval oracle result should exist");
    let oracle_request = oracle_request_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("receive GNU HTTP request");
    oracle_server.join().expect("join GNU HTTP fixture");

    let (emaxx_port, emaxx_request_rx, emaxx_server) = one_shot_http_fixture();
    let emaxx_program = http_retrieval_program(emaxx_port);
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(&emaxx_program)
        .read()
        .expect("HTTP retrieval contract should parse")
        .expect("HTTP retrieval contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("HTTP retrieval contract should evaluate");
    let emaxx_request = emaxx_request_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("receive Emaxx HTTP request");
    emaxx_server.join().expect("join Emaxx HTTP fixture");

    assert!(
        values_equal(&interp, &actual, &expected),
        "local HTTP retrieval differed from GNU:\nactual: {actual:?}\nexpected: {expected:?}"
    );
    for (request, port) in [
        (oracle_request.as_slice(), oracle_port),
        (emaxx_request.as_slice(), emaxx_port),
    ] {
        let request = String::from_utf8(request.to_vec()).expect("HTTP request should be ASCII");
        assert!(
            request.starts_with("GET /fixture?mode=exact HTTP/1.1\r\n"),
            "editor did not request the exact fixture path: {request:?}"
        );
        assert!(
            request
                .lines()
                .any(|line| line.eq_ignore_ascii_case(&format!("Host: 127.0.0.1:{port}"))),
            "editor did not send the fixture Host header: {request:?}"
        );
    }
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
                                  (equal minibuffer-completion-table '("a"))
                                  (let ((active (active-minibuffer-window)))
                                    (list
                                     (mapcar
                                      (lambda (window)
                                        (if (eq window active) 'mini 'ordinary))
                                      (window-list nil nil))
                                     (mapcar
                                      (lambda (window)
                                        (if (eq window active) 'mini 'ordinary))
                                      (window-list-1 (selected-window) nil nil))
                                     (memq active
                                           (window-list nil 'exclude))
                                     (length (get-buffer-window-list))))))))))
                        (let ((executing-kbd-macro t))
                          (completing-read "Prompt: " '("a")))))"#;
    let active_expected =
        r#"(1 "Prompt: " t t 9 9 "" t t t t ((mini ordinary) (mini ordinary) nil 1))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {active})"), active_expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
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
fn native_minibuffer_runs_initial_post_command_hook_before_input() {
    let setup = r#"(setq emaxx-initial-post-command-count 0
                         minibuffer-setup-hook
                         (list
                          (lambda ()
                            (add-hook
                             'post-command-hook
                             (lambda ()
                               (setq emaxx-initial-post-command-count
                                     (1+ emaxx-initial-post-command-count)))
                             nil t))))"#;
    let program = format!(
        r#"(progn
                       {setup}
                       (let ((executing-kbd-macro t)
                             (unread-command-events '(13)))
                         (completing-read "Prompt: " '("a")))
                       emaxx-initial-post-command-count)"#
    );
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "1");

    let (mut interp, mut env) = upstream_interactive_interpreter();
    let form = Reader::new(setup)
        .read()
        .expect("initial post-command setup should parse")
        .expect("initial post-command setup should contain a form");
    interp
        .eval(&form, &mut env)
        .expect("initial post-command setup should evaluate");
    let script = std::rc::Rc::new(std::cell::RefCell::new(vec![Value::Integer(13)]));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    let result = call(
        &mut interp,
        "completing-read",
        &[
            Value::String("Prompt: ".into()),
            Value::list([Value::String("a".into())]),
        ],
        &mut env,
    );
    set_tty_event_reader(None);
    assert_eq!(
        result.expect("RET should submit the minibuffer"),
        Value::String("".into())
    );
    assert_eq!(
        interp.lookup_var("emaxx-initial-post-command-count", &env),
        Some(Value::Integer(1))
    );
}

#[test]
fn reused_minibuffer_discards_overlays_from_the_previous_read() {
    let program = r#"(progn
                       (setq emaxx-minibuffer-overlay-counts nil
                             minibuffer-setup-hook
                             (list
                              (lambda ()
                                (push (length (overlays-at (point-min)))
                                      emaxx-minibuffer-overlay-counts)
                                (make-overlay (point-min) (point-min)))))
                       (let ((executing-kbd-macro t)
                             (unread-command-events '(13)))
                         (completing-read "First: " '("a")))
                       (let ((executing-kbd-macro t)
                             (unread-command-events '(13)))
                         (completing-read "Second: " '("b")))
                       (nreverse emaxx-minibuffer-overlay-counts))"#;
    let expected = "(0 0)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("minibuffer overlay reset contract should parse")
        .expect("minibuffer overlay reset contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("minibuffer overlay reset contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("minibuffer overlay reset result should parse")
            .expect("minibuffer overlay reset result should exist")
    );
}

#[test]
fn native_read_expression_history_matches_gnu_minibuf_c_value_cell() {
    let contract = r#"(progn
             (defalias 'sample-native-history-reader
               (function (lambda () read-expression-history)))
             (list
              (boundp 'read-expression-history)
              read-expression-history
              (let ((read-expression-history '(one)))
                (sample-native-history-reader))
              (default-boundp 'read-expression-history)
              (default-value 'read-expression-history)
              (special-variable-p 'read-expression-history)))"#;
    let expected = "(t nil (one) t nil t)";
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(contract)
        .read()
        .expect("read-expression-history contract should parse")
        .expect("read-expression-history contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("read-expression-history contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("read-expression-history result should parse")
            .expect("read-expression-history result should exist")
    );
}

#[test]
fn native_record_and_pseudovector_type_names_match_gnu_data_c() {
    let public_program = r#"(let* ((plain (record 'sample-record-type 1))
                                    (mutex (make-mutex "m"))
                                    (condition
                                     (make-condition-variable mutex "c")))
                               (list (type-of plain)
                                     (cl-type-of plain)
                                     (type-of condition)
                                     (cl-type-of condition)))"#;
    let public_expected =
        "(sample-record-type sample-record-type condition-variable condition-variable)";
    assert_upstream_primitive_contract(&format!("(prin1 {public_program})"), public_expected);

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(public_program)
        .read()
        .expect("type-name contract should parse")
        .expect("type-name contract should contain a form");
    let expected = Reader::new(public_expected)
        .read()
        .expect("type-name result should parse")
        .expect("type-name result should exist");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("type-name contract should evaluate"),
        expected
    );

    // These GNU PVEC_TS_* objects need a loaded grammar to construct through
    // Lisp, so cover the C decision-table names at Emaxx's typed host boundary.
    // GNU data.c returns exactly these `treesit-*` symbols for both `type-of`
    // and `cl-type-of`.
    for (variable, kind, type_name) in [
        (
            "parser-under-test",
            crate::lisp::eval::RecordKind::TreeSitterParser,
            "treesit-parser",
        ),
        (
            "node-under-test",
            crate::lisp::eval::RecordKind::TreeSitterNode,
            "treesit-node",
        ),
        (
            "query-under-test",
            crate::lisp::eval::RecordKind::TreeSitterCompiledQuery,
            "treesit-compiled-query",
        ),
    ] {
        let value = interp.create_pseudovector(kind, type_name, Vec::new());
        interp.set_global_binding(variable, value);
    }
    let form = Reader::new(
        "(list
           (type-of parser-under-test) (cl-type-of parser-under-test)
           (type-of node-under-test) (cl-type-of node-under-test)
           (type-of query-under-test) (cl-type-of query-under-test))",
    )
    .read()
    .expect("Tree-sitter type-name contract should parse")
    .expect("Tree-sitter type-name contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("Tree-sitter type-name contract should evaluate"),
        Value::list([
            Value::symbol("treesit-parser"),
            Value::symbol("treesit-parser"),
            Value::symbol("treesit-node"),
            Value::symbol("treesit-node"),
            Value::symbol("treesit-compiled-query"),
            Value::symbol("treesit-compiled-query"),
        ])
    );
}

#[test]
fn native_condition_wait_releases_and_restores_recursive_mutex_ownership() {
    let validation = r#"(let* (;; thread.c:499,558 spell the apostrophe ASCII and `error'
                               ;; requotes it per the effective style, so pin the
                               ;; style rather than inherit the ambient LANG.
                               (internal--text-quoting-flag t)
                               (mutex (make-mutex "m"))
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

    let synchronization = r#"(let* ((internal--text-quoting-flag t)
                                    (mutex (make-mutex "m"))
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
                                     '("Condition variable\u2019s mutex is not held by current thread"))))))"#;
    let synchronization_expected = "(t t t t)";
    assert_upstream_primitive_contract(
        &format!("(prin1 {synchronization})"),
        synchronization_expected,
    );

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
fn change_hook_nonlocal_exits_clear_the_active_hook_value() {
    let program = r#"(progn
                       (define-error 'emaxx-change-hook-error "change hook error")
                       (defun emaxx-change-hook-signal (&rest _)
                         (signal 'emaxx-change-hook-error nil))
                       (defun emaxx-change-hook-throw (&rest _)
                         (throw 'emaxx-change-hook-tag 'thrown))
                       (setq after-change-functions nil)
                       (condition-case nil
                           (with-temp-buffer
                             (add-hook 'after-change-functions
                                       #'emaxx-change-hook-signal 90)
                             (insert "a"))
                         (emaxx-change-hook-error nil))
                       (let ((after-signal after-change-functions))
                         (setq after-change-functions nil)
                         (catch 'emaxx-change-hook-tag
                           (with-temp-buffer
                             (add-hook 'after-change-functions
                                       #'emaxx-change-hook-throw 90)
                             (insert "b")))
                         (list after-signal after-change-functions)))"#;
    let expected = "(nil nil)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("change-hook cleanup contract should parse")
        .expect("change-hook cleanup contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("change-hook cleanup contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("change-hook cleanup result should parse")
            .expect("change-hook cleanup result should exist")
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

    let mut interp =
        crate::test_support::initialized_gnu_early_lisp_interpreter_with(&["emacs-lisp/macroexp"]);
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

        let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
fn native_keyboard_macro_family_publishes_gnu_status_messages() {
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    crate::lisp::primitives::set_echo_area_message(None);

    call(&mut interp, "start-kbd-macro", &[Value::Nil], &mut env)
        .expect("start a fresh keyboard macro");
    assert_eq!(
        crate::lisp::primitives::echo_area_message().as_deref(),
        Some("Defining kbd macro...")
    );

    call(&mut interp, "end-kbd-macro", &[], &mut env).expect("finish the keyboard macro");
    assert_eq!(
        crate::lisp::primitives::echo_area_message().as_deref(),
        Some("Keyboard macro defined")
    );

    call(
        &mut interp,
        "start-kbd-macro",
        &[Value::T, Value::T],
        &mut env,
    )
    .expect("append without replaying the previous macro");
    assert_eq!(
        crate::lisp::primitives::echo_area_message().as_deref(),
        Some("Appending to kbd macro...")
    );
}

#[test]
fn terminal_command_loop_records_keyboard_macro_events_and_nonmenu_event() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    interp.buffer.insert("xy");
    interp.buffer.goto_char(1);
    call(&mut interp, "start-kbd-macro", &[Value::Nil], &mut env)
        .expect("start keyboard macro recording");

    crate::lisp::primitives::execute_command_binding(
        &mut interp,
        &mut env,
        Value::Symbol("forward-char".into()),
        &[Value::Integer(6)],
        Value::Integer(6),
    )
    .expect("execute recorded command");
    call(&mut interp, "end-kbd-macro", &[], &mut env).expect("finish keyboard macro recording");

    assert_eq!(
        interp.lookup_var("last-kbd-macro", &env),
        Some(Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Integer(6),
        ]))
    );
    assert_eq!(
        interp.lookup_var("last-input-event", &env),
        Some(Value::Integer(6))
    );
    assert_eq!(
        interp.lookup_var("last-nonmenu-event", &env),
        Some(Value::Integer(6))
    );
}

#[test]
fn keyboard_macro_records_input_read_inside_a_command() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let definition =
        Reader::new("(defun emaxx-test-read-char-command () (interactive) (read-char))")
            .read()
            .expect("nested-input command should parse")
            .expect("nested-input command should exist");
    interp
        .eval(&definition, &mut env)
        .expect("define nested-input command");
    interp.set_variable(
        "unread-command-events",
        Value::list([Value::Integer(97)]),
        &mut env,
    );
    call(&mut interp, "start-kbd-macro", &[Value::Nil], &mut env)
        .expect("start keyboard macro recording");

    crate::lisp::primitives::execute_command_binding(
        &mut interp,
        &mut env,
        Value::Symbol("emaxx-test-read-char-command".into()),
        &[Value::Integer(3), Value::Integer(114)],
        Value::Integer(114),
    )
    .expect("execute a command that reads another event");
    call(&mut interp, "end-kbd-macro", &[], &mut env).expect("finish keyboard macro recording");

    assert_eq!(
        interp.lookup_var("last-kbd-macro", &env),
        Some(Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Integer(3),
            Value::Integer(114),
            Value::Integer(97),
        ]))
    );
}

#[test]
fn keyboard_macro_records_minibuffer_command_events_exactly_once() {
    let (mut interp, mut env) = upstream_interactive_interpreter();
    crate::test_support::eval_lisp(
        &mut interp,
        &mut env,
        r#"(progn
             (defun emaxx-test-completion-command (value)
               (interactive
                (list (completing-read
                       "Macro fruit: " '("apple" "banana") nil t)))
               (setq emaxx-test-completion-value value))
             (setq emaxx-test-completion-value nil))"#,
    )
    .expect("define a completing command for macro recording");
    let script = std::rc::Rc::new(std::cell::RefCell::new(
        "ba\t\r"
            .chars()
            .rev()
            .map(|ch| Value::Integer(ch as i64))
            .collect::<Vec<_>>(),
    ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));

    call(&mut interp, "start-kbd-macro", &[Value::Nil], &mut env)
        .expect("start keyboard macro recording");
    crate::lisp::primitives::execute_command_binding(
        &mut interp,
        &mut env,
        Value::Symbol("emaxx-test-completion-command".into()),
        &[Value::Integer(3), Value::Integer(99)],
        Value::Integer(99),
    )
    .expect("record a command and its minibuffer input");
    set_tty_event_reader(None);
    call(&mut interp, "end-kbd-macro", &[], &mut env).expect("finish keyboard macro recording");

    assert_eq!(
        interp.lookup_var("emaxx-test-completion-value", &env),
        Some(Value::String("banana".into()))
    );
    assert_eq!(
        interp.lookup_var("last-kbd-macro", &env),
        Some(Value::list([
            Value::Symbol("vector-literal".into()),
            Value::Integer(3),
            Value::Integer(99),
            Value::Integer(98),
            Value::Integer(97),
            Value::Integer(9),
            Value::Integer(13),
        ])),
        "recursive minibuffer reads already record their terminal events"
    );
}

#[test]
fn file_attributes_nil_matches_gnu_missing_file_contract() {
    assert_upstream_primitive_contract("(prin1 (file-attributes nil))", "nil");

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    assert_eq!(
        call(&mut interp, "file-attributes", &[Value::Nil], &mut env)
            .expect("nil denotes an absent file"),
        Value::Nil
    );
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
            r#"(let ((unread-command-events '((down-mouse-1 nil 1))))
                 (car-safe (aref (read-key-sequence nil) 0)))"#,
            "down-mouse-1",
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

        let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
fn char_property_primitives_accept_windows_and_filter_window_overlays() {
    let program = r#"(let* ((window (selected-window))
                            (old-buffer (window-buffer window))
                            (buffer (generate-new-buffer " *char-property-window*")))
                       (unwind-protect
                           (progn
                             (set-window-buffer window buffer)
                             (with-current-buffer buffer
                               (insert "x")
                               (put-text-property 1 2 'face 'text-face)
                               (let ((overlay (make-overlay 1 2 buffer)))
                                 (overlay-put overlay 'face 'window-face)
                                 (overlay-put overlay 'window window)
                                 (let ((pair (get-char-property-and-overlay
                                              1 'face window)))
                                   (list (get-char-property 1 'face window)
                                         (car pair)
                                         (overlayp (cdr pair)))))))
                         (set-window-buffer window old-buffer)
                         (kill-buffer buffer)))"#;
    let expected = "(window-face window-face t)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("window char-property contract should parse")
        .expect("window char-property contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("window char-property contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("window char-property result should parse")
            .expect("window char-property result should exist")
    );
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp =
        crate::test_support::initialized_gnu_early_lisp_interpreter_with(&["emacs-lisp/macroexp"]);
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

    let mut interp =
        crate::test_support::initialized_gnu_early_lisp_interpreter_with(&["emacs-lisp/macroexp"]);
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    // `x-file-dialog' and `x-select-font' are DEFUNed only in builds whose
    // configure compiled the X chooser code; a build without it leaves them
    // unbound and the condition-case catches plain `void-function'.  The
    // oracle is asked which build it is live; Emaxx models the X-compiled
    // headless build (the choosers exist and refuse without a display) —
    // the divergence is recorded in docs/honesty-audit-2026-08-18.md.
    let expected_with_choosers = |with_choosers: bool| {
        let chooser_rows = if with_choosers {
            concat!(
                "(error \"Window system is not in use or not initialized\") ",
                "(wrong-type-argument frame-live-p t) ",
                "(error \"Window system frame should be used\")"
            )
        } else {
            concat!(
                "(void-function x-file-dialog) ",
                "(void-function x-select-font) ",
                "(void-function x-select-font)"
            )
        };
        format!(
            "((wrong-type-argument listp 1) error \
             (wrong-type-argument stringp 1) \
             (wrong-type-argument frame-live-p t) \
             (error \"Window system frame should be used\") \
             {chooser_rows})"
        )
    };
    let oracle_has_choosers =
        upstream_oracle_stdout("(prin1 (and (fboundp 'x-file-dialog) (fboundp 'x-select-font) t))");
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        &expected_with_choosers(oracle_has_choosers.trim() == "t"),
    );

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("headless GUI action contract should parse")
        .expect("headless GUI action contract should contain a form");
    let actual = interp
        .eval(&form, &mut env)
        .expect("headless GUI action failures should be catchable");
    // Dispatch follows the host's C contract: the choosers exist for Emaxx
    // exactly where the host's oracle build compiled them.
    let host_has_choosers = is_builtin("x-file-dialog") && is_builtin("x-select-font");
    let expected = Reader::new(&expected_with_choosers(host_has_choosers))
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    // treesit.c's Ftreesit_library_abi_version reports the ABI of the
    // tree-sitter library THAT BUILD links: the oracle answers for the
    // host's libtree-sitter, Emaxx for its linked tree-sitter crate, and
    // the two libraries can legitimately differ in version.  Each side is
    // therefore checked against its own library's constants — the oracle's
    // fetched live, Emaxx's taken from the crate — while every other
    // element stays a shared pinned contract.
    let expected_with_abi = |abi: i64, min_abi: i64| {
        format!(
            "(t {abi} {min_abi} nil nil nil (nil not-found) nil nil nil t t nil nil t t \
             emaxx-definitely-missing t treesit-load-language-error \
             (wrong-type-argument treesit-compiled-query-p) \
             (wrong-type-argument treesit-node-p) \
             (treesit-load-language-error not-found) nil)"
        )
    };
    let oracle_abi = upstream_oracle_stdout(
        "(prin1 (list (treesit-library-abi-version) (treesit-library-abi-version t)))",
    );
    let mut oracle_abi_numbers = oracle_abi
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .split_whitespace()
        .map(|number| number.parse::<i64>().expect("oracle ABI numbers"));
    let oracle_abi = oracle_abi_numbers.next().expect("oracle library ABI");
    let oracle_min_abi = oracle_abi_numbers.next().expect("oracle minimum ABI");
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        &expected_with_abi(oracle_abi, oracle_min_abi),
    );
    let expected = expected_with_abi(
        tree_sitter::LANGUAGE_VERSION as i64,
        tree_sitter::MIN_COMPATIBLE_LANGUAGE_VERSION as i64,
    );

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("Tree-sitter capability contract should parse")
        .expect("Tree-sitter capability contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("Tree-sitter runtime introspection should evaluate"),
        Reader::new(&expected)
            .read()
            .expect("Tree-sitter capability result should parse")
            .expect("Tree-sitter capability result should exist")
    );
}

#[test]
fn native_treesit_parser_lifecycle_and_real_json_nodes_use_official_runtime() {
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
fn native_network_process_resolves_named_services_from_the_services_database() {
    // process.c normalizes a nil Internet host to loopback, then hands the
    // host and non-numeric :service to getaddrinfo ("domain" is UDP+TCP
    // port 53 in the services database).  An unknown name exposes the
    // platform's getaddrinfo diagnostic.  No packet is sent: only the
    // resolved :remote address is observed.
    let program = r#"(let ((dns (make-network-process
                                :name "dns-client"
                                :type 'datagram
                                :host "127.0.0.1"
                                :service "domain"
                                :sentinel 'ignore)))
                      (unwind-protect
                          (list (process-contact dns :remote)
                                (condition-case err
                                    (make-network-process
                                     :name "bogus-client"
                                     :type 'datagram
                                     :host "127.0.0.1"
                                     :service "emaxx-no-such-service")
                                  (error err))
                                (condition-case err
                                    (make-network-process
                                     :name "bogus-server"
                                     :server t
                                     :service "emaxx-no-such-service")
                                  (error err)))
                        (delete-process dns)))"#;
    let expected = upstream_primitive_contract_output(&format!("(prin1 {program})"));

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("named-service contract should parse")
        .expect("named-service contract should contain a form");
    let expected_value = Reader::new(&expected)
        .read()
        .expect("named-service oracle output should parse")
        .expect("named-service oracle output should contain a value");
    let actual = interp
        .eval(&form, &mut env)
        .expect("named-service contract should evaluate");
    assert!(
        values_equal(&interp, &actual, &expected_value),
        "named-service result differs from GNU:\nactual: {actual:?}\nexpected: {expected_value:?}"
    );
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

#[cfg(unix)]
#[test]
fn local_network_process_rejects_overlong_service_before_host_bind() {
    let program = r#"(condition-case err
                         (make-network-process
                          :name "overlong-local-socket"
                          :family 'local
                          :server t
                          :service
                          (make-string 200 ?x))
                       (error (list (car err) (cadr err))))"#;
    assert_upstream_primitive_contract(
        &format!("(prin1 {program})"),
        "(error \"Service name too long\")",
    );

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("overlong local socket contract should parse")
        .expect("overlong local socket contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("condition-case should catch overlong service"),
        Value::list([
            Value::symbol("error"),
            Value::string("Service name too long"),
        ])
    );
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

#[test]
fn native_network_accept_preserves_binary_bytes_without_listener_buffer() {
    // This is a live loopback contract, not a synthetic call into the output
    // helper.  Bytes above 0x7f expose accidental conversion to multibyte
    // byte8 characters, while the listener buffer exposes whether accepted
    // children incorrectly keep that buffer alive.
    let program = r#"(progn
                       (setq emaxx-test-tcp-received nil
                             emaxx-test-tcp-accepted nil)
                       (let*
                           ((buffer
                             (get-buffer-create
                              " *emaxx-network-contract*"))
                            (server
                             (make-network-process
                              :name "emaxx-network-contract"
                              :family 'ipv4
                              :server t
                              :host "127.0.0.1"
                              :service t
                              :buffer buffer
                              :coding 'binary
                              :filter
                              (lambda (process text)
                                (setq
                                 emaxx-test-tcp-accepted process
                                 emaxx-test-tcp-received
                                 (list
                                  (multibyte-string-p text)
                                  (vconcat text))))
                              :sentinel 'ignore
                              :noquery t))
                            (local (process-contact server :local))
                            (client
                             (make-network-process
                              :name "emaxx-network-contract-client"
                              :family 'ipv4
                              :host "127.0.0.1"
                              :service (aref local 4)
                              :coding 'binary
                              :sentinel 'ignore
                              :noquery t)))
                         (unwind-protect
                             (progn
                               (process-send-string
                                client
                                (unibyte-string
                                 0 127 128 184 216 255))
                               (let ((attempts 0))
                                 (while
                                     (and
                                      (null emaxx-test-tcp-received)
                                      (< attempts 100))
                                   (accept-process-output nil .02)
                                   (setq attempts (1+ attempts))))
                               (list
                                emaxx-test-tcp-received
                                (processp emaxx-test-tcp-accepted)
                                (null
                                 (process-buffer
                                  emaxx-test-tcp-accepted))
                                (process-status
                                 emaxx-test-tcp-accepted)))
                           (when (processp emaxx-test-tcp-accepted)
                             (set-process-query-on-exit-flag
                              emaxx-test-tcp-accepted nil)
                             (delete-process
                              emaxx-test-tcp-accepted))
                           (when (process-live-p client)
                             (delete-process client))
                           (when (process-live-p server)
                             (delete-process server))
                           (kill-buffer buffer))))"#;
    let expected = "((nil [0 127 128 184 216 255]) t t open)";
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), expected);

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let form = Reader::new(program)
        .read()
        .expect("network accept contract should parse")
        .expect("network accept contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("network accept contract should evaluate"),
        Reader::new(expected)
            .read()
            .expect("network accept result should parse")
            .expect("network accept result should exist")
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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

    let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
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
    let _permit = crate::test_support::acquire_exclusive_host_test_permit();
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
    // Wait for the process event rather than imposing a host scheduling
    // deadline.  The process-test gate prevents peer process tests from
    // competing, and accept-process-output owns the blocking contract being
    // tested here.
    assert_eq!(
        call(
            &mut interp,
            "accept-process-output",
            std::slice::from_ref(&process),
            &mut env,
        )
        .expect("wait for resumed output"),
        Value::T
    );
    let contents = interp
        .get_buffer_by_id(buffer_id)
        .expect("held-output buffer")
        .buffer_string();
    if contents != "held\n" {
        let process_id = interp
            .resolve_process_id(&process)
            .expect("resolve held-output process");
        let (pending_stdout, pending_stderr) = interp
            .poll_process_output(process_id)
            .expect("inspect undelivered held output");
        panic!(
            "held output was not delivered: status={:?}, filter={:?}, paused={}, deliveries={:?}, pending_stdout={pending_stdout:?}, pending_stderr={pending_stderr:?}",
            interp.process_status_value(process_id),
            interp.process_filter_value(process_id),
            interp.process_output_paused(process_id),
            interp.process_output_delivery_count(process_id),
        );
    }
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

#[test]
fn tty_event_reader_feeds_interactive_event_reads() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    set_tty_event_reader(Some(Box::new(|| Some(Value::Integer(121)))));
    let event = call(&mut interp, "read-event", &[], &mut env);
    set_tty_event_reader(None);
    assert_eq!(
        event.expect("tty reader supplies the event"),
        Value::Integer(121)
    );
}

#[test]
fn tty_event_reader_quit_signals_gnu_quit() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    set_tty_event_reader(Some(Box::new(|| None)));
    let event = call(&mut interp, "read-event", &[], &mut env);
    set_tty_event_reader(None);
    let Err(LispError::SignalValue(data)) = event else {
        panic!("C-g from the tty reader must signal, got {event:?}");
    };
    assert_eq!(data, Value::Symbol("quit".into()));
}

#[test]
fn tty_event_reader_does_not_preempt_queued_events() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    interp.set_variable(
        "unread-command-events",
        Value::list([Value::Integer(97)]),
        &mut env,
    );
    set_tty_event_reader(Some(Box::new(|| Some(Value::Integer(98)))));
    let event = call(&mut interp, "read-event", &[], &mut env);
    set_tty_event_reader(None);
    assert_eq!(
        event.expect("queued events win over the terminal"),
        Value::Integer(97)
    );
}

#[test]
fn blocking_tty_event_read_redraws_after_a_due_timer() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    crate::test_support::eval_lisp(
        &mut interp,
        &mut env,
        "(progn
           (setq emaxx-test-timer-fired nil)
           (run-at-time 0 nil (lambda () (setq emaxx-test-timer-fired t))))",
    )
    .expect("schedule an immediately due timer");

    let polls = std::rc::Rc::new(std::cell::Cell::new(0));
    let poll_count = std::rc::Rc::clone(&polls);
    set_tty_event_poller(Some(Box::new(move || {
        let count = poll_count.get();
        poll_count.set(count + 1);
        Some((count > 0).then_some(Value::Integer(120)))
    })));
    let saw_timer = std::rc::Rc::new(std::cell::Cell::new(false));
    let observed = std::rc::Rc::clone(&saw_timer);
    set_tty_frame_redraw(Some(Box::new(move |interp, env| {
        if interp
            .lookup_var("emaxx-test-timer-fired", env)
            .is_some_and(|value| value.is_truthy())
        {
            observed.set(true);
        }
    })));

    let event = call(&mut interp, "read-event", &[], &mut env);
    set_tty_frame_redraw(None);
    set_tty_event_poller(None);
    assert_eq!(
        event.expect("the second poll supplies input"),
        Value::Integer(120)
    );
    assert!(
        saw_timer.get(),
        "timer work performed inside read-event must reach redisplay"
    );
}

#[test]
fn blocking_tty_event_read_redraws_after_process_output() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    let process = crate::test_support::eval_lisp(
        &mut interp,
        &mut env,
        "(progn
           (setq emaxx-test-process-output nil)
           (make-process
            :name \"emaxx-tty-process-redraw\"
            :command (list shell-file-name \"-c\" \"printf ready\")
            :connection-type 'pipe
            :noquery t
            :filter (lambda (_process output)
                      (setq emaxx-test-process-output output))))",
    )
    .expect("start the redraw probe process");

    let saw_output = std::rc::Rc::new(std::cell::Cell::new(false));
    let poll_observation = std::rc::Rc::clone(&saw_output);
    let polls = std::rc::Rc::new(std::cell::Cell::new(0_usize));
    let poll_count = std::rc::Rc::clone(&polls);
    set_tty_event_poller(Some(Box::new(move || {
        let count = poll_count.get() + 1;
        poll_count.set(count);
        Some((poll_observation.get() || count >= 100_000).then_some(Value::Integer(120)))
    })));
    let redraw_observation = std::rc::Rc::clone(&saw_output);
    set_tty_frame_redraw(Some(Box::new(move |interp, env| {
        if interp
            .lookup_var("emaxx-test-process-output", env)
            .is_some_and(|value| value.is_truthy())
        {
            redraw_observation.set(true);
        }
    })));

    let event = call(&mut interp, "read-event", &[], &mut env);
    set_tty_frame_redraw(None);
    set_tty_event_poller(None);
    let _ = call(
        &mut interp,
        "delete-process",
        std::slice::from_ref(&process),
        &mut env,
    );
    assert_eq!(
        event.expect("the redraw observation releases the input poll"),
        Value::Integer(120)
    );
    assert!(
        saw_output.get(),
        "process output handled inside read-event must reach redisplay"
    );
}

#[test]
fn redisplay_dispatches_an_already_due_timer() {
    let program = r#"
        (let ((noninteractive nil)
              (timer-ran nil))
          (run-at-time '(0 0 0 0) nil (lambda () (setq timer-ran t)))
          (redisplay)
          timer-ran)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "t");

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read redisplay timer program")
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate redisplay timer program"),
        Value::T
    );
}

#[test]
fn input_pending_check_timers_dispatches_an_already_due_timer() {
    let program = r#"
        (let ((timer-ran nil))
          (run-at-time '(0 0 0 0) nil (lambda () (setq timer-ran t)))
          (list timer-ran
                (progn (input-pending-p t) timer-ran)))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {program})"), "(nil t)");

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let form = Reader::new(program)
        .read_all()
        .expect("read input-pending timer program")
        .remove(0);
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("evaluate input-pending timer program"),
        Value::list([Value::Nil, Value::T])
    );
}

#[test]
fn delayed_tty_timer_uses_the_native_clock_when_float_time_is_redefined() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    crate::test_support::eval_lisp(
        &mut interp,
        &mut env,
        "(progn
           (setq timer-list nil timer-idle-list nil emaxx-test-timer-fired nil)
           (run-at-time 60 nil (lambda () (setq emaxx-test-timer-fired t)))
           (defalias 'float-time (lambda (&rest _) 0.0)))",
    )
    .expect("schedule a delayed timer before pinning the presentation clock");

    assert!(
        !run_due_timers(&mut interp, &mut env, 0.0).expect("inspect delayed timer queue"),
        "redefining float-time must not make a future timer ripe"
    );
    assert_eq!(
        interp.lookup_var("emaxx-test-timer-fired", &env),
        Some(Value::Nil)
    );
}

#[test]
fn timed_tty_event_read_pumps_process_output_and_deferred_callbacks() {
    crate::test_support::mark_process_test();
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    set_tty_event_poller(Some(Box::new(|| Some(None))));
    let result = crate::test_support::eval_lisp(
        &mut interp,
        &mut env,
        r#"
          (let ((process
                 (make-process
                  :name "timed-tty-process-pump"
                  :command (list (executable-find "sh") "-c" "printf ready")
                  :noquery t
                  :filter
                  (lambda (_process _text)
                    (run-at-time
                     0 nil
                     (lambda () (throw 'timed-tty-process-ready 'ready)))))))
            (catch 'timed-tty-process-ready
              (read-event nil t 1)
              'timeout))
        "#,
    );
    set_tty_event_poller(None);
    assert_eq!(
        result.expect("process callback interrupts the timed TTY wait"),
        Value::symbol("ready")
    );
}

#[test]
fn live_minibuffer_recursive_commands_restore_the_outer_command_identity() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    interp.set_variable(
        "this-command",
        Value::Symbol("outer-command".into()),
        &mut env,
    );
    interp.set_variable(
        "real-this-command",
        Value::Symbol("outer-real-command".into()),
        &mut env,
    );
    interp.set_variable(
        "this-original-command",
        Value::Symbol("outer-original-command".into()),
        &mut env,
    );
    let script = std::rc::Rc::new(std::cell::RefCell::new(
        "answer\r"
            .chars()
            .rev()
            .map(|ch| Value::Integer(ch as i64))
            .collect::<Vec<_>>(),
    ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));

    let result = call(
        &mut interp,
        "read-from-minibuffer",
        &[Value::String("Input: ".into())],
        &mut env,
    );
    set_tty_event_reader(None);

    assert_eq!(
        result.expect("live minibuffer submits"),
        Value::String("answer".into())
    );
    assert_eq!(
        interp.lookup_var("this-command", &env),
        Some(Value::Symbol("outer-command".into()))
    );
    assert_eq!(
        interp.lookup_var("real-this-command", &env),
        Some(Value::Symbol("outer-real-command".into()))
    );
    assert_eq!(
        interp.lookup_var("this-original-command", &env),
        Some(Value::Symbol("outer-original-command".into()))
    );
}

#[test]
fn write_region_mustbenew_consumes_a_full_negative_answer() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    let directory = std::env::temp_dir().join(format!(
        "emaxx-write-region-mustbenew-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&directory).expect("create mustbenew fixture directory");
    let path = directory.join("existing.dat");
    std::fs::write(&path, b"existing bytes\n").expect("create mustbenew fixture file");

    let script = std::rc::Rc::new(std::cell::RefCell::new(
        "no\r"
            .chars()
            .rev()
            .map(|ch| Value::Integer(ch as i64))
            .collect::<Vec<_>>(),
    ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    let result = call(
        &mut interp,
        "write-region",
        &[
            Value::String("replacement bytes\n".into()),
            Value::Nil,
            Value::String(path.display().to_string().into()),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::T,
        ],
        &mut env,
    );
    set_tty_event_reader(None);

    assert!(result.is_err(), "declining overwrite must signal");
    assert!(
        script.borrow().is_empty(),
        "the overwrite prompt must consume the full `no RET` answer"
    );
    assert_eq!(
        std::fs::read(&path).expect("read declined-overwrite fixture"),
        b"existing bytes\n"
    );
    std::fs::remove_dir_all(directory).expect("remove mustbenew fixture directory");
}

#[test]
fn tty_events_answer_interactive_minibuffer_prompts() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    // The terminal feeds "answer.txt" then RET into the minibuffer loop.
    let script: std::rc::Rc<std::cell::RefCell<Vec<Value>>> =
        std::rc::Rc::new(std::cell::RefCell::new(
            "answer.txt\r"
                .chars()
                .rev()
                .map(|ch| Value::Integer(ch as i64))
                .collect(),
        ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    let result = call(
        &mut interp,
        "read-string",
        &[Value::String("File: ".into())],
        &mut env,
    );
    set_tty_event_reader(None);
    assert_eq!(
        result.expect("tty events answer the prompt"),
        Value::String("answer.txt".into())
    );
}

#[test]
fn read_string_history_keeps_the_minibuffer_map_and_initial_properties() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    crate::test_support::eval_lisp(
        &mut interp,
        &mut env,
        "(setq issue22-initial-face nil
               minibuffer-setup-hook
               (list (lambda ()
                       (setq issue22-initial-face
                             (get-text-property (minibuffer-prompt-end) 'face)))))",
    )
    .expect("install the initial-input observer");
    let initial = crate::test_support::eval_lisp(
        &mut interp,
        &mut env,
        "(propertize \"alpha\" 'face 'lsp-face-highlight-textual)",
    )
    .expect("construct propertized initial input");

    let script = std::rc::Rc::new(std::cell::RefCell::new(vec![Value::Integer(13)]));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    let result = call(
        &mut interp,
        "read-string",
        &[
            Value::String("Rename to: ".into()),
            initial,
            Value::Symbol("lsp-rename-history".into()),
        ],
        &mut env,
    );
    set_tty_event_reader(None);

    assert_eq!(
        result.expect("RET exits a read-string with a history symbol"),
        Value::String("alpha".into())
    );
    assert_eq!(
        interp.lookup_var("issue22-initial-face", &env),
        Some(Value::Symbol("lsp-face-highlight-textual".into())),
        "read-string copies the suggested value's face into the minibuffer"
    );
}

#[test]
fn live_read_string_records_an_accepted_default_in_history() {
    let (mut interp, mut env) = upstream_interactive_interpreter();
    interp.set_variable("emaxx-default-history", Value::Nil, &mut env);
    let script = std::rc::Rc::new(std::cell::RefCell::new(vec![Value::Integer(13)]));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));

    let result = call(
        &mut interp,
        "read-string",
        &[
            Value::String("First: ".into()),
            Value::Nil,
            Value::Symbol("emaxx-default-history".into()),
            Value::String("alpha".into()),
        ],
        &mut env,
    );
    set_tty_event_reader(None);

    assert_eq!(
        result.expect("RET accepts the read-string default"),
        Value::String("alpha".into())
    );
    assert_eq!(
        interp.lookup_var("emaxx-default-history", &env),
        Some(Value::list([Value::String("alpha".into())]))
    );
}

#[test]
fn tty_minibuffer_edits_complete_and_recall_history() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    interp.set_variable(
        "minibuffer-history",
        Value::list([Value::String("older-entry".into())]),
        &mut env,
    );
    // Type "zebraX", delete the X, recall history with M-p, come back
    // with M-n, then submit the edited fresh input.
    let keys = "zebraX\u{7f}\u{1b}p\u{1b}n\r";
    let script: std::rc::Rc<std::cell::RefCell<Vec<Value>>> =
        std::rc::Rc::new(std::cell::RefCell::new(
            keys.chars()
                .rev()
                .map(|ch| Value::Integer(ch as i64))
                .collect(),
        ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    let result = call(
        &mut interp,
        "read-from-minibuffer",
        &[
            Value::String("Input: ".into()),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Symbol("minibuffer-history".into()),
        ],
        &mut env,
    );
    set_tty_event_reader(None);
    assert_eq!(
        result.expect("edited input submits"),
        Value::String("zebra".into())
    );
    // The submission lands at the head of the history variable.
    let history = interp
        .lookup_var("minibuffer-history", &env)
        .expect("history variable");
    assert_eq!(
        history,
        Value::list([
            Value::String("zebra".into()),
            Value::String("older-entry".into())
        ])
    );
}

#[test]
fn tty_minibuffer_history_recall_submits_the_recalled_entry() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    interp.set_variable(
        "minibuffer-history",
        Value::list([Value::String("recalled".into())]),
        &mut env,
    );
    let script: std::rc::Rc<std::cell::RefCell<Vec<Value>>> =
        std::rc::Rc::new(std::cell::RefCell::new(
            "\u{1b}p\r"
                .chars()
                .rev()
                .map(|ch| Value::Integer(ch as i64))
                .collect(),
        ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    let result = call(
        &mut interp,
        "read-from-minibuffer",
        &[
            Value::String("Input: ".into()),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            Value::Symbol("minibuffer-history".into()),
        ],
        &mut env,
    );
    set_tty_event_reader(None);
    assert_eq!(
        result.expect("history recall submits"),
        Value::String("recalled".into())
    );
}

#[test]
fn tty_minibuffer_quit_signals_gnu_quit() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    // The frontend's reader answers None for C-g; the loop signals quit.
    set_tty_event_reader(Some(Box::new(|| None)));
    let result = call(
        &mut interp,
        "read-string",
        &[Value::String("File: ".into())],
        &mut env,
    );
    set_tty_event_reader(None);
    let Err(LispError::SignalValue(data)) = result else {
        panic!("C-g from the minibuffer loop must signal, got {result:?}");
    };
    assert_eq!(data, Value::Symbol("quit".into()));
}

#[test]
fn tty_completing_read_completes_with_tab() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    let script: std::rc::Rc<std::cell::RefCell<Vec<Value>>> =
        std::rc::Rc::new(std::cell::RefCell::new(
            "forw\t\r"
                .chars()
                .rev()
                .map(|ch| Value::Integer(ch as i64))
                .collect(),
        ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    let result = call(
        &mut interp,
        "completing-read",
        &[
            Value::String("Command: ".into()),
            Value::list([
                Value::String("forward-char".into()),
                Value::String("backward-char".into()),
            ]),
        ],
        &mut env,
    );
    set_tty_event_reader(None);
    assert_eq!(
        result.expect("TAB completes the unique prefix"),
        Value::String("forward-char".into())
    );
}

#[test]
fn tty_read_buffer_formats_a_buffer_default_into_the_prompt() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    let current = Value::buffer(interp.current_buffer_id(), interp.buffer.name.clone());
    set_tty_event_reader(Some(Box::new(|| Some(Value::Integer(13)))));
    let prompts = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let observed = std::rc::Rc::clone(&prompts);
    set_tty_frame_redraw(Some(Box::new(move |interp, _env| {
        if let Some(prompt) = interp.minibuffer_prompt_text() {
            observed.borrow_mut().push(prompt.to_string());
        }
    })));

    let result = call(
        &mut interp,
        "read-buffer",
        &[Value::String("Kill buffer: ".into()), current, Value::T],
        &mut env,
    );
    set_tty_frame_redraw(None);
    set_tty_event_reader(None);

    assert_eq!(
        result.expect("empty input chooses the current buffer default"),
        Value::String("*scratch*".into())
    );
    assert!(
        prompts
            .borrow()
            .iter()
            .any(|prompt| prompt == "Kill buffer (default *scratch*): "),
        "read-buffer must publish GNU's formatted default prompt, got {:?}",
        prompts.borrow()
    );
}

/// A batch interpreter with the upstream Lisp tree on the load path and
/// minibuffer.el preloaded — the interactive session's shape, minus the
/// terminal.
fn upstream_interactive_interpreter() -> (Interpreter, Env) {
    let root = crate::compat::project_root().join("../emacs");
    let load_path = crate::compat::emaxx_upstream_load_path(&root).expect("upstream load path");
    let options = crate::batch::BatchRunOptions {
        load_path,
        ..Default::default()
    };
    let mut interp =
        crate::batch::initialize_batch_interpreter(&options).expect("interpreter initializes");
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    interp
        .load_target("minibuffer")
        .expect("minibuffer.el loads");
    (interp, env)
}

#[test]
fn tty_real_minibuffer_loop_runs_the_lisp_minibuffer_commands() {
    let (mut interp, mut env) = upstream_interactive_interpreter();
    let script: std::rc::Rc<std::cell::RefCell<Vec<Value>>> =
        std::rc::Rc::new(std::cell::RefCell::new(
            "alp\t\r"
                .chars()
                .rev()
                .map(|ch| Value::Integer(ch as i64))
                .collect(),
        ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    let result = call(
        &mut interp,
        "completing-read",
        &[
            Value::String("Word: ".into()),
            Value::list([
                Value::String("alphabet".into()),
                Value::String("alphameric".into()),
            ]),
        ],
        &mut env,
    );
    set_tty_event_reader(None);
    // TAB ran minibuffer.el's `minibuffer-complete' (the common prefix of
    // the two candidates), and RET exited through `exit-minibuffer's
    // throw — the keymap-dispatched command is what last-command records.
    assert_eq!(
        result.expect("the real minibuffer loop submits"),
        Value::String("alpha".into())
    );
    assert_eq!(
        interp.lookup_var("last-command", &env),
        Some(Value::Symbol("exit-minibuffer".into())),
        "RET must dispatch minibuffer.el's exit-minibuffer"
    );
}

#[test]
fn minibuffer_prompt_carries_its_face_through_the_read() {
    let (mut interp, mut env) = upstream_interactive_interpreter();
    let script: std::rc::Rc<std::cell::RefCell<Vec<Value>>> =
        std::rc::Rc::new(std::cell::RefCell::new(
            "x\r"
                .chars()
                .rev()
                .map(|ch| Value::Integer(ch as i64))
                .collect(),
        ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    // Observe the live minibuffer from the frame-redraw hook, exactly
    // where the frontend composes the echo row.
    let observed: std::rc::Rc<std::cell::RefCell<Vec<(Value, Value)>>> =
        std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let sink = std::rc::Rc::clone(&observed);
    set_tty_frame_redraw(Some(Box::new(move |interp, _env| {
        let active = interp
            .active_minibuffer_buffer_id()
            .and_then(|id| interp.buffer_identity_value(id))
            .unwrap_or(Value::Nil);
        let face = interp
            .buffer
            .text_property_at(interp.buffer.point_min(), "face")
            .unwrap_or(Value::Nil);
        sink.borrow_mut().push((active, face));
    })));
    let result = call(
        &mut interp,
        "read-from-minibuffer",
        &[Value::String("P: ".into())],
        &mut env,
    );
    set_tty_frame_redraw(None);
    set_tty_event_reader(None);
    assert_eq!(result.expect("read submits"), Value::String("x".into()));
    let observed = observed.borrow();
    assert!(!observed.is_empty(), "the redraw hook runs during the read");
    let (active, face) = &observed[0];
    assert!(
        matches!(active, Value::Buffer(_)),
        "the native minibuffer runtime holds the live buffer, got {active:?}"
    );
    assert_eq!(
        face,
        &Value::Symbol("minibuffer-prompt".into()),
        "the prompt text carries minibuffer-prompt via minibuffer-prompt-properties"
    );
}

#[test]
fn active_minibuffer_selected_window_tracks_entry_across_nested_reads() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    assert_eq!(interp.active_minibuffer_activation_id(), None);
    let entry = call(&mut interp, "selected-window", &[], &mut env).expect("entry window");
    let outer = crate::lisp::primitives::activate_minibuffer(
        &mut interp,
        &Value::String("Outer: ".into()),
        &Value::String("".into()),
        Value::Nil,
        &mut env,
    )
    .expect("outer minibuffer activates");
    let outer_activation = interp
        .active_minibuffer_activation_id()
        .expect("outer activation has an identity");
    assert_eq!(
        call(&mut interp, "minibuffer-selected-window", &[], &mut env).expect("outer entry query"),
        entry,
        "the outer read remembers its ordinary entry window"
    );
    let outer_minibuffer =
        call(&mut interp, "active-minibuffer-window", &[], &mut env).expect("active window");

    let inner = crate::lisp::primitives::activate_minibuffer(
        &mut interp,
        &Value::String("Inner: ".into()),
        &Value::String("".into()),
        Value::Nil,
        &mut env,
    )
    .expect("nested minibuffer activates");
    let inner_activation = interp
        .active_minibuffer_activation_id()
        .expect("inner activation has an identity");
    assert!(inner_activation > outer_activation);
    assert_eq!(
        call(&mut interp, "minibuffer-selected-window", &[], &mut env).expect("inner entry query"),
        outer_minibuffer,
        "a nested read remembers the outer minibuffer window"
    );

    crate::lisp::primitives::restore_active_minibuffer(&mut interp, inner);
    assert_eq!(
        interp.active_minibuffer_activation_id(),
        Some(outer_activation),
        "unwinding a nested read restores its outer sizing identity"
    );
    assert_eq!(
        call(&mut interp, "minibuffer-selected-window", &[], &mut env)
            .expect("restored outer query"),
        entry,
        "unwinding the nested read restores the outer entry window"
    );
    crate::lisp::primitives::restore_active_minibuffer(&mut interp, outer);
    assert_eq!(interp.active_minibuffer_activation_id(), None);
    assert_eq!(
        call(&mut interp, "selected-window", &[], &mut env).expect("restored selection"),
        entry,
        "unwinding the outer read restores the ordinary selection"
    );
}

#[test]
fn tty_real_minibuffer_history_recalls_through_simple_el() {
    let (mut interp, mut env) = upstream_interactive_interpreter();
    let feed_events = |events: &str| {
        let script: std::rc::Rc<std::cell::RefCell<Vec<Value>>> =
            std::rc::Rc::new(std::cell::RefCell::new(
                events
                    .chars()
                    .rev()
                    .map(|ch| Value::Integer(ch as i64))
                    .collect(),
            ));
        let feed = std::rc::Rc::clone(&script);
        set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    };
    let history = Value::Symbol("emaxx--test-history".into());
    feed_events("first\r");
    let first = call(
        &mut interp,
        "read-from-minibuffer",
        &[
            Value::String("P: ".into()),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            history.clone(),
        ],
        &mut env,
    );
    assert_eq!(
        first.expect("first read submits"),
        Value::String("first".into())
    );
    // M-p arrives as ESC p and must dispatch simple.el's
    // previous-history-element against the recorded history.
    feed_events("\u{1b}p\r");
    let recalled = call(
        &mut interp,
        "read-from-minibuffer",
        &[
            Value::String("P: ".into()),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            history,
        ],
        &mut env,
    );
    set_tty_event_reader(None);
    assert_eq!(
        recalled.expect("history recall submits"),
        Value::String("first".into())
    );
}

#[test]
fn ascii_file_detection_records_undecided() {
    // coding.c's detection decides nothing for pure-ASCII bytes: the
    // recorded coding is `undecided' with the detected eol variant,
    // whatever the priorities say (prefer-utf-8 cannot even be
    // preferred, and a preferred utf-8-auto still answers
    // undecided-unix).  occur relies on this: it copies the searched
    // buffer's coding through set-buffer-file-coding-system, whose
    // merge with the buffer default only fires for undecided.  The
    // undecided mnemonic is `-' (coding.c:12281).
    let contract = r#"
        (let ((file (make-temp-file "emaxx-ascii" nil ".txt" "plain ascii\n")))
          (unwind-protect
              (with-temp-buffer
                (insert-file-contents file)
                (list last-coding-system-used
                      (coding-system-mnemonic 'undecided)))
            (delete-file file)))
    "#;
    let expected = r#"(undecided-unix 45)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);
}

#[test]
fn replace_match_grafts_replacement_string_properties() {
    // search.c Freplace_match hands the replacement to replace_range,
    // which grafts the string's text-property intervals into the buffer
    // — grep-filter paints its match highlight exactly this way.  A
    // backslash substitution rebuilds the text (build_string) and
    // GNU loses the properties there, so only same-length inserts keep
    // them.
    let contract = r#"
        (with-temp-buffer
          (insert "abcdef")
          (goto-char (point-min))
          (re-search-forward "cd")
          (replace-match (propertize "XY" 'font-lock-face 'match) t t)
          (list (buffer-string) (get-text-property 3 'font-lock-face)))
    "#;
    let expected = r#"(#("abXYef" 2 4 (font-lock-face match)) match)"#;
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);
}

#[test]
fn interactive_form_strips_command_modes_like_gnu() {
    // callint.c Finteractive_form answers exactly `(interactive SPEC)':
    // MODES entries after the descriptor belong to `command-modes', and
    // a bare `(interactive)' reports an explicit nil descriptor.
    let contract = r#"
        (progn
          (fset 'emaxx--tc-modes '(lambda () (interactive nil text-mode) 1))
          (fset 'emaxx--tc-arg '(lambda (n) (interactive "p" text-mode) n))
          (fset 'emaxx--tc-bare '(lambda () (interactive) 2))
          (list (interactive-form 'emaxx--tc-modes)
                (interactive-form 'emaxx--tc-arg)
                (interactive-form 'emaxx--tc-bare)))
    "#;
    let expected = r#"((interactive nil) (interactive "p") (interactive))"#;
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);

    let mut interp = Interpreter::new();
    let form = Reader::new(contract)
        .read()
        .expect("interactive-form contract should parse")
        .expect("interactive-form contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("interactive-form contract should evaluate")
            .to_string(),
        expected
    );
}

#[test]
fn indirect_buffers_share_text_properties_with_their_base() {
    // GNU keeps text properties in the intervals of the shared text, so
    // an indirect buffer sees the base's properties — those present at
    // creation and every later change — and the base sees changes made
    // through the indirect buffer (comint's input fontification reads
    // `field' through its indirect buffer this way).
    let contract = r#"
        (progn
          (save-current-buffer
            (set-buffer (get-buffer-create "b"))
            (insert "hello world")
            (put-text-property 1 6 'field 'output))
          (make-indirect-buffer "b" "i")
          (save-current-buffer
            (set-buffer "b") (put-text-property 7 9 'field 'late))
          (save-current-buffer
            (set-buffer "i") (put-text-property 9 11 'field 'from-ind))
          (list (save-current-buffer
                  (set-buffer "i")
                  (list (get-text-property 1 'field)
                        (get-text-property 5 'field)
                        (get-text-property 6 'field)
                        (get-text-property 7 'field)))
                (save-current-buffer
                  (set-buffer "b") (get-text-property 9 'field))))
    "#;
    let expected = "((output output nil late) from-ind)";
    assert_upstream_primitive_contract(&format!("(prin1 {contract})"), expected);

    let mut interp = Interpreter::new();
    let form = Reader::new(contract)
        .read()
        .expect("indirect property contract should parse")
        .expect("indirect property contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut Vec::new())
            .expect("indirect property contract should evaluate")
            .to_string(),
        expected
    );
}

#[test]
fn window_resize_apply_commits_staged_pixel_sizes() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let upper = call(&mut interp, "selected-window", &[], &mut env).expect("selected window");
    call(
        &mut interp,
        "split-window-internal",
        &[
            upper.clone(),
            Value::Integer(12),
            Value::Nil,
            Value::Float(0.5),
        ],
        &mut env,
    )
    .expect("split succeeds");
    let windows = call(&mut interp, "window-list", &[], &mut env)
        .expect("window list")
        .to_vec()
        .expect("list of windows");
    let lower = windows[1].clone();
    for (window, staged) in [(&upper, 16i64), (&lower, 8i64)] {
        call(
            &mut interp,
            "set-window-new-pixel",
            &[(*window).clone(), Value::Integer(staged)],
            &mut env,
        )
        .expect("staging succeeds");
    }
    call(&mut interp, "window-resize-apply", &[], &mut env).expect("apply succeeds");
    let edges = |interp: &mut Interpreter, env: &mut Env, window: &Value| {
        window_edges_from_natives(interp, env, std::slice::from_ref(window))
    };
    assert_eq!(
        edges(&mut interp, &mut env, &upper),
        vec![0, 0, 80, 16],
        "the upper window takes its staged 16 lines"
    );
    assert_eq!(
        edges(&mut interp, &mut env, &lower),
        vec![0, 16, 80, 24],
        "the lower window is laid out below the applied upper size"
    );
}

#[test]
fn window_text_pixel_size_measures_the_window_buffer_with_mode_lines() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.buffer.insert("aa\nbbbb\nc\n");
    let plain = call(&mut interp, "window-text-pixel-size", &[], &mut env).expect("size");
    assert_eq!(
        plain,
        Value::cons(Value::Integer(4), Value::Integer(3)),
        "widest line and line count in cell units"
    );
    let with_mode_line = call(
        &mut interp,
        "window-text-pixel-size",
        &[
            Value::Nil,
            Value::Nil,
            Value::T,
            Value::Nil,
            Value::Nil,
            Value::T,
        ],
        &mut env,
    )
    .expect("size with mode lines");
    assert_eq!(
        with_mode_line,
        Value::cons(Value::Integer(4), Value::Integer(4)),
        "MODE-LINES t adds the window's mode line — fit-window-to-buffer sizes from this"
    );
}

#[test]
fn marker_adjustments_stay_adjacent_to_their_deletion_in_the_undo_list() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    // `switch-to-buffer' is GNU window.el's; the undo protocol under
    // test only needs the buffer to become current.
    let undo_buffer = call(
        &mut interp,
        "get-buffer-create",
        &[Value::String("undo-markers".into())],
        &mut env,
    )
    .expect("buffer creates");
    call(&mut interp, "set-buffer", &[undo_buffer], &mut env).expect("buffer switches");
    call(&mut interp, "buffer-enable-undo", &[], &mut env).expect("undo enables");
    call(
        &mut interp,
        "insert",
        &[Value::String("one\ntwo\n".into())],
        &mut env,
    )
    .expect("insert succeeds");
    call(
        &mut interp,
        "set-buffer-modified-p",
        &[Value::Nil],
        &mut env,
    )
    .expect("modified clears");
    let marker = call(&mut interp, "make-marker", &[], &mut env).expect("marker");
    call(
        &mut interp,
        "set-marker",
        &[marker.clone(), Value::Integer(4)],
        &mut env,
    )
    .expect("marker set");
    call(&mut interp, "undo-boundary", &[], &mut env).expect("boundary");
    call(
        &mut interp,
        "delete-region",
        &[Value::Integer(1), Value::Integer(4)],
        &mut env,
    )
    .expect("delete succeeds");
    let undo_list = interp
        .lookup_var("buffer-undo-list", &env)
        .expect("undo list")
        .to_vec()
        .expect("list entries");
    // undo.c's order (oracle probe undomk.el): the deletion record first,
    // its marker adjustment directly after — primitive-undo consumes marker
    // riders only by that adjacency — then record_point's plain point entry
    // (point sat at 9, away from the deletion), with the first-change
    // (t . TIME) entry below all three.
    let deletion = undo_list[0].cons_values().expect("deletion entry");
    assert!(deletion.0.is_string(), "car is the deleted text");
    let rider = undo_list[1].cons_values().expect("marker rider");
    assert!(
        matches!(rider.0, Value::Marker(_)),
        "the marker adjustment follows its deletion, got {:?}",
        undo_list[1]
    );
    assert_eq!(rider.1, Value::Integer(-3));
    assert_eq!(
        undo_list[2],
        Value::Integer(9),
        "record_point's entry sits between the rider and the first-change cell"
    );
    let first_change = undo_list[3].cons_values().expect("first-change entry");
    assert_eq!(first_change.0, Value::T, "the (t . TIME) entry sits below");
}

#[test]
fn dumped_default_bindings_resolve_for_the_terminal_frontend() {
    // The dumped image's real global map, built by preloaded bindings.el
    // and friends.  Every expectation below matches GNU's raw dumped map
    // (probed via `lookup-key'/`key-binding' in `emacs -Q -batch').  The
    // pinned oracle is an NS build whose dump additionally loads
    // term/ns-win.el (rebinding e.g. <home> to `beginning-of-buffer');
    // Emaxx models a no-window-system build, so those platform rebinds are
    // deliberately absent and not asserted here.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    // GNU starts in *scratch*, whose `lisp-interaction-mode' map shadows DEL
    // with `backward-delete-char-untabify' -- both runtimes agree on that.
    // These expectations are about the dumped *global* map, so read them
    // where no major-mode map intervenes: GNU and Emaxx both answer
    // `delete-backward-char' for DEL in a `fundamental-mode' buffer.
    crate::test_support::eval_lisp(&mut interp, &mut env, "(fundamental-mode)")
        .expect("leave lisp-interaction-mode");
    for (keys, expected) in [
        (vec![Value::Integer(24), Value::Integer(19)], "save-buffer"),
        (
            vec![Value::Integer(24), Value::Integer(3)],
            "save-buffers-kill-terminal",
        ),
        (
            vec![Value::Integer(24), Value::Integer(98)],
            "switch-to-buffer",
        ),
        (vec![Value::Integer(24), Value::Integer(107)], "kill-buffer"),
        (vec![Value::Integer(24), Value::Integer(117)], "undo"),
        (vec![Value::Integer(127)], "delete-backward-char"),
        (vec![Value::Integer(31)], "undo"),
        (vec![Value::Symbol("up".into())], "previous-line"),
        (vec![Value::Symbol("down".into())], "next-line"),
        (vec![Value::Symbol("left".into())], "left-char"),
        (vec![Value::Symbol("right".into())], "right-char"),
        (vec![Value::Symbol("home".into())], "move-beginning-of-line"),
        (vec![Value::Symbol("end".into())], "move-end-of-line"),
        (
            vec![Value::Symbol("deletechar".into())],
            "delete-forward-char",
        ),
    ] {
        let key_vector = Value::list(
            std::iter::once(Value::Symbol("vector-literal".into())).chain(keys.iter().cloned()),
        );
        let binding = call(
            &mut interp,
            "key-binding",
            &[key_vector, Value::T],
            &mut env,
        )
        .expect("key-binding resolves dumped defaults");
        assert_eq!(
            binding,
            Value::Symbol(expected.into()),
            "binding for {keys:?}"
        );
    }
}

#[test]
fn command_remapping_finds_fresh_remap_bindings() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        "(let ((map (make-keymap)))
           (define-key map \"x\" 'foo)
           (define-key map \"y\" 'bar)
           (define-key map [remap foo] 'bar)
           map)",
    )
    .read()
    .expect("read keymap fixture")
    .expect("keymap fixture form");
    let map = interp.eval(&form, &mut env).expect("build keymap fixture");
    let remapped = call(
        &mut interp,
        "command-remapping",
        &[Value::Symbol("foo".into()), Value::Nil, map.clone()],
        &mut env,
    )
    .expect("command-remapping resolves");
    assert_eq!(remapped, Value::Symbol("bar".into()));
    let where_is = call(
        &mut interp,
        "where-is-internal",
        &[Value::Symbol("foo".into()), map, Value::T],
        &mut env,
    )
    .expect("where-is-internal resolves");
    assert_eq!(format!("{where_is}"), "[121]");
}

#[test]
fn window_end_and_posn_follow_published_interactive_geometry() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    for n in 0..60 {
        call(
            &mut interp,
            "insert",
            &[Value::String(format!("line {n:03}\n").into())],
            &mut env,
        )
        .expect("insert seeds the buffer");
    }
    let point_max = interp.buffer.point_max();

    // Batch sessions answer like GNU --batch: the whole buffer shows.
    let end = call(&mut interp, "window-end", &[], &mut env).expect("window-end");
    assert_eq!(end, Value::Integer(point_max as i64));
    let posn = call(&mut interp, "posn-at-point", &[], &mut env).expect("posn-at-point");
    assert_eq!(posn, Value::Nil, "no glyph matrix in batch");

    // A frontend publishing live geometry changes both answers.
    set_interactive_window_metrics(Some(InteractiveWindowMetrics {
        text_height: 22,
        window_end: 199,
    }));
    let end = call(&mut interp, "window-end", &[], &mut env).expect("window-end");
    assert_eq!(end, Value::Integer(199), "window-end reads the glass state");

    call(&mut interp, "goto-char", &[Value::Integer(10)], &mut env).expect("goto-char");
    let posn = call(&mut interp, "posn-at-point", &[], &mut env).expect("posn-at-point");
    let items = posn.to_vec().expect("posn is a list");
    assert_eq!(items[1], Value::Integer(10), "posn carries the position");
    assert_eq!(
        items[2],
        Value::cons(Value::Integer(0), Value::Integer(1)),
        "line 2 column 0 sits at x=0 y=1"
    );

    // Positions beyond the displayed extent answer nil, GNU's contract.
    call(&mut interp, "goto-char", &[Value::Integer(300)], &mut env).expect("goto-char");
    let posn = call(&mut interp, "posn-at-point", &[], &mut env).expect("posn-at-point");
    assert_eq!(posn, Value::Nil, "off-window positions have no posn");

    set_interactive_window_metrics(None);
}

#[test]
fn recenter_uses_the_published_window_height_for_negative_lines() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::T, &mut env);
    for n in 0..60 {
        call(
            &mut interp,
            "insert",
            &[Value::String(format!("line {n:03}\n").into())],
            &mut env,
        )
        .expect("insert seeds the buffer");
    }
    set_interactive_window_metrics(Some(InteractiveWindowMetrics {
        text_height: 22,
        window_end: 199,
    }));
    // Point on line 31; (recenter -3) puts it 3 rows above the bottom of
    // a 22-row window: 19 lines above the start, so the window starts at
    // line 12 (simple.el's end-of-buffer contract).
    call(
        &mut interp,
        "goto-char",
        &[Value::Integer((30 * 9 + 1) as i64)],
        &mut env,
    )
    .expect("goto-char");
    call(&mut interp, "recenter", &[Value::Integer(-3)], &mut env).expect("recenter");
    let start = call(&mut interp, "window-start", &[], &mut env).expect("window-start");
    set_interactive_window_metrics(None);
    assert_eq!(
        start,
        Value::Integer((11 * 9 + 1) as i64),
        "window starts 19 lines above point"
    );
}

#[test]
fn interactive_recenter_uses_the_live_shrunken_window_height() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    for n in 0..30 {
        call(
            &mut interp,
            "insert",
            &[Value::String(format!("line {n:02}\n").into())],
            &mut env,
        )
        .expect("insert seeds the buffer");
    }
    let selected =
        call(&mut interp, "selected-window", &[], &mut env).expect("selected window is live");
    call(
        &mut interp,
        "set-window-new-pixel",
        &[selected, Value::Integer(16)],
        &mut env,
    )
    .expect("stage the minibuffer-shrunken height");
    call(&mut interp, "window-resize-apply", &[], &mut env).expect("apply the live height");
    assert_eq!(
        call(&mut interp, "window-body-height", &[], &mut env).expect("body height"),
        Value::Integer(15)
    );
    // Model the stale pre-minibuffer glyph publication that used to win over
    // the live 15-row body while Consult temporarily selected this window.
    set_interactive_window_metrics(Some(InteractiveWindowMetrics {
        text_height: 21,
        window_end: interp.buffer.point_max(),
    }));
    call(&mut interp, "goto-char", &[Value::Integer(161)], &mut env).expect("line 21");
    call(&mut interp, "recenter", &[], &mut env).expect("recenter");
    let start = call(&mut interp, "window-start", &[], &mut env).expect("window start");
    set_interactive_window_metrics(None);
    assert_eq!(
        start,
        Value::Integer(105),
        "a 15-row body centers line 21 with line 14 at the top"
    );
}

#[test]
fn interactive_vertical_motion_honors_the_cons_goal_column() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    call(
        &mut interp,
        "insert",
        &[Value::String(
            format!("top\n{}\nbottom\n", "wide".repeat(50)).into(),
        )],
        &mut env,
    )
    .expect("insert seeds the buffer");
    // The wide line starts at position 5; its second screen row starts 79
    // characters later at 84 (80-column frame, continuation reserves one).
    let goal = Value::cons(Value::Integer(3), Value::Integer(1));

    // Batch ignores the goal column, GNU's --batch behavior.  `noninteractive'
    // is emacs.c's DEFVAR: nil in the dumped default, flipped to t by a
    // batch startup, so state the batch session explicitly here.
    interp.set_variable("noninteractive", Value::T, &mut env);
    call(&mut interp, "goto-char", &[Value::Integer(5)], &mut env).expect("goto-char");
    call(
        &mut interp,
        "vertical-motion",
        std::slice::from_ref(&goal),
        &mut env,
    )
    .expect("vertical-motion");
    assert_eq!(interp.buffer.point(), 84, "batch lands at the row start");

    // An interactive session moves to the goal column within the row,
    // line-move-visual's contract; a float goal (posn pixels divided by
    // the frame char width) works the same way.
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    call(&mut interp, "goto-char", &[Value::Integer(5)], &mut env).expect("goto-char");
    call(&mut interp, "vertical-motion", &[goal], &mut env).expect("vertical-motion");
    assert_eq!(interp.buffer.point(), 87, "goal column 3 within the row");

    let float_goal = Value::cons(Value::Float(3.0), Value::Integer(-1));
    call(&mut interp, "vertical-motion", &[float_goal], &mut env).expect("vertical-motion");
    assert_eq!(
        interp.buffer.point(),
        8,
        "float goal moves back up to column 3"
    );
}

#[test]
fn format_mode_line_renders_the_dumped_spec_interactively() {
    let mut interp = crate::batch::initialize_interactive_interpreter()
        .expect("interactive interpreter initializes");
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    call(
        &mut interp,
        "insert",
        &[Value::String("hello\n".into())],
        &mut env,
    )
    .expect("insert");
    set_interactive_window_metrics(Some(InteractiveWindowMetrics {
        text_height: 22,
        window_end: 7,
    }));
    let text = call(&mut interp, "format-mode-line", &[Value::Nil], &mut env)
        .expect("format-mode-line renders");
    let text = format!("{text}");
    // Spec strings written in the format are %-expanded; symbol string
    // values are literal; conditionals and paddings follow the spec.
    let pieces = call(
        &mut interp,
        "format-mode-line",
        &[Value::String("%l|%p|%b".into())],
        &mut env,
    )
    .expect("spec string renders");
    set_interactive_window_metrics(None);
    // The initial buffer is *scratch* in `lisp-interaction-mode', as in GNU.
    // Match the mode name by prefix: the minor-mode list that follows it is
    // not something this test has oracle evidence for.
    assert!(
        text.contains("(Lisp Interaction") && text.contains("L2") && text.contains("All"),
        "dumped spec renders the GNU shape, got {text:?}"
    );
    assert!(
        text.contains("**"),
        "a modified buffer shows the ** flags, got {text:?}"
    );
    assert_eq!(format!("{pieces}"), "\"2|All|*scratch*\"");
}

#[test]
fn mode_line_line_number_is_relative_to_the_accessible_region() {
    let mut interp = crate::batch::initialize_interactive_interpreter()
        .expect("interactive interpreter initializes");
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    call(
        &mut interp,
        "insert",
        &[Value::String("one\ntwo\nthree\n".into())],
        &mut env,
    )
    .expect("insert narrowed mode-line sample");
    let point_max = interp.buffer.point_max();
    call(
        &mut interp,
        "narrow-to-region",
        &[Value::Integer(5), Value::Integer(point_max as i64)],
        &mut env,
    )
    .expect("narrow to the second line");
    call(&mut interp, "goto-char", &[Value::Integer(5)], &mut env)
        .expect("move to accessible start");
    set_interactive_window_metrics(Some(InteractiveWindowMetrics {
        text_height: 22,
        window_end: point_max,
    }));
    let line = call(
        &mut interp,
        "format-mode-line",
        &[Value::String("%l".into())],
        &mut env,
    )
    .expect("render narrowed line number");
    set_interactive_window_metrics(None);
    assert_eq!(line, Value::String("1".into()));
}

// ── Scroll, recenter, and mode-line contracts (round: paging + mode line) ──

/// Evaluate PROGRAM in an initialized emaxx interpreter and return the
/// accumulated `contract-out' string; the same program princs it for the
/// oracle's stdout comparison.
fn emaxx_batch_output(program: &str) -> String {
    let mut interp =
        crate::batch::initialize_interactive_interpreter().expect("batch interpreter initializes");
    let mut env = Vec::new();
    let forms = Reader::new(program).read_all().expect("program parses");
    for form in &forms {
        interp
            .eval(form, &mut env)
            .unwrap_or_else(|error| panic!("emaxx eval failed: {error:?}"));
    }
    let value = interp
        .lookup_var("contract-out", &env)
        .expect("program sets contract-out");
    let Value::String(text) = value else {
        let text = format!("{value}");
        return text.trim_matches('"').to_string();
    };
    text.to_string()
}

const SCROLL_CONTRACT_PROGRAM: &str = "(progn (setq contract-out \"\")
  (defun contract-note (text) (setq contract-out (concat contract-out text)))
  (dotimes (n 100) (insert (format \"line %03d\\n\" n)))
  (goto-char (point-min)) (scroll-up)
  (contract-note (format \"A:%s,%s \" (window-start) (point)))
  (scroll-up 5) (contract-note (format \"B:%s,%s \" (window-start) (point)))
  (scroll-down) (contract-note (format \"C:%s,%s \" (window-start) (point)))
  (goto-char (point-max))
  (contract-note (format \"D:%s \" (condition-case e (progn (scroll-up) \"ok\") (error (car e)))))
  (goto-char (point-min))
  (contract-note (format \"E:%s \" (condition-case e (progn (scroll-down) \"ok\") (error (car e)))))
  (contract-note (format \"F:%s \" (condition-case e (progn (scroll-up 100) \"ok\") (error (car e)))))
  (goto-char 451) (scroll-down)
  (contract-note (format \"G:%s,%s\" (point) (window-start)))
  (princ contract-out))";

const SCROLL_CONTRACT_ANSWER: &str =
    "A:190,190 B:136,190 C:1,190 D:end-of-buffer E:beginning-of-buffer F:end-of-buffer G:361,163";

#[test]
fn batch_scroll_semantics_match_the_oracle() {
    assert_upstream_primitive_contract(SCROLL_CONTRACT_PROGRAM, SCROLL_CONTRACT_ANSWER);
    assert_eq!(
        emaxx_batch_output(SCROLL_CONTRACT_PROGRAM),
        SCROLL_CONTRACT_ANSWER
    );
}

const RECENTER_CONTRACT_PROGRAM: &str = "(progn (setq contract-out \"\")
  (defun contract-note (text) (setq contract-out (concat contract-out text)))
  (dotimes (n 100) (insert (format \"line %03d\\n\" n)))
  (goto-char 451) (recenter -3) (contract-note (format \"A:%s \" (window-start)))
  (recenter) (contract-note (format \"B:%s \" (window-start)))
  (let ((this-command (quote recenter-top-bottom)) (last-command nil))
    (recenter-top-bottom) (contract-note (format \"C:%s \" (window-start)))
    (setq last-command (quote recenter-top-bottom))
    (recenter-top-bottom) (contract-note (format \"D:%s \" (window-start)))
    (recenter-top-bottom) (contract-note (format \"E:%s \" (window-start))))
  (goto-char (point-min)) (recenter 0) (move-to-window-line nil)
  (contract-note (format \"F:%s\" (point)))
  (princ contract-out))";

const RECENTER_CONTRACT_ANSWER: &str = "A:271 B:352 C:352 D:451 E:253 F:100";

#[test]
fn batch_recenter_cycling_matches_the_oracle() {
    assert_upstream_primitive_contract(RECENTER_CONTRACT_PROGRAM, RECENTER_CONTRACT_ANSWER);
    assert_eq!(
        emaxx_batch_output(RECENTER_CONTRACT_PROGRAM),
        RECENTER_CONTRACT_ANSWER
    );
}

#[test]
fn coding_system_mnemonics_match_the_oracle() {
    let program = "(progn (setq contract-out (format \"%s\" (list (coding-system-mnemonic nil)
                        (coding-system-mnemonic (quote utf-8))
                        (coding-system-mnemonic (quote prefer-utf-8-unix))
                        (coding-system-mnemonic (quote iso-latin-1))
                        (coding-system-mnemonic (quote raw-text)))))
                   (princ contract-out))";
    let answer = "(61 85 45 49 116)";
    assert_upstream_primitive_contract(program, answer);
    assert_eq!(emaxx_batch_output(program), answer);
}

#[test]
fn paging_keys_carry_the_gnu_bindings() {
    let program = "(progn (setq contract-out (format \"%s\" (list (key-binding \"\\C-v\") (key-binding \"\\ev\") (key-binding \"\\C-l\"))))
                   (princ contract-out))";
    let answer = "(scroll-up-command scroll-down-command recenter-top-bottom)";
    assert_upstream_primitive_contract(program, answer);
    assert_eq!(emaxx_batch_output(program), answer);
}

#[test]
fn glass_mode_line_pads_min_width_spans_like_the_display_engine() {
    let mut interp = crate::batch::initialize_interactive_interpreter()
        .expect("interactive interpreter initializes");
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    for n in 0..60 {
        call(
            &mut interp,
            "insert",
            &[Value::String(format!("line {n:03}\n").into())],
            &mut env,
        )
        .expect("insert");
    }
    call(&mut interp, "goto-char", &[Value::Integer(1)], &mut env).expect("goto-char");
    set_interactive_window_metrics(Some(InteractiveWindowMetrics {
        text_height: 22,
        window_end: 199,
    }));
    let glass = crate::lisp::primitives::render_mode_line_glass(&mut interp, &mut env)
        .expect("glass render");
    let string =
        call(&mut interp, "format-mode-line", &[Value::Nil], &mut env).expect("string render");
    set_interactive_window_metrics(None);
    // The 7-column coding/modified span exceeds its min-width of 6, so
    // the glass inserts one stretch column (produce_stretch_glyph floors
    // negative widths at 1); the string form has none.
    let string = format!("{string}");
    assert!(
        glass.contains("-  F1  ") && string.contains("- F1  "),
        "glass adds the stretch column: glass={glass:?} string={string:?}"
    );
    assert!(
        glass.contains("Top   L1"),
        "percent span pads to five columns on the glass, got {glass:?}"
    );
}

#[test]
fn glass_mode_line_honors_font_lock_face_string_properties() {
    let mut interp = crate::batch::initialize_interactive_interpreter()
        .expect("interactive interpreter initializes");
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    let form = Reader::new(
        "(setq mode-line-format
               (list (propertize \" git failed\"
                                'font-lock-face 'error)))",
    )
    .read()
    .expect("mode-line form parses")
    .expect("mode-line form exists");
    interp.eval(&form, &mut env).expect("mode-line form runs");
    let window_id = interp.selected_window_id();
    let (_, spans) = crate::lisp::primitives::render_window_mode_line(
        &mut interp,
        &mut env,
        window_id,
        1,
        InteractiveWindowMetrics {
            text_height: 22,
            window_end: 1,
        },
    )
    .expect("mode line renders");
    assert_eq!(spans, vec![(0, 11, Value::Symbol("error".into()))]);
}

#[test]
fn undo_file_marker_records_the_visited_modtime() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    let directory = std::env::temp_dir().join(format!("emaxx-undo-marker-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("create test dir");
    let path = directory.join("marker.txt");
    std::fs::write(&path, "one\ntwo\n").expect("write fixture");
    call_via_lisp(
        &mut interp,
        "find-file",
        &[Value::String(path.display().to_string().into())],
        &mut env,
    )
    .expect("find-file");
    call(&mut interp, "delete-char", &[Value::Integer(3)], &mut env).expect("delete-char");
    let undo_list = interp.buffer.undo_list_value();
    let marker = undo_list
        .to_vec()
        .expect("undo list is a list")
        .into_iter()
        .find(|entry| {
            entry
                .to_vec()
                .ok()
                .and_then(|items| items.first().cloned())
                .is_some_and(|head| head == Value::T)
        })
        .expect("undo list carries the (t . TIME) marker");
    let time =
        call(&mut interp, "visited-file-modtime", &[], &mut env).expect("visited-file-modtime");
    let marker_items = marker.to_vec().expect("marker is a list");
    assert_eq!(
        Value::list(marker_items[1..].to_vec()),
        time,
        "the marker's TIME equals visited-file-modtime, primitive-undo's test"
    );
    let _ = std::fs::remove_dir_all(&directory);
}

#[test]
fn undo_host_records_match_gnu_save_point_property_and_multibyte_contracts() {
    let program = r#"(progn
      (setq contract-out
            (with-temp-buffer
              (buffer-enable-undo)
              (insert "x")
              (set-buffer-modified-p nil)
              (put-text-property 1 2 'face 'bold)
              (set-buffer-multibyte nil)
              (prin1-to-string buffer-undo-list)))
      (princ contract-out))"#;
    let expected = "((apply set-buffer-multibyte t) (nil face nil 1 . 2) (t . 0) (1 . 2) (t . 0))";
    assert_upstream_primitive_contract(program, expected);
    assert_eq!(emaxx_batch_output(program), expected);

    assert_upstream_primitive_contract(
        "(condition-case e (> 1 nil) (error (prin1 e)))",
        "(wrong-type-argument number-or-marker-p nil)",
    );
    assert_eq!(
        emaxx_batch_output(
            "(progn (setq contract-out (condition-case e (> 1 nil) (error (prin1-to-string e)))) (princ contract-out))"
        ),
        "(wrong-type-argument number-or-marker-p nil)"
    );
}

#[test]
fn push_mark_separates_marker_motion_from_transient_region_activation() {
    let program = r#"(progn
      (setq contract-out
            (let ((transient-mark-mode t))
              (with-temp-buffer
                (insert "abc")
                (push-mark nil t)
                (let ((plain (list (mark t) mark-active (region-active-p))))
                  (push-mark 2 t t)
                  (list plain (list (mark t) mark-active (region-active-p)))))))
      (princ (prin1-to-string contract-out)))"#;
    let expected = "((4 nil nil) (2 t t))";
    assert_upstream_primitive_contract(program, expected);
    assert_eq!(emaxx_batch_output(program), expected);
}

#[test]
fn interactive_undo_restores_the_unmodified_state() {
    // The tty command loop's per-command undo boundaries plus the
    // modtime-carrying (t . TIME) marker let simple.el's `undo' walk back
    // to the save point and clear the modified flags, GNU's behavior.
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp = crate::batch::initialize_batch_interpreter(&options)
        .expect("initialize GNU-compatible batch interpreter");
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    let directory = std::env::temp_dir().join(format!("emaxx-undo-clean-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("create test dir");
    let path = directory.join("clean.txt");
    std::fs::write(&path, "one\ntwo\nthree\n").expect("write fixture");
    call_via_lisp(
        &mut interp,
        "find-file",
        &[Value::String(path.display().to_string().into())],
        &mut env,
    )
    .expect("find-file");

    let run = |interp: &mut Interpreter, env: &mut Env, source: &str| {
        let forms = Reader::new(source).read_all().expect("form parses");
        for form in &forms {
            interp
                .eval(form, env)
                .unwrap_or_else(|error| panic!("{source}: {error:?}"));
        }
    };

    // One command cycle: boundary, then the edit.
    interp.buffer.push_undo_boundary();
    run(&mut interp, &mut env, "(kill-line)");
    assert!(interp.buffer.is_modified(), "kill-line modifies");

    // Next command cycle: boundary, then undo.
    interp.buffer.push_undo_boundary();
    run(&mut interp, &mut env, "(undo)");

    let text = interp
        .buffer
        .buffer_substring(1, interp.buffer.point_max())
        .expect("buffer text");
    let modified = interp.buffer.is_modified();
    let _ = std::fs::remove_dir_all(&directory);
    assert_eq!(text, "one\ntwo\nthree\n", "undo restores the killed line");
    assert!(
        !modified,
        "undoing back to the saved state clears the modified flag"
    );
}

#[test]
fn error_message_strings_match_the_oracle() {
    let program = "(progn (setq contract-out (mapconcat (lambda (e) (error-message-string e))
        (list (quote (quit)) (quote (beginning-of-buffer)) (quote (error \"boom\"))
              (quote (wrong-type-argument listp t))
              (quote (user-error \"No further undo information\"))
              (quote (file-missing \"Opening input file\" \"No such file\" \"/tmp/x\")))
        \"|\"))
        (princ contract-out))";
    let answer = "Quit|Beginning of buffer|boom|Wrong type argument: listp, t|No further undo information|Opening input file: No such file, /tmp/x";
    assert_upstream_primitive_contract(program, answer);
    assert_eq!(emaxx_batch_output(program), answer);
}

#[test]
fn error_message_strings_use_gnu_condition_specific_data_quoting() {
    let program = r#"(progn
      (put 'magit-like-error 'error-conditions '(magit-like-error error))
      (put 'magit-like-error 'error-message "Git error")
      (setq contract-out
            (prin1-to-string
             (list
              (error-message-string '(magit-like-error "one" "two"))
              (error-message-string '(error "lead" "tail"))
              (error-message-string '(end-of-file "one" "two")))))
      (princ contract-out))"#;
    let answer = r#"("Git error: \"one\", \"two\"" "lead: \"tail\"" "End of file during parsing: one, two")"#;
    assert_upstream_primitive_contract(program, answer);
    assert_eq!(emaxx_batch_output(program), answer);
}

fn window_edges_from_natives(
    interp: &mut Interpreter,
    env: &mut Env,
    window: &[Value],
) -> Vec<i64> {
    // GNU's `window-edges' is window.el Lisp over these window.c
    // primitives; compose the same (LEFT TOP RIGHT BOTTOM) answer here.
    let field = |interp: &mut Interpreter, env: &mut Env, name: &str| {
        call(interp, name, window, env)
            .expect(name)
            .as_integer()
            .expect("integer geometry")
    };
    let left = field(interp, env, "window-left-column");
    let top = field(interp, env, "window-top-line");
    let width = field(interp, env, "window-total-width");
    let height = field(interp, env, "window-total-height");
    vec![left, top, left + width, top + height]
}

#[test]
fn tty_frame_size_shapes_the_root_and_minibuffer_windows() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_tty_frame_size(80, 24);
    let root_edges = window_edges_from_natives(&mut interp, &mut env, &[]);
    assert_eq!(
        root_edges,
        vec![0, 1, 80, 23],
        "a 24-row tty reserves the menu-bar line above 22 root lines"
    );
    let minibuffer = call(&mut interp, "minibuffer-window", &[], &mut env).expect("window");
    let minibuffer_edges =
        window_edges_from_natives(&mut interp, &mut env, std::slice::from_ref(&minibuffer));
    assert_eq!(minibuffer_edges, vec![0, 23, 80, 24]);
}

#[test]
fn window_render_layout_reports_split_geometry_in_tree_order() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_tty_frame_size(80, 24);
    interp.buffer.insert("alpha\nbeta\ngamma\n");
    let upper = interp.selected_window_value();
    // C-x 2's shape on the 24-row tty: under the menu-bar line the
    // 22-line root splits 11/11.
    let lower = call(
        &mut interp,
        "split-window-internal",
        &[upper, Value::Integer(11), Value::Nil, Value::Float(0.5)],
        &mut env,
    )
    .expect("split-window-internal");
    let Value::Record(lower_id) = lower else {
        panic!("split answers the new window record");
    };

    let layout = crate::lisp::primitives::window_render_layout(&interp);
    assert_eq!(layout.len(), 2, "two live leaves after one split");
    let (top, bottom) = (&layout[0], &layout[1]);
    assert_eq!(
        (top.left, top.top, top.width, top.height),
        (0, 1, 80, 11),
        "the old window keeps the upper 11 lines"
    );
    assert!(top.selected, "the split leaves the old window selected");
    assert_eq!(
        (bottom.left, bottom.top, bottom.width, bottom.height),
        (0, 12, 80, 11)
    );
    assert_eq!(bottom.window_id, lower_id);
    assert!(!bottom.selected);
    assert_eq!(
        top.buffer_id, bottom.buffer_id,
        "both windows show the split buffer"
    );
    assert_eq!(bottom.start, 1);
    assert_eq!(bottom.point, 1);
}

#[test]
fn window_cycling_follows_tree_order_from_the_selected_window() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_tty_frame_size(80, 24);
    interp.buffer.insert("alpha\nbeta\n");
    let upper_left = interp.selected_window_value();
    // C-x 2 then C-x 3: the bottom window exists before the upper-right
    // one, so creation order and tree order disagree.
    let bottom = call(
        &mut interp,
        "split-window-internal",
        &[
            upper_left.clone(),
            Value::Integer(11),
            Value::Nil,
            Value::Float(0.5),
        ],
        &mut env,
    )
    .expect("split below");
    let upper_right = call(
        &mut interp,
        "split-window-internal",
        &[
            upper_left.clone(),
            Value::Integer(40),
            Value::T,
            Value::Float(0.5),
        ],
        &mut env,
    )
    .expect("split right");

    let next = call(&mut interp, "next-window", &[], &mut env).expect("next-window");
    assert_eq!(
        next, upper_right,
        "next-window walks the tree: upper-left, upper-right, bottom"
    );
    let previous = call(&mut interp, "previous-window", &[], &mut env).expect("previous-window");
    assert_eq!(previous, bottom, "previous-window wraps to the last leaf");

    let listed = call(&mut interp, "window-list", &[], &mut env).expect("window-list");
    assert_eq!(
        format!("{listed}"),
        format!("({upper_left} {upper_right} {bottom})"),
        "window-list starts at the selected window and follows tree order"
    );

    call(
        &mut interp,
        "select-window",
        std::slice::from_ref(&upper_right),
        &mut env,
    )
    .expect("select");
    let listed = call(&mut interp, "window-list", &[], &mut env).expect("window-list");
    assert_eq!(
        format!("{listed}"),
        format!("({upper_right} {bottom} {upper_left})"),
        "the cyclic order rotates to keep the selected window first"
    );
}

#[test]
fn window_mode_lines_render_in_each_windows_own_context() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_tty_frame_size(80, 24);
    interp.buffer.insert("alpha\nbeta\ngamma\ndelta\n");
    let upper = interp.selected_window_value();
    let lower = call(
        &mut interp,
        "split-window-internal",
        &[upper, Value::Integer(11), Value::Nil, Value::Float(0.5)],
        &mut env,
    )
    .expect("split");
    let Value::Record(lower_id) = lower.clone() else {
        panic!("window record");
    };
    // The lower window shows a different buffer with its own point.
    let other = call(
        &mut interp,
        "get-buffer-create",
        &[Value::String("other.txt".into())],
        &mut env,
    )
    .expect("buffer");
    call(
        &mut interp,
        "set-window-buffer",
        &[lower.clone(), other],
        &mut env,
    )
    .expect("set-window-buffer");
    {
        let saved = interp.current_buffer_id();
        let other = call(
            &mut interp,
            "get-buffer",
            &[Value::String("other.txt".into())],
            &mut env,
        )
        .expect("get-buffer");
        call(&mut interp, "set-buffer", &[other], &mut env).expect("set-buffer");
        interp.buffer.insert("one\ntwo\nthree\n");
        let _ = interp.set_current_buffer_id(saved);
    }
    call(
        &mut interp,
        "set-window-point",
        &[lower.clone(), Value::Integer(9)],
        &mut env,
    )
    .expect("set-window-point");
    // A spec exercising the per-window pieces: buffer name, the window's
    // line number, and the dedication mark.  mode-line-format is
    // buffer-local when set, so shape the default every buffer inherits.
    let spec = Reader::new(
        "(set-default 'mode-line-format
           (list \"%b L%l\"
                 '(:eval (cond ((eq (window-dedicated-p) t) \"D\")
                               ((window-dedicated-p) \"d\")
                               (t \"\")))))",
    )
    .read()
    .expect("spec parses")
    .expect("a form is present");
    interp.eval(&spec, &mut env).expect("spec installs");

    let selected_before = interp.selected_window_id();
    let buffer_before = interp.current_buffer_id();
    let point_before = interp.buffer.point();
    let metrics = crate::lisp::primitives::InteractiveWindowMetrics {
        text_height: 10,
        window_end: 14,
    };
    let (mode_line, _) = crate::lisp::primitives::render_window_mode_line(
        &mut interp,
        &mut env,
        lower_id,
        9,
        metrics,
    )
    .expect("mode line renders");
    assert!(
        mode_line.contains("other.txt"),
        "the window's own buffer names the mode line: {mode_line:?}"
    );
    assert!(
        mode_line.contains("L3"),
        "L reflects the window's point, not the selected window's: {mode_line:?}"
    );
    assert_eq!(
        interp.selected_window_id(),
        selected_before,
        "selection restored"
    );
    assert_eq!(
        interp.current_buffer_id(),
        buffer_before,
        "current buffer restored"
    );
    assert_eq!(interp.buffer.point(), point_before, "point restored");
    assert_eq!(
        call(
            &mut interp,
            "window-point",
            std::slice::from_ref(&lower),
            &mut env,
        )
        .expect("window-point after rendering"),
        Value::Integer(9),
        "rendering a non-selected mode line does not overwrite its window point"
    );

    // Dedication marks: display-buffer's weak kind shows `d', strong `D'.
    call(
        &mut interp,
        "set-window-dedicated-p",
        &[lower.clone(), Value::Symbol("soft".into())],
        &mut env,
    )
    .expect("dedicate softly");
    let (softly, _) = crate::lisp::primitives::render_window_mode_line(
        &mut interp,
        &mut env,
        lower_id,
        9,
        metrics,
    )
    .expect("mode line renders");
    assert!(softly.ends_with('d'), "weak dedication shows d: {softly:?}");
    call(
        &mut interp,
        "set-window-dedicated-p",
        &[lower, Value::T],
        &mut env,
    )
    .expect("dedicate strongly");
    let (strongly, _) = crate::lisp::primitives::render_window_mode_line(
        &mut interp,
        &mut env,
        lower_id,
        9,
        metrics,
    )
    .expect("mode line renders");
    assert!(
        strongly.ends_with('D'),
        "strong dedication shows D: {strongly:?}"
    );
}

#[test]
fn interactive_spec_i_passes_nil_without_reading_anything() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        "(defalias 'emaxx-test-spec-i (function (lambda (a b) (interactive \"i\\np\") (setq emaxx-test-spec-args (list a b)))))",
    )
    .read()
    .expect("defun parses")
    .expect("a form is present");
    interp.eval(&form, &mut env).expect("defun evaluates");
    call(
        &mut interp,
        "call-interactively",
        &[Value::Symbol("emaxx-test-spec-i".into())],
        &mut env,
    )
    .expect("call-interactively");
    let args = interp
        .lookup_var("emaxx-test-spec-args", &env)
        .expect("args recorded");
    assert_eq!(format!("{args}"), "(nil 1)");
}

#[test]
fn tty_ambiguous_tab_pops_the_completions_window_and_submit_dismisses_it() {
    // The pop-up is minibuffer.el's own machinery end to end:
    // minibuffer-complete detects no progress, minibuffer-completion-help
    // fills *Completions* and displays it through display-buffer, and the
    // exit teardown's window-configuration restore removes it.
    let (mut interp, mut env) = upstream_interactive_interpreter();
    interp.set_tty_frame_size(80, 24);
    interp.buffer.insert("alpha\nbeta\n");

    // "am" TAB completes to the common prefix; the second TAB makes no
    // progress and pops *Completions*; M-v selects that window and RET
    // submits its first candidate.
    let script: std::rc::Rc<std::cell::RefCell<Vec<Value>>> =
        std::rc::Rc::new(std::cell::RefCell::new(
            "am\t\t\x1bv\r"
                .chars()
                .rev()
                .map(|ch| Value::Integer(ch as i64))
                .collect(),
        ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    // The frame-redraw hook runs once per minibuffer iteration; observing
    // the layout there sees the pop-up while the read is still live.
    type LayoutSnapshots = Vec<(Option<String>, Vec<(String, usize, bool)>)>;
    let observed: std::rc::Rc<std::cell::RefCell<LayoutSnapshots>> =
        std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let sink = std::rc::Rc::clone(&observed);
    crate::lisp::primitives::set_tty_frame_redraw(Some(Box::new(move |interp, _env| {
        let windows = crate::lisp::primitives::window_render_layout(interp)
            .into_iter()
            .map(|info| {
                let name = if info.buffer_id == interp.current_buffer_id() {
                    interp.buffer.name.clone()
                } else {
                    interp
                        .get_buffer_by_id(info.buffer_id)
                        .map(|buffer| buffer.name.clone())
                        .unwrap_or_default()
                };
                (name, info.height, info.selected)
            })
            .collect();
        sink.borrow_mut()
            .push((crate::lisp::primitives::echo_area_message(), windows));
    })));
    let result = call(
        &mut interp,
        "completing-read",
        &[
            Value::String("Pick: ".into()),
            Value::list([
                Value::String("ambig1".into()),
                Value::String("ambig2".into()),
            ]),
        ],
        &mut env,
    );
    crate::lisp::primitives::set_tty_frame_redraw(None);
    set_tty_event_reader(None);
    assert_eq!(
        result.expect("the minibuffer submits"),
        Value::String("ambig1".into())
    );

    let observed = observed.borrow();
    let popped: Vec<_> = observed
        .iter()
        .filter(|(_, windows)| windows.iter().any(|(name, _, _)| name == "*Completions*"))
        .collect();
    assert!(
        !popped.is_empty(),
        "the ambiguous TAB shows *Completions* while the read is live: {observed:?}"
    );
    // Content: two help lines, a blank, the count line, two candidates —
    // six lines plus the mode line, GNU's fit-window-to-buffer answer.
    for (_, windows) in &popped {
        let (_, height, _) = windows
            .iter()
            .find(|(name, _, _)| name == "*Completions*")
            .expect("completions window in snapshot");
        assert_eq!(*height, 7, "the pop-up stays fitted after selection");
    }
    let (selected_echo, _) = popped
        .iter()
        .find(|(_, windows)| {
            windows
                .iter()
                .any(|(name, _, selected)| name == "*Completions*" && *selected)
        })
        .expect("M-v selects the completions window");
    assert_eq!(
        selected_echo.as_deref(),
        Some("Pick: ambig"),
        "selecting *Completions* keeps the active minibuffer in the echo area"
    );

    let final_layout = crate::lisp::primitives::window_render_layout(&interp);
    assert_eq!(
        final_layout.len(),
        1,
        "submitting the minibuffer removes the *Completions* window"
    );
    assert_eq!(
        final_layout[0].height, 22,
        "the surviving window takes the frame back under the menu bar"
    );

    // The buffer itself carries GNU 30.2's tty help text and shape.
    let (completions_id, _) = interp.find_buffer("*Completions*").expect("buffer exists");
    let saved = interp.current_buffer_id();
    interp
        .set_current_buffer_id(completions_id)
        .expect("switch");
    let text = interp
        .buffer
        .buffer_substring(1, interp.buffer.point_max())
        .expect("contents");
    let _ = interp.set_current_buffer_id(saved);
    // minibuffer.el's completion--insert-strings separates candidates
    // with newlines; the buffer ends at the last candidate.
    assert_eq!(
        text,
        "Type M-RET on a completion to select it.\n\
         Type M-<down> or M-<up> to move point between completions.\n\n\
         2 possible completions:\nambig1\nambig2"
    );
}

#[test]
fn tmm_nested_menu_keeps_the_completions_window_at_its_first_line() {
    // M-` opens the menu-bar level and `f' descends into File.  GNU keeps
    // the reused, non-selected *Completions* window's point at point-min
    // while tmm.el scans the buffer in `with-current-buffer'.
    let (mut interp, mut env) = upstream_interactive_interpreter();
    interp.set_tty_frame_size(80, 24);
    interp.buffer.insert("alpha\nbeta\ngamma\n");
    interp.load_target("tmm").expect("tmm.el loads");

    // Select File at the first prompt, then abort the nested prompt after
    // its redraw has exposed the reused completions window.
    let script: std::rc::Rc<std::cell::RefCell<Vec<Value>>> =
        std::rc::Rc::new(std::cell::RefCell::new(
            "f\u{7}"
                .chars()
                .rev()
                .map(|ch| Value::Integer(ch as i64))
                .collect(),
        ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    let observed: std::rc::Rc<std::cell::RefCell<Vec<usize>>> =
        std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let sink = std::rc::Rc::clone(&observed);
    crate::lisp::primitives::set_tty_frame_redraw(Some(Box::new(move |interp, _env| {
        for info in crate::lisp::primitives::window_render_layout(interp) {
            let name = if info.buffer_id == interp.current_buffer_id() {
                &interp.buffer.name
            } else if let Some(buffer) = interp.get_buffer_by_id(info.buffer_id) {
                &buffer.name
            } else {
                continue;
            };
            if name == "*Completions*" {
                sink.borrow_mut().push(info.point);
            }
        }
    })));
    let result = call_via_lisp(&mut interp, "tmm-menubar", &[], &mut env);
    crate::lisp::primitives::set_tty_frame_redraw(None);
    set_tty_event_reader(None);

    let observed = observed.borrow();
    assert!(
        observed.len() >= 2,
        "both menu levels display *Completions*: {observed:?}; result: {result:?}"
    );
    assert_eq!(
        observed.last(),
        Some(&1),
        "the nested menu keeps the completions window at its first line: {observed:?}"
    );
}

#[test]
fn minibuffer_reads_select_the_minibuffer_window_and_restore_the_entry_window() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("noninteractive", Value::Nil, &mut env);
    interp.set_tty_frame_size(80, 24);
    interp.buffer.insert("alpha\nbeta\ngamma\n");
    interp.buffer.goto_char(7); // line 2
    let entry_window = interp.selected_window_value();

    let script: std::rc::Rc<std::cell::RefCell<Vec<Value>>> =
        std::rc::Rc::new(std::cell::RefCell::new(
            "ok\r"
                .chars()
                .rev()
                .map(|ch| Value::Integer(ch as i64))
                .collect(),
        ));
    let feed = std::rc::Rc::clone(&script);
    set_tty_event_reader(Some(Box::new(move || feed.borrow_mut().pop())));
    let observed: std::rc::Rc<std::cell::RefCell<Vec<(Value, String)>>> =
        std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let sink = std::rc::Rc::clone(&observed);
    crate::lisp::primitives::set_tty_frame_redraw(Some(Box::new(move |interp, env| {
        let selected = interp.selected_window_value();
        let shown = call(
            interp,
            "window-buffer",
            std::slice::from_ref(&selected),
            env,
        )
        .ok()
        .and_then(|buffer| {
            call(interp, "buffer-name", &[buffer], env)
                .ok()
                .map(|name| format!("{name}"))
        })
        .unwrap_or_default();
        sink.borrow_mut().push((selected, shown));
    })));
    let result = call(
        &mut interp,
        "read-string",
        &[Value::String("Answer: ".into())],
        &mut env,
    );
    crate::lisp::primitives::set_tty_frame_redraw(None);
    set_tty_event_reader(None);
    assert_eq!(result.expect("read"), Value::String("ok".into()));

    let observed = observed.borrow();
    assert!(!observed.is_empty(), "the read repainted at least once");
    for (selected, shown) in observed.iter() {
        assert_ne!(
            selected, &entry_window,
            "the read runs in the minibuffer window, not the entry window"
        );
        assert!(
            shown.contains("*Minibuf-1*"),
            "the minibuffer window shows the minibuffer buffer: {shown}"
        );
    }
    assert_eq!(
        interp.selected_window_value(),
        entry_window,
        "finishing the read restores the entry window"
    );
    assert_eq!(
        interp.buffer.point(),
        7,
        "the entry buffer's point survives"
    );
}

#[test]
fn eager_load_expansion_invokes_the_lisp_owner_for_both_phases() {
    let mut interp = Interpreter::new();
    let mut env = Env::new();
    for form in Reader::new(
        "(setq eager-owner-calls nil)\n\
         (defalias 'internal-macroexpand-for-load\n\
           (function\n\
             (lambda (form full)\n\
               (setq eager-owner-calls (cons full eager-owner-calls))\n\
               (if full '(quote expanded-by-owner) form))))",
    )
    .read_all()
    .expect("owner fixture parses")
    {
        interp.eval(&form, &mut env).expect("install Lisp owner");
    }

    let form = Reader::new("(quote original)")
        .read_all()
        .expect("load form parses")
        .remove(0);
    let result = eager_expand_eval(&mut interp, &form, &mut env).expect("eager expansion");

    assert_eq!(result, Value::Symbol("expanded-by-owner".into()));
    assert_eq!(
        interp.lookup_var("eager-owner-calls", &env),
        Some(Value::list([Value::T, Value::Nil]))
    );
}

#[test]
fn coding_detection_follows_detect_coding_system() {
    // coding.c detect_coding_system: the head scan (ISO-2022 at the first
    // ESC/SO/SI, null and 8-bit bytes), the per-category detectors over
    // the representatives in `coding_priorities' order, the eol
    // subsidiaries chosen per candidate, and Fset_coding_system_priority
    // moving categories to the front while re-pointing the
    // `coding-category-XXX' variables and `coding-category-list'.
    let program = r#"
(let ((iso (unibyte-string #x1b #x24 #x42 #x24 #x33 #x1b #x28 #x42)))
  (list (detect-coding-string "caf\351")
        (detect-coding-string "caf\351" t)
        (detect-coding-string "abc")
        (detect-coding-string "\303\251\n")
        (detect-coding-string (unibyte-string #xa4 #xa2 #xa4 #xa4))
        (detect-coding-string (unibyte-string #x82 #xa0))
        (detect-coding-string (concat iso "\n"))
        (detect-coding-string iso t)
        (detect-coding-string (unibyte-string #xff #xfe #x61 0))
        (detect-coding-string "a\0b\r\n")
        (with-temp-buffer (insert "x\r\ny\r\n") (detect-coding-region (point-min) (point-max)))
        (coding-system-priority-list)
        (mapcar (lambda (c) (cons c (symbol-value c))) coding-category-list)
        (unwind-protect
            (progn (set-coding-system-priority 'japanese-shift-jis 'utf-8)
                   (list (detect-coding-string (unibyte-string #x82 #xa0) t)
                         (coding-system-priority-list t)
                         coding-category-sjis
                         (car coding-category-list)))
          (set-coding-system-priority 'utf-8 'iso-2022-7bit 'iso-latin-1))))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        r#"((iso-latin-1 emacs-mule in-is13194-devanagari chinese-iso-8bit iso-2022-8bit-ss2) iso-latin-1 (undecided) (utf-8-unix iso-latin-1-unix emacs-mule-unix in-is13194-devanagari-unix chinese-iso-8bit-unix utf-8-auto-unix japanese-shift-jis-unix chinese-big5-unix iso-2022-8bit-ss2-unix) (iso-latin-1 in-is13194-devanagari chinese-iso-8bit japanese-shift-jis chinese-big5 iso-2022-8bit-ss2) (emacs-mule japanese-shift-jis raw-text) (iso-2022-7bit-unix iso-2022-7bit-lock-unix iso-2022-8bit-ss2-unix iso-2022-jp-unix) iso-2022-7bit (no-conversion) (no-conversion) (undecided-dos) (utf-8 iso-2022-7bit iso-latin-1 iso-2022-7bit-lock iso-2022-8bit-ss2 emacs-mule raw-text iso-2022-jp in-is13194-devanagari chinese-iso-8bit utf-8-auto utf-8-with-signature utf-16 utf-16be-with-signature utf-16le-with-signature utf-16be utf-16le japanese-shift-jis chinese-big5 undecided) ((coding-category-utf-8 . utf-8) (coding-category-iso-7 . iso-2022-7bit) (coding-category-charset . iso-latin-1) (coding-category-iso-7-else . iso-2022-7bit-lock) (coding-category-iso-8-else . iso-2022-8bit-ss2) (coding-category-emacs-mule . emacs-mule) (coding-category-raw-text . raw-text) (coding-category-iso-7-tight . no-conversion) (coding-category-iso-8-1 . no-conversion) (coding-category-iso-8-2 . no-conversion) (coding-category-utf-8-auto . no-conversion) (coding-category-utf-8-sig . no-conversion) (coding-category-utf-16-auto . no-conversion) (coding-category-utf-16-be . no-conversion) (coding-category-utf-16-le . no-conversion) (coding-category-utf-16-be-nosig . no-conversion) (coding-category-utf-16-le-nosig . no-conversion) (coding-category-sjis . no-conversion) (coding-category-big5 . no-conversion) (coding-category-ccl . no-conversion) (coding-category-undecided . no-conversion)) (japanese-shift-jis japanese-shift-jis japanese-shift-jis coding-category-sjis))"#,
        "detection",
    );
}

#[test]
fn iso_2022_and_raw_text_encoders_follow_coding_c() {
    // coding.c encode_coding_iso_2022 over the attributes mule.el derives
    // from :designation/:flags (designation escapes, single shifts,
    // reset at eol/cntl, the default char for the unencodable, and a
    // `charset' text property as the preferred charset), and
    // encode_coding_raw_text writing a multibyte source in its internal
    // spelling (undecided encodes the same way).
    let program = r#"
(let ((text (concat "a" (string #xe9 #x20ac #x3042) "z")))
  (list (mapcar (lambda (cs) (condition-case e (append (encode-coding-string text cs) nil) (error e)))
                '(iso-2022-jp iso-2022-7bit euc-kr iso-2022-kr undecided raw-text iso-2022-7bit-dos iso-latin-1 euc-jp))
        last-coding-system-used
        (append (encode-coding-string (string-to-unibyte "a\351") 'raw-text) nil)
        (append (encode-coding-string (string #xe9) 'undecided) nil)
        (multibyte-string-p (encode-coding-string (string #xe9) 'undecided))
        (append (encode-coding-string (propertize (string #x3042) 'charset 'japanese-jisx0208) 'iso-2022-7bit) nil)
        (append (encode-coding-string (concat (string #x3042) "\n" (string #x3042)) 'iso-2022-jp) nil)
        (unencodable-char-position 0 5 'raw-text nil text)
        (unencodable-char-position 0 5 'iso-2022-jp 5 text)))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        r#"(((97 32 32 27 36 66 36 34 27 40 66 122) (97 27 44 65 105 27 44 70 36 27 36 65 36 34 27 40 66 122) (97 32 162 230 170 162 122) (97 32 27 36 40 67 34 102 42 34 27 40 66 122) (97 195 169 226 130 172 227 129 130 122) (97 195 169 226 130 172 227 129 130 122) (97 27 44 65 105 27 44 70 36 27 36 65 36 34 27 40 66 122) (97 233 32 32 122) (97 143 171 177 32 164 162 122)) euc-jp (97 233) (195 169) nil (27 36 66 36 34 27 40 66) (27 36 66 36 34 27 40 66 10 27 36 66 36 34 27 40 66) nil (1 2))"#,
        "iso-2022 encoding",
    );
}

#[test]
fn iso_2022_decoder_annotates_charsets_and_detection_reaches_regions_and_files() {
    // coding.c decode_coding_iso_2022 with produce_charset's `charset'
    // text properties; code_convert_string's ASCII fast path leaves a
    // 7-bit ISO-2022 string undecoded under `undecided' while a region
    // or a file goes through detect_coding and decodes.
    let program = r#"
(let* ((s (unibyte-string #x1b #x24 #x42 #x24 #x33 #x24 #x73 #x1b #x28 #x42 ?a))
       (d (decode-coding-string s 'iso-2022-7bit))
       (f (make-temp-file "emaxx-iso2022")))
  (unwind-protect
      (list (append d nil) (text-properties-at 0 d) (next-single-property-change 0 'charset d)
            last-coding-system-used
            (decode-coding-string s 'undecided)
            last-coding-system-used
            (with-temp-buffer (set-buffer-multibyte nil) (insert s)
                              (decode-coding-region (point-min) (point-max) 'undecided)
                              (list (append (buffer-string) nil) (text-properties-at 1) last-coding-system-used))
            (progn (with-temp-file f (set-buffer-multibyte nil) (insert s "\n"))
                   (with-temp-buffer (insert-file-contents f)
                                     (list (append (buffer-string) nil) (text-properties-at 1)
                                           buffer-file-coding-system last-coding-system-used)))
            (let ((x (decode-coding-string "\033$B$\"\r\n\033(B" 'iso-2022-jp)))
              (list (append x nil) (text-properties-at 0 x) last-coding-system-used))
            (append (decode-coding-string "\033$)C\016!!\017a" 'iso-2022-kr) nil)
            (append (decode-coding-string "\033$B$\"\033(B\016\033(J\\" 'iso-2022-jp) nil)
            (append (decode-coding-string "\216\261\217\260\241\244\242" 'euc-jp) nil))
    (delete-file f)))"#;
    assert_oracle_contract_matches_interpreter(
        program,
        r#"((12371 12435 97) (charset japanese-jisx0208) nil iso-2022-7bit "$B$3$s(Ba" undecided ((227 129 147 227 130 147 97) (charset japanese-jisx0208) iso-2022-7bit) ((12371 12435 97 10) (charset japanese-jisx0208) iso-2022-7bit-unix iso-2022-7bit-unix) ((12354 10) (charset japanese-jisx0208) iso-2022-jp-dos) (12288 97) (12354 14 165) (65393 19970 12354))"#,
        "iso-2022 decoding",
    );
}

#[test]
fn charset_decoders_annotate_like_produce_charset_and_emacs_mule_encodes_like_coding_c() {
    // coding.c decode_coding_sjis/_big5/_euc_jp/_emacs_mule/_charset all
    // ADD_CHARSET_DATA, so produce_charset gives every decoded string,
    // region (a multibyte source, its raw bytes as the decoder's input) and
    // file its `charset' text properties, the single-byte charset codings
    // (koi8-r, cp1252) included.  encode_coding_emacs_mule writes
    // EMACS_MULE_LEADING_CODES of the first Vemacs_mule_charset_list
    // charset that encodes the character, never steered by a `charset'
    // property (no CODING_ANNOTATE_CHARSET_MASK outside ISO-2022), and
    // the default char (a space) for the unencodable.
    let program = r##"
(let ((f (make-temp-file "emaxx-charset"))
	    (dec (lambda (bytes cs)
		   (let ((s (decode-coding-string bytes cs)) (r nil) (i 0))
		     (while (< i (length s))
		       (push (get-text-property i 'charset s) r)
		       (setq i (1+ i)))
		     (list (append s nil) (nreverse r))))))
	(unwind-protect
	    (list (funcall dec (unibyte-string ?a #x82 #xa0 ?b #xb1 ?c) 'sjis)
		  (funcall dec (unibyte-string #x82) 'sjis)
		  (funcall dec (unibyte-string ?x #xa4 #xa4 ?y) 'big5)
		  (funcall dec (unibyte-string ?a #xa4 #xa2 #x8e #xb1 #x8f #xb0 #xa1) 'euc-jp)
		  (funcall dec (unibyte-string #xc4 #xe3) 'chinese-iso-8bit)
		  (funcall dec (unibyte-string ?a #x81 #xa9 ?b #x81 #xaa) 'emacs-mule)
		  (funcall dec (unibyte-string #x92 #xa4 #xa2) 'emacs-mule)
		  (funcall dec (unibyte-string ?a #xe9) 'iso-latin-1)
		  (funcall dec (unibyte-string ?a #xe9) 'iso-latin-2)
		  (funcall dec (unibyte-string ?a #xc1 #xff #x80) 'koi8-r)
		  (funcall dec (unibyte-string ?a #x81 #x8d #x9d) 'cp1252)
		  (funcall dec (unibyte-string #xe9) 'undecided)
		  (with-temp-buffer
		    (insert (unibyte-string ?a #x82 #xa0 ?b))
		    (decode-coding-region (point-min) (point-max) 'sjis)
		    (list (append (buffer-string) nil)
			  (mapcar (lambda (i) (get-text-property i 'charset)) '(1 2 3))))
		  (with-temp-buffer
		    (insert (unibyte-string ?a #xe9))
		    (decode-coding-region (point-min) (point-max) 'iso-latin-2)
		    (list (append (buffer-string) nil)
			  (mapcar (lambda (i) (get-text-property i 'charset)) '(1 2))))
		  (progn (with-temp-file f (set-buffer-multibyte nil)
					 (insert (unibyte-string ?a #x82 #xa0 ?b #xa4 #xa4)))
			 (mapcar (lambda (cs)
				   (with-temp-buffer
				     (let ((coding-system-for-read cs)) (insert-file-contents f))
				     (list (append (buffer-string) nil)
					   (mapcar (lambda (i) (get-text-property i 'charset)) '(1 2 3 4 5 6)))))
				 '(sjis big5 euc-jp iso-latin-1)))
		  (append (encode-coding-string (string ?a #xe9 #x3042 #x20ac #xac00 #xff61 #x1f600) 'emacs-mule) nil)
		  (append (encode-coding-string (decode-coding-string (unibyte-string #x81 #xa9 #x92 #xa4 #xa2) 'emacs-mule) 'emacs-mule) nil)
		  (append (encode-coding-string (string #x3fffe9) 'emacs-mule) nil)
		  (append (encode-coding-string (decode-coding-string (unibyte-string #x82 #xa0) 'sjis) 'emacs-mule) nil)
		  (append (decode-coding-string (encode-coding-string (string ?a #xe9 #x3042 #x20ac #xac00 #xff61) 'emacs-mule) 'emacs-mule) nil)
		  (append (encode-coding-string (decode-coding-string (unibyte-string #x8e #xb1) 'euc-jp) 'iso-2022-jp) nil))
	  (delete-file f)))"##;
    assert_oracle_contract_matches_interpreter(
        program,
        r##"(((97 12354 98 65393 99) (nil japanese-jisx0208 japanese-jisx0208 katakana-jisx0201 katakana-jisx0201)) ((4194178) (nil)) ((120 20013 121) (nil big5 big5)) ((97 12354 65393 19970) (nil japanese-jisx0208 katakana-jisx0201 japanese-jisx0212)) ((20320) (chinese-gb2312)) ((97 169 98 170) (nil latin-iso8859-1 nil latin-iso8859-1)) ((12354) (japanese-jisx0208)) ((97 233) (iso-8859-1 iso-8859-1)) ((97 233) (iso-8859-2 iso-8859-2)) ((97 1072 1066 9472) (koi8-r koi8-r koi8-r koi8-r)) ((97 4194177 4194189 4194205) (windows-1252 windows-1252 windows-1252 windows-1252)) ((233) (iso-8859-1)) ((97 12354 98) (nil japanese-jisx0208 japanese-jisx0208)) ((97 233) (iso-8859-2 iso-8859-2)) (((97 12354 98 65380 65380) (nil japanese-jisx0208 japanese-jisx0208 katakana-jisx0201 katakana-jisx0201 nil)) ((97 4194178 4194208 98 20013) (nil nil nil nil big5 nil)) ((97 4194178 4194208 98 12356) (nil nil nil nil japanese-jisx0208 nil)) ((97 130 160 98 164 164) (iso-8859-1 iso-8859-1 iso-8859-1 iso-8859-1 iso-8859-1 iso-8859-1))) (97 129 233 145 164 162 134 164 147 176 161 137 161 32) (129 169 145 164 162) (233) (145 164 162) (97 233 12354 8364 44032 65377) (32))"##,
        "charset decoders and emacs-mule encoder",
    );
}

#[test]
fn print_prunes_charset_properties_like_print_prune_string_charset() {
    // print.c print_prune_string_charset: `print-charset-text-property'
    // t prints the `charset' properties as they are, nil never, and
    // `default' only when some charset span holds a non-ASCII character
    // whose CHAR_CHARSET is not the span's charset (a unibyte string's
    // bytes count as Latin-1 characters there, per
    // fetch_string_char_advance); the decision is per string, and the
    // other properties survive the pruning.
    let program = r##"
(let ((sj (decode-coding-string (unibyte-string #x82 #xa0) 'sjis))
	    (l1 (decode-coding-string (unibyte-string ?a #xe9) 'iso-latin-1))
	    (l2 (decode-coding-string (unibyte-string ?a #xe9) 'iso-latin-2))
	    (ascii (propertize "abc" 'charset 'japanese-jisx0208 'face 'bold))
	    (two (concat (propertize (string #xe9) 'charset 'iso-8859-2) "x"
			 (propertize (string #xe9) 'charset 'iso-8859-1 'face 'bold)))
	    (ub (propertize (unibyte-string 233) 'charset 'eight-bit))
	    (mb (propertize (string #x3fffe9) 'charset 'eight-bit))
	    (mb2 (propertize (string #x3fffe9) 'charset 'iso-8859-1)))
	(mapcar (lambda (v)
		  (let ((print-charset-text-property v))
		    (mapcar (lambda (s) (append (prin1-to-string s) nil))
			    (list sj l1 l2 ascii two ub mb mb2 (list sj) (format "%S" sj)))))
		'(t nil default)))"##;
    assert_oracle_contract_matches_interpreter(
        program,
        r##"(((35 40 34 12354 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 106 97 112 97 110 101 115 101 45 106 105 115 120 48 50 48 56 41 41) (35 40 34 97 233 34 32 48 32 50 32 40 99 104 97 114 115 101 116 32 105 115 111 45 56 56 53 57 45 49 41 41) (35 40 34 97 233 34 32 48 32 50 32 40 99 104 97 114 115 101 116 32 105 115 111 45 56 56 53 57 45 50 41 41) (35 40 34 97 98 99 34 32 48 32 51 32 40 99 104 97 114 115 101 116 32 106 97 112 97 110 101 115 101 45 106 105 115 120 48 50 48 56 32 102 97 99 101 32 98 111 108 100 41 41) (35 40 34 233 120 233 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 105 115 111 45 56 56 53 57 45 50 41 32 50 32 51 32 40 102 97 99 101 32 98 111 108 100 32 99 104 97 114 115 101 116 32 105 115 111 45 56 56 53 57 45 49 41 41) (35 40 34 92 51 53 49 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 101 105 103 104 116 45 98 105 116 41 41) (35 40 34 92 51 53 49 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 101 105 103 104 116 45 98 105 116 41 41) (35 40 34 92 51 53 49 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 105 115 111 45 56 56 53 57 45 49 41 41) (40 35 40 34 12354 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 106 97 112 97 110 101 115 101 45 106 105 115 120 48 50 48 56 41 41 41) (34 35 40 92 34 12354 92 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 106 97 112 97 110 101 115 101 45 106 105 115 120 48 50 48 56 41 41 34)) ((34 12354 34) (34 97 233 34) (34 97 233 34) (35 40 34 97 98 99 34 32 48 32 51 32 40 102 97 99 101 32 98 111 108 100 41 41) (35 40 34 233 120 233 34 32 50 32 51 32 40 102 97 99 101 32 98 111 108 100 41 41) (34 92 51 53 49 34) (34 92 51 53 49 34) (34 92 51 53 49 34) (40 34 12354 34 41) (34 92 34 12354 92 34 34)) ((35 40 34 12354 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 106 97 112 97 110 101 115 101 45 106 105 115 120 48 50 48 56 41 41) (35 40 34 97 233 34 32 48 32 50 32 40 99 104 97 114 115 101 116 32 105 115 111 45 56 56 53 57 45 49 41 41) (35 40 34 97 233 34 32 48 32 50 32 40 99 104 97 114 115 101 116 32 105 115 111 45 56 56 53 57 45 50 41 41) (35 40 34 97 98 99 34 32 48 32 51 32 40 102 97 99 101 32 98 111 108 100 41 41) (35 40 34 233 120 233 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 105 115 111 45 56 56 53 57 45 50 41 32 50 32 51 32 40 102 97 99 101 32 98 111 108 100 32 99 104 97 114 115 101 116 32 105 115 111 45 56 56 53 57 45 49 41 41) (35 40 34 92 51 53 49 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 101 105 103 104 116 45 98 105 116 41 41) (34 92 51 53 49 34) (35 40 34 92 51 53 49 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 105 115 111 45 56 56 53 57 45 49 41 41) (40 35 40 34 12354 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 106 97 112 97 110 101 115 101 45 106 105 115 120 48 50 48 56 41 41 41) (34 35 40 92 34 12354 92 34 32 48 32 49 32 40 99 104 97 114 115 101 116 32 106 97 112 97 110 101 115 101 45 106 105 115 120 48 50 48 56 41 41 34)))"##,
        "charset property printing",
    );
}

#[test]
fn char_charset_family_follows_charset_c() {
    // charset.c: char_charset walks the priority order and answers
    // `unicode' once Vcharset_non_preferred_head (the part of the order
    // `set-charset-priority' did not move) is reached for a Unicode
    // character, `eight-bit' for a raw byte; a coding-system RESTRICTION
    // reads coding_system_charset_list (Vemacs_mule_charset_list for
    // emacs-mule); `split-char' is the code's bytes per dimension;
    // `charset-after' and `find-charset-string'/`-region' report raw bytes
    // as `eight-bit'.  ENCODE_CHAR/DECODE_CHAR honour a charset's
    // `:min-code'/`:max-code' and the code-space index of an offset
    // charset, a map file's `FROM-TO C' lines advance by index, the
    // `unicode' and `emacs' code spaces end at MAX_UNICODE_CHAR and
    // MAX_5_BYTE_CHAR, and CHECK_CHARSET_GET_CHARSET signals for an
    // unknown charset.
    let program = r##"
(list (mapcar #'char-charset (list ?a #xe9 #x3042 #x20ac #x110000 #x3fffe9 #x3fff7f #x200000 #x1f600))
	    (char-charset #x3042 '(japanese-jisx0208 ascii))
	    (char-charset #xe9 '(japanese-jisx0208 ascii))
	    (char-charset ?a '(japanese-jisx0208))
	    (char-charset #x3042 'iso-2022-jp) (char-charset #xe9 'iso-2022-jp)
	    (char-charset #x3042 'utf-8) (char-charset #xe9 'iso-latin-2)
	    (mapcar #'split-char (list ?a #xe9 #x3042 #x110000 #x3fffe9 #x3fff7f #xac00))
	    (with-temp-buffer (insert (string #x3fffe9 #x3042 ?\s ?a))
			      (mapcar #'charset-after '(1 2 3 4 10)))
	    (find-charset-string (string ?a #x3fffe9 #x20ac #x3042))
	    (find-charset-string (unibyte-string ?a 233))
	    (find-charset-string "")
	    (find-charset-string (string ?a #xe9 #x3042) '(japanese-jisx0208 ascii iso-8859-1))
	    (with-temp-buffer (insert (string ?a #x3fffe9 #x3042)) (find-charset-region (point-min) (point-max)))
	    (with-temp-buffer (set-buffer-multibyte nil) (insert (unibyte-string ?a 233)) (find-charset-region (point-min) (point-max)))
	    (progn (set-charset-priority 'japanese-jisx0208)
		   (list (seq-take (charset-priority-list) 3)
			 (mapcar #'char-charset (list ?a #xe9 #x3042 #x20ac #x110000 #x3fffe9))
			 (mapcar #'split-char (list #xe9 #x3042 #x20ac))
			 (find-charset-string (string ?a #x3fffe9 #x20ac #x3042))))
	    (progn (set-charset-priority 'ascii)
		   (list (seq-take (charset-priority-list) 3) (char-charset #xe9) (char-charset #x3042)))
	    (progn (set-charset-priority 'ascii 'iso-8859-1)
		   (list (char-charset #xe9) (char-charset #x3042)))
	    (progn (set-charset-priority 'iso-8859-2)
		   (list (seq-take (charset-priority-list) 3) (char-charset #xe9) (char-charset #x3042) (char-charset #x20ac)))
	    (progn (set-charset-priority 'unicode)
		   (list (char-charset #xe9) (char-charset #x3042) (char-charset #x110000) (split-char #x3042)))
	    (list (decode-char 'unicode #x3fff7f) (decode-char 'emacs #x3fff7f) (decode-char 'emacs #x3fff80)
		  (decode-char 'eight-bit #xe9) (decode-char 'eight-bit #x7f)
		  (encode-char #x3fffe9 'unicode) (encode-char #x3fffe9 'emacs) (encode-char #x3fff7f 'emacs)
		  (encode-char #x3fff7f 'unicode) (encode-char #x3fffe9 'eight-bit) (encode-char #x3fffe9 'iso-8859-1)
		  (encode-char #x3fffe9 'japanese-jisx0208) (encode-char #x3fffe9 'latin-iso8859-1))
	    (list (encode-char #x3fff7f 'gb18030) (encode-char #x10ffff 'gb18030) (encode-char #x110000 'gb18030)
		  (encode-char #x80 'gb18030-4-byte-bmp)
		  (decode-char 'gb18030-4-byte-bmp #x81308130) (decode-char 'gb18030-4-byte-bmp #x81308435)
		  (decode-char 'gb18030-4-byte-bmp #x81308436)
		  (decode-char 'gb18030-4-byte-smp #x90308130) (decode-char 'gb18030-4-byte-smp #xE3329A35)
		  (decode-char 'gb18030-4-byte-smp #xE3329A36)
		  (decode-char 'gb18030-4-byte-ext-1 #x8431A530) (encode-char #x200000 'gb18030-4-byte-ext-1)
		  (decode-char 'gb18030 #x8431A530) (encode-char #x200000 'gb18030)
		  (decode-char 'gb18030-2-byte #x8140) (encode-char #x4E02 'gb18030-2-byte)
		  (decode-char 'japanese-jisx0208 #x3021) (encode-char #x4E9C 'japanese-jisx0208)
		  (decode-char 'big5 #xA440) (encode-char #x4E00 'big5)
		  (decode-char 'chinese-gb2312 #x3021) (decode-char 'korean-ksc5601 #x3021)
		  (decode-char 'cp932-2-byte #x8140) (decode-char 'japanese-jisx0213-1 #x2121)
		  (condition-case e (decode-char 'jisx0213-1 #x2121) (error e))
		  (condition-case e (encode-char ?a 'jisx0213-1) (error e))))"##;
    assert_oracle_contract_matches_interpreter(
        program,
        r##"((ascii unicode unicode unicode chinese-gb2312 eight-bit emacs gb18030 unicode) japanese-jisx0208 nil nil japanese-jisx0208 nil unicode iso-8859-2 ((ascii 97) (unicode 0 0 233) (unicode 0 48 66) (chinese-gb2312 33 33) (eight-bit 233) (emacs 63 255 127) (unicode 0 172 0)) (eight-bit unicode ascii ascii nil) (ascii unicode eight-bit) (ascii eight-bit) nil (ascii unicode) (ascii unicode eight-bit) (ascii eight-bit) ((japanese-jisx0208 ascii iso-8859-1) (ascii unicode japanese-jisx0208 unicode chinese-gb2312 eight-bit) ((unicode 0 0 233) (japanese-jisx0208 36 34) (unicode 0 32 172)) (ascii unicode eight-bit japanese-jisx0208)) ((ascii japanese-jisx0208 iso-8859-1) unicode unicode) (iso-8859-1 unicode) ((iso-8859-2 ascii iso-8859-1) iso-8859-2 unicode unicode) (unicode unicode chinese-gb2312 (unicode 0 48 66)) (nil 4194175 nil 4194281 nil nil nil 4194175 nil 233 nil nil nil) (nil 3811744309 nil 2167439664 128 163 165 65536 1114111 nil 2097152 2217846064 65530 2217846064 19970 33088 20124 12321 19968 42048 21834 44032 12288 12288 (wrong-type-argument charsetp jisx0213-1) (wrong-type-argument charsetp jisx0213-1)))"##,
        "char-charset family",
    );
}

#[test]
fn text_property_copies_follow_add_text_properties_order() {
    // textprop.c add_properties conses each new property onto the head
    // of the interval's plist, so every copy that goes through
    // add_text_properties (Fsubstring's copy_text_properties, `concat' and
    // `mapconcat' via concat_to_string, styled_format's argument and
    // format-string intervals) reverses a span's pairs, where
    // copy_intervals (`copy-sequence', buffer insertion) keeps them;
    // editfns.c styled_format returns a string argument itself for a
    // property-less "%s" format.  The printed forms swap their quotes
    // for apostrophes so the contract's own rendering stays unambiguous.
    let program = r##"
(let ((s (propertize "x" 'a 1 'b 2))
	    (p (lambda (x) (string-replace "\"" "'" (prin1-to-string x)))))
	(list (funcall p (concat s))
	      (funcall p (substring (propertize "xy" 'a 1 'b 2) 1))
	      (funcall p (mapconcat #'identity (list s (propertize "y" 'c 3 'd 4)) (propertize "-" 'e 5 'f 6)))
	      (funcall p (copy-sequence (propertize "xy" 'a 1 'b 2)))
	      (funcall p (string-trim (propertize " x " 'a 1 'b 2)))
	      (funcall p (concat (propertize "x" 'a 1) (propertize "y" 'a 1 'b 2)))
	      (funcall p (substring (concat (propertize "xy" 'a 1 'b 2 'c 3)) 1))
	      (with-temp-buffer (insert s) (funcall p (buffer-string)))
	      (let ((c (copy-sequence "x"))) (put-text-property 0 1 'a 1 c) (put-text-property 0 1 'b 2 c) (funcall p c))
	      (let ((c (copy-sequence "x"))) (add-text-properties 0 1 '(a 1 b 2) c) (funcall p c))
	      (let ((c (copy-sequence "x"))) (set-text-properties 0 1 '(a 1 b 2) c) (funcall p c))
	      (eq s (format "%s" s)) (eq s (format "%s" s 2))
	      (funcall p (format "%s" s))
	      (funcall p (format "%s " s))
	      (funcall p (format "%5s" s))
	      (funcall p (format "%d%s" 1 s))
	      (funcall p (format (propertize "%s" 'q 9 'r 8) "x"))
	      (funcall p (format (propertize "%s" 'q 9) s))
	      (funcall p (format "%s-%s" s (propertize "y" 'c 3 'd 4)))
	      (funcall p (format "%s" (concat s (propertize "y" 'c 3))))))"##;
    assert_oracle_contract_matches_interpreter(
        program,
        r##"("#('x' 0 1 (b 2 a 1))" "#('y' 0 1 (b 2 a 1))" "#('x-y' 0 1 (b 2 a 1) 1 2 (f 6 e 5) 2 3 (d 4 c 3))" "#('xy' 0 2 (a 1 b 2))" "#('x' 0 1 (a 1 b 2))" "#('xy' 0 1 (a 1) 1 2 (b 2 a 1))" "#('y' 0 1 (a 1 b 2 c 3))" "#('x' 0 1 (a 1 b 2))" "#('x' 0 1 (b 2 a 1))" "#('x' 0 1 (b 2 a 1))" "#('x' 0 1 (a 1 b 2))" t t "#('x' 0 1 (a 1 b 2))" "#('x ' 0 1 (b 2 a 1))" "#('    x' 4 5 (b 2 a 1))" "#('1x' 1 2 (b 2 a 1))" "#('x' 0 1 (r 8 q 9))" "#('x' 0 1 (b 2 a 1 q 9))" "#('x-y' 0 1 (b 2 a 1) 2 3 (d 4 c 3))" "#('xy' 0 1 (b 2 a 1) 1 2 (c 3))")"##,
        "text property copy order",
    );
}

#[test]
fn default_file_modes_is_the_process_umask_and_temp_files_are_private() {
    // fileio.c: `set-default-file-modes' sets the process's creation mask
    // (~MODE & 0777), so `make-directory' (mkdir 0777), `write-region'
    // (0666) and every subprocess see it, and `default-file-modes' is
    // its complement; gen_tempname makes `make-temp-file' entries 0600
    // and directories 0700 regardless of the mask's generosity.  A
    // temporary directory that came out 0755 made server.el's
    // server-ensure-safe-dir refuse it ("accessible by others").  The
    // startup value is the inherited umask's complement, so it is
    // checked against `sh -c umask' rather than a literal.
    let program = r##"
(let* ((d (make-temp-file "emaxx-modes" t))
       (f (make-temp-file "emaxx-modes"))
       (sub (expand-file-name "sub" d))
       (f2 (expand-file-name "f2" d))
       (u (expand-file-name "u" d))
       (orig (default-file-modes)))
  (unwind-protect
      (list (format "%o" (file-modes d)) (format "%o" (file-modes f))
            (progn (with-file-modes #o700 (make-directory sub)) (format "%o" (file-modes sub)))
            (progn (with-file-modes #o640 (write-region "" nil f2)) (format "%o" (file-modes f2)))
            (progn (with-file-modes #o600 (call-process "sh" nil nil nil "-c" (concat "umask > " u)))
                   (with-temp-buffer (insert-file-contents u) (string-trim (buffer-string))))
            (progn (with-file-modes #o777 (make-directory (expand-file-name "sub2" d)))
                   (format "%o" (file-modes (expand-file-name "sub2" d))))
            (progn (call-process "sh" nil nil nil "-c" (concat "umask > " u))
                   (with-temp-buffer (insert-file-contents u)
                                     (= (default-file-modes)
                                        (logand (lognot (string-to-number (string-trim (buffer-string)) 8)) #o777))))
            (progn (set-default-file-modes #o600) (prog1 (format "%o" (default-file-modes)) (set-default-file-modes orig)))
            (= (default-file-modes) orig))
    (delete-directory d t) (delete-file f)))"##;
    assert_oracle_contract_matches_interpreter(
        program,
        r##"("700" "600" "700" "640" "0177" "777" t "600" t)"##,
        "default file modes",
    );
}
