use super::*;

#[test]
fn buffer_positions_accept_integer_values_independent_of_internal_width() {
    let interp = Interpreter::new();
    assert_eq!(
        position_from_value(&interp, &Value::Integer(73)).expect("fixnum position"),
        73
    );
    assert_eq!(
        position_from_value(&interp, &Value::big_integer(BigInt::from(73)))
            .expect("small bignum representation"),
        73
    );
    assert_eq!(
        position_from_value(&interp, &Value::Integer(-30)).expect("negative fixnum position"),
        0
    );
}

#[test]
fn file_modes_reads_permissions_and_reports_arity() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let path = std::env::temp_dir().join(format!(
        "emaxx-file-modes-{}-{}",
        std::process::id(),
        nondeterministic_random_seed()
    ));
    std::fs::write(&path, "mode").expect("write temp file");

    call(
        &mut interp,
        "set-file-modes",
        &[
            Value::String(path.display().to_string().into()),
            Value::Integer(0o600),
        ],
        &mut env,
    )
    .expect("set file modes");
    assert_eq!(
        call(
            &mut interp,
            "file-modes",
            &[Value::String(path.display().to_string().into())],
            &mut env,
        )
        .expect("read file modes"),
        Value::Integer(0o600)
    );
    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Symbol("file-modes".into())],
            &mut env,
        )
        .expect("file-modes arity"),
        Value::cons(Value::Integer(1), Value::Integer(2))
    );
    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Symbol("set-file-modes".into())],
            &mut env,
        )
        .expect("set-file-modes arity"),
        Value::cons(Value::Integer(2), Value::Integer(3))
    );
    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Symbol("set-file-times".into())],
            &mut env,
        )
        .expect("set-file-times arity"),
        Value::cons(Value::Integer(1), Value::Integer(3))
    );
    call(
        &mut interp,
        "set-file-times",
        &[
            Value::String(path.display().to_string().into()),
            Value::Integer(1_700_000_000),
        ],
        &mut env,
    )
    .expect("set file times");
    assert_eq!(
        std::fs::metadata(&path)
            .expect("metadata")
            .modified()
            .expect("modified time")
            .duration_since(UNIX_EPOCH)
            .expect("duration")
            .as_secs(),
        1_700_000_000
    );

    std::fs::remove_file(&path).expect("cleanup temp file");
}

#[test]
fn directory_files_and_attributes_reports_entries_and_arity() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let directory = std::env::temp_dir().join(format!(
        "emaxx-directory-attributes-{}-{}",
        std::process::id(),
        nondeterministic_random_seed()
    ));
    std::fs::create_dir_all(&directory).expect("create temp dir");
    std::fs::write(directory.join("sample.txt"), "sample").expect("write sample");

    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Symbol("directory-files".into())],
            &mut env,
        )
        .expect("directory-files arity"),
        Value::cons(Value::Integer(1), Value::Integer(5))
    );
    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Symbol("directory-files-and-attributes".into())],
            &mut env,
        )
        .expect("directory-files-and-attributes arity"),
        Value::cons(Value::Integer(1), Value::Integer(6))
    );
    let entries = call(
        &mut interp,
        "directory-files-and-attributes",
        &[
            Value::String(directory.display().to_string().into()),
            Value::Nil,
            Value::String("sample".into()),
        ],
        &mut env,
    )
    .expect("directory entries with attributes")
    .to_vec()
    .expect("entry list");
    assert_eq!(entries.len(), 1);
    let (name, attributes) = entries[0].cons_values().expect("entry cons");
    assert_eq!(name, Value::String("sample.txt".into()));
    // `file-attribute-size' lives in GNU files.el; the bare runtime has no
    // preloaded Lisp, so index the C-owned attribute list directly.
    assert_eq!(
        call(
            &mut interp,
            "nth",
            &[Value::Integer(7), attributes],
            &mut env,
        )
        .expect("attribute size"),
        Value::Integer(6)
    );

    std::fs::remove_dir_all(&directory).expect("cleanup temp dir");
}

#[test]
fn forward_line_treats_nil_as_default_step() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.buffer = crate::buffer::Buffer::from_text("*lines*", "a\nb");

    assert_eq!(
        call(&mut interp, "forward-line", &[Value::Nil], &mut env).expect("forward-line with nil"),
        Value::Integer(0)
    );
    assert_eq!(interp.buffer.point(), 3);
}

#[test]
fn forward_char_treats_nil_as_default_step() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.buffer = crate::buffer::Buffer::from_text("*chars*", "ab");

    assert_eq!(
        call(&mut interp, "forward-char", &[Value::Nil], &mut env).expect("forward-char with nil"),
        Value::Nil
    );
    assert_eq!(interp.buffer.point(), 2);
}

#[test]
fn line_number_at_pos_treats_nil_as_point_and_checks_bounds() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.buffer = crate::buffer::Buffer::from_text("*lines*", "\n\n\n\n\n\n\n\n\n\n");
    interp.buffer.goto_char(interp.buffer.point_max());

    assert_eq!(
        call(&mut interp, "line-number-at-pos", &[Value::Nil], &mut env,)
            .expect("line-number-at-pos nil"),
        Value::Integer(11)
    );

    assert!(matches!(
        call(
            &mut interp,
            "line-number-at-pos",
            &[Value::Integer(-1)],
            &mut env,
        ),
        Err(LispError::SignalValue(value))
            if matches!(value.to_vec().ok().as_deref(), Some([
                Value::Symbol(name),
                Value::Integer(-1),
                Value::Integer(1),
                Value::Integer(11),
            ]) if name == "args-out-of-range")
    ));
    assert!(matches!(
        call(
            &mut interp,
            "line-number-at-pos",
            &[Value::Integer(100)],
            &mut env,
        ),
        Err(LispError::SignalValue(value))
            if matches!(value.to_vec().ok().as_deref(), Some([
                Value::Symbol(name),
                Value::Integer(100),
                Value::Integer(1),
                Value::Integer(11),
            ]) if name == "args-out-of-range")
    ));
}

#[test]
fn line_number_at_pos_counts_from_the_accessible_region_unless_absolute() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.buffer = crate::buffer::Buffer::from_text("*lines*", "a\nb\nc\nd\ne\nf");
    interp.buffer.narrow_to_region(3, 10);

    assert_eq!(
        Value::list([
            call(
                &mut interp,
                "line-number-at-pos",
                &[Value::Integer(10)],
                &mut env,
            )
            .expect("relative narrowed line"),
            call(
                &mut interp,
                "line-number-at-pos",
                &[Value::Integer(10), Value::T],
                &mut env,
            )
            .expect("absolute narrowed line"),
            call(
                &mut interp,
                "line-number-at-pos",
                &[Value::Integer(1)],
                &mut env,
            )
            .expect("position before accessible region"),
            call(
                &mut interp,
                "line-number-at-pos",
                &[Value::Integer(11)],
                &mut env,
            )
            .expect("position after accessible region"),
        ]),
        Value::list([
            Value::Integer(4),
            Value::Integer(5),
            Value::Integer(1),
            Value::Integer(4),
        ])
    );
}

#[test]
fn concat_matches_upstream_sequence_and_multibyte_cases() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (list
               (equal (concat) "")
               (equal (concat nil) "")
               (equal (concat []) "")
               (equal (concat [97 98]) "ab")
               (equal (concat '(97 98)) "ab")
               (equal (concat "ab" '(99 100) nil [101 102] "gh") "abcdefgh")
               (equal (concat "AB" (string-to-multibyte "\200") "cd")
                      (string-to-multibyte "AB\200cd"))
               (equal (concat "ab" '(#xe5) [255] "cd") "ab\u00e5\u00ffcd")
               (equal (concat '(#x3fffff) [#x3fff80] "xy") "\377\200xy")
               (equal (concat '(#x3fffff) [#x3fff80] "xy\u00a7") "\377\200xy\u00a7")
               (equal-including-properties
                (concat #("abc" 0 3 (a 1)) #("de" 0 2 (a 1)))
                #("abcde" 0 5 (a 1)))
               (equal-including-properties
                (concat #("abc" 0 3 (a 1)) "\u00a7\u00fc" #("\u00e7\u00e5" 0 2 (b 2)))
                #("abc\u00a7\u00fc\u00e7\u00e5" 0 3 (a 1) 5 7 (b 2)))
               (condition-case nil
                   (progn (concat "a" '(98 . 99)) nil)
                 (error t))))
            "#,
    )
    .read_all()
    .expect("concat compatibility test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("concat compatibility forms should evaluate");
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
        ])
    );
}

#[test]
fn vconcat_and_append_preserve_multibyte_raw_byte8_elements() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (list
               (equal (vconcat [1 2 3] nil '(4 5) "AB" "\u00e5"
                               "\377" (string-to-multibyte "\377")
                               (bool-vector t nil nil t nil))
                      [1 2 3 4 5 65 66 #xe5 255 #x3fffff t nil nil t nil])
               (equal (append [1 2 3] nil '(4 5) "AB" "\u00e5"
                              "\377" (string-to-multibyte "\377")
                              (bool-vector t nil nil t nil)
                              '(9 10))
                      '(1 2 3 4 5 65 66 #xe5 255 #x3fffff t nil nil t nil 9 10))))
            "#,
    )
    .read_all()
    .expect("vconcat/append raw-byte8 test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("vconcat/append raw-byte8 forms should evaluate");
    assert_eq!(result, Value::list([Value::T, Value::T]));
}

#[test]
fn string_to_unibyte_roundtrips_raw_bytes_and_rejects_multibyte_chars() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (let* ((u "\200\377")
                     (m (string-to-multibyte u))
                     (uu (string-to-unibyte m)))
                (list
                 (null (multibyte-string-p u))
                 (multibyte-string-p m)
                 (null (multibyte-string-p uu))
                 (equal u uu)
                 (equal (append m nil) '(#x3fff80 #x3fffff))
                 (condition-case nil
                     (progn (string-to-unibyte "\u00e5") nil)
                   (error t))
                 (condition-case nil
                     (progn (string-to-unibyte "ABC\u2200BC") nil)
                   (error t)))))
            "#,
    )
    .read_all()
    .expect("string-to-unibyte test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("string-to-unibyte forms should evaluate");
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
        ])
    );
}

#[test]
fn take_and_ntake_match_upstream_edge_cases() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (let ((list (list 'a 'b 'c)))
                (setcdr (nthcdr 2 list) (cdr list))
                (list
                 (equal (take 0 'a) nil)
                 (equal (ntake 0 'a) nil)
                 (condition-case nil
                     (progn (take 2 '(a . b)) nil)
                   (wrong-type-argument t))
                 (condition-case nil
                     (progn (ntake 2 '(a . b)) nil)
                   (wrong-type-argument t))
                 (equal (take 5 list) '(a b c b c))
                 (equal (ntake 10 list) '(a b)))))
            "#,
    )
    .read_all()
    .expect("take/ntake test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("take/ntake forms should evaluate");
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn nthcdr_handles_circular_lists_with_large_counts() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (let ((cycle (make-list 5 nil))
                    (huge (ash 1 12345)))
                (setcdr (nthcdr 4 cycle) cycle)
                (list
                 (eq (nthcdr -1 cycle) cycle)
                 (eq (nthcdr 0 cycle) cycle)
                 (eq (nthcdr 1 cycle) (cdr cycle))
                 (eq (nthcdr most-positive-fixnum cycle)
                     (nthcdr (mod most-positive-fixnum 5) cycle))
                 (eq (nthcdr huge cycle)
                     (nthcdr (mod huge 5) cycle)))))
            "#,
    )
    .read_all()
    .expect("nthcdr circular-list test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("nthcdr circular-list forms should evaluate");
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn nth_and_nthcdr_share_gnu_negative_count_and_keymap_semantics() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (let ((map (make-keymap)))
              (define-key map "x" 'sample-command)
              (list (eq (nth -2 '(first second)) 'first)
                    (eq (elt '(first second) -2) 'first)
                    (eq (nth -2 map) 'keymap)
                    (eq (nthcdr -2 map) map)
                    (char-table-p (nth 1 map))
                    ;; keymap.c stores a single character in the full
                    ;; keymap's char-table; the public list has no assoc
                    ;; pair for it (GNU: nil).
                    (car (nthcdr 2 map))))
            "#,
    )
    .read_all()
    .expect("keymap nth test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("keymap nth forms should evaluate");
    assert_eq!(
        result,
        Value::list(vec![
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::T,
            Value::Nil
        ])
    );
}

#[test]
fn nthcdr_value_reduces_large_counts_on_cycles() {
    let cycle = Value::list(vec![Value::Nil; 5]);
    nthcdr_value(&Value::Integer(4), &cycle)
        .expect("nthcdr should reach the last cons")
        .set_cdr(cycle.clone())
        .expect("last cons should become circular");

    let one_step = nthcdr_value(&Value::Integer(1), &cycle).expect("small nthcdr should work");
    let huge_fixnum = nthcdr_value(&Value::Integer(2_305_843_009_213_693_951), &cycle)
        .expect("large fixnum nthcdr should reduce by cycle length");
    let huge_bignum_value = BigInt::from(1u8) << 12345usize;
    let huge_bignum = nthcdr_value(&Value::big_integer(huge_bignum_value), &cycle)
        .expect("large bignum nthcdr should reduce by cycle length");
    let two_steps = nthcdr_value(&Value::Integer(2), &cycle).expect("small nthcdr should work");

    let cell_id = |value: &Value| {
        let (car, _) = value.cons_cells().expect("value should be a cons cell");
        car.cell_id()
    };

    assert_eq!(cell_id(&huge_fixnum), cell_id(&one_step));
    assert_eq!(cell_id(&huge_bignum), cell_id(&two_steps));
    assert_eq!(
        cell_id(
            &nthcdr_value(
                &Value::big_integer(BigInt::from(-1_000_000_000_000_i64)),
                &cycle,
            )
            .expect("negative nthcdr should return the original list"),
        ),
        cell_id(&cycle)
    );
}

#[test]
fn plist_defaults_to_eq_and_honors_equal_test_functions() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (let ((plist (list "1" "2" "a" "b")))
                (setq plist (plist-put plist (string ?a) "c"))
                (list
                 (equal plist '("1" "2" "a" "b" "a" "c"))
                 (null (plist-get plist (string ?a)))
                 (null (plist-member plist (string ?a)))))
              (let ((plist (list "1" "2" "a" "b")))
                (setq plist (plist-put plist (string ?a) "c" #'equal))
                (list
                 (equal plist '("1" "2" "a" "c"))
                 (equal (plist-get plist (string ?a) #'equal) "c")
                 (equal (plist-member plist (string ?a) #'equal) '("a" "c")))))
            "#,
    )
    .read_all()
    .expect("plist test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("plist forms should evaluate");
    assert_eq!(result, Value::list([Value::T, Value::T, Value::T]));
}

#[test]
fn plist_member_preserves_tails_and_structural_errors() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        r#"
            (let ((cycle (list 'a 1 'b 2)))
              (setcdr (nthcdr 3 cycle) cycle)
              (list
               (equal (plist-member '(a 1 b 2) 'b) '(b 2))
               (equal (plist-member '(a) 'a) '(a))
               (null (plist-get cycle 'missing))
               (eq (condition-case error
                       (plist-member cycle 'missing)
                     (error (car error)))
                   'circular-list)
               (eq (condition-case error
                       (plist-member '(a 1 . tail) 'missing)
                     (error (car error)))
                   'wrong-type-argument)
               (equal (plist-member '("a" 1) (string ?a) #'equal)
                      '("a" 1))))
            "#,
    )
    .read()
    .expect("plist structural contract should parse")
    .expect("plist structural contract should contain a form");
    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("plist structural contract should evaluate"),
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn string_distance_matches_upstream_multibyte_cases() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (list
               (= 1 (string-distance "heelo" "hello"))
               (= 6 (string-distance "ab" "ab我她" t))
               (= 3 (string-distance "ab" "a我b" t))
               (= 1 (string-distance "我" "她"))
               (= 1 (string-distance "" "x" t))))
            "#,
    )
    .read_all()
    .expect("string-distance test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("string-distance forms should evaluate");
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn sxhash_equal_matches_structured_runtime_values() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let forms = Reader::new(
        r#"
            (progn
              (list
               (= (sxhash-equal (point-marker))
                  (sxhash-equal (point-marker)))
               (= (sxhash-equal (make-bool-vector 1000 t))
                  (sxhash-equal (make-bool-vector 1000 t)))
               (= (sxhash-equal (make-char-table nil (make-string 10 ?a)))
                  (sxhash-equal (make-char-table nil (make-string 10 ?a))))
               (= (sxhash-equal (record 'a (make-string 10 ?a)))
                  (sxhash-equal (record 'a (make-string 10 ?a))))))
            "#,
    )
    .read_all()
    .expect("sxhash-equal test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("sxhash-equal forms should evaluate");
    assert_eq!(
        result,
        Value::list([Value::T, Value::T, Value::T, Value::T])
    );
}

#[test]
fn random_matches_emacs_limit_and_seed_behavior() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert!(matches!(
        call(&mut interp, "random", &[Value::Integer(0)], &mut env),
        Err(LispError::SignalValue(value))
            if matches!(value.to_vec().ok().as_deref(),
                Some([Value::Symbol(name), Value::Integer(0)]) if name == "args-out-of-range")
    ));
    assert!(matches!(
        call(&mut interp, "random", &[Value::Integer(-1)], &mut env),
        Err(LispError::SignalValue(value))
            if matches!(value.to_vec().ok().as_deref(),
                Some([Value::Symbol(name), Value::Integer(-1)]) if name == "args-out-of-range")
    ));

    let seeded_a = call(
        &mut interp,
        "random",
        &[Value::String("seed".into())],
        &mut env,
    )
    .expect("random should accept string seed");
    let seeded_b = call(
        &mut interp,
        "random",
        &[Value::String("seed".into())],
        &mut env,
    )
    .expect("random should deterministically reseed from string");
    assert_eq!(seeded_a, seeded_b);
    assert_eq!(
        call(&mut interp, "random", &[Value::Integer(1)], &mut env)
            .expect("random 1 should be bounded"),
        Value::Integer(0)
    );
}

#[test]
fn random_accepts_bignum_limits() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let limit = BigInt::from(i64::MAX) + BigInt::from(1u8);

    let value = call(
        &mut interp,
        "random",
        &[Value::BigInteger(limit.clone().into())],
        &mut env,
    )
    .expect("random should accept bignum limits");
    let numeric = integer_like_bigint(&interp, &value).expect("random should return an integer");
    assert!(numeric >= BigInt::zero());
    assert!(numeric < limit);
}

#[test]
fn length_equals_matches_sequence_lengths() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let table = interp.make_char_table(Some("fns-tests".into()), Value::Nil);

    assert_eq!(
        call(
            &mut interp,
            "length=",
            &[
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::Integer(2)
            ],
            &mut env,
        )
        .expect("length= on list"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "length=",
            &[Value::String("zip".into()), Value::Integer(2)],
            &mut env,
        )
        .expect("length= on string"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "length",
            std::slice::from_ref(&table),
            &mut env
        )
        .expect("length on char-table"),
        // fns.c Flength: a char-table's length is MAX_CHAR (GNU probe:
        // (length (make-char-table 'fns-tests)) => 4194303).
        Value::Integer(0x3f_ffff)
    );
    assert_eq!(
        call(
            &mut interp,
            "length=",
            &[table, Value::Integer(0x3f_ffff)],
            &mut env,
        )
        .expect("length= on char-table"),
        Value::T
    );
}

#[test]
fn string_multibyte_conversion_helpers_match_fns_expectations() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let ascii = call(
        &mut interp,
        "string-make-unibyte",
        &[Value::String("abc".into())],
        &mut env,
    )
    .expect("string-make-unibyte should accept ascii");
    let ascii_made = call(
        &mut interp,
        "string-make-multibyte",
        std::slice::from_ref(&ascii),
        &mut env,
    )
    .expect("string-make-multibyte should accept ascii");
    let ascii_as = call(&mut interp, "string-as-multibyte", &[ascii], &mut env)
        .expect("string-as-multibyte should accept ascii");
    assert_eq!(string_text(&ascii_made).expect("ascii text"), "abc");
    assert!(!string_like(&ascii_made).expect("ascii string").multibyte);
    assert_eq!(string_text(&ascii_as).expect("ascii text"), "abc");
    assert!(string_like(&ascii_as).expect("ascii string").multibyte);
    assert!(
        string_like(&ascii_as)
            .expect("ascii string")
            .props
            .is_empty()
    );

    let raw = call(
        &mut interp,
        "string-make-unibyte",
        &[Value::String("é".into())],
        &mut env,
    )
    .expect("string-make-unibyte should accept latin-1");
    let made = call(
        &mut interp,
        "string-make-multibyte",
        std::slice::from_ref(&raw),
        &mut env,
    )
    .expect("string-make-multibyte should accept the raw byte");
    // character.c's unibyte_char_to_multibyte: a non-ASCII byte becomes an
    // eight-bit character, not latin-1 -- the oracle answers 4194281
    // (raw #xE9) for (string-make-multibyte (string-make-unibyte "\u{e9}")).
    // The previous expectation here encoded the latin-1 shortcut this
    // batch removed.
    assert_eq!(
        string_text(&made).expect("decoded text"),
        crate::lisp::primitives::raw_byte_regex_char(0xE9).to_string()
    );
    assert!(string_like(&made).expect("decoded string").multibyte);
    let roundtripped = call(&mut interp, "string-as-unibyte", &[made], &mut env)
        .expect("string-as-unibyte should roundtrip");
    assert_eq!(
        string_text(&roundtripped).expect("roundtripped text"),
        string_text(&raw).expect("raw text")
    );
    assert!(
        !string_like(&roundtripped)
            .expect("roundtripped string")
            .multibyte
    );
}

#[test]
fn fillarray_mutates_supported_sequences() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let vector = Value::list([
        Value::symbol("vector-literal"),
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    call(
        &mut interp,
        "fillarray",
        &[vector.clone(), Value::Symbol("x".into())],
        &mut env,
    )
    .expect("fillarray should fill vectors");
    assert_eq!(
        vector_items(&vector).expect("filled vector"),
        vec![
            Value::Symbol("x".into()),
            Value::Symbol("x".into()),
            Value::Symbol("x".into())
        ]
    );

    let string = call(
        &mut interp,
        "string-make-unibyte",
        &[Value::String("aaa".into())],
        &mut env,
    )
    .expect("string-make-unibyte should build a mutable string");
    call(
        &mut interp,
        "fillarray",
        &[string.clone(), Value::Integer('b' as i64)],
        &mut env,
    )
    .expect("fillarray should fill strings");
    let filled = string_like(&string).expect("filled string");
    assert_eq!(filled.text, "bbb");
    assert!(!filled.multibyte);

    let bool_vector = call(
        &mut interp,
        "make-bool-vector",
        &[Value::Integer(4), Value::Nil],
        &mut env,
    )
    .expect("make-bool-vector should succeed");
    call(
        &mut interp,
        "fillarray",
        &[bool_vector.clone(), Value::T],
        &mut env,
    )
    .expect("fillarray should fill bool-vectors");
    assert_eq!(
        bool_vector_bits(&interp, &bool_vector).expect("filled bool-vector"),
        vec![true, true, true, true]
    );

    let char_table = interp.make_char_table(Some("fns-tests".into()), Value::Nil);
    call(
        &mut interp,
        "fillarray",
        &[char_table.clone(), Value::Symbol("z".into())],
        &mut env,
    )
    .expect("fillarray should fill char-tables");
    assert_eq!(
        call(
            &mut interp,
            "char-table-range",
            &[char_table, Value::Integer('a' as i64)],
            &mut env,
        )
        .expect("char-table-range should read the filled default"),
        Value::Symbol("z".into())
    );
}

#[test]
fn modify_syntax_entry_accepts_character_ranges() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let table = interp.make_char_table(Some("syntax-table".into()), Value::Nil);
    let range = Value::cons(Value::Integer('A' as i64), Value::Integer('Z' as i64));

    call(
        &mut interp,
        "modify-syntax-entry",
        &[range, Value::String("w".into()), table.clone()],
        &mut env,
    )
    .expect("modify-syntax-entry should accept a cons character range");

    assert_eq!(
        call(
            &mut interp,
            "char-table-range",
            &[table.clone(), Value::Integer('A' as i64)],
            &mut env,
        )
        .expect("range start should be set"),
        Value::list([Value::Integer(2)])
    );
    assert_eq!(
        call(
            &mut interp,
            "char-table-range",
            &[table, Value::Integer('M' as i64)],
            &mut env,
        )
        .expect("range middle should be set"),
        Value::list([Value::Integer(2)])
    );
}

#[test]
fn syntax_table_aref_and_range_return_encoded_entries() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let table = interp.make_char_table(Some("syntax-table".into()), Value::Nil);

    call(
        &mut interp,
        "modify-syntax-entry",
        &[
            Value::Integer('a' as i64),
            Value::String(". 1234".into()),
            table.clone(),
        ],
        &mut env,
    )
    .expect("modify syntax entry");
    assert_eq!(
        call(
            &mut interp,
            "aref",
            &[table.clone(), Value::Integer('a' as i64)],
            &mut env,
        )
        .expect("aref should expose encoded syntax descriptor"),
        Value::list([Value::Integer(983041)])
    );
    assert_eq!(
        call(
            &mut interp,
            "char-table-range",
            &[table.clone(), Value::Integer('a' as i64)],
            &mut env,
        )
        .expect("char-table-range should expose encoded syntax descriptor"),
        Value::list([Value::Integer(983041)])
    );

    call(
        &mut interp,
        "modify-syntax-entry",
        &[
            Value::Integer('(' as i64),
            Value::String("(] 1234".into()),
            table.clone(),
        ],
        &mut env,
    )
    .expect("modify matching syntax entry");
    assert_eq!(
        call(
            &mut interp,
            "aref",
            &[table, Value::Integer('(' as i64)],
            &mut env,
        )
        .expect("matching syntax descriptor"),
        Value::cons(Value::Integer(983044), Value::Integer(']' as i64))
    );
}

#[test]
fn libxml_available_p_tracks_builtin_xml_parser() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(&mut interp, "libxml-available-p", &[], &mut env)
            .expect("libxml-available-p should be callable"),
        Value::T
    );
}

#[test]
fn plist_put_appends_absent_property_in_place() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let plist = Value::list([
        Value::Symbol(":host".into()),
        Value::String("example".into()),
    ]);

    call(
        &mut interp,
        "plist-put",
        &[
            plist.clone(),
            Value::Symbol(":save-function".into()),
            Value::Symbol("save".into()),
        ],
        &mut env,
    )
    .expect("plist-put should append an absent property");

    assert_eq!(
        call(
            &mut interp,
            "plist-get",
            &[plist, Value::Symbol(":save-function".into())],
            &mut env,
        )
        .expect("appended property should be visible through original plist"),
        Value::Symbol("save".into())
    );
}

#[test]
fn reverse_and_nreverse_preserve_vector_types() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let literal_vector = Value::list([
        Value::symbol("vector-literal"),
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    assert_eq!(
        call(&mut interp, "reverse", &[literal_vector], &mut env).expect("reverse vector"),
        Value::list([
            Value::symbol("vector-literal"),
            Value::Integer(3),
            Value::Integer(2),
            Value::Integer(1),
        ])
    );

    let vector = Value::list([
        Value::symbol("vector-literal"),
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    assert_eq!(
        call(
            &mut interp,
            "nreverse",
            std::slice::from_ref(&vector),
            &mut env
        )
        .expect("nreverse vector"),
        vector
    );
    assert_eq!(
        vector_items(&vector).expect("mutated vector"),
        vec![Value::Integer(3), Value::Integer(2), Value::Integer(1)]
    );
}

#[test]
fn reverse_and_vconcat_support_bool_vectors() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    let bool_vector = call(
        &mut interp,
        "bool-vector",
        &[Value::T, Value::T, Value::Nil, Value::Nil],
        &mut env,
    )
    .expect("bool-vector should build");
    let reversed = call(
        &mut interp,
        "reverse",
        std::slice::from_ref(&bool_vector),
        &mut env,
    )
    .expect("reverse should accept bool-vectors");
    assert_eq!(
        bool_vector_bits(&interp, &reversed).expect("reversed bool-vector"),
        vec![false, false, true, true]
    );
    assert_eq!(
        call(
            &mut interp,
            "vconcat",
            std::slice::from_ref(&bool_vector),
            &mut env,
        )
        .expect("vconcat should accept bool-vectors"),
        Value::list([
            Value::symbol("vector-literal"),
            Value::T,
            Value::T,
            Value::Nil,
            Value::Nil,
        ])
    );
    assert_eq!(
        call(
            &mut interp,
            "nreverse",
            std::slice::from_ref(&bool_vector),
            &mut env,
        )
        .expect("nreverse should accept bool-vectors"),
        bool_vector
    );
    assert_eq!(
        bool_vector_bits(&interp, &bool_vector).expect("mutated bool-vector"),
        vec![false, false, true, true]
    );
}

#[test]
fn compare_strings_matches_fns_expectations() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "compare-strings",
            &[
                Value::String("foobaz".into()),
                Value::Nil,
                Value::Nil,
                Value::String("farbaz".into()),
                Value::Nil,
                Value::Nil,
            ],
            &mut env,
        )
        .expect("compare-strings should compare differing substrings"),
        Value::Integer(2)
    );
    assert_eq!(
        call(
            &mut interp,
            "compare-strings",
            &[
                Value::String("Test".into()),
                Value::Nil,
                Value::Nil,
                Value::String("test".into()),
                Value::Nil,
                Value::Nil,
                Value::T,
            ],
            &mut env,
        )
        .expect("compare-strings should support ignore-case"),
        Value::T
    );
}

// GNU/Linux collates for real (fns.c under __STDC_ISO_10646__, sysdep.c
// str_collate): a non-string LOCALE is a wrong-type-argument and POSIX
// collation orders "XYZZY" before "xyzzy".
#[cfg(target_os = "linux")]
#[test]
fn collation_functions_collate_through_the_libc_locale() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert!(
        call(
            &mut interp,
            "string-collate-equalp",
            &[
                Value::String("xyzzy".into()),
                Value::String("xyzzy".into()),
                Value::T
            ],
            &mut env,
        )
        .is_err(),
        "a non-string locale must signal like GNU's CHECK_STRING"
    );
    assert_eq!(
        call(
            &mut interp,
            "string-collate-lessp",
            &[
                Value::String("XYZZY".into()),
                Value::String("xyzzy".into()),
                Value::String("POSIX".into()),
            ],
            &mut env,
        )
        .expect("POSIX collation ordering should succeed"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "string-collate-equalp",
            &[
                Value::String("xyzzy".into()),
                Value::String("XYZZY".into()),
                Value::Nil,
                Value::T,
            ],
            &mut env,
        )
        .expect("case-folded collation equality should succeed"),
        Value::T
    );
}

// Without __STDC_ISO_10646__ (Darwin), GNU itself falls back to the
// lexicographic comparison and ignores the locale's collation order.
#[cfg(not(target_os = "linux"))]
#[test]
fn collation_functions_fall_back_to_lexicographic_comparison() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "string-collate-equalp",
            &[
                Value::String("xyzzy".into()),
                Value::String("xyzzy".into()),
                Value::T
            ],
            &mut env,
        )
        .expect("fallback collation equality should succeed"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "string-collate-lessp",
            &[
                Value::String("XYZZY".into()),
                Value::String("xyzzy".into()),
                Value::String("POSIX".into()),
            ],
            &mut env,
        )
        .expect("fallback collation ordering should succeed"),
        Value::T
    );
}

#[test]
fn file_writable_p_is_true_for_creatable_missing_files() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let path = std::env::temp_dir()
        .join(format!("emaxx-file-writable-{}", std::process::id()))
        .join("missing.txt");
    let _ = std::fs::remove_dir_all(path.parent().expect("temp dir"));
    std::fs::create_dir_all(path.parent().expect("temp dir")).expect("create temp dir");

    assert_eq!(
        call(
            &mut interp,
            "file-writable-p",
            &[Value::String(path.display().to_string().into())],
            &mut env,
        )
        .expect("file-writable-p for missing path"),
        Value::T
    );

    std::fs::remove_dir_all(path.parent().expect("temp dir")).expect("cleanup temp dir");
}

#[cfg(unix)]
#[test]
fn file_writable_p_is_nil_for_missing_files_in_unwritable_directories() {
    use std::os::unix::fs::PermissionsExt;

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let dir = std::env::temp_dir().join(format!("emaxx-file-unwritable-{}", std::process::id()));
    let path = dir.join("missing.txt");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let original_perms = std::fs::metadata(&dir).expect("metadata").permissions();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555))
        .expect("make temp dir unwritable");

    assert_eq!(
        call(
            &mut interp,
            "file-writable-p",
            &[Value::String(path.display().to_string().into())],
            &mut env,
        )
        .expect("file-writable-p for missing path"),
        Value::Nil
    );

    std::fs::set_permissions(&dir, original_perms).expect("restore original permissions");
    std::fs::remove_dir_all(&dir).expect("cleanup temp dir");
}

#[test]
fn selected_window_is_a_record_and_tracks_window_start() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "\n\n\n");
    let window = call(&mut interp, "selected-window", &[], &mut env).expect("selected window");
    assert!(matches!(window, Value::Record(_)));

    assert_eq!(
        call(
            &mut interp,
            "set-window-start",
            &[window.clone(), Value::Integer(2)],
            &mut env,
        )
        .expect("set-window-start"),
        Value::T
    );
    assert_eq!(
        call(&mut interp, "window-start", &[window], &mut env).expect("window-start"),
        Value::Integer(2)
    );

    assert_eq!(
        call(
            &mut interp,
            "set-window-start",
            &[Value::Nil, Value::Integer(3)],
            &mut env,
        )
        .expect("set-window-start with selected window default"),
        Value::T
    );
    assert_eq!(
        call(&mut interp, "window-start", &[], &mut env).expect("selected window-start"),
        Value::Integer(3)
    );
}

#[test]
fn get_buffer_window_only_reports_selected_buffer() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*not-visible*");
    let other_buffer = Value::buffer(buffer_id, buffer_name);

    assert_eq!(
        call(&mut interp, "get-buffer-window", &[other_buffer], &mut env)
            .expect("get-buffer-window for non-visible buffer"),
        Value::Nil
    );
    assert_eq!(
        call(
            &mut interp,
            "get-buffer-window",
            &[Value::String("*missing*".into())],
            &mut env,
        )
        .expect("get-buffer-window for missing buffer name"),
        Value::Nil
    );
    assert_eq!(
        call(&mut interp, "get-buffer-window", &[Value::Nil], &mut env)
            .expect("get-buffer-window for current buffer"),
        interp.selected_window_value()
    );
}

#[test]
fn find_operation_coding_system_accepts_file_buffer_cons() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    interp.set_variable(
        "file-coding-system-alist",
        Value::list([Value::cons(
            Value::String("\\.txt\\'".into()),
            Value::Symbol("utf-8-unix".into()),
        )]),
        &mut env,
    );

    let file_arg = Value::cons(
        Value::String("/tmp/demo.txt".into()),
        Value::buffer(interp.current_buffer_id(), interp.buffer.name.clone()),
    );
    assert_eq!(
        call(
            &mut interp,
            "find-operation-coding-system",
            &[
                Value::Symbol("insert-file-contents".into()),
                file_arg,
                Value::T,
            ],
            &mut env,
        )
        .expect("cons file argument is accepted"),
        Value::cons(
            Value::Symbol("utf-8-unix".into()),
            Value::Symbol("utf-8-unix".into()),
        )
    );
}

#[test]
fn file_system_info_reports_host_capacity_and_missing_paths() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let info = call(
        &mut interp,
        "file-system-info",
        &[Value::String(
            std::env::temp_dir().display().to_string().into(),
        )],
        &mut env,
    )
    .expect("file-system-info should query the host filesystem");
    let values = info
        .to_vec()
        .expect("file-system-info should return a three-element list");
    assert_eq!(values.len(), 3);
    let total = integer_like_bigint(&interp, &values[0]).expect("total bytes");
    let free = integer_like_bigint(&interp, &values[1]).expect("free bytes");
    let available = integer_like_bigint(&interp, &values[2]).expect("available bytes");
    assert!(total > BigInt::from(0));
    assert!(total >= free);
    assert!(free >= available);

    assert_eq!(
        call(
            &mut interp,
            "file-system-info",
            &[Value::String(
                "/definitely/missing/emaxx-file-system-info".into()
            )],
            &mut env,
        )
        .expect("a missing filesystem path is not an exceptional query"),
        Value::Nil
    );
}

#[test]
fn discard_input_clears_pending_events_and_keyboard_macro_definition() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let form = Reader::new(
        "(let ((unread-command-events '(97 98))
               (defining-kbd-macro t))
           (list (discard-input)
                 unread-command-events
                 defining-kbd-macro
                 (input-pending-p)))",
    )
    .read()
    .expect("discard-input regression should parse")
    .expect("discard-input regression should contain a form");

    assert_eq!(
        interp
            .eval(&form, &mut env)
            .expect("discard-input should clear batch-visible input state"),
        Value::list([Value::Nil, Value::Nil, Value::Nil, Value::Nil])
    );
}

#[test]
fn detect_eol_type_preserves_embedded_cr_in_unix_files() {
    assert_eq!(detect_eol_type(b"left\rmiddle\nnext\n"), 0);
    assert_eq!(
        decode_bytes_with_explicit_eol(
            b"left\rmiddle\nnext\n",
            detect_eol_type(b"left\rmiddle\nnext\n")
        ),
        b"left\rmiddle\nnext\n"
    );
}

#[test]
fn insert_file_contents_preserves_embedded_cr_in_unix_files() {
    let path = std::env::temp_dir().join(format!(
        "emaxx-insert-file-contents-eol-{}.txt",
        std::process::id()
    ));
    std::fs::write(&path, b"left\rmiddle\nnext\n").expect("write fixture");

    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    // The fixture's startup leaves the *scratch* banner in the current
    // buffer, as GNU's does; the test is about the inserted bytes.
    call(&mut interp, "erase-buffer", &[], &mut env).expect("erase the scratch banner");
    call(
        &mut interp,
        "insert-file-contents",
        &[Value::String(path.display().to_string().into())],
        &mut env,
    )
    .expect("insert file contents should preserve embedded carriage returns");
    assert_eq!(interp.buffer.buffer_string(), "left\rmiddle\nnext\n");

    std::fs::remove_file(path).expect("cleanup fixture");
}

// Finding 34 re-hosts: the originals ran on the bare interpreter against
// native facades and were deleted with them; GNU's own test tree covers
// none of these, so the coverage lives here on the full dumped image,
// with every expectation probed against the pinned oracle.

#[test]
fn skeleton_insert_inserts_strings_and_places_point_at_the_interesting_spot() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    assert_eq!(
        crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            r#"(progn
                 (require 'skeleton)
                 (with-temp-buffer
                   (skeleton-insert '(nil "f" _ "oo"))
                   (list (buffer-string) (point))))"#,
        )
        .expect("skeleton-insert on the dumped image"),
        Value::list([Value::String("foo".into()), Value::Integer(2)])
    );
}

#[test]
fn special_mode_sets_major_mode_read_only_and_its_keymap() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    assert_eq!(
        crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            r#"(with-temp-buffer
                 (special-mode)
                 (list major-mode buffer-read-only (key-binding "q") (key-binding " ")))"#,
        )
        .expect("special-mode on the dumped image"),
        Value::list([
            Value::symbol("special-mode"),
            Value::T,
            Value::symbol("quit-window"),
            Value::symbol("scroll-up-command"),
        ])
    );
}

#[test]
fn jka_compr_sniffs_compression_info_from_file_suffixes() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    assert_eq!(
        crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            r#"(progn
                 (require 'jka-compr)
                 (list (not (null (jka-compr-get-compression-info "foo.gz")))
                       (jka-compr-get-compression-info "foo.txt")
                       (not (null (jka-compr-get-compression-info "a.tar.bz2")))))"#,
        )
        .expect("jka-compr sniffing on the dumped image"),
        Value::list([Value::T, Value::Nil, Value::T])
    );
}

#[test]
fn display_buffer_honors_inhibit_same_window_and_action_function_returns() {
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    let mut env = Vec::new();
    assert_eq!(
        crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            // Oracle contract: with inhibit-same-window the chosen window is
            // real, not the selected one, and shows the buffer.  An action
            // function returning a non-window truthy value stops the chain
            // and display-buffer returns nil; a nil return falls through to
            // the default actions, which produce a real window.
            r#"(let ((buf (generate-new-buffer "*display-rehost*")))
                 (list
                  (let ((win (display-buffer buf '(nil (inhibit-same-window . t)))))
                    (list (windowp win)
                          (eq win (selected-window))
                          (eq (window-buffer win) buf)))
                  (let ((calls nil))
                    (list (display-buffer
                           buf
                           (list (lambda (b a)
                                   (push (list (buffer-name b) a) calls)
                                   'marker)))
                          calls))
                  (windowp
                   (display-buffer buf (list (lambda (_b _a) nil)
                                             (cons 'fallback nil))))))"#,
        )
        .expect("display-buffer on the dumped image"),
        Value::list([
            Value::list([Value::T, Value::Nil, Value::T]),
            Value::list([
                Value::Nil,
                Value::list([Value::list([
                    Value::String("*display-rehost*".into()),
                    Value::Nil,
                ])]),
            ]),
            Value::T,
        ])
    );
}
