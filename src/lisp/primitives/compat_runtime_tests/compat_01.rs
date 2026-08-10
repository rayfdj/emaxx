use super::*;

#[test]
fn count_lines_matches_emacs_boundary_behavior() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "a\nb\nc");
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "count-lines",
            &[Value::Integer(1), Value::Integer(1)],
            &mut env
        )
        .expect("count-lines at same position"),
        Value::Integer(0)
    );
    assert_eq!(
        call(
            &mut interp,
            "count-lines",
            &[Value::Integer(1), Value::Integer(3)],
            &mut env
        )
        .expect("count-lines to line start"),
        Value::Integer(1)
    );
    assert_eq!(
        call(
            &mut interp,
            "count-lines",
            &[Value::Integer(1), Value::Integer(4)],
            &mut env
        )
        .expect("count-lines through mid-line"),
        Value::Integer(2)
    );
    assert_eq!(
        call(
            &mut interp,
            "count-lines",
            &[Value::Integer(4), Value::Integer(1)],
            &mut env
        )
        .expect("count-lines is symmetric"),
        Value::Integer(2)
    );
}

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
fn file_modes_number_to_symbolic_formats_tar_modes() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "file-modes-number-to-symbolic",
            &[Value::Integer(0o700)],
            &mut env
        )
        .expect("format regular mode"),
        Value::String("-rwx------".into())
    );
    assert_eq!(
        call(
            &mut interp,
            "file-modes-number-to-symbolic",
            &[Value::Integer(0o4000)],
            &mut env
        )
        .expect("format setuid mode"),
        Value::String("---S------".into())
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
    assert_eq!(
        call(&mut interp, "file-attribute-size", &[attributes], &mut env,).expect("attribute size"),
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
                 (not (multibyte-string-p u))
                 (multibyte-string-p m)
                 (not (multibyte-string-p uu))
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
                (setcdr (last cycle) cycle)
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
                    (equal (car (nthcdr 2 map)) '(120 . sample-command))))
            "#,
    )
    .read_all()
    .expect("keymap nth test should parse");
    let result = forms
        .iter()
        .try_fold(Value::Nil, |_, form| interp.eval(form, &mut env))
        .expect("keymap nth forms should evaluate");
    assert_eq!(result, Value::list(vec![Value::T; 6]));
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
                 (not (plist-get plist (string ?a)))
                 (not (plist-member plist (string ?a)))))
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
              (setcdr (last cycle) cycle)
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
        Value::Integer(0x40_0000)
    );
    assert_eq!(
        call(
            &mut interp,
            "length=",
            &[table, Value::Integer(0x40_0000)],
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
    .expect("string-make-multibyte should decode latin-1 bytes");
    assert_eq!(string_text(&made).expect("decoded text"), "é");
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
fn text_mode_activates_major_mode_for_derived_mode_checks() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(&mut interp, "text-mode", &[], &mut env).expect("text-mode should activate"),
        Value::Nil
    );
    assert_eq!(
        interp.lookup_var("major-mode", &env),
        Some(Value::Symbol("text-mode".into()))
    );
    assert_eq!(
        call(
            &mut interp,
            "derived-mode-p",
            &[Value::Symbol("text-mode".into())],
            &mut env,
        )
        .expect("text-mode should match derived-mode-p"),
        Value::T
    );
}

#[test]
fn skeleton_insert_inserts_strings_and_restores_point_marker() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let skeleton = Value::list([
        Value::Nil,
        Value::String("f".into()),
        Value::Symbol("_".into()),
        Value::String("oo".into()),
    ]);

    assert_eq!(
        call(&mut interp, "skeleton-insert", &[skeleton], &mut env)
            .expect("skeleton-insert should insert simple skeletons"),
        Value::Nil
    );
    assert_eq!(interp.buffer.buffer_string(), "foo");
    assert_eq!(interp.buffer.point(), interp.buffer.point_min() + 1);
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
fn mode_function_for_file_name_handles_compressed_archives() {
    assert_eq!(
        mode_function_for_file_name("/tmp/archive.tar.gz"),
        Some("tar-mode")
    );
    assert_eq!(
        mode_function_for_file_name("/tmp/archive.tgz"),
        Some("tar-mode")
    );
    assert_eq!(
        mode_function_for_file_name("/tmp/archive.zip.gz"),
        Some("archive-mode")
    );
    assert_eq!(mode_function_for_file_name("/tmp/plain.txt.gz"), None);
}

#[test]
fn normal_mode_selects_archive_mode_for_zip_buffers() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_function_binding(
        "archive-mode",
        Some(Value::lambda(
            Vec::new().into(),
            vec![
                Value::list([
                    Value::Symbol("setq-local".into()),
                    Value::Symbol("major-mode".into()),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol("archive-mode".into()),
                    ]),
                ]),
                Value::Nil,
            ]
            .into(),
            shared_env(Vec::new()),
        )),
    );
    interp.set_variable(
        "buffer-file-name",
        Value::String("/tmp/demo.zip".into()),
        &mut env,
    );

    assert_eq!(
        call(&mut interp, "normal-mode", &[], &mut env).expect("dispatch normal-mode"),
        Value::Nil
    );
    assert_eq!(
        interp.lookup_var("major-mode", &env),
        Some(Value::Symbol("archive-mode".into()))
    );
}

#[test]
fn c_plus_plus_mode_marks_semantic_buffers_active_when_global_mode_is_enabled() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("semantic-mode", Value::T, &mut env);
    interp
        .eval(
            &crate::lisp::reader::Reader::new(
                "(defun semantic-lex-init ()
                       (setq semantic-lex-syntax-table (copy-syntax-table (syntax-table))))",
            )
            .read()
            .expect("read semantic lex init stub")
            .expect("semantic lex init form"),
            &mut env,
        )
        .expect("define semantic lex init stub");
    interp.set_variable(
        "buffer-file-name",
        Value::String("/tmp/sample.cpp".into()),
        &mut env,
    );

    assert_eq!(
        call(&mut interp, "normal-mode", &[], &mut env).expect("dispatch c++ normal-mode"),
        Value::Nil
    );
    assert_eq!(
        interp.lookup_var("major-mode", &env),
        Some(Value::Symbol("c++-mode".into()))
    );
    assert_eq!(
        interp.lookup_var("semantic-new-buffer-fcn-was-run", &env),
        Some(Value::T)
    );
    assert!(matches!(
        interp.lookup_var("semantic-lex-syntax-table", &env),
        Some(Value::CharTable(_))
    ));
    interp.insert_current_buffer("/* comment */x");
    interp.buffer.goto_char(1);
    assert_eq!(
        call(
            &mut interp,
            "forward-comment",
            &[Value::Integer(1)],
            &mut env
        )
        .expect("c syntax table should move over block comments"),
        Value::T
    );
    assert_eq!(interp.buffer.point(), 14);
    assert_eq!(
        call(
            &mut interp,
            "derived-mode-all-parents",
            &[Value::Symbol("c++-mode".into())],
            &mut env,
        )
        .expect("c++ mode parent chain should be available"),
        Value::list([
            Value::Symbol("c++-mode".into()),
            Value::Symbol("c-mode".into()),
            Value::Symbol("prog-mode".into()),
        ])
    );
}

#[test]
fn c_plus_plus_mode_runs_semantic_new_buffer_setup_when_available() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("semantic-mode", Value::T, &mut env);
    interp
        .eval(
            &crate::lisp::reader::Reader::new(
                "(defun semantic-new-buffer-fcn ()
                       (setq semantic-parser-ready t))",
            )
            .read()
            .expect("read semantic setup stub")
            .expect("semantic setup form"),
            &mut env,
        )
        .expect("define semantic setup stub");
    interp.set_variable(
        "buffer-file-name",
        Value::String("/tmp/sample.cpp".into()),
        &mut env,
    );

    assert_eq!(
        call(&mut interp, "normal-mode", &[], &mut env).expect("dispatch c++ normal-mode"),
        Value::Nil
    );
    assert_eq!(
        interp.lookup_var("semantic-parser-ready", &env),
        Some(Value::T)
    );
}

#[test]
fn normal_mode_consults_auto_mode_alist_before_dispatching() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_function_binding(
        "sample-custom-mode",
        Some(Value::lambda(
            Vec::new().into(),
            vec![
                Value::list([
                    Value::Symbol("setq-local".into()),
                    Value::Symbol("major-mode".into()),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol("sample-custom-mode".into()),
                    ]),
                ]),
                Value::Nil,
            ]
            .into(),
            shared_env(Vec::new()),
        )),
    );
    interp.set_variable(
        "auto-mode-alist",
        Value::list([Value::cons(
            Value::String("\\.sample\\'".into()),
            Value::Symbol("sample-custom-mode".into()),
        )]),
        &mut env,
    );
    interp.set_variable(
        "buffer-file-name",
        Value::String("/tmp/demo.sample".into()),
        &mut env,
    );

    assert_eq!(
        call(&mut interp, "normal-mode", &[], &mut env).expect("dispatch custom normal-mode"),
        Value::Nil
    );
    assert_eq!(
        interp.lookup_var("major-mode", &env),
        Some(Value::Symbol("sample-custom-mode".into()))
    );
}

#[test]
fn normal_mode_uses_gnu_default_perl_file_association() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_function_binding(
        "perl-mode",
        Some(Value::lambda(
            Vec::new().into(),
            vec![Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("major-mode".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("perl-mode".into()),
                ]),
            ])]
            .into(),
            shared_env(Vec::new()),
        )),
    );
    interp.set_variable(
        "buffer-file-name",
        Value::String("/tmp/example.pl".into()),
        &mut env,
    );

    assert_eq!(
        call(&mut interp, "normal-mode", &[], &mut env).expect("dispatch default Perl mode"),
        Value::Nil
    );
    assert_eq!(
        interp.lookup_var("major-mode", &env),
        Some(Value::Symbol("perl-mode".into()))
    );
}

#[test]
fn normal_mode_prefers_loaded_lisp_mode_over_the_native_fallback() {
    let root = std::env::temp_dir().join(format!(
        "emaxx-normal-mode-autoload-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create mode autoload directory");
    let target = root.join("sample-python-mode.el");
    std::fs::write(
        &target,
        "(defun python-mode ()\n\
           (setq loaded-python-mode-ran t)\n\
           (setq-local major-mode 'python-mode))\n",
    )
    .expect("write mode autoload");

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_load_path(vec![root.clone()]);
    let autoload =
        crate::lisp::reader::Reader::new("(autoload 'python-mode \"sample-python-mode\")")
            .read()
            .expect("read mode autoload")
            .expect("mode autoload form");
    interp
        .eval(&autoload, &mut env)
        .expect("install mode autoload");
    interp.set_variable(
        "auto-mode-alist",
        Value::list([Value::cons(
            Value::String("\\.py\\'".into()),
            Value::Symbol("python-mode".into()),
        )]),
        &mut env,
    );
    interp.set_variable(
        "buffer-file-name",
        Value::String("/tmp/example.py".into()),
        &mut env,
    );

    assert_eq!(
        call(&mut interp, "normal-mode", &[], &mut env).expect("dispatch Python normal-mode"),
        Value::Nil
    );
    assert_eq!(
        interp.lookup_var("loaded-python-mode-ran", &env),
        Some(Value::T),
        "normal-mode must funcall the installed Lisp definition before using a native fallback"
    );

    std::fs::remove_file(target).expect("remove mode autoload");
    std::fs::remove_dir(root).expect("remove mode autoload directory");
}

#[test]
fn normal_mode_dispatches_semantic_resource_modes() {
    for (path, mode) in [
        ("/tmp/Sample.java", "java-mode"),
        ("/tmp/test.mk", "makefile-bsdmake-mode"),
        ("/tmp/test.texi", "texinfo-mode"),
        ("/tmp/test.wy", "wisent-grammar-mode"),
        ("/tmp/test.srt", "srecode-template-mode"),
    ] {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        interp.set_variable("semantic-mode", Value::T, &mut env);
        interp.set_variable("buffer-file-name", Value::String(path.into()), &mut env);

        assert_eq!(
            call(&mut interp, "normal-mode", &[], &mut env).expect("dispatch normal-mode"),
            Value::Nil
        );
        assert_eq!(
            interp.lookup_var("major-mode", &env),
            Some(Value::Symbol(mode.into()))
        );
        assert_eq!(
            interp.lookup_var("semantic-new-buffer-fcn-was-run", &env),
            Some(Value::T)
        );
    }
}

#[test]
fn special_mode_sets_major_mode_and_read_only() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(&mut interp, "special-mode", &[], &mut env).expect("run special-mode"),
        Value::Nil
    );
    assert_eq!(
        interp.lookup_var("major-mode", &env),
        Some(Value::Symbol("special-mode".into()))
    );
    assert_eq!(interp.lookup_var("buffer-read-only", &env), Some(Value::T));
}

#[test]
fn jka_compr_get_compression_info_recognizes_gzip_suffixes() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "jka-compr-get-compression-info",
            &[Value::String("/tmp/demo.gz".into())],
            &mut env
        )
        .expect("gzip suffix is recognized"),
        Value::T
    );
    assert_eq!(
        call(
            &mut interp,
            "jka-compr-get-compression-info",
            &[Value::String("/tmp/demo.zip".into())],
            &mut env
        )
        .expect("plain zip is not compression info"),
        Value::Nil
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
fn find_file_marks_unwritable_files_read_only() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let dir =
        std::env::temp_dir().join(format!("emaxx-find-file-read-only-{}", std::process::id()));
    let path = dir.join("readonly.txt");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    std::fs::write(&path, "contents").expect("write temp file");
    let original_perms = std::fs::metadata(&path).expect("metadata").permissions();
    let mut perms = original_perms.clone();
    perms.set_readonly(true);
    std::fs::set_permissions(&path, perms).expect("make temp file read-only");

    let buffer = call(
        &mut interp,
        "find-file-noselect",
        &[Value::String(path.display().to_string().into())],
        &mut env,
    )
    .expect("find-file-noselect");
    let buffer_id = interp.resolve_buffer_id(&buffer).expect("buffer id");
    interp
        .switch_to_buffer_id(buffer_id)
        .expect("switch buffer");
    assert_eq!(interp.lookup_var("buffer-read-only", &env), Some(Value::T));
    assert_eq!(interp.lookup_var("read-only-mode", &env), Some(Value::T));

    std::fs::set_permissions(&path, original_perms).expect("restore original permissions");
    std::fs::remove_dir_all(&dir).expect("cleanup temp dir");
}

#[test]
fn file_visiting_apis_reuse_the_live_buffer_and_preserve_unsaved_text() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let path = std::env::temp_dir().join(format!(
        "emaxx-visited-buffer-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    std::fs::write(&path, "disk").expect("create visited file");
    let path_text = path.to_string_lossy().into_owned();

    let first = call(
        &mut interp,
        "find-file-noselect",
        &[Value::String(path_text.clone().into())],
        &mut env,
    )
    .expect("first file visit");
    let first_id = interp.resolve_buffer_id(&first).expect("first buffer id");
    interp
        .switch_to_buffer_id(first_id)
        .expect("select first file buffer");
    interp.buffer.goto_char(interp.buffer.point_max());
    call(
        &mut interp,
        "insert",
        &[Value::String("-unsaved".into())],
        &mut env,
    )
    .expect("modify visiting buffer");

    let exact = call(
        &mut interp,
        "get-file-buffer",
        &[Value::String(path_text.clone().into())],
        &mut env,
    )
    .expect("look up exact visited name");
    assert_eq!(exact, first);

    let canonical = std::fs::canonicalize(&path)
        .expect("canonical visited path")
        .to_string_lossy()
        .into_owned();
    let aliased = call(
        &mut interp,
        "find-buffer-visiting",
        &[Value::String(canonical.into())],
        &mut env,
    )
    .expect("look up canonical visited name");
    assert_eq!(aliased, first);

    let reopened = call(
        &mut interp,
        "find-file-noselect",
        &[Value::String(path_text.into())],
        &mut env,
    )
    .expect("reopen visited file");
    assert_eq!(reopened, first);
    assert_eq!(interp.buffer.buffer_string(), "disk-unsaved");

    interp.kill_buffer_id(first_id);
    std::fs::remove_file(path).expect("remove visited file");
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
fn display_buffer_honors_inhibit_same_window() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*display-buffer-inhibit*");
    let buffer = Value::buffer(buffer_id, buffer_name);

    assert_eq!(
        call(
            &mut interp,
            "display-buffer",
            &[
                buffer,
                Value::list([Value::cons(
                    Value::Symbol("inhibit-same-window".into()),
                    Value::T,
                )]),
            ],
            &mut env,
        )
        .expect("display-buffer with inhibit-same-window"),
        Value::Nil
    );
    assert_eq!(
        interp.selected_window_buffer_id(),
        interp.current_buffer_id()
    );
}

#[test]
fn display_buffer_calls_action_function_via_primitive_entrypoint() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("display-buffer-action-called", Value::Nil, &mut env);
    interp.set_function_binding(
        "demo-display",
        Some(Value::lambda(
            vec!["buffer".into(), "alist".into()].into(),
            vec![
                Value::list([
                    Value::Symbol("setq".into()),
                    Value::Symbol("display-buffer-action-called".into()),
                    Value::T,
                ]),
                Value::list([Value::Symbol("selected-window".into())]),
            ]
            .into(),
            shared_env(Vec::new()),
        )),
    );
    let (buffer_id, buffer_name) = interp.create_buffer("*display-buffer-action*");
    let buffer = Value::buffer(buffer_id, buffer_name);
    assert!(matches!(
        call(
            &mut interp,
            "display-buffer",
            &[buffer, Value::Symbol("demo-display".into())],
            &mut env,
        )
        .expect("display-buffer action function"),
        Value::Record(_)
    ));
    assert_eq!(
        interp
            .lookup_var("display-buffer-action-called", &env)
            .expect("display-buffer-action-called"),
        Value::T
    );
}

#[test]
fn display_buffer_action_function_can_return_nil() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    interp.set_variable("display-buffer-action-called", Value::Nil, &mut env);
    interp.set_function_binding(
        "demo-display",
        Some(Value::lambda(
            vec!["buffer".into(), "alist".into()].into(),
            vec![
                Value::list([
                    Value::Symbol("setq".into()),
                    Value::Symbol("display-buffer-action-called".into()),
                    Value::T,
                ]),
                Value::list([Value::Symbol("quote".into()), Value::Nil]),
            ]
            .into(),
            shared_env(Vec::new()),
        )),
    );
    let (buffer_id, buffer_name) = interp.create_buffer("*display-buffer-action-nil*");
    let buffer = Value::buffer(buffer_id, buffer_name);
    assert!(matches!(
        call(
            &mut interp,
            "display-buffer",
            &[buffer, Value::Symbol("demo-display".into())],
            &mut env,
        )
        .expect("display-buffer action function returning nil"),
        Value::Record(_)
    ));
    assert_eq!(
        interp
            .lookup_var("display-buffer-action-called", &env)
            .expect("display-buffer-action-called"),
        Value::T
    );
}

#[test]
fn find_operation_coding_system_accepts_file_buffer_cons() {
    let mut interp = Interpreter::new();
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
fn insert_file_contents_respects_auto_compression_mode_toggle() {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    std::io::Write::write_all(&mut encoder, "❄\n".as_bytes()).expect("write gzip payload");
    let compressed = encoder.finish().expect("finish gzip payload");

    let path = std::env::temp_dir().join(format!(
        "emaxx-auto-compression-{}.txt.gz",
        std::process::id()
    ));
    std::fs::write(&path, compressed).expect("write gzip file");
    let path_string = path.display().to_string();

    let mut compressed_interp = Interpreter::new();
    let mut compressed_env = Vec::new();
    compressed_interp.set_variable("auto-compression-mode", Value::Nil, &mut compressed_env);
    call(
        &mut compressed_interp,
        "insert-file-contents",
        &[Value::String(path_string.clone().into())],
        &mut compressed_env,
    )
    .expect("insert compressed contents literally when auto compression is disabled");
    assert_ne!(compressed_interp.buffer.buffer_string(), "❄\n");

    let mut decompressed_interp = Interpreter::new();
    let mut decompressed_env = Vec::new();
    decompressed_interp.set_variable("auto-compression-mode", Value::T, &mut decompressed_env);
    let auto_coding = Reader::new(
        "(defun emaxx-test-auto-coding (filename _size)
           (setq emaxx-test-auto-coding-file filename)
           (if (string-suffix-p \".gz\" filename) 'no-conversion nil))",
    )
    .read()
    .expect("auto-coding regression should parse")
    .expect("auto-coding regression should contain a form");
    decompressed_interp
        .eval(&auto_coding, &mut decompressed_env)
        .expect("auto-coding regression helper should evaluate");
    decompressed_interp.set_variable(
        "set-auto-coding-function",
        Value::Symbol("emaxx-test-auto-coding".into()),
        &mut decompressed_env,
    );
    call(
        &mut decompressed_interp,
        "insert-file-contents",
        &[Value::String(path_string.clone().into())],
        &mut decompressed_env,
    )
    .expect("insert decompressed contents when auto compression is enabled");
    assert_eq!(decompressed_interp.buffer.buffer_string(), "❄\n");
    assert_eq!(
        decompressed_interp
            .lookup_var("emaxx-test-auto-coding-file", &decompressed_env)
            .expect("auto-coding helper should record its filename"),
        Value::String(path_string.trim_end_matches(".gz").to_string().into())
    );

    std::fs::remove_file(path).expect("cleanup gzip file");
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

    let mut interp = Interpreter::new();
    let mut env = Vec::new();
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
