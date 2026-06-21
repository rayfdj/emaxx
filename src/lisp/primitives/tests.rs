use super::*;
use crate::lisp::reader::Reader;

fn upstream_emacs_repo() -> PathBuf {
    crate::compat::project_root().join("../emacs")
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
        Value::Integer(1)
    );
    assert_eq!(
        call(
            &mut interp,
            "string-to-syntax",
            &[Value::String(". 1234".into())],
            &mut env,
        )
        .expect("comment flag syntax"),
        Value::Integer(983041)
    );
    assert_eq!(
        call(
            &mut interp,
            "string-to-syntax",
            &[Value::String(". nb".into())],
            &mut env,
        )
        .expect("nested style-b syntax"),
        Value::Integer(6291457)
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
        ert_resource_directory_for("/Users/alpha/CodexProjects/emacs/test/src/syntax-tests.el"),
        "/Users/alpha/CodexProjects/emacs/test/src/syntax-resources/"
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
fn directory_files_returns_sorted_names_with_dot_entries() {
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
        &[Value::String(directory.display().to_string())],
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
    assert_eq!(interp.charset_id("unicode"), Some(1));
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
        compat_invocation_path_from_test_directory(&test_directory),
        Some(src_dir.join("emacs"))
    );
    assert_eq!(
        compat_emacsclient_path_from_test_directory(&test_directory),
        Some(lib_src_dir.join("emacsclient"))
    );
    let old = std::env::var("EMACS_TEST_DIRECTORY").ok();
    unsafe {
        std::env::set_var("EMACS_TEST_DIRECTORY", &test_directory);
    }
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
        Value::String(cwd.display().to_string()),
        &mut env,
    );

    let result = call(
        &mut interp,
        "process-lines",
        &[Value::String("/bin/pwd".into())],
        &mut env,
    )
    .expect("process-lines should succeed");

    assert_eq!(result, Value::list([Value::String(expected)]));

    let _ = std::fs::remove_dir_all(&cwd);
}

#[cfg(unix)]
#[test]
fn start_process_routes_command_output_to_its_buffer() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*process-output*");
    let buffer = Value::Buffer(buffer_id, buffer_name);

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
    assert_eq!(contents, "secret\nsecond\n");
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

#[cfg(unix)]
#[test]
fn deleted_process_is_not_returned_for_buffer() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let (buffer_id, buffer_name) = interp.create_buffer("*deleted-process*");
    let buffer = Value::Buffer(buffer_id, buffer_name);
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
    let callback = Value::Lambda(
        Vec::new(),
        vec![
            Value::list([
                Value::Symbol("setq".into()),
                Value::Symbol("timer-fired".into()),
                Value::T,
            ]),
            Value::T,
        ],
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
    let callback = Value::Lambda(
        Vec::new(),
        vec![
            Value::list([
                Value::Symbol("setq".into()),
                Value::Symbol("timer-fired".into()),
                Value::T,
            ]),
            Value::T,
        ],
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
fn indent_rigidly_shifts_each_line_in_region() {
    let mut interp = Interpreter::new();
    interp.buffer = crate::buffer::Buffer::from_text("*test*", "a\nb\n");
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
                Value::Buffer(_, _),
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
        key_sequence_binding_parts(&Value::String("C-c g".into()))
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
        where_is_internal(&mut interp, "keymap-tests-command", &[keymap], &mut env,)
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
fn completion_predicates_preserve_string_list_membership() {
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
                Value::T,
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
        shell_quote_argument("/Users/alpha/CodexProjects/emaxx/"),
        "/Users/alpha/CodexProjects/emaxx/"
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
