use std::fs;
use std::path::Path;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn rust_files_below(relative: &str) -> Vec<std::path::PathBuf> {
    fn visit(directory: &Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in fs::read_dir(directory).expect("read anti-cheat source directory") {
            let path = entry.expect("read anti-cheat directory entry").path();
            if path.is_dir() {
                visit(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    visit(&repo_root().join(relative), &mut files);
    files.sort();
    files
}

/// Every Rust file the facade/spelling gates must scan.  The Lisp runtime
/// tree plus the frontends and binaries: silent fallbacks have historically
/// hidden in src/tty.rs, which sat outside the original src/lisp scope.
fn facade_gate_files() -> Vec<std::path::PathBuf> {
    let mut files = rust_files_below("src/lisp");
    files.extend(rust_files_below("src/bin"));
    for extra in [
        "src/tty.rs",
        "src/batch.rs",
        "src/buffer.rs",
        "src/overlay.rs",
        "src/compat.rs",
    ] {
        let path = repo_root().join(extra);
        if path.exists() {
            files.push(path);
        }
    }
    files
}

pub(crate) fn repo_does_not_define_batch_report_delegation() {
    let delegation_files = ["src/compat.rs", "src/batch.rs", "src/bin/compat-harness.rs"];
    let banned_tokens = [
        concat!("ORACLE_BATCH_", "REPORT_OVERRIDES"),
        concat!("should_delegate_", "batch_report("),
        concat!("maybe_delegate_", "batch_report("),
        concat!("delegated_", "emaxx_artifacts("),
        concat!("compat-harness ", "delegated"),
        concat!("ORACLE_BATCH_", "REPORT_BRIDGES"),
        concat!("should_bridge_", "batch_report("),
        concat!("maybe_bridge_", "batch_report("),
        concat!("bridge_", "batch_report("),
        concat!("oracle batch ", "delegation"),
    ];

    for relative in delegation_files {
        let text =
            fs::read_to_string(repo_root().join(relative)).expect("read anti-cheat source file");
        for banned in banned_tokens {
            assert!(
                !text.contains(banned),
                "{relative} unexpectedly contains banned delegation token `{banned}`"
            );
        }
    }
}

pub(crate) fn production_batch_driver_can_only_call_audited_compat_helpers() {
    let text =
        fs::read_to_string(repo_root().join("src/batch.rs")).expect("read production batch driver");
    let production = text
        .split("#[cfg(test)]")
        .next()
        .expect("batch module has a production section");
    let allowed = [
        "BATCH_RESULT_FILE_ENV",
        "DUMP_SOURCE_DIRECTORY_ENV",
        "canonicalize_path",
        "relative_test_path",
        "repo_local_elisp_load_path",
    ];
    let reference =
        regex::Regex::new(r"compat::([A-Za-z0-9_]+)").expect("compile compat-reference pattern");
    for capture in reference.captures_iter(production) {
        let helper = &capture[1];
        assert!(
            allowed.contains(&helper),
            "production batch driver calls unaudited compat helper `{helper}`"
        );
    }
    for banned in [
        concat!("load_oracle_", "local_config"),
        concat!("oracle_helper_", "path"),
        concat!("emacs_", "binary"),
        concat!("Command::", "new"),
    ] {
        assert!(
            !production.contains(banned),
            "production batch driver contains oracle execution token `{banned}`"
        );
    }
}

pub(crate) fn runtime_code_does_not_shell_out_to_oracle_emacs() {
    let runtime_files = [
        "src/lisp/eval.rs",
        "src/lisp/primitives.rs",
        "src/buffer.rs",
        "src/main.rs",
        "src/lib.rs",
    ];
    let banned_tokens = [
        concat!("load_oracle_", "local_config()"),
        concat!("load_oracle_", "local_config("),
        concat!("oracle_helper_", "path("),
        concat!("lcms_", "oracle("),
        concat!("compat_lcms2_", "available("),
        concat!("Command::new", "(&local.emacs_binary)"),
        concat!("provided_features.push", "(\"lcms2\")"),
    ];

    for relative in runtime_files {
        let text =
            fs::read_to_string(repo_root().join(relative)).expect("read runtime anti-cheat file");
        for banned in banned_tokens {
            assert!(
                !text.contains(banned),
                "{relative} unexpectedly contains banned oracle-runtime token `{banned}`"
            );
        }
    }
}

pub(crate) fn runtime_does_not_publish_generated_or_compat_loaddefs() {
    let runtime_files = [
        "src/lisp/eval.rs",
        "src/lisp/eval/bindings.rs",
        "src/lisp/eval/macros.rs",
        "src/lisp/eval/runtime.rs",
        "src/lisp/primitives/dispatch.rs",
    ];
    let banned_tokens = [
        concat!("mod generated_", "autoloads"),
        concat!("generated_loaddefs_", "available"),
        concat!("generated_dumped_", "autoload("),
        concat!("generated_dumped_", "function("),
        concat!("generated_dumped_", "variable("),
        concat!("visible_builtin_", "autoload_function"),
        concat!("is_compat_", "preloaded_feature"),
    ];
    for relative in runtime_files {
        let text = fs::read_to_string(repo_root().join(relative))
            .expect("read loaddefs anti-cheat source file");
        for banned in banned_tokens {
            assert!(
                !text.contains(banned),
                "{relative} unexpectedly contains generated-loaddefs runtime token `{banned}`"
            );
        }
    }
}

pub(crate) fn runtime_keeps_interpreter_metadata_out_of_lisp_symbol_plists() {
    let banned_properties = [
        concat!("emaxx-", "struct-slot"),
        concat!("emaxx-", "struct-type"),
        concat!("emaxx-", "struct-slots"),
        concat!("emaxx-", "struct-defaults"),
        concat!("emaxx-", "struct-slot-descs"),
        concat!("emaxx-", "struct-sequence-type"),
        concat!("emaxx-", "struct-accessors"),
        concat!("emaxx-", "gv-setter"),
        concat!("emaxx-", "gv-setter-handler"),
        concat!("emaxx-", "function-arglist"),
        concat!("emaxx-", "cl-defgeneric-implicit"),
        concat!("emaxx-", "cl-defgeneric-lambda-list"),
        concat!("emaxx-", "cl-defgeneric-documentation"),
        concat!("emaxx-", "cl-defgeneric-argument-precedence-order"),
        concat!("emaxx-", "cl-defmethod-specializers"),
        concat!("emaxx-", "cl-defmethod-introspection"),
        concat!("emaxx-", "cl-defmethod-documentation"),
    ];

    for path in facade_gate_files() {
        if path
            .components()
            .any(|component| component.as_os_str() == "tests")
            || path.file_name().is_some_and(|name| name == "tests.rs")
        {
            continue;
        }
        let text = fs::read_to_string(&path).expect("read Lisp runtime source file");
        for property in banned_properties {
            let property_pattern = regex::Regex::new(&format!(
                r"(?:^|[^A-Za-z0-9_-]){}(?:[^A-Za-z0-9_-]|$)",
                regex::escape(property)
            ))
            .expect("compile internal-property pattern");
            assert!(
                !property_pattern.is_match(&text),
                "{} exposes interpreter metadata through Lisp property `{property}`",
                path.strip_prefix(repo_root()).unwrap_or(&path).display()
            );
        }
    }

    assert!(
        !repo_root().join("src/lisp/eval/preload.rs").exists(),
        "the deleted compatibility preload module must not return"
    );
}

pub(crate) fn runtime_does_not_reintroduce_removed_private_lisp_state() {
    let banned_names = [
        concat!("emaxx", "--visited-remote-prefix"),
        concat!("emaxx", "--active-minibuffer"),
        concat!("emaxx", "--active-minibuffer-window"),
        concat!("emaxx", "--minibuffer-depth"),
        concat!("emaxx", "--minibuffer-prompt"),
        concat!("emaxx-", "root-window"),
        concat!("emaxx-", "minibuffer-window"),
        concat!("emaxx-", "minibuffer-selected-window"),
        concat!("emaxx", "--treesit-linecol-cache"),
        concat!("emaxx", "--buffer-menu-entries"),
        concat!("emaxx", "--default-region-extract-function"),
        concat!("emaxx-default-region-", "insert-function"),
        concat!("emaxx", "--default-revert-buffer-function"),
    ];

    for path in facade_gate_files() {
        if path
            .components()
            .any(|component| component.as_os_str() == "tests")
            || path.file_name().is_some_and(|name| name == "tests.rs")
        {
            continue;
        }
        let text = fs::read_to_string(&path).expect("read Lisp runtime source file");
        for name in banned_names {
            assert!(
                !text.contains(name),
                "{} reintroduces removed private Lisp state `{name}`",
                path.strip_prefix(repo_root()).unwrap_or(&path).display()
            );
        }
    }
}

pub(crate) fn runtime_contains_no_project_private_lisp_namespace() {
    // This spelling gate is deliberately paired with the semantic ownership
    // gates: every native dispatch arm must be GNU C-owned, and the project
    // may ship no replacement Elisp.  A different prefix therefore cannot
    // turn a renamed bridge into an allowed implementation.
    let banned_namespaces = [concat!("emaxx", "--"), concat!("__", "emaxx")];
    for path in facade_gate_files() {
        if path
            .components()
            .any(|component| component.as_os_str() == "tests")
            || path.file_name().is_some_and(|name| name == "tests.rs")
        {
            continue;
        }
        let text = fs::read_to_string(&path).expect("read Lisp runtime source file");
        for namespace in banned_namespaces {
            assert!(
                !text.contains(namespace),
                "{} contains forbidden project-private Lisp namespace `{namespace}`",
                path.strip_prefix(repo_root()).unwrap_or(&path).display()
            );
        }
    }
}

pub(crate) fn runtime_does_not_reintroduce_removed_elisp_or_non_gnu_dispatch() {
    let banned_arms = [
        concat!("\"list-", "buffers\" =>"),
        concat!("\"list-buffers-", "noselect\" =>"),
        concat!("\"Buffer-menu-", "buffer\" =>"),
        concat!("\"treesit--linecol-", "cache\" =>"),
        concat!("\"treesit--linecol-cache-", "set\" =>"),
        concat!("\"treesit--linecol-", "at\" =>"),
        concat!("\"revert-", "buffer\" =>"),
        concat!("\"buffer-stale--default-", "function\" =>"),
        concat!("\"save-", "buffer\" =>"),
        concat!("\"ask-user-about-supersession-", "threat\" =>"),
        concat!("\"rename-visited-", "file\" =>"),
        concat!("\"file-name-", "split\" =>"),
        concat!("\"file-name-sans-", "extension\" =>"),
        concat!("\"file-name-sans-", "versions\" =>"),
        concat!("\"file-name-", "base\" =>"),
        concat!("\"file-name-", "extension\" =>"),
        concat!("\"make-temp-", "file\" =>"),
        concat!("\"advice-", "add\" =>"),
        concat!("\"advice-", "remove\" =>"),
        concat!("\"advice-member-", "p\" =>"),
        concat!("\"add-", "function\" =>"),
        concat!("\"remove-", "function\" =>"),
        concat!("\"define-", "advice\" =>"),
        concat!("\"emaxx", "--apply-around-advice\" =>"),
        concat!("\"emaxx", "--apply-after-advice\" =>"),
        concat!("\"emaxx", "--cl-generic-remove-loadhist-method\" =>"),
        concat!("\"get-char-code-", "property\" =>"),
        concat!("\"put-char-code-", "property\" =>"),
        concat!("\"char-code-property-", "description\" =>"),
        concat!("\"emaxx", "--general-category-description\" =>"),
    ];

    for path in facade_gate_files() {
        if path
            .components()
            .any(|component| component.as_os_str() == "tests")
            || path.file_name().is_some_and(|name| name == "tests.rs")
        {
            continue;
        }
        let text = fs::read_to_string(&path).expect("read Lisp runtime source file");
        for arm in banned_arms {
            assert!(
                !text.contains(arm),
                "{} reintroduces forbidden native dispatch arm `{arm}`",
                path.strip_prefix(repo_root()).unwrap_or(&path).display()
            );
        }
    }
}

pub(crate) fn runtime_native_dispatch_calls_only_configured_gnu_c_primitives() {
    // Calling `primitives::call` bypasses the ordinary Lisp function cell.
    // It is therefore valid only for a configured GNU C primitive.  This
    // gate specifically prevents repeats of the old timer scheduler calling
    // GNU-Elisp-owned `timerp` through the native dispatcher.
    // Match every spelling that reaches the native dispatcher with a literal
    // name: `primitives::call(`, `super::call(`, bare `call(` under a glob
    // import, and any future `.call(` method route.  The old pattern only
    // knew the fully-qualified spelling, so a dispatch module's
    // `super::call(interp, "buffer-narrowed-p", ...)` -- a Lisp-owned name
    // natively dispatched in the mode-line path -- sat in its blind spot
    // (finding 67, reported by the second audit).
    let direct_native_call = regex::Regex::new(r#"(?s)\bcall\s*\(\s*[^,()]+,\s*"([^"]+)""#)
        .expect("compile direct-native-call pattern");

    for path in facade_gate_files() {
        if path
            .components()
            .any(|component| component.as_os_str() == "tests")
            || path.file_name().is_some_and(|name| name == "tests.rs")
        {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read Lisp runtime source file");
        for capture in direct_native_call.captures_iter(&source) {
            let name = &capture[1];
            assert_eq!(
                crate::lisp::primitives::generated_gnu_c_primitive_available(name),
                Some(true),
                "{} bypasses the Lisp function cell for non-C owner `{name}`",
                path.strip_prefix(repo_root()).unwrap_or(&path).display()
            );
        }
    }
}

#[derive(Clone, Copy)]
struct NativeCFastPathContract {
    primitive: &'static str,
    owner: &'static str,
    contract_test: &'static str,
}

const EXACT_NATIVE_C_FAST_PATHS: &[NativeCFastPathContract] = &[
    NativeCFastPathContract {
        primitive: "stringp",
        owner: "data.c:Fstringp",
        contract_test: "native_stringp_is_a_tag_test_without_an_active_runtime",
    },
    NativeCFastPathContract {
        primitive: "<",
        owner: "data.c:Flss",
        contract_test: "native_numeric_comparisons_follow_data_c_fixnum_path",
    },
    NativeCFastPathContract {
        primitive: "<=",
        owner: "data.c:Fleq",
        contract_test: "native_numeric_comparisons_follow_data_c_fixnum_path",
    },
    NativeCFastPathContract {
        primitive: "=",
        owner: "data.c:Feqlsign",
        contract_test: "native_numeric_comparisons_follow_data_c_fixnum_path",
    },
    NativeCFastPathContract {
        primitive: ">",
        owner: "data.c:Fgtr",
        contract_test: "native_numeric_comparisons_follow_data_c_fixnum_path",
    },
    NativeCFastPathContract {
        primitive: ">=",
        owner: "data.c:Fgeq",
        contract_test: "native_numeric_comparisons_follow_data_c_fixnum_path",
    },
    NativeCFastPathContract {
        primitive: "apply",
        owner: "eval.c:Fapply",
        contract_test: "native_apply_spreads_the_final_list_into_funcall_words",
    },
    NativeCFastPathContract {
        primitive: "assq",
        owner: "fns.c:Fassq",
        contract_test: "native_assq_uses_the_fns_c_cons_walk",
    },
    NativeCFastPathContract {
        primitive: "atom",
        owner: "data.c:Fatom",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "bare-symbol-p",
        owner: "data.c:Fbare_symbol_p",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "car",
        owner: "data.c:Fcar",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "car-safe",
        owner: "data.c:Fcar_safe",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "cdr",
        owner: "data.c:Fcdr",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "cdr-safe",
        owner: "data.c:Fcdr_safe",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "cons",
        owner: "alloc.c:Fcons",
        contract_test: "native_cons_uses_one_two_word_body",
    },
    NativeCFastPathContract {
        primitive: "consp",
        owner: "data.c:Fconsp",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "eq",
        owner: "data.c:Feq",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "eql",
        owner: "fns.c:Feql",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "funcall",
        owner: "eval.c:Ffuncall",
        contract_test: "native_funcall_dispatches_builtin_on_the_word_abi",
    },
    NativeCFastPathContract {
        primitive: "get",
        owner: "fns.c:Fget",
        contract_test: "native_get_follows_fns_c_word_path",
    },
    NativeCFastPathContract {
        primitive: "identity",
        owner: "fns.c:Fidentity",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "length",
        owner: "fns.c:Flength",
        contract_test: "native_length_uses_fns_c_list_traversal",
    },
    NativeCFastPathContract {
        primitive: "list",
        owner: "alloc.c:Flist",
        contract_test: "native_list_is_the_alloc_c_reverse_cons_loop",
    },
    NativeCFastPathContract {
        primitive: "listp",
        owner: "data.c:Flistp",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "make-closure",
        owner: "alloc.c:Fmake_closure",
        contract_test: "native_make_closure_follows_alloc_c_copy_contract",
    },
    NativeCFastPathContract {
        primitive: "mapcar",
        owner: "fns.c:Fmapcar",
        contract_test: "native_mapcar_follows_fns_c_list_branch",
    },
    NativeCFastPathContract {
        primitive: "maphash",
        owner: "fns.c:Fmaphash",
        contract_test: "native_maphash_calls_each_live_slot_through_funcall",
    },
    NativeCFastPathContract {
        primitive: "memq",
        owner: "fns.c:Fmemq",
        contract_test: "native_memq_uses_the_fns_c_cons_walk",
    },
    NativeCFastPathContract {
        primitive: "nlistp",
        owner: "data.c:Fnlistp",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "nreverse",
        owner: "fns.c:Fnreverse",
        contract_test: "native_nreverse_follows_fns_c_list_branch",
    },
    NativeCFastPathContract {
        primitive: "null",
        owner: "data.c:Fnull",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "plist-member",
        owner: "fns.c:Fplist_member",
        contract_test: "native_plist_member_follows_fns_c_pair_traversal",
    },
    NativeCFastPathContract {
        primitive: "symbol-value",
        owner: "data.c:Fsymbol_value",
        contract_test: "native_symbol_value_and_type_of_follow_data_c",
    },
    NativeCFastPathContract {
        primitive: "symbolp",
        owner: "data.c:Fsymbolp",
        contract_test: "direct_word_subrs_use_the_gnu_c_fast_and_error_paths",
    },
    NativeCFastPathContract {
        primitive: "type-of",
        owner: "data.c:Ftype_of",
        contract_test: "native_symbol_value_and_type_of_follow_data_c",
    },
];

/// Deliberate semantic differences from GNU C require Ray's explicit approval
/// plus a regression test and a defensible written reason.  This list is empty
/// by default; adding an entry makes the exception visible to code review and
/// to the executable gate instead of hiding it inside a fast path.
struct ApprovedNativeCDeviation {
    primitive: &'static str,
    approval: &'static str,
    justification: &'static str,
    contract_test: &'static str,
}

const APPROVED_NATIVE_C_DEVIATIONS: &[ApprovedNativeCDeviation] = &[];

pub(crate) fn native_comp_fast_paths_are_audited_against_gnu_c() {
    use std::collections::BTreeSet;

    // Executable negative controls: a source inventory alone did not catch
    // the old parameter-name and vector-shape false positives.
    use crate::lisp::types::{Env, Value};
    let mut interpreter = crate::lisp::eval::Interpreter::new();
    let mut environment = Env::new();
    let lambda = Value::lambda(
        std::rc::Rc::new(["vals", "start", "end"].map(Into::into).to_vec()),
        std::rc::Rc::new(Vec::new()),
        std::rc::Rc::new(std::cell::RefCell::new(Env::new())),
    );
    let vector = Value::vector([
        Value::string("x"),
        Value::Integer(0),
        Value::Integer(1),
        Value::Nil,
    ]);
    assert!(
        crate::lisp::primitives::string_like(&vector).is_none(),
        "CHECK_STRING must not reinterpret an ordinary vector as a string",
    );
    for (name, value, expected) in [
        ("byte-code-function-p", lambda, Value::Nil),
        ("stringp", vector.clone(), Value::Nil),
        ("documentation-stringp", vector, Value::Nil),
        ("char-or-string-p", Value::Integer(0x11_0000), Value::T),
    ] {
        assert_eq!(
            crate::lisp::primitives::call(&mut interpreter, name, &[value], &mut environment,)
                .expect("GNU C type predicate"),
            expected,
            "{name} must inspect GNU object tags, not names or payload shapes",
        );
    }

    let runtime_path = repo_root().join("src/lisp/native_comp/runtime.rs");
    let runtime = fs::read_to_string(&runtime_path).expect("read native runtime source");
    let comments =
        regex::Regex::new(r"(?s:/\*.*?\*/)|(?m://.*$)").expect("compile Rust-comment pattern");
    let production = comments.replace_all(&runtime, "");
    let string_literal =
        regex::Regex::new(r#""([^"\\]+)""#).expect("compile native fast-path literal pattern");

    let mut audited_source = String::new();
    for (start, end) in [
        ("fn invoke_context_free_subr", "pub(crate) fn invoke_subr"),
        ("pub(crate) fn invoke_subr", "fn slow_unary_subr"),
        ("fn native_subr_address", "enum DirectFuncallTarget"),
    ] {
        let body = production
            .split_once(start)
            .unwrap_or_else(|| panic!("native runtime lost audited boundary `{start}`"))
            .1
            .split_once(end)
            .unwrap_or_else(|| panic!("native runtime lost audited boundary `{end}`"))
            .0;
        audited_source.push_str(body);
    }
    let actual = string_literal
        .captures_iter(&audited_source)
        .map(|capture| capture[1].to_owned())
        .filter(|name| {
            crate::lisp::primitives::generated_gnu_c_primitive_available(name) == Some(true)
        })
        .collect::<BTreeSet<_>>();

    let gnu_root = repo_root().join("../emacs/src");
    for test in [
        "native_symbol_value_errors_preserve_the_original_symbol",
        "native_assq_preserves_uninterned_lexical_binding_identity",
        "native_symbol_value_checks_symbol_before_reading_the_cell",
        "native_byte_code_function_p_checks_closure_and_code_tags",
        "native_string_type_predicates_do_not_read_payloads",
        "native_string_type_predicates_reject_vector_spoofing",
        "native_string_type_predicates_follow_gnu_array_and_character_classes",
    ] {
        assert!(
            runtime.contains(&format!("fn {test}")),
            "native runtime lost its required GNU C contract `{test}`"
        );
    }
    let mut declared = BTreeSet::new();
    for (file, tests) in [
        (
            "src/lisp/primitives/tests.rs",
            &[
                "intern_retains_the_supplied_name_and_does_not_replace_it_on_a_hit",
                "intern_uses_gnu_name_copy_and_type_check_boundaries",
            ][..],
        ),
        (
            "src/lisp/eval/tests/eval_03.rs",
            &[
                "eval_lambda_trims_unused_lexical_context_unless_marker_requests_it",
                "interpreted_closure_print_circle_tracks_the_closure_identity",
            ][..],
        ),
    ] {
        let source = fs::read_to_string(repo_root().join(file)).expect("read C contract tests");
        for test in tests {
            assert!(
                source.contains(&format!("fn {test}")),
                "missing C contract {test}"
            );
        }
    }
    for contract in EXACT_NATIVE_C_FAST_PATHS {
        assert!(
            declared.insert(contract.primitive),
            "native C fast-path contract is duplicated for `{}`",
            contract.primitive
        );
        assert_eq!(
            crate::lisp::primitives::generated_gnu_c_primitive_available(contract.primitive),
            Some(true),
            "native fast path `{}` is not an available GNU C primitive",
            contract.primitive
        );
        let (file, function) = contract.owner.split_once(':').unwrap_or_else(|| {
            panic!(
                "native fast path `{}` has malformed GNU owner `{}`",
                contract.primitive, contract.owner
            )
        });
        let owner = fs::read_to_string(gnu_root.join(file)).unwrap_or_else(|error| {
            panic!(
                "read GNU owner {} for `{}`: {error}",
                contract.owner, contract.primitive
            )
        });
        assert!(
            owner.contains(function),
            "GNU owner {} for native fast path `{}` no longer exists",
            contract.owner,
            contract.primitive
        );
        assert!(
            runtime.contains(&format!("fn {}", contract.contract_test)),
            "native fast path `{}` lacks declared contract test `{}`",
            contract.primitive,
            contract.contract_test
        );
    }
    for exception in APPROVED_NATIVE_C_DEVIATIONS {
        assert!(
            declared.insert(exception.primitive),
            "approved native C deviation duplicates `{}`",
            exception.primitive
        );
        assert!(
            exception.approval.starts_with("Ray approved "),
            "native C deviation `{}` lacks Ray's explicit dated approval",
            exception.primitive
        );
        assert!(
            !exception.justification.trim().is_empty(),
            "native C deviation `{}` lacks a written justification",
            exception.primitive
        );
        assert!(
            runtime.contains(&format!("fn {}", exception.contract_test)),
            "native C deviation `{}` lacks regression test `{}`",
            exception.primitive,
            exception.contract_test
        );
    }
    let expected = declared
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual, expected,
        "native C fast paths changed without updating the exact-contract or explicitly-approved-deviation inventory"
    );
}

pub(crate) fn bare_runtime_does_not_fabricate_gnu_elisp_owned_variable_values() {
    use crate::lisp::eval::Interpreter;
    use crate::lisp::types::{Env, Value};

    let interpreter = Interpreter::new();
    let environment = Env::new();
    assert_eq!(
        interpreter.lookup_var("region-extract-function", &environment),
        Some(Value::Nil),
        "casefiddle.c creates region-extract-function as nil before simple.el loads"
    );
    for elisp_owned in [
        "region-insert-function",
        "redisplay-highlight-region-function",
        "redisplay-unhighlight-region-function",
        "url-configuration-directory",
        "url-redirect-buffer",
        "url-retrieve-number-of-calls",
        "url-asynchronous",
        "url-dead-buffer-list",
    ] {
        assert_eq!(
            interpreter.lookup_var(elisp_owned, &environment),
            None,
            "bare runtime fabricated GNU Elisp-owned variable `{elisp_owned}`"
        );
    }
}

pub(crate) fn bare_runtime_rejects_gnu_elisp_owned_definition_forms() {
    use crate::lisp::eval::Interpreter;
    use crate::lisp::reader::Reader;
    use crate::lisp::types::{Env, LispError};

    let bare = Interpreter::new();
    assert!(
        !bare.has_feature("ert"),
        "bare runtime pre-provided the GNU emacs-lisp/ert.el feature"
    );
    assert!(
        matches!(
            bare.lookup_function("read-key", &Env::new()),
            Err(LispError::VoidFunction(ref missing)) if missing == "read-key"
        ),
        "bare runtime unexpectedly owned GNU subr.el function `read-key`"
    );

    // GNU 30.2 owns these callables in byte-run.el, subr.el, env.el, help.el,
    // files.el, minibuffer.el, keymap.el, emacs-lisp/lisp.el, cc-mode.el, custom.el,
    // cl-macs.el, cl-generic.el, ert.el, ert-x.el, and eieio.el.  A bare host may expose
    // only the GNU C primitives needed to load those files; it must not supply
    // an alternate Rust definition or a pre-provided feature that suppresses
    // the real owner.
    for (name, source) in [
        ("defun", "(defun ownership-probe () nil)"),
        ("when", "(when t nil)"),
        ("rassq-delete-all", "(rassq-delete-all nil nil)"),
        ("assq-delete-all", "(assq-delete-all nil nil)"),
        ("assoc-delete-all", "(assoc-delete-all nil nil)"),
        ("version<=", "(version<= \"1\" \"2\")"),
        ("copy-tree", "(copy-tree nil)"),
        ("flatten-tree", "(flatten-tree nil)"),
        ("split-string", "(split-string \"\")"),
        ("string-trim-left", "(string-trim-left \"\")"),
        ("string-trim-right", "(string-trim-right \"\")"),
        ("shell-quote-argument", "(shell-quote-argument \"x\")"),
        ("setenv", "(setenv \"OWNERSHIP_PROBE\" \"x\")"),
        ("format-prompt", "(format-prompt \"Prompt\" nil)"),
        ("file-relative-name", "(file-relative-name \"a\")"),
        ("locate-user-emacs-file", "(locate-user-emacs-file \"a\")"),
        ("substitute-command-keys", "(substitute-command-keys \"x\")"),
        (
            "display-warning",
            "(display-warning 'ownership-probe \"x\")",
        ),
        ("keymap-set-after", "(keymap-set-after nil \"x\" nil)"),
        (
            "defvar-keymap",
            "(defvar-keymap ownership-probe-map :doc \"ownership probe\")",
        ),
        ("down-list", "(down-list)"),
        ("up-list", "(up-list)"),
        ("beginning-of-defun", "(beginning-of-defun)"),
        ("end-of-defun", "(end-of-defun)"),
        ("region-active-p", "(region-active-p)"),
        ("deactivate-mark", "(deactivate-mark)"),
        ("c-mode", "(c-mode)"),
        ("seq-subseq", "(seq-subseq nil 0)"),
        ("seq-some", "(seq-some #'identity nil)"),
        ("visual-line-mode", "(visual-line-mode)"),
        ("forward-sexp", "(forward-sexp)"),
        ("font-lock-add-keywords", "(font-lock-add-keywords nil nil)"),
        ("facep", "(facep 'default)"),
        ("face-list", "(face-list)"),
        ("face-attribute", "(face-attribute 'default :foreground)"),
        (
            "set-face-attribute",
            "(set-face-attribute 'default nil :foreground \"black\")",
        ),
        ("face-equal", "(face-equal 'default 'default)"),
        (
            "face-differs-from-default-p",
            "(face-differs-from-default-p 'default)",
        ),
        ("emacs-version", "(emacs-version)"),
        (
            "defcustom",
            "(defcustom ownership-probe nil \"ownership probe\")",
        ),
        (
            "custom-declare-variable",
            "(custom-declare-variable 'ownership-probe nil \"ownership probe\")",
        ),
        ("defface", "(defface ownership-probe-face nil \"probe\")"),
        (
            "define-obsolete-face-alias",
            "(define-obsolete-face-alias 'old-probe-face 'default \"30.2\")",
        ),
        ("cl--find-class", "(cl--find-class 'ownership-probe)"),
        ("cl-functionp", "(cl-functionp nil)"),
        ("cl-proclaim", "(cl-proclaim '(special ownership-probe))"),
        ("cl-defstruct", "(cl-defstruct ownership-probe value)"),
        ("cl-deftype", "(cl-deftype ownership-probe () t)"),
        ("cl-typep", "(cl-typep nil t)"),
        (
            "cl-symbol-macrolet",
            "(cl-symbol-macrolet ((ownership-probe nil)) ownership-probe)",
        ),
        ("cl-letf", "(cl-letf ((ownership-probe nil)) nil)"),
        ("cl-struct-define", "(cl-struct-define)"),
        (
            "cl-old-struct-compat-mode",
            "(cl-old-struct-compat-mode -1)",
        ),
        ("cl-prin1", "(cl-prin1 nil)"),
        ("cl-defgeneric", "(cl-defgeneric ownership-probe (value))"),
        ("pcase", "(pcase nil (_ nil))"),
        ("ert-deftest", "(ert-deftest ownership-probe () nil)"),
        ("should-error", "(should-error nil)"),
        ("ert-simulate-command", "(ert-simulate-command '(ignore))"),
        ("defclass", "(defclass ownership-probe nil nil)"),
    ] {
        let form = Reader::new(source)
            .read()
            .expect("parse ownership probe")
            .expect("ownership probe contains one form");
        let result = Interpreter::new().eval(&form, &mut Env::new());
        assert!(
            matches!(result, Err(LispError::VoidFunction(ref missing)) if missing == name),
            "bare runtime unexpectedly owned GNU Elisp form `{name}`: {result:?}"
        );
    }
}

pub(crate) fn tty_frontend_does_not_reintroduce_silent_fallback_fabrications() {
    // Removed 2026-08-18: a native file visit that papered over a broken
    // `find-file', and a hand-painted GNU-shaped mode line covering render
    // failures.  Both kept the screen plausible while hiding breakage.
    let banned = [
        concat!("visit_file_", "directly"),
        concat!("-UUU:", "{modified}-"),
    ];
    for path in facade_gate_files() {
        if path.file_name().is_some_and(|name| name == "anti_cheat.rs") {
            continue;
        }
        let text = fs::read_to_string(&path).expect("read gate source file");
        for token in banned {
            assert!(
                !text.contains(token),
                "{} reintroduces a silent fallback fabrication `{token}`",
                path.strip_prefix(repo_root()).unwrap_or(&path).display()
            );
        }
    }
}

/// The oracle's reported `system-configuration'.
fn oracle_reported_configuration(oracle: &Path) -> String {
    crate::compat::oracle_reported_configuration(oracle).expect("query oracle system-configuration")
}

/// The committed C-primitive manifest that is authoritative for the
/// oracle build in front of us.  Each supported platform pins its own
/// (docs/oracle-build-contract.md): the Darwin NS build's surface and a
/// Linux no-window-system build's surface genuinely differ (ns-*, image
/// and GUI subrs), so byte identity is only meaningful per platform.  An
/// oracle from any other configuration is out of contract until its
/// manifest is generated and pinned here.
fn platform_c_manifest(reported_configuration: &str) -> &'static str {
    if reported_configuration.contains("apple-darwin") {
        "src/lisp/primitives/generated_gnu_c_primitives.rs"
    } else if reported_configuration.contains("linux-gnu") {
        "src/lisp/primitives/generated_gnu_c_primitives_linux.rs"
    } else {
        panic!(
            "no pinned manifest for an oracle reporting \
             `{reported_configuration}'; supported contracts are the Darwin \
             NS build and the Linux build -- see docs/oracle-build-contract.md"
        );
    }
}

pub(crate) fn gnu_c_manifest_matches_fresh_regeneration() {
    // A hand edit to the generated manifest could reclassify an Elisp-owned
    // name as C-owned and unlock a native dispatch arm.  Regenerate from the
    // pinned sibling checkout and require byte identity against THIS
    // platform's committed manifest.
    let root = repo_root();
    let oracle = root.join("../emacs/src/emacs");
    assert!(
        oracle.exists(),
        "pinned GNU sibling checkout required for the manifest regeneration gate"
    );
    let reported_configuration = oracle_reported_configuration(&oracle);
    let committed_manifest = platform_c_manifest(&reported_configuration);
    let fresh_path =
        std::env::temp_dir().join(format!("emaxx-manifest-regen-{}.rs", std::process::id()));
    let status = std::process::Command::new(&oracle)
        .current_dir(root)
        .args([
            "-Q",
            "--batch",
            "-l",
            "compat/generate_gnu_c_primitive_manifest.el",
            "--eval",
            &format!(
                "(emaxx-generate-gnu-c-primitive-manifest \"../emacs/src\" {:?})",
                fresh_path.display().to_string()
            ),
        ])
        .status()
        .expect("run the GNU C manifest generator with the oracle binary");
    assert!(status.success(), "manifest generator failed");
    let fmt = std::process::Command::new("rustfmt")
        .args(["--edition", "2024"])
        .arg(&fresh_path)
        .status()
        .expect("run rustfmt on the regenerated manifest");
    assert!(fmt.success(), "rustfmt failed on the regenerated manifest");
    let fresh = fs::read_to_string(&fresh_path).expect("read regenerated manifest");
    let committed = fs::read_to_string(root.join(committed_manifest))
        .unwrap_or_else(|error| panic!("read committed manifest {committed_manifest}: {error}"));
    assert_eq!(
        fresh, committed,
        "committed GNU C manifest {committed_manifest} does not match fresh \
         regeneration from the pinned checkout ({reported_configuration})"
    );
    let _ = fs::remove_file(&fresh_path);
}

pub(crate) fn gnu_c_defsym_manifest_matches_fresh_regeneration() {
    // DEFSYM names are interned into the production obarray during image
    // construction.  A hand edit could therefore fabricate a GNU-owned
    // symbol even though the DEFUN manifest and native-dispatch gates stay
    // clean.  Regenerate the source-level inventory from the pinned sibling
    // checkout and require byte identity.
    let root = repo_root();
    let oracle = root.join("../emacs/src/emacs");
    assert!(
        oracle.exists(),
        "pinned GNU sibling checkout required for the DEFSYM manifest regeneration gate"
    );
    let fresh_path =
        std::env::temp_dir().join(format!("emaxx-defsym-regen-{}.rs", std::process::id()));
    let status = std::process::Command::new(&oracle)
        .current_dir(root)
        .args([
            "-Q",
            "--batch",
            "-l",
            "compat/generate_gnu_c_defsym_manifest.el",
            "--eval",
            &format!(
                "(emaxx-generate-gnu-c-defsym-manifest \"../emacs/src\" {:?})",
                fresh_path.display().to_string()
            ),
        ])
        .status()
        .expect("run the GNU C DEFSYM manifest generator with the oracle binary");
    assert!(status.success(), "DEFSYM manifest generator failed");
    let fresh = fs::read_to_string(&fresh_path).expect("read regenerated DEFSYM manifest");
    let committed = fs::read_to_string(root.join("src/lisp/primitives/generated_gnu_c_defsyms.rs"))
        .expect("read committed DEFSYM manifest");
    assert_eq!(
        fresh, committed,
        "committed GNU C DEFSYM manifest does not match fresh regeneration from the pinned checkout"
    );
    let _ = fs::remove_file(&fresh_path);
}

pub(crate) fn gnu_c_bool_variable_manifest_matches_fresh_regeneration() {
    // This inventory controls data.c-style coercion in every variable store
    // path.  A hand edit could silently make an ordinary Lisp variable act
    // C-forwarded (or stop a real DEFVAR_BOOL from doing so), so compatibility
    // measurements must regenerate it from the pinned GNU sources first.
    let source_root = repo_root().join("../emacs/src");
    assert!(
        source_root.is_dir(),
        "pinned GNU sibling checkout required for the DEFVAR_BOOL manifest regeneration gate"
    );
    let pattern = regex::Regex::new(r#"DEFVAR_BOOL \("([^"]+)""#)
        .expect("compile DEFVAR_BOOL source pattern");
    let mut names = std::collections::BTreeSet::new();
    for entry in fs::read_dir(&source_root).expect("read pinned GNU src directory") {
        let path = entry.expect("read pinned GNU src entry").path();
        if path.extension().is_some_and(|extension| extension == "c") {
            // A few GNU C sources contain non-UTF-8 bytes in comments.
            let bytes = fs::read(&path).expect("read pinned GNU C source");
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
        "committed GNU C DEFVAR_BOOL manifest does not match fresh regeneration from the pinned checkout"
    );
}

pub(crate) fn builtin_arities_match_fresh_regeneration() {
    // The arities manifest feeds native dispatch arity checks and
    // interactive forms.  A hand edit could widen an arity or forge an
    // interactive spec, so regenerate from the committed C manifest plus
    // the pinned oracle and require byte identity, exactly like the C
    // manifest's own gate above.
    let root = repo_root();
    let oracle = root.join("../emacs/src/emacs");
    assert!(
        oracle.exists(),
        "pinned GNU sibling checkout required for the arities regeneration gate"
    );
    // The arities manifest is shared across the platform contracts: the
    // names emaxx dispatches carry identical arities on the Darwin NS and
    // Linux oracle builds (verified by generating from both and comparing
    // byte-for-byte).  Regenerate against whichever contracted oracle is
    // present and require identity with the one committed file; if the
    // platforms ever drift, this gate fails loudly on one of them and the
    // manifest splits per platform at that moment.  An oracle from any
    // other configuration stays out of contract.
    let reported_configuration = oracle_reported_configuration(&oracle);
    assert!(
        reported_configuration.contains("apple-darwin")
            || reported_configuration.contains("linux-gnu"),
        "arities regeneration requires a contracted oracle build (Darwin NS \
         or Linux); this oracle reports `{reported_configuration}'.  See \
         docs/oracle-build-contract.md"
    );
    let fresh_path =
        std::env::temp_dir().join(format!("emaxx-arities-regen-{}.rs", std::process::id()));
    let status = std::process::Command::new(&oracle)
        .current_dir(root)
        .args([
            "-Q",
            "--batch",
            "-l",
            "compat/generate_builtin_arities.el",
            "--eval",
            &format!(
                "(emaxx-generate-builtin-arities \"src/lisp\" {:?})",
                fresh_path.display().to_string()
            ),
        ])
        .status()
        .expect("run the arities generator with the oracle binary");
    assert!(status.success(), "arities generator failed");
    let fresh = fs::read_to_string(&fresh_path).expect("read regenerated arities");
    let committed =
        fs::read_to_string(root.join("src/lisp/primitives/generated_builtin_arities.rs"))
            .expect("read committed arities");
    assert_eq!(
        fresh, committed,
        "committed arities manifest does not match fresh regeneration from the pinned oracle"
    );
    let _ = fs::remove_file(&fresh_path);
}

/// Run every anti-cheat gate and collect the violations.  The gates were
/// once only `#[cfg(test)]' tests, so a measured run could be produced
/// from a tree that had never passed them; the compat harness now calls
/// this before it will write a summary (finding 24).  Each gate still has
/// a thin `#[test]' wrapper below, so `cargo test anti_cheat' behaves as
/// before.
pub fn enforce_all() -> Result<(), Vec<String>> {
    let gates: &[(&str, fn())] = &[
        (
            "repo_does_not_define_batch_report_delegation",
            repo_does_not_define_batch_report_delegation as fn(),
        ),
        (
            "production_batch_driver_can_only_call_audited_compat_helpers",
            production_batch_driver_can_only_call_audited_compat_helpers as fn(),
        ),
        (
            "runtime_code_does_not_shell_out_to_oracle_emacs",
            runtime_code_does_not_shell_out_to_oracle_emacs as fn(),
        ),
        (
            "runtime_does_not_publish_generated_or_compat_loaddefs",
            runtime_does_not_publish_generated_or_compat_loaddefs as fn(),
        ),
        (
            "runtime_keeps_interpreter_metadata_out_of_lisp_symbol_plists",
            runtime_keeps_interpreter_metadata_out_of_lisp_symbol_plists as fn(),
        ),
        (
            "runtime_does_not_reintroduce_removed_private_lisp_state",
            runtime_does_not_reintroduce_removed_private_lisp_state as fn(),
        ),
        (
            "runtime_contains_no_project_private_lisp_namespace",
            runtime_contains_no_project_private_lisp_namespace as fn(),
        ),
        (
            "runtime_does_not_reintroduce_removed_elisp_or_non_gnu_dispatch",
            runtime_does_not_reintroduce_removed_elisp_or_non_gnu_dispatch as fn(),
        ),
        (
            "runtime_native_dispatch_calls_only_configured_gnu_c_primitives",
            runtime_native_dispatch_calls_only_configured_gnu_c_primitives as fn(),
        ),
        (
            "native_comp_fast_paths_are_audited_against_gnu_c",
            native_comp_fast_paths_are_audited_against_gnu_c as fn(),
        ),
        (
            "bare_runtime_does_not_fabricate_gnu_elisp_owned_variable_values",
            bare_runtime_does_not_fabricate_gnu_elisp_owned_variable_values as fn(),
        ),
        (
            "bare_runtime_rejects_gnu_elisp_owned_definition_forms",
            bare_runtime_rejects_gnu_elisp_owned_definition_forms as fn(),
        ),
        (
            "tty_frontend_does_not_reintroduce_silent_fallback_fabrications",
            tty_frontend_does_not_reintroduce_silent_fallback_fabrications as fn(),
        ),
        (
            "gnu_c_manifest_matches_fresh_regeneration",
            gnu_c_manifest_matches_fresh_regeneration as fn(),
        ),
        (
            "gnu_c_defsym_manifest_matches_fresh_regeneration",
            gnu_c_defsym_manifest_matches_fresh_regeneration as fn(),
        ),
        (
            "gnu_c_bool_variable_manifest_matches_fresh_regeneration",
            gnu_c_bool_variable_manifest_matches_fresh_regeneration as fn(),
        ),
        (
            "builtin_arities_match_fresh_regeneration",
            builtin_arities_match_fresh_regeneration as fn(),
        ),
    ];
    let mut violations = Vec::new();
    for (name, gate) in gates {
        if let Err(panic) = std::panic::catch_unwind(gate) {
            let message = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|text| text.to_string()))
                .unwrap_or_else(|| "gate panicked without a message".into());
            violations.push(format!("{name}: {message}"));
        }
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

#[cfg(test)]
mod gate_tests {
    #[test]
    fn repo_does_not_define_batch_report_delegation() {
        super::repo_does_not_define_batch_report_delegation();
    }
    #[test]
    fn production_batch_driver_can_only_call_audited_compat_helpers() {
        super::production_batch_driver_can_only_call_audited_compat_helpers();
    }
    #[test]
    fn runtime_code_does_not_shell_out_to_oracle_emacs() {
        super::runtime_code_does_not_shell_out_to_oracle_emacs();
    }
    #[test]
    fn runtime_does_not_publish_generated_or_compat_loaddefs() {
        super::runtime_does_not_publish_generated_or_compat_loaddefs();
    }
    #[test]
    fn runtime_keeps_interpreter_metadata_out_of_lisp_symbol_plists() {
        super::runtime_keeps_interpreter_metadata_out_of_lisp_symbol_plists();
    }
    #[test]
    fn runtime_does_not_reintroduce_removed_private_lisp_state() {
        super::runtime_does_not_reintroduce_removed_private_lisp_state();
    }
    #[test]
    fn runtime_contains_no_project_private_lisp_namespace() {
        super::runtime_contains_no_project_private_lisp_namespace();
    }
    #[test]
    fn runtime_does_not_reintroduce_removed_elisp_or_non_gnu_dispatch() {
        super::runtime_does_not_reintroduce_removed_elisp_or_non_gnu_dispatch();
    }
    #[test]
    fn runtime_native_dispatch_calls_only_configured_gnu_c_primitives() {
        super::runtime_native_dispatch_calls_only_configured_gnu_c_primitives();
    }
    #[test]
    fn native_comp_fast_paths_are_audited_against_gnu_c() {
        super::native_comp_fast_paths_are_audited_against_gnu_c();
    }
    #[test]
    fn bare_runtime_does_not_fabricate_gnu_elisp_owned_variable_values() {
        super::bare_runtime_does_not_fabricate_gnu_elisp_owned_variable_values();
    }
    #[test]
    fn bare_runtime_rejects_gnu_elisp_owned_definition_forms() {
        super::bare_runtime_rejects_gnu_elisp_owned_definition_forms();
    }
    #[test]
    fn tty_frontend_does_not_reintroduce_silent_fallback_fabrications() {
        super::tty_frontend_does_not_reintroduce_silent_fallback_fabrications();
    }
    #[test]
    fn gnu_c_manifest_matches_fresh_regeneration() {
        super::gnu_c_manifest_matches_fresh_regeneration();
    }

    #[test]
    fn gnu_c_defsym_manifest_matches_fresh_regeneration() {
        super::gnu_c_defsym_manifest_matches_fresh_regeneration();
    }

    #[test]
    fn gnu_c_bool_variable_manifest_matches_fresh_regeneration() {
        super::gnu_c_bool_variable_manifest_matches_fresh_regeneration();
    }

    #[test]
    fn builtin_arities_match_fresh_regeneration() {
        super::builtin_arities_match_fresh_regeneration();
    }
}
