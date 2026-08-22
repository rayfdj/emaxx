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

#[test]
fn repo_does_not_define_batch_report_delegation() {
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

#[test]
fn production_batch_driver_can_only_call_audited_compat_helpers() {
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

#[test]
fn runtime_code_does_not_shell_out_to_oracle_emacs() {
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

#[test]
fn runtime_does_not_publish_generated_or_compat_loaddefs() {
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

#[test]
fn runtime_keeps_interpreter_metadata_out_of_lisp_symbol_plists() {
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

#[test]
fn runtime_does_not_reintroduce_removed_private_lisp_state() {
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

#[test]
fn runtime_contains_no_project_private_lisp_namespace() {
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

#[test]
fn runtime_does_not_reintroduce_removed_elisp_or_non_gnu_dispatch() {
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

#[test]
fn runtime_native_dispatch_calls_only_configured_gnu_c_primitives() {
    // Calling `primitives::call` bypasses the ordinary Lisp function cell.
    // It is therefore valid only for a configured GNU C primitive.  This
    // gate specifically prevents repeats of the old timer scheduler calling
    // GNU-Elisp-owned `timerp` through the native dispatcher.
    let direct_native_call =
        regex::Regex::new(r#"(?s)(?:crate::lisp::)?primitives::call\s*\(\s*[^,]+,\s*"([^"]+)""#)
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

#[test]
fn bare_runtime_does_not_fabricate_gnu_elisp_owned_variable_values() {
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

#[test]
fn bare_runtime_rejects_gnu_elisp_owned_definition_forms() {
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

#[test]
fn tty_frontend_does_not_reintroduce_silent_fallback_fabrications() {
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

#[test]
fn gnu_c_manifest_matches_fresh_regeneration() {
    // A hand edit to the generated manifest could reclassify an Elisp-owned
    // name as C-owned and unlock a native dispatch arm.  Regenerate from the
    // pinned sibling checkout and require byte identity.
    let root = repo_root();
    let oracle = root.join("../emacs/src/emacs");
    assert!(
        oracle.exists(),
        "pinned GNU sibling checkout required for the manifest regeneration gate"
    );
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
    let committed =
        fs::read_to_string(root.join("src/lisp/primitives/generated_gnu_c_primitives.rs"))
            .expect("read committed manifest");
    assert_eq!(
        fresh, committed,
        "committed GNU C manifest does not match fresh regeneration from the pinned checkout"
    );
    let _ = fs::remove_file(&fresh_path);
}
