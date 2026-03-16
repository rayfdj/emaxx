use std::fs;
use std::path::Path;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
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
