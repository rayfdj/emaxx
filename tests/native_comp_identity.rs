#![allow(clippy::unwrap_used)]

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

struct Fixture {
    relative_path: &'static str,
    coverage: &'static str,
    expected_artifact: bool,
}

const FIXTURES: &[Fixture] = &[
    Fixture {
        relative_path: "test/src/comp-resources/comp-test-45603.el",
        coverage: "lexical closures, mapcar lambda capture, aliases, and conditional function selection",
        expected_artifact: true,
    },
    Fixture {
        relative_path: "test/src/comp-resources/comp-test-funcs-dyn2.el",
        coverage: "the unchanged no-byte-compile file policy under dynamic binding",
        expected_artifact: false,
    },
    Fixture {
        relative_path: "test/src/comp-resources/comp-test-pure.el",
        coverage: "direct calls, recursion, arithmetic, and pure/impure relocation classification",
        expected_artifact: true,
    },
    Fixture {
        relative_path: "test/src/comp-resources/comp-test-funcs-dyn.el",
        coverage: "dynamic binding, fixed/optional/rest arguments, cl-loop, and cl-defun expansion",
        expected_artifact: true,
    },
    Fixture {
        relative_path: "test/lisp/emacs-lisp/comp-tests.el",
        coverage: "ERT and CL macro expansion, nested cleanup closures, filesystem control flow, and shared constants",
        expected_artifact: true,
    },
    Fixture {
        relative_path: "test/lisp/emacs-lisp/comp-cstr-tests.el",
        coverage: "constraint type conversion plus unions, intersections, negations, integer ranges, member sets, and conservative normalization",
        expected_artifact: true,
    },
    Fixture {
        relative_path: "test/src/comp-resources/comp-test-funcs.el",
        coverage: "broad opcode lowering: variables, aggregate primitives, argument ABIs, branches and jump tables, mutation, handlers and unwind, buffers, interactive forms, records, cyclic constants, non-ASCII names, and dead control flow",
        expected_artifact: true,
    },
    Fixture {
        relative_path: "test/src/comp-tests.el",
        coverage: "the full upstream native-compiler ERT suite definitions, resource orchestration, compiler options, diagnostics, asynchronous compilation, loading, and runtime assertions",
        expected_artifact: true,
    },
];

fn run_compiler(binary: &Path, source: &Path, home: &Path) -> Output {
    Command::new(binary)
        .args(["-Q", "--batch", "-f", "batch-native-compile"])
        .arg(source)
        .env("HOME", home)
        .output()
        .unwrap_or_else(|error| panic!("run {}: {error}", binary.display()))
}

fn assert_compiler_succeeded(binary: &Path, source: &Path, output: &Output) {
    assert!(
        output.status.success(),
        "{} failed to native-compile {}\nstdout:\n{}\nstderr:\n{}",
        binary.display(),
        source.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn artifact_prefix(source: &Path) -> String {
    format!(
        "{}-",
        source
            .file_stem()
            .and_then(OsStr::to_str)
            .expect("fixture has a UTF-8 file stem")
    )
}

fn artifacts_below(root: &Path, source: &Path) -> Vec<PathBuf> {
    if !root.exists() {
        return Vec::new();
    }
    let prefix = artifact_prefix(source);
    let mut artifacts = walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(walkdir::DirEntry::into_path)
        .filter(|path| {
            path.extension().is_some_and(|extension| extension == "eln")
                && path
                    .file_name()
                    .and_then(OsStr::to_str)
                    .is_some_and(|name| name.starts_with(&prefix))
        })
        .collect::<Vec<_>>();
    artifacts.sort();
    artifacts
}

fn only_artifact(mut artifacts: Vec<PathBuf>, compiler: &Path, source: &Path) -> PathBuf {
    assert_eq!(
        artifacts.len(),
        1,
        "{} produced {} artifacts for {} instead of exactly one: {artifacts:?}",
        compiler.display(),
        artifacts.len(),
        source.display(),
    );
    artifacts.pop().unwrap()
}

fn newly_created(before: &[PathBuf], after: Vec<PathBuf>) -> Vec<PathBuf> {
    after
        .into_iter()
        .filter(|path| !before.contains(path))
        .collect()
}

fn first_difference(left: &[u8], right: &[u8]) -> Option<usize> {
    left.iter()
        .zip(right)
        .position(|(left, right)| left != right)
        .or_else(|| (left.len() != right.len()).then_some(left.len().min(right.len())))
}

#[test]
#[ignore = "requires the sibling native-comp GNU build and intentionally compiles through two release-grade editors"]
fn unchanged_gnu_sources_produce_identical_native_artifacts() {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let gnu_root = project.join("../emacs");
    let gnu = gnu_root.join("src/emacs");
    let subject = PathBuf::from(env!("CARGO_BIN_EXE_emaxx"));
    let subject_cache = subject
        .parent()
        .and_then(Path::parent)
        .expect("Emaxx binary is inside a Cargo profile directory")
        .join("native-lisp");

    assert!(gnu.is_file(), "GNU oracle is missing: {}", gnu.display());

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock follows the Unix epoch")
        .as_nanos();
    let work = std::env::temp_dir().join(format!(
        "native-comp-identity-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&work).expect("create native-comp identity work directory");
    let home = work.join("home");
    let gnu_cache = home.join(".emacs.d/eln-cache");
    std::fs::create_dir_all(&gnu_cache).expect("create isolated GNU native-comp cache");

    for fixture in FIXTURES {
        let upstream = gnu_root.join(fixture.relative_path);
        let source = work.join(upstream.file_name().expect("fixture has a file name"));
        std::fs::copy(&upstream, &source).unwrap_or_else(|error| {
            panic!(
                "copy unchanged fixture {} to {}: {error}",
                upstream.display(),
                source.display()
            )
        });
        println!(
            "native-comp identity: {} bytes, {}: {}",
            std::fs::metadata(&source).expect("stat fixture").len(),
            source.file_name().unwrap().to_string_lossy(),
            fixture.coverage,
        );

        let gnu_before = artifacts_below(&gnu_cache, &source);
        let gnu_output = run_compiler(&gnu, &source, &home);
        assert_compiler_succeeded(&gnu, &source, &gnu_output);
        let gnu_artifacts = newly_created(&gnu_before, artifacts_below(&gnu_cache, &source));

        if !fixture.expected_artifact {
            assert!(
                gnu_artifacts.is_empty(),
                "GNU ignored the fixture's no-byte-compile policy: {gnu_artifacts:?}"
            );
            let subject_before = artifacts_below(&subject_cache, &source);
            let subject_output = run_compiler(&subject, &source, &home);
            assert_compiler_succeeded(&subject, &source, &subject_output);
            let subject_artifacts =
                newly_created(&subject_before, artifacts_below(&subject_cache, &source));
            assert!(
                subject_artifacts.is_empty(),
                "Emaxx ignored the fixture's no-byte-compile policy: {subject_artifacts:?}"
            );
            continue;
        }

        let gnu_artifact = only_artifact(gnu_artifacts, &gnu, &source);
        let reference = work.join(format!(
            "{}.gnu.eln",
            source.file_stem().unwrap().to_string_lossy()
        ));
        std::fs::copy(&gnu_artifact, &reference).expect("save GNU artifact outside its cache");
        std::fs::remove_file(&gnu_artifact)
            .expect("remove test-owned GNU artifact before Emaxx compilation");

        let subject_before = artifacts_below(&subject_cache, &source);
        let subject_output = run_compiler(&subject, &source, &home);
        assert_compiler_succeeded(&subject, &source, &subject_output);
        let subject_artifact = only_artifact(
            newly_created(&subject_before, artifacts_below(&subject_cache, &source)),
            &subject,
            &source,
        );
        let expected = std::fs::read(&reference).expect("read saved GNU artifact");
        let actual = std::fs::read(&subject_artifact).expect("read Emaxx artifact");
        if let Some(offset) = first_difference(&expected, &actual) {
            panic!(
                "native artifact differs for {} ({}) at byte {offset}; GNU size {}, Emaxx size {}; artifacts retained in {}",
                fixture.relative_path,
                fixture.coverage,
                expected.len(),
                actual.len(),
                work.display(),
            );
        }

        std::fs::remove_file(&subject_artifact).expect("remove test-owned Emaxx artifact");
        std::fs::remove_file(&reference).expect("remove saved GNU artifact");
    }

    std::fs::remove_dir_all(&work).expect("remove native-comp identity work directory");
}
