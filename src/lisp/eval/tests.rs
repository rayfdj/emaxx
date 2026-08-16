use super::*;
use crate::lisp::reader::Reader;
use std::path::PathBuf;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

fn panic_eval_error(interp: &mut Interpreter, error: LispError) -> ! {
    let rendered_error = match &error {
        LispError::SignalValue(value) => {
            crate::lisp::primitives::render_prin1_ephemeral(interp, value, &Vec::new())
                .unwrap_or_else(|_| error.to_string())
        }
        _ => error.to_string(),
    };
    let backtrace = interp
        .take_batch_error_backtrace()
        .map(|snapshot| {
            snapshot
                .frames
                .into_iter()
                .take(12)
                .map(|(_, function, args, _)| {
                    let mut frame = bounded_lisp_display(&function);
                    for arg in args.into_iter().take(5) {
                        frame.push(' ');
                        frame.push_str(&bounded_lisp_display(&arg));
                    }
                    frame
                })
                .collect::<Vec<_>>()
                .join(" <- ")
        })
        .unwrap_or_default();
    panic!("evaluation failed: {rendered_error}; Lisp backtrace: {backtrace}")
}

fn bounded_lisp_display(value: &Value) -> String {
    const LIMIT: usize = 120;
    let rendered = value
        .to_string()
        .chars()
        .flat_map(char::escape_debug)
        .collect::<String>();
    let mut chars = rendered.chars();
    let prefix = chars.by_ref().take(LIMIT).collect::<String>();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn eval_str_bare(src: &str) -> Value {
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let forms = Reader::new(src).read_all().unwrap();
    let mut result = Value::Nil;
    for form in &forms {
        // GNU's reader interns every symbol it reads, so `intern-soft'
        // must hit symbols that only occur in test source.
        interp.intern_symbols_in_value(form);
        result = interp
            .eval(form, &mut env)
            .unwrap_or_else(|error| panic_eval_error(&mut interp, error));
    }
    result
}

/// Evaluate ordinary Elisp test forms after executing GNU's real early Lisp
/// owners in their `loadup.el` order.  A user-visible GNU process has these
/// definitions in its dumped image; loading the upstream files here preserves
/// that ownership without restoring any Rust fallback.  Tests that explicitly
/// exercise the file-less C/Rust host must call `eval_str_bare` instead.
fn eval_str(src: &str) -> Value {
    eval_str_with_gnu_early_lisp(src)
}

fn eval_str_with(interp: &mut Interpreter, src: &str) -> Value {
    let mut env: Env = Vec::new();
    let forms = Reader::new(src).read_all().unwrap();
    let mut result = Value::Nil;
    for form in &forms {
        // GNU's reader interns every symbol it reads, so `intern-soft'
        // must hit symbols that only occur in test source.
        interp.intern_symbols_in_value(form);
        result = interp
            .eval(form, &mut env)
            .unwrap_or_else(|error| panic_eval_error(interp, error));
    }
    result
}

fn eval_str_with_upstream_load_path(src: &str) -> Value {
    let mut interp = Interpreter::new();
    interp.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
    );
    eval_str_with(&mut interp, src)
}

fn eval_str_with_upstream_batch(src: &str) -> Value {
    // GNU's batch image executes these same GNU Lisp owners from its dump.
    // Use their compiled `.elc' representation so each ownership-sensitive
    // test does not pay the unrelated source bootstrap cost.
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    eval_str_with(&mut interp, src)
}

fn eval_str_with_upstream_batch_feature(feature: &str, src: &str) -> Value {
    eval_str_with_upstream_batch_features(&[feature], src)
}

fn upstream_batch_interpreter_with_features(
    features: &[&str],
) -> (crate::test_support::HostTestPermit, Interpreter) {
    let permit = crate::test_support::acquire_host_test_permit();
    let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
    for feature in features {
        eval_str_with(&mut interp, &format!("(require '{feature})"));
    }
    (permit, interp)
}

fn eval_str_with_upstream_batch_features(features: &[&str], src: &str) -> Value {
    let (_permit, mut interp) = upstream_batch_interpreter_with_features(features);
    eval_str_with(&mut interp, src)
}

fn eval_str_with_gnu_early_lisp(src: &str) -> Value {
    let mut interp = gnu_early_lisp_interpreter();
    eval_str_with(&mut interp, src)
}

fn gnu_early_lisp_interpreter() -> Interpreter {
    crate::test_support::initialized_gnu_early_lisp_interpreter()
}

fn load_gnu_batch_runtime(interp: &mut Interpreter) {
    crate::test_support::replace_with_gnu_batch_runtime(interp);
}

fn upstream_emacs_repo() -> PathBuf {
    crate::compat::project_root().join("../emacs")
}

fn assert_string_value(value: Value, expected: &str) {
    assert_eq!(primitives::string_text(&value).unwrap(), expected);
}

fn assert_string_list(value: Value, expected: &[&str]) {
    let items = value.to_vec().unwrap();
    assert_eq!(items.len(), expected.len());
    for (item, expected) in items.iter().zip(expected.iter()) {
        assert_eq!(primitives::string_text(item).unwrap(), *expected);
    }
}

fn run_with_large_stack(test: impl FnOnce() + Send + 'static) {
    let permit = crate::test_support::acquire_host_test_permit();
    thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit;
            test();
        })
        .unwrap()
        .join()
        .unwrap();
}

fn run_large_stack_test(test_fn: fn()) {
    let permit = crate::test_support::acquire_host_test_permit();
    thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit;
            test_fn();
        })
        .unwrap()
        .join()
        .unwrap();
}

fn run_exclusive_with_large_stack(test: impl FnOnce() + Send + 'static) {
    let permit = crate::test_support::acquire_exclusive_host_test_permit();
    thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit;
            test();
        })
        .unwrap()
        .join()
        .unwrap();
}

mod eval_01;
mod eval_02;
mod eval_03;
mod eval_04;
mod eval_05;
