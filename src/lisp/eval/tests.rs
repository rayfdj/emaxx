use super::*;
use crate::lisp::reader::Reader;
use std::path::PathBuf;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

fn eval_str(src: &str) -> Value {
    let mut interp = Interpreter::new();
    let mut env: Env = Vec::new();
    let forms = Reader::new(src).read_all().unwrap();
    let mut result = Value::Nil;
    for form in &forms {
        result = interp.eval(form, &mut env).unwrap();
    }
    result
}

fn eval_str_with(interp: &mut Interpreter, src: &str) -> Value {
    let mut env: Env = Vec::new();
    let forms = Reader::new(src).read_all().unwrap();
    let mut result = Value::Nil;
    for form in &forms {
        result = interp.eval(form, &mut env).unwrap();
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
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
            .expect("upstream load path"),
        ..Default::default()
    };
    let mut interp = crate::batch::initialize_batch_interpreter(&options)
        .expect("initialize GNU-compatible batch interpreter");
    eval_str_with(&mut interp, src)
}

fn load_faces_compat(interp: &mut Interpreter) {
    let path = crate::compat::project_root().join("src/lisp/faces_compat.el");
    crate::lisp::load_file_strict(interp, &path).unwrap();
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
    thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(test)
        .unwrap()
        .join()
        .unwrap();
}

fn run_large_stack_test(test_fn: fn()) {
    thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(test_fn)
        .unwrap()
        .join()
        .unwrap();
}

mod eval_01;
mod eval_02;
mod eval_03;
mod eval_04;
mod eval_05;
