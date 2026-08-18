use super::*;
use crate::lisp::reader::Reader;
use flate2::Compression;
use flate2::write::GzEncoder;

fn make_compat_temp_file(interp: &mut Interpreter, env: &mut Env, prefix: &str) -> String {
    let absolute_prefix = std::env::temp_dir().join(prefix);
    call(
        interp,
        "make-temp-file-internal",
        &[
            Value::String(absolute_prefix.display().to_string().into()),
            Value::Nil,
            Value::String(String::new().into()),
            Value::Nil,
        ],
        env,
    )
    .expect("create compatibility fixture through GNU's C primitive")
    .as_string()
    .expect("temporary file path")
    .to_string()
}

fn run_with_large_stack(test: impl FnOnce() + Send + 'static) {
    let permit = crate::test_support::acquire_host_test_permit();
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit;
            test();
        })
        .expect("spawn large-stack test thread")
        .join()
        .expect("join large-stack test thread");
}

mod compat_01;
mod compat_02;
mod compat_03;
