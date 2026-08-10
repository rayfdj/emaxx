use super::*;
use crate::lisp::reader::Reader;
use flate2::Compression;
use flate2::write::GzEncoder;

fn upstream_emacs_repo() -> PathBuf {
    crate::compat::project_root().join("../emacs")
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
