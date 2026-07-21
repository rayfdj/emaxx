use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").expect("Cargo must provide OUT_DIR to build scripts");
    let out_dir = Path::new(&out_dir);
    let build_dir = out_dir
        .ancestors()
        .find(|path| path.file_name().is_some_and(|name| name == "build"))
        .expect("OUT_DIR must be below Cargo's build directory");
    let target_dir = build_dir
        .parent()
        .and_then(Path::parent)
        .expect("Cargo build directory must be below a target directory");
    println!(
        "cargo:rustc-env=EMAXX_COMPILED_TARGET_DIR={}",
        target_dir.display()
    );
}
