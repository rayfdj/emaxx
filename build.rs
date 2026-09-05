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
    // emacs.c sets `system-configuration' from EMACS_CONFIGURATION, the
    // canonical host triple autoconf's config.guess recorded when the build
    // was configured -- a build-time constant that keeps its kernel release
    // after the host updates.  Spell the triple the way config.guess does
    // for the same hardware, at build time.
    println!(
        "cargo:rustc-env=EMAXX_SYSTEM_CONFIGURATION={}",
        system_configuration_triple()
    );
    // comp.c uses the Lisp directory configured when the executable was
    // built, not the mutable Lisp source-directory variable. Our ordinary
    // source layout puts the unchanged GNU Lisp tree in the sibling emacs
    // checkout. Resolve symlinks now, never during native compilation.
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("Cargo provides its manifest directory");
    let lisp_directory = Path::new(&manifest)
        .parent()
        .expect("the project has a parent directory")
        .join("emacs/lisp");
    let configured_lisp_directory = match lisp_directory.canonicalize() {
        Ok(directory) => directory,
        // Building the executable does not require an installed Lisp tree;
        // retain the configured absolute path if it is not installed yet.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => lisp_directory,
        Err(error) => panic!("resolve configured Lisp directory: {error}"),
    };
    println!(
        "cargo:rustc-env=EMAXX_COMPILED_LISP_DIRECTORY={}",
        configured_lisp_directory.display()
    );
    println!("cargo:rerun-if-changed=build.rs");
}

fn system_configuration_triple() -> String {
    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_else(|_| env::consts::ARCH.to_string());
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_else(|_| env::consts::OS.to_string());
    let kernel_release = || {
        std::process::Command::new("uname")
            .arg("-r")
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
            .filter(|release| !release.is_empty())
            .unwrap_or_else(|| "0".to_string())
    };
    match os.as_str() {
        // config.guess: arm64-apple-darwin is canonicalized to aarch64.
        "macos" => format!("{arch}-apple-darwin{}", kernel_release()),
        // config.guess gives x86 hosts the "pc" vendor and everyone else
        // "unknown".
        "linux" if arch == "x86_64" || arch == "x86" => format!("{arch}-pc-linux-gnu"),
        "linux" => format!("{arch}-unknown-linux-gnu"),
        "freebsd" => format!("{arch}-unknown-freebsd{}", kernel_release()),
        "windows" => format!("{arch}-pc-windows-msvc"),
        other => format!("{arch}-{other}"),
    }
}
